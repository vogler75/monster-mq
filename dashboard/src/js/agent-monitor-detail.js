// Agent Detail Monitor - Per-agent live monitoring

class AgentDetailMonitorManager {
    constructor() {
        this.client = new GraphQLDashboardClient();
        this.ws = null;
        this.subscriptionId = 0;
        this.reconnectTimer = null;
        this.activeLogType = 'tools';
        this.logEntries = { tools: [], mcp: [], llm: [], errors: [] };
        this.maxLogEntries = 500;
        this.isInternal = false;
        this.inboxTopic = '';
        this.responseTopics = [];
        this.taskEntries = [];
        this.responseEntries = [];
        this.maxTaskEntries = 100;
        this.maxResponseEntries = 100;
        this.activeTaskId = null;
        this.activeTaskState = null;
        this.sendInFlight = false;

        // Parse URL params
        const params = new URLSearchParams(window.location.search);
        this.org = params.get('org') || '';
        this.site = params.get('site') || '';
        this.agentName = params.get('agent') || '';
        this.agentBaseTopic = `a2a/v1/${this.org}/${this.site}/agents/${this.agentName}`;
        this.discoveryTopic = `a2a/v1/${this.org}/${this.site}/discovery/${this.agentName}`;
        this.inboxTopic = `${this.agentBaseTopic}/inbox`;
        this.responseTopics = [`${this.agentBaseTopic}/response`];

        this.init();
    }

    async init() {
        if (!this.agentName) {
            document.getElementById('agent-name').textContent = 'No agent specified';
            return;
        }

        document.getElementById('agent-name').textContent = this.agentName;
        this.setupLogTabs();
        this.setupTestConsole();
        this.setDefaultHistoryDates();

        await this.loadInitialData();
        this.connectWebSocket();
        window.registerPageCleanup(() => this.cleanup());
    }

    cleanup() {
        if (this.ws) { try { this.ws.close(); } catch(e) {} this.ws = null; }
        if (this.reconnectTimer) { clearTimeout(this.reconnectTimer); this.reconnectTimer = null; }
    }

    // ===================== Initial Data =====================

    async loadInitialData() {
        try {
            const [cardResult, healthResult, agentsResult] = await Promise.all([
                this.client.query(`query($topic: String!) { retainedMessage(topic: $topic, format: JSON) { topic payload } }`,
                    { topic: this.discoveryTopic }),
                this.client.query(`query($topic: String!) { retainedMessage(topic: $topic, format: JSON) { topic payload } }`,
                    { topic: this.agentBaseTopic + '/health' }),
                this.client.query(`query($name: String!) { agent(name: $name) { name cronPrompt } }`,
                    { name: this.agentName })
            ]);

            // Check if internal
            const matchedAgent = agentsResult?.agent;
            this.isInternal = !!matchedAgent;

            // Pre-fill test input with cron prompt if available
            if (matchedAgent && matchedAgent.cronPrompt) {
                const inputEl = document.getElementById('test-message');
                if (inputEl && !inputEl.value.trim()) {
                    inputEl.value = matchedAgent.cronPrompt;
                }
            }

            // Render agent card
            if (cardResult && cardResult.retainedMessage) {
                const card = this.parsePayload(cardResult.retainedMessage.payload);
                if (card) {
                    this.updateAgentTopicsFromCard(card);
                    this.renderHeader(card);
                }
            }

            // Render health
            if (healthResult && healthResult.retainedMessage) {
                const health = this.parsePayload(healthResult.retainedMessage.payload);
                if (health) this.renderHealth(health);
            }

            this.renderTypeBadge();
            this.renderTestTopics();
        } catch(e) {
            console.error('Error loading agent detail:', e);
        }
    }

    parsePayload(payload) {
        if (typeof payload !== 'string') return payload;
        try {
            return JSON.parse(payload);
        } catch(e) {
            return payload;
        }
    }

    // ===================== Rendering =====================

    renderHeader(card) {
        const setEl = (id, val) => { const el = document.getElementById(id); if (el) el.textContent = val; };
        setEl('agent-name', card.name || this.agentName);
        setEl('agent-description', card.description || '');
        setEl('agent-org-site', `${this.org} / ${this.site}`);
        setEl('agent-version', card.version ? `v${card.version}` : '');
        const pm = (card.provider || '') + (card.model ? ' / ' + card.model : '');
        setEl('agent-provider-model', pm || '');

        this.renderStatusBadge(card.status);
    }

    updateAgentTopicsFromCard(card) {
        const discoveredInboxTopic = typeof card?.url === 'string' && card.url.trim()
            ? card.url.trim()
            : `${this.agentBaseTopic}/inbox`;
        const outputTopics = Array.isArray(card?.outputTopics)
            ? card.outputTopics.filter(topic => typeof topic === 'string' && topic.trim()).map(topic => topic.trim())
            : [];

        this.inboxTopic = discoveredInboxTopic;
        this.responseTopics = outputTopics.length > 0
            ? [...new Set(outputTopics)]
            : [`${this.agentBaseTopic}/response`];
    }

    renderStatusBadge(status) {
        const el = document.getElementById('agent-status-badge');
        if (!el) return;
        const s = (status || 'unknown').toLowerCase();
        const cls = ['running','ready','stopped'].includes(s) ? s : 'unknown';
        el.innerHTML = `<span class="status-badge status-${cls}">${this.esc(s)}</span>`;
    }

    renderTypeBadge() {
        const el = document.getElementById('agent-type-badge');
        if (!el) return;
        const cls = this.isInternal ? 'type-internal' : 'type-external';
        const label = this.isInternal ? 'Internal' : 'External';
        el.innerHTML = `<span class="type-badge ${cls}">${label}</span>`;
    }

    renderHealth(health) {
        const setEl = (id, val) => { const el = document.getElementById(id); if (el) el.textContent = val; };
        setEl('health-status', health.status || '-');
        setEl('health-messages', health.messagesProcessed != null ? Number(health.messagesProcessed).toLocaleString() : '-');
        setEl('health-llm', health.llmCalls != null ? Number(health.llmCalls).toLocaleString() : '-');
        setEl('health-errors', health.errors != null ? Number(health.errors).toLocaleString() : '-');
        setEl('health-input-tokens', health.inputTokens != null ? Number(health.inputTokens).toLocaleString() : '-');
        setEl('health-output-tokens', health.outputTokens != null ? Number(health.outputTokens).toLocaleString() : '-');
        setEl('health-total-tokens', health.totalTokens != null ? Number(health.totalTokens).toLocaleString() : '-');
        setEl('health-timestamp', this.formatTimestamp(health.timestamp));

        // Update status badge from health
        this.renderStatusBadge(health.status);

        // Color the errors value red if > 0
        const errEl = document.getElementById('health-errors');
        if (errEl && health.errors > 0) errEl.style.color = 'var(--monster-red)';
    }

    // ===================== WebSocket =====================

    getWebSocketUrl() {
        if (window.brokerManager) return window.brokerManager.getWsEndpoint();
        const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
        const host = window.location.hostname;
        const port = window.location.port;
        return `${protocol}//${host}${port ? ':' + port : ''}/graphql`;
    }

    connectWebSocket() {
        if (this.ws) return;
        this.updateWsStatus('connecting');

        try {
            this.ws = new WebSocket(this.getWebSocketUrl(), 'graphql-transport-ws');

            this.ws.onopen = () => {
                const payload = {};
                const token = safeStorage.getItem('monstermq_token');
                if (token && token !== 'null') payload.authorization = `Bearer ${token}`;
                this.ws.send(JSON.stringify({ type: 'connection_init', payload }));
            };

            this.ws.onmessage = (ev) => {
                try { this.handleWsMessage(JSON.parse(ev.data)); } catch(e) {}
            };

            this.ws.onerror = () => this.updateWsStatus('disconnected');
            this.ws.onclose = () => {
                this.ws = null;
                this.updateWsStatus('disconnected');
                this.reconnectTimer = setTimeout(() => this.connectWebSocket(), 5000);
            };
        } catch(e) {
            this.updateWsStatus('disconnected');
        }
    }

    handleWsMessage(msg) {
        switch (msg.type) {
            case 'connection_ack':
                this.updateWsStatus('connected');
                this.subscribeToUpdates();
                break;
            case 'next':
                if (msg.payload && msg.payload.data && msg.payload.data.topicUpdates) {
                    this.handleTopicUpdate(msg.payload.data.topicUpdates);
                }
                break;
        }
    }

    subscribeToUpdates() {
        this.subscriptionId++;
        const filters = this.buildSubscriptionFilters();
        const query = `subscription($filters: [String!]!) {
            topicUpdates(topicFilters: $filters) {
                topic payload format timestamp
            }
        }`;
        this.ws.send(JSON.stringify({
            id: String(this.subscriptionId),
            type: 'subscribe',
            payload: { query, variables: { filters } }
        }));
    }

    buildSubscriptionFilters() {
        const filters = [`${this.agentBaseTopic}/#`, this.discoveryTopic];
        this.responseTopics.forEach(topic => {
            if (typeof topic !== 'string' || !topic.trim()) return;
            const normalized = topic.trim();
            if (normalized.startsWith(`${this.agentBaseTopic}/`)) return;
            if (normalized === this.discoveryTopic) return;
            filters.push(normalized);
        });
        return [...new Set(filters)];
    }

    handleTopicUpdate(update) {
        const topic = update.topic;
        const payload = this.parsePayload(update.payload);

        if (topic === this.discoveryTopic) {
            if (payload) {
                this.updateAgentTopicsFromCard(payload);
                this.renderHeader(payload);
                this.renderTestTopics();
            }
            return;
        }

        if (topic === this.agentBaseTopic + '/health') {
            if (payload) this.renderHealth(payload);
            return;
        }

        if (topic.startsWith(this.agentBaseTopic + '/status/')) {
            this.handleTaskUpdate(update, payload);
            return;
        }

        if (this.isResponseTopic(topic)) {
            this.handleResponseUpdate(update, payload);
            return;
        }

        // Check log topics
        const logPrefix = this.agentBaseTopic + '/logs/';
        if (topic.startsWith(logPrefix)) {
            const logType = topic.substring(logPrefix.length);
            if (this.logEntries[logType]) {
                this.logEntries[logType].push({
                    timestamp: update.timestamp || Date.now(),
                    payload: update.payload
                });
                // Trim
                if (this.logEntries[logType].length > this.maxLogEntries) {
                    this.logEntries[logType] = this.logEntries[logType].slice(-this.maxLogEntries);
                }
                if (logType === this.activeLogType) {
                    this.renderLogs();
                }
            }
        }
    }

    updateWsStatus(state) {
        const dot = document.getElementById('ws-dot');
        const label = document.getElementById('ws-label');
        if (!dot || !label) return;
        dot.className = 'ws-dot ' + state;
        label.textContent = state === 'connected' ? 'Live' : state === 'connecting' ? 'Connecting...' : 'Disconnected';
    }

    // ===================== Test Console =====================

    setupTestConsole() {
        const sendButton = document.getElementById('send-test-message');
        const clearButton = document.getElementById('clear-live-results');
        const input = document.getElementById('test-message');

        if (sendButton) {
            sendButton.addEventListener('click', () => this.sendTestMessage());
        }

        if (clearButton) {
            clearButton.addEventListener('click', () => this.clearLiveResults());
        }

        if (input) {
            input.addEventListener('keydown', (event) => {
                if ((event.ctrlKey || event.metaKey) && event.key === 'Enter') {
                    event.preventDefault();
                    this.sendTestMessage();
                }
            });
        }

        this.renderTestTopics();
        this.renderTaskStream();
        this.renderResponseStream();
        this.renderActiveTaskSummary();
    }

    renderTestTopics() {
        const inboxEl = document.getElementById('test-inbox-topic');
        const responseEl = document.getElementById('test-response-topics');
        if (inboxEl) {
            inboxEl.innerHTML = this.renderTopicChips([this.inboxTopic]);
        }
        if (responseEl) {
            responseEl.innerHTML = this.renderTopicChips(this.responseTopics);
        }
    }

    renderTopicChips(topics) {
        const values = (topics || []).filter(topic => typeof topic === 'string' && topic.trim());
        if (values.length === 0) {
            return '<span class="topic-chip">-</span>';
        }
        return values.map(topic => `<span class="topic-chip">${this.esc(topic)}</span>`).join('');
    }

    async sendTestMessage() {
        if (this.sendInFlight) return;

        const inputEl = document.getElementById('test-message');
        const rawInput = inputEl ? inputEl.value.trim() : '';
        if (!rawInput) {
            this.setSendStatus('Enter a task message first.', 'error');
            return;
        }

        const taskId = this.generateTaskId();
        const publishTopic = `${this.inboxTopic}/${taskId}`;
        const replyTopic = `${this.agentBaseTopic}/status/${taskId}`;
        const envelope = {
            taskId,
            input: this.parseTestInput(rawInput),
            replyTo: replyTopic,
            callerAgent: 'dashboard',
            sourceTopic: 'dashboard/agent-monitor-detail'
        };

        this.setSendInFlight(true);
        this.setSendStatus('Publishing test task…');

        try {
            const result = await this.client.query(`
                mutation PublishAgentTest($input: PublishInput!) {
                    publish(input: $input) {
                        success
                        topic
                        timestamp
                        error
                    }
                }
            `, {
                input: {
                    topic: publishTopic,
                    payload: JSON.stringify(envelope),
                    format: 'JSON',
                    qos: 0,
                    retained: false
                }
            });

            if (!result?.publish?.success) {
                throw new Error(result?.publish?.error || 'The broker rejected the publish request.');
            }

            this.activeTaskId = taskId;
            this.activeTaskState = {
                taskId,
                status: 'sent',
                topic: publishTopic,
                replyTopic,
                timestamp: result.publish.timestamp || Date.now(),
                summary: 'Task published to agent inbox'
            };

            this.addTaskEntry({
                taskId,
                topic: publishTopic,
                timestamp: result.publish.timestamp || Date.now(),
                state: 'sent',
                summary: 'Task published to inbox',
                payload: envelope
            });

            this.renderTaskStream();
            this.renderActiveTaskSummary();
            this.setSendStatus(`Sent task ${taskId}. Waiting for status and response topics.`, 'success');
        } catch (e) {
            console.error('Error sending agent test message:', e);
            this.setSendStatus(`Failed to send test message: ${e.message}`, 'error');
        } finally {
            this.setSendInFlight(false);
        }
    }

    setSendInFlight(isBusy) {
        this.sendInFlight = isBusy;
        const sendButton = document.getElementById('send-test-message');
        if (sendButton) {
            sendButton.disabled = isBusy;
            sendButton.textContent = isBusy ? 'Sending…' : 'Send to Inbox';
        }
    }

    setSendStatus(message, state = '') {
        const el = document.getElementById('test-send-status');
        if (!el) return;
        el.textContent = message;
        el.className = 'test-send-status';
        if (state) el.classList.add(state);
    }

    clearLiveResults() {
        this.taskEntries = [];
        this.responseEntries = [];
        this.activeTaskId = null;
        this.activeTaskState = null;
        this.renderTaskStream();
        this.renderResponseStream();
        this.renderActiveTaskSummary();
        this.setSendStatus('Cleared live task and response output.');
    }

    parseTestInput(rawInput) {
        try {
            return JSON.parse(rawInput);
        } catch (e) {
            return rawInput;
        }
    }

    generateTaskId() {
        if (window.crypto && typeof window.crypto.randomUUID === 'function') {
            return window.crypto.randomUUID();
        }
        return `task-${Date.now()}-${Math.random().toString(16).slice(2, 10)}`;
    }

    isResponseTopic(topic) {
        return this.responseTopics.includes(topic);
    }

    handleTaskUpdate(update, payload) {
        const taskPrefix = `${this.agentBaseTopic}/status/`;
        const taskId = update.topic.startsWith(taskPrefix)
            ? update.topic.substring(taskPrefix.length)
            : (payload && typeof payload === 'object' ? payload.taskId : '');
        const state = this.getTaskState(payload);
        const summary = this.buildTaskSummary(payload, state);

        this.addTaskEntry({
            taskId,
            topic: update.topic,
            timestamp: update.timestamp || Date.now(),
            state,
            summary,
            payload
        });

        if (taskId && this.activeTaskId === taskId) {
            this.activeTaskState = {
                taskId,
                status: state,
                topic: update.topic,
                replyTopic: `${this.agentBaseTopic}/status/${taskId}`,
                timestamp: update.timestamp || Date.now(),
                summary
            };
            this.renderActiveTaskSummary();
        }

        this.renderTaskStream();
    }

    handleResponseUpdate(update, payload) {
        const summary = this.buildResponseSummary(payload);
        this.responseEntries.unshift({
            topic: update.topic,
            timestamp: update.timestamp || Date.now(),
            state: 'response',
            summary,
            payload
        });
        if (this.responseEntries.length > this.maxResponseEntries) {
            this.responseEntries = this.responseEntries.slice(0, this.maxResponseEntries);
        }
        this.renderResponseStream();
    }

    addTaskEntry(entry) {
        this.taskEntries.unshift(entry);
        if (this.taskEntries.length > this.maxTaskEntries) {
            this.taskEntries = this.taskEntries.slice(0, this.maxTaskEntries);
        }
    }

    getTaskState(payload) {
        const rawStatus = payload && typeof payload === 'object' ? payload.status : '';
        const status = typeof rawStatus === 'string' ? rawStatus.toLowerCase() : '';
        if (status === 'working' || status === 'completed' || status === 'failed') return status;
        if (payload && typeof payload === 'object' && payload.error) return 'failed';
        if (payload && typeof payload === 'object' && payload.result) return 'completed';
        return 'response';
    }

    buildTaskSummary(payload, fallbackState) {
        if (payload && typeof payload === 'object') {
            if (payload.error) return `Error: ${String(payload.error).slice(0, 220)}`;
            if (payload.result) return this.shortenText(payload.result, 220);
            if (payload.status) return `Task ${String(payload.status).toLowerCase()}`;
        }
        if (typeof payload === 'string') {
            return this.shortenText(payload, 220);
        }
        return fallbackState === 'response' ? 'Task update received' : `Task ${fallbackState}`;
    }

    buildResponseSummary(payload) {
        if (payload && typeof payload === 'object') {
            if (payload.result) return this.shortenText(payload.result, 220);
            if (payload.text) return this.shortenText(payload.text, 220);
        }
        if (typeof payload === 'string') return this.shortenText(payload, 220);
        return 'Response received';
    }

    shortenText(value, maxLength = 220) {
        let text;
        if (typeof value === 'string') {
            text = value;
        } else if (value && typeof value === 'object') {
            text = JSON.stringify(value);
        } else {
            text = String(value ?? '');
        }
        const normalized = text.replace(/\s+/g, ' ').trim();
        if (normalized.length <= maxLength) return normalized;
        return normalized.slice(0, maxLength - 1) + '…';
    }

    renderActiveTaskSummary() {
        const el = document.getElementById('active-task-summary');
        if (!el) return;

        if (!this.activeTaskState) {
            el.innerHTML = 'Waiting for a test task. The latest task ID and state will appear here.';
            return;
        }

        const lines = [
            `<strong>Latest task:</strong> <code>${this.esc(this.activeTaskState.taskId)}</code>`,
            `<strong>Status:</strong> ${this.esc(this.activeTaskState.status || 'unknown')}`,
            `<strong>Reply topic:</strong> <code>${this.esc(this.activeTaskState.replyTopic || this.activeTaskState.topic || '-')}</code>`,
            `<strong>Updated:</strong> ${this.esc(this.formatTimestamp(this.activeTaskState.timestamp))}`
        ];

        if (this.activeTaskState.summary) {
            lines.push(`<strong>Summary:</strong> ${this.esc(this.activeTaskState.summary)}`);
        }

        el.innerHTML = lines.join('<br>');
    }

    renderTaskStream() {
        this.renderEventStream('task-status-stream', this.taskEntries, 'Waiting for task status updates...');
    }

    renderResponseStream() {
        this.renderEventStream('response-stream', this.responseEntries, 'Waiting for agent responses...');
    }

    renderEventStream(containerId, entries, emptyText) {
        const container = document.getElementById(containerId);
        if (!container) return;

        if (!entries || entries.length === 0) {
            container.innerHTML = `<div class="log-empty">${this.esc(emptyText)}</div>`;
            return;
        }

        container.innerHTML = entries.map(entry => {
            const isActive = entry.taskId && this.activeTaskId && entry.taskId === this.activeTaskId;
            const meta = [
                `<span class="event-time">${this.esc(this.formatTimestamp(entry.timestamp))}</span>`,
                `<span class="event-topic">${this.esc(this.trimTopic(entry.topic || ''))}</span>`
            ];

            if (entry.taskId) {
                meta.push(`<span class="event-task">${this.esc(entry.taskId)}</span>`);
            }

            meta.push(`<span class="event-state ${this.esc(entry.state || 'response')}">${this.esc(entry.state || 'response')}</span>`);

            return `<div class="event-card${isActive ? ' active' : ''}">
                <div class="event-meta">${meta.join('')}</div>
                <div class="event-summary">${this.esc(entry.summary || 'Update received')}</div>
                <pre class="event-payload">${this.esc(this.stringifyPayload(entry.payload))}</pre>
            </div>`;
        }).join('');
    }

    stringifyPayload(payload) {
        if (payload == null) return '';
        if (typeof payload === 'string') return payload;
        try {
            return JSON.stringify(payload, null, 2);
        } catch (e) {
            return String(payload);
        }
    }

    // ===================== Log Tabs =====================

    setupLogTabs() {
        document.querySelectorAll('#log-tabs .log-tab').forEach(tab => {
            tab.addEventListener('click', () => {
                document.querySelectorAll('#log-tabs .log-tab').forEach(t => t.classList.remove('active'));
                tab.classList.add('active');
                this.activeLogType = tab.dataset.log;
                this.renderLogs();
            });
        });
    }

    renderLogs() {
        const container = document.getElementById('log-container');
        if (!container) return;

        const entries = this.logEntries[this.activeLogType] || [];
        if (entries.length === 0) {
            container.innerHTML = '<div class="log-empty">Waiting for log messages...</div>';
            return;
        }

        const wasAtBottom = container.scrollHeight - container.scrollTop <= container.clientHeight + 50;

        container.innerHTML = entries.map(entry => {
            const ts = this.formatTimestamp(entry.timestamp);
            const text = typeof entry.payload === 'string' ? entry.payload : JSON.stringify(entry.payload);
            return `<div class="log-entry"><span class="log-ts">${this.esc(ts)}</span>${this.esc(text)}</div>`;
        }).join('');

        if (wasAtBottom) {
            container.scrollTop = container.scrollHeight;
        }
    }

    // ===================== History =====================

    setDefaultHistoryDates() {
        const now = new Date();
        const oneHourAgo = new Date(now.getTime() - 3600000);

        const fmt = (d) => {
            const pad = (n) => String(n).padStart(2, '0');
            return `${d.getFullYear()}-${pad(d.getMonth()+1)}-${pad(d.getDate())}T${pad(d.getHours())}:${pad(d.getMinutes())}`;
        };

        document.getElementById('history-start').value = fmt(oneHourAgo);
        document.getElementById('history-end').value = fmt(now);
    }

    async loadHistory() {
        const subtopic = document.getElementById('history-subtopic').value;
        const startVal = document.getElementById('history-start').value;
        const endVal = document.getElementById('history-end').value;
        const archiveGroup = document.getElementById('history-archive').value || 'Agents';

        if (!startVal || !endVal) return;

        const topicFilter = `${this.agentBaseTopic}/${subtopic}`;
        const startTime = new Date(startVal).toISOString();
        const endTime = new Date(endVal).toISOString();

        try {
            const result = await this.client.query(`
                query($topicFilter: String!, $startTime: String, $endTime: String, $archiveGroup: String) {
                    archivedMessages(topicFilter: $topicFilter, startTime: $startTime, endTime: $endTime,
                        format: JSON, limit: 500, archiveGroup: $archiveGroup) {
                        topic timestamp payload
                    }
                }
            `, { topicFilter, startTime, endTime, archiveGroup });

            const messages = (result && result.archivedMessages) || [];
            const tableEl = document.getElementById('history-table');
            const emptyEl = document.getElementById('history-empty');
            const tbody = document.getElementById('history-table-body');

            if (messages.length === 0) {
                tableEl.style.display = 'none';
                emptyEl.style.display = 'block';
                return;
            }

            tableEl.style.display = 'block';
            emptyEl.style.display = 'none';
            tbody.innerHTML = messages.map(msg => {
                const ts = msg.timestamp ? new Date(Number(msg.timestamp)).toLocaleString() : '-';
                const shortTopic = this.trimTopic(msg.topic || '');
                const formattedPayload = this.formatPayload(msg.payload);
                return `<tr>
                    <td>${this.esc(ts)}</td>
                    <td style="color:var(--text-muted);font-size:0.8rem;white-space:nowrap">${this.esc(shortTopic)}</td>
                    <td class="payload-cell">${formattedPayload}</td>
                </tr>`;
            }).join('');
        } catch(e) {
            console.error('Error loading history:', e);
        }
    }

    // ===================== Helpers =====================

    trimTopic(topic) {
        // Strip the agent base prefix to show only the relative part (e.g. "inbox", "logs/tools", "health")
        if (topic.startsWith(this.agentBaseTopic + '/')) {
            return topic.substring(this.agentBaseTopic.length + 1);
        }
        if (topic === this.discoveryTopic) return 'discovery';
        return topic;
    }

    formatPayload(payload) {
        if (!payload) return '-';
        try {
            const obj = typeof payload === 'string' ? JSON.parse(payload) : payload;
            const pretty = JSON.stringify(obj, null, 2);
            return `<pre class="payload-json">${this.esc(pretty)}</pre>`;
        } catch(e) {
            return `<span>${this.esc(payload)}</span>`;
        }
    }

    formatTimestamp(ts) {
        if (!ts) return '-';
        try {
            const d = new Date(typeof ts === 'number' ? ts : ts);
            if (isNaN(d.getTime())) return '-';
            return d.toLocaleTimeString() + ' ' + d.toLocaleDateString();
        } catch(e) { return '-'; }
    }

    esc(text) {
        const div = document.createElement('div');
        div.textContent = text || '';
        return div.innerHTML;
    }
}

// Initialize
let agentDetailMonitor;
document.addEventListener('DOMContentLoaded', () => {
    agentDetailMonitor = new AgentDetailMonitorManager();
});
