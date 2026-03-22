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

        // Parse URL params
        const params = new URLSearchParams(window.location.search);
        this.org = params.get('org') || '';
        this.site = params.get('site') || '';
        this.agentName = params.get('agent') || '';
        this.agentBaseTopic = `a2a/v1/${this.org}/${this.site}/agents/${this.agentName}`;
        this.discoveryTopic = `a2a/v1/${this.org}/${this.site}/discovery/${this.agentName}`;

        this.init();
    }

    async init() {
        if (!this.agentName) {
            document.getElementById('agent-name').textContent = 'No agent specified';
            return;
        }

        document.getElementById('agent-name').textContent = this.agentName;
        this.setupLogTabs();
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
                this.client.query(`query { agents { name } }`)
            ]);

            // Check if internal
            if (agentsResult && agentsResult.agents) {
                this.isInternal = agentsResult.agents.some(a => a.name === this.agentName);
            }

            // Render agent card
            if (cardResult && cardResult.retainedMessage) {
                const card = this.parsePayload(cardResult.retainedMessage.payload);
                if (card) this.renderHeader(card);
            }

            // Render health
            if (healthResult && healthResult.retainedMessage) {
                const health = this.parsePayload(healthResult.retainedMessage.payload);
                if (health) this.renderHealth(health);
            }

            this.renderTypeBadge();
        } catch(e) {
            console.error('Error loading agent detail:', e);
        }
    }

    parsePayload(payload) {
        try { return typeof payload === 'string' ? JSON.parse(payload) : payload; } catch(e) { return null; }
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
        setEl('health-timestamp', this.formatTimestamp(health.timestamp));

        // Update status badge from health
        this.renderStatusBadge(health.status);

        // Color the errors value red if > 0
        const errEl = document.getElementById('health-errors');
        if (errEl && health.errors > 0) errEl.style.color = 'var(--monster-red)';
    }

    // ===================== WebSocket =====================

    getWebSocketUrl() {
        const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
        const host = window.location.hostname;
        const port = window.location.port;
        if (window.brokerManager) {
            const wsEndpoint = window.brokerManager.getWsEndpoint();
            if (wsEndpoint) return `${protocol}//${host}${port ? ':' + port : ''}${wsEndpoint}`;
        }
        return `${protocol}//${host}${port ? ':' + port : ''}/graphqlws`;
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
        const filters = [
            `${this.agentBaseTopic}/#`,
            this.discoveryTopic
        ];
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

    handleTopicUpdate(update) {
        const topic = update.topic;
        const payload = this.parsePayload(update.payload);

        if (topic === this.discoveryTopic) {
            if (payload) this.renderHeader(payload);
            return;
        }

        if (topic === this.agentBaseTopic + '/health') {
            if (payload) this.renderHealth(payload);
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
