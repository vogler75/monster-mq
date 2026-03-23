// Agent Monitor - Live monitoring of all A2A agents

class AgentMonitorManager {
    constructor() {
        this.client = new GraphQLDashboardClient();
        this.allAgents = new Map(); // agentId -> { card, health, isInternal }
        this.internalAgentNames = new Set();
        this.ws = null;
        this.subscriptionId = 0;
        this.reconnectTimer = null;
        this.pollTimer = null;
        this.showDescriptions = false;
        this.init();
    }

    async init() {
        await this.loadData();
        this.connectWebSocket();
        this.pollTimer = setInterval(() => this.loadData(), 30000);
        window.registerPageCleanup(() => this.cleanup());
    }

    cleanup() {
        if (this.ws) { try { this.ws.close(); } catch(e) {} this.ws = null; }
        if (this.reconnectTimer) { clearTimeout(this.reconnectTimer); this.reconnectTimer = null; }
        if (this.pollTimer) { clearInterval(this.pollTimer); this.pollTimer = null; }
    }

    // ===================== Data Loading =====================

    async loadData() {
        try {
            const [discoveryResult, healthResult, agentsResult] = await Promise.all([
                this.client.query(`
                    query { retainedMessages(topicFilter: "a2a/v1/+/+/discovery/+", format: JSON, limit: 1000) {
                        topic payload
                    }}
                `),
                this.client.query(`
                    query { retainedMessages(topicFilter: "a2a/v1/+/+/agents/+/health", format: JSON, limit: 1000) {
                        topic payload
                    }}
                `),
                this.client.query(`query { agents { name } }`)
            ]);

            // Clear and rebuild from scratch on each full load
            this.allAgents.clear();
            this.internalAgentNames.clear();
            if (agentsResult && agentsResult.agents) {
                agentsResult.agents.forEach(a => this.internalAgentNames.add(a.name));
            }

            // Process discovery messages
            if (discoveryResult && discoveryResult.retainedMessages) {
                discoveryResult.retainedMessages.forEach(msg => {
                    this.processDiscoveryMessage(msg.topic, msg.payload);
                });
            }

            // Process health messages
            if (healthResult && healthResult.retainedMessages) {
                healthResult.retainedMessages.forEach(msg => {
                    this.processHealthMessage(msg.topic, msg.payload);
                });
            }

            this.updateMetrics();
            this.renderTable();
        } catch (error) {
            console.error('Error loading agent data:', error);
            this.showError('Failed to load agent data: ' + error.message);
        }
    }

    processDiscoveryMessage(topic, payload) {
        const parsed = this.parseDiscoveryTopic(topic);
        if (!parsed) return;

        // Empty payload means the retained message was cleared — remove the agent
        if (!payload || payload === '' || payload === 'null') {
            this.allAgents.delete(parsed.agentId);
            return;
        }

        let card;
        try { card = typeof payload === 'string' ? JSON.parse(payload) : payload; } catch(e) { return; }
        if (!card || typeof card !== 'object') return;

        const key = parsed.agentId;
        const existing = this.allAgents.get(key) || {};
        this.allAgents.set(key, {
            ...existing,
            card: card,
            org: parsed.org,
            site: parsed.site,
            agentId: parsed.agentId,
            isInternal: this.internalAgentNames.has(card.name || parsed.agentId)
        });
    }

    processHealthMessage(topic, payload) {
        const parsed = this.parseHealthTopic(topic);
        if (!parsed) return;
        if (!payload || payload === '' || payload === 'null') return;

        let health;
        try { health = typeof payload === 'string' ? JSON.parse(payload) : payload; } catch(e) { return; }
        if (!health || typeof health !== 'object') return;

        // Only enrich existing agents — don't create entries from health alone
        const key = parsed.agentId;
        if (!this.allAgents.has(key)) return;
        const existing = this.allAgents.get(key);
        this.allAgents.set(key, { ...existing, health: health });
    }

    // ===================== Topic Parsing =====================

    parseDiscoveryTopic(topic) {
        // a2a/v1/{org}/{site}/discovery/{agentId}
        const p = topic.split('/');
        if (p.length < 6 || p[0] !== 'a2a' || p[4] !== 'discovery') return null;
        return { org: p[2], site: p[3], agentId: p[5] };
    }

    parseHealthTopic(topic) {
        // a2a/v1/{org}/{site}/agents/{agentId}/health
        const p = topic.split('/');
        if (p.length < 7 || p[0] !== 'a2a' || p[4] !== 'agents' || p[6] !== 'health') return null;
        return { org: p[2], site: p[3], agentId: p[5] };
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
                try { this.handleWsMessage(JSON.parse(ev.data)); } catch(e) { console.error('WS parse error:', e); }
            };

            this.ws.onerror = () => this.updateWsStatus('disconnected');

            this.ws.onclose = () => {
                this.ws = null;
                this.updateWsStatus('disconnected');
                this.reconnectTimer = setTimeout(() => this.connectWebSocket(), 5000);
            };
        } catch(e) {
            console.error('WebSocket connection error:', e);
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
                    const update = msg.payload.data.topicUpdates;
                    this.handleTopicUpdate(update);
                }
                break;
            case 'error':
                console.error('WS subscription error:', msg.payload);
                break;
        }
    }

    subscribeToUpdates() {
        this.subscriptionId++;
        const query = `subscription {
            topicUpdates(topicFilters: ["a2a/v1/+/+/discovery/+", "a2a/v1/+/+/agents/+/health"]) {
                topic payload format timestamp
            }
        }`;
        this.ws.send(JSON.stringify({
            id: String(this.subscriptionId),
            type: 'subscribe',
            payload: { query }
        }));
    }

    handleTopicUpdate(update) {
        const topic = update.topic;
        if (topic.includes('/discovery/')) {
            this.processDiscoveryMessage(topic, update.payload);
        } else if (topic.endsWith('/health')) {
            this.processHealthMessage(topic, update.payload);
        }
        this.updateMetrics();
        this.renderTable();
    }

    updateWsStatus(state) {
        const dot = document.getElementById('ws-dot');
        const label = document.getElementById('ws-label');
        if (!dot || !label) return;
        dot.className = 'ws-dot ' + state;
        label.textContent = state === 'connected' ? 'Live' : state === 'connecting' ? 'Connecting...' : 'Disconnected';
    }

    // ===================== Rendering =====================

    updateMetrics() {
        const agents = Array.from(this.allAgents.values());
        const total = agents.length;
        const running = agents.filter(a => this.getStatus(a) === 'running').length;
        const internal = agents.filter(a => a.isInternal).length;
        const external = total - internal;

        const setEl = (id, val) => { const el = document.getElementById(id); if (el) el.textContent = val; };
        setEl('metric-total', total);
        setEl('metric-running', running);
        setEl('metric-internal', internal);
        setEl('metric-external', external);
    }

    getStatus(agent) {
        if (agent.health && agent.health.status) return agent.health.status.toLowerCase();
        if (agent.card && agent.card.status) return agent.card.status.toLowerCase();
        return 'unknown';
    }

    renderTable() {
        const tbody = document.getElementById('agents-table-body');
        if (!tbody) return;

        const agents = Array.from(this.allAgents.values()).sort((a, b) => {
            const nameA = (a.card && a.card.name) || a.agentId || '';
            const nameB = (b.card && b.card.name) || b.agentId || '';
            return nameA.localeCompare(nameB);
        });

        if (agents.length === 0) {
            tbody.innerHTML = '<tr><td colspan="9" class="no-data">No agents discovered. Agents publish to a2a/v1/+/+/discovery/+ when started.</td></tr>';
            return;
        }

        tbody.innerHTML = '';
        agents.forEach(agent => {
            const row = document.createElement('tr');
            const card = agent.card || {};
            const health = agent.health || {};
            const name = card.name || agent.agentId || '?';
            const status = this.getStatus(agent);
            const statusClass = 'status-' + (['running','ready','stopped'].includes(status) ? status : 'unknown');
            const typeClass = agent.isInternal ? 'type-internal' : 'type-external';
            const typeLabel = agent.isInternal ? 'Internal' : 'External';
            const provider = card.provider || '';
            const model = card.model || '';
            const providerModel = provider ? (provider + (model ? ' / ' + model : '')) : (model || '-');
            const lastSeen = this.formatTimestamp(health.timestamp || card.timestamp);

            row.innerHTML = `
                <td>
                    <div style="font-weight:600; color:var(--text-primary)">${this.esc(name)}</div>
                    <small class="agent-desc" style="color:var(--text-muted);display:${this.showDescriptions ? 'block' : 'none'}">${this.esc(card.description || '')}</small>
                </td>
                <td>${this.esc((agent.org || '') + ' / ' + (agent.site || ''))}</td>
                <td><span class="status-badge ${statusClass}">${this.esc(status)}</span></td>
                <td><span class="type-badge ${typeClass}">${typeLabel}</span></td>
                <td>${this.esc(providerModel)}</td>
                <td class="numeric-cell">${this.formatNum(health.messagesProcessed)}</td>
                <td class="numeric-cell">${this.formatNum(health.llmCalls)}</td>
                <td class="numeric-cell">${this.formatNum(health.errors)}</td>
                <td style="color:var(--text-muted); font-size:0.85rem">${lastSeen}</td>
            `;

            row.addEventListener('click', () => {
                const params = new URLSearchParams({ org: agent.org || '', site: agent.site || '', agent: name });
                window.spaLocation.href = `/pages/agent-monitor-detail.html?${params.toString()}`;
            });
            tbody.appendChild(row);
        });
    }

    // ===================== Helpers =====================

    formatTimestamp(ts) {
        if (!ts) return '-';
        try {
            const d = new Date(typeof ts === 'number' ? ts : ts);
            if (isNaN(d.getTime())) return '-';
            const now = Date.now();
            const diff = now - d.getTime();
            if (diff < 60000) return Math.floor(diff / 1000) + 's ago';
            if (diff < 3600000) return Math.floor(diff / 60000) + 'm ago';
            if (diff < 86400000) return Math.floor(diff / 3600000) + 'h ago';
            return d.toLocaleDateString() + ' ' + d.toLocaleTimeString();
        } catch(e) { return '-'; }
    }

    formatNum(val) {
        if (val === undefined || val === null) return '-';
        return Number(val).toLocaleString();
    }

    esc(text) {
        const div = document.createElement('div');
        div.textContent = text || '';
        return div.innerHTML;
    }

    showError(message) {
        var existing = document.getElementById('error-toast');
        if (existing) existing.remove();
        var toast = document.createElement('div');
        toast.id = 'error-toast';
        toast.style.cssText = 'position:fixed;top:20px;left:50%;transform:translateX(-50%);background:var(--monster-red,#EF4444);color:#fff;padding:14px 24px;border-radius:8px;box-shadow:0 4px 12px rgba(0,0,0,0.4);z-index:10000;font-size:0.9rem;max-width:600px;display:flex;align-items:center;gap:10px;animation:slideDown 0.3s ease-out;';
        toast.innerHTML = '<span>' + this.esc(message) + '</span><button onclick="this.parentElement.remove()" style="background:none;border:none;color:#fff;cursor:pointer;margin-left:auto;font-size:1.1rem;">&times;</button>';
        if (!document.getElementById('toast-anim-style')) {
            var s = document.createElement('style'); s.id = 'toast-anim-style';
            s.textContent = '@keyframes slideDown{from{transform:translateX(-50%) translateY(-100%);opacity:0;}to{transform:translateX(-50%) translateY(0);opacity:1;}}';
            document.head.appendChild(s);
        }
        document.body.appendChild(toast);
        setTimeout(() => { if (toast.parentElement) toast.remove(); }, 8000);
    }

    toggleDescriptions(show) {
        this.showDescriptions = show;
        document.querySelectorAll('.agent-desc').forEach(el => {
            el.style.display = show ? 'block' : 'none';
        });
    }

    async refresh() {
        await this.loadData();
    }
}

// Initialize
let agentMonitor;
document.addEventListener('DOMContentLoaded', () => {
    agentMonitor = new AgentMonitorManager();
});
