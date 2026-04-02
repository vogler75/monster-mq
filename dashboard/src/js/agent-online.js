// Agent Online Graph - Live network visualization of agent interactions

class AgentOnlineGraphManager {
    constructor() {
        this.client = new GraphQLDashboardClient();
        this.chart = null;
        this.agents = new Map();       // agentId -> { name, org, site, provider, model, enabled, subAgents, health }
        this.edges = new Map();        // "caller->target" -> { caller, target, count, lastSeen }
        this.ws = null;
        this.subscriptionId = 0;
        this.reconnectTimer = null;
        this.layoutType = 'circular';

        this.STATUS_COLORS = {
            running: '#10B981',
            ready: '#3B82F6',
            stopped: '#EF4444',
            unknown: '#617d91'
        };

        this.init();
    }

    async init() {
        this.initChart();
        await this.loadAgents();
        this.renderGraph();
        this.connectWebSocket();
        window.registerPageCleanup(() => this.cleanup());

        window.addEventListener('resize', this._resizeHandler = () => {
            if (this.chart) this.chart.resize();
        });
    }

    cleanup() {
        if (this.chart) { this.chart.dispose(); this.chart = null; }
        if (this.ws) { try { this.ws.close(); } catch(e) {} this.ws = null; }
        if (this.reconnectTimer) { clearTimeout(this.reconnectTimer); this.reconnectTimer = null; }
        if (this._resizeHandler) { window.removeEventListener('resize', this._resizeHandler); }
    }

    initChart() {
        const container = document.getElementById('agent-graph');
        if (!container || typeof echarts === 'undefined') return;
        this.chart = echarts.init(container, null, { renderer: 'canvas' });

        this.chart.on('click', (params) => {
            if (params.dataType === 'node' && params.data) {
                const a = this.agents.get(params.data.id);
                if (a) {
                    const qs = new URLSearchParams({ org: a.org || '', site: a.site || '', agent: params.data.id });
                    window.spaLocation.href = `/pages/agent-monitor-detail.html?${qs.toString()}`;
                }
            }
        });
    }

    // ===================== Data Loading =====================

    async loadAgents() {
        try {
            const [agentsResult, healthResult] = await Promise.all([
                this.client.query(`query { agents { name org site enabled provider model subAgents } }`),
                this.client.query(`query { retainedMessages(topicFilter: "a2a/v1/+/+/agents/+/health", format: JSON, limit: 1000) { topic payload } }`)
            ]);

            // Build agent nodes from config
            if (agentsResult && agentsResult.agents) {
                agentsResult.agents.forEach(agent => {
                    const existing = this.agents.get(agent.name) || {};
                    this.agents.set(agent.name, {
                        ...existing,
                        name: agent.name,
                        org: agent.org,
                        site: agent.site,
                        enabled: agent.enabled,
                        provider: agent.provider,
                        model: agent.model,
                        subAgents: agent.subAgents || [],
                        isInternal: true
                    });
                });

                // Build edges from subAgents relationships
                agentsResult.agents.forEach(agent => {
                    if (agent.subAgents) {
                        agent.subAgents.forEach(sub => {
                            const edgeKey = `${agent.name}->${sub}`;
                            if (!this.edges.has(edgeKey)) {
                                this.edges.set(edgeKey, { caller: agent.name, target: sub, count: 0, lastSeen: 0 });
                            }
                        });
                    }
                });
            }

            // Apply current health state from retained messages
            if (healthResult && healthResult.retainedMessages) {
                healthResult.retainedMessages.forEach(msg => {
                    const parsed = this.parseHealthTopic(msg.topic);
                    if (!parsed) return;
                    const health = this.parseJSON(msg.payload);
                    if (!health) return;
                    const existing = this.agents.get(parsed.agentId) || {};
                    this.agents.set(parsed.agentId, { ...existing, health });
                });
            }
        } catch(e) {
            console.error('Error loading agents:', e);
        }
    }

    // ===================== Topic Parsing =====================

    parseHealthTopic(topic) {
        const p = topic.split('/');
        if (p.length < 7 || p[0] !== 'a2a' || p[4] !== 'agents' || p[6] !== 'health') return null;
        return { org: p[2], site: p[3], agentId: p[5] };
    }

    parseInboxTopic(topic) {
        const p = topic.split('/');
        if (p.length < 7 || p[0] !== 'a2a' || p[4] !== 'agents' || p[6] !== 'inbox') return null;
        return { org: p[2], site: p[3], agentId: p[5], taskId: p[7] || null };
    }

    parseJSON(payload) {
        try { return typeof payload === 'string' ? JSON.parse(payload) : payload; } catch(e) { return null; }
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
                this.wsSend({ type: 'connection_init', payload });
            };

            this.ws.onmessage = (ev) => {
                try {
                    this.handleWsMessage(JSON.parse(ev.data));
                } catch(e) { console.error('Agent graph WS error:', e); }
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

    wsSend(msg) {
        if (this.ws && this.ws.readyState === WebSocket.OPEN) {
            this.ws.send(JSON.stringify(msg));
        }
    }

    handleWsMessage(msg) {
        switch (msg.type) {
            case 'connection_ack':
                this.updateWsStatus('connected');
                this.subscribeToUpdates();
                break;
            case 'ping':
                this.wsSend({ type: 'pong' });
                break;
            case 'next':
                if (msg.payload && msg.payload.data && msg.payload.data.topicUpdates) {
                    this.handleTopicUpdate(msg.payload.data.topicUpdates);
                }
                break;
            case 'error':
                console.error('Agent graph subscription error:', msg.payload);
                break;
        }
    }

    subscribeToUpdates() {
        this.subscriptionId++;
        const topicFilters = [
            'a2a/v1/+/+/agents/+/health',
            'a2a/v1/+/+/agents/+/inbox',
            'a2a/v1/+/+/agents/+/inbox/+'
        ];
        const query = `subscription { topicUpdates(topicFilters: ${JSON.stringify(topicFilters)}) { topic payload format timestamp } }`;
        this.wsSend({
            id: String(this.subscriptionId),
            type: 'subscribe',
            payload: { query }
        });
    }

    handleTopicUpdate(update) {
        const topic = update.topic;

        if (topic.endsWith('/health')) {
            const parsed = this.parseHealthTopic(topic);
            if (parsed) {
                const health = this.parseJSON(update.payload);
                if (health) {
                    const existing = this.agents.get(parsed.agentId) || {};
                    this.agents.set(parsed.agentId, {
                        ...existing,
                        name: health.name || parsed.agentId,
                        org: existing.org || parsed.org,
                        site: existing.site || parsed.site,
                        health
                    });
                    this.renderGraph();
                }
            }
        } else if (topic.includes('/inbox')) {
            const parsed = this.parseInboxTopic(topic);
            if (!parsed) return;
            const data = this.parseJSON(update.payload);
            if (!data || !data.callerAgent) return;

            const caller = data.callerAgent;
            const target = parsed.agentId;
            const edgeKey = `${caller}->${target}`;
            const existing = this.edges.get(edgeKey) || { caller, target, count: 0, lastSeen: 0 };
            existing.count++;
            existing.lastSeen = update.timestamp || Date.now();
            this.edges.set(edgeKey, existing);
            this.renderGraph();
        }
    }

    updateWsStatus(state) {
        const dot = document.getElementById('ws-dot');
        const label = document.getElementById('ws-label');
        if (!dot || !label) return;
        dot.className = 'ws-dot ' + state;
        label.textContent = state === 'connected' ? 'Live' : state === 'connecting' ? 'Connecting...' : 'Disconnected';
    }

    // ===================== Graph Rendering =====================

    renderGraph() {
        if (!this.chart) return;
        this.chart.resize();

        const { nodes, links } = this.buildNodesAndLinks();

        const option = {
            backgroundColor: 'transparent',
            tooltip: {
                trigger: 'item',
                formatter: (params) => {
                    if (params.dataType === 'node') {
                        const a = this.agents.get(params.data.id);
                        if (!a) return params.data.name;
                        const health = a.health || {};
                        return `<b>${this.esc(params.data.name)}</b><br/>` +
                            `Status: ${health.status || 'unknown'}<br/>` +
                            `Type: ${a.isInternal ? 'Internal' : 'External'}<br/>` +
                            (a.provider ? `Provider: ${a.provider}${a.model ? ' / ' + a.model : ''}<br/>` : '') +
                            (health.messagesProcessed != null ? `Messages: ${health.messagesProcessed}<br/>` : '') +
                            (health.llmCalls != null ? `LLM Calls: ${health.llmCalls}<br/>` : '') +
                            (health.totalTokens != null ? `Total Tokens: ${Number(health.totalTokens).toLocaleString()}<br/>` : '');
                    }
                    if (params.dataType === 'edge') {
                        return `${params.data.source} → ${params.data.target}<br/>Calls: ${params.data.callCount || 0}`;
                    }
                    return '';
                }
            },
            series: [{
                type: 'graph',
                layout: this.layoutType,
                roam: true,
                draggable: true,
                force: {
                    repulsion: 250,
                    edgeLength: [120, 350],
                    gravity: 0.1,
                    friction: 0.6
                },
                circular: {
                    rotateLabel: true
                },
                data: nodes,
                links: links,
                label: {
                    show: true,
                    position: 'bottom',
                    fontSize: 11,
                    color: '#f0f6fc',
                    formatter: '{b}'
                },
                edgeLabel: {
                    show: false
                },
                lineStyle: {
                    color: 'rgba(124, 58, 237, 0.5)',
                    curveness: 0.15
                },
                emphasis: {
                    focus: 'adjacency',
                    lineStyle: { width: 3 }
                },
                edgeSymbol: ['none', 'arrow'],
                edgeSymbolSize: [0, 10],
                animation: true,
                animationDuration: 500
            }]
        };

        const prevZoom = this.chart.getOption()?.series?.[0]?.zoom;
        const prevCenter = this.chart.getOption()?.series?.[0]?.center;

        this.chart.setOption(option, true);

        if (prevZoom != null && prevZoom !== 1) {
            this.chart.setOption({ series: [{ zoom: prevZoom, center: prevCenter }] });
        }
    }

    buildNodesAndLinks() {
        const nodes = [];
        const links = [];
        const now = Date.now();
        const recentThreshold = 30000;

        this.agents.forEach((agent, agentId) => {
            const health = agent.health || {};
            const status = (health.status || 'unknown').toLowerCase();
            const color = this.STATUS_COLORS[status] || this.STATUS_COLORS.unknown;
            const msgs = health.messagesProcessed || 0;
            const size = Math.max(25, Math.min(65, 25 + Math.sqrt(msgs) * 2));

            nodes.push({
                id: agentId,
                name: agent.name || agentId,
                symbolSize: size,
                itemStyle: {
                    color: color,
                    borderColor: agent.isInternal ? 'rgba(255,255,255,0.6)' : 'rgba(255,255,255,0.4)',
                    borderWidth: 2,
                    borderType: agent.isInternal ? 'solid' : 'dashed',
                    shadowBlur: status === 'running' ? 12 : 0,
                    shadowColor: status === 'running' ? color : 'transparent'
                },
                label: { color: '#f0f6fc' }
            });
        });

        this.edges.forEach((edge) => {
            // Add placeholder nodes for agents not yet known
            if (!this.agents.has(edge.caller)) {
                this.agents.set(edge.caller, { name: edge.caller, isInternal: false });
                nodes.push({
                    id: edge.caller, name: edge.caller, symbolSize: 20,
                    itemStyle: { color: this.STATUS_COLORS.unknown, borderWidth: 2, borderType: 'dashed', borderColor: 'rgba(255,255,255,0.3)' }
                });
            }
            if (!this.agents.has(edge.target)) {
                this.agents.set(edge.target, { name: edge.target, isInternal: false });
                nodes.push({
                    id: edge.target, name: edge.target, symbolSize: 20,
                    itemStyle: { color: this.STATUS_COLORS.unknown, borderWidth: 2, borderType: 'dashed', borderColor: 'rgba(255,255,255,0.3)' }
                });
            }

            const isActive = (now - edge.lastSeen) < recentThreshold;
            const width = Math.max(1, Math.min(6, Math.sqrt(edge.count)));

            links.push({
                source: edge.caller,
                target: edge.target,
                callCount: edge.count,
                lineStyle: {
                    width: width,
                    color: isActive ? 'rgba(124, 58, 237, 0.8)' : 'rgba(124, 58, 237, 0.35)',
                    type: isActive ? 'solid' : 'dashed'
                },
                effect: isActive ? {
                    show: true, period: 3, trailLength: 0.3, symbolSize: 4, color: '#A78BFA'
                } : undefined
            });
        });

        return { nodes, links };
    }

    // ===================== Controls =====================

    onLayoutChange() {
        this.layoutType = document.getElementById('layout-type').value || 'force';
        this.renderGraph();
    }

    async refresh() {
        this.agents.clear();
        this.edges.clear();
        await this.loadAgents();
        this.renderGraph();
        // Resubscribe to get fresh state
        this.wsSend({ id: String(this.subscriptionId), type: 'complete' });
        this.subscribeToUpdates();
    }

    // ===================== Helpers =====================

    esc(text) {
        const div = document.createElement('div');
        div.textContent = text || '';
        return div.innerHTML;
    }
}

// Initialize
let agentGraph;
document.addEventListener('DOMContentLoaded', () => {
    agentGraph = new AgentOnlineGraphManager();
});
