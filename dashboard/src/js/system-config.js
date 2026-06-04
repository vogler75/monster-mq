// System Configuration Page

class SystemConfigManager {
    constructor() {
        this.init();
    }

    async init() {
        if (!window.isLoggedIn()) {
            window.location.href = '/pages/login.html';
            return;
        }

        await this.loadConfig();
    }

    async loadConfig() {
        this.showLoading(true);
        this.hideError();
        try {
            const query = `
                query GetSystemConfig {
                    brokerConfig {
                        nodeId version clustered
                        tcpPort wsPort tcpsPort wssPort natsPort
                        sessionStoreType retainedStoreType configStoreType
                        userManagementEnabled anonymousEnabled
                        mcpEnabled mcpPort
                        prometheusEnabled prometheusPort
                        i3xEnabled i3xPort
                        graphqlEnabled graphqlPort
                        metricsEnabled
                        genAiEnabled genAiProvider genAiModel
                        postgresUrl postgresUser
                        crateDbUrl crateDbUser
                        mongoDbUrl mongoDbDatabase
                        sqlitePath
                        kafkaServers
                    }
                }
            `;
            const result = await window.graphqlClient.query(query);
            if (!result || !result.brokerConfig) throw new Error('Invalid response');
            this.render(result.brokerConfig);
        } catch (e) {
            console.error('Error loading broker config:', e);
            this.showError('Failed to load configuration: ' + e.message);
        } finally {
            this.showLoading(false);
        }
    }

    render(cfg) {
        // Node & Cluster
        this.setText('cfg-node-id', cfg.nodeId);
        this.setText('cfg-version', cfg.version);
        this.setEl('cfg-clustered', cfg.clustered
            ? this.badge('enabled', 'Clustered')
            : this.badge('disabled', 'Standalone'));

        // MQTT Ports
        this.setEl('cfg-tcp-port', this.portBadge(cfg.tcpPort));
        this.setEl('cfg-tcps-port', this.portBadge(cfg.tcpsPort));
        this.setEl('cfg-ws-port', this.portBadge(cfg.wsPort));
        this.setEl('cfg-wss-port', this.portBadge(cfg.wssPort));
        this.setEl('cfg-nats-port', this.portBadge(cfg.natsPort));

        // Storage
        this.setEl('cfg-session-store', this.storeBadge(cfg.sessionStoreType));
        this.setEl('cfg-retained-store', this.storeBadge(cfg.retainedStoreType));
        this.setEl('cfg-config-store', this.storeBadge(cfg.configStoreType));

        // User Management
        this.setEl('cfg-user-mgmt', this.enabledBadge(cfg.userManagementEnabled));
        this.setEl('cfg-anonymous', this.allowedBadge(cfg.anonymousEnabled));

        // GenAI
        this.setEl('cfg-genai', this.enabledBadge(cfg.genAiEnabled));
        this.setText('cfg-genai-provider', cfg.genAiEnabled && cfg.genAiProvider ? cfg.genAiProvider : '-');
        this.setText('cfg-genai-model', cfg.genAiEnabled && cfg.genAiModel ? cfg.genAiModel : '-');

        // Database Connections
        this.setText('cfg-postgres-url', cfg.postgresUrl || '-');
        this.setText('cfg-postgres-user', cfg.postgresUser || '-');
        this.setText('cfg-cratedb-url', cfg.crateDbUrl || '-');
        this.setText('cfg-cratedb-user', cfg.crateDbUser || '-');
        this.setText('cfg-mongodb-url', cfg.mongoDbUrl || '-');
        this.setText('cfg-mongodb-db', cfg.mongoDbDatabase || '-');
        this.setText('cfg-sqlite-path', cfg.sqlitePath || '-');

        // Extensions
        this.setEl('cfg-graphql', this.enabledPortBadge(cfg.graphqlEnabled, cfg.graphqlPort));
        this.setEl('cfg-mcp', this.enabledPortBadge(cfg.mcpEnabled, cfg.mcpPort));
        this.setEl('cfg-prometheus', this.enabledPortBadge(cfg.prometheusEnabled, cfg.prometheusPort));
        this.setEl('cfg-i3x', this.enabledPortBadge(cfg.i3xEnabled, cfg.i3xPort));
        this.setEl('cfg-metrics', this.enabledBadge(cfg.metricsEnabled));
        this.setText('cfg-kafka-servers', cfg.kafkaServers || '-');

        document.getElementById('config-content').style.display = 'block';
    }

    badge(type, text) {
        return `<span class="badge badge-${type}">${text}</span>`;
    }

    portLabel(port) {
        return `<span class="badge badge-port">:${port}</span>`;
    }

    portBadge(port) {
        if (!port || port === 0) {
            return `<span class="port-disabled">Disabled</span>`;
        }
        return `${this.badge('enabled', 'Active')} ${this.portLabel(port)}`;
    }

    enabledPortBadge(enabled, port) {
        if (!enabled) {
            return this.badge('disabled', 'Disabled');
        }
        if (port && port > 0) {
            return `${this.badge('enabled', 'Enabled')} ${this.portLabel(port)}`;
        }
        return this.badge('enabled', 'Enabled');
    }

    storeBadge(type) {
        if (!type || type === 'NONE') {
            return `<span class="badge badge-disabled">None</span>`;
        }
        return `<span class="badge badge-info">${type}</span>`;
    }

    enabledBadge(enabled) {
        return enabled ? this.badge('enabled', 'Enabled') : this.badge('disabled', 'Disabled');
    }

    allowedBadge(allowed) {
        return allowed ? this.badge('enabled', 'Allowed') : this.badge('disabled', 'Blocked');
    }

    setText(id, value) {
        const el = document.getElementById(id);
        if (el) el.textContent = value ?? '-';
    }

    setEl(id, html) {
        const el = document.getElementById(id);
        if (el) el.innerHTML = html;
    }

    showLoading(show) {
        const el = document.getElementById('loading-indicator');
        if (el) el.style.display = show ? 'flex' : 'none';
    }

    showError(message) {
        const e = document.getElementById('error-message');
        const t = document.querySelector('#error-message .error-text');
        if (e && t) {
            t.textContent = message;
            e.style.display = 'flex';
        }
    }

    hideError() {
        const e = document.getElementById('error-message');
        if (e) e.style.display = 'none';
    }
}

document.addEventListener('DOMContentLoaded', () => {
    window.systemConfigManager = new SystemConfigManager();
});
