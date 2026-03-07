// Broker Configuration Page

class BrokerConfigManager {
    constructor() {
        this.client = new GraphQLDashboardClient('/graphql');
        this.init();
    }

    async init() {
        await this.loadConfig();
    }

    async loadConfig() {
        this.showLoading(true);
        this.hideError();
        try {
            const query = `
                query GetBrokerConfig {
                    brokerConfig {
                        nodeId version clustered
                        tcpPort wsPort tcpsPort wssPort natsPort
                        sessionStoreType retainedStoreType configStoreType
                        userManagementEnabled anonymousEnabled
                        mcpEnabled mcpPort
                        prometheusEnabled prometheusPort
                        i3xEnabled i3xPort
                        graphqlEnabled graphqlPort mqttApiEnabled
                        metricsEnabled
                        genAiEnabled genAiProvider genAiModel
                    }
                }
            `;
            const result = await this.client.query(query);
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
        this.setEl('cfg-user-mgmt', cfg.userManagementEnabled
            ? this.badge('enabled', 'Enabled')
            : this.badge('disabled', 'Disabled'));
        this.setEl('cfg-anonymous', cfg.anonymousEnabled
            ? this.badge('enabled', 'Allowed')
            : this.badge('disabled', 'Blocked'));

        // Extensions
        this.setEl('cfg-graphql', cfg.graphqlEnabled
            ? `${this.badge('enabled', 'Enabled')} ${this.portLabel(cfg.graphqlPort)}`
            : this.badge('disabled', 'Disabled'));
        this.setEl('cfg-mqtt-api', cfg.mqttApiEnabled
            ? this.badge('enabled', 'Enabled')
            : this.badge('disabled', 'Disabled'));
        this.setEl('cfg-mcp', cfg.mcpEnabled
            ? `${this.badge('enabled', 'Enabled')} ${this.portLabel(cfg.mcpPort)}`
            : this.badge('disabled', 'Disabled'));
        this.setEl('cfg-prometheus', cfg.prometheusEnabled
            ? `${this.badge('enabled', 'Enabled')} ${this.portLabel(cfg.prometheusPort)}`
            : this.badge('disabled', 'Disabled'));
        this.setEl('cfg-i3x', cfg.i3xEnabled
            ? `${this.badge('enabled', 'Enabled')} ${this.portLabel(cfg.i3xPort)}`
            : this.badge('disabled', 'Disabled'));
        this.setEl('cfg-metrics', cfg.metricsEnabled
            ? this.badge('enabled', 'Enabled')
            : this.badge('disabled', 'Disabled'));

        // GenAI
        this.setEl('cfg-genai', cfg.genAiEnabled
            ? this.badge('enabled', 'Enabled')
            : this.badge('disabled', 'Disabled'));
        this.setText('cfg-genai-provider', cfg.genAiEnabled && cfg.genAiProvider ? cfg.genAiProvider : '-');
        this.setText('cfg-genai-model', cfg.genAiEnabled && cfg.genAiModel ? cfg.genAiModel : '-');

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

    storeBadge(type) {
        if (!type || type === 'NONE') {
            return `<span class="badge badge-disabled">None</span>`;
        }
        return `<span class="badge badge-info">${type}</span>`;
    }

    setText(id, value) {
        const el = document.getElementById(id);
        if (el) el.textContent = value ?? '-';
    }

    setEl(id, html) {
        const el = document.getElementById(id);
        if (el) el.innerHTML = html;
    }

    showLoading(show) { const el = document.getElementById('loading-indicator'); if (el) el.style.display = show ? 'flex' : 'none'; }
    showError(message) { const e = document.getElementById('error-message'); const t = document.querySelector('#error-message .error-text'); if (e && t) { t.textContent = message; e.style.display = 'flex'; } }
    hideError() { const e = document.getElementById('error-message'); if (e) e.style.display = 'none'; }
}

let brokerConfigManager;
document.addEventListener('DOMContentLoaded', () => { brokerConfigManager = new BrokerConfigManager(); });
