// Broker Configuration Page

class BrokerConfigManager {
    constructor() {
        this.client = new GraphQLDashboardClient();
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

        // Databases
        this.renderDatabases(cfg);

        document.getElementById('config-content').style.display = 'block';
    }

    renderDatabases(cfg) {
        const dbs = [];
        if (cfg.postgresUrl) {
            dbs.push({ name: 'PostgreSQL', icon: '🐘', items: [
                { label: 'URL', value: cfg.postgresUrl, mono: true },
                { label: 'User', value: cfg.postgresUser, mono: true },
            ]});
        }
        if (cfg.crateDbUrl) {
            dbs.push({ name: 'CrateDB', icon: '📦', items: [
                { label: 'URL', value: cfg.crateDbUrl, mono: true },
                { label: 'User', value: cfg.crateDbUser, mono: true },
            ]});
        }
        if (cfg.mongoDbUrl) {
            dbs.push({ name: 'MongoDB', icon: '🍃', items: [
                { label: 'URL', value: cfg.mongoDbUrl, mono: true },
                { label: 'Database', value: cfg.mongoDbDatabase, mono: true },
            ]});
        }
        if (cfg.sqlitePath) {
            dbs.push({ name: 'SQLite', icon: '💾', items: [
                { label: 'Path', value: cfg.sqlitePath, mono: true },
            ]});
        }
        if (cfg.kafkaServers) {
            dbs.push({ name: 'Kafka', icon: '📨', items: [
                { label: 'Servers', value: cfg.kafkaServers, mono: true },
            ]});
        }
        const container = document.getElementById('cfg-db-list');
        if (!container) return;
        if (dbs.length === 0) {
            container.innerHTML = '<span style="color:var(--text-muted);font-size:0.9rem;">No databases configured</span>';
            return;
        }
        container.innerHTML = dbs.map(db => `
            <div style="margin-bottom:1.25rem;">
                <div style="display:flex;align-items:center;gap:0.5rem;margin-bottom:0.75rem;">
                    <span style="font-size:1rem;">${db.icon}</span>
                    <span style="font-size:0.85rem;font-weight:700;color:var(--text-secondary);text-transform:uppercase;letter-spacing:0.06em;">${db.name}</span>
                </div>
                <div class="config-grid">
                    ${db.items.map(it => `
                        <div class="config-item" style="${it.label === 'URL' || it.label === 'Path' ? 'grid-column:1/-1;' : ''}">
                            <span class="label">${it.label}</span>
                            <span class="value${it.mono ? ' monospace' : ''}" style="word-break:break-all;">${it.value || '-'}</span>
                        </div>`).join('')}
                </div>
            </div>`).join('');
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
    showError(message) {
        // Also update the inline error div if present
        var errorDiv = document.getElementById('error-message');
        if (errorDiv) {
            var errorText = errorDiv.querySelector('.error-text');
            if (errorText) errorText.textContent = message;
            errorDiv.style.display = 'flex';
        }

        // Show a fixed-position toast so the error is always visible
        var existing = document.getElementById('error-toast');
        if (existing) existing.remove();

        var toast = document.createElement('div');
        toast.id = 'error-toast';
        toast.style.cssText = 'position:fixed;top:20px;left:50%;transform:translateX(-50%);background:var(--monster-red,#EF4444);color:#fff;padding:14px 24px;border-radius:8px;box-shadow:0 4px 12px rgba(0,0,0,0.4);z-index:10000;font-size:0.9rem;max-width:600px;display:flex;align-items:center;gap:10px;animation:slideDown 0.3s ease-out;';
        toast.innerHTML = '<span style="font-size:1.2rem;">&#9888;</span><span>' + message + '</span><button onclick="this.parentElement.remove()" style="background:none;border:none;color:#fff;cursor:pointer;margin-left:auto;font-size:1.1rem;line-height:1;padding:0 4px;">&times;</button>';

        // Add animation
        if (!document.getElementById('error-toast-style')) {
            var style = document.createElement('style');
            style.id = 'error-toast-style';
            style.textContent = '@keyframes slideDown{from{transform:translateX(-50%) translateY(-100%);opacity:0;}to{transform:translateX(-50%) translateY(0);opacity:1;}}';
            document.head.appendChild(style);
        }

        document.body.appendChild(toast);

        setTimeout(function() {
            if (toast.parentElement) toast.remove();
            if (errorDiv) errorDiv.style.display = 'none';
        }, 8000);
    }
    hideError() { const e = document.getElementById('error-message'); if (e) e.style.display = 'none'; }
}

let brokerConfigManager;
document.addEventListener('DOMContentLoaded', () => { brokerConfigManager = new BrokerConfigManager(); });
