// Kafka Server Management JavaScript

class KafkaServerManager {
    constructor() {
        this.client = new GraphQLDashboardClient();
        this.servers = [];
        this.deleteServerName = null;
        this.startingPollInterval = null;
        this.init();
    }

    async init() {
        console.log('Initializing Kafka Server Manager...');
        await this.loadServers();
        // Auto refresh every 15 seconds for status updates
        this.refreshInterval = setInterval(() => this.loadServers(), 15000);
        window.registerPageCleanup(() => {
            clearInterval(this.refreshInterval);
            if (this.startingPollInterval) {
                clearInterval(this.startingPollInterval);
            }
        });
    }

    async loadServers() {
        this.showLoading(true);
        this.hideError();
        try {
            const query = `
                query GetKafkaServers {
                    kafkaServers { 
                        name host port nodeId enabled isOnCurrentNode status
                        streams { topicFilter retentionHours }
                    }
                }
            `;
            const result = await this.client.query(query);
            if (!result || !result.kafkaServers) throw new Error('Invalid response structure');
            this.servers = result.kafkaServers;
            this.updateMetrics();
            this.renderServersTable();
        } catch (e) {
            console.error('Error loading Kafka servers:', e);
            this.showError('Failed to load Kafka servers: ' + e.message);
        } finally {
            this.showLoading(false);
        }
    }

    updateMetrics() {
        const totalServers = this.servers.length;
        const runningServers = this.servers.filter(s => s.status === 'RUNNING').length;
        const currentNodeServers = this.servers.filter(s => s.isOnCurrentNode).length;
        
        let totalStreams = 0;
        this.servers.forEach(s => {
            if (s.streams) totalStreams += s.streams.length;
        });

        document.getElementById('total-servers').textContent = totalServers;
        document.getElementById('running-servers').textContent = runningServers;
        document.getElementById('current-node-servers').textContent = currentNodeServers;
        document.getElementById('total-streams').textContent = totalStreams;
    }

    renderServersTable() {
        const tbody = document.getElementById('kafka-servers-table-body');
        if (!tbody) return;
        tbody.innerHTML = '';
        if (this.servers.length === 0) {
            tbody.innerHTML = ui.emptyRow(6, 'No Kafka servers configured',
                'Use “Add Server” to get started.');
            return;
        }
        this.servers.forEach(server => {
            const row = document.createElement('tr');
            
            let statusClass = 'status-stopped';
            if (server.status === 'RUNNING') statusClass = 'status-running';
            else if (server.status === 'STARTING') statusClass = 'status-starting';
            else if (server.status === 'ERROR') statusClass = 'status-error';
            else if (!server.enabled) statusClass = 'status-stopped';
            
            const nodeIndicator = server.isOnCurrentNode ? '📍 ' : '';
            row.innerHTML = `
                <td><div class="client-name" style="font-weight:600; color:var(--text-primary);">${this.escapeHtml(server.name)}</div></td>
                <td><small class="client-namespace" style="font-family:monospace; color:var(--text-muted);">${this.escapeHtml(server.host)}</small></td>
                <td><span style="font-weight:500;">${server.port}</span></td>
                <td>${nodeIndicator}${this.escapeHtml(server.nodeId || '')}</td>
                <td><span class="status-badge ${statusClass}">${this.escapeHtml(server.status)}</span></td>
                <td>
                    <div class="action-buttons">
                        <ix-icon-button icon="highlight" variant="subtle-tertiary" size="24" title="Edit Server" class="btn-edit"></ix-icon-button>
                        <ix-icon-button icon="${server.enabled ? 'pause' : 'play'}" variant="subtle-tertiary" size="24" title="${server.enabled ? 'Disable Server' : 'Enable Server'}" class="btn-toggle"></ix-icon-button>
                        <ix-icon-button icon="trashcan" variant="subtle-tertiary" size="24" class="btn-delete" title="Delete Server"></ix-icon-button>
                    </div>
                </td>
            `;

            const editBtn = row.querySelector('.btn-edit');
            if (editBtn) editBtn.addEventListener('click', (e) => { e.stopPropagation(); this.viewServer(server.name); });
            const toggleBtn = row.querySelector('.btn-toggle');
            if (toggleBtn) toggleBtn.addEventListener('click', (e) => { e.stopPropagation(); this.toggleServer(server.name, !server.enabled); });
            const deleteBtn = row.querySelector('.btn-delete');
            if (deleteBtn) deleteBtn.addEventListener('click', (e) => { e.stopPropagation(); this.deleteServer(server.name); });

            tbody.appendChild(row);
        });

        // Fast status polling if any server is starting
        if (this.servers.some(s => s.status === 'STARTING')) {
            if (!this.startingPollInterval) {
                console.log('Starting fast status poll for deploying servers...');
                this.startingPollInterval = setInterval(() => this.loadServers(), 1000);
            }
        } else {
            if (this.startingPollInterval) {
                console.log('Clearing fast status poll...');
                clearInterval(this.startingPollInterval);
                this.startingPollInterval = null;
            }
        }
    }

    async toggleServer(serverName, enabled) {
        try {
            const mutation = `
                mutation ToggleKafkaServer($name: String!, $enabled: Boolean!) {
                    kafkaServer {
                        toggle(name: $name, enabled: $enabled) { 
                            success 
                            errors 
                            server { name enabled status } 
                        }
                    }
                }
            `;
            const result = await this.client.query(mutation, { name: serverName, enabled });
            const toggleRes = result && result.kafkaServer && result.kafkaServer.toggle;
            if (toggleRes && toggleRes.success) {
                await this.loadServers();
                this.showSuccess(`Kafka server "${serverName}" ${enabled ? 'enabled' : 'disabled'} successfully`);
            } else {
                const errors = (toggleRes && toggleRes.errors) || ['Unknown error'];
                this.showError('Failed to toggle Kafka server: ' + errors.join(', '));
            }
        } catch (e) {
            console.error('Error toggling Kafka server:', e);
            this.showError('Failed to toggle Kafka server: ' + e.message);
        }
    }

    async deleteServer(serverName) {
        this.deleteServerName = serverName;
        if (await ui.confirmDelete(serverName, { title: 'Delete Kafka server' })) {
            await this.confirmDeleteServer();
        } else {
            this.deleteServerName = null;
        }
    }

    async confirmDeleteServer() {
        if (!this.deleteServerName) return;
        try {
            const mutation = `
                mutation DeleteKafkaServer($name: String!) { 
                    kafkaServer { 
                        delete(name: $name) 
                    } 
                }
            `;
            const result = await this.client.query(mutation, { name: this.deleteServerName });
            if (result && result.kafkaServer && result.kafkaServer.delete) {
                await this.loadServers();
                this.showSuccess(`Kafka server "${this.deleteServerName}" deleted successfully`);
            } else {
                this.showError('Failed to delete Kafka server');
            }
        } catch (e) {
            console.error('Error deleting Kafka server:', e);
            this.showError('Failed to delete Kafka server: ' + e.message);
        }
        this.deleteServerName = null;
    }

    viewServer(serverName) {
        window.spaLocation.href = `/pages/kafka-server-detail.html?server=${encodeURIComponent(serverName)}`;
    }

    // UI helpers

    showLoading(show) { const el = document.getElementById('loading-indicator'); if (el) el.style.display = show ? 'flex' : 'none'; }
    showError(message) { ui.showError(message); }
    hideError() { const errorEl = document.getElementById('error-message'); if (errorEl) errorEl.style.display='none'; }
    showSuccess(message) { ui.success(message); }
    escapeHtml(text) { const div=document.createElement('div'); div.textContent=text; return div.innerHTML; }
    escapeAttr(text) {
        if (!text) return '';
        return text.replace(/&/g, '&amp;').replace(/"/g, '&quot;').replace(/'/g, '&#39;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
    }

    async refreshServers() { await this.loadServers(); }
}

// Global wrappers
var kafkaServerManager;

function refreshKafkaServers() {
    if (window.kafkaServerManager) window.kafkaServerManager.refreshServers();
}
var confirmDeleteKafkaServer = () => {
    if (window.kafkaServerManager) window.kafkaServerManager.confirmDeleteServer();
};

// Initialize
document.addEventListener('DOMContentLoaded', () => {
    kafkaServerManager = new KafkaServerManager();
    window.kafkaServerManager = kafkaServerManager;
});
