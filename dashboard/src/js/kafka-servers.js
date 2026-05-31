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
            tbody.innerHTML = `<tr><td colspan="6" class="no-data">No Kafka servers configured. Click "Add Server" to get started.</td></tr>`;
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
                        <ix-icon-button icon="highlight" variant="primary" ghost size="24" title="Edit Server" onclick="kafkaServerManager.viewServer('${server.name}')"></ix-icon-button>
                        <ix-icon-button icon="${server.enabled ? 'pause' : 'play'}" variant="primary" ghost size="24" title="${server.enabled ? 'Disable Server' : 'Enable Server'}" onclick="kafkaServerManager.toggleServer('${server.name}', ${!server.enabled})"></ix-icon-button>
                        <ix-icon-button icon="trashcan" variant="primary" ghost size="24" class="btn-delete" title="Delete Server" onclick="kafkaServerManager.deleteServer('${server.name}')"></ix-icon-button>
                    </div>
                </td>
            `;
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
            const toggleRes = result.kafkaServer.toggle;
            if (toggleRes.success) {
                await this.loadServers();
                this.showSuccess(`Kafka server "${serverName}" ${enabled ? 'enabled' : 'disabled'} successfully`);
            } else {
                const errors = toggleRes.errors || ['Unknown error'];
                this.showError('Failed to toggle Kafka server: ' + errors.join(', '));
            }
        } catch (e) {
            console.error('Error toggling Kafka server:', e);
            this.showError('Failed to toggle Kafka server: ' + e.message);
        }
    }

    deleteServer(serverName) {
        this.deleteServerName = serverName;
        document.getElementById('delete-kafka-server-name').textContent = serverName;
        this.showConfirmDeleteModal();
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
            if (result.kafkaServer.delete) {
                this.hideConfirmDeleteModal();
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
        window.navigateTo(`/pages/kafka-server-detail.html?server=${encodeURIComponent(serverName)}`);
    }

    // UI helpers
    showConfirmDeleteModal() { document.getElementById('confirm-delete-kafka-server-modal').style.display = 'flex'; }
    hideConfirmDeleteModal() { document.getElementById('confirm-delete-kafka-server-modal').style.display = 'none'; }

    showLoading(show) { const el = document.getElementById('loading-indicator'); if (el) el.style.display = show ? 'flex' : 'none'; }
    showError(message) { const errorEl = document.getElementById('error-message'); const errorText = document.querySelector('#error-message .error-text'); if (errorEl && errorText) { errorText.textContent = message; errorEl.style.display = 'flex'; setTimeout(()=>this.hideError(),5000);} }
    hideError() { const errorEl = document.getElementById('error-message'); if (errorEl) errorEl.style.display='none'; }
    showSuccess(message) { var existing = document.getElementById('success-toast'); if (existing) existing.remove(); var toast = document.createElement('div'); toast.id = 'success-toast'; toast.style.cssText = 'position:fixed;top:20px;left:50%;transform:translateX(-50%);background:var(--monster-green,#10B981);color:#fff;padding:14px 24px;border-radius:8px;box-shadow:0 4px 12px rgba(0,0,0,0.4);z-index:10000;font-size:0.9rem;max-width:600px;display:flex;align-items:center;gap:10px;animation:slideDown 0.3s ease-out;'; toast.innerHTML = '<span style="font-size:1.2rem;">&#10003;</span><span>' + this.escapeHtml(message) + '</span><button onclick="this.parentElement.remove()" style="background:none;border:none;color:#fff;cursor:pointer;margin-left:auto;font-size:1.1rem;line-height:1;padding:0 4px;">&times;</button>'; if (!document.getElementById('toast-anim-style')) { var s = document.createElement('style'); s.id = 'toast-anim-style'; s.textContent = '@keyframes slideDown{from{transform:translateX(-50%) translateY(-100%);opacity:0;}to{transform:translateX(-50%) translateY(0);opacity:1;}}@keyframes fadeOut{from{opacity:1;}to{opacity:0;}}'; document.head.appendChild(s); } document.body.appendChild(toast); setTimeout(function() { if (toast.parentElement) { toast.style.animation = 'fadeOut 0.3s ease-out forwards'; setTimeout(function() { if (toast.parentElement) toast.remove(); }, 300); } }, 3000); }
    escapeHtml(text) { const div=document.createElement('div'); div.textContent=text; return div.innerHTML; }

    async refreshServers() { await this.loadServers(); }
}

// Global wrappers
function refreshKafkaServers() { kafkaServerManager.refreshServers(); }
var hideConfirmDeleteKafkaServerModal = () => kafkaServerManager.hideConfirmDeleteModal();
var confirmDeleteKafkaServer = () => kafkaServerManager.confirmDeleteServer();

// Initialize
var kafkaServerManager;
document.addEventListener('DOMContentLoaded', () => { kafkaServerManager = new KafkaServerManager(); });

document.addEventListener('click', e => { if (e.target.classList.contains('modal')) { if (e.target.id === 'confirm-delete-kafka-server-modal') kafkaServerManager.hideConfirmDeleteModal(); }});
