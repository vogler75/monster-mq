// Kafka Client Management JavaScript

class KafkaClientManager {
    constructor() {
        this.client = new GraphQLDashboardClient();
        this.clients = [];
        this.clusterNodes = [];
        this.deleteClientName = null;
        this.init();
    }

    async init() {
        console.log('Initializing Kafka Client Manager...');
        await this.loadClients();
        setInterval(() => this.loadClients(), 30000);
    }

    async loadClients() {
        this.showLoading(true);
        this.hideError();
        try {
            const query = `
                query GetKafkaClients {
                    kafkaClients { 
                        name namespace nodeId enabled isOnCurrentNode createdAt updatedAt
                        config { groupId bootstrapServers destinationTopicPrefix reconnectDelayMs }
                        metrics { messagesIn messagesOut }
                    }
                }
            `;
            const result = await this.client.query(query);
            if (!result || !result.kafkaClients) throw new Error('Invalid response structure');
            this.clients = result.kafkaClients;
            this.updateMetrics();
            this.renderClientsTable();
        } catch (e) {
            console.error('Error loading Kafka clients:', e);
            this.showError('Failed to load Kafka clients: ' + e.message);
        } finally {
            this.showLoading(false);
        }
    }

    updateMetrics() {
        const totalClients = this.clients.length;
        const enabledClients = this.clients.filter(c => c.enabled).length;
        const currentNodeClients = this.clients.filter(c => c.isOnCurrentNode).length;
        document.getElementById('total-clients').textContent = totalClients;
        document.getElementById('enabled-clients').textContent = enabledClients;
        document.getElementById('current-node-clients').textContent = currentNodeClients;
    }

    renderClientsTable() {
        const tbody = document.getElementById('kafka-clients-table-body');
        if (!tbody) return;
        tbody.innerHTML = '';
        if (this.clients.length === 0) {
            tbody.innerHTML = `<tr><td colspan="7" class="no-data">No Kafka clients configured. Click "Add Client" to get started.</td></tr>`;
            return;
        }
        this.clients.forEach(client => {
            const row = document.createElement('tr');
            const statusClass = client.enabled ? 'status-enabled' : 'status-disabled';
            const statusText = client.enabled ? 'Enabled' : 'Disabled';
            const nodeIndicator = client.isOnCurrentNode ? '📍 ' : '';
            row.innerHTML = `
                <td><div class="client-name">${this.escapeHtml(client.name)}</div></td>
                <td><small class="client-namespace">${this.escapeHtml(client.namespace)}</small></td>
                <td>${nodeIndicator}${this.escapeHtml(client.nodeId || '')}</td>
                <td><span class="status-badge ${statusClass}">${statusText}</span></td>
                <td>${(client.metrics && client.metrics.length>0 ? Math.round(client.metrics[0].messagesIn) : 0)}</td>
                <td>${(client.metrics && client.metrics.length>0 ? Math.round(client.metrics[0].messagesOut) : 0)}</td>
                <td>
                    <div class="action-buttons">
                        <ix-icon-button icon="highlight" variant="primary" ghost size="24" title="Edit Client" onclick="kafkaClientManager.viewClient('${client.name}')"></ix-icon-button>
                        <ix-icon-button icon="${client.enabled ? 'pause' : 'play'}" variant="primary" ghost size="24" title="${client.enabled ? 'Disable Client' : 'Enable Client'}" onclick="kafkaClientManager.toggleClient('${client.name}', ${!client.enabled})"></ix-icon-button>
                        <ix-icon-button icon="trashcan" variant="primary" ghost size="24" class="btn-delete" title="Delete Client" onclick="kafkaClientManager.deleteClient('${client.name}')"></ix-icon-button>
                    </div>
                </td>
            `;
            tbody.appendChild(row);
        });
    }

    async toggleClient(clientName, enabled) {
        try {
            const mutation = `
                mutation ToggleKafkaClient($name: String!, $enabled: Boolean!) {
                    kafkaClient {
                        toggle(name: $name, enabled: $enabled) { success errors client { name enabled } }
                    }
                }
            `;
            const result = await this.client.query(mutation, { name: clientName, enabled });
            if (result.kafkaClient.toggle.success) {
                await this.loadClients();
                this.showSuccess(`Kafka client "${clientName}" ${enabled ? 'enabled' : 'disabled'} successfully`);
            } else {
                const errors = result.kafkaClient.toggle.errors || ['Unknown error'];
                this.showError('Failed to toggle Kafka client: ' + errors.join(', '));
            }
        } catch (e) {
            console.error('Error toggling Kafka client:', e);
            this.showError('Failed to toggle Kafka client: ' + e.message);
        }
    }

    deleteClient(clientName) {
        this.deleteClientName = clientName;
        document.getElementById('delete-kafka-client-name').textContent = clientName;
        this.showConfirmDeleteModal();
    }

    async confirmDeleteClient() {
        if (!this.deleteClientName) return;
        try {
            const mutation = `mutation DeleteKafkaClient($name: String!) { kafkaClient { delete(name: $name) } }`;
            const result = await this.client.query(mutation, { name: this.deleteClientName });
            if (result.kafkaClient.delete) {
                this.hideConfirmDeleteModal();
                await this.loadClients();
                this.showSuccess(`Kafka client "${this.deleteClientName}" deleted successfully`);
            } else {
                this.showError('Failed to delete Kafka client');
            }
        } catch (e) {
            console.error('Error deleting Kafka client:', e);
            this.showError('Failed to delete Kafka client: ' + e.message);
        }
        this.deleteClientName = null;
    }

    viewClient(clientName) {
        window.spaLocation.href = `/pages/kafka-client-detail.html?client=${encodeURIComponent(clientName)}`;
    }

    // UI helpers
    showConfirmDeleteModal() { document.getElementById('confirm-delete-kafka-client-modal').style.display = 'flex'; }
    hideConfirmDeleteModal() { document.getElementById('confirm-delete-kafka-client-modal').style.display = 'none'; }

    showLoading(show) { const el = document.getElementById('loading-indicator'); if (el) el.style.display = show ? 'flex' : 'none'; }
    showError(message) { const errorEl = document.getElementById('error-message'); const errorText = document.querySelector('#error-message .error-text'); if (errorEl && errorText) { errorText.textContent = message; errorEl.style.display = 'flex'; setTimeout(()=>this.hideError(),5000);} }
    hideError() { const errorEl = document.getElementById('error-message'); if (errorEl) errorEl.style.display='none'; }
    showSuccess(message) { var existing = document.getElementById('success-toast'); if (existing) existing.remove(); var toast = document.createElement('div'); toast.id = 'success-toast'; toast.style.cssText = 'position:fixed;top:20px;left:50%;transform:translateX(-50%);background:var(--monster-green,#10B981);color:#fff;padding:14px 24px;border-radius:8px;box-shadow:0 4px 12px rgba(0,0,0,0.4);z-index:10000;font-size:0.9rem;max-width:600px;display:flex;align-items:center;gap:10px;animation:slideDown 0.3s ease-out;'; toast.innerHTML = '<span style="font-size:1.2rem;">&#10003;</span><span>' + this.escapeHtml(message) + '</span><button onclick="this.parentElement.remove()" style="background:none;border:none;color:#fff;cursor:pointer;margin-left:auto;font-size:1.1rem;line-height:1;padding:0 4px;">&times;</button>'; if (!document.getElementById('toast-anim-style')) { var s = document.createElement('style'); s.id = 'toast-anim-style'; s.textContent = '@keyframes slideDown{from{transform:translateX(-50%) translateY(-100%);opacity:0;}to{transform:translateX(-50%) translateY(0);opacity:1;}}@keyframes fadeOut{from{opacity:1;}to{opacity:0;}}'; document.head.appendChild(s); } document.body.appendChild(toast); setTimeout(function() { if (toast.parentElement) { toast.style.animation = 'fadeOut 0.3s ease-out forwards'; setTimeout(function() { if (toast.parentElement) toast.remove(); }, 300); } }, 3000); }
    escapeHtml(text) { const div=document.createElement('div'); div.textContent=text; return div.innerHTML; }

    async refreshClients() { await this.loadClients(); }
}

// Global wrappers
function refreshKafkaClients() { kafkaClientManager.refreshClients(); }
function hideConfirmDeleteKafkaClientModal() { kafkaClientManager.hideConfirmDeleteModal(); }
function confirmDeleteKafkaClient() { kafkaClientManager.confirmDeleteClient(); }

// Initialize
let kafkaClientManager;
document.addEventListener('DOMContentLoaded', () => { kafkaClientManager = new KafkaClientManager(); });

document.addEventListener('click', e => { if (e.target.classList.contains('modal')) { if (e.target.id === 'confirm-delete-kafka-client-modal') kafkaClientManager.hideConfirmDeleteModal(); }});
