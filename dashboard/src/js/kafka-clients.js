// Kafka Client Management JavaScript

class KafkaClientManager {
    constructor() {
        this.client = new GraphQLDashboardClient('/graphql');
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
                        <button class="btn-icon btn-view" onclick="kafkaClientManager.viewClient('${client.name}')" title="Edit Client" aria-label="Edit Client">
                            <svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor"><path d="M3 17.25V21h3.75L17.81 9.94l-3.75-3.75L3 17.25zM20.71 7.04c.39-.39.39-1.02 0-1.41l-2.34-2.34c-.39-.39-1.02-.39-1.41 0l-1.83 1.83 3.75 3.75 1.83-1.83z"/></svg>
                        </button>
                        <button class="btn-icon ${client.enabled ? 'btn-pause' : 'btn-play'}" onclick="kafkaClientManager.toggleClient('${client.name}', ${!client.enabled})" title="${client.enabled ? 'Disable Client' : 'Enable Client'}" aria-label="${client.enabled ? 'Disable Client' : 'Enable Client'}">
                            ${client.enabled ? '<svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor"><path d="M6 19h4V5H6v14zm8-14v14h4V5h-4z"/></svg>' : '<svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor"><path d="M8 5v14l11-7z"/></svg>'}
                        </button>
                        <button class="btn-icon btn-delete" onclick="kafkaClientManager.deleteClient('${client.name}')" title="Delete Client" aria-label="Delete Client">
                            <svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor"><path d="M6 19c0 1.1.9 2 2 2h8c1.1 0 2-.9 2-2V7H6v12zM19 4h-3.5l-1-1h-5l-1 1H5v2h14V4z"/></svg>
                        </button>
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
        window.location.href = `/pages/kafka-client-detail.html?client=${encodeURIComponent(clientName)}`;
    }

    // UI helpers
    showConfirmDeleteModal() { document.getElementById('confirm-delete-kafka-client-modal').style.display = 'flex'; }
    hideConfirmDeleteModal() { document.getElementById('confirm-delete-kafka-client-modal').style.display = 'none'; }

    showLoading(show) { const el = document.getElementById('loading-indicator'); if (el) el.style.display = show ? 'flex' : 'none'; }
    showError(message) { const errorEl = document.getElementById('error-message'); const errorText = document.querySelector('#error-message .error-text'); if (errorEl && errorText) { errorText.textContent = message; errorEl.style.display = 'flex'; setTimeout(()=>this.hideError(),5000);} }
    hideError() { const errorEl = document.getElementById('error-message'); if (errorEl) errorEl.style.display='none'; }
    showSuccess(message) { const notification = document.createElement('div'); notification.className='success-notification'; notification.innerHTML = `<span class="success-icon">✅</span><span class="success-text">${this.escapeHtml(message)}</span>`; document.body.appendChild(notification); setTimeout(()=>{ if(notification.parentNode) notification.parentNode.removeChild(notification); },3000); }
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
