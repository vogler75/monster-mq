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

    async deleteClient(clientName) {
        this.deleteClientName = clientName;
        if (await ui.confirmDelete(clientName, { title: 'Delete Kafka client' })) {
            await this.confirmDeleteClient();
        } else {
            this.deleteClientName = null;
        }
    }

    async confirmDeleteClient() {
        if (!this.deleteClientName) return;
        try {
            const mutation = `mutation DeleteKafkaClient($name: String!) { kafkaClient { delete(name: $name) } }`;
            const result = await this.client.query(mutation, { name: this.deleteClientName });
            if (result.kafkaClient.delete) {
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

    showLoading(show) { const el = document.getElementById('loading-indicator'); if (el) el.style.display = show ? 'flex' : 'none'; }
    showError(message) { ui.showError(message); }
    hideError() { const errorEl = document.getElementById('error-message'); if (errorEl) errorEl.style.display='none'; }
    showSuccess(message) { ui.success(message); }
    escapeHtml(text) { const div=document.createElement('div'); div.textContent=text; return div.innerHTML; }

    async refreshClients() { await this.loadClients(); }
}

// Global wrappers
function refreshKafkaClients() { kafkaClientManager.refreshClients(); }

// Initialize
let kafkaClientManager;
document.addEventListener('DOMContentLoaded', () => { kafkaClientManager = new KafkaClientManager(); });

