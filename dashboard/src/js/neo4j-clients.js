// Neo4j Client Management JavaScript

class Neo4jClientManager {
    constructor() {
        this.client = new GraphQLDashboardClient();
        this.clients = [];
        this.clusterNodes = [];
        this.deleteClientName = null;
        this.init();
    }

    async init() {
        await this.loadClients();
        setInterval(() => this.loadClients(), 30000);
    }

    async loadClients() {
        this.showLoading(true);
        this.hideError();
        try {
            const query = `
                query GetNeo4jClients {
                    neo4jClients {
                        name namespace nodeId enabled isOnCurrentNode createdAt updatedAt
                        config { url username topicFilters queueSize batchSize reconnectDelayMs }
                        metrics { messagesIn messagesWritten errors pathQueueSize messagesInRate messagesWrittenRate }
                    }
                }
            `;
            const result = await this.client.query(query);
            if (!result || !result.neo4jClients) throw new Error('Invalid response structure');
            this.clients = result.neo4jClients;
            this.updateMetrics();
            this.renderClientsTable();
        } catch (e) {
            console.error('Error loading Neo4j clients:', e);
            this.showError('Failed to load Neo4j clients: ' + e.message);
        } finally {
            this.showLoading(false);
        }
    }

    updateMetrics() {
        const totalClients = this.clients.length;
        const enabledClients = this.clients.filter(c => c.enabled).length;
        const currentNodeClients = this.clients.filter(c => c.isOnCurrentNode).length;
        const totalMessages = this.clients.reduce((sum, c) => {
            return sum + (c.metrics && c.metrics.length > 0 ? c.metrics[0].messagesIn : 0);
        }, 0);

        document.getElementById('total-clients').textContent = totalClients;
        document.getElementById('enabled-clients').textContent = enabledClients;
        document.getElementById('current-node-clients').textContent = currentNodeClients;
        document.getElementById('total-messages').textContent = Math.round(totalMessages);
    }

    renderClientsTable() {
        const tbody = document.getElementById('neo4j-clients-table-body');
        if (!tbody) return;
        tbody.innerHTML = '';
        if (this.clients.length === 0) {
            tbody.innerHTML = ui.emptyRow(8, 'No Neo4j clients configured',
                'Use “Add Client” to get started.');
            return;
        }
        this.clients.forEach(client => {
            const row = document.createElement('tr');
            const statusClass = client.enabled ? 'status-enabled' : 'status-disabled';
            const statusText = client.enabled ? 'Enabled' : 'Disabled';
            const nodeIndicator = client.isOnCurrentNode ? '📍 ' : '';
            const metrics = client.metrics && client.metrics.length > 0 ? client.metrics[0] : null;

            row.innerHTML = `
                <td><div class="client-name">${this.escapeHtml(client.name)}</div></td>
                <td><small class="client-namespace">${this.escapeHtml(client.namespace)}</small></td>
                <td>${nodeIndicator}${this.escapeHtml(client.nodeId || '')}</td>
                <td><span class="status-badge ${statusClass}">${statusText}</span></td>
                <td>${metrics ? Math.round(metrics.messagesIn) : 0}</td>
                <td>${metrics ? Math.round(metrics.messagesWritten) : 0}</td>
                <td>${metrics ? metrics.pathQueueSize : 0}</td>
                <td>
                    <div class="action-buttons">
                        <ix-icon-button icon="highlight" variant="subtle-tertiary" size="24" title="Edit Client" onclick="neo4jClientManager.viewClient('${client.name}')"></ix-icon-button>
                        <ix-icon-button icon="${client.enabled ? 'pause' : 'play'}" variant="subtle-tertiary" size="24" title="${client.enabled ? 'Disable Client' : 'Enable Client'}" onclick="neo4jClientManager.toggleClient('${client.name}', ${!client.enabled})"></ix-icon-button>
                        <ix-icon-button icon="trashcan" variant="subtle-tertiary" size="24" class="btn-delete" title="Delete Client" onclick="neo4jClientManager.deleteClient('${client.name}')"></ix-icon-button>
                    </div>
                </td>
            `;
            tbody.appendChild(row);
        });
    }

    async toggleClient(clientName, enabled) {
        try {
            const mutation = `
                mutation ToggleNeo4jClient($name: String!, $enabled: Boolean!) {
                    neo4jClient {
                        toggle(name: $name, enabled: $enabled) { success errors client { name enabled } }
                    }
                }
            `;
            const result = await this.client.query(mutation, { name: clientName, enabled });
            if (result.neo4jClient.toggle.success) {
                await this.loadClients();
                this.showSuccess(`Neo4j client "${clientName}" ${enabled ? 'enabled' : 'disabled'} successfully`);
            } else {
                const errors = result.neo4jClient.toggle.errors || ['Unknown error'];
                this.showError('Failed to toggle Neo4j client: ' + errors.join(', '));
            }
        } catch (e) {
            console.error('Error toggling Neo4j client:', e);
            this.showError('Failed to toggle Neo4j client: ' + e.message);
        }
    }

    async deleteClient(clientName) {
        this.deleteClientName = clientName;
        if (await ui.confirmDelete(clientName, { title: 'Delete Neo4j client' })) {
            await this.confirmDeleteClient();
        } else {
            this.deleteClientName = null;
        }
    }

    async confirmDeleteClient() {
        if (!this.deleteClientName) return;
        try {
            const mutation = `mutation DeleteNeo4jClient($name: String!) { neo4jClient { delete(name: $name) } }`;
            const result = await this.client.query(mutation, { name: this.deleteClientName });
            if (result.neo4jClient.delete) {
                await this.loadClients();
                this.showSuccess(`Neo4j client "${this.deleteClientName}" deleted successfully`);
            } else {
                this.showError('Failed to delete Neo4j client');
            }
        } catch (e) {
            console.error('Error deleting Neo4j client:', e);
            this.showError('Failed to delete Neo4j client: ' + e.message);
        }
        this.deleteClientName = null;
    }

    viewClient(clientName) {
        window.spaLocation.href = `/pages/neo4j-client-detail.html?client=${encodeURIComponent(clientName)}`;
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
function refreshNeo4jClients() { neo4jClientManager.refreshClients(); }

// Initialize
let neo4jClientManager;
document.addEventListener('DOMContentLoaded', () => { neo4jClientManager = new Neo4jClientManager(); });
