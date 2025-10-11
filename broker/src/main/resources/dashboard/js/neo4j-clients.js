// Neo4j Client Management JavaScript

class Neo4jClientManager {
    constructor() {
        this.client = new GraphQLDashboardClient('/graphql');
        this.clients = [];
        this.clusterNodes = [];
        this.deleteClientName = null;
        this.init();
    }

    async init() {
        console.log('Initializing Neo4j Client Manager...');
        await this.loadClusterNodes();
        await this.loadClients();
        setInterval(() => this.loadClients(), 30000);
    }

    async loadClusterNodes() {
        try {
            const query = `
                query GetClusterNodes { clusterNodes { nodeId isCurrent } }
            `;
            const result = await this.client.query(query);
            this.clusterNodes = result.clusterNodes || [];
            const nodeSelect = document.getElementById('neo4j-client-node');
            if (nodeSelect) {
                nodeSelect.innerHTML = '<option value="">Select Node...</option>';
                this.clusterNodes.forEach(node => {
                    const option = document.createElement('option');
                    option.value = node.nodeId;
                    option.textContent = node.nodeId + (node.isCurrent ? ' (Current)' : '');
                    nodeSelect.appendChild(option);
                });
            }
        } catch (e) {
            console.error('Error loading cluster nodes', e);
        }
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
            tbody.innerHTML = `<tr><td colspan="8" class="no-data">No Neo4j clients configured. Click "Add Client" to get started.</td></tr>`;
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
                        <button class="btn-action btn-view" onclick="neo4jClientManager.viewClient('${client.name}')" title="Edit Client">
                            <svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor"><path d="M3 17.25V21h3.75L17.81 9.94l-3.75-3.75L3 17.25zM20.71 7.04c.39-.39.39-1.02 0-1.41l-2.34-2.34c-.39-.39-1.02-.39-1.41 0l-1.83 1.83 3.75 3.75 1.83-1.83z"/></svg>
                        </button>
                        <button class="btn-action ${client.enabled ? 'btn-pause' : 'btn-play'}" onclick="neo4jClientManager.toggleClient('${client.name}', ${!client.enabled})" title="${client.enabled ? 'Disable Client' : 'Enable Client'}">
                            ${client.enabled ? '<svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor"><path d="M6 19h4V5H6v14zm8-14v14h4V5h-4z"/></svg>' : '<svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor"><path d="M8 5v14l11-7z"/></svg>'}
                        </button>
                        <button class="btn-action btn-delete" onclick="neo4jClientManager.deleteClient('${client.name}')" title="Delete Client">
                            <svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor"><path d="M6 19c0 1.1.9 2 2 2h8c1.1 0 2-.9 2-2V7H6v12zM19 4h-3.5l-1-1h-5l-1 1H5v2h14V4z"/></svg>
                        </button>
                    </div>
                </td>
            `;
            tbody.appendChild(row);
        });
    }

    async addClient() {
        const form = document.getElementById('add-neo4j-client-form');
        if (!form.checkValidity()) { form.reportValidity(); return; }

        // Parse topic filters from textarea
        const topicFiltersText = document.getElementById('neo4j-client-topic-filters').value.trim();
        const topicFilters = topicFiltersText
            .split('\n')
            .map(line => line.trim())
            .filter(line => line.length > 0);

        const clientData = {
            name: document.getElementById('neo4j-client-name').value.trim(),
            namespace: document.getElementById('neo4j-client-namespace').value.trim(),
            nodeId: document.getElementById('neo4j-client-node').value,
            enabled: document.getElementById('neo4j-client-enabled').checked,
            config: {
                url: document.getElementById('neo4j-client-url').value.trim(),
                username: document.getElementById('neo4j-client-username').value.trim(),
                password: document.getElementById('neo4j-client-password').value,
                topicFilters: topicFilters,
                queueSize: parseInt(document.getElementById('neo4j-client-queue-size').value),
                batchSize: parseInt(document.getElementById('neo4j-client-batch-size').value),
                reconnectDelayMs: parseInt(document.getElementById('neo4j-client-reconnect-delay').value)
            }
        };

        try {
            const mutation = `
                mutation CreateNeo4jClient($input: Neo4jClientInput!) {
                    neo4jClient {
                        create(input: $input) { success errors client { name enabled } }
                    }
                }
            `;
            const result = await this.client.query(mutation, { input: clientData });
            if (result.neo4jClient.create.success) {
                this.hideAddClientModal();
                await this.loadClients();
                this.showSuccess(`Neo4j client "${clientData.name}" added successfully`);
            } else {
                const errors = result.neo4jClient.create.errors || ['Unknown error'];
                this.showError('Failed to add Neo4j client: ' + errors.join(', '));
            }
        } catch (e) {
            console.error('Error adding Neo4j client:', e);
            this.showError('Failed to add Neo4j client: ' + e.message);
        }
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

    deleteClient(clientName) {
        this.deleteClientName = clientName;
        document.getElementById('delete-neo4j-client-name').textContent = clientName;
        this.showConfirmDeleteModal();
    }

    async confirmDeleteClient() {
        if (!this.deleteClientName) return;
        try {
            const mutation = `mutation DeleteNeo4jClient($name: String!) { neo4jClient { delete(name: $name) } }`;
            const result = await this.client.query(mutation, { name: this.deleteClientName });
            if (result.neo4jClient.delete) {
                this.hideConfirmDeleteModal();
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
        window.location.href = `/pages/neo4j-client-detail.html?client=${encodeURIComponent(clientName)}`;
    }

    // UI helpers
    showAddClientModal() {
        document.getElementById('add-neo4j-client-modal').style.display = 'flex';
        document.getElementById('add-neo4j-client-form').reset();
        document.getElementById('neo4j-client-enabled').checked = true;
        document.getElementById('neo4j-client-url').value = 'bolt://localhost:7687';
        document.getElementById('neo4j-client-username').value = 'neo4j';
        document.getElementById('neo4j-client-topic-filters').value = '#';
        document.getElementById('neo4j-client-queue-size').value = '10000';
        document.getElementById('neo4j-client-batch-size').value = '100';
        document.getElementById('neo4j-client-reconnect-delay').value = '5000';
    }
    hideAddClientModal() { document.getElementById('add-neo4j-client-modal').style.display = 'none'; }
    showConfirmDeleteModal() { document.getElementById('confirm-delete-neo4j-client-modal').style.display = 'flex'; }
    hideConfirmDeleteModal() { document.getElementById('confirm-delete-neo4j-client-modal').style.display = 'none'; }

    showLoading(show) { const el = document.getElementById('loading-indicator'); if (el) el.style.display = show ? 'flex' : 'none'; }
    showError(message) { const errorEl = document.getElementById('error-message'); const errorText = document.querySelector('#error-message .error-text'); if (errorEl && errorText) { errorText.textContent = message; errorEl.style.display = 'flex'; setTimeout(()=>this.hideError(),5000);} }
    hideError() { const errorEl = document.getElementById('error-message'); if (errorEl) errorEl.style.display='none'; }
    showSuccess(message) { const notification = document.createElement('div'); notification.className='success-notification'; notification.innerHTML = `<span class="success-icon">✅</span><span class="success-text">${this.escapeHtml(message)}</span>`; document.body.appendChild(notification); setTimeout(()=>{ if(notification.parentNode) notification.parentNode.removeChild(notification); },3000); }
    escapeHtml(text) { const div=document.createElement('div'); div.textContent=text; return div.innerHTML; }

    async refreshClients() { await this.loadClients(); }
}

// Global wrappers
function showAddNeo4jClientModal() { neo4jClientManager.showAddClientModal(); }
function hideAddNeo4jClientModal() { neo4jClientManager.hideAddClientModal(); }
function addNeo4jClient() { neo4jClientManager.addClient(); }
function refreshNeo4jClients() { neo4jClientManager.refreshClients(); }
function hideConfirmDeleteNeo4jClientModal() { neo4jClientManager.hideConfirmDeleteModal(); }
function confirmDeleteNeo4jClient() { neo4jClientManager.confirmDeleteClient(); }

// Initialize
let neo4jClientManager;
document.addEventListener('DOMContentLoaded', () => { neo4jClientManager = new Neo4jClientManager(); });

document.addEventListener('click', e => { if (e.target.classList.contains('modal')) { if (e.target.id === 'add-neo4j-client-modal') neo4jClientManager.hideAddClientModal(); else if (e.target.id === 'confirm-delete-neo4j-client-modal') neo4jClientManager.hideConfirmDeleteModal(); }});
