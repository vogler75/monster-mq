// i3X Client Management JavaScript

class I3xClientManager {
    constructor() {
        this.client = new GraphQLDashboardClient();
        this.clients = [];
        this.clusterNodes = [];
        this.deleteClientName = null;
        this.init();
    }

    async init() {
        console.log('Initializing i3X Client Manager...');
        await this.loadClusterNodes();
        await this.loadClients();
        setInterval(() => this.loadClients(), 30000);
    }

    async loadClusterNodes() {
        try {
            const query = `
                query GetBrokers {
                    brokers {
                        nodeId
                        isCurrent
                    }
                }
            `;
            const result = await this.client.query(query);
            this.clusterNodes = result.brokers || [];
        } catch (error) {
            console.error('Error loading cluster nodes:', error);
        }
    }

    async loadClients() {
        this.showLoading(true);
        this.hideError();

        try {
            const query = `
                query GetI3xClients {
                    i3xClients {
                        name
                        namespace
                        nodeId
                        enabled
                        isOnCurrentNode
                        createdAt
                        updatedAt
                        metrics {
                            messagesIn
                            connected
                            timestamp
                        }
                        config {
                            url
                            authType
                            username
                            clientId
                            reconnectDelay
                            connectionTimeout
                            headers {
                                key
                                value
                            }
                            addresses {
                                elementId
                                topic
                                maxDepth
                                retained
                                qos
                                messageFormat
                                removePath
                                description
                            }
                        }
                    }
                }
            `;

            const result = await this.client.query(query);
            if (!result || !result.i3xClients) {
                throw new Error('Invalid response structure');
            }

            this.clients = result.i3xClients || [];
            this.updateMetrics();
            this.renderClientsTable();

        } catch (error) {
            console.error('Error loading i3X clients:', error);
            this.showError('Failed to load i3X clients: ' + error.message);
        } finally {
            this.showLoading(false);
        }
    }

    updateMetrics() {
        const totalClients = this.clients.length;
        const connectedClients = this.clients.filter(c => c.metrics && c.metrics[0] && c.metrics[0].connected).length;
        const currentNodeClients = this.clients.filter(c => c.isOnCurrentNode).length;
        const totalAddresses = this.clients.reduce((sum, c) => sum + (c.config?.addresses?.length || 0), 0);

        document.getElementById('total-clients').textContent = totalClients;
        document.getElementById('connected-clients').textContent = connectedClients;
        document.getElementById('current-node-clients').textContent = currentNodeClients;
        document.getElementById('total-addresses').textContent = totalAddresses;
    }

    renderClientsTable() {
        const tbody = document.getElementById('clients-table-body');
        if (!tbody) return;
        tbody.innerHTML = '';

        if (this.clients.length === 0) {
            tbody.innerHTML = ui.emptyRow(9, 'No i3X clients configured', 'Use "Add Client" to create one.');
            return;
        }

        this.clients.forEach(client => {
            const row = document.createElement('tr');
            const isConnected = client.metrics && client.metrics[0] && client.metrics[0].connected;
            const messagesIn = (client.metrics && client.metrics[0] && client.metrics[0].messagesIn !== undefined)
                ? client.metrics[0].messagesIn.toFixed(2)
                : '0.00';
            const addressCount = client.config?.addresses?.length || 0;
            const url = client.config?.url || '—';
            const authType = client.config?.authType || 'NONE';
            const nodeIndicator = client.isOnCurrentNode ? '📍 ' : '';

            row.innerHTML = `
                <td>
                    <div style="font-weight: 600;">
                        <a href="/pages/i3x-client-detail.html?client=${encodeURIComponent(client.name)}" class="device-link">
                            ${this.escapeHtml(client.name)}
                        </a>
                    </div>
                </td>
                <td>
                    <div title="${this.escapeHtml(url)}" style="max-width: 260px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;">
                        ${this.escapeHtml(url)}
                    </div>
                    <small style="color: var(--text-muted);">Client ID: ${this.escapeHtml(client.config?.clientId || '—')}</small>
                </td>
                <td>
                    <span class="status-badge status-info">${this.escapeHtml(authType)}</span>
                </td>
                <td><code>${this.escapeHtml(client.namespace || '—')}</code></td>
                <td>
                    <div class="node-assignment">
                        ${nodeIndicator}${client.nodeId === '*' ? 'Any Node (*)' : this.escapeHtml(client.nodeId)}
                    </div>
                </td>
                <td>
                    <span class="status-badge ${client.enabled ? 'status-enabled' : 'status-disabled'}">
                        ${client.enabled ? (isConnected ? 'Connected' : 'Enabled') : 'Disabled'}
                    </span>
                </td>
                <td>
                    <div class="address-count">
                        ${addressCount} mapping${addressCount === 1 ? '' : 's'}
                    </div>
                </td>
                <td>${messagesIn}</td>
                <td>
                    <div class="action-buttons">
                        <ix-icon-button icon="highlight" variant="subtle-tertiary" size="24" title="Edit Client" class="btn-edit"></ix-icon-button>
                        <ix-icon-button icon="${client.enabled ? 'pause' : 'play'}" variant="subtle-tertiary" size="24" title="${client.enabled ? 'Disable Client' : 'Enable Client'}" class="btn-toggle"></ix-icon-button>
                        <ix-icon-button icon="trashcan" variant="subtle-tertiary" size="24" class="btn-delete" title="Delete Client"></ix-icon-button>
                    </div>
                </td>
            `;

            const editBtn = row.querySelector('.btn-edit');
            if (editBtn) {
                editBtn.addEventListener('click', (e) => {
                    e.stopPropagation();
                    this.viewClient(client.name);
                });
            }

            const toggleBtn = row.querySelector('.btn-toggle');
            if (toggleBtn) {
                toggleBtn.addEventListener('click', (e) => {
                    e.stopPropagation();
                    this.toggleClient(client.name, !client.enabled);
                });
            }

            const deleteBtn = row.querySelector('.btn-delete');
            if (deleteBtn) {
                deleteBtn.addEventListener('click', (e) => {
                    e.stopPropagation();
                    this.deleteClient(client.name);
                });
            }

            tbody.appendChild(row);
        });
    }

    viewClient(clientName) {
        window.spaLocation.href = `/pages/i3x-client-detail.html?client=${encodeURIComponent(clientName)}`;
    }

    async toggleClient(name, enabled) {
        try {
            const mutation = `
                mutation ToggleI3xClient($name: String!, $enabled: Boolean!) {
                    i3xClient {
                        toggle(name: $name, enabled: $enabled) {
                            success
                            errors
                        }
                    }
                }
            `;

            const result = await this.client.query(mutation, { name, enabled });
            if (result.i3xClient.toggle.success) {
                await this.loadClients();
                ui.success(`Client "${name}" ${enabled ? 'enabled' : 'disabled'} successfully`);
            } else {
                const errors = result.i3xClient.toggle.errors || ['Unknown error'];
                this.showError(`Failed to toggle client: ${errors.join(', ')}`);
            }
        } catch (error) {
            console.error('Error toggling client:', error);
            this.showError('Failed to toggle client: ' + error.message);
        }
    }

    async deleteClient(name) {
        if (await ui.confirmDelete(name, { title: 'Delete i3X Client' })) {
            try {
                const mutation = `
                    mutation DeleteI3xClient($name: String!) {
                        i3xClient {
                            delete(name: $name)
                        }
                    }
                `;

                const result = await this.client.query(mutation, { name });
                if (result.i3xClient.delete) {
                    await this.loadClients();
                    ui.success(`Client "${name}" deleted successfully`);
                } else {
                    this.showError('Failed to delete client');
                }
            } catch (error) {
                console.error('Error deleting client:', error);
                this.showError('Failed to delete client: ' + error.message);
            }
        }
    }

    showLoading(show) {
        const indicator = document.getElementById('loading-indicator');
        if (indicator) indicator.style.display = show ? 'flex' : 'none';
    }

    showError(message) { ui.showError(message); }
    hideError() { ui.clearError(); }

    escapeHtml(text) {
        const div = document.createElement('div');
        div.textContent = text || '';
        return div.innerHTML;
    }

    async refreshClients() {
        await this.loadClients();
    }
}

var i3xClientManager;

function refreshClients() {
    if (window.i3xClientManager) window.i3xClientManager.refreshClients();
}

document.addEventListener('DOMContentLoaded', () => {
    i3xClientManager = new I3xClientManager();
    window.i3xClientManager = i3xClientManager;
});
