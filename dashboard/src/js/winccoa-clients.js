// WinCC OA Client Management JavaScript

class WinCCOaClientManager {
    constructor() {
        this.client = new GraphQLDashboardClient();
        this.clients = [];
        this.clusterNodes = [];
        this.deleteClientName = null;
        this.addressCounter = 0;
        this.init();
    }

    async init() {
        console.log('Initializing WinCC OA Client Manager...');
        // Load initial data
        await this.loadClusterNodes();
        await this.loadClients();
        // Set up periodic refresh
        setInterval(() => this.loadClients(), 30000); // Refresh every 30 seconds
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

            // Populate node selector in the add client form
            const nodeSelect = document.getElementById('client-node');
            if (nodeSelect) {
                nodeSelect.innerHTML = '<option value="">Select Node...</option>';
                this.clusterNodes.forEach(node => {
                    const option = document.createElement('option');
                    option.value = node.nodeId;
                    option.textContent = node.nodeId + (node.isCurrent ? ' (Current)' : '');
                    nodeSelect.appendChild(option);
                });
            }

        } catch (error) {
            console.error('Error loading cluster nodes:', error);
        }
    }

    async loadClients() {
        this.showLoading(true);
        this.hideError();

        try {
            const query = `
                query GetWinCCOaClients {
                    winCCOaClients {
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
                            graphqlEndpoint
                            websocketEndpoint
                            username
                            reconnectDelay
                            connectionTimeout
                            messageFormat
                            transformConfig {
                                removeSystemName
                                convertDotToSlash
                                convertUnderscoreToSlash
                                regexPattern
                                regexReplacement
                            }
                            addresses {
                                query
                                topic
                                description
                                answer
                                retained
                            }
                        }
                    }
                }
            `;

            const result = await this.client.query(query);
            console.log('Load clients result:', result);

            if (!result || !result.winCCOaClients) {
                throw new Error('Invalid response structure');
            }

            this.clients = result.winCCOaClients || [];
            this.updateMetrics();
            this.renderClientsTable();

        } catch (error) {
            console.error('Error loading clients:', error);
            this.showError('Failed to load WinCC OA Clients: ' + error.message);
        } finally {
            this.showLoading(false);
        }
    }

    updateMetrics() {
        const totalClients = this.clients.length;
        const connectedClients = this.clients.filter(c => {
            const metrics = c.metrics && c.metrics.length > 0 ? c.metrics[0] : null;
            return metrics && metrics.connected;
        }).length;
        const currentNodeClients = this.clients.filter(c => c.isOnCurrentNode).length;
        const totalAddresses = this.clients.reduce((sum, c) => sum + c.config.addresses.length, 0);

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
            tbody.innerHTML = ui.emptyRow(9, 'No WinCC OA clients configured',
                'Use “Add Bridge” to get started.');
            return;
        }

        this.clients.forEach(client => {
            const row = document.createElement('tr');

            const statusClass = client.enabled ? 'status-enabled' : 'status-disabled';
            const statusText = client.enabled ? 'Enabled' : 'Disabled';
            const nodeIndicator = client.isOnCurrentNode ? '📍 ' : '';

            // Format metrics
            const metrics = client.metrics && client.metrics.length > 0 ? client.metrics[0] : null;
            const messagesIn = metrics ? Math.round(metrics.messagesIn) : '0';
            const connected = metrics ? metrics.connected : false;
            const connectionClass = connected ? 'status-connected' : 'status-disconnected';
            const connectionText = connected ? 'Connected' : 'Disconnected';

            row.innerHTML = `
                <td>
                    <div class="client-name">${this.escapeHtml(client.name)}</div>
                    <small class="client-namespace">${this.escapeHtml(client.namespace)}</small>
                </td>
                <td>
                    <div class="endpoint-url" title="${this.escapeHtml(client.config.graphqlEndpoint)}">
                        ${this.escapeHtml(client.config.graphqlEndpoint)}
                    </div>
                    <small class="message-format">Format: ${this.escapeHtml(client.config.messageFormat)}</small>
                </td>
                <td>${this.escapeHtml(client.namespace)}</td>
                <td>
                    <div class="node-assignment">
                        ${nodeIndicator}${this.escapeHtml(client.nodeId)}
                    </div>
                </td>
                <td>
                    <span class="status-badge ${statusClass}">${statusText}</span>
                </td>
                <td>
                    <span class="${connectionClass}">${connectionText}</span>
                </td>
                <td>
                    <div class="address-count">
                        ${client.config.addresses.length}
                    </div>
                </td>
                <td>${messagesIn}</td>
                <td>
                    <div class="action-buttons">
                        <ix-icon-button icon="highlight" variant="subtle-tertiary" size="24" title="Edit Bridge" onclick="winCCOaClientManager.editClient('${client.name}')"></ix-icon-button>
                        <ix-icon-button icon="${client.enabled ? 'pause' : 'play'}" variant="subtle-tertiary" size="24" title="${client.enabled ? 'Stop Bridge' : 'Start Bridge'}" onclick="winCCOaClientManager.toggleClient('${client.name}', ${!client.enabled})"></ix-icon-button>
                        <ix-icon-button icon="trashcan" variant="subtle-tertiary" size="24" class="btn-delete" title="Delete Bridge" onclick="winCCOaClientManager.deleteClient('${client.name}')"></ix-icon-button>
                    </div>
                </td>
            `;

            tbody.appendChild(row);
        });
    }

    editClient(clientName) {
        window.spaLocation.href = '/pages/winccoa-client-detail.html?client=' + encodeURIComponent(clientName);
    }

    async toggleClient(clientName, enabled) {
        try {
            const mutation = `
                mutation ToggleWinCCOaClient($name: String!, $enabled: Boolean!) {
                    winCCOaDevice {
                        toggle(name: $name, enabled: $enabled) {
                            success
                            errors
                            client {
                                name
                                enabled
                            }
                        }
                    }
                }
            `;

            const result = await this.client.query(mutation, { name: clientName, enabled });

            if (result.winCCOaDevice.toggle.success) {
                await this.loadClients();
                this.showSuccess(`Bridge "${clientName}" ${enabled ? 'started' : 'stopped'} successfully`);
            } else {
                const errors = result.winCCOaDevice.toggle.errors || ['Unknown error'];
                this.showError('Failed to toggle bridge: ' + errors.join(', '));
            }

        } catch (error) {
            console.error('Error toggling client:', error);
            this.showError('Failed to toggle bridge: ' + error.message);
        }
    }

    async deleteClient(clientName) {
        this.deleteClientName = clientName;
        if (await ui.confirmDelete(clientName, { title: 'Delete WinCC OA client' })) {
            await this.confirmDeleteClient();
        } else {
            this.deleteClientName = null;
        }
    }

    async confirmDeleteClient() {
        if (!this.deleteClientName) return;

        try {
            const mutation = `
                mutation DeleteWinCCOaClient($name: String!) {
                    winCCOaDevice {
                        delete(name: $name)
                    }
                }
            `;

            const result = await this.client.query(mutation, { name: this.deleteClientName });

            if (result.winCCOaDevice.delete) {
                await this.loadClients();
                this.showSuccess(`Bridge "${this.deleteClientName}" deleted successfully`);
            } else {
                this.showError('Failed to delete bridge');
            }

        } catch (error) {
            console.error('Error deleting client:', error);
            this.showError('Failed to delete bridge: ' + error.message);
        }

        this.deleteClientName = null;
    }

    // UI Helper Methods

    showLoading(show) {
        const indicator = document.getElementById('loading-indicator');
        if (indicator) {
            indicator.style.display = show ? 'flex' : 'none';
        }
    }

    showError(message) { ui.showError(message); }

    hideError() {
        const errorEl = document.getElementById('error-message');
        if (errorEl) {
            errorEl.style.display = 'none';
        }
    }

    showSuccess(message) { ui.success(message); }

    escapeHtml(text) {
        const div = document.createElement('div');
        div.textContent = text;
        return div.innerHTML;
    }

    escapeAttr(text) {
        if (!text) return '';
        return text
            .replace(/&/g, '&amp;')
            .replace(/"/g, '&quot;')
            .replace(/'/g, '&#39;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;');
    }

    async refreshClients() {
        await this.loadClients();
    }
}

// Global functions for onclick handlers
function confirmDeleteClient() {
    winCCOaClientManager.confirmDeleteClient();
}

function refreshClients() {
    winCCOaClientManager.refreshClients();
}

// Initialize when DOM is loaded
let winCCOaClientManager;
document.addEventListener('DOMContentLoaded', () => {
    winCCOaClientManager = new WinCCOaClientManager();
});
