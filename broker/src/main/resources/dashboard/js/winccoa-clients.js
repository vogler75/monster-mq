// WinCC OA Client Management JavaScript

class WinCCOaClientManager {
    constructor() {
        this.client = new GraphQLDashboardClient('/graphql');
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
                query GetClusterNodes {
                    clusterNodes {
                        nodeId
                        isCurrent
                    }
                }
            `;

            const result = await this.client.query(query);
            this.clusterNodes = result.clusterNodes || [];

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
        const enabledClients = this.clients.filter(c => c.enabled).length;
        const currentNodeClients = this.clients.filter(c => c.isOnCurrentNode).length;
        const totalAddresses = this.clients.reduce((sum, c) => sum + c.config.addresses.length, 0);

        document.getElementById('total-clients').textContent = totalClients;
        document.getElementById('enabled-clients').textContent = enabledClients;
        document.getElementById('current-node-clients').textContent = currentNodeClients;
        document.getElementById('total-addresses').textContent = totalAddresses;
    }

    renderClientsTable() {
        const tbody = document.getElementById('clients-table-body');
        if (!tbody) return;

        tbody.innerHTML = '';

        if (this.clients.length === 0) {
            tbody.innerHTML = `
                <tr>
                    <td colspan="8" class="no-data">
                        No WinCC OA Clients configured. Click "Add Bridge" to get started.
                    </td>
                </tr>
            `;
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
                    <div class="address-count">
                        ${client.config.addresses.length} queries
                    </div>
                </td>
                <td>
                    <div class="metric-value" style="color: var(--monster-green); font-weight: 500;">
                        ${messagesIn}
                    </div>
                </td>
                <td>
                    <div class="action-buttons">
                        <button class="btn-action btn-view" onclick="winCCOaClientManager.editClient('${client.name}')" title="Edit Bridge">
                            <svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor">
                                <path d="M3 17.25V21h3.75L17.81 9.94l-3.75-3.75L3 17.25zM20.71 7.04c.39-.39.39-1.02 0-1.41l-2.34-2.34c-.39-.39-1.02-.39-1.41 0l-1.83 1.83 3.75 3.75 1.83-1.83z"/>
                            </svg>
                        </button>
                        <button class="btn-action ${client.enabled ? 'btn-pause' : 'btn-play'}"
                                onclick="winCCOaClientManager.toggleClient('${client.name}', ${!client.enabled})"
                                title="${client.enabled ? 'Stop Bridge' : 'Start Bridge'}">
                            ${client.enabled ?
                                '<svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor"><path d="M6 19h4V5H6v14zm8-14v14h4V5h-4z"/></svg>' :
                                '<svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor"><path d="M8 5v14l11-7z"/></svg>'
                            }
                        </button>
                        <button class="btn-action btn-delete" onclick="winCCOaClientManager.deleteClient('${client.name}')" title="Delete Bridge">
                            <svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor">
                                <path d="M6 19c0 1.1.9 2 2 2h8c1.1 0 2-.9 2-2V7H6v12zM19 4h-3.5l-1-1h-5l-1 1H5v2h14V4z"/>
                            </svg>
                        </button>
                    </div>
                </td>
            `;

            tbody.appendChild(row);
        });
    }

    editClient(clientName) {
        window.location.href = '/pages/winccoa-client-detail.html?client=' + encodeURIComponent(clientName);
    }

    async toggleClient(clientName, enabled) {
        try {
            const mutation = `
                mutation ToggleWinCCOaClient($name: String!, $enabled: Boolean!) {
                    toggleWinCCOaClient(name: $name, enabled: $enabled) {
                        success
                        errors
                        client {
                            name
                            enabled
                        }
                    }
                }
            `;

            const result = await this.client.query(mutation, { name: clientName, enabled });

            if (result.toggleWinCCOaClient.success) {
                await this.loadClients();
                this.showSuccess(`Bridge "${clientName}" ${enabled ? 'started' : 'stopped'} successfully`);
            } else {
                const errors = result.toggleWinCCOaClient.errors || ['Unknown error'];
                this.showError('Failed to toggle bridge: ' + errors.join(', '));
            }

        } catch (error) {
            console.error('Error toggling client:', error);
            this.showError('Failed to toggle bridge: ' + error.message);
        }
    }

    deleteClient(clientName) {
        this.deleteClientName = clientName;
        document.getElementById('delete-client-name').textContent = clientName;
        this.showConfirmDeleteModal();
    }

    async confirmDeleteClient() {
        if (!this.deleteClientName) return;

        try {
            const mutation = `
                mutation DeleteWinCCOaClient($name: String!) {
                    deleteWinCCOaClient(name: $name)
                }
            `;

            const result = await this.client.query(mutation, { name: this.deleteClientName });

            if (result.deleteWinCCOaClient) {
                this.hideConfirmDeleteModal();
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
    showConfirmDeleteModal() {
        document.getElementById('confirm-delete-modal').style.display = 'flex';
    }

    hideConfirmDeleteModal() {
        document.getElementById('confirm-delete-modal').style.display = 'none';
    }

    showLoading(show) {
        const indicator = document.getElementById('loading-indicator');
        if (indicator) {
            indicator.style.display = show ? 'flex' : 'none';
        }
    }

    showError(message) {
        const errorEl = document.getElementById('error-message');
        const errorText = document.querySelector('#error-message .error-text');
        if (errorEl && errorText) {
            errorText.textContent = message;
            errorEl.style.display = 'flex';
            setTimeout(() => this.hideError(), 5000);
        }
    }

    hideError() {
        const errorEl = document.getElementById('error-message');
        if (errorEl) {
            errorEl.style.display = 'none';
        }
    }

    showSuccess(message) {
        const notification = document.createElement('div');
        notification.className = 'success-notification';
        notification.innerHTML = `
            <span class="success-icon">✅</span>
            <span class="success-text">${this.escapeHtml(message)}</span>
        `;
        document.body.appendChild(notification);

        setTimeout(() => {
            if (notification.parentNode) {
                notification.parentNode.removeChild(notification);
            }
        }, 3000);
    }

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
function hideConfirmDeleteModal() {
    winCCOaClientManager.hideConfirmDeleteModal();
}

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
