// MQTT Bridge Management JavaScript

class MqttClientManager {
    constructor() {
        this.client = new GraphQLDashboardClient();
        this.clients = [];
        this.clusterNodes = [];
        this.deleteClientName = null;
        this.init();
    }

    async init() {
        console.log('Initializing MQTT Bridge Manager...');
        // Load initial data
        await this.loadClients();
        // Set up periodic refresh
        setInterval(() => this.loadClients(), 30000); // Refresh every 30 seconds
    }

    async loadClients() {
        this.showLoading(true);
        this.hideError();

        try {
            const query = `
                query GetMqttClients {
                    mqttClients {
                        name
                        namespace
                        nodeId
                        enabled
                        isOnCurrentNode
                        createdAt
                        updatedAt
                        config {
                            brokerUrl
                            username
                            clientId
                            cleanSession
                            keepAlive
                            reconnectDelay
                            connectionTimeout
                            addresses {
                                mode
                                remoteTopic
                                localTopic
                                removePath
                                qos
                            }
                        }
                        metrics {
                            messagesIn
                            messagesOut
                        }
                    }
                }
            `;

            const result = await this.client.query(query);
            console.log('Load clients result:', result);

            if (!result || !result.mqttClients) {
                throw new Error('Invalid response structure');
            }

            this.clients = result.mqttClients || [];
            this.updateMetrics();
            this.renderClientsTable();

        } catch (error) {
            console.error('Error loading clients:', error);
            this.showError('Failed to load MQTT Clients: ' + error.message);
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
                    <td colspan="9" class="no-data">
                        No MQTT Clients configured. Click \"Add Bridge\" to get started.
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
            const brokerUrl = client.config.brokerUrl;

            row.innerHTML = `
                <td>
                    <div class="client-name">${this.escapeHtml(client.name)}</div>
                    <small class="client-namespace">${this.escapeHtml(client.namespace)}</small>
                </td>
                <td>
                    <div class="broker-url" title="${this.escapeHtml(brokerUrl)}">
                        ${this.escapeHtml(brokerUrl)}
                    </div>
                    <small class="client-id">Client ID: ${this.escapeHtml(client.config.clientId)}</small>
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
                        ${client.config.addresses.length} mappings
                    </div>
                </td>
                <td>${(client.metrics && client.metrics.length>0 ? Math.round(client.metrics[0].messagesIn) : 0)}</td>
                <td>${(client.metrics && client.metrics.length>0 ? Math.round(client.metrics[0].messagesOut) : 0)}</td>
                <td>
                    <div class="action-buttons">
                        <ix-icon-button icon="highlight" variant="subtle-tertiary" size="24" title="Edit Bridge" onclick="mqttClientManager.viewClient('${client.name}')"></ix-icon-button>
                        <ix-icon-button icon="${client.enabled ? 'pause' : 'play'}" variant="subtle-tertiary" size="24" title="${client.enabled ? 'Stop Bridge' : 'Start Bridge'}" onclick="mqttClientManager.toggleClient('${client.name}', ${!client.enabled})"></ix-icon-button>
                        <ix-icon-button icon="trashcan" variant="subtle-tertiary" size="24" class="btn-delete" title="Delete Bridge" onclick="mqttClientManager.deleteClient('${client.name}')"></ix-icon-button>
                    </div>
                </td>
            `;

            tbody.appendChild(row);
        });
    }

    async toggleClient(clientName, enabled) {
        try {
            const mutation = `
                mutation ToggleMqttClient($name: String!, $enabled: Boolean!) {
                    mqttClient {
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

            if (result.mqttClient.toggle.success) {
                await this.loadClients();
                this.showSuccess(`Bridge \"${clientName}\" ${enabled ? 'started' : 'stopped'} successfully`);
            } else {
                const errors = result.mqttClient.toggle.errors || ['Unknown error'];
                this.showError('Failed to toggle bridge: ' + errors.join(', '));
            }

        } catch (error) {
            console.error('Error toggling client:', error);
            this.showError('Failed to toggle bridge: ' + error.message);
        }
    }

    async deleteClient(clientName) {
        this.deleteClientName = clientName;
        if (await ui.confirmDelete(clientName, { title: 'Delete MQTT client' })) {
            await this.confirmDeleteClient();
        } else {
            this.deleteClientName = null;
        }
    }

    async confirmDeleteClient() {
        if (!this.deleteClientName) return;

        try {
            const mutation = `
                mutation DeleteMqttClient($name: String!) {
                    mqttClient {
                        delete(name: $name)
                    }
                }
            `;

            const result = await this.client.query(mutation, { name: this.deleteClientName });

            if (result.mqttClient.delete) {
                await this.loadClients();
                this.showSuccess(`Bridge \"${this.deleteClientName}\" deleted successfully`);
            } else {
                this.showError('Failed to delete bridge');
            }

        } catch (error) {
            console.error('Error deleting client:', error);
            this.showError('Failed to delete bridge: ' + error.message);
        }

        this.deleteClientName = null;
    }

    viewClient(clientName) {
        window.spaLocation.href = `/pages/mqtt-client-detail.html?client=${encodeURIComponent(clientName)}`;
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

    async refreshClients() {
        await this.loadClients();
    }
}

// Global functions for onclick handlers
function confirmDeleteClient() {
    mqttClientManager.confirmDeleteClient();
}

function refreshClients() {
    mqttClientManager.refreshClients();
}

// Initialize when DOM is loaded
let mqttClientManager;
document.addEventListener('DOMContentLoaded', () => {
    mqttClientManager = new MqttClientManager();
});

// Handle modal clicks
document.addEventListener('click', (e) => {
    if (e.target.classList.contains('modal')) {
    }
});