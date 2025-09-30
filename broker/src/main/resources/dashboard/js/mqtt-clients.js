// MQTT Client Management JavaScript

class MqttClientManager {
    constructor() {
        this.client = new GraphQLDashboardClient('/graphql');
        this.clients = [];
        this.clusterNodes = [];
        this.deleteClientName = null;
        this.init();
    }

    async init() {
        console.log('Initializing MQTT Client Manager...');
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
                            protocol
                            hostname
                            port
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
                            }
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
            this.showError('Failed to load MQTT clients: ' + error.message);
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
                    <td colspan="7" class="no-data">
                        No MQTT clients configured. Click "Add Client" to get started.
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
            const brokerUrl = `${client.config.protocol}://${client.config.hostname}:${client.config.port}`;

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
                <td>
                    <div class="action-buttons">
                        <button class="btn-action btn-view" onclick="mqttClientManager.viewClient('${client.name}')" title="Edit Client">
                            <svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor">
                                <path d="M3 17.25V21h3.75L17.81 9.94l-3.75-3.75L3 17.25zM20.71 7.04c.39-.39.39-1.02 0-1.41l-2.34-2.34c-.39-.39-1.02-.39-1.41 0l-1.83 1.83 3.75 3.75 1.83-1.83z"/>
                            </svg>
                        </button>
                        <button class="btn-action ${client.enabled ? 'btn-pause' : 'btn-play'}"
                                onclick="mqttClientManager.toggleClient('${client.name}', ${!client.enabled})"
                                title="${client.enabled ? 'Stop Client' : 'Start Client'}">
                            ${client.enabled ?
                                '<svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor"><path d="M6 19h4V5H6v14zm8-14v14h4V5h-4z"/></svg>' :
                                '<svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor"><path d="M8 5v14l11-7z"/></svg>'
                            }
                        </button>
                        <button class="btn-action btn-delete" onclick="mqttClientManager.deleteClient('${client.name}')" title="Delete Client">
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

    async addClient() {
        const form = document.getElementById('add-client-form');
        if (!form.checkValidity()) {
            form.reportValidity();
            return;
        }

        const clientData = {
            name: document.getElementById('client-name').value.trim(),
            namespace: document.getElementById('client-namespace').value.trim(),
            nodeId: document.getElementById('client-node').value,
            enabled: document.getElementById('client-enabled').checked,
            config: {
                protocol: document.getElementById('client-protocol').value,
                hostname: document.getElementById('client-hostname').value.trim(),
                port: parseInt(document.getElementById('client-port').value),
                username: document.getElementById('client-username').value.trim() || null,
                password: document.getElementById('client-password').value || null,
                clientId: document.getElementById('client-id').value.trim(),
                cleanSession: document.getElementById('client-clean-session').checked,
                keepAlive: parseInt(document.getElementById('client-keep-alive').value),
                reconnectDelay: parseInt(document.getElementById('client-reconnect-delay').value),
                connectionTimeout: parseInt(document.getElementById('client-connection-timeout').value)
            }
        };

        try {
            const mutation = `
                mutation CreateMqttClient($input: MqttClientInput!) {
                    createMqttClient(input: $input) {
                        success
                        errors
                        client {
                            name
                            enabled
                        }
                    }
                }
            `;

            const result = await this.client.query(mutation, { input: clientData });

            if (result.createMqttClient.success) {
                this.hideAddClientModal();
                await this.loadClients();
                this.showSuccess(`Client "${clientData.name}" added successfully`);
            } else {
                const errors = result.createMqttClient.errors || ['Unknown error'];
                this.showError('Failed to add client: ' + errors.join(', '));
            }

        } catch (error) {
            console.error('Error adding client:', error);
            this.showError('Failed to add client: ' + error.message);
        }
    }

    async toggleClient(clientName, enabled) {
        try {
            const mutation = `
                mutation ToggleMqttClient($name: String!, $enabled: Boolean!) {
                    toggleMqttClient(name: $name, enabled: $enabled) {
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

            if (result.toggleMqttClient.success) {
                await this.loadClients();
                this.showSuccess(`Client "${clientName}" ${enabled ? 'started' : 'stopped'} successfully`);
            } else {
                const errors = result.toggleMqttClient.errors || ['Unknown error'];
                this.showError('Failed to toggle client: ' + errors.join(', '));
            }

        } catch (error) {
            console.error('Error toggling client:', error);
            this.showError('Failed to toggle client: ' + error.message);
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
                mutation DeleteMqttClient($name: String!) {
                    deleteMqttClient(name: $name)
                }
            `;

            const result = await this.client.query(mutation, { name: this.deleteClientName });

            if (result.deleteMqttClient) {
                this.hideConfirmDeleteModal();
                await this.loadClients();
                this.showSuccess(`Client "${this.deleteClientName}" deleted successfully`);
            } else {
                this.showError('Failed to delete client');
            }

        } catch (error) {
            console.error('Error deleting client:', error);
            this.showError('Failed to delete client: ' + error.message);
        }

        this.deleteClientName = null;
    }

    viewClient(clientName) {
        window.location.href = `/pages/mqtt-client-detail.html?client=${encodeURIComponent(clientName)}`;
    }

    // UI Helper Methods
    showAddClientModal() {
        document.getElementById('add-client-modal').style.display = 'flex';
        document.getElementById('add-client-form').reset();
        document.getElementById('client-enabled').checked = true;
        document.getElementById('client-clean-session').checked = true;
    }

    hideAddClientModal() {
        document.getElementById('add-client-modal').style.display = 'none';
    }

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

    async refreshClients() {
        await this.loadClients();
    }
}

// Global functions for onclick handlers
function showAddClientModal() {
    mqttClientManager.showAddClientModal();
}

function hideAddClientModal() {
    mqttClientManager.hideAddClientModal();
}

function addClient() {
    mqttClientManager.addClient();
}

function hideConfirmDeleteModal() {
    mqttClientManager.hideConfirmDeleteModal();
}

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
        if (e.target.id === 'add-client-modal') {
            mqttClientManager.hideAddClientModal();
        } else if (e.target.id === 'confirm-delete-modal') {
            mqttClientManager.hideConfirmDeleteModal();
        }
    }
});