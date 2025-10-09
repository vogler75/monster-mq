// PLC4X Clients Management JavaScript

class Plc4xClientManager {
    constructor() {
        this.client = new GraphQLDashboardClient('/graphql');
        this.clients = [];
        this.clusterNodes = [];
        this.deleteClientName = null;
        this.init();
    }

    async init() {
        console.log('Initializing PLC4X Client Manager...');

        // Since user management is disabled, skip authentication check
        console.log('Initializing without authentication check (user management disabled)');

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
                query GetPlc4xClients {
                    plc4xClients {
                        name
                        namespace
                        nodeId
                        enabled
                        isOnCurrentNode
                        createdAt
                        updatedAt
                        config {
                            protocol
                            connectionString
                            pollingInterval
                            reconnectDelay
                            enabled
                            addresses {
                                name
                                address
                                topic
                                qos
                                retained
                                scalingFactor
                                offset
                                deadband
                                enabled
                            }
                        }
                        metrics {
                            messagesInRate
                            connected
                        }
                    }
                }
            `;

            const result = await this.client.query(query);
            console.log('Load clients result:', result);

            if (!result) {
                throw new Error('Invalid response structure: missing result');
            }

            if (!result.plc4xClients) {
                throw new Error('Invalid response structure: missing plc4xClients property');
            }

            this.clients = result.plc4xClients || [];

            this.updateMetrics();
            this.renderClientsTable();

        } catch (error) {
            console.error('Error loading clients:', error);
            console.error('Error details:', error.stack);
            this.showError('Failed to load PLC4X clients: ' + error.message);
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
                        No PLC4X clients configured. Click "Add Client" to get started.
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

            // Format protocol for display
            const protocolDisplay = this.formatProtocol(client.config.protocol);

            // Extract host from connection string
            const connectionDisplay = this.formatConnectionString(client.config.connectionString);

            // Get metrics
            const messagesInRate = (client.metrics && client.metrics.length > 0)
                ? Math.round(client.metrics[0].messagesInRate)
                : 0;
            const connected = (client.metrics && client.metrics.length > 0)
                ? client.metrics[0].connected
                : false;
            const connectionIndicator = connected ? '🟢' : '🔴';

            row.innerHTML = `
                <td>
                    <div class="device-name">${this.escapeHtml(client.name)}</div>
                    <small class="device-namespace">${this.escapeHtml(client.namespace)}</small>
                </td>
                <td>
                    <span class="protocol-badge">${protocolDisplay}</span>
                </td>
                <td>
                    <div class="endpoint-url" title="${this.escapeHtml(client.config.connectionString)}">
                        ${connectionIndicator} ${this.escapeHtml(connectionDisplay)}
                    </div>
                    <small class="security-policy">Poll: ${client.config.pollingInterval}ms</small>
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
                        ${client.config.addresses.length} addresses
                    </div>
                </td>
                <td>
                    <span style="color: #06B6D4;">${messagesInRate}</span>
                </td>
                <td>
                    <div class="action-buttons">
                        <button class="btn-action btn-view" onclick="plc4xManager.viewClient('${client.name}')" title="Edit Client">
                            <svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor">
                                <path d="M3 17.25V21h3.75L17.81 9.94l-3.75-3.75L3 17.25zM20.71 7.04c.39-.39.39-1.02 0-1.41l-2.34-2.34c-.39-.39-1.02-.39-1.41 0l-1.83 1.83 3.75 3.75 1.83-1.83z"/>
                            </svg>
                        </button>
                        <button class="btn-action ${client.enabled ? 'btn-pause' : 'btn-play'}"
                                onclick="plc4xManager.toggleClient('${client.name}', ${!client.enabled})"
                                title="${client.enabled ? 'Disable Client' : 'Enable Client'}">
                            ${client.enabled ?
                                '<svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor"><path d="M6 19h4V5H6v14zm8-14v14h4V5h-4z"/></svg>' :
                                '<svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor"><path d="M8 5v14l11-7z"/></svg>'
                            }
                        </button>
                        <button class="btn-action btn-delete" onclick="plc4xManager.deleteClient('${client.name}')" title="Delete Client">
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

    formatProtocol(protocol) {
        // Convert enum values to readable names
        const protocolNames = {
            'AB_ETHERNET': 'AB Ethernet',
            'ADS': 'ADS',
            'BACNET_IP': 'BACnet/IP',
            'CANOPEN': 'CANopen',
            'EIP': 'EtherNet/IP',
            'FIRMATA': 'Firmata',
            'KNXNET_IP': 'KNXnet/IP',
            'MODBUS_ASCII': 'Modbus ASCII',
            'MODBUS_RTU': 'Modbus RTU',
            'MODBUS_TCP': 'Modbus TCP',
            'PROFINET': 'PROFINET',
            'S7': 'S7',
            'SIMULATED': 'Simulated'
        };
        return protocolNames[protocol] || protocol;
    }

    formatConnectionString(connectionString) {
        // Extract the meaningful part from connection string
        try {
            const url = new URL(connectionString);
            return url.host || connectionString;
        } catch {
            // If parsing fails, just return a shortened version
            return connectionString.length > 30
                ? connectionString.substring(0, 30) + '...'
                : connectionString;
        }
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
                connectionString: document.getElementById('client-connection-string').value.trim(),
                pollingInterval: parseInt(document.getElementById('polling-interval').value) || 1000,
                reconnectDelay: parseInt(document.getElementById('reconnect-delay').value) || 5000,
                enabled: document.getElementById('client-enabled').checked,
                addresses: []
            }
        };

        try {
            const mutation = `
                mutation AddPlc4xClient($input: Plc4xClientInput!) {
                    plc4xDevice {
                        create(input: $input) {
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

            const result = await this.client.query(mutation, { input: clientData });

            if (result.plc4xDevice.create.success) {
                this.hideAddClientModal();
                await this.loadClients();
                this.showSuccess(`Client "${clientData.name}" added successfully`);
            } else {
                const errors = result.plc4xDevice.create.errors || ['Unknown error'];
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
                mutation TogglePlc4xClient($name: String!, $enabled: Boolean!) {
                    plc4xDevice {
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

            if (result.plc4xDevice.toggle.success) {
                await this.loadClients();
                this.showSuccess(`Client "${clientName}" ${enabled ? 'enabled' : 'disabled'} successfully`);
            } else {
                const errors = result.plc4xDevice.toggle.errors || ['Unknown error'];
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
                mutation DeletePlc4xClient($name: String!) {
                    plc4xDevice {
                        delete(name: $name)
                    }
                }
            `;

            const result = await this.client.query(mutation, { name: this.deleteClientName });

            if (result.plc4xDevice.delete) {
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
        // Navigate to client detail page
        window.location.href = `/pages/plc4x-client-detail.html?client=${encodeURIComponent(clientName)}`;
    }

    // UI Helper Methods
    showAddClientModal() {
        document.getElementById('add-client-modal').style.display = 'flex';
        // Reset form
        document.getElementById('add-client-form').reset();
        document.getElementById('client-enabled').checked = true;
        document.getElementById('polling-interval').value = '1000';
        document.getElementById('reconnect-delay').value = '5000';
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
        // Create temporary success notification
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
    plc4xManager.showAddClientModal();
}

function hideAddClientModal() {
    plc4xManager.hideAddClientModal();
}

function addClient() {
    plc4xManager.addClient();
}

function hideConfirmDeleteModal() {
    plc4xManager.hideConfirmDeleteModal();
}

function confirmDeleteClient() {
    plc4xManager.confirmDeleteClient();
}

function refreshClients() {
    plc4xManager.refreshClients();
}

// Initialize when DOM is loaded
let plc4xManager;
document.addEventListener('DOMContentLoaded', () => {
    plc4xManager = new Plc4xClientManager();
});

// Handle modal clicks (close when clicking outside)
document.addEventListener('click', (e) => {
    if (e.target.classList.contains('modal')) {
        if (e.target.id === 'add-client-modal') {
            plc4xManager.hideAddClientModal();
        } else if (e.target.id === 'confirm-delete-modal') {
            plc4xManager.hideConfirmDeleteModal();
        }
    }
});
