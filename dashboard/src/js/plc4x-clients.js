// PLC4X Clients Management JavaScript

class Plc4xClientManager {
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
            tbody.innerHTML = ui.emptyRow(9, 'No PLC4X clients configured',
                'Use “Add Client” to get started.');
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
                        <ix-icon-button icon="highlight" variant="subtle-tertiary" size="24" title="Edit Client" class="btn-edit"></ix-icon-button>
                        <ix-icon-button icon="${client.enabled ? 'pause' : 'play'}" variant="subtle-tertiary" size="24" title="${client.enabled ? 'Disable Client' : 'Enable Client'}" class="btn-toggle"></ix-icon-button>
                        <ix-icon-button icon="trashcan" variant="subtle-tertiary" size="24" class="btn-delete" title="Delete Client"></ix-icon-button>
                    </div>
                </td>
            `;

            const editBtn = row.querySelector('.btn-edit');
            if (editBtn) editBtn.addEventListener('click', (e) => { e.stopPropagation(); this.viewClient(client.name); });
            const toggleBtn = row.querySelector('.btn-toggle');
            if (toggleBtn) toggleBtn.addEventListener('click', (e) => { e.stopPropagation(); this.toggleClient(client.name, !client.enabled); });
            const deleteBtn = row.querySelector('.btn-delete');
            if (deleteBtn) deleteBtn.addEventListener('click', (e) => { e.stopPropagation(); this.deleteClient(client.name); });

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

    async deleteClient(clientName) {
        this.deleteClientName = clientName;
        if (await ui.confirmDelete(clientName, { title: 'Delete PLC4X client' })) {
            await this.confirmDeleteClient();
        } else {
            this.deleteClientName = null;
        }
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
        window.spaLocation.href = `/pages/plc4x-client-detail.html?client=${encodeURIComponent(clientName)}`;
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
var plc4xManager;

function confirmDeleteClient() {
    if (window.plc4xManager) window.plc4xManager.confirmDeleteClient();
}

function refreshClients() {
    if (window.plc4xManager) window.plc4xManager.refreshClients();
}

// Initialize when DOM is loaded
document.addEventListener('DOMContentLoaded', () => {
    plc4xManager = new Plc4xClientManager();
    window.plc4xManager = plc4xManager;
});
