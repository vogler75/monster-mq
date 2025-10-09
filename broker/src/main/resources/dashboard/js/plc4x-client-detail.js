// PLC4X Client Detail Management JavaScript

class Plc4xClientDetailManager {
    constructor() {
        this.client = new GraphQLDashboardClient('/graphql');
        this.clientData = null;
        this.clusterNodes = [];
        this.clientName = null;
        this.deleteAddressName = null;
        this.init();
    }

    async init() {
        console.log('Initializing PLC4X Client Detail Manager...');

        // Since user management is disabled, skip authentication check
        console.log('Initializing without authentication check (user management disabled)');

        // Get client name from URL parameters
        const urlParams = new URLSearchParams(window.location.search);
        this.clientName = urlParams.get('client');

        if (!this.clientName) {
            this.showError('No client specified');
            return;
        }

        // Load data
        await this.loadClusterNodes();
        await this.loadClient();
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

            // Populate node selector
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

    async loadClient() {
        this.showLoading(true);
        this.hideError();

        try {
            const query = `
                query GetPlc4xClient($name: String!) {
                    plc4xClient(name: $name) {
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
                                publishOnChange
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

            const result = await this.client.query(query, { name: this.clientName });
            this.clientData = result.plc4xClient;

            if (!this.clientData) {
                this.showError(`Client "${this.clientName}" not found`);
                return;
            }

            this.renderClient();

        } catch (error) {
            console.error('Error loading client:', error);
            this.showError('Failed to load client: ' + error.message);
        } finally {
            this.showLoading(false);
        }
    }

    renderClient() {
        if (!this.clientData) return;

        // Update page title
        document.getElementById('client-title').textContent = `PLC4X Client: ${this.clientData.name}`;
        const protocolDisplay = this.formatProtocol(this.clientData.config.protocol);
        const connected = this.clientData.metrics && this.clientData.metrics.length > 0
            ? this.clientData.metrics[0].connected
            : false;
        const connStatus = connected ? 'Connected' : 'Disconnected';
        document.getElementById('client-subtitle').textContent =
            `${this.clientData.namespace} - ${protocolDisplay} - ${connStatus}`;

        // Populate form fields
        document.getElementById('client-name').value = this.clientData.name;
        document.getElementById('client-namespace').value = this.clientData.namespace;
        document.getElementById('client-protocol').value = this.clientData.config.protocol;
        document.getElementById('client-connection-string').value = this.clientData.config.connectionString;
        document.getElementById('client-node').value = this.clientData.nodeId;
        document.getElementById('client-enabled').checked = this.clientData.enabled;

        // Connection configuration
        document.getElementById('client-polling-interval').value = this.clientData.config.pollingInterval;
        document.getElementById('client-reconnect-delay').value = this.clientData.config.reconnectDelay;
        document.getElementById('client-config-enabled').checked = this.clientData.config.enabled;

        // Render addresses
        this.renderAddresses();

        // Show content
        document.getElementById('client-content').style.display = 'block';
    }

    renderAddresses() {
        const tbody = document.getElementById('addresses-table-body');
        const noAddresses = document.getElementById('no-addresses');
        const addressesTable = document.getElementById('addresses-table');

        if (!tbody) return;

        tbody.innerHTML = '';

        if (!this.clientData.config.addresses || this.clientData.config.addresses.length === 0) {
            addressesTable.style.display = 'none';
            noAddresses.style.display = 'block';
            return;
        }

        addressesTable.style.display = 'table';
        noAddresses.style.display = 'none';

        this.clientData.config.addresses.forEach(address => {
            const row = document.createElement('tr');

            const enabledIcon = address.enabled ? '✅' : '⏸️';
            const retainedIcon = address.retained ? '📌' : '📄';
            const transformIcon = (address.scalingFactor || address.offset) ? '📊' : '-';
            const deadbandIcon = address.deadband ? '🎯' : '-';
            const publishOnChangeIcon = address.publishOnChange ? '🔄' : '📋';
            const publishOnChangeTitle = address.publishOnChange ? 'Publish on change' : 'Publish always';

            row.innerHTML = `
                <td>
                    <div class="address-name" title="${this.escapeHtml(address.address)}">
                        ${enabledIcon} ${this.escapeHtml(address.address)}
                    </div>
                </td>
                <td>
                    <div class="topic-value">${this.escapeHtml(address.topic || 'Auto')}</div>
                    <small class="qos-value">QoS ${address.qos} ${retainedIcon}</small>
                </td>
                <td>
                    <div class="transform-value" title="Scaling: ${address.scalingFactor || 'none'}, Offset: ${address.offset || 'none'}">
                        ${transformIcon} ${address.scalingFactor ? 'x' + address.scalingFactor : ''} ${address.offset ? '+' + address.offset : ''}
                    </div>
                </td>
                <td>
                    <div class="deadband-value">
                        ${deadbandIcon} ${address.deadband || '-'}
                    </div>
                </td>
                <td>
                    <div title="${publishOnChangeTitle}">${publishOnChangeIcon}</div>
                </td>
                <td>
                    <span class="status-badge ${address.enabled ? 'status-enabled' : 'status-disabled'}">
                        ${address.enabled ? 'Enabled' : 'Disabled'}
                    </span>
                </td>
                <td>
                    <div class="action-buttons">
                        <button class="btn btn-sm btn-danger"
                                onclick="clientDetailManager.deleteAddress('${this.escapeHtml(address.address)}')"
                                title="Delete Address">
                            🗑️
                        </button>
                    </div>
                </td>
            `;

            tbody.appendChild(row);
        });
    }

    async addAddress() {
        const form = document.getElementById('add-address-form');
        if (!form.checkValidity()) {
            form.reportValidity();
            return;
        }

        const plcAddress = document.getElementById('address-address').value.trim();

        const addressData = {
            name: plcAddress,  // Use PLC address as the name
            address: plcAddress,
            topic: document.getElementById('address-topic').value.trim(),
            qos: parseInt(document.getElementById('address-qos').value),
            retained: document.getElementById('address-retained').checked,
            scalingFactor: parseFloat(document.getElementById('address-scaling').value) || null,
            offset: parseFloat(document.getElementById('address-offset').value) || null,
            deadband: parseFloat(document.getElementById('address-deadband').value) || null,
            publishOnChange: document.getElementById('address-publish-on-change').checked,
            enabled: document.getElementById('address-enabled').checked
        };

        try {
            const mutation = `
                mutation AddPlc4xAddress($deviceName: String!, $input: Plc4xAddressInput!) {
                    plc4xDevice {
                        addAddress(deviceName: $deviceName, input: $input) {
                            success
                            errors
                            client {
                                name
                            }
                        }
                    }
                }
            `;

            const result = await this.client.query(mutation, {
                deviceName: this.clientName,
                input: addressData
            });

            if (result.plc4xDevice.addAddress.success) {
                this.hideAddAddressModal();
                await this.loadClient();
                this.showSuccess(`Address "${plcAddress}" added successfully`);
            } else {
                const errors = result.plc4xDevice.addAddress.errors || ['Unknown error'];
                this.showError('Failed to add address: ' + errors.join(', '));
            }

        } catch (error) {
            console.error('Error adding address:', error);
            this.showError('Failed to add address: ' + error.message);
        }
    }

    deleteAddress(addressName) {
        this.deleteAddressName = addressName;
        document.getElementById('delete-address-name').textContent = addressName;
        this.showConfirmDeleteAddressModal();
    }

    async confirmDeleteAddress() {
        if (!this.deleteAddressName) return;

        try {
            const mutation = `
                mutation DeletePlc4xAddress($deviceName: String!, $addressName: String!) {
                    plc4xDevice {
                        deleteAddress(deviceName: $deviceName, addressName: $addressName) {
                            success
                            errors
                            client {
                                name
                            }
                        }
                    }
                }
            `;

            const result = await this.client.query(mutation, {
                deviceName: this.clientName,
                addressName: this.deleteAddressName
            });

            if (result.plc4xDevice.deleteAddress.success) {
                this.hideConfirmDeleteAddressModal();
                await this.loadClient();
                this.showSuccess(`Address "${this.deleteAddressName}" deleted successfully`);
            } else {
                const errors = result.plc4xDevice.deleteAddress.errors || ['Unknown error'];
                this.showError('Failed to delete address: ' + errors.join(', '));
            }

        } catch (error) {
            console.error('Error deleting address:', error);
            this.showError('Failed to delete address: ' + error.message);
        }

        this.deleteAddressName = null;
    }

    async saveClient() {
        if (!this.clientData) return;

        const clientData = {
            name: document.getElementById('client-name').value.trim(),
            namespace: document.getElementById('client-namespace').value.trim(),
            nodeId: document.getElementById('client-node').value,
            enabled: document.getElementById('client-enabled').checked,
            config: {
                protocol: document.getElementById('client-protocol').value,
                connectionString: document.getElementById('client-connection-string').value.trim(),
                pollingInterval: parseInt(document.getElementById('client-polling-interval').value),
                reconnectDelay: parseInt(document.getElementById('client-reconnect-delay').value),
                enabled: document.getElementById('client-config-enabled').checked,
                addresses: this.clientData.config.addresses
            }
        };

        try {
            const mutation = `
                mutation UpdatePlc4xClient($name: String!, $input: Plc4xClientInput!) {
                    plc4xDevice {
                        update(name: $name, input: $input) {
                            success
                            errors
                            client {
                                name
                            }
                        }
                    }
                }
            `;

            const result = await this.client.query(mutation, {
                name: this.clientName,
                input: clientData
            });

            if (result.plc4xDevice.update.success) {
                await this.loadClient();
                this.showSuccess(`Client "${this.clientName}" updated successfully`);
            } else {
                const errors = result.plc4xDevice.update.errors || ['Unknown error'];
                this.showError('Failed to update client: ' + errors.join(', '));
            }

        } catch (error) {
            console.error('Error updating client:', error);
            this.showError('Failed to update client: ' + error.message);
        }
    }

    formatProtocol(protocol) {
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
            'S7': 'S7'
        };
        return protocolNames[protocol] || protocol;
    }

    // UI Helper Methods
    showAddAddressModal() {
        document.getElementById('add-address-modal').style.display = 'flex';
        // Reset form
        document.getElementById('add-address-form').reset();
        document.getElementById('address-enabled').checked = true;
        document.getElementById('address-publish-on-change').checked = true;
        document.getElementById('address-qos').value = '0';
    }

    hideAddAddressModal() {
        document.getElementById('add-address-modal').style.display = 'none';
    }

    showConfirmDeleteAddressModal() {
        document.getElementById('confirm-delete-address-modal').style.display = 'flex';
    }

    hideConfirmDeleteAddressModal() {
        document.getElementById('confirm-delete-address-modal').style.display = 'none';
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

    formatDateTime(isoString) {
        try {
            return new Date(isoString).toLocaleString();
        } catch (e) {
            return isoString;
        }
    }

    goBack() {
        window.location.href = '/pages/plc4x-clients.html';
    }
}

// Global functions for onclick handlers
function showAddAddressModal() {
    clientDetailManager.showAddAddressModal();
}

function hideAddAddressModal() {
    clientDetailManager.hideAddAddressModal();
}

function addAddress() {
    clientDetailManager.addAddress();
}

function saveClient() {
    clientDetailManager.saveClient();
}

function hideConfirmDeleteAddressModal() {
    clientDetailManager.hideConfirmDeleteAddressModal();
}

function confirmDeleteAddress() {
    clientDetailManager.confirmDeleteAddress();
}

function goBack() {
    clientDetailManager.goBack();
}

// Initialize when DOM is loaded
let clientDetailManager;
document.addEventListener('DOMContentLoaded', () => {
    clientDetailManager = new Plc4xClientDetailManager();
});

// Handle modal clicks (close when clicking outside)
document.addEventListener('click', (e) => {
    if (e.target.classList.contains('modal')) {
        if (e.target.id === 'add-address-modal') {
            clientDetailManager.hideAddAddressModal();
        } else if (e.target.id === 'confirm-delete-address-modal') {
            clientDetailManager.hideConfirmDeleteAddressModal();
        }
    }
});
