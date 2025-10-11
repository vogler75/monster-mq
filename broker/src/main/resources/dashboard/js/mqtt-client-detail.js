// MQTT Client Detail Management JavaScript

class MqttClientDetailManager {
    constructor() {
        this.client = new GraphQLDashboardClient('/graphql');
        this.clientName = null;
        this.clientData = null;
        this.clusterNodes = [];
        this.deleteAddressRemoteTopic = null;
        this.init();
    }

    async init() {
        // Get client name from URL parameter
        const urlParams = new URLSearchParams(window.location.search);
        this.clientName = urlParams.get('client');

        if (!this.clientName) {
            this.showError('No bridge specified');
            return;
        }

        await this.loadClusterNodes();
        await this.loadClientData();
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

            const nodeSelect = document.getElementById('edit-client-node');
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

    async loadClientData() {
        this.showLoading(true);
        this.hideError();

        try {
            const query = `
                query GetMqttClients($name: String!) {
                    mqttClients(name: $name) {
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
                            bufferEnabled
                            bufferSize
                            persistBuffer
                            deleteOldestMessages
                            addresses {
                                mode
                                remoteTopic
                                localTopic
                                removePath
                                qos
                            }
                        }
                    }
                }
            `;

            const result = await this.client.query(query, { name: this.clientName });

            if (!result.mqttClients || result.mqttClients.length === 0) {
                throw new Error('Bridge not found');
            }

            this.clientData = result.mqttClients[0];
            this.renderClientInfo();
            this.renderAddressesList();

        } catch (error) {
            console.error('Error loading bridge:', error);
            this.showError('Failed to load bridge: ' + error.message);
        } finally {
            this.showLoading(false);
        }
    }

    renderClientInfo() {
        if (!this.clientData) return;

        document.getElementById('page-title').textContent = `MQTT Bridge: ${this.clientData.name}`;
        document.getElementById('client-name-display').textContent = this.clientData.name;
        document.getElementById('client-namespace').textContent = this.clientData.namespace;
        document.getElementById('client-broker-url').textContent =
            `${this.clientData.config.protocol}://${this.clientData.config.hostname}:${this.clientData.config.port}`;
        document.getElementById('client-node-id').textContent = this.clientData.nodeId;
        document.getElementById('client-client-id').textContent = this.clientData.config.clientId;
        document.getElementById('client-username').textContent = this.clientData.config.username || 'None';
        document.getElementById('client-clean-session').textContent = this.clientData.config.cleanSession ? 'Yes' : 'No';
        document.getElementById('client-keep-alive').textContent = `${this.clientData.config.keepAlive}s`;
        document.getElementById('client-reconnect-delay').textContent = `${this.clientData.config.reconnectDelay}ms`;
        document.getElementById('client-connection-timeout').textContent = `${this.clientData.config.connectionTimeout}ms`;
        document.getElementById('client-buffer-enabled').textContent = this.clientData.config.bufferEnabled ? 'Yes' : 'No';
        document.getElementById('client-buffer-size').textContent = `${this.clientData.config.bufferSize || 5000} messages`;
        document.getElementById('client-persist-buffer').textContent = this.clientData.config.persistBuffer ? 'Yes' : 'No';
        document.getElementById('client-delete-oldest').textContent = (this.clientData.config.deleteOldestMessages !== undefined ? this.clientData.config.deleteOldestMessages : true) ? 'Yes' : 'No';
        document.getElementById('client-created-at').textContent = new Date(this.clientData.createdAt).toLocaleString();
        document.getElementById('client-updated-at').textContent = new Date(this.clientData.updatedAt).toLocaleString();

        const statusBadge = document.getElementById('client-status');
        if (this.clientData.enabled) {
            statusBadge.className = 'status-badge status-enabled';
            statusBadge.textContent = 'ENABLED';
        } else {
            statusBadge.className = 'status-badge status-disabled';
            statusBadge.textContent = 'DISABLED';
        }

        // Update action buttons
        const toggleBtn = document.getElementById('toggle-client-btn');
        if (toggleBtn) {
            toggleBtn.textContent = this.clientData.enabled ? 'Stop Bridge' : 'Start Bridge';
            toggleBtn.className = this.clientData.enabled ? 'btn btn-warning' : 'btn btn-success';
        }

        // Populate edit form
        this.populateEditForm();
    }

    populateEditForm() {
        if (!this.clientData) return;

        document.getElementById('edit-client-name').value = this.clientData.name;
        document.getElementById('edit-client-namespace').value = this.clientData.namespace;
        document.getElementById('edit-client-protocol').value = this.clientData.config.protocol;
        document.getElementById('edit-client-hostname').value = this.clientData.config.hostname;
        document.getElementById('edit-client-port').value = this.clientData.config.port;
        document.getElementById('edit-client-username').value = this.clientData.config.username || '';
        document.getElementById('edit-client-id').value = this.clientData.config.clientId;
        document.getElementById('edit-client-node').value = this.clientData.nodeId;
        document.getElementById('edit-client-keep-alive').value = this.clientData.config.keepAlive;
        document.getElementById('edit-client-reconnect-delay').value = this.clientData.config.reconnectDelay;
        document.getElementById('edit-client-connection-timeout').value = this.clientData.config.connectionTimeout;
        document.getElementById('edit-client-enabled').checked = this.clientData.enabled;
        document.getElementById('edit-client-clean-session').checked = this.clientData.config.cleanSession;
        document.getElementById('edit-client-buffer-enabled').checked = this.clientData.config.bufferEnabled || false;
        document.getElementById('edit-client-buffer-size').value = this.clientData.config.bufferSize || 5000;
        document.getElementById('edit-client-persist-buffer').checked = this.clientData.config.persistBuffer || false;
        document.getElementById('edit-client-delete-oldest').checked = this.clientData.config.deleteOldestMessages !== undefined ? this.clientData.config.deleteOldestMessages : true;
    }

    renderAddressesList() {
        if (!this.clientData) return;

        const tbody = document.getElementById('addresses-table-body');
        tbody.innerHTML = '';

        if (this.clientData.config.addresses.length === 0) {
            tbody.innerHTML = `
                <tr>
                    <td colspan="5" class="no-data">
                        No address mappings configured. Click "Add Address" to create one.
                    </td>
                </tr>
            `;
            return;
        }

        this.clientData.config.addresses.forEach(address => {
            const row = document.createElement('tr');
            const modeClass = address.mode === 'SUBSCRIBE' ? 'mode-subscribe' : 'mode-publish';
            const modeIcon = address.mode === 'SUBSCRIBE' ? '⬇️' : '⬆️';

            row.innerHTML = `
                <td>
                    <span class="mode-badge ${modeClass}">
                        ${modeIcon} ${address.mode}
                    </span>
                </td>
                <td><code>${this.escapeHtml(address.remoteTopic)}</code></td>
                <td><code>${this.escapeHtml(address.localTopic)}</code></td>
                <td>${address.removePath ? 'Yes' : 'No'}</td>
                <td><span class="qos-badge">QoS ${address.qos ?? 0}</span></td>
                <td>
                    <button class="btn-action btn-delete"
                            onclick="mqttClientDetailManager.deleteAddress('${this.escapeHtml(address.remoteTopic)}')"
                            title="Delete Address">
                        <svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor">
                            <path d="M6 19c0 1.1.9 2 2 2h8c1.1 0 2-.9 2-2V7H6v12zM19 4h-3.5l-1-1h-5l-1 1H5v2h14V4z"/>
                        </svg>
                    </button>
                </td>
            `;
            tbody.appendChild(row);
        });
    }

    async updateClient() {
        const form = document.getElementById('edit-client-form');
        if (!form.checkValidity()) {
            form.reportValidity();
            return;
        }

        const updateData = {
            name: document.getElementById('edit-client-name').value.trim(),
            namespace: document.getElementById('edit-client-namespace').value.trim(),
            nodeId: document.getElementById('edit-client-node').value,
            enabled: document.getElementById('edit-client-enabled').checked,
            config: {
                protocol: document.getElementById('edit-client-protocol').value,
                hostname: document.getElementById('edit-client-hostname').value.trim(),
                port: parseInt(document.getElementById('edit-client-port').value),
                username: document.getElementById('edit-client-username').value.trim() || null,
                password: document.getElementById('edit-client-password').value || null,
                clientId: document.getElementById('edit-client-id').value.trim(),
                cleanSession: document.getElementById('edit-client-clean-session').checked,
                keepAlive: parseInt(document.getElementById('edit-client-keep-alive').value),
                reconnectDelay: parseInt(document.getElementById('edit-client-reconnect-delay').value),
                connectionTimeout: parseInt(document.getElementById('edit-client-connection-timeout').value),
                bufferEnabled: document.getElementById('edit-client-buffer-enabled').checked,
                bufferSize: parseInt(document.getElementById('edit-client-buffer-size').value),
                persistBuffer: document.getElementById('edit-client-persist-buffer').checked,
                deleteOldestMessages: document.getElementById('edit-client-delete-oldest').checked
            }
        };

        try {
            const mutation = `
                mutation UpdateMqttClient($name: String!, $input: MqttClientInput!) {
                    mqttClient {
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
                input: updateData
            });

            if (result.mqttClient.update.success) {
                this.hideEditModal();
                await this.loadClientData();
                this.showSuccess('Bridge updated successfully');
            } else {
                const errors = result.mqttClient.update.errors || ['Unknown error'];
                this.showError('Failed to update bridge: ' + errors.join(', '));
            }

        } catch (error) {
            console.error('Error updating client:', error);
            this.showError('Failed to update bridge: ' + error.message);
        }
    }

    async toggleClient() {
        if (!this.clientData) return;

        const newState = !this.clientData.enabled;

        try {
            const mutation = `
                mutation ToggleMqttClient($name: String!, $enabled: Boolean!) {
                    mqttClient {
                        toggle(name: $name, enabled: $enabled) {
                            success
                            errors
                        }
                    }
                }
            `;

            const result = await this.client.query(mutation, {
                name: this.clientName,
                enabled: newState
            });

            if (result.mqttClient.toggle.success) {
                await this.loadClientData();
                this.showSuccess(`Bridge ${newState ? 'started' : 'stopped'} successfully`);
            } else {
                const errors = result.mqttClient.toggle.errors || ['Unknown error'];
                this.showError('Failed to toggle bridge: ' + errors.join(', '));
            }

        } catch (error) {
            console.error('Error toggling client:', error);
            this.showError('Failed to toggle bridge: ' + error.message);
        }
    }

    async addAddress() {
        const form = document.getElementById('add-address-form');
        if (!form.checkValidity()) {
            form.reportValidity();
            return;
        }

        const addressData = {
            mode: document.getElementById('address-mode').value,
            remoteTopic: document.getElementById('address-remote-topic').value.trim(),
            localTopic: document.getElementById('address-local-topic').value.trim(),
            removePath: document.getElementById('address-remove-path').checked,
            qos: parseInt(document.getElementById('address-qos').value)
        };

        try {
            const mutation = `
                mutation AddMqttClientAddress($deviceName: String!, $input: MqttClientAddressInput!) {
                    mqttClient {
                        addAddress(deviceName: $deviceName, input: $input) {
                            success
                            errors
                        }
                    }
                }
            `;

            const result = await this.client.query(mutation, {
                deviceName: this.clientName,
                input: addressData
            });

            if (result.mqttClient.addAddress.success) {
                this.hideAddAddressModal();
                await this.loadClientData();
                this.showSuccess('Address mapping added successfully');
            } else {
                const errors = result.mqttClient.addAddress.errors || ['Unknown error'];
                this.showError('Failed to add address: ' + errors.join(', '));
            }

        } catch (error) {
            console.error('Error adding address:', error);
            this.showError('Failed to add address: ' + error.message);
        }
    }

    deleteAddress(remoteTopic) {
        this.deleteAddressRemoteTopic = remoteTopic;
        document.getElementById('delete-address-name').textContent = remoteTopic;
        this.showConfirmDeleteAddressModal();
    }

    async confirmDeleteAddress() {
        if (!this.deleteAddressRemoteTopic) return;

        try {
            const mutation = `
                mutation DeleteMqttClientAddress($deviceName: String!, $remoteTopic: String!) {
                    mqttClient {
                        deleteAddress(deviceName: $deviceName, remoteTopic: $remoteTopic) {
                            success
                            errors
                        }
                    }
                }
            `;

            const result = await this.client.query(mutation, {
                deviceName: this.clientName,
                remoteTopic: this.deleteAddressRemoteTopic
            });

            if (result.mqttClient.deleteAddress.success) {
                this.hideConfirmDeleteAddressModal();
                await this.loadClientData();
                this.showSuccess('Address mapping deleted successfully');
            } else {
                const errors = result.mqttClient.deleteAddress.errors || ['Unknown error'];
                this.showError('Failed to delete address: ' + errors.join(', '));
            }

        } catch (error) {
            console.error('Error deleting address:', error);
            this.showError('Failed to delete address: ' + error.message);
        }

        this.deleteAddressRemoteTopic = null;
    }

    // UI Helper Methods
    showEditModal() {
        this.populateEditForm();
        document.getElementById('edit-client-modal').style.display = 'flex';
    }

    hideEditModal() {
        document.getElementById('edit-client-modal').style.display = 'none';
    }

    showAddAddressModal() {
        document.getElementById('add-address-modal').style.display = 'flex';
        document.getElementById('add-address-form').reset();
        document.getElementById('address-remove-path').checked = true;
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

    goBack() {
        window.location.href = '/pages/mqtt-clients.html';
    }
}

// Global functions
function showEditModal() {
    mqttClientDetailManager.showEditModal();
}

function hideEditModal() {
    mqttClientDetailManager.hideEditModal();
}

function updateClient() {
    mqttClientDetailManager.updateClient();
}

function toggleClient() {
    mqttClientDetailManager.toggleClient();
}

function showAddAddressModal() {
    mqttClientDetailManager.showAddAddressModal();
}

function hideAddAddressModal() {
    mqttClientDetailManager.hideAddAddressModal();
}

function addAddress() {
    mqttClientDetailManager.addAddress();
}

function hideConfirmDeleteAddressModal() {
    mqttClientDetailManager.hideConfirmDeleteAddressModal();
}

function confirmDeleteAddress() {
    mqttClientDetailManager.confirmDeleteAddress();
}

function goBack() {
    mqttClientDetailManager.goBack();
}

// Initialize
let mqttClientDetailManager;
document.addEventListener('DOMContentLoaded', () => {
    mqttClientDetailManager = new MqttClientDetailManager();
});

// Handle modal clicks
document.addEventListener('click', (e) => {
    if (e.target.classList.contains('modal')) {
        if (e.target.id === 'edit-client-modal') {
            mqttClientDetailManager.hideEditModal();
        } else if (e.target.id === 'add-address-modal') {
            mqttClientDetailManager.hideAddAddressModal();
        } else if (e.target.id === 'confirm-delete-address-modal') {
            mqttClientDetailManager.hideConfirmDeleteAddressModal();
        }
    }
});