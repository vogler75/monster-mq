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
            const query = `query GetBrokers { brokers { nodeId isCurrent } }`;
            const result = await this.client.query(query);
            this.clusterNodes = result.brokers || [];

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

    async loadClientData() {
        this.showLoading(true);
        this.hideError();

        try {
            const query = `
                query GetMqttClients($name: String!) {
                    mqttClients(name: $name) {
                        name namespace nodeId enabled isOnCurrentNode createdAt updatedAt
                        config {
                            protocol hostname port username clientId cleanSession keepAlive reconnectDelay connectionTimeout
                            bufferEnabled bufferSize persistBuffer deleteOldestMessages
                            addresses { mode remoteTopic localTopic removePath qos }
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

        const d = this.clientData;
        const cfg = d.config;

        // Update page title
        document.getElementById('page-title').textContent = `MQTT Bridge: ${d.name}`;
        document.getElementById('page-subtitle').textContent = `${d.namespace} - ${cfg.protocol}://${cfg.hostname}:${cfg.port}`;

        // Populate form fields
        document.getElementById('client-name').value = d.name;
        document.getElementById('client-name').disabled = true; // Can't change name in edit mode
        document.getElementById('client-namespace').value = d.namespace;
        document.getElementById('client-protocol').value = cfg.protocol;
        document.getElementById('client-hostname').value = cfg.hostname;
        document.getElementById('client-port').value = cfg.port;
        document.getElementById('client-username').value = cfg.username || '';
        document.getElementById('client-id').value = cfg.clientId;
        document.getElementById('client-node').value = d.nodeId;
        document.getElementById('client-keep-alive').value = cfg.keepAlive;
        document.getElementById('client-reconnect-delay').value = cfg.reconnectDelay;
        document.getElementById('client-connection-timeout').value = cfg.connectionTimeout;
        document.getElementById('client-enabled').checked = d.enabled;
        document.getElementById('client-clean-session').checked = cfg.cleanSession;
        document.getElementById('client-buffer-enabled').checked = cfg.bufferEnabled || false;
        document.getElementById('client-buffer-size').value = cfg.bufferSize || 5000;
        document.getElementById('client-persist-buffer').checked = cfg.persistBuffer || false;
        document.getElementById('client-delete-oldest').checked = cfg.deleteOldestMessages !== undefined ? cfg.deleteOldestMessages : true;

        // Timestamps (read-only)
        this.setText('client-created-at', new Date(d.createdAt).toLocaleString());
        this.setText('client-updated-at', new Date(d.updatedAt).toLocaleString());

        // Status badge
        const statusBadge = document.getElementById('client-status');
        if (d.enabled) {
            statusBadge.className = 'status-badge status-enabled';
            statusBadge.textContent = 'ENABLED';
        } else {
            statusBadge.className = 'status-badge status-disabled';
            statusBadge.textContent = 'DISABLED';
        }

        // Update action buttons
        const toggleBtn = document.getElementById('toggle-client-btn');
        if (toggleBtn) {
            toggleBtn.textContent = d.enabled ? 'Stop Bridge' : 'Start Bridge';
            toggleBtn.className = d.enabled ? 'btn btn-warning' : 'btn btn-success';
        }

        // Show content
        document.getElementById('client-content').style.display = 'block';
    }

    renderAddressesList() {
        if (!this.clientData) return;

        const tbody = document.getElementById('addresses-table-body');
        tbody.innerHTML = '';

        if (this.clientData.config.addresses.length === 0) {
            tbody.innerHTML = `
                <tr>
                    <td colspan="6" class="no-data">
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

    async saveClient() {
        const form = document.getElementById('client-form');
        if (!form.checkValidity()) {
            form.reportValidity();
            return;
        }

        const updateData = {
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
                connectionTimeout: parseInt(document.getElementById('client-connection-timeout').value),
                bufferEnabled: document.getElementById('client-buffer-enabled').checked,
                bufferSize: parseInt(document.getElementById('client-buffer-size').value),
                persistBuffer: document.getElementById('client-persist-buffer').checked,
                deleteOldestMessages: document.getElementById('client-delete-oldest').checked
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

    setText(id, value) {
        const el = document.getElementById(id);
        if (el) el.textContent = value;
    }

    goBack() {
        window.location.href = '/pages/mqtt-clients.html';
    }
}

// Global functions
let mqttClientDetailManager;

function saveClient() {
    mqttClientDetailManager.saveClient();
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
document.addEventListener('DOMContentLoaded', () => {
    mqttClientDetailManager = new MqttClientDetailManager();
});

// Handle modal clicks
document.addEventListener('click', (e) => {
    if (e.target.classList.contains('modal')) {
        if (e.target.id === 'add-address-modal') {
            mqttClientDetailManager.hideAddAddressModal();
        } else if (e.target.id === 'confirm-delete-address-modal') {
            mqttClientDetailManager.hideConfirmDeleteAddressModal();
        }
    }
});
