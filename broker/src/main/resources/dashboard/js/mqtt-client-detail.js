// MQTT Client Detail Management JavaScript

class MqttClientDetailManager {
    constructor() {
        this.client = new GraphQLDashboardClient('/graphql');
        this.clientName = null;
        this.clientData = null;
        this.clusterNodes = [];
        this.deleteAddressRemoteTopic = null;
        this.editAddressOriginalRemoteTopic = null;
        this.init();
    }

    async init() {
        const urlParams = new URLSearchParams(window.location.search);
        this.clientName = urlParams.get('client');

        console.log('[DEBUG] Initializing MQTT Client Detail page, client:', this.clientName);

        if (!this.clientName) {
            console.error('[DEBUG] No client name provided in URL');
            this.showError('No bridge specified in URL. Please select a bridge from the list.');
            // Show a basic page structure even without client name
            document.getElementById('page-title').textContent = 'Error';
            document.getElementById('page-subtitle').textContent = 'Invalid Request';
            return;
        }

        this.showLoading(true);
        
        try {
            console.log('[DEBUG] Loading cluster nodes...');
            await this.loadClusterNodes();
            console.log('[DEBUG] Loading client data...');
            await this.loadClientData();
            console.log('[DEBUG] Initialization complete');
        } catch (error) {
            console.error('[DEBUG] Error during initialization:', error);
            this.showError('Failed to load bridge data: ' + error.message);
            // Update page title to show error state
            document.getElementById('page-title').textContent = 'Error Loading Bridge';
            document.getElementById('page-subtitle').textContent = this.clientName;
        } finally {
            this.showLoading(false);
        }
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
                            brokerUrl username clientId cleanSession keepAlive reconnectDelay connectionTimeout
                            bufferEnabled bufferSize persistBuffer deleteOldestMessages sslVerifyCertificate
                            protocolVersion sessionExpiryInterval receiveMaximum maximumPacketSize topicAliasMaximum
                            addresses { 
                                mode remoteTopic localTopic removePath qos 
                                noLocal retainHandling retainAsPublished
                                messageExpiryInterval contentType responseTopicPattern payloadFormatIndicator
                                userProperties { key value }
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
            console.log('[DEBUG] Loaded client data:', JSON.stringify(this.clientData.config, null, 2));
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
        document.getElementById('page-subtitle').textContent = `${d.namespace} - ${cfg.brokerUrl}`;

        // Populate form fields
        document.getElementById('client-name').value = d.name;
        document.getElementById('client-name').disabled = true; // Can't change name in edit mode
        document.getElementById('client-namespace').value = d.namespace;
        document.getElementById('client-broker-url').value = cfg.brokerUrl;
        document.getElementById('client-username').value = cfg.username || '';
        document.getElementById('client-id').value = cfg.clientId;
        document.getElementById('client-node').value = d.nodeId;
        document.getElementById('client-keep-alive').value = cfg.keepAlive;
        document.getElementById('client-reconnect-delay').value = cfg.reconnectDelay;
        document.getElementById('client-connection-timeout').value = cfg.connectionTimeout;
        
        // MQTT v5 properties (default to v3.1.1 if not specified)
        const protocolVersion = cfg.protocolVersion || 4;
        document.getElementById('client-protocol-version').value = protocolVersion;
        document.getElementById('client-session-expiry').value = cfg.sessionExpiryInterval || 0;
        document.getElementById('client-receive-maximum').value = cfg.receiveMaximum || 65535;
        document.getElementById('client-max-packet-size').value = cfg.maximumPacketSize || 268435455;
        document.getElementById('client-topic-alias-max').value = cfg.topicAliasMaximum || 10;
        
        // Toggle v5 sections visibility
        toggleMqtt5Options();
        
        document.getElementById('client-enabled').checked = d.enabled;
        document.getElementById('client-clean-session').checked = cfg.cleanSession;
        // Default to true (secure by default) if undefined or null
        const sslVerifyFromServer = (cfg.sslVerifyCertificate === true || cfg.sslVerifyCertificate === false) ? cfg.sslVerifyCertificate : true;
        console.log('[DEBUG] Setting SSL Verify checkbox to:', sslVerifyFromServer, 'from server value:', cfg.sslVerifyCertificate);
        document.getElementById('client-ssl-verify').checked = sslVerifyFromServer;
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
            
            // Build MQTT v5 subscription options badges (only for SUBSCRIBE mode)
            let mqtt5Badges = '';
            if (address.mode === 'SUBSCRIBE') {
                const hasV5Options = address.noLocal || address.retainHandling !== 0 || address.retainAsPublished;
                if (hasV5Options) {
                    const badges = [];
                    if (address.noLocal) badges.push('🚫 No Local');
                    if (address.retainHandling === 1) badges.push('📨 RH:1');
                    if (address.retainHandling === 2) badges.push('📨 RH:2');
                    if (address.retainAsPublished) badges.push('📌 RAP');
                    mqtt5Badges = `<br><small style="color: var(--text-muted);">${badges.join(' ')}</small>`;
                }
            }

            row.innerHTML = `
                <td>
                    <span class="mode-badge ${modeClass}">
                        ${modeIcon} ${address.mode}
                    </span>${mqtt5Badges}
                </td>
                <td><code>${this.escapeHtml(address.remoteTopic)}</code></td>
                <td><code>${this.escapeHtml(address.localTopic)}</code></td>
                <td>${address.removePath ? 'Yes' : 'No'}</td>
                <td><span class="qos-badge">QoS ${address.qos ?? 0}</span></td>
                <td>
                    <div class="action-buttons">
                        <button class="btn-icon btn-edit"
                                onclick="mqttClientDetailManager.editAddress('${this.escapeHtml(address.remoteTopic)}')"
                                title="Edit Address">
                            <svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor">
                                <path d="M3 17.25V21h3.75L17.81 9.94l-3.75-3.75L3 17.25zM20.71 7.04c.39-.39.39-1.02 0-1.41l-2.34-2.34c-.39-.39-1.02-.39-1.41 0l-1.83 1.83 3.75 3.75 1.83-1.83z"/>
                            </svg>
                        </button>
                        <button class="btn-icon btn-delete"
                                onclick="mqttClientDetailManager.deleteAddress('${this.escapeHtml(address.remoteTopic)}')"
                                title="Delete Address">
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

    async saveClient() {
        const form = document.getElementById('client-form');
        if (!form.checkValidity()) {
            form.reportValidity();
            return;
        }

        const sslVerifyValue = document.getElementById('client-ssl-verify').checked;
        console.log('[DEBUG] SSL Verify checkbox value:', sslVerifyValue);

        const protocolVersion = parseInt(document.getElementById('client-protocol-version').value);
        
        const updateData = {
            name: document.getElementById('client-name').value.trim(),
            namespace: document.getElementById('client-namespace').value.trim(),
            nodeId: document.getElementById('client-node').value,
            enabled: document.getElementById('client-enabled').checked,
            config: {
                brokerUrl: document.getElementById('client-broker-url').value.trim(),
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
                deleteOldestMessages: document.getElementById('client-delete-oldest').checked,
                sslVerifyCertificate: sslVerifyValue,
                // MQTT v5 properties
                protocolVersion: protocolVersion,
                sessionExpiryInterval: protocolVersion === 5 ? parseInt(document.getElementById('client-session-expiry').value) : null,
                receiveMaximum: protocolVersion === 5 ? parseInt(document.getElementById('client-receive-maximum').value) : null,
                maximumPacketSize: protocolVersion === 5 ? parseInt(document.getElementById('client-max-packet-size').value) : null,
                topicAliasMaximum: protocolVersion === 5 ? parseInt(document.getElementById('client-topic-alias-max').value) : null
            }
        };

        console.log('[DEBUG] Update data being sent:', JSON.stringify(updateData, null, 2));

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
                console.log('[DEBUG] Update successful, reloading data...');
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

        const mode = document.getElementById('address-mode').value;
        const protocolVersion = parseInt(document.getElementById('client-protocol-version').value);
        
        const addressData = {
            mode: mode,
            remoteTopic: document.getElementById('address-remote-topic').value.trim(),
            localTopic: document.getElementById('address-local-topic').value.trim(),
            removePath: document.getElementById('address-remove-path').checked,
            qos: parseInt(document.getElementById('address-qos').value)
        };
        
        // Add MQTT v5 subscription options if protocol is v5 and mode is SUBSCRIBE
        if (protocolVersion === 5 && mode === 'SUBSCRIBE') {
            addressData.noLocal = document.getElementById('address-no-local').checked;
            addressData.retainHandling = parseInt(document.getElementById('address-retain-handling').value);
            addressData.retainAsPublished = document.getElementById('address-retain-as-published').checked;
        }
        
        // Add MQTT v5 message properties if protocol is v5 and mode is PUBLISH
        if (protocolVersion === 5 && mode === 'PUBLISH') {
            const messageExpiry = document.getElementById('address-message-expiry').value;
            if (messageExpiry) addressData.messageExpiryInterval = parseInt(messageExpiry);
            
            const contentType = document.getElementById('address-content-type').value.trim();
            if (contentType) addressData.contentType = contentType;
            
            const responseTopic = document.getElementById('address-response-topic').value.trim();
            if (responseTopic) addressData.responseTopicPattern = responseTopic;
            
            addressData.payloadFormatIndicator = document.getElementById('address-payload-format').checked;
            
            // Collect user properties
            const userProps = [];
            document.querySelectorAll('#address-user-properties-list .user-property-row').forEach(row => {
                const key = row.querySelector('.user-property-key').value.trim();
                const value = row.querySelector('.user-property-value').value.trim();
                if (key && value) {
                    userProps.push({ key, value });
                }
            });
            if (userProps.length > 0) addressData.userProperties = userProps;
        }

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

    editAddress(remoteTopic) {
        // Find the address by remoteTopic
        const address = this.clientData.config.addresses.find(a => a.remoteTopic === remoteTopic);
        if (!address) {
            this.showError('Address not found');
            return;
        }

        // Store original remote topic for update
        this.editAddressOriginalRemoteTopic = remoteTopic;

        // Populate edit form
        document.getElementById('edit-address-mode').value = address.mode;
        document.getElementById('edit-address-remote-topic').value = address.remoteTopic;
        document.getElementById('edit-address-local-topic').value = address.localTopic;
        document.getElementById('edit-address-remove-path').checked = address.removePath;
        document.getElementById('edit-address-qos').value = address.qos ?? 0;
        
        // Populate MQTT v5 subscription options
        document.getElementById('edit-address-no-local').checked = address.noLocal ?? false;
        document.getElementById('edit-address-retain-handling').value = address.retainHandling ?? 0;
        document.getElementById('edit-address-retain-as-published').checked = address.retainAsPublished ?? false;
        
        // Populate MQTT v5 message properties
        document.getElementById('edit-address-message-expiry').value = address.messageExpiryInterval ?? '';
        document.getElementById('edit-address-content-type').value = address.contentType ?? '';
        document.getElementById('edit-address-response-topic').value = address.responseTopicPattern ?? '';
        document.getElementById('edit-address-payload-format').checked = address.payloadFormatIndicator ?? false;
        
        // Populate user properties
        const userPropsList = document.getElementById('edit-address-user-properties-list');
        userPropsList.innerHTML = '';
        if (address.userProperties && address.userProperties.length > 0) {
            address.userProperties.forEach(prop => {
                addEditUserProperty();
                const lastRow = userPropsList.lastElementChild;
                lastRow.querySelector('.edit-user-property-key').value = prop.key;
                lastRow.querySelector('.edit-user-property-value').value = prop.value;
            });
        }

        // Show modal
        this.showEditAddressModal();
        
        // Toggle visibility of subscription options and message properties based on mode and protocol version
        toggleMqtt5SubscriptionOptions();
        toggleMqtt5MessageProperties();
    }

    async updateAddress() {
        const form = document.getElementById('edit-address-form');
        if (!form.checkValidity()) {
            form.reportValidity();
            return;
        }

        const mode = document.getElementById('edit-address-mode').value;
        const protocolVersion = parseInt(document.getElementById('client-protocol-version').value);
        
        const updatedAddress = {
            mode: mode,
            remoteTopic: document.getElementById('edit-address-remote-topic').value.trim(),
            localTopic: document.getElementById('edit-address-local-topic').value.trim(),
            removePath: document.getElementById('edit-address-remove-path').checked,
            qos: parseInt(document.getElementById('edit-address-qos').value)
        };
        
        // Add MQTT v5 subscription options if protocol is v5 and mode is SUBSCRIBE
        if (protocolVersion === 5 && mode === 'SUBSCRIBE') {
            updatedAddress.noLocal = document.getElementById('edit-address-no-local').checked;
            updatedAddress.retainHandling = parseInt(document.getElementById('edit-address-retain-handling').value);
            updatedAddress.retainAsPublished = document.getElementById('edit-address-retain-as-published').checked;
        }
        
        // Add MQTT v5 message properties if protocol is v5 and mode is PUBLISH
        if (protocolVersion === 5 && mode === 'PUBLISH') {
            const messageExpiry = document.getElementById('edit-address-message-expiry').value;
            if (messageExpiry) updatedAddress.messageExpiryInterval = parseInt(messageExpiry);
            
            const contentType = document.getElementById('edit-address-content-type').value.trim();
            if (contentType) updatedAddress.contentType = contentType;
            
            const responseTopic = document.getElementById('edit-address-response-topic').value.trim();
            if (responseTopic) updatedAddress.responseTopicPattern = responseTopic;
            
            updatedAddress.payloadFormatIndicator = document.getElementById('edit-address-payload-format').checked;
            
            // Collect user properties
            const userProps = [];
            document.querySelectorAll('#edit-address-user-properties-list .user-property-row').forEach(row => {
                const key = row.querySelector('.edit-user-property-key').value.trim();
                const value = row.querySelector('.edit-user-property-value').value.trim();
                if (key && value) {
                    userProps.push({ key, value });
                }
            });
            if (userProps.length > 0) updatedAddress.userProperties = userProps;
        }

        try {
            const mutation = `
                mutation UpdateMqttClientAddress($deviceName: String!, $remoteTopic: String!, $input: MqttClientAddressInput!) {
                    mqttClient {
                        updateAddress(deviceName: $deviceName, remoteTopic: $remoteTopic, input: $input) {
                            success
                            errors
                        }
                    }
                }
            `;

            const result = await this.client.query(mutation, {
                deviceName: this.clientName,
                remoteTopic: this.editAddressOriginalRemoteTopic,
                input: updatedAddress
            });

            if (result.mqttClient.updateAddress.success) {
                this.hideEditAddressModal();
                await this.loadClientData();
                this.showSuccess('Address mapping updated successfully');
            } else {
                const errors = result.mqttClient.updateAddress.errors || ['Unknown error'];
                this.showError('Failed to update address: ' + errors.join(', '));
            }

        } catch (error) {
            console.error('Error updating address:', error);
            this.showError('Failed to update address: ' + error.message);
        }

        this.editAddressOriginalRemoteTopic = null;
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

    showEditAddressModal() {
        document.getElementById('edit-address-modal').style.display = 'flex';
    }

    hideEditAddressModal() {
        document.getElementById('edit-address-modal').style.display = 'none';
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
            // Ensure content area is visible so error message shows
            const contentEl = document.getElementById('client-content');
            if (contentEl) {
                contentEl.style.display = 'block';
            }
            setTimeout(() => this.hideError(), 10000); // Longer timeout for errors
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

function showEditAddressModal() {
    mqttClientDetailManager.showEditAddressModal();
}

function hideEditAddressModal() {
    mqttClientDetailManager.hideEditAddressModal();
}

function updateAddress() {
    mqttClientDetailManager.updateAddress();
}

function hideConfirmDeleteAddressModal() {
    mqttClientDetailManager.hideConfirmDeleteAddressModal();
}

function confirmDeleteAddress() {
    mqttClientDetailManager.confirmDeleteAddress();
}

function toggleMqtt5Options() {
    const protocolVersion = parseInt(document.getElementById('client-protocol-version').value);
    const mqtt5Sections = document.querySelectorAll('[id^="mqtt5-"]:not([id*="subscription-options"]):not([id*="message-properties"])');
    mqtt5Sections.forEach(section => {
        section.style.display = protocolVersion === 5 ? 'block' : 'none';
    });
    // Also toggle subscription options and message properties visibility
    toggleMqtt5SubscriptionOptions();
    toggleMqtt5MessageProperties();
}

function toggleMqtt5SubscriptionOptions() {
    const protocolVersion = parseInt(document.getElementById('client-protocol-version').value);
    const addMode = document.getElementById('address-mode')?.value;
    const editMode = document.getElementById('edit-address-mode')?.value;
    
    // Show subscription options only if protocol is v5 and mode is SUBSCRIBE
    const addSubOptions = document.getElementById('mqtt5-subscription-options');
    if (addSubOptions) {
        addSubOptions.style.display = (protocolVersion === 5 && addMode === 'SUBSCRIBE') ? 'block' : 'none';
    }
    
    const editSubOptions = document.getElementById('mqtt5-edit-subscription-options');
    if (editSubOptions) {
        editSubOptions.style.display = (protocolVersion === 5 && editMode === 'SUBSCRIBE') ? 'block' : 'none';
    }
}

function toggleMqtt5MessageProperties() {
    const protocolVersion = parseInt(document.getElementById('client-protocol-version').value);
    const addMode = document.getElementById('address-mode')?.value;
    const editMode = document.getElementById('edit-address-mode')?.value;
    
    // Show message properties only if protocol is v5 and mode is PUBLISH
    const addMsgProps = document.getElementById('mqtt5-message-properties');
    if (addMsgProps) {
        addMsgProps.style.display = (protocolVersion === 5 && addMode === 'PUBLISH') ? 'block' : 'none';
    }
    
    const editMsgProps = document.getElementById('mqtt5-edit-message-properties');
    if (editMsgProps) {
        editMsgProps.style.display = (protocolVersion === 5 && editMode === 'PUBLISH') ? 'block' : 'none';
    }
}

// User Properties Management for Add Address Modal
function addUserProperty() {
    const list = document.getElementById('address-user-properties-list');
    const index = list.children.length;
    
    const row = document.createElement('div');
    row.className = 'user-property-row';
    row.style.cssText = 'display: flex; gap: 0.5rem; margin-bottom: 0.5rem; align-items: center;';
    row.innerHTML = `
        <input type="text" placeholder="Key" style="flex: 1;" class="user-property-key">
        <input type="text" placeholder="Value" style="flex: 1;" class="user-property-value">
        <button type="button" class="btn-icon btn-delete" onclick="this.parentElement.remove()" title="Remove">
            <svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor">
                <path d="M6 19c0 1.1.9 2 2 2h8c1.1 0 2-.9 2-2V7H6v12zM19 4h-3.5l-1-1h-5l-1 1H5v2h14V4z"/>
            </svg>
        </button>
    `;
    list.appendChild(row);
}

// User Properties Management for Edit Address Modal
function addEditUserProperty() {
    const list = document.getElementById('edit-address-user-properties-list');
    const index = list.children.length;
    
    const row = document.createElement('div');
    row.className = 'user-property-row';
    row.style.cssText = 'display: flex; gap: 0.5rem; margin-bottom: 0.5rem; align-items: center;';
    row.innerHTML = `
        <input type="text" placeholder="Key" style="flex: 1;" class="edit-user-property-key">
        <input type="text" placeholder="Value" style="flex: 1;" class="edit-user-property-value">
        <button type="button" class="btn-icon btn-delete" onclick="this.parentElement.remove()" title="Remove">
            <svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor">
                <path d="M6 19c0 1.1.9 2 2 2h8c1.1 0 2-.9 2-2V7H6v12zM19 4h-3.5l-1-1h-5l-1 1H5v2h14V4z"/>
            </svg>
        </button>
    `;
    list.appendChild(row);
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
        } else if (e.target.id === 'edit-address-modal') {
            mqttClientDetailManager.hideEditAddressModal();
        } else if (e.target.id === 'confirm-delete-address-modal') {
            mqttClientDetailManager.hideConfirmDeleteAddressModal();
        }
    }
});
