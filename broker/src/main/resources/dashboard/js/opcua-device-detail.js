// OPC UA Device Detail Management JavaScript

class OpcUaDeviceDetailManager {
    constructor() {
        this.client = new GraphQLDashboardClient('/graphql');
        this.device = null;
        this.clusterNodes = [];
        this.deviceName = null;
        this.deleteAddressName = null;
        this.init();
    }

    async init() {
        const urlParams = new URLSearchParams(window.location.search);
        this.deviceName = urlParams.get('device');
        this.isNew = urlParams.get('new') === 'true';

        if (this.isNew) {
            await this.loadClusterNodes();
            this.showNewDeviceForm();
            return;
        }

        if (!this.deviceName) {
            this.showError('No device specified');
            return;
        }

        await this.loadClusterNodes();
        await this.loadDevice();
    }

    async loadClusterNodes() {
        try {
            const query = `query GetBrokers { brokers { nodeId isCurrent } }`;
            const result = await this.client.query(query);
            this.clusterNodes = result.brokers || [];
            const nodeSelect = document.getElementById('device-node');
            if (nodeSelect) {
                nodeSelect.innerHTML = '<option value="">Select Node...</option>';
                this.clusterNodes.forEach(node => {
                    const opt = document.createElement('option');
                    opt.value = node.nodeId;
                    opt.textContent = node.nodeId + (node.isCurrent ? ' (Current)' : '');
                    nodeSelect.appendChild(opt);
                });
            }
        } catch (e) {
            console.error('Error loading cluster nodes', e);
        }
    }

    async loadDevice() {
        this.showLoading(true);
        this.hideError();

        try {
            const query = `
                query GetOpcUaDevices($name: String!) {
                    opcUaDevices(name: $name) {
                        name
                        namespace
                        nodeId
                        enabled
                        isOnCurrentNode
                        createdAt
                        updatedAt
                        config {
                            endpointUrl
                            updateEndpointUrl
                            securityPolicy
                            username
                            subscriptionSamplingInterval
                            keepAliveFailuresAllowed
                            reconnectDelay
                            connectionTimeout
                            requestTimeout
                            monitoringParameters {
                                bufferSize
                                samplingInterval
                                discardOldest
                            }
                            addresses {
                                address
                                topic
                                publishMode
                                removePath
                            }
                            certificateConfig {
                                securityDir
                                applicationName
                                applicationUri
                                organization
                                organizationalUnit
                                localityName
                                countryCode
                                createSelfSigned
                                keystorePassword
                                validateServerCertificate
                                autoAcceptServerCertificates
                            }
                        }
                    }
                }
            `;

            const result = await this.client.query(query, { name: this.deviceName });
            this.device = result.opcUaDevices && result.opcUaDevices.length > 0 ? result.opcUaDevices[0] : null;

            if (!this.device) {
                this.showError(`Device "${this.deviceName}" not found`);
                return;
            }

            this.renderDevice();

        } catch (error) {
            console.error('Error loading device:', error);
            this.showError('Failed to load device: ' + error.message);
        } finally {
            this.showLoading(false);
        }
    }

    renderDevice() {
        if (!this.device) return;

        // Update page title
        document.getElementById('device-title').textContent = `OPC UA Client: ${this.device.name}`;
        document.getElementById('device-subtitle').textContent = `${this.device.namespace} - ${this.device.config.endpointUrl}`;

        // Device status
        const statusBadge = document.getElementById('device-status-badge');
        const statusClass = this.device.enabled ? 'status-enabled' : 'status-disabled';
        statusBadge.className = `status-badge ${statusClass}`;
        statusBadge.textContent = this.device.enabled ? 'Enabled' : 'Disabled';

        // Populate form fields - Device information
        document.getElementById('device-name').value = this.device.name;
        document.getElementById('device-namespace').value = this.device.namespace;
        document.getElementById('device-endpoint').value = this.device.config.endpointUrl;
        document.getElementById('device-security').value = this.device.config.securityPolicy;
        document.getElementById('device-node').value = this.device.nodeId;
        document.getElementById('device-update-endpoint').checked = this.device.config.updateEndpointUrl;
        document.getElementById('device-username').value = this.device.config.username || '';
        document.getElementById('device-enabled').checked = this.device.enabled;
        document.getElementById('device-created').textContent = this.formatDateTime(this.device.createdAt);

        // Connection configuration
        document.getElementById('config-sampling').value = this.device.config.subscriptionSamplingInterval;
        document.getElementById('config-keepalive').value = this.device.config.keepAliveFailuresAllowed;
        document.getElementById('config-reconnect').value = this.device.config.reconnectDelay;
        document.getElementById('config-connection-timeout').value = this.device.config.connectionTimeout;
        document.getElementById('config-request-timeout').value = this.device.config.requestTimeout;
        document.getElementById('config-buffer-size').value = this.device.config.monitoringParameters.bufferSize;
        document.getElementById('config-monitoring-sampling').value = this.device.config.monitoringParameters.samplingInterval;
        document.getElementById('config-discard-oldest').checked = this.device.config.monitoringParameters.discardOldest;

        // Certificate configuration
        const certConfig = this.device.config.certificateConfig;
        document.getElementById('cert-security-dir').value = certConfig.securityDir;
        document.getElementById('cert-application-name').value = certConfig.applicationName;
        document.getElementById('cert-application-uri').value = certConfig.applicationUri;
        document.getElementById('cert-organization').value = certConfig.organization;
        document.getElementById('cert-organizational-unit').value = certConfig.organizationalUnit;
        document.getElementById('cert-locality').value = certConfig.localityName;
        document.getElementById('cert-country').value = certConfig.countryCode;
        document.getElementById('cert-create-self-signed').checked = certConfig.createSelfSigned;
        document.getElementById('cert-validate-server').checked = certConfig.validateServerCertificate;
        document.getElementById('cert-auto-accept').checked = certConfig.autoAcceptServerCertificates;

        // Render addresses
        this.renderAddresses();

        // Show content
        document.getElementById('device-content').style.display = 'block';
    }

    showNewDeviceForm() {
        document.getElementById('device-title').textContent = 'Add OPC UA Client';
        document.getElementById('device-subtitle').textContent = 'Create a new OPC UA device connection';

        // Enable name field
        document.getElementById('device-name').value = '';
        document.getElementById('device-name').disabled = false;
        document.getElementById('device-namespace').value = '';
        document.getElementById('device-endpoint').value = '';
        document.getElementById('device-security').value = 'None';
        document.getElementById('device-update-endpoint').checked = false;
        document.getElementById('device-username').value = '';
        document.getElementById('device-enabled').checked = true;

        // Set connection defaults
        document.getElementById('config-sampling').value = '0';
        document.getElementById('config-keepalive').value = '3';
        document.getElementById('config-reconnect').value = '5000';
        document.getElementById('config-connection-timeout').value = '5000';
        document.getElementById('config-request-timeout').value = '5000';
        document.getElementById('config-buffer-size').value = '10';
        document.getElementById('config-monitoring-sampling').value = '0';
        document.getElementById('config-discard-oldest').checked = true;

        // Set certificate defaults
        document.getElementById('cert-security-dir').value = 'security';
        document.getElementById('cert-application-name').value = 'MonsterMQ OPC UA Client';
        document.getElementById('cert-application-uri').value = '';
        document.getElementById('cert-organization').value = '';
        document.getElementById('cert-organizational-unit').value = '';
        document.getElementById('cert-locality').value = '';
        document.getElementById('cert-country').value = '';
        document.getElementById('cert-create-self-signed').checked = true;
        document.getElementById('cert-validate-server').checked = false;
        document.getElementById('cert-auto-accept').checked = true;

        // Hide status badge and addresses section
        const statusBadge = document.getElementById('device-status-badge');
        if (statusBadge) statusBadge.style.display = 'none';
        const addressesSection = document.querySelector('.addresses-section');
        if (addressesSection) addressesSection.style.display = 'none';
        const createdRow = document.getElementById('device-created-row');
        if (createdRow) createdRow.style.display = 'none';

        // Update save button label
        const saveBtn = document.getElementById('save-device-btn');
        if (saveBtn) saveBtn.innerHTML = saveBtn.innerHTML.replace('Save Device', 'Create Device');

        document.getElementById('device-content').style.display = 'block';
    }

    renderAddresses() {
        const tbody = document.getElementById('addresses-table-body');
        const noAddresses = document.getElementById('no-addresses');
        const addressesTable = document.getElementById('addresses-table');

        if (!tbody) return;

        tbody.innerHTML = '';

        if (!this.device.config.addresses || this.device.config.addresses.length === 0) {
            addressesTable.style.display = 'none';
            noAddresses.style.display = 'block';
            return;
        }

        addressesTable.style.display = 'block';
        noAddresses.style.display = 'none';

        this.device.config.addresses.forEach(address => {
            const row = document.createElement('tr');

            const publishModeIcon = address.publishMode === 'SEPARATE' ? '📊' : '📈';
            const removePathIcon = address.removePath ? '✂️' : '📁';

            row.innerHTML = `
                <td>
                    <div class="address-value" title="${this.escapeHtml(address.address)}">
                        ${this.escapeHtml(address.address)}
                    </div>
                    <small class="address-type">
                        ${address.address.startsWith('BrowsePath://') ? 'Browse Path' : 'Node ID'}
                    </small>
                </td>
                <td>
                    <div class="topic-value">${this.escapeHtml(address.topic)}</div>
                </td>
                <td>
                    <div class="publish-mode">
                        ${publishModeIcon} ${address.publishMode}
                    </div>
                    <small class="publish-mode-desc">
                        ${address.publishMode === 'SEPARATE' ? 'Each node gets own topic' : 'All values on one topic'}
                    </small>
                </td>
                <td>
                    <div class="remove-path">
                        ${removePathIcon} ${address.removePath ? 'Yes' : 'No'}
                    </div>
                </td>
                <td>
                    <div class="action-buttons">
                        <button class="btn btn-sm btn-danger"
                                onclick="deviceDetailManager.deleteAddress('${this.escapeHtml(address.address)}')"
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

        const addressData = {
            address: document.getElementById('address-address').value.trim(),
            topic: document.getElementById('address-topic').value.trim(),
            publishMode: document.getElementById('address-publish-mode').value,
            removePath: document.getElementById('address-remove-path').checked
        };

        try {
            const mutation = `
                mutation AddOpcUaAddress($deviceName: String!, $input: OpcUaAddressInput!) {
                    opcUaDevice {
                        addAddress(deviceName: $deviceName, input: $input) {
                            success
                            errors
                            device {
                                name
                            }
                        }
                    }
                }
            `;

            const result = await this.client.query(mutation, {
                deviceName: this.deviceName,
                input: addressData
            });

            if (result.opcUaDevice.addAddress.success) {
                this.hideAddAddressModal();
                await this.loadDevice();
                this.showSuccess(`Address "${addressData.address}" added successfully`);
            } else {
                const errors = result.opcUaDevice.addAddress.errors || ['Unknown error'];
                this.showError('Failed to add address: ' + errors.join(', '));
            }

        } catch (error) {
            console.error('Error adding address:', error);
            this.showError('Failed to add address: ' + error.message);
        }
    }

    deleteAddress(address) {
        this.deleteAddressName = address;
        document.getElementById('delete-address-name').textContent = address;
        this.showConfirmDeleteAddressModal();
    }

    async confirmDeleteAddress() {
        if (!this.deleteAddressName) return;

        try {
            const mutation = `
                mutation DeleteOpcUaAddress($deviceName: String!, $address: String!) {
                    opcUaDevice {
                        deleteAddress(deviceName: $deviceName, address: $address) {
                            success
                            errors
                            device {
                                name
                            }
                        }
                    }
                }
            `;

            const result = await this.client.query(mutation, {
                deviceName: this.deviceName,
                address: this.deleteAddressName
            });

            if (result.opcUaDevice.deleteAddress.success) {
                this.hideConfirmDeleteAddressModal();
                await this.loadDevice();
                this.showSuccess(`Address "${this.deleteAddressName}" deleted successfully`);
            } else {
                const errors = result.opcUaDevice.deleteAddress.errors || ['Unknown error'];
                this.showError('Failed to delete address: ' + errors.join(', '));
            }

        } catch (error) {
            console.error('Error deleting address:', error);
            this.showError('Failed to delete address: ' + error.message);
        }

        this.deleteAddressName = null;
    }

    async saveDevice() {
        const form = document.getElementById('device-form');
        if (!form.checkValidity()) {
            form.reportValidity();
            return;
        }

        const deviceData = {
            name: document.getElementById('device-name').value.trim(),
            namespace: document.getElementById('device-namespace').value.trim(),
            nodeId: document.getElementById('device-node').value,
            enabled: document.getElementById('device-enabled').checked,
            config: {
                endpointUrl: document.getElementById('device-endpoint').value.trim(),
                updateEndpointUrl: document.getElementById('device-update-endpoint').checked,
                securityPolicy: document.getElementById('device-security').value,
                username: document.getElementById('device-username').value.trim() || null,
                password: document.getElementById('device-update-password').checked ?
                    document.getElementById('device-password').value || null : undefined,
                subscriptionSamplingInterval: parseFloat(document.getElementById('config-sampling').value),
                keepAliveFailuresAllowed: parseInt(document.getElementById('config-keepalive').value),
                reconnectDelay: parseInt(document.getElementById('config-reconnect').value),
                connectionTimeout: parseInt(document.getElementById('config-connection-timeout').value),
                requestTimeout: parseInt(document.getElementById('config-request-timeout').value),
                monitoringParameters: {
                    bufferSize: parseInt(document.getElementById('config-buffer-size').value),
                    samplingInterval: parseFloat(document.getElementById('config-monitoring-sampling').value),
                    discardOldest: document.getElementById('config-discard-oldest').checked
                },
                certificateConfig: {
                    securityDir: document.getElementById('cert-security-dir').value.trim(),
                    applicationName: document.getElementById('cert-application-name').value.trim(),
                    applicationUri: document.getElementById('cert-application-uri').value.trim(),
                    organization: document.getElementById('cert-organization').value.trim(),
                    organizationalUnit: document.getElementById('cert-organizational-unit').value.trim(),
                    localityName: document.getElementById('cert-locality').value.trim(),
                    countryCode: document.getElementById('cert-country').value.trim(),
                    createSelfSigned: document.getElementById('cert-create-self-signed').checked,
                    keystorePassword: document.getElementById('cert-update-keystore-password').checked ?
                        document.getElementById('cert-keystore-password').value || null : undefined,
                    validateServerCertificate: document.getElementById('cert-validate-server').checked,
                    autoAcceptServerCertificates: document.getElementById('cert-auto-accept').checked
                }
            }
        };

        if (this.isNew) {
            try {
                const mutation = `
                    mutation AddOpcUaDevice($input: OpcUaDeviceInput!) {
                        opcUaDevice {
                            add(input: $input) {
                                success
                                errors
                                device { name }
                            }
                        }
                    }
                `;
                const result = await this.client.query(mutation, { input: deviceData });
                if (result.opcUaDevice.add.success) {
                    this.showSuccess(`Device "${deviceData.name}" created successfully`);
                    setTimeout(() => { window.location.href = '/pages/opcua-devices.html'; }, 800);
                } else {
                    const errors = result.opcUaDevice.add.errors || ['Unknown error'];
                    this.showError('Failed to create device: ' + errors.join(', '));
                }
            } catch (error) {
                console.error('Error creating device:', error);
                this.showError('Failed to create device: ' + error.message);
            }
            return;
        }

        try {
            const mutation = `
                mutation UpdateOpcUaDevice($name: String!, $input: OpcUaDeviceInput!) {
                    opcUaDevice {
                        update(name: $name, input: $input) {
                            success
                            errors
                            device {
                                name
                            }
                        }
                    }
                }
            `;

            const result = await this.client.query(mutation, {
                name: this.deviceName,
                input: deviceData
            });

            if (result.opcUaDevice.update.success) {
                await this.loadDevice();
                this.showSuccess(`Device "${this.deviceName}" updated successfully`);
            } else {
                const errors = result.opcUaDevice.update.errors || ['Unknown error'];
                this.showError('Failed to update device: ' + errors.join(', '));
            }

        } catch (error) {
            console.error('Error updating device:', error);
            this.showError('Failed to update device: ' + error.message);
        }
    }

    // UI Helper Methods
    showAddAddressModal() {
        document.getElementById('add-address-modal').style.display = 'flex';
        // Reset form
        document.getElementById('add-address-form').reset();
        document.getElementById('address-remove-path').checked = true;
        document.getElementById('address-publish-mode').value = 'SEPARATE';
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
        window.location.href = '/pages/opcua-devices.html';
    }
}

// Global functions for onclick handlers
function showAddAddressModal() {
    deviceDetailManager.showAddAddressModal();
}

function hideAddAddressModal() {
    deviceDetailManager.hideAddAddressModal();
}

function addAddress() {
    deviceDetailManager.addAddress();
}

function saveDevice() {
    deviceDetailManager.saveDevice();
}

function hideConfirmDeleteAddressModal() {
    deviceDetailManager.hideConfirmDeleteAddressModal();
}

function confirmDeleteAddress() {
    deviceDetailManager.confirmDeleteAddress();
}

function goBack() {
    deviceDetailManager.goBack();
}

// Initialize when DOM is loaded
let deviceDetailManager;
document.addEventListener('DOMContentLoaded', () => {
    deviceDetailManager = new OpcUaDeviceDetailManager();
});

// Handle modal clicks (close when clicking outside)
document.addEventListener('click', (e) => {
    if (e.target.classList.contains('modal')) {
        if (e.target.id === 'add-address-modal') {
            deviceDetailManager.hideAddAddressModal();
        } else if (e.target.id === 'confirm-delete-address-modal') {
            deviceDetailManager.hideConfirmDeleteAddressModal();
        }
    }
});