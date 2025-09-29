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
        console.log('Initializing OPC UA Device Detail Manager...');

        // Check if authentication is required by testing a simple query
        try {
            const testQuery = `query { clusterNodes { nodeId } }`;
            await this.client.query(testQuery);
            console.log('Authentication check passed or not required');
        } catch (error) {
            // Only redirect to login if we get an authentication error
            if (error.message && (error.message.includes('Unauthorized') || error.message.includes('Authentication'))) {
                window.location.href = '/pages/login.html';
                return;
            }
            // For other errors, continue - authentication might be disabled
            console.log('Authentication appears to be disabled, continuing...');
        }

        // UI setup is now handled by sidebar.js

        // Get device name from URL parameters
        const urlParams = new URLSearchParams(window.location.search);
        this.deviceName = urlParams.get('device');

        if (!this.deviceName) {
            this.showError('No device specified');
            return;
        }

        // Load data
        await this.loadClusterNodes();
        await this.loadDevice();
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

            // Populate node selector in edit form
            const nodeSelect = document.getElementById('edit-device-node');
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

    async loadDevice() {
        this.showLoading(true);
        this.hideError();

        try {
            const query = `
                query GetOpcUaDevice($name: String!) {
                    opcUaDevice(name: $name) {
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
            this.device = result.opcUaDevice;

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
        document.getElementById('device-title').textContent = `OPC UA Device: ${this.device.name}`;
        document.getElementById('device-subtitle').textContent = `${this.device.namespace} - ${this.device.config.endpointUrl}`;

        // Device status
        const statusBadge = document.getElementById('device-status-badge');
        const statusClass = this.device.enabled ? 'status-enabled' : 'status-disabled';
        statusBadge.className = `status-badge ${statusClass}`;
        statusBadge.textContent = this.device.enabled ? 'Enabled' : 'Disabled';

        // Device information
        document.getElementById('device-name').textContent = this.device.name;
        document.getElementById('device-namespace').textContent = this.device.namespace;
        document.getElementById('device-endpoint').textContent = this.device.config.endpointUrl;
        document.getElementById('device-security').textContent = this.device.config.securityPolicy;
        document.getElementById('device-node').textContent =
            (this.device.isOnCurrentNode ? '📍 ' : '') + this.device.nodeId;
        document.getElementById('device-update-endpoint').textContent =
            this.device.config.updateEndpointUrl ? 'Yes' : 'No';
        document.getElementById('device-username').textContent =
            this.device.config.username || 'Not configured';
        document.getElementById('device-created').textContent =
            this.formatDateTime(this.device.createdAt);

        // Connection configuration
        document.getElementById('config-sampling').textContent =
            this.device.config.subscriptionSamplingInterval + ' ms';
        document.getElementById('config-keepalive').textContent =
            this.device.config.keepAliveFailuresAllowed;
        document.getElementById('config-reconnect').textContent =
            this.device.config.reconnectDelay + ' ms';
        document.getElementById('config-connection-timeout').textContent =
            this.device.config.connectionTimeout + ' ms';
        document.getElementById('config-request-timeout').textContent =
            this.device.config.requestTimeout + ' ms';
        document.getElementById('config-buffer-size').textContent =
            this.device.config.monitoringParameters.bufferSize;
        document.getElementById('config-monitoring-sampling').textContent =
            this.device.config.monitoringParameters.samplingInterval + ' ms';
        document.getElementById('config-discard-oldest').textContent =
            this.device.config.monitoringParameters.discardOldest ? 'Yes' : 'No';

        // Certificate configuration
        const certConfig = this.device.config.certificateConfig;
        document.getElementById('cert-security-dir').textContent = certConfig.securityDir;
        document.getElementById('cert-application-name').textContent = certConfig.applicationName;
        document.getElementById('cert-application-uri').textContent = certConfig.applicationUri;
        document.getElementById('cert-organization').textContent = certConfig.organization;
        document.getElementById('cert-create-self-signed').textContent = certConfig.createSelfSigned ? 'Yes' : 'No';
        document.getElementById('cert-validate-server').textContent = certConfig.validateServerCertificate ? 'Yes' : 'No';
        document.getElementById('cert-auto-accept').textContent = certConfig.autoAcceptServerCertificates ? 'Yes' : 'No';

        // Render addresses
        this.renderAddresses();

        // Show content
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

        addressesTable.style.display = 'table';
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
                    addOpcUaAddress(deviceName: $deviceName, input: $input) {
                        success
                        errors
                        device {
                            name
                        }
                    }
                }
            `;

            const result = await this.client.query(mutation, {
                deviceName: this.deviceName,
                input: addressData
            });

            if (result.addOpcUaAddress.success) {
                this.hideAddAddressModal();
                await this.loadDevice();
                this.showSuccess(`Address "${addressData.address}" added successfully`);
            } else {
                const errors = result.addOpcUaAddress.errors || ['Unknown error'];
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
                    deleteOpcUaAddress(deviceName: $deviceName, address: $address) {
                        success
                        errors
                        device {
                            name
                        }
                    }
                }
            `;

            const result = await this.client.query(mutation, {
                deviceName: this.deviceName,
                address: this.deleteAddressName
            });

            if (result.deleteOpcUaAddress.success) {
                this.hideConfirmDeleteAddressModal();
                await this.loadDevice();
                this.showSuccess(`Address "${this.deleteAddressName}" deleted successfully`);
            } else {
                const errors = result.deleteOpcUaAddress.errors || ['Unknown error'];
                this.showError('Failed to delete address: ' + errors.join(', '));
            }

        } catch (error) {
            console.error('Error deleting address:', error);
            this.showError('Failed to delete address: ' + error.message);
        }

        this.deleteAddressName = null;
    }

    editDevice() {
        if (!this.device) return;

        // Populate edit form
        document.getElementById('edit-device-name').value = this.device.name;
        document.getElementById('edit-device-namespace').value = this.device.namespace;
        document.getElementById('edit-device-endpoint').value = this.device.config.endpointUrl;
        document.getElementById('edit-device-node').value = this.device.nodeId;
        document.getElementById('edit-device-security').value = this.device.config.securityPolicy;
        document.getElementById('edit-device-username').value = this.device.config.username || '';
        document.getElementById('edit-device-password').value = '';
        document.getElementById('edit-device-enabled').checked = this.device.enabled;
        document.getElementById('edit-device-update-endpoint').checked = this.device.config.updateEndpointUrl;

        // Populate certificate configuration
        const certConfig = this.device.config.certificateConfig;
        document.getElementById('edit-cert-security-dir').value = certConfig.securityDir;
        document.getElementById('edit-cert-application-name').value = certConfig.applicationName;
        document.getElementById('edit-cert-application-uri').value = certConfig.applicationUri;
        document.getElementById('edit-cert-organization').value = certConfig.organization;
        document.getElementById('edit-cert-organizational-unit').value = certConfig.organizationalUnit;
        document.getElementById('edit-cert-locality').value = certConfig.localityName;
        document.getElementById('edit-cert-country').value = certConfig.countryCode;
        document.getElementById('edit-cert-create-self-signed').checked = certConfig.createSelfSigned;
        document.getElementById('edit-cert-validate-server-certificate').checked = certConfig.validateServerCertificate;
        document.getElementById('edit-cert-auto-accept-server-certificates').checked = certConfig.autoAcceptServerCertificates;
        document.getElementById('edit-cert-keystore-password').value = '';

        // Populate connection settings
        document.getElementById('edit-subscription-sampling').value = this.device.config.subscriptionSamplingInterval;
        document.getElementById('edit-keep-alive-failures').value = this.device.config.keepAliveFailuresAllowed;
        document.getElementById('edit-reconnect-delay').value = this.device.config.reconnectDelay;
        document.getElementById('edit-connection-timeout').value = this.device.config.connectionTimeout;
        document.getElementById('edit-request-timeout').value = this.device.config.requestTimeout;
        document.getElementById('edit-monitoring-buffer-size').value = this.device.config.monitoringParameters.bufferSize;
        document.getElementById('edit-monitoring-sampling').value = this.device.config.monitoringParameters.samplingInterval;
        document.getElementById('edit-monitoring-discard-oldest').checked = this.device.config.monitoringParameters.discardOldest;

        // Reset password update checkboxes and hide password fields
        document.getElementById('edit-device-update-password').checked = false;
        document.getElementById('password-field-group').style.display = 'none';
        document.getElementById('edit-cert-update-keystore-password').checked = false;
        document.getElementById('keystore-password-field-group').style.display = 'none';

        this.showEditDeviceModal();
    }

    async updateDevice() {
        const form = document.getElementById('edit-device-form');
        if (!form.checkValidity()) {
            form.reportValidity();
            return;
        }

        const deviceData = {
            name: document.getElementById('edit-device-name').value.trim(),
            namespace: document.getElementById('edit-device-namespace').value.trim(),
            nodeId: document.getElementById('edit-device-node').value,
            enabled: document.getElementById('edit-device-enabled').checked,
            config: {
                endpointUrl: document.getElementById('edit-device-endpoint').value.trim(),
                updateEndpointUrl: document.getElementById('edit-device-update-endpoint').checked,
                securityPolicy: document.getElementById('edit-device-security').value,
                username: document.getElementById('edit-device-username').value.trim() || null,
                password: document.getElementById('edit-device-update-password').checked ?
                    document.getElementById('edit-device-password').value || null : undefined,
                subscriptionSamplingInterval: parseFloat(document.getElementById('edit-subscription-sampling').value),
                keepAliveFailuresAllowed: parseInt(document.getElementById('edit-keep-alive-failures').value),
                reconnectDelay: parseInt(document.getElementById('edit-reconnect-delay').value),
                connectionTimeout: parseInt(document.getElementById('edit-connection-timeout').value),
                requestTimeout: parseInt(document.getElementById('edit-request-timeout').value),
                monitoringParameters: {
                    bufferSize: parseInt(document.getElementById('edit-monitoring-buffer-size').value),
                    samplingInterval: parseFloat(document.getElementById('edit-monitoring-sampling').value),
                    discardOldest: document.getElementById('edit-monitoring-discard-oldest').checked
                },
                certificateConfig: {
                    securityDir: document.getElementById('edit-cert-security-dir').value.trim(),
                    applicationName: document.getElementById('edit-cert-application-name').value.trim(),
                    applicationUri: document.getElementById('edit-cert-application-uri').value.trim(),
                    organization: document.getElementById('edit-cert-organization').value.trim(),
                    organizationalUnit: document.getElementById('edit-cert-organizational-unit').value.trim(),
                    localityName: document.getElementById('edit-cert-locality').value.trim(),
                    countryCode: document.getElementById('edit-cert-country').value.trim(),
                    createSelfSigned: document.getElementById('edit-cert-create-self-signed').checked,
                    keystorePassword: document.getElementById('edit-cert-update-keystore-password').checked ?
                        document.getElementById('edit-cert-keystore-password').value || null : undefined,
                    validateServerCertificate: document.getElementById('edit-cert-validate-server-certificate').checked,
                    autoAcceptServerCertificates: document.getElementById('edit-cert-auto-accept-server-certificates').checked
                }
            }
        };

        try {
            const mutation = `
                mutation UpdateOpcUaDevice($name: String!, $input: OpcUaDeviceInput!) {
                    updateOpcUaDevice(name: $name, input: $input) {
                        success
                        errors
                        device {
                            name
                        }
                    }
                }
            `;

            const result = await this.client.query(mutation, {
                name: this.deviceName,
                input: deviceData
            });

            if (result.updateOpcUaDevice.success) {
                this.hideEditDeviceModal();
                await this.loadDevice();
                this.showSuccess(`Device "${this.deviceName}" updated successfully`);
            } else {
                const errors = result.updateOpcUaDevice.errors || ['Unknown error'];
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

    showEditDeviceModal() {
        document.getElementById('edit-device-modal').style.display = 'flex';
    }

    hideEditDeviceModal() {
        document.getElementById('edit-device-modal').style.display = 'none';
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

function editDevice() {
    deviceDetailManager.editDevice();
}

function hideEditDeviceModal() {
    deviceDetailManager.hideEditDeviceModal();
}

function updateDevice() {
    deviceDetailManager.updateDevice();
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
        } else if (e.target.id === 'edit-device-modal') {
            deviceDetailManager.hideEditDeviceModal();
        } else if (e.target.id === 'confirm-delete-address-modal') {
            deviceDetailManager.hideConfirmDeleteAddressModal();
        }
    }
});