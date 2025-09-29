// OPC UA Devices Management JavaScript

class OpcUaDeviceManager {
    constructor() {
        this.client = new GraphQLDashboardClient('/graphql');
        this.devices = [];
        this.clusterNodes = [];
        this.deleteDeviceName = null;
        this.init();
    }

    async init() {
        console.log('Initializing OPC UA Device Manager...');

        // Since user management is disabled, skip authentication check
        console.log('Initializing without authentication check (user management disabled)');

        // UI setup is now handled by sidebar.js

        // Load initial data
        await this.loadClusterNodes();
        await this.loadDevices();

        // Set up periodic refresh
        setInterval(() => this.loadDevices(), 30000); // Refresh every 30 seconds
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

            // Populate node selector in the add device form
            const nodeSelect = document.getElementById('device-node');
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

    async loadDevices() {
        this.showLoading(true);
        this.hideError();

        try {
            const query = `
                query GetOpcUaDevices {
                    opcUaDevices {
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
                            addresses {
                                address
                                topic
                                publishMode
                                removePath
                            }
                        }
                    }
                }
            `;

            const result = await this.client.query(query);
            console.log('Load devices result:', result);

            if (!result) {
                throw new Error('Invalid response structure: missing result');
            }

            if (!result.opcUaDevices) {
                throw new Error('Invalid response structure: missing opcUaDevices property');
            }

            this.devices = result.opcUaDevices || [];

            this.updateMetrics();
            this.renderDevicesTable();

        } catch (error) {
            console.error('Error loading devices:', error);
            console.error('Error details:', error.stack);
            this.showError('Failed to load OPC UA devices: ' + error.message);
        } finally {
            this.showLoading(false);
        }
    }

    updateMetrics() {
        const totalDevices = this.devices.length;
        const enabledDevices = this.devices.filter(d => d.enabled).length;
        const currentNodeDevices = this.devices.filter(d => d.isOnCurrentNode).length;
        const totalAddresses = this.devices.reduce((sum, d) => sum + d.config.addresses.length, 0);

        document.getElementById('total-devices').textContent = totalDevices;
        document.getElementById('enabled-devices').textContent = enabledDevices;
        document.getElementById('current-node-devices').textContent = currentNodeDevices;
        document.getElementById('total-addresses').textContent = totalAddresses;
    }

    renderDevicesTable() {
        const tbody = document.getElementById('devices-table-body');
        if (!tbody) return;

        tbody.innerHTML = '';

        if (this.devices.length === 0) {
            tbody.innerHTML = `
                <tr>
                    <td colspan="7" class="no-data">
                        No OPC UA devices configured. Click "Add Device" to get started.
                    </td>
                </tr>
            `;
            return;
        }

        this.devices.forEach(device => {
            const row = document.createElement('tr');

            const statusClass = device.enabled ? 'status-enabled' : 'status-disabled';
            const statusText = device.enabled ? 'Enabled' : 'Disabled';
            const nodeIndicator = device.isOnCurrentNode ? '📍 ' : '';

            row.innerHTML = `
                <td>
                    <div class="device-name">${this.escapeHtml(device.name)}</div>
                    <small class="device-namespace">${this.escapeHtml(device.namespace)}</small>
                </td>
                <td>
                    <div class="endpoint-url" title="${this.escapeHtml(device.config.endpointUrl)}">
                        ${this.escapeHtml(device.config.endpointUrl)}
                    </div>
                    <small class="security-policy">Security: ${device.config.securityPolicy}</small>
                </td>
                <td>${this.escapeHtml(device.namespace)}</td>
                <td>
                    <div class="node-assignment">
                        ${nodeIndicator}${this.escapeHtml(device.nodeId)}
                    </div>
                </td>
                <td>
                    <span class="status-badge ${statusClass}">${statusText}</span>
                </td>
                <td>
                    <div class="address-count">
                        ${device.config.addresses.length} addresses
                    </div>
                </td>
                <td>
                    <div class="action-buttons">
                        <button class="btn-action btn-view" onclick="opcuaManager.viewDevice('${device.name}')" title="Edit Device">
                            <svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor">
                                <path d="M3 17.25V21h3.75L17.81 9.94l-3.75-3.75L3 17.25zM20.71 7.04c.39-.39.39-1.02 0-1.41l-2.34-2.34c-.39-.39-1.02-.39-1.41 0l-1.83 1.83 3.75 3.75 1.83-1.83z"/>
                            </svg>
                        </button>
                        <button class="btn-action ${device.enabled ? 'btn-pause' : 'btn-play'}"
                                onclick="opcuaManager.toggleDevice('${device.name}', ${!device.enabled})"
                                title="${device.enabled ? 'Disable Device' : 'Enable Device'}">
                            ${device.enabled ?
                                '<svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor"><path d="M6 19h4V5H6v14zm8-14v14h4V5h-4z"/></svg>' :
                                '<svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor"><path d="M8 5v14l11-7z"/></svg>'
                            }
                        </button>
                        <button class="btn-action btn-delete" onclick="opcuaManager.deleteDevice('${device.name}')" title="Delete Device">
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

    async addDevice() {
        const form = document.getElementById('add-device-form');
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
                password: document.getElementById('device-password').value || null,
                subscriptionSamplingInterval: parseFloat(document.getElementById('subscription-sampling').value),
                keepAliveFailuresAllowed: parseInt(document.getElementById('keep-alive-failures').value),
                reconnectDelay: parseInt(document.getElementById('reconnect-delay').value),
                connectionTimeout: parseInt(document.getElementById('connection-timeout').value),
                requestTimeout: parseInt(document.getElementById('request-timeout').value),
                monitoringParameters: {
                    bufferSize: parseInt(document.getElementById('monitoring-buffer-size').value),
                    samplingInterval: parseFloat(document.getElementById('monitoring-sampling').value),
                    discardOldest: document.getElementById('monitoring-discard-oldest').checked
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
                    keystorePassword: document.getElementById('cert-keystore-password').value || null,
                    validateServerCertificate: document.getElementById('cert-validate-server-certificate').checked,
                    autoAcceptServerCertificates: document.getElementById('cert-auto-accept-server-certificates').checked
                }
            }
        };

        try {
            const mutation = `
                mutation AddOpcUaDevice($input: OpcUaDeviceInput!) {
                    addOpcUaDevice(input: $input) {
                        success
                        errors
                        device {
                            name
                            enabled
                        }
                    }
                }
            `;

            const result = await this.client.query(mutation, { input: deviceData });

            if (result.addOpcUaDevice.success) {
                this.hideAddDeviceModal();
                await this.loadDevices();
                this.showSuccess(`Device "${deviceData.name}" added successfully`);
            } else {
                const errors = result.addOpcUaDevice.errors || ['Unknown error'];
                this.showError('Failed to add device: ' + errors.join(', '));
            }

        } catch (error) {
            console.error('Error adding device:', error);
            this.showError('Failed to add device: ' + error.message);
        }
    }

    async toggleDevice(deviceName, enabled) {
        try {
            const mutation = `
                mutation ToggleOpcUaDevice($name: String!, $enabled: Boolean!) {
                    toggleOpcUaDevice(name: $name, enabled: $enabled) {
                        success
                        errors
                        device {
                            name
                            enabled
                        }
                    }
                }
            `;

            const result = await this.client.query(mutation, { name: deviceName, enabled });

            if (result.toggleOpcUaDevice.success) {
                await this.loadDevices();
                this.showSuccess(`Device "${deviceName}" ${enabled ? 'enabled' : 'disabled'} successfully`);
            } else {
                const errors = result.toggleOpcUaDevice.errors || ['Unknown error'];
                this.showError('Failed to toggle device: ' + errors.join(', '));
            }

        } catch (error) {
            console.error('Error toggling device:', error);
            this.showError('Failed to toggle device: ' + error.message);
        }
    }

    deleteDevice(deviceName) {
        this.deleteDeviceName = deviceName;
        document.getElementById('delete-device-name').textContent = deviceName;
        this.showConfirmDeleteModal();
    }

    async confirmDeleteDevice() {
        if (!this.deleteDeviceName) return;

        try {
            const mutation = `
                mutation DeleteOpcUaDevice($name: String!) {
                    deleteOpcUaDevice(name: $name)
                }
            `;

            const result = await this.client.query(mutation, { name: this.deleteDeviceName });

            if (result.deleteOpcUaDevice) {
                this.hideConfirmDeleteModal();
                await this.loadDevices();
                this.showSuccess(`Device "${this.deleteDeviceName}" deleted successfully`);
            } else {
                this.showError('Failed to delete device');
            }

        } catch (error) {
            console.error('Error deleting device:', error);
            this.showError('Failed to delete device: ' + error.message);
        }

        this.deleteDeviceName = null;
    }

    viewDevice(deviceName) {
        // Navigate to device detail page
        window.location.href = `/pages/opcua-device-detail.html?device=${encodeURIComponent(deviceName)}`;
    }

    // UI Helper Methods
    showAddDeviceModal() {
        document.getElementById('add-device-modal').style.display = 'flex';
        // Reset form
        document.getElementById('add-device-form').reset();
        document.getElementById('device-enabled').checked = true;
        document.getElementById('device-update-endpoint').checked = true;
    }

    hideAddDeviceModal() {
        document.getElementById('add-device-modal').style.display = 'none';
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

    async refreshDevices() {
        await this.loadDevices();
    }
}

// Global functions for onclick handlers
function showAddDeviceModal() {
    opcuaManager.showAddDeviceModal();
}

function hideAddDeviceModal() {
    opcuaManager.hideAddDeviceModal();
}

function addDevice() {
    opcuaManager.addDevice();
}

function hideConfirmDeleteModal() {
    opcuaManager.hideConfirmDeleteModal();
}

function confirmDeleteDevice() {
    opcuaManager.confirmDeleteDevice();
}

function refreshDevices() {
    opcuaManager.refreshDevices();
}

// Initialize when DOM is loaded
let opcuaManager;
document.addEventListener('DOMContentLoaded', () => {
    opcuaManager = new OpcUaDeviceManager();
});

// Handle modal clicks (close when clicking outside)
document.addEventListener('click', (e) => {
    if (e.target.classList.contains('modal')) {
        if (e.target.id === 'add-device-modal') {
            opcuaManager.hideAddDeviceModal();
        } else if (e.target.id === 'confirm-delete-modal') {
            opcuaManager.hideConfirmDeleteModal();
        }
    }
});