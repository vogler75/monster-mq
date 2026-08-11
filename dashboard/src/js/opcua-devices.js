// OPC UA Devices Management JavaScript

class OpcUaDeviceManager {
    constructor() {
        this.client = new GraphQLDashboardClient();
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
        await this.loadDevices();

        // Set up periodic refresh
        setInterval(() => this.loadDevices(), 30000); // Refresh every 30 seconds
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
                                writable
                                publishRaw
                            }
                        }
                        metrics {
                            messagesIn
                            messagesOut
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
            tbody.innerHTML = ui.emptyRow(8, 'No OPC UA devices configured',
                'Use “Add Device” to get started.');
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
                    <span style="color: #06B6D4;">${(device.metrics && device.metrics.length>0 ? Math.round(device.metrics[0].messagesIn) : 0)}</span> /
                    <span style="color: #9333EA;">${(device.metrics && device.metrics.length>0 ? Math.round(device.metrics[0].messagesOut) : 0)}</span>
                </td>
                <td>
                    <div class="action-buttons">
                        <ix-icon-button icon="highlight" variant="subtle-tertiary" size="24" title="Edit Device" class="btn-edit"></ix-icon-button>
                        <ix-icon-button icon="${device.enabled ? 'pause' : 'play'}" variant="subtle-tertiary" size="24" title="${device.enabled ? 'Disable Device' : 'Enable Device'}" class="btn-toggle"></ix-icon-button>
                        <ix-icon-button icon="trashcan" variant="subtle-tertiary" size="24" class="btn-delete" title="Delete Device"></ix-icon-button>
                    </div>
                </td>
            `;

            const editBtn = row.querySelector('.btn-edit');
            if (editBtn) editBtn.addEventListener('click', (e) => { e.stopPropagation(); this.viewDevice(device.name); });
            const toggleBtn = row.querySelector('.btn-toggle');
            if (toggleBtn) toggleBtn.addEventListener('click', (e) => { e.stopPropagation(); this.toggleDevice(device.name, !device.enabled); });
            const deleteBtn = row.querySelector('.btn-delete');
            if (deleteBtn) deleteBtn.addEventListener('click', (e) => { e.stopPropagation(); this.deleteDevice(device.name); });

            tbody.appendChild(row);
        });
    }

    async toggleDevice(deviceName, enabled) {
        try {
            const mutation = `
                mutation ToggleOpcUaDevice($name: String!, $enabled: Boolean!) {
                    opcUaDevice {
                        toggle(name: $name, enabled: $enabled) {
                            success
                            errors
                            device {
                                name
                                enabled
                            }
                        }
                    }
                }
            `;

            const result = await this.client.query(mutation, { name: deviceName, enabled });

            if (result && result.opcUaDevice && result.opcUaDevice.toggle && result.opcUaDevice.toggle.success) {
                await this.loadDevices();
                this.showSuccess(`Device "${deviceName}" ${enabled ? 'enabled' : 'disabled'} successfully`);
            } else {
                const errors = (result && result.opcUaDevice && result.opcUaDevice.toggle && result.opcUaDevice.toggle.errors) || ['Unknown error'];
                this.showError('Failed to toggle device: ' + errors.join(', '));
            }

        } catch (error) {
            console.error('Error toggling device:', error);
            this.showError('Failed to toggle device: ' + error.message);
        }
    }

    async deleteDevice(deviceName) {
        this.deleteDeviceName = deviceName;
        if (await ui.confirmDelete(deviceName, { title: 'Delete OPC UA device' })) {
            await this.confirmDeleteDevice();
        } else {
            this.deleteDeviceName = null;
        }
    }

    async confirmDeleteDevice() {
        if (!this.deleteDeviceName) return;

        try {
            const mutation = `
                mutation DeleteOpcUaDevice($name: String!) {
                    opcUaDevice {
                        delete(name: $name)
                    }
                }
            `;

            const result = await this.client.query(mutation, { name: this.deleteDeviceName });

            if (result && result.opcUaDevice && result.opcUaDevice.delete) {
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
        window.spaLocation.href = `/pages/opcua-device-detail.html?device=${encodeURIComponent(deviceName)}`;
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

    escapeAttr(text) {
        if (!text) return '';
        return text.replace(/&/g, '&amp;').replace(/"/g, '&quot;').replace(/'/g, '&#39;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
    }

    async refreshDevices() {
        await this.loadDevices();
    }
}

// Global functions for onclick handlers
var opcuaManager;

function confirmDeleteDevice() {
    if (window.opcuaManager) window.opcuaManager.confirmDeleteDevice();
}

function refreshDevices() {
    if (window.opcuaManager) window.opcuaManager.refreshDevices();
}

// Initialize when DOM is loaded
document.addEventListener('DOMContentLoaded', () => {
    opcuaManager = new OpcUaDeviceManager();
    window.opcuaManager = opcuaManager;
});

// Handle modal clicks (close when clicking outside)
document.addEventListener('click', (e) => {
    if (e.target.classList.contains('modal')) {
    }
});
