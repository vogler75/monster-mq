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
            tbody.innerHTML = `
                <tr>
                    <td colspan="8" class="no-data">
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
                    <span style="color: #06B6D4;">${(device.metrics && device.metrics.length>0 ? Math.round(device.metrics[0].messagesIn) : 0)}</span> /
                    <span style="color: #9333EA;">${(device.metrics && device.metrics.length>0 ? Math.round(device.metrics[0].messagesOut) : 0)}</span>
                </td>
                <td>
                    <div class="action-buttons">
                        <ix-icon-button icon="highlight" variant="primary" ghost size="16" title="Edit Device" onclick="opcuaManager.viewDevice('${device.name}')"></ix-icon-button>
                        <ix-icon-button icon="${device.enabled ? 'pause' : 'play'}" variant="primary" ghost size="16" title="${device.enabled ? 'Disable Device' : 'Enable Device'}" onclick="opcuaManager.toggleDevice('${device.name}', ${!device.enabled})"></ix-icon-button>
                        <ix-icon-button icon="trashcan" variant="primary" ghost size="16" class="btn-delete" title="Delete Device" onclick="opcuaManager.deleteDevice('${device.name}')"></ix-icon-button>
                    </div>
                </td>
            `;

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

            if (result.opcUaDevice.toggle.success) {
                await this.loadDevices();
                this.showSuccess(`Device "${deviceName}" ${enabled ? 'enabled' : 'disabled'} successfully`);
            } else {
                const errors = result.opcUaDevice.toggle.errors || ['Unknown error'];
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
                    opcUaDevice {
                        delete(name: $name)
                    }
                }
            `;

            const result = await this.client.query(mutation, { name: this.deleteDeviceName });

            if (result.opcUaDevice.delete) {
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
        window.spaLocation.href = `/pages/opcua-device-detail.html?device=${encodeURIComponent(deviceName)}`;
    }

    // UI Helper Methods
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
        // Also update the inline error div if present
        var errorDiv = document.getElementById('error-message');
        if (errorDiv) {
            var errorText = errorDiv.querySelector('.error-text');
            if (errorText) errorText.textContent = message;
            errorDiv.style.display = 'flex';
        }

        // Show a fixed-position toast so the error is always visible
        var existing = document.getElementById('error-toast');
        if (existing) existing.remove();

        var toast = document.createElement('div');
        toast.id = 'error-toast';
        toast.style.cssText = 'position:fixed;top:20px;left:50%;transform:translateX(-50%);background:var(--monster-red,#EF4444);color:#fff;padding:14px 24px;border-radius:8px;box-shadow:0 4px 12px rgba(0,0,0,0.4);z-index:10000;font-size:0.9rem;max-width:600px;display:flex;align-items:center;gap:10px;animation:slideDown 0.3s ease-out;';
        toast.innerHTML = '<span style="font-size:1.2rem;">&#9888;</span><span>' + message + '</span><button onclick="this.parentElement.remove()" style="background:none;border:none;color:#fff;cursor:pointer;margin-left:auto;font-size:1.1rem;line-height:1;padding:0 4px;">&times;</button>';

        // Add animation
        if (!document.getElementById('error-toast-style')) {
            var style = document.createElement('style');
            style.id = 'error-toast-style';
            style.textContent = '@keyframes slideDown{from{transform:translateX(-50%) translateY(-100%);opacity:0;}to{transform:translateX(-50%) translateY(0);opacity:1;}}';
            document.head.appendChild(style);
        }

        document.body.appendChild(toast);

        setTimeout(function() {
            if (toast.parentElement) toast.remove();
            if (errorDiv) errorDiv.style.display = 'none';
        }, 8000);
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
        if (e.target.id === 'confirm-delete-modal') {
            opcuaManager.hideConfirmDeleteModal();
        }
    }
});