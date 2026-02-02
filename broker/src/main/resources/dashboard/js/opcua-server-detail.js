// OPC UA Server Detail Management JavaScript

class OpcUaServerDetailManager {
    constructor() {
        this.client = new GraphQLDashboardClient('/graphql');
        this.serverName = null; // URL param (original name for updates)
        this.serverData = null; // Loaded opcUaServer object
        this.clusterNodes = [];
        this.init();
    }

    async init() {
        const urlParams = new URLSearchParams(window.location.search);
        this.serverName = urlParams.get('server');
        if (!this.serverName) {
            this.showError('No OPC UA server specified');
            return;
        }
        await this.loadClusterNodes();
        await this.loadServerData();
    }

    async loadClusterNodes() {
        try {
            const query = `query GetBrokers { brokers { nodeId isCurrent } }`;
            const result = await this.client.query(query);
            this.clusterNodes = result.brokers || [];
            const nodeSelect = document.getElementById('server-node');
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

    async loadServerData() {
        this.showLoading(true);
        this.hideError();
        try {
            const query = `
                query GetOpcUaServers($name: String!) {
                    opcUaServers(name: $name) {
                        name
                        namespace
                        nodeId
                        enabled
                        port
                        path
                        hostname
                        bindAddress
                        namespaceUri
                        updateInterval
                        bufferSize
                        createdAt
                        updatedAt
                        addresses {
                            mqttTopic
                            dataType
                            accessLevel
                            browseName
                            displayName
                            description
                            unit
                        }
                        status {
                            status
                            serverName
                            nodeId
                            port
                            activeConnections
                            nodeCount
                        }
                    }
                }
            `;
            const result = await this.client.query(query, { name: this.serverName });
            if (!result.opcUaServers || result.opcUaServers.length === 0) {
                throw new Error('OPC UA server not found');
            }
            this.serverData = result.opcUaServers[0];
            this.renderServerInfo();
            this.renderAddresses();
            this.renderStatus();
        } catch (e) {
            console.error('Error loading OPC UA server', e);
            this.showError('Failed to load OPC UA server: ' + e.message);
        } finally {
            this.showLoading(false);
        }
    }

    renderServerInfo() {
        if (!this.serverData) return;
        const d = this.serverData;

        // Update page title and subtitle
        document.getElementById('page-title').textContent = `OPC UA Server: ${d.name}`;
        document.getElementById('page-subtitle').textContent = `${d.namespace} - Port ${d.port}`;

        // Populate form fields
        document.getElementById('server-name').value = d.name;
        document.getElementById('server-name').disabled = true; // Can't change name in edit mode
        document.getElementById('server-namespace').value = d.namespace;
        document.getElementById('server-node').value = d.nodeId;
        document.getElementById('server-port').value = d.port;
        document.getElementById('server-path').value = d.path || '';
        document.getElementById('server-hostname').value = d.hostname || '';
        document.getElementById('server-bind-address').value = d.bindAddress || '';
        document.getElementById('server-namespace-uri').value = d.namespaceUri || '';
        document.getElementById('server-update-interval').value = d.updateInterval;
        document.getElementById('server-buffer-size').value = d.bufferSize;
        document.getElementById('server-enabled').checked = d.enabled;

        // Status badge
        const statusBadge = document.getElementById('server-status');
        if (d.enabled) {
            statusBadge.className = 'status-badge status-enabled';
            statusBadge.textContent = 'ENABLED';
        } else {
            statusBadge.className = 'status-badge status-disabled';
            statusBadge.textContent = 'DISABLED';
        }

        // Toggle button
        const toggleBtn = document.getElementById('toggle-server-btn');
        if (toggleBtn) {
            toggleBtn.textContent = d.enabled ? 'Stop Server' : 'Start Server';
            toggleBtn.className = d.enabled ? 'btn btn-warning' : 'btn btn-success';
        }

        // Timestamps (read-only)
        this.setText('server-created-at', new Date(d.createdAt).toLocaleString());
        this.setText('server-updated-at', new Date(d.updatedAt).toLocaleString());

        // Show content
        document.getElementById('server-content').style.display = 'block';
    }

    renderAddresses() {
        if (!this.serverData || !this.serverData.addresses) return;
        const addresses = this.serverData.addresses;
        const tbody = document.getElementById('addresses-table-body');
        if (!tbody) return;

        tbody.innerHTML = '';
        if (addresses.length === 0) {
            tbody.innerHTML = '<tr><td colspan="5" style="text-align:center; color:var(--text-muted); padding:2rem;">No address mappings configured</td></tr>';
            return;
        }

        addresses.forEach((addr, index) => {
            const row = document.createElement('tr');
            row.innerHTML = `
                <td>${this.escapeHtml(addr.mqttTopic || '')}</td>
                <td>${this.escapeHtml(addr.displayName || '-')}</td>
                <td>${this.escapeHtml(addr.dataType || '')}</td>
                <td>${this.escapeHtml(addr.accessLevel || '')}</td>
                <td>
                    <button class="btn btn-danger" style="padding:0.3rem 0.6rem; font-size:0.7rem;" onclick="deleteAddress(${index})">Delete</button>
                </td>
            `;
            tbody.appendChild(row);
        });

        // Update count
        const countEl = document.getElementById('addresses-count');
        if (countEl) countEl.textContent = addresses.length;
    }

    renderStatus() {
        if (!this.serverData || !this.serverData.status) return;
        const s = this.serverData.status;

        this.setText('status-server-status', s.status || '-');
        this.setText('status-server-name', s.serverName || '-');
        this.setText('status-node-id', s.nodeId || '-');
        this.setText('status-port', s.port || '-');
        this.setText('status-active-connections', s.activeConnections || '0');
        this.setText('status-node-count', s.nodeCount || '0');
    }

    async saveServer() {
        const form = document.getElementById('server-form');
        if (!form.checkValidity()) {
            form.reportValidity();
            return;
        }

        const updatedInput = {
            name: document.getElementById('server-name').value.trim(),
            namespace: document.getElementById('server-namespace').value.trim(),
            nodeId: document.getElementById('server-node').value,
            enabled: document.getElementById('server-enabled').checked,
            port: parseInt(document.getElementById('server-port').value),
            path: document.getElementById('server-path').value.trim(),
            hostname: document.getElementById('server-hostname').value.trim() || null,
            bindAddress: document.getElementById('server-bind-address').value.trim() || null,
            namespaceUri: document.getElementById('server-namespace-uri').value.trim(),
            updateInterval: parseInt(document.getElementById('server-update-interval').value),
            bufferSize: parseInt(document.getElementById('server-buffer-size').value),
            addresses: this.serverData?.addresses || []
        };

        const prevNodeId = this.serverData ? this.serverData.nodeId : null;

        try {
            const mutation = `
                mutation UpdateOpcUaServer($name: String!, $input: OpcUaServerInput!) {
                    opcUaServer {
                        update(name: $name, input: $input) {
                            success
                            errors
                            server { name }
                        }
                    }
                }
            `;
            const result = await this.client.query(mutation, { name: this.serverName, input: updatedInput });
            if (result.opcUaServer.update.success) {
                // Handle potential rename
                const newName = updatedInput.name;
                if (newName !== this.serverName) {
                    this.serverName = newName;
                    const url = new URL(window.location.href);
                    url.searchParams.set('server', newName);
                    window.history.replaceState({}, '', url.toString());
                }
                // Reload data first
                await this.loadServerData();
                // If node changed, explicitly call reassign mutation for proper event semantics
                if (prevNodeId && updatedInput.nodeId && updatedInput.nodeId !== prevNodeId) {
                    await this.reassignServer(updatedInput.nodeId);
                }
                this.showSuccess('OPC UA server updated successfully');
            } else {
                const errors = result.opcUaServer.update.errors || ['Unknown error'];
                this.showError('Failed to update OPC UA server: ' + errors.join(', '));
            }
        } catch (e) {
            console.error('Error updating OPC UA server', e);
            this.showError('Failed to update OPC UA server: ' + e.message);
        }
    }

    async reassignServer(newNodeId) {
        try {
            const mutation = `
                mutation ReassignOpcUaServer($name: String!, $nodeId: String!) {
                    opcUaServer {
                        reassign(name: $name, nodeId: $nodeId) {
                            success
                            errors
                            server { nodeId }
                        }
                    }
                }
            `;
            const result = await this.client.query(mutation, { name: this.serverName, nodeId: newNodeId });
            if (!result.opcUaServer.reassign.success) {
                const errs = result.opcUaServer.reassign.errors || ['Unknown error'];
                this.showError('Reassign warning: ' + errs.join(', '));
            } else {
                await this.loadServerData(); // refresh node assignment visuals
                this.showSuccess('Server reassigned to node ' + newNodeId);
            }
        } catch (e) {
            console.error('Error reassigning server', e);
            this.showError('Failed to reassign OPC UA server: ' + e.message);
        }
    }

    async toggleServer() {
        if (!this.serverData) return;
        const newState = !this.serverData.enabled;
        try {
            const mutation = `
                mutation ToggleOpcUaServer($name: String!, $enabled: Boolean!) {
                    opcUaServer {
                        toggle(name: $name, enabled: $enabled) {
                            success
                            errors
                            server { enabled }
                        }
                    }
                }
            `;
            const result = await this.client.query(mutation, { name: this.serverName, enabled: newState });
            if (result.opcUaServer.toggle.success) {
                await this.loadServerData();
                this.showSuccess(`OPC UA server ${newState ? 'started' : 'stopped'} successfully`);
            } else {
                const errors = result.opcUaServer.toggle.errors || ['Unknown error'];
                this.showError('Failed to toggle OPC UA server: ' + errors.join(', '));
            }
        } catch (e) {
            console.error('Toggle error', e);
            this.showError('Failed to toggle OPC UA server: ' + e.message);
        }
    }

    async deleteServer() {
        try {
            const mutation = `
                mutation DeleteOpcUaServer($serverName: String!) {
                    opcUaServer {
                        delete(serverName: $serverName) {
                            success
                            message
                        }
                    }
                }
            `;
            const result = await this.client.query(mutation, { serverName: this.serverName });
            if (result.opcUaServer.delete.success) {
                this.showSuccess('OPC UA server deleted');
                setTimeout(() => {
                    window.location.href = '/pages/opcua-servers.html';
                }, 800);
            } else {
                const message = result.opcUaServer.delete.message || 'Unknown error';
                this.showError('Failed to delete OPC UA server: ' + message);
            }
        } catch (e) {
            console.error('Delete error', e);
            this.showError('Failed to delete OPC UA server: ' + e.message);
        }
    }

    // Address management
    showAddAddressModal() {
        // Clear form
        document.getElementById('address-mqtt-topic').value = '';
        document.getElementById('address-data-type').value = 'TEXT';
        document.getElementById('address-access-level').value = 'READ_ONLY';
        document.getElementById('address-display-name').value = '';
        document.getElementById('add-address-modal').style.display = 'flex';
    }

    hideAddAddressModal() {
        document.getElementById('add-address-modal').style.display = 'none';
    }

    async addAddress() {
        const form = document.getElementById('add-address-form');
        if (!form.checkValidity()) {
            form.reportValidity();
            return;
        }

        const newAddress = {
            mqttTopic: document.getElementById('address-mqtt-topic').value.trim(),
            dataType: document.getElementById('address-data-type').value,
            accessLevel: document.getElementById('address-access-level').value,
            displayName: document.getElementById('address-display-name').value.trim() || null
        };

        try {
            const mutation = `
                mutation AddOpcUaServerAddress($serverName: String!, $address: OpcUaServerAddressInput!) {
                    opcUaServer {
                        addAddress(serverName: $serverName, address: $address) {
                            success
                            message
                        }
                    }
                }
            `;
            const result = await this.client.query(mutation, { serverName: this.serverName, address: newAddress });
            if (result.opcUaServer.addAddress.success) {
                this.hideAddAddressModal();
                await this.loadServerData();
                this.showSuccess('Address mapping added successfully');
            } else {
                const message = result.opcUaServer.addAddress.message || 'Unknown error';
                this.showError('Failed to add address: ' + message);
            }
        } catch (e) {
            console.error('Error adding address', e);
            this.showError('Failed to add address: ' + e.message);
        }
    }

    async deleteAddress(index) {
        if (!this.serverData || !this.serverData.addresses) return;
        const addresses = this.serverData.addresses;
        if (index < 0 || index >= addresses.length) return;

        const address = addresses[index];

        try {
            const mutation = `
                mutation RemoveOpcUaServerAddress($serverName: String!, $mqttTopic: String!) {
                    opcUaServer {
                        removeAddress(serverName: $serverName, mqttTopic: $mqttTopic) {
                            success
                            message
                        }
                    }
                }
            `;
            const result = await this.client.query(mutation, { serverName: this.serverName, mqttTopic: address.mqttTopic });
            if (result.opcUaServer.removeAddress.success) {
                await this.loadServerData();
                this.showSuccess('Address mapping removed successfully');
            } else {
                const message = result.opcUaServer.removeAddress.message || 'Unknown error';
                this.showError('Failed to remove address: ' + message);
            }
        } catch (e) {
            console.error('Error removing address', e);
            this.showError('Failed to remove address: ' + e.message);
        }
    }

    // UI helpers
    showDeleteModal() {
        const span = document.getElementById('delete-server-name');
        if (span && this.serverData) span.textContent = this.serverData.name;
        document.getElementById('delete-server-modal').style.display = 'flex';
    }

    hideDeleteModal() {
        document.getElementById('delete-server-modal').style.display = 'none';
    }

    confirmDeleteServer() {
        this.hideDeleteModal();
        this.deleteServer();
    }

    goBack() {
        window.location.href = '/pages/opcua-servers.html';
    }

    showLoading(show) {
        const el = document.getElementById('loading-indicator');
        if (el) el.style.display = show ? 'flex' : 'none';
    }

    showError(message) {
        const errorEl = document.getElementById('error-message');
        const text = document.querySelector('#error-message .error-text');
        if (errorEl && text) {
            text.textContent = message;
            errorEl.style.display = 'flex';
            setTimeout(() => this.hideError(), 5000);
        }
    }

    hideError() {
        const errorEl = document.getElementById('error-message');
        if (errorEl) errorEl.style.display = 'none';
    }

    showSuccess(message) {
        const note = document.createElement('div');
        note.className = 'success-notification';
        note.innerHTML = `<span class="success-icon">✅</span><span class="success-text">${this.escapeHtml(message)}</span>`;
        document.body.appendChild(note);
        setTimeout(() => {
            if (note.parentNode) note.parentNode.removeChild(note);
        }, 3000);
    }

    escapeHtml(t) {
        const div = document.createElement('div');
        div.textContent = t;
        return div.innerHTML;
    }

    setText(id, value) {
        const el = document.getElementById(id);
        if (el) el.textContent = value;
    }
}

// Global wrappers
let opcUaServerDetailManager;
function saveServer() { opcUaServerDetailManager.saveServer(); }
function toggleServer() { opcUaServerDetailManager.toggleServer(); }
function goBack() { opcUaServerDetailManager.goBack(); }
function showDeleteModal() { opcUaServerDetailManager.showDeleteModal(); }
function hideDeleteModal() { opcUaServerDetailManager.hideDeleteModal(); }
function confirmDeleteServer() { opcUaServerDetailManager.confirmDeleteServer(); }
function showAddAddressModal() { opcUaServerDetailManager.showAddAddressModal(); }
function hideAddAddressModal() { opcUaServerDetailManager.hideAddAddressModal(); }
function addAddress() { opcUaServerDetailManager.addAddress(); }
function deleteAddress(index) { opcUaServerDetailManager.deleteAddress(index); }

// Initialize
document.addEventListener('DOMContentLoaded', () => {
    opcUaServerDetailManager = new OpcUaServerDetailManager();
});

// Close modals on backdrop click
document.addEventListener('click', e => {
    if (e.target.classList.contains('modal')) {
        if (e.target.id === 'delete-server-modal') opcUaServerDetailManager.hideDeleteModal();
        if (e.target.id === 'add-address-modal') opcUaServerDetailManager.hideAddAddressModal();
    }
});
