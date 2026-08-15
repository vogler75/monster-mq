// i3X Client Detail Management JavaScript

class I3xClientDetailManager {
    constructor() {
        this.client = new GraphQLDashboardClient();
        this.clientName = null;
        this.clientData = null;
        this.clusterNodes = [];
        this.editAddressOriginalElementId = null;
        this.init();
    }

    async init() {
        const urlParams = new URLSearchParams(window.location.search);
        this.clientName = urlParams.get('client') || urlParams.get('id');
        this.isNew = urlParams.get('new') === 'true';

        if (this.isNew) {
            await this.loadClusterNodes();
            this.showNewClientForm();
            return;
        }

        if (!this.clientName) {
            this.showError('No client specified in URL. Please select a client from the list.');
            document.getElementById('page-title').textContent = 'Error';
            document.getElementById('page-subtitle').textContent = 'Invalid Request';
            return;
        }

        this.showLoading(true);
        try {
            await this.loadClusterNodes();
            await this.loadClientData();

            const deleteBtn = document.getElementById('delete-btn');
            if (deleteBtn) {
                deleteBtn.style.display = 'inline-flex';
                deleteBtn.onclick = null;
                deleteBtn.addEventListener('click', (e) => {
                    e.preventDefault();
                    this.showDeleteModal();
                });
            }
        } catch (error) {
            this.showError('Failed to load i3X client data: ' + error.message);
            document.getElementById('page-title').textContent = 'Error Loading Client';
            document.getElementById('page-subtitle').textContent = this.clientName;
        } finally {
            this.showLoading(false);
        }
    }

    showNewClientForm() {
        document.getElementById('breadcrumb-name').textContent = 'New Client';
        document.getElementById('page-title').textContent = 'Add i3X Client';
        document.getElementById('page-subtitle').textContent = 'Create a new i3X server connection';

        document.getElementById('client-name').value = '';
        document.getElementById('client-name').disabled = false;
        document.getElementById('client-namespace').value = '';
        document.getElementById('client-server-url').value = 'http://localhost:3002/i3x/v1';
        document.getElementById('client-id').value = 'monstermq-i3x-client';
        document.getElementById('client-node').value = '*';
        document.getElementById('client-reconnect-delay').value = '5000';
        document.getElementById('client-connection-timeout').value = '10000';
        document.getElementById('client-auth-type').value = 'NONE';
        document.getElementById('client-username').value = '';
        document.getElementById('client-password').value = '';
        document.getElementById('client-token').value = '';
        document.getElementById('client-enabled').checked = true;

        toggleAuthOptions();

        const customHeadersList = document.getElementById('custom-headers-list');
        if (customHeadersList) customHeadersList.innerHTML = '';

        const saveBtn = document.getElementById('save-client-btn');
        if (saveBtn) saveBtn.innerHTML = saveBtn.innerHTML.replace('Save Client', 'Create Client');

        const deleteBtn = document.getElementById('delete-btn');
        if (deleteBtn) deleteBtn.style.display = 'none';

        const addressSection = document.getElementById('addresses-section');
        if (addressSection) addressSection.style.display = 'none';

        document.getElementById('client-content').style.display = 'block';
    }

    async loadClusterNodes() {
        try {
            const query = `query GetBrokers { brokers { nodeId isCurrent } }`;
            const result = await this.client.query(query);
            this.clusterNodes = result.brokers || [];

            const nodeSelect = document.getElementById('client-node');
            if (nodeSelect) {
                nodeSelect.innerHTML = '<option value="*">Any Node (*)</option>';
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
                query GetI3xClient($name: String!) {
                    i3xClients(name: $name) {
                        name
                        namespace
                        nodeId
                        enabled
                        isOnCurrentNode
                        createdAt
                        updatedAt
                        config {
                            url
                            authType
                            username
                            clientId
                            reconnectDelay
                            connectionTimeout
                            headers {
                                key
                                value
                            }
                            addresses {
                                elementId
                                topic
                                maxDepth
                                retained
                                qos
                                messageFormat
                                removePath
                                description
                            }
                        }
                    }
                }
            `;

            const result = await this.client.query(query, { name: this.clientName });
            if (!result || !result.i3xClients || result.i3xClients.length === 0) {
                throw new Error('Client not found');
            }

            this.clientData = result.i3xClients[0];
            this.renderClientInfo();
            this.renderAddressesList();

        } catch (error) {
            console.error('Error loading i3X client:', error);
            this.showError('Failed to load client: ' + error.message);
        } finally {
            this.showLoading(false);
        }
    }

    renderClientInfo() {
        if (!this.clientData) return;

        const d = this.clientData;
        const cfg = d.config || {};

        document.getElementById('breadcrumb-name').textContent = d.name;
        document.getElementById('page-title').textContent = `i3X Client: ${d.name}`;
        document.getElementById('page-subtitle').textContent = `${d.namespace || 'root'} — ${cfg.url}`;

        document.getElementById('client-name').value = d.name;
        document.getElementById('client-name').disabled = true;
        document.getElementById('client-namespace').value = d.namespace || '';
        document.getElementById('client-server-url').value = cfg.url || '';
        document.getElementById('client-id').value = cfg.clientId || 'monstermq-i3x-client';
        document.getElementById('client-node').value = d.nodeId || '*';
        document.getElementById('client-reconnect-delay').value = cfg.reconnectDelay || 5000;
        document.getElementById('client-connection-timeout').value = cfg.connectionTimeout || 10000;
        document.getElementById('client-auth-type').value = cfg.authType || 'NONE';
        document.getElementById('client-username').value = cfg.username || '';
        document.getElementById('client-password').value = '';
        document.getElementById('client-token').value = '';
        document.getElementById('client-enabled').checked = d.enabled;

        toggleAuthOptions();

        const headersList = document.getElementById('custom-headers-list');
        if (headersList) {
            headersList.innerHTML = '';
            if (cfg.headers && cfg.headers.length > 0) {
                cfg.headers.forEach(h => {
                    addCustomHeader();
                    const lastRow = headersList.lastElementChild;
                    if (lastRow) {
                        lastRow.querySelector('.custom-header-key').value = h.key;
                        lastRow.querySelector('.custom-header-value').value = h.value;
                    }
                });
            }
        }

        this.setText('client-created-at', d.createdAt ? new Date(d.createdAt).toLocaleString() : '-');
        this.setText('client-updated-at', d.updatedAt ? new Date(d.updatedAt).toLocaleString() : '-');

        const statusBadge = document.getElementById('client-status');
        if (statusBadge) {
            if (d.enabled) {
                statusBadge.className = 'status-badge status-enabled';
                statusBadge.textContent = 'ENABLED';
            } else {
                statusBadge.className = 'status-badge status-disabled';
                statusBadge.textContent = 'DISABLED';
            }
        }

        document.getElementById('client-content').style.display = 'block';
        const addressSection = document.getElementById('addresses-section');
        if (addressSection) addressSection.style.display = 'block';
    }

    renderAddressesList() {
        if (!this.clientData) return;

        const tbody = document.getElementById('addresses-table-body');
        if (!tbody) return;
        tbody.innerHTML = '';

        const addresses = this.clientData.config?.addresses || [];
        if (addresses.length === 0) {
            tbody.innerHTML = ui.emptyRow(9, 'No address mappings configured', 'Use "Add Address" to create one.');
            return;
        }

        addresses.forEach(addr => {
            const row = document.createElement('tr');
            const depthLabel = addr.maxDepth === 0 ? 'Recursive (0 / #)' : (addr.maxDepth === 1 ? 'Single (1)' : `Depth ${addr.maxDepth}`);

            row.innerHTML = `
                <td><code>${this.escapeHtml(addr.elementId)}</code></td>
                <td><code>${this.escapeHtml(addr.topic)}</code></td>
                <td><span class="status-badge ${addr.maxDepth === 0 ? 'status-enabled' : 'status-info'}">${depthLabel}</span></td>
                <td><span class="status-badge status-info">${this.escapeHtml(addr.messageFormat || 'RAW_VALUE')}</span></td>
                <td>QoS ${addr.qos ?? 0}</td>
                <td>${addr.retained ? '<span class="status-badge status-enabled">Retained</span>' : '<span style="color: var(--text-muted);">No</span>'}</td>
                <td>${addr.removePath ? '<span class="status-badge status-info">Yes</span>' : '<span style="color: var(--text-muted);">No</span>'}</td>
                <td>${this.escapeHtml(addr.description || '—')}</td>
                <td>
                    <div class="action-buttons">
                        <ix-icon-button icon="highlight" variant="subtle-tertiary" size="24" title="Edit Address" class="btn-edit"></ix-icon-button>
                        <ix-icon-button icon="trashcan" variant="subtle-tertiary" size="24" class="btn-delete" title="Delete Address"></ix-icon-button>
                    </div>
                </td>
            `;

            const editBtn = row.querySelector('.btn-edit');
            if (editBtn) {
                editBtn.addEventListener('click', (e) => {
                    e.stopPropagation();
                    this.editAddress(addr.elementId);
                });
            }

            const deleteBtn = row.querySelector('.btn-delete');
            if (deleteBtn) {
                deleteBtn.addEventListener('click', (e) => {
                    e.stopPropagation();
                    this.deleteAddress(addr.elementId);
                });
            }

            tbody.appendChild(row);
        });
    }

    async saveClient() {
        const form = document.getElementById('client-form');
        if (!form.checkValidity()) {
            form.reportValidity();
            return;
        }

        const name = document.getElementById('client-name').value.trim();
        const namespace = document.getElementById('client-namespace').value.trim();
        const serverUrl = document.getElementById('client-server-url').value.trim();
        const clientId = document.getElementById('client-id').value.trim() || 'monstermq-i3x-client';
        const nodeId = document.getElementById('client-node').value;
        const reconnectDelay = parseInt(document.getElementById('client-reconnect-delay').value, 10) || 5000;
        const connectionTimeout = parseInt(document.getElementById('client-connection-timeout').value, 10) || 10000;
        const authType = document.getElementById('client-auth-type').value;
        const enabled = document.getElementById('client-enabled').checked;

        const customHeaders = [];
        document.querySelectorAll('#custom-headers-list .custom-header-row').forEach(row => {
            const key = row.querySelector('.custom-header-key').value.trim();
            const value = row.querySelector('.custom-header-value').value.trim();
            if (key) customHeaders.push({ key, value });
        });

        const configInput = {
            url: serverUrl,
            clientId: clientId,
            authType: authType,
            reconnectDelay: reconnectDelay,
            connectionTimeout: connectionTimeout,
            headers: customHeaders
        };

        if (authType === 'BASIC') {
            const username = document.getElementById('client-username').value.trim();
            const password = document.getElementById('client-password').value;
            if (username) configInput.username = username;
            if (password) configInput.password = password;
        } else if (authType === 'BEARER') {
            const token = document.getElementById('client-token').value.trim();
            if (token) configInput.token = token;
        }

        const clientInput = {
            name: name,
            namespace: namespace,
            nodeId: nodeId,
            enabled: enabled,
            config: configInput
        };

        this.showLoading(true);
        try {
            if (this.isNew) {
                const mutation = `
                    mutation CreateI3xClient($input: I3xClientInput!) {
                        i3xClient {
                            create(input: $input) {
                                success
                                errors
                            }
                        }
                    }
                `;
                const result = await this.client.query(mutation, { input: clientInput });
                if (result.i3xClient.create.success) {
                    ui.success(`i3X Client "${name}" created successfully`);
                    setTimeout(() => {
                        window.spaLocation.href = `/pages/i3x-client-detail.html?client=${encodeURIComponent(name)}`;
                    }, 800);
                } else {
                    const errors = result.i3xClient.create.errors || ['Unknown error'];
                    this.showError('Failed to create client: ' + errors.join(', '));
                }
            } else {
                const mutation = `
                    mutation UpdateI3xClient($name: String!, $input: I3xClientInput!) {
                        i3xClient {
                            update(name: $name, input: $input) {
                                success
                                errors
                            }
                        }
                    }
                `;
                const result = await this.client.query(mutation, { name: this.clientName, input: clientInput });
                if (result.i3xClient.update.success) {
                    ui.success('i3X Client updated successfully');
                    await this.loadClientData();
                } else {
                    const errors = result.i3xClient.update.errors || ['Unknown error'];
                    this.showError('Failed to update client: ' + errors.join(', '));
                }
            }
        } catch (error) {
            console.error('Error saving client:', error);
            this.showError('Failed to save client: ' + error.message);
        } finally {
            this.showLoading(false);
        }
    }

    async showDeleteModal() {
        if (!this.clientName) return;
        if (await ui.confirmDelete(this.clientName, { title: 'Delete i3X Client' })) {
            this.showLoading(true);
            try {
                const mutation = `
                    mutation DeleteI3xClient($name: String!) {
                        i3xClient {
                            delete(name: $name)
                        }
                    }
                `;
                const result = await this.client.query(mutation, { name: this.clientName });
                if (result.i3xClient.delete) {
                    ui.success(`Client "${this.clientName}" deleted successfully`);
                    setTimeout(() => {
                        window.spaLocation.href = '/pages/i3x-clients.html';
                    }, 800);
                } else {
                    this.showError('Failed to delete client');
                }
            } catch (error) {
                console.error('Error deleting client:', error);
                this.showError('Failed to delete client: ' + error.message);
            } finally {
                this.showLoading(false);
            }
        }
    }

    showAddAddressModal() {
        document.getElementById('address-element-id').value = '';
        document.getElementById('address-local-topic').value = '';
        document.getElementById('address-max-depth').value = '1';
        document.getElementById('address-message-format').value = 'RAW_VALUE';
        document.getElementById('address-qos').value = '0';
        document.getElementById('address-retained').checked = false;
        document.getElementById('address-remove-path').checked = false;
        document.getElementById('address-description').value = '';
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

        const elementId = document.getElementById('address-element-id').value.trim();
        const localTopic = document.getElementById('address-local-topic').value.trim();
        const maxDepth = parseInt(document.getElementById('address-max-depth').value, 10);
        const messageFormat = document.getElementById('address-message-format').value;
        const qos = parseInt(document.getElementById('address-qos').value, 10);
        const retained = document.getElementById('address-retained').checked;
        const removePath = document.getElementById('address-remove-path').checked;
        const description = document.getElementById('address-description').value.trim();

        const addressData = {
            elementId,
            topic: localTopic,
            maxDepth,
            messageFormat,
            qos,
            retained,
            removePath,
            description
        };

        try {
            const mutation = `
                mutation AddI3xAddress($deviceName: String!, $input: I3xAddressInput!) {
                    i3xClient {
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

            if (result.i3xClient.addAddress.success) {
                this.hideAddAddressModal();
                await this.loadClientData();
                ui.success('Address mapping added successfully');
            } else {
                const errors = result.i3xClient.addAddress.errors || ['Unknown error'];
                this.showError('Failed to add address: ' + errors.join(', '));
            }
        } catch (error) {
            console.error('Error adding address:', error);
            this.showError('Failed to add address: ' + error.message);
        }
    }

    editAddress(elementId) {
        const address = this.clientData.config.addresses.find(a => a.elementId === elementId);
        if (!address) {
            this.showError('Address not found');
            return;
        }

        this.editAddressOriginalElementId = elementId;

        document.getElementById('edit-address-element-id').value = address.elementId;
        document.getElementById('edit-address-local-topic').value = address.topic;
        document.getElementById('edit-address-max-depth').value = String(address.maxDepth ?? 1);
        document.getElementById('edit-address-message-format').value = address.messageFormat || 'RAW_VALUE';
        document.getElementById('edit-address-qos').value = String(address.qos ?? 0);
        document.getElementById('edit-address-retained').checked = address.retained ?? false;
        document.getElementById('edit-address-remove-path').checked = address.removePath ?? false;
        document.getElementById('edit-address-description').value = address.description || '';

        this.showEditAddressModal();
    }

    showEditAddressModal() {
        document.getElementById('edit-address-modal').style.display = 'flex';
    }

    hideEditAddressModal() {
        document.getElementById('edit-address-modal').style.display = 'none';
        this.editAddressOriginalElementId = null;
    }

    async updateAddress() {
        const form = document.getElementById('edit-address-form');
        if (!form.checkValidity()) {
            form.reportValidity();
            return;
        }

        const elementId = document.getElementById('edit-address-element-id').value.trim();
        const localTopic = document.getElementById('edit-address-local-topic').value.trim();
        const maxDepth = parseInt(document.getElementById('edit-address-max-depth').value, 10);
        const messageFormat = document.getElementById('edit-address-message-format').value;
        const qos = parseInt(document.getElementById('edit-address-qos').value, 10);
        const retained = document.getElementById('edit-address-retained').checked;
        const removePath = document.getElementById('edit-address-remove-path').checked;
        const description = document.getElementById('edit-address-description').value.trim();

        const updatedAddress = {
            elementId,
            topic: localTopic,
            maxDepth,
            messageFormat,
            qos,
            retained,
            removePath,
            description
        };

        try {
            const mutation = `
                mutation UpdateI3xAddress($deviceName: String!, $elementId: String!, $input: I3xAddressInput!) {
                    i3xClient {
                        updateAddress(deviceName: $deviceName, elementId: $elementId, input: $input) {
                            success
                            errors
                        }
                    }
                }
            `;

            const result = await this.client.query(mutation, {
                deviceName: this.clientName,
                elementId: this.editAddressOriginalElementId,
                input: updatedAddress
            });

            if (result.i3xClient.updateAddress.success) {
                this.hideEditAddressModal();
                await this.loadClientData();
                ui.success('Address mapping updated successfully');
            } else {
                const errors = result.i3xClient.updateAddress.errors || ['Unknown error'];
                this.showError('Failed to update address: ' + errors.join(', '));
            }
        } catch (error) {
            console.error('Error updating address:', error);
            this.showError('Failed to update address: ' + error.message);
        }
    }

    async deleteAddress(elementId) {
        if (await ui.confirmDelete(elementId, { title: 'Delete address mapping' })) {
            try {
                const mutation = `
                    mutation DeleteI3xAddress($deviceName: String!, $elementId: String!) {
                        i3xClient {
                            deleteAddress(deviceName: $deviceName, elementId: $elementId) {
                                success
                                errors
                            }
                        }
                    }
                `;

                const result = await this.client.query(mutation, {
                    deviceName: this.clientName,
                    elementId
                });

                if (result.i3xClient.deleteAddress.success) {
                    await this.loadClientData();
                    ui.success('Address mapping deleted successfully');
                } else {
                    const errors = result.i3xClient.deleteAddress.errors || ['Unknown error'];
                    this.showError('Failed to delete address: ' + errors.join(', '));
                }
            } catch (error) {
                console.error('Error deleting address:', error);
                this.showError('Failed to delete address: ' + error.message);
            }
        }
    }

    setText(id, text) {
        const el = document.getElementById(id);
        if (el) el.textContent = text;
    }

    showLoading(show) {
        const indicator = document.getElementById('loading-indicator');
        if (indicator) indicator.style.display = show ? 'flex' : 'none';
    }

    showError(message) { ui.showError(message); }
    hideError() { ui.clearError(); }

    escapeHtml(text) {
        const div = document.createElement('div');
        div.textContent = text || '';
        return div.innerHTML;
    }
}

var i3xClientDetailManager;

function toggleAuthOptions() {
    const authType = document.getElementById('client-auth-type')?.value;
    const basicFields = document.getElementById('auth-basic-fields');
    const bearerFields = document.getElementById('auth-bearer-fields');

    if (basicFields) basicFields.style.display = authType === 'BASIC' ? 'block' : 'none';
    if (bearerFields) bearerFields.style.display = authType === 'BEARER' ? 'block' : 'none';
}

function addCustomHeader() {
    const container = document.getElementById('custom-headers-list');
    if (!container) return;
    const row = document.createElement('div');
    row.className = 'custom-header-row';
    row.innerHTML = `
        <input type="text" placeholder="Header Name (e.g. X-Api-Key)" class="custom-header-key">
        <input type="text" placeholder="Header Value" class="custom-header-value">
        <button type="button" class="btn btn-danger btn-small" onclick="removeCustomHeader(this)" title="Remove header">×</button>
    `;
    container.appendChild(row);
}

function removeCustomHeader(btn) {
    btn?.closest('.custom-header-row')?.remove();
}

function saveClient() {
    if (window.i3xClientDetailManager) window.i3xClientDetailManager.saveClient();
}

function showDeleteModal() {
    if (window.i3xClientDetailManager) window.i3xClientDetailManager.showDeleteModal();
}

function showAddAddressModal() {
    if (window.i3xClientDetailManager) window.i3xClientDetailManager.showAddAddressModal();
}

function hideAddAddressModal() {
    if (window.i3xClientDetailManager) window.i3xClientDetailManager.hideAddAddressModal();
}

function addAddress() {
    if (window.i3xClientDetailManager) window.i3xClientDetailManager.addAddress();
}

function showEditAddressModal() {
    if (window.i3xClientDetailManager) window.i3xClientDetailManager.showEditAddressModal();
}

function hideEditAddressModal() {
    if (window.i3xClientDetailManager) window.i3xClientDetailManager.hideEditAddressModal();
}

function updateAddress() {
    if (window.i3xClientDetailManager) window.i3xClientDetailManager.updateAddress();
}

document.addEventListener('DOMContentLoaded', () => {
    i3xClientDetailManager = new I3xClientDetailManager();
    window.i3xClientDetailManager = i3xClientDetailManager;
});
