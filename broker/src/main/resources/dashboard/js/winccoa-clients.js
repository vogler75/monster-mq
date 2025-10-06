// WinCC OA Bridge Management JavaScript

class WinCCOaClientManager {
    constructor() {
        this.client = new GraphQLDashboardClient('/graphql');
        this.clients = [];
        this.clusterNodes = [];
        this.deleteClientName = null;
        this.addressCounter = 0;
        this.init();
    }

    async init() {
        console.log('Initializing WinCC OA Bridge Manager...');
        // Load initial data
        await this.loadClusterNodes();
        await this.loadClients();
        // Set up periodic refresh
        setInterval(() => this.loadClients(), 30000); // Refresh every 30 seconds
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

            // Populate node selector in the add client form
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

    async loadClients() {
        this.showLoading(true);
        this.hideError();

        try {
            const query = `
                query GetWinCCOaClients {
                    winCCOaClients {
                        name
                        namespace
                        nodeId
                        enabled
                        isOnCurrentNode
                        createdAt
                        updatedAt
                        config {
                            graphqlEndpoint
                            websocketEndpoint
                            username
                            reconnectDelay
                            connectionTimeout
                            messageFormat
                            transformConfig {
                                removeSystemName
                                convertDotToSlash
                                convertUnderscoreToSlash
                                regexPattern
                                regexReplacement
                            }
                            addresses {
                                query
                                topic
                                description
                                answer
                                retained
                            }
                        }
                    }
                }
            `;

            const result = await this.client.query(query);
            console.log('Load clients result:', result);

            if (!result || !result.winCCOaClients) {
                throw new Error('Invalid response structure');
            }

            this.clients = result.winCCOaClients || [];
            this.updateMetrics();
            this.renderClientsTable();

        } catch (error) {
            console.error('Error loading clients:', error);
            this.showError('Failed to load WinCC OA bridges: ' + error.message);
        } finally {
            this.showLoading(false);
        }
    }

    updateMetrics() {
        const totalClients = this.clients.length;
        const enabledClients = this.clients.filter(c => c.enabled).length;
        const currentNodeClients = this.clients.filter(c => c.isOnCurrentNode).length;
        const totalAddresses = this.clients.reduce((sum, c) => sum + c.config.addresses.length, 0);

        document.getElementById('total-clients').textContent = totalClients;
        document.getElementById('enabled-clients').textContent = enabledClients;
        document.getElementById('current-node-clients').textContent = currentNodeClients;
        document.getElementById('total-addresses').textContent = totalAddresses;
    }

    renderClientsTable() {
        const tbody = document.getElementById('clients-table-body');
        if (!tbody) return;

        tbody.innerHTML = '';

        if (this.clients.length === 0) {
            tbody.innerHTML = `
                <tr>
                    <td colspan="7" class="no-data">
                        No WinCC OA bridges configured. Click "Add Bridge" to get started.
                    </td>
                </tr>
            `;
            return;
        }

        this.clients.forEach(client => {
            const row = document.createElement('tr');

            const statusClass = client.enabled ? 'status-enabled' : 'status-disabled';
            const statusText = client.enabled ? 'Enabled' : 'Disabled';
            const nodeIndicator = client.isOnCurrentNode ? '📍 ' : '';

            row.innerHTML = `
                <td>
                    <div class="client-name">${this.escapeHtml(client.name)}</div>
                    <small class="client-namespace">${this.escapeHtml(client.namespace)}</small>
                </td>
                <td>
                    <div class="endpoint-url" title="${this.escapeHtml(client.config.graphqlEndpoint)}">
                        ${this.escapeHtml(client.config.graphqlEndpoint)}
                    </div>
                    <small class="message-format">Format: ${this.escapeHtml(client.config.messageFormat)}</small>
                </td>
                <td>${this.escapeHtml(client.namespace)}</td>
                <td>
                    <div class="node-assignment">
                        ${nodeIndicator}${this.escapeHtml(client.nodeId)}
                    </div>
                </td>
                <td>
                    <span class="status-badge ${statusClass}">${statusText}</span>
                </td>
                <td>
                    <div class="address-count">
                        ${client.config.addresses.length} queries
                    </div>
                </td>
                <td>
                    <div class="action-buttons">
                        <button class="btn-action btn-view" onclick="winCCOaClientManager.editClient('${client.name}')" title="Edit Bridge">
                            <svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor">
                                <path d="M3 17.25V21h3.75L17.81 9.94l-3.75-3.75L3 17.25zM20.71 7.04c.39-.39.39-1.02 0-1.41l-2.34-2.34c-.39-.39-1.02-.39-1.41 0l-1.83 1.83 3.75 3.75 1.83-1.83z"/>
                            </svg>
                        </button>
                        <button class="btn-action ${client.enabled ? 'btn-pause' : 'btn-play'}"
                                onclick="winCCOaClientManager.toggleClient('${client.name}', ${!client.enabled})"
                                title="${client.enabled ? 'Stop Bridge' : 'Start Bridge'}">
                            ${client.enabled ?
                                '<svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor"><path d="M6 19h4V5H6v14zm8-14v14h4V5h-4z"/></svg>' :
                                '<svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor"><path d="M8 5v14l11-7z"/></svg>'
                            }
                        </button>
                        <button class="btn-action btn-delete" onclick="winCCOaClientManager.deleteClient('${client.name}')" title="Delete Bridge">
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

    addAddressRow(address = null) {
        const container = document.getElementById('addresses-container');
        const index = this.addressCounter++;

        const addressDiv = document.createElement('div');
        addressDiv.className = 'address-row';
        addressDiv.id = `address-row-${index}`;
        addressDiv.style.cssText = 'border: 1px solid var(--dark-border); border-radius: 8px; padding: 1rem; margin-bottom: 1rem; background: var(--dark-surface-2);';

        addressDiv.innerHTML = `
            <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 0.5rem;">
                <h5 style="margin: 0; color: var(--text-primary); font-size: 0.9rem;">Query #${index + 1}</h5>
                <button type="button" class="btn-action btn-delete" onclick="winCCOaClientManager.removeAddressRow(${index})" title="Remove Query">
                    <svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor">
                        <path d="M6 19c0 1.1.9 2 2 2h8c1.1 0 2-.9 2-2V7H6v12zM19 4h-3.5l-1-1h-5l-1 1H5v2h14V4z"/>
                    </svg>
                </button>
            </div>
            <div class="form-group">
                <label>GraphQL Query *</label>
                <input type="text" class="address-query" data-index="${index}" required placeholder="e.g., ExampleDP_Trend1.:_original.._value" value="${address?.query || ''}">
            </div>
            <div class="form-group">
                <label>MQTT Topic *</label>
                <input type="text" class="address-topic" data-index="${index}" required placeholder="e.g., plant/sensor/value" value="${address?.topic || ''}">
            </div>
            <div class="form-group">
                <label>Description</label>
                <input type="text" class="address-description" data-index="${index}" placeholder="Optional description" value="${address?.description || ''}">
            </div>
            <div class="form-group">
                <div class="checkbox-group">
                    <input type="checkbox" class="address-answer" data-index="${index}" ${address?.answer ? 'checked' : ''}>
                    <label>Request Answer Row</label>
                </div>
                <small style="color: var(--text-muted); margin-top: 0.5rem; display: block;">Request answer row in dpQueryConnectSingle</small>
            </div>
            <div class="form-group">
                <div class="checkbox-group">
                    <input type="checkbox" class="address-retained" data-index="${index}" ${address?.retained ? 'checked' : ''}>
                    <label>Publish with Retained Flag</label>
                </div>
                <small style="color: var(--text-muted); margin-top: 0.5rem; display: block;">Publish MQTT message with retained flag</small>
            </div>
        `;

        container.appendChild(addressDiv);
    }

    removeAddressRow(index) {
        const row = document.getElementById(`address-row-${index}`);
        if (row) {
            row.remove();
        }
    }

    getAddressesFromForm() {
        const addresses = [];
        const container = document.getElementById('addresses-container');
        const rows = container.querySelectorAll('.address-row');

        rows.forEach(row => {
            const query = row.querySelector('.address-query').value.trim();
            const topic = row.querySelector('.address-topic').value.trim();
            const description = row.querySelector('.address-description').value.trim();
            const answer = row.querySelector('.address-answer').checked;
            const retained = row.querySelector('.address-retained').checked;

            if (query && topic) {
                addresses.push({
                    query,
                    topic,
                    description,
                    answer,
                    retained
                });
            }
        });

        return addresses;
    }

    clearAddresses() {
        const container = document.getElementById('addresses-container');
        container.innerHTML = '';
        this.addressCounter = 0;
    }

    async addClient() {
        const form = document.getElementById('add-client-form');
        if (!form.checkValidity()) {
            form.reportValidity();
            return;
        }

        const addresses = this.getAddressesFromForm();
        if (addresses.length === 0) {
            this.showError('Please add at least one query');
            return;
        }

        const clientData = {
            name: document.getElementById('client-name').value.trim(),
            namespace: document.getElementById('client-namespace').value.trim(),
            nodeId: document.getElementById('client-node').value,
            enabled: document.getElementById('client-enabled').checked,
            config: {
                graphqlEndpoint: document.getElementById('client-graphql-endpoint').value.trim(),
                websocketEndpoint: document.getElementById('client-websocket-endpoint').value.trim() || null,
                username: document.getElementById('client-username').value.trim() || null,
                password: document.getElementById('client-password').value || null,
                token: document.getElementById('client-token').value.trim() || null,
                reconnectDelay: parseInt(document.getElementById('client-reconnect-delay').value),
                connectionTimeout: parseInt(document.getElementById('client-connection-timeout').value),
                messageFormat: document.getElementById('client-message-format').value,
                transformConfig: {
                    removeSystemName: document.getElementById('client-remove-system-name').checked,
                    convertDotToSlash: document.getElementById('client-convert-dot-to-slash').checked,
                    convertUnderscoreToSlash: document.getElementById('client-convert-underscore-to-slash').checked,
                    regexPattern: document.getElementById('client-regex-pattern').value.trim() || null,
                    regexReplacement: document.getElementById('client-regex-replacement').value.trim() || null
                },
                addresses: addresses
            }
        };

        try {
            const mutation = `
                mutation CreateWinCCOaClient($input: WinCCOaClientInput!) {
                    createWinCCOaClient(input: $input) {
                        success
                        errors
                        client {
                            name
                            enabled
                        }
                    }
                }
            `;

            const result = await this.client.query(mutation, { input: clientData });

            if (result.createWinCCOaClient.success) {
                this.hideAddClientModal();
                await this.loadClients();
                this.showSuccess(`Bridge "${clientData.name}" added successfully`);
            } else {
                const errors = result.createWinCCOaClient.errors || ['Unknown error'];
                this.showError('Failed to add bridge: ' + errors.join(', '));
            }

        } catch (error) {
            console.error('Error adding client:', error);
            this.showError('Failed to add bridge: ' + error.message);
        }
    }

    editClient(clientName) {
        const client = this.clients.find(c => c.name === clientName);
        if (!client) {
            this.showError(`Client '${clientName}' not found`);
            return;
        }

        // Store the original name for update
        this.editingClientName = clientName;

        // Show modal first (without resetting)
        document.getElementById('add-client-modal').style.display = 'flex';

        // Populate form with existing values
        document.getElementById('client-name').value = client.name;
        document.getElementById('client-name').disabled = true; // Can't change name
        document.getElementById('client-namespace').value = client.namespace;
        document.getElementById('client-graphql-endpoint').value = client.config.graphqlEndpoint;
        document.getElementById('client-websocket-endpoint').value = client.config.websocketEndpoint || '';
        document.getElementById('client-username').value = client.config.username || '';
        document.getElementById('client-password').value = ''; // Don't show existing password
        document.getElementById('client-token').value = client.config.token || '';
        document.getElementById('client-node').value = client.nodeId;
        document.getElementById('client-reconnect-delay').value = client.config.reconnectDelay;
        document.getElementById('client-connection-timeout').value = client.config.connectionTimeout;
        document.getElementById('client-message-format').value = client.config.messageFormat;
        document.getElementById('client-enabled').checked = client.enabled;

        // Transform config
        document.getElementById('client-remove-system-name').checked = client.config.transformConfig.removeSystemName;
        document.getElementById('client-convert-dot-to-slash').checked = client.config.transformConfig.convertDotToSlash;
        document.getElementById('client-convert-underscore-to-slash').checked = client.config.transformConfig.convertUnderscoreToSlash;
        document.getElementById('client-regex-pattern').value = client.config.transformConfig.regexPattern || '';
        document.getElementById('client-regex-replacement').value = client.config.transformConfig.regexReplacement || '';

        // Load addresses
        this.clearAddresses();
        if (client.config.addresses && client.config.addresses.length > 0) {
            client.config.addresses.forEach(address => {
                this.addAddressRow(address);
            });
        }

        // Update modal title and button
        document.querySelector('#add-client-modal .modal-header h3').textContent = 'Edit WinCC OA Bridge';
        document.getElementById('modal-submit-btn').textContent = 'Update Bridge';
        document.getElementById('modal-submit-btn').onclick = () => updateClient();
    }

    async updateClient() {
        const form = document.getElementById('add-client-form');
        if (!form.checkValidity()) {
            form.reportValidity();
            return;
        }

        const addresses = this.getAddressesFromForm();
        if (addresses.length === 0) {
            this.showError('Please add at least one query');
            return;
        }

        const clientData = {
            name: this.editingClientName, // Use original name
            namespace: document.getElementById('client-namespace').value.trim(),
            nodeId: document.getElementById('client-node').value,
            enabled: document.getElementById('client-enabled').checked,
            config: {
                graphqlEndpoint: document.getElementById('client-graphql-endpoint').value.trim(),
                websocketEndpoint: document.getElementById('client-websocket-endpoint').value.trim() || null,
                username: document.getElementById('client-username').value.trim() || null,
                password: document.getElementById('client-password').value || null,
                token: document.getElementById('client-token').value.trim() || null,
                reconnectDelay: parseInt(document.getElementById('client-reconnect-delay').value),
                connectionTimeout: parseInt(document.getElementById('client-connection-timeout').value),
                messageFormat: document.getElementById('client-message-format').value,
                transformConfig: {
                    removeSystemName: document.getElementById('client-remove-system-name').checked,
                    convertDotToSlash: document.getElementById('client-convert-dot-to-slash').checked,
                    convertUnderscoreToSlash: document.getElementById('client-convert-underscore-to-slash').checked,
                    regexPattern: document.getElementById('client-regex-pattern').value.trim() || null,
                    regexReplacement: document.getElementById('client-regex-replacement').value.trim() || null
                },
                addresses: addresses
            }
        };

        try {
            const mutation = `
                mutation UpdateWinCCOaClient($input: WinCCOaClientInput!) {
                    updateWinCCOaClient(input: $input) {
                        success
                        errors
                        client {
                            name
                            enabled
                        }
                    }
                }
            `;

            const result = await this.client.query(mutation, { input: clientData });

            if (result.updateWinCCOaClient.success) {
                this.hideAddClientModal();
                await this.loadClients();
                this.showSuccess(`Bridge "${clientData.name}" updated successfully`);
            } else {
                const errors = result.updateWinCCOaClient.errors || ['Unknown error'];
                this.showError('Failed to update bridge: ' + errors.join(', '));
            }

        } catch (error) {
            console.error('Error updating client:', error);
            this.showError('Failed to update bridge: ' + error.message);
        }
    }

    async toggleClient(clientName, enabled) {
        try {
            const mutation = `
                mutation ToggleWinCCOaClient($name: String!, $enabled: Boolean!) {
                    toggleWinCCOaClient(name: $name, enabled: $enabled) {
                        success
                        errors
                        client {
                            name
                            enabled
                        }
                    }
                }
            `;

            const result = await this.client.query(mutation, { name: clientName, enabled });

            if (result.toggleWinCCOaClient.success) {
                await this.loadClients();
                this.showSuccess(`Bridge "${clientName}" ${enabled ? 'started' : 'stopped'} successfully`);
            } else {
                const errors = result.toggleWinCCOaClient.errors || ['Unknown error'];
                this.showError('Failed to toggle bridge: ' + errors.join(', '));
            }

        } catch (error) {
            console.error('Error toggling client:', error);
            this.showError('Failed to toggle bridge: ' + error.message);
        }
    }

    deleteClient(clientName) {
        this.deleteClientName = clientName;
        document.getElementById('delete-client-name').textContent = clientName;
        this.showConfirmDeleteModal();
    }

    async confirmDeleteClient() {
        if (!this.deleteClientName) return;

        try {
            const mutation = `
                mutation DeleteWinCCOaClient($name: String!) {
                    deleteWinCCOaClient(name: $name)
                }
            `;

            const result = await this.client.query(mutation, { name: this.deleteClientName });

            if (result.deleteWinCCOaClient) {
                this.hideConfirmDeleteModal();
                await this.loadClients();
                this.showSuccess(`Bridge "${this.deleteClientName}" deleted successfully`);
            } else {
                this.showError('Failed to delete bridge');
            }

        } catch (error) {
            console.error('Error deleting client:', error);
            this.showError('Failed to delete bridge: ' + error.message);
        }

        this.deleteClientName = null;
    }

    // UI Helper Methods
    showAddClientModal() {
        // Reset editing state
        this.editingClientName = null;

        document.getElementById('add-client-modal').style.display = 'flex';
        document.getElementById('add-client-form').reset();
        document.getElementById('client-name').disabled = false;
        document.getElementById('client-enabled').checked = true;
        document.getElementById('client-remove-system-name').checked = true;
        document.getElementById('client-convert-dot-to-slash').checked = true;
        document.getElementById('client-message-format').value = 'JSON_ISO';

        // Clear addresses
        this.clearAddresses();

        // Reset modal title and button
        document.querySelector('#add-client-modal .modal-header h3').textContent = 'Add WinCC OA Bridge';
        document.getElementById('modal-submit-btn').textContent = 'Add Bridge';
        document.getElementById('modal-submit-btn').onclick = () => addClient();
    }

    hideAddClientModal() {
        document.getElementById('add-client-modal').style.display = 'none';
        this.editingClientName = null;
        document.getElementById('client-name').disabled = false;
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

    async refreshClients() {
        await this.loadClients();
    }
}

// Global functions for onclick handlers
function showAddClientModal() {
    winCCOaClientManager.showAddClientModal();
}

function hideAddClientModal() {
    winCCOaClientManager.hideAddClientModal();
}

function addClient() {
    winCCOaClientManager.addClient();
}

function updateClient() {
    winCCOaClientManager.updateClient();
}

function hideConfirmDeleteModal() {
    winCCOaClientManager.hideConfirmDeleteModal();
}

function confirmDeleteClient() {
    winCCOaClientManager.confirmDeleteClient();
}

function refreshClients() {
    winCCOaClientManager.refreshClients();
}

function addAddressRow() {
    winCCOaClientManager.addAddressRow();
}

// Initialize when DOM is loaded
let winCCOaClientManager;
document.addEventListener('DOMContentLoaded', () => {
    winCCOaClientManager = new WinCCOaClientManager();
});
