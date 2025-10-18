// WinCC OA Client Detail Management JavaScript

class WinCCOaClientDetailManager {
    constructor() {
        this.client = new GraphQLDashboardClient('/graphql');
        this.clientData = null;
        this.clusterNodes = [];
        this.clientName = null;
        this.isNewMode = false;
        this.deleteQueryName = null;
        this.editingQueryIndex = null;
        this.queries = [];
        this.hasUnsavedChanges = false;
        this.init();
    }

    async init() {
        console.log('Initializing WinCC OA Client Detail Manager...');

        // Warn user before leaving page with unsaved changes
        window.addEventListener('beforeunload', (e) => {
            if (this.hasUnsavedChanges) {
                e.preventDefault();
                e.returnValue = 'You have unsaved changes. Are you sure you want to leave?';
                return e.returnValue;
            }
        });

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

        // Get client name from URL parameters
        const urlParams = new URLSearchParams(window.location.search);
        this.clientName = urlParams.get('client');
        this.isNewMode = urlParams.get('new') === 'true';

        if (!this.clientName && !this.isNewMode) {
            this.showError('No client specified');
            return;
        }

        // Load data
        await this.loadClusterNodes();

        if (this.isNewMode) {
            this.setupNewClient();
        } else {
            await this.loadClient();
        }
    }

    async loadClusterNodes() {
        try {
            const query = `
                query GetBrokers {
                    brokers {
                        nodeId
                        isCurrent
                    }
                }
            `;

            const result = await this.client.query(query);
            this.clusterNodes = result.brokers || [];

            // Populate node selector
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

    setupNewClient() {
        // Update page title
        document.getElementById('client-title').textContent = 'New WinCC OA Client';
        document.getElementById('client-subtitle').textContent = 'Configure a new WinCC OA GraphQL connection';

        // Set default values
        document.getElementById('client-enabled').checked = true;
        document.getElementById('client-remove-system-name').checked = true;
        document.getElementById('client-convert-dot-to-slash').checked = true;
        document.getElementById('client-message-format').value = 'JSON_ISO';
        document.getElementById('client-reconnect-delay').value = '5000';
        document.getElementById('client-connection-timeout').value = '10000';

        // Initialize empty queries array
        this.queries = [];

        // Hide queries section in new mode (will be available after saving)
        const queriesSection = document.getElementById('queries-section');
        if (queriesSection) {
            queriesSection.style.display = 'none';
        }

        // Show content
        document.getElementById('client-content').style.display = 'block';

        // Update save button text
        document.getElementById('save-client-btn').textContent = 'Create Client';
    }

    async loadClient() {
        this.showLoading(true);
        this.hideError();

        try {
            const query = `
                query GetWinCCOaClients($name: String!) {
                    winCCOaClients(name: $name) {
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

            const result = await this.client.query(query, { name: this.clientName });
            this.clientData = result.winCCOaClients && result.winCCOaClients.length > 0 ? result.winCCOaClients[0] : null;

            if (!this.clientData) {
                this.showError(`Client "${this.clientName}" not found`);
                return;
            }

            this.renderClient();

        } catch (error) {
            console.error('Error loading client:', error);
            this.showError('Failed to load client: ' + error.message);
        } finally {
            this.showLoading(false);
        }
    }

    renderClient() {
        if (!this.clientData) return;

        // Update page title
        document.getElementById('client-title').textContent = `WinCC OA Client: ${this.clientData.name}`;
        document.getElementById('client-subtitle').textContent = `${this.clientData.namespace} - ${this.clientData.config.graphqlEndpoint}`;

        // Basic information
        document.getElementById('client-name').value = this.clientData.name;
        document.getElementById('client-name').disabled = true; // Can't change name in edit mode
        document.getElementById('client-namespace').value = this.clientData.namespace;
        document.getElementById('client-graphql-endpoint').value = this.clientData.config.graphqlEndpoint;
        document.getElementById('client-websocket-endpoint').value = this.clientData.config.websocketEndpoint || '';
        document.getElementById('client-node').value = this.clientData.nodeId;
        document.getElementById('client-message-format').value = this.clientData.config.messageFormat;
        document.getElementById('client-enabled').checked = this.clientData.enabled;

        // Authentication
        document.getElementById('client-username').value = this.clientData.config.username || '';
        document.getElementById('client-password').value = ''; // Don't show existing password
        document.getElementById('client-token').value = ''; // Don't show existing token

        // Connection settings
        document.getElementById('client-reconnect-delay').value = this.clientData.config.reconnectDelay;
        document.getElementById('client-connection-timeout').value = this.clientData.config.connectionTimeout;

        // Topic transformation
        const transformConfig = this.clientData.config.transformConfig;
        document.getElementById('client-remove-system-name').checked = transformConfig.removeSystemName;
        document.getElementById('client-convert-dot-to-slash').checked = transformConfig.convertDotToSlash;
        document.getElementById('client-convert-underscore-to-slash').checked = transformConfig.convertUnderscoreToSlash;
        document.getElementById('client-regex-pattern').value = transformConfig.regexPattern || '';
        document.getElementById('client-regex-replacement').value = transformConfig.regexReplacement || '';

        // Queries
        this.queries = this.clientData.config.addresses || [];
        this.renderQueries();

        // Show content
        document.getElementById('client-content').style.display = 'block';
    }

    renderQueries() {
        const tbody = document.getElementById('queries-table-body');
        const noQueries = document.getElementById('no-queries');
        const queriesTable = document.getElementById('queries-table');

        if (!tbody) return;

        tbody.innerHTML = '';

        if (!this.queries || this.queries.length === 0) {
            queriesTable.style.display = 'none';
            noQueries.style.display = 'block';
            return;
        }

        queriesTable.style.display = 'table';
        noQueries.style.display = 'none';

        this.queries.forEach((query, index) => {
            const row = document.createElement('tr');

            const optionsBadges = [];
            if (query.answer) optionsBadges.push('Answer');
            if (query.retained) optionsBadges.push('Retained');
            const optionsText = optionsBadges.length > 0 ? optionsBadges.join(', ') : 'None';

            row.innerHTML = `
                <td>
                    <div class="query-value" title="${this.escapeAttr(query.query)}">
                        ${this.escapeHtml(query.query)}
                    </div>
                </td>
                <td>
                    <div class="topic-value">${this.escapeHtml(query.topic)}</div>
                </td>
                <td>
                    <div class="description-value">${this.escapeHtml(query.description || '-')}</div>
                </td>
                <td>
                    <div class="options-value">${optionsText}</div>
                </td>
                <td>
                    <div class="action-buttons">
                        <button class="btn-action btn-delete"
                                onclick="clientDetailManager.deleteQuery(${index})"
                                title="Delete Query">
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

    showAddQueryModal() {
        this.editingQueryIndex = null;
        document.getElementById('add-query-modal').style.display = 'flex';
        // Reset form
        document.getElementById('add-query-form').reset();
        document.getElementById('query-answer').checked = false;
        document.getElementById('query-retained').checked = false;

        // Update modal title and button
        document.querySelector('#add-query-modal .modal-header h3').textContent = 'Add Query';
        document.getElementById('add-query-btn').textContent = 'Add Query';
    }

    hideAddQueryModal() {
        document.getElementById('add-query-modal').style.display = 'none';
        this.editingQueryIndex = null;
    }

    editQuery(index) {
        const query = this.queries[index];
        if (!query) return;

        this.editingQueryIndex = index;

        // Populate form with existing values
        document.getElementById('query-query').value = query.query || '';
        document.getElementById('query-topic').value = query.topic || '';
        document.getElementById('query-description').value = query.description || '';
        document.getElementById('query-answer').checked = query.answer || false;
        document.getElementById('query-retained').checked = query.retained || false;

        // Update modal title and button
        document.querySelector('#add-query-modal .modal-header h3').textContent = 'Edit Query';
        document.getElementById('add-query-btn').textContent = 'Update Query';

        // Show modal
        document.getElementById('add-query-modal').style.display = 'flex';
    }

    async addQuery() {
        const form = document.getElementById('add-query-form');
        if (!form.checkValidity()) {
            form.reportValidity();
            return;
        }

        const queryData = {
            query: document.getElementById('query-query').value.trim(),
            topic: document.getElementById('query-topic').value.trim(),
            description: document.getElementById('query-description').value.trim(),
            answer: document.getElementById('query-answer').checked,
            retained: document.getElementById('query-retained').checked
        };

        this.hideAddQueryModal();

        // Save immediately to server using addWinCCOaClientAddress mutation
        if (this.isNewMode) {
            this.showError('Please save the client first before adding queries');
            return;
        }

        try {
            const mutation = `
                mutation AddWinCCOaClientAddress($deviceName: String!, $input: WinCCOaAddressInput!) {
                    winCCOaDevice {
                        addAddress(deviceName: $deviceName, input: $input) {
                            success
                            errors
                            client {
                                name
                                enabled
                            }
                        }
                    }
                }
            `;

            const result = await this.client.query(mutation, {
                deviceName: this.clientName,
                input: queryData
            });

            if (result.winCCOaDevice.addAddress.success) {
                // Reload client data to get updated addresses
                await this.loadClient();
                this.showSuccess('Query added successfully');
            } else {
                const errors = result.winCCOaDevice.addAddress.errors || ['Unknown error'];
                this.showError('Failed to add query: ' + errors.join(', '));
            }
        } catch (error) {
            console.error('Error adding query:', error);
            this.showError('Failed to add query: ' + error.message);
        }
    }

    deleteQuery(index) {
        if (index < 0 || index >= this.queries.length) {
            this.showError('Invalid query index');
            return;
        }

        this.deleteQueryIndex = index;
        const query = this.queries[index];
        document.getElementById('delete-query-name').textContent = query.query;
        this.showConfirmDeleteQueryModal();
    }

    async confirmDeleteQuery() {
        if (this.deleteQueryIndex !== undefined && this.deleteQueryIndex !== null && this.deleteQueryIndex >= 0) {
            const query = this.queries[this.deleteQueryIndex];
            if (!query) {
                this.showError('Query not found');
                this.deleteQueryIndex = null;
                return;
            }

            this.hideConfirmDeleteQueryModal();

            // Delete immediately on server using deleteWinCCOaClientAddress mutation
            try {
                const mutation = `
                    mutation DeleteWinCCOaClientAddress($deviceName: String!, $query: String!) {
                        winCCOaDevice {
                            deleteAddress(deviceName: $deviceName, query: $query) {
                                success
                                errors
                                client {
                                    name
                                    enabled
                                }
                            }
                        }
                    }
                `;

                const result = await this.client.query(mutation, {
                    deviceName: this.clientName,
                    query: query.query
                });

                if (result.winCCOaDevice.deleteAddress.success) {
                    // Reload client data to get updated addresses
                    await this.loadClient();
                    this.showSuccess('Query deleted successfully');
                } else {
                    const errors = result.winCCOaDevice.deleteAddress.errors || ['Unknown error'];
                    this.showError('Failed to delete query: ' + errors.join(', '));
                }
            } catch (error) {
                console.error('Error deleting query:', error);
                this.showError('Failed to delete query: ' + error.message);
            }
        }
        this.deleteQueryIndex = null;
    }

    updateSaveButtonState() {
        const saveBtn = document.getElementById('save-client-btn');
        if (!saveBtn) return;

        if (this.hasUnsavedChanges) {
            saveBtn.style.animation = 'pulse 2s infinite';
            saveBtn.innerHTML = `
                <svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor">
                    <path d="M17 3H5c-1.11 0-2 .9-2 2v14c0 1.1.89 2 2 2h14c1.1 0 2-.9 2-2V7l-4-4zm-5 16c-1.66 0-3-1.34-3-3s1.34-3 3-3 3 1.34 3 3-1.34 3-3 3zm3-10H5V5h10v4z"/>
                </svg>
                Save Client (Unsaved Changes)
            `;
        } else {
            saveBtn.style.animation = '';
            saveBtn.innerHTML = `
                <svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor">
                    <path d="M17 3H5c-1.11 0-2 .9-2 2v14c0 1.1.89 2 2 2h14c1.1 0 2-.9 2-2V7l-4-4zm-5 16c-1.66 0-3-1.34-3-3s1.34-3 3-3 3 1.34 3 3-1.34 3-3 3zm3-10H5V5h10v4z"/>
                </svg>
                Save Client
            `;
        }
    }

    async saveClient() {
        // Validate required fields
        const name = document.getElementById('client-name').value.trim();
        const namespace = document.getElementById('client-namespace').value.trim();
        const graphqlEndpoint = document.getElementById('client-graphql-endpoint').value.trim();
        const nodeId = document.getElementById('client-node').value;

        if (!name || !namespace || !graphqlEndpoint || !nodeId) {
            this.showError('Please fill in all required fields');
            return;
        }

        // Only require at least one query in edit mode (not in new mode)
        if (!this.isNewMode && this.queries.length === 0) {
            this.showError('Please add at least one query');
            return;
        }

        const clientData = {
            name: name,
            namespace: namespace,
            nodeId: nodeId,
            enabled: document.getElementById('client-enabled').checked,
            config: {
                graphqlEndpoint: graphqlEndpoint,
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
                addresses: this.queries
            }
        };

        try {
            if (this.isNewMode) {
                await this.createClient(clientData);
            } else {
                await this.updateClient(clientData);
            }
        } catch (error) {
            console.error('Error saving client:', error);
            this.showError('Failed to save client: ' + error.message);
        }
    }

    async createClient(clientData) {
        const mutation = `
            mutation CreateWinCCOaClient($input: WinCCOaClientInput!) {
                winCCOaDevice {
                    create(input: $input) {
                        success
                        errors
                        client {
                            name
                            enabled
                        }
                    }
                }
            }
        `;

        const result = await this.client.query(mutation, { input: clientData });

        if (result.winCCOaDevice.create.success) {
            this.hasUnsavedChanges = false;
            this.updateSaveButtonState();
            this.showSuccess(`Client "${clientData.name}" created successfully. Redirecting to edit page...`);
            // Redirect to edit mode so the user can add queries
            setTimeout(() => {
                window.location.href = '/pages/winccoa-client-detail.html?client=' + encodeURIComponent(clientData.name);
            }, 1500);
        } else {
            const errors = result.winCCOaDevice.create.errors || ['Unknown error'];
            this.showError('Failed to create client: ' + errors.join(', '));
        }
    }

    async updateClient(clientData) {
        const mutation = `
            mutation UpdateWinCCOaClient($name: String!, $input: WinCCOaClientInput!) {
                winCCOaDevice {
                    update(name: $name, input: $input) {
                        success
                        errors
                        client {
                            name
                            enabled
                        }
                    }
                }
            }
        `;

        const result = await this.client.query(mutation, {
            name: this.clientName,
            input: clientData
        });

        if (result.winCCOaDevice.update.success) {
            this.hasUnsavedChanges = false;
            this.updateSaveButtonState();
            this.showSuccess(`Client "${clientData.name}" updated successfully`);
            // Reload the client data
            await this.loadClient();
        } else {
            const errors = result.winCCOaDevice.update.errors || ['Unknown error'];
            this.showError('Failed to update client: ' + errors.join(', '));
        }
    }

    // UI Helper Methods
    showConfirmDeleteQueryModal() {
        document.getElementById('confirm-delete-query-modal').style.display = 'flex';
    }

    hideConfirmDeleteQueryModal() {
        document.getElementById('confirm-delete-query-modal').style.display = 'none';
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

    escapeAttr(text) {
        if (!text) return '';
        return text
            .replace(/&/g, '&amp;')
            .replace(/"/g, '&quot;')
            .replace(/'/g, '&#39;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;');
    }

    goBack() {
        if (this.hasUnsavedChanges) {
            if (!confirm('You have unsaved changes. Are you sure you want to leave without saving?')) {
                return;
            }
        }
        window.location.href = '/pages/winccoa-clients.html';
    }
}

// Global functions for onclick handlers
function showAddQueryModal() {
    clientDetailManager.showAddQueryModal();
}

function hideAddQueryModal() {
    clientDetailManager.hideAddQueryModal();
}

function addQuery() {
    clientDetailManager.addQuery();
}

function hideConfirmDeleteQueryModal() {
    clientDetailManager.hideConfirmDeleteQueryModal();
}

function confirmDeleteQuery() {
    clientDetailManager.confirmDeleteQuery();
}

function saveClient() {
    clientDetailManager.saveClient();
}

function goBack() {
    clientDetailManager.goBack();
}

// Initialize when DOM is loaded
let clientDetailManager;
document.addEventListener('DOMContentLoaded', () => {
    clientDetailManager = new WinCCOaClientDetailManager();
});
