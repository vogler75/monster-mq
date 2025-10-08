// WinCC Unified Client Detail Management JavaScript

class WinCCUaClientDetailManager {
    constructor() {
        this.client = new GraphQLDashboardClient('/graphql');
        this.clientData = null;
        this.clusterNodes = [];
        this.clientName = null;
        this.isNewMode = false;
        this.deleteAddressName = null;
        this.editingAddressIndex = null;
        this.addresses = [];
        this.hasUnsavedChanges = false;
        this.init();
    }

    async init() {
        console.log('Initializing WinCC Unified Client Detail Manager...');

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
                query GetClusterNodes {
                    clusterNodes {
                        nodeId
                        isCurrent
                    }
                }
            `;

            const result = await this.client.query(query);
            this.clusterNodes = result.clusterNodes || [];

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
        document.getElementById('client-title').textContent = 'New WinCC Unified Client';
        document.getElementById('client-subtitle').textContent = 'Configure a new WinCC Unified GraphQL connection';

        // Set default values
        document.getElementById('client-enabled').checked = true;
        document.getElementById('client-convert-dot-to-slash').checked = true;
        document.getElementById('client-message-format').value = 'JSON_ISO';
        document.getElementById('client-reconnect-delay').value = '5000';
        document.getElementById('client-connection-timeout').value = '10000';

        // Initialize empty addresses array
        this.addresses = [];

        // Hide addresses section in new mode (will be available after saving)
        const addressesSection = document.getElementById('addresses-section');
        if (addressesSection) {
            addressesSection.style.display = 'none';
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
                query GetWinCCUaClient($name: String!) {
                    winCCUaClient(name: $name) {
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
                                convertDotToSlash
                                convertUnderscoreToSlash
                                regexPattern
                                regexReplacement
                            }
                            addresses {
                                type
                                topic
                                description
                                nameFilters
                                systemNames
                                filterString
                                retained
                            }
                        }
                    }
                }
            `;

            const result = await this.client.query(query, { name: this.clientName });
            this.clientData = result.winCCUaClient;

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
        document.getElementById('client-title').textContent = `WinCC Unified Client: ${this.clientData.name}`;
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

        // Show "Change Password" checkbox in edit mode
        const changePasswordGroup = document.getElementById('change-password-group');
        const changePasswordCheckbox = document.getElementById('change-password-checkbox');
        const passwordField = document.getElementById('client-password');

        if (!this.isNewClient) {
            changePasswordGroup.style.display = 'block';
            passwordField.disabled = true;
            passwordField.required = false;
            passwordField.placeholder = '••••••••';

            changePasswordCheckbox.addEventListener('change', (e) => {
                passwordField.disabled = !e.target.checked;
                passwordField.required = e.target.checked;
                passwordField.placeholder = e.target.checked ? 'Enter new password' : '••••••••';
                if (!e.target.checked) {
                    passwordField.value = '';
                }
            });
        } else {
            changePasswordGroup.style.display = 'none';
            passwordField.disabled = false;
            passwordField.required = true;
        }

        // Connection settings
        document.getElementById('client-reconnect-delay').value = this.clientData.config.reconnectDelay;
        document.getElementById('client-connection-timeout').value = this.clientData.config.connectionTimeout;

        // Topic transformation
        const transformConfig = this.clientData.config.transformConfig;
        document.getElementById('client-convert-dot-to-slash').checked = transformConfig.convertDotToSlash;
        document.getElementById('client-convert-underscore-to-slash').checked = transformConfig.convertUnderscoreToSlash;
        document.getElementById('client-regex-pattern').value = transformConfig.regexPattern || '';
        document.getElementById('client-regex-replacement').value = transformConfig.regexReplacement || '';

        // Addresses
        this.addresses = this.clientData.config.addresses || [];
        this.renderAddresses();

        // Show content
        document.getElementById('client-content').style.display = 'block';
    }

    renderAddresses() {
        const tbody = document.getElementById('addresses-table-body');
        const noAddresses = document.getElementById('no-addresses');
        const addressesTable = document.getElementById('addresses-table');

        if (!tbody) return;

        tbody.innerHTML = '';

        if (!this.addresses || this.addresses.length === 0) {
            addressesTable.style.display = 'none';
            noAddresses.style.display = 'block';
            return;
        }

        addressesTable.style.display = 'table';
        noAddresses.style.display = 'none';

        this.addresses.forEach((address, index) => {
            const row = document.createElement('tr');

            const optionsBadges = [];
            if (address.type === 'TAG_VALUES' && address.nameFilters) {
                optionsBadges.push(`Filters: ${this.escapeHtml(address.nameFilters.join(', '))}`);
            }
            if (address.type === 'ACTIVE_ALARMS') {
                if (address.systemNames) optionsBadges.push(`Systems: ${this.escapeHtml(address.systemNames.join(', '))}`);
                if (address.filterString) optionsBadges.push(`Filter: ${this.escapeHtml(address.filterString)}`);
            }
            if (address.retained) optionsBadges.push('Retained');
            const optionsText = optionsBadges.length > 0 ? optionsBadges.join(', ') : 'None';

            row.innerHTML = `
                <td>
                    <div class="type-value">${this.escapeHtml(address.type)}</div>
                </td>
                <td>
                    <div class="topic-value">${this.escapeHtml(address.topic)}</div>
                </td>
                <td>
                    <div class="description-value">${this.escapeHtml(address.description || '-')}</div>
                </td>
                <td>
                    <div class="options-value" title="${this.escapeAttr(optionsText)}">${optionsText}</div>
                </td>
                <td>
                    <div class="action-buttons">
                        <button class="btn-action btn-delete"
                                onclick="clientDetailManager.deleteAddress(${index})"
                                title="Delete Subscription">
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

    updateAddressFormFields() {
        const type = document.getElementById('address-type').value;
        const browseArgsGroup = document.getElementById('browse-args-group');
        const systemNamesGroup = document.getElementById('system-names-group');
        const filterStringGroup = document.getElementById('filter-string-group');

        // Hide all conditional fields
        browseArgsGroup.classList.remove('visible');
        systemNamesGroup.classList.remove('visible');
        filterStringGroup.classList.remove('visible');

        // Show relevant fields based on type
        if (type === 'TAG_VALUES') {
            browseArgsGroup.classList.add('visible');
        } else if (type === 'ACTIVE_ALARMS') {
            systemNamesGroup.classList.add('visible');
            filterStringGroup.classList.add('visible');
        }
    }

    showAddAddressModal() {
        this.editingAddressIndex = null;
        document.getElementById('add-address-modal').style.display = 'flex';
        // Reset form
        document.getElementById('add-address-form').reset();
        document.getElementById('address-retained').checked = false;

        // Hide all conditional fields
        this.updateAddressFormFields();

        // Update modal title and button
        document.querySelector('#add-address-modal .modal-header h3').textContent = 'Add Subscription';
        document.getElementById('add-address-btn').textContent = 'Add Subscription';
    }

    hideAddAddressModal() {
        document.getElementById('add-address-modal').style.display = 'none';
        this.editingAddressIndex = null;
    }

    editAddress(index) {
        const address = this.addresses[index];
        if (!address) return;

        this.editingAddressIndex = index;

        // Populate form with existing values
        document.getElementById('address-type').value = address.type || '';
        document.getElementById('address-topic').value = address.topic || '';
        document.getElementById('address-description').value = address.description || '';
        document.getElementById('address-browse-arguments').value = (address.nameFilters && address.nameFilters.length > 0) ? address.nameFilters.join(', ') : '';
        document.getElementById('address-system-names').value = (address.systemNames && address.systemNames.length > 0) ? address.systemNames.join(', ') : '';
        document.getElementById('address-filter-string').value = address.filterString || '';
        document.getElementById('address-retained').checked = address.retained || false;

        // Update conditional fields
        this.updateAddressFormFields();

        // Update modal title and button
        document.querySelector('#add-address-modal .modal-header h3').textContent = 'Edit Subscription';
        document.getElementById('add-address-btn').textContent = 'Update Subscription';

        // Show modal
        document.getElementById('add-address-modal').style.display = 'flex';
    }

    async addAddress() {
        const form = document.getElementById('add-address-form');
        if (!form.checkValidity()) {
            form.reportValidity();
            return;
        }

        const type = document.getElementById('address-type').value.trim();
        const addressData = {
            type: type,
            topic: document.getElementById('address-topic').value.trim(),
            description: document.getElementById('address-description').value.trim(),
            retained: document.getElementById('address-retained').checked
        };

        // Add type-specific fields
        if (type === 'TAG_VALUES') {
            const nameFiltersValue = document.getElementById('address-browse-arguments').value.trim();
            addressData.nameFilters = nameFiltersValue ? nameFiltersValue.split(',').map(f => f.trim()).filter(f => f.length > 0) : null;
        } else if (type === 'ACTIVE_ALARMS') {
            const systemNamesValue = document.getElementById('address-system-names').value.trim();
            addressData.systemNames = systemNamesValue ? systemNamesValue.split(',').map(s => s.trim()).filter(s => s.length > 0) : null;
            addressData.filterString = document.getElementById('address-filter-string').value.trim() || null;
        }

        this.hideAddAddressModal();

        // Save immediately to server using addWinCCUaClientAddress mutation
        if (this.isNewMode) {
            this.showError('Please save the client first before adding subscriptions');
            return;
        }

        try {
            const mutation = `
                mutation AddWinCCUaClientAddress($deviceName: String!, $input: WinCCUaAddressInput!) {
                    winCCUaDevice {
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
                input: addressData
            });

            if (result.winCCUaDevice.addAddress.success) {
                // Reload client data to get updated addresses
                await this.loadClient();
                this.showSuccess('Subscription added successfully');
            } else {
                const errors = result.winCCUaDevice.addAddress.errors || ['Unknown error'];
                this.showError('Failed to add subscription: ' + errors.join(', '));
            }
        } catch (error) {
            console.error('Error adding subscription:', error);
            this.showError('Failed to add subscription: ' + error.message);
        }
    }

    deleteAddress(index) {
        if (index < 0 || index >= this.addresses.length) {
            this.showError('Invalid address index');
            return;
        }

        this.deleteAddressIndex = index;
        const address = this.addresses[index];
        document.getElementById('delete-address-name').textContent = address.topic;
        this.showConfirmDeleteAddressModal();
    }

    async confirmDeleteAddress() {
        if (this.deleteAddressIndex !== undefined && this.deleteAddressIndex !== null && this.deleteAddressIndex >= 0) {
            const address = this.addresses[this.deleteAddressIndex];
            if (!address) {
                this.showError('Subscription not found');
                this.deleteAddressIndex = null;
                return;
            }

            this.hideConfirmDeleteAddressModal();

            // Delete immediately on server using deleteWinCCUaClientAddress mutation
            try {
                const mutation = `
                    mutation DeleteWinCCUaClientAddress($deviceName: String!, $topic: String!) {
                        winCCUaDevice {
                            deleteAddress(deviceName: $deviceName, topic: $topic) {
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
                    topic: address.topic
                });

                if (result.winCCUaDevice.deleteAddress.success) {
                    // Reload client data to get updated addresses
                    await this.loadClient();
                    this.showSuccess('Subscription deleted successfully');
                } else {
                    const errors = result.winCCUaDevice.deleteAddress.errors || ['Unknown error'];
                    this.showError('Failed to delete subscription: ' + errors.join(', '));
                }
            } catch (error) {
                console.error('Error deleting subscription:', error);
                this.showError('Failed to delete subscription: ' + error.message);
            }
        }
        this.deleteAddressIndex = null;
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

    getPasswordForSave() {
        // In new mode, always get password from field
        if (this.isNewMode) {
            const password = document.getElementById('client-password').value;
            if (!password) {
                throw new Error('Password is required for new clients');
            }
            return password;
        }

        // In edit mode, only include password if "Change Password" is checked
        const changePasswordCheckbox = document.getElementById('change-password-checkbox');
        if (changePasswordCheckbox && changePasswordCheckbox.checked) {
            const password = document.getElementById('client-password').value;
            if (!password) {
                throw new Error('Please enter a new password');
            }
            return password;
        }

        // Return empty string if not changing password - backend will keep existing password
        return '';
    }

    async saveClient() {
        // Validate required fields
        const name = document.getElementById('client-name').value.trim();
        const namespace = document.getElementById('client-namespace').value.trim();
        const graphqlEndpoint = document.getElementById('client-graphql-endpoint').value.trim();
        const nodeId = document.getElementById('client-node').value;
        const username = document.getElementById('client-username').value.trim();

        if (!name || !namespace || !graphqlEndpoint || !nodeId || !username) {
            this.showError('Please fill in all required fields');
            return;
        }

        // Validate password
        try {
            const password = this.getPasswordForSave();
            if (this.isNewMode && !password) {
                this.showError('Password is required for new clients');
                return;
            }
        } catch (error) {
            this.showError(error.message);
            return;
        }

        // Only require at least one address in edit mode (not in new mode)
        if (!this.isNewMode && this.addresses.length === 0) {
            this.showError('Please add at least one subscription');
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
                username: username,
                password: this.getPasswordForSave(),
                reconnectDelay: parseInt(document.getElementById('client-reconnect-delay').value),
                connectionTimeout: parseInt(document.getElementById('client-connection-timeout').value),
                messageFormat: document.getElementById('client-message-format').value,
                transformConfig: {
                    convertDotToSlash: document.getElementById('client-convert-dot-to-slash').checked,
                    convertUnderscoreToSlash: document.getElementById('client-convert-underscore-to-slash').checked,
                    regexPattern: document.getElementById('client-regex-pattern').value.trim() || null,
                    regexReplacement: document.getElementById('client-regex-replacement').value.trim() || null
                }
                // Note: addresses are managed separately via addAddress/deleteAddress mutations
                // and are automatically preserved during updates by the backend
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
            mutation CreateWinCCUaClient($input: WinCCUaClientInput!) {
                winCCUaDevice {
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

        if (result.winCCUaDevice.create.success) {
            this.hasUnsavedChanges = false;
            this.updateSaveButtonState();
            this.showSuccess(`Client "${clientData.name}" created successfully. Redirecting to edit page...`);
            // Redirect to edit mode so the user can add subscriptions
            setTimeout(() => {
                window.location.href = '/pages/winccua-client-detail.html?client=' + encodeURIComponent(clientData.name);
            }, 1500);
        } else {
            const errors = result.winCCUaDevice.create.errors || ['Unknown error'];
            this.showError('Failed to create client: ' + errors.join(', '));
        }
    }

    async updateClient(clientData) {
        const mutation = `
            mutation UpdateWinCCUaClient($name: String!, $input: WinCCUaClientInput!) {
                winCCUaDevice {
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

        if (result.winCCUaDevice.update.success) {
            this.hasUnsavedChanges = false;
            this.updateSaveButtonState();
            this.showSuccess(`Client "${clientData.name}" updated successfully`);
            // Reload the client data
            await this.loadClient();
        } else {
            const errors = result.winCCUaDevice.update.errors || ['Unknown error'];
            this.showError('Failed to update client: ' + errors.join(', '));
        }
    }

    // UI Helper Methods
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
        window.location.href = '/pages/winccua-clients.html';
    }
}

// Global functions for onclick handlers
function showAddAddressModal() {
    clientDetailManager.showAddAddressModal();
}

function hideAddAddressModal() {
    clientDetailManager.hideAddAddressModal();
}

function addAddress() {
    clientDetailManager.addAddress();
}

function hideConfirmDeleteAddressModal() {
    clientDetailManager.hideConfirmDeleteAddressModal();
}

function confirmDeleteAddress() {
    clientDetailManager.confirmDeleteAddress();
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
    clientDetailManager = new WinCCUaClientDetailManager();
});
