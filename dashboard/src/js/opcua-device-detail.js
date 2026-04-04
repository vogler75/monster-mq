// OPC UA Device Detail Management JavaScript

class OpcUaDeviceDetailManager {
    constructor() {
        this.client = new GraphQLDashboardClient();
        this.device = null;
        this.clusterNodes = [];
        this.deviceName = null;
        this.deleteAddressName = null;
        this.addressPage = 0;
        this.addressPageSize = 1000;
        this.init();
    }

    async init() {
        this.setupToggleListeners();

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

    setupToggleListeners() {
        const updatePasswordCheckbox = document.getElementById('device-update-password');
        const passwordFieldGroup = document.getElementById('password-field-group');
        if (updatePasswordCheckbox) {
            updatePasswordCheckbox.addEventListener('change', function() {
                passwordFieldGroup.style.display = this.checked ? 'block' : 'none';
                if (!this.checked) document.getElementById('device-password').value = '';
            });
        }

        const updateKeystorePasswordCheckbox = document.getElementById('cert-update-keystore-password');
        const keystorePasswordFieldGroup = document.getElementById('keystore-password-field-group');
        if (updateKeystorePasswordCheckbox) {
            updateKeystorePasswordCheckbox.addEventListener('change', function() {
                keystorePasswordFieldGroup.style.display = this.checked ? 'block' : 'none';
                if (!this.checked) document.getElementById('cert-keystore-password').value = '';
            });
        }

        const writeEnabledCheckbox = document.getElementById('write-enabled');
        const writeRrEnabledCheckbox = document.getElementById('write-rr-enabled');
        const writeConfigFields = document.getElementById('write-config-fields');
        const updateWriteFieldsVisibility = () => {
            const anyEnabled = writeEnabledCheckbox?.checked || writeRrEnabledCheckbox?.checked;
            writeConfigFields.style.display = anyEnabled ? 'block' : 'none';
            this.updateWriteTopicInfo();
        };
        if (writeEnabledCheckbox) writeEnabledCheckbox.addEventListener('change', updateWriteFieldsVisibility);
        if (writeRrEnabledCheckbox) writeRrEnabledCheckbox.addEventListener('change', updateWriteFieldsVisibility);

        // Update topic info when prefix fields change
        ['write-topic-prefix', 'write-request-topic-prefix', 'write-response-topic-prefix'].forEach(id => {
            const el = document.getElementById(id);
            if (el) el.addEventListener('input', () => this.updateWriteTopicInfo());
        });
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
                                monitoredItemsBatchSize
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
                            writeConfig {
                                enabled
                                requestResponseEnabled
                                topicPrefix
                                requestTopicPrefix
                                responseTopicPrefix
                                qos
                                writeTimeout
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
        document.getElementById('device-updated').textContent = this.formatDateTime(this.device.updatedAt);

        // Connection configuration
        document.getElementById('config-sampling').value = this.device.config.subscriptionSamplingInterval;
        document.getElementById('config-keepalive').value = this.device.config.keepAliveFailuresAllowed;
        document.getElementById('config-reconnect').value = this.device.config.reconnectDelay;
        document.getElementById('config-connection-timeout').value = this.device.config.connectionTimeout;
        document.getElementById('config-request-timeout').value = this.device.config.requestTimeout;
        document.getElementById('config-buffer-size').value = this.device.config.monitoringParameters.bufferSize;
        document.getElementById('config-monitoring-sampling').value = this.device.config.monitoringParameters.samplingInterval;
        document.getElementById('config-discard-oldest').checked = this.device.config.monitoringParameters.discardOldest;
        document.getElementById('config-batch-size').value = this.device.config.monitoringParameters.monitoredItemsBatchSize;

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

        // Write configuration
        const writeConfig = this.device.config.writeConfig;
        if (writeConfig) {
            document.getElementById('write-enabled').checked = writeConfig.enabled;
            document.getElementById('write-rr-enabled').checked = writeConfig.requestResponseEnabled;
            document.getElementById('write-topic-prefix').value = writeConfig.topicPrefix || 'write';
            document.getElementById('write-request-topic-prefix').value = writeConfig.requestTopicPrefix || 'request';
            document.getElementById('write-response-topic-prefix').value = writeConfig.responseTopicPrefix || 'response';
            document.getElementById('write-qos').value = writeConfig.qos != null ? writeConfig.qos : 1;
            document.getElementById('write-timeout').value = writeConfig.writeTimeout || 5000;
            document.getElementById('write-config-fields').style.display =
                (writeConfig.enabled || writeConfig.requestResponseEnabled) ? 'block' : 'none';
        }
        this.updateWriteTopicInfo();

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
        document.getElementById('config-batch-size').value = '1000';

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

        // Set write config defaults
        document.getElementById('write-enabled').checked = false;
        document.getElementById('write-rr-enabled').checked = false;
        document.getElementById('write-topic-prefix').value = 'write';
        document.getElementById('write-request-topic-prefix').value = 'request';
        document.getElementById('write-response-topic-prefix').value = 'response';
        document.getElementById('write-qos').value = '1';
        document.getElementById('write-timeout').value = '5000';
        document.getElementById('write-config-fields').style.display = 'none';

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

        const deleteBtn = document.getElementById('delete-btn');
        if (deleteBtn) deleteBtn.style.display = 'none';

        document.getElementById('device-content').style.display = 'block';
    }

    updateWriteTopicInfo() {
        const infoDiv = document.getElementById('write-topic-info');
        if (!infoDiv) return;

        const namespace = document.getElementById('device-namespace')?.value || '{namespace}';
        const prefix = document.getElementById('write-topic-prefix')?.value || 'write';
        const reqPrefix = document.getElementById('write-request-topic-prefix')?.value || 'request';
        const resPrefix = document.getElementById('write-response-topic-prefix')?.value || 'response';

        const ns = this.escapeHtml(namespace);
        const pf = this.escapeHtml(prefix);
        const rq = this.escapeHtml(reqPrefix);
        const rs = this.escapeHtml(resPrefix);
        const t = 'color:var(--monster-teal);';
        const p = 'color:var(--monster-purple);';

        infoDiv.innerHTML = `
            <strong style="color:var(--text-primary);">Topic Reference</strong><br><br>
            <strong>Fire &amp; Forget Write</strong> (no response):<br>
            <code style="${t}">${ns}/${pf}/{nodeId}</code>
            &nbsp; payload: <code>{"value": 42.5}</code> or <code>{"value": 42.5, "dataType": "Double"}</code><br><br>
            <strong>Request/Response Write</strong> (with status):<br>
            Publish: <code style="${t}">${ns}/${rq}/{nodeId}</code> &nbsp; payload: <code>{"value": 42.5}</code><br>
            Response: <code style="${p}">${ns}/${rs}/{nodeId}</code> &nbsp; <code>{"nodeId":"...","status":"Good","statusCode":0,"timestamp":"..."}</code><br><br>
            <strong>Request/Response Read</strong> (on-demand value):<br>
            Publish: <code style="${t}">${ns}/${rq}/{nodeId}</code> &nbsp; payload: <code>{}</code> (empty or no payload)<br>
            Response: <code style="${p}">${ns}/${rs}/{nodeId}</code> &nbsp; <code>{"nodeId":"...","value":42.5,"status":"Good","timestamp":"..."}</code><br><br>
            <strong>Batch</strong> (mixed read/write in one call):<br>
            Publish: <code style="${t}">${ns}/${rq}</code> or <code style="${t}">${ns}/${pf}</code> (writes only)<br>
            Payload: <code>[{"nodeId":"ns=2;i=1001"}, {"nodeId":"ns=2;i=1002","value":42.5}]</code><br>
            <span style="font-size:0.75rem;">Entries without <code>value</code> are reads, entries with <code>value</code> are writes.</span><br><br>
            <strong>dataType hints:</strong> Boolean, Byte, UByte, Int16, UInt16, Int32, UInt32, Int64, UInt64, Float, Double, String
        `;
    }

    renderAddresses() {
        const tbody = document.getElementById('addresses-table-body');
        const noAddresses = document.getElementById('no-addresses');
        const addressesTable = document.getElementById('addresses-table');

        if (!tbody) return;

        tbody.innerHTML = '';

        const allAddresses = this.device.config.addresses || [];
        if (allAddresses.length === 0) {
            addressesTable.style.display = 'none';
            noAddresses.style.display = 'block';
            this.renderAddressPagination(0, 0);
            return;
        }

        addressesTable.style.display = 'block';
        noAddresses.style.display = 'none';

        const totalPages = Math.ceil(allAddresses.length / this.addressPageSize);
        if (this.addressPage >= totalPages) this.addressPage = totalPages - 1;
        if (this.addressPage < 0) this.addressPage = 0;

        const start = this.addressPage * this.addressPageSize;
        const end = Math.min(start + this.addressPageSize, allAddresses.length);
        const pageAddresses = allAddresses.slice(start, end);

        this.renderAddressPagination(allAddresses.length, totalPages);

        pageAddresses.forEach(address => {
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
                    <div class="action-buttons" style="display:flex; gap:0.25rem;">
                        <ix-icon-button icon="pen" variant="primary" ghost size="16" title="Edit Address" onclick="deviceDetailManager.editAddress('${this.escapeHtml(address.address)}')"></ix-icon-button>
                        <ix-icon-button icon="trashcan" variant="primary" ghost size="16" class="btn-delete" title="Delete Address" onclick="deviceDetailManager.deleteAddress('${this.escapeHtml(address.address)}')"></ix-icon-button>
                    </div>
                </td>
            `;

            tbody.appendChild(row);
        });
    }

    renderAddressPagination(totalItems, totalPages) {
        const paginationTop = document.getElementById('addresses-pagination-top');
        const paginationBottom = document.getElementById('addresses-pagination-bottom');
        if (!paginationTop || !paginationBottom) return;

        if (totalPages <= 1) {
            paginationTop.style.display = totalItems > 0 ? 'flex' : 'none';
            paginationTop.innerHTML = totalItems > 0 ? `<span style="color:var(--theme-color-std-text);font-size:0.85rem;">${totalItems.toLocaleString()} addresses</span>` : '';
            paginationBottom.style.display = 'none';
            return;
        }

        const start = this.addressPage * this.addressPageSize + 1;
        const end = Math.min((this.addressPage + 1) * this.addressPageSize, totalItems);
        const current = this.addressPage;

        // Build page number buttons with ellipsis for large page counts
        const pageButtons = this.buildPageNumbers(current, totalPages).map(p => {
            if (p === '...') {
                return `<span style="color:var(--theme-color-std-text);font-size:0.85rem;padding:0 0.15rem;">&hellip;</span>`;
            }
            const idx = p;
            const active = idx === current;
            return `<button onclick="deviceDetailManager.goToAddressPage(${idx})"
                style="min-width:1.75rem;height:1.75rem;padding:0 0.35rem;border-radius:4px;border:1px solid ${active ? 'var(--theme-color-primary)' : 'var(--theme-color-std-bdr)'};
                background:${active ? 'var(--theme-color-primary)' : 'transparent'};color:${active ? 'var(--theme-color-primary--contrast)' : 'var(--theme-color-std-text)'};
                font-size:0.8rem;cursor:pointer;">${idx + 1}</button>`;
        }).join('');

        const html = `
            <span style="color:var(--theme-color-std-text);font-size:0.85rem;">
                ${start.toLocaleString()}&ndash;${end.toLocaleString()} of ${totalItems.toLocaleString()} addresses
            </span>
            <span style="display:flex;gap:0.25rem;align-items:center;">
                <ix-icon-button icon="chevron-left-small" ghost size="16" title="Previous page"
                    ${current === 0 ? 'disabled' : ''}
                    onclick="deviceDetailManager.goToAddressPage(${current - 1})"></ix-icon-button>
                ${pageButtons}
                <ix-icon-button icon="chevron-right-small" ghost size="16" title="Next page"
                    ${current >= totalPages - 1 ? 'disabled' : ''}
                    onclick="deviceDetailManager.goToAddressPage(${current + 1})"></ix-icon-button>
            </span>
        `;

        paginationTop.style.display = 'flex';
        paginationTop.innerHTML = html;
        paginationBottom.style.display = 'flex';
        paginationBottom.innerHTML = html;
    }

    /**
     * Build an array of page indices and '...' ellipsis markers.
     * Always shows first, last, and up to 2 pages around the current page.
     */
    buildPageNumbers(current, totalPages) {
        if (totalPages <= 7) {
            return Array.from({length: totalPages}, (_, i) => i);
        }
        const pages = new Set();
        pages.add(0);
        pages.add(totalPages - 1);
        for (let i = current - 1; i <= current + 1; i++) {
            if (i >= 0 && i < totalPages) pages.add(i);
        }
        const sorted = [...pages].sort((a, b) => a - b);
        const result = [];
        for (let i = 0; i < sorted.length; i++) {
            if (i > 0 && sorted[i] - sorted[i - 1] > 1) {
                result.push('...');
            }
            result.push(sorted[i]);
        }
        return result;
    }

    goToAddressPage(page) {
        this.addressPage = page;
        this.renderAddresses();
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

    editAddress(address) {
        const addr = this.device.config.addresses.find(a => a.address === address);
        if (!addr) return;

        document.getElementById('edit-address-original').value = addr.address;
        document.getElementById('edit-address-address').value = addr.address;
        document.getElementById('edit-address-topic').value = addr.topic;
        document.getElementById('edit-address-publish-mode').value = addr.publishMode;
        document.getElementById('edit-address-remove-path').checked = addr.removePath;
        document.getElementById('edit-address-modal').style.display = 'flex';
    }

    hideEditAddressModal() {
        document.getElementById('edit-address-modal').style.display = 'none';
    }

    async saveEditedAddress() {
        const form = document.getElementById('edit-address-form');
        if (!form.checkValidity()) {
            form.reportValidity();
            return;
        }

        const originalAddress = document.getElementById('edit-address-original').value;
        const newAddressData = {
            address: document.getElementById('edit-address-address').value.trim(),
            topic: document.getElementById('edit-address-topic').value.trim(),
            publishMode: document.getElementById('edit-address-publish-mode').value,
            removePath: document.getElementById('edit-address-remove-path').checked
        };

        try {
            // Delete old address first
            const deleteMutation = `
                mutation DeleteOpcUaAddress($deviceName: String!, $address: String!) {
                    opcUaDevice {
                        deleteAddress(deviceName: $deviceName, address: $address) {
                            success
                            errors
                        }
                    }
                }
            `;
            const deleteResult = await this.client.query(deleteMutation, {
                deviceName: this.deviceName,
                address: originalAddress
            });

            if (!deleteResult.opcUaDevice.deleteAddress.success) {
                const errors = deleteResult.opcUaDevice.deleteAddress.errors || ['Unknown error'];
                this.showError('Failed to update address: ' + errors.join(', '));
                return;
            }

            // Add new address
            const addMutation = `
                mutation AddOpcUaAddress($deviceName: String!, $input: OpcUaAddressInput!) {
                    opcUaDevice {
                        addAddress(deviceName: $deviceName, input: $input) {
                            success
                            errors
                        }
                    }
                }
            `;
            const addResult = await this.client.query(addMutation, {
                deviceName: this.deviceName,
                input: newAddressData
            });

            if (addResult.opcUaDevice.addAddress.success) {
                this.hideEditAddressModal();
                await this.loadDevice();
                this.showSuccess('Address updated successfully');
            } else {
                const errors = addResult.opcUaDevice.addAddress.errors || ['Unknown error'];
                this.showError('Failed to update address: ' + errors.join(', '));
                await this.loadDevice();
            }
        } catch (error) {
            console.error('Error editing address:', error);
            this.showError('Failed to update address: ' + error.message);
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
                    discardOldest: document.getElementById('config-discard-oldest').checked,
                    monitoredItemsBatchSize: parseInt(document.getElementById('config-batch-size').value)
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
                },
                writeConfig: {
                    enabled: document.getElementById('write-enabled').checked,
                    requestResponseEnabled: document.getElementById('write-rr-enabled').checked,
                    topicPrefix: document.getElementById('write-topic-prefix').value.trim() || 'write',
                    requestTopicPrefix: document.getElementById('write-request-topic-prefix').value.trim() || 'request',
                    responseTopicPrefix: document.getElementById('write-response-topic-prefix').value.trim() || 'response',
                    qos: parseInt(document.getElementById('write-qos').value),
                    writeTimeout: parseInt(document.getElementById('write-timeout').value) || 5000
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
                    setTimeout(() => { window.spaLocation.href = '/pages/opcua-devices.html'; }, 800);
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

        if (!document.getElementById('toast-anim-style')) {
            var style = document.createElement('style');
            style.id = 'toast-anim-style';
            style.textContent = '@keyframes slideDown{from{transform:translateX(-50%) translateY(-100%);opacity:0;}to{transform:translateX(-50%) translateY(0);opacity:1;}}@keyframes fadeOut{from{opacity:1;}to{opacity:0;}}';
            document.head.appendChild(style);
        }

        document.body.appendChild(toast);

        setTimeout(function() {
            if (toast.parentElement) {
                toast.style.animation = 'fadeOut 0.3s ease-out forwards';
                setTimeout(function() {
                    if (toast.parentElement) toast.remove();
                    if (errorDiv) errorDiv.style.display = 'none';
                }, 300);
            }
        }, 8000);
    }

    hideError() {
        const errorEl = document.getElementById('error-message');
        if (errorEl) {
            errorEl.style.display = 'none';
        }
    }

    showSuccess(message) {
        var existing = document.getElementById('success-toast');
        if (existing) existing.remove();

        var toast = document.createElement('div');
        toast.id = 'success-toast';
        toast.style.cssText = 'position:fixed;top:20px;left:50%;transform:translateX(-50%);background:var(--monster-green,#10B981);color:#fff;padding:14px 24px;border-radius:8px;box-shadow:0 4px 12px rgba(0,0,0,0.4);z-index:10000;font-size:0.9rem;max-width:600px;display:flex;align-items:center;gap:10px;animation:slideDown 0.3s ease-out;';
        toast.innerHTML = '<span style="font-size:1.2rem;">&#10003;</span><span>' + this.escapeHtml(message) + '</span><button onclick="this.parentElement.remove()" style="background:none;border:none;color:#fff;cursor:pointer;margin-left:auto;font-size:1.1rem;line-height:1;padding:0 4px;">&times;</button>';

        if (!document.getElementById('toast-anim-style')) {
            var style = document.createElement('style');
            style.id = 'toast-anim-style';
            style.textContent = '@keyframes slideDown{from{transform:translateX(-50%) translateY(-100%);opacity:0;}to{transform:translateX(-50%) translateY(0);opacity:1;}}@keyframes fadeOut{from{opacity:1;}to{opacity:0;}}';
            document.head.appendChild(style);
        }

        document.body.appendChild(toast);
        setTimeout(function() {
            if (toast.parentElement) {
                toast.style.animation = 'fadeOut 0.3s ease-out forwards';
                setTimeout(function() { if (toast.parentElement) toast.remove(); }, 300);
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

    async deleteClient() {
        try {
            const mutation = `mutation DeleteOpcUaDevice($name: String!) { opcUaDevice { delete(name: $name) } }`;
            const result = await this.client.query(mutation, { name: this.deviceName });
            if (result.opcUaDevice.delete) {
                this.showSuccess('OPC UA client deleted');
                setTimeout(() => { window.spaLocation.href = '/pages/opcua-devices.html'; }, 800);
            } else {
                this.showError('Failed to delete OPC UA client');
            }
        } catch (e) {
            console.error('Delete error', e);
            this.showError('Failed to delete OPC UA client: ' + e.message);
        }
    }

    showDeleteModal() {
        const span = document.getElementById('delete-client-name');
        if (span && this.device) span.textContent = this.device.name || this.deviceName;
        document.getElementById('delete-client-modal').style.display = 'flex';
    }
    hideDeleteModal() { document.getElementById('delete-client-modal').style.display = 'none'; }
    confirmDeleteClient() { this.hideDeleteModal(); this.deleteClient(); }

    goBack() {
        window.spaLocation.href = '/pages/opcua-devices.html';
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

function hideEditAddressModal() {
    deviceDetailManager.hideEditAddressModal();
}

function saveEditedAddress() {
    deviceDetailManager.saveEditedAddress();
}

function goBack() {
    deviceDetailManager.goBack();
}

function showDeleteModal() { deviceDetailManager.showDeleteModal(); }
function hideDeleteModal() { deviceDetailManager.hideDeleteModal(); }
function confirmDeleteClient() { deviceDetailManager.confirmDeleteClient(); }

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
        } else if (e.target.id === 'edit-address-modal') {
            deviceDetailManager.hideEditAddressModal();
        } else if (e.target.id === 'confirm-delete-address-modal') {
            deviceDetailManager.hideConfirmDeleteAddressModal();
        } else if (e.target.id === 'delete-client-modal') {
            deviceDetailManager.hideDeleteModal();
        }
    }
});