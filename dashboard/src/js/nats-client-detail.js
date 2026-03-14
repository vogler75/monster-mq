// NATS Client Detail Management JavaScript

class NatsClientDetailManager {
    constructor() {
        this.client = new GraphQLDashboardClient('/graphql');
        this.clientName = null;
        this.clientData = null;
        this.clusterNodes = [];
        this.metricsTimer = null;
        this.editingAddressIndex = null; // null = new, otherwise index in local addresses array
        this.editingOriginalSubject = null; // original natsSubject when editing an existing address
        this.addresses = []; // local copy of address mappings
        this.init();
    }

    async init() {
        const urlParams = new URLSearchParams(window.location.search);
        this.clientName = urlParams.get('client');
        this.isNew = urlParams.get('new') === 'true';

        if (this.isNew) {
            await this.loadClusterNodes();
            this.showNewClientForm();
            return;
        }

        if (!this.clientName) {
            this.showError('No NATS client specified');
            return;
        }
        await this.loadClusterNodes();
        await this.loadClientData();
        this.metricsTimer = setInterval(() => this.refreshMetrics(), 10000);
        window.addEventListener('beforeunload', () => this.cleanup());
    }

    cleanup() {
        if (this.metricsTimer) {
            clearInterval(this.metricsTimer);
            this.metricsTimer = null;
        }
    }

    async loadClusterNodes() {
        try {
            const query = `query GetBrokers { brokers { nodeId isCurrent } }`;
            const result = await this.client.query(query);
            this.clusterNodes = result.brokers || [];
            const nodeSelect = document.getElementById('client-node');
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

    async loadClientData() {
        this.showLoading(true);
        this.hideError();
        try {
            const query = `
                query GetNatsClients($name: String!) {
                    natsClients(name: $name) {
                        name namespace nodeId enabled isOnCurrentNode createdAt updatedAt
                        config {
                            servers authType username useJetStream streamName consumerDurableName
                            connectTimeoutMs reconnectDelayMs maxReconnectAttempts
                            addresses {
                                mode natsSubject mqttTopic qos autoConvert removePath
                            }
                        }
                        metrics { messagesIn messagesOut timestamp }
                    }
                }
            `;
            const result = await this.client.query(query, { name: this.clientName });
            if (!result.natsClients || result.natsClients.length === 0) throw new Error('NATS client not found');
            this.clientData = result.natsClients[0];
            this.addresses = (this.clientData.config.addresses || []).map(a => Object.assign({}, a));
            this.renderClientInfo();
            this.renderAddressesTable();
            this.renderMetrics();
        } catch (e) {
            console.error('Error loading NATS client', e);
            this.showError('Failed to load NATS client: ' + e.message);
        } finally {
            this.showLoading(false);
        }
    }

    showNewClientForm() {
        document.getElementById('page-title').textContent = 'Add NATS Client';
        document.getElementById('page-subtitle').textContent = 'Create a new NATS consumer/producer client';

        document.getElementById('client-name').value = '';
        document.getElementById('client-name').disabled = false;
        document.getElementById('client-namespace').value = '';
        document.getElementById('client-servers').value = 'nats://localhost:4222';
        document.getElementById('client-auth-type').value = 'ANONYMOUS';
        document.getElementById('client-username').value = '';
        document.getElementById('client-password').value = '';
        document.getElementById('client-token').value = '';
        document.getElementById('client-tls-ca').value = '';
        document.getElementById('client-jetstream').checked = false;
        document.getElementById('client-stream-name').value = '';
        document.getElementById('client-durable-name').value = '';
        document.getElementById('client-connect-timeout').value = '5000';
        document.getElementById('client-reconnect-delay').value = '5000';
        document.getElementById('client-max-reconnects').value = '-1';
        document.getElementById('client-enabled').checked = true;

        this.onAuthTypeChange('ANONYMOUS');
        this.onJetStreamChange(false);

        const toggleBtn = document.getElementById('toggle-client-btn');
        if (toggleBtn) toggleBtn.style.display = 'none';
        const deleteBtn = document.getElementById('delete-client-btn');
        if (deleteBtn) deleteBtn.style.display = 'none';
        const addressesSection = document.getElementById('addresses-section');
        if (addressesSection) addressesSection.style.display = 'none';
        const metricsSection = document.getElementById('metrics-section');
        if (metricsSection) metricsSection.style.display = 'none';
        const timestampsRow = document.getElementById('timestamps-row');
        if (timestampsRow) timestampsRow.style.display = 'none';

        const saveBtn = document.getElementById('save-client-btn');
        if (saveBtn) saveBtn.innerHTML = saveBtn.innerHTML.replace('Save Client', 'Create Client');

        document.getElementById('client-content').style.display = 'block';
    }

    renderClientInfo() {
        if (!this.clientData) return;
        const d = this.clientData;
        const cfg = d.config;

        document.getElementById('page-title').textContent = `NATS Client: ${d.name}`;
        document.getElementById('page-subtitle').textContent = `${d.namespace} — ${(cfg.servers || []).join(', ')}`;

        document.getElementById('client-name').value = d.name;
        document.getElementById('client-name').disabled = true;
        document.getElementById('client-namespace').value = d.namespace;
        document.getElementById('client-node').value = d.nodeId;
        document.getElementById('client-servers').value = (cfg.servers || []).join(', ');
        document.getElementById('client-auth-type').value = cfg.authType || 'ANONYMOUS';
        document.getElementById('client-username').value = cfg.username || '';
        // passwords and tokens are never returned from the server for security
        document.getElementById('client-password').value = '';
        document.getElementById('client-token').value = '';
        document.getElementById('client-tls-ca').value = cfg.tlsCaCertPath || '';
        document.getElementById('client-jetstream').checked = cfg.useJetStream || false;
        document.getElementById('client-stream-name').value = cfg.streamName || '';
        document.getElementById('client-durable-name').value = cfg.consumerDurableName || '';
        document.getElementById('client-connect-timeout').value = cfg.connectTimeoutMs || 5000;
        document.getElementById('client-reconnect-delay').value = cfg.reconnectDelayMs || 5000;
        document.getElementById('client-max-reconnects').value = cfg.maxReconnectAttempts !== undefined ? cfg.maxReconnectAttempts : -1;
        document.getElementById('client-enabled').checked = d.enabled;

        this.onAuthTypeChange(cfg.authType || 'ANONYMOUS');
        this.onJetStreamChange(cfg.useJetStream || false);

        this.setText('client-created-at', d.createdAt ? new Date(d.createdAt).toLocaleString() : '-');
        this.setText('client-updated-at', d.updatedAt ? new Date(d.updatedAt).toLocaleString() : '-');

        const statusBadge = document.getElementById('client-status');
        if (d.enabled) {
            statusBadge.className = 'status-badge status-enabled';
            statusBadge.textContent = 'ENABLED';
        } else {
            statusBadge.className = 'status-badge status-disabled';
            statusBadge.textContent = 'DISABLED';
        }

        document.getElementById('client-content').style.display = 'block';
        document.getElementById('addresses-section').style.display = 'block';
    }

    onAuthTypeChange(authType) {
        document.querySelectorAll('.auth-fields').forEach(el => el.style.display = 'none');
        if (authType === 'USERNAME_PASSWORD') {
            document.getElementById('detail-auth-fields-userpass').style.display = 'block';
        } else if (authType === 'TOKEN') {
            document.getElementById('detail-auth-fields-token').style.display = 'block';
        } else if (authType === 'TLS') {
            document.getElementById('detail-auth-fields-tls').style.display = 'block';
        }
    }

    onJetStreamChange(enabled) {
        document.getElementById('js-stream-group').style.display = enabled ? 'block' : 'none';
        document.getElementById('js-durable-group').style.display = enabled ? 'block' : 'none';
    }

    // ─── Addresses Table ─────────────────────────────────────────────────────

    renderAddressesTable() {
        const tbody = document.getElementById('addresses-table-body');
        if (!tbody) return;
        if (this.addresses.length === 0) {
            tbody.innerHTML = '<tr><td colspan="7" style="text-align:center;padding:1.5rem;color:var(--text-muted);">No address mappings configured.</td></tr>';
            return;
        }
        tbody.innerHTML = this.addresses.map((addr, idx) => `
            <tr>
                <td><span class="mode-badge ${addr.mode === 'SUBSCRIBE' ? 'mode-subscribe' : 'mode-publish'}">${addr.mode}</span></td>
                <td><code style="font-size:0.85rem;">${this.escapeHtml(addr.natsSubject || '')}</code></td>
                <td><code style="font-size:0.85rem;">${this.escapeHtml(addr.mqttTopic || '')}</code></td>
                <td>${addr.qos}</td>
                <td>${addr.autoConvert ? '✓' : '—'}</td>
                <td>${addr.removePath ? '✓' : '—'}</td>
                <td><div class="action-buttons">
                     <button class="btn-icon" title="Edit" onclick="natsDetailManager.showEditAddressModal(${idx})" style="color:var(--monster-purple);">
                         <svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor"><path d="M3 17.25V21h3.75L17.81 9.94l-3.75-3.75L3 17.25zM20.71 7.04c.39-.39.39-1.02 0-1.41l-2.34-2.34c-.39-.39-1.02-.39-1.41 0l-1.83 1.83 3.75 3.75 1.83-1.83z"/></svg>
                     </button>
                     <button class="btn-icon btn-delete" title="Delete" onclick="natsDetailManager.removeAddress(${idx})">
                        <svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor"><path d="M6 19c0 1.1.9 2 2 2h8c1.1 0 2-.9 2-2V7H6v12zM19 4h-3.5l-1-1h-5l-1 1H5v2h14V4z"/></svg>
                    </button>
                </div></td>
            </tr>
        `).join('');
    }

    showAddAddressModal() {
        this.editingAddressIndex = null;
        this.editingOriginalSubject = null;
        document.getElementById('address-modal-title').textContent = 'Add Address Mapping';
        document.getElementById('addr-mode').value = 'SUBSCRIBE';
        document.getElementById('addr-nats-subject').value = '';
        document.getElementById('addr-mqtt-topic').value = '';
        document.getElementById('addr-qos').value = '0';
        document.getElementById('addr-auto-convert').checked = true;
        document.getElementById('addr-remove-path').checked = true;
        document.getElementById('add-address-form').reset && document.getElementById('add-address-form').reset();
        document.getElementById('add-address-modal').style.display = 'flex';
    }

    showEditAddressModal(idx) {
        const addr = this.addresses[idx];
        if (!addr) return;
        this.editingAddressIndex = idx;
        this.editingOriginalSubject = addr.natsSubject;
        document.getElementById('address-modal-title').textContent = 'Edit Address Mapping';
        document.getElementById('addr-mode').value = addr.mode || 'SUBSCRIBE';
        document.getElementById('addr-nats-subject').value = addr.natsSubject || '';
        document.getElementById('addr-mqtt-topic').value = addr.mqttTopic || '';
        document.getElementById('addr-qos').value = String(addr.qos ?? 0);
        document.getElementById('addr-auto-convert').checked = addr.autoConvert !== false;
        document.getElementById('addr-remove-path').checked = addr.removePath !== false;
        document.getElementById('add-address-modal').style.display = 'flex';
    }

    hideAddAddressModal() {
        document.getElementById('add-address-modal').style.display = 'none';
        this.editingAddressIndex = null;
        this.editingOriginalSubject = null;
    }

    async saveAddressMapping() {
        const form = document.getElementById('add-address-form');
        const natsSubject = document.getElementById('addr-nats-subject').value.trim();
        const mqttTopic = document.getElementById('addr-mqtt-topic').value.trim();
        if (!natsSubject || !mqttTopic) {
            this.showError('NATS Subject and MQTT Topic are required');
            return;
        }

        const newAddr = {
            mode: document.getElementById('addr-mode').value,
            natsSubject,
            mqttTopic,
            qos: parseInt(document.getElementById('addr-qos').value),
            autoConvert: document.getElementById('addr-auto-convert').checked,
            removePath: document.getElementById('addr-remove-path').checked
        };

        try {
            if (this.editingAddressIndex === null) {
                // Add new
                const mutation = `
                    mutation AddNatsClientAddress($deviceName: String!, $input: NatsClientAddressInput!) {
                        natsClient { addAddress(deviceName: $deviceName, input: $input) { success errors } }
                    }
                `;
                const result = await this.client.query(mutation, { deviceName: this.clientName, input: newAddr });
                if (!result.natsClient.addAddress.success) {
                    const errs = result.natsClient.addAddress.errors || ['Unknown error'];
                    this.showError('Failed to add address: ' + errs.join(', '));
                    return;
                }
                this.addresses.push(newAddr);
            } else {
                // Update existing — keyed by the original natsSubject
                const originalSubject = this.editingOriginalSubject;
                const mutation = `
                    mutation UpdateNatsClientAddress($deviceName: String!, $natsSubject: String!, $input: NatsClientAddressInput!) {
                        natsClient { updateAddress(deviceName: $deviceName, natsSubject: $natsSubject, input: $input) { success errors } }
                    }
                `;
                const result = await this.client.query(mutation, { deviceName: this.clientName, natsSubject: originalSubject, input: newAddr });
                if (!result.natsClient.updateAddress.success) {
                    const errs = result.natsClient.updateAddress.errors || ['Unknown error'];
                    this.showError('Failed to update address: ' + errs.join(', '));
                    return;
                }
                this.addresses[this.editingAddressIndex] = newAddr;
            }

            this.hideAddAddressModal();
            this.renderAddressesTable();
            this.showSuccess('Address mapping saved');
        } catch (e) {
            console.error('Error saving address mapping', e);
            this.showError('Failed to save address mapping: ' + e.message);
        }
    }

    async removeAddress(idx) {
        if (!confirm('Delete this address mapping?')) return;
        const addr = this.addresses[idx];
        if (!addr) return;
        try {
            const mutation = `
                mutation DeleteNatsClientAddress($deviceName: String!, $natsSubject: String!) {
                    natsClient { deleteAddress(deviceName: $deviceName, natsSubject: $natsSubject) { success errors } }
                }
            `;
            const result = await this.client.query(mutation, { deviceName: this.clientName, natsSubject: addr.natsSubject });
            if (!result.natsClient.deleteAddress.success) {
                const errs = result.natsClient.deleteAddress.errors || ['Unknown error'];
                this.showError('Failed to delete address: ' + errs.join(', '));
                return;
            }
            this.addresses.splice(idx, 1);
            this.renderAddressesTable();
            this.showSuccess('Address mapping deleted');
        } catch (e) {
            console.error('Error deleting address mapping', e);
            this.showError('Failed to delete address mapping: ' + e.message);
        }
    }

    // ─── Metrics ─────────────────────────────────────────────────────────────

    renderMetrics() {
        if (!this.clientData || !this.clientData.metrics) return;
        const m = this.clientData.metrics;
        if (!m || m.length === 0) return;
        const latest = m[0];
        this.setText('metric-messages-in', Math.round(latest.messagesIn));
        this.setText('metric-messages-out', Math.round(latest.messagesOut));
    }

    async refreshMetrics() {
        if (!this.clientName) return;
        try {
            const query = `query GetNatsClientMetrics($name: String!) {
                natsClients(name: $name) { metrics { messagesIn messagesOut timestamp } }
            }`;
            const result = await this.client.query(query, { name: this.clientName });
            if (result.natsClients && result.natsClients.length > 0) {
                this.clientData.metrics = result.natsClients[0].metrics;
                this.renderMetrics();
            }
        } catch (e) {
            console.warn('Metrics refresh failed', e.message);
        }
    }

    // ─── Save / Toggle / Delete ───────────────────────────────────────────────

    collectFormData() {
        const authType = document.getElementById('client-auth-type').value;
        const servers = document.getElementById('client-servers').value.trim().split(',').map(s => s.trim()).filter(s => s.length > 0);
        const configInput = {
            servers,
            authType,
            useJetStream: document.getElementById('client-jetstream').checked,
            reconnectDelayMs: parseInt(document.getElementById('client-reconnect-delay').value) || 5000,
            connectTimeoutMs: parseInt(document.getElementById('client-connect-timeout').value) || 5000,
            maxReconnectAttempts: parseInt(document.getElementById('client-max-reconnects').value),
        };
        if (authType === 'USERNAME_PASSWORD') {
            configInput.username = document.getElementById('client-username').value.trim();
            const pwd = document.getElementById('client-password').value;
            if (pwd) configInput.password = pwd;
        } else if (authType === 'TOKEN') {
            const tok = document.getElementById('client-token').value;
            if (tok) configInput.token = tok;
        } else if (authType === 'TLS') {
            const ca = document.getElementById('client-tls-ca').value.trim();
            if (ca) configInput.tlsCaCertPath = ca;
        }
        if (configInput.useJetStream) {
            configInput.streamName = document.getElementById('client-stream-name').value.trim();
            configInput.consumerDurableName = document.getElementById('client-durable-name').value.trim();
        }
        return {
            name: document.getElementById('client-name').value.trim(),
            namespace: document.getElementById('client-namespace').value.trim(),
            nodeId: document.getElementById('client-node').value,
            enabled: document.getElementById('client-enabled').checked,
            config: configInput
        };
    }

    async saveClient() {
        const form = document.getElementById('client-form');
        if (!form.checkValidity()) { form.reportValidity(); return; }

        const data = this.collectFormData();

        if (this.isNew) {
            try {
                const mutation = `
                    mutation CreateNatsClient($input: NatsClientInput!) {
                        natsClient { create(input: $input) { success errors client { name } } }
                    }
                `;
                const result = await this.client.query(mutation, { input: data });
                if (result.natsClient.create.success) {
                    this.showSuccess(`NATS client "${data.name}" created successfully`);
                    setTimeout(() => { window.location.href = '/pages/nats-clients.html'; }, 800);
                } else {
                    const errors = result.natsClient.create.errors || ['Unknown error'];
                    this.showError('Failed to create NATS client: ' + errors.join(', '));
                }
            } catch (e) {
                console.error('Error creating NATS client', e);
                this.showError('Failed to create NATS client: ' + e.message);
            }
            return;
        }

        const prevNodeId = this.clientData ? this.clientData.nodeId : null;
        try {
            const mutation = `
                mutation UpdateNatsClient($name: String!, $input: NatsClientInput!) {
                    natsClient { update(name: $name, input: $input) { success errors client { name } } }
                }
            `;
            const result = await this.client.query(mutation, { name: this.clientName, input: data });
            if (result.natsClient.update.success) {
                const newName = data.name;
                if (newName !== this.clientName) {
                    this.clientName = newName;
                    const url = new URL(window.location.href);
                    url.searchParams.set('client', newName);
                    window.history.replaceState({}, '', url.toString());
                }
                await this.loadClientData();
                if (prevNodeId && data.nodeId && data.nodeId !== prevNodeId) {
                    await this.reassignClient(data.nodeId);
                }
                this.showSuccess('NATS client updated successfully');
            } else {
                const errors = result.natsClient.update.errors || ['Unknown error'];
                this.showError('Failed to update NATS client: ' + errors.join(', '));
            }
        } catch (e) {
            console.error('Error updating NATS client', e);
            this.showError('Failed to update NATS client: ' + e.message);
        }
    }

    async reassignClient(newNodeId) {
        try {
            const mutation = `
                mutation ReassignNatsClient($name: String!, $nodeId: String!) {
                    natsClient { reassign(name: $name, nodeId: $nodeId) { success errors client { nodeId } } }
                }
            `;
            const result = await this.client.query(mutation, { name: this.clientName, nodeId: newNodeId });
            if (!result.natsClient.reassign.success) {
                const errs = result.natsClient.reassign.errors || ['Unknown error'];
                this.showError('Reassign warning: ' + errs.join(', '));
            } else {
                await this.loadClientData();
                this.showSuccess('Client reassigned to node ' + newNodeId);
            }
        } catch (e) {
            console.error('Error reassigning client', e);
            this.showError('Failed to reassign NATS client: ' + e.message);
        }
    }

    async toggleClient() {
        if (!this.clientData) return;
        const newState = !this.clientData.enabled;
        try {
            const mutation = `
                mutation ToggleNatsClient($name: String!, $enabled: Boolean!) {
                    natsClient { toggle(name: $name, enabled: $enabled) { success errors client { enabled } } }
                }
            `;
            const result = await this.client.query(mutation, { name: this.clientName, enabled: newState });
            if (result.natsClient.toggle.success) {
                await this.loadClientData();
                this.showSuccess(`NATS client ${newState ? 'started' : 'stopped'} successfully`);
            } else {
                const errors = result.natsClient.toggle.errors || ['Unknown error'];
                this.showError('Failed to toggle NATS client: ' + errors.join(', '));
            }
        } catch (e) {
            console.error('Toggle error', e);
            this.showError('Failed to toggle NATS client: ' + e.message);
        }
    }

    async deleteClient() {
        try {
            const mutation = `
                mutation DeleteNatsClient($name: String!) {
                    natsClient { delete(name: $name) }
                }
            `;
            const result = await this.client.query(mutation, { name: this.clientName });
            if (result.natsClient.delete) {
                this.showSuccess('NATS client deleted');
                this.cleanup();
                setTimeout(() => { window.location.href = '/pages/nats-clients.html'; }, 800);
            } else {
                this.showError('Failed to delete NATS client');
            }
        } catch (e) {
            console.error('Delete error', e);
            this.showError('Failed to delete NATS client: ' + e.message);
        }
    }

    // ─── Modal / UI helpers ──────────────────────────────────────────────────

    showDeleteModal() {
        const span = document.getElementById('delete-client-name');
        if (span && this.clientData) span.textContent = this.clientData.name;
        document.getElementById('delete-client-modal').style.display = 'flex';
    }
    hideDeleteModal() { document.getElementById('delete-client-modal').style.display = 'none'; }
    confirmDeleteClient() { this.hideDeleteModal(); this.deleteClient(); }
    goBack() { this.cleanup(); window.location.href = '/pages/nats-clients.html'; }

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
        setTimeout(() => { if (note.parentNode) note.parentNode.removeChild(note); }, 3000);
    }
    escapeHtml(t) { const div = document.createElement('div'); div.textContent = t; return div.innerHTML; }
    setText(id, value) { const el = document.getElementById(id); if (el) el.textContent = value; }
}

// Global wrappers
let natsDetailManager;
function saveClient() { natsDetailManager.saveClient(); }
function toggleClient() { natsDetailManager.toggleClient(); }
function goBack() { natsDetailManager.goBack(); }
function showDeleteModal() { natsDetailManager.showDeleteModal(); }
function hideDeleteModal() { natsDetailManager.hideDeleteModal(); }
function confirmDeleteClient() { natsDetailManager.confirmDeleteClient(); }
function showAddAddressModal() { natsDetailManager.showAddAddressModal(); }
function hideAddAddressModal() { natsDetailManager.hideAddAddressModal(); }
function saveAddressMapping() { natsDetailManager.saveAddressMapping(); }

// Initialize
document.addEventListener('DOMContentLoaded', () => {
    natsDetailManager = new NatsClientDetailManager();
});

document.addEventListener('click', e => {
    if (e.target.classList.contains('modal')) {
        if (e.target.id === 'delete-client-modal') natsDetailManager.hideDeleteModal();
        if (e.target.id === 'add-address-modal') natsDetailManager.hideAddAddressModal();
    }
});
