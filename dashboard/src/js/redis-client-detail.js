// Redis Client Detail Management JavaScript

class RedisClientDetailManager {
    constructor() {
        this.client = new GraphQLDashboardClient();
        this.clientName = null;
        this.clientData = null;
        this.clusterNodes = [];
        this.metricsTimer = null;
        this.editingAddressIndex = null;
        this.editingOriginalChannel = null;
        this.addresses = [];
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
            this.showError('No Redis client specified');
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
                query GetRedisClients($name: String!) {
                    redisClients(name: $name) {
                        name namespace nodeId enabled isOnCurrentNode createdAt updatedAt
                        config {
                            host port database useSsl sslTrustAll connectionString
                            maxPoolSize reconnectDelayMs maxReconnectAttempts loopPrevention
                            addresses {
                                mode redisChannel mqttTopic qos usePatternSubscribe usePatternMatch kvPollIntervalMs publishOnChangeOnly removePath
                            }
                        }
                        metrics { messagesIn messagesOut timestamp }
                    }
                }
            `;
            const result = await this.client.query(query, { name: this.clientName });
            if (!result.redisClients || result.redisClients.length === 0) throw new Error('Redis client not found');
            this.clientData = result.redisClients[0];
            this.addresses = (this.clientData.config.addresses || []).map(a => Object.assign({}, a));
            this.renderClientInfo();
            this.renderAddressesTable();
            this.renderMetrics();
        } catch (e) {
            console.error('Error loading Redis client', e);
            this.showError('Failed to load Redis client: ' + e.message);
        } finally {
            this.showLoading(false);
        }
    }

    showNewClientForm() {
        document.getElementById('page-title').textContent = 'Add Redis Client';
        document.getElementById('page-subtitle').textContent = 'Create a new Redis Pub/Sub and KV bridge client';

        document.getElementById('client-name').value = '';
        document.getElementById('client-name').disabled = false;
        document.getElementById('client-namespace').value = '';
        document.getElementById('client-host').value = 'localhost';
        document.getElementById('client-port').value = '6379';
        document.getElementById('client-password').value = '';
        document.getElementById('client-database').value = '0';
        document.getElementById('client-connection-string').value = '';
        document.getElementById('client-use-ssl').checked = false;
        document.getElementById('client-ssl-trust-all').checked = false;
        document.getElementById('client-max-pool-size').value = '6';
        document.getElementById('client-reconnect-delay').value = '5000';
        document.getElementById('client-max-reconnects').value = '-1';
        document.getElementById('client-loop-prevention').checked = true;
        document.getElementById('client-enabled').checked = true;

        const deleteBtn = document.getElementById('delete-btn');
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

        document.getElementById('page-title').textContent = `Redis Client: ${d.name}`;
        document.getElementById('page-subtitle').textContent = `${d.namespace} \u2014 ${cfg.host || 'localhost'}:${cfg.port || 6379}/${cfg.database || 0}`;

        document.getElementById('client-name').value = d.name;
        document.getElementById('client-name').disabled = true;
        document.getElementById('client-namespace').value = d.namespace;
        document.getElementById('client-node').value = d.nodeId;
        document.getElementById('client-host').value = cfg.host || 'localhost';
        document.getElementById('client-port').value = cfg.port || 6379;
        document.getElementById('client-password').value = '';
        document.getElementById('client-database').value = cfg.database || 0;
        document.getElementById('client-connection-string').value = cfg.connectionString || '';
        document.getElementById('client-use-ssl').checked = cfg.useSsl || false;
        document.getElementById('client-ssl-trust-all').checked = cfg.sslTrustAll || false;
        document.getElementById('client-max-pool-size').value = cfg.maxPoolSize || 6;
        document.getElementById('client-reconnect-delay').value = cfg.reconnectDelayMs || 5000;
        document.getElementById('client-max-reconnects').value = cfg.maxReconnectAttempts !== undefined ? cfg.maxReconnectAttempts : -1;
        document.getElementById('client-loop-prevention').checked = cfg.loopPrevention !== false;
        document.getElementById('client-enabled').checked = d.enabled;

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

    // --- Addresses Table ---

    renderAddressesTable() {
        const tbody = document.getElementById('addresses-table-body');
        if (!tbody) return;
        if (this.addresses.length === 0) {
            tbody.innerHTML = '<tr><td colspan="7" style="text-align:center;padding:1.5rem;color:var(--text-muted);">No address mappings configured.</td></tr>';
            return;
        }
        tbody.innerHTML = this.addresses.map((addr, idx) => {
            const modeClass = addr.mode === 'SUBSCRIBE' ? 'mode-subscribe' : addr.mode === 'PUBLISH' ? 'mode-publish' : 'mode-kv_sync';
            const modeIcon = addr.mode === 'SUBSCRIBE' ? '⬇️' : addr.mode === 'PUBLISH' ? '⬆️' : '🔄';
            const patternInfo = addr.mode === 'KV_SYNC'
                ? (addr.usePatternMatch ? 'SCAN' : '\u2014')
                : (addr.usePatternSubscribe ? 'PSUB' : '\u2014');
            return `
            <tr>
                <td><span class="mode-badge ${modeClass}">${modeIcon} ${addr.mode}</span></td>
                <td><code style="font-size:0.85rem;">${this.escapeHtml(addr.redisChannel || '')}</code></td>
                <td><code style="font-size:0.85rem;">${this.escapeHtml(addr.mqttTopic || '')}</code></td>
                <td>${addr.qos}</td>
                <td>${patternInfo}</td>
                <td>${addr.removePath ? '\u2713' : '\u2014'}</td>
                <td><div class="action-buttons">
                    <ix-icon-button icon="pen" variant="primary" ghost size="16" title="Edit" onclick="redisDetailManager.showEditAddressModal(${idx})"></ix-icon-button>
                    <ix-icon-button icon="trashcan" variant="primary" ghost size="16" class="btn-delete" title="Delete" onclick="redisDetailManager.removeAddress(${idx})"></ix-icon-button>
                </div></td>
            </tr>`;
        }).join('');
    }

    showAddAddressModal() {
        this.editingAddressIndex = null;
        this.editingOriginalChannel = null;
        document.getElementById('address-modal-title').textContent = 'Add Address Mapping';
        document.getElementById('addr-mode').value = 'SUBSCRIBE';
        document.getElementById('addr-redis-channel').value = '';
        document.getElementById('addr-mqtt-topic').value = '';
        document.getElementById('addr-qos').value = '0';
        document.getElementById('addr-pattern-subscribe').checked = false;
        document.getElementById('addr-pattern-match').checked = false;
        document.getElementById('addr-kv-poll').value = '0';
        document.getElementById('addr-publish-on-change').checked = false;
        document.getElementById('addr-remove-path').checked = true;
        onAddrModeChange('SUBSCRIBE');
        this.updateTopicDirection();
        document.getElementById('add-address-modal').style.display = 'flex';
    }

    showEditAddressModal(idx) {
        const addr = this.addresses[idx];
        if (!addr) return;
        this.editingAddressIndex = idx;
        this.editingOriginalChannel = addr.redisChannel;
        document.getElementById('address-modal-title').textContent = 'Edit Address Mapping';
        document.getElementById('addr-mode').value = addr.mode || 'SUBSCRIBE';
        document.getElementById('addr-redis-channel').value = addr.redisChannel || '';
        document.getElementById('addr-mqtt-topic').value = addr.mqttTopic || '';
        document.getElementById('addr-qos').value = String(addr.qos ?? 0);
        document.getElementById('addr-pattern-subscribe').checked = addr.usePatternSubscribe || false;
        document.getElementById('addr-pattern-match').checked = addr.usePatternMatch || false;
        document.getElementById('addr-kv-poll').value = addr.kvPollIntervalMs || 0;
        document.getElementById('addr-publish-on-change').checked = addr.publishOnChangeOnly || false;
        document.getElementById('addr-remove-path').checked = addr.removePath !== false;
        onAddrModeChange(addr.mode || 'SUBSCRIBE');
        this.updateTopicDirection();
        document.getElementById('add-address-modal').style.display = 'flex';
    }

    updateTopicDirection() {
        const mode = document.getElementById('addr-mode')?.value;
        const arrow = document.querySelector('#addr-topic-direction .direction-arrow');
        if (arrow) {
            if (mode === 'SUBSCRIBE') {
                arrow.textContent = '⬇️';
                arrow.title = 'Redis → MQTT';
            } else if (mode === 'PUBLISH') {
                arrow.textContent = '⬆️';
                arrow.title = 'MQTT → Redis';
            } else {
                arrow.textContent = '🔄';
                arrow.title = 'Bidirectional Sync';
            }
        }
    }

    hideAddAddressModal() {
        document.getElementById('add-address-modal').style.display = 'none';
        this.editingAddressIndex = null;
        this.editingOriginalChannel = null;
    }

    async saveAddressMapping() {
        const mode = document.getElementById('addr-mode').value;
        const redisChannel = document.getElementById('addr-redis-channel').value.trim();
        const mqttTopic = document.getElementById('addr-mqtt-topic').value.trim();
        
        if (!redisChannel || !mqttTopic) {
            this.showError('Redis Channel and MQTT Topic are required');
            return;
        }

        // Wildcard validation
        if (mode === 'SUBSCRIBE') {
            // Redis -> MQTT: MQTT Topic (Local) must not have MQTT wildcards (+ or #)
            if (mqttTopic.includes('+') || mqttTopic.includes('#')) {
                this.showError('MQTT Topic must not contain wildcards (+ or #) in SUBSCRIBE mode.');
                return;
            }
        } else if (mode === 'PUBLISH') {
            // MQTT -> Redis: Redis Channel (Remote) must not have Redis wildcards (* or ?)
            if (redisChannel.includes('*') || redisChannel.includes('?')) {
                this.showError('Redis Channel must not contain wildcards (* or ?) in PUBLISH mode.');
                return;
            }
        }

        const newAddr = {
            mode: mode,
            redisChannel,
            mqttTopic,
            qos: parseInt(document.getElementById('addr-qos').value),
            usePatternSubscribe: document.getElementById('addr-pattern-subscribe').checked,
            usePatternMatch: document.getElementById('addr-pattern-match').checked,
            kvPollIntervalMs: parseInt(document.getElementById('addr-kv-poll').value) || 0,
            publishOnChangeOnly: document.getElementById('addr-publish-on-change').checked,
            removePath: document.getElementById('addr-remove-path').checked
        };

        try {
            if (this.editingAddressIndex === null) {
                const mutation = `
                    mutation AddRedisClientAddress($deviceName: String!, $input: RedisClientAddressInput!) {
                        redisClient { addAddress(deviceName: $deviceName, input: $input) { success errors } }
                    }
                `;
                const result = await this.client.query(mutation, { deviceName: this.clientName, input: newAddr });
                if (!result.redisClient.addAddress.success) {
                    this.showError('Failed to add address: ' + (result.redisClient.addAddress.errors || []).join(', '));
                    return;
                }
                this.addresses.push(newAddr);
            } else {
                const originalChannel = this.editingOriginalChannel;
                const mutation = `
                    mutation UpdateRedisClientAddress($deviceName: String!, $redisChannel: String!, $input: RedisClientAddressInput!) {
                        redisClient { updateAddress(deviceName: $deviceName, redisChannel: $redisChannel, input: $input) { success errors } }
                    }
                `;
                const result = await this.client.query(mutation, { deviceName: this.clientName, redisChannel: originalChannel, input: newAddr });
                if (!result.redisClient.updateAddress.success) {
                    this.showError('Failed to update address: ' + (result.redisClient.updateAddress.errors || []).join(', '));
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
                mutation DeleteRedisClientAddress($deviceName: String!, $redisChannel: String!) {
                    redisClient { deleteAddress(deviceName: $deviceName, redisChannel: $redisChannel) { success errors } }
                }
            `;
            const result = await this.client.query(mutation, { deviceName: this.clientName, redisChannel: addr.redisChannel });
            if (!result.redisClient.deleteAddress.success) {
                this.showError('Failed to delete address: ' + (result.redisClient.deleteAddress.errors || []).join(', '));
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

    // --- Metrics ---

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
            const query = `query GetRedisClientMetrics($name: String!) {
                redisClients(name: $name) { metrics { messagesIn messagesOut timestamp } }
            }`;
            const result = await this.client.query(query, { name: this.clientName });
            if (result.redisClients && result.redisClients.length > 0) {
                this.clientData.metrics = result.redisClients[0].metrics;
                this.renderMetrics();
            }
        } catch (e) {
            console.warn('Metrics refresh failed', e.message);
        }
    }

    // --- Save / Delete ---

    collectFormData() {
        const configInput = {
            host: document.getElementById('client-host').value.trim(),
            port: parseInt(document.getElementById('client-port').value) || 6379,
            database: parseInt(document.getElementById('client-database').value) || 0,
            useSsl: document.getElementById('client-use-ssl').checked,
            sslTrustAll: document.getElementById('client-ssl-trust-all').checked,
            maxPoolSize: parseInt(document.getElementById('client-max-pool-size').value) || 6,
            reconnectDelayMs: parseInt(document.getElementById('client-reconnect-delay').value) || 5000,
            maxReconnectAttempts: parseInt(document.getElementById('client-max-reconnects').value),
            loopPrevention: document.getElementById('client-loop-prevention').checked,
        };
        const pwd = document.getElementById('client-password').value;
        if (pwd) configInput.password = pwd;
        const connStr = document.getElementById('client-connection-string').value.trim();
        if (connStr) configInput.connectionString = connStr;
        // Include current addresses
        configInput.addresses = this.addresses.map(a => ({
            mode: a.mode,
            redisChannel: a.redisChannel,
            mqttTopic: a.mqttTopic,
            qos: a.qos,
            usePatternSubscribe: a.usePatternSubscribe || false,
            usePatternMatch: a.usePatternMatch || false,
            kvPollIntervalMs: a.kvPollIntervalMs || 0,
            publishOnChangeOnly: a.publishOnChangeOnly || false,
            removePath: a.removePath
        }));
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
                    mutation CreateRedisClient($input: RedisClientInput!) {
                        redisClient { create(input: $input) { success errors client { name } } }
                    }
                `;
                const result = await this.client.query(mutation, { input: data });
                if (result.redisClient.create.success) {
                    this.showSuccess(`Redis client "${data.name}" created successfully`);
                    setTimeout(() => { window.spaLocation.href = '/pages/redis-clients.html'; }, 800);
                } else {
                    this.showError('Failed to create Redis client: ' + (result.redisClient.create.errors || []).join(', '));
                }
            } catch (e) {
                console.error('Error creating Redis client', e);
                this.showError('Failed to create Redis client: ' + e.message);
            }
            return;
        }

        const prevNodeId = this.clientData ? this.clientData.nodeId : null;
        try {
            const mutation = `
                mutation UpdateRedisClient($name: String!, $input: RedisClientInput!) {
                    redisClient { update(name: $name, input: $input) { success errors client { name } } }
                }
            `;
            const result = await this.client.query(mutation, { name: this.clientName, input: data });
            if (result.redisClient.update.success) {
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
                this.showSuccess('Redis client updated successfully');
            } else {
                this.showError('Failed to update Redis client: ' + (result.redisClient.update.errors || []).join(', '));
            }
        } catch (e) {
            console.error('Error updating Redis client', e);
            this.showError('Failed to update Redis client: ' + e.message);
        }
    }

    async reassignClient(newNodeId) {
        try {
            const mutation = `
                mutation ReassignRedisClient($name: String!, $nodeId: String!) {
                    redisClient { reassign(name: $name, nodeId: $nodeId) { success errors client { nodeId } } }
                }
            `;
            const result = await this.client.query(mutation, { name: this.clientName, nodeId: newNodeId });
            if (!result.redisClient.reassign.success) {
                this.showError('Reassign warning: ' + (result.redisClient.reassign.errors || []).join(', '));
            } else {
                await this.loadClientData();
                this.showSuccess('Client reassigned to node ' + newNodeId);
            }
        } catch (e) {
            console.error('Error reassigning client', e);
            this.showError('Failed to reassign Redis client: ' + e.message);
        }
    }

    async deleteClient() {
        try {
            const mutation = `
                mutation DeleteRedisClient($name: String!) {
                    redisClient { delete(name: $name) }
                }
            `;
            const result = await this.client.query(mutation, { name: this.clientName });
            if (result.redisClient.delete) {
                this.showSuccess('Redis client deleted');
                this.cleanup();
                setTimeout(() => { window.spaLocation.href = '/pages/redis-clients.html'; }, 800);
            } else {
                this.showError('Failed to delete Redis client');
            }
        } catch (e) {
            console.error('Delete error', e);
            this.showError('Failed to delete Redis client: ' + e.message);
        }
    }

    // --- Modal / UI helpers ---

    showDeleteModal() {
        const span = document.getElementById('delete-client-name');
        if (span && this.clientData) span.textContent = this.clientData.name;
        document.getElementById('delete-client-modal').style.display = 'flex';
    }
    hideDeleteModal() { document.getElementById('delete-client-modal').style.display = 'none'; }
    confirmDeleteClient() { this.hideDeleteModal(); this.deleteClient(); }
    goBack() { this.cleanup(); window.spaLocation.href = '/pages/redis-clients.html'; }

    showLoading(show) {
        const el = document.getElementById('loading-indicator');
        if (el) el.style.display = show ? 'flex' : 'none';
    }
    showError(message) {
        var errorDiv = document.getElementById('error-message');
        if (errorDiv) {
            var errorText = errorDiv.querySelector('.error-text');
            if (errorText) errorText.textContent = message;
            errorDiv.style.display = 'flex';
        }
        var existing = document.getElementById('error-toast');
        if (existing) existing.remove();
        var toast = document.createElement('div');
        toast.id = 'error-toast';
        toast.style.cssText = 'position:fixed;top:20px;left:50%;transform:translateX(-50%);background:var(--monster-red,#EF4444);color:#fff;padding:14px 24px;border-radius:8px;box-shadow:0 4px 12px rgba(0,0,0,0.4);z-index:10000;font-size:0.9rem;max-width:600px;display:flex;align-items:center;gap:10px;animation:slideDown 0.3s ease-out;';
        toast.innerHTML = '<span style="font-size:1.2rem;">&#9888;</span><span>' + message + '</span><button onclick="this.parentElement.remove()" style="background:none;border:none;color:#fff;cursor:pointer;margin-left:auto;font-size:1.1rem;line-height:1;padding:0 4px;">&times;</button>';
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
        if (errorEl) errorEl.style.display = 'none';
    }
    showSuccess(message) {
        var existing = document.getElementById('success-toast'); if (existing) existing.remove();
        var toast = document.createElement('div'); toast.id = 'success-toast';
        toast.style.cssText = 'position:fixed;top:20px;left:50%;transform:translateX(-50%);background:var(--monster-green,#10B981);color:#fff;padding:14px 24px;border-radius:8px;box-shadow:0 4px 12px rgba(0,0,0,0.4);z-index:10000;font-size:0.9rem;max-width:600px;display:flex;align-items:center;gap:10px;animation:slideDown 0.3s ease-out;';
        toast.innerHTML = '<span style="font-size:1.2rem;">&#10003;</span><span>' + this.escapeHtml(message) + '</span><button onclick="this.parentElement.remove()" style="background:none;border:none;color:#fff;cursor:pointer;margin-left:auto;font-size:1.1rem;line-height:1;padding:0 4px;">&times;</button>';
        if (!document.getElementById('toast-anim-style')) { var s = document.createElement('style'); s.id = 'toast-anim-style'; s.textContent = '@keyframes slideDown{from{transform:translateX(-50%) translateY(-100%);opacity:0;}to{transform:translateX(-50%) translateY(0);opacity:1;}}@keyframes fadeOut{from{opacity:1;}to{opacity:0;}}'; document.head.appendChild(s); }
        document.body.appendChild(toast);
        setTimeout(function() { if (toast.parentElement) { toast.style.animation = 'fadeOut 0.3s ease-out forwards'; setTimeout(function() { if (toast.parentElement) toast.remove(); }, 300); } }, 3000);
    }
    escapeHtml(t) { const div = document.createElement('div'); div.textContent = t; return div.innerHTML; }
    setText(id, value) { const el = document.getElementById(id); if (el) el.textContent = value; }
}

// Toggle KV_SYNC-specific fields visibility
function onAddrModeChange(mode) {
    const kvFields = document.querySelectorAll('.kv-fields');
    const psubGroup = document.getElementById('addr-pattern-subscribe-group');
    if (mode === 'KV_SYNC') {
        kvFields.forEach(el => el.style.display = 'block');
        if (psubGroup) psubGroup.style.display = 'none';
    } else {
        kvFields.forEach(el => el.style.display = 'none');
        if (psubGroup) psubGroup.style.display = 'block';
    }
    if (redisDetailManager) {
        redisDetailManager.updateTopicDirection();
    }
}

// Global wrappers
let redisDetailManager;
function saveClient() { redisDetailManager.saveClient(); }
function toggleClient() { redisDetailManager.toggleClient(); }
function goBack() { redisDetailManager.goBack(); }
function showDeleteModal() { redisDetailManager.showDeleteModal(); }
function hideDeleteModal() { redisDetailManager.hideDeleteModal(); }
function confirmDeleteClient() { redisDetailManager.confirmDeleteClient(); }
function showAddAddressModal() { redisDetailManager.showAddAddressModal(); }
function hideAddAddressModal() { redisDetailManager.hideAddAddressModal(); }
function saveAddressMapping() { redisDetailManager.saveAddressMapping(); }

// Initialize
document.addEventListener('DOMContentLoaded', () => {
    redisDetailManager = new RedisClientDetailManager();
});

document.addEventListener('click', e => {
    if (e.target.classList.contains('modal')) {
        if (e.target.id === 'delete-client-modal') redisDetailManager.hideDeleteModal();
        if (e.target.id === 'add-address-modal') redisDetailManager.hideAddAddressModal();
    }
});
