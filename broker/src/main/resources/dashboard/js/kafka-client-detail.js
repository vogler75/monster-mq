// Kafka Client Detail Management JavaScript

class KafkaClientDetailManager {
    constructor() {
        this.client = new GraphQLDashboardClient('/graphql');
        this.clientName = null; // URL param (original name for updates)
        this.clientData = null; // Loaded kafkaClient object
        this.clusterNodes = [];
        this.metricsTimer = null;
        this.init();
    }

    async init() {
        const urlParams = new URLSearchParams(window.location.search);
        this.clientName = urlParams.get('client');
        if (!this.clientName) {
            this.showError('No Kafka client specified');
            return;
        }
        await this.loadClusterNodes();
        await this.loadClientData();
        // Periodic metrics refresh
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
            const query = `query GetClusterNodes { clusterNodes { nodeId isCurrent } }`;
            const result = await this.client.query(query);
            this.clusterNodes = result.clusterNodes || [];
            const nodeSelect = document.getElementById('edit-client-node');
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
                query GetKafkaClients($name: String!) {
                    kafkaClients(name: $name) {
                        name namespace nodeId enabled isOnCurrentNode createdAt updatedAt
                        config { bootstrapServers groupId payloadFormat destinationTopicPrefix extraConsumerConfig pollIntervalMs maxPollRecords reconnectDelayMs }
                        metrics { messagesIn messagesOut timestamp }
                    }
                }
            `;
            const result = await this.client.query(query, { name: this.clientName });
            if (!result.kafkaClients || result.kafkaClients.length === 0) throw new Error('Kafka client not found');
            this.clientData = result.kafkaClients[0];
            this.renderClientInfo();
            this.renderMetrics();
        } catch (e) {
            console.error('Error loading Kafka client', e);
            this.showError('Failed to load Kafka client: ' + e.message);
        } finally {
            this.showLoading(false);
        }
    }

    renderClientInfo() {
        if (!this.clientData) return;
        const d = this.clientData;
        const cfg = d.config;
        document.getElementById('page-title').textContent = `Kafka Client: ${d.name}`;
        this.setText('client-name-display', d.name);
        this.setText('client-namespace', d.namespace);
        this.setText('client-node-id', d.nodeId + (d.isOnCurrentNode ? ' (Current)' : ''));
        this.setText('client-bootstrap', cfg.bootstrapServers);

        this.setText('client-group-id', cfg.groupId);

        this.setText('client-payload-format', cfg.payloadFormat);
        const destPref = cfg.destinationTopicPrefix ? cfg.destinationTopicPrefix : '(none)';
        this.setText('client-destination-prefix', destPref);
 
        this.setText('client-poll-interval', `${cfg.pollIntervalMs} ms`);
        this.setText('client-max-poll', cfg.maxPollRecords);
        this.setText('client-reconnect-delay', `${cfg.reconnectDelayMs} ms`);
        this.setText('client-created-at', new Date(d.createdAt).toLocaleString());
        this.setText('client-updated-at', new Date(d.updatedAt).toLocaleString());
        // Extra config display
        const extraCfgEl = document.getElementById('client-extra-config');
        if (extraCfgEl) {
            const extra = cfg.extraConsumerConfig && Object.keys(cfg.extraConsumerConfig).length>0 ? JSON.stringify(cfg.extraConsumerConfig, null, 2) : 'None';
            extraCfgEl.textContent = extra;
        }

        const statusBadge = document.getElementById('client-status');
        if (d.enabled) {
            statusBadge.className = 'status-badge status-enabled';
            statusBadge.textContent = 'ENABLED';
        } else {
            statusBadge.className = 'status-badge status-disabled';
            statusBadge.textContent = 'DISABLED';
        }
        const toggleBtn = document.getElementById('toggle-client-btn');
        if (toggleBtn) {
            toggleBtn.textContent = d.enabled ? 'Stop Client' : 'Start Client';
            toggleBtn.className = d.enabled ? 'btn btn-warning' : 'btn btn-success';
        }
        this.populateEditForm();
    }

    renderMetrics() {
        if (!this.clientData || !this.clientData.metrics) return;
        const m = this.clientData.metrics;
        if (m.length === 0) return;
        const latest = m[0];
        this.setText('metric-messages-in', Math.round(latest.messagesIn));
        this.setText('metric-messages-out', Math.round(latest.messagesOut));
        this.setText('metric-timestamp', latest.timestamp ? new Date(latest.timestamp).toLocaleString() : '-');
    }

    async refreshMetrics() {
        if (!this.clientName) return;
        try {
            const query = `query GetKafkaClientMetrics($name: String!) { kafkaClients(name: $name) { metrics { messagesIn messagesOut timestamp } } }`;
            const result = await this.client.query(query, { name: this.clientName });
            if (result.kafkaClients && result.kafkaClients.length > 0) {
                this.clientData.metrics = result.kafkaClients[0].metrics;
                this.renderMetrics();
            }
        } catch (e) {
            console.warn('Metrics refresh failed', e.message);
        }
    }

    populateEditForm() {
        if (!this.clientData) return;
        const d = this.clientData; const cfg = d.config;
        this.setValue('edit-client-name', d.name);
        this.setValue('edit-client-namespace', d.namespace);
        this.setValue('edit-client-node', d.nodeId);
        this.setValue('edit-client-bootstrap', cfg.bootstrapServers);

        this.setValue('edit-client-group-id', cfg.groupId);
        this.setValue('edit-client-destination-prefix', cfg.destinationTopicPrefix || '');
 
        this.setValue('edit-client-payload-format', cfg.payloadFormat);


        this.setValue('edit-client-poll-interval', cfg.pollIntervalMs);
        this.setValue('edit-client-max-poll', cfg.maxPollRecords);
        this.setValue('edit-client-reconnect-delay', cfg.reconnectDelayMs);
        document.getElementById('edit-client-enabled').checked = d.enabled;
        const extra = cfg.extraConsumerConfig ? JSON.stringify(cfg.extraConsumerConfig, null, 2) : '';
        this.setValue('edit-client-extra', extra);
        // Ensure trailing slash visually (normalization handled server-side)
        const destInput = document.getElementById('edit-client-destination-prefix');
        if(destInput && destInput.value && !destInput.value.endsWith('/')) destInput.value = destInput.value + '/';
    }

    async updateClient() {
        const form = document.getElementById('edit-client-form');
        if (!form.checkValidity()) { form.reportValidity(); return; }
        // Build input
        let extraConfigText = document.getElementById('edit-client-extra').value.trim();
        let extraConfig = null;
        if (extraConfigText.length > 0) {
            try { extraConfig = JSON.parse(extraConfigText); } catch (e) { this.showError('Invalid JSON in Extra Consumer Config: ' + e.message); return; }
            if (extraConfig === null || Array.isArray(extraConfig) || typeof extraConfig !== 'object') {
                this.showError('Extra Consumer Config must be a JSON object'); return;
            }
        }
        const updatedInput = {
            name: document.getElementById('edit-client-name').value.trim(),
            namespace: document.getElementById('edit-client-namespace').value.trim(),
            nodeId: document.getElementById('edit-client-node').value,
            enabled: document.getElementById('edit-client-enabled').checked,
                config: {
                    bootstrapServers: document.getElementById('edit-client-bootstrap').value.trim(),
                    groupId: document.getElementById('edit-client-group-id').value.trim(),
                    destinationTopicPrefix: (function(){ const v=document.getElementById('edit-client-destination-prefix').value.trim(); return v.length>0? v : null; })(),
                    payloadFormat: document.getElementById('edit-client-payload-format').value,
                    extraConsumerConfig: extraConfig,
                    pollIntervalMs: parseInt(document.getElementById('edit-client-poll-interval').value),
                    maxPollRecords: parseInt(document.getElementById('edit-client-max-poll').value),
                    reconnectDelayMs: parseInt(document.getElementById('edit-client-reconnect-delay').value)
                }

        };

        const prevNodeId = this.clientData ? this.clientData.nodeId : null;

        try {
            const mutation = `mutation UpdateKafkaClient($name: String!, $input: KafkaClientInput!) { kafkaClient { update(name: $name, input: $input) { success errors client { name } } } }`;
            const result = await this.client.query(mutation, { name: this.clientName, input: updatedInput });
            if (result.kafkaClient.update.success) {
                // Handle potential rename
                const newName = updatedInput.name;
                if (newName !== this.clientName) {
                    this.clientName = newName;
                    const url = new URL(window.location.href);
                    url.searchParams.set('client', newName);
                    window.history.replaceState({}, '', url.toString());
                }
                // Reload data first
                await this.loadClientData();
                // If node changed, explicitly call reassign mutation for proper event semantics
                if (prevNodeId && updatedInput.nodeId && updatedInput.nodeId !== prevNodeId) {
                    await this.reassignClient(updatedInput.nodeId);
                }
                this.hideEditModal();
                this.showSuccess('Kafka client updated successfully');
            } else {
                const errors = result.kafkaClient.update.errors || ['Unknown error'];
                this.showError('Failed to update Kafka client: ' + errors.join(', '));
            }
        } catch (e) {
            console.error('Error updating Kafka client', e);
            this.showError('Failed to update Kafka client: ' + e.message);
        }
    }

    async reassignClient(newNodeId) {
        try {
            const mutation = `mutation ReassignKafkaClient($name: String!, $nodeId: String!) { kafkaClient { reassign(name: $name, nodeId: $nodeId) { success errors client { nodeId } } } }`;
            const result = await this.client.query(mutation, { name: this.clientName, nodeId: newNodeId });
            if (!result.kafkaClient.reassign.success) {
                const errs = result.kafkaClient.reassign.errors || ['Unknown error'];
                this.showError('Reassign warning: ' + errs.join(', '));
            } else {
                await this.loadClientData(); // refresh node assignment visuals
                this.showSuccess('Client reassigned to node ' + newNodeId);
            }
        } catch (e) {
            console.error('Error reassigning client', e);
            this.showError('Failed to reassign Kafka client: ' + e.message);
        }
    }

    async toggleClient() {
        if (!this.clientData) return;
        const newState = !this.clientData.enabled;
        try {
            const mutation = `mutation ToggleKafkaClient($name: String!, $enabled: Boolean!) { kafkaClient { toggle(name: $name, enabled: $enabled) { success errors client { enabled } } } }`;
            const result = await this.client.query(mutation, { name: this.clientName, enabled: newState });
            if (result.kafkaClient.toggle.success) {
                await this.loadClientData();
                this.showSuccess(`Kafka client ${newState ? 'started' : 'stopped'} successfully`);
            } else {
                const errors = result.kafkaClient.toggle.errors || ['Unknown error'];
                this.showError('Failed to toggle Kafka client: ' + errors.join(', '));
            }
        } catch (e) {
            console.error('Toggle error', e);
            this.showError('Failed to toggle Kafka client: ' + e.message);
        }
    }

    async deleteClient() {
        if (!confirm('Are you sure you want to delete this Kafka client?')) return; // fallback in case modal not shown
        try {
            const mutation = `mutation DeleteKafkaClient($name: String!) { kafkaClient { delete(name: $name) } }`;
            const result = await this.client.query(mutation, { name: this.clientName });
            if (result.kafkaClient.delete) {
                this.showSuccess('Kafka client deleted');
                this.cleanup();
                setTimeout(() => { window.location.href = '/pages/kafka-clients.html'; }, 800);
            } else {
                this.showError('Failed to delete Kafka client');
            }
        } catch (e) {
            console.error('Delete error', e);
            this.showError('Failed to delete Kafka client: ' + e.message);
        }
    }

    // UI helpers
    showEditModal() { this.populateEditForm(); document.getElementById('edit-client-modal').style.display = 'flex'; const input=document.getElementById('edit-client-destination-prefix'); if(input && input.value && !input.value.endsWith('/')) { input.value = input.value + '/'; }}
    hideEditModal() { document.getElementById('edit-client-modal').style.display = 'none'; }
    showDeleteModal() { const span=document.getElementById('delete-client-name'); if(span && this.clientData) span.textContent=this.clientData.name; document.getElementById('delete-client-modal').style.display = 'flex'; }
    hideDeleteModal() { document.getElementById('delete-client-modal').style.display = 'none'; }
    confirmDeleteClient() { this.hideDeleteModal(); this.deleteClient(); }
    goBack() { this.cleanup(); window.location.href = '/pages/kafka-clients.html'; }

    showLoading(show) { const el = document.getElementById('loading-indicator'); if (el) el.style.display = show ? 'flex' : 'none'; }
    showError(message) { const errorEl = document.getElementById('error-message'); const text = document.querySelector('#error-message .error-text'); if (errorEl && text) { text.textContent = message; errorEl.style.display='flex'; setTimeout(()=>this.hideError(),5000);} }
    hideError() { const errorEl = document.getElementById('error-message'); if (errorEl) errorEl.style.display='none'; }
    showSuccess(message) { const note=document.createElement('div'); note.className='success-notification'; note.innerHTML=`<span class="success-icon">✅</span><span class="success-text">${this.escapeHtml(message)}</span>`; document.body.appendChild(note); setTimeout(()=>{ if(note.parentNode) note.parentNode.removeChild(note); },3000); }
    escapeHtml(t){ const div=document.createElement('div'); div.textContent=t; return div.innerHTML; }

    setText(id, value) { const el = document.getElementById(id); if (el) el.textContent = value; }
    setValue(id, value) { const el = document.getElementById(id); if (el) el.value = value; }
}

// Global wrappers
let kafkaClientDetailManager;
function showEditModal() { kafkaClientDetailManager.showEditModal(); }
function hideEditModal() { kafkaClientDetailManager.hideEditModal(); }
function updateClient() { kafkaClientDetailManager.updateClient(); }
function toggleClient() { kafkaClientDetailManager.toggleClient(); }
function goBack() { kafkaClientDetailManager.goBack(); }
function showDeleteModal() { kafkaClientDetailManager.showDeleteModal(); }
function hideDeleteModal() { kafkaClientDetailManager.hideDeleteModal(); }
function confirmDeleteClient() { kafkaClientDetailManager.confirmDeleteClient(); }

// Initialize
document.addEventListener('DOMContentLoaded', () => { kafkaClientDetailManager = new KafkaClientDetailManager(); });

document.addEventListener('click', e => { if (e.target.classList.contains('modal')) { if (e.target.id === 'edit-client-modal') kafkaClientDetailManager.hideEditModal(); else if (e.target.id === 'delete-client-modal') kafkaClientDetailManager.hideDeleteModal(); }});
