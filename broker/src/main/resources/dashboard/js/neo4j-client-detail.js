// Neo4j Client Detail Management JavaScript

class Neo4jClientDetailManager {
    constructor() {
        this.client = new GraphQLDashboardClient('/graphql');
        this.clientName = null; // URL param (original name for updates)
        this.clientData = null; // Loaded neo4jClient object
        this.clusterNodes = [];
        this.metricsTimer = null;
        this.init();
    }

    async init() {
        const urlParams = new URLSearchParams(window.location.search);
        this.clientName = urlParams.get('client');
        if (!this.clientName) {
            this.showError('No Neo4j client specified');
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
                query GetNeo4jClients($name: String!) {
                    neo4jClients(name: $name) {
                        name namespace nodeId enabled isOnCurrentNode createdAt updatedAt
                        config { url username topicFilters queueSize batchSize reconnectDelayMs }
                        metrics { messagesIn messagesWritten errors pathQueueSize messagesInRate messagesWrittenRate timestamp }
                    }
                }
            `;
            const result = await this.client.query(query, { name: this.clientName });
            if (!result.neo4jClients || result.neo4jClients.length === 0) throw new Error('Neo4j client not found');
            this.clientData = result.neo4jClients[0];
            this.renderClientInfo();
            this.renderMetrics();
        } catch (e) {
            console.error('Error loading Neo4j client', e);
            this.showError('Failed to load Neo4j client: ' + e.message);
        } finally {
            this.showLoading(false);
        }
    }

    renderClientInfo() {
        if (!this.clientData) return;
        const d = this.clientData;
        const cfg = d.config;

        // Update page title and subtitle
        document.getElementById('page-title').textContent = `Neo4j Client: ${d.name}`;
        document.getElementById('page-subtitle').textContent = `${d.namespace} - ${cfg.url}`;

        // Populate form fields
        document.getElementById('client-name').value = d.name;
        document.getElementById('client-name').disabled = true; // Can't change name in edit mode
        document.getElementById('client-namespace').value = d.namespace;
        document.getElementById('client-node').value = d.nodeId;
        document.getElementById('client-url').value = cfg.url;
        document.getElementById('client-username').value = cfg.username;
        document.getElementById('client-queue-size').value = cfg.queueSize;
        document.getElementById('client-batch-size').value = cfg.batchSize;
        document.getElementById('client-reconnect-delay').value = cfg.reconnectDelayMs;
        document.getElementById('client-enabled').checked = d.enabled;

        // Topic filters - populate textarea
        const topicFilters = cfg.topicFilters && cfg.topicFilters.length > 0
            ? cfg.topicFilters.join('\n')
            : '#';
        document.getElementById('client-topic-filters').value = topicFilters;

        // Timestamps (read-only)
        this.setText('client-created-at', new Date(d.createdAt).toLocaleString());
        this.setText('client-updated-at', new Date(d.updatedAt).toLocaleString());

        // Status badge
        const statusBadge = document.getElementById('client-status');
        if (d.enabled) {
            statusBadge.className = 'status-badge status-enabled';
            statusBadge.textContent = 'ENABLED';
        } else {
            statusBadge.className = 'status-badge status-disabled';
            statusBadge.textContent = 'DISABLED';
        }

        // Toggle button
        const toggleBtn = document.getElementById('toggle-client-btn');
        if (toggleBtn) {
            toggleBtn.textContent = d.enabled ? 'Stop Client' : 'Start Client';
            toggleBtn.className = d.enabled ? 'btn btn-warning' : 'btn btn-success';
        }

        // Show content
        document.getElementById('client-content').style.display = 'block';
    }

    renderMetrics() {
        if (!this.clientData || !this.clientData.metrics) return;
        const m = this.clientData.metrics;
        if (m.length === 0) return;
        const latest = m[0];
        this.setText('metric-messages-in', Math.round(latest.messagesIn));
        this.setText('metric-messages-written', Math.round(latest.messagesWritten));
        this.setText('metric-errors', Math.round(latest.errors));
        this.setText('metric-path-queue-size', latest.pathQueueSize);
        this.setText('metric-messages-in-rate', Math.round(latest.messagesInRate * 10) / 10);
        this.setText('metric-messages-written-rate', Math.round(latest.messagesWrittenRate * 10) / 10);
        this.setText('metric-timestamp', latest.timestamp ? new Date(latest.timestamp).toLocaleString() : '-');
    }

    async refreshMetrics() {
        if (!this.clientName) return;
        try {
            const query = `
                query GetNeo4jClientMetrics($name: String!) {
                    neo4jClients(name: $name) {
                        metrics { messagesIn messagesWritten errors pathQueueSize messagesInRate messagesWrittenRate timestamp }
                    }
                }
            `;
            const result = await this.client.query(query, { name: this.clientName });
            if (result.neo4jClients && result.neo4jClients.length > 0) {
                this.clientData.metrics = result.neo4jClients[0].metrics;
                this.renderMetrics();
            }
        } catch (e) {
            console.warn('Metrics refresh failed', e.message);
        }
    }

    async saveClient() {
        const form = document.getElementById('client-form');
        if (!form.checkValidity()) { form.reportValidity(); return; }

        // Parse topic filters from textarea
        const topicFiltersText = document.getElementById('client-topic-filters').value.trim();
        const topicFilters = topicFiltersText
            .split('\n')
            .map(line => line.trim())
            .filter(line => line.length > 0);

        // Handle password - only include if changed
        const passwordField = document.getElementById('client-password');
        const password = passwordField.value.trim();

        const updatedInput = {
            name: document.getElementById('client-name').value.trim(),
            namespace: document.getElementById('client-namespace').value.trim(),
            nodeId: document.getElementById('client-node').value,
            enabled: document.getElementById('client-enabled').checked,
            config: {
                url: document.getElementById('client-url').value.trim(),
                username: document.getElementById('client-username').value.trim(),
                password: password.length > 0 ? password : this.clientData?.config?.password || '',
                topicFilters: topicFilters,
                queueSize: parseInt(document.getElementById('client-queue-size').value),
                batchSize: parseInt(document.getElementById('client-batch-size').value),
                reconnectDelayMs: parseInt(document.getElementById('client-reconnect-delay').value)
            }
        };

        const prevNodeId = this.clientData ? this.clientData.nodeId : null;

        try {
            const mutation = `
                mutation UpdateNeo4jClient($name: String!, $input: Neo4jClientInput!) {
                    neo4jClient {
                        update(name: $name, input: $input) { success errors client { name } }
                    }
                }
            `;
            const result = await this.client.query(mutation, { name: this.clientName, input: updatedInput });
            if (result.neo4jClient.update.success) {
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
                this.showSuccess('Neo4j client updated successfully');
            } else {
                const errors = result.neo4jClient.update.errors || ['Unknown error'];
                this.showError('Failed to update Neo4j client: ' + errors.join(', '));
            }
        } catch (e) {
            console.error('Error updating Neo4j client', e);
            this.showError('Failed to update Neo4j client: ' + e.message);
        }
    }

    async reassignClient(newNodeId) {
        try {
            const mutation = `
                mutation ReassignNeo4jClient($name: String!, $nodeId: String!) {
                    neo4jClient {
                        reassign(name: $name, nodeId: $nodeId) { success errors client { nodeId } }
                    }
                }
            `;
            const result = await this.client.query(mutation, { name: this.clientName, nodeId: newNodeId });
            if (!result.neo4jClient.reassign.success) {
                const errs = result.neo4jClient.reassign.errors || ['Unknown error'];
                this.showError('Reassign warning: ' + errs.join(', '));
            } else {
                await this.loadClientData(); // refresh node assignment visuals
                this.showSuccess('Client reassigned to node ' + newNodeId);
            }
        } catch (e) {
            console.error('Error reassigning client', e);
            this.showError('Failed to reassign Neo4j client: ' + e.message);
        }
    }

    async toggleClient() {
        if (!this.clientData) return;
        const newState = !this.clientData.enabled;
        try {
            const mutation = `
                mutation ToggleNeo4jClient($name: String!, $enabled: Boolean!) {
                    neo4jClient {
                        toggle(name: $name, enabled: $enabled) { success errors client { enabled } }
                    }
                }
            `;
            const result = await this.client.query(mutation, { name: this.clientName, enabled: newState });
            if (result.neo4jClient.toggle.success) {
                await this.loadClientData();
                this.showSuccess(`Neo4j client ${newState ? 'started' : 'stopped'} successfully`);
            } else {
                const errors = result.neo4jClient.toggle.errors || ['Unknown error'];
                this.showError('Failed to toggle Neo4j client: ' + errors.join(', '));
            }
        } catch (e) {
            console.error('Toggle error', e);
            this.showError('Failed to toggle Neo4j client: ' + e.message);
        }
    }

    async deleteClient() {
        try {
            const mutation = `mutation DeleteNeo4jClient($name: String!) { neo4jClient { delete(name: $name) } }`;
            const result = await this.client.query(mutation, { name: this.clientName });
            if (result.neo4jClient.delete) {
                this.showSuccess('Neo4j client deleted');
                this.cleanup();
                setTimeout(() => { window.location.href = '/pages/neo4j-clients.html'; }, 800);
            } else {
                this.showError('Failed to delete Neo4j client');
            }
        } catch (e) {
            console.error('Delete error', e);
            this.showError('Failed to delete Neo4j client: ' + e.message);
        }
    }

    // UI helpers
    showDeleteModal() {
        const span = document.getElementById('delete-client-name');
        if (span && this.clientData) span.textContent = this.clientData.name;
        document.getElementById('delete-client-modal').style.display = 'flex';
    }
    hideDeleteModal() { document.getElementById('delete-client-modal').style.display = 'none'; }
    confirmDeleteClient() { this.hideDeleteModal(); this.deleteClient(); }
    goBack() { this.cleanup(); window.location.href = '/pages/neo4j-clients.html'; }

    showLoading(show) { const el = document.getElementById('loading-indicator'); if (el) el.style.display = show ? 'flex' : 'none'; }
    showError(message) { const errorEl = document.getElementById('error-message'); const text = document.querySelector('#error-message .error-text'); if (errorEl && text) { text.textContent = message; errorEl.style.display='flex'; setTimeout(()=>this.hideError(),5000);} }
    hideError() { const errorEl = document.getElementById('error-message'); if (errorEl) errorEl.style.display='none'; }
    showSuccess(message) { const note=document.createElement('div'); note.className='success-notification'; note.innerHTML=`<span class="success-icon">✅</span><span class="success-text">${this.escapeHtml(message)}</span>`; document.body.appendChild(note); setTimeout(()=>{ if(note.parentNode) note.parentNode.removeChild(note); },3000); }
    escapeHtml(t){ const div=document.createElement('div'); div.textContent=t; return div.innerHTML; }

    setText(id, value) { const el = document.getElementById(id); if (el) el.textContent = value; }
}

// Global wrappers
let neo4jClientDetailManager;
function saveClient() { neo4jClientDetailManager.saveClient(); }
function toggleClient() { neo4jClientDetailManager.toggleClient(); }
function goBack() { neo4jClientDetailManager.goBack(); }
function showDeleteModal() { neo4jClientDetailManager.showDeleteModal(); }
function hideDeleteModal() { neo4jClientDetailManager.hideDeleteModal(); }
function confirmDeleteClient() { neo4jClientDetailManager.confirmDeleteClient(); }

// Initialize
document.addEventListener('DOMContentLoaded', () => { neo4jClientDetailManager = new Neo4jClientDetailManager(); });

document.addEventListener('click', e => { if (e.target.classList.contains('modal')) { if (e.target.id === 'delete-client-modal') neo4jClientDetailManager.hideDeleteModal(); }});
