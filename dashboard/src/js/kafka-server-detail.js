// Kafka Server Editor/Detail JavaScript

class KafkaServerDetail {
    constructor() {
        this.client = new GraphQLDashboardClient();
        this.serverName = null;
        this.isNew = true;
        this.storeOptionsHtml = '';
        this.detailPollInterval = null;
        this.init();
    }

    async init() {
        console.log('Initializing Kafka Server Detail page...');
        
        window.registerPageCleanup?.(() => {
            if (this.detailPollInterval) {
                clearInterval(this.detailPollInterval);
            }
        });
        
        // Parse URL params
        const params = new URLSearchParams(window.location.search);
        this.serverName = params.get('server');
        this.isNew = params.get('new') === 'true' || !this.serverName;

        this.showLoading(true);
        try {
            await this.loadClusterNodes();
            await this.loadDatabaseConnections();
            if (!this.isNew) {
                await this.loadServerDetails();
                document.getElementById('delete-btn').style.display = 'inline-flex';
                document.getElementById('server-name').disabled = true; // Name is read-only during edit
            } else {
                document.getElementById('page-title').textContent = 'New Kafka Server';
                document.getElementById('page-subtitle').textContent = 'Create a new Kafka protocol listener';
                document.getElementById('timestamp-section').style.display = 'none';
                document.getElementById('server-status').style.display = 'none';
                // Add one empty stream row to start
                this.addStreamRow('', '', 168);
            }
        } catch (e) {
            console.error('Initialization error:', e);
            this.showError('Initialization failed: ' + e.message);
        } finally {
            this.showLoading(false);
        }
    }

    async loadClusterNodes() {
        try {
            const query = `{ brokers { nodeId isCurrent } }`;
            const result = await this.client.query(query);
            const select = document.getElementById('server-node');
            if (result && result.brokers) {
                result.brokers.forEach(broker => {
                    const opt = document.createElement('option');
                    opt.value = broker.nodeId;
                    opt.textContent = `${broker.nodeId} ${broker.isCurrent ? '(Current)' : ''}`;
                    select.appendChild(opt);
                });
            }
        } catch (e) {
            console.error('Failed to load cluster nodes:', e);
        }
    }

    async loadDatabaseConnections() {
        try {
            const query = `
                query {
                    databaseConnections {
                        name
                        type
                    }
                }
            `;
            const result = await this.client.query(query);
            const select = document.getElementById('server-store');
            if (result && result.databaseConnections) {
                result.databaseConnections.forEach(conn => {
                    const nameUpper = conn.name.toUpperCase();
                    if (['POSTGRES', 'MONGODB', 'SQLITE', 'MEMORY', 'CRATEDB', 'DEFAULT'].includes(nameUpper)) {
                        return;
                    }
                    const opt = document.createElement('option');
                    opt.value = conn.name;
                    opt.textContent = `${conn.name} (${conn.type})`;
                    select.appendChild(opt);
                });
            }
            this.storeOptionsHtml = Array.from(select.options).map(opt => {
                if (opt.value === "") {
                    return `<option value="">Server Default</option>`;
                }
                return `<option value="${this.escapeHtml(opt.value)}">${this.escapeHtml(opt.textContent)}</option>`;
            }).join('');
        } catch (e) {
            console.error('Failed to load database connections:', e);
        }
    }

    async loadServerDetails() {
        const query = `
            query GetServer($name: String!) {
                kafkaServer(name: $name) {
                    name namespace nodeId enabled host port storeType status createdAt updatedAt
                    streams { streamName topicFilter retentionHours storeType }
                }
            }
        `;
        const result = await this.client.query(query, { name: this.serverName });
        const server = result.kafkaServer;
        if (!server) throw new Error(`Kafka server "${this.serverName}" not found`);

        document.getElementById('page-title').textContent = server.name;
        document.getElementById('page-subtitle').textContent = `Exposing MQTT topics on port ${server.port}`;

        // Populate fields
        document.getElementById('server-name').value = server.name;
        document.getElementById('server-namespace').value = server.namespace;
        document.getElementById('server-host').value = server.host;
        document.getElementById('server-port').value = server.port;
        document.getElementById('server-node').value = server.nodeId;
        document.getElementById('server-store').value = server.storeType || '';
        document.getElementById('server-enabled').checked = server.enabled;

        // Status badge
        const badge = document.getElementById('server-status');
        badge.textContent = server.status;
        badge.className = 'status-badge';
        if (server.status === 'RUNNING') badge.classList.add('status-enabled');
        else if (server.status === 'STARTING') badge.classList.add('status-starting');
        else if (server.status === 'ERROR') badge.classList.add('status-disabled');
        else badge.classList.add('status-disabled');

        // Dynamic fast status polling for deploying servers on detail page
        if (server.status === 'STARTING') {
            if (!this.detailPollInterval) {
                console.log('Starting fast status poll on detail page...');
                this.detailPollInterval = setInterval(() => this.loadServerDetails(), 1000);
            }
        } else {
            if (this.detailPollInterval) {
                console.log('Clearing fast status poll on detail page...');
                clearInterval(this.detailPollInterval);
                this.detailPollInterval = null;
            }
        }

        // Timestamps
        document.getElementById('server-created-at').textContent = new Date(server.createdAt).toLocaleString();
        document.getElementById('server-updated-at').textContent = new Date(server.updatedAt).toLocaleString();

        // Populate streams
        const tbody = document.getElementById('streams-table-body');
        tbody.innerHTML = '';
        if (server.streams && server.streams.length > 0) {
            server.streams.forEach(s => this.addStreamRow(s.streamName, s.topicFilter, s.retentionHours, s.storeType));
        } else {
            this.addStreamRow('', '', 168, '');
        }
    }

    addStreamRow(streamName = '', topicFilter = '', retentionHours = 168, storeType = '') {
        const tbody = document.getElementById('streams-table-body');
        const row = document.createElement('tr');
        row.className = 'stream-config-row';
        row.innerHTML = `
            <td>
                <input type="text" class="stream-name form-control" required value="${this.escapeHtml(streamName)}" placeholder="e.g. sensors_temp">
            </td>
            <td>
                <input type="text" class="stream-topic-filter form-control" required value="${this.escapeHtml(topicFilter)}" placeholder="e.g. sensors/temp/#">
            </td>
            <td>
                <input type="number" class="stream-retention form-control" required value="${retentionHours}" min="1" max="8760">
            </td>
            <td>
                <select class="stream-store form-control">
                    ${this.storeOptionsHtml || '<option value="">Server Default</option>'}
                </select>
            </td>
            <td style="text-align:center; vertical-align:middle;">
                <ix-icon-button icon="trashcan" variant="primary" ghost size="24" class="btn-delete" title="Remove stream" onclick="removeStreamRow(this)"></ix-icon-button>
            </td>
        `;
        
        const select = row.querySelector('.stream-store');
        if (select) {
            select.value = storeType || '';
        }
        
        tbody.appendChild(row);
    }

    async saveServer() {
        this.showLoading(true);
        this.hideError();

        try {
            const name = document.getElementById('server-name').value.trim();
            const namespace = document.getElementById('server-namespace').value.trim();
            const host = document.getElementById('server-host').value.trim();
            const port = parseInt(document.getElementById('server-port').value, 10);
            const nodeId = document.getElementById('server-node').value;
            const storeType = document.getElementById('server-store').value || null;
            const enabled = document.getElementById('server-enabled').checked;

            if (!name || !namespace || !host || !port) {
                throw new Error('Please fill in all required fields.');
            }

            // Gather streams
            const streams = [];
            const rows = document.querySelectorAll('.stream-config-row');
            for (const row of rows) {
                const streamName = row.querySelector('.stream-name').value.trim();
                const topicFilter = row.querySelector('.stream-topic-filter').value.trim();
                const retentionHours = parseInt(row.querySelector('.stream-retention').value, 10);
                const streamStore = row.querySelector('.stream-store').value || null;
                if (streamName && topicFilter) {
                    streams.push({ streamName, topicFilter, retentionHours, storeType: streamStore });
                }
            }

            if (streams.length === 0) {
                throw new Error('Please add at least one topic stream mapping.');
            }

            const input = { name, namespace, nodeId, enabled, host, port, storeType, streams };

            let success = false;
            let errors = [];

            if (this.isNew) {
                const mutation = `
                    mutation AddServer($input: KafkaServerInput!) {
                        kafkaServer {
                            add(input: $input) { success errors }
                        }
                    }
                `;
                const result = await this.client.query(mutation, { input });
                success = result.kafkaServer.add.success;
                errors = result.kafkaServer.add.errors || [];
            } else {
                const mutation = `
                    mutation UpdateServer($name: String!, $input: KafkaServerInput!) {
                        kafkaServer {
                            update(name: $name, input: $input) { success errors }
                        }
                    }
                `;
                const result = await this.client.query(mutation, { name: this.serverName, input });
                success = result.kafkaServer.update.success;
                errors = result.kafkaServer.update.errors || [];
            }

            if (success) {
                this.showSuccess(`Kafka server "${name}" saved successfully`);
                setTimeout(() => window.navigateTo('/pages/kafka-servers.html'), 1500);
            } else {
                throw new Error(errors.join(', ') || 'Unknown mutation error');
            }

        } catch (e) {
            console.error('Failed to save Kafka server:', e);
            this.showError(e.message);
        } finally {
            this.showLoading(false);
        }
    }

    showDeleteModal() {
        document.getElementById('delete-server-name').textContent = this.serverName;
        document.getElementById('delete-server-modal').style.display = 'flex';
    }

    hideDeleteModal() {
        document.getElementById('delete-server-modal').style.display = 'none';
    }

    async confirmDeleteServer() {
        this.hideDeleteModal();
        this.showLoading(true);
        try {
            const mutation = `
                mutation DeleteServer($name: String!) {
                    kafkaServer { delete(name: $name) }
                }
            `;
            const result = await this.client.query(mutation, { name: this.serverName });
            if (result.kafkaServer.delete) {
                this.showSuccess(`Kafka server "${this.serverName}" deleted successfully`);
                setTimeout(() => window.navigateTo('/pages/kafka-servers.html'), 1500);
            } else {
                throw new Error('Mutation returned false');
            }
        } catch (e) {
            console.error('Failed to delete Kafka server:', e);
            this.showError('Failed to delete Kafka server: ' + e.message);
        } finally {
            this.showLoading(false);
        }
    }

    // UI helpers
    showLoading(show) { const el = document.getElementById('loading-indicator'); if (el) el.style.display = show ? 'flex' : 'none'; }
    showError(message) { const errorEl = document.getElementById('error-message'); const errorText = document.querySelector('#error-message .error-text'); if (errorEl && errorText) { errorText.textContent = message; errorEl.style.display = 'flex'; setTimeout(()=>this.hideError(),5000);} }
    hideError() { const errorEl = document.getElementById('error-message'); if (errorEl) errorEl.style.display='none'; }
    showSuccess(message) { var existing = document.getElementById('success-toast'); if (existing) existing.remove(); var toast = document.createElement('div'); toast.id = 'success-toast'; toast.style.cssText = 'position:fixed;top:20px;left:50%;transform:translateX(-50%);background:var(--monster-green,#10B981);color:#fff;padding:14px 24px;border-radius:8px;box-shadow:0 4px 12px rgba(0,0,0,0.4);z-index:10000;font-size:0.9rem;max-width:600px;display:flex;align-items:center;gap:10px;animation:slideDown 0.3s ease-out;'; toast.innerHTML = '<span style="font-size:1.2rem;">&#10003;</span><span>' + this.escapeHtml(message) + '</span><button onclick="this.parentElement.remove()" style="background:none;border:none;color:#fff;cursor:pointer;margin-left:auto;font-size:1.1rem;line-height:1;padding:0 4px;">&times;</button>'; if (!document.getElementById('toast-anim-style')) { var s = document.createElement('style'); s.id = 'toast-anim-style'; s.textContent = '@keyframes slideDown{from{transform:translateX(-50%) translateY(-100%);opacity:0;}to{transform:translateX(-50%) translateY(0);opacity:1;}}@keyframes fadeOut{from{opacity:1;}to{opacity:0;}}'; document.head.appendChild(s); } document.body.appendChild(toast); setTimeout(function() { if (toast.parentElement) { toast.style.animation = 'fadeOut 0.3s ease-out forwards'; setTimeout(function() { if (toast.parentElement) toast.remove(); }, 300); } }, 3000); }
    escapeHtml(text) { const div=document.createElement('div'); div.textContent=text; return div.innerHTML; }
}

// Global wrappers
var saveServer = () => kafkaServerDetail.saveServer();
var showDeleteModal = () => kafkaServerDetail.showDeleteModal();
var hideDeleteModal = () => kafkaServerDetail.hideDeleteModal();
var confirmDeleteServer = () => kafkaServerDetail.confirmDeleteServer();
var addStreamRow = () => kafkaServerDetail.addStreamRow('', '', 168, '');
function removeStreamRow(btn) {
    const row = btn.closest('.stream-config-row');
    if (row) {
        const tbody = document.getElementById('streams-table-body');
        if (tbody.querySelectorAll('.stream-config-row').length > 1) {
            row.remove();
        } else {
            // Keep at least one row, just clear it
            row.querySelector('.stream-name').value = '';
            row.querySelector('.stream-topic-filter').value = '';
            row.querySelector('.stream-retention').value = '168';
            row.querySelector('.stream-store').value = '';
        }
    }
}
function goBack() {
    window.navigateTo('/pages/kafka-servers.html');
}

// Initialize
var kafkaServerDetail;
document.addEventListener('DOMContentLoaded', () => { kafkaServerDetail = new KafkaServerDetail(); });
document.addEventListener('click', e => { if (e.target.classList.contains('modal')) { if (e.target.id === 'delete-server-modal') kafkaServerDetail.hideDeleteModal(); }});
