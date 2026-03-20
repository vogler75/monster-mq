// Telegram Client Detail Management JavaScript

class TelegramClientDetailManager {
    constructor() {
        this.client = new GraphQLDashboardClient();
        this.clientName = null;
        this.clientData = null;
        this.clusterNodes = [];
        this.metricsTimer = null;
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
            this.showError('No Telegram client specified');
            return;
        }
        await this.loadClusterNodes();
        await this.loadClientData();
        this.metricsTimer = setInterval(() => this.refreshMetrics(), 10000);
        window.addEventListener('beforeunload', () => this.cleanup());
    }

    cleanup() {
        if (this.metricsTimer) { clearInterval(this.metricsTimer); this.metricsTimer = null; }
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
                query GetTelegramClients($name: String!) {
                    telegramClients(name: $name) {
                        name namespace nodeId enabled isOnCurrentNode createdAt updatedAt
                        config {
                            botToken pollingTimeoutSeconds reconnectDelayMs
                            proxyHost proxyPort parseMode allowedUsers
                        }
                        metrics { messagesIn messagesOut registeredChats timestamp }
                    }
                }
            `;
            const result = await this.client.query(query, { name: this.clientName });
            if (!result.telegramClients || result.telegramClients.length === 0) throw new Error('Telegram client not found');
            this.clientData = result.telegramClients[0];
            this.renderClientInfo();
            this.renderMetrics();
        } catch (e) {
            console.error('Error loading Telegram client', e);
            this.showError('Failed to load Telegram client: ' + e.message);
        } finally {
            this.showLoading(false);
        }
    }

    showNewClientForm() {
        document.getElementById('page-title').textContent = 'Add Telegram Client';
        document.getElementById('page-subtitle').textContent = 'Create a new Telegram Bot bridge';

        document.getElementById('client-name').value = '';
        document.getElementById('client-name').disabled = false;
        document.getElementById('client-namespace').value = '';
        document.getElementById('client-bot-token').value = '';
        document.getElementById('client-bot-token').placeholder = '123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11';
        document.getElementById('client-parse-mode').value = 'Text';
        document.getElementById('client-polling-timeout').value = '30';
        document.getElementById('client-reconnect-delay').value = '5000';
        document.getElementById('client-proxy-host').value = '';
        document.getElementById('client-proxy-port').value = '';
        document.getElementById('client-allowed-users').value = '';
        document.getElementById('client-enabled').checked = true;

        const deleteBtn = document.getElementById('delete-btn');
        if (deleteBtn) deleteBtn.style.display = 'none';
        const metricsSection = document.getElementById('metrics-section');
        if (metricsSection) metricsSection.style.display = 'none';
        const timestampsRow = document.getElementById('timestamps-row');
        if (timestampsRow) timestampsRow.style.display = 'none';
        const topicInfoSection = document.getElementById('topic-info-section');
        if (topicInfoSection) topicInfoSection.style.display = 'none';

        const saveBtn = document.getElementById('save-client-btn');
        if (saveBtn) saveBtn.innerHTML = saveBtn.innerHTML.replace('Save Client', 'Create Client');

        document.getElementById('client-content').style.display = 'block';
    }

    renderClientInfo() {
        if (!this.clientData) return;
        const d = this.clientData;
        const cfg = d.config;

        document.getElementById('page-title').textContent = `Telegram Client: ${d.name}`;
        document.getElementById('page-subtitle').textContent = `${d.namespace} \u2014 Bot token: ${cfg.botToken || '****'}`;

        document.getElementById('client-name').value = d.name;
        document.getElementById('client-name').disabled = true;
        document.getElementById('client-namespace').value = d.namespace;
        document.getElementById('client-node').value = d.nodeId;
        document.getElementById('client-bot-token').value = '';
        document.getElementById('client-bot-token').placeholder = '(unchanged) ' + (cfg.botToken || '****');
        document.getElementById('client-parse-mode').value = cfg.parseMode || 'Text';
        document.getElementById('client-polling-timeout').value = cfg.pollingTimeoutSeconds || 30;
        document.getElementById('client-reconnect-delay').value = cfg.reconnectDelayMs || 5000;
        document.getElementById('client-proxy-host').value = cfg.proxyHost || '';
        document.getElementById('client-proxy-port').value = cfg.proxyPort || '';
        document.getElementById('client-allowed-users').value = (cfg.allowedUsers || []).join(', ');
        document.getElementById('client-enabled').checked = d.enabled;

        // Topic structure display
        this.setText('topic-chats', d.namespace + '/chats');
        this.setText('topic-in', d.namespace + '/in/{chatId}');
        this.setText('topic-out', d.namespace + '/out/{chatId}');

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
    }

    // --- Metrics ---

    renderMetrics() {
        if (!this.clientData || !this.clientData.metrics) return;
        const m = this.clientData.metrics;
        if (!m || m.length === 0) return;
        const latest = m[0];
        this.setText('metric-messages-in', Math.round(latest.messagesIn));
        this.setText('metric-messages-out', Math.round(latest.messagesOut));
        this.setText('metric-registered-chats', latest.registeredChats || 0);
    }

    async refreshMetrics() {
        if (!this.clientName) return;
        try {
            const query = `query GetTelegramClientMetrics($name: String!) {
                telegramClients(name: $name) { metrics { messagesIn messagesOut registeredChats timestamp } }
            }`;
            const result = await this.client.query(query, { name: this.clientName });
            if (result.telegramClients && result.telegramClients.length > 0) {
                this.clientData.metrics = result.telegramClients[0].metrics;
                this.renderMetrics();
            }
        } catch (e) {
            console.warn('Metrics refresh failed', e.message);
        }
    }

    // --- Save / Delete ---

    collectFormData() {
        const botToken = document.getElementById('client-bot-token').value.trim();
        const configInput = {
            pollingTimeoutSeconds: parseInt(document.getElementById('client-polling-timeout').value) || 30,
            reconnectDelayMs: parseInt(document.getElementById('client-reconnect-delay').value) || 5000,
            parseMode: document.getElementById('client-parse-mode').value || 'Text',
        };
        if (botToken) configInput.botToken = botToken;

        const proxyHost = document.getElementById('client-proxy-host').value.trim();
        const proxyPort = document.getElementById('client-proxy-port').value.trim();
        if (proxyHost) configInput.proxyHost = proxyHost;
        if (proxyPort) configInput.proxyPort = parseInt(proxyPort);

        const allowedUsersStr = document.getElementById('client-allowed-users').value.trim();
        configInput.allowedUsers = allowedUsersStr ? allowedUsersStr.split(',').map(u => u.trim()).filter(u => u.length > 0) : [];

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

        if (this.isNew && !data.config.botToken) {
            this.showError('Bot Token is required for new clients');
            return;
        }

        if (this.isNew) {
            try {
                const mutation = `
                    mutation CreateTelegramClient($input: TelegramClientInput!) {
                        telegramClient { create(input: $input) { success errors client { name } } }
                    }
                `;
                const result = await this.client.query(mutation, { input: data });
                if (result.telegramClient.create.success) {
                    this.showSuccess(`Telegram client "${data.name}" created successfully`);
                    setTimeout(() => { window.spaLocation.href = '/pages/telegram-clients.html'; }, 800);
                } else {
                    const errors = result.telegramClient.create.errors || ['Unknown error'];
                    this.showError('Failed to create Telegram client: ' + errors.join(', '));
                }
            } catch (e) {
                console.error('Error creating Telegram client', e);
                this.showError('Failed to create Telegram client: ' + e.message);
            }
            return;
        }

        const prevNodeId = this.clientData ? this.clientData.nodeId : null;
        try {
            const mutation = `
                mutation UpdateTelegramClient($name: String!, $input: TelegramClientInput!) {
                    telegramClient { update(name: $name, input: $input) { success errors client { name } } }
                }
            `;
            const result = await this.client.query(mutation, { name: this.clientName, input: data });
            if (result.telegramClient.update.success) {
                await this.loadClientData();
                if (prevNodeId && data.nodeId && data.nodeId !== prevNodeId) {
                    await this.reassignClient(data.nodeId);
                }
                this.showSuccess('Telegram client updated successfully');
            } else {
                const errors = result.telegramClient.update.errors || ['Unknown error'];
                this.showError('Failed to update Telegram client: ' + errors.join(', '));
            }
        } catch (e) {
            console.error('Error updating Telegram client', e);
            this.showError('Failed to update Telegram client: ' + e.message);
        }
    }

    async reassignClient(newNodeId) {
        try {
            const mutation = `
                mutation ReassignTelegramClient($name: String!, $nodeId: String!) {
                    telegramClient { reassign(name: $name, nodeId: $nodeId) { success errors client { nodeId } } }
                }
            `;
            const result = await this.client.query(mutation, { name: this.clientName, nodeId: newNodeId });
            if (!result.telegramClient.reassign.success) {
                this.showError('Reassign warning: ' + (result.telegramClient.reassign.errors || []).join(', '));
            } else {
                await this.loadClientData();
                this.showSuccess('Client reassigned to node ' + newNodeId);
            }
        } catch (e) {
            this.showError('Failed to reassign Telegram client: ' + e.message);
        }
    }

    async deleteClient() {
        try {
            const mutation = `mutation DeleteTelegramClient($name: String!) { telegramClient { delete(name: $name) } }`;
            const result = await this.client.query(mutation, { name: this.clientName });
            if (result.telegramClient.delete) {
                this.showSuccess('Telegram client deleted');
                this.cleanup();
                setTimeout(() => { window.spaLocation.href = '/pages/telegram-clients.html'; }, 800);
            } else {
                this.showError('Failed to delete Telegram client');
            }
        } catch (e) {
            this.showError('Failed to delete Telegram client: ' + e.message);
        }
    }

    // --- UI helpers ---

    showDeleteModal() {
        const span = document.getElementById('delete-client-name');
        if (span && this.clientData) span.textContent = this.clientData.name;
        document.getElementById('delete-client-modal').style.display = 'flex';
    }
    hideDeleteModal() { document.getElementById('delete-client-modal').style.display = 'none'; }
    confirmDeleteClient() { this.hideDeleteModal(); this.deleteClient(); }
    goBack() { this.cleanup(); window.spaLocation.href = '/pages/telegram-clients.html'; }

    showLoading(show) { const el = document.getElementById('loading-indicator'); if (el) el.style.display = show ? 'flex' : 'none'; }
    showError(message) {
        var errorDiv = document.getElementById('error-message');
        if (errorDiv) { var errorText = errorDiv.querySelector('.error-text'); if (errorText) errorText.textContent = message; errorDiv.style.display = 'flex'; }
        var existing = document.getElementById('error-toast'); if (existing) existing.remove();
        var toast = document.createElement('div'); toast.id = 'error-toast';
        toast.style.cssText = 'position:fixed;top:20px;left:50%;transform:translateX(-50%);background:var(--monster-red,#EF4444);color:#fff;padding:14px 24px;border-radius:8px;box-shadow:0 4px 12px rgba(0,0,0,0.4);z-index:10000;font-size:0.9rem;max-width:600px;display:flex;align-items:center;gap:10px;animation:slideDown 0.3s ease-out;';
        toast.innerHTML = '<span style="font-size:1.2rem;">&#9888;</span><span>' + message + '</span><button onclick="this.parentElement.remove()" style="background:none;border:none;color:#fff;cursor:pointer;margin-left:auto;font-size:1.1rem;line-height:1;padding:0 4px;">&times;</button>';
        if (!document.getElementById('error-toast-style')) { var style = document.createElement('style'); style.id = 'error-toast-style'; style.textContent = '@keyframes slideDown{from{transform:translateX(-50%) translateY(-100%);opacity:0;}to{transform:translateX(-50%) translateY(0);opacity:1;}}'; document.head.appendChild(style); }
        document.body.appendChild(toast);
        setTimeout(function() { if (toast.parentElement) toast.remove(); if (errorDiv) errorDiv.style.display = 'none'; }, 8000);
    }
    hideError() { const el = document.getElementById('error-message'); if (el) el.style.display = 'none'; }
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

// Global wrappers
let telegramDetailManager;
function saveClient() { telegramDetailManager.saveClient(); }
function goBack() { telegramDetailManager.goBack(); }
function showDeleteModal() { telegramDetailManager.showDeleteModal(); }
function hideDeleteModal() { telegramDetailManager.hideDeleteModal(); }
function confirmDeleteClient() { telegramDetailManager.confirmDeleteClient(); }

document.addEventListener('DOMContentLoaded', () => { telegramDetailManager = new TelegramClientDetailManager(); });
document.addEventListener('click', e => {
    if (e.target.classList.contains('modal') && e.target.id === 'delete-client-modal') telegramDetailManager.hideDeleteModal();
});
