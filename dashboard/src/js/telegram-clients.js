// Telegram Client Management JavaScript

class TelegramClientManager {
    constructor() {
        this.client = new GraphQLDashboardClient();
        this.clients = [];
        this.deleteClientName = null;
        this.init();
    }

    async init() {
        await this.loadClients();
        setInterval(() => this.loadClients(), 30000);
    }

    async loadClients() {
        this.showLoading(true);
        this.hideError();
        try {
            const query = `
                query GetTelegramClients {
                    telegramClients {
                        name namespace nodeId enabled isOnCurrentNode createdAt updatedAt
                        config { botToken pollingTimeoutSeconds parseMode allowedUsers }
                        metrics { messagesIn messagesOut registeredChats }
                    }
                }
            `;
            const result = await this.client.query(query);
            if (!result || !result.telegramClients) throw new Error('Invalid response structure');
            this.clients = result.telegramClients;
            this.updateMetrics();
            this.renderClientsTable();
        } catch (e) {
            console.error('Error loading Telegram clients:', e);
            this.showError('Failed to load Telegram clients: ' + e.message);
        } finally {
            this.showLoading(false);
        }
    }

    updateMetrics() {
        document.getElementById('total-clients').textContent = this.clients.length;
        document.getElementById('enabled-clients').textContent = this.clients.filter(c => c.enabled).length;
        document.getElementById('current-node-clients').textContent = this.clients.filter(c => c.isOnCurrentNode).length;
        const totalChats = this.clients.reduce((sum, c) => sum + ((c.metrics && c.metrics.length > 0) ? c.metrics[0].registeredChats : 0), 0);
        document.getElementById('total-chats').textContent = totalChats;
    }

    renderClientsTable() {
        const tbody = document.getElementById('telegram-clients-table-body');
        if (!tbody) return;
        tbody.innerHTML = '';
        if (this.clients.length === 0) {
            tbody.innerHTML = ui.emptyRow(8, 'No Telegram clients configured',
                'Use “Add Client” to get started.');
            return;
        }
        this.clients.forEach(c => {
            const row = document.createElement('tr');
            const cfg = c.config || {};
            const botToken = cfg.botToken || '****';
            const statusClass = c.enabled ? 'status-enabled' : 'status-disabled';
            const statusText = c.enabled ? 'Enabled' : 'Disabled';
            const nodeIndicator = c.isOnCurrentNode ? '\u{1F4CD} ' : '';
            const metricsIn  = (c.metrics && c.metrics.length > 0) ? Math.round(c.metrics[0].messagesIn)  : 0;
            const metricsOut = (c.metrics && c.metrics.length > 0) ? Math.round(c.metrics[0].messagesOut) : 0;
            const chats = (c.metrics && c.metrics.length > 0) ? c.metrics[0].registeredChats : 0;
            row.innerHTML = `
                <td><div class="client-name">${this.escapeHtml(c.name)}</div>
                    <small style="color:var(--text-muted);">${this.escapeHtml(c.namespace)}</small></td>
                <td><small>${this.escapeHtml(botToken)}</small></td>
                <td>${nodeIndicator}${this.escapeHtml(c.nodeId || '')}</td>
                <td><span class="status-badge ${statusClass}">${statusText}</span></td>
                <td style="text-align:center;">${chats}</td>
                <td>${metricsIn}</td>
                <td>${metricsOut}</td>
                <td>
                    <div class="action-buttons">
                        <ix-icon-button icon="highlight" variant="subtle-tertiary" size="24" title="Edit client" class="btn-edit"></ix-icon-button>
                        <ix-icon-button icon="${c.enabled ? 'pause' : 'play'}" variant="subtle-tertiary" size="24" title="${c.enabled ? 'Disable' : 'Enable'}" class="btn-toggle"></ix-icon-button>
                        <ix-icon-button icon="trashcan" variant="subtle-tertiary" size="24" class="btn-delete" title="Delete"></ix-icon-button>
                    </div>
                </td>
            `;

            const editBtn = row.querySelector('.btn-edit');
            if (editBtn) editBtn.addEventListener('click', (e) => { e.stopPropagation(); this.viewClient(c.name); });
            const toggleBtn = row.querySelector('.btn-toggle');
            if (toggleBtn) toggleBtn.addEventListener('click', (e) => { e.stopPropagation(); this.toggleClient(c.name, !c.enabled); });
            const deleteBtn = row.querySelector('.btn-delete');
            if (deleteBtn) deleteBtn.addEventListener('click', (e) => { e.stopPropagation(); this.deleteClient(c.name); });

            tbody.appendChild(row);
        });
    }

    async toggleClient(clientName, enabled) {
        try {
            const mutation = `
                mutation ToggleTelegramClient($name: String!, $enabled: Boolean!) {
                    telegramClient { toggle(name: $name, enabled: $enabled) { success errors } }
                }
            `;
            const result = await this.client.query(mutation, { name: clientName, enabled });
            if (result && result.telegramClient && result.telegramClient.toggle && result.telegramClient.toggle.success) {
                await this.loadClients();
                this.showSuccess(`Telegram client "${clientName}" ${enabled ? 'enabled' : 'disabled'}`);
            } else {
                const errors = (result && result.telegramClient && result.telegramClient.toggle && result.telegramClient.toggle.errors) || [];
                this.showError('Failed to toggle: ' + errors.join(', '));
            }
        } catch (e) {
            this.showError('Failed to toggle Telegram client: ' + e.message);
        }
    }

    async deleteClient(clientName) {
        this.deleteClientName = clientName;
        if (await ui.confirmDelete(clientName, { title: 'Delete Telegram client' })) {
            await this.confirmDeleteClient();
        } else {
            this.deleteClientName = null;
        }
    }

    async confirmDeleteClient() {
        if (!this.deleteClientName) return;
        try {
            const mutation = `mutation DeleteTelegramClient($name: String!) { telegramClient { delete(name: $name) } }`;
            const result = await this.client.query(mutation, { name: this.deleteClientName });
            if (result && result.telegramClient && result.telegramClient.delete) {
                await this.loadClients();
                this.showSuccess(`Telegram client "${this.deleteClientName}" deleted`);
            } else {
                this.showError('Failed to delete Telegram client');
            }
        } catch (e) {
            this.showError('Failed to delete Telegram client: ' + e.message);
        }
        this.deleteClientName = null;
    }

    viewClient(clientName) {
        window.spaLocation.href = `/pages/telegram-client-detail.html?client=${encodeURIComponent(clientName)}`;
    }

    showLoading(show) { const el = document.getElementById('loading-indicator'); if (el) el.style.display = show ? 'flex' : 'none'; }
    showError(message) { ui.showError(message); }
    hideError() { const e = document.getElementById('error-message'); if (e) e.style.display='none'; }
    showSuccess(message) { ui.success(message); }
    escapeHtml(t) { const d=document.createElement('div'); d.textContent=t; return d.innerHTML; }
    escapeAttr(text) {
        if (!text) return '';
        return text.replace(/&/g, '&amp;').replace(/"/g, '&quot;').replace(/'/g, '&#39;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
    }
    async refreshClients() { await this.loadClients(); }
}

// Global wrappers
var telegramClientManager;

function refreshTelegramClients() {
    if (window.telegramClientManager) window.telegramClientManager.refreshClients();
}

document.addEventListener('DOMContentLoaded', () => {
    telegramClientManager = new TelegramClientManager();
    window.telegramClientManager = telegramClientManager;
});
