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
            tbody.innerHTML = `<tr><td colspan="8" class="no-data">No Telegram clients configured. Click "Add Client" to get started.</td></tr>`;
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
                        <ix-icon-button icon="highlight" variant="primary" ghost size="24" title="Edit client" onclick="telegramClientManager.viewClient('${c.name}')"></ix-icon-button>
                        <ix-icon-button icon="${c.enabled ? 'pause' : 'play'}" variant="primary" ghost size="24" title="${c.enabled ? 'Disable' : 'Enable'}" onclick="telegramClientManager.toggleClient('${c.name}', ${!c.enabled})"></ix-icon-button>
                        <ix-icon-button icon="trashcan" variant="primary" ghost size="24" class="btn-delete" title="Delete" onclick="telegramClientManager.deleteClient('${c.name}')"></ix-icon-button>
                    </div>
                </td>
            `;
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
            if (result.telegramClient.toggle.success) {
                await this.loadClients();
                this.showSuccess(`Telegram client "${clientName}" ${enabled ? 'enabled' : 'disabled'}`);
            } else {
                this.showError('Failed to toggle: ' + (result.telegramClient.toggle.errors || []).join(', '));
            }
        } catch (e) {
            this.showError('Failed to toggle Telegram client: ' + e.message);
        }
    }

    deleteClient(clientName) {
        this.deleteClientName = clientName;
        document.getElementById('delete-telegram-client-name').textContent = clientName;
        this.showConfirmDeleteModal();
    }

    async confirmDeleteClient() {
        if (!this.deleteClientName) return;
        try {
            const mutation = `mutation DeleteTelegramClient($name: String!) { telegramClient { delete(name: $name) } }`;
            const result = await this.client.query(mutation, { name: this.deleteClientName });
            if (result.telegramClient.delete) {
                this.hideConfirmDeleteModal();
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

    showConfirmDeleteModal() { document.getElementById('confirm-delete-telegram-client-modal').style.display = 'flex'; }
    hideConfirmDeleteModal() { document.getElementById('confirm-delete-telegram-client-modal').style.display = 'none'; }

    showLoading(show) { const el = document.getElementById('loading-indicator'); if (el) el.style.display = show ? 'flex' : 'none'; }
    showError(message) { const e = document.getElementById('error-message'); const t = document.querySelector('#error-message .error-text'); if (e && t) { t.textContent = message; e.style.display='flex'; setTimeout(()=>this.hideError(),5000); } }
    hideError() { const e = document.getElementById('error-message'); if (e) e.style.display='none'; }
    showSuccess(message) { var existing = document.getElementById('success-toast'); if (existing) existing.remove(); var toast = document.createElement('div'); toast.id = 'success-toast'; toast.style.cssText = 'position:fixed;top:20px;left:50%;transform:translateX(-50%);background:var(--monster-green,#10B981);color:#fff;padding:14px 24px;border-radius:8px;box-shadow:0 4px 12px rgba(0,0,0,0.4);z-index:10000;font-size:0.9rem;max-width:600px;display:flex;align-items:center;gap:10px;animation:slideDown 0.3s ease-out;'; toast.innerHTML = '<span style="font-size:1.2rem;">&#10003;</span><span>' + this.escapeHtml(message) + '</span><button onclick="this.parentElement.remove()" style="background:none;border:none;color:#fff;cursor:pointer;margin-left:auto;font-size:1.1rem;line-height:1;padding:0 4px;">&times;</button>'; if (!document.getElementById('toast-anim-style')) { var s = document.createElement('style'); s.id = 'toast-anim-style'; s.textContent = '@keyframes slideDown{from{transform:translateX(-50%) translateY(-100%);opacity:0;}to{transform:translateX(-50%) translateY(0);opacity:1;}}@keyframes fadeOut{from{opacity:1;}to{opacity:0;}}'; document.head.appendChild(s); } document.body.appendChild(toast); setTimeout(function() { if (toast.parentElement) { toast.style.animation = 'fadeOut 0.3s ease-out forwards'; setTimeout(function() { if (toast.parentElement) toast.remove(); }, 300); } }, 3000); }
    escapeHtml(t) { const d=document.createElement('div'); d.textContent=t; return d.innerHTML; }
    async refreshClients() { await this.loadClients(); }
}

// Global wrappers
function refreshTelegramClients()               { telegramClientManager.refreshClients(); }
function hideConfirmDeleteTelegramClientModal() { telegramClientManager.hideConfirmDeleteModal(); }
function confirmDeleteTelegramClient()          { telegramClientManager.confirmDeleteClient(); }

let telegramClientManager;
document.addEventListener('DOMContentLoaded', () => { telegramClientManager = new TelegramClientManager(); });
document.addEventListener('click', e => {
    if (e.target.classList.contains('modal') && e.target.id === 'confirm-delete-telegram-client-modal') telegramClientManager.hideConfirmDeleteModal();
});
