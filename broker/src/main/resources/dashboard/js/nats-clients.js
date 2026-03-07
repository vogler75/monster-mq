// NATS Client Management JavaScript

class NatsClientManager {
    constructor() {
        this.client = new GraphQLDashboardClient('/graphql');
        this.clients = [];
        this.clusterNodes = [];
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
                query GetNatsClients {
                    natsClients {
                        name namespace nodeId enabled isOnCurrentNode createdAt updatedAt
                        config { servers authType useJetStream addresses { mode natsSubject mqttTopic qos autoConvert removePath } }
                        metrics { messagesIn messagesOut }
                    }
                }
            `;
            const result = await this.client.query(query);
            if (!result || !result.natsClients) throw new Error('Invalid response structure');
            this.clients = result.natsClients;
            this.updateMetrics();
            this.renderClientsTable();
        } catch (e) {
            console.error('Error loading NATS clients:', e);
            this.showError('Failed to load NATS clients: ' + e.message);
        } finally {
            this.showLoading(false);
        }
    }

    updateMetrics() {
        document.getElementById('total-clients').textContent = this.clients.length;
        document.getElementById('enabled-clients').textContent = this.clients.filter(c => c.enabled).length;
        document.getElementById('current-node-clients').textContent = this.clients.filter(c => c.isOnCurrentNode).length;
        const totalAddresses = this.clients.reduce((sum, c) => sum + ((c.config && c.config.addresses) ? c.config.addresses.length : 0), 0);
        document.getElementById('total-addresses').textContent = totalAddresses;
    }

    renderClientsTable() {
        const tbody = document.getElementById('nats-clients-table-body');
        if (!tbody) return;
        tbody.innerHTML = '';
        if (this.clients.length === 0) {
            tbody.innerHTML = `<tr><td colspan="8" class="no-data">No NATS clients configured. Click "Add Client" to get started.</td></tr>`;
            return;
        }
        this.clients.forEach(c => {
            const row = document.createElement('tr');
            const cfg = c.config || {};
            const servers = (cfg.servers || []).join(', ');
            const maxServers = servers.length > 40 ? servers.substring(0, 40) + '…' : servers;
            const addrCount = (cfg.addresses || []).length;
            const statusClass = c.enabled ? 'status-enabled' : 'status-disabled';
            const statusText = c.enabled ? 'Enabled' : 'Disabled';
            const nodeIndicator = c.isOnCurrentNode ? '📍 ' : '';
            const jetStreamBadge = cfg.useJetStream ? ' <span style="font-size:0.65rem;background:rgba(16,185,129,0.15);color:var(--monster-green);padding:0.1rem 0.4rem;border-radius:8px;border:1px solid rgba(16,185,129,0.3);">JS</span>' : '';
            const metricsIn  = (c.metrics && c.metrics.length > 0) ? Math.round(c.metrics[0].messagesIn)  : 0;
            const metricsOut = (c.metrics && c.metrics.length > 0) ? Math.round(c.metrics[0].messagesOut) : 0;
            row.innerHTML = `
                <td><div class="client-name">${this.escapeHtml(c.name)}</div></td>
                <td><small title="${this.escapeHtml(servers)}">${this.escapeHtml(maxServers)}${jetStreamBadge}</small></td>
                <td>${nodeIndicator}${this.escapeHtml(c.nodeId || '')}</td>
                <td><span class="status-badge ${statusClass}">${statusText}</span></td>
                <td style="text-align:center;">${addrCount}</td>
                <td>${metricsIn}</td>
                <td>${metricsOut}</td>
                <td>
                    <div class="action-buttons">
                        <button class="btn-icon btn-view" onclick="natsClientManager.viewClient('${c.name}')" title="Edit client">
                            <svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor"><path d="M3 17.25V21h3.75L17.81 9.94l-3.75-3.75L3 17.25zM20.71 7.04c.39-.39.39-1.02 0-1.41l-2.34-2.34c-.39-.39-1.02-.39-1.41 0l-1.83 1.83 3.75 3.75 1.83-1.83z"/></svg>
                        </button>
                        <button class="btn-icon ${c.enabled ? 'btn-pause' : 'btn-play'}" onclick="natsClientManager.toggleClient('${c.name}', ${!c.enabled})" title="${c.enabled ? 'Disable' : 'Enable'}">
                            ${c.enabled
                                ? '<svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor"><path d="M6 19h4V5H6v14zm8-14v14h4V5h-4z"/></svg>'
                                : '<svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor"><path d="M8 5v14l11-7z"/></svg>'}
                        </button>
                        <button class="btn-icon btn-delete" onclick="natsClientManager.deleteClient('${c.name}')" title="Delete">
                            <svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor"><path d="M6 19c0 1.1.9 2 2 2h8c1.1 0 2-.9 2-2V7H6v12zM19 4h-3.5l-1-1h-5l-1 1H5v2h14V4z"/></svg>
                        </button>
                    </div>
                </td>
            `;
            tbody.appendChild(row);
        });
    }

    async toggleClient(clientName, enabled) {
        try {
            const mutation = `
                mutation ToggleNatsClient($name: String!, $enabled: Boolean!) {
                    natsClient { toggle(name: $name, enabled: $enabled) { success errors } }
                }
            `;
            const result = await this.client.query(mutation, { name: clientName, enabled });
            if (result.natsClient.toggle.success) {
                await this.loadClients();
                this.showSuccess(`NATS client "${clientName}" ${enabled ? 'enabled' : 'disabled'}`);
            } else {
                this.showError('Failed to toggle: ' + (result.natsClient.toggle.errors || []).join(', '));
            }
        } catch (e) {
            this.showError('Failed to toggle NATS client: ' + e.message);
        }
    }

    deleteClient(clientName) {
        this.deleteClientName = clientName;
        document.getElementById('delete-nats-client-name').textContent = clientName;
        this.showConfirmDeleteModal();
    }

    async confirmDeleteClient() {
        if (!this.deleteClientName) return;
        try {
            const mutation = `mutation DeleteNatsClient($name: String!) { natsClient { delete(name: $name) } }`;
            const result = await this.client.query(mutation, { name: this.deleteClientName });
            if (result.natsClient.delete) {
                this.hideConfirmDeleteModal();
                await this.loadClients();
                this.showSuccess(`NATS client "${this.deleteClientName}" deleted`);
            } else {
                this.showError('Failed to delete NATS client');
            }
        } catch (e) {
            this.showError('Failed to delete NATS client: ' + e.message);
        }
        this.deleteClientName = null;
    }

    viewClient(clientName) {
        window.location.href = `/pages/nats-client-detail.html?client=${encodeURIComponent(clientName)}`;
    }

    // Modal helpers
    showConfirmDeleteModal() { document.getElementById('confirm-delete-nats-client-modal').style.display = 'flex'; }
    hideConfirmDeleteModal() { document.getElementById('confirm-delete-nats-client-modal').style.display = 'none'; }

    showLoading(show) { const el = document.getElementById('loading-indicator'); if (el) el.style.display = show ? 'flex' : 'none'; }
    showError(message) { const e = document.getElementById('error-message'); const t = document.querySelector('#error-message .error-text'); if (e && t) { t.textContent = message; e.style.display='flex'; setTimeout(()=>this.hideError(),5000); } }
    hideError() { const e = document.getElementById('error-message'); if (e) e.style.display='none'; }
    showSuccess(msg) { const n=document.createElement('div'); n.className='success-notification'; n.innerHTML=`<span class="success-icon">✅</span><span class="success-text">${this.escapeHtml(msg)}</span>`; document.body.appendChild(n); setTimeout(()=>{ if(n.parentNode) n.parentNode.removeChild(n); },3000); }
    escapeHtml(t) { const d=document.createElement('div'); d.textContent=t; return d.innerHTML; }
    async refreshClients() { await this.loadClients(); }
}

// Global wrappers
function refreshNatsClients()               { natsClientManager.refreshClients(); }
function hideConfirmDeleteNatsClientModal() { natsClientManager.hideConfirmDeleteModal(); }
function confirmDeleteNatsClient()          { natsClientManager.confirmDeleteClient(); }

let natsClientManager;
document.addEventListener('DOMContentLoaded', () => { natsClientManager = new NatsClientManager(); });
document.addEventListener('click', e => {
    if (e.target.classList.contains('modal') && e.target.id === 'confirm-delete-nats-client-modal') natsClientManager.hideConfirmDeleteModal();
});
