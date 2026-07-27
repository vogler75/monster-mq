// NATS Client Management JavaScript

class NatsClientManager {
    constructor() {
        this.client = new GraphQLDashboardClient();
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
            tbody.innerHTML = ui.emptyRow(8, 'No NATS clients configured',
                'Use “Add Client” to get started.');
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
                        <ix-icon-button icon="highlight" variant="subtle-tertiary" size="24" title="Edit client" onclick="natsClientManager.viewClient('${c.name}')"></ix-icon-button>
                        <ix-icon-button icon="${c.enabled ? 'pause' : 'play'}" variant="subtle-tertiary" size="24" title="${c.enabled ? 'Disable' : 'Enable'}" onclick="natsClientManager.toggleClient('${c.name}', ${!c.enabled})"></ix-icon-button>
                        <ix-icon-button icon="trashcan" variant="subtle-tertiary" size="24" class="btn-delete" title="Delete" onclick="natsClientManager.deleteClient('${c.name}')"></ix-icon-button>
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

    async deleteClient(clientName) {
        this.deleteClientName = clientName;
        if (await ui.confirmDelete(clientName, { title: 'Delete NATS client' })) {
            await this.confirmDeleteClient();
        } else {
            this.deleteClientName = null;
        }
    }

    async confirmDeleteClient() {
        if (!this.deleteClientName) return;
        try {
            const mutation = `mutation DeleteNatsClient($name: String!) { natsClient { delete(name: $name) } }`;
            const result = await this.client.query(mutation, { name: this.deleteClientName });
            if (result.natsClient.delete) {
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
        window.spaLocation.href = `/pages/nats-client-detail.html?client=${encodeURIComponent(clientName)}`;
    }

    // Modal helpers

    showLoading(show) { const el = document.getElementById('loading-indicator'); if (el) el.style.display = show ? 'flex' : 'none'; }
    showError(message) { ui.showError(message); }
    hideError() { const e = document.getElementById('error-message'); if (e) e.style.display='none'; }
    showSuccess(message) { ui.success(message); }
    escapeHtml(t) { const d=document.createElement('div'); d.textContent=t; return d.innerHTML; }
    async refreshClients() { await this.loadClients(); }
}

// Global wrappers
function refreshNatsClients()               { natsClientManager.refreshClients(); }

let natsClientManager;
document.addEventListener('DOMContentLoaded', () => { natsClientManager = new NatsClientManager(); });
