// Redis Client Management

class RedisClientManager {
    constructor() {
        this.client = new GraphQLDashboardClient();
        this.clients = [];
        this.init();
    }

    async init() {
        await this.loadClients();
        setInterval(() => this.loadClients(), 30000);
    }

    async loadClients() {
        ui.setLoading(true);
        ui.clearError();
        try {
            const query = `
                query GetRedisClients {
                    redisClients {
                        name namespace nodeId enabled isOnCurrentNode createdAt updatedAt
                        config { host port database useSsl loopPrevention addresses { mode redisChannel mqttTopic qos usePatternSubscribe usePatternMatch kvPollIntervalMs publishOnChangeOnly removePath } }
                        metrics { messagesIn messagesOut }
                    }
                }
            `;
            const result = await this.client.query(query);
            if (!result || !result.redisClients) throw new Error('Invalid response structure');
            this.clients = result.redisClients;
            this.updateMetrics();
            this.renderClientsTable();
        } catch (e) {
            console.error('Error loading Redis clients:', e);
            ui.showError('Failed to load Redis clients: ' + e.message);
        } finally {
            ui.setLoading(false);
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
        const tbody = document.getElementById('redis-clients-table-body');
        if (!tbody) return;

        if (this.clients.length === 0) {
            tbody.innerHTML = ui.emptyRow(8, 'No Redis clients configured',
                'Use “Add Client” to bridge a Redis instance to MQTT.');
            return;
        }

        tbody.innerHTML = '';
        this.clients.forEach(c => {
            const cfg = c.config || {};
            const hostDisplay = (cfg.host || 'localhost') + ':' + (cfg.port || 6379) + '/' + (cfg.database || 0);
            const sslBadge = cfg.useSsl ? ' <span class="status-badge badge-ok">TLS</span>' : '';
            const addrCount = (cfg.addresses || []).length;
            const metrics = (c.metrics && c.metrics.length > 0) ? c.metrics[0] : { messagesIn: 0, messagesOut: 0 };
            const node = ui.escapeHtml(c.nodeId || '');

            const row = document.createElement('tr');
            row.innerHTML = `
                <td><strong>${ui.escapeHtml(c.name)}</strong></td>
                <td>${ui.escapeHtml(hostDisplay)}${sslBadge}</td>
                <td>${c.isOnCurrentNode ? `${node} <span class="status-badge badge-info">this node</span>` : node}</td>
                <td>${ui.statusBadge(c.enabled ? 'Enabled' : 'Disabled', c.enabled ? 'ok' : 'disabled')}</td>
                <td class="num">${addrCount}</td>
                <td class="num">${Math.round(metrics.messagesIn)}</td>
                <td class="num">${Math.round(metrics.messagesOut)}</td>
                <td>
                    <div class="action-buttons">
                        <ix-icon-button icon="pen" variant="subtle-tertiary" size="24" title="Edit client" class="btn-edit"></ix-icon-button>
                        <ix-icon-button icon="${c.enabled ? 'pause' : 'play'}" variant="subtle-tertiary" size="24" title="${c.enabled ? 'Disable' : 'Enable'}" data-requires-auth class="btn-toggle"></ix-icon-button>
                        <ix-icon-button icon="trashcan" variant="subtle-tertiary" size="24" class="btn-delete" title="Delete" data-requires-auth></ix-icon-button>
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
                mutation ToggleRedisClient($name: String!, $enabled: Boolean!) {
                    redisClient { toggle(name: $name, enabled: $enabled) { success errors } }
                }
            `;
            const result = await this.client.query(mutation, { name: clientName, enabled });
            if (result && result.redisClient && result.redisClient.toggle && result.redisClient.toggle.success) {
                await this.loadClients();
                ui.success(`Redis client “${clientName}” ${enabled ? 'enabled' : 'disabled'}`);
            } else {
                const errors = (result && result.redisClient && result.redisClient.toggle && result.redisClient.toggle.errors) || [];
                ui.error('Failed to toggle: ' + errors.join(', '));
            }
        } catch (e) {
            ui.error('Failed to toggle Redis client: ' + e.message);
        }
    }

    async deleteClient(clientName) {
        const confirmed = await ui.confirmDelete(clientName, {
            title: 'Delete Redis client',
            message: `Delete the Redis client “${clientName}”?\n` +
                'Its address mappings will be removed and the bridge will stop. ' +
                'This action cannot be undone.'
        });
        if (!confirmed) return;

        try {
            const mutation = `mutation DeleteRedisClient($name: String!) { redisClient { delete(name: $name) } }`;
            const result = await this.client.query(mutation, { name: clientName });
            if (result && result.redisClient && result.redisClient.delete) {
                await this.loadClients();
                ui.success(`Redis client “${clientName}” deleted`);
            } else {
                ui.error('Failed to delete Redis client');
            }
        } catch (e) {
            ui.error('Failed to delete Redis client: ' + e.message);
        }
    }

    viewClient(clientName) {
        window.spaLocation.href = `/pages/redis-client-detail.html?client=${encodeURIComponent(clientName)}`;
    }

    async refreshClients() { await this.loadClients(); }
}

var redisClientManager;

function refreshRedisClients() {
    if (window.redisClientManager) window.redisClientManager.refreshClients();
}

document.addEventListener('DOMContentLoaded', () => {
    redisClientManager = new RedisClientManager();
    window.redisClientManager = redisClientManager;
});
