/**
 * Redfish Gateways Management
 */
class RedfishGatewayManager {
    constructor() {
        this.gateways = [];
        this.init();
    }

    async init() {
        await this.loadGateways();
    }

    async loadGateways() {
        this.showLoading(true);
        this.hideError();
        try {
            const query = `
                query GetRedfishGateways {
                    redfishMappings {
                        name
                        nodeId
                        enabled
                        createdAt
                        updatedAt
                        isOnCurrentNode
                        config {
                            topicPrefix
                            topicFilters
                            chassisId
                            defaultReadingType
                            defaultReadingUnits
                        }
                    }
                }
            `;
            const result = await window.graphqlClient.query(query);
            this.gateways = result?.redfishMappings || [];
            this.updateMetrics();
            this.renderTable();
        } catch (e) {
            console.error('Error loading Redfish gateways:', e);
            this.showError('Failed to load Redfish gateways: ' + e.message);
        } finally {
            this.showLoading(false);
        }
    }

    updateMetrics() {
        const total = this.gateways.length;
        const enabled = this.gateways.filter(g => g.enabled).length;
        const local = this.gateways.filter(g => g.isOnCurrentNode).length;
        const totalFilters = this.gateways.reduce((sum, g) => sum + (g.config?.topicFilters?.length || 0), 0);

        const setVal = (id, val) => {
            const el = document.getElementById(id);
            if (el) el.textContent = val;
        };

        setVal('total-gateways', total);
        setVal('enabled-gateways', enabled);
        setVal('local-gateways', local);
        setVal('total-filters', totalFilters);
    }

    renderTable() {
        const tbody = document.getElementById('gateways-table-body');
        if (!tbody) return;

        if (this.gateways.length === 0) {
            tbody.innerHTML = `
                <tr>
                    <td colspan="8" style="text-align: center; color: var(--text-muted); padding: 3rem;">
                        <ix-icon name="server-rack" size="32" style="margin-bottom: 0.5rem; opacity: 0.4; display: block; margin: 0 auto 0.5rem auto;"></ix-icon>
                        No Redfish gateways configured yet.
                    </td>
                </tr>
            `;
            return;
        }

        tbody.innerHTML = this.gateways.map(g => {
            const cfg = g.config || {};
            const prefix = cfg.topicPrefix || 'redfish';
            const chassis = cfg.chassisId || 'EdgeNode';
            const readingType = cfg.defaultReadingType || 'Temperature';
            const units = cfg.defaultReadingUnits || 'Cel';
            const filters = Array.isArray(cfg.topicFilters) ? cfg.topicFilters : [];
            const filterBadges = filters.length > 0
                ? filters.map(f => `<span class="badge badge-info" style="font-size:0.75rem; margin-right:4px;">${this.escapeHtml(f)}</span>`).join('')
                : '<span class="text-muted" style="font-size:0.8rem; font-style:italic;">None</span>';

            const statusClass = g.enabled ? 'status-enabled' : 'status-disabled';
            const statusText = g.enabled ? 'Active' : 'Disabled';
            const nodeBadge = g.isOnCurrentNode
                ? `<span class="badge badge-enabled" style="font-size:0.75rem;">Local (${this.escapeHtml(g.nodeId || 'local')})</span>`
                : `<span class="badge badge-info" style="font-size:0.75rem;">${this.escapeHtml(g.nodeId || 'remote')}</span>`;

            return `
                <tr>
                    <td>
                        <a href="/pages/redfish-gateway-detail.html?name=${encodeURIComponent(g.name)}" style="font-weight:600; color:var(--monster-teal);">
                            ${this.escapeHtml(g.name)}
                        </a>
                    </td>
                    <td>${nodeBadge}</td>
                    <td><code style="font-size:0.8rem;">${this.escapeHtml(prefix)}</code></td>
                    <td><span class="badge badge-info" style="font-size:0.75rem;">${this.escapeHtml(chassis)}</span></td>
                    <td>
                        <span style="font-size:0.85rem; font-weight:500;">${this.escapeHtml(readingType)}</span>
                        <span class="text-muted" style="font-size:0.8rem;">(${this.escapeHtml(units)})</span>
                    </td>
                    <td>${filterBadges}</td>
                    <td>
                        <button class="status-badge ${statusClass}" style="cursor:pointer; border:none;" onclick="toggleGateway('${this.escapeHtml(g.name)}', ${!g.enabled})" title="Click to toggle status">
                            ${statusText}
                        </button>
                    </td>
                    <td>
                        <div style="display:flex; gap:0.25rem; align-items:center;">
                            <a href="/redfish/v1/Chassis/${encodeURIComponent(chassis)}/Sensors" target="_blank" class="btn btn-secondary btn-small" title="Open Redfish Sensors REST API">
                                <ix-icon name="launch" size="14"></ix-icon>
                            </a>
                            <a href="/pages/redfish-gateway-detail.html?name=${encodeURIComponent(g.name)}" class="btn btn-secondary btn-small" title="Edit Gateway">
                                <ix-icon name="pen" size="14"></ix-icon>
                            </a>
                            <button class="btn btn-danger btn-small" onclick="deleteGateway('${this.escapeHtml(g.name)}')" title="Delete Gateway">
                                <ix-icon name="trashcan" size="14"></ix-icon>
                            </button>
                        </div>
                    </td>
                </tr>
            `;
        }).join('');
    }

    async toggle(name, newEnabled) {
        try {
            const mutation = `
                mutation ToggleRedfish($name: String!, $enabled: Boolean!) {
                    toggleRedfishMapping(name: $name, enabled: $enabled) {
                        success
                        message
                        redfish {
                            name
                            enabled
                        }
                    }
                }
            `;
            const res = await window.graphqlClient.query(mutation, { name, enabled: newEnabled });
            if (res?.toggleRedfishMapping?.success) {
                window.ui.success(`Gateway "${name}" ${newEnabled ? 'enabled' : 'disabled'}`);
                await this.loadGateways();
            } else {
                throw new Error(res?.toggleRedfishMapping?.message || 'Failed to toggle status');
            }
        } catch (e) {
            console.error('Error toggling gateway:', e);
            window.ui.error('Error updating gateway: ' + e.message);
        }
    }

    async delete(name) {
        const confirmed = await window.ui.showConfirm({
            title: 'Delete Redfish Gateway',
            message: `Are you sure you want to delete gateway "${name}"? This will stop exposing Redfish REST endpoints for this gateway.`,
            confirmText: 'Delete',
            type: 'danger'
        });

        if (!confirmed) return;

        try {
            const mutation = `
                mutation DeleteRedfish($name: String!) {
                    deleteRedfishMapping(name: $name)
                }
            `;
            const res = await window.graphqlClient.query(mutation, { name });
            if (res?.deleteRedfishMapping) {
                window.ui.success(`Gateway "${name}" deleted successfully`);
                await this.loadGateways();
            } else {
                throw new Error('Delete returned false');
            }
        } catch (e) {
            console.error('Error deleting gateway:', e);
            window.ui.error('Failed to delete gateway: ' + e.message);
        }
    }

    showLoading(show) {
        const el = document.getElementById('loading-indicator');
        if (el) el.style.display = show ? 'flex' : 'none';
    }

    showError(msg) {
        const el = document.getElementById('error-message');
        const txt = document.getElementById('error-text');
        if (txt) txt.textContent = msg;
        if (el) el.style.display = 'block';
    }

    hideError() {
        const el = document.getElementById('error-message');
        if (el) el.style.display = 'none';
    }

    escapeHtml(str) {
        if (!str) return '';
        return String(str)
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;')
            .replace(/'/g, '&#039;');
    }
}

// Global hooks for onclick
let gatewayManager = null;
function refreshGateways() {
    if (gatewayManager) gatewayManager.loadGateways();
}
function toggleGateway(name, enabled) {
    if (gatewayManager) gatewayManager.toggle(name, enabled);
}
function deleteGateway(name) {
    if (gatewayManager) gatewayManager.delete(name);
}

document.addEventListener('DOMContentLoaded', () => {
    gatewayManager = new RedfishGatewayManager();
});
