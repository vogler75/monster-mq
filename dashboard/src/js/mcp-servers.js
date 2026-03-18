// MCP Server Management JavaScript

class McpServerManager {
    constructor() {
        this.client = new GraphQLDashboardClient();
        this.servers = [];
        this.deleteServerName = null;
        this.init();
    }

    async init() {
        console.log('Initializing MCP Server Manager...');
        await this.loadMcpServers();
        setInterval(() => this.loadMcpServers(), 30000);
    }

    async loadMcpServers() {
        this.showLoading(true);
        this.hideError();

        try {
            const query = `
                query GetMcpServers {
                    mcpServers {
                        name
                        namespace
                        nodeId
                        enabled
                        url
                        transport
                        createdAt
                        updatedAt
                    }
                }
            `;

            const result = await this.client.query(query);
            console.log('Load MCP servers result:', result);

            if (!result || !result.mcpServers) {
                throw new Error('Invalid response structure');
            }

            this.servers = result.mcpServers || [];
            this.updateMetrics();
            this.renderTable();

        } catch (error) {
            console.error('Error loading MCP servers:', error);
            this.showError('Failed to load MCP Servers: ' + error.message);
        } finally {
            this.showLoading(false);
        }
    }

    updateMetrics() {
        const totalServers = this.servers.length;
        const enabledServers = this.servers.filter(s => s.enabled).length;

        document.getElementById('total-servers').textContent = totalServers;
        document.getElementById('enabled-servers').textContent = enabledServers;
    }

    renderTable() {
        const tbody = document.getElementById('mcp-servers-table-body');
        if (!tbody) return;

        tbody.innerHTML = '';

        if (this.servers.length === 0) {
            tbody.innerHTML = `
                <tr>
                    <td colspan="5" class="no-data">
                        No MCP Servers configured. Click "Add MCP Server" to get started.
                    </td>
                </tr>
            `;
            return;
        }

        this.servers.forEach(server => {
            const row = document.createElement('tr');

            const statusClass = server.enabled ? 'status-enabled' : 'status-disabled';
            const statusText = server.enabled ? 'Enabled' : 'Disabled';

            row.innerHTML = `
                <td>
                    <div class="client-name">${this.escapeHtml(server.name)}</div>
                    <small class="client-namespace">${this.escapeHtml(server.namespace || '')}</small>
                </td>
                <td>${this.escapeHtml(server.url || '-')}</td>
                <td>
                    <span class="transport-badge">${this.escapeHtml(server.transport || 'http')}</span>
                </td>
                <td>
                    <span class="status-badge ${statusClass}">${statusText}</span>
                </td>
                <td>
                    <div class="action-buttons">
                        <ix-icon-button icon="pen" variant="primary" ghost size="24" title="Edit Server" onclick="event.stopPropagation(); window.spaLocation.href='/pages/mcp-server-detail.html?server=${encodeURIComponent(server.name)}'"></ix-icon-button>
                        <ix-icon-button icon="trashcan" variant="primary" ghost size="24" class="btn-delete" title="Delete Server" onclick="event.stopPropagation(); mcpServerManager.deleteServer('${this.escapeHtml(server.name)}')"></ix-icon-button>
                    </div>
                </td>
            `;

            row.addEventListener('click', () => window.spaLocation.href = `/pages/mcp-server-detail.html?server=${encodeURIComponent(server.name)}`);
            tbody.appendChild(row);
        });
    }

    deleteServer(serverName) {
        this.deleteServerName = serverName;
        document.getElementById('delete-server-name').textContent = serverName;
        this.showConfirmDeleteModal();
    }

    async confirmDeleteServer() {
        if (!this.deleteServerName) return;

        try {
            const mutation = `
                mutation DeleteMcpServer($name: String!) {
                    mcpServer {
                        delete(name: $name)
                    }
                }
            `;

            const result = await this.client.query(mutation, { name: this.deleteServerName });

            if (result.mcpServer.delete) {
                this.hideConfirmDeleteModal();
                await this.loadMcpServers();
                this.showSuccess(`MCP Server "${this.deleteServerName}" deleted successfully`);
            } else {
                this.showError('Failed to delete MCP server');
            }

        } catch (error) {
            console.error('Error deleting MCP server:', error);
            this.showError('Failed to delete MCP server: ' + error.message);
        }

        this.deleteServerName = null;
    }

    // UI Helper Methods
    showConfirmDeleteModal() {
        document.getElementById('confirm-delete-modal').style.display = 'flex';
    }

    hideConfirmDeleteModal() {
        document.getElementById('confirm-delete-modal').style.display = 'none';
    }

    showLoading(show) {
        const indicator = document.getElementById('loading-indicator');
        if (indicator) {
            indicator.style.display = show ? 'flex' : 'none';
        }
    }

    showError(message) {
        var errorDiv = document.getElementById('error-message');
        if (errorDiv) {
            var errorText = errorDiv.querySelector('.error-text');
            if (errorText) errorText.textContent = message;
            errorDiv.style.display = 'flex';
        }

        var existing = document.getElementById('error-toast');
        if (existing) existing.remove();

        var toast = document.createElement('div');
        toast.id = 'error-toast';
        toast.style.cssText = 'position:fixed;top:20px;left:50%;transform:translateX(-50%);background:var(--monster-red,#EF4444);color:#fff;padding:14px 24px;border-radius:8px;box-shadow:0 4px 12px rgba(0,0,0,0.4);z-index:10000;font-size:0.9rem;max-width:600px;display:flex;align-items:center;gap:10px;animation:slideDown 0.3s ease-out;';
        toast.innerHTML = '<span style="font-size:1.2rem;">&#9888;</span><span>' + message + '</span><button onclick="this.parentElement.remove()" style="background:none;border:none;color:#fff;cursor:pointer;margin-left:auto;font-size:1.1rem;line-height:1;padding:0 4px;">&times;</button>';

        if (!document.getElementById('error-toast-style')) {
            var style = document.createElement('style');
            style.id = 'error-toast-style';
            style.textContent = '@keyframes slideDown{from{transform:translateX(-50%) translateY(-100%);opacity:0;}to{transform:translateX(-50%) translateY(0);opacity:1;}}';
            document.head.appendChild(style);
        }

        document.body.appendChild(toast);

        setTimeout(function() {
            if (toast.parentElement) toast.remove();
            if (errorDiv) errorDiv.style.display = 'none';
        }, 8000);
    }

    hideError() {
        const errorEl = document.getElementById('error-message');
        if (errorEl) {
            errorEl.style.display = 'none';
        }
    }

    showSuccess(message) {
        var existing = document.getElementById('success-toast'); if (existing) existing.remove();
        var toast = document.createElement('div'); toast.id = 'success-toast';
        toast.style.cssText = 'position:fixed;top:20px;left:50%;transform:translateX(-50%);background:var(--monster-green,#10B981);color:#fff;padding:14px 24px;border-radius:8px;box-shadow:0 4px 12px rgba(0,0,0,0.4);z-index:10000;font-size:0.9rem;max-width:600px;display:flex;align-items:center;gap:10px;animation:slideDown 0.3s ease-out;';
        toast.innerHTML = '<span style="font-size:1.2rem;">&#10003;</span><span>' + this.escapeHtml(message) + '</span><button onclick="this.parentElement.remove()" style="background:none;border:none;color:#fff;cursor:pointer;margin-left:auto;font-size:1.1rem;line-height:1;padding:0 4px;">&times;</button>';
        if (!document.getElementById('toast-anim-style')) { var s = document.createElement('style'); s.id = 'toast-anim-style'; s.textContent = '@keyframes slideDown{from{transform:translateX(-50%) translateY(-100%);opacity:0;}to{transform:translateX(-50%) translateY(0);opacity:1;}}@keyframes fadeOut{from{opacity:1;}to{opacity:0;}}'; document.head.appendChild(s); }
        document.body.appendChild(toast);
        setTimeout(function() { if (toast.parentElement) { toast.style.animation = 'fadeOut 0.3s ease-out forwards'; setTimeout(function() { if (toast.parentElement) toast.remove(); }, 300); } }, 3000);
    }

    escapeHtml(text) {
        const div = document.createElement('div');
        div.textContent = text;
        return div.innerHTML;
    }

    async refreshMcpServers() {
        await this.loadMcpServers();
    }
}

// Global functions for onclick handlers
function hideConfirmDeleteModal() {
    mcpServerManager.hideConfirmDeleteModal();
}

function confirmDeleteServer() {
    mcpServerManager.confirmDeleteServer();
}

function refreshMcpServers() {
    mcpServerManager.refreshMcpServers();
}

// Initialize when DOM is loaded
let mcpServerManager;
document.addEventListener('DOMContentLoaded', () => {
    mcpServerManager = new McpServerManager();
});

// Handle modal clicks (close on backdrop click)
document.addEventListener('click', (e) => {
    if (e.target.classList.contains('modal')) {
        if (e.target.id === 'confirm-delete-modal') {
            mcpServerManager.hideConfirmDeleteModal();
        }
    }
});
