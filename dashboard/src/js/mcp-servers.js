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

    async deleteServer(serverName) {
        this.deleteServerName = serverName;
        if (await ui.confirmDelete(serverName, { title: 'Delete MCP server' })) {
            await this.confirmDeleteServer();
        } else {
            this.deleteServerName = null;
        }
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

    showLoading(show) {
        const indicator = document.getElementById('loading-indicator');
        if (indicator) {
            indicator.style.display = show ? 'flex' : 'none';
        }
    }

    showError(message) { ui.showError(message); }

    hideError() {
        const errorEl = document.getElementById('error-message');
        if (errorEl) {
            errorEl.style.display = 'none';
        }
    }

    showSuccess(message) { ui.success(message); }

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
    }
});
