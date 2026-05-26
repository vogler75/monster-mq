// MCP Server Detail Management JavaScript

class McpServerDetailManager {
    constructor() {
        this.client = new GraphQLDashboardClient();
        this.serverName = null;
        this.serverData = null;
        this.init();
    }

    async init() {
        const urlParams = new URLSearchParams(window.location.search);
        this.serverName = urlParams.get('server');
        this.isNew = urlParams.get('new') === 'true';

        if (this.isNew) {
            this.showNewForm();
            return;
        }

        if (!this.serverName) {
            this.showError('No server specified in URL. Please select a server from the list.');
            document.getElementById('page-title').textContent = 'Error';
            document.getElementById('page-subtitle').textContent = 'Invalid Request';
            return;
        }

        this.showLoading(true);
        try {
            await this.loadServerData();
        } catch (error) {
            this.showError('Failed to load server data: ' + error.message);
            document.getElementById('page-title').textContent = 'Error Loading Server';
            document.getElementById('page-subtitle').textContent = this.serverName;
        } finally {
            this.showLoading(false);
        }
    }

    showNewForm() {
        document.getElementById('page-title').textContent = 'Add MCP Server';
        document.getElementById('page-subtitle').textContent = 'Create a new MCP server configuration';

        // Set defaults
        document.getElementById('server-name').value = '';
        document.getElementById('server-name').disabled = false;
        document.getElementById('server-namespace').value = 'mcp';
        document.getElementById('server-url').value = '';
        document.getElementById('server-transport').value = 'http';
        document.getElementById('server-enabled').checked = true;

        // Update save button label
        const saveBtn = document.getElementById('save-server-btn');
        if (saveBtn) saveBtn.innerHTML = saveBtn.innerHTML.replace('Save Server', 'Create Server');

        // Hide delete button for new servers
        const deleteBtn = document.getElementById('delete-btn');
        if (deleteBtn) deleteBtn.style.display = 'none';

        // Hide timestamps section for new servers
        document.getElementById('timestamps-section').style.display = 'none';

        // Show configuration section
        document.getElementById('server-content').style.display = 'block';
    }

    async loadServerData() {
        this.showLoading(true);
        this.hideError();

        try {
            const query = `
                query GetMcpServer($name: String!) {
                    mcpServer(name: $name) {
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

            const result = await this.client.query(query, { name: this.serverName });

            if (!result.mcpServer) {
                throw new Error('MCP Server not found');
            }

            this.serverData = result.mcpServer;
            this.renderServerInfo();

        } catch (error) {
            console.error('Error loading MCP server:', error);
            this.showError('Failed to load MCP server: ' + error.message);
        } finally {
            this.showLoading(false);
        }
    }

    renderServerInfo() {
        if (!this.serverData) return;

        const d = this.serverData;

        // Update page title
        document.getElementById('page-title').textContent = `MCP Server: ${d.name}`;
        document.getElementById('page-subtitle').textContent = d.namespace || '';

        // Populate form fields
        document.getElementById('server-name').value = d.name;
        document.getElementById('server-name').disabled = true;
        document.getElementById('server-namespace').value = d.namespace || '';
        document.getElementById('server-url').value = d.url || '';
        document.getElementById('server-transport').value = d.transport || 'http';
        document.getElementById('server-enabled').checked = d.enabled;

        // Timestamps
        this.setText('server-created-at', d.createdAt ? new Date(d.createdAt).toLocaleString() : '-');
        this.setText('server-updated-at', d.updatedAt ? new Date(d.updatedAt).toLocaleString() : '-');

        // Status badge
        const statusBadge = document.getElementById('server-status');
        if (d.enabled) {
            statusBadge.className = 'status-badge status-enabled';
            statusBadge.textContent = 'ENABLED';
        } else {
            statusBadge.className = 'status-badge status-disabled';
            statusBadge.textContent = 'DISABLED';
        }

        // Show all sections
        document.getElementById('server-content').style.display = 'block';
        document.getElementById('timestamps-section').style.display = 'block';
    }

    collectFormData() {
        return {
            name: document.getElementById('server-name').value.trim(),
            namespace: document.getElementById('server-namespace').value.trim() || 'mcp',
            url: document.getElementById('server-url').value.trim(),
            transport: document.getElementById('server-transport').value,
            enabled: document.getElementById('server-enabled').checked
        };
    }

    async saveServer() {
        const form = document.getElementById('server-form');
        if (!form.checkValidity()) {
            form.reportValidity();
            return;
        }

        const data = this.collectFormData();

        if (!data.name) {
            this.showError('Server name is required');
            return;
        }
        if (!data.url) {
            this.showError('Server URL is required');
            return;
        }

        if (this.isNew) {
            try {
                const mutation = `
                    mutation CreateMcpServer($input: McpServerInput!) {
                        mcpServer {
                            create(input: $input) {
                                name
                            }
                        }
                    }
                `;
                const result = await this.client.query(mutation, { input: data });
                if (result.mcpServer.create) {
                    this.showSuccess(`MCP Server "${data.name}" created successfully`);
                    setTimeout(() => { window.spaLocation.href = '/pages/mcp-servers.html'; }, 800);
                } else {
                    this.showError('Failed to create MCP server');
                }
            } catch (error) {
                this.showError('Failed to create MCP server: ' + error.message);
            }
            return;
        }

        try {
            const { name, ...inputData } = data;
            const mutation = `
                mutation UpdateMcpServer($name: String!, $input: McpServerInput!) {
                    mcpServer {
                        update(name: $name, input: $input) {
                            name
                        }
                    }
                }
            `;
            const result = await this.client.query(mutation, { name: this.serverName, input: inputData });
            if (result.mcpServer.update) {
                await this.loadServerData();
                this.showSuccess('MCP Server updated successfully');
            } else {
                this.showError('Failed to update MCP server');
            }
        } catch (error) {
            this.showError('Failed to update MCP server: ' + error.message);
        }
    }

    async deleteServer() {
        try {
            const mutation = `mutation DeleteMcpServer($name: String!) { mcpServer { delete(name: $name) } }`;
            const result = await this.client.query(mutation, { name: this.serverName });
            if (result.mcpServer.delete) {
                this.showSuccess('MCP Server deleted');
                setTimeout(() => { window.spaLocation.href = '/pages/mcp-servers.html'; }, 800);
            } else {
                this.showError('Failed to delete MCP server');
            }
        } catch (e) {
            console.error('Delete error', e);
            this.showError('Failed to delete MCP server: ' + e.message);
        }
    }

    showDeleteModal() {
        const span = document.getElementById('delete-server-name');
        if (span && this.serverData) span.textContent = this.serverData.name;
        document.getElementById('delete-server-modal').style.display = 'flex';
    }

    hideDeleteModal() {
        document.getElementById('delete-server-modal').style.display = 'none';
    }

    confirmDeleteServer() {
        this.hideDeleteModal();
        this.deleteServer();
    }

    goBack() {
        window.spaLocation.href = '/pages/mcp-servers.html';
    }

    // UI Helper Methods
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
        var existing = document.getElementById('success-toast');
        if (existing) existing.remove();
        var toast = document.createElement('div');
        toast.id = 'success-toast';
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

    setText(id, value) {
        const el = document.getElementById(id);
        if (el) el.textContent = value;
    }
}

// Global functions
let mcpServerDetailManager;

function saveServer() {
    mcpServerDetailManager.saveServer();
}

function goBack() {
    mcpServerDetailManager.goBack();
}

function showDeleteModal() {
    mcpServerDetailManager.showDeleteModal();
}

function hideDeleteModal() {
    mcpServerDetailManager.hideDeleteModal();
}

function confirmDeleteServer() {
    mcpServerDetailManager.confirmDeleteServer();
}

// Initialize
document.addEventListener('DOMContentLoaded', () => {
    mcpServerDetailManager = new McpServerDetailManager();
});

// Handle modal clicks (close on backdrop)
document.addEventListener('click', (e) => {
    if (e.target.classList.contains('modal')) {
        if (e.target.id === 'delete-server-modal') {
            mcpServerDetailManager.hideDeleteModal();
        }
    }
});
