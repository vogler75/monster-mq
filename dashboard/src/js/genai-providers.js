// GenAI Provider List Page

class GenAiProviderManager {
    constructor() {
        this.client = new GraphQLDashboardClient();
        this.providers = [];
        this.pendingDeleteName = null;
        this.init();
    }

    async init() {
        if (!window.isLoggedIn || !window.isLoggedIn()) return;
        await this.loadProviders();
    }

    async loadProviders() {
        try {
            const result = await this.client.query(`
                query {
                    genAiProviders {
                        name type source model endpoint baseUrl enabled createdAt updatedAt
                    }
                }
            `);
            this.providers = result.genAiProviders || [];
            this.renderTable();
        } catch (e) {
            console.error('Error loading providers:', e);
            document.getElementById('providers-tbody').innerHTML =
                '<tr><td colspan="7" style="text-align:center; color: var(--monster-red);">Error loading providers: ' + e.message + '</td></tr>';
        }
    }

    renderTable() {
        const tbody = document.getElementById('providers-tbody');
        if (this.providers.length === 0) {
            tbody.innerHTML = '<tr><td colspan="7" style="text-align:center; color: var(--text-muted); padding: 2rem;">No providers configured. Click <strong>Add Provider</strong> to create one, or add providers in <code>config.yaml</code>.</td></tr>';
            return;
        }

        tbody.innerHTML = '';
        this.providers.forEach(p => {
            const isConfig = p.source === 'config';
            const sourceBadge = isConfig
                ? '<span class="source-badge-config">config.yaml</span>'
                : '<span class="source-badge-db">Database</span>';
            const endpointText = p.endpoint || p.baseUrl || '<span class="text-muted">—</span>';
            const modelText = p.model || '<span class="text-muted">default</span>';
            const statusBadge = p.enabled
                ? '<span class="status-badge status-enabled">Enabled</span>'
                : '<span class="status-badge status-disabled">Disabled</span>';

            const actions = isConfig
                ? '<span class="text-muted">Read-only</span>'
                : `<div class="action-buttons">
                       <ix-icon-button icon="pen" ghost variant="primary" title="Edit" data-name="${p.name}"></ix-icon-button>
                       <ix-icon-button icon="trashcan" ghost variant="secondary" title="Delete" data-name="${p.name}" data-action="delete"></ix-icon-button>
                   </div>`;

            const tr = document.createElement('tr');
            if (!isConfig) tr.classList.add('clickable-row');
            tr.innerHTML = `
                <td><strong>${p.name}</strong></td>
                <td><span class="type-badge">${p.type}</span></td>
                <td>${sourceBadge}</td>
                <td>${modelText}</td>
                <td style="max-width:200px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap;">${endpointText}</td>
                <td>${statusBadge}</td>
                <td style="text-align:center;">${actions}</td>
            `;

            if (!isConfig) {
                tr.addEventListener('click', (e) => {
                    if (e.target.closest('ix-icon-button')) return;
                    window.navigateTo('/pages/genai-provider-detail.html?name=' + encodeURIComponent(p.name));
                });
                const editBtn = tr.querySelector('ix-icon-button[icon="pen"]');
                if (editBtn) editBtn.addEventListener('click', (e) => {
                    e.stopPropagation();
                    window.navigateTo('/pages/genai-provider-detail.html?name=' + encodeURIComponent(p.name));
                });
                const deleteBtn = tr.querySelector('ix-icon-button[data-action="delete"]');
                if (deleteBtn) deleteBtn.addEventListener('click', (e) => {
                    e.stopPropagation();
                    this.deleteProvider(p.name);
                });
            }
            tbody.appendChild(tr);
        });
    }

    addProvider() {
        window.navigateTo('/pages/genai-provider-detail.html?new=true');
    }

    deleteProvider(name) {
        this.pendingDeleteName = name;
        document.getElementById('delete-provider-name').textContent = name;
        document.getElementById('delete-modal').style.display = 'flex';
    }

    cancelDelete() {
        this.pendingDeleteName = null;
        document.getElementById('delete-modal').style.display = 'none';
    }

    async confirmDelete() {
        const name = this.pendingDeleteName;
        document.getElementById('delete-modal').style.display = 'none';
        this.pendingDeleteName = null;
        try {
            const result = await this.client.query(`
                mutation DeleteProvider($name: String!) {
                    genAiProvider { delete(name: $name) }
                }
            `, { name });
            if (result.genAiProvider?.delete) {
                await this.loadProviders();
            }
        } catch (e) {
            console.error('Delete error:', e);
            alert('Failed to delete provider: ' + e.message);
        }
    }
}

const genAiProviderManager = new GenAiProviderManager();
