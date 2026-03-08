class TopicNamespacesManager {
    constructor() {
        this.namespaces = [];
        this.deleteNamespaceName = null;

        this.init();
    }

    init() {
        if (!this.isLoggedIn()) {
            window.location.href = '/pages/login.html';
            return;
        }

        this.setupEventListeners();
        this.loadData();
    }

    isLoggedIn() {
        return window.isLoggedIn();
    }

    setupEventListeners() {
        // Modal close on outside click
        window.onclick = (event) => {
            const deleteModal = document.getElementById('confirm-delete-modal');
            if (event.target === deleteModal) {
                this.hideConfirmDeleteModal();
            }
        };
    }

    async loadData() {
        try {
            console.log('Loading topic namespaces...');
            const result = await window.graphqlClient.query(`
                query GetTopicNamespaces {
                    topicNamespaces {
                        name
                        topicFilter
                        schemaPolicyName
                        enabled
                        enforcementMode
                        description
                        tags
                        createdAt
                        updatedAt
                    }
                }
            `);

            this.namespaces = result.topicNamespaces || [];
            this.renderNamespaces();
        } catch (error) {
            console.error('Error loading topic namespaces:', error);
            this.showError('Failed to load topic namespaces: ' + error.message);
        }
    }

    renderNamespaces() {
        const tbody = document.getElementById('namespaces-table-body');

        if (this.namespaces.length === 0) {
            tbody.innerHTML = `
                <tr>
                    <td colspan="6" style="text-align: center; padding: 2rem; color: var(--text-secondary);">
                        No topic namespaces found. Create your first namespace to bind a topic filter to a schema policy.
                    </td>
                </tr>
            `;
            return;
        }

        tbody.innerHTML = this.namespaces.map(ns => {
            const enforcementClass = ns.enforcementMode === 'REJECT' ? 'enforcement-reject' : 'enforcement-reject';

            return `
            <tr>
                <td>
                    <strong>${this.escapeHtml(ns.name)}</strong>
                    ${ns.description ? `<br><small style="color: var(--text-secondary);">${this.escapeHtml(ns.description)}</small>` : ''}
                </td>
                <td><span class="topic-filter-tag">${this.escapeHtml(ns.topicFilter || '-')}</span></td>
                <td>${this.escapeHtml(ns.schemaPolicyName || '-')}</td>
                <td><span class="enforcement-badge ${enforcementClass}">${this.escapeHtml(ns.enforcementMode || 'REJECT')}</span></td>
                <td>
                    <span class="status-badge ${ns.enabled ? 'status-enabled' : 'status-disabled'}" style="cursor: pointer;" onclick="namespacesManager.toggleNamespace('${this.escapeHtml(ns.name)}', ${!ns.enabled})">
                        <span class="status-indicator"></span>${ns.enabled ? 'Enabled' : 'Disabled'}
                    </span>
                </td>
                <td>
                    <div class="action-buttons">
                        <a href="/pages/topic-namespace-detail.html?name=${encodeURIComponent(ns.name)}" class="btn btn-icon btn-edit" title="Edit namespace">
                            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                                <path d="M11 4H4a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2v-7"></path>
                                <path d="M18.5 2.5a2.121 2.121 0 0 1 3 3L12 15l-4 1 1-4 9.5-9.5z"></path>
                            </svg>
                        </a>
                        <button class="btn btn-icon btn-delete" onclick="namespacesManager.showConfirmDeleteModal('${this.escapeHtml(ns.name)}')" title="Delete namespace">
                            <svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor">
                                <path d="M3 6h18M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6m3 0V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2"></path>
                            </svg>
                        </button>
                    </div>
                </td>
            </tr>`;
        }).join('');
    }

    async toggleNamespace(name, enabled) {
        try {
            console.log(`${enabled ? 'Enabling' : 'Disabling'} namespace:`, name);

            const result = await window.graphqlClient.query(`
                mutation ToggleNamespace($name: String!, $enabled: Boolean!) {
                    topicNamespace {
                        toggle(name: $name, enabled: $enabled) {
                            success
                            namespace {
                                name
                            }
                            errors
                        }
                    }
                }
            `, { name, enabled });

            if (result.topicNamespace.toggle.success) {
                console.log(`Namespace ${enabled ? 'enabled' : 'disabled'} successfully`);
                await this.loadData();
            } else {
                const errors = result.topicNamespace.toggle.errors || [];
                this.showError(errors.length > 0 ? errors.join(', ') : `Failed to ${enabled ? 'enable' : 'disable'} namespace`);
            }
        } catch (error) {
            console.error('Error toggling namespace:', error);
            this.showError('Failed to toggle namespace: ' + error.message);
        }
    }

    showConfirmDeleteModal(name) {
        this.deleteNamespaceName = name;
        document.getElementById('delete-namespace-name').textContent = name;
        document.getElementById('confirm-delete-modal').style.display = 'flex';
    }

    hideConfirmDeleteModal() {
        this.deleteNamespaceName = null;
        document.getElementById('confirm-delete-modal').style.display = 'none';
    }

    async confirmDeleteNamespace() {
        if (!this.deleteNamespaceName) return;

        const name = this.deleteNamespaceName;
        this.hideConfirmDeleteModal();

        try {
            console.log('Deleting namespace:', name);

            const result = await window.graphqlClient.query(`
                mutation DeleteNamespace($name: String!) {
                    topicNamespace {
                        delete(name: $name)
                    }
                }
            `, { name });

            if (result.topicNamespace.delete) {
                console.log('Namespace deleted successfully');
                await this.loadData();
            } else {
                this.showError('Failed to delete namespace');
            }
        } catch (error) {
            console.error('Error deleting namespace:', error);
            this.showError('Failed to delete namespace: ' + error.message);
        }
    }

    showError(message) {
        const errorDiv = document.getElementById('error-message');
        const errorText = errorDiv.querySelector('.error-text');
        errorText.textContent = message;
        errorDiv.style.display = 'block';

        setTimeout(() => {
            errorDiv.style.display = 'none';
        }, 5000);
    }

    escapeHtml(unsafe) {
        if (typeof unsafe !== 'string') return unsafe;
        return unsafe
            .replace(/&/g, "&amp;")
            .replace(/</g, "&lt;")
            .replace(/>/g, "&gt;")
            .replace(/"/g, "&quot;")
            .replace(/'/g, "&#039;");
    }
}

// Global functions for onclick handlers
window.refreshNamespaces = () => namespacesManager.loadData();
window.hideConfirmDeleteModal = () => namespacesManager.hideConfirmDeleteModal();
window.confirmDeleteNamespace = () => namespacesManager.confirmDeleteNamespace();

// Initialize when page loads
document.addEventListener('DOMContentLoaded', () => {
    window.namespacesManager = new TopicNamespacesManager();
});
