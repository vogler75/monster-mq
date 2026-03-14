class TopicSchemaPoliciesManager {
    constructor() {
        this.policies = [];
        this.deletePolicyName = null;

        this.init();
    }

    init() {
        if (!this.isLoggedIn()) {
            window.location.href = '/pages/login.html';
            return;
        }

        this.setupEventListeners();
        this.loadPolicies();
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

    async loadPolicies() {
        try {
            console.log('Loading topic schema policies...');
            const result = await window.graphqlClient.query(`
                query GetTopicSchemaPolicies {
                    topicSchemaPolicies {
                        name
                        payloadType
                        version
                        description
                        jsonSchema
                        contentType
                        examples
                        createdAt
                        updatedAt
                    }
                }
            `);

            this.policies = result.topicSchemaPolicies || [];
            this.renderPolicies();
            this.updateMetrics();
        } catch (error) {
            console.error('Error loading topic schema policies:', error);
            this.showError('Failed to load topic schema policies: ' + error.message);
        }
    }

    updateMetrics() {
        document.getElementById('total-policies').textContent = this.policies.length;
    }

    renderPolicies() {
        const tbody = document.getElementById('policies-table-body');

        if (this.policies.length === 0) {
            tbody.innerHTML = `
                <tr>
                    <td colspan="5" style="text-align: center; padding: 2rem; color: var(--text-secondary);">
                        No topic schema policies found. Create your first policy to get started.
                    </td>
                </tr>
            `;
            return;
        }

        tbody.innerHTML = this.policies.map(policy => {
            return `
            <tr>
                <td><strong>${this.escapeHtml(policy.name)}</strong></td>
                <td>${this.escapeHtml(policy.payloadType || 'JSON')}</td>
                <td>${this.escapeHtml(policy.version || '-')}</td>
                <td style="color: var(--text-secondary); max-width: 300px;">${this.escapeHtml(policy.description || '-')}</td>
                <td>
                    <div class="action-buttons">
                        <a href="/pages/topic-schema-policy-detail.html?name=${encodeURIComponent(policy.name)}" class="btn btn-icon btn-edit" title="Edit policy">
                            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                                <path d="M11 4H4a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2v-7"></path>
                                <path d="M18.5 2.5a2.121 2.121 0 0 1 3 3L12 15l-4 1 1-4 9.5-9.5z"></path>
                            </svg>
                        </a>
                        <button class="btn btn-icon btn-delete" onclick="policiesManager.showConfirmDeleteModal('${this.escapeHtml(policy.name)}')" title="Delete policy">
                            <svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor">
                                <path d="M3 6h18M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6m3 0V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2"></path>
                            </svg>
                        </button>
                    </div>
                </td>
            </tr>`;
        }).join('');
    }

    showConfirmDeleteModal(name) {
        this.deletePolicyName = name;
        document.getElementById('delete-policy-name').textContent = name;
        document.getElementById('confirm-delete-modal').style.display = 'flex';
    }

    hideConfirmDeleteModal() {
        this.deletePolicyName = null;
        document.getElementById('confirm-delete-modal').style.display = 'none';
    }

    async confirmDeletePolicy() {
        if (!this.deletePolicyName) return;

        const name = this.deletePolicyName;
        this.hideConfirmDeleteModal();

        try {
            console.log('Deleting policy:', name);

            const result = await window.graphqlClient.query(`
                mutation DeletePolicy($name: String!) {
                    topicSchemaPolicy {
                        delete(name: $name)
                    }
                }
            `, { name });

            if (result.topicSchemaPolicy.delete) {
                console.log('Policy deleted successfully');
                await this.loadPolicies();
            } else {
                this.showError('Failed to delete policy');
            }
        } catch (error) {
            console.error('Error deleting policy:', error);
            this.showError('Failed to delete policy: ' + error.message);
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
window.refreshPolicies = () => policiesManager.loadPolicies();
window.hideConfirmDeleteModal = () => policiesManager.hideConfirmDeleteModal();
window.confirmDeletePolicy = () => policiesManager.confirmDeletePolicy();

// Initialize when page loads
document.addEventListener('DOMContentLoaded', () => {
    window.policiesManager = new TopicSchemaPoliciesManager();
});
