class TopicNamespaceDetailManager {
    constructor() {
        this.isNewNamespace = false;
        this.namespaceName = null;
        this.originalNamespace = null;

        this.init();
    }

    async init() {
        if (!this.isLoggedIn()) {
            window.location.href = '/pages/login.html';
            return;
        }

        this.parseUrlParams();
        await this.loadSchemaPolicies();

        if (this.isNewNamespace) {
            this.initNewNamespace();
        } else {
            await this.loadNamespace();
        }
    }

    isLoggedIn() {
        return window.isLoggedIn();
    }

    parseUrlParams() {
        const params = new URLSearchParams(window.location.search);
        this.isNewNamespace = params.get('new') === 'true';
        this.namespaceName = params.get('name');

        if (!this.isNewNamespace && !this.namespaceName) {
            window.location.href = '/pages/topic-namespaces.html';
            return;
        }

        document.getElementById('namespace-title').textContent =
            this.isNewNamespace ? 'Create Topic Namespace' : `Edit Namespace: ${this.namespaceName}`;
        document.getElementById('namespace-subtitle').textContent =
            this.isNewNamespace ? 'Bind a topic filter to a schema policy' : 'Edit namespace binding';
        document.getElementById('save-namespace-btn').textContent =
            this.isNewNamespace ? 'Create Namespace' : 'Save Changes';

        if (!this.isNewNamespace) {
            document.getElementById('delete-namespace-btn').style.display = 'inline-flex';
        }
    }

    async loadSchemaPolicies() {
        try {
            const result = await window.graphqlClient.query(`
                query GetSchemaPolicies {
                    topicSchemaPolicies {
                        name
                    }
                }
            `);

            const policies = result.topicSchemaPolicies || [];
            const select = document.getElementById('namespace-schema-policy');
            select.innerHTML = '<option value="">-- Select a schema policy --</option>';
            policies.forEach(policy => {
                const option = document.createElement('option');
                option.value = policy.name;
                option.textContent = policy.name;
                select.appendChild(option);
            });
        } catch (error) {
            console.error('Error loading schema policies:', error);
        }
    }

    initNewNamespace() {
        document.getElementById('namespace-content').style.display = 'block';

        // Set defaults
        document.getElementById('namespace-enforcement-mode').value = 'REJECT';
        document.getElementById('namespace-enabled').checked = true;
    }

    async loadNamespace() {
        try {
            document.getElementById('loading-indicator').style.display = 'flex';
            document.getElementById('namespace-content').style.display = 'none';

            const result = await window.graphqlClient.query(`
                query GetTopicNamespace($name: String!) {
                    topicNamespace(name: $name) {
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
            `, { name: this.namespaceName });

            if (!result.topicNamespace) {
                throw new Error('Namespace not found');
            }

            this.originalNamespace = result.topicNamespace;
            this.populateForm();

            document.getElementById('loading-indicator').style.display = 'none';
            document.getElementById('namespace-content').style.display = 'block';
        } catch (error) {
            console.error('Error loading namespace:', error);
            this.showError('Failed to load namespace: ' + error.message);
            setTimeout(() => {
                window.location.href = '/pages/topic-namespaces.html';
            }, 2000);
        }
    }

    populateForm() {
        const ns = this.originalNamespace;

        document.getElementById('namespace-name').value = ns.name;
        document.getElementById('namespace-name').disabled = true;
        document.getElementById('namespace-description').value = ns.description || '';
        document.getElementById('namespace-tags').value = (ns.tags || []).join(', ');

        document.getElementById('namespace-topic-filter').value = ns.topicFilter || '';
        document.getElementById('namespace-schema-policy').value = ns.schemaPolicyName || '';
        document.getElementById('namespace-enforcement-mode').value = ns.enforcementMode || 'REJECT';
        document.getElementById('namespace-enabled').checked = ns.enabled;
    }

    validateForm() {
        const name = document.getElementById('namespace-name').value.trim();
        const topicFilter = document.getElementById('namespace-topic-filter').value.trim();
        const schemaPolicyName = document.getElementById('namespace-schema-policy').value;

        if (!name) {
            this.showError('Namespace name is required');
            return false;
        }

        if (!topicFilter) {
            this.showError('Topic filter is required');
            return false;
        }

        if (!schemaPolicyName) {
            this.showError('Schema policy is required');
            return false;
        }

        return true;
    }

    async saveNamespace() {
        if (!this.validateForm()) {
            return;
        }

        try {
            const name = document.getElementById('namespace-name').value.trim();
            const topicFilter = document.getElementById('namespace-topic-filter').value.trim();
            const schemaPolicyName = document.getElementById('namespace-schema-policy').value;
            const enforcementMode = document.getElementById('namespace-enforcement-mode').value;
            const enabled = document.getElementById('namespace-enabled').checked;
            const description = document.getElementById('namespace-description').value.trim() || null;
            const tagsText = document.getElementById('namespace-tags').value.trim();
            const tags = tagsText ? tagsText.split(',').map(t => t.trim()).filter(t => t.length > 0) : [];

            const input = { name, topicFilter, schemaPolicyName, enabled, enforcementMode, description, tags };

            console.log('Saving topic namespace with input:', input);

            let result;
            if (this.isNewNamespace) {
                result = await window.graphqlClient.query(`
                    mutation CreateTopicNamespace($input: TopicNamespaceInput!) {
                        topicNamespace {
                            create(input: $input) {
                                success
                                namespace {
                                    name
                                }
                                errors
                            }
                        }
                    }
                `, { input });

                if (result.topicNamespace.create.success) {
                    console.log('Namespace created successfully');
                    window.location.href = '/pages/topic-namespaces.html';
                } else {
                    const errors = result.topicNamespace.create.errors || [];
                    const errorMessage = errors.length > 0 ? errors.join(', ') : 'Failed to create namespace';
                    this.showError(errorMessage);
                }
            } else {
                result = await window.graphqlClient.query(`
                    mutation UpdateTopicNamespace($name: String!, $input: TopicNamespaceInput!) {
                        topicNamespace {
                            update(name: $name, input: $input) {
                                success
                                namespace {
                                    name
                                }
                                errors
                            }
                        }
                    }
                `, { name: this.namespaceName, input });

                if (result.topicNamespace.update.success) {
                    console.log('Namespace updated successfully');
                    window.location.href = '/pages/topic-namespaces.html';
                } else {
                    const errors = result.topicNamespace.update.errors || [];
                    const errorMessage = errors.length > 0 ? errors.join(', ') : 'Failed to update namespace';
                    this.showError(errorMessage);
                }
            }
        } catch (error) {
            console.error('Error saving namespace:', error);
            this.showError('Failed to save namespace: ' + error.message);
        }
    }

    showDeleteModal() {
        document.getElementById('delete-namespace-name').textContent = this.namespaceName;
        document.getElementById('delete-namespace-modal').style.display = 'flex';
    }

    hideDeleteModal() {
        document.getElementById('delete-namespace-modal').style.display = 'none';
    }

    async confirmDeleteNamespace() {
        this.hideDeleteModal();

        try {
            console.log('Deleting namespace:', this.namespaceName);

            const result = await window.graphqlClient.query(`
                mutation DeleteNamespace($name: String!) {
                    topicNamespace {
                        delete(name: $name)
                    }
                }
            `, { name: this.namespaceName });

            if (result.topicNamespace.delete) {
                console.log('Namespace deleted successfully');
                window.location.href = '/pages/topic-namespaces.html';
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

// Global functions
function goBack() {
    window.location.href = '/pages/topic-namespaces.html';
}

function saveNamespace() {
    window.namespaceDetailManager.saveNamespace();
}

function showDeleteModal() {
    window.namespaceDetailManager.showDeleteModal();
}

function hideDeleteModal() {
    window.namespaceDetailManager.hideDeleteModal();
}

function confirmDeleteNamespace() {
    window.namespaceDetailManager.confirmDeleteNamespace();
}

// Initialize when page loads
document.addEventListener('DOMContentLoaded', () => {
    window.namespaceDetailManager = new TopicNamespaceDetailManager();
});

// Handle modal clicks (close when clicking outside)
window.onclick = (event) => {
    const deleteModal = document.getElementById('delete-namespace-modal');
    if (event.target === deleteModal) {
        hideDeleteModal();
    }
};
