class TopicSchemaPolicyDetailManager {
    constructor() {
        this.isNewPolicy = false;
        this.policyName = null;
        this.originalPolicy = null;

        this.init();
    }

    async init() {
        if (!this.isLoggedIn()) {
            window.location.href = '/pages/login.html';
            return;
        }

        this.parseUrlParams();

        if (this.isNewPolicy) {
            this.initNewPolicy();
        } else {
            await this.loadPolicy();
        }
    }

    isLoggedIn() {
        return window.isLoggedIn();
    }

    parseUrlParams() {
        const params = new URLSearchParams(window.location.search);
        this.isNewPolicy = params.get('new') === 'true';
        this.policyName = params.get('name');

        if (!this.isNewPolicy && !this.policyName) {
            window.location.href = '/pages/topic-schema-policies.html';
            return;
        }

        document.getElementById('policy-title').textContent =
            this.isNewPolicy ? 'Create Topic Schema Policy' : `Edit Policy: ${this.policyName}`;
        document.getElementById('policy-subtitle').textContent =
            this.isNewPolicy ? 'Configure a new schema definition' : 'Edit schema definition';
        document.getElementById('save-policy-btn').textContent =
            this.isNewPolicy ? 'Create Policy' : 'Save Changes';

        if (!this.isNewPolicy) {
            document.getElementById('delete-policy-btn').style.display = 'inline-flex';
        }
    }

    initNewPolicy() {
        document.getElementById('policy-content').style.display = 'block';

        // Set defaults
        document.getElementById('policy-payload-type').value = 'JSON';
        document.getElementById('policy-version').value = '1.0.0';
        document.getElementById('policy-content-type').value = 'application/json';

        // Default JSON Schema
        const defaultSchema = {
            "type": "object",
            "properties": {},
            "required": []
        };
        document.getElementById('policy-json-schema').value = JSON.stringify(defaultSchema, null, 2);
    }

    async loadPolicy() {
        try {
            document.getElementById('loading-indicator').style.display = 'flex';
            document.getElementById('policy-content').style.display = 'none';

            const result = await window.graphqlClient.query(`
                query GetTopicSchemaPolicy($name: String!) {
                    topicSchemaPolicy(name: $name) {
                        name
                        jsonSchema
                        payloadType
                        contentType
                        version
                        description
                        examples
                        createdAt
                        updatedAt
                    }
                }
            `, { name: this.policyName });

            if (!result.topicSchemaPolicy) {
                throw new Error('Policy not found');
            }

            this.originalPolicy = result.topicSchemaPolicy;
            this.populateForm();

            document.getElementById('loading-indicator').style.display = 'none';
            document.getElementById('policy-content').style.display = 'block';
        } catch (error) {
            console.error('Error loading policy:', error);
            this.showError('Failed to load policy: ' + error.message);
            setTimeout(() => {
                window.location.href = '/pages/topic-schema-policies.html';
            }, 2000);
        }
    }

    populateForm() {
        const policy = this.originalPolicy;

        document.getElementById('policy-name').value = policy.name;
        document.getElementById('policy-name').disabled = true;
        document.getElementById('policy-description').value = policy.description || '';
        document.getElementById('policy-version').value = policy.version || '';

        document.getElementById('policy-payload-type').value = policy.payloadType || 'JSON';
        document.getElementById('policy-content-type').value = policy.contentType || 'application/json';

        // JSON Schema
        const schema = policy.jsonSchema;
        document.getElementById('policy-json-schema').value =
            typeof schema === 'string' ? schema : JSON.stringify(schema, null, 2);
    }

    validateForm() {
        const name = document.getElementById('policy-name').value.trim();
        const jsonSchemaText = document.getElementById('policy-json-schema').value.trim();

        if (!name) {
            this.showError('Policy name is required');
            return false;
        }

        if (!jsonSchemaText) {
            this.showError('JSON Schema is required');
            return false;
        }

        try {
            JSON.parse(jsonSchemaText);
        } catch (e) {
            this.showError('Invalid JSON Schema: ' + e.message);
            return false;
        }

        return true;
    }

    async savePolicy() {
        if (!this.validateForm()) {
            return;
        }

        try {
            const name = document.getElementById('policy-name').value.trim();
            const description = document.getElementById('policy-description').value.trim() || null;
            const version = document.getElementById('policy-version').value.trim() || null;

            const payloadType = document.getElementById('policy-payload-type').value;
            const contentType = document.getElementById('policy-content-type').value.trim() || null;

            const jsonSchemaText = document.getElementById('policy-json-schema').value.trim();
            const jsonSchema = JSON.parse(jsonSchemaText);

            const input = {
                name,
                jsonSchema,
                payloadType,
                contentType,
                version,
                description
            };

            console.log('Saving topic schema policy with input:', input);

            let result;
            if (this.isNewPolicy) {
                result = await window.graphqlClient.query(`
                    mutation CreateTopicSchemaPolicy($input: TopicSchemaPolicyInput!) {
                        topicSchemaPolicy {
                            create(input: $input) {
                                success
                                policy {
                                    name
                                }
                                errors
                            }
                        }
                    }
                `, { input });

                if (result.topicSchemaPolicy.create.success) {
                    console.log('Policy created successfully');
                    window.location.href = '/pages/topic-schema-policies.html';
                } else {
                    const errors = result.topicSchemaPolicy.create.errors || [];
                    const errorMessage = errors.length > 0 ? errors.join(', ') : 'Failed to create policy';
                    this.showError(errorMessage);
                }
            } else {
                result = await window.graphqlClient.query(`
                    mutation UpdateTopicSchemaPolicy($name: String!, $input: TopicSchemaPolicyInput!) {
                        topicSchemaPolicy {
                            update(name: $name, input: $input) {
                                success
                                policy {
                                    name
                                }
                                errors
                            }
                        }
                    }
                `, { name: this.policyName, input });

                if (result.topicSchemaPolicy.update.success) {
                    console.log('Policy updated successfully');
                    window.location.href = '/pages/topic-schema-policies.html';
                } else {
                    const errors = result.topicSchemaPolicy.update.errors || [];
                    const errorMessage = errors.length > 0 ? errors.join(', ') : 'Failed to update policy';
                    this.showError(errorMessage);
                }
            }
        } catch (error) {
            console.error('Error saving policy:', error);
            this.showError('Failed to save policy: ' + error.message);
        }
    }

    async validateTestPayload() {
        const payloadText = document.getElementById('test-payload').value.trim();
        const resultsDiv = document.getElementById('test-results');
        const resultsTitle = document.getElementById('test-results-title');
        const resultsBody = document.getElementById('test-results-body');

        if (!payloadText) {
            this.showError('Please enter a test payload');
            return;
        }

        // Validate JSON
        try {
            JSON.parse(payloadText);
        } catch (e) {
            resultsDiv.className = 'test-results invalid';
            resultsTitle.textContent = 'Invalid JSON';
            resultsBody.innerHTML = `<p>${this.escapeHtml(e.message)}</p>`;
            return;
        }

        // If this is an existing policy, use server-side validation
        if (!this.isNewPolicy && this.policyName) {
            try {
                const result = await window.graphqlClient.query(`
                    query ValidatePayload($policyName: String!, $payload: String!) {
                        topicSchemaValidate(policyName: $policyName, payload: $payload) {
                            policyName
                            topicFilter
                            valid
                            errorCategory
                            errorDetail
                        }
                    }
                `, { policyName: this.policyName, payload: payloadText });

                const validation = result.topicSchemaValidate;
                if (validation.valid) {
                    resultsDiv.className = 'test-results valid';
                    resultsTitle.textContent = 'Validation Passed';
                    resultsBody.innerHTML = '<p>The payload matches the schema.</p>';
                } else {
                    resultsDiv.className = 'test-results invalid';
                    resultsTitle.textContent = 'Validation Failed';
                    const detail = validation.errorDetail || 'Unknown error';
                    const category = validation.errorCategory || '';
                    resultsBody.innerHTML = `<p>${category ? `<strong>${this.escapeHtml(category)}:</strong> ` : ''}${this.escapeHtml(detail)}</p>`;
                }
            } catch (error) {
                console.error('Error validating payload:', error);
                resultsDiv.className = 'test-results invalid';
                resultsTitle.textContent = 'Validation Error';
                resultsBody.innerHTML = `<p>${this.escapeHtml(error.message)}</p>`;
            }
        } else {
            // For new policies, just confirm it's valid JSON
            resultsDiv.className = 'test-results valid';
            resultsTitle.textContent = 'Valid JSON';
            resultsBody.innerHTML = '<p>The payload is valid JSON. Save the policy first to test against the schema.</p>';
        }
    }

    showDeleteModal() {
        document.getElementById('delete-policy-name').textContent = this.policyName;
        document.getElementById('delete-policy-modal').style.display = 'flex';
    }

    hideDeleteModal() {
        document.getElementById('delete-policy-modal').style.display = 'none';
    }

    async confirmDeletePolicy() {
        this.hideDeleteModal();

        try {
            console.log('Deleting policy:', this.policyName);

            const result = await window.graphqlClient.query(`
                mutation DeletePolicy($name: String!) {
                    topicSchemaPolicy {
                        delete(name: $name)
                    }
                }
            `, { name: this.policyName });

            if (result.topicSchemaPolicy.delete) {
                console.log('Policy deleted successfully');
                window.location.href = '/pages/topic-schema-policies.html';
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

// Global functions
function goBack() {
    window.location.href = '/pages/topic-schema-policies.html';
}

function savePolicy() {
    window.policyDetailManager.savePolicy();
}

function showDeleteModal() {
    window.policyDetailManager.showDeleteModal();
}

function hideDeleteModal() {
    window.policyDetailManager.hideDeleteModal();
}

function confirmDeletePolicy() {
    window.policyDetailManager.confirmDeletePolicy();
}

function validatePayload() {
    window.policyDetailManager.validateTestPayload();
}

// Initialize when page loads
document.addEventListener('DOMContentLoaded', () => {
    window.policyDetailManager = new TopicSchemaPolicyDetailManager();
});

// Handle modal clicks (close when clicking outside)
window.onclick = (event) => {
    const deleteModal = document.getElementById('delete-policy-modal');
    if (event.target === deleteModal) {
        hideDeleteModal();
    }
};
