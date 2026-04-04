// GenAI Provider Detail Page

class GenAiProviderDetailManager {
    constructor() {
        this.client = new GraphQLDashboardClient();
        this.providerName = null;
        this.isNew = false;
        this.providerData = null;
        this.init();
    }

    async init() {
        if (!window.isLoggedIn || !window.isLoggedIn()) return;
        const params = new URLSearchParams(window.location.search);
        this.providerName = params.get('name');
        this.isNew = params.get('new') === 'true';

        if (this.isNew) {
            this.showNewForm();
        } else if (this.providerName) {
            await this.loadProvider();
        } else {
            this.showAlert('No provider specified.', 'error');
        }
    }

    showNewForm() {
        document.getElementById('page-title').textContent = 'Add AI Provider';
        document.getElementById('page-subtitle').textContent = 'Create a new GenAI provider';
        document.getElementById('provider-name').disabled = false;
        document.getElementById('save-btn').textContent = 'Create Provider';
        document.getElementById('delete-btn').style.display = 'none';
        document.getElementById('provider-content').style.display = 'block';
        this.onTypeChange();
    }

    async loadProvider() {
        try {
            const result = await this.client.query(`
                query GetProvider($name: String!) {
                    genAiProvider(name: $name) {
                        name type model apiKey endpoint serviceVersion baseUrl temperature maxTokens enabled source createdAt updatedAt
                    }
                }
            `, { name: this.providerName });

            if (!result.genAiProvider) {
                this.showAlert('Provider not found.', 'error');
                return;
            }

            this.providerData = result.genAiProvider;
            this.populateForm(result.genAiProvider);

            // Timestamps
            if (!this.isNew) {
                document.getElementById('timestamps-section').style.display = 'block';
                document.getElementById('created-at').textContent = this.formatDateTime(result.genAiProvider.createdAt);
                document.getElementById('updated-at').textContent = this.formatDateTime(result.genAiProvider.updatedAt);
            }
        } catch (e) {
            this.showAlert('Failed to load provider: ' + e.message, 'error');
        }
    }

    formatDateTime(isoString) {
        if (!isoString) return '-';
        try {
            return new Date(isoString).toLocaleString();
        } catch (e) {
            return isoString;
        }
    }

    populateForm(p) {
        document.getElementById('page-title').textContent = 'AI Provider: ' + p.name;
        document.getElementById('page-subtitle').textContent = p.type + (p.source === 'config' ? ' · from config.yaml' : '');

        document.getElementById('provider-name').value = p.name;
        document.getElementById('provider-name').disabled = true;
        document.getElementById('provider-type').value = p.type || 'gemini';
        document.getElementById('provider-model').value = p.model || '';
        document.getElementById('provider-api-key').value = '';
        document.getElementById('provider-api-key').placeholder = p.apiKey ? 'Stored (enter to replace)' : 'No key configured';
        document.getElementById('provider-endpoint').value = p.endpoint || '';
        document.getElementById('provider-service-version').value = p.serviceVersion || '';
        document.getElementById('provider-base-url').value = p.baseUrl || '';
        document.getElementById('provider-temperature').value = p.temperature != null ? p.temperature : 0.7;
        document.getElementById('provider-max-tokens').value = p.maxTokens || '';
        document.getElementById('provider-enabled').checked = p.enabled !== false;

        if (p.source === 'config') {
            // Read-only: from config.yaml
            document.getElementById('alert-area').innerHTML =
                '<div class="source-notice">⚙️ This provider is configured in <strong>config.yaml</strong> and cannot be edited here. Restart the broker to apply changes.</div>';
            document.querySelectorAll('#provider-content input, #provider-content select').forEach(el => el.disabled = true);
            document.getElementById('save-btn').style.display = 'none';
            document.getElementById('delete-btn').style.display = 'none';
        } else {
            document.getElementById('delete-btn').style.display = '';
        }

        document.getElementById('provider-content').style.display = 'block';
        this.onTypeChange();
    }

    onTypeChange() {
        const type = document.getElementById('provider-type').value;
        const isAzure = type === 'azure-openai';
        const isOpenAI = type === 'openai';
        const isOllama = type === 'ollama';

        document.getElementById('provider-endpoint-group').style.display = (isAzure || isOpenAI) ? '' : 'none';
        document.getElementById('provider-service-version-group').style.display = isAzure ? '' : 'none';
        document.getElementById('provider-base-url-group').style.display = isOllama ? '' : 'none';

        const endpointLabel = document.getElementById('provider-endpoint-label');
        const endpointInput = document.getElementById('provider-endpoint');
        if (isAzure) {
            endpointLabel.textContent = 'Azure Endpoint';
            endpointInput.placeholder = 'https://<resource>.openai.azure.com/';
        } else if (isOpenAI) {
            endpointLabel.textContent = 'Custom Endpoint (optional)';
            endpointInput.placeholder = 'https://api.openai.com/v1 (leave blank for default)';
        }

        const modelInput = document.getElementById('provider-model');
        const placeholders = {
            'gemini': 'gemini-2.0-flash',
            'claude': 'claude-sonnet-4-20250514',
            'openai': 'gpt-4o',
            'ollama': 'llama3',
            'azure-openai': 'deployment-name'
        };
        modelInput.placeholder = placeholders[type] || 'Model name';
    }

    collectFormData() {
        const data = {
            type:           document.getElementById('provider-type').value,
            model:          document.getElementById('provider-model').value.trim() || null,
            endpoint:       document.getElementById('provider-endpoint').value.trim() || null,
            serviceVersion: document.getElementById('provider-service-version').value.trim() || null,
            baseUrl:        document.getElementById('provider-base-url').value.trim() || null,
            temperature:    parseFloat(document.getElementById('provider-temperature').value) || 0.7,
            maxTokens:      parseInt(document.getElementById('provider-max-tokens').value) || null,
            enabled:        document.getElementById('provider-enabled').checked
        };
        const apiKey = document.getElementById('provider-api-key').value;
        if (apiKey) data.apiKey = apiKey;
        return data;
    }

    async save() {
        const name = document.getElementById('provider-name').value.trim();

        if (this.isNew) {
            if (!name || !/^[a-zA-Z0-9_-]+$/.test(name)) {
                this.showAlert('Invalid name: only letters, digits, underscores and hyphens are allowed.', 'error');
                return;
            }
        }

        const input = this.collectFormData();

        try {
            if (this.isNew) {
                const result = await this.client.query(`
                    mutation CreateProvider($name: String!, $input: GenAiProviderInput!) {
                        genAiProvider { create(name: $name, input: $input) { name } }
                    }
                `, { name, input });
                if (result.genAiProvider?.create) {
                    this.showAlert('Provider "' + name + '" created successfully.', 'success');
                    setTimeout(() => window.navigateTo('/pages/genai-providers.html'), 800);
                }
            } else {
                const result = await this.client.query(`
                    mutation UpdateProvider($name: String!, $input: GenAiProviderInput!) {
                        genAiProvider { update(name: $name, input: $input) { name } }
                    }
                `, { name: this.providerName, input });
                if (result.genAiProvider?.update) {
                    this.showAlert('Provider updated successfully.', 'success');
                    await this.loadProvider();
                }
            }
        } catch (e) {
            this.showAlert('Save failed: ' + e.message, 'error');
        }
    }

    async deleteProvider() {
        if (!confirm('Delete provider "' + this.providerName + '"? This cannot be undone.')) return;
        try {
            const result = await this.client.query(`
                mutation DeleteProvider($name: String!) {
                    genAiProvider { delete(name: $name) }
                }
            `, { name: this.providerName });
            if (result.genAiProvider?.delete) {
                window.navigateTo('/pages/genai-providers.html');
            }
        } catch (e) {
            this.showAlert('Delete failed: ' + e.message, 'error');
        }
    }

    showAlert(msg, type) {
        document.getElementById('alert-area').innerHTML =
            '<div class="alert alert-' + type + '">' + msg + '</div>';
        if (type === 'success') {
            setTimeout(() => { document.getElementById('alert-area').innerHTML = ''; }, 3000);
        }
    }
}

const providerDetailManager = new GenAiProviderDetailManager();
