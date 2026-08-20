// MonsterMQ Dashboard — Data Catalog Instance Detail Manager

class DataCatalogInstanceDetailManager {
    constructor() {
        this.instanceId = null;
        this.isNew = true;
        this.types = [];

        this.init();
    }

    async init() {
        if (!this.isLoggedIn()) {
            window.location.href = '/pages/login.html';
            return;
        }

        await this.loadTypes();

        const params = new URLSearchParams(window.location.search);
        this.instanceId = params.get('id');
        this.isNew = !this.instanceId || params.get('new') === 'true';

        if (this.isNew) {
            document.getElementById('page-title').textContent = 'Create Object Instance';
            document.getElementById('breadcrumb-instance-name').textContent = 'New Instance';
            document.getElementById('instance-properties').value = JSON.stringify({
                "location": "Plant Floor 1",
                "vendor": "Acme Corp"
            }, null, 2);
        } else {
            document.getElementById('instance-id').disabled = true;
            document.getElementById('btn-delete').style.display = 'inline-flex';
            await this.loadInstance();
        }
    }

    isLoggedIn() {
        return window.isLoggedIn ? window.isLoggedIn() : true;
    }

    async loadTypes() {
        try {
            const res = await window.graphqlClient.query(`
                query GetTypesForInstance {
                    dataCatalogTypes {
                        id
                        name
                        description
                        topicPattern
                    }
                }
            `);

            this.types = res.dataCatalogTypes || [];
            const select = document.getElementById('instance-type-id');
            select.innerHTML = '<option value="">Select Object Type...</option>' +
                this.types.map(t => `<option value="${this.escapeHtml(t.id)}">${this.escapeHtml(t.name)} (${this.escapeHtml(t.id)})</option>`).join('');
        } catch (e) {
            console.error('Failed to load types:', e);
        }
    }

    onTypeChange() {
        const typeId = document.getElementById('instance-type-id').value;
        const type = this.types.find(t => t.id === typeId);
        const infoEl = document.getElementById('selected-type-schema-info');
        if (type) {
            infoEl.textContent = `Type Pattern: ${type.topicPattern || 'None'} • ${type.description || ''}`;
        } else {
            infoEl.textContent = '';
        }
    }

    formatJson() {
        const textarea = document.getElementById('instance-properties');
        try {
            const parsed = JSON.parse(textarea.value);
            textarea.value = JSON.stringify(parsed, null, 2);
            window.ui.success('JSON formatted.');
        } catch (e) {
            window.ui.error('Invalid JSON: ' + e.message);
        }
    }

    async checkLiveTopic() {
        const baseTopic = document.getElementById('instance-base-topic').value.trim();
        if (!baseTopic) {
            window.ui.error('Please enter a Base Topic path first.');
            return;
        }

        const statusText = document.getElementById('live-topic-status-text');
        const payloadBox = document.getElementById('live-topic-payload');

        statusText.innerHTML = '<ix-spinner size="16"></ix-spinner> Querying broker topics...';
        payloadBox.style.display = 'none';

        try {
            const res = await window.graphqlClient.query(`
                query CheckTopicValue($topic: String!) {
                    topics(topicFilter: $topic) {
                        name
                        value
                        lastUpdated
                    }
                }
            `, { topic: baseTopic.endsWith('#') ? baseTopic : baseTopic + '/#' });

            const topics = res.topics || [];
            if (topics.length === 0) {
                statusText.innerHTML = `<span style="color: var(--monster-orange);">⚠️ No active messages found under topic <code>${this.escapeHtml(baseTopic)}</code>. (Topic may not have published yet).</span>`;
            } else {
                statusText.innerHTML = `<span style="color: var(--monster-green);">✓ Active! Found ${topics.length} topic leaf nodes under this base path.</span>`;
                payloadBox.style.display = 'block';
                payloadBox.textContent = JSON.stringify(topics.map(t => ({
                    topic: t.name,
                    value: t.value,
                    lastUpdated: t.lastUpdated
                })), null, 2);
            }
        } catch (e) {
            statusText.innerHTML = `<span style="color: var(--monster-red);">Error checking topic: ${this.escapeHtml(e.message)}</span>`;
        }
    }

    async loadInstance() {
        try {
            window.ui.setLoading(true);
            const res = await window.graphqlClient.query(`
                query GetInstance($id: String!) {
                    dataCatalogInstance(id: $id) {
                        id
                        typeId
                        name
                        baseTopic
                        properties
                    }
                }
            `, { id: this.instanceId });

            const inst = res.dataCatalogInstance;
            if (!inst) {
                window.ui.showError(`Object Instance "${this.instanceId}" not found.`);
                return;
            }

            document.getElementById('instance-id').value = inst.id;
            document.getElementById('instance-type-id').value = inst.typeId;
            document.getElementById('instance-name').value = inst.name || '';
            document.getElementById('instance-base-topic').value = inst.baseTopic || '';
            document.getElementById('instance-properties').value = JSON.stringify(inst.properties || {}, null, 2);

            document.getElementById('page-title').textContent = `Edit ${inst.name || inst.id}`;
            document.getElementById('breadcrumb-instance-name').textContent = inst.name || inst.id;

            this.onTypeChange();
            window.ui.setLoading(false);
        } catch (error) {
            console.error('Error loading instance:', error);
            window.ui.setLoading(false);
            window.ui.showError('Failed to load instance: ' + error.message);
        }
    }

    async saveInstance() {
        const id = document.getElementById('instance-id').value.trim();
        const typeId = document.getElementById('instance-type-id').value.trim();
        const name = document.getElementById('instance-name').value.trim();
        const baseTopic = document.getElementById('instance-base-topic').value.trim();
        const propertiesText = document.getElementById('instance-properties').value.trim();

        if (!id) {
            window.ui.error('Please enter an Instance ID.');
            return;
        }

        if (!typeId) {
            window.ui.error('Please select an Object Type.');
            return;
        }

        if (!name) {
            window.ui.error('Please enter a Display Name.');
            return;
        }

        if (!baseTopic) {
            window.ui.error('Please enter a Base Topic path.');
            return;
        }

        let properties = {};
        if (propertiesText) {
            try {
                properties = JSON.parse(propertiesText);
            } catch (e) {
                window.ui.error('Properties must be valid JSON: ' + e.message);
                return;
            }
        }

        const input = {
            id,
            typeId,
            name,
            baseTopic,
            properties
        };

        try {
            window.ui.setLoading(true);
            await window.graphqlClient.query(`
                mutation SaveInstance($input: DataCatalogInstanceInput!) {
                    dataCatalog {
                        saveInstance(input: $input) {
                            id
                            name
                        }
                    }
                }
            `, { input });

            window.ui.setLoading(false);
            window.ui.success(`Object Instance "${name}" saved successfully!`);

            if (this.isNew) {
                this.isNew = false;
                this.instanceId = id;
                document.getElementById('instance-id').disabled = true;
                document.getElementById('btn-delete').style.display = 'inline-flex';
                document.getElementById('page-title').textContent = `Edit ${name}`;
                document.getElementById('breadcrumb-instance-name').textContent = name;
                window.history.replaceState({ page: `/pages/data-catalog-instance-detail.html?id=${encodeURIComponent(id)}` }, '', `/pages/data-catalog-instance-detail.html?id=${encodeURIComponent(id)}`);
            }
        } catch (error) {
            console.error('Error saving instance:', error);
            window.ui.setLoading(false);
            window.ui.error('Failed to save Object Instance: ' + error.message);
        }
    }

    async deleteInstance() {
        if (!this.instanceId) return;

        const confirmed = await window.ui.showConfirm({
            title: 'Delete Object Instance',
            message: `Are you sure you want to delete object instance "${this.instanceId}"?`,
            confirmText: 'Delete',
            type: 'danger'
        });

        if (!confirmed) return;

        try {
            await window.graphqlClient.query(`
                mutation DeleteInstance($id: String!) {
                    dataCatalog {
                        deleteInstance(id: $id)
                    }
                }
            `, { id: this.instanceId });

            window.ui.success('Object Instance deleted.');
            window.location.href = '/pages/data-catalog.html';
        } catch (error) {
            window.ui.error('Failed to delete instance: ' + error.message);
        }
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

const instanceDetailManager = new DataCatalogInstanceDetailManager();
window.instanceDetailManager = instanceDetailManager;
