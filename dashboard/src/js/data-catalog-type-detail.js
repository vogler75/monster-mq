// MonsterMQ Dashboard — Data Catalog Type Detail Manager

class DataCatalogTypeDetailManager {
    constructor() {
        this.typeId = null;
        this.isNew = true;

        this.templates = {
            telemetry: {
                "$schema": "http://json-schema.org/draft-07/schema#",
                "type": "object",
                "title": "EnvironmentalTelemetry",
                "properties": {
                    "temperature": { "type": "number", "description": "Temperature in Celsius" },
                    "humidity": { "type": "number", "description": "Relative humidity percentage" },
                    "pressure": { "type": "number", "description": "Atmospheric pressure in hPa" },
                    "timestamp": { "type": "string", "format": "date-time" }
                },
                "required": ["temperature"]
            },
            device: {
                "$schema": "http://json-schema.org/draft-07/schema#",
                "type": "object",
                "title": "MachineAsset",
                "properties": {
                    "state": { "type": "string", "enum": ["RUNNING", "STOPPED", "ERROR", "MAINTENANCE"] },
                    "rpm": { "type": "number", "description": "Current rotational speed" },
                    "powerKw": { "type": "number", "description": "Active power draw" },
                    "errorCode": { "type": "integer" }
                },
                "required": ["state"]
            },
            simple: {
                "$schema": "http://json-schema.org/draft-07/schema#",
                "type": "object",
                "title": "SimpleDatapoint",
                "properties": {
                    "value": { "type": "number" },
                    "quality": { "type": "string", "default": "GOOD" },
                    "timestamp": { "type": "string", "format": "date-time" }
                },
                "required": ["value"]
            }
        };

        this.init();
    }

    async init() {
        if (!this.isLoggedIn()) {
            window.location.href = '/pages/login.html';
            return;
        }

        const params = new URLSearchParams(window.location.search);
        this.typeId = params.get('id');
        this.isNew = !this.typeId || params.get('new') === 'true';

        if (this.isNew) {
            document.getElementById('page-title').textContent = 'Create Object Type';
            document.getElementById('breadcrumb-type-name').textContent = 'New Type';
            document.getElementById('type-structure').value = JSON.stringify(this.templates.telemetry, null, 2);
        } else {
            document.getElementById('type-id').disabled = true;
            document.getElementById('btn-delete').style.display = 'inline-flex';
            await this.loadType();
        }
    }

    isLoggedIn() {
        return window.isLoggedIn ? window.isLoggedIn() : true;
    }

    applyTemplate(name) {
        if (this.templates[name]) {
            document.getElementById('type-structure').value = JSON.stringify(this.templates[name], null, 2);
            window.ui.success(`Applied ${name} schema template.`);
        }
    }

    formatJson() {
        const textarea = document.getElementById('type-structure');
        try {
            const parsed = JSON.parse(textarea.value);
            textarea.value = JSON.stringify(parsed, null, 2);
            window.ui.success('JSON formatted.');
        } catch (e) {
            window.ui.error('Invalid JSON: ' + e.message);
        }
    }

    async loadType() {
        try {
            window.ui.setLoading(true);
            const res = await window.graphqlClient.query(`
                query GetType($id: String!) {
                    dataCatalogType(id: $id) {
                        id
                        namespace
                        name
                        description
                        structure
                        topicPattern
                    }
                }
            `, { id: this.typeId });

            const type = res.dataCatalogType;
            if (!type) {
                window.ui.showError(`Object Type "${this.typeId}" not found.`);
                return;
            }

            document.getElementById('type-id').value = type.id;
            document.getElementById('type-namespace').value = type.namespace || 'default';
            document.getElementById('type-name').value = type.name || '';
            document.getElementById('type-topic-pattern').value = type.topicPattern || '';
            document.getElementById('type-description').value = type.description || '';
            document.getElementById('type-structure').value = JSON.stringify(type.structure || {}, null, 2);

            document.getElementById('page-title').textContent = `Edit ${type.name || type.id}`;
            document.getElementById('breadcrumb-type-name').textContent = type.name || type.id;

            window.ui.setLoading(false);
        } catch (error) {
            console.error('Error loading type:', error);
            window.ui.setLoading(false);
            window.ui.showError('Failed to load type: ' + error.message);
        }
    }

    async saveType() {
        const id = document.getElementById('type-id').value.trim();
        const namespace = document.getElementById('type-namespace').value.trim() || 'default';
        const name = document.getElementById('type-name').value.trim();
        const topicPattern = document.getElementById('type-topic-pattern').value.trim() || null;
        const description = document.getElementById('type-description').value.trim() || null;
        const structureText = document.getElementById('type-structure').value.trim();

        if (!id) {
            window.ui.error('Please enter a Type ID.');
            return;
        }

        if (!name) {
            window.ui.error('Please enter a Display Name.');
            return;
        }

        let structure = {};
        if (structureText) {
            try {
                structure = JSON.parse(structureText);
            } catch (e) {
                window.ui.error('Structure must be valid JSON: ' + e.message);
                return;
            }
        }

        const input = {
            id,
            namespace,
            name,
            topicPattern,
            description,
            structure
        };

        try {
            window.ui.setLoading(true);
            await window.graphqlClient.query(`
                mutation SaveType($input: DataCatalogTypeInput!) {
                    dataCatalog {
                        saveType(input: $input) {
                            id
                            name
                        }
                    }
                }
            `, { input });

            window.ui.setLoading(false);
            window.ui.success(`Object Type "${name}" saved successfully!`);

            if (this.isNew) {
                // Update URL to edit mode without full reload
                this.isNew = false;
                this.typeId = id;
                document.getElementById('type-id').disabled = true;
                document.getElementById('btn-delete').style.display = 'inline-flex';
                document.getElementById('page-title').textContent = `Edit ${name}`;
                document.getElementById('breadcrumb-type-name').textContent = name;
                window.history.replaceState({ page: `/pages/data-catalog-type-detail.html?id=${encodeURIComponent(id)}` }, '', `/pages/data-catalog-type-detail.html?id=${encodeURIComponent(id)}`);
            }
        } catch (error) {
            console.error('Error saving type:', error);
            window.ui.setLoading(false);
            window.ui.error('Failed to save Object Type: ' + error.message);
        }
    }

    async deleteType() {
        if (!this.typeId) return;

        const confirmed = await window.ui.showConfirm({
            title: 'Delete Object Type',
            message: `Are you sure you want to delete object type "${this.typeId}"?`,
            confirmText: 'Delete',
            type: 'danger'
        });

        if (!confirmed) return;

        try {
            await window.graphqlClient.query(`
                mutation DeleteType($id: String!) {
                    dataCatalog {
                        deleteType(id: $id)
                    }
                }
            `, { id: this.typeId });

            window.ui.success('Object Type deleted.');
            window.location.href = '/pages/data-catalog.html';
        } catch (error) {
            window.ui.error('Failed to delete type: ' + error.message);
        }
    }
}

const typeDetailManager = new DataCatalogTypeDetailManager();
window.typeDetailManager = typeDetailManager;
