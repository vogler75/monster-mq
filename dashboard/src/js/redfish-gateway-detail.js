/**
 * Redfish Gateway Detail & Configuration Manager
 */
class RedfishDetailManager {
    static instance = null;

    constructor() {
        RedfishDetailManager.instance = this;
        this.gatewayName = null;
        this.isNew = false;
        this.selectedArchiveGroup = 'Default';
        this.treeNodes = new Map();

        // Topic browser panel state
        this.panel = document.getElementById('topic-browser-panel');
        this.resizeHandle = document.getElementById('topic-panel-resize-handle');
        this.isOpen = false;
        this.panelWidth = 380;

        this.init();
    }

    async init() {
        const params = new URLSearchParams(window.location.search);
        this.gatewayName = params.get('name');
        this.isNew = params.get('new') === 'true' || !this.gatewayName;

        this.setupDropZone();
        this.setupResize();
        await this.loadArchiveGroups();

        if (this.isNew) {
            this.setupNewGateway();
        } else {
            await this.loadExistingGateway(this.gatewayName);
        }
    }

    setupNewGateway() {
        document.getElementById('breadcrumb-name').textContent = 'New Gateway';
        document.getElementById('page-title').textContent = 'New Redfish Gateway';
        document.getElementById('page-subtitle').textContent = 'Create a new MQTT to DMTF Redfish sensor mapping';
        document.getElementById('delete-btn').style.display = 'none';
        document.getElementById('api-btn').style.display = 'none';
        document.getElementById('gateway-status').textContent = 'NEW';
        document.getElementById('gateway-status').className = 'status-badge status-info';

        document.getElementById('gateway-name').value = 'edge-redfish-1';
        document.getElementById('gateway-chassis-id').value = 'EdgeNode';
        document.getElementById('gateway-topic-prefix').value = 'redfish';
        document.getElementById('gateway-enabled').checked = true;
        document.getElementById('gateway-reading-type').value = 'Temperature';
        document.getElementById('gateway-reading-units').value = 'Cel';

        this.clearTopicFilters();
        this.addTopicFilterRow('sensors/+/temperature');
        this.applyTemplate('single');

        document.getElementById('gateway-content').style.display = 'block';
    }

    async loadExistingGateway(name) {
        this.showLoading(true);
        this.hideError();
        try {
            const query = `
                query GetRedfishGateway($name: String!) {
                    redfishMapping(name: $name) {
                        name
                        nodeId
                        enabled
                        createdAt
                        updatedAt
                        isOnCurrentNode
                        config {
                            topicPrefix
                            topicFilters
                            chassisId
                            defaultReadingType
                            defaultReadingUnits
                            thresholds {
                                upperCaution
                                upperCritical
                                lowerCaution
                                lowerCritical
                            }
                            jsonSchema
                        }
                    }
                }
            `;
            const result = await window.graphqlClient.query(query, { name });
            const gw = result?.redfishMapping;
            if (!gw) throw new Error(`Gateway "${name}" not found`);

            const cfg = gw.config || {};
            document.getElementById('breadcrumb-name').textContent = gw.name;
            document.getElementById('page-title').textContent = `Gateway: ${gw.name}`;
            document.getElementById('page-subtitle').textContent = `Node: ${gw.nodeId || 'local'} | DMTF Redfish Chassis: ${cfg.chassisId || 'EdgeNode'}`;

            const nameInput = document.getElementById('gateway-name');
            nameInput.value = gw.name;
            nameInput.disabled = true;

            document.getElementById('gateway-chassis-id').value = cfg.chassisId || 'EdgeNode';
            document.getElementById('gateway-topic-prefix').value = cfg.topicPrefix || 'redfish';
            document.getElementById('gateway-enabled').checked = gw.enabled;
            document.getElementById('gateway-reading-type').value = cfg.defaultReadingType || 'Temperature';
            document.getElementById('gateway-reading-units').value = cfg.defaultReadingUnits || 'Cel';

            const statusBadge = document.getElementById('gateway-status');
            statusBadge.textContent = gw.enabled ? 'ACTIVE' : 'DISABLED';
            statusBadge.className = `status-badge ${gw.enabled ? 'status-enabled' : 'status-disabled'}`;

            // Thresholds
            const th = cfg.thresholds || {};
            document.getElementById('thresh-upper-critical').value = th.upperCritical ?? '';
            document.getElementById('thresh-upper-caution').value = th.upperCaution ?? '';
            document.getElementById('thresh-lower-caution').value = th.lowerCaution ?? '';
            document.getElementById('thresh-lower-critical').value = th.lowerCritical ?? '';

            // Filters
            this.clearTopicFilters();
            const filters = Array.isArray(cfg.topicFilters) && cfg.topicFilters.length > 0
                ? cfg.topicFilters
                : ['sensors/+/temperature'];
            filters.forEach(f => this.addTopicFilterRow(f));

            // JSON Schema
            const schemaObj = cfg.jsonSchema || {};
            document.getElementById('gateway-schema').value = JSON.stringify(schemaObj, null, 2);

            // Action buttons
            document.getElementById('delete-btn').style.display = 'inline-block';
            const apiBtn = document.getElementById('api-btn');
            apiBtn.href = `/redfish/v1/Chassis/${encodeURIComponent(cfg.chassisId || 'EdgeNode')}/Sensors`;
            apiBtn.style.display = 'inline-block';

            document.getElementById('gateway-content').style.display = 'block';

            // Show and load live sensors
            document.getElementById('live-sensors-card').style.display = 'block';
            await this.loadLiveSensors(cfg.chassisId || 'EdgeNode');
        } catch (e) {
            console.error('Error loading gateway:', e);
            this.showError('Failed to load gateway: ' + e.message);
        } finally {
            this.showLoading(false);
        }
    }

    async saveGateway() {
        const name = document.getElementById('gateway-name').value.trim();
        const chassisId = document.getElementById('gateway-chassis-id').value.trim();
        const topicPrefix = document.getElementById('gateway-topic-prefix').value.trim();
        const enabled = document.getElementById('gateway-enabled').checked;
        const defaultReadingType = document.getElementById('gateway-reading-type').value;
        const defaultReadingUnits = document.getElementById('gateway-reading-units').value;
        const topicFilters = this.getTopicFilters();

        if (!name) {
            window.ui.error('Please enter a Gateway Name');
            return;
        }
        if (!chassisId) {
            window.ui.error('Please enter a Redfish Chassis ID');
            return;
        }
        if (!topicPrefix) {
            window.ui.error('Please enter an Internal Topic Prefix');
            return;
        }
        if (topicFilters.length === 0) {
            window.ui.error('Please specify at least one MQTT Topic Filter');
            return;
        }

        let jsonSchema = {};
        const schemaText = document.getElementById('gateway-schema').value.trim();
        if (schemaText) {
            try {
                jsonSchema = JSON.parse(schemaText);
            } catch (err) {
                window.ui.error('Invalid JSON Schema format: ' + err.message);
                return;
            }
        }

        // Thresholds
        const parseNum = id => {
            const v = document.getElementById(id).value.trim();
            return v === '' ? null : parseFloat(v);
        };
        const thresholds = {
            upperCritical: parseNum('thresh-upper-critical'),
            upperCaution: parseNum('thresh-upper-caution'),
            lowerCaution: parseNum('thresh-lower-caution'),
            lowerCritical: parseNum('thresh-lower-critical')
        };

        const configInput = {
            topicPrefix,
            topicFilters,
            chassisId,
            defaultReadingType,
            defaultReadingUnits,
            thresholds,
            jsonSchema
        };

        try {
            const mutation = `
                mutation SaveRedfish($name: String!, $config: RedfishMappingConfigInput!, $enabled: Boolean) {
                    saveRedfishMapping(name: $name, config: $config, enabled: $enabled) {
                        success
                        message
                        redfish {
                            name
                            enabled
                            config {
                                chassisId
                            }
                        }
                    }
                }
            `;
            const result = await window.graphqlClient.query(mutation, {
                name,
                config: configInput,
                enabled
            });

            if (result?.saveRedfishMapping?.success) {
                window.ui.success(`Redfish Gateway "${name}" saved successfully`);
                if (this.isNew) {
                    window.location.href = `/pages/redfish-gateway-detail.html?name=${encodeURIComponent(name)}`;
                } else {
                    await this.loadExistingGateway(name);
                }
            } else {
                throw new Error(result?.saveRedfishMapping?.message || 'Save returned failure');
            }
        } catch (e) {
            console.error('Error saving gateway:', e);
            window.ui.error('Failed to save gateway: ' + e.message);
        }
    }

    async deleteGateway() {
        const name = document.getElementById('gateway-name').value.trim();
        const confirmed = await window.ui.showConfirm({
            title: 'Delete Redfish Gateway',
            message: `Are you sure you want to delete gateway "${name}"? This action cannot be undone.`,
            confirmText: 'Delete',
            type: 'danger'
        });

        if (!confirmed) return;

        try {
            const mutation = `
                mutation DeleteRedfish($name: String!) {
                    deleteRedfishMapping(name: $name)
                }
            `;
            const res = await window.graphqlClient.query(mutation, { name });
            if (res?.deleteRedfishMapping) {
                window.ui.success(`Gateway "${name}" deleted`);
                window.location.href = '/pages/redfish-gateways.html';
            } else {
                throw new Error('Delete returned false');
            }
        } catch (e) {
            console.error('Error deleting gateway:', e);
            window.ui.error('Failed to delete gateway: ' + e.message);
        }
    }

    async loadLiveSensors(chassisId) {
        const tbody = document.getElementById('live-sensors-tbody');
        if (!tbody) return;
        tbody.innerHTML = '<tr><td colspan="8" style="text-align:center; color:var(--text-muted); padding:1.5rem;">Loading live sensors...</td></tr>';

        try {
            const targetChassis = chassisId || document.getElementById('gateway-chassis-id').value.trim() || 'EdgeNode';
            const query = `
                query GetLiveSensors($chassisId: String) {
                    redfishLiveSensors(chassisId: $chassisId) {
                        id
                        name
                        chassisId
                        topic
                        reading
                        readingType
                        readingUnits
                        health
                        state
                        lastUpdated
                    }
                }
            `;
            const result = await window.graphqlClient.query(query, { chassisId: targetChassis });
            const sensors = result?.redfishLiveSensors || [];

            if (sensors.length === 0) {
                tbody.innerHTML = `
                    <tr>
                        <td colspan="8" style="text-align:center; color:var(--text-muted); padding:2rem;">
                            No sensor readings recorded yet. Once MQTT messages are published to matching topics, sensors will appear here.
                        </td>
                    </tr>
                `;
                return;
            }

            tbody.innerHTML = sensors.map(s => {
                const healthClass = s.health === 'OK' ? 'badge-enabled' : s.health === 'Warning' ? 'badge-warning' : 'badge-danger';
                return `
                    <tr>
                        <td><strong style="color:var(--monster-teal); font-family:monospace;">${this.escapeHtml(s.id)}</strong></td>
                        <td>${this.escapeHtml(s.name || s.id)}</td>
                        <td><span style="font-size:0.95rem; font-weight:600;">${s.reading}</span> <span class="text-muted">${this.escapeHtml(s.readingUnits || '')}</span></td>
                        <td><span class="badge badge-info" style="font-size:0.75rem;">${this.escapeHtml(s.readingType || 'Reading')}</span></td>
                        <td><span class="badge ${healthClass}" style="font-size:0.75rem;">${this.escapeHtml(s.health || 'OK')}</span></td>
                        <td><span style="font-size:0.8rem;">${this.escapeHtml(s.state || 'Enabled')}</span></td>
                        <td><code style="font-size:0.75rem;">${this.escapeHtml(s.topic || '')}</code></td>
                        <td><span class="text-muted" style="font-size:0.75rem;">${this.escapeHtml(s.lastUpdated || '-')}</span></td>
                    </tr>
                `;
            }).join('');
        } catch (e) {
            console.error('Error fetching live sensors:', e);
            tbody.innerHTML = `<tr><td colspan="8" style="text-align:center; color:var(--monster-red); padding:1rem;">Failed to load live sensors: ${this.escapeHtml(e.message)}</td></tr>`;
        }
    }

    // ================= Topic Filters UI =================

    clearTopicFilters() {
        const container = document.getElementById('topic-filters-container');
        if (container) container.innerHTML = '';
    }

    addTopicFilterRow(initialVal = '') {
        const container = document.getElementById('topic-filters-container');
        if (!container) return;

        const row = document.createElement('div');
        row.className = 'filter-row';
        row.innerHTML = `
            <input type="text" class="topic-filter-input" value="${this.escapeHtml(initialVal)}" placeholder="e.g. sensors/+/temperature, chassis/#">
            <button type="button" class="btn btn-danger btn-small" onclick="removeTopicFilterRow(this)" title="Remove topic filter">
                <ix-icon name="trashcan" size="14"></ix-icon>
            </button>
        `;
        container.appendChild(row);
    }

    getTopicFilters() {
        const inputs = document.querySelectorAll('.topic-filter-input');
        const list = [];
        inputs.forEach(input => {
            const val = input.value.trim();
            if (val && !list.includes(val)) list.push(val);
        });
        return list;
    }

    setupDropZone() {
        const dropZone = document.getElementById('topic-drop-zone');
        if (!dropZone) return;

        dropZone.addEventListener('dragover', (e) => {
            e.preventDefault();
            e.dataTransfer.dropEffect = 'copy';
            dropZone.classList.add('drag-over');
        });

        dropZone.addEventListener('dragleave', () => {
            dropZone.classList.remove('drag-over');
        });

        dropZone.addEventListener('drop', (e) => {
            e.preventDefault();
            dropZone.classList.remove('drag-over');
            const topic = e.dataTransfer.getData('text/plain');
            if (topic) {
                this.addTopicFilterRow(topic);
                window.ui.success(`Added topic "${topic}" to filters`);
            }
        });
    }

    // ================= Schema Templates =================

    applyTemplate(type) {
        let schema = {};
        if (type === 'single') {
            schema = {
                "$schema": "http://json-schema.org/draft-07/schema#",
                "sensorId": "sensor1",
                "name": "Main Temperature",
                "reading": "temp",
                "readingType": "Temperature",
                "readingUnits": "Cel"
            };
        } else if (type === 'nested') {
            schema = {
                "$schema": "http://json-schema.org/draft-07/schema#",
                "sensorId": "chassis_temp",
                "name": "telemetry.temperature.label",
                "reading": "telemetry.temperature.value",
                "readingType": "telemetry.temperature.type",
                "readingUnits": "telemetry.temperature.unit",
                "health": "telemetry.status.health"
            };
        } else if (type === 'array') {
            schema = {
                "$schema": "http://json-schema.org/draft-07/schema#",
                "sensors": "$[*]",
                "sensorId": "id",
                "name": "label",
                "reading": "value",
                "readingType": "type",
                "readingUnits": "unit"
            };
        } else if (type === 'map') {
            schema = {
                "$schema": "http://json-schema.org/draft-07/schema#",
                "sensors": "metrics",
                "sensorId": "$key",
                "reading": "$value"
            };
        }
        document.getElementById('gateway-schema').value = JSON.stringify(schema, null, 2);
    }

    formatJsonSchema() {
        const textarea = document.getElementById('gateway-schema');
        const text = textarea.value.trim();
        if (!text) return;
        try {
            const parsed = JSON.parse(text);
            textarea.value = JSON.stringify(parsed, null, 2);
            window.ui.success('JSON Schema formatted');
        } catch (e) {
            window.ui.error('Invalid JSON: ' + e.message);
        }
    }

    // ================= Topic Browser Side Panel & Layout Contraction =================

    setupResize() {
        if (!this.resizeHandle) return;

        let isResizing = false;
        let startX = 0;
        let startWidth = 0;

        this.resizeHandle.addEventListener('mousedown', (e) => {
            isResizing = true;
            startX = e.clientX;
            startWidth = this.panel.offsetWidth;
            document.body.style.cursor = 'col-resize';
            document.body.style.userSelect = 'none';
            e.preventDefault();
        });

        document.addEventListener('mousemove', (e) => {
            if (!isResizing) return;
            const deltaX = startX - e.clientX;
            const newWidth = Math.max(300, Math.min(700, startWidth + deltaX));
            this.panelWidth = newWidth;
            this.panel.style.width = `${newWidth}px`;
            if (this.isOpen) {
                this.updateMainContentMargin();
            }
        });

        document.addEventListener('mouseup', () => {
            if (isResizing) {
                isResizing = false;
                document.body.style.cursor = '';
                document.body.style.userSelect = '';
            }
        });
    }

    updateMainContentMargin() {
        const mainContent = document.querySelector('.main-content');
        if (mainContent) {
            mainContent.style.marginRight = this.isOpen ? `${this.panelWidth}px` : '0';
        }
    }

    toggleTopicBrowser() {
        if (this.isOpen) {
            this.closeTopicBrowser();
        } else {
            this.openTopicBrowser();
        }
    }

    openTopicBrowser() {
        if (!this.panel) return;
        this.panel.classList.add('open');
        this.isOpen = true;
        this.updateMainContentMargin();
        this.browseRoot();
    }

    closeTopicBrowser() {
        if (!this.panel) return;
        this.panel.classList.remove('open');
        this.isOpen = false;
        this.updateMainContentMargin();
    }

    async loadArchiveGroups() {
        const select = document.getElementById('side-archive-group-select');
        if (!select) return;
        try {
            const query = `query GetArchiveGroups { archiveGroups(enabled: true) { name } }`;
            const res = await window.graphqlClient.query(query);
            const groups = res?.archiveGroups || [];
            if (groups.length > 0) {
                select.innerHTML = groups.map(g => `<option value="${this.escapeHtml(g.name)}">${this.escapeHtml(g.name)}</option>`).join('');
                this.selectedArchiveGroup = groups[0].name;
            }
            select.addEventListener('change', () => {
                this.selectedArchiveGroup = select.value;
                this.browseRoot();
            });
        } catch (e) {
            console.warn('Could not load archive groups for topic browser:', e);
        }
    }

    browseRoot() {
        const tree = document.getElementById('side-topic-tree');
        if (!tree) return;
        tree.innerHTML = '';
        this.treeNodes.clear();

        const rootItem = this.createTreeItem('Broker Root', 'root', false, true);
        tree.appendChild(rootItem);

        const childContainer = document.createElement('ul');
        childContainer.className = 'tree-children';
        rootItem.appendChild(childContainer);

        this.loadTopicLevel('+', childContainer, '');
    }

    async loadTopicLevel(pattern, container, parentPath = '') {
        try {
            const loading = document.createElement('li');
            loading.className = 'tree-node';
            loading.innerHTML = '<span style="color:var(--text-muted); font-size:0.75rem; padding:0.25rem 0.5rem; display:block;">Loading...</span>';
            container.appendChild(loading);

            const query = `
                query BrowseTopics($topic: String!, $archiveGroup: String!) {
                    browseTopics(topic: $topic, archiveGroup: $archiveGroup) {
                        name
                    }
                }
            `;
            const res = await window.graphqlClient.query(query, {
                topic: pattern,
                archiveGroup: this.selectedArchiveGroup
            });

            container.removeChild(loading);

            const topics = res?.browseTopics || [];
            if (topics.length === 0) {
                const empty = document.createElement('li');
                empty.className = 'tree-node';
                empty.innerHTML = '<span style="color:var(--text-muted); font-size:0.75rem; padding:0.25rem 0.5rem; font-style:italic; display:block;">No topics found</span>';
                container.appendChild(empty);
                return;
            }

            const grouped = this.groupTopicsByLevel(topics.map(t => t.name), parentPath);
            grouped.forEach((data, levelName) => {
                const fullPath = parentPath ? `${parentPath}/${levelName}` : levelName;
                const nodeItem = this.createTreeItem(levelName, fullPath, data.hasValue, data.hasChildren);
                container.appendChild(nodeItem);
            });
        } catch (e) {
            console.error('Error loading topic level:', e);
        }
    }

    groupTopicsByLevel(topicNames, parentPath) {
        const map = new Map();
        const parentLevels = parentPath ? parentPath.split('/').length : 0;

        topicNames.forEach(name => {
            const parts = name.split('/');
            if (parts.length > parentLevels) {
                const nextLevel = parts[parentLevels];
                const hasChildren = parts.length > parentLevels + 1;
                const hasValue = parts.length === parentLevels + 1;

                if (!map.has(nextLevel)) {
                    map.set(nextLevel, { hasValue, hasChildren });
                } else {
                    const existing = map.get(nextLevel);
                    existing.hasValue = existing.hasValue || hasValue;
                    if (hasChildren) existing.hasChildren = true;
                }
            }
        });
        return map;
    }

    createTreeItem(name, fullPath, hasValue, hasChildren) {
        const li = document.createElement('li');
        li.className = 'tree-node';

        const item = document.createElement('div');
        item.className = 'tree-item';
        if (hasValue) item.classList.add('has-data');

        const toggle = document.createElement('button');
        toggle.className = 'tree-toggle';
        if (hasChildren) {
            toggle.innerHTML = '&#9654;';
        }

        const icon = document.createElement('ix-icon');
        icon.setAttribute('name', hasChildren ? 'folder' : 'link');
        icon.setAttribute('size', '12');
        icon.style.marginRight = '0.35rem';
        icon.style.color = hasChildren ? 'var(--monster-purple)' : 'var(--monster-teal)';

        const span = document.createElement('span');
        span.textContent = name;
        span.style.flex = '1';

        item.appendChild(toggle);
        item.appendChild(icon);
        item.appendChild(span);
        li.appendChild(item);

        // Interaction
        if (hasChildren) {
            let expanded = false;
            let childrenContainer = null;
            const toggleHandler = (e) => {
                e.stopPropagation();
                expanded = !expanded;
                if (expanded) {
                    toggle.classList.add('expanded');
                    if (!childrenContainer) {
                        childrenContainer = document.createElement('ul');
                        childrenContainer.className = 'tree-children';
                        li.appendChild(childrenContainer);
                        this.loadTopicLevel(`${fullPath}/+`, childrenContainer, fullPath);
                    } else {
                        childrenContainer.classList.remove('collapsed');
                    }
                } else {
                    toggle.classList.remove('expanded');
                    if (childrenContainer) childrenContainer.classList.add('collapsed');
                }
            };
            toggle.addEventListener('click', toggleHandler);
        }

        // Draggable and clickable
        item.draggable = true;
        item.addEventListener('dragstart', (e) => {
            e.dataTransfer.setData('text/plain', fullPath);
            e.dataTransfer.effectAllowed = 'copy';
        });

        item.addEventListener('dblclick', () => {
            this.addTopicFilterRow(fullPath);
            window.ui.success(`Added topic "${fullPath}" to filters`);
        });

        item.addEventListener('click', () => {
            this.previewTopicPayload(fullPath);
        });

        return li;
    }

    async previewTopicPayload(topic) {
        const panelData = document.getElementById('side-topic-data');
        const payloadText = document.getElementById('side-topic-payload');
        if (!panelData || !payloadText) return;

        panelData.style.display = 'block';
        payloadText.textContent = `Reading ${topic}...`;

        try {
            const query = `
                query GetCurrentValue($topic: String!, $archiveGroup: String!) {
                    currentValue(topic: $topic, archiveGroup: $archiveGroup) {
                        topic
                        payload
                        time
                    }
                }
            `;
            const res = await window.graphqlClient.query(query, {
                topic,
                archiveGroup: this.selectedArchiveGroup
            });
            const val = res?.currentValue;
            if (val && val.payload) {
                try {
                    const parsed = JSON.parse(val.payload);
                    payloadText.textContent = JSON.stringify(parsed, null, 2);
                } catch {
                    payloadText.textContent = val.payload;
                }
            } else {
                payloadText.textContent = '(No current value available for this topic)';
            }
        } catch (e) {
            payloadText.textContent = 'Error: ' + e.message;
        }
    }

    async searchTopicBrowser() {
        const input = document.getElementById('side-search-input');
        const queryPattern = input.value.trim();
        if (!queryPattern) {
            this.browseRoot();
            return;
        }

        const tree = document.getElementById('side-topic-tree');
        tree.innerHTML = '<span style="color:var(--text-muted); font-size:0.75rem; padding:0.5rem; display:block;">Searching...</span>';

        try {
            const query = `
                query SearchTopics($query: String!, $archiveGroup: String!) {
                    searchTopics(query: $query, archiveGroup: $archiveGroup) {
                        name
                    }
                }
            `;
            const res = await window.graphqlClient.query(query, {
                query: queryPattern,
                archiveGroup: this.selectedArchiveGroup
            });
            const topics = res?.searchTopics || [];

            tree.innerHTML = '';
            if (topics.length === 0) {
                tree.innerHTML = '<span style="color:var(--text-muted); font-size:0.75rem; padding:0.5rem; font-style:italic; display:block;">No matching topics found</span>';
                return;
            }

            topics.forEach(t => {
                const nodeItem = this.createTreeItem(t.name, t.name, true, false);
                tree.appendChild(nodeItem);
            });
        } catch (e) {
            tree.innerHTML = `<span style="color:var(--monster-red); font-size:0.75rem; padding:0.5rem; display:block;">Search error: ${this.escapeHtml(e.message)}</span>`;
        }
    }

    // ================= Utilities =================

    showLoading(show) {
        const el = document.getElementById('loading-indicator');
        if (el) el.style.display = show ? 'flex' : 'none';
    }

    showError(msg) {
        const el = document.getElementById('error-message');
        const txt = document.getElementById('error-text');
        if (txt) txt.textContent = msg;
        if (el) el.style.display = 'block';
    }

    hideError() {
        const el = document.getElementById('error-message');
        if (el) el.style.display = 'none';
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

// Global functions for inline HTML event handlers
function saveGateway() {
    if (RedfishDetailManager.instance) RedfishDetailManager.instance.saveGateway();
}
function deleteGateway() {
    if (RedfishDetailManager.instance) RedfishDetailManager.instance.deleteGateway();
}
function loadLiveSensors() {
    if (RedfishDetailManager.instance) RedfishDetailManager.instance.loadLiveSensors();
}
function addTopicFilterRow(val) {
    if (RedfishDetailManager.instance) RedfishDetailManager.instance.addTopicFilterRow(val);
}
function removeTopicFilterRow(btn) {
    const row = btn.closest('.filter-row');
    if (row) row.remove();
}
function applyTemplate(type) {
    if (RedfishDetailManager.instance) RedfishDetailManager.instance.applyTemplate(type);
}
function formatJsonSchema() {
    if (RedfishDetailManager.instance) RedfishDetailManager.instance.formatJsonSchema();
}
function toggleTopicBrowser() {
    if (RedfishDetailManager.instance) RedfishDetailManager.instance.toggleTopicBrowser();
}
function openTopicBrowser() {
    if (RedfishDetailManager.instance) RedfishDetailManager.instance.openTopicBrowser();
}
function closeTopicBrowser() {
    if (RedfishDetailManager.instance) RedfishDetailManager.instance.closeTopicBrowser();
}
function refreshTopicBrowser() {
    if (RedfishDetailManager.instance) RedfishDetailManager.instance.browseRoot();
}
function searchTopicBrowser() {
    if (RedfishDetailManager.instance) RedfishDetailManager.instance.searchTopicBrowser();
}

document.addEventListener('DOMContentLoaded', () => {
    new RedfishDetailManager();

    const searchInput = document.getElementById('side-search-input');
    if (searchInput) {
        searchInput.addEventListener('keypress', (e) => {
            if (e.key === 'Enter') searchTopicBrowser();
        });
    }
});
