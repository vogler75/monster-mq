// MonsterMQ Dashboard — Data Catalog Manager

class DataCatalogManager {
    constructor() {
        this.types = [];
        this.instances = [];
        this.relations = [];
        this.namespaces = new Set();
        this.currentTab = 'types';
        this.discoveryMode = 'ai';
        this.currentProposal = null;

        this.init();
    }

    async init() {
        if (!this.isLoggedIn()) {
            window.location.href = '/pages/login.html';
            return;
        }

        this.setupEventListeners();
        await this.loadArchiveGroups();
        await this.loadData();
    }

    isLoggedIn() {
        return window.isLoggedIn ? window.isLoggedIn() : true;
    }

    setupEventListeners() {
        window.addEventListener('click', (e) => {
            if (e.target.classList.contains('modal-overlay')) {
                e.target.style.display = 'none';
            }
        });
    }

    async loadArchiveGroups() {
        try {
            const res = await window.graphqlClient.query(`
                query GetArchiveGroups {
                    archiveGroups {
                        name
                    }
                }
            `);
            const groups = res.archiveGroups || [];
            const select = document.getElementById('discovery-archive-group');
            if (select && groups.length > 0) {
                select.innerHTML = groups.map(g => `<option value="${this.escapeHtml(g.name)}">${this.escapeHtml(g.name)}</option>`).join('');
            }
        } catch (e) {
            console.warn('Failed to load archive groups:', e);
        }
    }

    async loadData() {
        try {
            window.ui.setLoading(true);
            const res = await window.graphqlClient.query(`
                query GetDataCatalog {
                    dataCatalogTypes {
                        id
                        namespace
                        name
                        description
                        structure
                        topicPattern
                        createdAt
                        updatedAt
                    }
                    dataCatalogInstances {
                        id
                        typeId
                        name
                        baseTopic
                        properties
                        createdAt
                        updatedAt
                    }
                    dataCatalogRelations {
                        sourceId
                        targetId
                        relationType
                    }
                }
            `);

            this.types = res.dataCatalogTypes || [];
            this.instances = res.dataCatalogInstances || [];
            this.relations = res.dataCatalogRelations || [];

            this.namespaces = new Set(this.types.map(t => t.namespace).filter(Boolean));

            this.updateMetrics();
            this.renderTypes();
            this.renderInstances();
            this.renderRelations();
            this.populateInstanceTypeFilter();

            window.ui.setLoading(false);
        } catch (error) {
            console.error('Error loading Data Catalog:', error);
            window.ui.setLoading(false);
            window.ui.showError('Failed to load Data Catalog: ' + error.message);
        }
    }

    updateMetrics() {
        const setTxt = (id, val) => {
            const el = document.getElementById(id);
            if (el) el.textContent = val;
        };
        setTxt('metric-types-count', this.types.length);
        setTxt('metric-instances-count', this.instances.length);
        setTxt('metric-relations-count', this.relations.length);
        setTxt('metric-namespaces-count', this.namespaces.size || 1);

        setTxt('tab-badge-types', this.types.length);
        setTxt('tab-badge-instances', this.instances.length);
        setTxt('tab-badge-relations', this.relations.length);
    }

    switchTab(tabName) {
        this.currentTab = tabName;
        document.querySelectorAll('.catalog-tab-btn').forEach(btn => btn.classList.remove('active'));
        document.querySelectorAll('.catalog-tab-content').forEach(content => content.classList.remove('active'));

        const btn = document.getElementById(`tab-btn-${tabName}`);
        const content = document.getElementById(`tab-content-${tabName}`);
        if (btn) btn.classList.add('active');
        if (content) content.classList.add('active');
    }

    // ===================== Object Types =====================

    populateInstanceTypeFilter() {
        const select = document.getElementById('filter-instance-type');
        if (!select) return;
        const currentVal = select.value;
        select.innerHTML = '<option value="">All Types</option>' +
            this.types.map(t => `<option value="${this.escapeHtml(t.id)}">${this.escapeHtml(t.name || t.id)}</option>`).join('');
        select.value = currentVal;
    }

    renderTypes() {
        const tbody = document.getElementById('types-table-body');
        const filter = (document.getElementById('search-types')?.value || '').toLowerCase();

        const filtered = this.types.filter(t => 
            t.id.toLowerCase().includes(filter) ||
            t.name.toLowerCase().includes(filter) ||
            (t.namespace && t.namespace.toLowerCase().includes(filter)) ||
            (t.description && t.description.toLowerCase().includes(filter))
        );

        if (filtered.length === 0) {
            tbody.innerHTML = window.ui.emptyRow(6, 'No Object Types Found', 'Define object types manually or use Auto-Discover to create them from topic trees.');
            return;
        }

        tbody.innerHTML = filtered.map(t => {
            const instanceCount = this.instances.filter(i => i.typeId === t.id).length;
            return `
            <tr>
                <td>
                    <strong>${this.escapeHtml(t.name)}</strong>
                    <br><small style="color: var(--text-secondary); font-family: monospace;">${this.escapeHtml(t.id)}</small>
                </td>
                <td><span class="status-badge" style="background: rgba(139, 92, 246, 0.15); color: #8b5cf6;">${this.escapeHtml(t.namespace || 'default')}</span></td>
                <td><span class="topic-pattern-tag">${this.escapeHtml(t.topicPattern || '-')}</span></td>
                <td style="color: var(--text-secondary); max-width: 250px;">${this.escapeHtml(t.description || '-')}</td>
                <td>
                    <button class="btn btn-secondary btn-sm" onclick="catalogManager.viewSchema('${this.escapeHtml(t.id)}')">
                        <ix-icon name="document" size="14"></ix-icon> Schema
                    </button>
                    <span style="font-size: 0.8rem; color: var(--text-secondary); margin-left: 0.5rem;">${instanceCount} inst.</span>
                </td>
                <td>
                    <div class="action-buttons" style="display: flex; gap: 0.25rem;">
                        <a href="/pages/data-catalog-type-detail.html?id=${encodeURIComponent(t.id)}">
                            <ix-icon-button icon="highlight" variant="subtle-tertiary" size="24" title="Edit Type"></ix-icon-button>
                        </a>
                        <ix-icon-button icon="trashcan" variant="subtle-tertiary" size="24" class="btn-delete" title="Delete Type" onclick="catalogManager.deleteType('${this.escapeHtml(t.id)}')"></ix-icon-button>
                    </div>
                </td>
            </tr>`;
        }).join('');
    }

    filterTypes() {
        this.renderTypes();
    }

    viewSchema(typeId) {
        const type = this.types.find(t => t.id === typeId);
        if (!type) return;

        document.getElementById('schema-viewer-title').textContent = `JSON Schema — ${type.name || type.id}`;
        document.getElementById('schema-viewer-content').textContent = JSON.stringify(type.structure, null, 2);
        document.getElementById('schema-viewer-modal').style.display = 'flex';
    }

    async deleteType(id) {
        const confirmed = await window.ui.showConfirm({
            title: 'Delete Object Type',
            message: `Are you sure you want to delete object type "${id}"? This will delete associated metadata in the catalog.`,
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
            `, { id });

            window.ui.success(`Object type "${id}" deleted.`);
            await this.loadData();
        } catch (error) {
            window.ui.error('Failed to delete object type: ' + error.message);
        }
    }

    // ===================== Object Instances =====================

    renderInstances() {
        const tbody = document.getElementById('instances-table-body');
        const filter = (document.getElementById('search-instances')?.value || '').toLowerCase();
        const typeFilter = document.getElementById('filter-instance-type')?.value || '';

        const filtered = this.instances.filter(inst => {
            const matchesType = !typeFilter || inst.typeId === typeFilter;
            const matchesSearch = inst.id.toLowerCase().includes(filter) ||
                inst.name.toLowerCase().includes(filter) ||
                inst.baseTopic.toLowerCase().includes(filter);
            return matchesType && matchesSearch;
        });

        if (filtered.length === 0) {
            tbody.innerHTML = window.ui.emptyRow(5, 'No Object Instances Found', 'Add object instances or discover them from the topic tree.');
            return;
        }

        tbody.innerHTML = filtered.map(inst => {
            const typeObj = this.types.find(t => t.id === inst.typeId);
            const typeName = typeObj ? typeObj.name : inst.typeId;
            const propCount = Object.keys(inst.properties || {}).length;

            return `
            <tr>
                <td>
                    <strong>${this.escapeHtml(inst.name)}</strong>
                    <br><small style="color: var(--text-secondary); font-family: monospace;">${this.escapeHtml(inst.id)}</small>
                </td>
                <td><span class="status-badge" style="background: rgba(59, 130, 246, 0.15); color: #3b82f6;">${this.escapeHtml(typeName)}</span></td>
                <td><span class="topic-pattern-tag">${this.escapeHtml(inst.baseTopic)}</span></td>
                <td><small style="color: var(--text-secondary);">${propCount} custom props</small></td>
                <td>
                    <div class="action-buttons" style="display: flex; gap: 0.25rem;">
                        <a href="/pages/topic-browser.html?topic=${encodeURIComponent(inst.baseTopic)}" title="View Topic in Browser">
                            <ix-icon-button icon="search" variant="subtle-tertiary" size="24"></ix-icon-button>
                        </a>
                        <a href="/pages/data-catalog-instance-detail.html?id=${encodeURIComponent(inst.id)}" title="Edit Instance">
                            <ix-icon-button icon="highlight" variant="subtle-tertiary" size="24"></ix-icon-button>
                        </a>
                        <ix-icon-button icon="trashcan" variant="subtle-tertiary" size="24" class="btn-delete" title="Delete Instance" onclick="catalogManager.deleteInstance('${this.escapeHtml(inst.id)}')"></ix-icon-button>
                    </div>
                </td>
            </tr>`;
        }).join('');
    }

    filterInstances() {
        this.renderInstances();
    }

    async deleteInstance(id) {
        const confirmed = await window.ui.showConfirm({
            title: 'Delete Object Instance',
            message: `Are you sure you want to delete instance "${id}"?`,
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
            `, { id });

            window.ui.success(`Instance "${id}" deleted.`);
            await this.loadData();
        } catch (error) {
            window.ui.error('Failed to delete instance: ' + error.message);
        }
    }

    // ===================== Relations =====================

    renderRelations() {
        const tbody = document.getElementById('relations-table-body');
        const filter = (document.getElementById('search-relations')?.value || '').toLowerCase();

        const filtered = this.relations.filter(r =>
            r.sourceId.toLowerCase().includes(filter) ||
            r.targetId.toLowerCase().includes(filter) ||
            r.relationType.toLowerCase().includes(filter)
        );

        if (filtered.length === 0) {
            tbody.innerHTML = window.ui.emptyRow(4, 'No Relationships Defined', 'Create relationships to define hierarchies, dependencies, and connections.');
            return;
        }

        tbody.innerHTML = filtered.map(r => `
            <tr>
                <td><strong>${this.escapeHtml(r.sourceId)}</strong></td>
                <td><span class="relation-badge">${this.escapeHtml(r.relationType)}</span></td>
                <td><strong>${this.escapeHtml(r.targetId)}</strong></td>
                <td>
                    <ix-icon-button icon="trashcan" variant="subtle-tertiary" size="24" class="btn-delete" title="Delete Relation" onclick="catalogManager.deleteRelation('${this.escapeHtml(r.sourceId)}', '${this.escapeHtml(r.targetId)}', '${this.escapeHtml(r.relationType)}')"></ix-icon-button>
                </td>
            </tr>
        `).join('');
    }

    filterRelations() {
        this.renderRelations();
    }

    openAddRelationModal() {
        document.getElementById('rel-source-id').value = '';
        document.getElementById('rel-target-id').value = '';
        document.getElementById('rel-type-select').value = 'HasComponent';
        document.getElementById('rel-type-custom').style.display = 'none';
        document.getElementById('add-relation-modal').style.display = 'flex';
    }

    closeAddRelationModal() {
        document.getElementById('add-relation-modal').style.display = 'none';
    }

    onRelationTypeChange() {
        const select = document.getElementById('rel-type-select');
        const customInput = document.getElementById('rel-type-custom');
        customInput.style.display = select.value === 'custom' ? 'block' : 'none';
    }

    async saveNewRelation() {
        const sourceId = document.getElementById('rel-source-id').value.trim();
        const targetId = document.getElementById('rel-target-id').value.trim();
        const selectVal = document.getElementById('rel-type-select').value;
        const relationType = selectVal === 'custom'
            ? document.getElementById('rel-type-custom').value.trim()
            : selectVal;

        if (!sourceId || !targetId || !relationType) {
            window.ui.error('Please enter source ID, target ID, and relationship type.');
            return;
        }

        try {
            await window.graphqlClient.query(`
                mutation SaveRelation($input: DataCatalogRelationInput!) {
                    dataCatalog {
                        saveRelation(input: $input) {
                            sourceId
                            targetId
                            relationType
                        }
                    }
                }
            `, { input: { sourceId, targetId, relationType } });

            window.ui.success('Relationship created.');
            this.closeAddRelationModal();
            await this.loadData();
        } catch (error) {
            window.ui.error('Failed to create relationship: ' + error.message);
        }
    }

    async deleteRelation(sourceId, targetId, relationType) {
        const confirmed = await window.ui.showConfirm({
            title: 'Delete Relationship',
            message: `Remove relationship "${sourceId} --[${relationType}]--> ${targetId}"?`,
            confirmText: 'Delete',
            type: 'danger'
        });

        if (!confirmed) return;

        try {
            await window.graphqlClient.query(`
                mutation DeleteRelation($sourceId: String!, $targetId: String!, $relationType: String!) {
                    dataCatalog {
                        deleteRelation(sourceId: $sourceId, targetId: $targetId, relationType: $relationType)
                    }
                }
            `, { sourceId, targetId, relationType });

            window.ui.success('Relationship deleted.');
            await this.loadData();
        } catch (error) {
            window.ui.error('Failed to delete relationship: ' + error.message);
        }
    }

    // ===================== Export / Import =====================

    openExportImportModal() {
        document.getElementById('import-json-text').value = '';
        document.getElementById('export-import-modal').style.display = 'flex';
    }

    closeExportImportModal() {
        document.getElementById('export-import-modal').style.display = 'none';
    }

    async exportCatalogJson() {
        try {
            const res = await window.graphqlClient.query(`
                mutation ExportCatalog {
                    dataCatalog {
                        exportCatalog
                    }
                }
            `);

            const data = res.dataCatalog?.exportCatalog || {};
            const dataStr = "data:text/json;charset=utf-8," + encodeURIComponent(JSON.stringify(data, null, 2));
            const downloadAnchor = document.createElement('a');
            downloadAnchor.setAttribute("href", dataStr);
            downloadAnchor.setAttribute("download", `datacatalog-export-${Date.now()}.json`);
            document.body.appendChild(downloadAnchor);
            downloadAnchor.click();
            downloadAnchor.remove();

            window.ui.success('Catalog exported successfully.');
        } catch (error) {
            window.ui.error('Failed to export catalog: ' + error.message);
        }
    }

    handleImportFileUpload(event) {
        const file = event.target.files[0];
        if (!file) return;

        const reader = new FileReader();
        reader.onload = (e) => {
            document.getElementById('import-json-text').value = e.target.result;
        };
        reader.readAsText(file);
    }

    async importCatalogJson() {
        const jsonText = document.getElementById('import-json-text').value.trim();
        if (!jsonText) {
            window.ui.error('Please provide JSON catalog content to import.');
            return;
        }

        try {
            const data = JSON.parse(jsonText);
            const res = await window.graphqlClient.query(`
                mutation ImportCatalog($data: JSON!) {
                    dataCatalog {
                        importCatalog(data: $data) {
                            success
                            typesImported
                            instancesImported
                            relationsImported
                            failed
                            errors
                        }
                    }
                }
            `, { data });

            const result = res.dataCatalog?.importCatalog;
            if (result && result.success) {
                window.ui.success(`Import complete! Imported ${result.typesImported} types, ${result.instancesImported} instances, ${result.relationsImported} relations.`);
                this.closeExportImportModal();
                await this.loadData();
            } else {
                window.ui.error(`Import failed: ${result?.errors?.join(', ') || 'Unknown error'}`);
            }
        } catch (e) {
            window.ui.error('Invalid JSON: ' + e.message);
        }
    }

    // ===================== Auto-Discovery & AI Wizard =====================

    openDiscoveryModal() {
        const resultsEl = document.getElementById('discovery-results');
        if (resultsEl) resultsEl.style.display = 'none';
        const applyBtn = document.getElementById('btn-apply-discovery');
        if (applyBtn) applyBtn.style.display = 'none';
        const modal = document.getElementById('discovery-modal');
        if (modal) modal.style.display = 'flex';
    }

    closeDiscoveryModal() {
        const modal = document.getElementById('discovery-modal');
        if (modal) modal.style.display = 'none';
    }

    selectDiscoveryMode(mode) {
        this.discoveryMode = mode;
        const aiCard = document.getElementById('mode-card-ai');
        if (aiCard) aiCard.classList.toggle('selected', mode === 'ai');
        const heurCard = document.getElementById('mode-card-heuristic');
        if (heurCard) heurCard.classList.toggle('selected', mode === 'heuristic');
        const promptGroup = document.getElementById('ai-prompt-group');
        if (promptGroup) promptGroup.style.display = mode === 'ai' ? 'block' : 'none';
    }

    async runDiscovery() {
        const pattern = document.getElementById('discovery-topic-pattern')?.value?.trim() || '#';
        const archiveGroup = document.getElementById('discovery-archive-group')?.value || 'Default';
        const prompt = document.getElementById('discovery-ai-prompt')?.value?.trim() || '';

        const btn = document.getElementById('btn-run-discovery');
        if (btn) {
            btn.disabled = true;
            btn.innerHTML = '<ix-spinner size="16"></ix-spinner> Analyzing Topics...';
        }

        try {
            let proposal = null;

            if (this.discoveryMode === 'ai') {
                const res = await window.graphqlClient.query(`
                    query ProposeWithAi($topicPattern: String!, $archiveGroup: String, $prompt: String) {
                        genai {
                            proposeDataCatalog(topicPattern: $topicPattern, archiveGroup: $archiveGroup, prompt: $prompt) {
                                types {
                                    id
                                    namespace
                                    name
                                    description
                                    structure
                                    topicPattern
                                }
                                instances {
                                    id
                                    typeId
                                    name
                                    baseTopic
                                    properties
                                }
                                relations {
                                    sourceId
                                    targetId
                                    relationType
                                }
                                topicsAnalyzed
                                summary
                                error
                            }
                        }
                    }
                `, { topicPattern: pattern, archiveGroup, prompt });

                proposal = res.genai?.proposeDataCatalog;
            } else {
                const res = await window.graphqlClient.query(`
                    query InferHeuristic($topicPattern: String!, $archiveGroup: String) {
                        inferDataCatalog(topicPattern: $topicPattern, archiveGroup: $archiveGroup) {
                            types {
                                id
                                namespace
                                name
                                description
                                structure
                                topicPattern
                            }
                            instances {
                                id
                                typeId
                                name
                                baseTopic
                                properties
                            }
                            relations {
                                sourceId
                                targetId
                                relationType
                            }
                            topicsAnalyzed
                            summary
                            error
                        }
                    }
                `, { topicPattern: pattern, archiveGroup });

                proposal = res.inferDataCatalog;
            }

            if (!proposal || proposal.error) {
                window.ui.error(proposal?.error || 'Discovery failed. Check broker logs.');
                btn.disabled = false;
                btn.innerHTML = '<ix-icon name="search" size="16"></ix-icon> Analyze Topic Tree';
                return;
            }

            this.currentProposal = proposal;
            this.renderDiscoveryProposal(proposal);

            btn.disabled = false;
            btn.innerHTML = '<ix-icon name="search" size="16"></ix-icon> Re-Analyze';
        } catch (error) {
            console.error('Discovery error:', error);
            window.ui.error('Discovery failed: ' + error.message);
            btn.disabled = false;
            btn.innerHTML = '<ix-icon name="search" size="16"></ix-icon> Analyze Topic Tree';
        }
    }

    renderDiscoveryProposal(proposal) {
        document.getElementById('discovery-summary-badge').textContent = `${proposal.topicsAnalyzed} topics analyzed`;
        document.getElementById('proposal-types-count').textContent = proposal.types.length;
        document.getElementById('proposal-instances-count').textContent = proposal.instances.length;
        document.getElementById('proposal-relations-count').textContent = proposal.relations.length;

        // Render Types
        const typesList = document.getElementById('proposal-types-list');
        if (proposal.types.length === 0) {
            typesList.innerHTML = '<p style="color: var(--text-secondary); font-size: 0.85rem;">No object types proposed.</p>';
        } else {
            typesList.innerHTML = proposal.types.map((t, idx) => `
                <div class="proposal-card">
                    <div class="proposal-card-header">
                        <label style="display: flex; align-items: center; gap: 0.5rem; cursor: pointer;">
                            <input type="checkbox" id="prop-type-cb-${idx}" checked>
                            <strong>${this.escapeHtml(t.name)}</strong> (<code>${this.escapeHtml(t.id)}</code>)
                        </label>
                        <span class="topic-pattern-tag">${this.escapeHtml(t.topicPattern || '-')}</span>
                    </div>
                    <p style="margin: 0 0 0.5rem 0; font-size: 0.8rem; color: var(--text-secondary);">${this.escapeHtml(t.description || '')}</p>
                    <details style="font-size: 0.8rem;">
                        <summary style="cursor: pointer; color: var(--primary);">View Inferred JSON Schema</summary>
                        <pre style="background: var(--dark-bg); padding: 0.5rem; border-radius: 4px; overflow-x: auto; margin-top: 0.25rem;">${JSON.stringify(t.structure, null, 2)}</pre>
                    </details>
                </div>
            `).join('');
        }

        // Render Instances
        const instancesList = document.getElementById('proposal-instances-list');
        if (proposal.instances.length === 0) {
            instancesList.innerHTML = '<p style="color: var(--text-secondary); font-size: 0.85rem;">No instances proposed.</p>';
        } else {
            instancesList.innerHTML = proposal.instances.map((i, idx) => `
                <div class="proposal-card">
                    <div class="proposal-card-header">
                        <label style="display: flex; align-items: center; gap: 0.5rem; cursor: pointer;">
                            <input type="checkbox" id="prop-inst-cb-${idx}" checked>
                            <strong>${this.escapeHtml(i.name)}</strong> (Type: <code>${this.escapeHtml(i.typeId)}</code>)
                        </label>
                        <span class="topic-pattern-tag">${this.escapeHtml(i.baseTopic)}</span>
                    </div>
                </div>
            `).join('');
        }

        // Render Relations
        const relationsList = document.getElementById('proposal-relations-list');
        if (proposal.relations.length === 0) {
            relationsList.innerHTML = '<p style="color: var(--text-secondary); font-size: 0.85rem;">No relations proposed.</p>';
        } else {
            relationsList.innerHTML = proposal.relations.map((r, idx) => `
                <div class="proposal-card" style="padding: 0.5rem 1rem;">
                    <label style="display: flex; align-items: center; gap: 0.5rem; cursor: pointer;">
                        <input type="checkbox" id="prop-rel-cb-${idx}" checked>
                        <code>${this.escapeHtml(r.sourceId)}</code>
                        <span class="relation-badge">${this.escapeHtml(r.relationType)}</span>
                        <code>${this.escapeHtml(r.targetId)}</code>
                    </label>
                </div>
            `).join('');
        }

        document.getElementById('discovery-results').style.display = 'block';
        document.getElementById('btn-apply-discovery').style.display = 'inline-flex';
    }

    async applyDiscoveryProposal() {
        if (!this.currentProposal) return;

        const selectedTypes = this.currentProposal.types.filter((_, idx) => {
            const cb = document.getElementById(`prop-type-cb-${idx}`);
            return cb && cb.checked;
        });

        const selectedInstances = this.currentProposal.instances.filter((_, idx) => {
            const cb = document.getElementById(`prop-inst-cb-${idx}`);
            return cb && cb.checked;
        });

        const selectedRelations = this.currentProposal.relations.filter((_, idx) => {
            const cb = document.getElementById(`prop-rel-cb-${idx}`);
            return cb && cb.checked;
        });

        if (selectedTypes.length === 0 && selectedInstances.length === 0 && selectedRelations.length === 0) {
            window.ui.error('Please select at least one item to apply to the catalog.');
            return;
        }

        const btn = document.getElementById('btn-apply-discovery');
        btn.disabled = true;
        btn.innerHTML = '<ix-spinner size="16"></ix-spinner> Applying to Catalog...';

        try {
            // 1. Save Types
            for (const type of selectedTypes) {
                await window.graphqlClient.query(`
                    mutation SaveType($input: DataCatalogTypeInput!) {
                        dataCatalog {
                            saveType(input: $input) {
                                id
                            }
                        }
                    }
                `, { input: type });
            }

            // 2. Save Instances
            for (const inst of selectedInstances) {
                await window.graphqlClient.query(`
                    mutation SaveInstance($input: DataCatalogInstanceInput!) {
                        dataCatalog {
                            saveInstance(input: $input) {
                                id
                            }
                        }
                    }
                `, { input: inst });
            }

            // 3. Save Relations
            for (const rel of selectedRelations) {
                await window.graphqlClient.query(`
                    mutation SaveRelation($input: DataCatalogRelationInput!) {
                        dataCatalog {
                            saveRelation(input: $input) {
                                sourceId
                            }
                        }
                    }
                `, { input: rel });
            }

            window.ui.success(`Catalog updated! Created ${selectedTypes.length} types, ${selectedInstances.length} instances, ${selectedRelations.length} relations.`);
            this.closeDiscoveryModal();
            await this.loadData();
        } catch (error) {
            console.error('Error applying proposal:', error);
            window.ui.error('Failed to apply proposal: ' + error.message);
        } finally {
            btn.disabled = false;
            btn.innerHTML = '<ix-icon name="check" size="16"></ix-icon> Apply Selected to Catalog';
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

// Instantiate and attach globally
const catalogManager = new DataCatalogManager();
window.catalogManager = catalogManager;
