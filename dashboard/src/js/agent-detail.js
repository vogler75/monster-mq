// AI Agent Detail Management JavaScript

class AgentDetailManager {
    constructor() {
        this.client = new GraphQLDashboardClient();
        this.agentName = null;
        this.agentData = null;
        this.clusterNodes = [];
        this.availableMcpServers = [];
        this.availableAgents = [];
        this.init();
    }

    async init() {
        const urlParams = new URLSearchParams(window.location.search);
        this.agentName = urlParams.get('agent');
        this.isNew = urlParams.get('new') === 'true';

        if (this.isNew) {
            await this.loadClusterNodes();
            await this.loadMcpServers();
            await this.loadAgents();
            this.showNewAgentForm();
            return;
        }

        if (!this.agentName) {
            this.showError('No agent specified in URL. Please select an agent from the list.');
            document.getElementById('page-title').textContent = 'Error';
            document.getElementById('page-subtitle').textContent = 'Invalid Request';
            return;
        }

        this.showLoading(true);
        try {
            await this.loadClusterNodes();
            await this.loadMcpServers();
            await this.loadAgents();
            await this.loadAgentData();
        } catch (error) {
            this.showError('Failed to load agent data: ' + error.message);
            document.getElementById('page-title').textContent = 'Error Loading Agent';
            document.getElementById('page-subtitle').textContent = this.agentName;
        } finally {
            this.showLoading(false);
        }
    }

    showNewAgentForm() {
        document.getElementById('page-title').textContent = 'Add AI Agent';
        document.getElementById('page-subtitle').textContent = 'Create a new AI agent';

        // Set defaults
        document.getElementById('agent-name').value = '';
        document.getElementById('agent-name').disabled = false;
        document.getElementById('agent-namespace').value = '';
        document.getElementById('agent-org').value = 'default';
        document.getElementById('agent-site').value = 'default';
        document.getElementById('agent-version').value = '1.0.0';
        document.getElementById('agent-node').value = '*';
        document.getElementById('agent-description').value = '';
        document.getElementById('agent-enabled').checked = true;
        document.getElementById('agent-provider').value = 'gemini';
        document.getElementById('agent-model').value = '';
        document.getElementById('agent-api-key').value = '';
        document.getElementById('agent-temperature').value = '0.7';
        document.getElementById('agent-max-tokens').value = '';
        document.getElementById('agent-max-tool-iterations').value = '10';
        document.getElementById('agent-memory-window-size').value = '20';
        document.getElementById('agent-trigger-type').value = 'MQTT';
        document.getElementById('agent-schedule-mode').value = 'interval';
        document.getElementById('agent-interval-value').value = '';
        document.getElementById('agent-interval-unit').value = 'minutes';
        document.getElementById('agent-daily-time').value = '22:00:00';
        document.getElementById('agent-cron-expression').value = '';
        document.getElementById('agent-cron-prompt').value = '';
        document.getElementById('agent-input-topics').value = '';
        document.getElementById('agent-output-topics').value = '';
        document.getElementById('agent-system-prompt').value = '';
        document.getElementById('agent-task-timeout-ms').value = '60000';
        document.getElementById('agent-context-lastval-topics').value = '';
        document.getElementById('agent-context-retained').value = '';

        // Update save button label
        const saveBtn = document.getElementById('save-agent-btn');
        if (saveBtn) saveBtn.innerHTML = saveBtn.innerHTML.replace('Save Agent', 'Create Agent');

        // Hide delete button for new agents
        const deleteBtn = document.getElementById('delete-btn');
        if (deleteBtn) deleteBtn.style.display = 'none';

        // Hide timestamps section for new agents
        document.getElementById('timestamps-section').style.display = 'none';

        // Show all other sections
        document.getElementById('agent-content').style.display = 'block';
        document.getElementById('provider-section').style.display = 'block';
        document.getElementById('mcp-section').style.display = 'block';
        document.getElementById('trigger-section').style.display = 'block';
        document.getElementById('context-section').style.display = 'block';
        document.getElementById('prompt-section').style.display = 'block';

        // Update model placeholder
        updateModelPlaceholder();
        // Toggle trigger fields
        toggleTriggerFields();
    }

    async loadClusterNodes() {
        try {
            const query = `query GetBrokers { brokers { nodeId isCurrent } }`;
            const result = await this.client.query(query);
            this.clusterNodes = result.brokers || [];

            const nodeSelect = document.getElementById('agent-node');
            if (nodeSelect) {
                nodeSelect.innerHTML = '<option value="*">* (Any Node)</option>';
                this.clusterNodes.forEach(node => {
                    const option = document.createElement('option');
                    option.value = node.nodeId;
                    option.textContent = node.nodeId + (node.isCurrent ? ' (Current)' : '');
                    nodeSelect.appendChild(option);
                });
            }
        } catch (error) {
            console.error('Error loading cluster nodes:', error);
        }
    }

    async loadMcpServers() {
        try {
            const query = `query { mcpServers { name url enabled } }`;
            const result = await this.client.query(query);
            this.availableMcpServers = result.mcpServers || [];
            this.renderMcpServerCheckboxes();
        } catch (error) {
            console.error('Error loading MCP servers:', error);
        }
    }

    renderMcpServerCheckboxes(selectedServers = []) {
        const container = document.getElementById('mcp-server-list');
        if (!container) return;

        if (this.availableMcpServers.length === 0) {
            container.innerHTML = '<div style="color: var(--text-muted);">No MCP servers configured. <a href="/pages/mcp-server-detail.html?new=true" style="color: var(--monster-teal);">Add one</a></div>';
            return;
        }

        container.innerHTML = this.availableMcpServers.map(server => {
            const checked = selectedServers.includes(server.name) ? 'checked' : '';
            const statusColor = server.enabled ? 'var(--monster-green)' : 'var(--text-muted)';
            return `
                <label style="display: flex; align-items: center; gap: 0.75rem; padding: 0.75rem; background: var(--dark-bg); border-radius: 8px; border: 1px solid var(--dark-border); cursor: pointer;">
                    <input type="checkbox" class="mcp-server-checkbox" value="${server.name}" ${checked} style="accent-color: var(--monster-purple);">
                    <div>
                        <div style="color: var(--text-primary); font-weight: 500;">${server.name}</div>
                        <div style="color: var(--text-muted); font-size: 0.8rem;">${server.url}</div>
                    </div>
                    <span style="margin-left: auto; width: 8px; height: 8px; border-radius: 50%; background: ${statusColor};"></span>
                </label>
            `;
        }).join('');
    }

    async loadAgents() {
        try {
            const query = `query { agents { name description enabled } }`;
            const result = await this.client.query(query);
            this.availableAgents = result.agents || [];
            this.renderSubAgentCheckboxes();
        } catch (error) {
            console.error('Error loading agents:', error);
        }
    }

    renderSubAgentCheckboxes(selectedAgents = []) {
        const container = document.getElementById('sub-agent-list');
        if (!container) return;

        // Filter out the current agent being edited
        const otherAgents = this.availableAgents.filter(a => a.name !== this.agentName);

        if (otherAgents.length === 0) {
            container.innerHTML = '<div style="color: var(--text-muted);">No other agents available.</div>';
            return;
        }

        container.innerHTML = otherAgents.map(agent => {
            const checked = selectedAgents.includes(agent.name) ? 'checked' : '';
            const statusColor = agent.enabled ? 'var(--monster-green)' : 'var(--text-muted)';
            return `
                <label style="display: flex; align-items: center; gap: 0.75rem; padding: 0.75rem; background: var(--dark-bg); border-radius: 8px; border: 1px solid var(--dark-border); cursor: pointer;">
                    <input type="checkbox" class="sub-agent-checkbox" value="${agent.name}" ${checked} style="accent-color: var(--monster-purple);">
                    <div>
                        <div style="color: var(--text-primary); font-weight: 500;">${agent.name}</div>
                        <div style="color: var(--text-muted); font-size: 0.8rem;">${agent.description || ''}</div>
                    </div>
                    <span style="margin-left: auto; width: 8px; height: 8px; border-radius: 50%; background: ${statusColor};"></span>
                </label>
            `;
        }).join('');
    }

    getSelectedSubAgents() {
        return Array.from(document.querySelectorAll('.sub-agent-checkbox:checked')).map(cb => cb.value);
    }

    parseContextLastvalTopics() {
        const raw = document.getElementById('agent-context-lastval-topics').value.trim();
        if (!raw) return null;
        try {
            return JSON.parse(raw);
        } catch (e) {
            this.showError('Invalid JSON in Lastval Message Topics: ' + e.message);
            return null;
        }
    }

    // --- History Query Management ---

    renderHistoryQueries(queries) {
        const container = document.getElementById('history-queries-list');
        container.innerHTML = '';
        (queries || []).forEach((q, idx) => this.addHistoryQueryCard(q, idx));
    }

    addHistoryQueryCard(query, idx) {
        const container = document.getElementById('history-queries-list');
        const card = document.createElement('div');
        card.className = 'history-query-card';
        card.dataset.index = idx !== undefined ? idx : container.children.length;

        const isRaw = !query || query.interval === 'RAW';

        card.innerHTML = `
            <div class="history-query-header">
                <span>Query #${parseInt(card.dataset.index) + 1}</span>
                <button type="button" class="history-query-remove" title="Remove">&times;</button>
            </div>
            <div class="history-query-grid">
                <div class="full">
                    <label>Topics (one per line)</label>
                    <textarea class="hq-topics" rows="2" data-drop-zone-enabled="true" placeholder="sensor/temperature&#10;sensor/humidity">${(query?.topics || []).join('\n')}</textarea>
                </div>
                <div>
                    <label>Archive Group</label>
                    <input type="text" class="hq-archive-group" value="${query?.archiveGroup || 'Default'}" placeholder="Default">
                </div>
                <div>
                    <label>Last Seconds</label>
                    <input type="number" class="hq-last-seconds" value="${query?.lastSeconds || 3600}" min="1">
                </div>
                <div>
                    <label>Interval</label>
                    <select class="hq-interval">
                        <option value="RAW" ${isRaw ? 'selected' : ''}>RAW</option>
                        <option value="ONE_MINUTE" ${query?.interval === 'ONE_MINUTE' ? 'selected' : ''}>1 Minute</option>
                        <option value="FIVE_MINUTES" ${query?.interval === 'FIVE_MINUTES' ? 'selected' : ''}>5 Minutes</option>
                        <option value="FIFTEEN_MINUTES" ${query?.interval === 'FIFTEEN_MINUTES' ? 'selected' : ''}>15 Minutes</option>
                        <option value="ONE_HOUR" ${query?.interval === 'ONE_HOUR' ? 'selected' : ''}>1 Hour</option>
                        <option value="ONE_DAY" ${query?.interval === 'ONE_DAY' ? 'selected' : ''}>1 Day</option>
                    </select>
                </div>
                <div>
                    <label>Aggregation</label>
                    <select class="hq-function" ${isRaw ? 'disabled' : ''}>
                        <option value="AVG" ${query?.function === 'AVG' || !query?.function ? 'selected' : ''}>AVG</option>
                        <option value="MIN" ${query?.function === 'MIN' ? 'selected' : ''}>MIN</option>
                        <option value="MAX" ${query?.function === 'MAX' ? 'selected' : ''}>MAX</option>
                    </select>
                </div>
                <div class="full">
                    <label>JSON Fields (optional, comma-separated)</label>
                    <input type="text" class="hq-fields" value="${(query?.fields || []).join(', ')}" placeholder="e.g. temperature, pressure" ${isRaw ? 'disabled' : ''}>
                </div>
            </div>
        `;

        // Wire up remove button
        card.querySelector('.history-query-remove').addEventListener('click', () => {
            card.remove();
            this.renumberHistoryQueries();
        });

        // Toggle aggregation function enabled/disabled based on interval
        const intervalSelect = card.querySelector('.hq-interval');
        const functionSelect = card.querySelector('.hq-function');
        const fieldsInput = card.querySelector('.hq-fields');
        intervalSelect.addEventListener('change', () => {
            const raw = intervalSelect.value === 'RAW';
            functionSelect.disabled = raw;
            fieldsInput.disabled = raw;
        });

        // Set up drop zone for topics textarea (append behavior)
        const topicsTextarea = card.querySelector('.hq-topics');
        topicsTextarea.classList.add('drop-zone');
        topicsTextarea.addEventListener('dragover', (e) => { e.preventDefault(); e.dataTransfer.dropEffect = 'copy'; topicsTextarea.classList.add('drag-over'); });
        topicsTextarea.addEventListener('dragenter', (e) => { e.preventDefault(); topicsTextarea.classList.add('drag-over'); });
        topicsTextarea.addEventListener('dragleave', (e) => { e.preventDefault(); if (!topicsTextarea.contains(e.relatedTarget)) topicsTextarea.classList.remove('drag-over'); });
        topicsTextarea.addEventListener('drop', (e) => {
            e.preventDefault();
            e.stopImmediatePropagation();
            topicsTextarea.classList.remove('drag-over');
            const topic = e.dataTransfer.getData('text/plain');
            if (topic) {
                const lines = topicsTextarea.value.trim() ? topicsTextarea.value.trim().split('\n').map(l => l.trim()).filter(l => l) : [];
                if (!lines.includes(topic)) lines.push(topic);
                topicsTextarea.value = lines.join('\n');
                topicsTextarea.style.backgroundColor = 'rgba(34, 197, 94, 0.1)';
                setTimeout(() => { topicsTextarea.style.backgroundColor = ''; }, 500);
            }
        });

        container.appendChild(card);
    }

    renumberHistoryQueries() {
        document.querySelectorAll('.history-query-card').forEach((card, i) => {
            card.dataset.index = i;
            card.querySelector('.history-query-header span').textContent = `Query #${i + 1}`;
        });
    }

    collectHistoryQueries() {
        const cards = document.querySelectorAll('.history-query-card');
        const queries = [];
        cards.forEach(card => {
            const topics = card.querySelector('.hq-topics').value.split('\n').map(t => t.trim()).filter(t => t);
            if (topics.length === 0) return;
            const fields = card.querySelector('.hq-fields').value.split(',').map(f => f.trim()).filter(f => f);
            queries.push({
                archiveGroup: card.querySelector('.hq-archive-group').value.trim() || 'Default',
                topics: topics,
                lastSeconds: parseInt(card.querySelector('.hq-last-seconds').value) || 3600,
                interval: card.querySelector('.hq-interval').value,
                function: card.querySelector('.hq-function').value,
                fields: fields
            });
        });
        return queries;
    }

    getSelectedMcpServers() {
        return Array.from(document.querySelectorAll('.mcp-server-checkbox:checked')).map(cb => cb.value);
    }

    async loadAgentData() {
        this.showLoading(true);
        this.hideError();

        try {
            const query = `
                query GetAgent($name: String!) {
                    agent(name: $name) {
                        name
                        description
                        version
                        namespace
                        org
                        site
                        nodeId
                        enabled
                        inputTopics
                        outputTopics
                        triggerType
                        cronExpression
                        cronIntervalMs
                        cronPrompt
                        provider
                        model
                        systemPrompt
                        temperature
                        maxTokens
                        maxToolIterations
                        memoryWindowSize
                        stateEnabled
                        mcpServers
                        useMonsterMqMcp
                        defaultArchiveGroup
                        contextLastvalTopics
                        contextRetainedTopics
                        contextHistoryQueries { archiveGroup topics lastSeconds interval function fields }
                        taskTimeoutMs
                        subAgents
                        createdAt
                        updatedAt
                    }
                }
            `;

            const result = await this.client.query(query, { name: this.agentName });

            if (!result.agent) {
                throw new Error('Agent not found');
            }

            this.agentData = result.agent;
            this.renderAgentInfo();

        } catch (error) {
            console.error('Error loading agent:', error);
            this.showError('Failed to load agent: ' + error.message);
        } finally {
            this.showLoading(false);
        }
    }

    renderAgentInfo() {
        if (!this.agentData) return;

        const d = this.agentData;

        // Update page title
        document.getElementById('page-title').textContent = `AI Agent: ${d.name}`;
        document.getElementById('page-subtitle').textContent = d.description || d.namespace || '';

        // Populate Agent Configuration
        document.getElementById('agent-name').value = d.name;
        document.getElementById('agent-name').disabled = true;
        document.getElementById('agent-namespace').value = d.namespace || '';
        document.getElementById('agent-org').value = d.org || 'default';
        document.getElementById('agent-site').value = d.site || 'default';
        document.getElementById('agent-version').value = d.version || '1.0.0';
        document.getElementById('agent-node').value = d.nodeId || '*';
        document.getElementById('agent-description').value = d.description || '';
        document.getElementById('agent-enabled').checked = d.enabled;

        // Populate AI Provider
        document.getElementById('agent-provider').value = d.provider || 'gemini';
        updateModelPlaceholder();
        document.getElementById('agent-model').value = d.model || '';
        document.getElementById('agent-api-key').value = '';
        document.getElementById('agent-temperature').value = d.temperature != null ? d.temperature : 0.7;
        document.getElementById('agent-max-tokens').value = d.maxTokens || '';
        document.getElementById('agent-max-tool-iterations').value = d.maxToolIterations != null ? d.maxToolIterations : 10;
        document.getElementById('agent-memory-window-size').value = d.memoryWindowSize != null ? d.memoryWindowSize : 20;
        document.getElementById('agent-task-timeout-ms').value = d.taskTimeoutMs != null ? d.taskTimeoutMs : 60000;

        // Populate Trigger Configuration
        document.getElementById('agent-trigger-type').value = d.triggerType || 'MQTT';

        // Reverse-parse schedule mode from stored data
        if (d.cronExpression) {
            // Try to detect "daily at time" pattern: "SS MM HH * * ? *" or "SS MM HH * * ?"
            const dailyMatch = d.cronExpression.match(/^(\d+)\s+(\d+)\s+(\d+)\s+\*\s+\*\s+\?\s*\*?$/);
            if (dailyMatch) {
                document.getElementById('agent-schedule-mode').value = 'daily';
                const hh = dailyMatch[3].padStart(2, '0');
                const mm = dailyMatch[2].padStart(2, '0');
                const ss = dailyMatch[1].padStart(2, '0');
                document.getElementById('agent-daily-time').value = `${hh}:${mm}:${ss}`;
            } else {
                document.getElementById('agent-schedule-mode').value = 'custom';
                document.getElementById('agent-cron-expression').value = d.cronExpression;
            }
        } else if (d.cronIntervalMs) {
            document.getElementById('agent-schedule-mode').value = 'interval';
            // Convert ms back to best unit
            const ms = d.cronIntervalMs;
            if (ms % 3600000 === 0) {
                document.getElementById('agent-interval-value').value = ms / 3600000;
                document.getElementById('agent-interval-unit').value = 'hours';
            } else if (ms % 60000 === 0) {
                document.getElementById('agent-interval-value').value = ms / 60000;
                document.getElementById('agent-interval-unit').value = 'minutes';
            } else {
                document.getElementById('agent-interval-value').value = ms / 1000;
                document.getElementById('agent-interval-unit').value = 'seconds';
            }
        } else {
            document.getElementById('agent-schedule-mode').value = 'interval';
        }

        document.getElementById('agent-cron-prompt').value = d.cronPrompt || '';
        document.getElementById('agent-input-topics').value = (d.inputTopics || []).join('\n');
        document.getElementById('agent-output-topics').value = (d.outputTopics || []).join('\n');

        // Populate MCP Servers
        document.getElementById('agent-use-monstermq-mcp').checked = d.useMonsterMqMcp || false;
        document.getElementById('agent-default-archive-group').value = d.defaultArchiveGroup || 'Default';
        this.renderMcpServerCheckboxes(d.mcpServers || []);

        // Populate Sub-Agents
        this.renderSubAgentCheckboxes(d.subAgents || []);

        // Populate Context Data
        document.getElementById('agent-context-lastval-topics').value = d.contextLastvalTopics ? JSON.stringify(d.contextLastvalTopics, null, 2) : '';
        document.getElementById('agent-context-retained').value = (d.contextRetainedTopics || []).join('\n');
        this.renderHistoryQueries(d.contextHistoryQueries);

        // Populate System Prompt
        document.getElementById('agent-system-prompt').value = d.systemPrompt || '';

        // Timestamps
        this.setText('agent-created-at', d.createdAt ? new Date(d.createdAt).toLocaleString() : '-');
        this.setText('agent-updated-at', d.updatedAt ? new Date(d.updatedAt).toLocaleString() : '-');

        // Status badge
        const statusBadge = document.getElementById('agent-status');
        if (d.enabled) {
            statusBadge.className = 'status-badge status-enabled';
            statusBadge.textContent = 'ENABLED';
        } else {
            statusBadge.className = 'status-badge status-disabled';
            statusBadge.textContent = 'DISABLED';
        }

        // Show all sections
        document.getElementById('agent-content').style.display = 'block';
        document.getElementById('provider-section').style.display = 'block';
        document.getElementById('mcp-section').style.display = 'block';
        document.getElementById('trigger-section').style.display = 'block';
        document.getElementById('context-section').style.display = 'block';
        document.getElementById('prompt-section').style.display = 'block';
        document.getElementById('timestamps-section').style.display = 'block';

        // Toggle trigger fields visibility
        toggleTriggerFields();
    }

    collectFormData() {
        const inputTopics = document.getElementById('agent-input-topics').value
            .split('\n').map(t => t.trim()).filter(t => t.length > 0);
        const outputTopics = document.getElementById('agent-output-topics').value
            .split('\n').map(t => t.trim()).filter(t => t.length > 0);

        const data = {
            name: document.getElementById('agent-name').value.trim(),
            namespace: document.getElementById('agent-namespace').value.trim(),
            org: document.getElementById('agent-org').value.trim() || 'default',
            site: document.getElementById('agent-site').value.trim() || 'default',
            version: document.getElementById('agent-version').value.trim() || '1.0.0',
            nodeId: document.getElementById('agent-node').value,
            description: document.getElementById('agent-description').value.trim() || null,
            enabled: document.getElementById('agent-enabled').checked,
            provider: document.getElementById('agent-provider').value,
            model: document.getElementById('agent-model').value.trim() || null,
            systemPrompt: document.getElementById('agent-system-prompt').value || null,
            temperature: parseFloat(document.getElementById('agent-temperature').value) || null,
            maxTokens: parseInt(document.getElementById('agent-max-tokens').value) || null,
            maxToolIterations: parseInt(document.getElementById('agent-max-tool-iterations').value) || null,
            memoryWindowSize: parseInt(document.getElementById('agent-memory-window-size').value) || null,
            taskTimeoutMs: parseInt(document.getElementById('agent-task-timeout-ms').value) || null,
            triggerType: document.getElementById('agent-trigger-type').value,
            cronExpression: null,
            cronIntervalMs: null,
            cronPrompt: document.getElementById('agent-cron-prompt').value.trim() || null,
            inputTopics: inputTopics,
            outputTopics: outputTopics,
            mcpServers: this.getSelectedMcpServers(),
            useMonsterMqMcp: document.getElementById('agent-use-monstermq-mcp').checked,
            defaultArchiveGroup: document.getElementById('agent-default-archive-group').value.trim() || 'Default',
            contextLastvalTopics: this.parseContextLastvalTopics(),
            contextRetainedTopics: document.getElementById('agent-context-retained').value
                .split('\n').map(t => t.trim()).filter(t => t.length > 0),
            contextHistoryQueries: this.collectHistoryQueries(),
            subAgents: this.getSelectedSubAgents()
        };

        // Generate cronExpression or cronIntervalMs based on schedule mode
        if (data.triggerType === 'CRON') {
            const mode = document.getElementById('agent-schedule-mode').value;
            if (mode === 'interval') {
                const val = parseInt(document.getElementById('agent-interval-value').value);
                const unit = document.getElementById('agent-interval-unit').value;
                if (val > 0) {
                    const multipliers = { seconds: 1000, minutes: 60000, hours: 3600000 };
                    data.cronIntervalMs = val * (multipliers[unit] || 60000);
                }
            } else if (mode === 'daily') {
                const time = document.getElementById('agent-daily-time').value || '22:00:00';
                const parts = time.split(':');
                const hh = parseInt(parts[0]) || 0;
                const mm = parseInt(parts[1]) || 0;
                const ss = parseInt(parts[2]) || 0;
                data.cronExpression = `${ss} ${mm} ${hh} * * ? *`;
            } else if (mode === 'custom') {
                data.cronExpression = document.getElementById('agent-cron-expression').value.trim() || null;
            }
        }

        const apiKey = document.getElementById('agent-api-key').value;
        if (apiKey) {
            data.apiKey = apiKey;
        }

        return data;
    }

    async saveAgent() {
        const form = document.getElementById('agent-form');
        if (!form.checkValidity()) {
            form.reportValidity();
            return;
        }

        const data = this.collectFormData();

        if (this.isNew) {
            try {
                const mutation = `
                    mutation CreateAgent($input: AgentInput!) {
                        agent {
                            create(input: $input) {
                                name
                            }
                        }
                    }
                `;
                const result = await this.client.query(mutation, { input: data });
                if (result.agent.create) {
                    this.showSuccess(`Agent "${data.name}" created successfully`);
                    setTimeout(() => { window.spaLocation.href = '/pages/agents.html'; }, 800);
                } else {
                    this.showError('Failed to create agent');
                }
            } catch (error) {
                this.showError('Failed to create agent: ' + error.message);
            }
            return;
        }

        try {
            const mutation = `
                mutation UpdateAgent($name: String!, $input: AgentInput!) {
                    agent {
                        update(name: $name, input: $input) {
                            name
                        }
                    }
                }
            `;
            const result = await this.client.query(mutation, { name: this.agentName, input: data });
            if (result.agent.update) {
                await this.loadAgentData();
                this.showSuccess('Agent updated successfully');
            } else {
                this.showError('Failed to update agent');
            }
        } catch (error) {
            this.showError('Failed to update agent: ' + error.message);
        }
    }

    async deleteAgent() {
        try {
            const mutation = `mutation DeleteAgent($name: String!) { agent { delete(name: $name) } }`;
            const result = await this.client.query(mutation, { name: this.agentName });
            if (result.agent.delete) {
                this.showSuccess('Agent deleted');
                setTimeout(() => { window.spaLocation.href = '/pages/agents.html'; }, 800);
            } else {
                this.showError('Failed to delete agent');
            }
        } catch (e) {
            console.error('Delete error', e);
            this.showError('Failed to delete agent: ' + e.message);
        }
    }

    showDeleteModal() {
        const span = document.getElementById('delete-agent-name');
        if (span && this.agentData) span.textContent = this.agentData.name;
        document.getElementById('delete-agent-modal').style.display = 'flex';
    }

    hideDeleteModal() {
        document.getElementById('delete-agent-modal').style.display = 'none';
    }

    confirmDeleteAgent() {
        this.hideDeleteModal();
        this.deleteAgent();
    }

    goBack() {
        window.spaLocation.href = '/pages/agents.html';
    }

    // UI Helper Methods
    showLoading(show) {
        const indicator = document.getElementById('loading-indicator');
        if (indicator) {
            indicator.style.display = show ? 'flex' : 'none';
        }
    }

    showError(message) {
        var errorDiv = document.getElementById('error-message');
        if (errorDiv) {
            var errorText = errorDiv.querySelector('.error-text');
            if (errorText) errorText.textContent = message;
            errorDiv.style.display = 'flex';
        }

        var existing = document.getElementById('error-toast');
        if (existing) existing.remove();

        var toast = document.createElement('div');
        toast.id = 'error-toast';
        toast.style.cssText = 'position:fixed;top:20px;left:50%;transform:translateX(-50%);background:var(--monster-red,#EF4444);color:#fff;padding:14px 24px;border-radius:8px;box-shadow:0 4px 12px rgba(0,0,0,0.4);z-index:10000;font-size:0.9rem;max-width:600px;display:flex;align-items:center;gap:10px;animation:slideDown 0.3s ease-out;';
        toast.innerHTML = '<span style="font-size:1.2rem;">&#9888;</span><span>' + message + '</span><button onclick="this.parentElement.remove()" style="background:none;border:none;color:#fff;cursor:pointer;margin-left:auto;font-size:1.1rem;line-height:1;padding:0 4px;">&times;</button>';

        if (!document.getElementById('error-toast-style')) {
            var style = document.createElement('style');
            style.id = 'error-toast-style';
            style.textContent = '@keyframes slideDown{from{transform:translateX(-50%) translateY(-100%);opacity:0;}to{transform:translateX(-50%) translateY(0);opacity:1;}}';
            document.head.appendChild(style);
        }

        document.body.appendChild(toast);

        setTimeout(function() {
            if (toast.parentElement) toast.remove();
            if (errorDiv) errorDiv.style.display = 'none';
        }, 8000);
    }

    hideError() {
        const errorEl = document.getElementById('error-message');
        if (errorEl) {
            errorEl.style.display = 'none';
        }
    }

    showSuccess(message) {
        var existing = document.getElementById('success-toast');
        if (existing) existing.remove();
        var toast = document.createElement('div');
        toast.id = 'success-toast';
        toast.style.cssText = 'position:fixed;top:20px;left:50%;transform:translateX(-50%);background:var(--monster-green,#10B981);color:#fff;padding:14px 24px;border-radius:8px;box-shadow:0 4px 12px rgba(0,0,0,0.4);z-index:10000;font-size:0.9rem;max-width:600px;display:flex;align-items:center;gap:10px;animation:slideDown 0.3s ease-out;';
        toast.innerHTML = '<span style="font-size:1.2rem;">&#10003;</span><span>' + this.escapeHtml(message) + '</span><button onclick="this.parentElement.remove()" style="background:none;border:none;color:#fff;cursor:pointer;margin-left:auto;font-size:1.1rem;line-height:1;padding:0 4px;">&times;</button>';
        if (!document.getElementById('toast-anim-style')) { var s = document.createElement('style'); s.id = 'toast-anim-style'; s.textContent = '@keyframes slideDown{from{transform:translateX(-50%) translateY(-100%);opacity:0;}to{transform:translateX(-50%) translateY(0);opacity:1;}}@keyframes fadeOut{from{opacity:1;}to{opacity:0;}}'; document.head.appendChild(s); }
        document.body.appendChild(toast);
        setTimeout(function() { if (toast.parentElement) { toast.style.animation = 'fadeOut 0.3s ease-out forwards'; setTimeout(function() { if (toast.parentElement) toast.remove(); }, 300); } }, 3000);
    }

    escapeHtml(text) {
        const div = document.createElement('div');
        div.textContent = text;
        return div.innerHTML;
    }

    setText(id, value) {
        const el = document.getElementById(id);
        if (el) el.textContent = value;
    }
}

// Global functions
let agentDetailManager;

function saveAgent() {
    agentDetailManager.saveAgent();
}

function goBack() {
    agentDetailManager.goBack();
}

function showDeleteModal() {
    agentDetailManager.showDeleteModal();
}

function hideDeleteModal() {
    agentDetailManager.hideDeleteModal();
}

function confirmDeleteAgent() {
    agentDetailManager.confirmDeleteAgent();
}

function toggleTriggerFields() {
    const triggerType = document.getElementById('agent-trigger-type').value;
    const cronScheduleGroup = document.getElementById('cron-schedule-group');
    if (cronScheduleGroup) {
        cronScheduleGroup.style.display = triggerType === 'CRON' ? 'block' : 'none';
    }
    const cronPromptGroup = document.getElementById('cron-prompt-group');
    if (cronPromptGroup) {
        cronPromptGroup.style.display = triggerType === 'CRON' ? 'block' : 'none';
    }
    if (triggerType === 'CRON') {
        toggleScheduleMode();
    } else {
        document.getElementById('schedule-interval-group').style.display = 'none';
        document.getElementById('schedule-daily-group').style.display = 'none';
        document.getElementById('schedule-custom-group').style.display = 'none';
    }
}

function toggleScheduleMode() {
    const mode = document.getElementById('agent-schedule-mode').value;
    document.getElementById('schedule-interval-group').style.display = mode === 'interval' ? 'block' : 'none';
    document.getElementById('schedule-daily-group').style.display = mode === 'daily' ? 'block' : 'none';
    document.getElementById('schedule-custom-group').style.display = mode === 'custom' ? 'block' : 'none';
}

function updateModelPlaceholder() {
    const provider = document.getElementById('agent-provider').value;
    const modelInput = document.getElementById('agent-model');
    if (!modelInput) return;

    const placeholders = {
        'gemini': 'gemini-2.0-flash',
        'claude': 'claude-sonnet-4-20250514',
        'openai': 'gpt-4o',
        'ollama': 'llama3'
    };
    modelInput.placeholder = placeholders[provider] || 'Model name';
}

// Set up custom drop handlers for context data textareas (append instead of replace)
function setupContextDropZones() {
    const retainedTextarea = document.getElementById('agent-context-retained');
    const lastvalTextarea = document.getElementById('agent-context-lastval-topics');
    const inputTopicsTextarea = document.getElementById('agent-input-topics');
    const outputTopicsTextarea = document.getElementById('agent-output-topics');

    // Helper: visual drop feedback on a textarea
    function addDragStyles(textarea) {
        textarea.setAttribute('data-drop-zone-enabled', 'true'); // prevent side panel generic handler
        textarea.classList.add('drop-zone');
        textarea.addEventListener('dragover', (e) => { e.preventDefault(); e.dataTransfer.dropEffect = 'copy'; textarea.classList.add('drag-over'); });
        textarea.addEventListener('dragenter', (e) => { e.preventDefault(); textarea.classList.add('drag-over'); });
        textarea.addEventListener('dragleave', (e) => { e.preventDefault(); if (!textarea.contains(e.relatedTarget)) textarea.classList.remove('drag-over'); });
    }

    function flashGreen(el) {
        el.style.backgroundColor = 'rgba(34, 197, 94, 0.1)';
        setTimeout(() => { el.style.backgroundColor = ''; }, 500);
    }

    // Append topic on a new line for plain-text topic list textareas
    [retainedTextarea, inputTopicsTextarea, outputTopicsTextarea].forEach(textarea => {
        if (!textarea) return;
        addDragStyles(textarea);
        textarea.addEventListener('drop', (e) => {
            e.preventDefault();
            e.stopImmediatePropagation();
            textarea.classList.remove('drag-over');
            const topic = e.dataTransfer.getData('text/plain');
            if (topic) {
                const current = textarea.value.trim();
                // Append on new line, avoid duplicates
                const lines = current ? current.split('\n').map(l => l.trim()).filter(l => l) : [];
                if (!lines.includes(topic)) lines.push(topic);
                textarea.value = lines.join('\n');
                textarea.focus();
                textarea.dispatchEvent(new Event('change', { bubbles: true }));
                flashGreen(textarea);
            }
        });
    });

    // For JSON archive-group textareas: add topic under the selected archive group
    function setupJsonArchiveGroupDrop(textarea) {
        if (!textarea) return;
        addDragStyles(textarea);
        textarea.addEventListener('drop', (e) => {
            e.preventDefault();
            e.stopImmediatePropagation();
            textarea.classList.remove('drag-over');
            const topic = e.dataTransfer.getData('text/plain');
            if (topic) {
                // Get the archive group from the side panel selector
                const archiveGroupSelect = document.getElementById('topic-panel-archive-group');
                const archiveGroup = archiveGroupSelect ? archiveGroupSelect.value || 'Default' : 'Default';

                // Parse existing JSON or start fresh
                let obj = {};
                try {
                    const current = textarea.value.trim();
                    if (current) obj = JSON.parse(current);
                } catch (_) { /* start fresh if invalid JSON */ }

                // Ensure the archive group array exists and append topic (no duplicates)
                if (!Array.isArray(obj[archiveGroup])) obj[archiveGroup] = [];
                if (!obj[archiveGroup].includes(topic)) obj[archiveGroup].push(topic);

                textarea.value = JSON.stringify(obj, null, 2);
                textarea.focus();
                textarea.dispatchEvent(new Event('change', { bubbles: true }));
                flashGreen(textarea);
            }
        });
    }

    setupJsonArchiveGroupDrop(lastvalTextarea);
}

// Initialize
document.addEventListener('DOMContentLoaded', () => {
    agentDetailManager = new AgentDetailManager();
    setupContextDropZones();

    // Add History Query button
    document.getElementById('add-history-query-btn').addEventListener('click', () => {
        agentDetailManager.addHistoryQueryCard(null);
    });
});

// Handle modal clicks (close on backdrop)
document.addEventListener('click', (e) => {
    if (e.target.classList.contains('modal')) {
        if (e.target.id === 'delete-agent-modal') {
            agentDetailManager.hideDeleteModal();
        }
    }
});
