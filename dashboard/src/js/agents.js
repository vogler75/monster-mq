// AI Agent Management JavaScript

class AgentManager {
    constructor() {
        this.client = new GraphQLDashboardClient();
        this.agents = [];
        this.deleteAgentName = null;
        this.editingAgentName = null; // null = create mode, string = edit mode
        this.showDescriptions = false;
        this.loadGeneration = 0; // guards against concurrent loadAgents() calls
        this.init();
    }

    async init() {
        console.log('Initializing AI Agent Manager...');
        await this.loadAgents();
        setInterval(() => this.loadAgents(), 30000);
    }

    async loadAgents() {
        const generation = ++this.loadGeneration;
        this.showLoading(true);
        this.hideError();

        try {
            const query = `
                query GetAgents {
                    agents {
                        name
                        description
                        namespace
                        nodeId
                        enabled
                        inputTopics
                        outputTopics
                        triggerType
                        provider
                        model
                        systemPrompt
                        cronIntervalMs
                        temperature
                        maxTokens
                        maxToolIterations
                        memoryWindowSize
                        stateEnabled
                        createdAt
                        updatedAt
                    }
                }
            `;

            const result = await this.client.query(query);
            if (generation !== this.loadGeneration) return; // superseded by a newer call
            console.log('Load agents result:', result);

            if (!result || !result.agents) {
                throw new Error('Invalid response structure');
            }

            this.agents = result.agents || [];
            this.updateMetrics();
            this.renderAgentsTable();

        } catch (error) {
            if (generation !== this.loadGeneration) return;
            console.error('Error loading agents:', error);
            this.showError('Failed to load AI Agents: ' + error.message);
        } finally {
            if (generation === this.loadGeneration) {
                this.showLoading(false);
            }
        }
    }

    updateMetrics() {
        const totalAgents = this.agents.length;
        const enabledAgents = this.agents.filter(a => a.enabled).length;
        const mqttAgents = this.agents.filter(a => a.triggerType === 'MQTT').length;
        const cronAgents = this.agents.filter(a => a.triggerType === 'CRON').length;

        document.getElementById('total-agents').textContent = totalAgents;
        document.getElementById('enabled-agents').textContent = enabledAgents;
        document.getElementById('mqtt-agents').textContent = mqttAgents;
        document.getElementById('cron-agents').textContent = cronAgents;
    }

    renderAgentsTable() {
        const tbody = document.getElementById('agents-table-body');
        if (!tbody) return;

        tbody.innerHTML = '';

        if (this.agents.length === 0) {
            tbody.innerHTML = `
                <tr>
                    <td colspan="8" class="no-data">
                        No AI Agents configured. Click "Add Agent" to get started.
                    </td>
                </tr>
            `;
            return;
        }

        this.agents.forEach(agent => {
            const row = document.createElement('tr');

            const statusClass = agent.enabled ? 'status-enabled' : 'status-disabled';
            const statusText = agent.enabled ? 'Enabled' : 'Disabled';

            const triggerClass = agent.triggerType === 'MQTT' ? 'trigger-mqtt'
                : agent.triggerType === 'CRON' ? 'trigger-cron'
                : 'trigger-manual';

            const inputTopics = (agent.inputTopics || []).join(', ') || '-';
            const outputTopics = (agent.outputTopics || []).join(', ') || '-';
            const providerModel = this.escapeHtml(agent.provider || '') + (agent.model ? ' / ' + this.escapeHtml(agent.model) : '');

            row.innerHTML = `
                <td>
                    <div class="client-name">${this.escapeHtml(agent.name)}</div>
                    <small class="client-namespace agent-desc" style="display:${this.showDescriptions ? 'block' : 'none'}">${this.escapeHtml(agent.description || '')}</small>
                </td>
                <td>${providerModel || '-'}</td>
                <td>
                    <span class="trigger-type-badge ${triggerClass}">${this.escapeHtml(agent.triggerType || 'MANUAL')}</span>
                </td>
                <td>
                    <div class="topics-cell" title="${this.escapeHtml(inputTopics)}">${this.escapeHtml(inputTopics)}</div>
                </td>
                <td>
                    <div class="topics-cell" title="${this.escapeHtml(outputTopics)}">${this.escapeHtml(outputTopics)}</div>
                </td>
                <td>${this.escapeHtml(agent.nodeId || '*')}</td>
                <td>
                    <span class="status-badge ${statusClass}">${statusText}</span>
                </td>
                <td><div class="action-buttons"></div></td>
            `;

            // Create action buttons programmatically so ix-icon-button web components
            // properly initialize their shadow DOM on re-render
            const actionsDiv = row.querySelector('.action-buttons');
            const agentName = agent.name;

            const editBtn = document.createElement('ix-icon-button');
            editBtn.setAttribute('icon', 'pen');
            editBtn.setAttribute('variant', 'primary');
            editBtn.setAttribute('ghost', '');
            editBtn.setAttribute('size', '24');
            editBtn.setAttribute('title', 'Edit Agent');
            editBtn.addEventListener('click', (e) => { e.stopPropagation(); window.spaLocation.href = '/pages/agent-detail.html?agent=' + encodeURIComponent(agentName); });
            actionsDiv.appendChild(editBtn);

            const toggleBtn = document.createElement('ix-icon-button');
            toggleBtn.setAttribute('icon', agent.enabled ? 'pause' : 'play');
            toggleBtn.setAttribute('variant', 'primary');
            toggleBtn.setAttribute('ghost', '');
            toggleBtn.setAttribute('size', '24');
            toggleBtn.setAttribute('title', agent.enabled ? 'Stop Agent' : 'Start Agent');
            toggleBtn.addEventListener('click', (e) => { e.stopPropagation(); agentManager.toggleAgent(agentName, !agent.enabled); });
            actionsDiv.appendChild(toggleBtn);

            const deleteBtn = document.createElement('ix-icon-button');
            deleteBtn.setAttribute('icon', 'trashcan');
            deleteBtn.setAttribute('variant', 'primary');
            deleteBtn.setAttribute('ghost', '');
            deleteBtn.setAttribute('size', '24');
            deleteBtn.className = 'btn-delete';
            deleteBtn.setAttribute('title', 'Delete Agent');
            deleteBtn.addEventListener('click', (e) => { e.stopPropagation(); agentManager.deleteAgent(agentName); });
            actionsDiv.appendChild(deleteBtn);

            row.addEventListener('click', () => window.spaLocation.href = `/pages/agent-detail.html?agent=${encodeURIComponent(agentName)}`);
            tbody.appendChild(row);
        });
    }

    // Modal methods

    showCreateModal() {
        this.editingAgentName = null;
        document.getElementById('modal-title').textContent = 'Create Agent';
        this.resetForm();
        document.getElementById('agent-name').disabled = false;
        document.getElementById('agent-modal').style.display = 'flex';
    }

    async showEditModal(agentName) {
        this.editingAgentName = agentName;
        document.getElementById('modal-title').textContent = 'Edit Agent';

        try {
            const query = `
                query GetAgent($name: String!) {
                    agent(name: $name) {
                        name
                        description
                        namespace
                        nodeId
                        enabled
                        inputTopics
                        outputTopics
                        triggerType
                        provider
                        model
                        systemPrompt
                        cronIntervalMs
                        temperature
                        maxTokens
                        maxToolIterations
                        memoryWindowSize
                    }
                }
            `;

            const result = await this.client.query(query, { name: agentName });
            if (!result || !result.agent) {
                throw new Error('Agent not found');
            }

            const agent = result.agent;
            document.getElementById('agent-name').value = agent.name || '';
            document.getElementById('agent-name').disabled = true;
            document.getElementById('agent-namespace').value = agent.namespace || '';
            document.getElementById('agent-nodeId').value = agent.nodeId || '*';
            document.getElementById('agent-description').value = agent.description || '';
            document.getElementById('agent-provider').value = agent.provider || 'gemini';
            document.getElementById('agent-model').value = agent.model || '';
            document.getElementById('agent-systemPrompt').value = agent.systemPrompt || '';
            document.getElementById('agent-triggerType').value = agent.triggerType || 'MQTT';
            document.getElementById('agent-cronIntervalMs').value = agent.cronIntervalMs || '';
            document.getElementById('agent-inputTopics').value = (agent.inputTopics || []).join(', ');
            document.getElementById('agent-outputTopics').value = (agent.outputTopics || []).join(', ');
            document.getElementById('agent-temperature').value = agent.temperature != null ? agent.temperature : 0.7;
            document.getElementById('agent-maxTokens').value = agent.maxTokens || '';
            document.getElementById('agent-maxToolIterations').value = agent.maxToolIterations != null ? agent.maxToolIterations : 10;
            document.getElementById('agent-memoryWindowSize').value = agent.memoryWindowSize != null ? agent.memoryWindowSize : 20;
            document.getElementById('agent-enabled').checked = agent.enabled || false;

            this.onTriggerTypeChange();
            document.getElementById('agent-modal').style.display = 'flex';

        } catch (error) {
            console.error('Error loading agent:', error);
            this.showError('Failed to load agent: ' + error.message);
        }
    }

    hideModal() {
        document.getElementById('agent-modal').style.display = 'none';
        this.editingAgentName = null;
    }

    resetForm() {
        document.getElementById('agent-name').value = '';
        document.getElementById('agent-namespace').value = '';
        document.getElementById('agent-nodeId').value = '*';
        document.getElementById('agent-description').value = '';
        document.getElementById('agent-provider').value = 'gemini';
        document.getElementById('agent-model').value = '';
        document.getElementById('agent-systemPrompt').value = '';
        document.getElementById('agent-triggerType').value = 'MQTT';
        document.getElementById('agent-cronIntervalMs').value = '';
        document.getElementById('agent-inputTopics').value = '';
        document.getElementById('agent-outputTopics').value = '';
        document.getElementById('agent-temperature').value = '0.7';
        document.getElementById('agent-maxTokens').value = '';
        document.getElementById('agent-maxToolIterations').value = '10';
        document.getElementById('agent-memoryWindowSize').value = '20';
        document.getElementById('agent-enabled').checked = false;
        this.onTriggerTypeChange();
    }

    onTriggerTypeChange() {
        const triggerType = document.getElementById('agent-triggerType').value;
        const cronGroup = document.getElementById('cron-interval-group');
        cronGroup.style.display = triggerType === 'CRON' ? '' : 'none';
    }

    getFormData() {
        const parseTopics = (val) => {
            if (!val || !val.trim()) return [];
            return val.split(',').map(t => t.trim()).filter(t => t.length > 0);
        };

        const data = {
            name: document.getElementById('agent-name').value.trim(),
            namespace: document.getElementById('agent-namespace').value.trim(),
            nodeId: document.getElementById('agent-nodeId').value.trim() || '*',
            description: document.getElementById('agent-description').value.trim(),
            provider: document.getElementById('agent-provider').value,
            model: document.getElementById('agent-model').value.trim(),
            systemPrompt: document.getElementById('agent-systemPrompt').value,
            triggerType: document.getElementById('agent-triggerType').value,
            inputTopics: parseTopics(document.getElementById('agent-inputTopics').value),
            outputTopics: parseTopics(document.getElementById('agent-outputTopics').value),
            temperature: parseFloat(document.getElementById('agent-temperature').value) || 0.7,
            maxToolIterations: parseInt(document.getElementById('agent-maxToolIterations').value) || 10,
            memoryWindowSize: parseInt(document.getElementById('agent-memoryWindowSize').value) || 20,
            enabled: document.getElementById('agent-enabled').checked,
        };

        const maxTokens = document.getElementById('agent-maxTokens').value;
        if (maxTokens && parseInt(maxTokens) > 0) {
            data.maxTokens = parseInt(maxTokens);
        }

        const cronIntervalMs = document.getElementById('agent-cronIntervalMs').value;
        if (data.triggerType === 'CRON' && cronIntervalMs && parseInt(cronIntervalMs) > 0) {
            data.cronIntervalMs = parseInt(cronIntervalMs);
        }

        return data;
    }

    async saveAgent() {
        const data = this.getFormData();

        if (!data.name) {
            this.showError('Agent name is required');
            return;
        }
        if (!data.namespace) {
            this.showError('Namespace is required');
            return;
        }

        try {
            if (this.editingAgentName) {
                // Update existing agent
                const inputFields = this.buildInputFields(data, true);
                const mutation = `
                    mutation UpdateAgent($name: String!, $input: AgentInput!) {
                        agent {
                            update(name: $name, input: $input) {
                                name
                                enabled
                            }
                        }
                    }
                `;

                const result = await this.client.query(mutation, {
                    name: this.editingAgentName,
                    input: inputFields
                });

                if (result && result.agent && result.agent.update) {
                    this.hideModal();
                    await this.loadAgents();
                    this.showSuccess(`Agent "${data.name}" updated successfully`);
                } else {
                    this.showError('Failed to update agent');
                }
            } else {
                // Create new agent
                const inputFields = this.buildInputFields(data, false);
                const mutation = `
                    mutation CreateAgent($input: AgentInput!) {
                        agent {
                            create(input: $input) {
                                name
                                enabled
                            }
                        }
                    }
                `;

                const result = await this.client.query(mutation, { input: inputFields });

                if (result && result.agent && result.agent.create) {
                    this.hideModal();
                    await this.loadAgents();
                    this.showSuccess(`Agent "${data.name}" created successfully`);
                } else {
                    this.showError('Failed to create agent');
                }
            }
        } catch (error) {
            console.error('Error saving agent:', error);
            this.showError('Failed to save agent: ' + error.message);
        }
    }

    buildInputFields(data, isUpdate) {
        const input = {};
        if (!isUpdate) {
            input.name = data.name;
        }
        input.namespace = data.namespace;
        input.nodeId = data.nodeId;
        input.description = data.description;
        input.provider = data.provider;
        input.model = data.model;
        input.systemPrompt = data.systemPrompt;
        input.triggerType = data.triggerType;
        input.inputTopics = data.inputTopics;
        input.outputTopics = data.outputTopics;
        input.temperature = data.temperature;
        input.maxToolIterations = data.maxToolIterations;
        input.memoryWindowSize = data.memoryWindowSize;
        input.enabled = data.enabled;
        if (data.maxTokens) input.maxTokens = data.maxTokens;
        if (data.cronIntervalMs) input.cronIntervalMs = data.cronIntervalMs;
        return input;
    }

    async toggleAgent(agentName, start) {
        try {
            const mutation = start
                ? `mutation StartAgent($name: String!) { agent { start(name: $name) { name enabled } } }`
                : `mutation StopAgent($name: String!) { agent { stop(name: $name) { name enabled } } }`;

            const result = await this.client.query(mutation, { name: agentName });
            const actionResult = start ? result.agent.start : result.agent.stop;

            if (actionResult) {
                await this.loadAgents();
                this.showSuccess(`Agent "${agentName}" ${start ? 'started' : 'stopped'} successfully`);
            } else {
                this.showError(`Failed to ${start ? 'start' : 'stop'} agent`);
            }

        } catch (error) {
            console.error('Error toggling agent:', error);
            this.showError(`Failed to ${start ? 'start' : 'stop'} agent: ` + error.message);
        }
    }

    deleteAgent(agentName) {
        this.deleteAgentName = agentName;
        document.getElementById('delete-agent-name').textContent = agentName;
        this.showConfirmDeleteModal();
    }

    async confirmDeleteAgent() {
        if (!this.deleteAgentName) return;

        try {
            const mutation = `
                mutation DeleteAgent($name: String!) {
                    agent {
                        delete(name: $name)
                    }
                }
            `;

            const result = await this.client.query(mutation, { name: this.deleteAgentName });

            if (result.agent.delete) {
                this.hideConfirmDeleteModal();
                await this.loadAgents();
                this.showSuccess(`Agent "${this.deleteAgentName}" deleted successfully`);
            } else {
                this.showError('Failed to delete agent');
            }

        } catch (error) {
            console.error('Error deleting agent:', error);
            this.showError('Failed to delete agent: ' + error.message);
        }

        this.deleteAgentName = null;
    }

    // UI Helper Methods
    showConfirmDeleteModal() {
        document.getElementById('confirm-delete-modal').style.display = 'flex';
    }

    hideConfirmDeleteModal() {
        document.getElementById('confirm-delete-modal').style.display = 'none';
    }

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
        var existing = document.getElementById('success-toast'); if (existing) existing.remove();
        var toast = document.createElement('div'); toast.id = 'success-toast';
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

    toggleDescriptions(show) {
        this.showDescriptions = show;
        document.querySelectorAll('.agent-desc').forEach(el => {
            el.style.display = show ? 'block' : 'none';
        });
    }

    async refreshAgents() {
        await this.loadAgents();
    }
}

// Global functions for onclick handlers
function hideConfirmDeleteModal() {
    agentManager.hideConfirmDeleteModal();
}

function confirmDeleteAgent() {
    agentManager.confirmDeleteAgent();
}

function refreshAgents() {
    agentManager.refreshAgents();
}

// Initialize when DOM is loaded
let agentManager;
document.addEventListener('DOMContentLoaded', () => {
    agentManager = new AgentManager();
});

// Handle modal clicks (close on backdrop click)
document.addEventListener('click', (e) => {
    if (e.target.classList.contains('modal')) {
        if (e.target.id === 'confirm-delete-modal') {
            agentManager.hideConfirmDeleteModal();
        } else if (e.target.id === 'agent-modal') {
            agentManager.hideModal();
        }
    }
});
