// AI Agent Detail Management JavaScript

class AgentDetailManager {
    constructor() {
        this.client = new GraphQLDashboardClient();
        this.agentName = null;
        this.agentData = null;
        this.clusterNodes = [];
        this.availableMcpServers = [];
        this.init();
    }

    async init() {
        const urlParams = new URLSearchParams(window.location.search);
        this.agentName = urlParams.get('agent');
        this.isNew = urlParams.get('new') === 'true';

        if (this.isNew) {
            await this.loadClusterNodes();
            await this.loadMcpServers();
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
        document.getElementById('agent-cron-interval').value = '';
        document.getElementById('agent-input-topics').value = '';
        document.getElementById('agent-output-topics').value = '';
        document.getElementById('agent-system-prompt').value = '';

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
                        namespace
                        nodeId
                        enabled
                        inputTopics
                        outputTopics
                        triggerType
                        cronExpression
                        cronIntervalMs
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

        // Populate Trigger Configuration
        document.getElementById('agent-trigger-type').value = d.triggerType || 'MQTT';
        document.getElementById('agent-cron-interval').value = d.cronIntervalMs || '';
        document.getElementById('agent-input-topics').value = (d.inputTopics || []).join('\n');
        document.getElementById('agent-output-topics').value = (d.outputTopics || []).join('\n');

        // Populate MCP Servers
        document.getElementById('agent-use-monstermq-mcp').checked = d.useMonsterMqMcp || false;
        this.renderMcpServerCheckboxes(d.mcpServers || []);

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
            triggerType: document.getElementById('agent-trigger-type').value,
            cronIntervalMs: parseInt(document.getElementById('agent-cron-interval').value) || null,
            inputTopics: inputTopics,
            outputTopics: outputTopics,
            mcpServers: this.getSelectedMcpServers(),
            useMonsterMqMcp: document.getElementById('agent-use-monstermq-mcp').checked
        };

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
    const cronGroup = document.getElementById('cron-interval-group');
    if (cronGroup) {
        cronGroup.style.display = triggerType === 'CRON' ? 'block' : 'none';
    }
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

// Initialize
document.addEventListener('DOMContentLoaded', () => {
    agentDetailManager = new AgentDetailManager();
});

// Handle modal clicks (close on backdrop)
document.addEventListener('click', (e) => {
    if (e.target.classList.contains('modal')) {
        if (e.target.id === 'delete-agent-modal') {
            agentDetailManager.hideDeleteModal();
        }
    }
});
