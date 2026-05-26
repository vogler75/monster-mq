/**
 * MonsterMQ Script Editor Detail Controller
 * Handles loading, form interaction, AI generation, syntax validation,
 * and saving (as FlowClass & FlowInstance) under the 'scripts' namespace.
 */

let editingScriptName = null; // Stored if editing existing script
let originalClassData = null;
let originalInstanceData = null;
let databaseNodeCounter = 0;

document.addEventListener('DOMContentLoaded', initScriptDetailPage);

async function initScriptDetailPage() {
    toggleTriggerPanels();
    parseQueryParams();
    
    if (editingScriptName) {
        await loadScriptForEditing();
    } else {
        // New Script: Add one blank topic row by default
        addTopicRow();
        renderDatabaseNodes([]);
    }
}

// ---------------------- GraphQL & Data Fetching ----------------------
async function graphqlQuery(query, variables = {}) {
    try {
        return await window.graphqlClient.query(query, variables);
    } catch (e) {
        showNotification('GraphQL error: ' + e.message, 'error');
        throw e;
    }
}

function parseQueryParams() {
    const params = new URLSearchParams(window.location.search);
    editingScriptName = params.get('name');
}

async function loadScriptForEditing() {
    document.getElementById('editor-title').textContent = 'Edit Script';
    document.getElementById('sc-name').disabled = true; // Disable name editing
    document.getElementById('delete-btn').style.display = 'inline-flex';

    try {
        const classesQuery = `
            query {
                flowClasses {
                    name
                    namespace
                    description
                    nodes {
                        id
                        type
                        name
                        config
                        inputs
                        outputs
                    }
                }
            }
        `;
        const instancesQuery = `
            query {
                flowInstances {
                    name
                    namespace
                    flowClassId
                    enabled
                    nodeId
                    inputMappings {
                        nodeInput
                        type
                        value
                    }
                }
            }
        `;

        const [classesData, instancesData] = await Promise.all([
            graphqlQuery(classesQuery),
            graphqlQuery(instancesQuery)
        ]);

        const classes = classesData.flowClasses || [];
        const instances = instancesData.flowInstances || [];

        originalInstanceData = instances.find(inst => inst.name === editingScriptName);
        if (!originalInstanceData) {
            showNotification('Script deployment not found', 'error');
            return;
        }

        originalClassData = classes.find(c => c.name === originalInstanceData.flowClassId);
        if (!originalClassData) {
            showNotification('Script class definition not found', 'error');
            return;
        }

        // 1. Populate metadata
        const displayName = originalInstanceData.name.replace(/^script-instance-/, '').replace(/^script-/, '');
        document.getElementById('sc-name').value = displayName;
        document.getElementById('sc-description').value = originalClassData.description || '';
        document.getElementById('sc-enabled').checked = originalInstanceData.enabled;
        document.getElementById('editor-subtitle').textContent = `Editing: ${displayName}`;

        // 2. Populate script code
        const functionNode = originalClassData.nodes.find(n => n.type === 'function');
        if (functionNode && functionNode.config && functionNode.config.script) {
            document.getElementById('sc-code').value = functionNode.config.script;
        }

        // 3. Populate triggers
        const timerNode = originalClassData.nodes.find(n => n.type === 'timer');
        if (timerNode) {
            // Timer triggered
            document.querySelector('input[name="trigger-type"][value="Timer"]').checked = true;
            toggleTriggerPanels();

            const frequencyMs = timerNode.config?.frequency || 10000;
            const { value, unit } = parseIntervalMs(frequencyMs);
            document.getElementById('sc-interval').value = value;
            document.getElementById('sc-interval-unit').value = unit;
        } else {
            // Topic triggered
            document.querySelector('input[name="trigger-type"][value="MQTT"]').checked = true;
            toggleTriggerPanels();

            const topics = originalInstanceData.inputMappings
                .filter(m => m.type === 'TOPIC')
                .map(m => m.value);

            if (topics.length > 0) {
                topics.forEach(topic => addTopicRow(topic));
            } else {
                addTopicRow();
            }
        }

        // 4. Populate JDBC database nodes exposed through dbs.get("<nodeId>")
        const databaseNodes = originalClassData.nodes.filter(n => n.type === 'database');
        renderDatabaseNodes(databaseNodes);

    } catch (e) {
        console.error('Failed to load script details:', e);
        showNotification('Failed to load script: ' + e.message, 'error');
    }
}

// ---------------------- Form Panels & Interaction ----------------------
function toggleTriggerPanels() {
    const triggerType = document.querySelector('input[name="trigger-type"]:checked').value;
    
    const panelMqtt = document.getElementById('panel-mqtt');
    const panelTimer = document.getElementById('panel-timer');

    if (triggerType === 'MQTT') {
        panelMqtt.classList.add('active');
        panelTimer.classList.remove('active');
    } else {
        panelMqtt.classList.remove('active');
        panelTimer.classList.add('active');
    }
}

function addTopicRow(value = '') {
    const container = document.getElementById('topic-inputs-container');
    const rowId = 'topic-row-' + Date.now() + '-' + Math.floor(Math.random() * 1000);
    
    const html = `
        <div class="topic-row" id="${rowId}">
            <input type="text" class="form-control topic-input-field" placeholder="e.g. sensor/temp" value="${escapeHtml(value)}" required style="flex: 1;">
            <ix-icon-button icon="trashcan" variant="primary" ghost size="24" title="Remove topic" onclick="removeTopicRow('${rowId}')"></ix-icon-button>
        </div>
    `;
    
    container.insertAdjacentHTML('beforeend', html);
}

function removeTopicRow(rowId) {
    const row = document.getElementById(rowId);
    if (row) {
        // Ensure there is at least one row left
        const container = document.getElementById('topic-inputs-container');
        if (container.children.length > 1) {
            row.remove();
        } else {
            showNotification('At least one trigger topic is required for MQTT triggers.', 'info');
        }
    }
}

// ---------------------- Database Nodes ----------------------
function renderDatabaseNodes(nodes) {
    const container = document.getElementById('database-nodes-container');
    if (!container) return;

    container.innerHTML = '';
    if (!nodes || nodes.length === 0) {
        container.innerHTML = '<div class="database-empty-state">No database nodes configured.</div>';
        return;
    }

    nodes.forEach(node => addDatabaseNode(node));
}

function addDatabaseNode(node = null) {
    const container = document.getElementById('database-nodes-container');
    if (!container) return;

    const emptyState = container.querySelector('.database-empty-state');
    if (emptyState) emptyState.remove();

    databaseNodeCounter += 1;
    const cardId = 'database-node-' + Date.now() + '-' + databaseNodeCounter;
    const config = node?.config || {};
    const defaultId = node?.id || `db${databaseNodeCounter}`;
    const defaultName = node?.name || defaultId;

    const html = `
        <div class="database-node-card" id="${cardId}">
            <div class="database-node-header">
                <div class="database-node-title">JDBC Database</div>
                <ix-icon-button icon="trashcan" variant="primary" ghost size="24" title="Remove database node" onclick="removeDatabaseNode('${cardId}')"></ix-icon-button>
            </div>
            <div class="database-node-body">
                <div class="form-group">
                    <label>Node ID</label>
                    <input type="text" class="form-control db-node-id" placeholder="e.g. ordersDb" value="${escapeHtml(defaultId)}">
                </div>
                <div class="form-group">
                    <label>Name</label>
                    <input type="text" class="form-control db-node-name" placeholder="e.g. Orders DB" value="${escapeHtml(defaultName)}">
                </div>
                <div class="form-group">
                    <label>Database Driver</label>
                    <select class="form-control db-driver-class">
                        <option value="">-- Select Database Type --</option>
                        <option value="org.postgresql.Driver" ${config.driverClassName === 'org.postgresql.Driver' ? 'selected' : ''}>PostgreSQL</option>
                        <option value="com.mysql.cj.jdbc.Driver" ${config.driverClassName === 'com.mysql.cj.jdbc.Driver' ? 'selected' : ''}>MySQL</option>
                        <option value="org.neo4j.jdbc.Neo4jDriver" ${config.driverClassName === 'org.neo4j.jdbc.Neo4jDriver' ? 'selected' : ''}>Neo4j</option>
                    </select>
                </div>
                <div class="form-group full-width">
                    <label>JDBC URL</label>
                    <input type="text" class="form-control db-jdbc-url" placeholder="jdbc:postgresql://localhost:5432/mydb" value="${escapeHtml(config.jdbcUrl || '')}">
                </div>
                <div class="form-group">
                    <label>Username</label>
                    <input type="text" class="form-control db-username" placeholder="database_user" value="${escapeHtml(config.username || '')}">
                </div>
                <div class="form-group">
                    <label>Password</label>
                    <input type="password" class="form-control db-password" placeholder="password" value="${escapeHtml(config.password || '')}">
                </div>
                <div class="form-group">
                    <label>Connection Mode</label>
                    <select class="form-control db-connection-mode">
                        <option value="PER_TRIGGER" ${(config.connectionMode || 'PER_TRIGGER') === 'PER_TRIGGER' ? 'selected' : ''}>Per Trigger</option>
                        <option value="FLOW_INSTANCE" ${config.connectionMode === 'FLOW_INSTANCE' ? 'selected' : ''}>Flow Instance</option>
                    </select>
                </div>
                <div class="form-group">
                    <label>Health Check Interval (ms)</label>
                    <input type="number" class="form-control db-health-check" min="0" step="1000" value="${escapeHtml(config.healthCheckInterval ?? 60000)}">
                </div>
            </div>
        </div>
    `;

    container.insertAdjacentHTML('beforeend', html);
}

function removeDatabaseNode(cardId) {
    const card = document.getElementById(cardId);
    if (card) card.remove();

    const container = document.getElementById('database-nodes-container');
    if (container && !container.querySelector('.database-node-card')) {
        renderDatabaseNodes([]);
    }
}

function collectDatabaseNodes() {
    const cards = Array.from(document.querySelectorAll('.database-node-card'));
    const nodes = [];
    const seenIds = new Set(['scriptNode', 'timerNode']);

    for (const [idx, card] of cards.entries()) {
        const id = card.querySelector('.db-node-id').value.trim();
        const name = card.querySelector('.db-node-name').value.trim() || id;
        const jdbcUrl = card.querySelector('.db-jdbc-url').value.trim();

        if (!id) {
            showNotification('Database node ID is required.', 'error');
            card.querySelector('.db-node-id').focus();
            return null;
        }
        if (!/^[a-zA-Z0-9_-]+$/.test(id)) {
            showNotification('Database node ID must only contain letters, numbers, dashes, and underscores.', 'error');
            card.querySelector('.db-node-id').focus();
            return null;
        }
        if (seenIds.has(id)) {
            showNotification(`Database node ID "${id}" is duplicated or reserved.`, 'error');
            card.querySelector('.db-node-id').focus();
            return null;
        }
        if (!jdbcUrl) {
            showNotification(`JDBC URL is required for database node "${id}".`, 'error');
            card.querySelector('.db-jdbc-url').focus();
            return null;
        }

        seenIds.add(id);
        nodes.push({
            id,
            type: 'database',
            name,
            config: {
                driverClassName: card.querySelector('.db-driver-class').value,
                jdbcUrl,
                username: card.querySelector('.db-username').value.trim(),
                password: card.querySelector('.db-password').value,
                operationType: 'QUERY',
                sqlStatement: '',
                connectionMode: card.querySelector('.db-connection-mode').value || 'PER_TRIGGER',
                enableDynamicSql: false,
                healthCheckInterval: parseInt(card.querySelector('.db-health-check').value || '60000', 10)
            },
            inputs: ['in'],
            outputs: ['out'],
            position: { x: 750, y: 180 + (idx * 120) }
        });
    }

    return nodes;
}

function getDatabaseNodesForPrompt() {
    return Array.from(document.querySelectorAll('.database-node-card')).map(card => ({
        id: card.querySelector('.db-node-id').value.trim(),
        jdbcUrl: card.querySelector('.db-jdbc-url').value.trim()
    })).filter(db => db.id);
}

function getDatabaseAccessSummary() {
    const dbNodes = getDatabaseNodesForPrompt();
    if (dbNodes.length === 0) return 'none configured';
    return dbNodes.map(db => `dbs.get("${db.id}")`).join(', ');
}

// ---------------------- Code Validation ----------------------
function validateScriptCode() {
    const code = document.getElementById('sc-code').value.trim();
    const validationBox = document.getElementById('sc-validation');

    if (!code) {
        validationBox.className = 'script-editor-validation warning';
        validationBox.style.display = 'block';
        validationBox.textContent = '⚠ Script is empty';
        return false;
    }

    try {
        // Try parsing as JS function
        new Function(code);
        validationBox.className = 'script-editor-validation success';
        validationBox.style.display = 'block';
        validationBox.textContent = '✓ JavaScript syntax is valid';
        return true;
    } catch (e) {
        validationBox.className = 'script-editor-validation error';
        validationBox.style.display = 'block';
        validationBox.textContent = `✗ Syntax error: ${e.message}`;
        return false;
    }
}

// ---------------------- Maximize Fullscreen Editor ----------------------
function expandFullCodeEditor() {
    const codeArea = document.getElementById('sc-code');
    const triggerType = document.querySelector('input[name="trigger-type"]:checked').value;
    
    let triggerInfo = '';
    if (triggerType === 'MQTT') {
        const topics = Array.from(document.querySelectorAll('.topic-input-field'))
            .map(input => input.value.trim())
            .filter(Boolean);
        triggerInfo = `Topic Trigger: ${topics.join(', ')}`;
    } else {
        const val = document.getElementById('sc-interval').value;
        const unit = document.getElementById('sc-interval-unit').value;
        const unitLabel = unit === 's' ? 'Seconds' : unit === 'm' ? 'Minutes' : 'Hours';
        triggerInfo = `Timer Trigger: Every ${val} ${unitLabel}`;
    }

    const scriptName = document.getElementById('sc-name').value.trim() || 'New Script';

    window.ScriptEditorModal.open({
        title: 'Script Code Editor',
        subtitle: `Script: ${scriptName} (${triggerInfo})`,
        initialScript: codeArea.value,
        inputs: 'msg (value, topic, timestamp)',
        outputs: `mqtt.publish(topic, value), databases: ${getDatabaseAccessSummary()}`,
        onSave: (updatedScript) => {
            codeArea.value = updatedScript;
            validateScriptCode();
            showNotification('Code saved from full editor', 'success');
        }
    });
}

// ---------------------- AI Generation ----------------------
async function generateScriptWithAI() {
    const promptInput = document.getElementById('ai-prompt-input');
    const question = promptInput.value.trim();
    if (!question) {
        showNotification('Please enter a prompt first', 'info');
        return;
    }

    const genBtn = document.getElementById('ai-btn-generate');
    const feedback = document.getElementById('ai-feedback');

    genBtn.disabled = true;
    feedback.className = 'ai-feedback-box loading';
    feedback.style.display = 'block';
    feedback.innerHTML = '⏳ AI is generating your script code...';

    try {
        const currentCode = document.getElementById('sc-code').value;
        const triggerType = document.querySelector('input[name="trigger-type"]:checked').value;

        // Build prompt context
        let triggerContext = '';
        if (triggerType === 'MQTT') {
            const topics = Array.from(document.querySelectorAll('.topic-input-field'))
                .map(input => input.value.trim())
                .filter(Boolean);
            triggerContext = `MQTT Trigger Topics: ${topics.join(', ')}`;
        } else {
            triggerContext = `Timer Trigger: periodically runs`;
        }
        const dbNodes = getDatabaseNodesForPrompt();
        const dbContext = dbNodes.length > 0
            ? `\n- Database Nodes: ${dbNodes.map(db => `${db.id} (${db.jdbcUrl})`).join(', ')}`
            : '\n- Database Nodes: none configured';

        const systemPrompt = `You are a script coding assistant for MonsterMQ edge/broker script engines.
The scripts are executed in a JavaScript (GraalVM ES2022) environment with the following global bindings:
- 'msg': JavaScript object representing the triggering input: { value, topic, timestamp }
- 'mqtt': Object containing 'mqtt.publish(topic, payload, qos, retain)' which returns a boolean.
- 'console': Object with console.log, console.warn, console.error.
- 'dbs': Wrapper to execute sql statements on configured database nodes. Use dbs.get("nodeId").execute(sql, args) to run queries.
- 'archive': Object to fetch history or last values: archive.getLastValue(topic), archive.getHistory(topicFilter, startTime, endTime).

Goal: Return the COMPLETE JavaScript script to run inside the node. Do not write extra functions wrapping it. Just write the executable JavaScript block directly.`;

        const context = `Script Context:\n- Trigger: ${triggerContext}${dbContext}`;

        const promptParts = [];
        promptParts.push(`User request: ${question}`);
        promptParts.push('');
        promptParts.push('Current script:');
        promptParts.push('```javascript');
        promptParts.push(currentCode || '// Empty script');
        promptParts.push('```');
        promptParts.push('');
        promptParts.push('Please return the updated complete script inside a standard javascript code block, followed by a short explanation.');

        const fullPrompt = promptParts.join('\n');

        // Fetch GraphQL endpoint
        const endpoint = await window.graphqlClient.resolveEndpoint();
        const response = await fetch(endpoint, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                query: `
                    query GenerateAI($prompt: String!, $context: String, $docs: [String!]) {
                        genai {
                            generate(prompt: $prompt, context: $context, docs: $docs) {
                                response
                                model
                                error
                            }
                        }
                    }
                `,
                variables: {
                    prompt: `${systemPrompt}\n\n${fullPrompt}`,
                    context: context,
                    docs: ['script-assistant.md']
                }
            })
        });

        const result = await response.json();
        if (result.errors) throw new Error(result.errors[0].message);

        const aiResponse = result.data?.genai?.generate;
        if (aiResponse.error) throw new Error(aiResponse.error);

        const responseText = aiResponse.response;

        // Extract code block
        const codeBlockRegex = /```(?:javascript|js)?\s*\n([\s\S]*?)\n```/i;
        const codeMatch = responseText.match(codeBlockRegex);

        let extractedCode = null;
        if (codeMatch && codeMatch[1]) {
            extractedCode = codeMatch[1].trim();
        }

        let explanation = '';
        if (codeMatch) {
            explanation = responseText.substring(codeMatch.index + codeMatch[0].length).trim();
        } else {
            explanation = responseText;
        }

        if (extractedCode) {
            document.getElementById('sc-code').value = extractedCode;
            validateScriptCode();
            feedback.className = 'ai-feedback-box success';
            feedback.innerHTML = `
                <div style="font-weight: 600; margin-bottom: 4px;">✓ Code Generated Successfully (${aiResponse.model})</div>
                <div style="opacity: 0.8; font-size: 0.8rem; margin-top: 4px; white-space: pre-wrap;">${escapeHtml(explanation.replace(/\*\*Explanation:\*\*/gi, ''))}</div>
            `;
            showNotification('Script generated successfully!', 'success');
        } else {
            feedback.className = 'ai-feedback-box success';
            feedback.innerHTML = `
                <div style="font-weight: 600; margin-bottom: 4px;">💡 Suggestions (${aiResponse.model})</div>
                <div style="opacity: 0.8; font-size: 0.8rem; margin-top: 4px; white-space: pre-wrap;">${escapeHtml(responseText)}</div>
            `;
        }

    } catch (error) {
        console.error('AI assistant error:', error);
        feedback.className = 'ai-feedback-box error';
        feedback.innerHTML = `✗ AI failed to generate script: ${escapeHtml(error.message)}`;
        showNotification('AI generation failed', 'error');
    } finally {
        genBtn.disabled = false;
    }
}

// ---------------------- Saving ----------------------
async function saveScript() {
    const displayName = document.getElementById('sc-name').value.trim();
    const description = document.getElementById('sc-description').value.trim();
    const enabled = document.getElementById('sc-enabled').checked;
    const triggerType = document.querySelector('input[name="trigger-type"]:checked').value;
    const userCode = document.getElementById('sc-code').value;

    if (!displayName) {
        showNotification('Script Name is required', 'error');
        document.getElementById('sc-name').focus();
        return;
    }

    // Name must be alphanumeric/dash
    if (!/^[a-zA-Z0-9_-]+$/.test(displayName)) {
        showNotification('Script Name must only contain letters, numbers, dashes, and underscores.', 'error');
        document.getElementById('sc-name').focus();
        return;
    }

    const classId = `script-class-${displayName}`;
    const instanceId = `script-instance-${displayName}`;

    // Construct the Class and Instance structures
    let nodes = [];
    let connections = [];
    let inputMappings = [];
    const databaseNodes = collectDatabaseNodes();
    if (databaseNodes === null) return;

    if (triggerType === 'MQTT') {
        const topics = Array.from(document.querySelectorAll('.topic-input-field'))
            .map(input => input.value.trim())
            .filter(Boolean);

        if (topics.length === 0) {
            showNotification('At least one trigger topic is required for MQTT trigger.', 'error');
            return;
        }

        // Single function node, inputs are mapped dynamically
        const nodeInputs = topics.map((_, idx) => `input_${idx}`);
        nodes = [{
            id: 'scriptNode',
            type: 'function',
            name: displayName,
            config: { script: userCode },
            inputs: nodeInputs,
            outputs: ['out'],
            language: 'javascript',
            position: { x: 400, y: 250 }
        }];

        connections = [];

        inputMappings = topics.map((topic, idx) => ({
            nodeInput: `scriptNode.input_${idx}`,
            type: 'TOPIC',
            value: topic
        }));

    } else {
        const intervalValue = parseInt(document.getElementById('sc-interval').value);
        const intervalUnit = document.getElementById('sc-interval-unit').value;

        if (isNaN(intervalValue) || intervalValue <= 0) {
            showNotification('Timer interval frequency must be greater than 0', 'error');
            document.getElementById('sc-interval').focus();
            return;
        }

        // Convert interval frequency to milliseconds
        const frequencyMs = convertToMs(intervalValue, intervalUnit);

        // Class has two nodes connected
        nodes = [
            {
                id: 'timerNode',
                type: 'timer',
                name: 'Interval Timer',
                config: { frequency: frequencyMs, value: '' },
                inputs: [],
                outputs: ['tick'],
                language: 'javascript',
                position: { x: 200, y: 250 }
            },
            {
                id: 'scriptNode',
                type: 'function',
                name: displayName,
                config: { script: userCode },
                inputs: ['tick'],
                outputs: ['out'],
                language: 'javascript',
                position: { x: 500, y: 250 }
            }
        ];

        connections = [
            {
                fromNode: 'timerNode',
                fromOutput: 'tick',
                toNode: 'scriptNode',
                toInput: 'tick'
            }
        ];

        inputMappings = [];
    }

    nodes = nodes.concat(databaseNodes);

    const classInput = {
        name: classId,
        namespace: 'scripts',
        version: '1.0.0',
        description: description || null,
        nodes: nodes,
        connections: connections
    };

    const instanceInput = {
        name: instanceId,
        namespace: 'scripts',
        nodeId: '*', // Automatically assigned
        flowClassId: classId,
        inputMappings: inputMappings,
        outputMappings: [],
        variables: {},
        enabled: enabled
    };

    const isUpdate = !!editingScriptName;
    let isActuallyUpdate = isUpdate;

    // If updating an old script format (e.g. name was script-TempAlert),
    // delete the old records first to avoid duplicates/collisions
    if (isUpdate && editingScriptName !== instanceId) {
        try {
            const deleteInstanceMutation = `mutation($name:String!){ flow { deleteInstance(name:$name) } }`;
            const deleteClassMutation = `mutation($name:String!){ flow { deleteClass(name:$name) } }`;
            await graphqlQuery(deleteInstanceMutation, { name: editingScriptName });
            await graphqlQuery(deleteClassMutation, { name: editingScriptName });
            isActuallyUpdate = false; // Treat as new creation
        } catch (e) {
            console.warn("Clean up of old script format failed:", e);
        }
    }

    const classMutation = isActuallyUpdate
        ? `mutation($name:String!,$input:FlowClassInput!){ flow { updateClass(name:$name,input:$input){ name } } }`
        : `mutation($input:FlowClassInput!){ flow { createClass(input:$input){ name } } }`;

    const instanceMutation = isActuallyUpdate
        ? `mutation($name:String!,$input:FlowInstanceInput!){ flow { updateInstance(name:$name,input:$input){ name } } }`
        : `mutation($input:FlowInstanceInput!){ flow { createInstance(input:$input){ name } } }`;

    try {
        // Save the class (template) first
        const classVars = isActuallyUpdate ? { name: classId, input: classInput } : { input: classInput };
        await graphqlQuery(classMutation, classVars);

        // Save the instance (deployment) next
        const instanceVars = isActuallyUpdate ? { name: instanceId, input: instanceInput } : { input: instanceInput };
        await graphqlQuery(instanceMutation, instanceVars);

        showNotification('Script saved successfully!', 'success');
        
        // Return to scripts list
        setTimeout(goBackToScriptsList, 800);

    } catch (e) {
        console.error('Failed to save script:', e);
        showNotification('Save failed: ' + e.message, 'error');
    }
}

async function deleteCurrentScript() {
    if (!editingScriptName) return;
    
    const displayName = editingScriptName.replace(/^script-instance-/, '').replace(/^script-/, '');
    showConfirmModal('Confirm Delete', `Are you sure you want to delete script "<b>${escapeHtml(displayName)}</b>"?<br><br>This action cannot be undone.`, async () => {
        try {
            const classId = `script-class-${displayName}`;
            const instanceId = `script-instance-${displayName}`;
            
            const deleteInstanceMutation = `mutation($name:String!){ flow { deleteInstance(name:$name) } }`;
            const deleteClassMutation = `mutation($name:String!){ flow { deleteClass(name:$name) } }`;
            
            await graphqlQuery(deleteInstanceMutation, { name: instanceId });
            await graphqlQuery(deleteClassMutation, { name: classId });
            
            if (editingScriptName !== instanceId) {
                await graphqlQuery(deleteInstanceMutation, { name: editingScriptName });
                await graphqlQuery(deleteClassMutation, { name: editingScriptName });
            }
            
            showNotification('Script deleted successfully', 'success');
            setTimeout(goBackToScriptsList, 600);
        } catch (e) {
            console.error('Failed to delete script:', e);
            showNotification('Deletion failed: ' + e.message, 'error');
        }
    });
}

function goBackToScriptsList() {
    window.spaLocation.href = '/pages/scripts.html';
}

// ---------------------- Utility Handlers ----------------------
function convertToMs(val, unit) {
    switch (unit) {
        case 's': return val * 1000;
        case 'm': return val * 60 * 1000;
        case 'h': return val * 60 * 60 * 1000;
        default: return val * 1000;
    }
}

function parseIntervalMs(ms) {
    if (ms >= 60 * 60 * 1000 && ms % (60 * 60 * 1000) === 0) {
        return { value: ms / (60 * 60 * 1000), unit: 'h' };
    }
    if (ms >= 60 * 1000 && ms % (60 * 1000) === 0) {
        return { value: ms / (60 * 1000), unit: 'm' };
    }
    return { value: ms / 1000, unit: 's' };
}

function escapeHtml(text) {
    if (text === null || text === undefined) return '';
    const div = document.createElement('div');
    div.textContent = String(text);
    return div.innerHTML;
}

function showNotification(message, type = 'info') {
    const notification = document.createElement('div');
    notification.textContent = message;
    notification.style.cssText = `position:fixed;top:20px;right:20px;padding:.6rem 1rem;background:${type === 'success' ? '#28a745' : type === 'error' ? '#dc3545' : '#17a2b8'};color:#fff;border-radius:4px;z-index:10000;font-size:.75rem;opacity:1;transition:opacity .3s;animation:notify-dismiss 2.8s forwards;`;
    document.body.appendChild(notification);
    notification.addEventListener('animationend', () => notification.remove());
    if (!document.getElementById('notify-keyframes')) {
        const style = document.createElement('style');
        style.id = 'notify-keyframes';
        style.textContent = '@keyframes notify-dismiss{0%,85%{opacity:1}100%{opacity:0}}';
        document.head.appendChild(style);
    }
}

function showConfirmModal(title, message, onConfirm, confirmLabel = 'Delete') {
    const overlay = document.createElement('div');
    overlay.style.cssText = 'position:fixed;top:0;left:0;width:100%;height:100%;background:rgba(0,0,0,0.8);display:flex;justify-content:center;align-items:center;z-index:9999;padding:2rem;box-sizing:border-box;';
    overlay.innerHTML = `
        <div style="background:var(--dark-surface);border-radius:12px;border:1px solid var(--dark-border);max-width:500px;width:100%;box-shadow:0 20px 40px rgba(0,0,0,0.5);">
            <div style="padding:1.5rem 2rem;border-bottom:1px solid var(--dark-border);display:flex;justify-content:space-between;align-items:center;">
                <h3 style="margin:0;color:var(--text-primary);font-size:1.25rem;font-weight:600;">${title}</h3>
                <button class="modal-close-btn" style="background:none;border:none;color:var(--text-muted);font-size:1.5rem;cursor:pointer;padding:0.25rem;line-height:1;">×</button>
            </div>
            <div style="padding:2rem;color:var(--text-primary);">${message}</div>
            <div style="padding:1.5rem 2rem;border-top:1px solid var(--dark-border);display:flex;justify-content:flex-end;gap:1rem;">
                <button class="btn btn-secondary modal-cancel-btn">Cancel</button>
                <button class="btn btn-danger modal-confirm-btn">${confirmLabel}</button>
            </div>
        </div>`;
    document.body.appendChild(overlay);
    const close = () => overlay.remove();
    overlay.querySelector('.modal-close-btn').onclick = close;
    overlay.querySelector('.modal-cancel-btn').onclick = close;
    overlay.querySelector('.modal-confirm-btn').onclick = () => { close(); onConfirm(); };
    overlay.addEventListener('click', (e) => { if (e.target === overlay) close(); });
}
