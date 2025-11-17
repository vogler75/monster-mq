// SparkplugB Decoder Detail View

let isEditMode = false;
let currentDecoderName = null;
let rules = [];

// GraphQL wrapper
async function graphqlQuery(query, variables = {}) {
    try {
        return await window.graphqlClient.query(query, variables);
    } catch (e) {
        console.error('GraphQL error:', e);
        throw e;
    }
}

async function init() {
    // Load cluster nodes first
    await loadClusterNodes();

    const params = new URLSearchParams(window.location.search);
    currentDecoderName = params.get('name');

    if (currentDecoderName) {
        isEditMode = true;
        document.getElementById('pageTitle').textContent = '⚡ Edit SparkplugB Decoder';
        document.getElementById('name').disabled = true; // Name cannot be changed in edit mode
        await loadDecoder();
    } else {
        // Create mode - add one default rule
        addRule();
    }
}

async function loadClusterNodes() {
    try {
        const query = `
            query GetBrokers {
                brokers {
                    nodeId
                    isCurrent
                }
            }
        `;

        const result = await graphqlQuery(query);
        const clusterNodes = result.brokers || [];

        // Populate node selector
        const nodeSelect = document.getElementById('nodeId');
        if (nodeSelect) {
            nodeSelect.innerHTML = '<option value="">Select Node...</option>';
            clusterNodes.forEach(node => {
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

async function loadDecoder() {
    try {
        const query = `
            query {
                sparkplugBDecoders(name: "${escapeGraphQL(currentDecoderName)}") {
                    name
                    namespace
                    nodeId
                    enabled
                    config {
                        sourceNamespace
                        rules {
                            name
                            nodeIdRegex
                            deviceIdRegex
                            destinationTopic
                            transformations
                        }
                    }
                }
            }
        `;

        const result = await graphqlQuery(query);
        const decoders = result.sparkplugBDecoders || [];

        if (decoders.length === 0) {
            alert('Decoder not found');
            window.location.href = 'sparkplugb-decoders.html';
            return;
        }

        const decoder = decoders[0];
        populateForm(decoder);
    } catch (error) {
        console.error('Error loading decoder:', error);
        alert('Failed to load decoder: ' + error.message);
    }
}

function populateForm(decoder) {
    document.getElementById('name').value = decoder.name;
    document.getElementById('namespace').value = decoder.namespace;
    document.getElementById('nodeId').value = decoder.nodeId;
    document.getElementById('enabled').checked = decoder.enabled;
    document.getElementById('sourceNamespace').value = decoder.config.sourceNamespace || 'spBv1.0';

    rules = decoder.config.rules || [];
    renderRules();
}

function renderRules() {
    const container = document.getElementById('rulesList');

    if (rules.length === 0) {
        container.innerHTML = '<p style="color: var(--text-secondary); text-align: center; padding: 1rem;">No rules configured. Click "Add Rule" to create one.</p>';
        return;
    }

    container.innerHTML = rules.map((rule, index) => `
        <div class="rule-item">
            <div class="rule-header">
                <h3>Rule: ${escapeHtml(rule.name || `Rule ${index + 1}`)}</h3>
                <button type="button" class="btn btn-icon btn-remove" onclick="removeRule(${index})" title="Remove Rule">
                    <svg width="18" height="18" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16"></path>
                    </svg>
                </button>
            </div>
            <div class="rule-fields">
                <div class="form-group">
                    <label>Rule Name *</label>
                    <input type="text" value="${escapeHtml(rule.name || '')}" onchange="updateRuleName(${index}, this.value)" required>
                    <span class="help-text">Unique identifier for this rule</span>
                </div>
                <div class="form-group">
                    <label>Node ID Regex *</label>
                    <input type="text" value="${escapeHtml(rule.nodeIdRegex || '')}" onchange="updateRuleField(${index}, 'nodeIdRegex', this.value)" required>
                    <span class="help-text">Regex pattern to match SparkplugB node ID (e.g., "^acme\\..*" or ".*")</span>
                </div>
                <div class="form-group">
                    <label>Device ID Regex *</label>
                    <input type="text" value="${escapeHtml(rule.deviceIdRegex || '')}" onchange="updateRuleField(${index}, 'deviceIdRegex', this.value)" required>
                    <span class="help-text">Regex pattern to match SparkplugB device ID (e.g., "^sensor-" or ".*")</span>
                </div>
                <div class="form-group">
                    <label>Destination Topic Template *</label>
                    <input type="text" value="${escapeHtml(rule.destinationTopic || '')}" onchange="updateRuleField(${index}, 'destinationTopic', this.value)" required>
                    <span class="help-text">Topic template with variables: $nodeId, $deviceId (e.g., "factory/decoded/$nodeId/$deviceId")</span>
                </div>
                <div class="form-group">
                    <label>Transformations</label>
                    <div id="transformations-${index}">
                        ${renderTransformations(rule.transformations || {}, index)}
                    </div>
                    <button type="button" class="btn btn-secondary" onclick="addTransformation(${index})">Add Transformation</button>
                    <span class="help-text">Optional regex transformations on variables (e.g., s/\\./\\//g to convert dots to slashes)</span>
                </div>
            </div>
        </div>
    `).join('');
}

function renderTransformations(transformations, ruleIndex) {
    const entries = Object.entries(transformations);

    if (entries.length === 0) {
        return '<p style="color: var(--text-secondary); font-size: 0.875rem; margin: 0.5rem 0;">No transformations configured</p>';
    }

    return entries.map(([key, value], transformIndex) => `
        <div class="transformation-item">
            <select onchange="updateTransformationKey(${ruleIndex}, ${transformIndex}, this.value, '${escapeHtml(key)}')">
                <option value="nodeId" ${key === 'nodeId' ? 'selected' : ''}>nodeId</option>
                <option value="deviceId" ${key === 'deviceId' ? 'selected' : ''}>deviceId</option>
            </select>
            <input type="text" value="${escapeHtml(value)}" onchange="updateTransformationValue(${ruleIndex}, '${escapeHtml(key)}', this.value)" placeholder="s/pattern/replacement/flags">
            <button type="button" class="btn btn-icon btn-remove" onclick="removeTransformation(${ruleIndex}, '${escapeHtml(key)}')">
                <svg width="16" height="16" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12"></path>
                </svg>
            </button>
        </div>
    `).join('');
}

function addRule() {
    rules.push({
        name: `Rule ${rules.length + 1}`,
        nodeIdRegex: '.*',
        deviceIdRegex: '.*',
        destinationTopic: 'decoded/$nodeId/$deviceId',
        transformations: {}
    });
    renderRules();
}

function removeRule(index) {
    if (confirm('Are you sure you want to remove this rule?')) {
        rules.splice(index, 1);
        renderRules();
    }
}

function updateRuleName(index, value) {
    rules[index].name = value;
}

function updateRuleField(index, field, value) {
    rules[index][field] = value;
}

function addTransformation(ruleIndex) {
    if (!rules[ruleIndex].transformations) {
        rules[ruleIndex].transformations = {};
    }

    // Find the next available key
    let key = 'nodeId';
    if (rules[ruleIndex].transformations.hasOwnProperty('nodeId')) {
        key = 'deviceId';
    }

    rules[ruleIndex].transformations[key] = 's/\\\\./_/g';
    renderRules();
}

function removeTransformation(ruleIndex, key) {
    delete rules[ruleIndex].transformations[key];
    renderRules();
}

function updateTransformationKey(ruleIndex, transformIndex, newKey, oldKey) {
    const transformations = rules[ruleIndex].transformations;
    const value = transformations[oldKey];
    delete transformations[oldKey];
    transformations[newKey] = value;
    renderRules();
}

function updateTransformationValue(ruleIndex, key, value) {
    rules[ruleIndex].transformations[key] = value;
}

async function saveDecoder(event) {
    event.preventDefault();

    const name = document.getElementById('name').value.trim();
    const namespace = document.getElementById('namespace').value.trim();
    const nodeId = document.getElementById('nodeId').value.trim();
    const enabled = document.getElementById('enabled').checked;
    const sourceNamespace = document.getElementById('sourceNamespace').value.trim() || 'spBv1.0';

    // Validation
    if (!name || !namespace || !nodeId) {
        alert('Please fill in all required fields');
        return;
    }

    if (rules.length === 0) {
        alert('Please add at least one decoder rule');
        return;
    }

    // Validate all rules
    for (const rule of rules) {
        if (!rule.name || !rule.nodeIdRegex || !rule.deviceIdRegex || !rule.destinationTopic) {
            alert('Please fill in all required fields for each rule');
            return;
        }
    }

    try {
        const input = {
            name: name,
            namespace: namespace,
            nodeId: nodeId,
            enabled: enabled,
            config: {
                sourceNamespace: sourceNamespace,
                rules: rules.map(rule => ({
                    name: rule.name,
                    nodeIdRegex: rule.nodeIdRegex,
                    deviceIdRegex: rule.deviceIdRegex,
                    destinationTopic: rule.destinationTopic,
                    transformations: Object.keys(rule.transformations || {}).length > 0 ? rule.transformations : null
                }))
            }
        };

        const mutation = isEditMode
            ? `mutation UpdateSparkplugBDecoder($name: String!, $input: SparkplugBDecoderInput!) {
                sparkplugBDecoder {
                    update(name: $name, input: $input) {
                        success
                        errors
                        decoder {
                            name
                        }
                    }
                }
            }`
            : `mutation CreateSparkplugBDecoder($input: SparkplugBDecoderInput!) {
                sparkplugBDecoder {
                    create(input: $input) {
                        success
                        errors
                        decoder {
                            name
                        }
                    }
                }
            }`;

        const variables = isEditMode
            ? { name: currentDecoderName, input: input }
            : { input: input };

        const result = await graphqlQuery(mutation, variables);
        const response = isEditMode ? result.sparkplugBDecoder?.update : result.sparkplugBDecoder?.create;

        if (response?.success) {
            alert('Decoder saved successfully!');
            window.location.href = 'sparkplugb-decoders.html';
        } else {
            alert('Failed to save decoder:\n' + (response?.errors?.join('\n') || 'Unknown error'));
        }
    } catch (error) {
        console.error('Error saving decoder:', error);
        alert('Failed to save decoder: ' + error.message);
    }
}

function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

function escapeGraphQL(text) {
    return text.replace(/\\/g, '\\\\').replace(/"/g, '\\"').replace(/\n/g, '\\n');
}

// Initialize
document.addEventListener('DOMContentLoaded', () => {
    init();
    document.getElementById('decoderForm').addEventListener('submit', saveDecoder);
});
