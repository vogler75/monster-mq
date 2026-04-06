class TimeBaseLoggerDetail {
    constructor() {
        const params = new URLSearchParams(window.location.search);
        this.loggerName = params.get('name');
        this.isNew = params.get('new') === 'true';
        this.init();
    }

    async init() {
        if (!window.isLoggedIn()) { window.location.href = '/pages/login.html'; return; }
        await this.loadNodes();
        if (!this.isNew && this.loggerName) await this.loadLogger();
        this.updateAuthUI();
    }

    async loadNodes() {
        try {
            const result = await window.graphqlClient.query(`query { brokers { nodeId } }`);
            const select = document.getElementById('nodeId');
            result.brokers.forEach(b => {
                const opt = document.createElement('option');
                opt.value = b.nodeId; opt.textContent = b.nodeId;
                select.appendChild(opt);
            });
        } catch (e) { console.error(e); }
    }

    async loadLogger() {
        try {
            const result = await window.graphqlClient.query(`
                query GetTimeBaseLogger($name: String!) {
                    timebaseLoggers(name: $name) {
                        name namespace nodeId enabled
                        config {
                            endpointUrl authType username
                            topicFilters tableName tableNameJsonPath queueType queueSize diskPath bulkSize bulkTimeoutMs jsonSchema headers
                        }
                    }
                }
            `, { name: this.loggerName });

            if (result.timebaseLoggers?.length > 0) {
                const l = result.timebaseLoggers[0];
                const c = l.config;
                document.getElementById('name').value = l.name;
                document.getElementById('name').disabled = true;
                document.getElementById('namespace').value = l.namespace;
                document.getElementById('nodeId').value = l.nodeId;
                document.getElementById('enabled').checked = l.enabled;
                document.getElementById('endpointUrl').value = c.endpointUrl;
                document.getElementById('authType').value = c.authType;
                document.getElementById('username').value = c.username || '';
                document.getElementById('topicFilters').value = (c.topicFilters || []).join('\n');
                document.getElementById('tableName').value = c.tableName || '';
                document.getElementById('tableNameJsonPath').value = c.tableNameJsonPath || '';
                document.getElementById('logger-queue-type').value = c.queueType || 'MEMORY';
                document.getElementById('logger-queue-size').value = c.queueSize || 10000;
                document.getElementById('logger-disk-path').value = c.diskPath || './buffer';
                document.getElementById('bulkSize').value = c.bulkSize;
                document.getElementById('bulkTimeoutMs').value = c.bulkTimeoutMs;
                document.getElementById('jsonSchema').value = JSON.stringify(c.jsonSchema, null, 2);
                document.getElementById('headers').value = JSON.stringify(c.headers, null, 2);
                this.updateAuthUI();
            }
        } catch (e) { console.error(e); }
    }

    updateAuthUI() {
        const type = document.getElementById('authType').value;
        document.getElementById('auth-basic').style.display = type === 'BASIC' ? 'block' : 'none';
        document.getElementById('auth-token').style.display = type === 'TOKEN' ? 'block' : 'none';
    }

    async save() {
        try {
            const input = {
                name: document.getElementById('name').value,
                namespace: document.getElementById('namespace').value,
                nodeId: document.getElementById('nodeId').value,
                enabled: document.getElementById('enabled').checked,
                config: {
                    endpointUrl: document.getElementById('endpointUrl').value,
                    authType: document.getElementById('authType').value,
                    username: document.getElementById('username').value || null,
                    password: document.getElementById('password').value || null,
                    token: document.getElementById('token').value || null,
                    topicFilters: document.getElementById('topicFilters').value.split('\n').map(s => s.trim()).filter(s => s),
                    tableName: document.getElementById('tableName').value || null,
                    tableNameJsonPath: document.getElementById('tableNameJsonPath').value || null,
                    queueType: document.getElementById('logger-queue-type').value,
                    queueSize: parseInt(document.getElementById('logger-queue-size').value),
                    diskPath: document.getElementById('logger-disk-path').value.trim(),
                    bulkSize: parseInt(document.getElementById('bulkSize').value),
                    bulkTimeoutMs: parseInt(document.getElementById('bulkTimeoutMs').value),
                    jsonSchema: JSON.parse(document.getElementById('jsonSchema').value),
                    headers: JSON.parse(document.getElementById('headers').value)
                }
            };

            const mutation = this.isNew ? `
                mutation Create($input: TimeBaseLoggerInput!) {
                    timebaseLogger { create(input: $input) { success errors } }
                }
            ` : `
                mutation Update($name: String!, $input: TimeBaseLoggerInput!) {
                    timebaseLogger { update(name: $name, input: $input) { success errors } }
                }
            `;

            const vars = this.isNew ? { input } : { name: this.loggerName, input };
            const result = await window.graphqlClient.query(mutation, vars);
            const res = this.isNew ? result.timebaseLogger.create : result.timebaseLogger.update;

            if (res.success) {
                window.spaLocation.href = '/pages/timebase-loggers.html';
            } else {
                alert('Error: ' + res.errors.join(', '));
            }
        } catch (e) {
            alert('Error: ' + e.message);
        }
    }
}

window.timebaseLoggerDetail = new TimeBaseLoggerDetail();

async function showSchemaHelp() {
    const modal = document.getElementById('schema-help-modal');
    if (!modal.dataset.loaded) {
        const resp = await fetch('/pages/schema-help.html');
        modal.innerHTML = await resp.text();
        modal.dataset.loaded = 'true';
    }
    modal.style.display = 'flex';
}

function hideSchemaHelp() {
    document.getElementById('schema-help-modal').style.display = 'none';
}

async function generateSchemaFromExample() {
    const exampleJsonEl = document.getElementById('ai-example-json');
    const statusEl = document.getElementById('ai-schema-status');
    const btn = document.getElementById('ai-generate-schema-btn');
    const exampleJson = exampleJsonEl.value.trim();

    if (!exampleJson) {
        statusEl.style.display = 'inline';
        statusEl.className = 'ai-schema-status error';
        statusEl.textContent = 'Please paste an example JSON payload first.';
        return;
    }

    btn.disabled = true;
    statusEl.style.display = 'inline';
    statusEl.className = 'ai-schema-status';
    statusEl.innerHTML = 'Generating schema<span class="loading-dots"></span>';

    try {
        const prompt = `Generate a MonsterMQ JSON Schema for the following. The user may provide an example JSON payload, notes, or both. Use the notes to guide your schema decisions. Return ONLY the raw JSON schema object, no explanation, no markdown, no code fences.\n\nUser input:\n${exampleJson}`;

        const response = await fetch(window.graphqlClient.endpoint, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                query: `
                    query GenerateSchema($prompt: String!, $docs: [String!]) {
                        genai { generate(prompt: $prompt, docs: $docs) { response model error } }
                    }
                `,
                variables: { prompt, docs: ['json-schema-assistant.md'] }
            })
        });

        const result = await response.json();
        if (result.errors) throw new Error(result.errors[0].message);
        const aiResponse = result.data?.genai?.generate;
        if (!aiResponse) throw new Error('AI is not configured. Please set up a GenAI provider first.');
        if (aiResponse.error) throw new Error(aiResponse.error);

        let responseText = aiResponse.response.trim();
        const codeMatch = responseText.match(/```(?:json)?\s*\n?([\s\S]*?)\n?```/);
        if (codeMatch) responseText = codeMatch[1].trim();

        const schema = JSON.parse(responseText);
        document.getElementById('jsonSchema').value = JSON.stringify(schema, null, 2);
        statusEl.className = 'ai-schema-status success';
        statusEl.textContent = `Schema generated successfully (${aiResponse.model})`;
    } catch (error) {
        console.error('AI schema generation failed:', error);
        statusEl.className = 'ai-schema-status error';
        statusEl.textContent = 'Error: ' + error.message;
    } finally {
        btn.disabled = false;
    }
}
