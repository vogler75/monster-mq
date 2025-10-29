/**
 * MonsterMQ Workflows List Page (Refactored)
 * Table-based listing of Flow Classes & Flow Instances.
 * Editing moved to workflows-edit.html + workflows-edit.js.
 */

let flowClasses = [];
let flowInstances = [];
let sortState = { classes: { key: 'name', dir: 'asc' }, instances: { key: 'name', dir: 'asc' } };
let filters = { classes: '', instances: '' };
let selectedFlowClassName = ''; // Track selected flow class

document.addEventListener('DOMContentLoaded', initListPage);

async function initListPage(){
    await loadFlowClasses();
    setupListInteractions();
    renderClassesTable();

    // Restore previously selected class from sessionStorage
    const selectedClass = sessionStorage.getItem('selectedFlowClass');
    if(selectedClass && flowClasses.some(c => c.name === selectedClass)){
        await selectFlowClass(selectedClass);
    }
}

// ---------------------- GraphQL ----------------------
async function graphqlQuery(query, variables = {}) {
    try { return await window.graphqlClient.query(query, variables); }
    catch (e) { showNotification('GraphQL error: '+e.message,'error'); throw e; }
}

async function loadFlowClasses(){
    const q = `query { flowClasses { name namespace version description nodes { id } connections { fromNode toNode fromOutput toInput } updatedAt } }`;
    try { const data = await graphqlQuery(q); flowClasses = data.flowClasses||[]; }
    catch(e){ console.error(e); showNotification('Failed to load flow classes','error'); }
}

async function loadFlowInstances(){
    // If a flow class is selected, reload instances for that class
    if(selectedFlowClassName) {
        await loadFlowInstancesForClass(selectedFlowClassName);
        renderInstancesTable();
        return;
    }
    // Otherwise, skip loading instances (loaded on demand)
}

async function loadFlowInstancesForClass(flowClassName){
    const q = `query($flowClassId:String){ flowInstances(flowClassId:$flowClassId) { name namespace nodeId flowClassId inputMappings { nodeInput } outputMappings { nodeOutput } enabled status { running executionCount errorCount } updatedAt } }`;
    try {
        const data = await graphqlQuery(q, { flowClassId: flowClassName });
        flowInstances = data.flowInstances||[];
        console.log(`Loaded ${flowInstances.length} instances for flow class "${flowClassName}"`);
    }
    catch(e){ console.error('Error loading flow instances:', e); showNotification('Failed to load flow instances','error'); }
}

// ---------------------- Interactions ----------------------
function setupListInteractions(){
    const clsSearch = document.getElementById('class-search');
    if(clsSearch) clsSearch.addEventListener('input', e=>{ filters.classes=e.target.value.toLowerCase(); renderClassesTable(); });
    const instSearch = document.getElementById('instance-search');
    if(instSearch) instSearch.addEventListener('input', e=>{ filters.instances=e.target.value.toLowerCase(); renderInstancesTable(); });
    document.querySelectorAll('#classes-table thead th[data-sort]').forEach(th=>{ th.style.cursor='pointer'; th.addEventListener('click', ()=>toggleSort('classes', th.getAttribute('data-sort'))); });
    document.querySelectorAll('#instances-table thead th[data-sort]').forEach(th=>{ th.style.cursor='pointer'; th.addEventListener('click', ()=>toggleSort('instances', th.getAttribute('data-sort'))); });
}

// ---------------------- Flow Class Selection ----------------------
async function selectFlowClass(flowClassName){
    selectedFlowClassName = flowClassName;
    const instanceSection = document.getElementById('instances-section');
    const selectedNameEl = document.getElementById('selected-class-name');

    if(selectedFlowClassName) {
        if(instanceSection) instanceSection.style.display = 'block';
        if(selectedNameEl) selectedNameEl.textContent = selectedFlowClassName;
        sessionStorage.setItem('selectedFlowClass', selectedFlowClassName); // Remember selection
        await loadFlowInstancesForClass(selectedFlowClassName);
    } else {
        if(instanceSection) instanceSection.style.display = 'none';
        if(selectedNameEl) selectedNameEl.textContent = '-';
        flowInstances = [];
        sessionStorage.removeItem('selectedFlowClass');
    }

    renderClassesTable(); // Re-render to show selection highlight
    renderInstancesTable();
}

function createNewInstance(){
    if(!selectedFlowClassName){
        showNotification('Please select a flow class first','error');
        return;
    }
    location.href = `/pages/workflows-edit-instance.html?type=instance&flowClassId=${encodeURIComponent(selectedFlowClassName)}`;
}

// ---------------------- Rendering ----------------------
function renderClassesTable(){
    const tbody = document.querySelector('#classes-table tbody'); if(!tbody) return;
    let rows = flowClasses.slice();
    if(filters.classes) rows = rows.filter(r => (r.name+" "+r.namespace).toLowerCase().includes(filters.classes));
    const { key, dir } = sortState.classes; rows.sort((a,b)=>compareValues(a,b,key,dir));
    if(rows.length===0){ tbody.innerHTML='<tr><td colspan="7" style="text-align:center; color:#6c757d;">No flow classes</td></tr>'; return; }
    tbody.innerHTML = rows.map(r=>`<tr style="cursor:pointer; ${selectedFlowClassName === r.name ? 'background: var(--background-secondary); border-left: 3px solid var(--primary-color);' : ''}" onclick="selectFlowClass('${escapeHtml(r.name)}')">
        <td>${escapeHtml(r.name)}</td>
        <td>${escapeHtml(r.namespace||'')}</td>
        <td>${escapeHtml(r.version||'')}</td>
        <td style="text-align:right;">${r.nodes.length}</td>
        <td style="text-align:right;">${r.connections.length}</td>
        <td>${formatDateTime(r.updatedAt)}</td>
          <td onclick="event.stopPropagation();"><div style="display:flex; gap:.4rem;">
              <button class="btn-action btn-edit" onclick="location.href='/pages/workflows-visual.html?name=${encodeURIComponent(r.name)}'">Edit</button>
              <button class="btn-action btn-secondary" title="Restart all instances of this class" onclick="restartAllInstancesOfClass('${escapeHtml(r.name)}')">Restart All</button>
              <button class="btn-action btn-delete" onclick="listPageDeleteFlowClass('${escapeHtml(r.name)}')">Delete</button>
          </div></td>
    </tr>`).join('');
}

function renderInstancesTable(){
    const tbody = document.querySelector('#instances-table tbody'); if(!tbody) return;
    let rows = flowInstances.slice();
    // Filter by search text (instances are already filtered by selected flow class)
    if(filters.instances) rows = rows.filter(r => (r.name+" "+r.namespace+" "+r.flowClassId).toLowerCase().includes(filters.instances));
    const { key, dir } = sortState.instances; rows.sort((a,b)=>compareValues(a,b,key,dir));
    if(rows.length===0){
        const msg = selectedFlowClassName ? 'No flow instances for this class' : 'Select a flow class to view instances';
        tbody.innerHTML=`<tr><td colspan="9" style="text-align:center; color:#6c757d;">${msg}</td></tr>`;
        return;
    }
    tbody.innerHTML = rows.map(r=>{
        const startStopBtn = r.enabled
            ? `<button class="btn-icon" title="Stop" onclick="listPageStopFlowInstance('${escapeHtml(r.name)}')"><svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="6" y="4" width="4" height="16"></rect><rect x="14" y="4" width="4" height="16"></rect></svg></button>`
            : `<button class="btn-icon" title="Start" onclick="listPageStartFlowInstance('${escapeHtml(r.name)}')"><svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polygon points="5 3 19 12 5 21 5 3"></polygon></svg></button>`;
        return `<tr>
        <td>${escapeHtml(r.name)}</td>
        <td>${escapeHtml(r.namespace||'')}</td>
        <td>${escapeHtml(r.flowClassId)}</td>
        <td>${escapeHtml(r.nodeId||'')}</td>
        <td style="text-align:right;">${r.inputMappings.length}</td>
        <td style="text-align:right;">${r.outputMappings.length}</td>
        <td>${r.enabled?'<span style="color:#2ed573;">Yes</span>':'<span style="color:#ff4757;">No</span>'}</td>
        <td>${formatDateTime(r.updatedAt)}</td>
    <td><div style="display:flex; gap:.4rem;">
        ${startStopBtn}
        <button class="btn-action btn-edit" onclick="location.href='/pages/workflows-edit-instance.html?type=instance&name=${encodeURIComponent(r.name)}&flowClassId=${encodeURIComponent(r.flowClassId)}'">Edit</button>
        <button class="btn-action btn-delete" onclick="listPageDeleteFlowInstance('${escapeHtml(r.name)}')">Delete</button>
    </div></td>
    </tr>`; }).join('');
}

// ---------------------- Actions ----------------------
// Use unique internal names to avoid being shadowed by legacy functions defined later in file.
async function listPageDeleteFlowClass(name){
    if(!confirm(`Delete flow class "${name}"?`)) return;
    const mutation = `mutation($name:String!){ flow { deleteClass(name:$name) } }`;
    try { await graphqlQuery(mutation,{name}); showNotification('Deleted','success'); await loadFlowClasses(); renderClassesTable(); }
    catch(e){ console.error(e); showNotification('Delete failed','error'); }
}

async function listPageDeleteFlowInstance(name){
    if(!confirm(`Delete flow instance "${name}"?`)) return;
    const mutation = `mutation($name:String!){ flow { deleteInstance(name:$name) } }`;
    try { await graphqlQuery(mutation,{name}); showNotification('Deleted','success'); if(selectedFlowClassName) await loadFlowInstancesForClass(selectedFlowClassName); renderInstancesTable(); }
    catch(e){ console.error(e); showNotification('Delete failed','error'); }
}

async function listPageStartFlowInstance(name){
    const mutation = `mutation($name:String!){ flow { enableInstance(name:$name) { name } } }`;
    try { await graphqlQuery(mutation,{name}); showNotification('Instance enabled','success'); if(selectedFlowClassName) await loadFlowInstancesForClass(selectedFlowClassName); renderInstancesTable(); }
    catch(e){ console.error(e); showNotification('Enable failed: '+e.message,'error'); }
}

async function listPageStopFlowInstance(name){
    const mutation = `mutation($name:String!){ flow { disableInstance(name:$name) { name } } }`;
    try { await graphqlQuery(mutation,{name}); showNotification('Instance disabled','success'); if(selectedFlowClassName) await loadFlowInstancesForClass(selectedFlowClassName); renderInstancesTable(); }
    catch(e){ console.error(e); showNotification('Disable failed: '+e.message,'error'); }
}

async function restartAllInstancesOfClass(flowClassName){
    if(!flowClassName){
        showNotification('Please select a flow class first','error');
        return;
    }

    // Load instances for this flow class
    await loadFlowInstancesForClass(flowClassName);
    const instancesToRestart = flowInstances;

    if(instancesToRestart.length === 0){
        showNotification('No instances found for this flow class','info');
        return;
    }

    if(!confirm(`Restart all ${instancesToRestart.length} instance(s) of class "${flowClassName}"?`)) return;

    try {
        let successCount = 0;
        let failCount = 0;

        // Disable all instances
        for(const instance of instancesToRestart){
            try {
                const mutation = `mutation($name:String!){ flow { disableInstance(name:$name) { name } } }`;
                await graphqlQuery(mutation, {name: instance.name});
                successCount++;
            } catch(e){
                console.error(e);
                failCount++;
            }
        }

        // Small delay between disable and enable
        await new Promise(resolve => setTimeout(resolve, 500));

        // Enable all instances again
        for(const instance of instancesToRestart){
            if(instance.enabled){ // Only re-enable if it was previously enabled
                try {
                    const mutation = `mutation($name:String!){ flow { enableInstance(name:$name) { name } } }`;
                    await graphqlQuery(mutation, {name: instance.name});
                } catch(e){
                    console.error(e);
                    failCount++;
                }
            }
        }

        // Reload instances
        await loadFlowInstancesForClass(flowClassName);
        renderInstancesTable();

        if(failCount === 0){
            showNotification(`Successfully restarted ${successCount} instance(s)`, 'success');
        } else {
            showNotification(`Restarted ${successCount} instance(s), ${failCount} failed`, 'error');
        }
    } catch(e){
        console.error(e);
        showNotification('Restart failed: '+e.message, 'error');
    }
}

// ---------------------- Sorting & Helpers ----------------------
function compareValues(a,b,key,dir){
    let av,bv;
    switch(key){
        case 'nodes': av=a.nodes.length; bv=b.nodes.length; break;
        case 'connections': av=a.connections.length; bv=b.connections.length; break;
        case 'inputs': av=a.inputMappings.length; bv=b.inputMappings.length; break;
        case 'outputs': av=a.outputMappings.length; bv=b.outputMappings.length; break;
        case 'executions': av=a.status? a.status.executionCount:0; bv=b.status? b.status.executionCount:0; break;
        default: av=a[key]||''; bv=b[key]||''; break;
    }
    if(typeof av==='string') av=av.toLowerCase();
    if(typeof bv==='string') bv=bv.toLowerCase();
    const cmp = av>bv?1: av<bv?-1:0; return dir==='asc'?cmp:-cmp;
}
function toggleSort(table,key){ const st=sortState[table]; if(st.key===key){ st.dir=st.dir==='asc'?'desc':'asc'; } else { st.key=key; st.dir='asc'; } if(table==='classes') renderClassesTable(); else renderInstancesTable(); }

// ---------------------- Utilities ----------------------
function escapeHtml(text){ if(text===null||text===undefined) return ''; const div=document.createElement('div'); div.textContent=String(text); return div.innerHTML; }

function formatDateTime(dateStr) {
    if (!dateStr) return '';
    // Remove fractional seconds (e.g., 2024-01-15T10:30:45.123456Z -> 2024-01-15 10:30:45)
    return dateStr.replace(/\.\d+Z?$/, '').replace('T', ' ');
}

function showNotification(message, type='info') {
    const notification = document.createElement('div');
    notification.textContent = message;
    notification.style.cssText = `position:fixed;top:20px;right:20px;padding:.6rem 1rem;background:${type==='success'?'#28a745':type==='error'?'#dc3545':'#17a2b8'};color:#fff;border-radius:4px;z-index:10000;font-size:.75rem;`;
    document.body.appendChild(notification);
    setTimeout(()=>{ notification.style.opacity='0'; setTimeout(()=>notification.remove(),300); },2500);
}

async function initWorkflowsPage() {
    canvas = document.getElementById('flow-canvas');
    svgLayer = document.getElementById('connections-svg');

    // Load initial data
    await Promise.all([
        loadFlowClasses(),
        loadFlowInstances(),
        loadFlowNodeTypes()
    ]);

    // Set up event listeners
    setupEventListeners();

    // Show first tab
    switchTab('classes');
}

function setupEventListeners() {
    // Canvas events for node dragging
    if (canvas) {
        canvas.addEventListener('mousedown', handleCanvasMouseDown);
        canvas.addEventListener('mousemove', handleCanvasMouseMove);
        canvas.addEventListener('mouseup', handleCanvasMouseUp);
    }
}

// ======================
// Tab Management
// ======================

function switchTab(tabName) {
    // Update tab buttons
    document.querySelectorAll('.tab').forEach(btn => {
        btn.classList.remove('active');
    });
    document.querySelector(`[onclick="switchTab('${tabName}')"]`)?.classList.add('active');

    // Update tab content
    document.querySelectorAll('.tab-content').forEach(content => {
        content.classList.remove('active');
    });
    document.getElementById(`${tabName}-tab`)?.classList.add('active');
}

// ======================
// GraphQL API Functions
// ======================

async function graphqlQuery(query, variables = {}) {
    try {
        return await window.graphqlClient.query(query, variables);
    } catch (error) {
        showNotification('GraphQL error: ' + error.message, 'error');
        throw error;
    }
}

// ======================
// Load Data Functions
// ======================

async function loadFlowClasses() {
    const query = `
        query {
            flowClasses {
                name
                namespace
                version
                description
                nodes {
                    id
                    type
                    name
                    config
                    inputs
                    outputs
                    language
                    position { x y }
                }
                connections {
                    fromNode
                    fromOutput
                    toNode
                    toInput
                }
                createdAt
                updatedAt
            }
        }
    `;

    try {
        const data = await graphqlQuery(query);
        flowClasses = data.flowClasses || [];
        renderFlowClasses();
    } catch (error) {
        console.error('Error loading flow classes:', error);
        showNotification('Failed to load flow classes', 'error');
    }
}

async function loadFlowInstances() {
    const query = `
        query {
            flowInstances {
                name
                namespace
                nodeId
                flowClassId
                inputMappings {
                    nodeInput
                    type
                    value
                }
                outputMappings {
                    nodeOutput
                    topic
                }
                variables
                enabled
                status {
                    running
                    lastExecution
                    executionCount
                    errorCount
                    lastError
                    subscribedTopics
                }
                createdAt
                updatedAt
                isOnCurrentNode
            }
        }
    `;

    try {
        const data = await graphqlQuery(query);
        flowInstances = data.flowInstances || [];
        renderFlowInstances();
    } catch (error) {
        console.error('Error loading flow instances:', error);
        showNotification('Failed to load flow instances', 'error');
    }
}

async function loadFlowNodeTypes() {
    const query = `
        query {
            flowNodeTypes {
                type
                category
                description
                defaultInputs
                defaultOutputs
                configSchema
                icon
            }
        }
    `;

    try {
        const data = await graphqlQuery(query);
        // Filter to show only function nodes
        flowNodeTypes = (data.flowNodeTypes || []).filter(nt => nt.type === 'function');
        renderNodePalette();
    } catch (error) {
        console.error('Error loading node types:', error);
    }
}

// ======================
// Render Functions
// ======================

function renderFlowClasses() {
    const grid = document.getElementById('classes-grid');
    if (!grid) return;

    if (flowClasses.length === 0) {
        grid.innerHTML = '<p style="grid-column: 1/-1; text-align: center; color: #6c757d;">No flow classes defined. Create your first workflow!</p>';
        return;
    }

    grid.innerHTML = flowClasses.map(flowClass => `
        <div class="workflow-card">
            <div class="workflow-card-header">
                <h3 class="workflow-card-title">${escapeHtml(flowClass.name)}</h3>
                <span class="workflow-card-badge">${escapeHtml(flowClass.version)}</span>
            </div>
            <div class="workflow-card-info">
                <div><strong>Namespace:</strong> ${escapeHtml(flowClass.namespace)}</div>
                ${flowClass.description ? `<div><strong>Description:</strong> ${escapeHtml(flowClass.description)}</div>` : ''}
                <div><strong>Nodes:</strong> ${flowClass.nodes.length}</div>
                <div><strong>Connections:</strong> ${flowClass.connections.length}</div>
            </div>
            <div class="workflow-card-actions">
                <button class="btn btn-primary" onclick="editFlowClass('${escapeHtml(flowClass.name)}')">
                    Edit
                </button>
                <button class="btn btn-danger" onclick="deleteFlowClass('${escapeHtml(flowClass.name)}')">
                    Delete
                </button>
            </div>
        </div>
    `).join('');
}

function renderFlowInstances() {
    const grid = document.getElementById('instances-grid');
    if (!grid) return;

    if (flowInstances.length === 0) {
        grid.innerHTML = '<p style="grid-column: 1/-1; text-align: center; color: #6c757d;">No flow instances defined. Create an instance of a flow class!</p>';
        return;
    }

    grid.innerHTML = flowInstances.map(instance => {
        const statusBadge = instance.enabled
            ? '<span class="workflow-card-badge enabled">Enabled</span>'
            : '<span class="workflow-card-badge disabled">Disabled</span>';

        const executionInfo = instance.status
            ? `<div><strong>Executions:</strong> ${instance.status.executionCount}</div>
               ${instance.status.errorCount > 0 ? `<div style="color: #dc3545;"><strong>Errors:</strong> ${instance.status.errorCount}</div>` : ''}`
            : '';

        return `
            <div class="workflow-card">
                <div class="workflow-card-header">
                    <h3 class="workflow-card-title">${escapeHtml(instance.name)}</h3>
                    ${statusBadge}
                </div>
                <div class="workflow-card-info">
                    <div><strong>Namespace:</strong> ${escapeHtml(instance.namespace)}</div>
                    <div><strong>Flow Class:</strong> ${escapeHtml(instance.flowClassId)}</div>
                    <div><strong>Node:</strong> ${escapeHtml(instance.nodeId)}</div>
                    <div><strong>Inputs:</strong> ${instance.inputMappings.length}</div>
                    <div><strong>Outputs:</strong> ${instance.outputMappings.length}</div>
                    ${executionInfo}
                </div>
                <div class="workflow-card-actions">
                    <button class="btn btn-primary" onclick="editFlowInstance('${escapeHtml(instance.name)}')">
                        Edit
                    </button>
                    <button class="btn btn-danger" onclick="deleteFlowInstance('${escapeHtml(instance.name)}')">
                        Delete
                    </button>
                </div>
            </div>
        `;
    }).join('');
}

function renderNodePalette() {
    const palette = document.querySelector('.node-palette');
    if (!palette) return;

    palette.innerHTML = '<h4>Node Types</h4>' + flowNodeTypes.map(nodeType => `
        <div class="palette-item" onclick="addNodeToCanvas('${nodeType.type}')">
            <div class="palette-icon">${nodeType.icon || '📦'}</div>
            <div class="palette-label">${nodeType.type}</div>
        </div>
    `).join('');
}

// ======================
// Flow Class Management
// ======================

function createFlowClass() {
    currentFlowClass = {
        name: '',
        namespace: 'default',
        version: '1.0.0',
        description: '',
        nodes: [],
        connections: []
    };

    nodes = [];
    connections = [];
    selectedNode = null;

    document.getElementById('class-name').value = '';
    document.getElementById('class-namespace').value = 'default';
    document.getElementById('class-description').value = '';

    // Open modal first
    document.getElementById('class-editor-modal').classList.add('active');

    // Re-initialize canvas and SVG layer now that modal is visible
    canvas = document.getElementById('flow-canvas');
    svgLayer = document.getElementById('connections-svg');

    renderNodes();
    renderConnections();
    updateNodeConfig();
}

function editFlowClass(name) {
    const flowClass = flowClasses.find(fc => fc.name === name);
    if (!flowClass) return;

    currentFlowClass = JSON.parse(JSON.stringify(flowClass)); // Deep copy
    nodes = currentFlowClass.nodes.map(node => ({
        ...node,
        position: node.position || { x: 100, y: 100 }
    }));
    connections = currentFlowClass.connections;
    selectedNode = null;

    document.getElementById('class-name').value = currentFlowClass.name;
    document.getElementById('class-namespace').value = currentFlowClass.namespace;
    document.getElementById('class-description').value = currentFlowClass.description || '';

    // Open modal first
    document.getElementById('class-editor-modal').classList.add('active');

    // Re-initialize canvas and SVG layer now that modal is visible
    canvas = document.getElementById('flow-canvas');
    svgLayer = document.getElementById('connections-svg');

    renderNodes();
    renderConnections();
    updateNodeConfig();
}

async function saveFlowClass() {
    const name = document.getElementById('class-name').value.trim();
    const namespace = document.getElementById('class-namespace').value.trim();
    const description = document.getElementById('class-description').value.trim();

    if (!name || !namespace) {
        showNotification('Name and namespace are required', 'error');
        return;
    }

    const input = {
        name,
        namespace,
        version: currentFlowClass?.version || '1.0.0',
        description: description || null,
        nodes: nodes.map(node => ({
            id: node.id,
            type: node.type,
            name: node.name,
            config: node.config,
            inputs: node.inputs,
            outputs: node.outputs,
            language: node.language || 'javascript',
            position: node.position
        })),
        connections: connections.map(conn => ({
            fromNode: conn.fromNode,
            fromOutput: conn.fromOutput,
            toNode: conn.toNode,
            toInput: conn.toInput
        }))
    };

    const mutation = currentFlowClass && currentFlowClass.name === name
        ? `mutation($name: String!, $input: FlowClassInput!) {
            flow {
                updateClass(name: $name, input: $input) {
                    name
                }
            }
        }`
        : `mutation($input: FlowClassInput!) {
            flow {
                createClass(input: $input) {
                    name
                }
            }
        }`;

    const variables = currentFlowClass && currentFlowClass.name === name
        ? { name: currentFlowClass.name, input }
        : { input };

    try {
        await graphqlQuery(mutation, variables);
        showNotification('Flow class saved successfully', 'success');
        closeClassEditor();
        await loadFlowClasses();
    } catch (error) {
        console.error('Error saving flow class:', error);
        showNotification('Failed to save flow class', 'error');
    }
}

async function deleteFlowClass(name) {
    if (!confirm(`Are you sure you want to delete flow class "${name}"?`)) {
        return;
    }

    const mutation = `
        mutation($name: String!) {
            flow {
                deleteClass(name: $name)
            }
        }
    `;

    try {
        await graphqlQuery(mutation, { name });
        showNotification('Flow class deleted successfully', 'success');
        await loadFlowClasses();
    } catch (error) {
        console.error('Error deleting flow class:', error);
        showNotification('Failed to delete flow class', 'error');
    }
}

function deleteCurrentClass() {
    if (currentFlowClass && currentFlowClass.name) {
        deleteFlowClass(currentFlowClass.name);
    }
}

function closeClassEditor() {
    document.getElementById('class-editor-modal').classList.remove('active');
    currentFlowClass = null;
    nodes = [];
    connections = [];
    selectedNode = null;
}

// ======================
// Visual Node Editor
// ======================

function addNodeToCanvas(type) {
    const nodeType = flowNodeTypes.find(nt => nt.type === type);
    if (!nodeType) return;

    const nodeId = 'node_' + Date.now();
    const node = {
        id: nodeId,
        type: type,
        name: type + '_' + nodes.length,
        config: {},
        inputs: [...nodeType.defaultInputs],
        outputs: [...nodeType.defaultOutputs],
        language: 'javascript',
        position: {
            x: 50 + (nodes.length * 30) % 400,
            y: 50 + Math.floor(nodes.length / 10) * 100
        }
    };

    nodes.push(node);
    renderNodes();
    selectNode(nodeId);
}

function renderNodes() {
    if (!canvas) return;

    // Remove old nodes
    canvas.querySelectorAll('.flow-node').forEach(el => el.remove());

    // Render new nodes
    nodes.forEach(node => {
        const nodeEl = document.createElement('div');
        nodeEl.className = 'flow-node' + (selectedNode === node.id ? ' selected' : '');
        nodeEl.style.left = node.position.x + 'px';
        nodeEl.style.top = node.position.y + 'px';
        nodeEl.setAttribute('data-node-id', node.id);

        nodeEl.innerHTML = `
            <div class="flow-node-header">${escapeHtml(node.name)}</div>
            <div class="flow-node-ports">
                <div class="flow-node-inputs">
                    ${node.inputs.map(input => `
                        <div class="node-port input-port"
                             title="Input port. ${connectingFrom ? (connectingFrom.portType==='output' ? 'Click to connect output → this input' : 'Outputs start connections') : 'Connections end here'}"
                             data-port="${escapeHtml(input)}"
                             onclick="startConnection('${node.id}', '${escapeHtml(input)}', 'input')">
                            <span>${escapeHtml(input)}</span><span class="port-hint-badge">IN</span>
                        </div>
                    `).join('')}
                </div>
                <div class="flow-node-outputs">
                    ${node.outputs.map(output => `
                        <div class="node-port output-port ${connectingFrom && connectingFrom.nodeId===node.id && connectingFrom.portName===output ? 'connecting' : ''}"
                             title="Output port. ${connectingFrom ? (connectingFrom.portType==='output' ? 'Select an input to finish' : 'Start from an output') : 'Click to start a connection'}"
                             data-port="${escapeHtml(output)}"
                             onclick="startConnection('${node.id}', '${escapeHtml(output)}', 'output')">
                            <span>${escapeHtml(output)}</span><span class="port-hint-badge">OUT</span>
                        </div>
                    `).join('')}
                </div>
            </div>
        `;

        nodeEl.addEventListener('mousedown', (e) => {
            if (e.target.closest('.node-port')) return; // Don't drag when clicking ports
            selectNode(node.id);
            draggedNode = node;
            e.stopPropagation();
        });

        canvas.appendChild(nodeEl);
    });

    renderConnections();
}

function selectNode(nodeId) {
    selectedNode = nodeId;
    renderNodes();
    updateNodeConfig();
}

function updateNodeConfig() {
    const configPanel = document.getElementById('node-config-content');
    if (!configPanel) return;

    if (!selectedNode) {
        configPanel.innerHTML = '<p style="color: #6c757d; padding: 1rem;">Select a node to configure</p>';
        return;
    }

    const node = nodes.find(n => n.id === selectedNode);
    if (!node) return;

    const nodeType = flowNodeTypes.find(nt => nt.type === node.type);
    const scriptRequired = node.type === 'function';

    configPanel.innerHTML = `
        <h4>Configure Node</h4>
        <div class="form-group">
            <label>Node Name:</label>
            <input type="text" id="node-name" class="form-control" value="${escapeHtml(node.name)}">
        </div>
        <div class="form-group">
            <label>Execution Type:</label>
            <select id="node-execution-type" class="form-control" onchange="toggleExecutionConfig()">
                <option value="event" ${(node.config.executionType || 'event') === 'event' ? 'selected' : ''}>Event-Driven (on input)</option>
                <option value="periodic" ${node.config.executionType === 'periodic' ? 'selected' : ''}>Periodic (timer-based)</option>
            </select>
        </div>
        <div id="periodic-config" style="display: ${node.config.executionType === 'periodic' ? 'block' : 'none'};">
            <div class="form-group">
                <label>Interval (milliseconds):</label>
                <input type="number" id="node-interval" class="form-control" value="${node.config.interval || 1000}" min="100" step="100">
                <small style="color: #6c757d;">Minimum: 100ms. Example: 1000 = 1 second, 60000 = 1 minute</small>
            </div>
        </div>
        <div class="form-group">
            <label>Inputs (comma-separated):</label>
            <input type="text" id="node-inputs" class="form-control" value="${node.inputs.join(', ')}">
            <small style="color: #6c757d;">Leave empty for periodic execution without inputs</small>
        </div>
        <div class="form-group">
            <label>Outputs (comma-separated):</label>
            <input type="text" id="node-outputs" class="form-control" value="${node.outputs.join(', ')}">
        </div>
        ${scriptRequired ? `
            <div class="form-group">
                <label>Language:</label>
                <select id="node-language" class="form-control">
                    <option value="javascript" ${node.language === 'javascript' ? 'selected' : ''}>JavaScript</option>
                    <option value="python" ${node.language === 'python' ? 'selected' : ''}>Python</option>
                </select>
            </div>
            <div class="form-group">
                <label>Script:</label>
                <button class="btn btn-secondary" onclick="openScriptEditor()" style="width: 100%; margin-bottom: 0.5rem;">
                    <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="vertical-align: middle; margin-right: 0.5rem;">
                        <rect x="3" y="3" width="18" height="18" rx="2" ry="2"></rect>
                        <line x1="9" y1="3" x2="9" y2="21"></line>
                    </svg>
                    Open Full Editor
                </button>
                <textarea id="node-script" class="form-control" rows="5" style="font-family: monospace; font-size: 0.85rem;">${escapeHtml(node.config.script || '')}</textarea>
                <small style="color: #6c757d;">
                    Available: inputs (object), state (object), variables (object), outputs.send(port, value)
                </small>
            </div>
        ` : ''}
        <div class="form-group" style="display: flex; gap: 0.5rem;">
            <button class="btn btn-primary" onclick="saveNodeConfig()">Save</button>
            <button class="btn btn-danger" onclick="deleteSelectedNode()">Delete Node</button>
        </div>
    `;
}

function toggleExecutionConfig() {
    const executionType = document.getElementById('node-execution-type').value;
    const periodicConfig = document.getElementById('periodic-config');
    if (periodicConfig) {
        periodicConfig.style.display = executionType === 'periodic' ? 'block' : 'none';
    }
}

function saveNodeConfig() {
    if (!selectedNode) return;

    const node = nodes.find(n => n.id === selectedNode);
    if (!node) return;

    node.name = document.getElementById('node-name')?.value || node.name;
    node.inputs = document.getElementById('node-inputs')?.value.split(',').map(s => s.trim()).filter(s => s) || node.inputs;
    node.outputs = document.getElementById('node-outputs')?.value.split(',').map(s => s.trim()).filter(s => s) || node.outputs;

    if (node.type === 'function') {
        node.language = document.getElementById('node-language')?.value || 'javascript';
        node.config.script = document.getElementById('node-script')?.value || '';
        node.config.executionType = document.getElementById('node-execution-type')?.value || 'event';

        if (node.config.executionType === 'periodic') {
            const interval = parseInt(document.getElementById('node-interval')?.value || '1000');
            node.config.interval = Math.max(100, interval); // Minimum 100ms
        } else {
            delete node.config.interval;
        }
    }

    renderNodes();
    showNotification('Node configuration saved', 'success');
}

function deleteSelectedNode() {
    if (!selectedNode) return;

    // Remove node
    nodes = nodes.filter(n => n.id !== selectedNode);

    // Remove connections involving this node
    connections = connections.filter(conn =>
        conn.fromNode !== selectedNode && conn.toNode !== selectedNode
    );

    selectedNode = null;
    renderNodes();
    updateNodeConfig();
    showNotification('Node deleted', 'success');
}

function handleCanvasMouseDown(e) {
    if (e.target === canvas) {
        selectedNode = null;
        renderNodes();
        updateNodeConfig();
    }
}

function handleCanvasMouseMove(e) {
    if (!draggedNode) return;

    const rect = canvas.getBoundingClientRect();
    draggedNode.position.x = e.clientX - rect.left - 75; // Center on cursor
    draggedNode.position.y = e.clientY - rect.top - 20;

    renderNodes();
}

function handleCanvasMouseUp(e) {
    draggedNode = null;
}

// ======================
// Connection Management
// ======================

function startConnection(nodeId, portName, portType) {
    if (!connectingFrom) {
        // Start connection
        if (portType === 'output') {
            connectingFrom = { nodeId, portName, portType };
            showNotification('Click on an input port to complete connection', 'info');
            // Add valid-target class to all input ports
            canvas.querySelectorAll('.input-port').forEach(el => {
                el.classList.add('valid-target');
            });
            renderNodes();
        } else {
            showNotification('Connections must start from output ports', 'error');
        }
    } else {
        // Complete connection
        if (portType === 'input' && connectingFrom.portType === 'output') {
            const connection = {
                fromNode: connectingFrom.nodeId,
                fromOutput: connectingFrom.portName,
                toNode: nodeId,
                toInput: portName
            };

            // Check for duplicate
            const exists = connections.some(c =>
                c.fromNode === connection.fromNode &&
                c.fromOutput === connection.fromOutput &&
                c.toNode === connection.toNode &&
                c.toInput === connection.toInput
            );

            if (!exists) {
                connections.push(connection);
                renderConnections();
                showNotification('Connection created', 'success');
            }
        } else {
            showNotification('Invalid connection', 'error');
        }
        connectingFrom = null;
        // Remove valid-target highlight
        canvas.querySelectorAll('.input-port.valid-target').forEach(el => el.classList.remove('valid-target'));
        renderNodes();
    }
}

function renderConnections() {
    if (!svgLayer || !canvas) return;

    svgLayer.innerHTML = '';

    connections.forEach((conn, idx) => {
        const fromNode = nodes.find(n => n.id === conn.fromNode);
        const toNode = nodes.find(n => n.id === conn.toNode);

        if (!fromNode || !toNode) return;

        // Calculate port positions
        const fromEl = canvas.querySelector(`[data-node-id="${conn.fromNode}"]`);
        const toEl = canvas.querySelector(`[data-node-id="${conn.toNode}"]`);

        if (!fromEl || !toEl) return;

        const fromRect = fromEl.getBoundingClientRect();
        const toRect = toEl.getBoundingClientRect();
        const canvasRect = canvas.getBoundingClientRect();

        const x1 = fromRect.right - canvasRect.left;
        const y1 = fromRect.top - canvasRect.top + 40;
        const x2 = toRect.left - canvasRect.left;
        const y2 = toRect.top - canvasRect.top + 40;

        // Create curved path
        const dx = x2 - x1;
        const dy = y2 - y1;
        const curve = Math.min(Math.abs(dx) / 2, 50);

        const path = document.createElementNS('http://www.w3.org/2000/svg', 'path');
        path.setAttribute('d', `M ${x1} ${y1} C ${x1 + curve} ${y1}, ${x2 - curve} ${y2}, ${x2} ${y2}`);
        path.setAttribute('class', 'connection-line');
        path.style.cursor = 'pointer';
        path.onclick = () => deleteConnection(idx);

        svgLayer.appendChild(path);
    });
}

function deleteConnection(index) {
    if (confirm('Delete this connection?')) {
        connections.splice(index, 1);
        renderConnections();
        showNotification('Connection deleted', 'success');
    }
}

// ======================
// Flow Instance Management
// ======================

function createFlowInstance() {
    if (flowClasses.length === 0) {
        showNotification('Create a flow class first', 'error');
        return;
    }

    currentFlowInstance = {
        name: '',
        namespace: 'default',
        nodeId: 'local',
        flowClassId: flowClasses[0].name,
        inputMappings: [],
        outputMappings: [],
        variables: {},
        enabled: true
    };

    populateInstanceForm();
    document.getElementById('instance-editor-modal').classList.add('active');
}

function editFlowInstance(name) {
    const instance = flowInstances.find(fi => fi.name === name);
    if (!instance) return;

    currentFlowInstance = JSON.parse(JSON.stringify(instance));
    populateInstanceForm();
    document.getElementById('instance-editor-modal').classList.add('active');
}

function populateInstanceForm() {
    document.getElementById('instance-name').value = currentFlowInstance.name;
    document.getElementById('instance-namespace').value = currentFlowInstance.namespace;
    document.getElementById('instance-node-id').value = currentFlowInstance.nodeId;
    document.getElementById('instance-enabled').checked = currentFlowInstance.enabled;

    // Populate flow class dropdown
    const select = document.getElementById('instance-flow-class');
    select.innerHTML = flowClasses.map(fc =>
        `<option value="${escapeHtml(fc.name)}" ${fc.name === currentFlowInstance.flowClassId ? 'selected' : ''}>
            ${escapeHtml(fc.name)}
        </option>`
    ).join('');

    renderInputMappings();
    renderOutputMappings();
}

function renderInputMappings() {
    const container = document.getElementById('input-mappings');
    if (!container) return;
    // Header
    let html = '<h4 style="margin-bottom:0.5rem;">Input Mappings</h4>';
    const flowClass = flowClasses.find(fc => fc.name === currentFlowInstance.flowClassId);
    if (!flowClass) {
        container.innerHTML = html + '<p style="color:#6c757d;">Select a flow class first</p>';
        return;
    }

    html += '<div class="mapping-table-wrapper"><table class="mapping-table"><thead><tr>' +
        '<th style="width:25%">Node Input</th>' +
        '<th style="width:15%">Type</th>' +
        '<th style="width:45%">Value</th>' +
        '<th style="width:15%">Actions</th>' +
        '</tr></thead><tbody>';

    if (currentFlowInstance.inputMappings.length === 0) {
        html += '<tr><td colspan="4" style="text-align:center; color:#6c757d; font-style:italic;">No input mappings</td></tr>';
    } else {
        currentFlowInstance.inputMappings.forEach((mapping, idx) => {
            html += `<tr>
                <td><input type="text" value="${escapeHtml(mapping.nodeInput)}" placeholder="nodeId.input" onchange="updateInputMapping(${idx}, 'nodeInput', this.value)"></td>
                <td><select onchange="updateInputMapping(${idx}, 'type', this.value)">
                        <option value="TOPIC" ${mapping.type === 'TOPIC' ? 'selected' : ''}>TOPIC</option>
                    </select></td>
                <td><input type="text" value="${escapeHtml(mapping.value)}" placeholder="sensor/#" onchange="updateInputMapping(${idx}, 'value', this.value)">
                    <div style="font-size:0.6rem; color:#17a2b8; margin-top:2px;">Wildcards: # multi, + single</div>
                </td>
                <td><div class="table-actions"><button class="btn-icon-small" title="Duplicate" onclick="duplicateInputMapping(${idx})">⧉</button><button class="btn-icon-small" title="Remove" onclick="removeInputMapping(${idx})">✕</button></div></td>
            </tr>`;
        });
    }

    html += '</tbody></table></div>';
    html += '<div class="add-row-bar"><button class="btn btn-secondary" onclick="addInputMapping()">+ Add Input</button></div>';
    container.innerHTML = html;
}

function addInputMapping() {
    currentFlowInstance.inputMappings.push({
        nodeInput: '',
        type: 'TOPIC',
        value: ''
    });
    renderInputMappings();
}

function updateInputMapping(idx, field, value) {
    if (currentFlowInstance.inputMappings[idx]) {
        currentFlowInstance.inputMappings[idx][field] = value;
        if (field === 'type') {
            renderInputMappings(); // Re-render to update placeholder
        }
    }
}

function removeInputMapping(idx) {
    currentFlowInstance.inputMappings.splice(idx, 1);
    renderInputMappings();
}

function duplicateInputMapping(idx) {
    const src = currentFlowInstance.inputMappings[idx];
    if (src) {
        currentFlowInstance.inputMappings.splice(idx + 1, 0, { ...src });
        renderInputMappings();
    }
}

function renderOutputMappings() {
    const container = document.getElementById('output-mappings');
    if (!container) return;
    let html = '<h4 style="margin-bottom:0.5rem;">Output Mappings</h4>';

    html += '<div class="mapping-table-wrapper"><table class="mapping-table"><thead><tr>' +
        '<th style="width:35%">Node Output</th>' +
        '<th style="width:50%">MQTT Topic</th>' +
        '<th style="width:15%">Actions</th>' +
        '</tr></thead><tbody>';

    if (currentFlowInstance.outputMappings.length === 0) {
        html += '<tr><td colspan="3" style="text-align:center; color:#6c757d; font-style:italic;">No output mappings</td></tr>';
    } else {
        currentFlowInstance.outputMappings.forEach((mapping, idx) => {
            html += `<tr>
                <td><input type="text" value="${escapeHtml(mapping.nodeOutput)}" placeholder="nodeId.output" onchange="updateOutputMapping(${idx}, 'nodeOutput', this.value)"></td>
                <td><input type="text" value="${escapeHtml(mapping.topic)}" placeholder="sensor/output" onchange="updateOutputMapping(${idx}, 'topic', this.value)"></td>
                <td><div class="table-actions"><button class="btn-icon-small" title="Duplicate" onclick="duplicateOutputMapping(${idx})">⧉</button><button class="btn-icon-small" title="Remove" onclick="removeOutputMapping(${idx})">✕</button></div></td>
            </tr>`;
        });
    }

    html += '</tbody></table></div>';
    html += '<div class="add-row-bar"><button class="btn btn-secondary" onclick="addOutputMapping()">+ Add Output</button></div>';
    container.innerHTML = html;
}

function addOutputMapping() {
    currentFlowInstance.outputMappings.push({
        nodeOutput: '',
        topic: ''
    });
    renderOutputMappings();
}

function updateOutputMapping(idx, field, value) {
    if (currentFlowInstance.outputMappings[idx]) {
        currentFlowInstance.outputMappings[idx][field] = value;
    }
}

function removeOutputMapping(idx) {
    currentFlowInstance.outputMappings.splice(idx, 1);
    renderOutputMappings();
}

function duplicateOutputMapping(idx) {
    const src = currentFlowInstance.outputMappings[idx];
    if (src) {
        currentFlowInstance.outputMappings.splice(idx + 1, 0, { ...src });
        renderOutputMappings();
    }
}

async function saveFlowInstance() {
    const name = document.getElementById('instance-name').value.trim();
    const namespace = document.getElementById('instance-namespace').value.trim();
    const nodeId = document.getElementById('instance-node-id').value.trim();
    const flowClassId = document.getElementById('instance-flow-class').value;
    const enabled = document.getElementById('instance-enabled').checked;

    if (!name || !namespace || !nodeId || !flowClassId) {
        showNotification('All required fields must be filled', 'error');
        return;
    }

    // Validate wildcard topics
    const topicMappings = currentFlowInstance.inputMappings.filter(m => m.type === 'TOPIC');
    for (const mapping of topicMappings) {
        if (!validateMqttTopic(mapping.value)) {
            showNotification(`Invalid MQTT topic: ${mapping.value}`, 'error');
            return;
        }
    }

    const input = {
        name,
        namespace,
        nodeId,
        flowClassId,
        inputMappings: currentFlowInstance.inputMappings.map(m => ({
            nodeInput: m.nodeInput,
            type: m.type,
            value: m.value
        })),
        outputMappings: currentFlowInstance.outputMappings.map(m => ({
            nodeOutput: m.nodeOutput,
            topic: m.topic
        })),
        variables: currentFlowInstance.variables || {},
        enabled
    };

    const mutation = currentFlowInstance && flowInstances.some(fi => fi.name === currentFlowInstance.name && fi.name === name)
        ? `mutation($name: String!, $input: FlowInstanceInput!) {
            flow {
                updateInstance(name: $name, input: $input) {
                    name
                }
            }
        }`
        : `mutation($input: FlowInstanceInput!) {
            flow {
                createInstance(input: $input) {
                    name
                }
            }
        }`;

    const variables = currentFlowInstance && flowInstances.some(fi => fi.name === currentFlowInstance.name && fi.name === name)
        ? { name: currentFlowInstance.name, input }
        : { input };

    try {
        await graphqlQuery(mutation, variables);
        showNotification('Flow instance saved successfully', 'success');
        closeInstanceEditor();
        await loadFlowInstances();
    } catch (error) {
        console.error('Error saving flow instance:', error);
        showNotification('Failed to save flow instance', 'error');
    }
}

async function deleteFlowInstance(name) {
    if (!confirm(`Are you sure you want to delete flow instance "${name}"?`)) {
        return;
    }

    const mutation = `
        mutation($name: String!) {
            flow {
                deleteInstance(name: $name)
            }
        }
    `;

    try {
        await graphqlQuery(mutation, { name });
        showNotification('Flow instance deleted successfully', 'success');
        await loadFlowInstances();
    } catch (error) {
        console.error('Error deleting flow instance:', error);
        showNotification('Failed to delete flow instance', 'error');
    }
}

function deleteCurrentInstance() {
    if (currentFlowInstance && currentFlowInstance.name) {
        deleteFlowInstance(currentFlowInstance.name);
    }
}

function closeInstanceEditor() {
    document.getElementById('instance-editor-modal').classList.remove('active');
    currentFlowInstance = null;
}

// ======================
// Utility Functions
// ======================

function validateMqttTopic(topic) {
    if (!topic) return false;

    // MQTT topic validation rules:
    // - Cannot be empty
    // - Cannot start or end with /
    // - + wildcard matches single level
    // - # wildcard matches multiple levels (must be last character)
    // - # can only appear at end preceded by /

    if (topic.startsWith('/') || topic.endsWith('/')) return false;

    const hashIndex = topic.indexOf('#');
    if (hashIndex !== -1 && hashIndex !== topic.length - 1) return false;
    if (hashIndex > 0 && topic[hashIndex - 1] !== '/') return false;

    return true;
}

function escapeHtml(text) {
    if (text === null || text === undefined) return '';
    const div = document.createElement('div');
    div.textContent = String(text);
    return div.innerHTML;
}

function showNotification(message, type = 'info') {
    // Simple notification system
    const notification = document.createElement('div');
    notification.className = `notification notification-${type}`;
    notification.textContent = message;
    notification.style.cssText = `
        position: fixed;
        top: 20px;
        right: 20px;
        padding: 1rem 1.5rem;
        background: ${type === 'success' ? '#28a745' : type === 'error' ? '#dc3545' : '#17a2b8'};
        color: white;
        border-radius: 4px;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        z-index: 10000;
        animation: slideIn 0.3s ease;
    `;

    document.body.appendChild(notification);

    setTimeout(() => {
        notification.style.animation = 'slideOut 0.3s ease';
        setTimeout(() => notification.remove(), 300);
    }, 3000);
}

// ======================
// Script Editor Modal
// ======================

function openScriptEditor() {
    if (!selectedNode) return;

    const node = nodes.find(n => n.id === selectedNode);
    if (!node) return;

    // Sync with inline editor before opening
    const inlineScript = document.getElementById('node-script');
    if (inlineScript) {
        node.config.script = inlineScript.value;
    }

    const modal = document.getElementById('script-editor-modal');
    const textarea = document.getElementById('script-editor-textarea');
    const languageSelect = document.getElementById('script-editor-language');
    const nodeName = document.getElementById('script-editor-node-name');

    if (modal && textarea) {
        textarea.value = node.config.script || '';
        languageSelect.value = node.language || 'javascript';
        nodeName.textContent = node.name;
        modal.classList.add('active');
        textarea.focus();
    }
}

function closeScriptEditor() {
    const modal = document.getElementById('script-editor-modal');
    if (modal) {
        modal.classList.remove('active');
    }
}

function saveScriptFromEditor() {
    if (!selectedNode) return;

    const node = nodes.find(n => n.id === selectedNode);
    if (!node) return;

    const textarea = document.getElementById('script-editor-textarea');
    const languageSelect = document.getElementById('script-editor-language');

    if (textarea && languageSelect) {
        node.config.script = textarea.value;
        node.language = languageSelect.value;

        // Update inline editor too
        const inlineScript = document.getElementById('node-script');
        const inlineLanguage = document.getElementById('node-language');
        if (inlineScript) inlineScript.value = textarea.value;
        if (inlineLanguage) inlineLanguage.value = languageSelect.value;

        showNotification('Script saved', 'success');
        closeScriptEditor();
    }
}

// Add animation keyframes
const style = document.createElement('style');
style.textContent = `
    @keyframes slideIn {
        from { transform: translateX(400px); opacity: 0; }
        to { transform: translateX(0); opacity: 1; }
    }
    @keyframes slideOut {
        from { transform: translateX(0); opacity: 1; }
        to { transform: translateX(400px); opacity: 0; }
    }
`;
document.head.appendChild(style);
