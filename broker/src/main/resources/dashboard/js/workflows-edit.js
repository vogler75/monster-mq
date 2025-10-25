// Workflows Edit Page Script
// Handles create/update for Flow Classes and Flow Instances without modals.

const FlowEdit = (() => {
  const state = {
    type: null, // 'class' | 'instance'
    name: null,
    flowClass: null,
    flowInstance: null,
    flowClasses: [],
    clusterNodes: [],
    // For class editing
    nodes: [],
    connections: [],
    // For instance editing
    inputMappings: [],
    outputMappings: [],
    variables: {},
    dirty: false
  };

  function qs(sel){ return document.querySelector(sel); }
  function qsa(sel){ return Array.from(document.querySelectorAll(sel)); }

  function parseParams() {
    const p = new URLSearchParams(location.search);
    state.type = p.get('type');
    state.name = p.get('name');
  }

  async function init() {
    parseParams();
    if(!['class','instance'].includes(state.type)) {
      qs('#page-title').textContent = 'Invalid parameters';
      return;
    }
    if(state.type === 'class') {
      await loadAllForClass();
      buildClassForm();
    } else {
      await loadAllForInstance();
      buildInstanceForm();
    }
    qs('#header-actions').style.display = 'flex';
  }

  async function graphql(query, variables={}) {
    try { return await window.graphqlClient.query(query, variables); }
    catch(e){ notify('GraphQL error: '+e.message,'error'); throw e; }
  }

  async function loadClusterNodes() {
    try {
      const result = await graphql(`query GetBrokers { brokers { nodeId isCurrent } }`);
      state.clusterNodes = result.brokers || [];
    } catch(e) {
      console.error('Failed to load cluster nodes:', e);
      state.clusterNodes = [];
    }
  }

  function populateNodeSelect() {
    const nodeSelect = qs('#fi-nodeId');
    if(!nodeSelect || nodeSelect.tagName !== 'SELECT') return;

    // Clear existing options except the first one
    while(nodeSelect.options.length > 1) nodeSelect.remove(1);

    // Add "*" option for automatic assignment
    const autoOption = document.createElement('option');
    autoOption.value = '*';
    autoOption.textContent = '* (Automatic Assignment)';
    nodeSelect.appendChild(autoOption);

    // Add cluster nodes
    state.clusterNodes.forEach(node => {
      const option = document.createElement('option');
      option.value = node.nodeId;
      option.textContent = node.nodeId + (node.isCurrent ? ' (Current)' : '');
      nodeSelect.appendChild(option);
    });
  }

  async function loadAllForClass() {
    if(state.name) {
      // Removed unused required variable from original query which caused runtime error.
      // Query only needed all classes; filter client-side. Alternatively, we could query with name filter.
  // Note: connections belong inside each flowClass selection; adjust query accordingly.
  const query = `query { flowClasses { name namespace version description nodes { id type name config inputs outputs language position { x y } } connections { fromNode fromOutput toNode toInput } } }`;
      let data;
      try {
        data = await graphql(query);
      } catch(e) {
        console.error('Failed to load flow classes for editing', e);
        throw e;
      }
      const list = (data && data.flowClasses) || [];
      state.flowClass = list.find(fc => fc.name === state.name) || null;
      if(state.flowClass) {
        state.nodes = state.flowClass.nodes.map(n => ({ ...n }));
        state.connections = state.flowClass.connections.map(c => ({ ...c }));
      } else {
        notify('Flow class not found','error');
        // Provide a safe default object so buildClassForm does not crash on null access.
        state.flowClass = { name: state.name, namespace:'default', version:'1.0.0', description:'', nodes:[], connections:[] };
        state.nodes = [];
        state.connections = [];
      }
    } else {
      state.flowClass = { name:'', namespace:'default', version:'1.0.0', description:'', nodes:[], connections:[] };
      state.nodes = [];
      state.connections = [];
    }
  }

  async function loadAllForInstance() {
    // Load cluster nodes for Node ID dropdown
    await loadClusterNodes();

    // Need flow classes for dropdown
  const classesQuery = `query { flowClasses { name namespace version nodes { id inputs outputs } connections { fromNode fromOutput toNode toInput } } }`;
    const classData = await graphql(classesQuery);
    state.flowClasses = classData.flowClasses || [];

    if(state.name) {
      // Load all instances and pick the one we need (backend currently only supports list query)
      const instQuery = `query { flowInstances { name namespace nodeId flowClassId inputMappings { nodeInput type value } outputMappings { nodeOutput topic } variables enabled status { executionCount errorCount } } }`;
      const instData = await graphql(instQuery);
      const list = instData.flowInstances || [];
      state.flowInstance = list.find(fi => fi.name === state.name) || null;
      if(state.flowInstance) {
        state.inputMappings = state.flowInstance.inputMappings.map(m=>({...m}));
        state.outputMappings = state.flowInstance.outputMappings.map(m=>({...m}));
        state.variables = {...(state.flowInstance.variables||{})};
      } else {
        notify('Flow instance not found','error');
        state.flowInstance = { name: state.name, namespace:'default', nodeId:'local', flowClassId: state.flowClasses[0]?.name || '', inputMappings:[], outputMappings:[], variables:{}, enabled:true };
      }
    } else {
      state.flowInstance = { name:'', namespace:'default', nodeId:'local', flowClassId: state.flowClasses[0]?.name || '', inputMappings:[], outputMappings:[], variables:{}, enabled:true };
    }
  }

  function buildClassForm() {
    const editing = !!state.name;
    qs('#page-title').textContent = editing? 'Edit Flow Class' : 'New Flow Class';
    qs('#page-subtitle').textContent = editing? state.name : 'Create a new flow class';
    qs('#delete-button').style.display = editing ? 'inline-block' : 'none';

    // Remove enabled checkbox if it exists (when switching from instance to class)
    const headerActions = qs('#header-actions');
    const existingCheckbox = headerActions.querySelector('.header-checkbox-wrapper');
    if (existingCheckbox) {
      existingCheckbox.remove();
    }

    // Add restart button for existing flow classes
    const existingRestartBtn = headerActions.querySelector('.restart-instances-btn');
    if (existingRestartBtn) {
      existingRestartBtn.remove();
    }
    if (editing) {
      const restartBtn = document.createElement('button');
      restartBtn.className = 'btn btn-secondary btn-small restart-instances-btn';
      restartBtn.textContent = 'Restart All Instances';
      restartBtn.style.whiteSpace = 'nowrap';
      restartBtn.onclick = () => FlowEdit.restartAllInstances();
      // Insert right after save button (before delete button if it exists)
      const deleteBtn = qs('#delete-button');
      if (deleteBtn) {
        deleteBtn.parentNode.insertBefore(restartBtn, deleteBtn);
      } else {
        headerActions.appendChild(restartBtn);
      }
    }
    
    const root = qs('#form-section');
    root.innerHTML = `
      <div class="section-card">
        <div class="section-header"><h2>Flow Class Details</h2></div>
        <div class="section-content">
          <div class="form-grid">
            <div class="form-group"><label>Name</label><input id="fc-name" class="form-control" value="${escape(state.flowClass.name||'')}"></div>
            <div class="form-group"><label>Namespace</label><input id="fc-namespace" class="form-control" value="${escape(state.flowClass.namespace||'default')}"></div>
            <div class="form-group"><label>Version</label><input id="fc-version" class="form-control" value="${escape(state.flowClass.version||'1.0.0')}"></div>
            <div class="form-group" style="grid-column:1/-1;"><label>Description</label><textarea id="fc-description" class="form-control" rows="2">${escape(state.flowClass.description||'')}</textarea></div>
          </div>
        </div>
      </div>`;
    qs('#nodes-section').style.display = 'block';
    qs('#connections-section').style.display = 'block';
    renderNodesTable();
    renderConnectionsTable();

    // Auto-open editor for single node flows
    if (state.nodes.length === 1) {
      editNode(state.nodes[0].id);
    }
  }

  function buildInstanceForm() {
    const editing = !!state.name;
    qs('#page-title').textContent = editing? 'Edit Flow Instance' : 'New Flow Instance';
    qs('#page-subtitle').textContent = editing? state.name : 'Create a new flow instance';
    qs('#delete-button').style.display = editing ? 'inline-block' : 'none';
    
    const root = qs('#form-section');
    const classOptions = state.flowClasses.map(fc => `<option value="${escape(fc.name)}" ${fc.name===state.flowInstance.flowClassId?'selected':''}>${escape(fc.name)}</option>`).join('');
    root.innerHTML = `
      <div class="section-card">
        <div class="section-header">
          <h2>Flow Instance Details</h2>
          <div class="header-checkbox-wrapper">
            <label class="header-checkbox">
              <input type="checkbox" id="fi-enabled" ${state.flowInstance.enabled?'checked':''}>
              <span>Enabled</span>
            </label>
          </div>
        </div>
        <div class="section-content">
          <div class="form-grid">
            <div class="form-group"><label>Name</label><input id="fi-name" class="form-control" value="${escape(state.flowInstance.name||'')}"></div>
            <div class="form-group"><label>Namespace</label><input id="fi-namespace" class="form-control" value="${escape(state.flowInstance.namespace||'default')}"></div>
            <div class="form-group"><label>Node ID</label><select id="fi-nodeId" class="form-control"><option value="">Select Node...</option></select></div>
            <div class="form-group"><label>Flow Class</label><select id="fi-flowClass" class="form-control">${classOptions}</select></div>
          </div>
        </div>
      </div>`;
    qs('#mappings-section').style.display = 'block';
    computeAvailableIO();
    renderInputMappings();
    renderOutputMappings();
    renderVariables();
    const flowClassSelect = qs('#fi-flowClass');
    flowClassSelect.addEventListener('change', ()=>{
      state.flowInstance.flowClassId = flowClassSelect.value;
      computeAvailableIO();
      // prune invalid existing mappings
      state.inputMappings = state.inputMappings.filter(m=>availableInputs.includes(m.nodeInput));
      state.outputMappings = state.outputMappings.filter(m=>availableOutputs.includes(m.nodeOutput));
      renderInputMappings();
      renderOutputMappings();
    });

    // Populate Node ID dropdown and set current value
    populateNodeSelect();
    const nodeIdSelect = qs('#fi-nodeId');
    if(nodeIdSelect) {
      nodeIdSelect.value = state.flowInstance.nodeId || '*';
    }
  }

  let availableInputs = [];
  let availableOutputs = [];
  function computeAvailableIO(){
    const fc = state.flowClasses.find(c=>c.name===state.flowInstance.flowClassId);
    if(!fc){ availableInputs=[]; availableOutputs=[]; return; }
    availableInputs = [];
    availableOutputs = [];
    fc.nodes.forEach(n=>{
      (n.inputs||[]).forEach(inp=> availableInputs.push(`${n.id}.${inp}`));
      (n.outputs||[]).forEach(out=> availableOutputs.push(`${n.id}.${out}`));
    });
  }

  function renderNodesTable() {
    const tbody = qs('#nodes-table tbody');
    tbody.innerHTML = state.nodes.map(n => `<tr>
      <td>${escape(n.id)}</td>
      <td>${escape(n.name)}</td>
      <td>${n.inputs.length}</td>
      <td>${n.outputs.length}</td>
      <td>${escape(n.language||'js')}</td>
      <td class="action-buttons">
        <button class="btn-icon btn-view" onclick="FlowEdit.editNode('${n.id}')" title="Edit Node">
          <svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor">
            <path d="M3 17.25V21h3.75L17.81 9.94l-3.75-3.75L3 17.25zM20.71 7.04c.39-.39.39-1.02 0-1.41l-2.34-2.34c-.39-.39-1.02-.39-1.41 0l-1.83 1.83 3.75 3.75 1.83-1.83z"/>
          </svg>
        </button>
        <button class="btn-icon btn-delete" onclick="FlowEdit.removeNode('${n.id}')" title="Delete Node">
          <svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor">
            <path d="M6 19c0 1.1.9 2 2 2h8c1.1 0 2-.9 2-2V7H6v12zM19 4h-3.5l-1-1h-5l-1 1H5v2h14V4z"/>
          </svg>
        </button>
      </td>
    </tr>`).join('');
  }

  function renderConnectionsTable() {
    const tbody = qs('#connections-table tbody');
    tbody.innerHTML = state.connections.map((c,idx) => `<tr>
      <td>${escape(c.fromNode)}</td><td>${escape(c.fromOutput)}</td><td>${escape(c.toNode)}</td><td>${escape(c.toInput)}</td>
      <td>
        <button class="btn-icon btn-delete" onclick="FlowEdit.removeConnection(${idx})" title="Delete Connection">
          <svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor">
            <path d="M6 19c0 1.1.9 2 2 2h8c1.1 0 2-.9 2-2V7H6v12zM19 4h-3.5l-1-1h-5l-1 1H5v2h14V4z"/>
          </svg>
        </button>
      </td></tr>`).join('');
  }

  function addConnectionRow() {
    state.connections.push({ fromNode:'', fromOutput:'', toNode:'', toInput:'' });
    renderConnectionsTable();
    // make editable inputs by converting cells
    const tbody = qs('#connections-table tbody');
    const last = tbody.lastElementChild;
    if(last) {
      ['fromNode','fromOutput','toNode','toInput'].forEach((field,i) => {
        const cell = last.children[i];
        const input = document.createElement('input');
        input.value = '';
        input.className = 'form-control';
        input.onchange = e => { state.connections[state.connections.length-1][field] = e.target.value.trim(); };
        cell.textContent='';
        cell.appendChild(input);
      });
    }
  }

  function editNode(id) {
    const node = state.nodes.find(n=>n.id===id); if(!node) return;
    const box = qs('#node-inline-editor');
    box.style.display='block';
    box.innerHTML = `
      <h3 style="margin-top:0;">Edit Node</h3>
      <div class="form-grid">
        <div class="form-group"><label>ID</label><input id="n-id" class="form-control" value="${escape(node.id)}" disabled></div>
        <div class="form-group"><label>Name</label><input id="n-name" class="form-control" value="${escape(node.name)}"></div>
        <div class="form-group"><label>Language</label><select id="n-language" class="form-control"><option value="javascript" ${node.language==='javascript'?'selected':''}>JavaScript</option></select></div>
        <div class="form-group" style="grid-column:1/-1;"><label>Inputs (comma)</label><input id="n-inputs" class="form-control" value="${escape(node.inputs.join(', '))}"></div>
        <div class="form-group" style="grid-column:1/-1;"><label>Outputs (comma)</label><input id="n-outputs" class="form-control" value="${escape(node.outputs.join(', '))}"></div>
        <div class="form-group" style="grid-column:1/-1;">
          <label style="display:flex; justify-content:space-between; align-items:center;">
            <span>Script</span>
            <button class="btn btn-secondary btn-small" style="padding:0.35rem 0.75rem; font-size:0.75rem;" onclick="FlowEdit.openScriptEditor('${node.id}')" title="Open full-screen script editor">
              <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="vertical-align:middle; margin-right:0.25rem;">
                <path d="M21 2v6h-6M3 12a9 9 0 0 1 15-6.7L21 8M3 22v-6h6M21 12a9 9 0 0 1-15 6.7L3 16"></path>
              </svg>
              Expand Editor
            </button>
          </label>
          <textarea id="n-script" class="script">${escape(node.config?.script||'')}</textarea>
        </div>
      </div>
      <div style="margin-top:1rem; display:flex; gap:.5rem;">
        <button class="btn btn-primary" onclick="FlowEdit.saveNode('${node.id}')">Save Node</button>
        <button class="btn btn-secondary" onclick="FlowEdit.cancelNodeEdit()">Cancel</button>
      </div>`;
  }

  function addNode() {
    const id = 'node_'+Date.now();
    state.nodes.push({ id, type:'function', name:id, inputs:[], outputs:['out'], config:{script:''}, language:'javascript', position:null });
    renderNodesTable();
    editNode(id);
  }

  function saveNode(id) {
    const node = state.nodes.find(n=>n.id===id); if(!node) return;
    node.name = qs('#n-name').value.trim()||node.id;
    node.language = 'javascript';
    node.inputs = qs('#n-inputs').value.split(',').map(s=>s.trim()).filter(Boolean);
    node.outputs = qs('#n-outputs').value.split(',').map(s=>s.trim()).filter(Boolean);
    node.config.script = qs('#n-script').value;
    renderNodesTable();
    notify('Node saved','success');
  }

  function openScriptEditor(id) {
    const node = state.nodes.find(n=>n.id===id); if(!node) return;
    const scriptEl = qs('#n-script');
    if(!scriptEl) return;

    const currentScript = scriptEl.value;
    const nodeName = node.name;

    // Open shared modal
    ScriptEditorModal.open({
      title: 'Script Editor',
      subtitle: `Node: ${nodeName}`,
      initialScript: currentScript,
      onSave: (updatedScript) => {
        scriptEl.value = updatedScript;
        node.config.script = updatedScript;
        notify('Script updated', 'success');
      }
    });
  }

  function cancelNodeEdit(){ qs('#node-inline-editor').style.display='none'; }
  function removeNode(id){ state.nodes = state.nodes.filter(n=>n.id!==id); renderNodesTable(); cancelNodeEdit(); }
  function removeConnection(idx){ state.connections.splice(idx,1); renderConnectionsTable(); }

  function renderInputMappings() {
    const tbody = qs('#input-mappings-table tbody');
    tbody.innerHTML = state.inputMappings.map((m,idx)=> {
      const options = ['','---'].concat(availableInputs).map(val=>{
        if(val==='') return `<option value="">(select)</option>`;
        if(val==='---') return `<option disabled>── inputs ──</option>`;
        return `<option value="${escape(val)}" ${m.nodeInput===val?'selected':''}>${escape(val)}</option>`;
      }).join('');
      return `<tr>
        <td><select class="form-control" onchange="FlowEdit.updateInputMapping(${idx},'nodeInput',this.value)">${options}</select></td>
        <td><select class="form-control" onchange="FlowEdit.updateInputMapping(${idx},'type',this.value)"><option value="TOPIC" ${m.type==='TOPIC'?'selected':''}>TOPIC</option></select></td>
        <td><input class="form-control" value="${escape(m.value)}" onchange="FlowEdit.updateInputMapping(${idx},'value',this.value)" placeholder="MQTT topic"></td>
        <td>
          <button class="btn-icon btn-delete" onclick="FlowEdit.removeInputMapping(${idx})" title="Delete Input Mapping">
            <svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor">
              <path d="M6 19c0 1.1.9 2 2 2h8c1.1 0 2-.9 2-2V7H6v12zM19 4h-3.5l-1-1h-5l-1 1H5v2h14V4z"/>
            </svg>
          </button>
        </td>
      </tr>`;
    }).join('');
  }
  function renderOutputMappings() {
    const tbody = qs('#output-mappings-table tbody');
    tbody.innerHTML = state.outputMappings.map((m,idx)=> {
      const options = ['','---'].concat(availableOutputs).map(val=>{
        if(val==='') return `<option value="">(select)</option>`;
        if(val==='---') return `<option disabled>── outputs ──</option>`;
        return `<option value="${escape(val)}" ${m.nodeOutput===val?'selected':''}>${escape(val)}</option>`;
      }).join('');
      return `<tr>
        <td><select class="form-control" onchange="FlowEdit.updateOutputMapping(${idx},'nodeOutput',this.value)">${options}</select></td>
        <td><input class="form-control" value="${escape(m.topic)}" onchange="FlowEdit.updateOutputMapping(${idx},'topic',this.value)" placeholder="MQTT topic"></td>
        <td>
          <button class="btn-icon btn-delete" onclick="FlowEdit.removeOutputMapping(${idx})" title="Delete Output Mapping">
            <svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor">
              <path d="M6 19c0 1.1.9 2 2 2h8c1.1 0 2-.9 2-2V7H6v12zM19 4h-3.5l-1-1h-5l-1 1H5v2h14V4z"/>
            </svg>
          </button>
        </td>
      </tr>`;
    }).join('');
  }
  function renderVariables() {
    const tbody = qs('#variables-table tbody');
    const entries = Object.entries(state.variables);
    tbody.innerHTML = entries.map(([k,v],idx)=> `<tr>
      <td><input class="form-control" value="${escape(k)}" onchange="FlowEdit.updateVariableKey(${idx}, this.value)"></td>
      <td><input class="form-control" value="${escape(v)}" onchange="FlowEdit.updateVariableVal('${escape(k)}', this.value)"></td>
      <td>
        <button class="btn-icon btn-delete" onclick="FlowEdit.removeVariable('${escape(k)}')" title="Delete Variable">
          <svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor">
            <path d="M6 19c0 1.1.9 2 2 2h8c1.1 0 2-.9 2-2V7H6v12zM19 4h-3.5l-1-1h-5l-1 1H5v2h14V4z"/>
          </svg>
        </button>
      </td>
    </tr>`).join('');
  }

  function addInputMapping(){ state.inputMappings.push({ nodeInput:'', type:'TOPIC', value:'' }); renderInputMappings(); }
  function updateInputMapping(i,f,val){ const m=state.inputMappings[i]; if(m){ m[f]=val; }}
  function removeInputMapping(i){ state.inputMappings.splice(i,1); renderInputMappings(); }
  function addOutputMapping(){ state.outputMappings.push({ nodeOutput:'', topic:'' }); renderOutputMappings(); }
  function updateOutputMapping(i,f,val){ const m=state.outputMappings[i]; if(m){ m[f]=val; }}
  function removeOutputMapping(i){ state.outputMappings.splice(i,1); renderOutputMappings(); }
  function addVariable(){ state.variables['key_'+Date.now()]=''; renderVariables(); }
  function updateVariableKey(index,newKey){ const oldKey=Object.keys(state.variables)[index]; if(!oldKey) return; const val=state.variables[oldKey]; delete state.variables[oldKey]; state.variables[newKey]=val; renderVariables(); }
  function updateVariableVal(key,val){ if(key in state.variables) state.variables[key]=val; }
  function removeVariable(key){ delete state.variables[key]; renderVariables(); }

  function save() {
    if(state.type==='class') return saveClass();
    return saveInstance();
  }

  async function saveClass() {
    const name = qs('#fc-name').value.trim();
    const namespace = qs('#fc-namespace').value.trim();
    const version = qs('#fc-version').value.trim()||'1.0.0';
    if(!name || !namespace){ notify('Name & namespace required','error'); return; }
    const input = {
      name, namespace, version,
      description: qs('#fc-description').value.trim()||null,
      nodes: state.nodes.map(n=>({ id:n.id, type:n.type, name:n.name, config:{ script:n.config.script }, inputs:n.inputs, outputs:n.outputs, language:n.language, position: n.position })),
      connections: state.connections.map(c=>({ fromNode:c.fromNode, fromOutput:c.fromOutput, toNode:c.toNode, toInput:c.toInput }))
    };
    const isUpdate = !!state.name && state.name===name;
    const mutation = isUpdate ? `mutation($name:String!,$input:FlowClassInput!){ flow { updateClass(name:$name,input:$input){ name } } }` : `mutation($input:FlowClassInput!){ flow { createClass(input:$input){ name } } }`;
    const vars = isUpdate? { name, input } : { input };
    await graphql(mutation, vars);
    notify('Saved flow class','success');
    // Stay on the page instead of navigating away
  }

  async function saveInstance() {
    const name = qs('#fi-name').value.trim();
    const namespace = qs('#fi-namespace').value.trim();
    const nodeId = qs('#fi-nodeId').value.trim();
    const flowClassId = qs('#fi-flowClass').value;
    const enabled = qs('#fi-enabled').checked;
    if(!name || !namespace || !nodeId || !flowClassId){ notify('All fields required','error'); return; }
    const input = {
      name, namespace, nodeId, flowClassId, enabled,
      inputMappings: state.inputMappings.map(m=>({ nodeInput:m.nodeInput, type:m.type, value:m.value })),
      outputMappings: state.outputMappings.map(m=>({ nodeOutput:m.nodeOutput, topic:m.topic })),
      variables: state.variables
    };
    const isUpdate = !!state.name && state.name===name;
    const mutation = isUpdate ? `mutation($name:String!,$input:FlowInstanceInput!){ flow { updateInstance(name:$name,input:$input){ name } } }` : `mutation($input:FlowInstanceInput!){ flow { createInstance(input:$input){ name } } }`;
    const vars = isUpdate? { name, input } : { input };
    await graphql(mutation, vars);
    notify('Saved flow instance','success');
    location.href = '/pages/workflows.html';
  }

  async function deleteItem() {
    if(!confirm('Delete this item?')) return;
    if(state.type==='class') {
      await graphql(`mutation($name:String!){ flow { deleteClass(name:$name) } }`, { name: state.name });
    } else {
      await graphql(`mutation($name:String!){ flow { deleteInstance(name:$name) } }`, { name: state.name });
    }
    notify('Deleted','success');
    location.href = '/pages/workflows.html';
  }

  async function restartAllInstances() {
    if(!state.flowClass || !state.flowClass.name) {
      notify('No flow class loaded','error');
      return;
    }
    const flowClassName = state.flowClass.name;
    try {
      // Query all instances and filter by this flow class
      const instQuery = `query { flowInstances { name namespace flowClassId } }`;
      const instData = await graphql(instQuery);
      const instances = (instData.flowInstances || []).filter(fi => fi.flowClassId === flowClassName);

      if(instances.length === 0) {
        notify('No instances found for this flow class','info');
        return;
      }

      // Restart each instance (disable then enable)
      for(const inst of instances) {
        await graphql(`mutation($name:String!){ flow { disableInstance(name:$name) { name } } }`, { name: inst.name });
        await graphql(`mutation($name:String!){ flow { enableInstance(name:$name) { name } } }`, { name: inst.name });
      }
      notify(`Restarted ${instances.length} instance${instances.length===1?'':'s'}`, 'success');
    } catch(e) {
      console.error('Failed to restart instances:', e);
      notify('Failed to restart instances','error');
    }
  }

  function cancel(){
    // Cancel should not ask for confirmation per new UX requirement.
    state.dirty = false; // prevent any beforeunload handler (if added later) from prompting
    location.href='/pages/workflows.html';
  }

  function notify(msg,type='info') {
    const div = document.createElement('div');
    div.textContent = msg;
    div.style.cssText = `position:fixed;top:20px;right:20px;background:${type==='error'?'#dc3545':type==='success'?'#28a745':'#17a2b8'};color:#fff;padding:.6rem 1rem;border-radius:4px;z-index:10000;font-size:.75rem;`;
    document.body.appendChild(div); setTimeout(()=>{ div.style.opacity='0'; setTimeout(()=>div.remove(),300); },2500);
  }

  function escape(str){ return (str??'').replace(/[&<>"']/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;","\"":"&quot;","'":"&#39;"}[c])); }

  return {
    init, addNode, editNode, saveNode, openScriptEditor, cancelNodeEdit, removeNode, addConnectionRow, removeConnection,
    addInputMapping, updateInputMapping, removeInputMapping, addOutputMapping, updateOutputMapping, removeOutputMapping,
    addVariable, updateVariableKey, updateVariableVal, removeVariable, save, deleteItem, cancel, restartAllInstances, addOutputMappingRow: addOutputMapping
  };
})();

document.addEventListener('DOMContentLoaded', () => FlowEdit.init());
