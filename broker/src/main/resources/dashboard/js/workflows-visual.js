// Visual Flow Editor (extracted & simplified)
// Provides canvas-based editing for Flow Classes.

const VisualFlow = (() => {
  const state = {
    loading: true,
    className: null,
    flowClass: null,
    nodes: [],
    connections: [],
    nodeTypes: [],
    selectedNodeId: null,
    selectedConnectionIndex: null,
    dragging: { node: null, offsetX:0, offsetY:0 },
    connectingFrom: null,
    dirty: false,
    view: { scale: 1, x: 0, y: 0, isPanning: false, panLast: { x:0, y:0 } }
  };

  // ------------- DOM helpers -------------
  const qs = sel => document.querySelector(sel);
  const ce = tag => document.createElement(tag);
  const escape = s => (s??'').replace(/[&<>"']/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;","\"":"&quot;","'":"&#39;"}[c]));

  // ------------- Init -------------
  async function init(){
    parseParams();
    try {
      await Promise.all([loadNodeTypes(), loadFlowClassIfNeeded()]);
      buildPalette();
      populateClassForm();
      renderAll();
      refreshConnectionHelper();
      qs('#status-text').textContent = 'Ready';
    } catch(e){
      notify('Init error: '+e.message,'error');
      console.error(e);
      qs('#status-text').textContent = 'Error';
    }
  }
  function parseParams(){
    const p = new URLSearchParams(location.search);
    state.className = p.get('name');
  }

  // ------------- GraphQL -------------
  async function gql(query, variables={}) {
    try { return await window.graphqlClient.query(query, variables); }
    catch(e){ throw e; }
  }

  async function loadNodeTypes(){
    const q = `query { flowNodeTypes { type category description defaultInputs defaultOutputs icon configSchema } }`;
    const data = await gql(q);
    // Include all node types (function, database, etc.)
    state.nodeTypes = (data.flowNodeTypes||[]);
  }

  async function loadFlowClassIfNeeded(){
    if(!state.className) return; // new class
    // Request script inside config (object) if supported by schema
    // If the server returns config as a JSON scalar, this still works (ignored sub-selection will error) – fallback handled below.
    // If it errors in practice, adjust schema query accordingly.
    let data;
    try {
      // Try fetching config as an object with all fields
      const q = `query($name:String){ flowClasses(name:$name){ name namespace version description nodes { id type name config inputs outputs language position { x y } } connections { fromNode fromOutput toNode toInput } } }`;
      data = await gql(q,{name: state.className});
    } catch(err){
      console.warn('Query with config failed:', err.message);
      // Fallback: fetch without config details
      const qFallback = `query($name:String){ flowClasses(name:$name){ name namespace version description nodes { id type name inputs outputs language position { x y } } connections { fromNode fromOutput toNode toInput } } }`;
      data = await gql(qFallback,{name: state.className});
    }
    const list = data.flowClasses||[];
    state.flowClass = list[0]||null;
    if(!state.flowClass){ notify('Flow class not found','error'); return; }
    state.nodes = state.flowClass.nodes.map(n=>({
      ...n,
      // Normalize config to preserve all fields (not just script)
      config: (n.config && typeof n.config === 'object') ? n.config : {},
      position: n.position||{x:100,y:100}
    }));
    state.connections = state.flowClass.connections.map(c=>({ ...c }));
  }

  // ------------- Palette & Class Form -------------
  function getNodeIcon(type){
    const icons = {
      function: '<svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="16 18 22 12 16 6"></polyline><polyline points="8 6 2 12 8 18"></polyline></svg>',
      database: '<svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><ellipse cx="12" cy="5" rx="9" ry="3"></ellipse><path d="M21 12c0 1.66-4 3-9 3s-9-1.34-9-3"></path><path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5"></path></svg>',
      timer: '<svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="13" r="8"></circle><path d="M12 9v4l3 2"></path><path d="M9 2h6"></path></svg>'
    };
    return icons[type] || '<svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="3" y="3" width="18" height="18" rx="2" ry="2"></rect></svg>';
  }

  function buildPalette(){
    const pal = qs('#node-palette'); if(!pal) return;
    pal.innerHTML = state.nodeTypes.map(nt=>`
      <div class="palette-item" onclick="VisualFlow.addNodeType('${nt.type}')">
        <div class="palette-item-icon">${getNodeIcon(nt.type)}</div>
        <div class="palette-item-info">
          <div class="palette-item-name">${escape(nt.type)}</div>
          <div class="palette-item-desc">Add to workflow</div>
        </div>
      </div>
    `).join('');
  }
  function populateClassForm(){
    const fc = state.flowClass || { name:'', namespace:'default', version:'1.0.0', description:''};
    qs('#fc-name').value = fc.name||'';
    qs('#fc-namespace').value = fc.namespace||'default';
    qs('#fc-version').value = fc.version||'1.0.0';
    qs('#fc-description').value = fc.description||'';
    qs('#page-title').textContent = state.flowClass? 'Visual Editor: '+fc.name : 'New Flow Class (Visual)';
    qs('#page-subtitle').textContent = state.flowClass? 'Edit visually' : 'Create and design nodes visually';
    qs('#delete-class-btn').style.display = state.flowClass? 'inline-block':'none';
    if(state.flowClass) loadInstancesForClass();
  }

  async function loadInstancesForClass(){
    if(!state.flowClass) return;
    const flowClassName = state.flowClass.name;
    const q = `query($flowClassId:String){ flowInstances(flowClassId:$flowClassId) { name enabled } }`;
    try {
      const data = await gql(q, { flowClassId: flowClassName });
      const instances = data.flowInstances || [];
      const select = qs('#restart-instance-select');
      const controls = qs('#restart-controls');
      const btn = qs('#instance-btn');

      if(!select || !controls || !btn) return;

      if(instances.length === 0){
        controls.style.display = 'none';
        btn.style.display = 'inline-block';
        btn.textContent = 'Create Instance';
        return;
      }

      controls.style.display = 'flex';
      btn.style.display = 'inline-block';
      btn.textContent = 'Edit Instance';
      select.innerHTML = instances.map((inst, idx) =>
        `<option value="${inst.name}" ${idx === 0 ? 'selected' : ''}>${inst.name}</option>`
      ).join('');
    } catch(e){
      console.error('Failed to load instances:', e);
    }
  }

  async function restartInstance(){
    const instanceName = qs('#restart-instance-select')?.value;
    if(!instanceName){
      notify('Please select an instance', 'error');
      return;
    }

    try {
      // Disable the instance
      const disableMutation = `mutation($name:String!){ flow { disableInstance(name:$name) { name } } }`;
      await gql(disableMutation, { name: instanceName });

      // Wait 500ms
      await new Promise(resolve => setTimeout(resolve, 500));

      // Enable the instance
      const enableMutation = `mutation($name:String!){ flow { enableInstance(name:$name) { name } } }`;
      await gql(enableMutation, { name: instanceName });

      notify(`Instance "${instanceName}" restarted successfully`, 'success');
    } catch(e){
      console.error(e);
      notify('Restart failed: '+e.message, 'error');
    }
  }

  // ------------- Node Operations -------------
  function addNodeType(type){
    const def = state.nodeTypes.find(nt=>nt.type===type); if(!def) return;
    const id = 'node_'+Date.now();

    // Initialize config based on node type
    let config = {};
    if(type === 'function'){
      config = { script: '' };
    } else if(type === 'database'){
      config = {
        driverClassName: '',
        jdbcUrl: '',
        username: '',
        password: '',
        operationType: 'QUERY',
        sqlStatement: '',
        connectionMode: 'PER_TRIGGER',
        enableDynamicSql: false,
        healthCheckInterval: 60000
      };
    } else if(type === 'timer'){
      config = {
        frequency: 1000
      };
    }

    const nodeObj = { id, type, name: type+'_'+state.nodes.length, config, inputs:[...def.defaultInputs], outputs:[...def.defaultOutputs], position:{ x:120 + (state.nodes.length*30)%400, y:120 + Math.floor(state.nodes.length/10)*100 }};
    // Only add language field for code execution nodes
    if(type === 'function' || type === 'timer') {
      nodeObj.language = 'javascript';
    }
    state.nodes.push(nodeObj);
    selectNode(id);
    renderAll();
    refreshConnectionHelper();
    markDirty();
  }
  function selectNode(id){ state.selectedNodeId = id; state.selectedConnectionIndex=null; updateNodePanel(); renderNodes(); }
  function deleteNode(){ if(!state.selectedNodeId) return; state.connections = state.connections.filter(c=>c.fromNode!==state.selectedNodeId && c.toNode!==state.selectedNodeId); state.nodes = state.nodes.filter(n=>n.id!==state.selectedNodeId); state.selectedNodeId=null; updateNodePanel(); renderAll(); refreshConnectionHelper(); markDirty(); }
  function saveNode(){
    const n=currentNode();
    if(!n) return;

    // Get new ID and validate
    const newId = qs('#n-id').value.trim();
    if(!newId) {
      notify('Node ID cannot be empty', 'error');
      return;
    }

    const oldId = n.id;

    // Check if ID changed and validate uniqueness
    if(newId !== oldId) {
      if(state.nodes.some(node => node.id === newId && node !== n)) {
        notify('Node ID already exists. Choose a unique ID.', 'error');
        return;
      }

      // Update all connections that reference the old ID
      state.connections.forEach(c => {
        if(c.fromNode === oldId) c.fromNode = newId;
        if(c.toNode === oldId) c.toNode = newId;
      });

      // Update the node ID
      n.id = newId;

      // Update selected node ID
      state.selectedNodeId = newId;
    }

    n.name=qs('#n-name').value.trim()||n.id;
    n.inputs=qs('#n-inputs').value.split(',').map(s=>s.trim()).filter(Boolean);
    n.outputs=qs('#n-outputs').value.split(',').map(s=>s.trim()).filter(Boolean);

    // Save config based on node type
    if(n.type === 'function'){
      n.config.script=qs('#n-script').value;
    } else if(n.type === 'database'){
      n.config.driverClassName = qs('#n-driver-class')?.value || '';
      n.config.jdbcUrl = qs('#n-jdbc-url')?.value || '';
      n.config.username = qs('#n-username')?.value || '';
      n.config.password = qs('#n-password')?.value || '';
      n.config.operationType = qs('#n-operation-type')?.value || 'QUERY';
      n.config.sqlStatement = qs('#n-sql-statement')?.value || '';
      n.config.connectionMode = qs('#n-connection-mode')?.value || 'PER_TRIGGER';
      n.config.enableDynamicSql = qs('#n-dynamic-sql')?.checked || false;
      n.config.healthCheckInterval = parseInt(qs('#n-health-check')?.value || '60000');
    } else if(n.type === 'timer'){
      n.config.frequency = parseInt(qs('#n-frequency')?.value || '1000');
      n.config.value = qs('#n-timer-value')?.value ?? '';
    }

    renderAll();
    refreshConnectionHelper();
    markDirty();
    notify('Node updated','success');
  }
  function currentNode(){ return state.nodes.find(n=>n.id===state.selectedNodeId); }

  // ------------- Canvas Rendering -------------
  function renderAll(){ renderNodes(); renderConnections(); renderConnectionsList(); }
  function renderNodes(){ const canvas=qs('#flow-canvas'); if(!canvas) return; canvas.querySelectorAll('.flow-node').forEach(el=>el.remove()); state.nodes.forEach(node=>{ const el=ce('div'); el.className='flow-node'+(state.selectedNodeId===node.id?' selected':''); el.style.left=node.position.x+'px'; el.style.top=node.position.y+'px'; el.dataset.nodeId=node.id; el.innerHTML = `<div class=\"flow-node-header\"><span>${escape(node.name)}</span></div><div class=\"flow-node-ports\"><div>${node.inputs.map(p=>`<div class='node-port input-port' data-port='${escape(p)}' onclick=\"VisualFlow.handlePortClick('${node.id}','${escape(p)}','input')\">${escape(p)} <span class='port-hint-badge'>IN</span></div>`).join('')}</div><div>${node.outputs.map(p=>`<div class='node-port output-port ${(state.connectingFrom && state.connectingFrom.nodeId===node.id && state.connectingFrom.portName===p)?'connecting':''}' data-port='${escape(p)}' onclick=\"VisualFlow.handlePortClick('${node.id}','${escape(p)}','output')\">${escape(p)} <span class='port-hint-badge'>OUT</span></div>`).join('')}</div></div>`; el.addEventListener('mousedown',e=>{ if(e.target.closest('.node-port')) return; state.dragging.node=node; state.dragging.offsetX=e.offsetX; state.dragging.offsetY=e.offsetY; selectNode(node.id); }); canvas.appendChild(el); }); applyViewTransform(); }
  function renderConnections(){
    const svg=qs('#connections-svg');
    const canvas=qs('#flow-canvas');
    const stage=qs('#stage');
    if(!svg||!canvas||!stage) return;

    svg.setAttribute('width', stage.style.width||'2000px');
    svg.setAttribute('height', stage.style.height||'1200px');
    svg.innerHTML='';

    state.connections.forEach((c,i)=>{
      const fromNode=canvas.querySelector(`[data-node-id='${c.fromNode}']`);
      const toNode=canvas.querySelector(`[data-node-id='${c.toNode}']`);
      if(!fromNode||!toNode) return;

      // Find the specific output port
      const fromPort=fromNode.querySelector(`.output-port[data-port='${c.fromOutput}']`);
      const toPort=toNode.querySelector(`.input-port[data-port='${c.toInput}']`);

      let x1, y1, x2, y2;

      if(fromPort){
        x1 = fromNode.offsetLeft + fromNode.offsetWidth;
        y1 = fromNode.offsetTop + fromPort.offsetTop + fromPort.offsetHeight/2;
      } else {
        x1 = fromNode.offsetLeft + fromNode.offsetWidth;
        y1 = fromNode.offsetTop + 40;
      }

      if(toPort){
        x2 = toNode.offsetLeft;
        y2 = toNode.offsetTop + toPort.offsetTop + toPort.offsetHeight/2;
      } else {
        x2 = toNode.offsetLeft;
        y2 = toNode.offsetTop + 40;
      }

      const dx=x2-x1;
      const curve=Math.min(Math.abs(dx)/2,60);

      const path=document.createElementNS('http://www.w3.org/2000/svg','path');
      path.setAttribute('d',`M ${x1} ${y1} C ${x1+curve} ${y1}, ${x2-curve} ${y2}, ${x2} ${y2}`);
      path.setAttribute('class',`connection-line${state.selectedConnectionIndex===i?' selected':''}`);
      path.dataset.connectionIndex=i;
      path.onclick=(e)=>{
        e.stopPropagation();
        state.selectedConnectionIndex=i;
        state.selectedNodeId=null;
        updateNodePanel();
        renderAll();
      };
      svg.appendChild(path);
    });

    applyViewTransform();
  }
  function renderConnectionsList(){ const list=qs('#connections-list'); if(!list) return; if(state.connections.length===0){ const noConn=qs('#no-connections'); if(noConn){ noConn.style.display='block'; } else { list.innerHTML='<p id="no-connections" class="empty-state">No connections yet</p>'; } list.querySelectorAll('.conn-item').forEach(e=>e.remove()); return; } const noConn=qs('#no-connections'); if(noConn){ noConn.style.display='none'; } list.innerHTML = state.connections.map((c,i)=>`<div class='conn-item' style='margin:.25rem 0; display:flex; justify-content:space-between; gap:.25rem;'><span>${escape(c.fromNode)}.${escape(c.fromOutput)} → ${escape(c.toNode)}.${escape(c.toInput)}</span><button class='btn btn-danger btn-small' style='padding:2px 4px;font-size:.55rem;' onclick='VisualFlow.deleteConnection(${i})'>✕</button></div>`).join(''); }

  // ------------- Interaction -------------
  function handleMouseMove(e){
    if(state.view.isPanning){
      const factor = 1/state.view.scale;
      state.view.x += e.movementX * factor;
      state.view.y += e.movementY * factor;
      applyViewTransform();
      return;
    }
    if(!state.dragging.node) return; state.dragging.node.position.x += e.movementX / state.view.scale; state.dragging.node.position.y += e.movementY / state.view.scale; renderAll(); markDirty(); }
  function handleMouseUp(){ if(state.dragging.node){ state.dragging.node=null; } state.view.isPanning=false; }
  function handlePortClick(nodeId, portName, portType){
    if(!state.connectingFrom){
      if(portType==='output'){
        state.connectingFrom={ nodeId, portName, portType };
        highlightInputs(true);
        notify('Click an input port to connect, or press ESC/right-click to cancel','info');
      }
    } else {
      if(portType==='input' && state.connectingFrom.portType==='output'){
        const connection={ fromNode: state.connectingFrom.nodeId, fromOutput: state.connectingFrom.portName, toNode: nodeId, toInput: portName };
        if(!state.connections.some(c=>c.fromNode===connection.fromNode && c.fromOutput===connection.fromOutput && c.toNode===connection.toNode && c.toInput===connection.toInput)){
          state.connections.push(connection);
          renderAll();
          markDirty();
          notify('Connection added','success');
        } else {
          notify('Connection already exists','error');
        }
      }
      state.connectingFrom=null;
      highlightInputs(false);
      renderNodes();
    }
  }
  function highlightInputs(on){ const canvas=qs('#flow-canvas'); canvas.querySelectorAll('.input-port').forEach(el=>{ if(on) el.classList.add('valid-target'); else el.classList.remove('valid-target'); }); }
  function deleteConnection(i){ state.connections.splice(i,1); renderAll(); }

  // ------------- Node Panel -------------
  function updateNodePanel(){
    const n=currentNode();
    const empty=qs('#node-panel-empty');
    const form=qs('#node-form');
    const funcForm=qs('#node-form-function');
    const dbForm=qs('#node-form-database');

    if(!n){
      empty.style.display='block';
      form.style.display='none';
      return;
    }

    empty.style.display='none';
    form.style.display='block';

    // Common fields
    qs('#n-id').value=n.id;
    qs('#n-name').value=n.name;
    qs('#n-inputs').value=n.inputs.join(', ');
    qs('#n-outputs').value=n.outputs.join(', ');

    // Show/hide node-type-specific forms
    const timerForm = qs('#node-form-timer');
    if(funcForm) funcForm.style.display = n.type === 'function' ? 'block' : 'none';
    if(dbForm) dbForm.style.display = n.type === 'database' ? 'block' : 'none';
    if(timerForm) timerForm.style.display = n.type === 'timer' ? 'block' : 'none';

    // Hide inputs/outputs for timer and database nodes (they have fixed ports)
    const inputsGroup = qs('#inputs-group');
    const outputsGroup = qs('#outputs-group');
    const hideInputsOutputs = n.type === 'timer' || n.type === 'database';
    if(inputsGroup) inputsGroup.style.display = hideInputsOutputs ? 'none' : 'block';
    if(outputsGroup) outputsGroup.style.display = hideInputsOutputs ? 'none' : 'block';

    // Populate function form
    if(n.type === 'function'){
      const scriptField = qs('#n-script');
      if(scriptField){
        scriptField.value=n.config?.script||'';
        enhanceScriptEditor();
      }
    }

    // Populate database form
    if(n.type === 'database'){
      const driverField = qs('#n-driver-class');
      const jdbcUrlField = qs('#n-jdbc-url');
      const usernameField = qs('#n-username');
      const passwordField = qs('#n-password');
      const operationTypeField = qs('#n-operation-type');
      const sqlField = qs('#n-sql-statement');
      const modeField = qs('#n-connection-mode');
      const dynamicField = qs('#n-dynamic-sql');
      const healthField = qs('#n-health-check');

      if(driverField) driverField.value = n.config?.driverClassName || '';
      if(jdbcUrlField) jdbcUrlField.value = n.config?.jdbcUrl || '';
      if(usernameField) usernameField.value = n.config?.username || '';
      if(passwordField) passwordField.value = n.config?.password || '';
      if(operationTypeField) operationTypeField.value = n.config?.operationType || 'QUERY';
      if(sqlField) sqlField.value = n.config?.sqlStatement || '';
      if(modeField) modeField.value = n.config?.connectionMode || 'PER_TRIGGER';
      if(dynamicField) dynamicField.checked = n.config?.enableDynamicSql || false;
      if(healthField) healthField.value = n.config?.healthCheckInterval || 60000;
    }

    // Populate timer form
    if(n.type === 'timer'){
      const frequencyField = qs('#n-frequency');
      const timerValueField = qs('#n-timer-value');

      if(frequencyField) frequencyField.value = n.config?.frequency || 1000;
      if(timerValueField) timerValueField.value = n.config?.value ?? '';
    }
  }
  // Enhance script area after panel update
  function enhanceScriptEditor(){
    const ta=qs('#n-script');
    if(!ta) return;

    if(!ta._enhanced){
      // Handle Tab key to insert spaces
      ta.addEventListener('keydown',e=>{
        if(e.key==='Tab'){
          e.preventDefault();
          const start=ta.selectionStart;
          const end=ta.selectionEnd;
          const val=ta.value;
          ta.value=val.substring(0,start)+'  '+val.substring(end);
          ta.selectionStart=ta.selectionEnd=start+2;
        }
      });

      // Auto-grow height
      const autoGrow=()=>{
        ta.style.height='auto';
        ta.style.height=Math.min(500, ta.scrollHeight)+'px';
      };
      ta.addEventListener('input',autoGrow);

      ta._enhanced=true;
      autoGrow();
    }
  }

  // ------------- Save/Delete Class -------------
  async function saveClass(){
    // If a node is being edited, save it first
    if(state.selectedNodeId) {
      saveNode();
    }

    const name=qs('#fc-name').value.trim();
    const namespace=qs('#fc-namespace').value.trim();
    const version=qs('#fc-version').value.trim()||'1.0.0';
    if(!name||!namespace){
      notify('Name & namespace required','error');
      return;
    }
    const input={
      name, namespace, version,
      description: qs('#fc-description').value.trim()||null,
      nodes: state.nodes.map(n=>{
        const nodeObj = {
          id:n.id, type:n.type, name:n.name,
          config: n.config||{},  // Preserve full config object for all node types
          inputs:n.inputs, outputs:n.outputs,
          position: n.position? { x:n.position.x, y:n.position.y }: null
        };
        // Only include language for code execution nodes
        if(n.type === 'function' || n.type === 'timer') {
          nodeObj.language = n.language || 'javascript';
        }
        return nodeObj;
      }),
      connections: state.connections.map(c=>({
        fromNode:c.fromNode, fromOutput:c.fromOutput,
        toNode:c.toNode, toInput:c.toInput
      }))
    };
    const isUpdate = !!state.flowClass && state.flowClass.name === name;
    const mutation=isUpdate?
      `mutation($name:String!,$input:FlowClassInput!){ flow { updateClass(name:$name,input:$input){ name } } }`:
      `mutation($input:FlowClassInput!){ flow { createClass(input:$input){ name } } }`;
    try {
      await gql(mutation, isUpdate? { name, input } : { input });
      notify('Class saved','success');
      state.dirty=false;
      updateTitleDirty();
      if(!isUpdate){
        // redirect with name param
        location.href='/pages/workflows-visual.html?name='+encodeURIComponent(name);
      }
      state.flowClass={ name, namespace, version };
    }
    catch(e){
      console.error(e);
      notify('Save failed: '+e.message,'error');
    }
  }
  async function deleteClass(){ if(!state.flowClass){ notify('Nothing to delete','error'); return; } if(!confirm('Delete this flow class?')) return; try { await gql(`mutation($name:String!){ flow { deleteClass(name:$name) } }`, { name: state.flowClass.name }); notify('Deleted','success'); location.href='/pages/workflows.html'; } catch(e){ console.error(e); notify('Delete failed: '+e.message,'error'); } }

  async function restartAllInstances(){
    if(!state.flowClass){
      notify('No flow class loaded','error');
      return;
    }

    const flowClassName = state.flowClass.name;
    const q = `query($flowClassId:String){ flowInstances(flowClassId:$flowClassId) { name enabled } }`;

    try {
      const data = await gql(q, { flowClassId: flowClassName });
      const instances = data.flowInstances || [];

      if(instances.length === 0){
        notify('No instances found for this flow class','info');
        return;
      }

      if(!confirm(`Restart all ${instances.length} instance(s) of class "${flowClassName}"?`)) return;

      let successCount = 0;
      let failCount = 0;

      // Disable all instances
      for(const instance of instances){
        try {
          const mutation = `mutation($name:String!){ flow { disableInstance(name:$name) { name } } }`;
          await gql(mutation, { name: instance.name });
          successCount++;
        } catch(e){
          console.error(e);
          failCount++;
        }
      }

      // Small delay between disable and enable
      await new Promise(resolve => setTimeout(resolve, 500));

      // Enable all instances again
      for(const instance of instances){
        if(instance.enabled){
          try {
            const mutation = `mutation($name:String!){ flow { enableInstance(name:$name) { name } } }`;
            await gql(mutation, { name: instance.name });
          } catch(e){
            console.error(e);
            failCount++;
          }
        }
      }

      if(failCount === 0){
        notify(`Successfully restarted ${successCount} instance(s)`, 'success');
      } else {
        notify(`Restarted ${successCount} instance(s), ${failCount} failed`, 'error');
      }
    } catch(e){
      console.error(e);
      notify('Restart failed: '+e.message, 'error');
    }
  }

  // ------------- View Helpers -------------
  function resetView(){ state.view.scale=1; state.view.x=0; state.view.y=0; applyViewTransform(); }
  function applyViewTransform(){ const stage=qs('#stage'); if(!stage) return; const { scale,x,y } = state.view; stage.style.transform = `translate(${x}px, ${y}px) scale(${scale})`; const zi=qs('#zoom-indicator'); if(zi) zi.textContent = Math.round(scale*100)+'%'; }
  function zoom(deltaY, centerX, centerY){
    const prevScale=state.view.scale;
    // deltaY < 0 (wheel up) => zoom in, deltaY > 0 (wheel down) => zoom out
    const direction = deltaY < 0 ? 1 : -1;
    const factor = direction > 0 ? 1.1 : 0.9;
    const newScale = Math.min(2.5, Math.max(0.2, prevScale * factor));
    if(newScale===prevScale) return;
    const wrapper=qs('#canvas-wrapper'); const rect=wrapper.getBoundingClientRect();
    const cx = centerX - rect.left; const cy = centerY - rect.top;
    // Keep cursor position stable
    state.view.x = cx/prevScale - (cx/newScale - state.view.x);
    state.view.y = cy/prevScale - (cy/newScale - state.view.y);
    state.view.scale = newScale; applyViewTransform();
  }

  function handleWheel(e){
    // Only zoom when Shift + wheel (avoid interfering with scroll)
    if(e.ctrlKey || e.metaKey) return;
    if(!e.shiftKey) return;
    // Prevent page scroll / horizontal scroll translation
    e.preventDefault();
    // Some devices send inverted signs or use deltaX when Shift is held.
    let dy = e.deltaY;
    if(Math.abs(dy) < 0.01 && Math.abs(e.deltaX) > Math.abs(dy)) {
      // Fallback: use horizontal as vertical intent while holding Shift
      dy = e.deltaX;
    }
    // Normalize dy to a consistent magnitude bucket so tiny deltas still trigger a step
    if(dy === 0) return;
    // On some trackpads positive deltaY means scroll down (should zoom out)
    zoom(dy, e.clientX, e.clientY);
  }
  function keyboardZoom(dir){
    const wrapper=qs('#canvas-wrapper');
    const rect=wrapper.getBoundingClientRect();
    const cx = rect.left + rect.width/2;
    const cy = rect.top + rect.height/2;
    zoom(dir>0 ? -1 : 1, cx, cy); // reuse zoom semantics (deltaY<0 => in)
  }
  function handleKeyDown(e){
    // Don't handle shortcuts if user is typing in an input/textarea
    const target = e.target;
    const isInputField = target.tagName === 'INPUT' || target.tagName === 'TEXTAREA' || target.tagName === 'SELECT';

    if(e.code==='Space' && !isInputField){
      if(!state.view.isPanning){ state.view.isPanning=true; document.body.style.cursor='grab'; }
      e.preventDefault();
    } else if(((e.key==='+') || (e.key==='=' && (e.shiftKey||!e.shiftKey))) && !isInputField){
      keyboardZoom(1); e.preventDefault();
    } else if((e.key==='-' || e.key==='_') && !isInputField){
      keyboardZoom(-1); e.preventDefault();
    } else if(e.key==='Escape'){
      if(state.connectingFrom){
        state.connectingFrom=null;
        highlightInputs(false);
        renderNodes();
        notify('Connection cancelled','info');
        e.preventDefault();
      } else if(state.selectedConnectionIndex!==null){
        state.selectedConnectionIndex=null;
        renderAll();
        e.preventDefault();
      }
    } else if((e.key==='Delete' || e.key==='Backspace') && !isInputField){
      if(state.selectedConnectionIndex!==null){
        state.connections.splice(state.selectedConnectionIndex,1);
        state.selectedConnectionIndex=null;
        renderAll();
        refreshConnectionHelper();
        markDirty();
        notify('Connection deleted','success');
        e.preventDefault();
      }
    }
  }
  function handleKeyUp(e){ if(e.code==='Space'){ state.view.isPanning=false; document.body.style.cursor=''; } }

  // allow middle mouse pan
  document.addEventListener('mousedown', e=>{ if(e.button===1){ state.view.isPanning=true; e.preventDefault(); }});
  document.addEventListener('mouseup', e=>{ if(e.button===1){ state.view.isPanning=false; }});

  // Right-click to cancel connections
  document.addEventListener('contextmenu', e=>{
    if(state.connectingFrom){
      e.preventDefault();
      state.connectingFrom=null;
      highlightInputs(false);
      renderNodes();
      notify('Connection cancelled','info');
    }
  });

  // Click on canvas to deselect
  document.addEventListener('click', e=>{
    const canvasWrapper = qs('#canvas-wrapper');
    if(canvasWrapper && e.target === canvasWrapper){
      if(state.selectedConnectionIndex!==null){
        state.selectedConnectionIndex=null;
        renderAll();
      }
    }
  });

  // ------------- Dirty Tracking -------------
  function markDirty(){ if(!state.dirty){ state.dirty=true; updateTitleDirty(); } }
  function updateTitleDirty(){ const t = state.dirty? '● '+document.title.replace(/^●\s+/,'') : document.title.replace(/^●\s+/,''); document.title=t; const status=qs('#status-text'); if(status){ status.textContent = state.dirty? 'Unsaved changes' : 'Ready'; }
    refreshConnectionHelper();
  }
  window.addEventListener('beforeunload', (e)=>{ if(state.dirty){ e.preventDefault(); e.returnValue='You have unsaved changes.'; } });

  // Mark dirty on form edits
  ['fc-name','fc-namespace','fc-version','fc-description','n-id','n-name','n-inputs','n-outputs','n-script'].forEach(id=>{
    document.addEventListener('input', ev=>{ if(ev.target && ev.target.id===id) markDirty(); });
  });

  // ------------- Connection Helper -------------
  function refreshConnectionHelper(){
    const fromSel=qs('#conn-from');
    const toSel=qs('#conn-to');
    if(!fromSel||!toSel) return;

    const fromOptions=[];
    state.nodes.forEach(n=>{
      n.outputs.forEach(o=> fromOptions.push({ value: `${n.id}.${o}`, label: `${n.name}.${o}` }));
    });

    const toOptions=[];
    state.nodes.forEach(n=>{
      n.inputs.forEach(i=> toOptions.push({ value: `${n.id}.${i}`, label: `${n.name}.${i}` }));
    });

    const selValFrom=fromSel.value;
    const selValTo=toSel.value;

    fromSel.innerHTML='<option value="">(select)</option>'+fromOptions.map(opt=>`<option value="${opt.value}">${escape(opt.label)}</option>`).join('');
    toSel.innerHTML='<option value="">(select)</option>'+toOptions.map(opt=>`<option value="${opt.value}">${escape(opt.label)}</option>`).join('');

    if(fromOptions.some(opt=>opt.value===selValFrom)) fromSel.value=selValFrom;
    if(toOptions.some(opt=>opt.value===selValTo)) toSel.value=selValTo;
  }

  function addConnectionHelper(){
    const from=qs('#conn-from').value;
    const to=qs('#conn-to').value;
    if(!from||!to) { notify('Select both endpoints','error'); return; }

    const [fromNode, fromOutput]=from.split('.');
    const [toNode,toInput]=to.split('.');

    if(fromNode===toNode){ notify('Cannot connect node to itself','error'); return; }
    if(state.connections.some(c=>c.fromNode===fromNode && c.fromOutput===fromOutput && c.toNode===toNode && c.toInput===toInput)){
      notify('Connection exists','error');
      return;
    }

    state.connections.push({ fromNode, fromOutput, toNode, toInput });
    renderAll();
    markDirty();
    notify('Connection added','success');
  }

  // initial population after load will call refresh in updateTitleDirty()

  // ------------- Notifications -------------
  function notify(msg,type='info'){ const div=ce('div'); div.textContent=msg; div.style.cssText=`position:fixed;top:18px;right:18px;background:${type==='error'?'#dc3545':type==='success'?'#28a745':'#17a2b8'};color:#fff;padding:.5rem .75rem;border-radius:4px;font-size:.65rem;z-index:10000;`; document.body.appendChild(div); setTimeout(()=>{ div.style.opacity='0'; setTimeout(()=>div.remove(),300); },2200); }

  // ------------- Events -------------
  document.addEventListener('mousemove', handleMouseMove);
  document.addEventListener('mouseup', handleMouseUp);
  document.addEventListener('wheel', handleWheel, { passive:false });
  document.addEventListener('keydown', handleKeyDown);
  document.addEventListener('keyup', handleKeyUp);

  // ------------- Script Validation -------------
  function validateScript(){
    const scriptEl = qs('#n-script');
    const validationEl = qs('#script-validation');
    if(!scriptEl || !validationEl) return;

    const script = scriptEl.value.trim();
    if(!script){
      validationEl.style.display = 'block';
      validationEl.style.background = 'rgba(255,193,7,0.15)';
      validationEl.style.borderColor = '#ffc107';
      validationEl.style.color = '#ffc107';
      validationEl.innerHTML = '⚠ Script is empty';
      return;
    }

    try {
      // Validate JavaScript syntax using Function constructor
      new Function(script);
      validationEl.style.display = 'block';
      validationEl.style.background = 'rgba(46,213,115,0.15)';
      validationEl.style.borderColor = '#2ed573';
      validationEl.style.color = '#2ed573';
      validationEl.innerHTML = '✓ JavaScript syntax is valid';
      notify('Script validated successfully', 'success');
    } catch(e) {
      validationEl.style.display = 'block';
      validationEl.style.background = 'rgba(220,53,69,0.15)';
      validationEl.style.borderColor = '#dc3545';
      validationEl.style.color = '#dc3545';
      validationEl.innerHTML = `✗ Syntax error: ${escape(e.message)}`;
      notify('Script has syntax errors', 'error');
    }
  }

  // ------------- Script Editor Modal -------------
  function openScriptInWindow(){
    const scriptEl = qs('#n-script');
    if(!scriptEl) return;

    const node = currentNode();
    if(!node) {
      notify('No node selected', 'error');
      return;
    }

    const script = scriptEl.value;
    const nodeName = node.name;

    // Open shared modal
    ScriptEditorModal.open({
      title: 'Script Editor',
      subtitle: `Node: ${nodeName}`,
      initialScript: script,
      onSave: (updatedScript) => {
        scriptEl.value = updatedScript;
        if(node){
          node.config.script = updatedScript;
          markDirty();
        }
        notify('Script updated', 'success');
      }
    });
  }

  function cancel(){ state.dirty=false; location.href='/pages/workflows.html'; }

  function handleInstanceButton(){
    const btn = qs('#instance-btn');
    if(!btn) return;

    if(btn.textContent === 'Create Instance'){
      // Navigate to create new instance with current class
      if(state.flowClass){
        location.href = `/pages/workflows-edit-instance.html?type=instance&class=${encodeURIComponent(state.flowClass.name)}&from=visual`;
      }
    } else if(btn.textContent === 'Edit Instance'){
      // Navigate to edit selected instance
      const selectedInstance = qs('#restart-instance-select')?.value;
      if(selectedInstance){
        location.href = `/pages/workflows-edit-instance.html?type=instance&name=${encodeURIComponent(selectedInstance)}&class=${encodeURIComponent(state.flowClass.name)}&from=visual`;
      } else {
        notify('Please select an instance', 'error');
      }
    }
  }

  return { init, addNodeType, saveClass, deleteClass, restartInstance, handlePortClick, deleteConnection, saveNode, deleteNode, addNodeType: addNodeType, resetView, addConnectionHelper, refreshConnectionHelper, keyboardZoom, validateScript, openScriptInWindow, cancel, handleInstanceButton };
})();

// Make VisualFlow accessible globally
window.VisualFlow = VisualFlow;

document.addEventListener('DOMContentLoaded', ()=> VisualFlow.init());
