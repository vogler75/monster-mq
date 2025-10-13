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
    const q = `query { flowNodeTypes { type defaultInputs defaultOutputs icon } }`;
    const data = await gql(q);
    // limit to function nodes for now
    state.nodeTypes = (data.flowNodeTypes||[]).filter(nt=>nt.type==='function');
  }

  async function loadFlowClassIfNeeded(){
    if(!state.className) return; // new class
    const q = `query($name:String){ flowClasses(name:$name){ name namespace version description nodes { id type name config inputs outputs language position { x y } } connections { fromNode fromOutput toNode toInput } } }`;
    const data = await gql(q,{name: state.className});
    const list = data.flowClasses||[];
    state.flowClass = list[0]||null;
    if(!state.flowClass){ notify('Flow class not found','error'); return; }
    state.nodes = state.flowClass.nodes.map(n=>({ ...n, position: n.position||{x:100,y:100} }));
    state.connections = state.flowClass.connections.map(c=>({ ...c }));
  }

  // ------------- Palette & Class Form -------------
  function buildPalette(){
    const pal = qs('#node-palette'); if(!pal) return;
    pal.innerHTML = state.nodeTypes.map(nt=>`<div class="palette-item" onclick="VisualFlow.addNodeType('${nt.type}')">${escape(nt.icon||'📦')} <span>${escape(nt.type)}</span></div>`).join('');
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
  }

  // ------------- Node Operations -------------
  function addNodeType(type){
    const def = state.nodeTypes.find(nt=>nt.type===type); if(!def) return;
    const id = 'node_'+Date.now();
    state.nodes.push({ id, type, name: type+'_'+state.nodes.length, config:{ script:''}, inputs:[...def.defaultInputs], outputs:[...def.defaultOutputs], language:'javascript', position:{ x:120 + (state.nodes.length*30)%400, y:120 + Math.floor(state.nodes.length/10)*100 }});
    selectNode(id);
    renderAll();
    markDirty();
  }
  function selectNode(id){ state.selectedNodeId = id; updateNodePanel(); renderNodes(); }
  function deleteNode(){ if(!state.selectedNodeId) return; state.connections = state.connections.filter(c=>c.fromNode!==state.selectedNodeId && c.toNode!==state.selectedNodeId); state.nodes = state.nodes.filter(n=>n.id!==state.selectedNodeId); state.selectedNodeId=null; updateNodePanel(); renderAll(); markDirty(); }
  function saveNode(){ const n=currentNode(); if(!n) return; n.name=qs('#n-name').value.trim()||n.id; n.inputs=qs('#n-inputs').value.split(',').map(s=>s.trim()).filter(Boolean); n.outputs=qs('#n-outputs').value.split(',').map(s=>s.trim()).filter(Boolean); n.config.script=qs('#n-script').value; renderAll(); markDirty(); notify('Node updated','success'); }
  function currentNode(){ return state.nodes.find(n=>n.id===state.selectedNodeId); }

  // ------------- Canvas Rendering -------------
  function renderAll(){ renderNodes(); renderConnections(); renderConnectionsList(); }
  function renderNodes(){ const canvas=qs('#flow-canvas'); if(!canvas) return; canvas.querySelectorAll('.flow-node').forEach(el=>el.remove()); state.nodes.forEach(node=>{ const el=ce('div'); el.className='flow-node'+(state.selectedNodeId===node.id?' selected':''); el.style.left=node.position.x+'px'; el.style.top=node.position.y+'px'; el.dataset.nodeId=node.id; el.innerHTML = `<div class=\"flow-node-header\"><span>${escape(node.name)}</span></div><div class=\"flow-node-ports\"><div>${node.inputs.map(p=>`<div class='node-port input-port' data-port='${escape(p)}' onclick=\"VisualFlow.handlePortClick('${node.id}','${escape(p)}','input')\">${escape(p)} <span class='port-hint-badge'>IN</span></div>`).join('')}</div><div>${node.outputs.map(p=>`<div class='node-port output-port ${(state.connectingFrom && state.connectingFrom.nodeId===node.id && state.connectingFrom.portName===p)?'connecting':''}' data-port='${escape(p)}' onclick=\"VisualFlow.handlePortClick('${node.id}','${escape(p)}','output')\">${escape(p)} <span class='port-hint-badge'>OUT</span></div>`).join('')}</div></div>`; el.addEventListener('mousedown',e=>{ if(e.target.closest('.node-port')) return; state.dragging.node=node; state.dragging.offsetX=e.offsetX; state.dragging.offsetY=e.offsetY; selectNode(node.id); }); canvas.appendChild(el); }); applyViewTransform(); }
  function renderConnections(){ const svg=qs('#connections-svg'); const canvas=qs('#flow-canvas'); const stage=qs('#stage'); if(!svg||!canvas||!stage) return; svg.setAttribute('width', stage.style.width||'2000px'); svg.setAttribute('height', stage.style.height||'1200px'); svg.innerHTML=''; state.connections.forEach((c,i)=>{ const from=canvas.querySelector(`[data-node-id='${c.fromNode}']`); const to=canvas.querySelector(`[data-node-id='${c.toNode}']`); if(!from||!to) return; const x1=from.offsetLeft + from.offsetWidth; const y1=from.offsetTop + 40; const x2=to.offsetLeft; const y2=to.offsetTop + 40; const dx=x2-x1; const curve=Math.min(Math.abs(dx)/2,60); const path=document.createElementNS('http://www.w3.org/2000/svg','path'); path.setAttribute('d',`M ${x1} ${y1} C ${x1+curve} ${y1}, ${x2-curve} ${y2}, ${x2} ${y2}`); path.setAttribute('class','connection-line'); path.onclick=()=>{ if(confirm('Delete connection?')){ state.connections.splice(i,1); renderAll(); markDirty(); } }; svg.appendChild(path); }); applyViewTransform(); }
  function renderConnectionsList(){ const list=qs('#connections-list'); if(!list) return; if(state.connections.length===0){ qs('#no-connections').style.display='block'; list.querySelectorAll('.conn-item').forEach(e=>e.remove()); return; } qs('#no-connections').style.display='none'; list.innerHTML = state.connections.map((c,i)=>`<div class='conn-item' style='margin:.25rem 0; display:flex; justify-content:space-between; gap:.25rem;'><span>${escape(c.fromNode)}.${escape(c.fromOutput)} → ${escape(c.toNode)}.${escape(c.toInput)}</span><button class='btn btn-danger btn-small' style='padding:2px 4px;font-size:.55rem;' onclick='VisualFlow.deleteConnection(${i})'>✕</button></div>`).join(''); }

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
  function handlePortClick(nodeId, portName, portType){ if(!state.connectingFrom){ if(portType==='output'){ state.connectingFrom={ nodeId, portName, portType }; highlightInputs(true); } } else { if(portType==='input' && state.connectingFrom.portType==='output'){ const connection={ fromNode: state.connectingFrom.nodeId, fromOutput: state.connectingFrom.portName, toNode: nodeId, toInput: portName }; if(!state.connections.some(c=>c.fromNode===connection.fromNode && c.fromOutput===connection.fromOutput && c.toNode===connection.toNode && c.toInput===connection.toInput)){ state.connections.push(connection); renderAll(); markDirty(); notify('Connection added','success'); } } state.connectingFrom=null; highlightInputs(false); renderNodes(); } }
  function highlightInputs(on){ const canvas=qs('#flow-canvas'); canvas.querySelectorAll('.input-port').forEach(el=>{ if(on) el.classList.add('valid-target'); else el.classList.remove('valid-target'); }); }
  function deleteConnection(i){ state.connections.splice(i,1); renderAll(); }

  // ------------- Node Panel -------------
  function updateNodePanel(){ const n=currentNode(); const empty=qs('#node-panel-empty'); const form=qs('#node-form'); if(!n){ empty.style.display='block'; form.style.display='none'; return; } empty.style.display='none'; form.style.display='block'; qs('#n-id').value=n.id; qs('#n-name').value=n.name; qs('#n-inputs').value=n.inputs.join(', '); qs('#n-outputs').value=n.outputs.join(', '); qs('#n-script').value=n.config?.script||''; enhanceScriptEditor(); }
  // Enhance script area after panel update
  function enhanceScriptEditor(){ const ta=qs('#n-script'); const gutter=qs('#n-script-lines'); if(!ta||!gutter) return; const updateLines=()=>{ const count = ta.value.split(/\n/).length; let html=''; for(let i=1;i<=count;i++) html+=i+'\n'; gutter.textContent=html.trimEnd(); }; if(!ta._enhanced){ ta.addEventListener('input',()=>{ updateLines(); autoGrow(); }); ta.addEventListener('keydown',e=>{ if(e.key==='Tab'){ e.preventDefault(); const start=ta.selectionStart; const end=ta.selectionEnd; const val=ta.value; ta.value=val.substring(0,start)+'  '+val.substring(end); ta.selectionStart=ta.selectionEnd=start+2; ta.dispatchEvent(new Event('input')); }}); const autoGrow=()=>{ ta.style.height='auto'; ta.style.height=Math.min(500, ta.scrollHeight)+'px'; }; ta._enhanced=true; updateLines(); autoGrow(); } else { updateLines(); }
    // Sync scroll
    ta.addEventListener('scroll',()=>{ gutter.scrollTop=ta.scrollTop; }); }
  enhanceScriptEditor();

  // ------------- Save/Delete Class -------------
  async function saveClass(){ const name=qs('#fc-name').value.trim(); const namespace=qs('#fc-namespace').value.trim(); const version=qs('#fc-version').value.trim()||'1.0.0'; if(!name||!namespace){ notify('Name & namespace required','error'); return; } const input={ name, namespace, version, description: qs('#fc-description').value.trim()||null, nodes: state.nodes.map(n=>({ id:n.id, type:n.type, name:n.name, config:{ script:n.config.script||'' }, inputs:n.inputs, outputs:n.outputs, language:n.language||'javascript', position: n.position? { x:n.position.x, y:n.position.y }: null })), connections: state.connections.map(c=>({ fromNode:c.fromNode, fromOutput:c.fromOutput, toNode:c.toNode, toInput:c.toInput })) }; const isUpdate = !!state.flowClass && state.flowClass.name === name; const mutation=isUpdate?`mutation($name:String!,$input:FlowClassInput!){ flow { updateClass(name:$name,input:$input){ name } } }`:`mutation($input:FlowClassInput!){ flow { createClass(input:$input){ name } } }`; try { await gql(mutation, isUpdate? { name, input } : { input }); notify('Class saved','success'); state.dirty=false; updateTitleDirty(); if(!isUpdate){ // redirect with name param
        location.href='/pages/workflows-visual.html?name='+encodeURIComponent(name); }
    state.flowClass={ name, namespace, version }; }
    catch(e){ console.error(e); notify('Save failed: '+e.message,'error'); }
  }
  async function deleteClass(){ if(!state.flowClass){ notify('Nothing to delete','error'); return; } if(!confirm('Delete this flow class?')) return; try { await gql(`mutation($name:String!){ flow { deleteClass(name:$name) } }`, { name: state.flowClass.name }); notify('Deleted','success'); location.href='/pages/workflows.html'; } catch(e){ console.error(e); notify('Delete failed: '+e.message,'error'); } }

  // ------------- View Helpers -------------
  function resetView(){ state.view.scale=1; state.view.x=0; state.view.y=0; applyViewTransform(); }
  function applyViewTransform(){ const stage=qs('#stage'); if(!stage) return; const { scale,x,y } = state.view; stage.style.transform = `translate(${x}px, ${y}px) scale(${scale})`; }
  function zoom(delta, centerX, centerY){ const prevScale=state.view.scale; const newScale = Math.min(2, Math.max(0.25, prevScale * (delta>0?0.9:1.1))); if(newScale===prevScale) return; const wrapper=qs('#canvas-wrapper'); const rect=wrapper.getBoundingClientRect(); const cx = centerX - rect.left; const cy = centerY - rect.top; // adjust translation so point stays stable
    state.view.x = cx/prevScale - (cx/newScale - state.view.x);
    state.view.y = cy/prevScale - (cy/newScale - state.view.y);
    state.view.scale = newScale; applyViewTransform(); }

  function handleWheel(e){
    // Only zoom when Shift + wheel (and not ctrl/cmd pinch gesture)
    if(e.ctrlKey || e.metaKey) return; // let browser handle pinch zoom or native behaviors
    if(e.shiftKey){
      e.preventDefault();
      zoom(e.deltaY, e.clientX, e.clientY);
    } // else: allow normal vertical scroll of the canvas wrapper
  }
  function handleKeyDown(e){ if(e.code==='Space'){ if(!state.view.isPanning){ state.view.isPanning=true; document.body.style.cursor='grab'; } e.preventDefault(); } }
  function handleKeyUp(e){ if(e.code==='Space'){ state.view.isPanning=false; document.body.style.cursor=''; } }

  // allow middle mouse pan
  document.addEventListener('mousedown', e=>{ if(e.button===1){ state.view.isPanning=true; e.preventDefault(); }});
  document.addEventListener('mouseup', e=>{ if(e.button===1){ state.view.isPanning=false; }});

  // ------------- Dirty Tracking -------------
  function markDirty(){ if(!state.dirty){ state.dirty=true; updateTitleDirty(); } }
  function updateTitleDirty(){ const t = state.dirty? '● '+document.title.replace(/^●\s+/,'') : document.title.replace(/^●\s+/,''); document.title=t; const status=qs('#status-text'); if(status){ status.textContent = state.dirty? 'Unsaved changes' : 'Ready'; }
    refreshConnectionHelper();
  }
  window.addEventListener('beforeunload', (e)=>{ if(state.dirty){ e.preventDefault(); e.returnValue='You have unsaved changes.'; } });

  // Mark dirty on form edits
  ['fc-name','fc-namespace','fc-version','fc-description','n-name','n-inputs','n-outputs','n-script'].forEach(id=>{
    document.addEventListener('input', ev=>{ if(ev.target && ev.target.id===id) markDirty(); });
  });

  // ------------- Connection Helper -------------
  function refreshConnectionHelper(){ const fromSel=qs('#conn-from'); const toSel=qs('#conn-to'); if(!fromSel||!toSel) return; const nodeMap = Object.fromEntries(state.nodes.map(n=>[n.id,n])); const fromOptions=[]; state.nodes.forEach(n=>{ n.outputs.forEach(o=> fromOptions.push(`${n.id}.${o}`)); }); const toOptions=[]; state.nodes.forEach(n=>{ n.inputs.forEach(i=> toOptions.push(`${n.id}.${i}`)); }); const selValFrom=fromSel.value; const selValTo=toSel.value; fromSel.innerHTML='<option value="">(select)</option>'+fromOptions.map(v=>`<option>${v}</option>`).join(''); toSel.innerHTML='<option value="">(select)</option>'+toOptions.map(v=>`<option>${v}</option>`).join(''); if(fromOptions.includes(selValFrom)) fromSel.value=selValFrom; if(toOptions.includes(selValTo)) toSel.value=selValTo; }
  function addConnectionHelper(){ const from=qs('#conn-from').value; const to=qs('#conn-to').value; if(!from||!to) { notify('Select both endpoints','error'); return; } const [fromNode, fromOutput]=from.split('.'); const [toNode,toInput]=to.split('.'); if(fromNode===toNode){ notify('Cannot connect node to itself','error'); return; } if(state.connections.some(c=>c.fromNode===fromNode && c.fromOutput===fromOutput && c.toNode===toNode && c.toInput===toInput)){ notify('Connection exists','error'); return; } state.connections.push({ fromNode, fromOutput, toNode, toInput }); renderAll(); markDirty(); notify('Connection added','success'); }

  // initial population after load will call refresh in updateTitleDirty()

  // ------------- Notifications -------------
  function notify(msg,type='info'){ const div=ce('div'); div.textContent=msg; div.style.cssText=`position:fixed;top:18px;right:18px;background:${type==='error'?'#dc3545':type==='success'?'#28a745':'#17a2b8'};color:#fff;padding:.5rem .75rem;border-radius:4px;font-size:.65rem;z-index:10000;`; document.body.appendChild(div); setTimeout(()=>{ div.style.opacity='0'; setTimeout(()=>div.remove(),300); },2200); }

  // ------------- Events -------------
  document.addEventListener('mousemove', handleMouseMove);
  document.addEventListener('mouseup', handleMouseUp);
  document.addEventListener('wheel', handleWheel, { passive:false });
  document.addEventListener('keydown', handleKeyDown);
  document.addEventListener('keyup', handleKeyUp);

  return { init, addNodeType, saveClass, deleteClass, handlePortClick, deleteConnection, saveNode, deleteNode, addNodeType: addNodeType, resetView, addConnectionHelper, refreshConnectionHelper };
})();

document.addEventListener('DOMContentLoaded', ()=> VisualFlow.init());
