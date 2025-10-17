/**
 * Log Viewer Component (clean implementation with resize)
 */
class LogViewer {
  constructor(containerId, options = {}) {
    this.containerId = containerId;
    this.options = {
      collapsed: options.collapsed !== false,
      filters: options.filters || {},
      maxLines: options.maxLines || 1000,
      autoScroll: options.autoScroll !== false,
      wsUrl: options.wsUrl || this.getWebSocketUrl(),
      title: options.title || 'System Logs',
      ...options
    };
    this.logs = [];
    this.ws = null;
    this.subscriptionId = 1;
    this.hasSubscribed = false; // Track if initial subscription was made
    this.reconnectAttempts = 0;
    this.maxReconnectAttempts = 10;
    this.reconnectDelay = 2000;
    this.isConnected = false;
    this.isCollapsed = this.options.collapsed;
    this.newLogsCount = 0;
    // Resize related
    this.defaultExpandedHeight = 400;
    this.minHeight = 160;
    this.maxHeight = 800;
    this.currentHeight = parseInt(localStorage.getItem('monstermq_logviewer_height'), 10) || this.defaultExpandedHeight;
    this.isResizing = false;
    this.init();
  }

  getWebSocketUrl() {
    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const host = window.location.hostname;
    const port = window.location.port;
    const portPart = port ? `:${port}` : '';
    return `${protocol}//${host}${portPart}/graphqlws`;
  }

  init() {
    this.createUI();
    this.attachEventListeners();
    if (!this.options.deferConnection) {
      this.connect();
    }
    this.applyBodyPadding();
  }

  createUI() {
    const container = document.getElementById(this.containerId);
    if (!container) {
      console.error(`Log Viewer: Container #${this.containerId} not found`);
      return;
    }
    container.innerHTML = `
      <style>
        .log-viewer { position: fixed; bottom:0; left:0; right:0; background:#1e1e1e; color:#d4d4d4; font-family:Consolas,Monaco,'Courier New',monospace; font-size:12px; border-top:2px solid #007acc; box-shadow:0 -2px 10px rgba(0,0,0,.3); z-index:9999; display:flex; flex-direction:column; transition:height .25s ease; backdrop-filter:blur(4px);} 
        .log-viewer.expanded { height: var(--log-viewer-height, 400px); }
        .log-viewer.collapsed { height:36px !important; }
        .log-viewer-header { display:flex; align-items:center; padding:6px 10px; background:#2d2d2d; border-bottom:1px solid #3e3e3e; cursor:pointer; user-select:none; }
        .log-viewer-title { flex:1; font-weight:bold; display:flex; align-items:center; gap:8px; }
        .log-viewer-title .chevron { transition: transform .25s ease; font-size:14px; }
        .log-viewer.collapsed .chevron { transform:rotate(-90deg); }
        .log-viewer-badge { background:#007acc; color:#fff; padding:2px 8px; border-radius:10px; font-size:11px; font-weight:bold; }
        .log-viewer-status { margin-right:10px; display:flex; align-items:center; gap:6px; font-size:11px; }
        .log-viewer-status-dot { width:8px; height:8px; border-radius:50%; background:#6c757d; }
        .log-viewer-status.connected .log-viewer-status-dot { background:#28a745; box-shadow:0 0 6px #28a745; }
        .log-viewer-status.disconnected .log-viewer-status-dot { background:#dc3545; }
        .log-viewer-status.connecting .log-viewer-status-dot { background:#ffc107; animation:pulse 1s infinite; }
        @keyframes pulse { 0%,100%{opacity:1;} 50%{opacity:.5;} }
        .log-viewer-actions { display:flex; gap:6px; }
        .log-viewer-btn { background:#3e3e3e; border:1px solid #555; color:#d4d4d4; padding:3px 10px; border-radius:3px; cursor:pointer; font-size:11px; }
        .log-viewer-btn:hover { background:#505050; }
        .log-viewer-resize-handle { height:6px; cursor:ns-resize; background:linear-gradient(90deg,#007acc,#004f80); border-top:1px solid #004f80; }
        .log-viewer-resize-handle:hover { background:linear-gradient(90deg,#0090ff,#006ba8); }
        .log-viewer-filters { padding:6px 10px; background:#252526; border-bottom:1px solid #3e3e3e; display:flex; gap:12px; flex-wrap:wrap; }
        .log-viewer-filter { display:flex; align-items:center; gap:6px; }
        .log-viewer-filter label { font-size:11px; color:#999; }
        .log-viewer-filter input[type=text], .log-viewer-filter select { background:#3c3c3c; border:1px solid #555; color:#d4d4d4; padding:4px 6px; border-radius:3px; font-size:11px; }
        .log-viewer-filter input[type=text]:focus { outline:none; border-color:#007acc; }
        .log-viewer-level-checkboxes { display:flex; gap:10px; }
        .log-viewer-content { flex:1; overflow-y:auto; padding:6px 8px; background:#1e1e1e; }
        .log-viewer-logs { display:flex; flex-direction:column; gap:2px; }
        .log-entry { padding:4px 8px; border-left:3px solid transparent; }
        .log-entry:hover { background:#2d2d30; }
        .log-entry.level-INFO { border-left-color:#007acc; }
        .log-entry.level-WARNING { border-left-color:#ff9800; }
        .log-entry.level-SEVERE { border-left-color:#f44336; }
        .log-entry-header { display:flex; gap:8px; font-size:12px; }
        .log-entry-time { color:#6a9955; min-width:80px; }
        .log-entry-level { font-weight:bold; min-width:70px; }
        .log-entry-level.INFO { color:#4fc3f7; }
        .log-entry-level.WARNING { color:#ffb74d; }
        .log-entry-level.SEVERE { color:#ef5350; }
        .log-entry-logger { color:#9cdcfe; max-width:300px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
        .log-entry-message { color:#d4d4d4; flex:1; word-wrap:break-word; }
        .log-entry-exception { margin-top:6px; padding:6px; background:#2d2d2d; border-left:3px solid #f44336; border-radius:3px; }
        .exception-header { color:#ef5350; font-weight:bold; margin-bottom:4px; cursor:pointer; display:flex; align-items:center; gap:6px; font-size:12px; }
        .exception-header .chevron { transition: transform .2s ease; font-size:12px; }
        .exception-header.collapsed .chevron { transform:rotate(-90deg); }
        .exception-stacktrace { margin:6px 0 0 0; padding:6px; background:#1e1e1e; border-radius:3px; font-size:11px; color:#ce9178; white-space:pre-wrap; word-wrap:break-word; max-height:200px; overflow-y:auto; }
        .exception-stacktrace.collapsed { display:none; }
      </style>
      <div class="log-viewer ${this.isCollapsed ? 'collapsed' : 'expanded'}" style="--log-viewer-height:${this.currentHeight}px;">
        <div class="log-viewer-header">
          <div class="log-viewer-title">
            <span class="chevron">▼</span>
            ${this.options.title}
            <span class="log-viewer-badge" style="display:none;">0</span>
          </div>
          <div class="log-viewer-status connecting">
            <span class="log-viewer-status-dot"></span>
            <span class="log-viewer-status-text">Connecting...</span>
          </div>
          <div class="log-viewer-actions">
            <button class="log-viewer-btn" data-action="clear">Clear</button>
            <button class="log-viewer-btn" data-action="export">Export</button>
          </div>
        </div>
        <div class="log-viewer-resize-handle" title="Drag to resize"></div>
        <div class="log-viewer-filters">
          <div class="log-viewer-filter">
            <label>Node:</label>
            <select data-filter="node"><option value="">All</option></select>
          </div>
          <div class="log-viewer-filter">
            <label>Level:</label>
            <div class="log-viewer-level-checkboxes">
              <label class="log-viewer-level-checkbox"><input type="checkbox" data-level="INFO" checked><span>INFO</span></label>
              <label class="log-viewer-level-checkbox"><input type="checkbox" data-level="WARNING" checked><span>WARNING</span></label>
              <label class="log-viewer-level-checkbox"><input type="checkbox" data-level="SEVERE" checked><span>SEVERE</span></label>
            </div>
          </div>
          <div class="log-viewer-filter"><label>Logger:</label><input type="text" data-filter="loggerRegex" placeholder="Regex..."></div>
          <div class="log-viewer-filter"><label>Source:</label><input type="text" data-filter="sourceClassRegex" placeholder="Regex..."></div>
          <div class="log-viewer-filter"><label>Message:</label><input type="text" data-filter="messageRegex" placeholder="Regex..."></div>
        </div>
        <div class="log-viewer-content"><div class="log-viewer-logs"></div></div>
      </div>`;
    this.elements = {
      root: container.querySelector('.log-viewer'),
      header: container.querySelector('.log-viewer-header'),
      badge: container.querySelector('.log-viewer-badge'),
      status: container.querySelector('.log-viewer-status'),
      statusText: container.querySelector('.log-viewer-status-text'),
      resizeHandle: container.querySelector('.log-viewer-resize-handle'),
      content: container.querySelector('.log-viewer-content'),
      logs: container.querySelector('.log-viewer-logs'),
      filters: {
        node: container.querySelector('[data-filter="node"]'),
        loggerRegex: container.querySelector('[data-filter="loggerRegex"]'),
        sourceClassRegex: container.querySelector('[data-filter="sourceClassRegex"]'),
        messageRegex: container.querySelector('[data-filter="messageRegex"]'),
        levels: {
          INFO: container.querySelector('[data-level="INFO"]'),
          WARNING: container.querySelector('[data-level="WARNING"]'),
          SEVERE: container.querySelector('[data-level="SEVERE"]')
        }
      }
    };
    this.applyInitialFilters();
  }

  applyInitialFilters() {
    const f = this.options.filters;
    if (!f) return;
    if (f.node) this.elements.filters.node.value = f.node;
    if (f.loggerRegex) this.elements.filters.loggerRegex.value = f.loggerRegex;
    if (f.sourceClassRegex) this.elements.filters.sourceClassRegex.value = f.sourceClassRegex;
    if (f.messageRegex) this.elements.filters.messageRegex.value = f.messageRegex;
    if (Array.isArray(f.level)) {
      Object.values(this.elements.filters.levels).forEach(cb => cb.checked = false);
      f.level.forEach(l => { if (this.elements.filters.levels[l]) this.elements.filters.levels[l].checked = true; });
    }
  }

  attachEventListeners() {
    // Collapse toggle
    this.elements.header.addEventListener('click', e => {
      if (e.target.closest('.log-viewer-actions')) return;
      this.toggleCollapse();
    });
    // Actions
    this.elements.root.addEventListener('click', e => {
      const btn = e.target.closest('[data-action]');
      if (!btn) return;
      if (btn.dataset.action === 'clear') this.clear();
      else if (btn.dataset.action === 'export') this.export();
    });
    // Filters debounce
    let t;
    const inputs = [
      this.elements.filters.node,
      this.elements.filters.loggerRegex,
      this.elements.filters.sourceClassRegex,
      this.elements.filters.messageRegex,
      ...Object.values(this.elements.filters.levels)
    ];
    inputs.forEach(inp => {
      const handler = () => { clearTimeout(t); t = setTimeout(() => this.reconnectWithFilters(), 400); };
      inp.addEventListener('change', handler);
      if (inp.type === 'text') inp.addEventListener('input', handler);
    });
    // Resize handle
    if (this.elements.resizeHandle) {
      const move = e => {
        if (!this.isResizing) return;
        const vh = window.innerHeight;
        const h = Math.min(this.maxHeight, Math.max(this.minHeight, vh - e.clientY));
        this.currentHeight = h;
        this.elements.root.style.setProperty('--log-viewer-height', h + 'px');
        localStorage.setItem('monstermq_logviewer_height', String(h));
        this.applyBodyPadding();
      };
      const up = () => {
        if (!this.isResizing) return;
        this.isResizing = false;
        document.body.style.userSelect = '';
        window.removeEventListener('mousemove', move);
        window.removeEventListener('mouseup', up);
      };
      this.elements.resizeHandle.addEventListener('mousedown', e => {
        e.preventDefault();
        if (this.isCollapsed) return;
        this.isResizing = true;
        document.body.style.userSelect = 'none';
        window.addEventListener('mousemove', move);
        window.addEventListener('mouseup', up);
      });
    }
    // Scroll isolation
    this.elements.content.addEventListener('wheel', e => {
      const atTop = this.elements.content.scrollTop === 0;
      const atBottom = Math.ceil(this.elements.content.scrollTop + this.elements.content.clientHeight) >= this.elements.content.scrollHeight;
      if ((e.deltaY < 0 && atTop) || (e.deltaY > 0 && atBottom)) e.preventDefault();
      e.stopPropagation();
    }, { passive: false });
  }

  toggleCollapse() {
    this.isCollapsed = !this.isCollapsed;
    if (this.isCollapsed) {
      this.elements.root.classList.add('collapsed');
      this.elements.root.classList.remove('expanded');
      this.applyBodyPadding();
    } else {
      this.elements.root.classList.remove('collapsed');
      this.elements.root.classList.add('expanded');
      this.applyBodyPadding();
      if (this.options.autoScroll) this.elements.content.scrollTop = this.elements.content.scrollHeight;
      this.newLogsCount = 0; this.elements.badge.style.display = 'none';
    }
  }

  applyBodyPadding() {
    // Adjust layout so main content shrinks rather than being covered.
    const main = document.getElementById('main-content');
    const viewerHeight = this.isCollapsed ? 36 : this.currentHeight;
    if (main) {
      // Set explicit max-height instead of height to work with min-height: 100vh
      main.style.maxHeight = `calc(100vh - ${viewerHeight}px)`;
      main.style.height = `calc(100vh - ${viewerHeight}px)`;
      main.style.overflowY = 'auto';
      main.style.boxSizing = 'border-box';
      // Clear body padding
      document.body.style.paddingBottom = '0';
    } else {
      // Fallback: body padding if no main-content container.
      document.body.style.paddingBottom = viewerHeight + 'px';
    }
  }

  getCurrentFilters() {
    const filters = {};
    const node = this.elements.filters.node.value; if (node) filters.node = node;
    const loggerRegex = this.elements.filters.loggerRegex.value.trim(); if (loggerRegex) filters.loggerRegex = loggerRegex;
    const sourceClassRegex = this.elements.filters.sourceClassRegex.value.trim(); if (sourceClassRegex) filters.sourceClassRegex = sourceClassRegex;
    const messageRegex = this.elements.filters.messageRegex.value.trim(); if (messageRegex) filters.messageRegex = messageRegex;
    const levels = Object.entries(this.elements.filters.levels).filter(([_, cb]) => cb.checked).map(([lvl]) => lvl);
    if (levels.length > 0 && levels.length < 3) filters.level = levels; // if all selected skip sending
    return filters;
  }

  reconnectWithFilters() { 
    this.disconnect(); 
    // Small delay to ensure disconnect completes before reconnecting
    setTimeout(() => this.connect(), 100); 
  }

  connect() {
    this.updateStatus('connecting', 'Connecting...');
    try {
      this.ws = new WebSocket(this.options.wsUrl, 'graphql-transport-ws');
      this.ws.onopen = () => this.send({ type: 'connection_init', payload: {} });
      this.ws.onmessage = ev => { try { this.handleMessage(JSON.parse(ev.data)); } catch (e) { console.error('LogViewer parse error', e); } };
      this.ws.onerror = err => { console.error('LogViewer WS error', err); this.updateStatus('disconnected', 'Error'); };
      this.ws.onclose = () => { this.isConnected = false; this.updateStatus('disconnected', 'Disconnected'); this.attemptReconnect(); };
    } catch (e) {
      console.error('LogViewer WS create error', e); this.updateStatus('disconnected', 'Failed'); this.attemptReconnect();
    }
  }

  handleMessage(msg) {
    switch (msg.type) {
      case 'connection_ack':
        this.isConnected = true; this.reconnectAttempts = 0; this.updateStatus('connected', 'Connected'); this.subscribe(); break;
      case 'next':
        if (msg.payload?.data?.systemLogs) this.addLog(msg.payload.data.systemLogs); break;
      case 'error':
        console.error('LogViewer subscription error', msg.payload); this.updateStatus('disconnected', 'Error'); break;
      case 'complete':
        break;
      case 'ping':
        this.send({ type: 'pong' }); break;
      default:
        break;
    }
  }

  subscribe() {
    this.subscriptionId++; // Increment for each new subscription
    this.hasSubscribed = true; // Mark that we've made a subscription
    const f = this.getCurrentFilters();
    const query = `subscription SystemLogs($node:String,$level:String,$logger:String,$thread:Long,$sourceClass:String,$sourceMethod:String,$message:String){systemLogs(node:$node,level:$level,logger:$logger,thread:$thread,sourceClass:$sourceClass,sourceMethod:$sourceMethod,message:$message){timestamp level logger message thread node sourceClass sourceMethod parameters exception{class message stackTrace}}}`;
    const vars = {};
    if (f.node) vars.node = f.node;
    if (f.loggerRegex) vars.logger = f.loggerRegex;
    if (f.messageRegex) vars.message = f.messageRegex;
    if (f.sourceClassRegex) vars.sourceClass = f.sourceClassRegex;
    if (f.sourceMethodRegex) vars.sourceMethod = f.sourceMethodRegex;
    if (f.level) { if (Array.isArray(f.level) && f.level.length === 1) vars.level = f.level[0]; }
    this.send({ id: String(this.subscriptionId), type: 'subscribe', payload: { query, variables: vars } });
  }

  send(m) { if (this.ws && this.ws.readyState === WebSocket.OPEN) this.ws.send(JSON.stringify(m)); }

  disconnect() {
    if (this.ws) { this.send({ id: String(this.subscriptionId), type: 'complete' }); this.ws.close(); this.ws = null; }
    this.isConnected = false;
  }

  attemptReconnect() {
    if (this.reconnectAttempts >= this.maxReconnectAttempts) { this.updateStatus('disconnected', 'Failed'); return; }
    this.reconnectAttempts++; const delay = this.reconnectDelay * Math.min(this.reconnectAttempts, 5);
    this.updateStatus('connecting', `Reconnecting (${this.reconnectAttempts})`);
    setTimeout(() => this.connect(), delay);
  }

  addLog(entry) {
    this.logs.push(entry); if (this.logs.length > this.options.maxLines) this.logs.shift();
    const el = this.createLogElement(entry); this.elements.logs.appendChild(el);
    while (this.elements.logs.children.length > this.options.maxLines) this.elements.logs.removeChild(this.elements.logs.firstChild);
    if (this.isCollapsed) { this.newLogsCount++; this.elements.badge.textContent = this.newLogsCount; this.elements.badge.style.display = 'inline-block'; }
    if (this.options.autoScroll && !this.isCollapsed) this.elements.content.scrollTop = this.elements.content.scrollHeight;
    this.updateNodeFilter(entry.node);
  }

  createLogElement(log) {
    const div = document.createElement('div'); div.className = `log-entry level-${log.level}`;
    const time = new Date(log.timestamp).toLocaleTimeString('en-US', { hour12: false });
    const loggerShort = log.logger.length > 40 ? '...' + log.logger.slice(-37) : log.logger;
    let html = `<div class="log-entry-header"><span class="log-entry-time">${time}</span><span class="log-entry-level ${log.level}">[${log.level}]</span><span class="log-entry-logger" title="${this.escapeHtml(log.logger)}">[${this.escapeHtml(loggerShort)}]</span><span class="log-entry-message">${this.escapeHtml(log.message)}</span></div>`;
    if (log.exception) {
      const id = 'exc-' + Date.now() + '-' + Math.random().toString(36).slice(2);
      html += `<div class="log-entry-exception"><div class="exception-header" data-exception-id="${id}"><span class="chevron">▼</span>⚠️ ${this.escapeHtml(log.exception.class)}: ${this.escapeHtml(log.exception.message||'No message')}</div><pre class="exception-stacktrace" id="${id}">${this.escapeHtml(log.exception.stackTrace)}</pre></div>`;
    }
    div.innerHTML = html;
    const eh = div.querySelector('.exception-header');
    if (eh) eh.addEventListener('click', e => { e.stopPropagation(); const st = document.getElementById(eh.dataset.exceptionId); st.classList.toggle('collapsed'); eh.classList.toggle('collapsed'); });
    return div;
  }

  updateNodeFilter(node) {
    if (!node) return; const sel = this.elements.filters.node; if ([...sel.options].some(o=>o.value===node)) return; const opt=document.createElement('option'); opt.value=node; opt.textContent=node; sel.appendChild(opt);
  }

  updateStatus(state, text) { this.elements.status.className = `log-viewer-status ${state}`; this.elements.statusText.textContent = text; }

  clear() { this.logs=[]; this.elements.logs.innerHTML=''; this.newLogsCount=0; this.elements.badge.style.display='none'; }

  export() { const data = JSON.stringify(this.logs, null, 2); const blob = new Blob([data], { type:'application/json' }); const url = URL.createObjectURL(blob); const a=document.createElement('a'); a.href=url; a.download=`system-logs-${new Date().toISOString()}.json`; document.body.appendChild(a); a.click(); document.body.removeChild(a); URL.revokeObjectURL(url); }

  setFilters(f) { 
    // Track if any filter actually changed
    let changed = false;
    
    if (f.node!==undefined && this.elements.filters.node.value !== (f.node||'')) { 
      this.elements.filters.node.value=f.node||''; 
      changed = true;
    } 
    if (f.loggerRegex!==undefined && this.elements.filters.loggerRegex.value !== (f.loggerRegex||'')) { 
      this.elements.filters.loggerRegex.value=f.loggerRegex||''; 
      changed = true;
    } 
    if (f.sourceClassRegex!==undefined && this.elements.filters.sourceClassRegex.value !== (f.sourceClassRegex||'')) { 
      this.elements.filters.sourceClassRegex.value=f.sourceClassRegex||''; 
      changed = true;
    } 
    if (f.messageRegex!==undefined && this.elements.filters.messageRegex.value !== (f.messageRegex||'')) { 
      this.elements.filters.messageRegex.value=f.messageRegex||''; 
      changed = true;
    } 
    if (Array.isArray(f.level)) { 
      const currentLevels = Object.entries(this.elements.filters.levels)
        .filter(([_, cb]) => cb.checked)
        .map(([lvl]) => lvl)
        .sort()
        .join(',');
      const newLevels = [...f.level].sort().join(',');
      if (currentLevels !== newLevels) {
        Object.values(this.elements.filters.levels).forEach(cb=>cb.checked=false); 
        f.level.forEach(l=>{ if (this.elements.filters.levels[l]) this.elements.filters.levels[l].checked=true; }); 
        changed = true;
      }
    } 
    
    // Only reconnect if something actually changed AND we've already subscribed
    // If we haven't subscribed yet, the filters will be picked up on the first subscription
    if (changed && this.hasSubscribed) {
      this.reconnectWithFilters(); 
    }
  }

  destroy() { this.disconnect(); const c=document.getElementById(this.containerId); if (c) c.innerHTML=''; }

  escapeHtml(txt) { if (!txt) return ''; const d=document.createElement('div'); d.textContent=txt; return d.innerHTML; }
}

if (typeof module !== 'undefined' && module.exports) module.exports = LogViewer;
