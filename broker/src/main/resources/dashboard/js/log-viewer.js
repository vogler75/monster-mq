/**
 * Log Viewer Component
 * 
 * A reusable, collapsible log viewer that connects to the GraphQL systemLogs subscription
 * using the graphql-transport-ws protocol (GraphQL over WebSocket Protocol).
 * 
 * Features:
 * - Real-time log streaming via GraphQL WebSocket
 * - Collapsible panel at bottom of page
 * - Auto-scroll to latest logs
 * - Color-coded log levels (INFO, WARNING, SEVERE)
 * - Filtering by node, level, logger regex, sourceClass regex, message regex
 * - Connection status indicator
 * - Auto-reconnect on connection loss
 * - Clear logs and export functionality
 * - Exception display with expandable stacktrace
 */

class LogViewer {
    constructor(containerId, options = {}) {
        this.containerId = containerId;
        this.options = {
            collapsed: options.collapsed !== false, // Default: collapsed
            filters: options.filters || {},
            maxLines: options.maxLines || 1000,
            autoScroll: options.autoScroll !== false,
            wsUrl: options.wsUrl || this.getWebSocketUrl(),
            title: options.title || 'System Logs',
            ...options
        };
        
        this.logs = [];
        this.ws = null;
        this.subscriptionId = '1';
        this.reconnectAttempts = 0;
        this.maxReconnectAttempts = 10;
        this.reconnectDelay = 2000;
        this.isConnected = false;
        this.isCollapsed = this.options.collapsed;
        this.newLogsCount = 0;
        
        this.init();
    }
    
    /**
     * Get WebSocket URL from current location
     */
    getWebSocketUrl() {
        const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
        const host = window.location.hostname;
        const port = window.location.port;
        // Only include port if explicitly set (not using standard 80/443)
        const portPart = port ? `:${port}` : '';
        return `${protocol}//${host}${portPart}/graphqlws`;
    }
    
    /**
     * Initialize the log viewer UI and connect to WebSocket
     */
    init() {
        this.createUI();
        this.attachEventListeners();
        this.connect();
    }
    
    /**
     * Create the log viewer UI
     */
    createUI() {
        const container = document.getElementById(this.containerId);
        if (!container) {
            console.error(`Log Viewer: Container #${this.containerId} not found`);
            return;
        }
        
        container.innerHTML = `
            <style>
                .log-viewer {
                    position: fixed;
                    bottom: 0;
                    left: 0;
                    right: 0;
                    background: #1e1e1e;
                    color: #d4d4d4;
                    font-family: 'Consolas', 'Monaco', 'Courier New', monospace;
                    font-size: 12px;
                    border-top: 2px solid #007acc;
                    box-shadow: 0 -2px 10px rgba(0,0,0,0.3);
                    z-index: 9999;
                    display: flex;
                    flex-direction: column;
                    transition: height 0.3s ease;
                }
                
                .log-viewer.collapsed {
                    height: 36px !important;
                }
                
                .log-viewer.expanded {
                    height: 400px;
                }
                
                .log-viewer-header {
                    display: flex;
                    align-items: center;
                    padding: 8px 12px;
                    background: #2d2d2d;
                    border-bottom: 1px solid #3e3e3e;
                    cursor: pointer;
                    user-select: none;
                }
                
                .log-viewer-title {
                    flex: 1;
                    font-weight: bold;
                    display: flex;
                    align-items: center;
                    gap: 8px;
                }
                
                .log-viewer-title .chevron {
                    transition: transform 0.3s ease;
                    font-size: 14px;
                }
                
                .log-viewer.collapsed .chevron {
                    transform: rotate(-90deg);
                }
                
                .log-viewer-badge {
                    background: #007acc;
                    color: white;
                    padding: 2px 8px;
                    border-radius: 10px;
                    font-size: 11px;
                    font-weight: bold;
                    margin-left: 8px;
                }
                
                .log-viewer-status {
                    margin-right: 12px;
                    display: flex;
                    align-items: center;
                    gap: 6px;
                    font-size: 11px;
                }
                
                .log-viewer-status-dot {
                    width: 8px;
                    height: 8px;
                    border-radius: 50%;
                    background: #6c757d;
                }
                
                .log-viewer-status.connected .log-viewer-status-dot {
                    background: #28a745;
                    box-shadow: 0 0 6px #28a745;
                }
                
                .log-viewer-status.disconnected .log-viewer-status-dot {
                    background: #dc3545;
                }
                
                .log-viewer-status.connecting .log-viewer-status-dot {
                    background: #ffc107;
                    animation: pulse 1s infinite;
                }
                
                @keyframes pulse {
                    0%, 100% { opacity: 1; }
                    50% { opacity: 0.5; }
                }
                
                .log-viewer-actions {
                    display: flex;
                    gap: 8px;
                }
                
                .log-viewer-btn {
                    background: #3e3e3e;
                    border: 1px solid #555;
                    color: #d4d4d4;
                    padding: 4px 12px;
                    border-radius: 3px;
                    cursor: pointer;
                    font-size: 11px;
                    transition: background 0.2s;
                }
                
                .log-viewer-btn:hover {
                    background: #505050;
                }
                
                .log-viewer-filters {
                    padding: 8px 12px;
                    background: #252526;
                    border-bottom: 1px solid #3e3e3e;
                    display: flex;
                    gap: 12px;
                    flex-wrap: wrap;
                    align-items: center;
                }
                
                .log-viewer-filter {
                    display: flex;
                    align-items: center;
                    gap: 6px;
                }
                
                .log-viewer-filter label {
                    font-size: 11px;
                    color: #999;
                }
                
                .log-viewer-filter input[type="text"] {
                    background: #3c3c3c;
                    border: 1px solid #555;
                    color: #d4d4d4;
                    padding: 4px 8px;
                    border-radius: 3px;
                    font-size: 11px;
                    width: 150px;
                }
                
                .log-viewer-filter input[type="text"]:focus {
                    outline: none;
                    border-color: #007acc;
                }
                
                .log-viewer-filter select {
                    background: #3c3c3c;
                    border: 1px solid #555;
                    color: #d4d4d4;
                    padding: 4px 8px;
                    border-radius: 3px;
                    font-size: 11px;
                }
                
                .log-viewer-level-checkboxes {
                    display: flex;
                    gap: 12px;
                }
                
                .log-viewer-level-checkbox {
                    display: flex;
                    align-items: center;
                    gap: 4px;
                    cursor: pointer;
                }
                
                .log-viewer-level-checkbox input[type="checkbox"] {
                    cursor: pointer;
                }
                
                .log-viewer-content {
                    flex: 1;
                    overflow-y: auto;
                    padding: 8px;
                    background: #1e1e1e;
                }
                
                .log-viewer-logs {
                    display: flex;
                    flex-direction: column;
                    gap: 2px;
                }
                
                .log-entry {
                    padding: 4px 8px;
                    border-left: 3px solid transparent;
                    transition: background 0.1s;
                }
                
                .log-entry:hover {
                    background: #2d2d30;
                }
                
                .log-entry.level-INFO {
                    border-left-color: #007acc;
                }
                
                .log-entry.level-WARNING {
                    border-left-color: #ff9800;
                }
                
                .log-entry.level-SEVERE {
                    border-left-color: #f44336;
                }
                
                .log-entry-header {
                    display: flex;
                    gap: 8px;
                    font-size: 12px;
                }
                
                .log-entry-time {
                    color: #6a9955;
                    min-width: 80px;
                }
                
                .log-entry-level {
                    font-weight: bold;
                    min-width: 70px;
                }
                
                .log-entry-level.INFO {
                    color: #4fc3f7;
                }
                
                .log-entry-level.WARNING {
                    color: #ffb74d;
                }
                
                .log-entry-level.SEVERE {
                    color: #ef5350;
                }
                
                .log-entry-logger {
                    color: #9cdcfe;
                    flex: 0 0 auto;
                    max-width: 300px;
                    overflow: hidden;
                    text-overflow: ellipsis;
                    white-space: nowrap;
                }
                
                .log-entry-message {
                    color: #d4d4d4;
                    flex: 1;
                    word-wrap: break-word;
                }
                
                .log-entry-exception {
                    margin-top: 8px;
                    padding: 8px;
                    background: #2d2d2d;
                    border-left: 3px solid #f44336;
                    border-radius: 3px;
                }
                
                .exception-header {
                    color: #ef5350;
                    font-weight: bold;
                    margin-bottom: 4px;
                    cursor: pointer;
                    display: flex;
                    align-items: center;
                    gap: 6px;
                }
                
                .exception-header .chevron {
                    transition: transform 0.2s ease;
                    font-size: 12px;
                }
                
                .exception-header.collapsed .chevron {
                    transform: rotate(-90deg);
                }
                
                .exception-stacktrace {
                    margin: 8px 0 0 0;
                    padding: 8px;
                    background: #1e1e1e;
                    border-radius: 3px;
                    font-size: 11px;
                    color: #ce9178;
                    white-space: pre-wrap;
                    word-wrap: break-word;
                    max-height: 200px;
                    overflow-y: auto;
                }
                
                .exception-stacktrace.collapsed {
                    display: none;
                }
                
                .log-viewer-empty {
                    padding: 20px;
                    text-align: center;
                    color: #6c757d;
                }
            </style>
            
            <div class="log-viewer ${this.isCollapsed ? 'collapsed' : 'expanded'}">
                <div class="log-viewer-header">
                    <div class="log-viewer-title">
                        <span class="chevron">▼</span>
                        ${this.options.title}
                        <span class="log-viewer-badge" style="display: none;">0</span>
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
                
                <div class="log-viewer-filters">
                    <div class="log-viewer-filter">
                        <label>Node:</label>
                        <select data-filter="node">
                            <option value="">All</option>
                        </select>
                    </div>
                    
                    <div class="log-viewer-filter">
                        <label>Level:</label>
                        <div class="log-viewer-level-checkboxes">
                            <label class="log-viewer-level-checkbox">
                                <input type="checkbox" data-level="INFO" checked>
                                <span>INFO</span>
                            </label>
                            <label class="log-viewer-level-checkbox">
                                <input type="checkbox" data-level="WARNING" checked>
                                <span>WARNING</span>
                            </label>
                            <label class="log-viewer-level-checkbox">
                                <input type="checkbox" data-level="SEVERE" checked>
                                <span>SEVERE</span>
                            </label>
                        </div>
                    </div>
                    
                    <div class="log-viewer-filter">
                        <label>Logger:</label>
                        <input type="text" data-filter="loggerRegex" placeholder="Regex...">
                    </div>
                    
                    <div class="log-viewer-filter">
                        <label>Source:</label>
                        <input type="text" data-filter="sourceClassRegex" placeholder="Regex...">
                    </div>
                    
                    <div class="log-viewer-filter">
                        <label>Message:</label>
                        <input type="text" data-filter="messageRegex" placeholder="Regex...">
                    </div>
                </div>
                
                <div class="log-viewer-content">
                    <div class="log-viewer-logs"></div>
                </div>
            </div>
        `;
        
        this.elements = {
            viewer: container.querySelector('.log-viewer'),
            header: container.querySelector('.log-viewer-header'),
            badge: container.querySelector('.log-viewer-badge'),
            status: container.querySelector('.log-viewer-status'),
            statusText: container.querySelector('.log-viewer-status-text'),
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
        
        // Apply initial filters from options
        this.applyInitialFilters();
    }
    
    /**
     * Apply initial filters from constructor options
     */
    applyInitialFilters() {
        const filters = this.options.filters;
        
        if (filters.node) {
            this.elements.filters.node.value = filters.node;
        }
        
        if (filters.loggerRegex) {
            this.elements.filters.loggerRegex.value = filters.loggerRegex;
        }
        
        if (filters.sourceClassRegex) {
            this.elements.filters.sourceClassRegex.value = filters.sourceClassRegex;
        }
        
        if (filters.messageRegex) {
            this.elements.filters.messageRegex.value = filters.messageRegex;
        }
        
        if (filters.level && Array.isArray(filters.level)) {
            // Uncheck all first
            Object.values(this.elements.filters.levels).forEach(cb => cb.checked = false);
            // Check only specified levels
            filters.level.forEach(level => {
                if (this.elements.filters.levels[level]) {
                    this.elements.filters.levels[level].checked = true;
                }
            });
        }
    }
    
    /**
     * Attach event listeners
     */
    attachEventListeners() {
        // Toggle collapse/expand
        this.elements.header.addEventListener('click', (e) => {
            // Don't toggle if clicking on buttons
            if (e.target.closest('.log-viewer-actions')) return;
            this.toggle();
        });
        
        // Action buttons
        this.elements.viewer.addEventListener('click', (e) => {
            const btn = e.target.closest('[data-action]');
            if (!btn) return;
            
            const action = btn.dataset.action;
            if (action === 'clear') {
                this.clear();
            } else if (action === 'export') {
                this.export();
            }
        });
        
        // Filter changes - debounced
        let filterTimeout;
        const filterInputs = [
            this.elements.filters.node,
            this.elements.filters.loggerRegex,
            this.elements.filters.sourceClassRegex,
            this.elements.filters.messageRegex,
            ...Object.values(this.elements.filters.levels)
        ];
        
        filterInputs.forEach(input => {
            input.addEventListener('change', () => {
                clearTimeout(filterTimeout);
                filterTimeout = setTimeout(() => {
                    this.reconnectWithFilters();
                }, 500);
            });
            
            if (input.type === 'text') {
                input.addEventListener('input', () => {
                    clearTimeout(filterTimeout);
                    filterTimeout = setTimeout(() => {
                        this.reconnectWithFilters();
                    }, 500);
                });
            }
        });
    }
    
    /**
     * Get current filter values from UI
     */
    getCurrentFilters() {
        const filters = {};
        
        const node = this.elements.filters.node.value;
        if (node) filters.node = node;
        
        const loggerRegex = this.elements.filters.loggerRegex.value.trim();
        if (loggerRegex) filters.loggerRegex = loggerRegex;
        
        const sourceClassRegex = this.elements.filters.sourceClassRegex.value.trim();
        if (sourceClassRegex) filters.sourceClassRegex = sourceClassRegex;
        
        const messageRegex = this.elements.filters.messageRegex.value.trim();
        if (messageRegex) filters.messageRegex = messageRegex;
        
        const checkedLevels = Object.entries(this.elements.filters.levels)
            .filter(([_, cb]) => cb.checked)
            .map(([level, _]) => level);
        
        if (checkedLevels.length > 0 && checkedLevels.length < 3) {
            filters.level = checkedLevels;
        }
        
        return filters;
    }
    
    /**
     * Reconnect with updated filters
     */
    reconnectWithFilters() {
        this.disconnect();
        this.connect();
    }
    
    /**
     * Connect to GraphQL WebSocket
     */
    connect() {
        this.updateStatus('connecting', 'Connecting...');
        
        try {
            // Use graphql-transport-ws subprotocol (newer GraphQL over WebSocket Protocol)
            // This is different from the older 'graphql-ws' subprotocol used by subscriptions-transport-ws
            this.ws = new WebSocket(this.options.wsUrl, 'graphql-transport-ws');
            
            this.ws.onopen = () => {
                console.log('Log Viewer: WebSocket connected');
                
                // Send connection_init (graphql-ws protocol)
                this.send({
                    type: 'connection_init',
                    payload: {}
                });
            };
            
            this.ws.onmessage = (event) => {
                try {
                    const message = JSON.parse(event.data);
                    this.handleMessage(message);
                } catch (e) {
                    console.error('Log Viewer: Error parsing message:', e);
                }
            };
            
            this.ws.onerror = (error) => {
                console.error('Log Viewer: WebSocket error:', error);
                this.updateStatus('disconnected', 'Error');
            };
            
            this.ws.onclose = () => {
                console.log('Log Viewer: WebSocket closed');
                this.isConnected = false;
                this.updateStatus('disconnected', 'Disconnected');
                this.attemptReconnect();
            };
            
        } catch (e) {
            console.error('Log Viewer: Error creating WebSocket:', e);
            this.updateStatus('disconnected', 'Connection Failed');
            this.attemptReconnect();
        }
    }
    
    /**
     * Handle WebSocket messages
     */
    handleMessage(message) {
        switch (message.type) {
            case 'connection_ack':
                console.log('Log Viewer: Connection acknowledged');
                this.isConnected = true;
                this.reconnectAttempts = 0;
                this.updateStatus('connected', 'Connected');
                this.subscribe();
                break;
                
            case 'next':
                // graphql-transport-ws uses 'next' for data messages
                if (message.payload?.data?.systemLogs) {
                    this.addLog(message.payload.data.systemLogs);
                }
                break;
                
            case 'error':
                console.error('Log Viewer: Subscription error:', message.payload);
                this.updateStatus('disconnected', 'Error');
                break;
                
            case 'complete':
                console.log('Log Viewer: Subscription completed');
                break;
                
            case 'ping':
                // Respond to ping with pong
                this.send({ type: 'pong' });
                break;
                
            case 'pong':
                // Pong received, connection is alive
                break;
                
            default:
                console.log('Log Viewer: Unknown message type:', message.type);
                break;
        }
    }
    
    /**
     * Subscribe to systemLogs
     */
    subscribe() {
        const uiFilters = this.getCurrentFilters();
        
        const query = `
            subscription SystemLogs(
                $node: String,
                $level: String,
                $logger: String,
                $thread: Long,
                $sourceClass: String,
                $sourceMethod: String,
                $message: String
            ) {
                systemLogs(
                    node: $node,
                    level: $level,
                    logger: $logger,
                    thread: $thread,
                    sourceClass: $sourceClass,
                    sourceMethod: $sourceMethod,
                    message: $message
                ) {
                    timestamp
                    level
                    logger
                    message
                    thread
                    node
                    sourceClass
                    sourceMethod
                    parameters
                    exception { class message stackTrace }
                }
            }
        `;
        
        const variables = {};
        if (uiFilters.node) variables.node = uiFilters.node;
        if (uiFilters.loggerRegex) variables.logger = uiFilters.loggerRegex;
        if (uiFilters.messageRegex) variables.message = uiFilters.messageRegex;
        if (uiFilters.sourceClassRegex) variables.sourceClass = uiFilters.sourceClassRegex;
        if (uiFilters.sourceMethodRegex) variables.sourceMethod = uiFilters.sourceMethodRegex;
        if (uiFilters.thread) variables.thread = uiFilters.thread; // reserved if added later
        if (uiFilters.level) {
            if (Array.isArray(uiFilters.level) && uiFilters.level.length === 1) {
                variables.level = uiFilters.level[0];
            } else if (!Array.isArray(uiFilters.level)) {
                variables.level = uiFilters.level;
            }
        }
        
        this.send({
            id: this.subscriptionId,
            type: 'subscribe',
            payload: {
                query,
                variables
            }
        });
        console.log('Log Viewer: Subscribed with variables:', variables);
    }
    
    /**
     * Send message to WebSocket
     */
    send(message) {
        if (this.ws && this.ws.readyState === WebSocket.OPEN) {
            this.ws.send(JSON.stringify(message));
        }
    }
    
    /**
     * Disconnect WebSocket
     */
    disconnect() {
        if (this.ws) {
            // Unsubscribe using 'complete' type for graphql-transport-ws protocol
            this.send({
                id: this.subscriptionId,
                type: 'complete'
            });
            
            this.ws.close();
            this.ws = null;
        }
        this.isConnected = false;
    }
    
    /**
     * Attempt to reconnect
     */
    attemptReconnect() {
        if (this.reconnectAttempts >= this.maxReconnectAttempts) {
            console.log('Log Viewer: Max reconnect attempts reached');
            this.updateStatus('disconnected', 'Connection Failed');
            return;
        }
        
        this.reconnectAttempts++;
        const delay = this.reconnectDelay * Math.min(this.reconnectAttempts, 5);
        
        console.log(`Log Viewer: Reconnecting in ${delay}ms (attempt ${this.reconnectAttempts})`);
        this.updateStatus('connecting', `Reconnecting... (${this.reconnectAttempts})`);
        
        setTimeout(() => {
            this.connect();
        }, delay);
    }
    
    /**
     * Add a log entry
     */
    addLog(logEntry) {
        this.logs.push(logEntry);
        
        // Trim logs if exceeding max
        if (this.logs.length > this.options.maxLines) {
            this.logs.shift();
        }
        
        // Create log element
        const logEl = this.createLogElement(logEntry);
        this.elements.logs.appendChild(logEl);
        
        // Remove old logs from DOM
        while (this.elements.logs.children.length > this.options.maxLines) {
            this.elements.logs.removeChild(this.elements.logs.firstChild);
        }
        
        // Update new logs badge if collapsed
        if (this.isCollapsed) {
            this.newLogsCount++;
            this.elements.badge.textContent = this.newLogsCount;
            this.elements.badge.style.display = 'inline-block';
        }
        
        // Auto-scroll
        if (this.options.autoScroll) {
            this.elements.content.scrollTop = this.elements.content.scrollHeight;
        }
        
        // Update node filter dropdown if needed
        this.updateNodeFilter(logEntry.node);
    }
    
    /**
     * Create log entry element
     */
    createLogElement(log) {
        const div = document.createElement('div');
        div.className = `log-entry level-${log.level}`;
        
        // Format timestamp (HH:MM:SS)
        const time = new Date(log.timestamp);
        const timeStr = time.toLocaleTimeString('en-US', { hour12: false });
        
        // Format logger (shorten if too long)
        const logger = log.logger.length > 40 
            ? '...' + log.logger.slice(-37) 
            : log.logger;
        
        let html = `
            <div class="log-entry-header">
                <span class="log-entry-time">${timeStr}</span>
                <span class="log-entry-level ${log.level}">[${log.level}]</span>
                <span class="log-entry-logger" title="${log.logger}">[${logger}]</span>
                <span class="log-entry-message">${this.escapeHtml(log.message)}</span>
            </div>
        `;
        
        // Add exception if present
        if (log.exception) {
            const exceptionId = 'exc-' + Date.now() + '-' + Math.random().toString(36).substr(2, 9);
            html += `
                <div class="log-entry-exception">
                    <div class="exception-header" data-exception-id="${exceptionId}">
                        <span class="chevron">▼</span>
                        ⚠️ ${this.escapeHtml(log.exception.class)}: ${this.escapeHtml(log.exception.message || 'No message')}
                    </div>
                    <pre class="exception-stacktrace" id="${exceptionId}">${this.escapeHtml(log.exception.stackTrace)}</pre>
                </div>
            `;
        }
        
        div.innerHTML = html;
        
        // Add exception toggle handler
        const exceptionHeader = div.querySelector('.exception-header');
        if (exceptionHeader) {
            exceptionHeader.addEventListener('click', (e) => {
                e.stopPropagation();
                const id = exceptionHeader.dataset.exceptionId;
                const stackTrace = document.getElementById(id);
                const chevron = exceptionHeader.querySelector('.chevron');
                
                stackTrace.classList.toggle('collapsed');
                exceptionHeader.classList.toggle('collapsed');
            });
        }
        
        return div;
    }
    
    /**
     * Update node filter dropdown
     */
    updateNodeFilter(node) {
        if (!node) return;
        
        const select = this.elements.filters.node;
        const exists = Array.from(select.options).some(opt => opt.value === node);
        
        if (!exists) {
            const option = document.createElement('option');
            option.value = node;
            option.textContent = node;
            select.appendChild(option);
        }
    }
    
    /**
     * Update connection status
     */
    updateStatus(status, text) {
        this.elements.status.className = `log-viewer-status ${status}`;
        this.elements.statusText.textContent = text;
    }
    
    /**
     * Toggle collapse/expand
     */
    toggle() {
        if (this.isCollapsed) {
            this.show();
        } else {
            this.hide();
        }
    }
    
    /**
     * Show log viewer
     */
    show() {
        this.isCollapsed = false;
        this.elements.viewer.classList.remove('collapsed');
        this.elements.viewer.classList.add('expanded');
        this.newLogsCount = 0;
        this.elements.badge.style.display = 'none';
        
        // Scroll to bottom
        if (this.options.autoScroll) {
            this.elements.content.scrollTop = this.elements.content.scrollHeight;
        }
    }
    
    /**
     * Hide log viewer
     */
    hide() {
        this.isCollapsed = true;
        this.elements.viewer.classList.remove('expanded');
        this.elements.viewer.classList.add('collapsed');
    }
    
    /**
     * Clear all logs
     */
    clear() {
        this.logs = [];
        this.elements.logs.innerHTML = '';
        this.newLogsCount = 0;
        this.elements.badge.style.display = 'none';
    }
    
    /**
     * Export logs
     */
    export() {
        const data = JSON.stringify(this.logs, null, 2);
        const blob = new Blob([data], { type: 'application/json' });
        const url = URL.createObjectURL(blob);
        
        const a = document.createElement('a');
        a.href = url;
        a.download = `system-logs-${new Date().toISOString()}.json`;
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        URL.revokeObjectURL(url);
    }
    
    /**
     * Set filters programmatically
     */
    setFilters(filters) {
        if (filters.node !== undefined) {
            this.elements.filters.node.value = filters.node || '';
        }
        if (filters.loggerRegex !== undefined) {
            this.elements.filters.loggerRegex.value = filters.loggerRegex || '';
        }
        if (filters.sourceClassRegex !== undefined) {
            this.elements.filters.sourceClassRegex.value = filters.sourceClassRegex || '';
        }
        if (filters.messageRegex !== undefined) {
            this.elements.filters.messageRegex.value = filters.messageRegex || '';
        }
        if (filters.level && Array.isArray(filters.level)) {
            Object.values(this.elements.filters.levels).forEach(cb => cb.checked = false);
            filters.level.forEach(level => {
                if (this.elements.filters.levels[level]) {
                    this.elements.filters.levels[level].checked = true;
                }
            });
        }
        
        this.reconnectWithFilters();
    }
    
    /**
     * Destroy the log viewer
     */
    destroy() {
        this.disconnect();
        const container = document.getElementById(this.containerId);
        if (container) {
            container.innerHTML = '';
        }
    }
    
    /**
     * Escape HTML to prevent XSS
     */
    escapeHtml(text) {
        if (!text) return '';
        const div = document.createElement('div');
        div.textContent = text;
        return div.innerHTML;
    }
}

// Export for use in other scripts
if (typeof module !== 'undefined' && module.exports) {
    module.exports = LogViewer;
}
