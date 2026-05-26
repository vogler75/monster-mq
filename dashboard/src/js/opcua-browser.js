// MonsterMQ Dashboard — OPC UA Browser Orchestrator
//
// Manages interactive, lazy-loaded tree navigation of OPC UA Server Address Spaces
// and maps selected node/wildcard paths to device subscriptions.

class OPCUABrowserManager {
    constructor() {
        this.selectedServerId = null;
        this.selectedNode = null;
        this.client = window.graphqlClient || null;
        this.init();
    }

    async init() {
        if (!this.client) {
            console.error('GraphQL Dashboard Client not found!');
            this.showError('GraphQL Client not initialized.');
            return;
        }

        this.setupEventListeners();
        await this.loadServers();
    }

    setupEventListeners() {
        const serverSelect = document.getElementById('server-select');
        const browseRootBtn = document.getElementById('browse-root-btn');
        const startNodeInput = document.getElementById('start-node-input');

        serverSelect.addEventListener('change', (e) => {
            this.selectedServerId = e.target.value;
            this.selectedNode = null;
            this.clearTree();
            this.showEmptyDetails();
            if (this.selectedServerId) {
                this.browseNode(this.selectedServerId, startNodeInput.value.trim());
            }
        });

        browseRootBtn.addEventListener('click', () => {
            if (!this.selectedServerId) {
                alert('Please select an OPC UA Client device first.');
                return;
            }
            this.selectedNode = null;
            this.clearTree();
            this.showEmptyDetails();
            this.browseNode(this.selectedServerId, startNodeInput.value.trim());
        });

        startNodeInput.addEventListener('keypress', (e) => {
            if (e.key === 'Enter') {
                browseRootBtn.click();
            }
        });
    }

    async loadServers() {
        const serverSelect = document.getElementById('server-select');
        try {
            const query = `
                query GetOpcuaServers {
                    opcuaServers {
                        id
                        name
                        endpoint
                        enabled
                        connected
                    }
                }
            `;
            const result = await this.client.query(query);
            const servers = result.opcuaServers || [];

            serverSelect.innerHTML = '<option value="">-- Select OPC UA Client --</option>';

            if (servers.length === 0) {
                serverSelect.innerHTML = '<option value="">No OPC UA Client Devices configured</option>';
                return;
            }

            servers.forEach(s => {
                const status = s.connected ? 'Connected' : (s.enabled ? 'Disconnected' : 'Disabled');
                const opt = document.createElement('option');
                opt.value = s.id;
                
                // Extract host:port to keep select dropdown clean and compact
                let hostPort = s.endpoint || '';
                try {
                    hostPort = hostPort.replace(/^opc\.tcp:\/\//i, '');
                    const slashIdx = hostPort.indexOf('/');
                    if (slashIdx !== -1) {
                        hostPort = hostPort.substring(0, slashIdx);
                    }
                } catch (e) {}

                opt.textContent = `${s.name} [${status}] (${hostPort})`;
                if (!s.enabled) opt.disabled = true;
                serverSelect.appendChild(opt);
            });

        } catch (e) {
            console.error('Failed to load OPC UA servers:', e);
            this.showError('Failed to fetch configured OPC UA Client devices from the broker.');
        }
    }

    clearTree() {
        const tree = document.getElementById('opcua-tree');
        tree.innerHTML = '';
    }

    showEmptyDetails() {
        const viewer = document.getElementById('detail-viewer');
        viewer.className = 'detail-viewer empty';
        viewer.innerHTML = `
            <div>
                <p>Select an OPC UA node from the tree to view its detailed properties</p>
                <p style="margin-top: 0.5rem; font-size: 0.875rem;">Expand folders to browse deeper levels</p>
            </div>
        `;
    }

    showError(msg) {
        const tree = document.getElementById('opcua-tree');
        tree.innerHTML = `<div class="error-box">${msg}</div>`;
    }

    async browseNode(serverId, nodeId, parentLi = null) {
        let container = document.getElementById('opcua-tree');
        if (parentLi) {
            let childUl = parentLi.querySelector('ul.tree-children');
            if (!childUl) {
                childUl = document.createElement('ul');
                childUl.className = 'tree-children';
                parentLi.appendChild(childUl);
            }
            container = childUl;
            container.classList.remove('collapsed');
        }

        // Show loading
        const loader = document.createElement('div');
        loader.className = 'loading';
        loader.innerHTML = '<ix-spinner size="small"></ix-spinner><span>Browsing nodes...</span>';
        container.appendChild(loader);

        try {
            const query = `
                query BrowseOpcuaNode($serverId: String!, $nodeId: String!) {
                    opcuaNodeBrowse(serverId: $serverId, nodeId: $nodeId) {
                        nodeId
                        browseName
                        displayName
                        nodeClass
                        description
                        dataType
                        value
                        hasChildren
                        writable
                        timestamp
                    }
                }
            `;
            const vars = { serverId, nodeId };
            const result = await this.client.query(query, vars);
            const nodes = result.opcuaNodeBrowse || [];

            loader.remove();

            if (nodes.length === 0) {
                if (parentLi) {
                    const toggle = parentLi.querySelector('.tree-toggle');
                    if (toggle) toggle.style.visibility = 'hidden';
                    container.remove();
                } else {
                    container.innerHTML = '<div class="loading">No nodes found under this starting path.</div>';
                }
                return;
            }

            nodes.forEach(node => {
                const li = document.createElement('li');
                li.className = 'tree-node';
                li.dataset.nodeId = node.nodeId;
                li.dataset.browseName = node.browseName;
                li.dataset.displayName = node.displayName;
                li.dataset.nodeClass = node.nodeClass;
                li.dataset.hasChildren = node.hasChildren;

                const item = document.createElement('div');
                item.className = 'tree-item';

                // Toggle button for folders
                const toggle = document.createElement('button');
                toggle.className = 'tree-toggle';
                if (node.hasChildren) {
                    toggle.innerHTML = '▶';
                    toggle.addEventListener('click', (e) => {
                        e.stopPropagation();
                        this.toggleFolder(li, toggle);
                    });
                }
                item.appendChild(toggle);

                // Icon based on class
                const icon = document.createElement('ix-icon');
                if (node.nodeClass === 'Object') {
                    icon.className = 'tree-icon folder';
                    icon.setAttribute('name', 'folder');
                } else {
                    icon.className = 'tree-icon variable';
                    icon.setAttribute('name', 'tag');
                }
                item.appendChild(icon);

                // Name label
                const label = document.createElement('span');
                label.textContent = node.displayName || node.browseName;
                item.appendChild(label);

                // Select node on click
                item.addEventListener('click', (e) => {
                    document.querySelectorAll('.tree-item').forEach(el => el.classList.remove('selected'));
                    item.classList.add('selected');
                    this.showNodeDetails(node, li);
                });

                li.appendChild(item);
                container.appendChild(li);
            });

            if (parentLi) {
                const toggle = parentLi.querySelector('.tree-toggle');
                if (toggle) toggle.classList.add('expanded');
            }

        } catch (e) {
            loader.remove();
            console.error('Failed to browse OPC UA node:', e);
            const errDiv = document.createElement('div');
            errDiv.className = 'error-box';
            errDiv.textContent = e.message || 'Error loading children nodes. The OPC UA server may have disconnected.';
            container.appendChild(errDiv);
        }
    }

    toggleFolder(li, toggle) {
        const childUl = li.querySelector('ul.tree-children');
        if (childUl) {
            if (childUl.classList.contains('collapsed')) {
                childUl.classList.remove('collapsed');
                toggle.classList.add('expanded');
            } else {
                childUl.classList.add('collapsed');
                toggle.classList.remove('expanded');
            }
        } else {
            const nodeId = li.dataset.nodeId;
            this.browseNode(this.selectedServerId, nodeId, li);
        }
    }

    getBrowsePathForNode(li) {
        const pathParts = [];
        let current = li;
        while (current && current.dataset.browseName) {
            // Escape slashes in browse names
            const escapedName = current.dataset.browseName.replace(/\//g, '\\/');
            pathParts.unshift(escapedName);
            
            // Go up to the next li
            const parentUl = current.closest('ul.tree-children');
            current = parentUl ? parentUl.closest('li[data-node-id]') : null;
        }
        return pathParts.join('/');
    }

    async showNodeDetails(nodeSummary, li) {
        const viewer = document.getElementById('detail-viewer');
        viewer.className = 'detail-viewer';
        viewer.innerHTML = '<div class="loading"><ix-spinner></ix-spinner><span>Reading full properties...</span></div>';

        const serverId = this.selectedServerId;
        const nodeId = nodeSummary.nodeId;
        const browsePath = this.getBrowsePathForNode(li);

        try {
            const query = `
                query ReadOpcuaNode($serverId: String!, $nodeId: String!) {
                    opcuaNodeRead(serverId: $serverId, nodeId: $nodeId) {
                        nodeId
                        browseName
                        displayName
                        nodeClass
                        description
                        dataType
                        value
                        hasChildren
                        writable
                        timestamp
                    }
                }
            `;
            const result = await this.client.query(query, { serverId, nodeId });
            const node = result.opcuaNodeRead;

            if (!node) {
                viewer.innerHTML = '<div class="error-box">Failed to read node properties.</div>';
                return;
            }

            // Create default topic from browse path
            const safePath = browsePath
                .toLowerCase()
                .replace(/[^a-z0-9\/_-]/g, '')
                .replace(/\/+/g, '/');
            const defaultTopic = `path/${safePath}`;

            viewer.innerHTML = `
                <div class="data-header">
                    <div class="data-title">${node.displayName || node.browseName}</div>
                    <div class="data-subtitle">${node.nodeId}</div>
                </div>

                <div class="detail-grid">
                    <div class="detail-label">Browse Name:</div>
                    <div class="detail-value">${node.browseName}</div>

                    <div class="detail-label">Node Class:</div>
                    <div class="detail-value">${node.nodeClass}</div>

                    <div class="detail-label">Relative Path:</div>
                    <div class="detail-value">${browsePath}</div>

                    ${node.nodeClass === 'Variable' ? `
                        <div class="detail-label">Data Type:</div>
                        <div class="detail-value">${node.dataType}</div>

                        <div class="detail-label">Current Value:</div>
                        <div class="detail-value" style="font-family: monospace; font-weight: bold; color: var(--monster-teal);">${node.value || 'null'}</div>

                        <div class="detail-label">Write Access:</div>
                        <div class="detail-value">${node.writable ? 'Writable' : 'Read-Only'}</div>

                        <div class="detail-label">Last Updated:</div>
                        <div class="detail-value">${node.timestamp ? new Date(node.timestamp).toLocaleString() : '-'}</div>
                    ` : ''}

                    <div class="detail-label">Description:</div>
                    <div class="detail-value">${node.description || 'No description available'}</div>
                </div>

                <div class="subscription-card">
                    <h3 class="sub-title"><ix-icon name="link"></ix-icon> Subscribe in Client</h3>
                    
                    <div id="sub-alert-container"></div>

                    <div class="form-group">
                        <span class="form-label">Subscription Address Type</span>
                        <div class="form-radio-group">
                            ${node.nodeClass === 'Variable' ? `
                                <label class="form-radio-label">
                                    <input type="radio" name="sub-type" value="node" checked>
                                    NodeId Sub: NodeId://${node.nodeId}
                                </label>
                                <label class="form-radio-label">
                                    <input type="radio" name="sub-type" value="path">
                                    BrowsePath Sub: BrowsePath://${browsePath}
                                </label>
                            ` : `
                                <label class="form-radio-label">
                                    <input type="radio" name="sub-type" value="path-wildcard-recursive" checked>
                                    Subtree Sub (Recursive Wildcard): BrowsePath://${browsePath}/#
                                </label>
                                <label class="form-radio-label">
                                    <input type="radio" name="sub-type" value="path-wildcard-level">
                                    Single-Level Wildcard: BrowsePath://${browsePath}/+
                                </label>
                                <label class="form-radio-label">
                                    <input type="radio" name="sub-type" value="path-exact">
                                    Exact Folder Sub: BrowsePath://${browsePath}
                                </label>
                            `}
                        </div>
                    </div>

                    <div class="form-group">
                        <label class="form-label" for="sub-topic">MQTT Topic Name</label>
                        <input type="text" id="sub-topic" class="search-input" value="${defaultTopic}">
                        <span style="font-size: 0.75rem; color: var(--text-muted); margin-top: 4px; display: block;">
                            Emitted topic: {deviceNamespace}/${node.nodeClass === 'Variable' ? 'sub-topic' : 'sub-topic/[wildcard_tail]'}
                        </span>
                    </div>

                    <div class="form-group">
                        <label class="form-label">Publish Mode</label>
                        <div class="form-radio-group" style="flex-direction: row; gap: 2rem;">
                            <label class="form-radio-label">
                                <input type="radio" name="sub-mode" value="SEPARATE" checked> SEPARATE (Default)
                            </label>
                            <label class="form-radio-label">
                                <input type="radio" name="sub-mode" value="SINGLE"> SINGLE
                            </label>
                        </div>
                    </div>

                    <div class="form-group" id="remove-path-container" style="display: ${node.nodeClass === 'Variable' ? 'none' : 'block'};">
                        <label class="form-checkbox-label">
                            <input type="checkbox" id="sub-remove-path" checked> Remove BrowsePath Prefix from wildcards
                        </label>
                    </div>

                    <div style="margin-top: 1.5rem; display: flex; justify-content: flex-end;">
                        <button id="btn-subscribe" class="search-button" style="background: var(--monster-green);">
                            Add Subscription
                        </button>
                    </div>
                </div>
            `;

            // Setup radio toggles
            const typeRadios = document.getElementsByName('sub-type');
            const removePathContainer = document.getElementById('remove-path-container');
            typeRadios.forEach(radio => {
                radio.addEventListener('change', (e) => {
                    const val = e.target.value;
                    const isWildcard = val.includes('wildcard');
                    removePathContainer.style.display = isWildcard ? 'block' : 'none';
                    
                    const topicInput = document.getElementById('sub-topic');
                    if (isWildcard && val === 'path-wildcard-recursive') {
                        topicInput.value = `path/${safePath}/#`;
                    } else if (isWildcard && val === 'path-wildcard-level') {
                        topicInput.value = `path/${safePath}/+`;
                    } else {
                        topicInput.value = `path/${safePath}`;
                    }
                });
            });

            // Submit subscription button
            document.getElementById('btn-subscribe').addEventListener('click', () => {
                this.addSubscription(node, browsePath, safePath);
            });

        } catch (e) {
            console.error('Failed to read node attributes:', e);
            viewer.innerHTML = `<div class="error-box">Failed to read node details: ${e.message || 'Server timeout'}</div>`;
        }
    }

    async addSubscription(node, browsePath, safePath) {
        const subAlert = document.getElementById('sub-alert-container');
        subAlert.innerHTML = '';

        const typeVal = document.querySelector('input[name="sub-type"]:checked').value;
        const topicVal = document.getElementById('sub-topic').value.trim();
        const modeVal = document.querySelector('input[name="sub-mode"]:checked').value;
        const removePathVal = document.getElementById('sub-remove-path')?.checked || false;

        if (!topicVal) {
            subAlert.innerHTML = '<div class="error-box" style="margin: 0 0 1rem 0;">MQTT Topic is required.</div>';
            return;
        }

        // Map selected address format
        let address = '';
        if (typeVal === 'node') {
            address = `NodeId://${node.nodeId}`;
        } else if (typeVal === 'path') {
            address = `BrowsePath://${browsePath}`;
        } else if (typeVal === 'path-exact') {
            address = `BrowsePath://${browsePath}`;
        } else if (typeVal === 'path-wildcard-recursive') {
            address = `BrowsePath://${browsePath}/#`;
        } else if (typeVal === 'path-wildcard-level') {
            address = `BrowsePath://${browsePath}/+`;
        }

        const subscribeBtn = document.getElementById('btn-subscribe');
        subscribeBtn.disabled = true;
        subscribeBtn.textContent = 'Subscribing...';

        try {
            const mutation = `
                mutation AddOpcUaAddress($deviceName: String!, $input: OpcUaAddressInput!) {
                    opcUaDevice {
                        addAddress(deviceName: $deviceName, input: $input) {
                            success
                            errors
                        }
                    }
                }
            `;
            const vars = {
                deviceName: this.selectedServerId,
                input: {
                    address: address,
                    topic: topicVal,
                    publishMode: modeVal,
                    removePath: removePathVal
                }
            };

            const result = await this.client.query(mutation, vars);
            const status = result.opcUaDevice?.addAddress;

            subscribeBtn.disabled = false;
            subscribeBtn.textContent = 'Add Subscription';

            if (status && status.success) {
                subAlert.innerHTML = `
                    <div class="success-box">
                        Successfully added subscription!<br>
                        <strong>Address:</strong> ${address}<br>
                        <strong>Topic:</strong> ${topicVal}
                    </div>
                `;
            } else {
                const errors = status?.errors || ['Failed to add subscription.'];
                subAlert.innerHTML = `<div class="error-box" style="margin: 0 0 1rem 0;">${errors.join('<br>')}</div>`;
            }

        } catch (e) {
            subscribeBtn.disabled = false;
            subscribeBtn.textContent = 'Add Subscription';
            console.error('Failed to execute addAddress:', e);
            subAlert.innerHTML = `<div class="error-box" style="margin: 0 0 1rem 0;">Error: ${e.message || 'GraphQL mutation failure'}</div>`;
        }
    }
}

// Instantiate the manager for this page session
var opcuaBrowserManager = new OPCUABrowserManager();
window.registerPageCleanup(() => {
    opcuaBrowserManager = null;
});
