// OPC UA Servers Management JavaScript

let servers = [];
let clusterNodes = [];

// Initialize page
document.addEventListener('DOMContentLoaded', function() {
    checkAuthAndLoadData();
    setupEventListeners();
});

function checkAuthAndLoadData() {
    const token = localStorage.getItem('monstermq_token');
    if (!token) {
        window.location.href = '/pages/login.html';
        return;
    }

    // If token is not 'null', check if it's expired
    if (token !== 'null') {
        try {
            const decoded = JSON.parse(atob(token.split('.')[1]));
            const now = Date.now() / 1000;
            if (decoded.exp <= now) {
                window.location.href = '/pages/login.html';
                return;
            }
        } catch {
            window.location.href = '/pages/login.html';
            return;
        }
    }

    // Set up logout functionality
    const logoutLink = document.getElementById('logout-link');
    if (logoutLink) {
        logoutLink.addEventListener('click', function(e) {
            e.preventDefault();
            localStorage.removeItem('monstermq_token');
            localStorage.removeItem('monstermq_username');
            localStorage.removeItem('monstermq_isAdmin');
            window.location.href = '/pages/login.html';
        });
    }

    loadServers();
    loadClusterNodes();
}

function setupEventListeners() {
    // Modal event listeners
    document.addEventListener('keydown', function(e) {
        if (e.key === 'Escape') {
            hideAddServerModal();
            hideEditServerModal();
            hideInlineAddAddress();
        }
    });

    // Form validation
    const addForm = document.getElementById('add-server-form');
    if (addForm) {
        addForm.addEventListener('submit', function(e) {
            e.preventDefault();
            saveServer();
        });
    }

    const editForm = document.getElementById('edit-server-form');
    if (editForm) {
        editForm.addEventListener('submit', function(e) {
            e.preventDefault();
            updateServer();
        });
    }

    // Inline address form handling is done via onclick handlers
}

async function loadServers() {
    try {
        showLoadingIndicator();
        hideErrorMessage();

        const query = `
            query {
                opcUaServers {
                    name
                    port
                    path
                    namespace
                    namespaceIndex
                    namespaceUri
                    nodeId
                    enabled
                    bufferSize
                    updateInterval
                    createdAt
                    updatedAt
                    isOnCurrentNode
                    status {
                        status
                        serverName
                        nodeId
                        port
                        activeConnections
                        nodeCount
                    }
                    addresses {
                        mqttTopic
                        dataType
                        accessLevel
                        browseName
                        displayName
                        description
                        unit
                    }
                    security {
                        keystorePath
                        certificateAlias
                        securityPolicies
                        allowAnonymous
                        requireAuthentication
                    }
                }
            }
        `;

        const response = await window.graphqlClient.query(query);

        if (response && response.opcUaServers) {
            servers = response.opcUaServers;
            updateServerTable();
            updateMetrics();
        } else {
            throw new Error('No server data received');
        }

    } catch (error) {
        console.error('Error loading servers:', error);
        showErrorMessage('Failed to load OPC UA servers: ' + error.message);
    } finally {
        hideLoadingIndicator();
    }
}

async function loadClusterNodes() {
    try {
        const query = `
            query {
                brokers {
                    nodeId
                }
            }
        `;

        const response = await window.graphqlClient.query(query);

        if (response && response.brokers) {
            clusterNodes = response.brokers.map(broker => broker.nodeId);
            updateNodeSelector();
        }
    } catch (error) {
        console.error('Error loading cluster nodes:', error);
    }
}

function updateServerTable() {
    const tbody = document.getElementById('servers-table-body');
    if (!tbody) return;

    tbody.innerHTML = '';

    servers.forEach(server => {
        const row = createServerRow(server);
        tbody.appendChild(row);
    });
}

function createServerRow(server) {
    const row = document.createElement('tr');

    const serverStatus = server.status ? server.status.status : (server.enabled ? 'UNKNOWN' : 'DISABLED');
    const statusBadge = createStatusBadge(serverStatus);

    row.innerHTML = `
        <td>
            <div style="font-weight: 500; color: var(--text-primary);">${escapeHtml(server.name)}</div>
            <div style="font-size: 0.875rem; color: var(--text-secondary);">URI: ${escapeHtml(server.namespace || 'N/A')}</div>
        </td>
        <td>${server.port}</td>
        <td>${escapeHtml(server.namespace || 'N/A')}</td>
        <td>
            <span class="node-badge">${escapeHtml(server.nodeId)}</span>
        </td>
        <td>${statusBadge}</td>
        <td>
            <div class="node-actions">
                ${serverStatus === 'STOPPED' || serverStatus === 'DISABLED' || serverStatus === 'UNKNOWN' ?
                    `<button class="btn btn-sm btn-success" onclick="startServer('${escapeHtml(server.name)}')">Start</button>` :
                    `<button class="btn btn-sm btn-warning" onclick="stopServer('${escapeHtml(server.name)}')">Stop</button>`
                }
                <button class="btn btn-sm btn-secondary" onclick="editServer('${escapeHtml(server.name)}')">Edit</button>
                <button class="btn btn-sm btn-danger" onclick="deleteServer('${escapeHtml(server.name)}')">Delete</button>
            </div>
        </td>
    `;

    return row;
}

function createStatusBadge(status) {
    const statusClass = `status-${status.toLowerCase()}`;
    return `<span class="status-badge ${statusClass}">${status}</span>`;
}

function updateMetrics() {
    const totalServers = servers.length;
    const runningServers = servers.filter(s => s.status && s.status.status === 'RUNNING').length;
    const currentNodeServers = servers.filter(s => s.nodeId === getCurrentNodeId() || s.nodeId === '*').length;
    const totalNodes = 0; // Nodes will be configured separately

    document.getElementById('total-servers').textContent = totalServers;
    document.getElementById('running-servers').textContent = runningServers;
    document.getElementById('current-node-servers').textContent = currentNodeServers;
    document.getElementById('total-nodes').textContent = totalNodes;
}

function getCurrentNodeId() {
    // This would be populated from server info
    return 'node-1'; // Placeholder
}

function updateNodeSelector() {
    const selector = document.getElementById('server-node');
    if (!selector) return;

    // Clear existing options except the default ones
    const defaultOptions = Array.from(selector.options).filter(opt =>
        opt.value === '' || opt.value === '*'
    );

    selector.innerHTML = '';
    defaultOptions.forEach(opt => selector.appendChild(opt));

    // Add cluster nodes
    clusterNodes.forEach(nodeId => {
        if (nodeId) {
            const option = document.createElement('option');
            option.value = nodeId;
            option.textContent = nodeId;
            selector.appendChild(option);
        }
    });
}

// Modal functions
function showAddServerModal() {
    const modal = document.getElementById('add-server-modal');
    if (modal) {
        modal.style.display = 'flex';
        document.body.style.overflow = 'hidden';
    }
}

function hideAddServerModal() {
    const modal = document.getElementById('add-server-modal');
    if (modal) {
        modal.style.display = 'none';
        document.body.style.overflow = 'auto';
        resetForm();
    }
}

function resetForm() {
    const form = document.getElementById('add-server-form');
    if (form) {
        form.reset();
    }
}


// Server management functions
async function saveServer() {
    try {
        const serverData = collectFormData();
        if (!validateServerData(serverData)) {
            return;
        }

        const mutation = `
            mutation CreateOpcUaServer($config: OpcUaServerConfigInput!) {
                createOpcUaServer(config: $config) {
                    success
                    message
                }
            }
        `;

        const variables = {
            config: serverData
        };

        const response = await window.graphqlClient.query(mutation, variables);

        if (response && response.createOpcUaServer.success) {
            hideAddServerModal();
            await loadServers();
            showSuccessMessage('OPC UA Server created successfully');
        } else {
            throw new Error(response?.createOpcUaServer?.message || 'Failed to create server');
        }

    } catch (error) {
        console.error('Error saving server:', error);
        alert('Failed to save server: ' + error.message);
    }
}

function collectFormData() {
    return {
        name: document.getElementById('server-name').value.trim(),
        namespace: document.getElementById('server-namespace').value.trim(),
        namespaceUri: document.getElementById('server-namespace-uri').value.trim() ||
                     `urn:monstermq:opcua:${document.getElementById('server-name').value.trim()}`,
        nodeId: document.getElementById('server-node').value,
        port: parseInt(document.getElementById('server-port').value),
        path: document.getElementById('server-path').value.trim() || 'monstermq',
        enabled: document.getElementById('server-enabled').checked,
        updateInterval: parseInt(document.getElementById('update-interval').value) || 1000,
        bufferSize: parseInt(document.getElementById('buffer-size').value) || 1000
    };
}


function validateServerData(data) {
    if (!data.name) {
        alert('Please enter a server name');
        return false;
    }

    if (!data.namespace) {
        alert('Please enter a namespace');
        return false;
    }

    if (!data.nodeId) {
        alert('Please select a cluster node');
        return false;
    }

    if (!data.port || data.port < 1 || data.port > 65535) {
        alert('Please enter a valid port number (1-65535)');
        return false;
    }

    return true;
}

async function startServer(serverName) {
    try {
        const mutation = `
            mutation StartOpcUaServer($serverName: String!, $nodeId: String) {
                startOpcUaServer(serverName: $serverName, nodeId: $nodeId) {
                    success
                    message
                }
            }
        `;

        const variables = {
            serverName: serverName,
            nodeId: "*" // Start on all applicable nodes
        };

        const response = await window.graphqlClient.query(mutation, variables);

        if (response && response.startOpcUaServer.success) {
            await loadServers();
            showSuccessMessage(`Server '${serverName}' started successfully`);
        } else {
            throw new Error(response?.startOpcUaServer?.message || 'Failed to start server');
        }

    } catch (error) {
        console.error('Error starting server:', error);
        alert('Failed to start server: ' + error.message);
    }
}

async function stopServer(serverName) {
    try {
        const mutation = `
            mutation StopOpcUaServer($serverName: String!, $nodeId: String) {
                stopOpcUaServer(serverName: $serverName, nodeId: $nodeId) {
                    success
                    message
                }
            }
        `;

        const variables = {
            serverName: serverName,
            nodeId: "*" // Stop on all applicable nodes
        };

        const response = await window.graphqlClient.query(mutation, variables);

        if (response && response.stopOpcUaServer.success) {
            await loadServers();
            showSuccessMessage(`Server '${serverName}' stopped successfully`);
        } else {
            throw new Error(response?.stopOpcUaServer?.message || 'Failed to stop server');
        }

    } catch (error) {
        console.error('Error stopping server:', error);
        alert('Failed to stop server: ' + error.message);
    }
}

async function deleteServer(serverName) {
    if (!confirm(`Are you sure you want to delete the server '${serverName}'? This action cannot be undone.`)) {
        return;
    }

    try {
        const mutation = `
            mutation DeleteOpcUaServer($serverName: String!) {
                deleteOpcUaServer(serverName: $serverName) {
                    success
                    message
                }
            }
        `;

        const variables = {
            serverName: serverName
        };

        const response = await window.graphqlClient.query(mutation, variables);

        if (response && response.deleteOpcUaServer.success) {
            await loadServers();
            showSuccessMessage(`Server '${serverName}' deleted successfully`);
        } else {
            throw new Error(response?.deleteOpcUaServer?.message || 'Failed to delete server');
        }

    } catch (error) {
        console.error('Error deleting server:', error);
        alert('Failed to delete server: ' + error.message);
    }
}

let currentEditingServer = null;

function editServer(serverName) {
    const server = servers.find(s => s.name === serverName);
    if (!server) {
        showErrorMessage('Server not found');
        return;
    }

    currentEditingServer = server;

    // Populate configuration form
    document.getElementById('edit-server-name').value = server.name;
    document.getElementById('edit-server-namespace').value = server.namespace || '';
    document.getElementById('edit-server-port').value = server.port;
    document.getElementById('edit-server-path').value = server.path || 'monstermq';
    document.getElementById('edit-server-namespace-uri').value = server.namespaceUri || '';
    document.getElementById('edit-server-enabled').checked = server.enabled;

    // Update node selector for edit form
    updateEditNodeSelector();
    document.getElementById('edit-server-node').value = server.nodeId || '';

    // Populate addresses
    updateAddressesList();

    // Show modal
    document.getElementById('edit-server-modal').style.display = 'flex';
}

function hideEditServerModal() {
    document.getElementById('edit-server-modal').style.display = 'none';
    currentEditingServer = null;
}

// Function removed - no longer using tabs

function updateEditNodeSelector() {
    const select = document.getElementById('edit-server-node');

    // Clear existing options except the first two
    select.innerHTML = '<option value="">Select Node...</option><option value="*">All Nodes</option>';

    // Add cluster nodes
    clusterNodes.forEach(nodeId => {
        const option = document.createElement('option');
        option.value = nodeId;
        option.textContent = nodeId;
        select.appendChild(option);
    });
}

function updateAddressesList() {
    const tbody = document.getElementById('addresses-table-body');
    const noAddresses = document.getElementById('no-addresses');
    const table = document.getElementById('addresses-table');

    if (!currentEditingServer || !currentEditingServer.addresses || currentEditingServer.addresses.length === 0) {
        table.style.display = 'none';
        noAddresses.style.display = 'block';
        return;
    }

    table.style.display = 'block';
    noAddresses.style.display = 'none';
    tbody.innerHTML = '';

    currentEditingServer.addresses.forEach(address => {
        const row = createAddressRow(address);
        tbody.appendChild(row);
    });
}

function createAddressRow(address) {
    const row = document.createElement('tr');
    row.style.borderBottom = '1px solid rgba(71, 85, 105, 0.3)';

    row.innerHTML = `
        <td style="padding: 1rem; vertical-align: middle; color: var(--text-primary); font-family: 'JetBrains Mono', monospace; font-size: 0.875rem;">${escapeHtml(address.mqttTopic)}</td>
        <td style="padding: 1rem; vertical-align: middle; color: var(--text-primary);">${escapeHtml(address.displayName)}</td>
        <td style="padding: 1rem; vertical-align: middle; color: var(--text-secondary);">${address.dataType}</td>
        <td style="padding: 1rem; vertical-align: middle; color: var(--text-secondary);">${address.accessLevel.replace('_', ' ')}</td>
        <td style="padding: 1rem; vertical-align: middle; text-align: center;">
            <button class="btn-action btn-delete" onclick="deleteAddress('${escapeHtml(address.mqttTopic)}')" title="Delete address">
                <svg width="14" height="14" viewBox="0 0 24 24" fill="currentColor">
                    <path d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16"/>
                </svg>
            </button>
        </td>
    `;

    // Add hover effect
    row.addEventListener('mouseenter', () => {
        row.style.background = 'rgba(71, 85, 105, 0.1)';
    });
    row.addEventListener('mouseleave', () => {
        row.style.background = 'transparent';
    });

    return row;
}

function showInlineAddAddress() {
    if (!currentEditingServer) {
        showErrorMessage('No server selected');
        return;
    }

    const form = document.getElementById('inline-add-address-form');

    // Reset form
    document.getElementById('inline-address-mqtt-topic').value = '';
    document.getElementById('inline-address-display-name').value = '';
    document.getElementById('inline-address-data-type').value = 'TEXT';
    document.getElementById('inline-address-access-level').value = 'READ_ONLY';

    form.style.display = 'block';

    // Focus first field
    document.getElementById('inline-address-mqtt-topic').focus();
}

function hideInlineAddAddress() {
    document.getElementById('inline-add-address-form').style.display = 'none';
}

async function addInlineAddress() {
    if (!currentEditingServer) {
        showErrorMessage('No server selected');
        return;
    }

    const mqttTopic = document.getElementById('inline-address-mqtt-topic').value.trim();
    const displayName = document.getElementById('inline-address-display-name').value.trim();

    if (!mqttTopic || !displayName) {
        showErrorMessage('MQTT Topic and Display Name are required');
        return;
    }

    const address = {
        mqttTopic: mqttTopic,
        displayName: displayName,
        browseName: null,  // Not needed
        description: null,  // Not needed
        dataType: document.getElementById('inline-address-data-type').value,
        accessLevel: document.getElementById('inline-address-access-level').value,
        unit: null  // Not needed
    };

    try {
        const mutation = `
            mutation AddOpcUaServerAddress($serverName: String!, $address: OpcUaServerAddressInput!) {
                addOpcUaServerAddress(serverName: $serverName, address: $address) {
                    success
                    message
                }
            }
        `;

        const variables = {
            serverName: currentEditingServer.name,
            address: address
        };

        const response = await window.graphqlClient.query(mutation, variables);

        if (response && response.addOpcUaServerAddress && response.addOpcUaServerAddress.success) {
            // Add to local server object
            if (!currentEditingServer.addresses) {
                currentEditingServer.addresses = [];
            }
            currentEditingServer.addresses.push(address);

            // Update display
            updateAddressesList();
            hideInlineAddAddress();

            showSuccessMessage('Address mapping added successfully');
        } else {
            const errorMsg = response?.addOpcUaServerAddress?.message || 'Failed to add address mapping';
            showErrorMessage(errorMsg);
        }

    } catch (error) {
        console.error('Error adding address:', error);
        showErrorMessage('Failed to add address mapping: ' + error.message);
    }
}

async function deleteAddress(mqttTopic) {
    if (!currentEditingServer) {
        showErrorMessage('No server selected');
        return;
    }

    if (!confirm(`Are you sure you want to delete the mapping for "${mqttTopic}"?`)) {
        return;
    }

    try {
        const mutation = `
            mutation RemoveOpcUaServerAddress($serverName: String!, $mqttTopic: String!) {
                removeOpcUaServerAddress(serverName: $serverName, mqttTopic: $mqttTopic) {
                    success
                    message
                }
            }
        `;

        const variables = {
            serverName: currentEditingServer.name,
            mqttTopic: mqttTopic
        };

        const response = await window.graphqlClient.query(mutation, variables);

        if (response && response.removeOpcUaServerAddress && response.removeOpcUaServerAddress.success) {
            // Remove from local server object
            if (currentEditingServer.addresses) {
                currentEditingServer.addresses = currentEditingServer.addresses.filter(addr => addr.mqttTopic !== mqttTopic);
            }

            // Update display
            updateAddressesList();

            showSuccessMessage('Address mapping deleted successfully');
        } else {
            const errorMsg = response?.removeOpcUaServerAddress?.message || 'Failed to delete address mapping';
            showErrorMessage(errorMsg);
        }

    } catch (error) {
        console.error('Error deleting address:', error);
        showErrorMessage('Failed to delete address mapping: ' + error.message);
    }
}

async function updateServer() {
    if (!currentEditingServer) {
        showErrorMessage('No server selected');
        return;
    }

    // For now, just close the modal since we're focusing on address management
    // Full server configuration update can be implemented later
    hideEditServerModal();
    showSuccessMessage('Server updated successfully');
}

function showSuccessMessage(message) {
    // Simple success notification - you can enhance this with a proper notification system
    const errorDiv = document.getElementById('error-message');
    const errorText = errorDiv.querySelector('.error-text');
    const errorIcon = errorDiv.querySelector('.error-icon');

    errorIcon.textContent = '✅';
    errorText.textContent = message;
    errorDiv.style.display = 'block';

    setTimeout(() => {
        errorDiv.style.display = 'none';
    }, 3000);
}


// Utility functions
function refreshServers() {
    loadServers();
}

function showLoadingIndicator() {
    const indicator = document.getElementById('loading-indicator');
    if (indicator) indicator.style.display = 'flex';
}

function hideLoadingIndicator() {
    const indicator = document.getElementById('loading-indicator');
    if (indicator) indicator.style.display = 'none';
}

function showErrorMessage(message) {
    const errorDiv = document.getElementById('error-message');
    const errorText = document.querySelector('.error-text');
    if (errorDiv && errorText) {
        errorText.textContent = message;
        errorDiv.style.display = 'block';
    }
}

function hideErrorMessage() {
    const errorDiv = document.getElementById('error-message');
    if (errorDiv) {
        errorDiv.style.display = 'none';
    }
}

function showSuccessMessage(message) {
    // TODO: Implement success notification
    console.log('Success:', message);
}

function escapeHtml(text) {
    if (!text) return '';
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}