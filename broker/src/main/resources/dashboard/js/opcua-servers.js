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
        }
    });

    // Form validation
    const form = document.getElementById('add-server-form');
    if (form) {
        form.addEventListener('submit', function(e) {
            e.preventDefault();
            saveServer();
        });
    }
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
                    namespace
                    nodeId
                    enabled
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

function editServer(serverName) {
    // TODO: Implement edit functionality
    alert('Edit functionality will be implemented in the next version');
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