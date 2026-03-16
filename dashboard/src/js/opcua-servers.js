// OPC UA Servers Management JavaScript

let servers = [];
let clusterNodes = [];

// Initialize page
document.addEventListener('DOMContentLoaded', function () {
    checkAuthAndLoadData();
    setupEventListeners();
});

function checkAuthAndLoadData() {
    if (!window.isLoggedIn()) {
        window.location.href = '/pages/login.html';
        return;
    }

    loadServers();
}

function setupEventListeners() {
    // No-op: modal event listeners removed
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
                    hostname
                    bindAddress
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
            <div class="action-buttons">
                <ix-icon-button icon="highlight" variant="primary" ghost size="16" title="Edit Server" onclick="editServer('${escapeHtml(server.name)}')"></ix-icon-button>
                <ix-icon-button icon="${serverStatus === 'STOPPED' || serverStatus === 'DISABLED' || serverStatus === 'UNKNOWN' ? 'play' : 'pause'}" variant="primary" ghost size="16" title="${serverStatus === 'STOPPED' || serverStatus === 'DISABLED' || serverStatus === 'UNKNOWN' ? 'Start Server' : 'Stop Server'}" onclick="${serverStatus === 'STOPPED' || serverStatus === 'DISABLED' || serverStatus === 'UNKNOWN' ? `startServer('${escapeHtml(server.name)}')` : `stopServer('${escapeHtml(server.name)}')`}"></ix-icon-button>
                <ix-icon-button icon="lock-closed" variant="primary" ghost size="16" title="Manage Certificates" onclick="manageCertificates('${escapeHtml(server.name)}')"></ix-icon-button>
                <ix-icon-button icon="trashcan" variant="primary" ghost size="16" class="btn-delete" title="Delete Server" onclick="deleteServer('${escapeHtml(server.name)}')"></ix-icon-button>
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

async function startServer(serverName) {
    try {
        const mutation = `
            mutation StartOpcUaServer($serverName: String!, $nodeId: String) {
                opcUaServer {
                    start(serverName: $serverName, nodeId: $nodeId) {
                        success
                        message
                    }
                }
            }
        `;

        const variables = {
            serverName: serverName,
            nodeId: "*" // Start on all applicable nodes
        };

        const response = await window.graphqlClient.query(mutation, variables);

        if (response && response.opcUaServer.start.success) {
            await loadServers();
            showSuccessMessage(`Server '${serverName}' started successfully`);
        } else {
            throw new Error(response?.opcUaServer?.start?.message || 'Failed to start server');
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
                opcUaServer {
                    stop(serverName: $serverName, nodeId: $nodeId) {
                        success
                        message
                    }
                }
            }
        `;

        const variables = {
            serverName: serverName,
            nodeId: "*" // Stop on all applicable nodes
        };

        const response = await window.graphqlClient.query(mutation, variables);

        if (response && response.opcUaServer.stop.success) {
            await loadServers();
            showSuccessMessage(`Server '${serverName}' stopped successfully`);
        } else {
            throw new Error(response?.opcUaServer?.stop?.message || 'Failed to stop server');
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
                opcUaServer {
                    delete(serverName: $serverName) {
                        success
                        message
                    }
                }
            }
        `;

        const variables = {
            serverName: serverName
        };

        const response = await window.graphqlClient.query(mutation, variables);

        if (response && response.opcUaServer.delete.success) {
            await loadServers();
            showSuccessMessage(`Server '${serverName}' deleted successfully`);
        } else {
            throw new Error(response?.opcUaServer?.delete?.message || 'Failed to delete server');
        }

    } catch (error) {
        console.error('Error deleting server:', error);
        alert('Failed to delete server: ' + error.message);
    }
}

function editServer(serverName) {
    window.spaLocation.href = `/pages/opcua-server-detail.html?server=${encodeURIComponent(serverName)}`;
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

function manageCertificates(serverName) {
    // Navigate to the certificate management page for the specific server
    window.spaLocation.href = `/pages/opcua-server-certificates.html?server=${encodeURIComponent(serverName)}`;
}

function escapeHtml(text) {
    if (!text) return '';
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}