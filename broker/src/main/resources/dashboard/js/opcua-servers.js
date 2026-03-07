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
                <button class="btn-icon btn-view" onclick="editServer('${escapeHtml(server.name)}')" title="Edit Server" aria-label="Edit Server">
                    <svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor">
                        <path d="M3 17.25V21h3.75L17.81 9.94l-3.75-3.75L3 17.25zM20.71 7.04c.39-.39.39-1.02 0-1.41l-2.34-2.34c-.39-.39-1.02-.39-1.41 0l-1.83 1.83 3.75 3.75 1.83-1.83z"/>
                    </svg>
                </button>
        <button class="btn-icon ${serverStatus === 'STOPPED' || serverStatus === 'DISABLED' || serverStatus === 'UNKNOWN' ? 'btn-play' : 'btn-pause'}"
                        onclick="${serverStatus === 'STOPPED' || serverStatus === 'DISABLED' || serverStatus === 'UNKNOWN' ? `startServer('${escapeHtml(server.name)}')` : `stopServer('${escapeHtml(server.name)}')`}"
                        title="${serverStatus === 'STOPPED' || serverStatus === 'DISABLED' || serverStatus === 'UNKNOWN' ? 'Start Server' : 'Stop Server'}">
                    ${serverStatus === 'STOPPED' || serverStatus === 'DISABLED' || serverStatus === 'UNKNOWN' ?
            '<svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor"><path d="M8 5v14l11-7z"/></svg>' :
            '<svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor"><path d="M6 19h4V5H6v14zm8-14v14h4V5h-4z"/></svg>'
        }
                </button>
                <button class="btn-icon btn-cert" onclick="manageCertificates('${escapeHtml(server.name)}')" title="Manage Certificates" aria-label="Manage Certificates">
                    <svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor">
                        <path d="M18 8h-1V6c0-2.76-2.24-5-5-5S7 3.24 7 6v2H6c-1.1 0-2 .9-2 2v10c0 1.1.9 2 2 2h12c1.1 0 2-.9 2-2V10c0-1.1-.9-2-2-2zm-6 9c-1.1 0-2-.9-2-2s.9-2 2-2 2 .9 2 2-.9 2-2 2zm3.1-9H8.9V6c0-1.71 1.39-3.1 3.1-3.1 1.71 0 3.1 1.39 3.1 3.1v2z"/>
                    </svg>
                </button>
                <button class="btn-icon btn-delete" onclick="deleteServer('${escapeHtml(server.name)}')" title="Delete Server" aria-label="Delete Server">
                    <svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor">
                        <path d="M6 19c0 1.1.9 2 2 2h8c1.1 0 2-.9 2-2V7H6v12zM19 4h-3.5l-1-1h-5l-1 1H5v2h14V4z"/>
                    </svg>
                </button>
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
    window.location.href = `/pages/opcua-server-detail.html?server=${encodeURIComponent(serverName)}`;
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
    window.location.href = `/pages/opcua-server-certificates.html?server=${encodeURIComponent(serverName)}`;
}

function escapeHtml(text) {
    if (!text) return '';
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}