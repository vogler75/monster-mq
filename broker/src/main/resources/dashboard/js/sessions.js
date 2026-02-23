class SessionManager {
    constructor() {
        this.sessions = [];
        this.distributionChart = null;
        this.currentNodeFilter = '';
        this.currentConnectionFilter = '';
        this.selectedSessions = new Set();

        // Load filters from URL parameters
        this.loadFiltersFromURL();

        this.init();
    }

    init() {
        if (!this.isLoggedIn()) {
            window.location.href = '/pages/login.html';
            return;
        }

        this.setupUI();
        this.setupChart();
        this.loadSessions();
    }

    loadFiltersFromURL() {
        const urlParams = new URLSearchParams(window.location.search);
        this.currentNodeFilter = urlParams.get('nodeId') || '';
        this.currentConnectionFilter = urlParams.get('connected') || '';
    }

    updateURL() {
        const urlParams = new URLSearchParams();
        if (this.currentNodeFilter) {
            urlParams.set('nodeId', this.currentNodeFilter);
        }
        if (this.currentConnectionFilter !== '') {
            urlParams.set('connected', this.currentConnectionFilter);
        }

        const newUrl = `${window.location.pathname}${urlParams.toString() ? '?' + urlParams.toString() : ''}`;
        window.history.replaceState({}, '', newUrl);
    }

    isLoggedIn() {
        const token = safeStorage.getItem('monstermq_token');
        if (!token) return false;

        // If token is 'null', authentication is disabled
        if (token === 'null') return true;

        try {
            const decoded = JSON.parse(atob(token.split('.')[1]));
            const now = Date.now() / 1000;
            return decoded.exp > now;
        } catch {
            return false;
        }
    }

    getAuthHeaders() {
        const token = safeStorage.getItem('monstermq_token');
        const headers = {
            'Content-Type': 'application/json'
        };

        // Only add Authorization header if token is not null
        if (token && token !== 'null') {
            headers.Authorization = `Bearer ${token}`;
        }

        return headers;
    }

    setupUI() {
        // UI setup is now handled by sidebar.js

        document.getElementById('refresh-sessions').addEventListener('click', () => {
            this.loadSessions();
        });

        document.getElementById('node-filter').addEventListener('change', (e) => {
            this.currentNodeFilter = e.target.value;
            this.updateURL();
            this.loadSessions();
        });

        document.getElementById('connection-filter').addEventListener('change', (e) => {
            this.currentConnectionFilter = e.target.value;
            this.updateURL();
            this.loadSessions();
        });

        document.getElementById('close-modal').addEventListener('click', () => {
            document.getElementById('session-modal').style.display = 'none';
        });

        document.addEventListener('click', (e) => {
            if (e.target.classList.contains('view-session-btn')) {
                const clientId = e.target.dataset.clientId;
                const nodeId = e.target.dataset.nodeId;
                this.showSessionDetails(clientId, nodeId);
            }
        });

        // Select all sessions checkbox
        document.getElementById('select-all-sessions').addEventListener('change', (e) => {
            const checked = e.target.checked;
            document.querySelectorAll('.session-checkbox').forEach(checkbox => {
                checkbox.checked = checked;
                if (checked) {
                    this.selectedSessions.add(checkbox.value);
                } else {
                    this.selectedSessions.delete(checkbox.value);
                }
            });
            this.updateRemoveButton();
        });

        // Remove selected sessions button
        document.getElementById('remove-selected-sessions').addEventListener('click', () => {
            this.removeSessions();
        });

        // Restore filter values from URL to UI
        this.restoreFilterUI();
    }

    restoreFilterUI() {
        const nodeFilter = document.getElementById('node-filter');
        const connectionFilter = document.getElementById('connection-filter');

        if (nodeFilter && this.currentNodeFilter) {
            nodeFilter.value = this.currentNodeFilter;
        }
        if (connectionFilter && this.currentConnectionFilter) {
            connectionFilter.value = this.currentConnectionFilter;
        }
    }

    logout() {
        localStorage.removeItem('monstermq_token');
        localStorage.removeItem('monstermq_username');
        localStorage.removeItem('monstermq_isAdmin');
        window.location.href = '/';
    }

    setupChart() {
        const ctx = document.getElementById('sessionDistributionChart').getContext('2d');

        Chart.defaults.color = '#CBD5E1';
        Chart.defaults.borderColor = '#475569';

        this.distributionChart = new Chart(ctx, {
            type: 'doughnut',
            data: {
                labels: [],
                datasets: [{
                    data: [],
                    backgroundColor: [
                        '#7C3AED',
                        '#10B981',
                        '#14B8A6',
                        '#F97316',
                        '#EF4444',
                        '#8B5CF6'
                    ],
                    borderColor: '#334155',
                    borderWidth: 2
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                aspectRatio: 2,
                plugins: {
                    legend: {
                        position: 'right',
                    }
                }
            }
        });
    }

    async loadSessions() {
        try {
            this.setRefreshLoading(true);

            // Prepare GraphQL parameters from current filters
            const nodeId = this.currentNodeFilter || null;
            const connected = this.currentConnectionFilter !== '' ? (this.currentConnectionFilter === 'true') : null;

            // Use GraphQL client with server-side filtering
            this.sessions = await window.graphqlClient.getSessions(nodeId, connected);
            this.updateMetrics();
            this.updateChart();
            this.updateNodeFilter();
            this.renderSessions();

        } catch (error) {
            console.error('Error loading sessions:', error);
            this.showError('Failed to load sessions: ' + error.message);
        } finally {
            this.setRefreshLoading(false);
        }
    }

    setRefreshLoading(loading) {
        const refreshBtn = document.getElementById('refresh-sessions');
        const refreshText = document.getElementById('refresh-text');
        const refreshSpinner = document.getElementById('refresh-spinner');

        refreshBtn.disabled = loading;

        if (loading) {
            refreshText.style.display = 'none';
            refreshSpinner.style.display = 'inline-block';
        } else {
            refreshText.style.display = 'inline';
            refreshSpinner.style.display = 'none';
        }
    }

    updateMetrics() {
        const connected = this.sessions.filter(s => s.connected).length;
        const disconnected = this.sessions.length - connected;
        const totalQueued = this.sessions.reduce((sum, s) => sum + (s.queuedMessageCount || 0), 0);

        document.getElementById('total-sessions').textContent = this.sessions.length;
        document.getElementById('connected-sessions').textContent = connected;
        document.getElementById('disconnected-sessions').textContent = disconnected;
        document.getElementById('total-queued').textContent = this.formatNumber(totalQueued);
    }

    updateChart() {
        const nodeDistribution = {};
        this.sessions.forEach(session => {
            nodeDistribution[session.nodeId] = (nodeDistribution[session.nodeId] || 0) + 1;
        });

        this.distributionChart.data.labels = Object.keys(nodeDistribution);
        this.distributionChart.data.datasets[0].data = Object.values(nodeDistribution);
        this.distributionChart.update();
    }

    async updateNodeFilter() {
        const nodeFilter = document.getElementById('node-filter');

        // Use already-loaded session data instead of making another query
        const nodes = [...new Set(this.sessions.map(s => s.nodeId))].sort();
        nodeFilter.innerHTML = '<option value="">All Nodes</option>' +
            nodes.map(node => `<option value="${node}">${node}</option>`).join('');

        // Restore the current filter value
        if (this.currentNodeFilter) {
            nodeFilter.value = this.currentNodeFilter;
        }
    }



    renderSessions() {
        const sessionsToRender = this.sessions;
        const tableBody = document.getElementById('sessions-table-body');

        // Clear selection state when re-rendering
        this.selectedSessions.clear();
        document.getElementById('select-all-sessions').checked = false;
        this.updateRemoveButton();

        if (sessionsToRender.length === 0) {
            tableBody.innerHTML = `
                <tr>
                    <td colspan="10" style="text-align: center; color: var(--text-muted); padding: 2rem;">
                        No sessions found
                    </td>
                </tr>
            `;
            return;
        }

        const self = this;
        tableBody.innerHTML = sessionsToRender.map(session => {
            const metrics = session.metrics && session.metrics.length > 0 ? session.metrics[0] : { messagesIn: 0, messagesOut: 0 };

            return `
                <tr>
                    <td>
                        <input type="checkbox" class="session-checkbox" value="${this.escapeHtml(session.clientId)}" style="cursor: pointer;">
                    </td>
                    <td><strong>${this.escapeHtml(session.clientId)}</strong></td>
                    <td>${this.escapeHtml(session.nodeId)}</td>
                    <td>${this.getProtocolBadge(session.protocolVersion)}</td>
                    <td>
                        <span class="status-indicator ${session.connected ? 'status-online' : 'status-offline'}">
                            <span class="status-dot"></span>
                            ${session.connected ? 'Connected' : 'Disconnected'}
                        </span>
                    </td>
                    <td>${this.formatNumber(metrics.messagesIn)}</td>
                    <td>${this.formatNumber(metrics.messagesOut)}</td>
                    <td>${this.formatNumber(session.queuedMessageCount || 0)}</td>
                    <td>
                        <span class="${session.cleanSession ? 'status-online' : 'status-offline'}">
                            ${session.cleanSession ? 'Yes' : 'No'}
                        </span>
                    </td>
                    <td>${this.escapeHtml(session.clientAddress || 'N/A')}</td>
                    <td>
                        <button class="btn btn-secondary view-session-btn"
                                data-client-id="${this.escapeHtml(session.clientId)}"
                                data-node-id="${this.escapeHtml(session.nodeId)}"
                                style="padding: 0.25rem 0.5rem; font-size: 0.75rem;">
                            View Details
                        </button>
                    </td>
                </tr>
            `;
        }).join('');

        // Add event listeners to checkboxes after rendering
        document.querySelectorAll('.session-checkbox').forEach(checkbox => {
            checkbox.addEventListener('change', (e) => {
                if (e.target.checked) {
                    self.selectedSessions.add(e.target.value);
                } else {
                    self.selectedSessions.delete(e.target.value);
                }
                self.updateRemoveButton();
            });
        });
    }

    async showSessionDetails(clientId, nodeId) {
        try {
            // Use GraphQL client directly
            const session = await window.graphqlClient.getSession(clientId, nodeId);
            if (session) {
                this.renderSessionModal(session);
            } else {
                throw new Error('Session not found');
            }
        } catch (error) {
            console.error('Error loading session details:', error);
            this.showError('Failed to load session details: ' + error.message);
        }
    }

    renderSessionModal(session) {
        const metrics = session.metrics && session.metrics.length > 0 ? session.metrics[0] : { messagesIn: 0, messagesOut: 0 };
        const subscriptions = session.subscriptions || [];
        const isV5 = session.protocolVersion === 5;

        document.getElementById('modal-title').textContent = `Session: ${session.clientId}`;
        document.getElementById('session-detail-content').innerHTML = `
            <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); gap: 1.5rem; margin-bottom: 2rem;">
                <div class="metric-card">
                    <div class="metric-header">
                        <span class="metric-title">Messages In</span>
                        <div class="metric-icon">📥</div>
                    </div>
                    <div class="metric-value">${this.formatNumber(metrics.messagesIn)}</div>
                    <div class="metric-label">Total Received</div>
                </div>

                <div class="metric-card">
                    <div class="metric-header">
                        <span class="metric-title">Messages Out</span>
                        <div class="metric-icon">📤</div>
                    </div>
                    <div class="metric-value">${this.formatNumber(metrics.messagesOut)}</div>
                    <div class="metric-label">Total Sent</div>
                </div>

                <div class="metric-card">
                    <div class="metric-header">
                        <span class="metric-title">Queue Depth</span>
                        <div class="metric-icon">⏳</div>
                    </div>
                    <div class="metric-value">${this.formatNumber(session.queuedMessageCount || 0)}</div>
                    <div class="metric-label">Pending Messages</div>
                </div>

                <div class="metric-card">
                    <div class="metric-header">
                        <span class="metric-title">Subscriptions</span>
                        <div class="metric-icon">📋</div>
                    </div>
                    <div class="metric-value">${subscriptions.length}</div>
                    <div class="metric-label">Active Topics</div>
                </div>
            </div>

            <div class="data-table">
                <div style="padding: 1rem; background: var(--dark-bg); border-bottom: 1px solid var(--dark-border);">
                    <h3 style="margin: 0; color: var(--text-primary);">Session Information</h3>
                </div>
                <table>
                    <tbody>
                        <tr>
                            <td style="font-weight: 600; width: 200px;">Client ID</td>
                            <td>${this.escapeHtml(session.clientId)}</td>
                        </tr>
                        <tr>
                            <td style="font-weight: 600;">Node ID</td>
                            <td>${this.escapeHtml(session.nodeId)}</td>
                        </tr>
                        <tr>
                            <td style="font-weight: 600;">Protocol Version</td>
                            <td>${this.getProtocolBadge(session.protocolVersion)}</td>
                        </tr>
                        <tr>
                            <td style="font-weight: 600;">Status</td>
                            <td>
                                <span class="status-indicator ${session.connected ? 'status-online' : 'status-offline'}">
                                    <span class="status-dot"></span>
                                    ${session.connected ? 'Connected' : 'Disconnected'}
                                </span>
                            </td>
                        </tr>
                        <tr>
                            <td style="font-weight: 600;">Clean Session</td>
                            <td>${session.cleanSession ? 'Yes' : 'No'}</td>
                        </tr>
                        <tr>
                            <td style="font-weight: 600;">Session Expiry</td>
                            <td>${session.sessionExpiryInterval || 0} seconds</td>
                        </tr>
                        <tr>
                            <td style="font-weight: 600;">Client Address</td>
                            <td>${this.escapeHtml(session.clientAddress || 'N/A')}</td>
                        </tr>
                    </tbody>
                </table>
            </div>

            ${isV5 ? `
            <div class="data-table" style="margin-top: 1.5rem;">
                <div style="padding: 1rem; background: var(--dark-bg); border-bottom: 1px solid var(--dark-border); display: flex; align-items: center; gap: 0.5rem;">
                    <h3 style="margin: 0; color: var(--text-primary);">MQTT v5 Connection Properties</h3>
                    <span class="protocol-badge protocol-v5">v5.0</span>
                </div>
                <table>
                    <tbody>
                        <tr>
                            <td style="font-weight: 600; width: 200px;">Receive Maximum</td>
                            <td>${session.receiveMaximum !== null && session.receiveMaximum !== undefined ? session.receiveMaximum : 'N/A'}</td>
                        </tr>
                        <tr>
                            <td style="font-weight: 600;">Maximum Packet Size</td>
                            <td>${session.maximumPacketSize !== null && session.maximumPacketSize !== undefined ? this.formatBytes(session.maximumPacketSize) : 'N/A'}</td>
                        </tr>
                        <tr>
                            <td style="font-weight: 600;">Topic Alias Maximum</td>
                            <td>${session.topicAliasMaximum !== null && session.topicAliasMaximum !== undefined ? session.topicAliasMaximum : 'N/A'}</td>
                        </tr>
                    </tbody>
                </table>
            </div>
            ` : ''}

            ${session.information ? `
            <div class="data-table" style="margin-top: 1.5rem;">
                <div style="padding: 1rem; background: var(--dark-bg); border-bottom: 1px solid var(--dark-border);">
                    <h3 style="margin: 0; color: var(--text-primary);">Connection Information</h3>
                </div>
                <div style="padding: 1rem;">
                    <pre style="background: var(--dark-card); padding: 1rem; border-radius: 6px; margin: 0; overflow-x: auto; font-size: 0.9rem; color: var(--text-primary);">${this.formatJson(session.information)}</pre>
                </div>
            </div>
            ` : ''}

            ${subscriptions.length > 0 ? `
                <div class="data-table" style="margin-top: 2rem;">
                    <div style="padding: 1rem; background: var(--dark-bg); border-bottom: 1px solid var(--dark-border);">
                        <h3 style="margin: 0; color: var(--text-primary);">Subscriptions (${subscriptions.length})</h3>
                    </div>
                    <table>
                        <thead>
                            <tr>
                                <th>Topic Filter</th>
                                <th>QoS</th>
                                ${isV5 ? '<th>Options</th>' : ''}
                            </tr>
                        </thead>
                        <tbody>
                            ${subscriptions.map(sub => `
                                <tr>
                                    <td style="font-family: monospace;">${this.escapeHtml(sub.topicFilter)}</td>
                                    <td>
                                        <span style="padding: 0.25rem 0.5rem; background: var(--monster-purple); color: white; border-radius: 4px; font-size: 0.75rem;">
                                            QoS ${sub.qos}
                                        </span>
                                    </td>
                                    ${isV5 ? `<td>${this.getSubscriptionOptions(sub)}</td>` : ''}
                                </tr>
                            `).join('')}
                        </tbody>
                    </table>
                </div>
            ` : ''}
        `;

        document.getElementById('session-modal').style.display = 'block';
    }

    formatNumber(num) {
        if (num >= 1000000) {
            return (num / 1000000).toFixed(1) + 'M';
        } else if (num >= 1000) {
            return (num / 1000).toFixed(1) + 'K';
        }
        return num.toString();
    }

    escapeHtml(text) {
        const div = document.createElement('div');
        div.textContent = text;
        return div.innerHTML;
    }

    formatJson(jsonString) {
        try {
            const parsed = JSON.parse(jsonString);
            return JSON.stringify(parsed, null, 2);
        } catch (e) {
            return jsonString; // Return as-is if not valid JSON
        }
    }

    showError(message) {
        // Simple error display - could be enhanced with toast notifications
        console.error(message);
        alert(message);
    }

    updateRemoveButton() {
        const removeButton = document.getElementById('remove-selected-sessions');
        const selectedCount = document.getElementById('selected-count');

        if (this.selectedSessions.size > 0) {
            removeButton.style.display = 'inline-block';
            selectedCount.textContent = this.selectedSessions.size;
        } else {
            removeButton.style.display = 'none';
        }
    }

    async removeSessions() {
        if (this.selectedSessions.size === 0) {
            return;
        }

        const clientIds = Array.from(this.selectedSessions);
        const confirmMessage = `Are you sure you want to remove ${clientIds.length} session(s)?\n\nThis will:\n- Disconnect active sessions\n- Remove persistent session data from the database\n\nClient IDs:\n${clientIds.join('\n')}`;

        if (!confirm(confirmMessage)) {
            return;
        }

        try {
            const mutation = `
                mutation RemoveSessions($clientIds: [String!]!) {
                    session {
                        removeSessions(clientIds: $clientIds) {
                            success
                            message
                            removedCount
                            results {
                                clientId
                                success
                                error
                                nodeId
                            }
                        }
                    }
                }
            `;

            const headers = this.getAuthHeaders();
            const response = await fetch('/graphql', {
                method: 'POST',
                headers: headers,
                body: JSON.stringify({
                    query: mutation,
                    variables: { clientIds }
                })
            });

            const result = await response.json();

            if (result.errors) {
                throw new Error(result.errors[0].message);
            }

            const removalResult = result.data.session.removeSessions;

            // Show detailed results
            const failedRemovals = removalResult.results.filter(r => !r.success);
            let message = removalResult.message;

            if (failedRemovals.length > 0) {
                message += '\n\nFailed removals:\n' + failedRemovals.map(r =>
                    `- ${r.clientId}: ${r.error}`
                ).join('\n');
            }

            alert(message);

            // Clear selection and reload sessions
            this.selectedSessions.clear();
            await this.loadSessions();

        } catch (error) {
            console.error('Error removing sessions:', error);
            this.showError('Failed to remove sessions: ' + error.message);
        }
    }

    getProtocolBadge(protocolVersion) {
        if (protocolVersion === 5) {
            return '<span class="protocol-badge protocol-v5">MQTT v5.0</span>';
        } else if (protocolVersion === 4) {
            return '<span class="protocol-badge protocol-v3">MQTT v3.1.1</span>';
        } else {
            return '<span class="protocol-badge">v' + (protocolVersion || '?') + '</span>';
        }
    }

    getSubscriptionOptions(subscription) {
        const options = [];
        
        if (subscription.noLocal) {
            options.push('<span class="sub-option-badge sub-no-local" title="No Local: Don\'t receive messages I publish">🚫 No Local</span>');
        }
        
        if (subscription.retainHandling !== null && subscription.retainHandling !== undefined && subscription.retainHandling !== 0) {
            const retainLabels = {
                1: 'If New',
                2: 'Never'
            };
            const label = retainLabels[subscription.retainHandling] || subscription.retainHandling;
            options.push(`<span class="sub-option-badge sub-retain-handling" title="Retain Handling: ${label}">📨 Retain: ${label}</span>`);
        }
        
        if (subscription.retainAsPublished) {
            options.push('<span class="sub-option-badge sub-rap" title="Retain As Published: Preserve original retain flag">📌 RAP</span>');
        }
        
        return options.length > 0 ? options.join(' ') : '<span style="color: var(--text-muted);">None</span>';
    }

    formatBytes(bytes) {
        if (bytes >= 1048576) {
            return (bytes / 1048576).toFixed(2) + ' MB';
        } else if (bytes >= 1024) {
            return (bytes / 1024).toFixed(2) + ' KB';
        }
        return bytes + ' bytes';
    }
}

document.addEventListener('DOMContentLoaded', () => {
    new SessionManager();
});