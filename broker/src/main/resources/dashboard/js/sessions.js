class SessionManager {
    constructor() {
        this.sessions = [];
        this.distributionChart = null;
        this.currentNodeFilter = '';
        this.currentConnectionFilter = '';

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

    isLoggedIn() {
        const token = localStorage.getItem('monstermq_token');
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
        const token = localStorage.getItem('monstermq_token');
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
        const isAdmin = localStorage.getItem('monstermq_isAdmin') === 'true';
        const usersLink = document.getElementById('users-link');
        if (isAdmin) {
            usersLink.style.display = 'inline';
        }

        document.getElementById('logout-link').addEventListener('click', (e) => {
            e.preventDefault();
            this.logout();
        });

        document.getElementById('refresh-sessions').addEventListener('click', () => {
            this.loadSessions();
        });

        document.getElementById('node-filter').addEventListener('change', (e) => {
            this.currentNodeFilter = e.target.value;
            this.filterSessions();
        });

        document.getElementById('connection-filter').addEventListener('change', (e) => {
            this.currentConnectionFilter = e.target.value;
            this.filterSessions();
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

            // Use GraphQL client directly
            this.sessions = await window.graphqlClient.getSessions();
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

    updateNodeFilter() {
        const nodeFilter = document.getElementById('node-filter');
        const nodes = [...new Set(this.sessions.map(s => s.nodeId))].sort();

        nodeFilter.innerHTML = '<option value="">All Nodes</option>' +
            nodes.map(node => `<option value="${node}">${node}</option>`).join('');
    }

    filterSessions() {
        let filtered = [...this.sessions];

        if (this.currentNodeFilter) {
            filtered = filtered.filter(s => s.nodeId === this.currentNodeFilter);
        }

        if (this.currentConnectionFilter !== '') {
            const isConnected = this.currentConnectionFilter === 'true';
            filtered = filtered.filter(s => s.connected === isConnected);
        }

        this.renderSessions(filtered);
    }

    renderSessions(sessions = null) {
        const sessionsToRender = sessions || this.sessions;
        const tableBody = document.getElementById('sessions-table-body');

        if (sessionsToRender.length === 0) {
            tableBody.innerHTML = `
                <tr>
                    <td colspan="9" style="text-align: center; color: var(--text-muted); padding: 2rem;">
                        No sessions found
                    </td>
                </tr>
            `;
            return;
        }

        tableBody.innerHTML = sessionsToRender.map(session => {
            const metrics = session.metrics || { messagesIn: 0, messagesOut: 0 };

            return `
                <tr>
                    <td><strong>${this.escapeHtml(session.clientId)}</strong></td>
                    <td>${this.escapeHtml(session.nodeId)}</td>
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
        const metrics = session.metrics || { messagesIn: 0, messagesOut: 0 };
        const subscriptions = session.subscriptions || [];

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

    showError(message) {
        // Simple error display - could be enhanced with toast notifications
        console.error(message);
        alert(message);
    }
}

document.addEventListener('DOMContentLoaded', () => {
    new SessionManager();
});