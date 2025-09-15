class DashboardManager {
    constructor() {
        this.pollingInterval = null;
        this.trafficChart = null;
        this.messageBusChart = null;
        this.trafficData = [];
        this.messageBusData = [];
        this.maxDataPoints = 50;
        this.currentTimeframe = '5m';
        this.pollingIntervalMs = 5000; // Poll every 5 seconds

        this.init();
    }

    init() {
        if (!this.isLoggedIn()) {
            window.location.href = '/pages/login.html';
            return;
        }

        this.setupUI();
        this.setupCharts();
        this.startPolling();
        this.loadInitialData();
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

        document.querySelectorAll('.chart-controls button').forEach(btn => {
            btn.addEventListener('click', (e) => {
                document.querySelectorAll('.chart-controls button').forEach(b => b.classList.remove('active'));
                e.target.classList.add('active');
                this.currentTimeframe = e.target.dataset.timeframe;
                this.updateChartTimeframe();
            });
        });
    }

    logout() {
        localStorage.removeItem('monstermq_token');
        localStorage.removeItem('monstermq_username');
        localStorage.removeItem('monstermq_isAdmin');
        window.location.href = '/pages/login.html';
    }

    setupCharts() {
        const trafficCtx = document.getElementById('trafficChart').getContext('2d');
        const messageBusCtx = document.getElementById('messageBusChart').getContext('2d');

        Chart.defaults.color = '#CBD5E1';
        Chart.defaults.borderColor = '#475569';

        this.trafficChart = new Chart(trafficCtx, {
            type: 'line',
            data: {
                labels: [],
                datasets: [
                    {
                        label: 'Messages In',
                        data: [],
                        borderColor: '#10B981',
                        backgroundColor: 'rgba(16, 185, 129, 0.1)',
                        tension: 0.4,
                        fill: true
                    },
                    {
                        label: 'Messages Out',
                        data: [],
                        borderColor: '#7C3AED',
                        backgroundColor: 'rgba(124, 58, 237, 0.1)',
                        tension: 0.4,
                        fill: true
                    }
                ]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                aspectRatio: 2,
                scales: {
                    y: {
                        beginAtZero: true,
                        grid: { color: '#334155' }
                    },
                    x: {
                        grid: { color: '#334155' }
                    }
                },
                plugins: {
                    legend: {
                        position: 'top',
                    }
                }
            }
        });

        this.messageBusChart = new Chart(messageBusCtx, {
            type: 'bar',
            data: {
                labels: [],
                datasets: [
                    {
                        label: 'Bus In',
                        data: [],
                        backgroundColor: '#14B8A6',
                        borderColor: '#14B8A6',
                        borderWidth: 1
                    },
                    {
                        label: 'Bus Out',
                        data: [],
                        backgroundColor: '#F97316',
                        borderColor: '#F97316',
                        borderWidth: 1
                    }
                ]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                aspectRatio: 2,
                scales: {
                    y: {
                        beginAtZero: true,
                        grid: { color: '#334155' }
                    },
                    x: {
                        grid: { color: '#334155' }
                    }
                },
                plugins: {
                    legend: {
                        position: 'top',
                    }
                }
            }
        });
    }

    startPolling() {
        console.log('Starting metrics polling every', this.pollingIntervalMs, 'ms');

        // Start polling immediately, then continue at intervals
        this.pollingInterval = setInterval(() => {
            this.loadInitialData();
        }, this.pollingIntervalMs);
    }

    stopPolling() {
        if (this.pollingInterval) {
            clearInterval(this.pollingInterval);
            this.pollingInterval = null;
            console.log('Stopped metrics polling');
        }
    }

    async loadInitialData() {
        try {
            // Use GraphQL client directly
            const brokers = await window.graphqlClient.getBrokers();
            this.updateMetrics(brokers);
        } catch (error) {
            console.error('Failed to load initial data:', error);
        }
    }

    updateMetrics(brokers) {
        this.updateOverviewCards(brokers);
        this.updateBrokerTable(brokers);
        this.updateCharts(brokers);
    }

    updateOverviewCards(brokers) {
        const clusterTotals = brokers.reduce((acc, broker) => {
            const metrics = broker.metrics && broker.metrics.length > 0 ? broker.metrics[0] : {};
            return {
                messagesIn: acc.messagesIn + (metrics.messagesIn || 0),
                messagesOut: acc.messagesOut + (metrics.messagesOut || 0),
                totalSessions: acc.totalSessions + (metrics.nodeSessionCount || 0),
                queuedMessages: acc.queuedMessages + (metrics.queuedMessagesCount || 0),
                messageBusIn: acc.messageBusIn + (metrics.messageBusIn || 0),
                messageBusOut: acc.messageBusOut + (metrics.messageBusOut || 0)
            };
        }, {
            messagesIn: 0,
            messagesOut: 0,
            totalSessions: 0,
            queuedMessages: 0,
            messageBusIn: 0,
            messageBusOut: 0
        });

        const overviewContainer = document.getElementById('cluster-overview');
        overviewContainer.innerHTML = `
            <div class="metric-card">
                <div class="metric-header">
                    <span class="metric-title">Total Messages In</span>
                    <div class="metric-icon">📥</div>
                </div>
                <div class="metric-value">${this.formatNumber(clusterTotals.messagesIn)}</div>
                <div class="metric-label">Cluster Total</div>
            </div>

            <div class="metric-card">
                <div class="metric-header">
                    <span class="metric-title">Total Messages Out</span>
                    <div class="metric-icon">📤</div>
                </div>
                <div class="metric-value">${this.formatNumber(clusterTotals.messagesOut)}</div>
                <div class="metric-label">Cluster Total</div>
            </div>

            <div class="metric-card">
                <div class="metric-header">
                    <span class="metric-title">Active Sessions</span>
                    <div class="metric-icon">👥</div>
                </div>
                <div class="metric-value">${clusterTotals.totalSessions}</div>
                <div class="metric-label">${brokers.length} Node${brokers.length !== 1 ? 's' : ''}</div>
            </div>

            <div class="metric-card">
                <div class="metric-header">
                    <span class="metric-title">Queued Messages</span>
                    <div class="metric-icon">⏳</div>
                </div>
                <div class="metric-value">${this.formatNumber(clusterTotals.queuedMessages)}</div>
                <div class="metric-label">Pending Delivery</div>
            </div>

            <div class="metric-card">
                <div class="metric-header">
                    <span class="metric-title">Bus Messages In</span>
                    <div class="metric-icon">🔄</div>
                </div>
                <div class="metric-value">${this.formatNumber(clusterTotals.messageBusIn)}</div>
                <div class="metric-label">Inter-node Communication</div>
            </div>

            <div class="metric-card">
                <div class="metric-header">
                    <span class="metric-title">Bus Messages Out</span>
                    <div class="metric-icon">📡</div>
                </div>
                <div class="metric-value">${this.formatNumber(clusterTotals.messageBusOut)}</div>
                <div class="metric-label">Inter-node Communication</div>
            </div>
        `;
    }

    updateBrokerTable(brokers) {
        const tableBody = document.getElementById('broker-table-body');
        tableBody.innerHTML = brokers.map(broker => {
            const metrics = broker.metrics && broker.metrics.length > 0 ? broker.metrics[0] : {};
            const isHealthy = metrics.nodeSessionCount >= 0; // Simple health check

            return `
                <tr>
                    <td><strong>${broker.nodeId}</strong></td>
                    <td>
                        <span class="status-indicator ${isHealthy ? 'status-online' : 'status-offline'}">
                            <span class="status-dot"></span>
                            ${isHealthy ? 'Online' : 'Offline'}
                        </span>
                    </td>
                    <td>${this.formatNumber(metrics.messagesIn || 0)}</td>
                    <td>${this.formatNumber(metrics.messagesOut || 0)}</td>
                    <td>${metrics.nodeSessionCount || 0}</td>
                    <td>${this.formatNumber(metrics.queuedMessagesCount || 0)}</td>
                    <td>
                        <span style="color: #14B8A6;">${this.formatNumber(metrics.messageBusIn || 0)}</span> /
                        <span style="color: #F97316;">${this.formatNumber(metrics.messageBusOut || 0)}</span>
                    </td>
                </tr>
            `;
        }).join('');
    }

    updateCharts(brokers) {
        const now = new Date();
        const timeLabel = now.toLocaleTimeString();

        const clusterTotals = brokers.reduce((acc, broker) => {
            const metrics = broker.metrics && broker.metrics.length > 0 ? broker.metrics[0] : {};
            return {
                messagesIn: acc.messagesIn + (metrics.messagesIn || 0),
                messagesOut: acc.messagesOut + (metrics.messagesOut || 0),
                messageBusIn: acc.messageBusIn + (metrics.messageBusIn || 0),
                messageBusOut: acc.messageBusOut + (metrics.messageBusOut || 0)
            };
        }, { messagesIn: 0, messagesOut: 0, messageBusIn: 0, messageBusOut: 0 });

        if (this.trafficChart.data.labels.length >= this.maxDataPoints) {
            this.trafficChart.data.labels.shift();
            this.trafficChart.data.datasets[0].data.shift();
            this.trafficChart.data.datasets[1].data.shift();
        }

        this.trafficChart.data.labels.push(timeLabel);
        this.trafficChart.data.datasets[0].data.push(clusterTotals.messagesIn);
        this.trafficChart.data.datasets[1].data.push(clusterTotals.messagesOut);
        this.trafficChart.update('none');

        this.messageBusChart.data.labels = brokers.map(b => b.nodeId);
        this.messageBusChart.data.datasets[0].data = brokers.map(b => {
            const metrics = b.metrics && b.metrics.length > 0 ? b.metrics[0] : {};
            return metrics.messageBusIn || 0;
        });
        this.messageBusChart.data.datasets[1].data = brokers.map(b => {
            const metrics = b.metrics && b.metrics.length > 0 ? b.metrics[0] : {};
            return metrics.messageBusOut || 0;
        });
        this.messageBusChart.update('none');
    }

    updateChartTimeframe() {
        // Implementation for different timeframes would require historical data API
        console.log(`Switching to ${this.currentTimeframe} timeframe`);
    }

    formatNumber(num) {
        if (num >= 1000000) {
            return (num / 1000000).toFixed(1) + 'M';
        } else if (num >= 1000) {
            return (num / 1000).toFixed(1) + 'K';
        }
        return num.toString();
    }
}

document.addEventListener('DOMContentLoaded', () => {
    new DashboardManager();
});