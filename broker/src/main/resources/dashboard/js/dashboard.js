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
        this.historicalDataLoaded = false;
        this.liveUpdatesEnabled = true;

        this.init();
    }

    init() {
        if (!this.isLoggedIn()) {
            window.location.href = '/pages/login.html';
            return;
        }

        this.setupUI();
        this.setupCharts();
        this.loadInitialDataWithHistory();
        this.startPolling();
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
        // Dashboard-specific UI setup

        document.querySelectorAll('.chart-controls button').forEach(btn => {
            btn.addEventListener('click', (e) => {
                document.querySelectorAll('.chart-controls button').forEach(b => b.classList.remove('active'));
                e.target.classList.add('active');
                this.currentTimeframe = e.target.dataset.timeframe;
                this.loadHistoricalDataForTimeframe();
            });
        });

        // Live updates toggle
        document.getElementById('live-updates-toggle').addEventListener('change', (e) => {
            this.liveUpdatesEnabled = e.target.checked;
            console.log('Live updates', this.liveUpdatesEnabled ? 'enabled' : 'disabled');
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
                    },
                    {
                        label: 'Bridge In',
                        data: [],
                        borderColor: '#0EA5E9',
                        backgroundColor: 'rgba(14, 165, 233, 0.1)',
                        tension: 0.4,
                        fill: true
                    },
                    {
                        label: 'Bridge Out',
                        data: [],
                        borderColor: '#F59E0B',
                        backgroundColor: 'rgba(245, 158, 11, 0.1)',
                        tension: 0.4,
                        fill: true
                    },
                    {
                        label: 'OPC UA In',
                        data: [],
                        borderColor: '#06B6D4',
                        backgroundColor: 'rgba(6, 182, 212, 0.1)',
                        tension: 0.4,
                        fill: true
                    },
                    {
                        label: 'OPC UA Out',
                        data: [],
                        borderColor: '#9333EA',
                        backgroundColor: 'rgba(147, 51, 234, 0.1)',
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

        // Start polling for real-time updates
        this.pollingInterval = setInterval(() => {
            this.loadRealtimeUpdate();
        }, this.pollingIntervalMs);
    }

    stopPolling() {
        if (this.pollingInterval) {
            clearInterval(this.pollingInterval);
            this.pollingInterval = null;
            console.log('Stopped metrics polling');
        }
    }

    async loadInitialDataWithHistory() {
        try {
            // Load historical data based on current timeframe
            await this.loadHistoricalDataForTimeframe();
            this.historicalDataLoaded = true;
        } catch (error) {
            console.error('Failed to load initial data with history:', error);
        }
    }

    async loadRealtimeUpdate() {
        try {
            // Only proceed if live updates are enabled
            if (!this.liveUpdatesEnabled) {
                return;
            }

            // Only load current metrics for real-time updates
            const brokers = await window.graphqlClient.getBrokers();
            this.updateMetrics(brokers);

            // Add real-time data point to charts if historical data is loaded
            if (this.historicalDataLoaded) {
                this.addRealtimeDataPoint(brokers);
            }
        } catch (error) {
            console.error('Failed to load real-time data:', error);
        }
    }

    async loadHistoricalDataForTimeframe() {
        try {
            const minutes = this.getMinutesForTimeframe(this.currentTimeframe);
            console.log(`Loading historical data for ${this.currentTimeframe} (${minutes} minutes)`);

            const brokers = await window.graphqlClient.getBrokersWithHistory(minutes);
            console.log('Historical data received:', brokers);

            this.updateMetrics(brokers);
            this.initializeChartsWithHistory(brokers);

            // Also show current vs historical comparison
            const currentBrokers = await window.graphqlClient.getBrokers();
            console.log('Current data (for comparison):', currentBrokers);

        } catch (error) {
            console.error('Failed to load historical data:', error);
        }
    }

    getMinutesForTimeframe(timeframe) {
        switch (timeframe) {
            case '5m': return 5;
            case '1h': return 60;
            case '24h': return 1440;
            default: return 5;
        }
    }

    updateMetrics(brokers) {
        this.updateOverviewCards(brokers);
        this.updateBrokerTable(brokers);
        this.updateVersionInfo(brokers);
    }

    initializeChartsWithHistory(brokers) {
        // Clear existing chart data
        this.trafficChart.data.labels = [];
        this.trafficChart.data.datasets[0].data = [];
        this.trafficChart.data.datasets[1].data = [];
        this.trafficChart.data.datasets[2].data = [];
        this.trafficChart.data.datasets[3].data = [];
        this.trafficChart.data.datasets[4].data = [];
        this.trafficChart.data.datasets[5].data = [];

        console.log('Initializing charts with brokers:', brokers);

        // Aggregate all historical data from all brokers
        const allHistoricalData = [];

        brokers.forEach(broker => {
            console.log(`Broker ${broker.nodeId} metricsHistory:`, broker.metricsHistory);
            if (broker.metricsHistory && broker.metricsHistory.length > 0) {
                broker.metricsHistory.forEach(historicalPoint => {
                    console.log('Historical point:', historicalPoint);
                    allHistoricalData.push(historicalPoint);
                });
            }
        });

        console.log('All historical data:', allHistoricalData);

        // Sort by timestamp
        allHistoricalData.sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));

        // Aggregate data by timestamp (combine metrics from all brokers for each time point)
        const aggregatedData = {};
        allHistoricalData.forEach(point => {
            const timeKey = point.timestamp;
            if (!aggregatedData[timeKey]) {
                aggregatedData[timeKey] = {
                    messagesIn: 0,
                    messagesOut: 0,
                    messageBusIn: 0,
                    messageBusOut: 0,
                    mqttBridgeIn: 0,
                    mqttBridgeOut: 0,
                    opcUaIn: 0,
                    opcUaOut: 0
                };
            }
            aggregatedData[timeKey].messagesIn += point.messagesIn || 0;
            aggregatedData[timeKey].messagesOut += point.messagesOut || 0;
            aggregatedData[timeKey].messageBusIn += point.messageBusIn || 0;
            aggregatedData[timeKey].messageBusOut += point.messageBusOut || 0;
            aggregatedData[timeKey].mqttBridgeIn += point.mqttBridgeIn || 0;
            aggregatedData[timeKey].mqttBridgeOut += point.mqttBridgeOut || 0;
            aggregatedData[timeKey].opcUaIn += point.opcUaIn || 0;
            aggregatedData[timeKey].opcUaOut += point.opcUaOut || 0;
        });

        console.log('Aggregated data:', aggregatedData);

        // Populate charts with aggregated historical data
        Object.keys(aggregatedData).forEach(timestamp => {
            const data = aggregatedData[timestamp];
            const timeLabel = this.formatTimestamp(timestamp);

            console.log(`Adding chart data: ${timeLabel} - In: ${data.messagesIn}, Out: ${data.messagesOut}, BridgeIn: ${data.mqttBridgeIn}, BridgeOut: ${data.mqttBridgeOut}`);

            this.trafficChart.data.labels.push(timeLabel);
            this.trafficChart.data.datasets[0].data.push(data.messagesIn);
            this.trafficChart.data.datasets[1].data.push(data.messagesOut);
            this.trafficChart.data.datasets[2].data.push(data.mqttBridgeIn);
            this.trafficChart.data.datasets[3].data.push(data.mqttBridgeOut);
            this.trafficChart.data.datasets[4].data.push(data.opcUaIn);
            this.trafficChart.data.datasets[5].data.push(data.opcUaOut);
        });

        // Update message bus chart with current broker distribution
        this.messageBusChart.data.labels = brokers.map(b => b.nodeId);
        this.messageBusChart.data.datasets[0].data = brokers.map(b => {
            const metrics = b.metrics && b.metrics.length > 0 ? b.metrics[0] : {};
            return metrics.messageBusIn || 0;
        });
        this.messageBusChart.data.datasets[1].data = brokers.map(b => {
            const metrics = b.metrics && b.metrics.length > 0 ? b.metrics[0] : {};
            return metrics.messageBusOut || 0;
        });

        console.log('Chart datasets after population:');
        console.log('Messages In data:', this.trafficChart.data.datasets[0].data);
        console.log('Messages Out data:', this.trafficChart.data.datasets[1].data);

        this.trafficChart.update();
        this.messageBusChart.update();
    }

    addRealtimeDataPoint(brokers) {
        const now = new Date();
        const timeLabel = now.toLocaleTimeString();

        console.log('Adding realtime data point with brokers:', brokers);

        const clusterTotals = brokers.reduce((acc, broker) => {
            const metrics = broker.metrics && broker.metrics.length > 0 ? broker.metrics[0] : {};
            console.log(`Broker ${broker.nodeId} metrics:`, metrics);
            return {
                messagesIn: acc.messagesIn + (metrics.messagesIn || 0),
                messagesOut: acc.messagesOut + (metrics.messagesOut || 0),
                messageBusIn: acc.messageBusIn + (metrics.messageBusIn || 0),
                messageBusOut: acc.messageBusOut + (metrics.messageBusOut || 0),
                mqttBridgeIn: acc.mqttBridgeIn + (metrics.mqttBridgeIn || 0),
                mqttBridgeOut: acc.mqttBridgeOut + (metrics.mqttBridgeOut || 0),
                opcUaIn: acc.opcUaIn + (metrics.opcUaIn || 0),
                opcUaOut: acc.opcUaOut + (metrics.opcUaOut || 0)
            };
        }, { messagesIn: 0, messagesOut: 0, messageBusIn: 0, messageBusOut: 0, mqttBridgeIn: 0, mqttBridgeOut: 0, opcUaIn: 0, opcUaOut: 0 });

        console.log('Cluster totals for realtime update:', clusterTotals);

        // Remove oldest point if we have too many
        if (this.trafficChart.data.labels.length >= this.maxDataPoints) {
            this.trafficChart.data.labels.shift();
            this.trafficChart.data.datasets[0].data.shift();
            this.trafficChart.data.datasets[1].data.shift();
            this.trafficChart.data.datasets[2].data.shift();
            this.trafficChart.data.datasets[3].data.shift();
            this.trafficChart.data.datasets[4].data.shift();
            this.trafficChart.data.datasets[5].data.shift();
        }

        console.log(`Adding realtime point: ${timeLabel} - In: ${clusterTotals.messagesIn}, Out: ${clusterTotals.messagesOut}, BridgeIn: ${clusterTotals.mqttBridgeIn}, BridgeOut: ${clusterTotals.mqttBridgeOut}`);

        this.trafficChart.data.labels.push(timeLabel);
        this.trafficChart.data.datasets[0].data.push(clusterTotals.messagesIn);
        this.trafficChart.data.datasets[1].data.push(clusterTotals.messagesOut);
        this.trafficChart.data.datasets[2].data.push(clusterTotals.mqttBridgeIn);
        this.trafficChart.data.datasets[3].data.push(clusterTotals.mqttBridgeOut);
        this.trafficChart.data.datasets[4].data.push(clusterTotals.opcUaIn);
        this.trafficChart.data.datasets[5].data.push(clusterTotals.opcUaOut);
        this.trafficChart.update('none');

        // Update message bus chart with current data
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

    formatTimestamp(isoString) {
        const date = new Date(isoString);
        if (this.currentTimeframe === '24h') {
            return date.toLocaleString('en-US', {
                month: 'short',
                day: 'numeric',
                hour: '2-digit',
                minute: '2-digit'
            });
        } else if (this.currentTimeframe === '1h') {
            return date.toLocaleTimeString('en-US', {
                hour: '2-digit',
                minute: '2-digit'
            });
        } else {
            return date.toLocaleTimeString('en-US', {
                hour: '2-digit',
                minute: '2-digit',
                second: '2-digit'
            });
        }
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
                messageBusOut: acc.messageBusOut + (metrics.messageBusOut || 0),
                mqttBridgeIn: acc.mqttBridgeIn + (metrics.mqttBridgeIn || 0),
                mqttBridgeOut: acc.mqttBridgeOut + (metrics.mqttBridgeOut || 0),
                opcUaIn: acc.opcUaIn + (metrics.opcUaIn || 0),
                opcUaOut: acc.opcUaOut + (metrics.opcUaOut || 0)
            };
        }, {
            messagesIn: 0,
            messagesOut: 0,
            totalSessions: 0,
            queuedMessages: 0,
            messageBusIn: 0,
            messageBusOut: 0,
            mqttBridgeIn: 0,
            mqttBridgeOut: 0,
            opcUaIn: 0,
            opcUaOut: 0
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
                    <span class="metric-title">Bridge Messages In</span>
                    <div class="metric-icon">🛬</div>
                </div>
                <div class="metric-value">${this.formatNumber(clusterTotals.mqttBridgeIn)}</div>
                <div class="metric-label">External Bridges</div>
            </div>

            <div class="metric-card">
                <div class="metric-header">
                    <span class="metric-title">Bridge Messages Out</span>
                    <div class="metric-icon">🛫</div>
                </div>
                <div class="metric-value">${this.formatNumber(clusterTotals.mqttBridgeOut)}</div>
                <div class="metric-label">External Bridges</div>
            </div>

            <div class="metric-card">
                <div class="metric-header">
                    <span class="metric-title">OPC UA In</span>
                    <div class="metric-icon">⚙️</div>
                </div>
                <div class="metric-value">${this.formatNumber(clusterTotals.opcUaIn)}</div>
                <div class="metric-label">Industrial Protocol</div>
            </div>

            <div class="metric-card">
                <div class="metric-header">
                    <span class="metric-title">OPC UA Out</span>
                    <div class="metric-icon">⚙️</div>
                </div>
                <div class="metric-value">${this.formatNumber(clusterTotals.opcUaOut)}</div>
                <div class="metric-label">Industrial Protocol</div>
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
                    <td><code style="font-size: 0.875rem; background: var(--bg-secondary); padding: 2px 6px; border-radius: 4px;">${broker.version || 'unknown'}</code></td>
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
                    <td>
                        <span style="color: #0EA5E9;">${this.formatNumber(metrics.mqttBridgeIn || 0)}</span> /
                        <span style="color: #F59E0B;">${this.formatNumber(metrics.mqttBridgeOut || 0)}</span>
                    </td>
                </tr>
            `;
        }).join('');
    }

    updateVersionInfo(brokers) {
        const versionElement = document.getElementById('version-info');

        if (brokers && brokers.length > 0) {
            // Get the version from the first broker (assuming all nodes have the same version)
            const version = brokers[0].version || 'unknown';

            // Check if all brokers have the same version
            const allSameVersion = brokers.every(broker => broker.version === version);

            if (allSameVersion) {
                versionElement.innerHTML = `
                    <div style="color: var(--text-secondary);">MonsterMQ</div>
                    <div style="font-family: 'Courier New', monospace; color: var(--text-primary); font-weight: 500;">${version}</div>
                `;
            } else {
                // Mixed versions in cluster
                versionElement.innerHTML = `
                    <div style="color: var(--text-secondary);">MonsterMQ</div>
                    <div style="font-family: 'Courier New', monospace; color: #F59E0B; font-weight: 500;">Mixed Versions</div>
                `;
            }
        } else {
            versionElement.innerHTML = `
                <div style="color: var(--text-secondary);">MonsterMQ</div>
                <div style="font-family: 'Courier New', monospace; color: var(--text-tertiary); font-weight: 500;">unknown</div>
            `;
        }
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