class DashboardManager {
    constructor() {
        this.pollingInterval = null;
        this.trafficChart = null;
        this.messageBusChart = null;
        this.archiveGroupsChart = null;
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
        const archiveGroupsCtx = document.getElementById('archiveGroupsChart').getContext('2d');

        Chart.defaults.color = '#CBD5E1';
        Chart.defaults.borderColor = '#475569';

        this.trafficChart = new Chart(trafficCtx, {
            type: 'line',
            data: {
                labels: [],
                datasets: [
                    {   // 0
                        label: 'Messages In',
                        data: [],
                        borderColor: '#10B981',
                        backgroundColor: 'rgba(16, 185, 129, 0.1)',
                        tension: 0.4,
                        fill: true
                    },
                    {   // 1
                        label: 'Messages Out',
                        data: [],
                        borderColor: '#7C3AED',
                        backgroundColor: 'rgba(124, 58, 237, 0.1)',
                        tension: 0.4,
                        fill: true
                    },
                    {   // 2
                        label: 'MQTT Bridge In',
                        data: [],
                        borderColor: '#0EA5E9',
                        backgroundColor: 'rgba(14, 165, 233, 0.1)',
                        tension: 0.4,
                        fill: true
                    },
                    {   // 3
                        label: 'MQTT Bridge Out',
                        data: [],
                        borderColor: '#F59E0B',
                        backgroundColor: 'rgba(245, 158, 11, 0.1)',
                        tension: 0.4,
                        fill: true
                    },
                    {   // 4
                        label: 'Kafka Bridge In',
                        data: [],
                        borderColor: '#DC2626',
                        backgroundColor: 'rgba(220, 38, 38, 0.1)',
                        tension: 0.4,
                        fill: true
                    },
                    {   // 5
                        label: 'Kafka Bridge Out',
                        data: [],
                        borderColor: '#F43F5E',
                        backgroundColor: 'rgba(244, 63, 94, 0.1)',
                        tension: 0.4,
                        fill: true
                    },
                    {   // 6
                        label: 'OPC UA In',
                        data: [],
                        borderColor: '#06B6D4',
                        backgroundColor: 'rgba(6, 182, 212, 0.1)',
                        tension: 0.4,
                        fill: true
                    },
                    {   // 7
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
                    legend: { position: 'top' }
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
                    y: { beginAtZero: true, grid: { color: '#334155' } },
                    x: { grid: { color: '#334155' } }
                },
                plugins: { legend: { position: 'top' } }
            }
        });

        this.archiveGroupsChart = new Chart(archiveGroupsCtx, {
            type: 'line',
            data: { labels: [], datasets: [] }, // populated dynamically
            options: {
                responsive: true,
                maintainAspectRatio: false,
                aspectRatio: 2,
                scales: {
                    y: {
                        beginAtZero: true,
                        grid: { color: '#334155' },
                        title: { display: true, text: 'Messages/sec' }
                    },
                    x: { grid: { color: '#334155' } }
                },
                plugins: { legend: { position: 'top' } }
            }
        });
    }

    startPolling() {
        console.log('Starting metrics polling every', this.pollingIntervalMs, 'ms');
        this.pollingInterval = setInterval(() => { this.loadRealtimeUpdate(); }, this.pollingIntervalMs);
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
            await this.loadHistoricalDataForTimeframe();
            this.historicalDataLoaded = true;
        } catch (error) {
            console.error('Failed to load initial data with history:', error);
        }
    }

    async loadRealtimeUpdate() {
        try {
            if (!this.liveUpdatesEnabled) return;
            const brokers = await window.graphqlClient.getBrokers();
            this.updateMetrics(brokers);
            const archiveGroups = await this.loadArchiveGroups();
            if (this.historicalDataLoaded) {
                this.addRealtimeDataPoint(brokers);
                this.addArchiveGroupsRealtimeDataPoint(archiveGroups);
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
            const archiveGroups = await this.loadArchiveGroupsWithHistory(minutes);
            this.updateMetrics(brokers);
            this.initializeChartsWithHistory(brokers);
            this.initializeArchiveGroupsChart(archiveGroups);
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
        this.trafficChart.data.labels = [];
        for (let i = 0; i < this.trafficChart.data.datasets.length; i++) {
            this.trafficChart.data.datasets[i].data = [];
        }

        const allHistoricalData = [];
        brokers.forEach(broker => {
            if (broker.metricsHistory && broker.metricsHistory.length > 0) {
                broker.metricsHistory.forEach(h => allHistoricalData.push(h));
            }
        });
        allHistoricalData.sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));

        const aggregatedData = {};
        allHistoricalData.forEach(point => {
            const timeKey = point.timestamp;
            if (!aggregatedData[timeKey]) {
                aggregatedData[timeKey] = {
                    messagesIn: 0, messagesOut: 0,
                    messageBusIn: 0, messageBusOut: 0,
                    mqttBridgeIn: 0, mqttBridgeOut: 0,
                    kafkaBridgeIn: 0, kafkaBridgeOut: 0,
                    opcUaIn: 0, opcUaOut: 0
                };
            }
            aggregatedData[timeKey].messagesIn += point.messagesIn || 0;
            aggregatedData[timeKey].messagesOut += point.messagesOut || 0;
            aggregatedData[timeKey].messageBusIn += point.messageBusIn || 0;
            aggregatedData[timeKey].messageBusOut += point.messageBusOut || 0;
            aggregatedData[timeKey].mqttBridgeIn += point.mqttBridgeIn || 0;
            aggregatedData[timeKey].mqttBridgeOut += point.mqttBridgeOut || 0;
            aggregatedData[timeKey].kafkaBridgeIn += point.kafkaBridgeIn || 0;
            aggregatedData[timeKey].kafkaBridgeOut += point.kafkaBridgeOut || 0;
            aggregatedData[timeKey].opcUaIn += point.opcUaIn || 0;
            aggregatedData[timeKey].opcUaOut += point.opcUaOut || 0;
        });

        Object.keys(aggregatedData).forEach(timestamp => {
            const data = aggregatedData[timestamp];
            const timeLabel = this.formatTimestamp(timestamp);
            this.trafficChart.data.labels.push(timeLabel);
            this.trafficChart.data.datasets[0].data.push(data.messagesIn);
            this.trafficChart.data.datasets[1].data.push(data.messagesOut);
            this.trafficChart.data.datasets[2].data.push(data.mqttBridgeIn);
            this.trafficChart.data.datasets[3].data.push(data.mqttBridgeOut);
            this.trafficChart.data.datasets[4].data.push(data.kafkaBridgeIn);
            this.trafficChart.data.datasets[5].data.push(data.kafkaBridgeOut);
            this.trafficChart.data.datasets[6].data.push(data.opcUaIn);
            this.trafficChart.data.datasets[7].data.push(data.opcUaOut);
        });

        this.messageBusChart.data.labels = brokers.map(b => b.nodeId);
        this.messageBusChart.data.datasets[0].data = brokers.map(b => (b.metrics && b.metrics.length > 0 ? b.metrics[0].messageBusIn : 0) || 0);
        this.messageBusChart.data.datasets[1].data = brokers.map(b => (b.metrics && b.metrics.length > 0 ? b.metrics[0].messageBusOut : 0) || 0);

        this.trafficChart.update();
        this.messageBusChart.update();
    }

    addRealtimeDataPoint(brokers) {
        const now = new Date();
        const timeLabel = now.toLocaleTimeString();

        const clusterTotals = brokers.reduce((acc, broker) => {
            const metrics = broker.metrics && broker.metrics.length > 0 ? broker.metrics[0] : {};
            return {
                messagesIn: acc.messagesIn + (metrics.messagesIn || 0),
                messagesOut: acc.messagesOut + (metrics.messagesOut || 0),
                messageBusIn: acc.messageBusIn + (metrics.messageBusIn || 0),
                messageBusOut: acc.messageBusOut + (metrics.messageBusOut || 0),
                mqttBridgeIn: acc.mqttBridgeIn + (metrics.mqttBridgeIn || 0),
                mqttBridgeOut: acc.mqttBridgeOut + (metrics.mqttBridgeOut || 0),
                kafkaBridgeIn: acc.kafkaBridgeIn + (metrics.kafkaBridgeIn || 0),
                kafkaBridgeOut: acc.kafkaBridgeOut + (metrics.kafkaBridgeOut || 0),
                opcUaIn: acc.opcUaIn + (metrics.opcUaIn || 0),
                opcUaOut: acc.opcUaOut + (metrics.opcUaOut || 0)
            };
        }, { messagesIn: 0, messagesOut: 0, messageBusIn: 0, messageBusOut: 0, mqttBridgeIn: 0, mqttBridgeOut: 0, kafkaBridgeIn: 0, kafkaBridgeOut: 0, opcUaIn: 0, opcUaOut: 0 });

        if (this.trafficChart.data.labels.length >= this.maxDataPoints) {
            this.trafficChart.data.labels.shift();
            for (let i = 0; i < this.trafficChart.data.datasets.length; i++) {
                this.trafficChart.data.datasets[i].data.shift();
            }
        }

        this.trafficChart.data.labels.push(timeLabel);
        this.trafficChart.data.datasets[0].data.push(clusterTotals.messagesIn);
        this.trafficChart.data.datasets[1].data.push(clusterTotals.messagesOut);
        this.trafficChart.data.datasets[2].data.push(clusterTotals.mqttBridgeIn);
        this.trafficChart.data.datasets[3].data.push(clusterTotals.mqttBridgeOut);
        this.trafficChart.data.datasets[4].data.push(clusterTotals.kafkaBridgeIn);
        this.trafficChart.data.datasets[5].data.push(clusterTotals.kafkaBridgeOut);
        this.trafficChart.data.datasets[6].data.push(clusterTotals.opcUaIn);
        this.trafficChart.data.datasets[7].data.push(clusterTotals.opcUaOut);
        this.trafficChart.update('none');

        this.messageBusChart.data.labels = brokers.map(b => b.nodeId);
        this.messageBusChart.data.datasets[0].data = brokers.map(b => (b.metrics && b.metrics.length > 0 ? b.metrics[0].messageBusIn : 0) || 0);
        this.messageBusChart.data.datasets[1].data = brokers.map(b => (b.metrics && b.metrics.length > 0 ? b.metrics[0].messageBusOut : 0) || 0);
        this.messageBusChart.update('none');
    }

    formatTimestamp(isoString) {
        const date = new Date(isoString);
        if (this.currentTimeframe === '24h') {
            return date.toLocaleString('en-US', { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' });
        } else if (this.currentTimeframe === '1h') {
            return date.toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit' });
        } else {
            return date.toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit', second: '2-digit' });
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
                kafkaBridgeIn: acc.kafkaBridgeIn + (metrics.kafkaBridgeIn || 0),
                kafkaBridgeOut: acc.kafkaBridgeOut + (metrics.kafkaBridgeOut || 0),
                opcUaIn: acc.opcUaIn + (metrics.opcUaIn || 0),
                opcUaOut: acc.opcUaOut + (metrics.opcUaOut || 0)
            };
        }, {
            messagesIn: 0, messagesOut: 0, totalSessions: 0, queuedMessages: 0,
            messageBusIn: 0, messageBusOut: 0,
            mqttBridgeIn: 0, mqttBridgeOut: 0,
            kafkaBridgeIn: 0, kafkaBridgeOut: 0,
            opcUaIn: 0, opcUaOut: 0
        });

        const overviewContainer = document.getElementById('cluster-overview');
        overviewContainer.innerHTML = `
            <div class="metric-card"><div class="metric-header"><span class="metric-title">MQTT Messages In</span><div class="metric-icon">📥</div></div><div class="metric-value">${this.formatNumber(clusterTotals.messagesIn)}</div><div class="metric-label">Cluster Total</div></div>
            <div class="metric-card"><div class="metric-header"><span class="metric-title">MQTT Messages Out</span><div class="metric-icon">📤</div></div><div class="metric-value">${this.formatNumber(clusterTotals.messagesOut)}</div><div class="metric-label">Cluster Total</div></div>
            <div class="metric-card"><div class="metric-header"><span class="metric-title">Active Sessions</span><div class="metric-icon">👥</div></div><div class="metric-value">${clusterTotals.totalSessions}</div><div class="metric-label">${brokers.length} Node${brokers.length !== 1 ? 's' : ''}</div></div>
            <div class="metric-card"><div class="metric-header"><span class="metric-title">Queued Messages</span><div class="metric-icon">⏳</div></div><div class="metric-value">${this.formatNumber(clusterTotals.queuedMessages)}</div><div class="metric-label">Pending Delivery</div></div>
            <div class="metric-card"><div class="metric-header"><span class="metric-title">MQTT Bridge In</span><div class="metric-icon">🛬</div></div><div class="metric-value">${this.formatNumber(clusterTotals.mqttBridgeIn)}</div><div class="metric-label">External Bridges</div></div>
            <div class="metric-card"><div class="metric-header"><span class="metric-title">MQTT Bridge Out</span><div class="metric-icon">🛫</div></div><div class="metric-value">${this.formatNumber(clusterTotals.mqttBridgeOut)}</div><div class="metric-label">External Bridges</div></div>
            <div class="metric-card"><div class="metric-header"><span class="metric-title">Kafka Bridge In</span><div class="metric-icon">📦</div></div><div class="metric-value">${this.formatNumber(clusterTotals.kafkaBridgeIn)}</div><div class="metric-label">Kafka</div></div>
            <div class="metric-card"><div class="metric-header"><span class="metric-title">Kafka Bridge Out</span><div class="metric-icon">📦</div></div><div class="metric-value">${this.formatNumber(clusterTotals.kafkaBridgeOut)}</div><div class="metric-label">Kafka</div></div>
            <div class="metric-card"><div class="metric-header"><span class="metric-title">OPC UA In</span><div class="metric-icon">⚙️</div></div><div class="metric-value">${this.formatNumber(clusterTotals.opcUaIn)}</div><div class="metric-label">Industrial Protocol</div></div>
            <div class="metric-card"><div class="metric-header"><span class="metric-title">OPC UA Out</span><div class="metric-icon">⚙️</div></div><div class="metric-value">${this.formatNumber(clusterTotals.opcUaOut)}</div><div class="metric-label">Industrial Protocol</div></div>`;
    }

    updateBrokerTable(brokers) {
        const tableBody = document.getElementById('broker-table-body');
        tableBody.innerHTML = brokers.map(broker => {
            const metrics = broker.metrics && broker.metrics.length > 0 ? broker.metrics[0] : {};
            const isHealthy = metrics.nodeSessionCount >= 0;
            return `
                <tr>
                    <td><strong>${broker.nodeId}</strong></td>
                    <td><code style="font-size: 0.875rem; background: var(--bg-secondary); padding: 2px 6px; border-radius: 4px;">${broker.version || 'unknown'}</code></td>
                    <td><span class="status-indicator ${isHealthy ? 'status-online' : 'status-offline'}"><span class="status-dot"></span>${isHealthy ? 'Online' : 'Offline'}</span></td>
                    <td>${this.formatNumber(metrics.messagesIn || 0)}</td>
                    <td>${this.formatNumber(metrics.messagesOut || 0)}</td>
                    <td>${metrics.nodeSessionCount || 0}</td>
                    <td>${this.formatNumber(metrics.queuedMessagesCount || 0)}</td>
                    <td><span style="color: #14B8A6;">${this.formatNumber(metrics.messageBusIn || 0)}</span> / <span style="color: #F97316;">${this.formatNumber(metrics.messageBusOut || 0)}</span></td>
                    <td><span style="color: #0EA5E9;">${this.formatNumber(metrics.mqttBridgeIn || 0)}</span> / <span style="color: #F59E0B;">${this.formatNumber(metrics.mqttBridgeOut || 0)}</span></td>
                    <td><span style="color: #DC2626;">${this.formatNumber(metrics.kafkaBridgeIn || 0)}</span> / <span style="color: #F43F5E;">${this.formatNumber(metrics.kafkaBridgeOut || 0)}</span></td>
                </tr>`;
        }).join('');
    }

    updateVersionInfo(brokers) {
        const versionElement = document.getElementById('version-info');
        if (brokers && brokers.length > 0) {
            const version = brokers[0].version || 'unknown';
            const allSameVersion = brokers.every(b => b.version === version);
            if (allSameVersion) {
                versionElement.innerHTML = `<div style="color: var(--text-secondary);">MonsterMQ</div><div style="font-family: 'Courier New', monospace; color: var(--text-primary); font-weight: 500;">${version}</div>`;
            } else {
                versionElement.innerHTML = `<div style="color: var(--text-secondary);">MonsterMQ</div><div style=\"font-family: 'Courier New', monospace; color: #F59E0B; font-weight: 500;\">Mixed Versions</div>`;
            }
        } else {
            versionElement.innerHTML = `<div style="color: var(--text-secondary);">MonsterMQ</div><div style="font-family: 'Courier New', monospace; color: var(--text-tertiary); font-weight: 500;">unknown</div>`;
        }
    }

    formatNumber(num) {
        if (num >= 1000000) return (num / 1000000).toFixed(1) + 'M';
        if (num >= 1000) return (num / 1000).toFixed(1) + 'K';
        return num.toString();
    }

    async loadArchiveGroups() {
        try {
            const result = await window.graphqlClient.query(`
                query GetArchiveGroups { archiveGroups { name enabled metrics { messagesOut bufferSize timestamp } } }`);
            return result.archiveGroups || [];
        } catch (error) {
            console.error('Failed to load archive groups:', error);
            return [];
        }
    }

    async loadArchiveGroupsWithHistory(minutes) {
        try {
            const result = await window.graphqlClient.query(`
                query GetArchiveGroupsWithHistory($lastMinutes: Int) { archiveGroups { name enabled metricsHistory(lastMinutes: $lastMinutes) { messagesOut bufferSize timestamp } } }`, { lastMinutes: minutes });
            return result.archiveGroups || [];
        } catch (error) {
            console.error('Failed to load archive groups with history:', error);
            return [];
        }
    }

    initializeArchiveGroupsChart(archiveGroups) {
        this.archiveGroupsChart.data.labels = [];
        this.archiveGroupsChart.data.datasets = [];
        const colors = ['#10B981','#3B82F6','#F59E0B','#EF4444','#8B5CF6','#14B8A6','#06B6D4','#F97316','#EC4899','#6366F1'];
        archiveGroups.forEach((group, index) => {
            if (!group.enabled || !group.metricsHistory || group.metricsHistory.length === 0) return;
            const color = colors[index % colors.length];
            this.archiveGroupsChart.data.datasets.push({ label: group.name, data: [], borderColor: color, backgroundColor: color + '20', fill: false, tension: 0.4 });
        });
        if (archiveGroups.length > 0 && archiveGroups[0].metricsHistory) {
            const timestamps = archiveGroups[0].metricsHistory.map(m => this.formatTimestamp(m.timestamp));
            this.archiveGroupsChart.data.labels = timestamps;
            archiveGroups.forEach(group => {
                if (!group.enabled || !group.metricsHistory) return;
                const idx = this.archiveGroupsChart.data.datasets.findIndex(ds => ds.label === group.name);
                if (idx >= 0) this.archiveGroupsChart.data.datasets[idx].data = group.metricsHistory.map(m => m.messagesOut || 0);
            });
        }
        this.archiveGroupsChart.update();
    }

    addArchiveGroupsRealtimeDataPoint(archiveGroups) {
        const now = new Date();
        const timeLabel = now.toLocaleTimeString();
        this.archiveGroupsChart.data.labels.push(timeLabel);
        if (this.archiveGroupsChart.data.labels.length > this.maxDataPoints) this.archiveGroupsChart.data.labels.shift();
        archiveGroups.forEach(group => {
            if (!group.enabled) return;
            const idx = this.archiveGroupsChart.data.datasets.findIndex(ds => ds.label === group.name);
            const currentMetric = group.metrics && group.metrics.length > 0 ? group.metrics[0] : { messagesOut: 0 };
            if (idx >= 0) {
                this.archiveGroupsChart.data.datasets[idx].data.push(currentMetric.messagesOut || 0);
                if (this.archiveGroupsChart.data.datasets[idx].data.length > this.maxDataPoints) this.archiveGroupsChart.data.datasets[idx].data.shift();
            } else {
                const colors = ['#10B981','#3B82F6','#F59E0B','#EF4444','#8B5CF6','#14B8A6','#06B6D4','#F97316','#EC4899','#6366F1'];
                const color = colors[this.archiveGroupsChart.data.datasets.length % colors.length];
                this.archiveGroupsChart.data.datasets.push({ label: group.name, data: [currentMetric.messagesOut || 0], borderColor: color, backgroundColor: color + '20', fill: false, tension: 0.4 });
            }
        });
        this.archiveGroupsChart.update('none');
    }
}

document.addEventListener('DOMContentLoaded', () => { new DashboardManager(); });