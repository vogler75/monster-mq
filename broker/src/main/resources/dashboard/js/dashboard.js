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
        this.archiveLiveUpdatesEnabled = true;
        this.archiveCurrentTimeframe = '5m';

        this.init();
    }

    init() {
        if (!this.isLoggedIn()) {
            window.location.href = '/pages/login.html';
            return;
        }

        this.setupUI();
        this.setupCharts();
        this.setupArchiveGroupsControls();
        this.loadInitialDataWithHistory();
        this.startPolling();
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
        // Global traffic timeframe buttons
        document.querySelectorAll('.chart-controls button[data-timeframe]').forEach(btn => {
            btn.addEventListener('click', (e) => {
                document.querySelectorAll('.chart-controls button').forEach(b => b.classList.remove('active'));
                e.target.classList.add('active');
                this.currentTimeframe = e.target.dataset.timeframe;
                this.loadHistoricalDataForTimeframe();
            });
        });

        // Live updates toggle (traffic chart)
        document.getElementById('live-updates-toggle').addEventListener('change', (e) => {
            this.liveUpdatesEnabled = e.target.checked;
            console.log('Traffic live updates', this.liveUpdatesEnabled ? 'enabled' : 'disabled');
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
                        label: 'Msg/In',
                        data: [],
                        borderColor: '#22C55E',
                        backgroundColor: 'rgba(34, 197, 94, 0.12)',
                        tension: 0.35,
                        fill: true
                    },
                    {   // 1
                        label: 'Msg/Out',
                        data: [],
                        borderColor: '#6366F1',
                        backgroundColor: 'rgba(99, 102, 241, 0.12)',
                        tension: 0.35,
                        fill: true
                    },
                    {   // 2
                        label: 'MQTT/In',
                        data: [],
                        borderColor: '#0EA5E9',
                        backgroundColor: 'rgba(14, 165, 233, 0.12)',
                        tension: 0.35,
                        fill: true
                    },
                    {   // 3
                        label: 'MQTT/Out',
                        data: [],
                        borderColor: '#F59E0B',
                        backgroundColor: 'rgba(245, 158, 11, 0.12)',
                        tension: 0.35,
                        fill: true
                    },
                    {   // 4
                        label: 'Kafka/In',
                        data: [],
                        borderColor: '#EF4444',
                        backgroundColor: 'rgba(239, 68, 68, 0.12)',
                        tension: 0.35,
                        fill: true
                    },
                    {   // 5
                        label: 'OPC UA/In',
                        data: [],
                        borderColor: '#14B8A6',
                        backgroundColor: 'rgba(20, 184, 166, 0.12)',
                        tension: 0.35,
                        fill: true
                    },
                    {   // 6
                        label: 'OPC UA/Out',
                        data: [],
                        borderColor: '#9333EA',
                        backgroundColor: 'rgba(147, 51, 234, 0.12)',
                        tension: 0.35,
                        fill: true
                    },
                    {   // 7
                        label: 'WinCCOA/In',
                        data: [],
                        borderColor: '#EC4899',
                        backgroundColor: 'rgba(236, 72, 153, 0.12)',
                        tension: 0.35,
                        fill: true
                    },
                    {   // 8
                        label: 'WinCCUA/In',
                        data: [],
                        borderColor: '#A78BFA',
                        backgroundColor: 'rgba(167, 139, 250, 0.12)',
                        tension: 0.35,
                        fill: true
                    }
                ]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                aspectRatio: 2,
                spanGaps: true,
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

    setupArchiveGroupsControls() {
        // Archive groups timeframe buttons
        document.querySelectorAll('#archive-groups-controls button[data-arch-timeframe]').forEach(btn => {
            btn.addEventListener('click', (e) => {
                document.querySelectorAll('#archive-groups-controls button').forEach(b => b.classList.remove('active'));
                e.target.classList.add('active');
                this.archiveCurrentTimeframe = e.target.dataset.archTimeframe;
                this.loadArchiveGroupsHistorical();
            });
        });
        // Archive live updates toggle
        const liveToggle = document.getElementById('archive-live-updates-toggle');
        if (liveToggle) {
            liveToggle.addEventListener('change', (e) => {
                this.archiveLiveUpdatesEnabled = e.target.checked;
                console.log('Archive live updates', this.archiveLiveUpdatesEnabled ? 'enabled' : 'disabled');
            });
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
                if (this.archiveLiveUpdatesEnabled) {
                    this.addArchiveGroupsRealtimeDataPoint(archiveGroups);
                }
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

    async loadArchiveGroupsHistorical() {
        try {
            const minutes = this.getMinutesForTimeframe(this.archiveCurrentTimeframe);
            console.log(`Loading archive groups historical data for ${this.archiveCurrentTimeframe} (${minutes} minutes)`);
            const archiveGroups = await this.loadArchiveGroupsWithHistory(minutes);
            this.initializeArchiveGroupsChart(archiveGroups);
        } catch (error) {
            console.error('Failed to load archive groups historical data:', error);
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
                    mqttClientIn: 0, mqttClientOut: 0,
                    kafkaClientIn: 0,
                    opcUaClientIn: 0, opcUaClientOut: 0,
                    winCCOaClientIn: 0,
                    winCCUaClientIn: 0
                };
            }
            aggregatedData[timeKey].messagesIn += point.messagesIn || 0;
            aggregatedData[timeKey].messagesOut += point.messagesOut || 0;
            aggregatedData[timeKey].messageBusIn += point.messageBusIn || 0;
            aggregatedData[timeKey].messageBusOut += point.messageBusOut || 0;
            aggregatedData[timeKey].mqttClientIn += point.mqttClientIn || 0;
            aggregatedData[timeKey].mqttClientOut += point.mqttClientOut || 0;
            aggregatedData[timeKey].kafkaClientIn += point.kafkaClientIn || 0;
            // Removed kafkaClientOut (always zero / unused)
            aggregatedData[timeKey].opcUaClientIn += point.opcUaClientIn || 0;
            aggregatedData[timeKey].opcUaClientOut += point.opcUaClientOut || 0;
            aggregatedData[timeKey].winCCOaClientIn += point.winCCOaClientIn || 0;
            aggregatedData[timeKey].winCCUaClientIn += point.winCCUaClientIn || 0;
        });

        // Preserve chronological order by iterating sorted allHistoricalData sequence
        const orderedTimestamps = allHistoricalData.map(p => p.timestamp).filter((v, i, a) => a.indexOf(v) === i);
        orderedTimestamps.forEach(timestamp => {
            const data = aggregatedData[timestamp];
            if (!data) return; // safety guard
            const timeLabel = this.formatTimestampFor(this.currentTimeframe, timestamp);
            this.trafficChart.data.labels.push(timeLabel);
            this.trafficChart.data.datasets[0].data.push(data.messagesIn);
            this.trafficChart.data.datasets[1].data.push(data.messagesOut);
            this.trafficChart.data.datasets[2].data.push(data.mqttClientIn);
            this.trafficChart.data.datasets[3].data.push(data.mqttClientOut);
            this.trafficChart.data.datasets[4].data.push(data.kafkaClientIn);
            this.trafficChart.data.datasets[5].data.push(data.opcUaClientIn);
            this.trafficChart.data.datasets[6].data.push(data.opcUaClientOut);
            this.trafficChart.data.datasets[7].data.push(data.winCCOaClientIn ?? null);
            this.trafficChart.data.datasets[8].data.push(data.winCCUaClientIn ?? null);
        });

        console.debug('Traffic history loaded', {
            timeframe: this.currentTimeframe,
            points: this.trafficChart.data.labels.length,
            sampleLast5KafkaIn: this.trafficChart.data.datasets[4].data.slice(-5),
            sampleLast5OpcUaIn: this.trafficChart.data.datasets[5].data.slice(-5)
        });

        this.messageBusChart.data.labels = brokers.map(b => b.nodeId);
        this.messageBusChart.data.datasets[0].data = brokers.map(b => (b.metrics && b.metrics.length > 0 ? b.metrics[0].messageBusIn : 0) || 0);
        this.messageBusChart.data.datasets[1].data = brokers.map(b => (b.metrics && b.metrics.length > 0 ? b.metrics[0].messageBusOut : 0) || 0);

        if (this.trafficChart.data.labels.length === 0) {
            this.trafficChart.data.labels = ['No Data'];
            this.trafficChart.data.datasets.forEach(ds => ds.data = [0]);
        }

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
                mqttClientIn: acc.mqttClientIn + (metrics.mqttClientIn || 0),
                mqttClientOut: acc.mqttClientOut + (metrics.mqttClientOut || 0),
                kafkaClientIn: acc.kafkaClientIn + (metrics.kafkaClientIn || 0),
                // kafkaClientOut removed
                opcUaClientIn: acc.opcUaClientIn + (metrics.opcUaClientIn || 0),
                opcUaClientOut: acc.opcUaClientOut + (metrics.opcUaClientOut || 0),
                winCCOaClientIn: acc.winCCOaClientIn + (metrics.winCCOaClientIn || 0),
                winCCUaClientIn: acc.winCCUaClientIn + (metrics.winCCUaClientIn || 0)
            };
        }, { messagesIn: 0, messagesOut: 0, messageBusIn: 0, messageBusOut: 0, mqttClientIn: 0, mqttClientOut: 0, kafkaClientIn: 0, opcUaClientIn: 0, opcUaClientOut: 0, winCCOaClientIn: 0, winCCUaClientIn: 0 });

        if (this.trafficChart.data.labels.length >= this.maxDataPoints) {
            this.trafficChart.data.labels.shift();
            for (let i = 0; i < this.trafficChart.data.datasets.length; i++) {
                this.trafficChart.data.datasets[i].data.shift();
            }
        }

        this.trafficChart.data.labels.push(timeLabel);
        this.trafficChart.data.datasets[0].data.push(clusterTotals.messagesIn);
        this.trafficChart.data.datasets[1].data.push(clusterTotals.messagesOut);
        this.trafficChart.data.datasets[2].data.push(clusterTotals.mqttClientIn);
        this.trafficChart.data.datasets[3].data.push(clusterTotals.mqttClientOut);
        this.trafficChart.data.datasets[4].data.push(clusterTotals.kafkaClientIn);
        // Removed kafkaClientOut dataset push
        this.trafficChart.data.datasets[5].data.push(clusterTotals.opcUaClientIn);
        this.trafficChart.data.datasets[6].data.push(clusterTotals.opcUaClientOut);
        this.trafficChart.data.datasets[7].data.push(clusterTotals.winCCOaClientIn ?? null);
        this.trafficChart.data.datasets[8].data.push(clusterTotals.winCCUaClientIn ?? null);
        this.trafficChart.update('none');

        this.messageBusChart.data.labels = brokers.map(b => b.nodeId);
        this.messageBusChart.data.datasets[0].data = brokers.map(b => (b.metrics && b.metrics.length > 0 ? b.metrics[0].messageBusIn : 0) || 0);
        this.messageBusChart.data.datasets[1].data = brokers.map(b => (b.metrics && b.metrics.length > 0 ? b.metrics[0].messageBusOut : 0) || 0);
        this.messageBusChart.update('none');
    }

    formatTimestampFor(timeframe, isoString) {
        const date = new Date(isoString);
        if (timeframe === '24h') {
            return date.toLocaleString('en-US', { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' });
        } else if (timeframe === '1h') {
            return date.toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit' });
        } else {
            return date.toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit', second: '2-digit' });
        }
    }

    // Backward compatibility (legacy calls)
    formatTimestamp(isoString) { return this.formatTimestampFor(this.currentTimeframe, isoString); }

    async updateOverviewCards(brokers) {
        const clusterTotals = brokers.reduce((acc, broker) => {
            const metrics = broker.metrics && broker.metrics.length > 0 ? broker.metrics[0] : {};
            return {
                messagesIn: acc.messagesIn + (metrics.messagesIn || 0),
                messagesOut: acc.messagesOut + (metrics.messagesOut || 0),
                totalSessions: acc.totalSessions + (metrics.nodeSessionCount || 0),
                queuedMessages: acc.queuedMessages + (metrics.queuedMessagesCount || 0),
                messageBusIn: acc.messageBusIn + (metrics.messageBusIn || 0),
                messageBusOut: acc.messageBusOut + (metrics.messageBusOut || 0),
                mqttClientIn: acc.mqttClientIn + (metrics.mqttClientIn || 0),
                mqttClientOut: acc.mqttClientOut + (metrics.mqttClientOut || 0),
                kafkaClientIn: acc.kafkaClientIn + (metrics.kafkaClientIn || 0),
                // kafkaClientOut removed
                opcUaClientIn: acc.opcUaClientIn + (metrics.opcUaClientIn || 0),
                opcUaClientOut: acc.opcUaClientOut + (metrics.opcUaClientOut || 0),
                winCCOaClientIn: acc.winCCOaClientIn + (metrics.winCCOaClientIn || 0),
                winCCUaClientIn: acc.winCCUaClientIn + (metrics.winCCUaClientIn || 0)
            };
        }, {
            messagesIn: 0, messagesOut: 0, totalSessions: 0, queuedMessages: 0,
            messageBusIn: 0, messageBusOut: 0,
            mqttClientIn: 0, mqttClientOut: 0,
            kafkaClientIn: 0, // kafkaClientOut removed
            opcUaClientIn: 0, opcUaClientOut: 0,
            winCCOaClientIn: 0,
            winCCUaClientIn: 0
        });

        // Fetch MQTT v5 statistics
        let mqtt5Stats = null;
        try {
            const result = await window.graphqlClient.query(`
                query {
                    mqtt5Statistics {
                        totalSessions
                        mqtt5Sessions
                        mqtt3Sessions
                        mqtt5Percentage
                    }
                }
            `);
            mqtt5Stats = result.mqtt5Statistics;
        } catch (error) {
            console.error('Failed to fetch MQTT v5 statistics:', error);
        }

        const overviewContainer = document.getElementById('cluster-overview');
        const mqtt5Card = mqtt5Stats ? `
            <div class="metric-card">
                <div class="metric-header"><span class="metric-title">MQTT v5.0 Clients</span><div class="metric-icon">🔄</div></div>
                <div class="metric-value">${mqtt5Stats.mqtt5Sessions} <span style="font-size: 0.7em; color: var(--text-secondary);">/ ${mqtt5Stats.totalSessions}</span></div>
                <div class="metric-label">${mqtt5Stats.mqtt5Percentage.toFixed(1)}% Adoption</div>
            </div>` : '';

        overviewContainer.innerHTML = `
            <div class="metric-card"><div class="metric-header"><span class="metric-title">Active Sessions</span><div class="metric-icon">👥</div></div><div class="metric-value">${clusterTotals.totalSessions}</div><div class="metric-label">${brokers.length} Node${brokers.length !== 1 ? 's' : ''}</div></div>
            <div class="metric-card"><div class="metric-header"><span class="metric-title">Queued Messages</span><div class="metric-icon">⏳</div></div><div class="metric-value">${this.formatNumber(clusterTotals.queuedMessages)}</div><div class="metric-label">Pending Delivery</div></div>
            ${mqtt5Card}
            <div class="metric-card">
                <div class="metric-header"><span class="metric-title">MQTT Messages</span><div class="metric-icon">📨</div></div>
                <div class="metric-value"><span style="color: #22C55E;">${this.formatNumber(clusterTotals.messagesIn)}</span> / <span style="color: #6366F1;">${this.formatNumber(clusterTotals.messagesOut)}</span></div>
                <div class="metric-label">In / Out</div>
            </div>
            <div class="metric-card">
                <div class="metric-header"><span class="metric-title">MQTT Bridges</span><div class="metric-icon">🌉</div></div>
                <div class="metric-value"><span style="color: #0EA5E9;">${this.formatNumber(clusterTotals.mqttClientIn)}</span> / <span style="color: #F59E0B;">${this.formatNumber(clusterTotals.mqttClientOut)}</span></div>
                <div class="metric-label">In / Out</div>
            </div>
            <div class="metric-card">
                <div class="metric-header"><span class="metric-title">Kafka Bridges</span><div class="metric-icon">📦</div></div>
                <div class="metric-value">${this.formatNumber(clusterTotals.kafkaClientIn)}</div>
                <div class="metric-label">In</div>
            </div>
            <div class="metric-card">
                <div class="metric-header"><span class="metric-title">OPC UA Clients</span><div class="metric-icon">⚙️</div></div>
                <div class="metric-value"><span style="color: #14B8A6;">${this.formatNumber(clusterTotals.opcUaClientIn)}</span> / <span style="color: #9333EA;">${this.formatNumber(clusterTotals.opcUaClientOut)}</span></div>
                <div class="metric-label">In / Out</div>
            </div>
            <div class="metric-card">
                <div class="metric-header"><span class="metric-title">WinCC OA Clients</span><div class="metric-icon">🏭</div></div>
                <div class="metric-value">${this.formatNumber(clusterTotals.winCCOaClientIn || 0)}</div>
                <div class="metric-label">In</div>
            </div>
            <div class="metric-card">
                <div class="metric-header"><span class="metric-title">WinCC UA Clients</span><div class="metric-icon">🔧</div></div>
                <div class="metric-value">${this.formatNumber(clusterTotals.winCCUaClientIn || 0)}</div>
                <div class="metric-label">In</div>
            </div>`;
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
                    <td><span style="color: #0EA5E9;">${this.formatNumber(metrics.mqttClientIn || 0)}</span> / <span style="color: #F59E0B;">${this.formatNumber(metrics.mqttClientOut || 0)}</span></td>
                    <td><span style="color: #EF4444;">${this.formatNumber(metrics.kafkaClientIn || 0)}</span></td>
                    <td><span style="color: #14B8A6;">${this.formatNumber(metrics.opcUaClientIn || 0)}</span> / <span style="color: #9333EA;">${this.formatNumber(metrics.opcUaClientOut || 0)}</span></td>
                    <td><span style="color: #EC4899;">${this.formatNumber(metrics.winCCOaClientIn || 0)}</span></td>
                    <td><span style="color: #A78BFA;">${this.formatNumber(metrics.winCCUaClientIn || 0)}</span></td>
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
        const colors = ['#10B981', '#3B82F6', '#F59E0B', '#EF4444', '#8B5CF6', '#14B8A6', '#06B6D4', '#F97316', '#EC4899', '#6366F1'];

        // Build dataset shells for enabled groups with any history
        const enabledWithHistory = archiveGroups.filter(g => g.enabled && g.metricsHistory && g.metricsHistory.length > 0);
        enabledWithHistory.forEach((group, index) => {
            const color = colors[index % colors.length];
            this.archiveGroupsChart.data.datasets.push({ label: group.name, data: [], borderColor: color, backgroundColor: color + '20', fill: false, tension: 0.4, spanGaps: true });
        });

        if (enabledWithHistory.length === 0) {
            // No data available
            this.archiveGroupsChart.data.labels = ['No Data'];
            this.archiveGroupsChart.update();
            console.debug('Archive groups history loaded', {
                timeframe: timeframe,
                labels: this.archiveGroupsChart.data.labels.length,
                datasets: this.archiveGroupsChart.data.datasets.map(d => ({ label: d.label, points: d.data.length, last5: d.data.slice(-5) }))
            });
            return;
        }

        // Collect unique timestamps across all enabled groups
        const timestampSet = new Set();
        enabledWithHistory.forEach(group => group.metricsHistory.forEach(m => timestampSet.add(m.timestamp)));
        const sortedTimestamps = Array.from(timestampSet).sort((a, b) => new Date(a) - new Date(b));

        const timeframe = this.archiveCurrentTimeframe || this.currentTimeframe;
        this.archiveGroupsChart.data.labels = sortedTimestamps.map(ts => this.formatTimestampFor(timeframe, ts));

        // Map each group's history by timestamp for quick lookup
        enabledWithHistory.forEach(group => {
            const historyMap = {};
            group.metricsHistory.forEach(m => { historyMap[m.timestamp] = m.messagesOut || 0; });
            const dsIndex = this.archiveGroupsChart.data.datasets.findIndex(ds => ds.label === group.name);
            if (dsIndex >= 0) {
                this.archiveGroupsChart.data.datasets[dsIndex].data = sortedTimestamps.map(ts => historyMap[ts] !== undefined ? historyMap[ts] : null);
            }
        });

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
                const colors = ['#10B981', '#3B82F6', '#F59E0B', '#EF4444', '#8B5CF6', '#14B8A6', '#06B6D4', '#F97316', '#EC4899', '#6366F1'];
                const color = colors[this.archiveGroupsChart.data.datasets.length % colors.length];
                this.archiveGroupsChart.data.datasets.push({ label: group.name, data: [currentMetric.messagesOut || 0], borderColor: color, backgroundColor: color + '20', fill: false, tension: 0.4, spanGaps: true });
            }
        });
        this.archiveGroupsChart.update('none');
    }
}

document.addEventListener('DOMContentLoaded', () => { new DashboardManager(); });