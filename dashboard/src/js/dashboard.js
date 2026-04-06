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
        return window.isLoggedIn();
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
                    {   // 0 - MQTT Messages total (in+out)
                        label: 'MQTT Messages',
                        data: [],
                        borderColor: '#22C55E',
                        backgroundColor: 'rgba(34, 197, 94, 0.12)',
                        tension: 0.35,
                        fill: true
                    },
                    {   // 1 - MQTT Bridges total (in+out)
                        label: 'MQTT Bridges',
                        data: [],
                        borderColor: '#0EA5E9',
                        backgroundColor: 'rgba(14, 165, 233, 0.12)',
                        tension: 0.35,
                        fill: true
                    },
                    {   // 2 - Kafka total
                        label: 'Kafka',
                        data: [],
                        borderColor: '#EF4444',
                        backgroundColor: 'rgba(239, 68, 68, 0.12)',
                        tension: 0.35,
                        fill: true
                    },
                    {   // 3 - OPC UA total (in+out)
                        label: 'OPC UA',
                        data: [],
                        borderColor: '#14B8A6',
                        backgroundColor: 'rgba(20, 184, 166, 0.12)',
                        tension: 0.35,
                        fill: true
                    },
                    {   // 4 - WinCC OA
                        label: 'WinCC OA',
                        data: [],
                        borderColor: '#EC4899',
                        backgroundColor: 'rgba(236, 72, 153, 0.12)',
                        tension: 0.35,
                        fill: true
                    },
                    {   // 5 - WinCC UA
                        label: 'WinCC UA',
                        data: [],
                        borderColor: '#A78BFA',
                        backgroundColor: 'rgba(167, 139, 250, 0.12)',
                        tension: 0.35,
                        fill: true
                    },
                    {   // 6 - NATS total (in+out)
                        label: 'NATS',
                        data: [],
                        borderColor: '#F59E0B',
                        backgroundColor: 'rgba(245, 158, 11, 0.12)',
                        tension: 0.35,
                        fill: true
                    },
                    {   // 7 - Redis total (in+out)
                        label: 'Redis',
                        data: [],
                        borderColor: '#6366F1',
                        backgroundColor: 'rgba(99, 102, 241, 0.12)',
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
            if (!document.getElementById('cluster-overview')) return; // page navigated away
            const brokers = await window.graphqlClient.getBrokers();
            this.updateMetrics(brokers);
            this.updateLoggerMetrics();
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
            this.updateLoggerMetrics();
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
                    kafkaClientIn: 0, kafkaClientOut: 0,
                    opcUaClientIn: 0, opcUaClientOut: 0,
                    winCCOaClientIn: 0,
                    winCCUaClientIn: 0,
                    natsClientIn: 0, natsClientOut: 0,
                    redisClientIn: 0, redisClientOut: 0
                };
            }
            aggregatedData[timeKey].messagesIn += point.messagesIn || 0;
            aggregatedData[timeKey].messagesOut += point.messagesOut || 0;
            aggregatedData[timeKey].messageBusIn += point.messageBusIn || 0;
            aggregatedData[timeKey].messageBusOut += point.messageBusOut || 0;
            aggregatedData[timeKey].mqttClientIn += point.mqttClientIn || 0;
            aggregatedData[timeKey].mqttClientOut += point.mqttClientOut || 0;
            aggregatedData[timeKey].kafkaClientIn += point.kafkaClientIn || 0;
            aggregatedData[timeKey].kafkaClientOut += point.kafkaClientOut || 0;
            aggregatedData[timeKey].opcUaClientIn += point.opcUaClientIn || 0;
            aggregatedData[timeKey].opcUaClientOut += point.opcUaClientOut || 0;
            aggregatedData[timeKey].winCCOaClientIn += point.winCCOaClientIn || 0;
            aggregatedData[timeKey].winCCUaClientIn += point.winCCUaClientIn || 0;
            aggregatedData[timeKey].natsClientIn += point.natsClientIn || 0;
            aggregatedData[timeKey].natsClientOut += point.natsClientOut || 0;
            aggregatedData[timeKey].redisClientIn += point.redisClientIn || 0;
            aggregatedData[timeKey].redisClientOut += point.redisClientOut || 0;
        });

        // Preserve chronological order by iterating sorted allHistoricalData sequence
        const orderedTimestamps = allHistoricalData.map(p => p.timestamp).filter((v, i, a) => a.indexOf(v) === i);
        orderedTimestamps.forEach(timestamp => {
            const data = aggregatedData[timestamp];
            if (!data) return; // safety guard
            const timeLabel = this.formatTimestampFor(this.currentTimeframe, timestamp);
            this.trafficChart.data.labels.push(timeLabel);
            this.trafficChart.data.datasets[0].data.push(data.messagesIn + data.messagesOut);  // MQTT Messages
            this.trafficChart.data.datasets[1].data.push(data.mqttClientIn + data.mqttClientOut);  // MQTT Bridges
            this.trafficChart.data.datasets[2].data.push(data.kafkaClientIn + data.kafkaClientOut);  // Kafka
            this.trafficChart.data.datasets[3].data.push(data.opcUaClientIn + data.opcUaClientOut);  // OPC UA
            this.trafficChart.data.datasets[4].data.push(data.winCCOaClientIn);  // WinCC OA (in only)
            this.trafficChart.data.datasets[5].data.push(data.winCCUaClientIn);  // WinCC UA (in only)
            this.trafficChart.data.datasets[6].data.push(data.natsClientIn + data.natsClientOut);  // NATS
            this.trafficChart.data.datasets[7].data.push(data.redisClientIn + data.redisClientOut);  // Redis
        });

        console.debug('Traffic history loaded', {
            timeframe: this.currentTimeframe,
            points: this.trafficChart.data.labels.length,
            sampleLast5Kafka: this.trafficChart.data.datasets[2].data.slice(-5),
            sampleLast5OpcUa: this.trafficChart.data.datasets[3].data.slice(-5)
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
                kafkaClientOut: acc.kafkaClientOut + (metrics.kafkaClientOut || 0),
                opcUaClientIn: acc.opcUaClientIn + (metrics.opcUaClientIn || 0),
                opcUaClientOut: acc.opcUaClientOut + (metrics.opcUaClientOut || 0),
                winCCOaClientIn: acc.winCCOaClientIn + (metrics.winCCOaClientIn || 0),
                winCCUaClientIn: acc.winCCUaClientIn + (metrics.winCCUaClientIn || 0),
                natsClientIn: acc.natsClientIn + (metrics.natsClientIn || 0),
                natsClientOut: acc.natsClientOut + (metrics.natsClientOut || 0),
                redisClientIn: acc.redisClientIn + (metrics.redisClientIn || 0),
                redisClientOut: acc.redisClientOut + (metrics.redisClientOut || 0)
            };
        }, { messagesIn: 0, messagesOut: 0, messageBusIn: 0, messageBusOut: 0, mqttClientIn: 0, mqttClientOut: 0, kafkaClientIn: 0, kafkaClientOut: 0, opcUaClientIn: 0, opcUaClientOut: 0, winCCOaClientIn: 0, winCCUaClientIn: 0, natsClientIn: 0, natsClientOut: 0, redisClientIn: 0, redisClientOut: 0 });

        if (this.trafficChart.data.labels.length >= this.maxDataPoints) {
            this.trafficChart.data.labels.shift();
            for (let i = 0; i < this.trafficChart.data.datasets.length; i++) {
                this.trafficChart.data.datasets[i].data.shift();
            }
        }

        this.trafficChart.data.labels.push(timeLabel);
        this.trafficChart.data.datasets[0].data.push(clusterTotals.messagesIn + clusterTotals.messagesOut);  // MQTT Messages
        this.trafficChart.data.datasets[1].data.push(clusterTotals.mqttClientIn + clusterTotals.mqttClientOut);  // MQTT Bridges
        this.trafficChart.data.datasets[2].data.push(clusterTotals.kafkaClientIn + clusterTotals.kafkaClientOut);  // Kafka
        this.trafficChart.data.datasets[3].data.push(clusterTotals.opcUaClientIn + clusterTotals.opcUaClientOut);  // OPC UA
        this.trafficChart.data.datasets[4].data.push(clusterTotals.winCCOaClientIn);  // WinCC OA
        this.trafficChart.data.datasets[5].data.push(clusterTotals.winCCUaClientIn);  // WinCC UA
        this.trafficChart.data.datasets[6].data.push(clusterTotals.natsClientIn + clusterTotals.natsClientOut);  // NATS
        this.trafficChart.data.datasets[7].data.push(clusterTotals.redisClientIn + clusterTotals.redisClientOut);  // Redis
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
                kafkaClientOut: acc.kafkaClientOut + (metrics.kafkaClientOut || 0),
                opcUaClientIn: acc.opcUaClientIn + (metrics.opcUaClientIn || 0),
                opcUaClientOut: acc.opcUaClientOut + (metrics.opcUaClientOut || 0),
                winCCOaClientIn: acc.winCCOaClientIn + (metrics.winCCOaClientIn || 0),
                winCCUaClientIn: acc.winCCUaClientIn + (metrics.winCCUaClientIn || 0),
                natsClientIn: acc.natsClientIn + (metrics.natsClientIn || 0),
                natsClientOut: acc.natsClientOut + (metrics.natsClientOut || 0),
                redisClientIn: acc.redisClientIn + (metrics.redisClientIn || 0),
                redisClientOut: acc.redisClientOut + (metrics.redisClientOut || 0)
            };
        }, {
            messagesIn: 0, messagesOut: 0, totalSessions: 0, queuedMessages: 0,
            messageBusIn: 0, messageBusOut: 0,
            mqttClientIn: 0, mqttClientOut: 0,
            kafkaClientIn: 0, kafkaClientOut: 0,
            opcUaClientIn: 0, opcUaClientOut: 0,
            winCCOaClientIn: 0,
            winCCUaClientIn: 0,
            natsClientIn: 0, natsClientOut: 0,
            redisClientIn: 0, redisClientOut: 0
        });

        // Collect all enabled features across all cluster nodes (union)
        const allEnabledFeatures = new Set(brokers.flatMap(b => b.enabledFeatures || []));
        // If any node returns an empty array, features were explicitly configured;
        // if ALL nodes return empty arrays, treat as "all explicitly disabled"
        const anyNodeHasExplicitConfig = brokers.some(b => Array.isArray(b.enabledFeatures));
        const isEnabled = (feature) => {
            if (!anyNodeHasExplicitConfig) return true; // no feature config — show all
            return allEnabledFeatures.has(feature);
        };

        const overviewContainer = document.getElementById('cluster-overview');

        const cIn = '#22C55E';  // green for In
        const cOut = '#6366F1'; // indigo for Out

        const overallIn = clusterTotals.messagesIn + clusterTotals.mqttClientIn + clusterTotals.kafkaClientIn + clusterTotals.opcUaClientIn + (clusterTotals.winCCOaClientIn || 0) + (clusterTotals.winCCUaClientIn || 0) + clusterTotals.natsClientIn + clusterTotals.redisClientIn;
        const overallOut = clusterTotals.messagesOut + clusterTotals.mqttClientOut + clusterTotals.kafkaClientOut + clusterTotals.opcUaClientOut + clusterTotals.natsClientOut + clusterTotals.redisClientOut;

        const cards = [
            `<div class="metric-card"><div class="metric-header"><span class="metric-title">Active Sessions</span><div class="metric-icon">👥</div></div><div class="metric-value">${clusterTotals.totalSessions}</div><div class="metric-label">${brokers.length} Node${brokers.length !== 1 ? 's' : ''}</div></div>`,
            `<div class="metric-card"><div class="metric-header"><span class="metric-title">Queued Messages</span><div class="metric-icon">⏳</div></div><div class="metric-value">${this.formatNumber(clusterTotals.queuedMessages)}</div><div class="metric-label">Pending Delivery</div></div>`,
            `<div class="metric-card">
                <div class="metric-header"><span class="metric-title">Overall Messages</span><div class="metric-icon">📊</div></div>
                <div class="metric-value"><span style="color: ${cIn};">${this.formatNumber(overallIn)}</span> / <span style="color: ${cOut};">${this.formatNumber(overallOut)}</span></div>
                <div class="metric-label">In / Out (all sources)</div>
            </div>`,
            `<div class="metric-card">
                <div class="metric-header"><span class="metric-title">MQTT Messages</span><div class="metric-icon">📨</div></div>
                <div class="metric-value"><span style="color: ${cIn};">${this.formatNumber(clusterTotals.messagesIn)}</span> / <span style="color: ${cOut};">${this.formatNumber(clusterTotals.messagesOut)}</span></div>
                <div class="metric-label">In / Out</div>
            </div>`,
            isEnabled('MqttClient') ? `<div class="metric-card">
                <div class="metric-header"><span class="metric-title">MQTT Bridges</span><div class="metric-icon">🌉</div></div>
                <div class="metric-value"><span style="color: ${cIn};">${this.formatNumber(clusterTotals.mqttClientIn)}</span> / <span style="color: ${cOut};">${this.formatNumber(clusterTotals.mqttClientOut)}</span></div>
                <div class="metric-label">In / Out</div>
            </div>` : '',
            isEnabled('Kafka') ? `<div class="metric-card">
                <div class="metric-header"><span class="metric-title">Kafka Bridges</span><div class="metric-icon">📦</div></div>
                <div class="metric-value"><span style="color: ${cIn};">${this.formatNumber(clusterTotals.kafkaClientIn)}</span> / <span style="color: ${cOut};">${this.formatNumber(clusterTotals.kafkaClientOut)}</span></div>
                <div class="metric-label">In / Out</div>
            </div>` : '',
            (isEnabled('OpcUa') || isEnabled('OpcUaServer')) ? `<div class="metric-card">
                <div class="metric-header"><span class="metric-title">OPC UA Clients</span><div class="metric-icon">⚙️</div></div>
                <div class="metric-value"><span style="color: ${cIn};">${this.formatNumber(clusterTotals.opcUaClientIn)}</span> / <span style="color: ${cOut};">${this.formatNumber(clusterTotals.opcUaClientOut)}</span></div>
                <div class="metric-label">In / Out</div>
            </div>` : '',
            isEnabled('WinCCOa') ? `<div class="metric-card">
                <div class="metric-header"><span class="metric-title">WinCC OA Clients</span><div class="metric-icon">🏭</div></div>
                <div class="metric-value"><span style="color: ${cIn};">${this.formatNumber(clusterTotals.winCCOaClientIn || 0)}</span></div>
                <div class="metric-label">In</div>
            </div>` : '',
            isEnabled('WinCCUa') ? `<div class="metric-card">
                <div class="metric-header"><span class="metric-title">WinCC UA Clients</span><div class="metric-icon">🔧</div></div>
                <div class="metric-value"><span style="color: ${cIn};">${this.formatNumber(clusterTotals.winCCUaClientIn || 0)}</span></div>
                <div class="metric-label">In</div>
            </div>` : '',
            isEnabled('Nats') ? `<div class="metric-card">
                <div class="metric-header"><span class="metric-title">NATS Bridges</span><div class="metric-icon">📡</div></div>
                <div class="metric-value"><span style="color: ${cIn};">${this.formatNumber(clusterTotals.natsClientIn)}</span> / <span style="color: ${cOut};">${this.formatNumber(clusterTotals.natsClientOut)}</span></div>
                <div class="metric-label">In / Out</div>
            </div>` : '',
            isEnabled('Redis') ? `<div class="metric-card">
                <div class="metric-header"><span class="metric-title">Redis Bridges</span><div class="metric-icon">🗄️</div></div>
                <div class="metric-value"><span style="color: ${cIn};">${this.formatNumber(clusterTotals.redisClientIn)}</span> / <span style="color: ${cOut};">${this.formatNumber(clusterTotals.redisClientOut)}</span></div>
                <div class="metric-label">In / Out</div>
            </div>` : '',
        ];

        overviewContainer.innerHTML = cards.join('');
    }

    async updateLoggerMetrics() {
        try {
            const cIn = '#22C55E';
            const container = document.getElementById('logger-overview');
            const section = document.getElementById('logger-section');
            if (!container || !section) return;

            // Collect all enabled features
            const brokers = await window.graphqlClient.getBrokers();
            const allEnabledFeatures = new Set(brokers.flatMap(b => b.enabledFeatures || []));
            const anyNodeHasExplicitConfig = brokers.some(b => Array.isArray(b.enabledFeatures));
            const isEnabled = (feature) => !anyNodeHasExplicitConfig || allEnabledFeatures.has(feature);

            const loggerCards = [];

            // JDBC Loggers
            if (isEnabled('JdbcLogger')) {
                try {
                    const result = await window.graphqlClient.query(`query { jdbcLoggers { metrics { messagesIn messagesWritten } } }`);
                    const loggers = result.jdbcLoggers || [];
                    const totalIn = loggers.reduce((sum, l) => sum + (l.metrics?.[0]?.messagesIn || 0), 0);
                    const totalOut = loggers.reduce((sum, l) => sum + (l.metrics?.[0]?.messagesWritten || 0), 0);
                    if (loggers.length > 0) loggerCards.push(`<div class="metric-card"><div class="metric-header"><span class="metric-title">JDBC Loggers</span><div class="metric-icon">🗃️</div></div><div class="metric-value"><span style="color: ${cIn};">${this.formatNumber(totalIn)}</span> / <span style="color: #6366F1;">${this.formatNumber(totalOut)}</span></div><div class="metric-label">In / Written (${loggers.length})</div></div>`);
                } catch (e) { /* ignore */ }
            }

            // InfluxDB Loggers
            if (isEnabled('InfluxDBLogger')) {
                try {
                    const result = await window.graphqlClient.query(`query { influxdbLoggers { metrics { messagesIn messagesWritten } } }`);
                    const loggers = result.influxdbLoggers || [];
                    const totalIn = loggers.reduce((sum, l) => sum + (l.metrics?.messagesIn || 0), 0);
                    const totalOut = loggers.reduce((sum, l) => sum + (l.metrics?.messagesWritten || 0), 0);
                    if (loggers.length > 0) loggerCards.push(`<div class="metric-card"><div class="metric-header"><span class="metric-title">InfluxDB Loggers</span><div class="metric-icon">📈</div></div><div class="metric-value"><span style="color: ${cIn};">${this.formatNumber(totalIn)}</span> / <span style="color: #6366F1;">${this.formatNumber(totalOut)}</span></div><div class="metric-label">In / Written (${loggers.length})</div></div>`);
                } catch (e) { /* ignore */ }
            }

            // TimeBase Loggers
            if (isEnabled('TimeBaseLogger')) {
                try {
                    const result = await window.graphqlClient.query(`query { timebaseLoggers { metrics { messagesIn messagesWritten } } }`);
                    const loggers = result.timebaseLoggers || [];
                    const totalIn = loggers.reduce((sum, l) => sum + (l.metrics?.messagesIn || 0), 0);
                    const totalOut = loggers.reduce((sum, l) => sum + (l.metrics?.messagesWritten || 0), 0);
                    if (loggers.length > 0) loggerCards.push(`<div class="metric-card"><div class="metric-header"><span class="metric-title">TimeBase Loggers</span><div class="metric-icon">⏱️</div></div><div class="metric-value"><span style="color: ${cIn};">${this.formatNumber(totalIn)}</span> / <span style="color: #6366F1;">${this.formatNumber(totalOut)}</span></div><div class="metric-label">In / Written (${loggers.length})</div></div>`);
                } catch (e) { /* ignore */ }
            }

            if (loggerCards.length > 0) {
                container.innerHTML = loggerCards.join('');
                section.style.display = '';
            } else {
                container.innerHTML = '';
                section.style.display = 'none';
            }
        } catch (e) {
            console.error('Failed to update logger metrics:', e);
        }
    }

    updateBrokerTable(brokers) {
        const tableBody = document.getElementById('broker-table-body');
        if (!tableBody) return;

        // Detect feature mismatches across cluster nodes
        // null/absent means "all enabled (default)" — distinct from [] (none enabled)
        const allFeatureSets = brokers.map(b =>
            Array.isArray(b.enabledFeatures)
                ? b.enabledFeatures.slice().sort().join(',')
                : '__default__'
        );
        const hasMismatch = allFeatureSets.length > 1 && new Set(allFeatureSets).size > 1;

        // Collect union of enabled features to decide which columns to show
        const allEnabledFeatures = new Set(brokers.flatMap(b => b.enabledFeatures || []));
        const anyExplicit = brokers.some(b => Array.isArray(b.enabledFeatures));
        const colVisible = (feature) => !anyExplicit || allEnabledFeatures.has(feature);

        // Update column header visibility
        const table = tableBody.closest('table');
        const colMap = {
            'th-mqtt-io':   colVisible('MqttClient'),
            'th-kafka-io':  colVisible('Kafka'),
            'th-opcua-io':  colVisible('OpcUa') || colVisible('OpcUaServer'),
            'th-winccoa-io': colVisible('WinCCOa'),
            'th-winccua-io': colVisible('WinCCUa'),
            'th-nats-io':   colVisible('Nats'),
            'th-redis-io':  colVisible('Redis'),
        };
        if (table) {
            Object.entries(colMap).forEach(([id, visible]) => {
                const th = table.querySelector(`#${id}`);
                if (th) th.style.display = visible ? '' : 'none';
            });
        }

        tableBody.innerHTML = brokers.map(broker => {
            const metrics = broker.metrics && broker.metrics.length > 0 ? broker.metrics[0] : {};
            const isHealthy = metrics.nodeSessionCount >= 0;
            const features = broker.enabledFeatures;
            const hasExplicit = Array.isArray(features);
            const featureList = features || [];
            const featureTooltip = !hasExplicit ? 'all enabled (default)' : featureList.length === 0 ? 'none enabled' : featureList.join(', ');
            const featureLabel = !hasExplicit ? 'all' : featureList.length === 0 ? 'none' : `${featureList.length} enabled`;
            const featureWarning = hasMismatch
                ? `<span title="Feature mismatch across cluster nodes! This node: ${featureTooltip}" style="color:#F59E0B;cursor:default;">&#9888; ${featureLabel}</span>`
                : `<span title="${featureTooltip}" style="color:var(--text-secondary);cursor:default;">${featureLabel}</span>`;
            const td = (visible, content) => visible ? `<td>${content}</td>` : '';
            return `
                <tr${hasMismatch ? ' style="background:rgba(245,158,11,0.05);"' : ''}>
                    <td><strong>${broker.nodeId}</strong></td>
                    <td><code style="font-size: 0.875rem; background: var(--bg-secondary); padding: 2px 6px; border-radius: 4px;">${broker.version || 'unknown'}</code></td>
                    <td><span class="status-indicator ${isHealthy ? 'status-online' : 'status-offline'}"><span class="status-dot"></span>${isHealthy ? 'Online' : 'Offline'}</span></td>
                    <td>${this.formatNumber(metrics.messagesIn || 0)}</td>
                    <td>${this.formatNumber(metrics.messagesOut || 0)}</td>
                    <td>${metrics.nodeSessionCount || 0}</td>
                    <td>${this.formatNumber(metrics.queuedMessagesCount || 0)}</td>
                    <td><span style="color: #14B8A6;">${this.formatNumber(metrics.messageBusIn || 0)}</span> / <span style="color: #F97316;">${this.formatNumber(metrics.messageBusOut || 0)}</span></td>
                    ${td(colVisible('MqttClient'), `<span style="color: #0EA5E9;">${this.formatNumber(metrics.mqttClientIn || 0)}</span> / <span style="color: #F59E0B;">${this.formatNumber(metrics.mqttClientOut || 0)}</span>`)}
                    ${td(colVisible('Kafka'), `<span style="color: #EF4444;">${this.formatNumber(metrics.kafkaClientIn || 0)}</span>`)}
                    ${td(colVisible('OpcUa') || colVisible('OpcUaServer'), `<span style="color: #14B8A6;">${this.formatNumber(metrics.opcUaClientIn || 0)}</span> / <span style="color: #9333EA;">${this.formatNumber(metrics.opcUaClientOut || 0)}</span>`)}
                    ${td(colVisible('WinCCOa'), `<span style="color: #EC4899;">${this.formatNumber(metrics.winCCOaClientIn || 0)}</span>`)}
                    ${td(colVisible('WinCCUa'), `<span style="color: #A78BFA;">${this.formatNumber(metrics.winCCUaClientIn || 0)}</span>`)}
                    ${td(colVisible('Nats'), `<span style="color: #F59E0B;">${this.formatNumber(metrics.natsClientIn || 0)}</span> / <span style="color: #F59E0B;">${this.formatNumber(metrics.natsClientOut || 0)}</span>`)}
                    ${td(colVisible('Redis'), `<span style="color: #6366F1;">${this.formatNumber(metrics.redisClientIn || 0)}</span> / <span style="color: #6366F1;">${this.formatNumber(metrics.redisClientOut || 0)}</span>`)}
                    <td>${featureWarning}</td>
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
        return (Math.round(num * 10) / 10).toString();
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