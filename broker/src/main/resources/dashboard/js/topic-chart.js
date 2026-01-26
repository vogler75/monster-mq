/**
 * Topic Chart - Time series visualization for MQTT topics
 */

// Color palette for chart datasets
const CHART_COLORS = [
    '#22C55E', // Green
    '#6366F1', // Indigo
    '#F59E0B', // Amber
    '#EF4444', // Red
    '#14B8A6', // Teal
    '#EC4899', // Pink
    '#8B5CF6', // Purple
    '#0EA5E9', // Sky
    '#F97316', // Orange
    '#06B6D4', // Cyan
];

class TopicChartManager {
    constructor() {
        this.chart = null;
        this.topics = []; // Array of {topic, field, color}
        this.timeRangeMinutes = 10;
        this.endTime = new Date();
        this.archiveGroup = 'Default';
        this.aggregationMode = 'RAW'; // RAW, ONE_MINUTE, FIVE_MINUTES, FIFTEEN_MINUTES, ONE_HOUR, ONE_DAY
        this.isLoading = false;

        this.init();
    }

    async init() {
        if (!this.isLoggedIn()) {
            window.location.href = '/pages/login.html';
            return;
        }

        this.setupChart();
        this.setupEventListeners();
        await this.loadArchiveGroups();
        this.loadState();
        this.updateTimeDisplay();

        if (this.topics.length > 0) {
            this.refreshChart();
        }
    }

    isLoggedIn() {
        const token = safeStorage.getItem('monstermq_token');
        if (!token) return false;
        if (token === 'null') return true;

        try {
            const decoded = JSON.parse(atob(token.split('.')[1]));
            const now = Date.now() / 1000;
            return decoded.exp > now;
        } catch {
            return false;
        }
    }

    setupChart() {
        const ctx = document.getElementById('topicChart').getContext('2d');

        Chart.defaults.color = '#CBD5E1';
        Chart.defaults.borderColor = '#475569';

        this.chart = new Chart(ctx, {
            type: 'line',
            data: {
                datasets: []
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                interaction: {
                    mode: 'index',
                    intersect: false
                },
                scales: {
                    x: {
                        type: 'time',
                        time: {
                            displayFormats: {
                                second: 'HH:mm:ss',
                                minute: 'HH:mm',
                                hour: 'HH:mm',
                                day: 'MMM d'
                            }
                        },
                        grid: { color: '#334155' },
                        ticks: { maxRotation: 0 }
                    },
                    y: {
                        beginAtZero: false,
                        grid: { color: '#334155' }
                    }
                },
                plugins: {
                    legend: {
                        position: 'top',
                        labels: {
                            usePointStyle: true,
                            padding: 20
                        }
                    },
                    tooltip: {
                        callbacks: {
                            title: function(context) {
                                const date = new Date(context[0].parsed.x);
                                return date.toLocaleString();
                            }
                        }
                    }
                }
            }
        });
    }

    setupEventListeners() {
        // Time range select
        document.getElementById('time-range-select').addEventListener('change', (e) => {
            this.timeRangeMinutes = parseInt(e.target.value);
            this.saveState();
            this.updateTimeDisplay();
            this.refreshChart();
        });

        // Archive group select
        document.getElementById('archive-group-select').addEventListener('change', (e) => {
            this.archiveGroup = e.target.value;
            this.saveState();
            this.refreshChart();
        });

        // Aggregation mode select
        document.getElementById('aggregation-mode-select').addEventListener('change', (e) => {
            this.aggregationMode = e.target.value;
            this.saveState();
            this.refreshChart();
        });

        // Navigation buttons
        document.getElementById('btn-refresh').addEventListener('click', () => this.jumpToNow());
        document.getElementById('btn-jump-back').addEventListener('click', () => this.jumpBack());
        document.getElementById('btn-step-back').addEventListener('click', () => this.stepBack());
        document.getElementById('btn-zoom-out').addEventListener('click', () => this.zoomOut());
        document.getElementById('btn-zoom-in').addEventListener('click', () => this.zoomIn());
        document.getElementById('btn-step-forward').addEventListener('click', () => this.stepForward());
        document.getElementById('btn-jump-now').addEventListener('click', () => this.jumpToNow());

        // Add topic button
        document.getElementById('btn-add-topic').addEventListener('click', () => this.addTopicFromInput());

        // Enter key on inputs
        document.getElementById('topic-input').addEventListener('keypress', (e) => {
            if (e.key === 'Enter') this.addTopicFromInput();
        });
        document.getElementById('json-field-input').addEventListener('keypress', (e) => {
            if (e.key === 'Enter') this.addTopicFromInput();
        });

        // Modal buttons
        document.getElementById('modal-cancel').addEventListener('click', () => this.hideJsonFieldModal());
        document.getElementById('modal-add').addEventListener('click', () => this.confirmJsonFieldModal());
        document.getElementById('modal-use-raw').addEventListener('click', () => this.useRawValue());
        document.getElementById('modal-json-field').addEventListener('keypress', (e) => {
            if (e.key === 'Enter') this.confirmJsonFieldModal();
        });
    }

    async loadArchiveGroups() {
        try {
            const query = `
                query GetArchiveGroups {
                    archiveGroups(enabled: true) {
                        name
                    }
                }
            `;

            const response = await graphqlClient.query(query);

            if (response && response.archiveGroups && response.archiveGroups.length > 0) {
                const groups = response.archiveGroups;
                const mainSelect = document.getElementById('archive-group-select');
                const panelSelect = document.getElementById('topic-panel-archive-group');

                mainSelect.innerHTML = '';
                panelSelect.innerHTML = '';

                groups.forEach(group => {
                    const option1 = document.createElement('option');
                    option1.value = group.name;
                    option1.textContent = group.name;
                    mainSelect.appendChild(option1);

                    const option2 = document.createElement('option');
                    option2.value = group.name;
                    option2.textContent = group.name;
                    panelSelect.appendChild(option2);
                });

                // Set default
                if (groups.length > 0) {
                    const defaultGroup = groups.find(g => g.name === 'Default') || groups[0];
                    this.archiveGroup = defaultGroup.name;
                    mainSelect.value = this.archiveGroup;
                    panelSelect.value = this.archiveGroup;
                }
            }
        } catch (error) {
            console.error('Error loading archive groups:', error);
        }
    }

    // Time navigation methods
    jumpBack() {
        this.endTime = new Date(this.endTime.getTime() - this.timeRangeMinutes * 60 * 1000);
        this.updateTimeDisplay();
        this.refreshChart();
    }

    stepBack() {
        const stepMs = this.timeRangeMinutes * 60 * 1000 * 0.1;
        this.endTime = new Date(this.endTime.getTime() - stepMs);
        this.updateTimeDisplay();
        this.refreshChart();
    }

    stepForward() {
        const stepMs = this.timeRangeMinutes * 60 * 1000 * 0.1;
        const now = new Date();
        this.endTime = new Date(Math.min(this.endTime.getTime() + stepMs, now.getTime()));
        this.updateTimeDisplay();
        this.refreshChart();
    }

    jumpToNow() {
        this.endTime = new Date();
        this.updateTimeDisplay();
        this.refreshChart();
    }

    zoomIn() {
        if (this.timeRangeMinutes > 1) {
            this.timeRangeMinutes = Math.max(1, Math.floor(this.timeRangeMinutes / 2));
            document.getElementById('time-range-select').value = this.findClosestTimeRange(this.timeRangeMinutes);
            this.saveState();
            this.updateTimeDisplay();
            this.refreshChart();
        }
    }

    zoomOut() {
        this.timeRangeMinutes = Math.min(10080, this.timeRangeMinutes * 2); // Max 7 days
        document.getElementById('time-range-select').value = this.findClosestTimeRange(this.timeRangeMinutes);
        this.saveState();
        this.updateTimeDisplay();
        this.refreshChart();
    }

    findClosestTimeRange(minutes) {
        const options = [5, 10, 30, 60, 360, 1440, 10080];
        let closest = options[0];
        let minDiff = Math.abs(minutes - closest);

        for (const opt of options) {
            const diff = Math.abs(minutes - opt);
            if (diff < minDiff) {
                minDiff = diff;
                closest = opt;
            }
        }
        return closest.toString();
    }

    updateTimeDisplay() {
        const startTime = new Date(this.endTime.getTime() - this.timeRangeMinutes * 60 * 1000);
        const formatOptions = this.timeRangeMinutes >= 1440
            ? { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' }
            : { hour: '2-digit', minute: '2-digit', second: '2-digit' };

        const startStr = startTime.toLocaleString('en-US', formatOptions);
        const endStr = this.endTime.toLocaleString('en-US', formatOptions);

        document.getElementById('time-display').textContent = `${startStr} - ${endStr}`;
    }

    // Topic management
    addTopicFromInput() {
        const topicInput = document.getElementById('topic-input');
        const fieldInput = document.getElementById('json-field-input');

        const topic = topicInput.value.trim();
        const field = fieldInput.value.trim();

        if (!topic) {
            topicInput.focus();
            return;
        }

        this.addTopic(topic, field);

        topicInput.value = '';
        fieldInput.value = '';
        topicInput.focus();
    }

    addTopic(topic, field = '') {
        // Check for duplicates
        const exists = this.topics.some(t => t.topic === topic && t.field === field);
        if (exists) {
            console.log('Topic already exists:', topic, field);
            return;
        }

        // Assign color
        const usedColors = this.topics.map(t => t.color);
        const availableColor = CHART_COLORS.find(c => !usedColors.includes(c)) || CHART_COLORS[this.topics.length % CHART_COLORS.length];

        this.topics.push({
            topic,
            field,
            color: availableColor,
            enabled: true
        });

        this.saveState();
        this.renderTopicsList();
        this.refreshChart();
    }

    removeTopic(index) {
        this.topics.splice(index, 1);
        this.saveState();
        this.renderTopicsList();
        this.refreshChart();
    }

    changeTopicColor(index, color) {
        this.topics[index].color = color;
        this.saveState();
        this.refreshChart();
    }

    toggleTopic(index) {
        this.topics[index].enabled = !this.topics[index].enabled;
        this.saveState();
        this.refreshChart();
    }

    renderTopicsList() {
        const container = document.getElementById('topics-list');

        if (this.topics.length === 0) {
            container.innerHTML = '<div class="empty-state">No topics added. Add a topic above or browse to select one.</div>';
            return;
        }

        container.innerHTML = this.topics.map((t, index) => `
            <div class="topic-item${t.enabled === false ? ' disabled' : ''}">
                <input type="checkbox" class="topic-toggle" ${t.enabled !== false ? 'checked' : ''}
                       onchange="topicChartManager.toggleTopic(${index})"
                       title="Show/hide on chart">
                <input type="color" class="topic-color" value="${t.color}"
                       onchange="topicChartManager.changeTopicColor(${index}, this.value)"
                       title="Change color">
                <div class="topic-info">
                    <div class="topic-name">${this.escapeHtml(t.topic)}</div>
                    ${t.field ? `<div class="topic-field">.${this.escapeHtml(t.field)}</div>` : ''}
                </div>
                <button class="topic-remove" onclick="topicChartManager.removeTopic(${index})" title="Remove topic">
                    <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                        <line x1="18" y1="6" x2="6" y2="18"></line>
                        <line x1="6" y1="6" x2="18" y2="18"></line>
                    </svg>
                </button>
            </div>
        `).join('');
    }

    escapeHtml(text) {
        const div = document.createElement('div');
        div.textContent = text;
        return div.innerHTML;
    }

    // Chart data loading
    async refreshChart() {
        if (this.isLoading || this.topics.length === 0) {
            if (this.topics.length === 0) {
                this.chart.data.datasets = [];
                this.chart.update();
            }
            return;
        }

        this.isLoading = true;

        try {
            const startTime = new Date(this.endTime.getTime() - this.timeRangeMinutes * 60 * 1000);
            let datasets = [];

            if (this.aggregationMode === 'RAW') {
                // Use raw data fetching (existing logic)
                datasets = await this.fetchRawDatasets(startTime, this.endTime);
            } else {
                // Use aggregated data fetching
                datasets = await this.fetchAggregatedDatasets(startTime, this.endTime);
            }

            this.chart.data.datasets = datasets;
            this.chart.options.scales.x.min = startTime.getTime();
            this.chart.options.scales.x.max = this.endTime.getTime();
            this.chart.update();

        } catch (error) {
            console.error('Error refreshing chart:', error);
        } finally {
            this.isLoading = false;
        }
    }

    async fetchRawDatasets(startTime, endTime) {
        const datasets = [];
        const enabledTopics = this.topics.filter(t => t.enabled !== false);

        for (const topicConfig of enabledTopics) {
            const data = await this.fetchTopicData(topicConfig.topic, startTime, endTime);
            const processedData = this.processData(data, topicConfig.field);

            const label = topicConfig.field
                ? `${topicConfig.topic} .${topicConfig.field}`
                : topicConfig.topic;

            datasets.push({
                label,
                data: processedData,
                borderColor: topicConfig.color,
                backgroundColor: topicConfig.color + '20',
                tension: 0.3,
                fill: false,
                pointRadius: processedData.length < 100 ? 3 : 1,
                pointHoverRadius: 5
            });
        }

        return datasets;
    }

    async fetchAggregatedDatasets(startTime, endTime) {
        const datasets = [];
        const enabledTopics = this.topics.filter(t => t.enabled !== false);

        if (enabledTopics.length === 0) {
            return datasets;
        }

        // Build topic/field combinations for the query (only enabled topics)
        const topicNames = enabledTopics.map(t => t.topic);
        const fields = [...new Set(enabledTopics.map(t => t.field).filter(f => f))];

        try {
            const data = await this.fetchAggregatedData(topicNames, fields, startTime, endTime);

            if (!data || !data.columns || !data.rows) {
                console.warn('No aggregated data returned');
                return datasets;
            }

            // Parse the table-based response into datasets
            // Columns format: ["timestamp", "topic_field_avg", "topic_field_avg", ...]
            const columns = data.columns;

            // Create a dataset for each enabled topic/field combination
            for (const topicConfig of enabledTopics) {
                // Find the column index for this topic/field
                const topicAlias = topicConfig.topic.replace(/\//g, '_').replace(/\./g, '_');
                const fieldAlias = topicConfig.field ? `.${topicConfig.field.replace(/\./g, '_')}` : '';
                const colName = `${topicConfig.topic}${fieldAlias}_avg`;

                const colIndex = columns.indexOf(colName);
                if (colIndex === -1) {
                    console.warn(`Column not found: ${colName}`);
                    continue;
                }

                // Extract data points
                const dataPoints = [];
                for (const row of data.rows) {
                    const timestamp = new Date(row[0]).getTime();
                    const value = row[colIndex];
                    if (value !== null && value !== undefined) {
                        dataPoints.push({ x: timestamp, y: value });
                    }
                }

                const label = topicConfig.field
                    ? `${topicConfig.topic} .${topicConfig.field}`
                    : topicConfig.topic;

                datasets.push({
                    label,
                    data: dataPoints,
                    borderColor: topicConfig.color,
                    backgroundColor: topicConfig.color + '20',
                    tension: 0.3,
                    fill: false,
                    pointRadius: dataPoints.length < 100 ? 3 : 1,
                    pointHoverRadius: 5
                });
            }
        } catch (error) {
            console.error('Error fetching aggregated data:', error);
        }

        return datasets;
    }

    async fetchAggregatedData(topics, fields, startTime, endTime) {
        const query = `
            query GetAggregatedMessages($topics: [String!]!, $interval: AggregationInterval!, $startTime: String!, $endTime: String!, $fields: [String!], $archiveGroup: String!) {
                aggregatedMessages(
                    topics: $topics
                    interval: $interval
                    startTime: $startTime
                    endTime: $endTime
                    functions: [AVG]
                    fields: $fields
                    archiveGroup: $archiveGroup
                ) {
                    columns
                    rows
                    rowCount
                }
            }
        `;

        try {
            const response = await graphqlClient.query(query, {
                topics: topics,
                interval: this.aggregationMode,
                startTime: startTime.toISOString(),
                endTime: endTime.toISOString(),
                fields: fields.length > 0 ? fields : null,
                archiveGroup: this.archiveGroup
            });

            return response.aggregatedMessages || null;
        } catch (error) {
            console.error('Error fetching aggregated data:', error);
            return null;
        }
    }

    async fetchTopicData(topic, startTime, endTime) {
        const query = `
            query GetArchivedMessages($topic: String!, $startTime: String!, $endTime: String!, $archiveGroup: String!) {
                archivedMessages(
                    topicFilter: $topic
                    startTime: $startTime
                    endTime: $endTime
                    format: JSON
                    limit: 10000
                    archiveGroup: $archiveGroup
                    includeTopic: false
                ) {
                    payload
                    timestamp
                }
            }
        `;

        try {
            const response = await graphqlClient.query(query, {
                topic: topic,
                startTime: startTime.toISOString(),
                endTime: endTime.toISOString(),
                archiveGroup: this.archiveGroup
            });

            return response.archivedMessages || [];
        } catch (error) {
            console.error('Error fetching topic data:', error);
            return [];
        }
    }

    processData(messages, field) {
        const dataPoints = [];

        for (const msg of messages) {
            const timestamp = new Date(msg.timestamp).getTime();
            let value = null;

            // Try to parse as JSON first
            try {
                const json = JSON.parse(msg.payload);

                if (field) {
                    // Extract field from JSON
                    value = this.extractJsonField(json, field);
                } else if (typeof json === 'number') {
                    // JSON is just a number
                    value = json;
                } else if (typeof json === 'object' && json !== null) {
                    // Try common field names
                    const commonFields = ['value', 'v', 'val', 'data', 'reading'];
                    for (const f of commonFields) {
                        if (json[f] !== undefined) {
                            const parsed = parseFloat(json[f]);
                            if (!isNaN(parsed)) {
                                value = parsed;
                                break;
                            }
                        }
                    }
                }
            } catch {
                // Not JSON, try to parse as float
                if (!field) {
                    const parsed = parseFloat(msg.payload);
                    if (!isNaN(parsed)) {
                        value = parsed;
                    }
                }
            }

            if (value !== null && !isNaN(value)) {
                dataPoints.push({ x: timestamp, y: value });
            }
        }

        // Sort by timestamp
        dataPoints.sort((a, b) => a.x - b.x);

        return dataPoints;
    }

    extractJsonField(obj, fieldPath) {
        // Remove leading dot if present
        if (fieldPath.startsWith('.')) {
            fieldPath = fieldPath.substring(1);
        }

        const parts = fieldPath.split('.');
        let current = obj;

        for (const part of parts) {
            if (current === null || current === undefined) {
                return null;
            }
            current = current[part];
        }

        const value = parseFloat(current);
        return isNaN(value) ? null : value;
    }

    // State persistence
    saveState() {
        safeStorage.setItem('topicChart.topics', JSON.stringify(this.topics));
        safeStorage.setItem('topicChart.timeRange', this.timeRangeMinutes.toString());
        safeStorage.setItem('topicChart.archiveGroup', this.archiveGroup);
        safeStorage.setItem('topicChart.aggregationMode', this.aggregationMode);
    }

    loadState() {
        try {
            const topicsStr = safeStorage.getItem('topicChart.topics');
            if (topicsStr) {
                this.topics = JSON.parse(topicsStr);
            }

            const timeRange = safeStorage.getItem('topicChart.timeRange');
            if (timeRange) {
                this.timeRangeMinutes = parseInt(timeRange);
                document.getElementById('time-range-select').value = this.findClosestTimeRange(this.timeRangeMinutes);
            }

            const archiveGroup = safeStorage.getItem('topicChart.archiveGroup');
            if (archiveGroup) {
                this.archiveGroup = archiveGroup;
                document.getElementById('archive-group-select').value = archiveGroup;
            }

            const aggregationMode = safeStorage.getItem('topicChart.aggregationMode');
            if (aggregationMode) {
                this.aggregationMode = aggregationMode;
                document.getElementById('aggregation-mode-select').value = aggregationMode;
            }

            this.renderTopicsList();
        } catch (error) {
            console.error('Error loading state:', error);
        }
    }

    // JSON Field Modal for side panel
    pendingTopicFromBrowser = null;
    selectedJsonField = '';

    async showJsonFieldModal(topic, archiveGroup = null) {
        this.pendingTopicFromBrowser = topic;
        this.selectedJsonField = '';
        // Use provided archive group or fall back to manager's archive group
        const queryArchiveGroup = archiveGroup || this.archiveGroup;

        // Update title
        document.getElementById('modal-topic-title').textContent = `Select Field: ${topic}`;

        // Show modal with loading state
        this.showModalLoading();
        document.getElementById('json-field-modal').classList.remove('hidden');

        // Fetch current value
        try {
            const value = await this.fetchCurrentValue(topic, queryArchiveGroup);

            if (value && value.payload) {
                try {
                    const json = JSON.parse(value.payload);
                    if (typeof json === 'object' && json !== null && !Array.isArray(json)) {
                        // Show JSON viewer
                        this.showJsonViewer(topic, json);
                    } else if (typeof json === 'number') {
                        // It's a number - add directly
                        this.hideJsonFieldModal();
                        this.addTopic(topic, '');
                    } else {
                        // Array or other - show simple input
                        this.showSimpleFieldInput(topic);
                    }
                } catch {
                    // Not JSON - check if it's a number
                    const parsed = parseFloat(value.payload);
                    if (!isNaN(parsed)) {
                        this.hideJsonFieldModal();
                        this.addTopic(topic, '');
                    } else {
                        this.showSimpleFieldInput(topic);
                    }
                }
            } else {
                // No value - show simple input as fallback
                this.showSimpleFieldInput(topic);
            }
        } catch (error) {
            console.error('Error fetching current value:', error);
            this.showSimpleFieldInput(topic);
        }
    }

    async fetchCurrentValue(topic, archiveGroup = null) {
        const query = `
            query GetCurrentValue($topic: String!, $archiveGroup: String!) {
                currentValue(topic: $topic, archiveGroup: $archiveGroup) {
                    topic
                    payload
                    timestamp
                }
            }
        `;
        const response = await graphqlClient.query(query, {
            topic: topic,
            archiveGroup: archiveGroup || this.archiveGroup
        });
        return response?.currentValue;
    }

    showModalLoading() {
        document.getElementById('modal-loading').classList.remove('hidden');
        document.getElementById('modal-json-section').classList.add('hidden');
        document.getElementById('modal-simple-section').classList.add('hidden');
        document.getElementById('modal-raw-section').classList.add('hidden');
        document.getElementById('modal-use-raw').classList.add('hidden');
    }

    showJsonViewer(topic, json) {
        document.getElementById('modal-loading').classList.add('hidden');
        document.getElementById('modal-json-section').classList.remove('hidden');
        document.getElementById('modal-simple-section').classList.add('hidden');
        document.getElementById('modal-raw-section').classList.add('hidden');
        document.getElementById('modal-use-raw').classList.remove('hidden');

        // Render JSON with clickable fields
        const viewer = document.getElementById('modal-json-viewer');
        viewer.innerHTML = this.renderJsonWithPaths(json, 0, '');

        // Reset selection
        this.selectedJsonField = '';
        document.getElementById('modal-selected-field').textContent = '(none)';

        // Add click handlers
        viewer.querySelectorAll('.json-line.clickable').forEach(line => {
            line.addEventListener('click', () => {
                // Remove previous selection
                viewer.querySelectorAll('.json-line.selected').forEach(el => el.classList.remove('selected'));
                // Add selection to clicked line
                line.classList.add('selected');
                // Update selected field
                this.selectedJsonField = line.dataset.path;
                document.getElementById('modal-selected-field').textContent = this.selectedJsonField;
            });
        });
    }

    renderJsonWithPaths(obj, indent, pathPrefix) {
        const indentStr = '  '.repeat(indent);
        let html = '';

        if (typeof obj !== 'object' || obj === null) {
            return this.renderJsonValue(obj);
        }

        const keys = Object.keys(obj);
        html += `<span class="json-line">${indentStr}{</span>\n`;

        keys.forEach((key, index) => {
            const value = obj[key];
            const currentPath = pathPrefix ? `${pathPrefix}.${key}` : key;
            const isLast = index === keys.length - 1;
            const comma = isLast ? '' : ',';

            if (typeof value === 'object' && value !== null && !Array.isArray(value)) {
                // Nested object
                html += `<span class="json-line">${indentStr}  <span class="json-key">"${this.escapeHtml(key)}"</span>: {</span>\n`;
                html += this.renderJsonObjectContent(value, indent + 2, currentPath);
                html += `<span class="json-line">${indentStr}  }${comma}</span>\n`;
            } else if (Array.isArray(value)) {
                // Array - show collapsed
                html += `<span class="json-line">${indentStr}  <span class="json-key">"${this.escapeHtml(key)}"</span>: [...]${comma}</span>\n`;
            } else {
                // Primitive value
                const isNumeric = typeof value === 'number';
                const clickableClass = isNumeric ? 'clickable' : '';
                const dataPath = isNumeric ? `data-path="${this.escapeHtml(currentPath)}"` : '';
                html += `<span class="json-line ${clickableClass}" ${dataPath}>${indentStr}  <span class="json-key">"${this.escapeHtml(key)}"</span>: ${this.renderJsonValue(value)}${comma}</span>\n`;
            }
        });

        html += `<span class="json-line">${indentStr}}</span>`;
        return html;
    }

    renderJsonObjectContent(obj, indent, pathPrefix) {
        const indentStr = '  '.repeat(indent);
        let html = '';
        const keys = Object.keys(obj);

        keys.forEach((key, index) => {
            const value = obj[key];
            const currentPath = pathPrefix ? `${pathPrefix}.${key}` : key;
            const isLast = index === keys.length - 1;
            const comma = isLast ? '' : ',';

            if (typeof value === 'object' && value !== null && !Array.isArray(value)) {
                // Nested object
                html += `<span class="json-line">${indentStr}<span class="json-key">"${this.escapeHtml(key)}"</span>: {</span>\n`;
                html += this.renderJsonObjectContent(value, indent + 1, currentPath);
                html += `<span class="json-line">${indentStr}}${comma}</span>\n`;
            } else if (Array.isArray(value)) {
                // Array - show collapsed
                html += `<span class="json-line">${indentStr}<span class="json-key">"${this.escapeHtml(key)}"</span>: [...]${comma}</span>\n`;
            } else {
                // Primitive value
                const isNumeric = typeof value === 'number';
                const clickableClass = isNumeric ? 'clickable' : '';
                const dataPath = isNumeric ? `data-path="${this.escapeHtml(currentPath)}"` : '';
                html += `<span class="json-line ${clickableClass}" ${dataPath}>${indentStr}<span class="json-key">"${this.escapeHtml(key)}"</span>: ${this.renderJsonValue(value)}${comma}</span>\n`;
            }
        });

        return html;
    }

    renderJsonValue(value) {
        if (value === null) {
            return '<span class="json-null">null</span>';
        } else if (typeof value === 'boolean') {
            return `<span class="json-boolean">${value}</span>`;
        } else if (typeof value === 'number') {
            return `<span class="json-number">${value}</span>`;
        } else if (typeof value === 'string') {
            return `<span class="json-string">"${this.escapeHtml(value)}"</span>`;
        } else {
            return `<span class="json-string">${this.escapeHtml(String(value))}</span>`;
        }
    }

    showSimpleFieldInput(topic) {
        document.getElementById('modal-loading').classList.add('hidden');
        document.getElementById('modal-json-section').classList.add('hidden');
        document.getElementById('modal-simple-section').classList.remove('hidden');
        document.getElementById('modal-raw-section').classList.add('hidden');
        document.getElementById('modal-use-raw').classList.remove('hidden');

        const input = document.getElementById('modal-json-field');
        input.value = '';
        input.focus();
    }

    hideJsonFieldModal() {
        document.getElementById('json-field-modal').classList.add('hidden');
        this.pendingTopicFromBrowser = null;
        this.selectedJsonField = '';
    }

    confirmJsonFieldModal() {
        if (this.pendingTopicFromBrowser) {
            let field = '';

            // Check if JSON viewer is visible (field was selected by clicking)
            if (!document.getElementById('modal-json-section').classList.contains('hidden')) {
                field = this.selectedJsonField;
            }
            // Check if simple input is visible
            else if (!document.getElementById('modal-simple-section').classList.contains('hidden')) {
                field = document.getElementById('modal-json-field').value.trim();
            }
            // Raw section means no field extraction needed

            this.addTopic(this.pendingTopicFromBrowser, field);
            this.hideJsonFieldModal();
        }
    }

    useRawValue() {
        if (this.pendingTopicFromBrowser) {
            this.addTopic(this.pendingTopicFromBrowser, '');
            this.hideJsonFieldModal();
        }
    }
}

/**
 * Topic Browser Side Panel for Topic Visualizer
 */
class TopicChartSidePanel {
    static instance = null;

    constructor() {
        this.panel = document.getElementById('topic-browser-panel');
        this.tree = document.getElementById('topic-panel-tree-root');
        this.archiveGroupSelect = document.getElementById('topic-panel-archive-group');
        this.toggleButton = document.getElementById('topic-browser-floating-toggle');
        this.resizeHandle = document.getElementById('topic-panel-resize-handle');

        this.treeNodes = new Map();
        this.selectedArchiveGroup = 'Default';
        this.isOpen = false;
        this.panelWidth = 380;

        this.init();
    }

    async init() {
        this.archiveGroupSelect.addEventListener('change', (e) => {
            this.selectedArchiveGroup = e.target.value;
            this.browseRoot();
        });

        this.createRootNode();
        this.setupResize();
    }

    static toggle() {
        if (TopicChartSidePanel.instance) {
            TopicChartSidePanel.instance.toggle();
        }
    }

    static close() {
        if (TopicChartSidePanel.instance) {
            TopicChartSidePanel.instance.close();
        }
    }

    static open() {
        if (TopicChartSidePanel.instance) {
            TopicChartSidePanel.instance.open();
        }
    }

    toggle() {
        if (this.isOpen) {
            this.close();
        } else {
            this.open();
        }
    }

    open() {
        this.panel.classList.add('open');
        this.toggleButton.classList.add('active');
        this.updateMainContentMargin();
        this.isOpen = true;

        // Sync archive group with main selector
        const mainArchiveGroup = document.getElementById('archive-group-select').value;
        if (mainArchiveGroup) {
            this.selectedArchiveGroup = mainArchiveGroup;
            this.archiveGroupSelect.value = mainArchiveGroup;
            this.browseRoot();
        }
    }

    close() {
        this.panel.classList.remove('open');
        this.toggleButton.classList.remove('active');
        const mainContent = document.querySelector('.main-content');
        if (mainContent) {
            mainContent.style.marginRight = '0';
        }
        this.isOpen = false;
    }

    updateMainContentMargin() {
        const mainContent = document.querySelector('.main-content');
        if (mainContent) {
            mainContent.style.marginRight = `${this.panelWidth}px`;
        }
    }

    setupResize() {
        let isResizing = false;
        let startX = 0;
        let startWidth = 0;

        this.resizeHandle.addEventListener('mousedown', (e) => {
            isResizing = true;
            startX = e.clientX;
            startWidth = this.panel.offsetWidth;
            document.body.style.cursor = 'col-resize';
            document.body.style.userSelect = 'none';
            e.preventDefault();
        });

        document.addEventListener('mousemove', (e) => {
            if (!isResizing) return;
            const deltaX = startX - e.clientX;
            const newWidth = Math.max(300, Math.min(600, startWidth + deltaX));
            this.panelWidth = newWidth;
            this.panel.style.width = `${newWidth}px`;
            if (this.isOpen) {
                this.updateMainContentMargin();
            }
        });

        document.addEventListener('mouseup', () => {
            if (isResizing) {
                isResizing = false;
                document.body.style.cursor = '';
                document.body.style.userSelect = '';
            }
        });
    }

    browseRoot() {
        this.tree.innerHTML = '';
        this.treeNodes.clear();
        this.createRootNode();
    }

    createRootNode() {
        this.loadTopicLevel('+', this.tree, '');
    }

    async loadTopicLevel(pattern, container, parentPath = '') {
        try {
            const loadingItem = this.createLoadingItem();
            container.appendChild(loadingItem);

            const query = `
                query BrowseTopics($topic: String!, $archiveGroup: String!) {
                    browseTopics(topic: $topic, archiveGroup: $archiveGroup) {
                        name
                    }
                }
            `;

            const response = await graphqlClient.query(query, {
                topic: pattern,
                archiveGroup: this.selectedArchiveGroup
            });

            container.removeChild(loadingItem);

            if (response && response.browseTopics && response.browseTopics.length > 0) {
                const topics = response.browseTopics;
                const topicList = topics.map(topic => ({
                    topic: topic.name,
                    hasValue: true
                }));

                const groupedTopics = this.groupTopicsByLevel(topicList, parentPath);

                if (groupedTopics.size === 0) {
                    const emptyItem = document.createElement('li');
                    emptyItem.className = 'tree-node';
                    emptyItem.innerHTML = '<div class="tree-item" style="color: var(--text-muted); font-style: italic;">No topics found</div>';
                    container.appendChild(emptyItem);
                    return;
                }

                for (const [levelName, topicData] of groupedTopics) {
                    const fullPath = parentPath ? `${parentPath}/${levelName}` : levelName;
                    const treeItem = this.createTreeItem(levelName, fullPath, topicData.hasValue, topicData.hasChildren);
                    container.appendChild(treeItem);
                }
            } else {
                const emptyItem = document.createElement('li');
                emptyItem.className = 'tree-node';
                emptyItem.innerHTML = '<div class="tree-item" style="color: var(--text-muted); font-style: italic;">No topics found</div>';
                container.appendChild(emptyItem);
            }
        } catch (error) {
            console.error('Error loading topic level:', error);
            const loadingItems = container.querySelectorAll('.loading-item');
            loadingItems.forEach(item => container.removeChild(item));
            const errorItem = this.createErrorItem(error.message);
            container.appendChild(errorItem);
        }
    }

    groupTopicsByLevel(topics, parentPath) {
        const grouped = new Map();
        const parentLevels = parentPath ? parentPath.split('/').length : 0;

        for (const topic of topics) {
            const levels = topic.topic.split('/');

            if (parentLevels === 0) {
                const topLevel = levels[0];
                const hasChildren = true;
                const hasValue = levels.length === 1 && topic.hasValue;

                if (!grouped.has(topLevel)) {
                    grouped.set(topLevel, { hasValue, hasChildren });
                } else {
                    const existing = grouped.get(topLevel);
                    existing.hasValue = existing.hasValue || hasValue;
                    existing.hasChildren = existing.hasChildren || hasChildren;
                }
            } else if (levels.length > parentLevels) {
                const nextLevel = levels[parentLevels];
                const hasChildren = true;
                const hasValue = levels.length === parentLevels + 1 && topic.hasValue;

                if (!grouped.has(nextLevel)) {
                    grouped.set(nextLevel, { hasValue, hasChildren });
                } else {
                    const existing = grouped.get(nextLevel);
                    existing.hasValue = existing.hasValue || hasValue;
                    existing.hasChildren = existing.hasChildren || hasChildren;
                }
            }
        }

        return grouped;
    }

    createTreeItem(name, fullPath, hasValue, hasChildren) {
        const li = document.createElement('li');
        li.className = 'tree-node';

        const item = document.createElement('div');
        item.className = 'tree-item';
        if (hasValue) {
            item.classList.add('has-data');
        }

        const toggle = document.createElement('button');
        toggle.className = 'tree-toggle';
        toggle.innerHTML = hasChildren ? '&#9654;' : '';

        const icon = document.createElement('span');
        icon.className = 'tree-icon';
        if (hasChildren) {
            icon.className += ' folder';
            icon.innerHTML = '&#128193;';
        } else {
            icon.className += ' topic';
            icon.innerHTML = '&#128196;';
        }

        const nameSpan = document.createElement('span');
        nameSpan.textContent = name;

        item.appendChild(toggle);
        item.appendChild(icon);
        item.appendChild(nameSpan);
        li.appendChild(item);

        const nodeData = {
            element: li,
            item,
            toggle,
            expanded: false,
            hasChildren,
            hasValue,
            fullPath,
            name
        };

        this.treeNodes.set(fullPath, nodeData);

        if (hasChildren) {
            toggle.addEventListener('click', (e) => {
                e.stopPropagation();
                this.toggleNode(fullPath);
            });
        }

        // Click to add topic to chart
        item.addEventListener('click', (e) => {
            e.stopPropagation();
            if (hasValue) {
                // Show modal to ask for JSON field, passing the panel's selected archive group
                topicChartManager.showJsonFieldModal(fullPath, this.selectedArchiveGroup);
            } else if (hasChildren) {
                this.toggleNode(fullPath);
            }
        });

        return li;
    }

    async toggleNode(fullPath) {
        const nodeData = this.treeNodes.get(fullPath);
        if (!nodeData || !nodeData.hasChildren) return;

        if (nodeData.expanded) {
            const childContainer = nodeData.element.querySelector('.tree-children');
            if (childContainer) {
                childContainer.classList.add('collapsed');
            }
            nodeData.toggle.classList.remove('expanded');
            nodeData.expanded = false;
        } else {
            let childContainer = nodeData.element.querySelector('.tree-children');
            if (!childContainer) {
                childContainer = document.createElement('ul');
                childContainer.className = 'tree-children';
                nodeData.element.appendChild(childContainer);

                const pattern = fullPath === 'root' ? '+' : `${fullPath}/+`;
                await this.loadTopicLevel(pattern, childContainer, fullPath === 'root' ? '' : fullPath);
            }

            childContainer.classList.remove('collapsed');
            nodeData.toggle.classList.add('expanded');
            nodeData.expanded = true;
        }
    }

    createLoadingItem() {
        const li = document.createElement('li');
        li.className = 'tree-node loading-item';
        li.innerHTML = '<div class="tree-item" style="color: var(--text-muted); font-style: italic;">Loading...</div>';
        return li;
    }

    createErrorItem(message) {
        const li = document.createElement('li');
        li.className = 'tree-node';
        li.innerHTML = `<div class="tree-item" style="color: var(--monster-red); font-style: italic;">Error: ${message}</div>`;
        return li;
    }
}

// Initialize when DOM is ready
let topicChartManager;
document.addEventListener('DOMContentLoaded', () => {
    topicChartManager = new TopicChartManager();
    TopicChartSidePanel.instance = new TopicChartSidePanel();
});
