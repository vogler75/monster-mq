class ArchiveGroupDetailManager {
    constructor() {
        this.groupName = null;
        this.isNew = false;
        this.groupData = null;
        this.databaseConnections = [];
        this.connectionNamesByType = {};
        this.redisServerEnabled = false;
        this.chart = null;
        this.init();
    }

    async init() {
        if (!window.isLoggedIn()) {
            window.location.href = '/pages/login.html';
            return;
        }

        await this.loadEnabledFeatures();
        this.updateRedisDbNumberVisibility();
        await this.filterStorageOptions();

        const urlParams = new URLSearchParams(window.location.search);
        this.isNew = urlParams.get('new') === 'true';
        this.groupName = urlParams.get('name');

        if (this.isNew) {
            document.getElementById('page-title').textContent = 'New Archive Group';
            document.getElementById('page-subtitle').textContent = 'Create a new archive group';
            return;
        }

        if (!this.groupName) {
            this.showError('No archive group specified in URL.');
            return;
        }

        this.showLoading(true);
        try {
            await this.loadData();
            this.renderForm();
        } catch (error) {
            this.showError('Failed to load archive group: ' + error.message);
        } finally {
            this.showLoading(false);
        }

        window.registerPageCleanup?.(() => this.cleanup());
        window.addEventListener('resize', this._resizeHandler = () => {
            if (this.chart) this.chart.resize();
        });
    }

    async loadEnabledFeatures() {
        try {
            const features = await window.graphqlClient.getEnabledFeatures();
            this.redisServerEnabled = Array.isArray(features) && features.includes('RedisServer');
        } catch (e) {
            console.warn('Failed to fetch enabled features for archive group detail:', e);
            this.redisServerEnabled = false;
        }
    }

    updateRedisDbNumberVisibility() {
        const input = document.getElementById('group-redis-db-number');
        const group = input?.closest('.form-group');
        if (!input || !group) return;

        group.style.display = this.redisServerEnabled ? '' : 'none';
        input.disabled = !this.redisServerEnabled;
        if (!this.redisServerEnabled) input.value = '';
    }

    async loadData() {
        const redisDbNumberField = this.redisServerEnabled ? 'redisDbNumber' : '';
        const result = await window.graphqlClient.query(`
            query GetArchiveGroup($name: String!) {
                archiveGroup(name: $name) {
                    name
                    enabled
                    deployed
                    deploymentId
                    topicFilter
                    retainedOnly
                    lastValType
                    archiveType
                    databaseConnectionName
                    ${redisDbNumberField}
                    payloadFormat
                    lastValRetention
                    archiveRetention
                    purgeInterval
                    createdAt
                    updatedAt
                    connectionStatus {
                        nodeId
                        messageArchive
                        lastValueStore
                    }
                }
            }
        `, { name: this.groupName });

        this.groupData = result.archiveGroup;
        if (!this.groupData) {
            throw new Error('Archive group not found');
        }
    }

    renderForm() {
        const g = this.groupData;

        document.getElementById('page-title').textContent = g.name;
        document.getElementById('page-subtitle').textContent = 'Edit archive group';

        // Basic settings
        document.getElementById('group-name').value = g.name;
        document.getElementById('group-name').disabled = true;
        document.getElementById('group-topic-filter').value = (g.topicFilter || []).join('\n');
        document.getElementById('group-retained-only').checked = !!g.retainedOnly;

        // Storage
        document.getElementById('group-last-val-type').value = g.lastValType || 'NONE';
        document.getElementById('group-archive-type').value = g.archiveType || 'NONE';
        document.getElementById('group-payload-format').value = g.payloadFormat || 'DEFAULT';
        this.updateRedisDbNumberVisibility();
        if (this.redisServerEnabled) {
            document.getElementById('group-redis-db-number').value = g.redisDbNumber ?? '';
        }
        this.updateDatabaseConnectionOptions();

        // Retention
        document.getElementById('group-last-val-retention').value = g.lastValRetention || '';
        document.getElementById('group-archive-retention').value = g.archiveRetention || '';
        document.getElementById('group-purge-interval').value = g.purgeInterval || '';

        this.updateLastValRetentionHelp();

        // Status badge
        const badge = document.getElementById('status-badge');
        badge.style.display = 'inline-flex';
        badge.className = 'status-badge ' + (g.enabled ? 'status-enabled' : 'status-disabled');
        badge.textContent = g.enabled ? 'ENABLED' : 'DISABLED';

        // Header buttons
        document.getElementById('delete-btn').style.display = 'inline-flex';
        document.getElementById('delete-group-name').textContent = g.name;

        const toggleBtn = document.getElementById('toggle-btn');
        toggleBtn.style.display = 'inline-flex';
        toggleBtn.textContent = g.enabled ? 'Disable' : 'Enable';
        toggleBtn.className = 'btn ' + (g.enabled ? 'btn-warning' : 'btn-success');

        // Status section
        document.getElementById('status-section').style.display = 'block';
        document.getElementById('status-enabled').textContent = g.enabled ? 'Yes' : 'No';
        document.getElementById('status-deployed').textContent = g.deployed ? 'Yes' : 'No';
        document.getElementById('status-created-at').textContent = g.createdAt ? new Date(g.createdAt).toLocaleString() : '-';
        document.getElementById('status-updated-at').textContent = g.updatedAt ? new Date(g.updatedAt).toLocaleString() : '-';

        // Connection status
        const indicators = document.getElementById('connection-indicators');
        if (g.connectionStatus && g.connectionStatus.length > 0) {
            indicators.innerHTML = g.connectionStatus.map(node => `
                <div class="connection-item">
                    <span class="connection-dot ${node.messageArchive ? 'connected' : 'disconnected'}"></span>
                    <span class="connection-label">${this.escapeHtml(node.nodeId)} (Archive)</span>
                </div>
                <div class="connection-item">
                    <span class="connection-dot ${node.lastValueStore ? 'connected' : 'disconnected'}"></span>
                    <span class="connection-label">${this.escapeHtml(node.nodeId)} (Last Val)</span>
                </div>
            `).join('');
        } else {
            indicators.innerHTML = '<span style="color: var(--text-muted); font-size: 0.85rem;">No connection data available</span>';
        }

        // Setup lazy-loaded Archive Analytics section
        const analyticsSection = document.getElementById('analytics-section');
        if (analyticsSection) {
            if (!this.isNew && g.archiveType !== 'NONE') {
                analyticsSection.style.display = 'block';
                document.getElementById('analytics-trigger-container').style.display = 'flex';
                document.getElementById('analytics-details-container').style.display = 'none';
            } else {
                analyticsSection.style.display = 'none';
            }
        }
    }

    async filterStorageOptions() {
        try {
            const result = await window.graphqlClient.query(`
                query { brokerConfig {
                    clustered postgresUrl crateDbUrl mongoDbUrl sqlitePath kafkaServers
                }
                databaseConnections {
                    name type url username database readOnly
                } }
            `);
            const cfg = result.brokerConfig;
            this.databaseConnections = result.databaseConnections || [];

            const lastValAllowed = new Set(['NONE', 'MEMORY']);
            if (cfg.clustered) lastValAllowed.add('HAZELCAST');
            if (cfg.postgresUrl || this.databaseConnections.some(conn => conn.type === 'POSTGRES')) lastValAllowed.add('POSTGRES');
            if (cfg.crateDbUrl) lastValAllowed.add('CRATEDB');
            if (cfg.mongoDbUrl || this.databaseConnections.some(conn => conn.type === 'MONGODB')) lastValAllowed.add('MONGODB');
            if (cfg.sqlitePath) lastValAllowed.add('SQLITE');

            const archiveAllowed = new Set(['NONE']);
            if (cfg.postgresUrl || this.databaseConnections.some(conn => conn.type === 'POSTGRES')) archiveAllowed.add('POSTGRES');
            if (cfg.crateDbUrl) archiveAllowed.add('CRATEDB');
            if (cfg.mongoDbUrl || this.databaseConnections.some(conn => conn.type === 'MONGODB')) archiveAllowed.add('MONGODB');
            if (cfg.sqlitePath) archiveAllowed.add('SQLITE');

            const filterSelect = (id, allowed) => {
                const select = document.getElementById(id);
                if (!select) return;
                for (const opt of Array.from(select.options)) {
                    if (!allowed.has(opt.value)) opt.remove();
                }
            };

            filterSelect('group-last-val-type', lastValAllowed);
            filterSelect('group-archive-type', archiveAllowed);

            const lastValSelect = document.getElementById('group-last-val-type');
            const archiveSelect = document.getElementById('group-archive-type');
            if (lastValSelect) lastValSelect.addEventListener('change', () => this.updateDatabaseConnectionOptions());
            if (archiveSelect) archiveSelect.addEventListener('change', () => this.updateDatabaseConnectionOptions());
            await this.updateDatabaseConnectionOptions();
        } catch (e) {
            console.warn('Failed to fetch broker config for storage filtering:', e);
        }
    }

    async loadConnectionNames(type) {
        if (this.connectionNamesByType[type]) return this.connectionNamesByType[type];
        const result = await window.graphqlClient.query(`
            query GetDatabaseConnectionNames($type: DatabaseConnectionType!) {
                databaseConnectionNames(type: $type)
            }
        `, { type });
        this.connectionNamesByType[type] = result.databaseConnectionNames || [];
        return this.connectionNamesByType[type];
    }

    async updateDatabaseConnectionOptions() {
        const group = document.getElementById('database-connection-group');
        const select = document.getElementById('group-database-connection');
        if (!group || !select) return;

        const lastValType = document.getElementById('group-last-val-type')?.value;
        const archiveType = document.getElementById('group-archive-type')?.value;
        const requiredTypes = new Set();
        if (lastValType === 'POSTGRES' || archiveType === 'POSTGRES') requiredTypes.add('POSTGRES');
        if (lastValType === 'MONGODB' || archiveType === 'MONGODB') requiredTypes.add('MONGODB');
        if (lastValType === 'SQLITE' || archiveType === 'SQLITE') requiredTypes.add('SQLITE');

        if (requiredTypes.size === 0) {
            group.style.display = 'none';
            select.value = '';
            return;
        }

        if (requiredTypes.size > 1) {
            group.style.display = 'none';
            select.innerHTML = '<option value="">Config-file defaults</option>';
            select.value = '';
            return;
        }

        group.style.display = 'block';
        const current = select.value || this.groupData?.databaseConnectionName || '';
        select.innerHTML = '<option value="">Select connection...</option>';

        const requiredType = Array.from(requiredTypes)[0];
        try {
            const names = await this.loadConnectionNames(requiredType);
            names.forEach(name => {
                const option = document.createElement('option');
                option.value = name;
                option.textContent = name;
                select.appendChild(option);
            });
            if (current && names.includes(current)) {
                select.value = current;
            } else if (!current && names.includes('Default')) {
                select.value = 'Default';
            }
        } catch (e) {
            console.warn('Failed to load database connection names:', e);
        }
    }

    collectFormData() {
        const topicFilterText = document.getElementById('group-topic-filter').value.trim();
        const topicFilter = topicFilterText.split('\n').map(l => l.trim()).filter(l => l.length > 0);

        const data = {
            name: this.isNew ? document.getElementById('group-name').value.trim() : this.groupName,
            topicFilter,
            lastValType: document.getElementById('group-last-val-type').value,
            archiveType: document.getElementById('group-archive-type').value,
            databaseConnectionName: document.getElementById('group-database-connection').value || null,
            payloadFormat: document.getElementById('group-payload-format').value,
            retainedOnly: document.getElementById('group-retained-only').checked,
            lastValRetention: document.getElementById('group-last-val-retention').value.trim() || null,
            archiveRetention: document.getElementById('group-archive-retention').value.trim() || null,
            purgeInterval: document.getElementById('group-purge-interval').value.trim() || null,
        };

        if (this.redisServerEnabled) {
            const redisDbNumberText = document.getElementById('group-redis-db-number').value.trim();
            data.redisDbNumber = redisDbNumberText === '' ? null : Number(redisDbNumberText);
        }

        return data;
    }

    validateFormData(data) {
        if (!data.name) {
            this.showError('Name is required.');
            return false;
        }
        const nameError = window.validateNameInput(data.name, 'Archive group');
        if (this.isNew && nameError) {
            this.showError(nameError);
            return false;
        }
        if (data.topicFilter.length === 0) {
            this.showError('At least one topic filter is required.');
            return false;
        }
        if (!data.lastValType) {
            this.showError('Last Value Type is required.');
            return false;
        }
        if (!data.archiveType) {
            this.showError('Archive Type is required.');
            return false;
        }
        const dbTypes = new Set();
        if (data.lastValType === 'POSTGRES' || data.archiveType === 'POSTGRES') dbTypes.add('POSTGRES');
        if (data.lastValType === 'MONGODB' || data.archiveType === 'MONGODB') dbTypes.add('MONGODB');
        if (dbTypes.size > 1 && data.databaseConnectionName && data.databaseConnectionName !== 'Default') {
            this.showError('Mixed PostgreSQL and MongoDB storage cannot use a named database connection.');
            return false;
        }
        if (dbTypes.size === 1 && !data.databaseConnectionName) {
            this.showError('Database Connection is required for PostgreSQL and MongoDB archive storage.');
            return false;
        }
        if (this.redisServerEnabled && data.redisDbNumber !== null && (!Number.isInteger(data.redisDbNumber) || data.redisDbNumber < 0)) {
            this.showError('Redis DB Number must be a non-negative integer.');
            return false;
        }
        // Validate lastValRetention format with MEMORY store
        if (data.lastValType === 'MEMORY' && data.lastValRetention) {
            if (/^[0-9]+(d|h|m|s|w|M|y)$/.test(data.lastValRetention)) {
                this.showError('MEMORY store does not support time-based retention. Use size-based format, e.g. "50k" or "100000k".');
                return false;
            }
            if (!data.lastValRetention.endsWith('k')) {
                this.showError('MEMORY store requires size-based retention format, e.g. "50k" or "100000k".');
                return false;
            }
        }
        return true;
    }

    async saveGroup() {
        const data = this.collectFormData();
        if (!this.validateFormData(data)) return;

        try {
            let result;
            if (this.isNew) {
                const input = { ...data };
                if (!input.lastValRetention) delete input.lastValRetention;
                if (!input.archiveRetention) delete input.archiveRetention;
                if (!input.purgeInterval) delete input.purgeInterval;
                if (!input.databaseConnectionName) delete input.databaseConnectionName;
                if (!this.redisServerEnabled || input.redisDbNumber === null) delete input.redisDbNumber;

                result = await window.graphqlClient.query(`
                    mutation CreateArchiveGroup($input: CreateArchiveGroupInput!) {
                        archiveGroup {
                            create(input: $input) {
                                success
                                message
                            }
                        }
                    }
                `, { input });

                if (result.archiveGroup.create.success) {
                    this.showSuccess('Archive group created successfully.');
                    setTimeout(() => { window.spaLocation.href = '/pages/archive-group-detail.html?name=' + encodeURIComponent(data.name); }, 1000);
                } else {
                    this.showError(result.archiveGroup.create.message || 'Failed to create archive group.');
                }
            } else {
                result = await window.graphqlClient.query(`
                    mutation UpdateArchiveGroup($input: UpdateArchiveGroupInput!) {
                        archiveGroup {
                            update(input: $input) {
                                success
                                message
                            }
                        }
                    }
                `, { input: data });

                if (result.archiveGroup.update.success) {
                    this.showSuccess('Archive group saved successfully.');
                    await this.loadData();
                    this.renderForm();
                } else {
                    this.showError(result.archiveGroup.update.message || 'Failed to update archive group.');
                }
            }
        } catch (error) {
            this.showError('Failed to save archive group: ' + error.message);
        }
    }

    async toggleEnabled() {
        if (!this.groupData) return;
        const enable = !this.groupData.enabled;
        const methodName = enable ? 'enable' : 'disable';
        try {
            const result = await window.graphqlClient.query(`
                mutation ToggleArchiveGroup($name: String!) {
                    archiveGroup {
                        ${methodName}(name: $name) {
                            success
                            message
                        }
                    }
                }
            `, { name: this.groupName });

            if (result.archiveGroup[methodName].success) {
                this.showSuccess(`Archive group ${enable ? 'enabled' : 'disabled'}.`);
                await this.loadData();
                this.renderForm();
            } else {
                this.showError(result.archiveGroup[methodName].message || `Failed to ${methodName} archive group.`);
            }
        } catch (error) {
            this.showError(`Failed to ${methodName} archive group: ` + error.message);
        }
    }

    showDeleteModal() {
        document.getElementById('delete-modal').style.display = 'flex';
    }

    hideDeleteModal() {
        document.getElementById('delete-modal').style.display = 'none';
    }

    async confirmDelete() {
        this.hideDeleteModal();
        try {
            const result = await window.graphqlClient.query(`
                mutation DeleteArchiveGroup($name: String!) {
                    archiveGroup {
                        delete(name: $name) {
                            success
                            message
                        }
                    }
                }
            `, { name: this.groupName });

            if (result.archiveGroup.delete.success) {
                this.showSuccess('Archive group deleted.');
                setTimeout(() => this.goBack(), 800);
            } else {
                this.showError(result.archiveGroup.delete.message || 'Failed to delete archive group.');
            }
        } catch (error) {
            this.showError('Failed to delete archive group: ' + error.message);
        }
    }

    goBack() {
        window.spaLocation.href = '/pages/archive-groups.html';
    }

    updateLastValRetentionHelp() {
        const lastValType = document.getElementById('group-last-val-type').value;
        const help = document.getElementById('last-val-retention-help');
        if (!help) return;
        if (lastValType === 'MEMORY') {
            help.textContent = 'Size-based format required for MEMORY store (e.g., 50k, 100000k = number of entries)';
        } else if (lastValType === 'NONE') {
            help.textContent = 'No retention needed for NONE store';
        } else if (['POSTGRES', 'CRATEDB', 'MONGODB', 'SQLITE', 'HAZELCAST'].includes(lastValType)) {
            help.textContent = 'Time-based format (e.g., 7d, 24h, 60m, 1y)';
        } else {
            help.textContent = 'Size-based (MEMORY): 50k, 100000k | Time-based (others): 7d, 24h, 60m';
        }
    }

    showLoading(show) {
        const el = document.getElementById('loading-indicator');
        if (el) el.style.display = show ? 'flex' : 'none';
    }

    showError(msg) {
        const el = document.getElementById('error-message');
        if (el) {
            el.querySelector('.error-text').textContent = msg;
            el.style.display = 'block';
        }
    }

    showSuccess(msg) {
        var existing = document.getElementById('success-toast'); if (existing) existing.remove();
        var toast = document.createElement('div'); toast.id = 'success-toast';
        toast.style.cssText = 'position:fixed;top:20px;left:50%;transform:translateX(-50%);background:var(--monster-green,#10B981);color:#fff;padding:14px 24px;border-radius:8px;box-shadow:0 4px 12px rgba(0,0,0,0.4);z-index:10000;font-size:0.9rem;max-width:600px;display:flex;align-items:center;gap:10px;animation:slideDown 0.3s ease-out;';
        toast.innerHTML = '<span style="font-size:1.2rem;">&#10003;</span><span>' + this.escapeHtml(msg) + '</span><button onclick="this.parentElement.remove()" style="background:none;border:none;color:#fff;cursor:pointer;margin-left:auto;font-size:1.1rem;line-height:1;padding:0 4px;">&times;</button>';
        if (!document.getElementById('toast-anim-style')) { var s = document.createElement('style'); s.id = 'toast-anim-style'; s.textContent = '@keyframes slideDown{from{transform:translateX(-50%) translateY(-100%);opacity:0;}to{transform:translateX(-50%) translateY(0);opacity:1;}}@keyframes fadeOut{from{opacity:1;}to{opacity:0;}}'; document.head.appendChild(s); }
        document.body.appendChild(toast);
        setTimeout(function() { if (toast.parentElement) { toast.style.animation = 'fadeOut 0.3s ease-out forwards'; setTimeout(function() { if (toast.parentElement) toast.remove(); }, 300); } }, 3000);
    }

    escapeHtml(unsafe) {
        if (typeof unsafe !== 'string') return unsafe;
        return unsafe
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;')
            .replace(/'/g, '&#039;');
    }

    async loadStats() {
        const archiveType = this.groupData?.archiveType || 'NONE';
        const analyticsSection = document.getElementById('analytics-section');
        if (!analyticsSection) return;

        if (this.isNew || archiveType === 'NONE') {
            analyticsSection.style.display = 'none';
            return;
        }

        analyticsSection.style.display = 'block';

        // Check and default date inputs if empty
        const startInput = document.getElementById('stats-start-date');
        const endInput = document.getElementById('stats-end-date');
        
        if (startInput && endInput && (!startInput.value || !endInput.value)) {
            const today = new Date();
            const getLocalDateStr = (d) => {
                const year = d.getFullYear();
                const month = String(d.getMonth() + 1).padStart(2, '0');
                const day = String(d.getDate()).padStart(2, '0');
                return `${year}-${month}-${day}`;
            };
            
            const endStr = getLocalDateStr(today);
            
            const sevenDaysAgo = new Date(today);
            sevenDaysAgo.setDate(today.getDate() - 7);
            const startStr = getLocalDateStr(sevenDaysAgo);
            
            startInput.value = startStr;
            endInput.value = endStr;
        }

        const startTime = startInput && startInput.value ? startInput.value + 'T00:00:00Z' : null;
        const endTime = endInput && endInput.value ? endInput.value + 'T23:59:59Z' : null;

        // Hide trigger, show details container
        const triggerContainer = document.getElementById('analytics-trigger-container');
        const detailsContainer = document.getElementById('analytics-details-container');
        if (triggerContainer) triggerContainer.style.display = 'none';
        if (detailsContainer) detailsContainer.style.display = 'block';

        // Toggles buttons to loading/calculating state
        const loadBtn = document.getElementById('btn-load-analytics');
        const applyBtn = document.getElementById('btn-apply-range');
        if (loadBtn) {
            loadBtn.disabled = true;
            loadBtn.textContent = 'Calculating...';
        }
        if (applyBtn) {
            applyBtn.disabled = true;
            applyBtn.textContent = 'Calculating...';
        }

        // Show ECharts native spinner if initialized, else page-level spinner
        if (this.chart) {
            this.chart.showLoading({
                text: 'Calculating Statistics...',
                color: '#7c3aed',
                textColor: '#8b8ba7',
                maskColor: 'rgba(23, 23, 37, 0.8)',
                zlevel: 0
            });
        } else {
            this.showLoading(true);
        }

        try {
            const result = await window.graphqlClient.query(`
                query GetArchiveStats($archiveGroup: String!, $startTime: String, $endTime: String) {
                    archiveStats(archiveGroup: $archiveGroup, startTime: $startTime, endTime: $endTime) {
                        minTimestamp
                        dailyCounts {
                            date
                            count
                        }
                    }
                }
            `, { 
                archiveGroup: this.groupName,
                startTime: startTime,
                endTime: endTime
            });

            const stats = result.archiveStats;
            if (stats) {
                console.log('Archive stats loaded:', stats);
                this.renderStats(stats);
            }
        } catch (e) {
            console.warn('Failed to fetch archive stats:', e);
            document.getElementById('stats-min-timestamp').textContent = 'Error loading stats';
            document.getElementById('stats-total-days').textContent = '-';
        } finally {
            if (loadBtn) {
                loadBtn.disabled = false;
                loadBtn.textContent = 'Load Analytics';
            }
            if (applyBtn) {
                applyBtn.disabled = false;
                applyBtn.textContent = 'Apply Range';
            }
            if (this.chart) {
                this.chart.hideLoading();
            }
            this.showLoading(false);
        }
    }

    renderStats(stats) {
        // Earliest timestamp
        const minTsEl = document.getElementById('stats-min-timestamp');
        if (minTsEl) {
            if (stats.minTimestamp) {
                minTsEl.textContent = new Date(stats.minTimestamp).toLocaleString();
            } else {
                minTsEl.textContent = 'No values archived';
            }
        }

        // Total Days Active
        const totalDaysEl = document.getElementById('stats-total-days');
        if (totalDaysEl) {
            totalDaysEl.textContent = stats.dailyCounts ? stats.dailyCounts.length.toString() : '0';
        }

        // Setup ECharts
        const container = document.getElementById('stats-chart');
        if (!container || typeof echarts === 'undefined') return;

        if (this.chart) {
            this.chart.dispose();
        }

        this.chart = echarts.init(container, null, { renderer: 'canvas' });

        const dailyCounts = stats.dailyCounts || [];
        if (dailyCounts.length === 0) {
            this.chart.setOption({
                title: {
                    text: 'No message history available',
                    left: 'center',
                    top: 'center',
                    textStyle: {
                        color: '#617d91',
                        fontSize: 14,
                        fontWeight: 'normal'
                    }
                },
                backgroundColor: 'transparent'
            });
            return;
        }

        const dates = dailyCounts.map(d => d.date);
        const counts = dailyCounts.map(d => d.count);

        // Premium Dark Gradient styling matching Siemens iX themes
        const option = {
            backgroundColor: 'transparent',
            tooltip: {
                trigger: 'axis',
                axisPointer: {
                    type: 'shadow'
                },
                backgroundColor: 'rgba(23, 23, 37, 0.95)',
                borderColor: '#2e2e4f',
                borderWidth: 1,
                textStyle: {
                    color: '#f0f6fc',
                    fontSize: 12
                },
                formatter: (params) => {
                    const item = params[0];
                    return `<span style="color: var(--text-muted); font-size: 0.75rem;">${item.name}</span><br/>` +
                           `<span style="color: #a78bfa; font-weight: bold;">${Number(item.value).toLocaleString()}</span> messages`;
                }
            },
            grid: {
                top: '10%',
                left: '3%',
                right: '4%',
                bottom: '5%',
                containLabel: true
            },
            xAxis: {
                type: 'category',
                data: dates,
                axisLine: {
                    lineStyle: {
                        color: '#2e2e4f'
                    }
                },
                axisLabel: {
                    color: '#8b8ba7',
                    fontSize: 10,
                    margin: 12
                },
                axisTick: {
                    show: false
                }
            },
            yAxis: {
                type: 'value',
                splitLine: {
                    lineStyle: {
                        color: 'rgba(46, 46, 79, 0.3)',
                        type: 'dashed'
                    }
                },
                axisLine: {
                    show: false
                },
                axisLabel: {
                    color: '#8b8ba7',
                    fontSize: 10
                }
            },
            series: [{
                name: 'Volume',
                type: 'bar',
                barWidth: '60%',
                data: counts,
                itemStyle: {
                    color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [
                        { offset: 0, color: '#7c3aed' },
                        { offset: 1, color: '#0d9488' }
                    ]),
                    borderRadius: [4, 4, 0, 0]
                },
                emphasis: {
                    itemStyle: {
                        color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [
                            { offset: 0, color: '#a78bfa' },
                            { offset: 1, color: '#14b8a6' }
                        ])
                    }
                }
            }]
        };

        this.chart.setOption(option);
    }

    cleanup() {
        if (this.chart) {
            this.chart.dispose();
            this.chart = null;
        }
        if (this._resizeHandler) {
            window.removeEventListener('resize', this._resizeHandler);
        }
    }
}

document.addEventListener('DOMContentLoaded', () => {
    window.archiveGroupDetail = new ArchiveGroupDetailManager();
});
