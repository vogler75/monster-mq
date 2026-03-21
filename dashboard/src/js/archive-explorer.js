// MonsterMQ Dashboard — Archive Explorer

var ArchiveExplorerManager = class ArchiveExplorerManager {
    constructor() {
        this.messages = [];
        this.selectedIndex = -1;
        this.visibleColumns = new Set(['timestamp', 'topic', 'payload', 'qos']);
        this.columnWidths = { timestamp: 210, qos: 50 };
        this.allColumns = [
            { key: 'timestamp', label: 'Timestamp' },
            { key: 'topic', label: 'Topic' },
            { key: 'payload', label: 'Payload' },
            { key: 'qos', label: 'QoS' },
            { key: 'clientId', label: 'Client ID' },
            { key: 'contentType', label: 'Content Type' },
            { key: 'responseTopic', label: 'Response Topic' },
            { key: 'payloadFormatIndicator', label: 'Format Indicator' },
            { key: 'userProperties', label: 'User Properties' }
        ];
    }

    init() {
        if (typeof isLoggedIn === 'function' && !isLoggedIn()) {
            window.location.href = '/pages/login.html';
            return;
        }

        this.setDefaultTimeRange();
        this.loadArchiveGroups();
        this.buildColumnsMenu();
        this.renderTableHeader();
        this.setupEventListeners();
        this.setupDropTarget();
    }

    setDefaultTimeRange() {
        const now = new Date();
        const oneHourAgo = new Date(now.getTime() - 60 * 60 * 1000);
        document.getElementById('from-time').value = this.toLocalISOString(oneHourAgo);
        document.getElementById('to-time').value = this.toLocalISOString(now);
    }

    toLocalISOString(date) {
        const pad = n => String(n).padStart(2, '0');
        return date.getFullYear() + '-' + pad(date.getMonth() + 1) + '-' + pad(date.getDate()) +
            'T' + pad(date.getHours()) + ':' + pad(date.getMinutes());
    }

    async loadArchiveGroups() {
        const select = document.getElementById('archive-group-select');
        const panelSelect = document.getElementById('topic-panel-archive-group');
        try {
            const result = await graphqlClient.query('{ archiveGroups { name enabled } }');
            const groups = result?.archiveGroups || [];
            select.innerHTML = '';
            panelSelect.innerHTML = '';
            if (groups.length === 0) {
                select.innerHTML = '<option value="">No archive groups</option>';
                panelSelect.innerHTML = '<option value="">No archive groups</option>';
                return;
            }
            groups.forEach(g => {
                const opt = document.createElement('option');
                opt.value = g.name;
                opt.textContent = g.name + (g.enabled ? '' : ' (disabled)');
                select.appendChild(opt);
                panelSelect.appendChild(opt.cloneNode(true));
            });
        } catch (e) {
            console.error('Failed to load archive groups:', e);
            select.innerHTML = '<option value="">Error loading</option>';
            panelSelect.innerHTML = '<option value="">Error loading</option>';
        }
    }

    setupDropTarget() {
        const topicInput = document.getElementById('topic-filter');
        topicInput.addEventListener('dragover', (e) => {
            e.preventDefault();
            e.dataTransfer.dropEffect = 'copy';
            topicInput.classList.add('drag-over');
        });
        topicInput.addEventListener('dragleave', () => {
            topicInput.classList.remove('drag-over');
        });
        topicInput.addEventListener('drop', (e) => {
            e.preventDefault();
            topicInput.classList.remove('drag-over');
            const topic = e.dataTransfer.getData('text/plain');
            if (topic) {
                topicInput.value = topic;
            }
        });
    }

    buildColumnsMenu() {
        const menu = document.getElementById('columns-menu');
        menu.innerHTML = '';
        this.allColumns.forEach(col => {
            const label = document.createElement('label');
            const cb = document.createElement('input');
            cb.type = 'checkbox';
            cb.checked = this.visibleColumns.has(col.key);
            cb.dataset.col = col.key;
            cb.addEventListener('change', () => this.toggleColumn(col.key, cb.checked));
            label.appendChild(cb);
            label.appendChild(document.createTextNode(col.label));
            menu.appendChild(label);
        });
    }

    setupEventListeners() {
        document.getElementById('btn-query').addEventListener('click', () => this.executeQuery());
        document.getElementById('btn-back').addEventListener('click', () => this.navigateTime(-1));
        document.getElementById('btn-forward').addEventListener('click', () => this.navigateTime(1));
        document.getElementById('detail-close').addEventListener('click', () => this.closeDetail());

        // Columns dropdown toggle
        const colToggle = document.getElementById('columns-toggle');
        const colMenu = document.getElementById('columns-menu');
        colToggle.addEventListener('click', (e) => {
            e.stopPropagation();
            colMenu.classList.toggle('open');
        });
        document.addEventListener('click', () => colMenu.classList.remove('open'));
        colMenu.addEventListener('click', (e) => e.stopPropagation());

        // Enter key on topic filter
        document.getElementById('topic-filter').addEventListener('keydown', (e) => {
            if (e.key === 'Enter') this.executeQuery();
        });

        // Close detail on Escape
        document.addEventListener('keydown', (e) => {
            if (e.key === 'Escape') this.closeDetail();
        });
    }

    async executeQuery() {
        const archiveGroup = document.getElementById('archive-group-select').value;
        const topicFilter = document.getElementById('topic-filter').value || '#';
        const fromTime = document.getElementById('from-time').value;
        const toTime = document.getElementById('to-time').value;
        const maxRows = parseInt(document.getElementById('max-rows').value);
        const format = document.getElementById('format-select').value;

        const tbody = document.getElementById('table-body');
        tbody.innerHTML = '<tr class="loading-row"><td colspan="' + this.visibleColumns.size + '"><div class="loading-spinner"></div> Loading...</td></tr>';

        const query = `query ArchivedMessages($topicFilter: String!, $startTime: String, $endTime: String, $format: DataFormat, $limit: Int, $archiveGroup: String) {
            archivedMessages(
                topicFilter: $topicFilter
                startTime: $startTime
                endTime: $endTime
                format: $format
                limit: $limit
                archiveGroup: $archiveGroup
            ) {
                topic timestamp payload format qos clientId
                contentType responseTopic payloadFormatIndicator
                userProperties { key value }
            }
        }`;

        const variables = {
            topicFilter,
            limit: maxRows,
            format
        };
        if (archiveGroup) variables.archiveGroup = archiveGroup;
        if (fromTime) variables.startTime = new Date(fromTime).toISOString();
        if (toTime) variables.endTime = new Date(toTime).toISOString();

        try {
            const result = await graphqlClient.query(query, variables);
            this.messages = result?.archivedMessages || [];
            this.selectedIndex = -1;
            this.closeDetail();
            this.renderTable();
            document.getElementById('result-count').textContent =
                this.messages.length + ' message' + (this.messages.length !== 1 ? 's' : '') + ' returned';
        } catch (e) {
            console.error('Archive query failed:', e);
            tbody.innerHTML = '<tr><td colspan="' + this.visibleColumns.size + '" class="empty-state">Query failed: ' + this.escapeHtml(e.message) + '</td></tr>';
            document.getElementById('result-count').textContent = '';
        }
    }

    renderTableHeader() {
        const row = document.getElementById('table-header-row');
        row.innerHTML = '';
        const visibleCols = this.allColumns.filter(c => this.visibleColumns.has(c.key));
        visibleCols.forEach(col => {
            const th = document.createElement('th');
            th.textContent = col.label;
            if (col.key === 'timestamp') th.className = 'timestamp-cell';
            th.dataset.col = col.key;

            // Apply saved width
            if (this.columnWidths && this.columnWidths[col.key]) {
                th.style.width = this.columnWidths[col.key] + 'px';
            }

            // Add resize handle
            const handle = document.createElement('div');
            handle.className = 'col-resize-handle';
            th.appendChild(handle);

            this.setupColumnResize(handle, th, col.key);
            row.appendChild(th);
        });
    }

    setupColumnResize(handle, th, colKey) {
        let startX, startWidth;

        const onMouseDown = (e) => {
            e.preventDefault();
            e.stopPropagation();
            startX = e.clientX;
            startWidth = th.offsetWidth;
            handle.classList.add('active');
            document.body.style.cursor = 'col-resize';
            document.body.style.userSelect = 'none';
            document.addEventListener('mousemove', onMouseMove);
            document.addEventListener('mouseup', onMouseUp);
        };

        const onMouseMove = (e) => {
            const newWidth = Math.max(40, startWidth + (e.clientX - startX));
            th.style.width = newWidth + 'px';
        };

        const onMouseUp = () => {
            handle.classList.remove('active');
            document.body.style.cursor = '';
            document.body.style.userSelect = '';
            // Save width
            if (!this.columnWidths) this.columnWidths = {};
            this.columnWidths[colKey] = th.offsetWidth;
            document.removeEventListener('mousemove', onMouseMove);
            document.removeEventListener('mouseup', onMouseUp);
        };

        handle.addEventListener('mousedown', onMouseDown);
    }

    renderTable() {
        this.renderTableHeader();
        const tbody = document.getElementById('table-body');

        if (this.messages.length === 0) {
            tbody.innerHTML = '<tr><td colspan="' + this.visibleColumns.size + '" class="empty-state">No messages found for the given filters.</td></tr>';
            return;
        }

        tbody.innerHTML = '';
        this.messages.forEach((msg, idx) => {
            const tr = document.createElement('tr');
            if (idx === this.selectedIndex) tr.classList.add('selected');
            tr.addEventListener('click', () => this.selectRow(idx));

            this.allColumns.forEach(col => {
                if (!this.visibleColumns.has(col.key)) return;
                const td = document.createElement('td');
                switch (col.key) {
                    case 'timestamp':
                        td.textContent = this.formatTimestamp(msg.timestamp);
                        td.className = 'timestamp-cell';
                        td.title = msg.timestamp;
                        break;
                    case 'topic':
                        td.textContent = msg.topic || '';
                        td.className = 'topic-cell';
                        td.title = msg.topic || '';
                        break;
                    case 'payload':
                        td.textContent = this.truncate(msg.payload || '', 100);
                        td.className = 'payload-cell';
                        td.title = msg.payload || '';
                        break;
                    case 'qos':
                        td.innerHTML = '<span class="qos-badge qos-' + (msg.qos || 0) + '">' + (msg.qos || 0) + '</span>';
                        break;
                    case 'clientId':
                        td.textContent = msg.clientId || '';
                        break;
                    case 'contentType':
                        td.textContent = msg.contentType || '';
                        break;
                    case 'responseTopic':
                        td.textContent = msg.responseTopic || '';
                        break;
                    case 'payloadFormatIndicator':
                        td.textContent = msg.payloadFormatIndicator != null ? msg.payloadFormatIndicator : '';
                        break;
                    case 'userProperties':
                        const ups = msg.userProperties || [];
                        td.textContent = ups.length > 0 ? ups.map(p => p.key + '=' + p.value).join(', ') : '';
                        td.title = td.textContent;
                        break;
                }
                tr.appendChild(td);
            });

            tbody.appendChild(tr);
        });
    }

    selectRow(index) {
        this.selectedIndex = index;
        const msg = this.messages[index];

        // Highlight row
        document.querySelectorAll('#table-body tr').forEach((tr, i) => {
            tr.classList.toggle('selected', i === index);
        });

        // Build detail content
        const body = document.getElementById('detail-body');
        let html = '';

        // Timestamp
        html += this.detailField('Timestamp', this.formatTimestampFull(msg.timestamp) +
            '<br><span style="color:var(--text-muted);font-size:0.75rem;">' + this.relativeTime(msg.timestamp) + '</span>');

        // Topic
        html += this.detailField('Topic', '<span class="mono">' + this.escapeHtml(msg.topic || '') + '</span>');

        // QoS
        html += this.detailField('QoS', '<span class="qos-badge qos-' + (msg.qos || 0) + '">' + (msg.qos || 0) + '</span>');

        // Client ID
        if (msg.clientId) {
            html += this.detailField('Client ID', this.escapeHtml(msg.clientId));
        }

        // Payload
        let payloadDisplay = msg.payload || '';
        try {
            const parsed = JSON.parse(payloadDisplay);
            payloadDisplay = JSON.stringify(parsed, null, 2);
        } catch (_) { /* not JSON, show raw */ }
        html += '<div class="detail-field"><div class="detail-field-label">Payload</div>' +
            '<div class="detail-payload">' + this.escapeHtml(payloadDisplay) + '</div></div>';

        // MQTT v5 fields
        if (msg.contentType) {
            html += this.detailField('Content Type', this.escapeHtml(msg.contentType));
        }
        if (msg.responseTopic) {
            html += this.detailField('Response Topic', '<span class="mono">' + this.escapeHtml(msg.responseTopic) + '</span>');
        }
        if (msg.payloadFormatIndicator != null) {
            html += this.detailField('Payload Format Indicator', msg.payloadFormatIndicator);
        }

        // User Properties
        if (msg.userProperties && msg.userProperties.length > 0) {
            let propsHtml = '<table class="detail-user-props">';
            msg.userProperties.forEach(p => {
                propsHtml += '<tr><td>' + this.escapeHtml(p.key) + '</td><td>' + this.escapeHtml(p.value) + '</td></tr>';
            });
            propsHtml += '</table>';
            html += '<div class="detail-field"><div class="detail-field-label">User Properties</div>' + propsHtml + '</div>';
        }

        // Format
        if (msg.format) {
            html += this.detailField('Format', msg.format);
        }

        body.innerHTML = html;

        // Open panel
        document.getElementById('detail-panel').classList.add('open');
    }

    closeDetail() {
        document.getElementById('detail-panel').classList.remove('open');
        this.selectedIndex = -1;
        document.querySelectorAll('#table-body tr.selected').forEach(tr => tr.classList.remove('selected'));
    }

    toggleColumn(col, visible) {
        if (visible) {
            this.visibleColumns.add(col);
        } else {
            this.visibleColumns.delete(col);
        }
        if (this.messages.length > 0) {
            this.renderTable();
        } else {
            this.renderTableHeader();
        }
    }

    navigateTime(direction) {
        const fromInput = document.getElementById('from-time');
        const toInput = document.getElementById('to-time');
        const from = new Date(fromInput.value);
        const to = new Date(toInput.value);

        if (isNaN(from.getTime()) || isNaN(to.getTime())) return;

        const duration = to.getTime() - from.getTime();
        const shift = duration * direction;

        fromInput.value = this.toLocalISOString(new Date(from.getTime() + shift));
        toInput.value = this.toLocalISOString(new Date(to.getTime() + shift));

        this.executeQuery();
    }

    // --- Helpers ---

    formatTimestamp(ts) {
        if (!ts) return '';
        const d = new Date(ts);
        if (isNaN(d.getTime())) return ts;
        const pad = n => String(n).padStart(2, '0');
        const ms = String(d.getMilliseconds()).padStart(3, '0');
        return pad(d.getDate()) + '.' + pad(d.getMonth() + 1) + '.' + d.getFullYear() + ' ' +
            pad(d.getHours()) + ':' + pad(d.getMinutes()) + ':' + pad(d.getSeconds()) + '.' + ms;
    }

    formatTimestampFull(ts) {
        if (!ts) return '';
        const d = new Date(ts);
        if (isNaN(d.getTime())) return this.escapeHtml(ts);
        const pad = n => String(n).padStart(2, '0');
        return this.escapeHtml(pad(d.getDate()) + '.' + pad(d.getMonth() + 1) + '.' + d.getFullYear() + ' ' +
            pad(d.getHours()) + ':' + pad(d.getMinutes()) + ':' + pad(d.getSeconds()) + '.' + String(d.getMilliseconds()).padStart(3, '0'));
    }

    relativeTime(ts) {
        if (!ts) return '';
        const d = new Date(ts);
        if (isNaN(d.getTime())) return '';
        const diff = Date.now() - d.getTime();
        if (diff < 0) return 'in the future';
        if (diff < 60000) return Math.floor(diff / 1000) + 's ago';
        if (diff < 3600000) return Math.floor(diff / 60000) + 'm ago';
        if (diff < 86400000) return Math.floor(diff / 3600000) + 'h ago';
        return Math.floor(diff / 86400000) + 'd ago';
    }

    truncate(str, max) {
        return str.length > max ? str.substring(0, max) + '...' : str;
    }

    escapeHtml(str) {
        const div = document.createElement('div');
        div.textContent = String(str);
        return div.innerHTML;
    }

    detailField(label, value) {
        return '<div class="detail-field"><div class="detail-field-label">' + label +
            '</div><div class="detail-field-value">' + value + '</div></div>';
    }
};

/**
 * Topic Browser Side Panel for Archive Explorer (with drag-and-drop)
 */
var ExplorerTopicBrowser = class ExplorerTopicBrowser {
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

    init() {
        this.toggleButton.addEventListener('click', () => this.toggle());
        document.getElementById('topic-panel-close').addEventListener('click', () => this.close());

        this.archiveGroupSelect.addEventListener('change', (e) => {
            this.selectedArchiveGroup = e.target.value;
            this.browseRoot();
        });

        this.createRootNode();
        this.setupResize();
    }

    toggle() {
        if (this.isOpen) this.close(); else this.open();
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
        if (mainContent) mainContent.style.marginRight = '0';
        this.isOpen = false;
    }

    updateMainContentMargin() {
        const mainContent = document.querySelector('.main-content');
        if (mainContent) mainContent.style.marginRight = this.panelWidth + 'px';
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
            this.panel.style.width = newWidth + 'px';
            if (this.isOpen) this.updateMainContentMargin();
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

    async loadTopicLevel(pattern, container, parentPath) {
        try {
            const loadingItem = document.createElement('li');
            loadingItem.className = 'tree-node loading-item';
            loadingItem.innerHTML = '<div class="tree-item" style="color:var(--text-muted);font-style:italic;">Loading...</div>';
            container.appendChild(loadingItem);

            const query = 'query BrowseTopics($topic: String!, $archiveGroup: String!) { browseTopics(topic: $topic, archiveGroup: $archiveGroup) { name } }';
            const response = await graphqlClient.query(query, {
                topic: pattern,
                archiveGroup: this.selectedArchiveGroup
            });

            container.removeChild(loadingItem);

            if (response && response.browseTopics && response.browseTopics.length > 0) {
                const topics = response.browseTopics.map(t => ({ topic: t.name, hasValue: true }));
                const grouped = this.groupTopicsByLevel(topics, parentPath);

                if (grouped.size === 0) {
                    container.appendChild(this.emptyItem());
                    return;
                }

                for (const [levelName, topicData] of grouped) {
                    const fullPath = parentPath ? parentPath + '/' + levelName : levelName;
                    container.appendChild(this.createTreeItem(levelName, fullPath, topicData.hasValue, topicData.hasChildren));
                }
            } else {
                container.appendChild(this.emptyItem());
            }
        } catch (error) {
            console.error('Error loading topic level:', error);
            const loadingItems = container.querySelectorAll('.loading-item');
            loadingItems.forEach(item => container.removeChild(item));
            const errItem = document.createElement('li');
            errItem.className = 'tree-node';
            errItem.innerHTML = '<div class="tree-item" style="color:var(--monster-red);font-style:italic;">Error: ' + error.message + '</div>';
            container.appendChild(errItem);
        }
    }

    groupTopicsByLevel(topics, parentPath) {
        const grouped = new Map();
        const parentLevels = parentPath ? parentPath.split('/').length : 0;

        for (const topic of topics) {
            const levels = topic.topic.split('/');
            const targetLevel = parentLevels === 0 ? 0 : parentLevels;

            if (levels.length > targetLevel) {
                const nextLevel = levels[targetLevel];
                const hasChildren = true;
                const hasValue = levels.length === targetLevel + 1 && topic.hasValue;

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
        if (hasValue) item.classList.add('has-data');

        // Make draggable
        item.setAttribute('draggable', 'true');
        item.addEventListener('dragstart', (e) => {
            e.dataTransfer.setData('text/plain', fullPath);
            e.dataTransfer.effectAllowed = 'copy';
        });

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

        this.treeNodes.set(fullPath, { element: li, item, toggle, expanded: false, hasChildren, fullPath });

        if (hasChildren) {
            toggle.addEventListener('click', (e) => {
                e.stopPropagation();
                this.toggleNode(fullPath);
            });
        }

        // Click to set topic filter directly
        item.addEventListener('click', (e) => {
            e.stopPropagation();
            if (hasValue) {
                document.getElementById('topic-filter').value = fullPath;
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
            if (childContainer) childContainer.classList.add('collapsed');
            nodeData.toggle.classList.remove('expanded');
            nodeData.expanded = false;
        } else {
            let childContainer = nodeData.element.querySelector('.tree-children');
            if (!childContainer) {
                childContainer = document.createElement('ul');
                childContainer.className = 'tree-children';
                nodeData.element.appendChild(childContainer);
                const pattern = fullPath + '/+';
                await this.loadTopicLevel(pattern, childContainer, fullPath);
            }
            childContainer.classList.remove('collapsed');
            nodeData.toggle.classList.add('expanded');
            nodeData.expanded = true;
        }
    }

    emptyItem() {
        const li = document.createElement('li');
        li.className = 'tree-node';
        li.innerHTML = '<div class="tree-item" style="color:var(--text-muted);font-style:italic;">No topics found</div>';
        return li;
    }
};

// --- Init ---
document.addEventListener('DOMContentLoaded', function() {
    window.archiveExplorer = new ArchiveExplorerManager();
    window.archiveExplorer.init();
    window.explorerTopicBrowser = new ExplorerTopicBrowser();
});
