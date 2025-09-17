class TopicBrowser {
    constructor() {
        this.tree = document.getElementById('topic-tree');
        this.dataViewer = document.getElementById('data-viewer');
        this.searchInput = document.getElementById('search-input');
        this.searchButton = document.getElementById('search-button');

        this.treeNodes = new Map(); // topic path -> TreeNode
        this.selectedTopic = null;

        this.init();
    }

    init() {
        // Check authentication first
        if (!this.isLoggedIn()) {
            window.location.href = '/pages/login.html';
            return;
        }

        this.setupUI();

        // Set up event listeners
        this.browseButton = document.getElementById('browse-button');
        this.browseMode = document.querySelector('input[name="browse-mode"][value="browse"]');
        this.searchMode = document.querySelector('input[name="browse-mode"][value="search"]');

        this.searchButton.addEventListener('click', () => this.performSearch());
        this.browseButton.addEventListener('click', () => this.browseRoot());

        this.searchInput.addEventListener('keypress', (e) => {
            if (e.key === 'Enter') {
                if (this.searchMode.checked) {
                    this.performSearch();
                }
            }
        });

        // Mode switching
        this.browseMode.addEventListener('change', () => this.switchMode('browse'));
        this.searchMode.addEventListener('change', () => this.switchMode('search'));

        // Load initial tree structure with root node
        this.createRootNode();
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
    }

    logout() {
        localStorage.removeItem('monstermq_token');
        localStorage.removeItem('monstermq_username');
        localStorage.removeItem('monstermq_isAdmin');
        window.location.href = '/';
    }

    switchMode(mode) {
        if (mode === 'browse') {
            this.searchInput.disabled = true;
            this.searchInput.placeholder = 'Select "Search Topics" mode to search';
            this.browseButton.style.display = 'inline-block';
            this.searchButton.style.display = 'none';
        } else {
            this.searchInput.disabled = false;
            this.searchInput.placeholder = 'Search topics (e.g., sensor/+/temperature)...';
            this.browseButton.style.display = 'none';
            this.searchButton.style.display = 'inline-block';
        }
    }

    browseRoot() {
        // Clear current tree and reload from root
        this.tree.innerHTML = '';
        this.treeNodes.clear();
        this.selectedTopic = null;
        this.showEmptyDataViewer();
        this.createRootNode();
    }

    createRootNode() {
        // Create a root "Broker" node
        const rootItem = this.createTreeItem('Broker', 'root', false, true);
        this.tree.appendChild(rootItem);

        // Create child container for the root
        const childContainer = document.createElement('ul');
        childContainer.className = 'tree-children';
        rootItem.appendChild(childContainer);

        // Load the first level topics under the root
        this.loadTopicLevel('+', childContainer, '');

        // Auto-expand the root node
        const rootData = this.treeNodes.get('root');
        if (rootData) {
            rootData.toggle.classList.add('expanded');
            rootData.expanded = true;
        }
    }

    async loadTopicLevel(pattern, container, parentPath = '') {
        try {
            // Show loading state
            const loadingItem = this.createLoadingItem();
            container.appendChild(loadingItem);

            // Use GraphQL to browse topics
            const query = `
                query BrowseTopics($topic: String!) {
                    browseTopics(topic: $topic) {
                        name
                    }
                }
            `;

            const response = await graphqlClient.query(query, { topic: pattern });

            // Remove loading item
            container.removeChild(loadingItem);

            console.log('GraphQL response:', response);
            if (response && response.browseTopics && response.browseTopics.length > 0) {
                const topics = response.browseTopics;

                // Convert topic names to our expected format and check for values
                const topicList = topics.map(topic => ({
                    topic: topic.name,
                    hasValue: true // browseTopics only returns topics that have values
                }));
                console.log('Topic list:', topicList);

                const groupedTopics = this.groupTopicsByLevel(topicList, parentPath);
                console.log('Grouped topics:', groupedTopics);

                if (groupedTopics.size === 0) {
                    console.log('No grouped topics found');
                    const emptyItem = document.createElement('li');
                    emptyItem.className = 'tree-node';
                    emptyItem.innerHTML = '<div class="tree-item" style="color: var(--text-muted); font-style: italic;">No topics found after grouping</div>';
                    container.appendChild(emptyItem);
                    return;
                }

                for (const [levelName, topicData] of groupedTopics) {
                    const fullPath = parentPath ? `${parentPath}/${levelName}` : levelName;
                    console.log('Creating tree item for level:', levelName);
                    console.log('Parent path:', parentPath);
                    console.log('Full path:', fullPath);
                    console.log('Topic data:', topicData);
                    const treeItem = this.createTreeItem(levelName, fullPath, topicData.hasValue, topicData.hasChildren);
                    console.log('Created tree item element:', treeItem);
                    container.appendChild(treeItem);
                    console.log('Added tree item to container');
                }
            } else {
                // Show a message when no topics are found
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

        console.log('Grouping topics:', topics, 'Parent path:', parentPath, 'Parent levels:', parentLevels);

        for (const topic of topics) {
            const levels = topic.topic.split('/');
            console.log('Processing topic:', topic.topic, 'Levels:', levels);

            if (parentLevels === 0) {
                // Root level - show the first part of each topic
                const topLevel = levels[0];

                // For browse mode, assume all first-level topics can be expanded
                // since browseTopics only returns topics that exist
                const hasChildren = true; // Always assume children for browse results
                const hasValue = levels.length === 1 && topic.hasValue;

                console.log('Root level processing:', topLevel, 'hasChildren:', hasChildren, 'hasValue:', hasValue);

                if (!grouped.has(topLevel)) {
                    grouped.set(topLevel, {
                        hasValue: hasValue,
                        hasChildren: hasChildren
                    });
                } else {
                    const existing = grouped.get(topLevel);
                    existing.hasValue = existing.hasValue || hasValue;
                    existing.hasChildren = existing.hasChildren || hasChildren;
                }

                console.log('Final result for', topLevel, ':', grouped.get(topLevel));
            } else if (levels.length > parentLevels) {
                // Deeper levels
                const nextLevel = levels[parentLevels];

                // For browse mode, assume all returned topics can potentially have children
                // since browseTopics only returns topics that exist in the hierarchy
                const hasChildren = true; // Always assume children for browse results
                const hasValue = levels.length === parentLevels + 1 && topic.hasValue;

                console.log('Deeper level processing:', nextLevel, 'hasChildren:', hasChildren, 'hasValue:', hasValue);

                if (!grouped.has(nextLevel)) {
                    grouped.set(nextLevel, {
                        hasValue: hasValue,
                        hasChildren: hasChildren
                    });
                } else {
                    const existing = grouped.get(nextLevel);
                    existing.hasValue = existing.hasValue || hasValue;
                    existing.hasChildren = existing.hasChildren || hasChildren;
                }

                console.log('Final result for', nextLevel, ':', grouped.get(nextLevel));
            }
        }

        console.log('Final grouped result:', grouped);
        return grouped;
    }

    createTreeItem(name, fullPath, hasValue, hasChildren) {
        console.log('createTreeItem called with:', { name, fullPath, hasValue, hasChildren });
        const li = document.createElement('li');
        li.className = 'tree-node';
        console.log('Created li element:', li);

        const item = document.createElement('div');
        item.className = 'tree-item';
        if (hasValue) {
            item.classList.add('has-data');
        }

        // Toggle button for expandable nodes
        const toggle = document.createElement('button');
        toggle.className = 'tree-toggle';
        if (hasChildren) {
            toggle.innerHTML = '▶';
            toggle.addEventListener('click', (e) => {
                e.stopPropagation();
                this.toggleNode(li, fullPath);
            });
        }

        // Icon
        const icon = document.createElement('span');
        icon.className = `tree-icon ${hasChildren ? 'folder' : 'topic'}`;
        icon.innerHTML = hasChildren ? '📁' : '📄';

        // Label
        const label = document.createElement('span');
        label.textContent = name;

        item.appendChild(toggle);
        item.appendChild(icon);
        item.appendChild(label);

        // Click handler for topic selection
        item.addEventListener('click', () => {
            this.selectTopic(fullPath, item);
        });

        li.appendChild(item);

        // Store reference
        this.treeNodes.set(fullPath, {
            element: li,
            item: item,
            toggle: toggle,
            expanded: false,
            hasChildren: hasChildren,
            hasValue: hasValue
        });

        console.log('Completed tree item:', li);
        console.log('Tree item HTML:', li.outerHTML);
        return li;
    }

    async toggleNode(nodeElement, topicPath) {
        const nodeData = this.treeNodes.get(topicPath);
        if (!nodeData || !nodeData.hasChildren) return;

        if (nodeData.expanded) {
            // Collapse
            const childContainer = nodeElement.querySelector('.tree-children');
            if (childContainer) {
                childContainer.classList.add('collapsed');
                nodeData.toggle.classList.remove('expanded');
                nodeData.expanded = false;
            }
        } else {
            // Expand
            let childContainer = nodeElement.querySelector('.tree-children');
            if (!childContainer) {
                childContainer = document.createElement('ul');
                childContainer.className = 'tree-children';
                nodeElement.appendChild(childContainer);

                // Load children with correct pattern format
                const pattern = topicPath === 'root' ? '+' : `${topicPath}/+`;
                const parentPath = topicPath === 'root' ? '' : topicPath;
                await this.loadTopicLevel(pattern, childContainer, parentPath);
            } else {
                childContainer.classList.remove('collapsed');
            }

            nodeData.toggle.classList.add('expanded');
            nodeData.expanded = true;
        }
    }

    async selectTopic(topicPath, itemElement) {
        // Update UI selection
        document.querySelectorAll('.tree-item.selected').forEach(item => {
            item.classList.remove('selected');
        });
        itemElement.classList.add('selected');

        this.selectedTopic = topicPath;

        // Don't try to load message data for the root node
        if (topicPath === 'root') {
            this.showEmptyDataViewer('Select a topic from the tree to view its message data');
            return;
        }

        // Load message data if this topic has a value
        const nodeData = this.treeNodes.get(topicPath);
        if (nodeData && nodeData.hasValue) {
            await this.loadMessageData(topicPath);
        } else {
            this.showEmptyDataViewer();
        }
    }

    async loadMessageData(topicPath) {
        try {
            this.showLoadingDataViewer();

            const query = `
                query GetCurrentValue($topic: String!) {
                    currentValue(topic: $topic) {
                        topic
                        payload
                        format
                        timestamp
                        qos
                    }
                }
            `;

            const response = await graphqlClient.query(query, { topic: topicPath });

            if (response && response.currentValue) {
                this.displayMessageData(response.currentValue);
            } else {
                this.showEmptyDataViewer('No message data found for this topic');
            }
        } catch (error) {
            console.error('Error loading message data:', error);
            this.showErrorDataViewer(error.message);
        }
    }

    displayMessageData(message) {
        const { topic, payload, qos, format, timestamp } = message;

        this.dataViewer.className = 'data-viewer';

        const header = document.createElement('div');
        header.className = 'data-header';

        const topicDiv = document.createElement('div');
        topicDiv.className = 'data-topic';
        topicDiv.textContent = topic;

        const infoDiv = document.createElement('div');
        infoDiv.className = 'data-info';
        infoDiv.innerHTML = `
            <span>QoS: ${qos || 'Unknown'}</span>
            <span>Format: ${format || 'Unknown'}</span>
            <span>Time: ${timestamp ? new Date(timestamp).toLocaleString() : 'Unknown'}</span>
        `;

        header.appendChild(topicDiv);
        header.appendChild(infoDiv);

        const content = document.createElement('div');
        content.className = 'data-content';

        // Determine payload type and display accordingly based on format
        if (format === 'JSON') {
            try {
                const jsonData = JSON.parse(payload);
                const formattedJson = this.formatJson(jsonData);
                content.innerHTML = `<div class="data-json">${formattedJson}</div>`;
            } catch {
                // If JSON parsing fails, display as text
                content.innerHTML = `<div class="data-text">${this.escapeHtml(payload)}</div>`;
            }
        } else if (format === 'BINARY' || format === 'BASE64') {
            // Binary data
            const size = typeof payload === 'string' ? payload.length : (payload?.byteLength || payload?.length || 0);
            const displaySize = format === 'BASE64' ? Math.floor(size * 3 / 4) : size; // Estimate original size for base64
            content.innerHTML = `
                <div class="data-binary">
                    <span class="binary-icon">🗄️</span>
                    <div>
                        <div>Binary Data (${format})</div>
                        <div style="font-size: 0.75rem; color: var(--text-muted);">Size: ${this.formatBytes(displaySize)}</div>
                    </div>
                </div>
            `;
        } else {
            // TEXT or unknown format - display as plain text
            content.innerHTML = `<div class="data-text">${this.escapeHtml(String(payload))}</div>`;
        }

        this.dataViewer.innerHTML = '';
        this.dataViewer.appendChild(header);
        this.dataViewer.appendChild(content);
    }

    formatJson(obj, indent = 0) {
        const indentStr = '  '.repeat(indent);

        if (obj === null) {
            return '<span class="json-null">null</span>';
        }

        if (typeof obj === 'boolean') {
            return `<span class="json-boolean">${obj}</span>`;
        }

        if (typeof obj === 'number') {
            return `<span class="json-number">${obj}</span>`;
        }

        if (typeof obj === 'string') {
            return `<span class="json-string">"${this.escapeHtml(obj)}"</span>`;
        }

        if (Array.isArray(obj)) {
            if (obj.length === 0) return '[]';

            const items = obj.map(item => `${indentStr}  ${this.formatJson(item, indent + 1)}`);
            return `[\n${items.join(',\n')}\n${indentStr}]`;
        }

        if (typeof obj === 'object') {
            const keys = Object.keys(obj);
            if (keys.length === 0) return '{}';

            const items = keys.map(key => {
                const keySpan = `<span class="json-key">"${this.escapeHtml(key)}"</span>`;
                const value = this.formatJson(obj[key], indent + 1);
                return `${indentStr}  ${keySpan}: ${value}`;
            });
            return `{\n${items.join(',\n')}\n${indentStr}}`;
        }

        return String(obj);
    }

    escapeHtml(text) {
        const div = document.createElement('div');
        div.textContent = text;
        return div.innerHTML;
    }

    formatBytes(bytes) {
        const sizes = ['Bytes', 'KB', 'MB', 'GB'];
        if (bytes === 0) return '0 Bytes';
        const i = Math.floor(Math.log(bytes) / Math.log(1024));
        return Math.round(bytes / Math.pow(1024, i) * 100) / 100 + ' ' + sizes[i];
    }

    async performSearch() {
        const searchTerm = this.searchInput.value.trim();
        if (!searchTerm) return;

        try {
            this.showSearchLoading();

            // Use the search functionality
            const query = `
                query SearchTopics($pattern: String!) {
                    searchTopics(pattern: $pattern)
                }
            `;

            const response = await graphqlClient.query(query, { pattern: searchTerm });

            if (response && response.searchTopics) {
                this.buildSearchTree(response.searchTopics);
            }
        } catch (error) {
            console.error('Error searching topics:', error);
            this.showSearchError(error.message);
        }
    }

    buildSearchTree(topics) {
        // Clear current tree
        this.tree.innerHTML = '';
        this.treeNodes.clear();

        if (topics.length === 0) {
            const emptyItem = document.createElement('li');
            emptyItem.className = 'tree-node';
            emptyItem.innerHTML = '<div class="tree-item" style="color: var(--text-muted);">No topics found</div>';
            this.tree.appendChild(emptyItem);
            return;
        }

        // Convert topic names to our expected format
        // searchTopics returns an array of strings, not objects
        const topicList = topics.map(topicName => ({
            topic: topicName,
            hasValue: true // searchTopics only returns topics that have values
        }));

        // Build tree structure from search results
        const treeStructure = this.buildTreeStructure(topicList);
        this.renderTreeStructure(treeStructure, this.tree, '');
    }

    buildTreeStructure(topics) {
        const structure = new Map();

        for (const topic of topics) {
            const parts = topic.topic.split('/');
            let currentLevel = structure;

            for (let i = 0; i < parts.length; i++) {
                const part = parts[i];

                if (!currentLevel.has(part)) {
                    currentLevel.set(part, {
                        children: new Map(),
                        hasValue: false,
                        fullPath: parts.slice(0, i + 1).join('/')
                    });
                }

                const node = currentLevel.get(part);
                if (i === parts.length - 1) {
                    node.hasValue = topic.hasValue;
                }

                currentLevel = node.children;
            }
        }

        return structure;
    }

    renderTreeStructure(structure, container, parentPath) {
        for (const [name, data] of structure) {
            const hasChildren = data.children.size > 0;
            const treeItem = this.createTreeItem(name, data.fullPath, data.hasValue, hasChildren);
            container.appendChild(treeItem);

            if (hasChildren) {
                const childContainer = document.createElement('ul');
                childContainer.className = 'tree-children';
                treeItem.appendChild(childContainer);

                this.renderTreeStructure(data.children, childContainer, data.fullPath);

                // Auto-expand search results
                const nodeData = this.treeNodes.get(data.fullPath);
                if (nodeData) {
                    nodeData.toggle.classList.add('expanded');
                    nodeData.expanded = true;
                }
            }
        }
    }

    showSearchLoading() {
        this.tree.innerHTML = '<li class="loading">Searching topics...</li>';
    }

    showSearchError(message) {
        this.tree.innerHTML = `<li class="error">Search error: ${message}</li>`;
    }

    showLoadingDataViewer() {
        this.dataViewer.className = 'data-viewer';
        this.dataViewer.innerHTML = '<div class="loading">Loading message data...</div>';
    }

    showEmptyDataViewer(message = 'Select a topic from the tree to view its message data') {
        this.dataViewer.className = 'data-viewer empty';
        this.dataViewer.innerHTML = `<div><p>${message}</p></div>`;
    }

    showErrorDataViewer(message) {
        this.dataViewer.className = 'data-viewer';
        this.dataViewer.innerHTML = `<div class="error">Error loading data: ${message}</div>`;
    }

    createLoadingItem() {
        const li = document.createElement('li');
        li.className = 'loading-item';
        li.innerHTML = '<div class="loading">Loading...</div>';
        return li;
    }

    createErrorItem(message) {
        const li = document.createElement('li');
        li.className = 'error-item';
        li.innerHTML = `<div class="error">Error: ${message}</div>`;
        return li;
    }
}

// Initialize when DOM is loaded
document.addEventListener('DOMContentLoaded', () => {
    // Initialize topic browser (auth check is handled internally)
    new TopicBrowser();
});