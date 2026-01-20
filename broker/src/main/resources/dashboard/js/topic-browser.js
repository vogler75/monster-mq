class TopicBrowser {
    constructor() {
        this.tree = document.getElementById('topic-tree');
        this.dataViewer = document.getElementById('data-viewer');
        this.searchInput = document.getElementById('search-input');
        this.searchButton = document.getElementById('search-button');
        this.archiveGroupSelect = document.getElementById('archive-group-select');

        this.treeNodes = new Map(); // topic path -> TreeNode
        this.selectedTopic = null;
        this.selectedArchiveGroup = 'Default'; // Default archiveGroup

        this.init();
    }

    async init() {
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

        // Archive group selection
        this.archiveGroupSelect.addEventListener('change', (e) => {
            this.selectedArchiveGroup = e.target.value;
            localStorage.setItem('monstermq_selected_archive_group', this.selectedArchiveGroup);
            this.browseRoot();
        });

        // Load archive groups first, then load initial tree
        await this.loadArchiveGroups();

        // Load initial tree structure with root node
        this.createRootNode();
    }

    async loadArchiveGroups() {
        try {
            const query = `
                query GetArchiveGroups {
                    archiveGroups(enabled: true, lastValTypeNotEquals: NONE) {
                        name
                    }
                }
            `;

            const response = await graphqlClient.query(query);

            if (response && response.archiveGroups && response.archiveGroups.length > 0) {
                const groups = response.archiveGroups;

                // Clear loading option
                this.archiveGroupSelect.innerHTML = '';

                // Populate dropdown
                groups.forEach(group => {
                    const option = document.createElement('option');
                    option.value = group.name;
                    option.textContent = group.name;
                    this.archiveGroupSelect.appendChild(option);
                });

                // Set first group as default or restore saved selection
                const savedGroup = localStorage.getItem('monstermq_selected_archive_group');
                if (savedGroup && groups.some(g => g.name === savedGroup)) {
                    this.selectedArchiveGroup = savedGroup;
                    this.archiveGroupSelect.value = savedGroup;
                } else if (groups.length > 0) {
                    this.selectedArchiveGroup = groups[0].name;
                    this.archiveGroupSelect.value = groups[0].name;
                }
            } else {
                // No groups found - show error
                this.archiveGroupSelect.innerHTML = '<option value="">No archive groups available</option>';
                console.error('No archive groups with lastValType found');
            }
        } catch (error) {
            console.error('Error loading archive groups:', error);
            this.archiveGroupSelect.innerHTML = '<option value="">Error loading groups</option>';
        }
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

    setupUI() {
        // UI setup is now handled by sidebar.js
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
        // Load the root level topics directly (don't create a synthetic root)
        this.loadTopicLevel('+', this.tree, '');
    }

    async loadTopicLevel(pattern, container, parentPath = '') {
        try {
            // Show loading state
            const loadingItem = this.createLoadingItem();
            container.appendChild(loadingItem);

            // Use GraphQL to browse topics
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

            // Remove loading item
            container.removeChild(loadingItem);

            if (response && response.browseTopics && response.browseTopics.length > 0) {
                const topics = response.browseTopics;

                // Convert topic names to our expected format and check for values
                const topicList = topics.map(topic => ({
                    topic: topic.name,
                    hasValue: true // browseTopics only returns topics that have values
                }));

                const groupedTopics = this.groupTopicsByLevel(topicList, parentPath);

                if (groupedTopics.size === 0) {
                    const emptyItem = document.createElement('li');
                    emptyItem.className = 'tree-node';
                    emptyItem.innerHTML = '<div class="tree-item" style="color: var(--text-muted); font-style: italic;">No topics found after grouping</div>';
                    container.appendChild(emptyItem);
                    return;
                }

                for (const [levelName, topicData] of groupedTopics) {
                    const fullPath = parentPath ? `${parentPath}/${levelName}` : levelName;
                    const treeItem = this.createTreeItem(levelName, fullPath, topicData.hasValue, topicData.hasChildren);
                    container.appendChild(treeItem);
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

        for (const topic of topics) {
            const levels = topic.topic.split('/');

            if (parentLevels === 0) {
                // Root level - show the first part of each topic
                const topLevel = levels[0];

                // For browse mode, assume all first-level topics can be expanded
                // since browseTopics only returns topics that exist
                const hasChildren = true; // Always assume children for browse results
                const hasValue = levels.length === 1 && topic.hasValue;

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
            } else if (levels.length > parentLevels) {
                // Deeper levels
                const nextLevel = levels[parentLevels];

                // For browse mode, assume all returned topics can potentially have children
                // since browseTopics only returns topics that exist in the hierarchy
                const hasChildren = true; // Always assume children for browse results
                const hasValue = levels.length === parentLevels + 1 && topic.hasValue;

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
                query GetCurrentValue($topic: String!, $archiveGroup: String!) {
                    currentValue(topic: $topic, archiveGroup: $archiveGroup) {
                        topic
                        payload
                        format
                        timestamp
                        qos
                    }
                }
            `;

            const response = await graphqlClient.query(query, {
                topic: topicPath,
                archiveGroup: this.selectedArchiveGroup
            });

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

/**
 * AI Analysis functionality for Topic Browser
 */
class TopicBrowserAI {
    constructor(topicBrowser) {
        this.topicBrowser = topicBrowser;
        this.chatHistory = [];  // Stores {role: 'user'|'assistant', content: string}
        this.contextLoaded = false;  // Track if topic data has been sent
        this.quickActions = [];  // Loaded from config file
        this.defaultSystemPrompt = `You are an MQTT topic tree analyst helping users understand their IoT data.
The data below shows MQTT topics and their current values in a hierarchical tree format using ASCII art (├── └── │).

**Output format:** Use Markdown formatting in your responses:
- Use ## headings for sections
- Use **bold** for emphasis
- Use \`code\` for topic names and values
- Use bullet lists for findings
- Use tables when comparing data

When analyzing:
- Identify naming patterns and hierarchy structure
- Spot anomalies or unusual values
- Recognize IoT patterns (Sparkplug, Homie, etc.)
- Provide concise, actionable insights

Topic data:`;

        this.init();
    }

    async init() {
        // Load saved system prompt or use default
        this.systemPrompt = localStorage.getItem('monstermq_ai_system_prompt') || this.defaultSystemPrompt;

        const systemPromptEl = document.getElementById('ai-system-prompt');
        if (systemPromptEl) {
            systemPromptEl.value = this.systemPrompt;
        }

        // Load quick actions from config file
        await this.loadQuickActions();

        // Event listeners
        this.setupEventListeners();
    }

    /**
     * Load quick action prompts from config file
     */
    async loadQuickActions() {
        try {
            const response = await fetch('/config/ai-prompts.json');
            if (response.ok) {
                const config = await response.json();
                this.quickActions = config.quickActions || [];
                this.renderQuickActionButtons();
            } else {
                console.warn('Could not load ai-prompts.json, using defaults');
                this.useDefaultQuickActions();
            }
        } catch (e) {
            console.warn('Error loading ai-prompts.json:', e.message);
            this.useDefaultQuickActions();
        }
    }

    /**
     * Fallback default prompts if config file is not available
     */
    useDefaultQuickActions() {
        this.quickActions = [
            {
                id: 'summarize',
                label: 'Summarize',
                title: 'Summarize the topic tree',
                prompt: 'Provide a high-level summary of this topic tree. What kind of data is being collected? What systems or devices are represented?'
            },
            {
                id: 'find-structure',
                label: 'Find Structure',
                title: 'Analyze naming patterns and hierarchy',
                prompt: 'Analyze the MQTT topic tree to identify naming patterns, hierarchy structure, and potential data models.'
            },
            {
                id: 'detect-anomalies',
                label: 'Detect Anomalies',
                title: 'Find unusual values or outliers',
                prompt: 'Look for anomalies in this data: unusual values, potential errors, outliers, or values that seem inconsistent with their neighbors.'
            }
        ];
        this.renderQuickActionButtons();
    }

    /**
     * Dynamically render quick action buttons from config
     */
    renderQuickActionButtons() {
        const container = document.getElementById('ai-quick-actions');
        if (!container) return;

        // Clear existing buttons
        container.innerHTML = '';

        // Create buttons from config
        for (const action of this.quickActions) {
            const button = document.createElement('button');
            button.id = `ai-${action.id}`;
            button.className = 'ai-action-btn';
            button.title = action.title || action.label;
            button.textContent = action.label;
            button.addEventListener('click', () => this.quickAction(action.prompt));
            container.appendChild(button);
        }
    }

    setupEventListeners() {
        // Toggle panel
        const toggleBtn = document.getElementById('toggle-ai-panel');
        const closeBtn = document.getElementById('close-ai-panel');

        if (toggleBtn) {
            toggleBtn.addEventListener('click', () => this.togglePanel());
        }

        if (closeBtn) {
            closeBtn.addEventListener('click', () => this.togglePanel(false));
        }

        // Send button and enter key
        const sendBtn = document.getElementById('ai-send');
        const questionInput = document.getElementById('ai-question');

        if (sendBtn) {
            sendBtn.addEventListener('click', () => this.analyze());
        }

        if (questionInput) {
            questionInput.addEventListener('keydown', (e) => {
                if (e.key === 'Enter' && !e.shiftKey) {
                    e.preventDefault();
                    this.analyze();
                }
            });
        }

        // Quick action buttons are now dynamically created in renderQuickActionButtons()

        // System prompt
        const resetPromptBtn = document.getElementById('ai-reset-prompt');
        const systemPromptEl = document.getElementById('ai-system-prompt');

        if (resetPromptBtn) {
            resetPromptBtn.addEventListener('click', () => this.resetPrompt());
        }

        if (systemPromptEl) {
            systemPromptEl.addEventListener('change', (e) => {
                this.systemPrompt = e.target.value;
                localStorage.setItem('monstermq_ai_system_prompt', this.systemPrompt);
            });
        }

        // Clear context button
        const clearContextBtn = document.getElementById('ai-clear-context');
        if (clearContextBtn) {
            clearContextBtn.addEventListener('click', () => this.clearContext());
        }
    }

    /**
     * Clear chat history and context - starts fresh conversation
     */
    clearContext() {
        this.chatHistory = [];
        this.contextLoaded = false;

        // Clear chat history UI
        const chatHistory = document.getElementById('ai-chat-history');
        if (chatHistory) {
            chatHistory.innerHTML = '';
        }

        // Reset topic count
        const topicCountEl = document.getElementById('ai-topic-count');
        if (topicCountEl) {
            topicCountEl.textContent = '-';
            topicCountEl.style.color = 'var(--monster-teal)';
        }

        // Update context indicator
        this.updateContextIndicator();

        console.log('AI context cleared - next question will reload topic data');
    }

    /**
     * Update the visual indicator showing if context is loaded
     */
    updateContextIndicator() {
        const indicator = document.getElementById('ai-context-status');
        if (indicator) {
            if (this.contextLoaded) {
                indicator.textContent = '● Context loaded';
                indicator.style.color = 'var(--monster-green)';
                indicator.title = 'Topic data is cached. Follow-up questions will be faster.';
            } else {
                indicator.textContent = '○ No context';
                indicator.style.color = 'var(--text-muted)';
                indicator.title = 'Next question will load topic data.';
            }
        }
    }

    togglePanel(forceState = null) {
        const panel = document.getElementById('ai-panel');
        const toggleBtn = document.getElementById('toggle-ai-panel');
        const layout = document.querySelector('.topic-browser-layout');

        if (!panel || !layout) return;

        const shouldShow = forceState !== null ? forceState : panel.style.display === 'none';

        if (shouldShow) {
            panel.style.display = 'flex';
            layout.classList.add('ai-open');
            if (toggleBtn) toggleBtn.classList.add('active');
            this.updateContextInfo();
        } else {
            panel.style.display = 'none';
            layout.classList.remove('ai-open');
            if (toggleBtn) toggleBtn.classList.remove('active');
        }
    }

    updateContextInfo() {
        const topicPatternEl = document.getElementById('ai-topic-pattern');
        const archiveGroupEl = document.getElementById('ai-archive-group');

        if (topicPatternEl) {
            topicPatternEl.textContent = this.getTopicPattern();
        }

        if (archiveGroupEl) {
            archiveGroupEl.textContent = this.topicBrowser.selectedArchiveGroup || 'Default';
        }
    }

    updateTopicCount(count, maxTopics) {
        const topicCountEl = document.getElementById('ai-topic-count');
        if (topicCountEl) {
            if (count >= maxTopics) {
                // Show warning color when limit reached
                topicCountEl.textContent = `${count}+`;
                topicCountEl.style.color = 'var(--monster-orange, #f59e0b)';
            } else {
                topicCountEl.textContent = count;
                topicCountEl.style.color = 'var(--monster-teal)';
            }
        }
    }

    getTopicPattern() {
        // Use selected topic + subtopics, or all topics
        if (this.topicBrowser.selectedTopic && this.topicBrowser.selectedTopic !== 'root') {
            return this.topicBrowser.selectedTopic + '/#';
        }
        return '#'; // All topics
    }

    async analyze(question = null) {
        const questionInput = document.getElementById('ai-question');
        const q = question || (questionInput ? questionInput.value.trim() : '');

        if (!q) return;

        // Add user message to chat UI
        this.addMessage('user', q);

        if (questionInput) {
            questionInput.value = '';
        }

        // Update context info
        this.updateContextInfo();

        // Show loading
        const loadingId = this.addMessage('assistant', 'Analyzing topics...', true);

        try {
            const maxTopics = this.getMaxTopics();
            const result = await this.callAnalyzeAPI(q);
            this.removeMessage(loadingId);

            // Update topic count display
            this.updateTopicCount(result.topicsAnalyzed, maxTopics);

            if (result.error) {
                this.addMessage('assistant', `Error: ${result.error}`, false, true);
            } else {
                // Mark context as loaded after successful first request
                if (!this.contextLoaded) {
                    this.contextLoaded = true;
                    this.updateContextIndicator();
                }

                // Save messages to chat history for follow-up questions
                // Note: Topic data is always sent to the backend, chat history provides conversation context
                this.chatHistory.push({ role: 'user', content: q });
                this.chatHistory.push({ role: 'assistant', content: result.response });

                let statsText = '';
                if (result.topicsAnalyzed > 0) {
                    if (result.topicsAnalyzed >= maxTopics) {
                        statsText = `\n\n_(Analyzed ${result.topicsAnalyzed} topics - limit reached, increase "Max" for more)_`;
                    } else {
                        statsText = `\n\n_(Analyzed ${result.topicsAnalyzed} topics)_`;
                    }
                }
                const responseText = result.response + statsText;
                this.addMessage('assistant', responseText);
            }
        } catch (err) {
            this.removeMessage(loadingId);
            this.addMessage('assistant', `Error: ${err.message}`, false, true);
        }
    }

    quickAction(question) {
        const questionInput = document.getElementById('ai-question');
        if (questionInput) {
            questionInput.value = question;
        }
        this.analyze(question);
    }

    getMaxTopics() {
        const maxTopicsEl = document.getElementById('ai-max-topics');
        return maxTopicsEl ? parseInt(maxTopicsEl.value, 10) : 100;
    }

    async callAnalyzeAPI(question) {
        const query = `
            query AnalyzeTopics($archiveGroup: String!, $topicPattern: String!, $question: String!, $systemPrompt: String, $maxTopics: Int, $chatHistory: [ChatMessage!]) {
                genai {
                    analyzeTopics(
                        archiveGroup: $archiveGroup
                        topicPattern: $topicPattern
                        question: $question
                        systemPrompt: $systemPrompt
                        maxTopics: $maxTopics
                        chatHistory: $chatHistory
                    ) {
                        response
                        topicsAnalyzed
                        model
                        error
                    }
                }
            }
        `;

        const maxTopics = this.getMaxTopics();
        const isFollowUp = this.contextLoaded && this.chatHistory.length > 0;

        const variables = {
            archiveGroup: this.topicBrowser.selectedArchiveGroup || 'Default',
            topicPattern: this.getTopicPattern(),
            question: question,
            systemPrompt: this.systemPrompt !== this.defaultSystemPrompt ? this.systemPrompt : null,
            maxTopics: maxTopics,
            // Only send chat history for follow-up questions (after context is loaded)
            chatHistory: isFollowUp ? this.chatHistory : null
        };

        console.log('=== AI Analysis Request ===');
        console.log('Is follow-up:', isFollowUp);
        console.log('Chat history length:', this.chatHistory.length);

        const result = await graphqlClient.query(query, variables);

        if (!result || !result.genai || !result.genai.analyzeTopics) {
            return { error: 'GenAI is not available. Check your configuration.' };
        }

        // Log the full LLM response for debugging
        console.log('=== AI Analysis Response ===');
        console.log('Topics analyzed:', result.genai.analyzeTopics.topicsAnalyzed);
        console.log('Model:', result.genai.analyzeTopics.model);
        console.log('Error:', result.genai.analyzeTopics.error);
        console.log('Response length:', result.genai.analyzeTopics.response?.length || 0);
        // Log FULL response - copy from console to see complete text
        console.log('FULL RESPONSE START >>>');
        console.log(result.genai.analyzeTopics.response);
        console.log('<<< FULL RESPONSE END');
        console.log('============================');

        return result.genai.analyzeTopics;
    }

    addMessage(role, content, isLoading = false, isError = false) {
        const id = 'msg-' + Date.now() + '-' + Math.random().toString(36).substr(2, 9);
        const chatHistory = document.getElementById('ai-chat-history');

        if (!chatHistory) return id;

        // Log content being added to help debug truncation issues
        console.log(`=== Adding ${role} message ===`);
        console.log('Content length:', content?.length || 0);
        console.log('Content preview (first 500 chars):', content?.substring(0, 500));
        console.log('Content preview (last 500 chars):', content?.substring(content.length - 500));

        const msgDiv = document.createElement('div');
        msgDiv.id = id;
        msgDiv.className = `chat-message chat-${role}`;

        if (isLoading) msgDiv.classList.add('loading');
        if (isError) msgDiv.classList.add('error');

        // Use textContent for plain text to avoid any HTML parsing issues
        // Then apply formatting
        const formattedContent = this.formatMessage(content);
        console.log('Formatted content length:', formattedContent?.length || 0);
        msgDiv.innerHTML = formattedContent;

        chatHistory.appendChild(msgDiv);
        chatHistory.scrollTop = chatHistory.scrollHeight;

        return id;
    }

    removeMessage(id) {
        const msg = document.getElementById(id);
        if (msg) {
            msg.remove();
        }
    }

    formatMessage(content) {
        // Use marked.js for full markdown rendering if available
        if (typeof marked !== 'undefined') {
            try {
                // Configure marked for safe rendering
                marked.setOptions({
                    breaks: true,      // Convert \n to <br>
                    gfm: true,         // GitHub Flavored Markdown
                    headerIds: false,  // Don't add IDs to headers
                    mangle: false      // Don't mangle email addresses
                });
                return marked.parse(content);
            } catch (e) {
                console.warn('Markdown parsing failed, falling back to simple formatting:', e);
            }
        }

        // Fallback: simple formatting if marked is not available
        let result = content
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;');

        result = result.replace(/\*\*([^*\n]+)\*\*/g, '<strong>$1</strong>');
        result = result.replace(/`([^`\n]+)`/g, '<code>$1</code>');
        result = result.replace(/\n/g, '<br>');

        return result;
    }

    resetPrompt() {
        this.systemPrompt = this.defaultSystemPrompt;
        const systemPromptEl = document.getElementById('ai-system-prompt');
        if (systemPromptEl) {
            systemPromptEl.value = this.systemPrompt;
        }
        localStorage.removeItem('monstermq_ai_system_prompt');
    }
}

// Initialize when DOM is loaded
document.addEventListener('DOMContentLoaded', () => {
    // Initialize topic browser (auth check is handled internally)
    const topicBrowser = new TopicBrowser();

    // Initialize AI functionality
    window.topicBrowserAI = new TopicBrowserAI(topicBrowser);
});