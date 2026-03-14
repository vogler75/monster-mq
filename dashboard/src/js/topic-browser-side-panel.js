class TopicBrowserSidePanel {
    constructor() {
        this.panel = document.getElementById('topic-browser-panel');
        this.tree = document.getElementById('topic-panel-tree-root');
        this.archiveGroupSelect = document.getElementById('topic-panel-archive-group');
        this.toggleButton = document.getElementById('topic-browser-floating-toggle');
        this.resizeHandle = document.getElementById('topic-panel-resize-handle');

        this.treeNodes = new Map(); // topic path -> TreeNode data
        this.selectedTopic = null;
        this.selectedArchiveGroup = 'Default';
        this.isOpen = false;
        this.panelWidth = 380;

        this.init();
    }

    async init() {
        // Set up event listeners
        this.archiveGroupSelect.addEventListener('change', (e) => {
            this.selectedArchiveGroup = e.target.value;
            this.browseRoot();
        });

        // Load archive groups and initialize tree
        await this.loadArchiveGroups();
        this.createRootNode();

        // Set up drag and drop event listeners for inputs
        this.setupDropZones();
        
        // Set up resize functionality
        this.setupResize();
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

                // Set first group as default
                if (groups.length > 0) {
                    this.selectedArchiveGroup = groups[0].name;
                    this.archiveGroupSelect.value = groups[0].name;
                }
            } else {
                this.archiveGroupSelect.innerHTML = '<option value="">No archive groups available</option>';
            }
        } catch (error) {
            console.error('Error loading archive groups:', error);
            this.archiveGroupSelect.innerHTML = '<option value="">Error loading groups</option>';
        }
    }

    setupDropZones() {
        // Add drag and drop support to all input fields in tables
        const observer = new MutationObserver((mutations) => {
            mutations.forEach((mutation) => {
                if (mutation.type === 'childList') {
                    mutation.addedNodes.forEach((node) => {
                        if (node.nodeType === Node.ELEMENT_NODE) {
                            this.addDropZonesToInputs(node);
                        }
                    });
                }
            });
        });

        observer.observe(document.body, {
            childList: true,
            subtree: true
        });

        // Add to existing inputs
        this.addDropZonesToInputs(document);
    }

    addDropZonesToInputs(container) {
        const inputs = container.querySelectorAll('input[type="text"], textarea');
        inputs.forEach(input => {
            if (!input.hasAttribute('data-drop-zone-enabled')) {
                input.setAttribute('data-drop-zone-enabled', 'true');
                input.classList.add('drop-zone');

                input.addEventListener('dragover', (e) => {
                    e.preventDefault();
                    e.dataTransfer.dropEffect = 'copy';
                    input.classList.add('drag-over');
                });

                input.addEventListener('dragenter', (e) => {
                    e.preventDefault();
                    input.classList.add('drag-over');
                });

                input.addEventListener('dragleave', (e) => {
                    e.preventDefault();
                    // Only remove class if we're actually leaving the element
                    if (!input.contains(e.relatedTarget)) {
                        input.classList.remove('drag-over');
                    }
                });

                input.addEventListener('drop', (e) => {
                    e.preventDefault();
                    e.stopPropagation();
                    input.classList.remove('drag-over');
                    
                    const topicPath = e.dataTransfer.getData('text/plain');
                    console.log('Dropped topic:', topicPath);
                    if (topicPath) {
                        // Replace the entire field content with the topic path
                        input.value = topicPath;
                        input.focus();
                        
                        // Trigger change event
                        const changeEvent = new Event('change', { bubbles: true });
                        input.dispatchEvent(changeEvent);
                        
                        // Also trigger input event for better compatibility
                        const inputEvent = new Event('input', { bubbles: true });
                        input.dispatchEvent(inputEvent);
                        
                        // Visual feedback
                        input.style.backgroundColor = 'rgba(34, 197, 94, 0.1)';
                        setTimeout(() => {
                            input.style.backgroundColor = '';
                        }, 500);
                    }
                });
            }
        });
    }

    static toggle() {
        if (window.topicBrowserSidePanel) {
            window.topicBrowserSidePanel.toggle();
        }
    }

    static close() {
        if (window.topicBrowserSidePanel) {
            window.topicBrowserSidePanel.close();
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
    }

    close() {
        this.panel.classList.remove('open');
        this.toggleButton.classList.remove('active');
        // Reset main content margin
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

            const deltaX = startX - e.clientX; // Reverse direction for right panel
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
        // Clear current tree and reload from root
        this.tree.innerHTML = '';
        this.treeNodes.clear();
        this.selectedTopic = null;
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
            item.classList.add('draggable');
        }

        // Toggle button for expandable nodes
        const toggle = document.createElement('button');
        toggle.className = 'tree-toggle';
        toggle.innerHTML = hasChildren ? '▶' : '';

        // Icon
        const icon = document.createElement('span');
        icon.className = 'tree-icon';
        if (hasChildren) {
            icon.className += ' folder';
            icon.innerHTML = '📁';
        } else {
            icon.className += ' topic';
            icon.innerHTML = '📄';
        }

        // Name
        const nameSpan = document.createElement('span');
        nameSpan.textContent = name;

        item.appendChild(toggle);
        item.appendChild(icon);
        item.appendChild(nameSpan);
        li.appendChild(item);

        // Store node data
        const nodeData = {
            element: li,
            item: item,
            toggle: toggle,
            expanded: false,
            hasChildren: hasChildren,
            hasValue: hasValue,
            fullPath: fullPath,
            name: name
        };

        this.treeNodes.set(fullPath, nodeData);

        // Set up event listeners
        if (hasChildren) {
            toggle.addEventListener('click', (e) => {
                e.stopPropagation();
                this.toggleNode(fullPath);
            });
        }

        // Make draggable if it has value
        if (hasValue) {
            item.draggable = true;
            item.addEventListener('dragstart', (e) => {
                console.log('Drag started for topic:', fullPath);
                e.dataTransfer.setData('text/plain', fullPath);
                e.dataTransfer.effectAllowed = 'copy';
                // Add visual feedback
                item.style.opacity = '0.6';
                
                // Add a drag image for better UX
                const dragImage = document.createElement('div');
                dragImage.textContent = fullPath;
                dragImage.style.cssText = `
                    position: absolute;
                    top: -1000px;
                    background: var(--monster-purple);
                    color: white;
                    padding: 0.5rem;
                    border-radius: 4px;
                    font-size: 0.875rem;
                    white-space: nowrap;
                `;
                document.body.appendChild(dragImage);
                e.dataTransfer.setDragImage(dragImage, 10, 10);
                
                // Clean up drag image after drag starts
                setTimeout(() => {
                    document.body.removeChild(dragImage);
                }, 0);
            });
            
            item.addEventListener('dragend', (e) => {
                // Reset visual feedback
                item.style.opacity = '1';
            });
        }

        // Click to select and view data
        item.addEventListener('click', (e) => {
            e.stopPropagation();
            if (hasValue) {
                this.selectTopic(fullPath);
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
            // Collapse
            const childContainer = nodeData.element.querySelector('.tree-children');
            if (childContainer) {
                childContainer.classList.add('collapsed');
            }
            nodeData.toggle.classList.remove('expanded');
            nodeData.expanded = false;
        } else {
            // Expand
            let childContainer = nodeData.element.querySelector('.tree-children');
            if (!childContainer) {
                childContainer = document.createElement('ul');
                childContainer.className = 'tree-children';
                nodeData.element.appendChild(childContainer);

                // Load children
                const pattern = fullPath === 'root' ? '+' : `${fullPath}/+`;
                await this.loadTopicLevel(pattern, childContainer, fullPath === 'root' ? '' : fullPath);
            }

            childContainer.classList.remove('collapsed');
            nodeData.toggle.classList.add('expanded');
            nodeData.expanded = true;
        }
    }

    selectTopic(fullPath) {
        // Update UI selection
        const allItems = document.querySelectorAll('.topic-panel-tree .tree-item');
        allItems.forEach(item => item.classList.remove('selected'));

        const nodeData = this.treeNodes.get(fullPath);
        if (nodeData) {
            nodeData.item.classList.add('selected');
        }

        this.selectedTopic = fullPath;
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

    // Static helper methods for convenient access
    static toggle() {
        if (window.topicBrowserSidePanel) window.topicBrowserSidePanel.toggle();
    }

    static close() {
        if (window.topicBrowserSidePanel) window.topicBrowserSidePanel.close();
    }

    static open() {
        if (window.topicBrowserSidePanel) window.topicBrowserSidePanel.open();
    }
}

// Initialize when DOM is ready
document.addEventListener('DOMContentLoaded', () => {
    window.topicBrowserSidePanel = new TopicBrowserSidePanel();
});