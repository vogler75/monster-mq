class ArchiveGroupsManager {
    constructor() {
        this.isEditing = false;
        this.editingName = null;
        this.archiveGroups = [];

        this.init();
    }

    init() {
        if (!this.isLoggedIn()) {
            window.location.href = '/pages/login.html';
            return;
        }

        this.setupUI();
        this.setupEventListeners();
        this.loadArchiveGroups();
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
        // UI setup is now handled by sidebar.js
    }

    setupEventListeners() {
        // Modal close on outside click
        window.onclick = (event) => {
            const modal = document.getElementById('archiveGroupModal');
            if (event.target === modal) {
                this.closeModal();
            }
        };

        // Form submission
        document.getElementById('archiveGroupForm').addEventListener('submit', (e) => {
            e.preventDefault();
            this.saveArchiveGroup();
        });

        // Update lastValRetention help text based on LastValType selection
        const lastValTypeSelect = document.getElementById('lastValType');
        if (lastValTypeSelect) {
            lastValTypeSelect.addEventListener('change', (e) => {
                this.updateLastValRetentionHelp(e.target.value);
            });
        }
    }

    updateLastValRetentionHelp(lastValType) {
        const helpDiv = document.querySelector('label[for="lastValRetention"] + input + .help-text') ||
                       document.querySelector('[for="lastValRetention"]').parentElement.querySelector('.help-text');

        if (!helpDiv) return;

        if (lastValType === 'MEMORY') {
            helpDiv.textContent = 'Size-based format required for MEMORY store (e.g., 50k, 100000k = number of entries)';
        } else if (lastValType === 'NONE') {
            helpDiv.textContent = 'No retention needed for NONE store';
        } else if (['POSTGRES', 'CRATEDB', 'MONGODB', 'SQLITE', 'HAZELCAST'].includes(lastValType)) {
            helpDiv.textContent = 'Time-based format (e.g., 7d, 24h, 60m, 1y)';
        } else {
            helpDiv.textContent = 'Size-based (MEMORY): 50k, 100000k | Time-based (others): 7d, 24h, 60m';
        }
    }

    async loadArchiveGroups() {
        try {
            console.log('Loading archive groups...');
            const result = await window.graphqlClient.query(`
                query GetArchiveGroups {
                    archiveGroups {
                        name
                        enabled
                        deployed
                        deploymentId
                        topicFilter
                        retainedOnly
                        lastValType
                        archiveType
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
                        metrics {
                            messagesOut
                            bufferSize
                            timestamp
                        }
                    }
                }
            `);

            this.archiveGroups = result.archiveGroups || [];
            this.renderArchiveGroups();
        } catch (error) {
            console.error('Error loading archive groups:', error);
            this.showError('Failed to load archive groups: ' + error.message);
        }
    }

    getConnectionStatus(group) {
        // Helper function to check if stores are connected across nodes
        const hasArchive = group.archiveType && group.archiveType !== 'NONE';
        const hasLastVal = group.lastValType && group.lastValType !== 'NONE';

        let archiveConnected = true;
        let lastValConnected = true;

        // Check connection status across all nodes for this specific group
        if (group.connectionStatus && Array.isArray(group.connectionStatus)) {
            group.connectionStatus.forEach(nodeStatus => {
                if (hasArchive && !nodeStatus.messageArchive) {
                    archiveConnected = false;
                }
                if (hasLastVal && !nodeStatus.lastValueStore) {
                    lastValConnected = false;
                }
            });
        } else {
            // If no connection status data available, assume disconnected for safety
            archiveConnected = false;
            lastValConnected = false;
        }

        return {
            archive: hasArchive ? archiveConnected : null,
            lastVal: hasLastVal ? lastValConnected : null
        };
    }

    renderArchiveGroups() {
        const tbody = document.getElementById('archive-groups-tbody');

        if (this.archiveGroups.length === 0) {
            tbody.innerHTML = `
                <tr>
                    <td colspan="8" style="text-align: center; padding: 2rem; color: var(--text-secondary);">
                        No archive groups found. Create your first archive group to get started.
                    </td>
                </tr>
            `;
            return;
        }

        tbody.innerHTML = this.archiveGroups.map(group => {
            const connectionStatus = this.getConnectionStatus(group);
            return `
            <tr>
                <td>
                    <strong>${this.escapeHtml(group.name)}</strong>
                    ${group.deployed ? '<br><small style="color: var(--text-secondary);">Deployed</small>' : ''}
                </td>
                <td>
                    <span class="status-badge ${group.enabled ? 'status-enabled' : 'status-disabled'}">
                        <span class="status-indicator"></span>
                        ${group.enabled ? 'Enabled' : 'Disabled'}
                    </span>
                    <div class="connection-indicators">
                        ${connectionStatus.archive !== null ? `
                            <div class="connection-item">
                                <span class="connection-dot ${connectionStatus.archive ? 'connected' : 'disconnected'}"></span>
                                <span class="connection-label">Archive</span>
                            </div>
                        ` : ''}
                        ${connectionStatus.lastVal !== null ? `
                            <div class="connection-item">
                                <span class="connection-dot ${connectionStatus.lastVal ? 'connected' : 'disconnected'}"></span>
                                <span class="connection-label">Last Val</span>
                            </div>
                        ` : ''}
                    </div>
                </td>
                <td>
                    <div class="topic-filters">
                        ${group.topicFilter.slice(0, 3).map(filter =>
                            `<span class="topic-filter-tag">${this.escapeHtml(filter)}</span>`
                        ).join('')}
                        ${group.topicFilter.length > 3 ?
                            `<span class="topic-filter-tag" style="background: var(--accent-blue); color: white;">+${group.topicFilter.length - 3} more</span>` :
                            ''
                        }
                    </div>
                </td>
                <td>${(() => {
                    const lv = group.lastValType;
                    const ar = group.archiveType;
                    const showLv = lv && lv !== 'NONE';
                    const showAr = ar && ar !== 'NONE';
                    if (showLv && showAr) return this.escapeHtml(`${lv}`) + '<br>' + this.escapeHtml(`${ar}`);
                    if (showLv) return this.escapeHtml(lv);
                    if (showAr) return this.escapeHtml(ar);
                    return 'NONE';
                })()}</td>
                <td>${this.escapeHtml(group.payloadFormat || 'DEFAULT')}</td>
                <td>${group.retainedOnly ? 'Yes' : 'No'}</td>
                <td style="color: #9333EA; white-space: nowrap;">${(() => {
                    const msgs = group.metrics && group.metrics.length > 0 ? Math.round(group.metrics[0].messagesOut) : 0;
                    const buf = group.metrics && group.metrics.length > 0 ? group.metrics[0].bufferSize : 0;
                    return `${msgs} msg/s<br><span style=\"color: var(--text-secondary);\">${buf} buf</span>`;
                })()}</td>
                <td>
                    <div class="action-buttons">
                        ${group.enabled ?
                            `<button class="btn-action btn-disable" onclick="archiveGroupsManager.toggleArchiveGroup('${this.escapeHtml(group.name)}', false)">
                                Disable
                            </button>` :
                            `<button class="btn-action btn-enable" onclick="archiveGroupsManager.toggleArchiveGroup('${this.escapeHtml(group.name)}', true)">
                                Enable
                            </button>`
                        }
                        <button class="btn-action btn-edit"
                                onclick="archiveGroupsManager.editArchiveGroup('${this.escapeHtml(group.name)}')">
                            Edit
                        </button>
                        <button class="btn-action btn-delete"
                                onclick="archiveGroupsManager.deleteArchiveGroup('${this.escapeHtml(group.name)}')"
                                ${group.enabled ? 'disabled title="Disable the archive group first to delete"' : ''}>
                            Delete
                        </button>
                    </div>
                </td>
            </tr>`;
        }).join('');
    }

    async toggleArchiveGroup(name, enable) {
        try {
            console.log(`${enable ? 'Enabling' : 'Disabling'} archive group:`, name);

            const methodName = enable ? 'enable' : 'disable';
            const result = await window.graphqlClient.query(`
                mutation ${enable ? 'EnableArchiveGroup' : 'DisableArchiveGroup'}($name: String!) {
                    archiveGroup {
                        ${methodName}(name: $name) {
                            success
                            message
                        }
                    }
                }
            `, { name });

            if (result.archiveGroup[methodName].success) {
                console.log(`Archive group ${enable ? 'enabled' : 'disabled'} successfully`);
                await this.loadArchiveGroups(); // Reload to update UI
            } else {
                this.showError(result.archiveGroup[methodName].message || `Failed to ${enable ? 'enable' : 'disable'} archive group`);
            }
        } catch (error) {
            console.error(`Error ${enable ? 'enabling' : 'disabling'} archive group:`, error);
            this.showError(`Failed to ${enable ? 'enable' : 'disable'} archive group: ` + error.message);
        }
    }

    async deleteArchiveGroup(name) {
        if (!confirm(`Are you sure you want to delete the archive group "${name}"?\n\nThis action cannot be undone.`)) {
            return;
        }

        try {
            console.log('Deleting archive group:', name);

            const result = await window.graphqlClient.query(`
                mutation DeleteArchiveGroup($name: String!) {
                    archiveGroup {
                        delete(name: $name) {
                            success
                            message
                        }
                    }
                }
            `, { name });

            if (result.archiveGroup.delete.success) {
                console.log('Archive group deleted successfully');
                await this.loadArchiveGroups(); // Reload to update UI
            } else {
                this.showError(result.archiveGroup.delete.message || 'Failed to delete archive group');
            }
        } catch (error) {
            console.error('Error deleting archive group:', error);
            this.showError('Failed to delete archive group: ' + error.message);
        }
    }

    openCreateModal() {
        this.isEditing = false;
        this.editingName = null;

        document.getElementById('modalTitle').textContent = 'Create Archive Group';
        document.getElementById('archiveGroupForm').reset();
        document.getElementById('name').disabled = false;

        document.getElementById('archiveGroupModal').style.display = 'block';
    }

    async editArchiveGroup(name) {
        this.isEditing = true;
        this.editingName = name;

        // Find the archive group
        const group = this.archiveGroups.find(g => g.name === name);
        if (!group) {
            this.showError('Archive group not found');
            return;
        }

        document.getElementById('modalTitle').textContent = 'Edit Archive Group';

        // Populate form
        document.getElementById('name').value = group.name;
        document.getElementById('name').disabled = true; // Can't change name
        document.getElementById('topicFilter').value = group.topicFilter.join('\n');
        document.getElementById('lastValType').value = group.lastValType;
        document.getElementById('archiveType').value = group.archiveType;
        document.getElementById('payloadFormat').value = group.payloadFormat || 'DEFAULT';
        document.getElementById('retainedOnly').checked = group.retainedOnly;
        document.getElementById('lastValRetention').value = group.lastValRetention || '';
        document.getElementById('archiveRetention').value = group.archiveRetention || '';
        document.getElementById('purgeInterval').value = group.purgeInterval || '';

        document.getElementById('archiveGroupModal').style.display = 'block';
    }

    closeModal() {
        document.getElementById('archiveGroupModal').style.display = 'none';
        document.getElementById('archiveGroupForm').reset();
        this.isEditing = false;
        this.editingName = null;
    }

    async saveArchiveGroup() {
        // Get values directly from DOM elements instead of using FormData
        const name = document.getElementById('name').value.trim();
        const topicFilterText = document.getElementById('topicFilter').value.trim();
        const lastValType = document.getElementById('lastValType').value;
        const archiveType = document.getElementById('archiveType').value;
        const retainedOnly = document.getElementById('retainedOnly').checked;
        const payloadFormat = document.getElementById('payloadFormat').value;
        const lastValRetention = document.getElementById('lastValRetention').value.trim();
        const archiveRetention = document.getElementById('archiveRetention').value.trim();
        const purgeInterval = document.getElementById('purgeInterval').value.trim();


        if (!name || !topicFilterText || !lastValType || !archiveType) {
            this.showError('Please fill in all required fields');
            return;
        }

        // Validate lastValRetention format with MEMORY store
        if (lastValType === 'MEMORY' && lastValRetention) {
            // Check if retention is time-based (ends with d, h, m, s, w, M, y)
            if (/^[0-9]+(d|h|m|s|w|M|y)$/.test(lastValRetention)) {
                this.showError(
                    `MEMORY store does not support time-based retention ("${lastValRetention}").\n\n` +
                    `Use size-based format instead, e.g., "50k" or "100000k" (number of entries).\n\n` +
                    `Or switch to a persistent store (POSTGRES, CRATEDB, MONGODB, SQLITE) to use time-based retention.`
                );
                return;
            }
            // Check if it's size-based (ends with k)
            if (!lastValRetention.endsWith('k')) {
                this.showError(
                    `MEMORY store requires size-based retention format.\n\n` +
                    `Use format like "50k" or "100000k" where k = 1,000 entries.\n\n` +
                    `Examples:\n` +
                    `- 50k = keep last 50,000 entries\n` +
                    `- 100k = keep last 100,000 entries`
                );
                return;
            }
        }

        // Parse topic filters
        const topicFilter = topicFilterText.split('\n')
            .map(line => line.trim())
            .filter(line => line.length > 0);

        if (topicFilter.length === 0) {
            this.showError('At least one topic filter is required');
            return;
        }

        try {
            const input = {
                name,
                topicFilter,
                lastValType,
                archiveType,
                payloadFormat,
                retainedOnly
            };

            // Always include retention fields when editing (could be empty to clear them)
            if (this.isEditing) {
                input.lastValRetention = lastValRetention || null;
                input.archiveRetention = archiveRetention || null;
                input.purgeInterval = purgeInterval || null;
            } else {
                // Only include retention fields if they have values when creating
                if (lastValRetention) input.lastValRetention = lastValRetention;
                if (archiveRetention) input.archiveRetention = archiveRetention;
                if (purgeInterval) input.purgeInterval = purgeInterval;
            }


            let result;
            if (this.isEditing) {
                // For update, use the original name to identify the archive group
                const updateInput = {
                    name: this.editingName, // Original name to identify the archive group
                    topicFilter: input.topicFilter,
                    lastValType: input.lastValType,
                    archiveType: input.archiveType,
                    retainedOnly: input.retainedOnly,
                    payloadFormat: input.payloadFormat,
                    lastValRetention: input.lastValRetention,
                    archiveRetention: input.archiveRetention,
                    purgeInterval: input.purgeInterval
                };


                result = await window.graphqlClient.query(`
                    mutation UpdateArchiveGroup($input: UpdateArchiveGroupInput!) {
                        archiveGroup {
                            update(input: $input) {
                                success
                                message
                            }
                        }
                    }
                `, { input: updateInput });

                if (result.archiveGroup.update.success) {
                    console.log('Archive group updated successfully');
                    this.closeModal();
                    await this.loadArchiveGroups();
                } else {
                    this.showError(result.archiveGroup.update.message || 'Failed to update archive group');
                }
            } else {
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
                    console.log('Archive group created successfully');
                    this.closeModal();
                    await this.loadArchiveGroups();
                } else {
                    this.showError(result.archiveGroup.create.message || 'Failed to create archive group');
                }
            }
        } catch (error) {
            console.error('Error saving archive group:', error);
            this.showError('Failed to save archive group: ' + error.message);
        }
    }

    showError(message) {
        // Simple error display - could be enhanced with a proper notification system
        alert('Error: ' + message);
    }

    escapeHtml(unsafe) {
        if (typeof unsafe !== 'string') return unsafe;
        return unsafe
            .replace(/&/g, "&amp;")
            .replace(/</g, "&lt;")
            .replace(/>/g, "&gt;")
            .replace(/"/g, "&quot;")
            .replace(/'/g, "&#039;");
    }
}

// Global functions for onclick handlers
window.openCreateModal = () => archiveGroupsManager.openCreateModal();
window.closeModal = () => archiveGroupsManager.closeModal();
window.saveArchiveGroup = () => archiveGroupsManager.saveArchiveGroup();

// Initialize when page loads
document.addEventListener('DOMContentLoaded', () => {
    window.archiveGroupsManager = new ArchiveGroupsManager();
});

// Also add GraphQL client extensions for archive groups
if (window.graphqlClient) {
    // Add archive group methods to the existing GraphQL client
    window.graphqlClient.getArchiveGroups = async function() {
        const query = `
            query GetArchiveGroups {
                archiveGroups {
                    name
                    enabled
                    deployed
                    deploymentId
                    topicFilter
                    retainedOnly
                    lastValType
                    archiveType
                    payloadFormat
                    lastValRetention
                    archiveRetention
                    purgeInterval
                    createdAt
                    updatedAt
                }
            }
        `;

        const result = await this.query(query);
        return result.archiveGroups;
    };

    window.graphqlClient.createArchiveGroup = async function(input) {
        const mutation = `
            mutation CreateArchiveGroup($input: CreateArchiveGroupInput!) {
                archiveGroup {
                    create(input: $input) {
                        success
                        message
                        archiveGroup {
                            name
                            enabled
                            deployed
                        }
                    }
                }
            }
        `;

        const result = await this.query(mutation, { input });
        return result.archiveGroup.create;
    };

    window.graphqlClient.updateArchiveGroup = async function(input) {
        const mutation = `
            mutation UpdateArchiveGroup($input: UpdateArchiveGroupInput!) {
                archiveGroup {
                    update(input: $input) {
                        success
                        message
                        archiveGroup {
                            name
                            enabled
                            deployed
                        }
                    }
                }
            }
        `;

        const result = await this.query(mutation, { input });
        return result.archiveGroup.update;
    };

    window.graphqlClient.deleteArchiveGroup = async function(name) {
        const mutation = `
            mutation DeleteArchiveGroup($name: String!) {
                archiveGroup {
                    delete(name: $name) {
                        success
                        message
                    }
                }
            }
        `;

        const result = await this.query(mutation, { name });
        return result.archiveGroup.delete;
    };

    window.graphqlClient.enableArchiveGroup = async function(name) {
        const mutation = `
            mutation EnableArchiveGroup($name: String!) {
                archiveGroup {
                    enable(name: $name) {
                        success
                        message
                        archiveGroup {
                            name
                            enabled
                            deployed
                        }
                    }
                }
            }
        `;

        const result = await this.query(mutation, { name });
        return result.archiveGroup.enable;
    };

    window.graphqlClient.disableArchiveGroup = async function(name) {
        const mutation = `
            mutation DisableArchiveGroup($name: String!) {
                archiveGroup {
                    disable(name: $name) {
                        success
                        message
                        archiveGroup {
                            name
                            enabled
                            deployed
                        }
                    }
                }
            }
        `;

        const result = await this.query(mutation, { name });
        return result.archiveGroup.disable;
    };
}