class ArchiveGroupsManager {
    constructor() {
        this.archiveGroups = [];
        this.init();
    }

    init() {
        if (!this.isLoggedIn()) {
            window.location.href = '/pages/login.html';
            return;
        }
        this.loadArchiveGroups();
    }

    isLoggedIn() {
        return window.isLoggedIn();
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
        const hasArchive = group.archiveType && group.archiveType !== 'NONE';
        const hasLastVal = group.lastValType && group.lastValType !== 'NONE';

        let archiveConnected = true;
        let lastValConnected = true;

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
                    `<ix-icon-button icon="pause" variant="primary" ghost size="24" title="Disable" onclick="archiveGroupsManager.toggleArchiveGroup('${this.escapeHtml(group.name)}', false)"></ix-icon-button>` :
                    `<ix-icon-button icon="play" variant="primary" ghost size="24" title="Enable" onclick="archiveGroupsManager.toggleArchiveGroup('${this.escapeHtml(group.name)}', true)"></ix-icon-button>`
                }
                        <a href="/pages/archive-group-detail.html?name=${encodeURIComponent(group.name)}"><ix-icon-button icon="highlight" variant="primary" ghost size="24" title="Edit"></ix-icon-button></a>
                        <ix-icon-button icon="trashcan" variant="primary" ghost size="24" class="btn-delete" title="${group.enabled ? 'Disable the archive group first to delete' : 'Delete'}"
                                onclick="archiveGroupsManager.deleteArchiveGroup('${this.escapeHtml(group.name)}')"
                                ${group.enabled ? 'disabled' : ''}></ix-icon-button>
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
                await this.loadArchiveGroups();
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
                await this.loadArchiveGroups();
            } else {
                this.showError(result.archiveGroup.delete.message || 'Failed to delete archive group');
            }
        } catch (error) {
            console.error('Error deleting archive group:', error);
            this.showError('Failed to delete archive group: ' + error.message);
        }
    }

    async exportArchiveGroups() {
        try {
            const result = await window.graphqlClient.query(`
                query GetArchiveGroups {
                    archiveGroups {
                        name
                        enabled
                        topicFilter
                        retainedOnly
                        lastValType
                        archiveType
                        payloadFormat
                        lastValRetention
                        archiveRetention
                        purgeInterval
                    }
                }
            `);

            const groups = (result.archiveGroups || []).map(g => {
                const obj = {
                    Name: g.name,
                    Enabled: g.enabled,
                    TopicFilter: g.topicFilter,
                    RetainedOnly: g.retainedOnly,
                    LastValType: g.lastValType,
                    ArchiveType: g.archiveType,
                    PayloadFormat: g.payloadFormat
                };
                if (g.lastValRetention) obj.LastValRetention = g.lastValRetention;
                if (g.archiveRetention) obj.ArchiveRetention = g.archiveRetention;
                if (g.purgeInterval) obj.PurgeInterval = g.purgeInterval;
                return obj;
            });

            const json = JSON.stringify(groups, null, 2);
            const blob = new Blob([json], { type: 'application/json' });
            const url = URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = 'archive-groups.json';
            document.body.appendChild(a);
            a.click();
            document.body.removeChild(a);
            URL.revokeObjectURL(url);
        } catch (error) {
            console.error('Error exporting archive groups:', error);
            this.showError('Failed to export archive groups: ' + error.message);
        }
    }

    showError(message) {
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

// Initialize when page loads
document.addEventListener('DOMContentLoaded', () => {
    window.archiveGroupsManager = new ArchiveGroupsManager();
});

// Also add GraphQL client extensions for archive groups
if (window.graphqlClient) {
    window.graphqlClient.getArchiveGroups = async function () {
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

    window.graphqlClient.createArchiveGroup = async function (input) {
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

    window.graphqlClient.updateArchiveGroup = async function (input) {
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

    window.graphqlClient.deleteArchiveGroup = async function (name) {
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

    window.graphqlClient.enableArchiveGroup = async function (name) {
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

    window.graphqlClient.disableArchiveGroup = async function (name) {
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
