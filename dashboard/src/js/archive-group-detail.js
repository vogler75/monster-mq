class ArchiveGroupDetailManager {
    constructor() {
        this.groupName = null;
        this.isNew = false;
        this.groupData = null;
        this.init();
    }

    async init() {
        if (!window.isLoggedIn()) {
            window.location.href = '/pages/login.html';
            return;
        }

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
    }

    async loadData() {
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
    }

    collectFormData() {
        const topicFilterText = document.getElementById('group-topic-filter').value.trim();
        const topicFilter = topicFilterText.split('\n').map(l => l.trim()).filter(l => l.length > 0);

        return {
            name: this.isNew ? document.getElementById('group-name').value.trim() : this.groupName,
            topicFilter,
            lastValType: document.getElementById('group-last-val-type').value,
            archiveType: document.getElementById('group-archive-type').value,
            payloadFormat: document.getElementById('group-payload-format').value,
            retainedOnly: document.getElementById('group-retained-only').checked,
            lastValRetention: document.getElementById('group-last-val-retention').value.trim() || null,
            archiveRetention: document.getElementById('group-archive-retention').value.trim() || null,
            purgeInterval: document.getElementById('group-purge-interval').value.trim() || null,
        };
    }

    validateFormData(data) {
        if (!data.name) {
            this.showError('Name is required.');
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
        const toast = document.getElementById('success-toast');
        if (toast) {
            toast.textContent = msg;
            toast.style.display = 'block';
            setTimeout(() => { toast.style.display = 'none'; }, 3000);
        }
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
}

document.addEventListener('DOMContentLoaded', () => {
    window.archiveGroupDetail = new ArchiveGroupDetailManager();
});
