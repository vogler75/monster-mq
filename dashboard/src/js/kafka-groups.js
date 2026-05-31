// Kafka Consumer Groups Dashboard JavaScript

class KafkaGroupsManager {
    constructor() {
        this.client = new GraphQLDashboardClient();
        this.groups = [];
        this.filteredList = [];
        this.deleteGroupId = null;
        this.refreshInterval = null;
        this.init();
    }

    async init() {
        console.log('Initializing Kafka Consumer Groups Manager...');
        await this.loadGroups();
        
        // Auto refresh every 15 seconds for status updates
        this.refreshInterval = setInterval(() => this.loadGroups(false), 15000);
        
        window.registerPageCleanup(() => {
            if (this.refreshInterval) {
                clearInterval(this.refreshInterval);
            }
        });
    }

    async loadGroups(showLoadingState = true) {
        if (showLoadingState) {
            this.showLoading(true);
        }
        this.hideError();
        try {
            const query = `
                query GetKafkaConsumerGroups {
                    kafkaConsumerGroups {
                        groupId
                        topics
                        lastCommitTime
                    }
                }
            `;
            const result = await this.client.query(query);
            if (!result || !result.kafkaConsumerGroups) throw new Error('Invalid response structure');
            
            // Sort by last commit time descending
            this.groups = result.kafkaConsumerGroups.sort((a, b) => {
                return new Date(b.lastCommitTime).getTime() - new Date(a.lastCommitTime).getTime();
            });
            
            this.filterGroups();
        } catch (e) {
            console.error('Error loading Kafka consumer groups:', e);
            this.showError('Failed to load consumer groups: ' + e.message);
        } finally {
            if (showLoadingState) {
                this.showLoading(false);
            }
        }
    }

    filterGroups() {
        const searchInput = document.getElementById('group-search');
        const query = searchInput ? searchInput.value.toLowerCase().trim() : '';
        
        if (query === '') {
            this.filteredList = [...this.groups];
        } else {
            this.filteredList = this.groups.filter(g => g.groupId.toLowerCase().includes(query));
        }
        
        this.updateMetrics();
        this.renderTable();
    }

    updateMetrics() {
        const totalGroups = this.groups.length;
        
        // Count unique topics across all groups
        const uniqueTopics = new Set();
        this.groups.forEach(g => {
            if (g.topics) {
                g.topics.forEach(t => uniqueTopics.add(t));
            }
        });
        
        // Find most recent commit time
        let maxTime = 0;
        let mostRecentStr = 'Never';
        this.groups.forEach(g => {
            const t = new Date(g.lastCommitTime).getTime();
            if (t > maxTime) {
                maxTime = t;
                mostRecentStr = this.formatRelativeTime(g.lastCommitTime);
            }
        });

        document.getElementById('total-groups').textContent = totalGroups;
        document.getElementById('active-topics').textContent = uniqueTopics.size;
        
        const activityEl = document.getElementById('recent-activity');
        activityEl.textContent = mostRecentStr;
        if (maxTime > 0) {
            activityEl.style.color = 'var(--monster-green, #10B981)';
        } else {
            activityEl.style.color = 'var(--text-muted)';
        }
    }

    renderTable() {
        const tbody = document.getElementById('kafka-groups-table-body');
        if (!tbody) return;
        tbody.innerHTML = '';
        
        if (this.filteredList.length === 0) {
            tbody.innerHTML = `<tr><td colspan="4" class="no-data">No consumer groups found.</td></tr>`;
            return;
        }

        this.filteredList.forEach(group => {
            const row = document.createElement('tr');
            
            // Render subscribed topic chips
            let chipsHtml = '';
            if (group.topics && group.topics.length > 0) {
                chipsHtml = `<div class="chip-container">` + 
                    group.topics.map(t => `<span class="topic-chip">${this.escapeHtml(t)}</span>`).join('') + 
                    `</div>`;
            } else {
                chipsHtml = `<span style="color:var(--text-muted); font-size:0.875rem;">None</span>`;
            }
            
            // Format commit time
            const commitDate = new Date(group.lastCommitTime);
            const dateStr = isNaN(commitDate.getTime()) || commitDate.getTime() <= 0
                ? 'Never' 
                : `${commitDate.toLocaleString()} <span class="relative-time">(${this.formatRelativeTime(group.lastCommitTime)})</span>`;

            row.innerHTML = `
                <td>
                    <div style="font-weight:600; color:var(--text-primary); font-family:monospace; font-size:0.95rem;">
                        ${this.escapeHtml(group.groupId)}
                    </div>
                </td>
                <td>${chipsHtml}</td>
                <td>${dateStr}</td>
                <td>
                    <div class="action-buttons">
                        <ix-icon-button icon="trashcan" variant="primary" ghost size="24" class="btn-delete" title="Remove consumer group" onclick="kafkaGroupsManager.deleteGroup('${group.groupId}')"></ix-icon-button>
                    </div>
                </td>
            `;
            tbody.appendChild(row);
        });
    }

    deleteGroup(groupId) {
        this.deleteGroupId = groupId;
        document.getElementById('delete-group-name').textContent = groupId;
        document.getElementById('confirm-delete-group-modal').style.display = 'flex';
    }

    async confirmDelete() {
        if (!this.deleteGroupId) return;
        
        this.showLoading(true);
        try {
            const mutation = `
                mutation DeleteKafkaConsumerGroup($groupId: String!) {
                    kafkaServer {
                        deleteConsumerGroup(groupId: $groupId)
                    }
                }
            `;
            const variables = { groupId: this.deleteGroupId };
            const result = await this.client.query(mutation, variables);
            
            if (result && result.kafkaServer && result.kafkaServer.deleteConsumerGroup) {
                console.log(`Successfully deleted consumer group: ${this.deleteGroupId}`);
                this.loadGroups(true);
            } else {
                throw new Error('Deletion request returned failure');
            }
        } catch (e) {
            console.error('Failed to delete consumer group:', e);
            this.showError('Failed to remove consumer group: ' + e.message);
            this.showLoading(false);
        } finally {
            this.hideDeleteModal();
        }
    }

    hideDeleteModal() {
        this.deleteGroupId = null;
        document.getElementById('confirm-delete-group-modal').style.display = 'none';
    }

    formatRelativeTime(isoString) {
        const date = new Date(isoString);
        if (isNaN(date.getTime()) || date.getTime() <= 0) return 'Never';
        
        const now = new Date();
        const diffMs = now.getTime() - date.getTime();
        const diffSecs = Math.max(0, Math.floor(diffMs / 1000));
        
        if (diffSecs < 5) return 'just now';
        if (diffSecs < 60) return `${diffSecs}s ago`;
        
        const diffMins = Math.floor(diffSecs / 60);
        if (diffMins < 60) return `${diffMins}m ago`;
        
        const diffHours = Math.floor(diffMins / 60);
        if (diffHours < 24) return `${diffHours}h ago`;
        
        const diffDays = Math.floor(diffHours / 24);
        return `${diffDays}d ago`;
    }

    escapeHtml(str) {
        if (!str) return '';
        return str
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;')
            .replace(/'/g, '&#039;');
    }

    showLoading(show) {
        const loader = document.getElementById('loading-indicator');
        if (loader) {
            loader.style.display = show ? 'flex' : 'none';
        }
    }

    showError(msg) {
        const errorEl = document.getElementById('error-message');
        if (errorEl) {
            errorEl.querySelector('.error-text').textContent = msg;
            errorEl.style.display = 'flex';
        }
    }

    hideError() {
        const errorEl = document.getElementById('error-message');
        if (errorEl) {
            errorEl.style.display = 'none';
        }
    }
}

// Global hookups
var kafkaGroupsManager;
document.addEventListener('DOMContentLoaded', () => {
    kafkaGroupsManager = new KafkaGroupsManager();
});

function refreshKafkaGroups() {
    if (kafkaGroupsManager) {
        kafkaGroupsManager.loadGroups(true);
    }
}

function hideConfirmDeleteGroupModal() {
    if (kafkaGroupsManager) {
        kafkaGroupsManager.hideDeleteModal();
    }
}

function confirmDeleteGroup() {
    if (kafkaGroupsManager) {
        kafkaGroupsManager.confirmDelete();
    }
}
