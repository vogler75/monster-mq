class JDBCLoggersManager {
    constructor() {
        this.loggers = [];
        this.deleteLoggerName = null;

        this.init();
    }

    init() {
        if (!this.isLoggedIn()) {
            window.location.href = '/pages/login.html';
            return;
        }

        this.setupEventListeners();
        this.loadLoggers();
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

    setupEventListeners() {
        // Modal close on outside click
        window.onclick = (event) => {
            const deleteModal = document.getElementById('confirm-delete-modal');
            if (event.target === deleteModal) {
                this.hideConfirmDeleteModal();
            }
        };
    }

    async loadLoggers() {
        try {
            console.log('Loading JDBC loggers...');
            const result = await window.graphqlClient.query(`
                query GetJDBCLoggers {
                    jdbcLoggers {
                        name
                        namespace
                        nodeId
                        enabled
                        isOnCurrentNode
                        createdAt
                        updatedAt
                        config {
                            databaseType
                            jdbcUrl
                            username
                            topicFilters
                            tableName
                            tableNameJsonPath
                            payloadFormat
                            queueType
                            queueSize
                            diskPath
                            bulkSize
                            bulkTimeoutMs
                            reconnectDelayMs
                        }
                        metrics {
                            messagesIn
                            messagesWritten
                            queueSize
                            timestamp
                        }
                    }
                }
            `);

            this.loggers = result.jdbcLoggers || [];
            this.renderLoggers();
            this.updateMetrics();
        } catch (error) {
            console.error('Error loading JDBC loggers:', error);
            this.showError('Failed to load JDBC loggers: ' + error.message);
        }
    }

    updateMetrics() {
        const totalLoggers = this.loggers.length;
        const enabledLoggers = this.loggers.filter(l => l.enabled).length;
        const currentNodeLoggers = this.loggers.filter(l => l.isOnCurrentNode).length;

        // Calculate total messages rate
        let totalMessagesRate = 0;
        this.loggers.forEach(logger => {
            if (logger.metrics && logger.metrics.length > 0) {
                totalMessagesRate += logger.metrics[0].messagesIn || 0;
            }
        });

        document.getElementById('total-loggers').textContent = totalLoggers;
        document.getElementById('enabled-loggers').textContent = enabledLoggers;
        document.getElementById('current-node-loggers').textContent = currentNodeLoggers;
        document.getElementById('messages-rate').textContent = Math.round(totalMessagesRate);
    }

    renderLoggers() {
        const tbody = document.getElementById('loggers-table-body');

        if (this.loggers.length === 0) {
            tbody.innerHTML = `
                <tr>
                    <td colspan="8" style="text-align: center; padding: 2rem; color: var(--text-secondary);">
                        No JDBC loggers found. Create your first logger to get started.
                    </td>
                </tr>
            `;
            return;
        }

        tbody.innerHTML = this.loggers.map(logger => {
            const messagesIn = logger.metrics && logger.metrics.length > 0 ?
                Math.round(logger.metrics[0].messagesIn || 0) : 0;
            const messagesOut = logger.metrics && logger.metrics.length > 0 ?
                Math.round(logger.metrics[0].messagesWritten || 0) : 0;

            const tableName = logger.config.tableName ||
                (logger.config.tableNameJsonPath ? `${logger.config.tableNameJsonPath}` : 'Dynamic');

            return `
            <tr>
                <td>
                    <strong>${this.escapeHtml(logger.name)}</strong>
                    <br><small style="color: var(--text-secondary);">${this.escapeHtml(logger.namespace)}</small>
                </td>
                <td>${this.escapeHtml(logger.config.databaseType)}</td>
                <td>
                    <div class="topic-filters">
                        ${logger.config.topicFilters.slice(0, 2).map(filter =>
                            `<span class="topic-filter-tag">${this.escapeHtml(filter)}</span>`
                        ).join('')}
                        ${logger.config.topicFilters.length > 2 ?
                            `<span class="topic-filter-tag" style="background: var(--accent-blue); color: white;">+${logger.config.topicFilters.length - 2} more</span>` :
                            ''
                        }
                    </div>
                </td>
                <td><code style="font-size: 0.8rem;">${this.escapeHtml(tableName)}</code></td>
                <td>
                    ${this.escapeHtml(logger.nodeId)}
                    ${logger.isOnCurrentNode ? '<br><small style="color: var(--monster-green);">● Current</small>' : ''}
                </td>
                <td>
                    <span class="status-badge ${logger.enabled ? 'status-enabled' : 'status-disabled'}">
                        <span class="status-indicator"></span>
                        ${logger.enabled ? 'Enabled' : 'Disabled'}
                    </span>
                </td>
                <td style="color: #9333EA; white-space: nowrap;">
                    ${messagesIn} / ${messagesOut}
                </td>
                <td>
                    <div class="action-buttons">
                        ${logger.enabled ?
                            `<button class="btn btn-icon btn-pause" onclick="jdbcLoggersManager.toggleLogger('${this.escapeHtml(logger.name)}', false)" title="Stop logger">
                                <svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor">
                                    <rect x="6" y="4" width="4" height="16"></rect>
                                    <rect x="14" y="4" width="4" height="16"></rect>
                                </svg>
                            </button>` :
                            `<button class="btn btn-icon btn-play" onclick="jdbcLoggersManager.toggleLogger('${this.escapeHtml(logger.name)}', true)" title="Start logger">
                                <svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor">
                                    <polygon points="5 3 19 12 5 21 5 3"></polygon>
                                </svg>
                            </button>`
                        }
                        <button class="btn btn-icon btn-delete" onclick="jdbcLoggersManager.showConfirmDeleteModal('${this.escapeHtml(logger.name)}')" title="Delete logger">
                            <svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor">
                                <path d="M3 6h18M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6m3 0V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2"></path>
                            </svg>
                        </button>
                    </div>
                </td>
            </tr>`;
        }).join('');
    }

    async toggleLogger(name, enable) {
        try {
            console.log(`${enable ? 'Starting' : 'Stopping'} logger:`, name);

            const methodName = enable ? 'start' : 'stop';
            const result = await window.graphqlClient.query(`
                mutation ${enable ? 'StartLogger' : 'StopLogger'}($name: String!) {
                    jdbcLogger {
                        ${methodName}(name: $name) {
                            success
                            logger {
                                name
                                enabled
                            }
                        }
                    }
                }
            `, { name });

            if (result.jdbcLogger[methodName].success) {
                console.log(`Logger ${enable ? 'started' : 'stopped'} successfully`);
                await this.loadLoggers(); // Reload to update UI
            } else {
                this.showError(`Failed to ${enable ? 'start' : 'stop'} logger`);
            }
        } catch (error) {
            console.error(`Error ${enable ? 'starting' : 'stopping'} logger:`, error);
            this.showError(`Failed to ${enable ? 'start' : 'stop'} logger: ` + error.message);
        }
    }

    showConfirmDeleteModal(name) {
        this.deleteLoggerName = name;
        document.getElementById('delete-logger-name').textContent = name;
        document.getElementById('confirm-delete-modal').style.display = 'flex';
    }

    hideConfirmDeleteModal() {
        this.deleteLoggerName = null;
        document.getElementById('confirm-delete-modal').style.display = 'none';
    }

    async confirmDeleteLogger() {
        if (!this.deleteLoggerName) return;

        const name = this.deleteLoggerName;
        this.hideConfirmDeleteModal();

        try {
            console.log('Deleting logger:', name);

            const result = await window.graphqlClient.query(`
                mutation DeleteLogger($name: String!) {
                    jdbcLogger {
                        delete(name: $name)
                    }
                }
            `, { name });

            if (result.jdbcLogger.delete) {
                console.log('Logger deleted successfully');
                await this.loadLoggers(); // Reload to update UI
            } else {
                this.showError('Failed to delete logger');
            }
        } catch (error) {
            console.error('Error deleting logger:', error);
            this.showError('Failed to delete logger: ' + error.message);
        }
    }

    showAddLoggerModal() {
        // TODO: Implement add logger modal
        alert('Add logger functionality will be implemented in a future update.\n\nFor now, please use the GraphQL API or configuration files to add JDBC loggers.');
    }

    showError(message) {
        const errorDiv = document.getElementById('error-message');
        const errorText = errorDiv.querySelector('.error-text');
        errorText.textContent = message;
        errorDiv.style.display = 'block';

        setTimeout(() => {
            errorDiv.style.display = 'none';
        }, 5000);
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
window.refreshLoggers = () => jdbcLoggersManager.loadLoggers();
window.showAddLoggerModal = () => jdbcLoggersManager.showAddLoggerModal();
window.hideConfirmDeleteModal = () => jdbcLoggersManager.hideConfirmDeleteModal();
window.confirmDeleteLogger = () => jdbcLoggersManager.confirmDeleteLogger();

// Initialize when page loads
document.addEventListener('DOMContentLoaded', () => {
    window.jdbcLoggersManager = new JDBCLoggersManager();
});
