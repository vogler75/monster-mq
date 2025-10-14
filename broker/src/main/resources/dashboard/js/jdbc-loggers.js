class JDBCLoggersManager {
    constructor() {
        this.loggers = [];
        this.deleteLoggerName = null;
        this.clusterNodes = [];

        this.init();
    }

    init() {
        if (!this.isLoggedIn()) {
            window.location.href = '/pages/login.html';
            return;
        }

        this.setupEventListeners();
        this.loadClusterNodes();
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
            const addModal = document.getElementById('add-logger-modal');
            if (event.target === deleteModal) {
                this.hideConfirmDeleteModal();
            }
            if (event.target === addModal) {
                this.hideAddLoggerModal();
            }
        };
    }

    async loadClusterNodes() {
        try {
            const result = await window.graphqlClient.query(`
                query GetClusterNodes {
                    clusterNodes {
                        nodeId
                        isCurrent
                    }
                }
            `);

            this.clusterNodes = result.clusterNodes || [];
            this.populateNodeSelect();
        } catch (error) {
            console.error('Error loading cluster nodes:', error);
        }
    }

    populateNodeSelect() {
        const nodeSelect = document.getElementById('logger-node');
        if (!nodeSelect) return;

        // Clear existing options except the first one
        while (nodeSelect.options.length > 1) {
            nodeSelect.remove(1);
        }

        // Add "*" option for automatic assignment
        const autoOption = document.createElement('option');
        autoOption.value = '*';
        autoOption.textContent = '* (Automatic Assignment)';
        nodeSelect.appendChild(autoOption);

        // Add cluster nodes
        this.clusterNodes.forEach(node => {
            const option = document.createElement('option');
            option.value = node.nodeId;
            option.textContent = node.nodeId + (node.isCurrent ? ' (Current)' : '');
            nodeSelect.appendChild(option);
        });

        // Select the automatic assignment by default
        nodeSelect.value = '*';
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
        document.getElementById('add-logger-modal').style.display = 'flex';
        // Reset form
        document.getElementById('add-logger-form').reset();
        // Set defaults
        document.getElementById('logger-queue-type').value = 'MEMORY';
        document.getElementById('logger-payload-format').value = 'JSON';
        document.getElementById('logger-queue-size').value = '10000';
        document.getElementById('logger-bulk-size').value = '1000';
        document.getElementById('logger-bulk-timeout').value = '5000';
        document.getElementById('logger-reconnect-delay').value = '5000';
        document.getElementById('logger-disk-path').value = './buffer';
        document.getElementById('logger-enabled').checked = true;
        // Repopulate nodes
        this.populateNodeSelect();
    }

    hideAddLoggerModal() {
        document.getElementById('add-logger-modal').style.display = 'none';
        document.getElementById('add-logger-form').reset();
    }

    async addLogger() {
        try {
            // Get form values
            const name = document.getElementById('logger-name').value.trim();
            const namespace = document.getElementById('logger-namespace').value.trim();
            const nodeId = document.getElementById('logger-node').value;
            const enabled = document.getElementById('logger-enabled').checked;

            // Database configuration
            const databaseType = document.getElementById('logger-db-type').value;
            const jdbcUrl = document.getElementById('logger-jdbc-url').value.trim();
            const username = document.getElementById('logger-username').value.trim();
            const password = document.getElementById('logger-password').value;

            // Topic and table configuration
            const topicFiltersText = document.getElementById('logger-topic-filters').value.trim();
            const topicFilters = topicFiltersText.split('\n').map(line => line.trim()).filter(line => line.length > 0);
            const tableName = document.getElementById('logger-table-name').value.trim() || null;
            const tableNameJsonPath = document.getElementById('logger-table-jsonpath').value.trim() || null;
            const payloadFormat = document.getElementById('logger-payload-format').value;

            // JSON Schema
            const jsonSchemaText = document.getElementById('logger-json-schema').value.trim();

            // Queue configuration
            const queueType = document.getElementById('logger-queue-type').value;
            const queueSize = parseInt(document.getElementById('logger-queue-size').value);
            const diskPath = document.getElementById('logger-disk-path').value.trim();

            // Bulk configuration
            const bulkSize = parseInt(document.getElementById('logger-bulk-size').value);
            const bulkTimeoutMs = parseInt(document.getElementById('logger-bulk-timeout').value);
            const reconnectDelayMs = parseInt(document.getElementById('logger-reconnect-delay').value);

            // Validation
            if (!name || !namespace || !nodeId) {
                this.showError('Please fill in all required fields');
                return;
            }

            if (!databaseType || !jdbcUrl || !username || !password) {
                this.showError('Please fill in all database configuration fields');
                return;
            }

            if (topicFilters.length === 0) {
                this.showError('At least one topic filter is required');
                return;
            }

            if (!tableName && !tableNameJsonPath) {
                this.showError('Either fixed table name or table name JSONPath is required');
                return;
            }

            if (tableName && tableNameJsonPath) {
                this.showError('Cannot specify both fixed table name and JSONPath');
                return;
            }

            if (!jsonSchemaText) {
                this.showError('JSON Schema is required');
                return;
            }

            // Parse and validate JSON Schema
            let jsonSchema;
            try {
                jsonSchema = JSON.parse(jsonSchemaText);
            } catch (e) {
                this.showError('Invalid JSON Schema: ' + e.message);
                return;
            }

            // Build input object
            const input = {
                name,
                namespace,
                nodeId,
                enabled,
                config: {
                    databaseType,
                    jdbcUrl,
                    username,
                    password,
                    topicFilters,
                    payloadFormat,
                    jsonSchema,
                    queueType,
                    queueSize,
                    diskPath,
                    bulkSize,
                    bulkTimeoutMs,
                    reconnectDelayMs
                }
            };

            // Add optional table name fields
            if (tableName) {
                input.config.tableName = tableName;
            }
            if (tableNameJsonPath) {
                input.config.tableNameJsonPath = tableNameJsonPath;
            }

            console.log('Creating JDBC logger with input:', input);

            const result = await window.graphqlClient.query(`
                mutation CreateJDBCLogger($input: JDBCLoggerInput!) {
                    jdbcLogger {
                        create(input: $input) {
                            success
                            logger {
                                name
                                enabled
                            }
                        }
                    }
                }
            `, { input });

            if (result.jdbcLogger.create.success) {
                console.log('Logger created successfully');
                this.hideAddLoggerModal();
                await this.loadLoggers(); // Reload to show new logger
            } else {
                this.showError('Failed to create logger');
            }
        } catch (error) {
            console.error('Error creating logger:', error);
            this.showError('Failed to create logger: ' + error.message);
        }
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
window.hideAddLoggerModal = () => jdbcLoggersManager.hideAddLoggerModal();
window.addLogger = () => jdbcLoggersManager.addLogger();
window.hideConfirmDeleteModal = () => jdbcLoggersManager.hideConfirmDeleteModal();
window.confirmDeleteLogger = () => jdbcLoggersManager.confirmDeleteLogger();

// Initialize when page loads
document.addEventListener('DOMContentLoaded', () => {
    window.jdbcLoggersManager = new JDBCLoggersManager();
});
