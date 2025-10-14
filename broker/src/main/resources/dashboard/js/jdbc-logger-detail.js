class JDBCLoggerDetailManager {
    constructor() {
        this.isNewLogger = false;
        this.loggerName = null;
        this.originalLogger = null;
        this.clusterNodes = [];
        this.metricsTimer = null;

        this.init();
    }

    init() {
        if (!this.isLoggedIn()) {
            window.location.href = '/pages/login.html';
            return;
        }

        this.parseUrlParams();
        this.loadClusterNodes();

        if (this.isNewLogger) {
            this.initNewLogger();
        } else {
            this.loadLogger();
        }

        // Cleanup on page unload
        window.addEventListener('beforeunload', () => this.cleanup());
    }

    cleanup() {
        if (this.metricsTimer) {
            clearInterval(this.metricsTimer);
            this.metricsTimer = null;
        }
    }

    isLoggedIn() {
        const token = localStorage.getItem('monstermq_token');
        if (!token) return false;
        if (token === 'null') return true;

        try {
            const decoded = JSON.parse(atob(token.split('.')[1]));
            const now = Date.now() / 1000;
            return decoded.exp > now;
        } catch {
            return false;
        }
    }

    parseUrlParams() {
        const params = new URLSearchParams(window.location.search);
        this.isNewLogger = params.get('new') === 'true';
        this.loggerName = params.get('name');

        if (!this.isNewLogger && !this.loggerName) {
            window.location.href = '/pages/jdbc-loggers.html';
            return;
        }

        document.getElementById('logger-title').textContent =
            this.isNewLogger ? 'Create JDBC Logger' : `Edit Logger: ${this.loggerName}`;
        document.getElementById('logger-subtitle').textContent =
            this.isNewLogger ? 'Configure a new JDBC logger' : 'Edit logger configuration';
        document.getElementById('save-logger-btn').textContent =
            this.isNewLogger ? 'Create Logger' : 'Save Changes';
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
            this.showError('Failed to load cluster nodes: ' + error.message);
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
    }

    initNewLogger() {
        document.getElementById('loading-indicator').style.display = 'none';
        document.getElementById('logger-content').style.display = 'block';

        // Set defaults
        document.getElementById('logger-enabled').checked = true;
        document.getElementById('logger-queue-type').value = 'MEMORY';
        document.getElementById('logger-payload-format').value = 'JSON';
        document.getElementById('logger-queue-size').value = '10000';
        document.getElementById('logger-bulk-size').value = '1000';
        document.getElementById('logger-bulk-timeout').value = '5000';
        document.getElementById('logger-reconnect-delay').value = '5000';
        document.getElementById('logger-disk-path').value = './buffer';

        // Set default JSON schema with timestamp format and mapping
        const defaultSchema = {
            "type": "object",
            "properties": {
                "ts": {
                    "type": "string",
                    "format": "timestamp"
                },
                "sensor_id": {
                    "type": "string"
                },
                "value": {
                    "type": "number"
                }
            },
            "required": ["value"],
            "arrayPath": "$.sensors[*]",
            "mapping": {
                "ts": "$.timestamp",
                "sensor_id": "$.id",
                "value": "$.value"
            }
        };
        document.getElementById('logger-json-schema').value = JSON.stringify(defaultSchema, null, 2);

        // Hide timestamps and metrics sections (only for editing)
        document.getElementById('timestamps-section').style.display = 'none';
        document.getElementById('metrics-section').style.display = 'none';
    }

    async loadLogger() {
        try {
            document.getElementById('loading-indicator').style.display = 'flex';
            document.getElementById('logger-content').style.display = 'none';

            const result = await window.graphqlClient.query(`
                query GetJDBCLogger($name: String!) {
                    jdbcLoggers(name: $name) {
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
                            jsonSchema
                            queueType
                            queueSize
                            diskPath
                            bulkSize
                            bulkTimeoutMs
                            reconnectDelayMs
                        }
                        metrics {
                            messagesIn
                            messagesValidated
                            messagesWritten
                            messagesSkipped
                            validationErrors
                            writeErrors
                            queueSize
                            queueCapacity
                            queueFull
                            timestamp
                        }
                    }
                }
            `, { name: this.loggerName });

            if (!result.jdbcLoggers || result.jdbcLoggers.length === 0) {
                throw new Error('Logger not found');
            }

            this.originalLogger = result.jdbcLoggers[0];
            this.populateForm();
            this.renderMetrics();

            // Start periodic metrics refresh
            if (this.metricsTimer) clearInterval(this.metricsTimer);
            this.metricsTimer = setInterval(() => this.refreshMetrics(), 10000);

            document.getElementById('loading-indicator').style.display = 'none';
            document.getElementById('logger-content').style.display = 'block';
        } catch (error) {
            console.error('Error loading logger:', error);
            this.showError('Failed to load logger: ' + error.message);
            setTimeout(() => {
                window.location.href = '/pages/jdbc-loggers.html';
            }, 2000);
        }
    }

    populateForm() {
        const logger = this.originalLogger;

        // Basic info
        document.getElementById('logger-name').value = logger.name;
        document.getElementById('logger-name').disabled = true; // Can't change name when editing
        document.getElementById('logger-namespace').value = logger.namespace;
        document.getElementById('logger-node').value = logger.nodeId;
        document.getElementById('logger-enabled').checked = logger.enabled;

        // Database config
        document.getElementById('logger-db-type').value = logger.config.databaseType;
        document.getElementById('logger-jdbc-url').value = logger.config.jdbcUrl;
        document.getElementById('logger-username').value = logger.config.username;
        document.getElementById('logger-password').value = ''; // Don't populate password
        document.getElementById('logger-password').placeholder = 'Leave blank to keep current password';

        // MQTT config
        document.getElementById('logger-topic-filters').value = logger.config.topicFilters.join('\n');
        document.getElementById('logger-payload-format').value = logger.config.payloadFormat;

        // Table config
        document.getElementById('logger-table-name').value = logger.config.tableName || '';
        document.getElementById('logger-table-jsonpath').value = logger.config.tableNameJsonPath || '';

        // JSON Schema
        document.getElementById('logger-json-schema').value =
            typeof logger.config.jsonSchema === 'string' ?
            logger.config.jsonSchema :
            JSON.stringify(logger.config.jsonSchema, null, 2);

        // Queue config
        document.getElementById('logger-queue-type').value = logger.config.queueType;
        document.getElementById('logger-queue-size').value = logger.config.queueSize;
        document.getElementById('logger-disk-path').value = logger.config.diskPath || './buffer';

        // Bulk write config
        document.getElementById('logger-bulk-size').value = logger.config.bulkSize;
        document.getElementById('logger-bulk-timeout').value = logger.config.bulkTimeoutMs;
        document.getElementById('logger-reconnect-delay').value = logger.config.reconnectDelayMs;

        // Timestamps
        document.getElementById('logger-created-at').textContent =
            logger.createdAt ? new Date(logger.createdAt).toLocaleString() : '-';
        document.getElementById('logger-updated-at').textContent =
            logger.updatedAt ? new Date(logger.updatedAt).toLocaleString() : '-';
        document.getElementById('timestamps-section').style.display = 'block';
    }

    renderMetrics() {
        if (!this.originalLogger || !this.originalLogger.metrics) return;
        const m = this.originalLogger.metrics;
        if (m.length === 0) return;

        const metrics = m[0];
        document.getElementById('metric-messages-in').textContent = Math.round(metrics.messagesIn || 0);
        document.getElementById('metric-messages-validated').textContent = Math.round(metrics.messagesValidated || 0);
        document.getElementById('metric-messages-written').textContent = Math.round(metrics.messagesWritten || 0);
        document.getElementById('metric-messages-skipped').textContent = Math.round(metrics.messagesSkipped || 0);
        document.getElementById('metric-validation-errors').textContent = Math.round(metrics.validationErrors || 0);
        document.getElementById('metric-write-errors').textContent = Math.round(metrics.writeErrors || 0);
        document.getElementById('metric-queue-size').textContent = Math.round(metrics.queueSize || 0);
        document.getElementById('metric-queue-capacity').textContent = Math.round(metrics.queueCapacity || 0);
        document.getElementById('metric-queue-full').textContent = metrics.queueFull ? 'FULL' : 'Normal';
        document.getElementById('metric-timestamp').textContent =
            metrics.timestamp ? new Date(metrics.timestamp).toLocaleString() : '-';
        document.getElementById('metrics-section').style.display = 'block';
    }

    async refreshMetrics() {
        if (!this.loggerName) return;
        try {
            const result = await window.graphqlClient.query(`
                query GetJDBCLoggerMetrics($name: String!) {
                    jdbcLoggers(name: $name) {
                        metrics {
                            messagesIn
                            messagesValidated
                            messagesWritten
                            messagesSkipped
                            validationErrors
                            writeErrors
                            queueSize
                            queueCapacity
                            queueFull
                            timestamp
                        }
                    }
                }
            `, { name: this.loggerName });

            if (result.jdbcLoggers && result.jdbcLoggers.length > 0) {
                this.originalLogger.metrics = result.jdbcLoggers[0].metrics;
                this.renderMetrics();
            }
        } catch (error) {
            console.warn('Metrics refresh failed:', error.message);
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

    validateForm() {
        // Required fields
        const name = document.getElementById('logger-name').value.trim();
        const namespace = document.getElementById('logger-namespace').value.trim();
        const nodeId = document.getElementById('logger-node').value;
        const databaseType = document.getElementById('logger-db-type').value;
        const jdbcUrl = document.getElementById('logger-jdbc-url').value.trim();
        const username = document.getElementById('logger-username').value.trim();
        const password = document.getElementById('logger-password').value;
        const topicFiltersText = document.getElementById('logger-topic-filters').value.trim();
        const tableName = document.getElementById('logger-table-name').value.trim();
        const tableNameJsonPath = document.getElementById('logger-table-jsonpath').value.trim();
        const jsonSchemaText = document.getElementById('logger-json-schema').value.trim();

        if (!name || !namespace || !nodeId) {
            this.showError('Please fill in all required fields (Name, Namespace, Node)');
            return false;
        }

        if (!databaseType || !jdbcUrl || !username) {
            this.showError('Please fill in all database configuration fields');
            return false;
        }

        // Password is required for new loggers but optional for editing
        if (this.isNewLogger && !password) {
            this.showError('Password is required for new loggers');
            return false;
        }

        const topicFilters = topicFiltersText.split('\n').map(line => line.trim()).filter(line => line.length > 0);
        if (topicFilters.length === 0) {
            this.showError('At least one topic filter is required');
            return false;
        }

        if (!tableName && !tableNameJsonPath) {
            this.showError('Either fixed table name or table name JSONPath is required');
            return false;
        }

        if (tableName && tableNameJsonPath) {
            this.showError('Cannot specify both fixed table name and JSONPath');
            return false;
        }

        if (!jsonSchemaText) {
            this.showError('JSON Schema is required');
            return false;
        }

        // Validate JSON Schema
        try {
            JSON.parse(jsonSchemaText);
        } catch (e) {
            this.showError('Invalid JSON Schema: ' + e.message);
            return false;
        }

        return true;
    }

    async saveLogger() {
        if (!this.validateForm()) {
            return;
        }

        try {
            const name = document.getElementById('logger-name').value.trim();
            const namespace = document.getElementById('logger-namespace').value.trim();
            const nodeId = document.getElementById('logger-node').value;
            const enabled = document.getElementById('logger-enabled').checked;

            const databaseType = document.getElementById('logger-db-type').value;
            const jdbcUrl = document.getElementById('logger-jdbc-url').value.trim();
            const username = document.getElementById('logger-username').value.trim();
            const password = document.getElementById('logger-password').value;

            const topicFiltersText = document.getElementById('logger-topic-filters').value.trim();
            const topicFilters = topicFiltersText.split('\n').map(line => line.trim()).filter(line => line.length > 0);
            const tableName = document.getElementById('logger-table-name').value.trim() || null;
            const tableNameJsonPath = document.getElementById('logger-table-jsonpath').value.trim() || null;
            const payloadFormat = document.getElementById('logger-payload-format').value;

            const jsonSchemaText = document.getElementById('logger-json-schema').value.trim();
            const jsonSchema = JSON.parse(jsonSchemaText);

            const queueType = document.getElementById('logger-queue-type').value;
            const queueSize = parseInt(document.getElementById('logger-queue-size').value);
            const diskPath = document.getElementById('logger-disk-path').value.trim();

            const bulkSize = parseInt(document.getElementById('logger-bulk-size').value);
            const bulkTimeoutMs = parseInt(document.getElementById('logger-bulk-timeout').value);
            const reconnectDelayMs = parseInt(document.getElementById('logger-reconnect-delay').value);

            const input = {
                name,
                namespace,
                nodeId,
                enabled,
                config: {
                    databaseType,
                    jdbcUrl,
                    username,
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

            // Only include password if provided
            // - For new loggers: password is required (validated above)
            // - For editing: if empty, password won't be changed on backend
            if (password && password.trim().length > 0) {
                input.config.password = password;
            }

            // Add table name fields
            if (tableName) {
                input.config.tableName = tableName;
            }
            if (tableNameJsonPath) {
                input.config.tableNameJsonPath = tableNameJsonPath;
            }

            console.log('Saving JDBC logger with input:', input);

            let result;
            if (this.isNewLogger) {
                // Create new logger
                result = await window.graphqlClient.query(`
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
                    window.location.href = '/pages/jdbc-loggers.html';
                } else {
                    this.showError('Failed to create logger');
                }
            } else {
                // Update existing logger
                result = await window.graphqlClient.query(`
                    mutation UpdateJDBCLogger($name: String!, $input: JDBCLoggerInput!) {
                        jdbcLogger {
                            update(name: $name, input: $input) {
                                success
                                logger {
                                    name
                                    enabled
                                }
                            }
                        }
                    }
                `, { name: this.loggerName, input });

                if (result.jdbcLogger.update.success) {
                    console.log('Logger updated successfully');
                    window.location.href = '/pages/jdbc-loggers.html';
                } else {
                    this.showError('Failed to update logger');
                }
            }
        } catch (error) {
            console.error('Error saving logger:', error);
            this.showError('Failed to save logger: ' + error.message);
        }
    }

    showDeleteModal() {
        document.getElementById('delete-logger-name').textContent = this.loggerName;
        document.getElementById('delete-logger-modal').style.display = 'flex';
    }

    hideDeleteModal() {
        document.getElementById('delete-logger-modal').style.display = 'none';
    }

    async confirmDeleteLogger() {
        this.hideDeleteModal();

        try {
            console.log('Deleting logger:', this.loggerName);

            const result = await window.graphqlClient.query(`
                mutation DeleteLogger($name: String!) {
                    jdbcLogger {
                        delete(name: $name)
                    }
                }
            `, { name: this.loggerName });

            if (result.jdbcLogger.delete) {
                console.log('Logger deleted successfully');
                window.location.href = '/pages/jdbc-loggers.html';
            } else {
                this.showError('Failed to delete logger');
            }
        } catch (error) {
            console.error('Error deleting logger:', error);
            this.showError('Failed to delete logger: ' + error.message);
        }
    }
}

// Global functions
function goBack() {
    window.location.href = '/pages/jdbc-loggers.html';
}

function saveLogger() {
    window.loggerDetailManager.saveLogger();
}

function showDeleteModal() {
    window.loggerDetailManager.showDeleteModal();
}

function hideDeleteModal() {
    window.loggerDetailManager.hideDeleteModal();
}

function confirmDeleteLogger() {
    window.loggerDetailManager.confirmDeleteLogger();
}

function showSchemaHelp() {
    document.getElementById('schema-help-modal').style.display = 'flex';
}

function hideSchemaHelp() {
    document.getElementById('schema-help-modal').style.display = 'none';
}

// Initialize when page loads
document.addEventListener('DOMContentLoaded', () => {
    window.loggerDetailManager = new JDBCLoggerDetailManager();
});

// Global function for refresh button
function refreshMetrics() {
    if (window.loggerDetailManager) {
        window.loggerDetailManager.refreshMetrics();
    }
}

// Handle modal clicks (close when clicking outside)
window.onclick = (event) => {
    const deleteModal = document.getElementById('delete-logger-modal');
    const helpModal = document.getElementById('schema-help-modal');

    if (event.target === deleteModal) {
        hideDeleteModal();
    }
    if (event.target === helpModal) {
        hideSchemaHelp();
    }
};
