class TimeBaseLoggersManager {
    constructor() {
        this.loggers = [];
        this.deleteLoggerName = null;
        this.init();
    }

    init() {
        if (!this.isLoggedIn()) { window.location.href = '/pages/login.html'; return; }
        this.loadLoggers();
    }

    isLoggedIn() { return window.isLoggedIn && window.isLoggedIn(); }

    async loadLoggers() {
        try {
            const result = await window.graphqlClient.query(`
                query GetTimeBaseLoggers {
                    timebaseLoggers {
                        name namespace nodeId enabled isLocal
                        config { endpointUrl authType topicFilters tableName }
                        metrics { messagesIn messagesWritten connected }
                    }
                }
            `);
            this.loggers = result.timebaseLoggers || [];
            this.renderLoggers();
            this.updateMetrics();
        } catch (error) {
            console.error('Error loading TimeBase loggers:', error);
            this.showError('Failed to load TimeBase loggers: ' + error.message);
        }
    }

    updateMetrics() {
        const total = this.loggers.length;
        const enabled = this.loggers.filter(l => l.enabled).length;
        const local = this.loggers.filter(l => l.isLocal).length;
        const rate = this.loggers.reduce((acc, l) => acc + (l.metrics?.messagesIn || 0), 0);
        document.getElementById('total-loggers').textContent = total;
        document.getElementById('enabled-loggers').textContent = enabled;
        document.getElementById('current-node-loggers').textContent = local;
        document.getElementById('messages-rate').textContent = Math.round(rate);
    }

    renderLoggers() {
        const tbody = document.getElementById('loggers-table-body');
        tbody.innerHTML = '';
        this.loggers.forEach(logger => {
            const tr = document.createElement('tr');
            tr.innerHTML = `
                <td><strong>${logger.name}</strong></td>
                <td><small>${logger.config.endpointUrl}</small></td>
                <td>${logger.config.tableName ? logger.config.tableName : '<em style="color:var(--text-muted)">not set</em>'}</td>
                <td>${logger.config.topicFilters.join(', ')}</td>
                <td>${logger.nodeId} ${logger.isLocal ? '(Local)' : ''}</td>
                <td><span class="status-badge ${logger.enabled ? 'status-enabled' : 'status-disabled'}">${logger.enabled ? 'Enabled' : 'Disabled'}</span></td>
                <td>${Math.round(logger.metrics?.messagesIn || 0)}</td>
                <td>
                    <div class="action-buttons">
                        <a href="/pages/timebase-logger-detail.html?name=${encodeURIComponent(logger.name)}"><ix-icon-button icon="pen" variant="primary" ghost size="24" title="Edit logger"></ix-icon-button></a>
                        ${logger.enabled ?
                            `<ix-icon-button icon="pause" variant="primary" ghost size="24" title="Stop logger" onclick="timebaseLoggersManager.toggleLogger('${logger.name}', false)"></ix-icon-button>` :
                            `<ix-icon-button icon="play" variant="primary" ghost size="24" title="Start logger" onclick="timebaseLoggersManager.toggleLogger('${logger.name}', true)"></ix-icon-button>`
                        }
                        <ix-icon-button icon="trashcan" variant="primary" ghost size="24" class="btn-delete" title="Delete logger" onclick="timebaseLoggersManager.showConfirmDeleteModal('${logger.name}')"></ix-icon-button>
                    </div>
                </td>`;
            tbody.appendChild(tr);
        });
    }

    async toggleLogger(name, enabled) {
        try {
            await window.graphqlClient.query(`
                mutation ToggleTimeBaseLogger($name: String!, $enabled: Boolean!) {
                    timebaseLogger { toggle(name: $name, enabled: $enabled) }
                }
            `, { name, enabled });
            this.loadLoggers();
        } catch (error) { this.showError('Failed to toggle logger: ' + error.message); }
    }

    showConfirmDeleteModal(name) {
        this.deleteLoggerName = name;
        document.getElementById('delete-logger-name').textContent = name;
        document.getElementById('confirm-delete-modal').style.display = 'flex';
    }

    hideConfirmDeleteModal() { document.getElementById('confirm-delete-modal').style.display = 'none'; }

    async confirmDeleteLogger() {
        if (!this.deleteLoggerName) return;
        try {
            await window.graphqlClient.query(`
                mutation DeleteTimeBaseLogger($name: String!) {
                    timebaseLogger { delete(name: $name) }
                }
            `, { name: this.deleteLoggerName });
            this.hideConfirmDeleteModal();
            this.loadLoggers();
        } catch (error) { this.showError('Failed to delete logger: ' + error.message); }
    }

    showError(msg) {
        const errDiv = document.getElementById('error-message');
        errDiv.querySelector('.error-text').textContent = msg;
        errDiv.style.display = 'block';
        setTimeout(() => errDiv.style.display = 'none', 5000);
    }
}

window.timebaseLoggersManager = new TimeBaseLoggersManager();
window.refreshLoggers = () => window.timebaseLoggersManager.loadLoggers();
window.hideConfirmDeleteModal = () => window.timebaseLoggersManager.hideConfirmDeleteModal();
window.confirmDeleteLogger = () => window.timebaseLoggersManager.confirmDeleteLogger();
