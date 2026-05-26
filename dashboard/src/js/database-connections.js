class DatabaseConnectionsManager {
    constructor() {
        this.connections = [];
        this.editingName = null;
        this.init();
    }

    async init() {
        if (!window.isLoggedIn()) {
            window.location.href = '/pages/login.html';
            return;
        }

        document.getElementById('type-filter')?.addEventListener('change', () => this.loadConnections());
        document.getElementById('refresh-btn')?.addEventListener('click', () => this.loadConnections());
        document.getElementById('new-btn')?.addEventListener('click', () => this.showEditor());
        document.getElementById('cancel-btn')?.addEventListener('click', () => this.hideEditor());
        document.getElementById('save-btn')?.addEventListener('click', () => this.saveConnection());
        document.getElementById('connection-type')?.addEventListener('change', () => this.updateTypeFields());

        await this.loadConnections();
    }

    async loadConnections() {
        this.showLoading(true);
        this.hideError();
        try {
            const type = document.getElementById('type-filter')?.value || null;
            const query = type
                ? `query GetConnections($type: DatabaseConnectionType) {
                    databaseConnections(type: $type) { name type url username database schema readOnly }
                }`
                : `query GetConnections {
                    databaseConnections { name type url username database schema readOnly }
                }`;
            const result = await window.graphqlClient.query(query, type ? { type } : {});
            this.connections = result.databaseConnections || [];
            this.renderTable();
        } catch (e) {
            this.showError('Failed to load database connections: ' + e.message);
        } finally {
            this.showLoading(false);
        }
    }

    renderTable() {
        const tbody = document.getElementById('connections-tbody');
        if (!tbody) return;
        if (this.connections.length === 0) {
            tbody.innerHTML = '<tr><td colspan="7" style="color:var(--text-muted);padding:1rem;">No database connections available for this filter</td></tr>';
            return;
        }

        tbody.innerHTML = this.connections.map((conn, index) => `
            <tr>
                <td class="mono">${this.escapeHtml(conn.name)}</td>
                <td><span class="badge badge-info">${conn.type}</span></td>
                <td class="mono">${this.escapeHtml(conn.url || '-')}</td>
                <td>${this.escapeHtml(conn.username || '-')}</td>
                <td>${this.escapeHtml(conn.type === 'MONGODB' ? (conn.database || '-') : (conn.schema || '-'))}</td>
                <td>${conn.readOnly ? '<span class="badge badge-default">Default</span>' : '<span class="badge badge-info">ConfigStore</span>'}</td>
                <td>
                    <div class="actions">
                        ${conn.readOnly ? '' : `<button class="btn btn-secondary btn-small" data-edit-index="${index}">Edit</button><button class="btn btn-danger btn-small" data-delete-name="${this.escapeHtml(conn.name)}">Delete</button>`}
                    </div>
                </td>
            </tr>
        `).join('');

        tbody.querySelectorAll('[data-edit-index]').forEach(btn => {
            btn.addEventListener('click', () => this.showEditor(this.connections[Number(btn.dataset.editIndex)]));
        });
        tbody.querySelectorAll('[data-delete-name]').forEach(btn => {
            btn.addEventListener('click', () => this.deleteConnection(btn.dataset.deleteName));
        });
    }

    showEditor(connection = null) {
        this.editingName = connection?.name || null;
        document.getElementById('editor-card').style.display = 'block';
        document.getElementById('editor-title').textContent = connection ? `Edit ${connection.name}` : 'New Connection';
        document.getElementById('connection-name').value = connection?.name || '';
        document.getElementById('connection-name').disabled = !!connection;
        document.getElementById('connection-type').value = connection?.type || 'POSTGRES';
        document.getElementById('connection-url').value = connection?.url || '';
        document.getElementById('connection-username').value = connection?.username || '';
        document.getElementById('connection-password').value = '';
        document.getElementById('connection-database').value = connection?.database || '';
        document.getElementById('connection-schema').value = connection?.schema || '';
        this.updateTypeFields();
        document.getElementById('editor-card').scrollIntoView({ behavior: 'smooth', block: 'start' });
    }

    updateTypeFields() {
        const type = document.getElementById('connection-type')?.value;
        const mongoGroup = document.getElementById('mongo-database-group');
        const postgresGroup = document.getElementById('postgres-schema-group');
        const usernameGroup = document.getElementById('username-group');
        const passwordGroup = document.getElementById('password-group');
        const urlLabel = document.querySelector('label[for="connection-url"]');
        const urlInput = document.getElementById('connection-url');

        if (mongoGroup) mongoGroup.style.display = type === 'MONGODB' ? 'flex' : 'none';
        if (postgresGroup) postgresGroup.style.display = type === 'POSTGRES' ? 'flex' : 'none';
        if (usernameGroup) usernameGroup.style.display = type === 'SQLITE' ? 'none' : 'flex';
        if (passwordGroup) passwordGroup.style.display = type === 'SQLITE' ? 'none' : 'flex';

        if (urlLabel && urlInput) {
            if (type === 'SQLITE') {
                urlLabel.textContent = 'Path';
                urlInput.placeholder = 'sqlite/monstermq-archive.db';
            } else if (type === 'MONGODB') {
                urlLabel.textContent = 'URL';
                urlInput.placeholder = 'mongodb://host:27017';
            } else {
                urlLabel.textContent = 'URL';
                urlInput.placeholder = 'jdbc:postgresql://host:5432/db';
            }
        }
    }

    hideEditor() {
        this.editingName = null;
        document.getElementById('editor-card').style.display = 'none';
    }

    collectInput() {
        const input = {
            name: document.getElementById('connection-name').value.trim(),
            type: document.getElementById('connection-type').value,
            url: document.getElementById('connection-url').value.trim(),
            username: document.getElementById('connection-username').value.trim() || null,
            password: document.getElementById('connection-password').value || null,
            database: document.getElementById('connection-database').value.trim() || null,
            schema: document.getElementById('connection-schema').value.trim() || null,
        };
        if (input.type === 'POSTGRES') {
            delete input.database;
        }
        if (input.type === 'MONGODB') {
            delete input.schema;
        }
        if (input.type === 'SQLITE') {
            delete input.username;
            delete input.password;
            delete input.database;
            delete input.schema;
        }
        if (!input.password) delete input.password;
        return input;
    }

    async saveConnection() {
        const input = this.collectInput();
        if (!input.name || !input.url) {
            this.showError('Name and URL are required.');
            return;
        }
        if (input.name === 'Default') {
            this.showError('Default is reserved for the read-only YAML connection.');
            return;
        }

        const mutationName = this.editingName ? 'updateDatabaseConnection' : 'createDatabaseConnection';
        const inputType = this.editingName ? 'UpdateDatabaseConnectionInput' : 'CreateDatabaseConnectionInput';
        try {
            const result = await window.graphqlClient.query(`
                mutation SaveDatabaseConnection($input: ${inputType}!) {
                    archiveGroup {
                        ${mutationName}(input: $input) {
                            success
                            message
                        }
                    }
                }
            `, { input });
            const payload = result.archiveGroup[mutationName];
            if (!payload.success) {
                this.showError(payload.message || 'Failed to save database connection.');
                return;
            }
            this.hideEditor();
            await this.loadConnections();
        } catch (e) {
            this.showError('Failed to save database connection: ' + e.message);
        }
    }

    async deleteConnection(name) {
        if (!confirm(`Delete database connection "${name}"?`)) return;
        try {
            const result = await window.graphqlClient.query(`
                mutation DeleteDatabaseConnection($name: String!) {
                    archiveGroup {
                        deleteDatabaseConnection(name: $name) {
                            success
                            message
                        }
                    }
                }
            `, { name });
            const payload = result.archiveGroup.deleteDatabaseConnection;
            if (!payload.success) {
                this.showError(payload.message || 'Failed to delete database connection.');
                return;
            }
            await this.loadConnections();
        } catch (e) {
            this.showError('Failed to delete database connection: ' + e.message);
        }
    }

    showLoading(show) {
        const el = document.getElementById('loading-indicator');
        if (el) el.style.display = show ? 'flex' : 'none';
    }

    showError(message) {
        const el = document.getElementById('error-message');
        if (el) {
            el.querySelector('.error-text').textContent = message;
            el.style.display = 'block';
        }
    }

    hideError() {
        const el = document.getElementById('error-message');
        if (el) el.style.display = 'none';
    }

    escapeHtml(value) {
        if (typeof value !== 'string') return value;
        return value
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;')
            .replace(/'/g, '&#039;');
    }
}

document.addEventListener('DOMContentLoaded', () => {
    window.databaseConnectionsManager = new DatabaseConnectionsManager();
});
