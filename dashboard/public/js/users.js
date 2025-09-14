class UserManager {
    constructor() {
        this.users = [];
        this.currentEditingUser = null;

        this.init();
    }

    init() {
        if (!this.isLoggedIn()) {
            window.location.href = '/pages/login.html';
            return;
        }

        if (!this.isAdmin()) {
            window.location.href = '/pages/dashboard.html';
            return;
        }

        this.setupUI();
        this.loadUsers();
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

    isAdmin() {
        return localStorage.getItem('monstermq_isAdmin') === 'true';
    }

    getAuthHeaders() {
        const token = localStorage.getItem('monstermq_token');
        const headers = {
            'Content-Type': 'application/json'
        };

        // Only add Authorization header if token is not null
        if (token && token !== 'null') {
            headers.Authorization = `Bearer ${token}`;
        }

        return headers;
    }

    setupUI() {
        document.getElementById('logout-link').addEventListener('click', (e) => {
            e.preventDefault();
            this.logout();
        });

        document.getElementById('refresh-users').addEventListener('click', () => {
            this.loadUsers();
        });

        document.getElementById('add-user-btn').addEventListener('click', () => {
            this.showUserModal();
        });

        document.getElementById('close-user-modal').addEventListener('click', () => {
            this.hideUserModal();
        });

        document.getElementById('cancel-user-btn').addEventListener('click', () => {
            this.hideUserModal();
        });

        document.getElementById('close-acl-modal').addEventListener('click', () => {
            this.hideAclModal();
        });

        document.getElementById('user-form').addEventListener('submit', (e) => {
            this.handleUserSubmit(e);
        });

        // Event delegation for dynamically created buttons
        document.addEventListener('click', (e) => {
            if (e.target.classList.contains('edit-user-btn')) {
                const username = e.target.dataset.username;
                this.editUser(username);
            } else if (e.target.classList.contains('delete-user-btn')) {
                const username = e.target.dataset.username;
                this.deleteUser(username);
            } else if (e.target.classList.contains('view-acl-btn')) {
                const username = e.target.dataset.username;
                this.showAclRules(username);
            }
        });
    }

    logout() {
        localStorage.removeItem('monstermq_token');
        localStorage.removeItem('monstermq_username');
        localStorage.removeItem('monstermq_isAdmin');
        window.location.href = '/';
    }

    async loadUsers() {
        try {
            this.setRefreshLoading(true);

            // Use GraphQL client directly
            this.users = await window.graphqlClient.getUsers();
            this.updateMetrics();
            this.renderUsers();

        } catch (error) {
            console.error('Error loading users:', error);
            this.showAlert('Failed to load users: ' + error.message, 'error');
        } finally {
            this.setRefreshLoading(false);
        }
    }

    setRefreshLoading(loading) {
        const refreshBtn = document.getElementById('refresh-users');
        const refreshText = document.getElementById('refresh-text');
        const refreshSpinner = document.getElementById('refresh-spinner');

        refreshBtn.disabled = loading;

        if (loading) {
            refreshText.style.display = 'none';
            refreshSpinner.style.display = 'inline-block';
        } else {
            refreshText.style.display = 'inline';
            refreshSpinner.style.display = 'none';
        }
    }

    updateMetrics() {
        const activeUsers = this.users.filter(u => u.enabled).length;
        const adminUsers = this.users.filter(u => u.isAdmin).length;
        const totalAclRules = this.users.reduce((sum, u) => sum + (u.aclRules?.length || 0), 0);

        document.getElementById('total-users').textContent = this.users.length;
        document.getElementById('active-users').textContent = activeUsers;
        document.getElementById('admin-users').textContent = adminUsers;
        document.getElementById('total-acl-rules').textContent = totalAclRules;
    }

    renderUsers() {
        const tableBody = document.getElementById('users-table-body');

        if (this.users.length === 0) {
            tableBody.innerHTML = `
                <tr>
                    <td colspan="7" style="text-align: center; color: var(--text-muted); padding: 2rem;">
                        No users found
                    </td>
                </tr>
            `;
            return;
        }

        tableBody.innerHTML = this.users.map(user => {
            const permissions = [];
            if (user.canSubscribe) permissions.push('Sub');
            if (user.canPublish) permissions.push('Pub');

            return `
                <tr>
                    <td><strong>${this.escapeHtml(user.username)}</strong></td>
                    <td>
                        <span class="status-indicator ${user.enabled ? 'status-online' : 'status-offline'}">
                            <span class="status-dot"></span>
                            ${user.enabled ? 'Enabled' : 'Disabled'}
                        </span>
                    </td>
                    <td>
                        ${permissions.map(p => `
                            <span style="padding: 0.125rem 0.5rem; background: var(--monster-purple); color: white; border-radius: 12px; font-size: 0.75rem; margin-right: 0.25rem;">
                                ${p}
                            </span>
                        `).join('')}
                    </td>
                    <td>
                        ${user.isAdmin ?
                            '<span style="color: var(--monster-orange);">⚡ Admin</span>' :
                            '<span style="color: var(--text-muted);">User</span>'
                        }
                    </td>
                    <td>
                        <button class="btn btn-secondary view-acl-btn"
                                data-username="${this.escapeHtml(user.username)}"
                                style="padding: 0.25rem 0.5rem; font-size: 0.75rem;">
                            ${(user.aclRules?.length || 0)} rules
                        </button>
                    </td>
                    <td>${user.createdAt ? new Date(user.createdAt).toLocaleDateString() : 'N/A'}</td>
                    <td>
                        <div style="display: flex; gap: 0.5rem;">
                            <button class="btn btn-secondary edit-user-btn"
                                    data-username="${this.escapeHtml(user.username)}"
                                    style="padding: 0.25rem 0.5rem; font-size: 0.75rem;">
                                Edit
                            </button>
                            <button class="btn delete-user-btn"
                                    data-username="${this.escapeHtml(user.username)}"
                                    style="padding: 0.25rem 0.5rem; font-size: 0.75rem; background: var(--monster-red); color: white; border: 1px solid var(--monster-red);">
                                Delete
                            </button>
                        </div>
                    </td>
                </tr>
            `;
        }).join('');
    }

    showUserModal(user = null) {
        this.currentEditingUser = user;
        const isEditing = user !== null;

        document.getElementById('user-modal-title').textContent = isEditing ? 'Edit User' : 'Add New User';
        document.getElementById('save-user-text').textContent = isEditing ? 'Update User' : 'Create User';

        // Reset form
        const form = document.getElementById('user-form');
        form.reset();

        // Show/hide password field based on edit mode
        const passwordGroup = document.getElementById('password-group');
        const passwordInput = document.getElementById('user-password');
        if (isEditing) {
            passwordGroup.style.display = 'none';
            passwordInput.removeAttribute('required');
        } else {
            passwordGroup.style.display = 'block';
            passwordInput.setAttribute('required', '');
        }

        // Populate form if editing
        if (isEditing) {
            document.getElementById('user-username').value = user.username;
            document.getElementById('user-username').disabled = true;
            document.getElementById('user-enabled').checked = user.enabled;
            document.getElementById('user-can-subscribe').checked = user.canSubscribe;
            document.getElementById('user-can-publish').checked = user.canPublish;
            document.getElementById('user-is-admin').checked = user.isAdmin;
        } else {
            document.getElementById('user-username').disabled = false;
            document.getElementById('user-enabled').checked = true;
            document.getElementById('user-can-subscribe').checked = true;
            document.getElementById('user-can-publish').checked = true;
            document.getElementById('user-is-admin').checked = false;
        }

        document.getElementById('user-modal').style.display = 'block';
    }

    hideUserModal() {
        document.getElementById('user-modal').style.display = 'none';
        this.currentEditingUser = null;
    }

    async handleUserSubmit(e) {
        e.preventDefault();

        const formData = new FormData(e.target);
        const userData = {
            username: formData.get('username'),
            enabled: formData.get('enabled') === 'on',
            canSubscribe: formData.get('canSubscribe') === 'on',
            canPublish: formData.get('canPublish') === 'on',
            isAdmin: formData.get('isAdmin') === 'on'
        };

        const password = formData.get('password');
        if (password) {
            userData.password = password;
        }

        try {
            this.setSaveUserLoading(true);

            const isEditing = this.currentEditingUser !== null;
            let result;

            if (isEditing) {
                result = await window.graphqlClient.updateUser(userData);
            } else {
                result = await window.graphqlClient.createUser(userData);
            }

            if (result.success) {
                this.showAlert(`User ${isEditing ? 'updated' : 'created'} successfully!`, 'success');
                this.hideUserModal();
                this.loadUsers();
            } else {
                this.showAlert(result.message || 'Failed to save user', 'error');
            }
        } catch (error) {
            console.error('Error saving user:', error);
            this.showAlert('Error occurred: ' + error.message, 'error');
        } finally {
            this.setSaveUserLoading(false);
        }
    }

    setSaveUserLoading(loading) {
        const saveBtn = document.getElementById('save-user-btn');
        const saveText = document.getElementById('save-user-text');
        const saveSpinner = document.getElementById('save-user-spinner');

        saveBtn.disabled = loading;

        if (loading) {
            saveText.style.display = 'none';
            saveSpinner.style.display = 'inline-block';
        } else {
            saveText.style.display = 'inline';
            saveSpinner.style.display = 'none';
        }
    }

    editUser(username) {
        const user = this.users.find(u => u.username === username);
        if (user) {
            this.showUserModal(user);
        }
    }

    async deleteUser(username) {
        if (!confirm(`Are you sure you want to delete user "${username}"?`)) {
            return;
        }

        try {
            const result = await window.graphqlClient.deleteUser(username);

            if (result.success) {
                this.showAlert('User deleted successfully!', 'success');
                this.loadUsers();
            } else {
                this.showAlert(result.message || 'Failed to delete user', 'error');
            }
        } catch (error) {
            console.error('Error deleting user:', error);
            this.showAlert('Error occurred: ' + error.message, 'error');
        }
    }

    showAclRules(username) {
        const user = this.users.find(u => u.username === username);
        if (!user) return;

        document.getElementById('acl-modal-title').textContent = `ACL Rules for ${username}`;

        const aclRules = user.aclRules || [];
        const aclContent = document.getElementById('acl-rules-content');

        if (aclRules.length === 0) {
            aclContent.innerHTML = `
                <div style="text-align: center; color: var(--text-muted); padding: 2rem;">
                    No ACL rules defined for this user
                </div>
            `;
        } else {
            aclContent.innerHTML = `
                <div class="data-table">
                    <table>
                        <thead>
                            <tr>
                                <th>Topic Pattern</th>
                                <th>Subscribe</th>
                                <th>Publish</th>
                                <th>Priority</th>
                                <th>Created</th>
                            </tr>
                        </thead>
                        <tbody>
                            ${aclRules.map(rule => `
                                <tr>
                                    <td style="font-family: monospace;">${this.escapeHtml(rule.topicPattern)}</td>
                                    <td>
                                        ${rule.canSubscribe ?
                                            '<span style="color: var(--monster-green);">✓</span>' :
                                            '<span style="color: var(--text-muted);">✗</span>'
                                        }
                                    </td>
                                    <td>
                                        ${rule.canPublish ?
                                            '<span style="color: var(--monster-green);">✓</span>' :
                                            '<span style="color: var(--text-muted);">✗</span>'
                                        }
                                    </td>
                                    <td>${rule.priority}</td>
                                    <td>${rule.createdAt ? new Date(rule.createdAt).toLocaleDateString() : 'N/A'}</td>
                                </tr>
                            `).join('')}
                        </tbody>
                    </table>
                </div>
            `;
        }

        document.getElementById('acl-modal').style.display = 'block';
    }

    hideAclModal() {
        document.getElementById('acl-modal').style.display = 'none';
    }

    showAlert(message, type = 'error') {
        const alertContainer = document.getElementById('alert-container');
        alertContainer.innerHTML = `
            <div class="alert alert-${type}">
                ${message}
            </div>
        `;

        setTimeout(() => {
            alertContainer.innerHTML = '';
        }, 5000);
    }

    escapeHtml(text) {
        const div = document.createElement('div');
        div.textContent = text;
        return div.innerHTML;
    }
}

document.addEventListener('DOMContentLoaded', () => {
    new UserManager();
});