class UserManager {
    constructor() {
        this.users = [];
        this.currentEditingUser = null;
        this.currentAclUsername = null;

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

        // Check if user management is enabled and show warning if disabled
        const userManagementEnabled = localStorage.getItem('monstermq_userManagementEnabled') === 'true';
        const disabledAlert = document.getElementById('user-mgmt-disabled-alert');

        if (!userManagementEnabled && disabledAlert) {
            disabledAlert.style.display = 'block';
        }

        this.setupUI();
        this.loadUsers();
    }

    isLoggedIn() {
        const token = safeStorage.getItem('monstermq_token');
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
        const token = safeStorage.getItem('monstermq_token');
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
        // UI setup is now handled by sidebar.js

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

        // Add ACL rule button (delegated inside modal after open)
        document.addEventListener('click', (e) => {
            if (e.target && e.target.id === 'add-acl-rule-btn') {
                if (this.currentAclUsername) {
                    this.addAclRule(this.currentAclUsername);
                }
            }
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
            } else if (e.target.classList.contains('delete-acl-btn')) {
                const ruleId = e.target.dataset.id;
                const username = e.target.dataset.username;
                this.deleteAclRule(username, ruleId);
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
            document.getElementById('user-username').setAttribute('readonly', '');
            document.getElementById('user-enabled').checked = user.enabled;
            document.getElementById('user-can-subscribe').checked = user.canSubscribe;
            document.getElementById('user-can-publish').checked = user.canPublish;
            document.getElementById('user-is-admin').checked = user.isAdmin;
        } else {
            document.getElementById('user-username').removeAttribute('readonly');
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
        let usernameField = formData.get('username');
        if (!usernameField && this.currentEditingUser) {
            usernameField = this.currentEditingUser.username;
        }
        if (!usernameField) {
            this.showAlert('Username is required', 'error');
            return;
        }
        const userData = {
            username: usernameField,
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

    async addAclRule(username) {
        const topicPattern = document.getElementById('acl-topic-pattern').value.trim();
        const priority = parseInt(document.getElementById('acl-priority').value.trim() || '0', 10);
        const canSubscribe = document.getElementById('acl-can-subscribe').checked;
        const canPublish = document.getElementById('acl-can-publish').checked;

        if (!topicPattern) {
            this.showAlert('Topic pattern is required', 'error');
            return;
        }
        if (!canSubscribe && !canPublish) {
            this.showAlert('Select at least one permission (Subscribe or Publish)', 'error');
            return;
        }

        try {
            document.getElementById('add-acl-rule-btn').disabled = true;
            const input = { username, topicPattern, canSubscribe, canPublish, priority };
            const result = await window.graphqlClient.createAclRule(input);
            if (result.success) {
                this.showAlert('ACL rule added', 'success');
                await this.loadUsers(); // refresh list
                this.showAclRules(username); // re-open / refresh modal content
                // reset minimal fields
                document.getElementById('acl-topic-pattern').value = '';
                document.getElementById('acl-can-subscribe').checked = false;
                document.getElementById('acl-can-publish').checked = false;
                document.getElementById('acl-priority').value = '0';
            } else {
                this.showAlert(result.message || 'Failed to add rule', 'error');
            }
        } catch (err) {
            console.error('Add ACL rule error', err);
            this.showAlert('Error adding rule: ' + err.message, 'error');
        } finally {
            document.getElementById('add-acl-rule-btn').disabled = false;
        }
    }

    async deleteAclRule(username, ruleId) {
        if (!confirm('Delete this ACL rule?')) return;
        try {
            const result = await window.graphqlClient.deleteAclRule(ruleId);
            if (result.success) {
                this.showAlert('ACL rule deleted', 'success');
                await this.loadUsers();
                // Refresh only the ACL rules table, not the entire modal (keeps tree state)
                this.refreshAclRulesTable();
            } else {
                this.showAlert(result.message || 'Failed to delete rule', 'error');
            }
        } catch (err) {
            console.error('Delete ACL rule error', err);
            this.showAlert('Error deleting rule: ' + err.message, 'error');
        }
    }

    refreshAclRulesTable() {
        if (!this.currentAclUsername) return;

        const user = this.users.find(u => u.username === this.currentAclUsername);
        if (!user) return;

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
                <div style="margin-bottom: 1rem; display: flex; gap: 0.5rem; align-items: center;">
                    <button id="delete-selected-acl-btn" class="btn" style="padding: 0.4rem 0.75rem; font-size: 0.8rem; background: var(--monster-red); border: 1px solid var(--monster-red); color: #fff; opacity: 0.5; cursor: not-allowed;" disabled>
                        Delete Selected (<span id="selected-count">0</span>)
                    </button>
                    <button id="select-all-acl-btn" class="btn btn-secondary" style="padding: 0.4rem 0.75rem; font-size: 0.8rem;">
                        Select All
                    </button>
                </div>
                <div style="overflow-x: auto;">
                    <div class="data-table">
                        <table style="min-width: 600px;">
                            <thead>
                                <tr>
                                    <th style="width: 40px; text-align: center;">
                                        <input type="checkbox" id="select-all-checkbox" style="cursor: pointer;">
                                    </th>
                                    <th style="min-width: 150px;">Topic Pattern</th>
                                    <th style="width: 50px; text-align: center;">Sub</th>
                                    <th style="width: 50px; text-align: center;">Pub</th>
                                    <th style="width: 60px; text-align: center;">Prio</th>
                                    <th style="width: 100px;">Created</th>
                                    <th style="width: 50px; text-align: center;"></th>
                                </tr>
                            </thead>
                            <tbody>
                                ${aclRules.map(rule => `
                                    <tr>
                                        <td style="text-align: center;">
                                            <input type="checkbox" class="acl-rule-checkbox" data-id="${rule.id}" style="cursor: pointer;">
                                        </td>
                                        <td style="font-family: monospace; word-break: break-all;">${this.escapeHtml(rule.topicPattern)}</td>
                                        <td style="text-align: center;">
                                            ${rule.canSubscribe ?
                    '<span style="color: var(--monster-green);">✓</span>' :
                    '<span style="color: var(--text-muted);">✗</span>'
                }
                                        </td>
                                        <td style="text-align: center;">
                                            ${rule.canPublish ?
                    '<span style="color: var(--monster-green);">✓</span>' :
                    '<span style="color: var(--text-muted);">✗</span>'
                }
                                        </td>
                                        <td style="text-align: center;">${rule.priority}</td>
                                        <td style="font-size: 0.75rem;">${rule.createdAt ? new Date(rule.createdAt).toLocaleDateString() : 'N/A'}</td>
                                        <td style="text-align: center;">
                                            <button class="btn delete-acl-btn" data-id="${rule.id}" data-username="${this.escapeHtml(this.currentAclUsername)}"
                                                    style="padding: 0.25rem 0.4rem; font-size: 0.75rem; background: var(--monster-red); border: 1px solid var(--monster-red); color: #fff; min-width: 0;"
                                                    title="Delete this rule">✕</button>
                                        </td>
                                    </tr>
                                `).join('')}
                            </tbody>
                        </table>
                    </div>
                </div>
            `;

        }

        // Re-setup drop zone after DOM replacement
        this.setupDropZone();
        this.setupTopicPatternDropZone();

        // Setup event handlers for checkboxes AFTER drop zone setup
        // (drop zone clones the element which removes event handlers)
        this.setupAclCheckboxHandlers();
    }

    setupAclCheckboxHandlers() {
        const selectAllCheckbox = document.getElementById('select-all-checkbox');
        const checkboxes = document.querySelectorAll('.acl-rule-checkbox');
        const deleteSelectedBtn = document.getElementById('delete-selected-acl-btn');
        const selectAllBtn = document.getElementById('select-all-acl-btn');
        const selectedCount = document.getElementById('selected-count');

        // Guard: if there are no ACL rules, these elements won't exist
        if (!selectAllCheckbox || !deleteSelectedBtn || !selectedCount || checkboxes.length === 0) {
            return;
        }

        const updateDeleteButton = () => {
            const checkedBoxes = document.querySelectorAll('.acl-rule-checkbox:checked');
            selectedCount.textContent = checkedBoxes.length;

            if (checkedBoxes.length > 0) {
                deleteSelectedBtn.disabled = false;
                deleteSelectedBtn.style.opacity = '1';
                deleteSelectedBtn.style.cursor = 'pointer';
            } else {
                deleteSelectedBtn.disabled = true;
                deleteSelectedBtn.style.opacity = '0.5';
                deleteSelectedBtn.style.cursor = 'not-allowed';
            }

            // Update select-all checkbox state
            if (checkedBoxes.length === 0) {
                selectAllCheckbox.checked = false;
                selectAllCheckbox.indeterminate = false;
            } else if (checkedBoxes.length === checkboxes.length) {
                selectAllCheckbox.checked = true;
                selectAllCheckbox.indeterminate = false;
            } else {
                selectAllCheckbox.checked = false;
                selectAllCheckbox.indeterminate = true;
            }
        };

        // Individual checkbox change
        checkboxes.forEach(checkbox => {
            checkbox.addEventListener('change', updateDeleteButton);
        });

        // Select all checkbox
        if (selectAllCheckbox) {
            selectAllCheckbox.addEventListener('change', (e) => {
                checkboxes.forEach(cb => cb.checked = e.target.checked);
                updateDeleteButton();
            });
        }

        // Select all button
        if (selectAllBtn) {
            selectAllBtn.addEventListener('click', () => {
                const allChecked = Array.from(checkboxes).every(cb => cb.checked);
                checkboxes.forEach(cb => cb.checked = !allChecked);
                updateDeleteButton();
            });
        }

        // Delete selected button
        if (deleteSelectedBtn) {
            deleteSelectedBtn.addEventListener('click', () => {
                this.deleteSelectedAclRules();
            });
        }

        updateDeleteButton();
    }

    async deleteSelectedAclRules() {
        const checkedBoxes = document.querySelectorAll('.acl-rule-checkbox:checked');
        const ruleIds = Array.from(checkedBoxes).map(cb => cb.dataset.id);

        if (ruleIds.length === 0) return;

        const confirmMsg = `Delete ${ruleIds.length} selected ACL rule${ruleIds.length > 1 ? 's' : ''}?`;
        if (!confirm(confirmMsg)) return;

        try {
            let successCount = 0;
            let failCount = 0;

            for (const ruleId of ruleIds) {
                try {
                    const result = await window.graphqlClient.deleteAclRule(ruleId);
                    if (result.success) {
                        successCount++;
                    } else {
                        failCount++;
                    }
                } catch (err) {
                    failCount++;
                    console.error('Error deleting rule:', ruleId, err);
                }
            }

            if (successCount > 0) {
                this.showAlert(`Successfully deleted ${successCount} ACL rule${successCount > 1 ? 's' : ''}`, 'success');
                await this.loadUsers();
                this.refreshAclRulesTable();
            }

            if (failCount > 0) {
                this.showAlert(`Failed to delete ${failCount} ACL rule${failCount > 1 ? 's' : ''}`, 'error');
            }
        } catch (err) {
            console.error('Delete selected rules error', err);
            this.showAlert('Error deleting rules: ' + err.message, 'error');
        }
    }

    showAclRules(username) {
        this.currentAclUsername = username;
        const user = this.users.find(u => u.username === username);
        if (!user) return;

        document.getElementById('acl-modal-title').textContent = `ACL Rules for ${username}`;

        // Render the table
        this.refreshAclRulesTable();

        // Initialize topic browser if not already initialized
        if (!this.aclTopicBrowser) {
            this.aclTopicBrowser = new AclTopicBrowser(this);
        }
        this.aclTopicBrowser.init();

        document.getElementById('acl-modal').style.display = 'block';
    }

    hideAclModal() {
        document.getElementById('acl-modal').style.display = 'none';
    }

    setupDropZone() {
        const dropZone = document.getElementById('acl-rules-content');
        if (!dropZone) return;

        // Use bound methods to avoid duplicates - get fresh reference each time
        if (!this._dropZoneHandlers) {
            this._dropZoneHandlers = {
                dragover: (e) => {
                    e.preventDefault();
                    e.dataTransfer.dropEffect = 'copy';
                    const zone = document.getElementById('acl-rules-content');
                    if (zone) zone.classList.add('drag-over');
                },
                dragleave: (e) => {
                    const zone = document.getElementById('acl-rules-content');
                    if (zone && e.target === zone) {
                        zone.classList.remove('drag-over');
                    }
                },
                drop: async (e) => {
                    e.preventDefault();
                    const zone = document.getElementById('acl-rules-content');
                    if (zone) zone.classList.remove('drag-over');

                    const topic = e.dataTransfer.getData('text/plain');
                    if (!topic || !this.currentAclUsername) return;

                    await this.addTopicToAcl(topic);
                }
            };
        }

        // Remove old listeners if they exist
        dropZone.removeEventListener('dragover', this._dropZoneHandlers.dragover);
        dropZone.removeEventListener('dragleave', this._dropZoneHandlers.dragleave);
        dropZone.removeEventListener('drop', this._dropZoneHandlers.drop);

        // Add new listeners
        dropZone.addEventListener('dragover', this._dropZoneHandlers.dragover);
        dropZone.addEventListener('dragleave', this._dropZoneHandlers.dragleave);
        dropZone.addEventListener('drop', this._dropZoneHandlers.drop);
    }

    setupTopicPatternDropZone() {
        const inputField = document.getElementById('acl-topic-pattern');
        if (!inputField) return;

        // Use bound methods to avoid duplicates
        if (!this._inputDropHandlers) {
            this._inputDropHandlers = {
                dragover: (e) => {
                    e.preventDefault();
                    e.dataTransfer.dropEffect = 'copy';
                    const input = document.getElementById('acl-topic-pattern');
                    if (input) input.classList.add('drag-over');
                },
                dragleave: (e) => {
                    const input = document.getElementById('acl-topic-pattern');
                    if (input && e.target === input) {
                        input.classList.remove('drag-over');
                    }
                },
                drop: (e) => {
                    e.preventDefault();
                    const input = document.getElementById('acl-topic-pattern');
                    if (input) input.classList.remove('drag-over');

                    const topic = e.dataTransfer.getData('text/plain');
                    if (!topic) return;

                    // Just populate the input field, don't add the rule yet
                    input.value = topic;
                    input.focus();
                }
            };
        }

        // Remove old listeners if they exist
        inputField.removeEventListener('dragover', this._inputDropHandlers.dragover);
        inputField.removeEventListener('dragleave', this._inputDropHandlers.dragleave);
        inputField.removeEventListener('drop', this._inputDropHandlers.drop);

        // Add new listeners
        inputField.addEventListener('dragover', this._inputDropHandlers.dragover);
        inputField.addEventListener('dragleave', this._inputDropHandlers.dragleave);
        inputField.addEventListener('drop', this._inputDropHandlers.drop);
    }

    async addTopicToAcl(topic) {
        if (!this.currentAclUsername) return;

        // Get selected permissions from drag controls
        const canSubscribe = document.getElementById('drag-can-subscribe').checked;
        const canPublish = document.getElementById('drag-can-publish').checked;
        const useWildcard = document.getElementById('drag-wildcard').checked;
        const priority = parseInt(document.getElementById('drag-priority').value || '0', 10);

        if (!canSubscribe && !canPublish) {
            this.showAlert('Please select at least one permission (Subscribe or Publish)', 'error');
            return;
        }

        // Apply wildcard if checked
        let topicPattern = topic;
        if (useWildcard) {
            topicPattern = topic + '/#';
        }

        // Create ACL rule
        try {
            const input = {
                username: this.currentAclUsername,
                topicPattern: topicPattern,
                canSubscribe,
                canPublish,
                priority
            };
            const result = await window.graphqlClient.createAclRule(input);
            if (result.success) {
                this.showAlert(`ACL rule added for topic: ${topicPattern}`, 'success');
                await this.loadUsers();
                // Refresh only the ACL rules table, not the entire modal (keeps tree state)
                this.refreshAclRulesTable();
            } else {
                this.showAlert(result.message || 'Failed to add rule', 'error');
            }
        } catch (err) {
            console.error('Add ACL rule error', err);
            this.showAlert('Error adding rule: ' + err.message, 'error');
        }
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

// ACL Topic Browser for drag-and-drop functionality
class AclTopicBrowser {
    constructor(userManager) {
        this.userManager = userManager;
        this.tree = document.getElementById('acl-topic-tree');
        this.searchInput = document.getElementById('acl-search-input');
        this.searchButton = document.getElementById('acl-search-button');
        this.browseButton = document.getElementById('acl-browse-button');
        this.archiveGroupSelect = document.getElementById('acl-archive-group-select');

        this.treeNodes = new Map();
        this.selectedArchiveGroup = 'Default';
        this.initialized = false;
    }

    async init() {
        if (!this.initialized) {
            this.setupEventListeners();
            await this.loadArchiveGroups();
            this.initialized = true;
        }
        this.browseRoot();
    }

    setupEventListeners() {
        // Browse/Search mode switching
        const browseMode = document.querySelector('input[name="acl-browse-mode"][value="browse"]');
        const searchMode = document.querySelector('input[name="acl-browse-mode"][value="search"]');

        browseMode.addEventListener('change', () => this.switchMode('browse'));
        searchMode.addEventListener('change', () => this.switchMode('search'));

        // Search/Browse buttons
        this.searchButton.addEventListener('click', () => this.performSearch());
        this.browseButton.addEventListener('click', () => this.browseRoot());

        // Enter key in search input
        this.searchInput.addEventListener('keypress', (e) => {
            if (e.key === 'Enter') {
                this.performSearch();
            }
        });

        // Archive group selection
        this.archiveGroupSelect.addEventListener('change', (e) => {
            this.selectedArchiveGroup = e.target.value;
            this.browseRoot();
        });
    }

    switchMode(mode) {
        if (mode === 'browse') {
            this.searchInput.disabled = true;
            this.searchInput.placeholder = 'Select "Search Topics" mode to search';
            this.browseButton.style.display = 'inline-block';
            this.searchButton.style.display = 'none';
        } else {
            this.searchInput.disabled = false;
            this.searchInput.placeholder = 'Search topics (e.g., sensor/+/temperature)...';
            this.browseButton.style.display = 'none';
            this.searchButton.style.display = 'inline-block';
        }
    }

    async loadArchiveGroups() {
        try {
            const query = `
                query GetArchiveGroups {
                    archiveGroups(enabled: true, lastValTypeNotEquals: NONE) {
                        name
                    }
                }
            `;

            const response = await graphqlClient.query(query);

            if (response && response.archiveGroups && response.archiveGroups.length > 0) {
                const groups = response.archiveGroups;
                this.archiveGroupSelect.innerHTML = '';

                groups.forEach(group => {
                    const option = document.createElement('option');
                    option.value = group.name;
                    option.textContent = group.name;
                    this.archiveGroupSelect.appendChild(option);
                });

                this.selectedArchiveGroup = groups[0].name;
                this.archiveGroupSelect.value = groups[0].name;
            } else {
                this.archiveGroupSelect.innerHTML = '<option value="">No archive groups available</option>';
            }
        } catch (error) {
            console.error('Error loading archive groups:', error);
            this.archiveGroupSelect.innerHTML = '<option value="">Error loading groups</option>';
        }
    }

    browseRoot() {
        this.tree.innerHTML = '';
        this.treeNodes.clear();
        this.createRootNode();
    }

    createRootNode() {
        const rootItem = this.createTreeItem('Broker', 'root', false, true);
        this.tree.appendChild(rootItem);

        const childContainer = document.createElement('ul');
        childContainer.className = 'tree-children';
        rootItem.appendChild(childContainer);

        this.loadTopicLevel('+', childContainer, '');

        const rootData = this.treeNodes.get('root');
        if (rootData) {
            rootData.toggle.classList.add('expanded');
            rootData.expanded = true;
        }
    }

    async loadTopicLevel(pattern, container, parentPath = '') {
        try {
            const loadingItem = document.createElement('li');
            loadingItem.className = 'loading';
            loadingItem.textContent = 'Loading...';
            container.appendChild(loadingItem);

            const query = `
                query BrowseTopics($topic: String!, $archiveGroup: String!) {
                    browseTopics(topic: $topic, archiveGroup: $archiveGroup) {
                        name
                    }
                }
            `;

            const response = await graphqlClient.query(query, {
                topic: pattern,
                archiveGroup: this.selectedArchiveGroup
            });

            container.removeChild(loadingItem);

            if (response && response.browseTopics && response.browseTopics.length > 0) {
                const topics = response.browseTopics;
                const topicList = topics.map(topic => ({
                    topic: topic.name,
                    hasValue: true
                }));

                const groupedTopics = this.groupTopicsByLevel(topicList, parentPath);

                for (const [levelName, topicData] of groupedTopics) {
                    const fullPath = parentPath ? `${parentPath}/${levelName}` : levelName;
                    const treeItem = this.createTreeItem(levelName, fullPath, topicData.hasValue, topicData.hasChildren);
                    container.appendChild(treeItem);
                }
            } else {
                const emptyItem = document.createElement('li');
                emptyItem.className = 'tree-node';
                emptyItem.innerHTML = '<div class="tree-item" style="color: var(--text-muted); font-style: italic;">No topics found</div>';
                container.appendChild(emptyItem);
            }
        } catch (error) {
            console.error('Error loading topic level:', error);
            const errorItem = document.createElement('li');
            errorItem.className = 'error';
            errorItem.textContent = 'Error: ' + error.message;
            container.appendChild(errorItem);
        }
    }

    groupTopicsByLevel(topics, parentPath) {
        const grouped = new Map();
        const parentLevels = parentPath ? parentPath.split('/').length : 0;

        for (const topic of topics) {
            const levels = topic.topic.split('/');

            if (parentLevels === 0) {
                const topLevel = levels[0];
                // For browse mode, assume topics can be expanded (we can't know without querying deeper)
                // Check if THIS specific topic has more levels (e.g., "winccoa/alerts" vs "winccoa")
                const topicHasChildren = levels.length > 1;
                const hasValue = levels.length === 1 && topic.hasValue;

                if (!grouped.has(topLevel)) {
                    // Start with assumption that topic can be expanded
                    grouped.set(topLevel, { hasValue, hasChildren: true });
                } else {
                    const existing = grouped.get(topLevel);
                    existing.hasValue = existing.hasValue || hasValue;
                    // If we see a deeper topic, we know for sure it has children
                    if (topicHasChildren) {
                        existing.hasChildren = true;
                    }
                }
            } else if (levels.length > parentLevels) {
                const nextLevel = levels[parentLevels];
                // Check if this topic has more levels beyond the current one
                const topicHasChildren = levels.length > parentLevels + 1;
                const hasValue = levels.length === parentLevels + 1 && topic.hasValue;

                if (!grouped.has(nextLevel)) {
                    // Start with assumption that topic can be expanded
                    grouped.set(nextLevel, { hasValue, hasChildren: true });
                } else {
                    const existing = grouped.get(nextLevel);
                    existing.hasValue = existing.hasValue || hasValue;
                    // If we see a deeper topic, we know for sure it has children
                    if (topicHasChildren) {
                        existing.hasChildren = true;
                    }
                }
            }
        }

        return grouped;
    }

    createTreeItem(name, fullPath, hasValue, hasChildren) {
        const li = document.createElement('li');
        li.className = 'tree-node';

        const item = document.createElement('div');
        item.className = 'tree-item';
        if (hasValue) {
            item.classList.add('has-data');
        }

        // Make topics with values draggable and double-clickable
        // Note: Topics can be both expandable AND draggable (e.g., "winccoa" can be expanded
        // to see children, but can also be dragged to add "winccoa/#" as an ACL rule)
        if (hasValue) {
            item.draggable = true;
            item.classList.add('draggable');
            this.setupDragHandlers(item, fullPath);

            // Add double-click handler as alternative to drag-and-drop
            item.addEventListener('dblclick', () => {
                this.userManager.addTopicToAcl(fullPath);
            });

            // Add visual hint for double-click
            item.title = 'Drag to ACL rules or double-click to add';
        }

        const toggle = document.createElement('button');
        toggle.className = 'tree-toggle';
        if (hasChildren) {
            toggle.innerHTML = '▶';
            toggle.addEventListener('click', (e) => {
                e.stopPropagation();
                this.toggleNode(li, fullPath);
            });
        }

        const icon = document.createElement('span');
        icon.className = `tree-icon ${hasChildren ? 'folder' : 'topic'}`;
        icon.innerHTML = hasChildren ? '📁' : '📄';

        const label = document.createElement('span');
        label.textContent = name;

        item.appendChild(toggle);
        item.appendChild(icon);
        item.appendChild(label);
        li.appendChild(item);

        this.treeNodes.set(fullPath, {
            element: li,
            item: item,
            toggle: toggle,
            expanded: false,
            hasChildren: hasChildren,
            hasValue: hasValue
        });

        return li;
    }

    setupDragHandlers(item, topicPath) {
        item.addEventListener('dragstart', (e) => {
            e.dataTransfer.effectAllowed = 'copy';
            e.dataTransfer.setData('text/plain', topicPath);
            item.classList.add('dragging');

            // Create drag ghost element
            const ghost = document.createElement('div');
            ghost.className = 'drag-ghost';
            ghost.textContent = `📄 ${topicPath}`;
            document.body.appendChild(ghost);
            e.dataTransfer.setDragImage(ghost, 0, 0);

            setTimeout(() => {
                document.body.removeChild(ghost);
            }, 0);
        });

        item.addEventListener('dragend', () => {
            item.classList.remove('dragging');
        });
    }

    async toggleNode(nodeElement, topicPath) {
        const nodeData = this.treeNodes.get(topicPath);
        if (!nodeData || !nodeData.hasChildren) return;

        if (nodeData.expanded) {
            const childContainer = nodeElement.querySelector('.tree-children');
            if (childContainer) {
                childContainer.classList.add('collapsed');
                nodeData.toggle.classList.remove('expanded');
                nodeData.expanded = false;
            }
        } else {
            let childContainer = nodeElement.querySelector('.tree-children');
            if (!childContainer) {
                childContainer = document.createElement('ul');
                childContainer.className = 'tree-children';
                nodeElement.appendChild(childContainer);

                const pattern = topicPath === 'root' ? '+' : `${topicPath}/+`;
                const parentPath = topicPath === 'root' ? '' : topicPath;
                await this.loadTopicLevel(pattern, childContainer, parentPath);
            } else {
                childContainer.classList.remove('collapsed');
            }

            nodeData.toggle.classList.add('expanded');
            nodeData.expanded = true;
        }
    }

    async performSearch() {
        const searchTerm = this.searchInput.value.trim();
        if (!searchTerm) return;

        try {
            this.tree.innerHTML = '<li class="loading">Searching topics...</li>';

            const query = `
                query SearchTopics($pattern: String!) {
                    searchTopics(pattern: $pattern)
                }
            `;

            const response = await graphqlClient.query(query, { pattern: searchTerm });

            if (response && response.searchTopics) {
                this.buildSearchTree(response.searchTopics);
            }
        } catch (error) {
            console.error('Error searching topics:', error);
            this.tree.innerHTML = `<li class="error">Search error: ${error.message}</li>`;
        }
    }

    buildSearchTree(topics) {
        this.tree.innerHTML = '';
        this.treeNodes.clear();

        if (topics.length === 0) {
            const emptyItem = document.createElement('li');
            emptyItem.className = 'tree-node';
            emptyItem.innerHTML = '<div class="tree-item" style="color: var(--text-muted);">No topics found</div>';
            this.tree.appendChild(emptyItem);
            return;
        }

        const topicList = topics.map(topicName => ({
            topic: topicName,
            hasValue: true
        }));

        const treeStructure = this.buildTreeStructure(topicList);
        this.renderTreeStructure(treeStructure, this.tree, '');
    }

    buildTreeStructure(topics) {
        const structure = new Map();

        for (const topic of topics) {
            const parts = topic.topic.split('/');
            let currentLevel = structure;

            for (let i = 0; i < parts.length; i++) {
                const part = parts[i];

                if (!currentLevel.has(part)) {
                    currentLevel.set(part, {
                        children: new Map(),
                        hasValue: false,
                        fullPath: parts.slice(0, i + 1).join('/')
                    });
                }

                const node = currentLevel.get(part);
                if (i === parts.length - 1) {
                    node.hasValue = topic.hasValue;
                }

                currentLevel = node.children;
            }
        }

        return structure;
    }

    renderTreeStructure(structure, container, parentPath) {
        for (const [name, data] of structure) {
            const hasChildren = data.children.size > 0;
            const treeItem = this.createTreeItem(name, data.fullPath, data.hasValue, hasChildren);
            container.appendChild(treeItem);

            if (hasChildren) {
                const childContainer = document.createElement('ul');
                childContainer.className = 'tree-children';
                treeItem.appendChild(childContainer);

                this.renderTreeStructure(data.children, childContainer, data.fullPath);

                const nodeData = this.treeNodes.get(data.fullPath);
                if (nodeData) {
                    nodeData.toggle.classList.add('expanded');
                    nodeData.expanded = true;
                }
            }
        }
    }
}

document.addEventListener('DOMContentLoaded', () => {
    new UserManager();
});