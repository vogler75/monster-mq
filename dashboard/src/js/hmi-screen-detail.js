// HMI Screen Detail Page Manager

class HmiScreenDetailManager {
    constructor() {
        this.hmiName = null;
        this.isNew = true;
        this.hmiData = null;
        this.files = [];
        this.init();
    }

    async init() {
        if (!window.isLoggedIn()) {
            window.location.href = '/pages/login.html';
            return;
        }

        const params = new URLSearchParams(window.location.search);
        this.hmiName = params.get('name');
        this.isNew = params.get('new') === 'true' || !this.hmiName;

        this.attachEventListeners();
        await this.loadClusterNodes();

        if (!this.isNew && this.hmiName) {
            await this.loadHmi();
        } else {
            this.setupNewMode();
        }
    }

    async loadClusterNodes() {
        try {
            const query = `query GetBrokers { brokers { nodeId isCurrent } }`;
            const result = await window.graphqlClient.query(query);
            const brokers = result?.brokers || [];
            const select = document.getElementById('node-id-select');
            if (select) {
                select.innerHTML = '<option value="local">local (Current Node)</option>';
                brokers.forEach(b => {
                    if (b.nodeId && b.nodeId !== 'local') {
                        const opt = document.createElement('option');
                        opt.value = b.nodeId;
                        opt.textContent = b.nodeId + (b.isCurrent ? ' (Current)' : '');
                        select.appendChild(opt);
                    }
                });
            }
        } catch (e) {
            console.error('Failed to load cluster nodes:', e);
        }
    }

    attachEventListeners() {
        const saveBtn = document.getElementById('save-btn');
        if (saveBtn) saveBtn.addEventListener('click', () => this.saveHmi());

        const deleteBtn = document.getElementById('delete-btn');
        if (deleteBtn) deleteBtn.addEventListener('click', () => this.deleteHmi());

        const exportBtn = document.getElementById('export-btn');
        if (exportBtn) exportBtn.addEventListener('click', () => this.exportZip());

        const uploadPkgBtn = document.getElementById('upload-pkg-btn');
        const fileInput = document.getElementById('zip-file-input');

        if (uploadPkgBtn && fileInput) {
            uploadPkgBtn.addEventListener('click', () => fileInput.click());
            fileInput.addEventListener('change', (e) => {
                const file = e.target.files?.[0];
                if (file) {
                    this.handleZipUpload(file);
                    fileInput.value = '';
                }
            });
        }
    }

    setupNewMode() {
        const crumb = document.getElementById('breadcrumb-name') || document.getElementById('breadcrumb-title');
        if (crumb) crumb.textContent = 'New HMI Screen';
        document.getElementById('page-title').textContent = 'Configure New HMI Screen';
        document.getElementById('name-input').disabled = false;
        document.getElementById('delete-btn').style.display = 'none';
        document.getElementById('export-btn').style.display = 'none';
        document.getElementById('launch-btn').style.display = 'none';
        document.getElementById('files-section').style.display = 'none';
    }

    async loadHmi() {
        window.ui.setLoading(true);
        window.ui.clearError();
        try {
            const query = `
                query GetHmiDetail($name: String!) {
                    hmi(name: $name) {
                        name
                        nodeId
                        enabled
                        createdAt
                        updatedAt
                        isOnCurrentNode
                        fileCount
                        sizeBytes
                        config {
                            urlPath
                            isMain
                            title
                            description
                            entryPoint
                        }
                    }
                    hmiFiles(name: $name) {
                        path
                        sizeBytes
                    }
                }
            `;
            const result = await window.graphqlClient.query(query, { name: this.hmiName });
            this.hmiData = result?.hmi;
            this.files = result?.hmiFiles || [];

            if (!this.hmiData) {
                throw new Error(`HMI screen "${this.hmiName}" not found`);
            }

            this.populateForm();
        } catch (e) {
            console.error('Failed to load HMI detail:', e);
            window.ui.showError('Failed to load HMI details: ' + e.message);
        } finally {
            window.ui.setLoading(false);
        }
    }

    populateForm() {
        const d = this.hmiData;
        const cfg = d.config || {};

        const crumb = document.getElementById('breadcrumb-name') || document.getElementById('breadcrumb-title');
        if (crumb) crumb.textContent = d.name;
        document.getElementById('page-title').textContent = `HMI: ${d.name}`;

        const nameInput = document.getElementById('name-input');
        nameInput.value = d.name || '';
        nameInput.disabled = true;

        const nodeSelect = document.getElementById('node-id-select');
        if (nodeSelect) {
            let found = false;
            for (let i = 0; i < nodeSelect.options.length; i++) {
                if (nodeSelect.options[i].value === d.nodeId) {
                    found = true;
                    break;
                }
            }
            if (!found && d.nodeId) {
                const opt = document.createElement('option');
                opt.value = d.nodeId;
                opt.textContent = d.nodeId;
                nodeSelect.appendChild(opt);
            }
            nodeSelect.value = d.nodeId || 'local';
        }
        document.getElementById('enabled-toggle').checked = d.enabled !== false;
        document.getElementById('is-main-toggle').checked = cfg.isMain === true;

        document.getElementById('title-input').value = cfg.title || '';
        document.getElementById('url-path-input').value = cfg.urlPath || '';
        document.getElementById('entry-point-input').value = cfg.entryPoint || 'index.html';
        document.getElementById('description-input').value = cfg.description || '';

        // Show buttons
        document.getElementById('delete-btn').style.display = 'inline-flex';
        document.getElementById('export-btn').style.display = 'inline-flex';

        const launchBtn = document.getElementById('launch-btn');
        const urlPath = cfg.urlPath || d.name;
        const viewUrl = cfg.isMain ? '/hmi/' : `/hmi/${encodeURIComponent(urlPath)}/`;
        launchBtn.href = viewUrl;
        launchBtn.style.display = 'inline-flex';

        // Render files
        document.getElementById('files-section').style.display = 'block';
        this.renderFilesTable();
    }

    renderFilesTable() {
        const tbody = document.getElementById('files-table-body');
        if (!tbody) return;

        if (this.files.length === 0) {
            tbody.innerHTML = `
                <tr>
                    <td colspan="2" style="text-align:center; color: var(--text-muted); padding: 1.5rem;">
                        No assets found in package directory. Click "Upload ZIP Package" to deploy assets.
                    </td>
                </tr>
            `;
            return;
        }

        tbody.innerHTML = this.files.map(f => `
            <tr>
                <td><code style="font-size: 0.85rem;">${this.escapeHtml(f.path)}</code></td>
                <td class="num" style="font-size: 0.85rem; color: var(--text-muted);">${this.formatBytes(f.sizeBytes)}</td>
            </tr>
        `).join('');
    }

    async saveHmi() {
        const nameInput = document.getElementById('name-input');
        const name = nameInput.value.trim();
        if (!name) {
            window.ui.showError('Application Name is required');
            return;
        }

        const nodeSelect = document.getElementById('node-id-select');
        const nodeId = nodeSelect ? nodeSelect.value : 'local';
        const enabled = document.getElementById('enabled-toggle').checked;
        const isMain = document.getElementById('is-main-toggle').checked;
        const title = document.getElementById('title-input').value.trim();
        const urlPath = document.getElementById('url-path-input').value.trim();
        const entryPoint = document.getElementById('entry-point-input').value.trim() || 'index.html';
        const description = document.getElementById('description-input').value.trim();

        const input = {
            name: name,
            nodeId: nodeId,
            enabled: enabled,
            config: {
                isMain: isMain,
                urlPath: urlPath,
                title: title,
                description: description,
                entryPoint: entryPoint
            }
        };

        try {
            window.ui.setLoading(true);
            window.ui.clearError();

            const mutation = this.isNew ? `
                mutation CreateHmi($input: HmiInput!) {
                    hmi {
                        create(input: $input) {
                            success
                            message
                            hmi {
                                name
                            }
                        }
                    }
                }
            ` : `
                mutation UpdateHmi($name: String!, $input: HmiInput!) {
                    hmi {
                        update(name: $name, input: $input) {
                            success
                            message
                            hmi {
                                name
                            }
                        }
                    }
                }
            `;

            const variables = this.isNew ? { input } : { name, input };
            const result = await window.graphqlClient.query(mutation, variables);
            const res = this.isNew ? result?.hmi?.create : result?.hmi?.update;

            if (res && res.success) {
                window.ui.success(`HMI screen "${name}" saved successfully`);
                if (this.isNew) {
                    window.location.href = `/pages/hmi-screen-detail.html?name=${encodeURIComponent(name)}`;
                } else {
                    await this.loadHmi();
                }
            } else {
                throw new Error(res?.message || 'Failed to save HMI configuration');
            }
        } catch (e) {
            console.error('Error saving HMI:', e);
            window.ui.showError('Failed to save HMI configuration: ' + e.message);
        } finally {
            window.ui.setLoading(false);
        }
    }

    async deleteHmi() {
        if (!this.hmiName) return;

        const confirmed = await window.ui.showConfirm({
            title: 'Delete HMI Screen',
            message: `Are you sure you want to delete HMI screen "${this.hmiName}"? All hosted assets will be removed.`,
            confirmText: 'Delete',
            type: 'danger'
        });

        if (!confirmed) return;

        try {
            window.ui.setLoading(true);
            const mutation = `
                mutation DeleteHmi($name: String!) {
                    hmi {
                        delete(name: $name) {
                            success
                            message
                        }
                    }
                }
            `;
            const result = await window.graphqlClient.query(mutation, { name: this.hmiName });
            const res = result?.hmi?.delete;

            if (res && res.success) {
                window.ui.success(`HMI screen "${this.hmiName}" deleted successfully`);
                window.location.href = '/pages/hmi-screens.html';
            } else {
                throw new Error(res?.message || 'Failed to delete HMI screen');
            }
        } catch (e) {
            console.error('Error deleting HMI:', e);
            window.ui.showError('Failed to delete HMI screen: ' + e.message);
        } finally {
            window.ui.setLoading(false);
        }
    }

    async handleZipUpload(file) {
        if (!this.hmiName) return;

        try {
            window.ui.setLoading(true);
            const base64 = await this.fileToBase64(file);

            const isMain = document.getElementById('is-main-toggle')?.checked || false;
            const mutation = `
                mutation UploadZip($name: String!, $zipBase64: String!, $setAsMain: Boolean) {
                    hmi {
                        uploadZip(name: $name, zipBase64: $zipBase64, setAsMain: $setAsMain) {
                            success
                            message
                        }
                    }
                }
            `;
            const result = await window.graphqlClient.query(mutation, {
                name: this.hmiName,
                zipBase64: base64,
                setAsMain: isMain
            });
            const res = result?.hmi?.uploadZip;

            if (res && res.success) {
                window.ui.success(`Uploaded package "${file.name}" successfully`);
                await this.loadHmi();
            } else {
                throw new Error(res?.message || 'Failed to upload package');
            }
        } catch (e) {
            console.error('Error uploading ZIP:', e);
            window.ui.showError('Failed to upload package: ' + e.message);
        } finally {
            window.ui.setLoading(false);
        }
    }

    async exportZip() {
        if (!this.hmiName) return;

        try {
            window.ui.setLoading(true);
            const query = `
                query ExportHmiZip($name: String!) {
                    exportHmiZip(name: $name)
                }
            `;
            const result = await window.graphqlClient.query(query, { name: this.hmiName });
            const zipBase64 = result?.exportHmiZip;

            if (!zipBase64) {
                throw new Error('No package data returned by broker');
            }

            const binaryString = window.atob(zipBase64);
            const bytes = new Uint8Array(binaryString.length);
            for (let i = 0; i < binaryString.length; i++) {
                bytes[i] = binaryString.charCodeAt(i);
            }

            const blob = new Blob([bytes], { type: 'application/zip' });
            const link = document.createElement('a');
            link.href = URL.createObjectURL(blob);
            link.download = `${this.hmiName}.zip`;
            document.body.appendChild(link);
            link.click();
            document.body.removeChild(link);
            URL.revokeObjectURL(link.href);

            window.ui.success(`Exported "${this.hmiName}.zip" successfully`);
        } catch (e) {
            console.error('Error exporting HMI ZIP:', e);
            window.ui.showError('Failed to export HMI package: ' + e.message);
        } finally {
            window.ui.setLoading(false);
        }
    }

    fileToBase64(file) {
        return new Promise((resolve, reject) => {
            const reader = new FileReader();
            reader.readAsDataURL(file);
            reader.onload = () => {
                const result = reader.result;
                const base64 = result.substring(result.indexOf(',') + 1);
                resolve(base64);
            };
            reader.onerror = error => reject(error);
        });
    }

    formatBytes(bytes) {
        if (!bytes || bytes === 0) return '0 B';
        const k = 1024;
        const sizes = ['B', 'KB', 'MB', 'GB'];
        const i = Math.floor(Math.log(bytes) / Math.log(k));
        return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + ' ' + sizes[i];
    }

    escapeHtml(str) {
        if (!str) return '';
        return String(str)
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;')
            .replace(/'/g, '&#039;');
    }
}

document.addEventListener('DOMContentLoaded', () => {
    window.hmiDetailManager = new HmiScreenDetailManager();
});
