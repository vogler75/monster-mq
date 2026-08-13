// HMI Screens List Page Manager

class HmiScreensManager {
    constructor() {
        this.hmis = [];
        this.init();
    }

    async init() {
        if (!window.isLoggedIn()) {
            window.location.href = '/pages/login.html';
            return;
        }

        this.attachEventListeners();
        await this.loadHmis();
    }

    attachEventListeners() {
        const refreshBtn = document.getElementById('refresh-btn');
        if (refreshBtn) refreshBtn.addEventListener('click', () => this.loadHmis());

        const uploadBtn = document.getElementById('upload-btn');
        const fileInput = document.getElementById('zip-file-input');

        if (uploadBtn && fileInput) {
            uploadBtn.addEventListener('click', () => fileInput.click());
            fileInput.addEventListener('change', (e) => {
                const file = e.target.files?.[0];
                if (file) {
                    this.handleZipUpload(file);
                    fileInput.value = '';
                }
            });
        }
    }

    async loadHmis() {
        window.ui.setLoading(true);
        window.ui.clearError();
        try {
            const query = `
                query GetHmis {
                    hmis {
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
                }
            `;
            const result = await window.graphqlClient.query(query);
            this.hmis = result?.hmis || [];
            this.renderMetrics();
            this.renderTable();
        } catch (e) {
            console.error('Failed to load HMIs:', e);
            window.ui.showError('Failed to load HMI screens: ' + e.message);
        } finally {
            window.ui.setLoading(false);
        }
    }

    renderMetrics() {
        const total = this.hmis.length;
        const enabledCount = this.hmis.filter(h => h.enabled).length;
        const mainHmi = this.hmis.find(h => h.config?.isMain);
        const localCount = this.hmis.filter(h => h.isOnCurrentNode).length;

        document.getElementById('total-hmis').textContent = total;
        document.getElementById('enabled-hmis').textContent = enabledCount;
        document.getElementById('main-hmi').textContent = mainHmi ? mainHmi.name : '-';
        document.getElementById('local-hmis').textContent = localCount;
    }

    renderTable() {
        const tbody = document.getElementById('hmi-table-body');
        if (!tbody) return;

        if (this.hmis.length === 0) {
            tbody.innerHTML = `
                <tr>
                    <td colspan="8" style="text-align: center; color: var(--text-muted); padding: 2rem;">
                        No HMI screens configured yet. Click "Add Screen" or "Upload ZIP" to get started.
                    </td>
                </tr>
            `;
            return;
        }

        tbody.innerHTML = this.hmis.map(hmi => {
            const isMain = hmi.config?.isMain;
            const urlPath = hmi.config?.urlPath || hmi.name;
            const title = hmi.config?.title || hmi.name;
            const viewUrl = isMain ? '/hmi/' : `/hmi/${encodeURIComponent(urlPath)}/`;
            const fileCount = hmi.fileCount ?? 0;
            const sizeBytes = hmi.sizeBytes ?? 0;
            const formattedSize = this.formatBytes(sizeBytes);
            const updated = hmi.updatedAt ? new Date(hmi.updatedAt).toLocaleString() : '-';

            return `
                <tr>
                    <td>
                        <a href="/pages/hmi-screen-detail.html?name=${encodeURIComponent(hmi.name)}" style="font-weight: 600; color: var(--text-primary);">
                            ${this.escapeHtml(hmi.name)}
                        </a>
                    </td>
                    <td>
                        <div style="font-weight: 600;">${this.escapeHtml(title)}</div>
                        <code style="font-size: 0.75rem; color: var(--text-muted);">${this.escapeHtml(viewUrl)}</code>
                    </td>
                    <td><code style="font-size: 0.8rem; background: var(--dark-bg); padding: 2px 6px; border-radius: 4px;">${this.escapeHtml(hmi.nodeId || 'local')}</code></td>
                    <td>
                        ${isMain ? '<span class="status-badge badge-primary">Main Screen</span>' : '<span class="status-badge" style="background: rgba(148, 163, 184, 0.12); color: var(--text-muted);">Standard</span>'}
                    </td>
                    <td>
                        <span class="status-badge ${hmi.enabled ? 'badge-success' : 'badge-danger'}">
                            ${hmi.enabled ? 'Active' : 'Disabled'}
                        </span>
                    </td>
                    <td>
                        <span style="font-size: 0.85rem;">${fileCount} file${fileCount !== 1 ? 's' : ''}</span>
                        <span style="font-size: 0.75rem; color: var(--text-muted); display: block;">${formattedSize}</span>
                    </td>
                    <td style="font-size: 0.8rem; color: var(--text-muted);">${updated}</td>
                    <td>
                        <div style="display: flex; gap: 0.35rem; align-items: center;">
                            <a href="${viewUrl}" target="_blank" class="btn btn-secondary btn-sm" title="View HMI in new tab">
                                <ix-icon name="open-external" size="14"></ix-icon> Launch
                            </a>
                            <ix-icon-button icon="${hmi.enabled ? 'pause' : 'play'}" variant="subtle-tertiary" size="24"
                                onclick="window.hmiManager.toggleHmi('${this.escapeHtml(hmi.name)}', ${hmi.enabled})"
                                title="${hmi.enabled ? 'Disable' : 'Enable'} HMI"></ix-icon-button>
                            <ix-icon-button icon="download" variant="subtle-tertiary" size="24"
                                onclick="window.hmiManager.exportZip('${this.escapeHtml(hmi.name)}')"
                                title="Export ZIP package"></ix-icon-button>
                            <a href="/pages/hmi-screen-detail.html?name=${encodeURIComponent(hmi.name)}" class="btn btn-secondary btn-sm" title="Edit HMI configuration">
                                <ix-icon name="pen" size="14"></ix-icon> Edit
                            </a>
                            <ix-icon-button icon="trashcan" variant="subtle-tertiary" size="24" class="btn-delete"
                                onclick="window.hmiManager.deleteHmi('${this.escapeHtml(hmi.name)}')"
                                title="Delete HMI Screen"></ix-icon-button>
                        </div>
                    </td>
                </tr>
            `;
        }).join('');
    }

    async toggleHmi(name, currentEnabled) {
        const action = currentEnabled ? 'disable' : 'enable';
        try {
            window.ui.setLoading(true);
            const mutation = `
                mutation ToggleHmi($name: String!, $enabled: Boolean!) {
                    hmi {
                        toggle(name: $name, enabled: $enabled) {
                            success
                            message
                        }
                    }
                }
            `;
            const result = await window.graphqlClient.query(mutation, { name, enabled: !currentEnabled });
            const res = result?.hmi?.toggle;
            if (res && res.success) {
                window.ui.success(`HMI screen "${name}" ${action}d successfully`);
                await this.loadHmis();
            } else {
                throw new Error(res?.message || `Failed to ${action} HMI screen`);
            }
        } catch (e) {
            console.error(`Error toggling HMI ${name}:`, e);
            window.ui.showError(`Failed to ${action} HMI screen: ` + e.message);
        } finally {
            window.ui.setLoading(false);
        }
    }

    async deleteHmi(name) {
        const confirmed = await window.ui.showConfirm({
            title: 'Delete HMI Screen',
            message: `Are you sure you want to delete HMI screen "${name}" and all its files? This action cannot be undone.`,
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
            const result = await window.graphqlClient.query(mutation, { name });
            const res = result?.hmi?.delete;
            if (res && res.success) {
                window.ui.success(`HMI screen "${name}" deleted successfully`);
                await this.loadHmis();
            } else {
                throw new Error(res?.message || 'Failed to delete HMI screen');
            }
        } catch (e) {
            console.error(`Error deleting HMI ${name}:`, e);
            window.ui.showError('Failed to delete HMI screen: ' + e.message);
        } finally {
            window.ui.setLoading(false);
        }
    }

    async handleZipUpload(file) {
        const nameInput = prompt('Enter a name for this HMI screen package:', file.name.replace(/\.zip$/i, ''));
        if (!nameInput || !nameInput.trim()) return;

        const name = nameInput.trim();

        try {
            window.ui.setLoading(true);
            const base64 = await this.fileToBase64(file);

            const mutation = `
                mutation UploadZip($name: String!, $zipBase64: String!) {
                    hmi {
                        uploadZip(name: $name, zipBase64: $zipBase64) {
                            success
                            message
                        }
                    }
                }
            `;
            const result = await window.graphqlClient.query(mutation, { name, zipBase64: base64 });
            const res = result?.hmi?.uploadZip;

            if (res && res.success) {
                window.ui.success(`HMI package "${name}" uploaded successfully`);
                await this.loadHmis();
            } else {
                throw new Error(res?.message || 'Failed to upload ZIP package');
            }
        } catch (e) {
            console.error('Error uploading HMI ZIP:', e);
            window.ui.showError('Failed to upload HMI ZIP: ' + e.message);
        } finally {
            window.ui.setLoading(false);
        }
    }

    async exportZip(name) {
        try {
            window.ui.setLoading(true);
            const query = `
                query ExportHmiZip($name: String!) {
                    exportHmiZip(name: $name)
                }
            `;
            const result = await window.graphqlClient.query(query, { name });
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
            link.download = `${name}.zip`;
            document.body.appendChild(link);
            link.click();
            document.body.removeChild(link);
            URL.revokeObjectURL(link.href);

            window.ui.success(`Exported "${name}.zip" successfully`);
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
    window.hmiManager = new HmiScreensManager();
});
