// SparkplugB Decoders List View

let autoRefreshInterval = null;
let pendingDeleteName = null;

async function loadDecoders() {
    showLoading(true);
    hideError();
    try {
        const query = `
            query {
                sparkplugBDecoders {
                    name namespace nodeId enabled isOnCurrentNode
                    config {
                        sourceNamespace
                        rules { name nodeIdRegex deviceIdRegex destinationTopic }
                    }
                    metrics { messagesIn messagesOut messagesSkipped }
                }
            }
        `;
        const result = await window.graphqlClient.query(query);
        const decoders = result.sparkplugBDecoders || [];
        updateMetrics(decoders);
        renderDecoders(decoders);
    } catch (error) {
        console.error('Error loading SparkplugB decoders:', error);
        showError('Failed to load decoders: ' + error.message);
    } finally {
        showLoading(false);
    }
}

function updateMetrics(decoders) {
    document.getElementById('total-decoders').textContent = decoders.length;
    document.getElementById('enabled-decoders').textContent = decoders.filter(d => d.enabled).length;
    document.getElementById('local-decoders').textContent = decoders.filter(d => d.isOnCurrentNode).length;
    const totalRules = decoders.reduce((sum, d) => sum + ((d.config && d.config.rules) ? d.config.rules.length : 0), 0);
    document.getElementById('total-rules').textContent = totalRules;
}

function renderDecoders(decoders) {
    const tbody = document.getElementById('decodersTableBody');
    if (decoders.length === 0) {
        tbody.innerHTML = `<tr><td colspan="10" class="no-data">No SparkplugB decoders configured. Click "Create Decoder" to get started.</td></tr>`;
        return;
    }
    tbody.innerHTML = decoders.map(decoder => {
        const metrics = decoder.metrics || {};
        const config = decoder.config || {};
        const rules = config.rules || [];
        const statusClass = decoder.enabled ? 'status-enabled' : 'status-disabled';
        const statusText = decoder.enabled ? 'Enabled' : 'Disabled';
        const nodeIndicator = decoder.isOnCurrentNode ? '📍 ' : '';
        return `
            <tr>
                <td><div class="client-name">${escapeHtml(decoder.name)}</div></td>
                <td><small>${escapeHtml(decoder.namespace)}</small></td>
                <td><code style="font-size:0.8rem;">${escapeHtml(config.sourceNamespace || 'spBv1.0')}</code></td>
                <td>${nodeIndicator}${escapeHtml(decoder.nodeId || '')}</td>
                <td class="rule-count">${rules.length} rule${rules.length !== 1 ? 's' : ''}</td>
                <td><span class="status-badge ${statusClass}">${statusText}</span></td>
                <td class="metric-value-table">${formatNumber(metrics.messagesIn || 0)}</td>
                <td class="metric-value-table">${formatNumber(metrics.messagesOut || 0)}</td>
                <td class="metric-value-table">${formatNumber(metrics.messagesSkipped || 0)}</td>
                <td>
                    <div class="action-buttons">
                        <ix-icon-button icon="highlight" variant="primary" ghost size="24" title="Edit" onclick="viewDecoder('${escapeHtml(decoder.name)}')"></ix-icon-button>
                        ${decoder.enabled
                            ? `<ix-icon-button icon="pause" variant="primary" ghost size="24" title="Disable Decoder" onclick="toggleDecoder('${escapeHtml(decoder.name)}', false)"></ix-icon-button>`
                            : `<ix-icon-button icon="play" variant="primary" ghost size="24" title="Enable Decoder" onclick="toggleDecoder('${escapeHtml(decoder.name)}', true)"></ix-icon-button>`
                        }
                        <ix-icon-button icon="trashcan" variant="primary" ghost size="24" class="btn-delete" title="Delete" onclick="deleteDecoder('${escapeHtml(decoder.name)}')"></ix-icon-button>
                    </div>
                </td>
            </tr>
        `;
    }).join('');
}

function viewDecoder(name) {
    window.spaLocation.href = `sparkplugb-decoder-detail.html?name=${encodeURIComponent(name)}`;
}

async function toggleDecoder(name, enabled) {
    try {
        const mutation = `
            mutation ToggleDecoder($name: String!, $enabled: Boolean!) {
                sparkplugBDecoder { toggle(name: $name, enabled: $enabled) { success errors } }
            }
        `;
        const result = await window.graphqlClient.query(mutation, { name, enabled });
        const response = result.sparkplugBDecoder?.toggle;
        if (response?.success) {
            await loadDecoders();
        } else {
            showError('Failed to toggle decoder: ' + (response?.errors?.join(', ') || 'Unknown error'));
        }
    } catch (error) {
        console.error('Error toggling decoder:', error);
        showError('Failed to toggle decoder: ' + error.message);
    }
}

function deleteDecoder(name) {
    pendingDeleteName = name;
    document.getElementById('delete-decoder-name').textContent = name;
    document.getElementById('confirm-delete-modal').style.display = 'flex';
}

function hideConfirmDeleteModal() {
    document.getElementById('confirm-delete-modal').style.display = 'none';
    pendingDeleteName = null;
}

async function confirmDeleteDecoder() {
    if (!pendingDeleteName) return;
    const name = pendingDeleteName;
    hideConfirmDeleteModal();
    try {
        const mutation = `
            mutation DeleteDecoder($name: String!) {
                sparkplugBDecoder { delete(name: $name) { success errors } }
            }
        `;
        const result = await window.graphqlClient.query(mutation, { name });
        const response = result.sparkplugBDecoder?.delete;
        if (response?.success) {
            await loadDecoders();
            showSuccess(`Decoder "${name}" deleted successfully`);
        } else {
            showError('Failed to delete decoder: ' + (response?.errors?.join(', ') || 'Unknown error'));
        }
    } catch (error) {
        console.error('Error deleting decoder:', error);
        showError('Failed to delete decoder: ' + error.message);
    }
}

function formatNumber(value) {
    if (value === 0) return '0';
    if (value < 0.01) return value.toFixed(4);
    if (value < 1) return value.toFixed(2);
    if (value < 100) return value.toFixed(1);
    return Math.round(value).toString();
}

function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

function showLoading(show) { const el = document.getElementById('loading-indicator'); if (el) el.style.display = show ? 'flex' : 'none'; }
function showError(message) { const e = document.getElementById('error-message'); const t = document.querySelector('#error-message .error-text'); if (e && t) { t.textContent = message; e.style.display = 'flex'; setTimeout(() => hideError(), 5000); } }
function hideError() { const e = document.getElementById('error-message'); if (e) e.style.display = 'none'; }
function showSuccess(message) { var existing = document.getElementById('success-toast'); if (existing) existing.remove(); var toast = document.createElement('div'); toast.id = 'success-toast'; toast.style.cssText = 'position:fixed;top:20px;left:50%;transform:translateX(-50%);background:var(--monster-green,#10B981);color:#fff;padding:14px 24px;border-radius:8px;box-shadow:0 4px 12px rgba(0,0,0,0.4);z-index:10000;font-size:0.9rem;max-width:600px;display:flex;align-items:center;gap:10px;animation:slideDown 0.3s ease-out;'; toast.innerHTML = '<span style="font-size:1.2rem;">&#10003;</span><span>' + escapeHtml(message) + '</span><button onclick="this.parentElement.remove()" style="background:none;border:none;color:#fff;cursor:pointer;margin-left:auto;font-size:1.1rem;line-height:1;padding:0 4px;">&times;</button>'; if (!document.getElementById('toast-anim-style')) { var s = document.createElement('style'); s.id = 'toast-anim-style'; s.textContent = '@keyframes slideDown{from{transform:translateX(-50%) translateY(-100%);opacity:0;}to{transform:translateX(-50%) translateY(0);opacity:1;}}@keyframes fadeOut{from{opacity:1;}to{opacity:0;}}'; document.head.appendChild(s); } document.body.appendChild(toast); setTimeout(function() { if (toast.parentElement) { toast.style.animation = 'fadeOut 0.3s ease-out forwards'; setTimeout(function() { if (toast.parentElement) toast.remove(); }, 300); } }, 3000); }

// Initialize
document.addEventListener('DOMContentLoaded', () => {
    loadDecoders();
    autoRefreshInterval = setInterval(loadDecoders, 30000);
});

document.addEventListener('click', e => {
    if (e.target.classList.contains('modal') && e.target.id === 'confirm-delete-modal') hideConfirmDeleteModal();
});

window.addEventListener('beforeunload', () => {
    if (autoRefreshInterval) clearInterval(autoRefreshInterval);
});
