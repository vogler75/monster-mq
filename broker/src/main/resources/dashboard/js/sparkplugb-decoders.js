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
                        <button class="btn-icon btn-view" onclick="viewDecoder('${escapeHtml(decoder.name)}')" title="Edit" aria-label="Edit">
                            <svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor"><path d="M3 17.25V21h3.75L17.81 9.94l-3.75-3.75L3 17.25zM20.71 7.04c.39-.39.39-1.02 0-1.41l-2.34-2.34c-.39-.39-1.02-.39-1.41 0l-1.83 1.83 3.75 3.75 1.83-1.83z"/></svg>
                        </button>
                        ${decoder.enabled
                            ? `<button class="btn-icon btn-pause" onclick="toggleDecoder('${escapeHtml(decoder.name)}', false)" title="Disable Decoder"><svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor"><path d="M6 19h4V5H6v14zm8-14v14h4V5h-4z"/></svg></button>`
                            : `<button class="btn-icon btn-play" onclick="toggleDecoder('${escapeHtml(decoder.name)}', true)" title="Enable Decoder"><svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor"><path d="M8 5v14l11-7z"/></svg></button>`
                        }
                        <button class="btn-icon btn-delete" onclick="deleteDecoder('${escapeHtml(decoder.name)}')" title="Delete" aria-label="Delete">
                            <svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor"><path d="M6 19c0 1.1.9 2 2 2h8c1.1 0 2-.9 2-2V7H6v12zM19 4h-3.5l-1-1h-5l-1 1H5v2h14V4z"/></svg>
                        </button>
                    </div>
                </td>
            </tr>
        `;
    }).join('');
}

function viewDecoder(name) {
    window.location.href = `sparkplugb-decoder-detail.html?name=${encodeURIComponent(name)}`;
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
function showSuccess(msg) { const n = document.createElement('div'); n.className = 'success-notification'; n.innerHTML = `<span class="success-icon">✅</span><span class="success-text">${escapeHtml(msg)}</span>`; document.body.appendChild(n); setTimeout(() => { if (n.parentNode) n.parentNode.removeChild(n); }, 3000); }

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
