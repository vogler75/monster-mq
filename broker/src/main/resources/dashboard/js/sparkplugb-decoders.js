// SparkplugB Decoders List View

let autoRefreshInterval = null;

// GraphQL wrapper
async function graphqlQuery(query, variables = {}) {
    try {
        return await window.graphqlClient.query(query, variables);
    } catch (e) {
        console.error('GraphQL error:', e);
        throw e;
    }
}

async function loadDecoders() {
    try {
        const query = `
            query {
                sparkplugBDecoders {
                    name
                    namespace
                    nodeId
                    enabled
                    config {
                        sourceNamespace
                        rules {
                            name
                            nodeIdRegex
                            deviceIdRegex
                            destinationTopic
                        }
                    }
                    metrics {
                        messagesIn
                        messagesOut
                        messagesSkipped
                        timestamp
                    }
                    isOnCurrentNode
                }
            }
        `;

        const result = await graphqlQuery(query);
        const decoders = result.sparkplugBDecoders || [];

        renderDecoders(decoders);
    } catch (error) {
        console.error('Error loading SparkplugB decoders:', error);
        showError('Failed to load decoders: ' + error.message);
    }
}

function renderDecoders(decoders) {
    const tbody = document.getElementById('decodersTableBody');

    if (decoders.length === 0) {
        tbody.innerHTML = `
            <tr>
                <td colspan="9" style="text-align: center; padding: 2rem; color: var(--text-secondary);">
                    No SparkplugB decoders configured. Click "Create Decoder" to add one.
                </td>
            </tr>
        `;
        return;
    }

    tbody.innerHTML = decoders.map(decoder => {
        const metrics = decoder.metrics || {};
        const config = decoder.config || {};
        const rules = config.rules || [];

        return `
            <tr>
                <td>
                    <strong>${escapeHtml(decoder.name)}</strong>
                    ${decoder.isOnCurrentNode ? '<span style="color: var(--monster-cyan); font-size: 0.75rem;">● LOCAL</span>' : ''}
                </td>
                <td>${escapeHtml(decoder.namespace)}</td>
                <td><code style="font-size: 0.875rem;">${escapeHtml(config.sourceNamespace || 'spBv1.0')}</code></td>
                <td class="rule-count">${rules.length} rule${rules.length !== 1 ? 's' : ''}</td>
                <td>
                    <span class="status-badge ${decoder.enabled ? 'enabled' : 'disabled'}">
                        ${decoder.enabled ? '● Enabled' : '○ Disabled'}
                    </span>
                </td>
                <td class="metric-value">${formatNumber(metrics.messagesIn || 0)}/s</td>
                <td class="metric-value">${formatNumber(metrics.messagesOut || 0)}/s</td>
                <td class="metric-value">${formatNumber(metrics.messagesSkipped || 0)}/s</td>
                <td>
                    <div class="action-buttons">
                        <button class="btn btn-icon btn-view" onclick="viewDecoder('${escapeHtml(decoder.name)}')" title="View Details">
                            <svg width="18" height="18" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M15 12a3 3 0 11-6 0 3 3 0 016 0z"></path>
                                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M2.458 12C3.732 7.943 7.523 5 12 5c4.478 0 8.268 2.943 9.542 7-1.274 4.057-5.064 7-9.542 7-4.477 0-8.268-2.943-9.542-7z"></path>
                            </svg>
                        </button>
                        ${decoder.enabled ?
                            `<button class="btn btn-icon btn-pause" onclick="toggleDecoder('${escapeHtml(decoder.name)}', false)" title="Disable">
                                <svg width="18" height="18" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M10 9v6m4-6v6m7-3a9 9 0 11-18 0 9 9 0 0118 0z"></path>
                                </svg>
                            </button>` :
                            `<button class="btn btn-icon btn-play" onclick="toggleDecoder('${escapeHtml(decoder.name)}', true)" title="Enable">
                                <svg width="18" height="18" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M14.752 11.168l-3.197-2.132A1 1 0 0010 9.87v4.263a1 1 0 001.555.832l3.197-2.132a1 1 0 000-1.664z"></path>
                                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M21 12a9 9 0 11-18 0 9 9 0 0118 0z"></path>
                                </svg>
                            </button>`
                        }
                        <button class="btn btn-icon btn-delete" onclick="deleteDecoder('${escapeHtml(decoder.name)}')" title="Delete">
                            <svg width="18" height="18" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16"></path>
                            </svg>
                        </button>
                    </div>
                </td>
            </tr>
        `;
    }).join('');
}

function createDecoder() {
    window.location.href = 'sparkplugb-decoder-detail.html';
}

function viewDecoder(name) {
    window.location.href = `sparkplugb-decoder-detail.html?name=${encodeURIComponent(name)}`;
}

async function toggleDecoder(name, enabled) {
    try {
        const mutation = `
            mutation {
                sparkplugBDecoder {
                    toggle(name: "${escapeGraphQL(name)}", enabled: ${enabled}) {
                        success
                        errors
                    }
                }
            }
        `;

        const result = await graphqlQuery(mutation);
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

async function deleteDecoder(name) {
    if (!confirm(`Are you sure you want to delete the decoder "${name}"?\n\nThis action cannot be undone.`)) {
        return;
    }

    try {
        const mutation = `
            mutation {
                sparkplugBDecoder {
                    delete(name: "${escapeGraphQL(name)}") {
                        success
                        errors
                    }
                }
            }
        `;

        const result = await graphqlQuery(mutation);
        const response = result.sparkplugBDecoder?.delete;

        if (response?.success) {
            await loadDecoders();
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

function escapeGraphQL(text) {
    return text.replace(/\\/g, '\\\\').replace(/"/g, '\\"').replace(/\n/g, '\\n');
}

function showError(message) {
    alert(message);
}

// Initialize
document.addEventListener('DOMContentLoaded', () => {
    loadDecoders();

    // Auto-refresh every 5 seconds
    autoRefreshInterval = setInterval(loadDecoders, 5000);
});

// Cleanup on page unload
window.addEventListener('beforeunload', () => {
    if (autoRefreshInterval) {
        clearInterval(autoRefreshInterval);
    }
});
