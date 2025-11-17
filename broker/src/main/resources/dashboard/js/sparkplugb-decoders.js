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
                        subscriptions {
                            groupId
                            nodeId
                            deviceIds
                        }
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
                        <button class="btn-icon btn-view" onclick="viewDecoder('${escapeHtml(decoder.name)}')" title="Edit" aria-label="Edit">
                            <svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor">
                                <path d="M3 17.25V21h3.75L17.81 9.94l-3.75-3.75L3 17.25zM20.71 7.04c.39-.39.39-1.02 0-1.41l-2.34-2.34c-.39-.39-1.02-.39-1.41 0l-1.83 1.83 3.75 3.75 1.83-1.83z"/>
                            </svg>
                        </button>
                        ${decoder.enabled ?
                            `<button class="btn-icon btn-pause" onclick="toggleDecoder('${escapeHtml(decoder.name)}', false)" title="Stop Decoder">
                                <svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor">
                                    <path d="M6 19h4V5H6v14zm8-14v14h4V5h-4z"/>
                                </svg>
                            </button>` :
                            `<button class="btn-icon btn-play" onclick="toggleDecoder('${escapeHtml(decoder.name)}', true)" title="Start Decoder">
                                <svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor">
                                    <path d="M8 5v14l11-7z"/>
                                </svg>
                            </button>`
                        }
                        <button class="btn-icon btn-delete" onclick="deleteDecoder('${escapeHtml(decoder.name)}')" title="Delete" aria-label="Delete">
                            <svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor">
                                <path d="M6 19c0 1.1.9 2 2 2h8c1.1 0 2-.9 2-2V7H6v12zM19 4h-3.5l-1-1h-5l-1 1H5v2h14V4z"/>
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
