// Device Configuration Export/Import Page
// Handles all device types generically

let allDevices = [];
let selectedFile = null;
let importedConfigs = [];

// Initialize on page load
document.addEventListener('DOMContentLoaded', () => {
    loadExportDevices();
});

// Load all devices for export
async function loadExportDevices() {
    const deviceList = document.getElementById('export-device-list');
    deviceList.innerHTML = '<div class="empty-state"><div class="empty-state-icon">⏳</div><div>Loading devices...</div></div>';

    try {
        // Query to get all devices (lightweight - without config)
        const query = `
            query {
                getDevices {
                    name
                    namespace
                    nodeId
                    enabled
                    type
                }
            }
        `;

        const result = await window.graphqlClient.query(query);

        if (result.errors) {
            showErrorMessage('Failed to load devices: ' + result.errors[0].message);
            deviceList.innerHTML = '<div class="empty-state"><div class="empty-state-icon">⚠️</div><div>Failed to load devices</div></div>';
            return;
        }

        allDevices = result.getDevices || [];

        console.log('Devices loaded:', allDevices);

        if (allDevices.length === 0) {
            deviceList.innerHTML = '<div class="empty-state"><div class="empty-state-icon">📭</div><div>No devices available</div></div>';
            console.warn('No devices found in response');
        } else {
            console.log(`Rendering ${allDevices.length} devices`);
            renderDeviceList();
        }
    } catch (error) {
        console.error('Error loading devices:', error);
        console.error('Error details:', error.stack);
        showErrorMessage('Failed to load devices: ' + error.message);
        deviceList.innerHTML = '<div class="empty-state"><div class="empty-state-icon">⚠️</div><div>Failed to load devices: ' + error.message + '</div></div>';
    }
}

// Render device list with checkboxes
function renderDeviceList() {
    const deviceList = document.getElementById('export-device-list');

    deviceList.innerHTML = allDevices.map((device, index) => `
        <div class="device-item">
            <input type="checkbox" id="device-checkbox-${index}" data-device-name="${device.name}" onchange="updateSelectionCount()">
            <label for="device-checkbox-${index}">
                <div class="device-item-name">${device.name}</div>
                <div class="device-item-info">${device.namespace} @ ${device.nodeId} (${device.enabled ? 'Enabled' : 'Disabled'})</div>
            </label>
            <div class="device-type-badge">${device.type || 'UNKNOWN'}</div>
        </div>
    `).join('');

    updateSelectionCount();
}

// Update selection count display
function updateSelectionCount() {
    const checkboxes = document.querySelectorAll('#export-device-list input[type="checkbox"]:checked');
    const count = checkboxes.length;
    const selectionInfo = document.getElementById('export-selection-info');
    const countSpan = document.getElementById('export-selected-count');

    if (count > 0) {
        countSpan.textContent = count;
        selectionInfo.style.display = 'block';
    } else {
        selectionInfo.style.display = 'none';
    }
}

// Select all devices
function selectAllDevices() {
    document.querySelectorAll('#export-device-list input[type="checkbox"]').forEach(checkbox => {
        checkbox.checked = true;
    });
    updateSelectionCount();
}

// Deselect all devices
function deselectAllDevices() {
    document.querySelectorAll('#export-device-list input[type="checkbox"]').forEach(checkbox => {
        checkbox.checked = false;
    });
    updateSelectionCount();
}

// Export selected devices
async function exportSelectedDevices() {
    const checkboxes = document.querySelectorAll('#export-device-list input[type="checkbox"]:checked');

    if (checkboxes.length === 0) {
        showErrorMessage('Please select at least one device to export');
        return;
    }

    const selectedNames = Array.from(checkboxes).map(cb => cb.dataset.deviceName);

    try {
        // Query to export specific devices (with full config details)
        const query = `
            query {
                getDevices(names: ${JSON.stringify(selectedNames)}) {
                    name
                    namespace
                    nodeId
                    config
                    enabled
                    type
                    createdAt
                    updatedAt
                }
            }
        `;

        const result = await window.graphqlClient.query(query);

        if (result.errors) {
            showErrorMessage('Export failed: ' + result.errors[0].message);
            return;
        }

        const configs = result.getDevices || [];

        if (configs.length === 0) {
            showErrorMessage('No configurations to export');
            return;
        }

        // Create and download JSON file
        const dataStr = JSON.stringify(configs, null, 2);
        const dataBlob = new Blob([dataStr], { type: 'application/json' });
        const url = URL.createObjectURL(dataBlob);
        const link = document.createElement('a');
        link.href = url;
        link.download = `device-configs-${new Date().toISOString().split('T')[0]}.json`;
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
        URL.revokeObjectURL(url);

        showSuccessMessage(`Successfully exported ${configs.length} device configuration(s)`);
    } catch (error) {
        console.error('Export error:', error);
        showErrorMessage('Failed to export devices');
    }
}

// Handle file selection
function handleFileSelect(event) {
    const files = event.target.files;
    if (files.length === 0) return;

    const file = files[0];

    if (!file.name.endsWith('.json')) {
        showErrorMessage('Please select a JSON file');
        document.getElementById('file-input').value = '';
        return;
    }

    selectedFile = file;
    const reader = new FileReader();

    reader.onload = function(e) {
        try {
            importedConfigs = JSON.parse(e.target.result);

            if (!Array.isArray(importedConfigs)) {
                showErrorMessage('Invalid file format: expected JSON array');
                selectedFile = null;
                document.getElementById('file-input').value = '';
                return;
            }

            // Show file info
            document.getElementById('file-name').textContent = file.name;
            document.getElementById('file-device-count').textContent = importedConfigs.length;
            document.getElementById('file-info').classList.add('show');
            document.getElementById('import-button').disabled = false;

            showSuccessMessage(`File loaded with ${importedConfigs.length} device(s)`);
        } catch (error) {
            showErrorMessage('Failed to parse JSON file: ' + error.message);
            selectedFile = null;
            document.getElementById('file-input').value = '';
        }
    };

    reader.readAsText(file);
}

// Handle drag over
function handleDragOver(event) {
    event.preventDefault();
    event.stopPropagation();
    event.currentTarget.classList.add('dragover');
}

// Handle drag leave
function handleDragLeave(event) {
    event.preventDefault();
    event.stopPropagation();
    event.currentTarget.classList.remove('dragover');
}

// Handle drop
function handleDrop(event) {
    event.preventDefault();
    event.stopPropagation();
    event.currentTarget.classList.remove('dragover');

    const files = event.dataTransfer.files;
    if (files.length > 0) {
        document.getElementById('file-input').files = files;
        handleFileSelect({ target: { files: files } });
    }
}

// Import devices
async function importDevices() {
    if (!selectedFile || importedConfigs.length === 0) {
        showErrorMessage('No file selected or file is empty');
        return;
    }

    const button = document.getElementById('import-button');
    button.disabled = true;

    try {
        // Show progress
        document.getElementById('import-progress').classList.add('show');
        document.getElementById('import-results').classList.remove('show');
        document.getElementById('import-results-content').innerHTML = '';

        // Call GraphQL mutation to import devices
        const query = `
            mutation ImportDevices($configs: [DeviceInput!]!) {
                importDevices(configs: $configs) {
                    success
                    imported
                    failed
                    total
                    errors
                }
            }
        `;

        const variables = { configs: importedConfigs };
        const result = await window.graphqlClient.query(query, variables);

        if (result.errors) {
            showErrorMessage('Import failed: ' + result.errors[0].message);
            return;
        }

        const importResult = result.importDevices;

        // Update progress bar
        const progressPercent = (importResult.imported / importResult.total) * 100;
        document.getElementById('import-progress-bar').style.width = progressPercent + '%';

        // Hide progress and show results
        document.getElementById('import-progress').classList.remove('show');
        document.getElementById('import-results').classList.add('show');

        // Build results HTML
        let resultsHTML = '';
        resultsHTML += `
            <div class="import-result-item import-result-success">
                <div class="import-result-icon">✓</div>
                <div class="import-result-text">Successfully imported ${importResult.imported} of ${importResult.total} device(s)</div>
            </div>
        `;

        if (importResult.failed > 0) {
            resultsHTML += `
                <div class="import-result-item import-result-error">
                    <div class="import-result-icon">✕</div>
                    <div class="import-result-text">${importResult.failed} device(s) failed to import</div>
                </div>
            `;
        }

        if (importResult.errors && importResult.errors.length > 0) {
            importResult.errors.forEach(error => {
                resultsHTML += `
                    <div class="import-result-item import-result-error">
                        <div class="import-result-icon">!</div>
                        <div class="import-result-text">${error}</div>
                    </div>
                `;
            });
        }

        document.getElementById('import-results-content').innerHTML = resultsHTML;

        // Show success message
        if (importResult.success) {
            showSuccessMessage(`Successfully imported ${importResult.imported} device configuration(s)`);
        }
    } catch (error) {
        console.error('Import error:', error);
        showErrorMessage('Failed to import devices: ' + error.message);
    } finally {
        button.disabled = false;
    }
}

// Reset import form
function resetImportForm() {
    selectedFile = null;
    importedConfigs = [];
    document.getElementById('file-input').value = '';
    document.getElementById('file-info').classList.remove('show');
    document.getElementById('import-progress').classList.remove('show');
    document.getElementById('import-results').classList.remove('show');
    document.getElementById('import-button').disabled = true;
}

// Show error message
function showErrorMessage(message) {
    const errorDiv = document.getElementById('error-message');
    if (errorDiv) {
        const errorText = errorDiv.querySelector('.error-text');
        if (errorText) {
            errorText.textContent = message;
            errorDiv.style.display = 'flex';
            setTimeout(() => {
                errorDiv.style.display = 'none';
            }, 5000);
        }
    }
}

// Show success message
function showSuccessMessage(message) {
    const successDiv = document.createElement('div');
    successDiv.style.cssText = `
        position: fixed;
        top: 1rem;
        right: 1rem;
        background: rgba(16, 185, 129, 0.9);
        color: white;
        padding: 1rem 1.5rem;
        border-radius: 8px;
        z-index: 10000;
        box-shadow: 0 4px 12px rgba(0, 0, 0, 0.3);
        animation: slideIn 0.3s ease;
    `;
    successDiv.innerHTML = `
        <div style="display: flex; align-items: center; gap: 0.75rem;">
            <span style="font-size: 1.25rem;">✓</span>
            <span>${message}</span>
        </div>
    `;

    document.body.appendChild(successDiv);

    setTimeout(() => {
        successDiv.style.animation = 'slideOut 0.3s ease';
        setTimeout(() => {
            document.body.removeChild(successDiv);
        }, 300);
    }, 3000);
}

// Add animation styles
const style = document.createElement('style');
style.textContent = `
    @keyframes slideIn {
        from {
            transform: translateX(400px);
            opacity: 0;
        }
        to {
            transform: translateX(0);
            opacity: 1;
        }
    }

    @keyframes slideOut {
        from {
            transform: translateX(0);
            opacity: 1;
        }
        to {
            transform: translateX(400px);
            opacity: 0;
        }
    }
`;
document.head.appendChild(style);
