// Device Export/Import Functionality

let allDevices = [];
let selectedFile = null;
let importedConfigs = [];

// Show Export Modal
async function showExportModal() {
    const modal = document.getElementById('export-modal');
    const deviceList = document.getElementById('export-device-list');

    try {
        // Fetch all devices
        const query = `
            query {
                exportDeviceConfigs {
                    name
                    namespace
                    nodeId
                    enabled
                    createdAt
                }
            }
        `;

        const response = await graphqlClient.request(query);

        if (response.errors) {
            showErrorMessage('Failed to load devices: ' + response.errors[0].message);
            return;
        }

        allDevices = response.data.exportDeviceConfigs || [];

        // Populate device list
        if (allDevices.length === 0) {
            deviceList.innerHTML = '<div style="padding: 1rem; color: var(--text-muted); text-align: center;">No devices available for export</div>';
        } else {
            deviceList.innerHTML = allDevices.map((device, index) => `
                <div class="device-item">
                    <input type="checkbox" id="device-checkbox-${index}" data-device-name="${device.name}">
                    <label for="device-checkbox-${index}">
                        <div class="device-item-name">${device.name}</div>
                        <div class="device-item-info">${device.namespace} @ ${device.nodeId} (${device.enabled ? 'Enabled' : 'Disabled'})</div>
                    </label>
                </div>
            `).join('');
        }

        modal.style.display = 'flex';
    } catch (error) {
        console.error('Error loading devices:', error);
        showErrorMessage('Failed to load devices for export');
    }
}

// Hide Export Modal
function hideExportModal() {
    document.getElementById('export-modal').style.display = 'none';
}

// Select All Devices
function selectAllDevices() {
    document.querySelectorAll('#export-device-list input[type="checkbox"]').forEach(checkbox => {
        checkbox.checked = true;
    });
}

// Deselect All Devices
function deselectAllDevices() {
    document.querySelectorAll('#export-device-list input[type="checkbox"]').forEach(checkbox => {
        checkbox.checked = false;
    });
}

// Export Selected Devices
async function exportSelectedDevices() {
    const checkboxes = document.querySelectorAll('#export-device-list input[type="checkbox"]:checked');

    if (checkboxes.length === 0) {
        showErrorMessage('Please select at least one device to export');
        return;
    }

    const selectedNames = Array.from(checkboxes).map(cb => cb.dataset.deviceName);

    try {
        // Query to export specific devices
        const query = `
            query {
                exportDeviceConfigs(names: ${JSON.stringify(selectedNames)}) {
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

        const response = await graphqlClient.request(query);

        if (response.errors) {
            showErrorMessage('Export failed: ' + response.errors[0].message);
            return;
        }

        const configs = response.data.exportDeviceConfigs || [];

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

        // Show success message
        showSuccessMessage(`Successfully exported ${configs.length} device configuration(s)`);
        hideExportModal();
    } catch (error) {
        console.error('Export error:', error);
        showErrorMessage('Failed to export devices');
    }
}

// Show Import Modal
function showImportModal() {
    document.getElementById('import-modal').style.display = 'flex';
    resetImportModal();
}

// Hide Import Modal
function hideImportModal() {
    document.getElementById('import-modal').style.display = 'none';
}

// Reset Import Modal
function resetImportModal() {
    selectedFile = null;
    importedConfigs = [];
    document.getElementById('file-input').value = '';
    document.getElementById('import-file-info').style.display = 'none';
    document.getElementById('import-progress').style.display = 'none';
    document.getElementById('import-results').style.display = 'none';
    document.getElementById('import-button').disabled = true;
}

// Handle File Select
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
            document.getElementById('import-file-name').textContent = file.name;
            document.getElementById('import-file-count').textContent = importedConfigs.length;
            document.getElementById('import-file-info').style.display = 'block';
            document.getElementById('import-button').disabled = false;
        } catch (error) {
            showErrorMessage('Failed to parse JSON file: ' + error.message);
            selectedFile = null;
            document.getElementById('file-input').value = '';
        }
    };

    reader.readAsText(file);
}

// Handle Drag Over
function handleDragOver(event) {
    event.preventDefault();
    event.stopPropagation();
    event.currentTarget.classList.add('dragover');
}

// Handle Drag Leave
function handleDragLeave(event) {
    event.preventDefault();
    event.stopPropagation();
    event.currentTarget.classList.remove('dragover');
}

// Handle Drop
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

// Import Devices
async function importDevices() {
    if (!selectedFile || importedConfigs.length === 0) {
        showErrorMessage('No file selected or file is empty');
        return;
    }

    const button = document.getElementById('import-button');
    button.disabled = true;

    try {
        // Show progress
        document.getElementById('import-progress').style.display = 'block';
        document.getElementById('import-results').style.display = 'none';
        document.getElementById('import-results-content').innerHTML = '';

        // Call GraphQL mutation to import devices
        const query = `
            mutation {
                importDeviceConfigs(configs: ${JSON.stringify(importedConfigs)}) {
                    success
                    imported
                    failed
                    total
                    errors
                }
            }
        `;

        const response = await graphqlClient.request(query);

        if (response.errors) {
            showErrorMessage('Import failed: ' + response.errors[0].message);
            return;
        }

        const result = response.data.importDeviceConfigs;

        // Update progress bar
        const progressPercent = (result.imported / result.total) * 100;
        document.getElementById('import-progress-bar').style.width = progressPercent + '%';

        // Hide progress and show results
        document.getElementById('import-progress').style.display = 'none';
        document.getElementById('import-results').style.display = 'block';

        // Build results HTML
        let resultsHTML = '';
        resultsHTML += `
            <div class="import-result-item import-result-success">
                <div class="import-result-icon">✓</div>
                <div class="import-result-text">Successfully imported ${result.imported} of ${result.total} device(s)</div>
            </div>
        `;

        if (result.failed > 0) {
            resultsHTML += `
                <div class="import-result-item import-result-error">
                    <div class="import-result-icon">✕</div>
                    <div class="import-result-text">${result.failed} device(s) failed to import</div>
                </div>
            `;
        }

        if (result.errors && result.errors.length > 0) {
            result.errors.forEach(error => {
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
        if (result.success) {
            showSuccessMessage(`Successfully imported ${result.imported} device configuration(s)`);
            // Refresh devices after a short delay
            setTimeout(() => {
                refreshDevices();
            }, 1000);
        }
    } catch (error) {
        console.error('Import error:', error);
        showErrorMessage('Failed to import devices: ' + error.message);
    } finally {
        button.disabled = false;
    }
}

// Show Error Message
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

// Show Success Message
function showSuccessMessage(message) {
    // Create a temporary success message
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
