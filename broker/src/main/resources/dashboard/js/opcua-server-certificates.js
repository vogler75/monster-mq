// OPC UA Server Certificate Management JavaScript

let serverName = '';
let trustedCertificates = [];
let rejectedCertificates = [];
let selectedTrusted = new Set();
let selectedRejected = new Set();

// Initialize page
document.addEventListener('DOMContentLoaded', function() {
    checkAuthAndLoadData();
    setupEventListeners();
});

function checkAuthAndLoadData() {
    const token = localStorage.getItem('monstermq_token');
    if (!token) {
        window.location.href = '/pages/login.html';
        return;
    }

    // If token is not 'null', check if it's expired
    if (token !== 'null') {
        try {
            const decoded = JSON.parse(atob(token.split('.')[1]));
            const now = Date.now() / 1000;
            if (decoded.exp <= now) {
                window.location.href = '/pages/login.html';
                return;
            }
        } catch {
            window.location.href = '/pages/login.html';
            return;
        }
    }

    // Set up logout functionality
    const logoutLink = document.getElementById('logout-link');
    if (logoutLink) {
        logoutLink.addEventListener('click', function(e) {
            e.preventDefault();
            localStorage.removeItem('monstermq_token');
            localStorage.removeItem('monstermq_username');
            localStorage.removeItem('monstermq_isAdmin');
            window.location.href = '/pages/login.html';
        });
    }

    // Check if user is admin and show Users menu
    const isAdmin = localStorage.getItem('monstermq_isAdmin') === 'true';
    const usersLink = document.getElementById('users-link');
    if (isAdmin && usersLink) {
        usersLink.style.display = 'inline';
    }

    // Get server name from URL parameters
    const urlParams = new URLSearchParams(window.location.search);
    serverName = urlParams.get('server');

    if (!serverName) {
        showError('Server name is required');
        return;
    }

    // Update page title and breadcrumb
    document.getElementById('server-name-breadcrumb').textContent = `${serverName} - Certificate Management`;
    document.getElementById('server-subtitle').textContent = `Manage trusted and rejected certificates for OPC UA server "${serverName}"`;
    document.title = `MonsterMQ - ${serverName} Certificates`;

    loadCertificates();
}

function setupEventListeners() {
    // Checkbox event listeners will be set up when certificates are loaded
}

function loadCertificates() {
    showLoading();
    clearMessages();

    const query = `
        query GetCertificates($serverName: String!) {
            opcUaServerCertificates(serverName: $serverName, trusted: false) {
                fingerprint
                subject
                issuer
                validFrom
                validTo
                trusted
                filePath
                firstSeen
            }
            trustedCerts: opcUaServerCertificates(serverName: $serverName, trusted: true) {
                fingerprint
                subject
                issuer
                validFrom
                validTo
                trusted
                filePath
                firstSeen
            }
        }
    `;

    window.graphqlClient.query(query, { serverName: serverName })
        .then(data => {
            rejectedCertificates = data.opcUaServerCertificates || [];
            trustedCertificates = data.trustedCerts || [];

            renderCertificates();
            updateCounters();
            hideLoading();
        })
        .catch(error => {
            hideLoading();
            showError('Failed to load certificates: ' + error.message);
        });
}

function renderCertificates() {
    renderRejectedCertificates();
    renderTrustedCertificates();
}

function renderRejectedCertificates() {
    const tbody = document.getElementById('rejected-certificates-tbody');
    const noDataDiv = document.getElementById('no-rejected-certificates');
    const tableDiv = document.getElementById('rejected-certificates-table');

    if (rejectedCertificates.length === 0) {
        tableDiv.style.display = 'none';
        noDataDiv.style.display = 'block';
    } else {
        tableDiv.style.display = 'block';
        noDataDiv.style.display = 'none';

        tbody.innerHTML = rejectedCertificates.map(cert => renderCertificateRow(cert, 'rejected')).join('');
    }
}

function renderTrustedCertificates() {
    const tbody = document.getElementById('trusted-certificates-tbody');
    const noDataDiv = document.getElementById('no-trusted-certificates');
    const tableDiv = document.getElementById('trusted-certificates-table');

    if (trustedCertificates.length === 0) {
        tableDiv.style.display = 'none';
        noDataDiv.style.display = 'block';
    } else {
        tableDiv.style.display = 'block';
        noDataDiv.style.display = 'none';

        tbody.innerHTML = trustedCertificates.map(cert => renderCertificateRow(cert, 'trusted')).join('');
    }
}

function renderCertificateRow(cert, type) {
    const validFrom = new Date(cert.validFrom).toLocaleDateString();
    const validTo = new Date(cert.validTo).toLocaleDateString();
    const firstSeen = new Date(cert.firstSeen).toLocaleString();

    // Extract common name from subject
    const subjectMatch = cert.subject.match(/CN=([^,]+)/);
    const commonName = subjectMatch ? subjectMatch[1] : 'Unknown';

    return `
        <tr class="cert-row" data-fingerprint="${escapeHtml(cert.fingerprint)}" data-type="${type}">
            <td>
                <input type="checkbox" class="cert-checkbox"
                       onchange="toggleCertificateSelection('${escapeHtml(cert.fingerprint)}', '${type}', this)">
            </td>
            <td>
                <div class="cert-subject">${escapeHtml(commonName)}</div>
                <div class="cert-details">Issuer: ${escapeHtml(cert.issuer)}</div>
                <div class="cert-details">Subject: ${escapeHtml(cert.subject)}</div>
            </td>
            <td>
                <div class="cert-fingerprint">${escapeHtml(cert.fingerprint)}</div>
            </td>
            <td>
                <div class="cert-validity">From: ${validFrom}</div>
                <div class="cert-validity">To: ${validTo}</div>
            </td>
            <td>
                <div class="cert-first-seen">${firstSeen}</div>
            </td>
        </tr>
    `;
}

function toggleCertificateSelection(fingerprint, type, checkbox) {
    const row = checkbox.closest('tr');

    if (type === 'rejected') {
        if (checkbox.checked) {
            selectedRejected.add(fingerprint);
            row.classList.add('selected');
        } else {
            selectedRejected.delete(fingerprint);
            row.classList.remove('selected');
        }
    } else {
        if (checkbox.checked) {
            selectedTrusted.add(fingerprint);
            row.classList.add('selected');
        } else {
            selectedTrusted.delete(fingerprint);
            row.classList.remove('selected');
        }
    }

    updateButtonStates();
    updateSelectionCounters();
}

function toggleSelectAllRejected() {
    const selectAllCheckbox = document.getElementById('select-all-rejected');
    const checkboxes = document.querySelectorAll('#rejected-certificates-tbody .cert-checkbox');

    checkboxes.forEach(checkbox => {
        const fingerprint = checkbox.closest('tr').dataset.fingerprint;
        const row = checkbox.closest('tr');

        checkbox.checked = selectAllCheckbox.checked;

        if (selectAllCheckbox.checked) {
            selectedRejected.add(fingerprint);
            row.classList.add('selected');
        } else {
            selectedRejected.delete(fingerprint);
            row.classList.remove('selected');
        }
    });

    updateButtonStates();
    updateSelectionCounters();
}

function toggleSelectAllTrusted() {
    const selectAllCheckbox = document.getElementById('select-all-trusted');
    const checkboxes = document.querySelectorAll('#trusted-certificates-tbody .cert-checkbox');

    checkboxes.forEach(checkbox => {
        const fingerprint = checkbox.closest('tr').dataset.fingerprint;
        const row = checkbox.closest('tr');

        checkbox.checked = selectAllCheckbox.checked;

        if (selectAllCheckbox.checked) {
            selectedTrusted.add(fingerprint);
            row.classList.add('selected');
        } else {
            selectedTrusted.delete(fingerprint);
            row.classList.remove('selected');
        }
    });

    updateButtonStates();
    updateSelectionCounters();
}

function updateButtonStates() {
    const trustBtn = document.getElementById('trust-selected-btn');
    const deleteRejectedBtn = document.getElementById('delete-rejected-btn');
    const deleteTrustedBtn = document.getElementById('delete-trusted-btn');

    trustBtn.disabled = selectedRejected.size === 0;
    deleteRejectedBtn.disabled = selectedRejected.size === 0;
    deleteTrustedBtn.disabled = selectedTrusted.size === 0;
}

function updateSelectionCounters() {
    document.getElementById('rejected-selected-count').textContent = selectedRejected.size;
    document.getElementById('trusted-selected-count').textContent = selectedTrusted.size;
}

function updateCounters() {
    document.getElementById('rejected-count').textContent = rejectedCertificates.length;
    document.getElementById('trusted-count').textContent = trustedCertificates.length;
    document.getElementById('rejected-total-count').textContent = rejectedCertificates.length;
    document.getElementById('trusted-total-count').textContent = trustedCertificates.length;
}

function trustSelectedCertificates() {
    if (selectedRejected.size === 0) return;

    const fingerprints = Array.from(selectedRejected);

    const mutation = `
        mutation TrustCertificates($serverName: String!, $fingerprints: [String!]!) {
            trustOpcUaServerCertificates(serverName: $serverName, fingerprints: $fingerprints) {
                success
                message
                affectedCertificates {
                    fingerprint
                    subject
                    trusted
                }
            }
        }
    `;

    clearMessages();
    showLoading();

    window.graphqlClient.query(mutation, {
        serverName: serverName,
        fingerprints: fingerprints
    })
        .then(data => {
            hideLoading();
            const result = data.trustOpcUaServerCertificates;

            if (result.success) {
                showSuccess(`Successfully trusted ${result.affectedCertificates.length} certificate(s)`);
                selectedRejected.clear();
                loadCertificates(); // Reload to reflect changes
            } else {
                showError('Failed to trust certificates: ' + result.message);
            }
        })
        .catch(error => {
            hideLoading();
            showError('Failed to trust certificates: ' + error.message);
        });
}

function deleteSelectedRejectedCertificates() {
    if (selectedRejected.size === 0) return;

    if (!confirm(`Are you sure you want to delete ${selectedRejected.size} rejected certificate(s)? This action cannot be undone.`)) {
        return;
    }

    deleteCertificates(Array.from(selectedRejected), 'rejected');
}

function deleteSelectedTrustedCertificates() {
    if (selectedTrusted.size === 0) return;

    if (!confirm(`Are you sure you want to delete ${selectedTrusted.size} trusted certificate(s)? This action cannot be undone.`)) {
        return;
    }

    deleteCertificates(Array.from(selectedTrusted), 'trusted');
}

function deleteCertificates(fingerprints, type) {
    const mutation = `
        mutation DeleteCertificates($serverName: String!, $fingerprints: [String!]!) {
            deleteOpcUaServerCertificates(serverName: $serverName, fingerprints: $fingerprints) {
                success
                message
                affectedCertificates {
                    fingerprint
                    subject
                    trusted
                }
            }
        }
    `;

    clearMessages();
    showLoading();

    window.graphqlClient.query(mutation, {
        serverName: serverName,
        fingerprints: fingerprints
    })
        .then(data => {
            hideLoading();
            const result = data.deleteOpcUaServerCertificates;

            if (result.success) {
                showSuccess(`Successfully deleted ${result.affectedCertificates.length} certificate(s)`);

                // Clear selections
                if (type === 'rejected') {
                    selectedRejected.clear();
                } else {
                    selectedTrusted.clear();
                }

                loadCertificates(); // Reload to reflect changes
            } else {
                showError('Failed to delete certificates: ' + result.message);
            }
        })
        .catch(error => {
            hideLoading();
            showError('Failed to delete certificates: ' + error.message);
        });
}

function refreshCertificates() {
    // Clear selections
    selectedRejected.clear();
    selectedTrusted.clear();

    // Uncheck all checkboxes
    document.querySelectorAll('.cert-checkbox').forEach(checkbox => {
        checkbox.checked = false;
    });
    document.getElementById('select-all-rejected').checked = false;
    document.getElementById('select-all-trusted').checked = false;

    // Remove selected styling
    document.querySelectorAll('.cert-row').forEach(row => {
        row.classList.remove('selected');
    });

    loadCertificates();
}

function showLoading() {
    document.getElementById('loading-indicator').style.display = 'flex';
}

function hideLoading() {
    document.getElementById('loading-indicator').style.display = 'none';
}

function showError(message) {
    const errorDiv = document.getElementById('error-message');
    errorDiv.textContent = message;
    errorDiv.style.display = 'block';

    // Auto-hide after 5 seconds
    setTimeout(() => {
        errorDiv.style.display = 'none';
    }, 5000);
}

function showSuccess(message) {
    const successDiv = document.getElementById('success-message');
    successDiv.textContent = message;
    successDiv.style.display = 'block';

    // Auto-hide after 3 seconds
    setTimeout(() => {
        successDiv.style.display = 'none';
    }, 3000);
}

function clearMessages() {
    document.getElementById('error-message').style.display = 'none';
    document.getElementById('success-message').style.display = 'none';
}

function escapeHtml(text) {
    const map = {
        '&': '&amp;',
        '<': '&lt;',
        '>': '&gt;',
        '"': '&quot;',
        "'": '&#039;'
    };
    return text.replace(/[&<>"']/g, function(m) { return map[m]; });
}