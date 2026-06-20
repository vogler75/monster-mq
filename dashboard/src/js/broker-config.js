// Broker list configuration page

var safeStorage = window.safeStorage;
var BROKER_CONFIG_TOKEN_KEY = 'monstermq_broker_config_token';

class BrokerConfigManager {
    constructor() {
        this.currentToken = '';
        this._abortController = new AbortController();
        this._apiSupportsWrite = null;
        this._authenticated = false;
        this.init();
    }

    init() {
        this.currentToken = safeStorage.getItem(BROKER_CONFIG_TOKEN_KEY) || '';
        this._prepareEvents();
        var isLocal = window.isElectron || (window.brokerManager && window.brokerManager.isLocalMode);
        if (isLocal) {
            this._authenticated = true;
            this._apiSupportsWrite = true;
            var tokenCard = document.getElementById('broker-admin-token') ? document.getElementById('broker-admin-token').closest('.section-card') : null;
            if (tokenCard) {
                tokenCard.style.display = 'none';
            }
            var subtitle = document.querySelector('.page-subtitle');
            if (subtitle) {
                subtitle.textContent = window.isElectron ? 
                    'Manage broker endpoints stored locally in this desktop app.' :
                    'Manage broker endpoints stored locally in this browser.';
            }
            this._setEditorLocked(false);
            this.loadBrokers().catch((e) => {
                console.error('Failed to load local brokers:', e);
            });
        } else {
            this._setEditorLocked(true);
        }
    }

    _prepareEvents() {
        var tokenInput = document.getElementById('broker-admin-token');
        if (tokenInput) {
            tokenInput.value = this.currentToken;
            tokenInput.addEventListener('input', () => {
                if (tokenInput.value.trim() !== this.currentToken) {
                    this._authenticated = false;
                    this._setEditorLocked(true);
                }
                this._refreshTokenState();
            });
        }

        var saveTokenBtn = document.getElementById('save-token-btn');
        if (saveTokenBtn) {
            saveTokenBtn.addEventListener('click', () => this.saveToken());
        }

        var addBtn = document.getElementById('add-broker-btn');
        if (addBtn) {
            addBtn.addEventListener('click', () => this.addBrokerRow());
        }

        var saveBtn = document.getElementById('save-brokers-btn');
        if (saveBtn) {
            saveBtn.addEventListener('click', () => this.saveBrokers());
        }

        var refreshBtn = document.getElementById('refresh-brokers-btn');
        if (refreshBtn) {
            refreshBtn.addEventListener('click', () => {
                this.loadBrokers({ requireAuth: true }).catch((error) => {
                    this._authenticated = false;
                    this._setEditorLocked(true);
                    this.showError(error.message || 'Could not refresh broker configuration.');
                    this._refreshTokenState();
                });
            });
        }

        const listContainer = document.getElementById('broker-list');
        if (listContainer) {
            listContainer.addEventListener('click', (event) => {
                var removeBtn = event.target.closest('.broker-remove');
                if (removeBtn) {
                    this.removeBrokerRow(removeBtn.closest('.broker-row'));
                }

                var setDefaultBtn = event.target.closest('.broker-set-default');
                if (setDefaultBtn) {
                    this.setDefaultBroker(setDefaultBtn.closest('.broker-row'));
                }
            });
        }

        this._refreshTokenState();

        if (window.registerPageCleanup) {
            window.registerPageCleanup(() => this._abortController.abort());
        }
    }

    _navigate(url) {
        if (window.spaLocation) {
            window.spaLocation.href = url;
        } else {
            window.location.href = url;
        }
    }

    showLoading(show) {
        var loading = document.getElementById('loading-indicator');
        if (loading) loading.style.display = show ? 'flex' : 'none';
    }

    showError(message) {
        var container = document.getElementById('error-message');
        if (!container) return;
        if (!message) {
            container.style.display = 'none';
            return;
        }

        var body = container.querySelector('.error-text');
        if (body) body.textContent = message;
        container.style.display = 'flex';

        setTimeout(function() {
            if (container) container.style.display = 'none';
        }, 8000);
    }

    clearStatus() {
        this.showStatus('');
    }

    showStatus(message, type) {
        var status = document.getElementById('save-status');
        if (!status) return;
        status.textContent = message;
        status.className = 'save-status ' + (type || '');
    }

    getAuthHeader() {
        return this.currentToken ? { 'Authorization': 'Bearer ' + this.currentToken } : {};
    }

    async saveToken() {
        var input = document.getElementById('broker-admin-token');
        if (!input) return;

        var nextToken = input.value.trim();
        if (!nextToken) {
            this.showError('Enter an admin token before saving.');
            return;
        }

        var previousToken = this.currentToken;
        this.currentToken = nextToken;
        this.showError('');
        this.showStatus('Verifying token...');

        try {
            await this.loadBrokers({ requireAuth: true });
            safeStorage.setItem(BROKER_CONFIG_TOKEN_KEY, this.currentToken);
            this._authenticated = true;
            this._setEditorLocked(false);
            this.showStatus('Token verified. Broker configuration unlocked.', 'success');
        } catch (error) {
            this.currentToken = previousToken;
            this._authenticated = false;
            this._setEditorLocked(true);
            this.showError(error.message || 'Could not verify admin token.');
        } finally {
            this._refreshTokenState();
        }
    }

    _refreshTokenState() {
        var isLocal = window.isElectron || (window.brokerManager && window.brokerManager.isLocalMode);
        if (isLocal) {
            var saveBtn = document.getElementById('save-brokers-btn');
            if (saveBtn) {
                saveBtn.removeAttribute('disabled');
                saveBtn.title = '';
            }
            return;
        }
        var tokenInput = document.getElementById('broker-admin-token');
        var saveTokenBtn = document.getElementById('save-token-btn');
        var tokenState = document.getElementById('token-state');
        var saveBtn = document.getElementById('save-brokers-btn');
        if (!tokenInput || !saveTokenBtn) return;

        var enteredToken = tokenInput.value.trim();
        var tokenSaved = !!this.currentToken;
        var tokenMatchesStored = enteredToken === this.currentToken;
        var hasTokenToSave = !!enteredToken && !tokenMatchesStored;
        var hasTokenToUnlock = !!enteredToken && !this._authenticated;

        saveTokenBtn.disabled = !hasTokenToSave && !hasTokenToUnlock;
        saveTokenBtn.textContent = this._authenticated ? 'Unlocked' : 'Unlock configuration';
        saveTokenBtn.title = enteredToken ? 'Verify this token and unlock configuration editing.' : 'Enter an admin token to unlock configuration editing.';

        if (tokenState) {
            if (this._authenticated) {
                tokenState.textContent = 'Unlocked';
                tokenState.className = 'token-state';
            } else if (tokenSaved) {
                tokenState.textContent = enteredToken ? 'Token ready' : 'No token entered';
                tokenState.className = enteredToken ? 'token-state empty' : 'token-state empty';
            } else {
                tokenState.textContent = enteredToken ? 'Token ready' : 'No token set';
                tokenState.className = enteredToken ? 'token-state' : 'token-state empty';
            }
        }

        if (!saveBtn) return;
        if (!this._authenticated) {
            saveBtn.setAttribute('disabled', 'disabled');
            saveBtn.title = 'Unlock configuration with an admin token before saving.';
            return;
        }

        if (this._apiSupportsWrite === false) {
            return;
        }

        if (this._apiSupportsWrite === true) {
            saveBtn.removeAttribute('disabled');
            saveBtn.title = '';
        }
    }

    _setEditorLocked(locked) {
        var editor = document.getElementById('broker-editor');
        var lockNotice = document.getElementById('broker-editor-lock-notice');
        if (editor) editor.style.display = locked ? 'none' : '';
        if (lockNotice) lockNotice.style.display = locked ? 'block' : 'none';

        var addBtn = document.getElementById('add-broker-btn');
        var refreshBtn = document.getElementById('refresh-brokers-btn');
        var saveBtn = document.getElementById('save-brokers-btn');
        [addBtn, refreshBtn, saveBtn].forEach(function(button) {
            if (!button) return;
            button.disabled = locked;
        });

        if (locked) {
            var list = document.getElementById('broker-list');
            if (list) list.innerHTML = '';
        }
    }

    async loadBrokers(options) {
        options = options || {};
        var requireAuth = options.requireAuth === true;
        var isLocal = window.isElectron || (window.brokerManager && window.brokerManager.isLocalMode);

        if (isLocal) {
            this.showLoading(true);
            this.showError('');
            this._setSaveCapability(true);
            try {
                var brokers = [];
                if (window.isElectron && window.ElectronAPI) {
                    var config = await window.ElectronAPI.readConfig();
                    brokers = config ? config.brokers : [];
                } else {
                    var stored = safeStorage.getItem('monstermq_desktop_brokers');
                    brokers = stored ? JSON.parse(stored) : [];
                }
                if (!brokers || !brokers.length) {
                    brokers = [{ name: 'Local', host: window.isElectron ? 'localhost' : '', port: window.isElectron ? 4000 : 0, tls: false, default: true, endpoint: '/graphql' }];
                }
                this.renderBrokers(brokers);
                if (window.brokerManager && typeof window.brokerManager.setBrokers === 'function') {
                    window.brokerManager.setBrokers(brokers);
                }
                this.showStatus('Loaded ' + brokers.length + ' broker(s) from local configuration.');
            } catch (e) {
                this.showError('Failed to load local broker config: ' + e.message);
            } finally {
                this.showLoading(false);
            }
            return;
        }

        if (requireAuth && !this.currentToken) {
            throw new Error('Enter an admin token before unlocking configuration.');
        }

        this.showLoading(true);
        this.showError('');
        if (!requireAuth) this.clearStatus();
        this._setSaveCapability(null);
        this._refreshTokenState();

        try {
            try {
                var response = await fetch('/api/brokers', {
                    cache: 'no-store',
                    headers: requireAuth ? this.getAuthHeader() : {},
                    signal: this._abortController.signal
                });

                if (!response.ok) {
                    var responseText = await response.text();
                    var responsePayload = responseText ? JSON.parse(responseText) : {};
                    if (response.status === 404) {
                        throw new Error('BROKER_API_NOT_AVAILABLE');
                    }
                    throw new Error((responsePayload && responsePayload.error) || ('Failed to load brokers: HTTP ' + response.status));
                }

                var payload = await response.json();
                var brokers = Array.isArray(payload?.brokers) ? payload.brokers : [];
                if (!brokers.length) {
                    throw new Error('No brokers found');
                }

                this.renderBrokers(brokers);
                if (window.brokerManager && typeof window.brokerManager.setBrokers === 'function') {
                    window.brokerManager.setBrokers(brokers);
                }
                this._setSaveCapability(true);
                this.showStatus('Loaded ' + brokers.length + ' broker(s).');
                return;
            } catch (apiError) {
                if (apiError && apiError.message === 'BROKER_API_NOT_AVAILABLE') {
                    throw apiError;
                }

                if (apiError && apiError.name === 'SyntaxError') {
                    throw new Error('BROKER_API_NOT_AVAILABLE');
                }

                throw apiError;
            }
        } catch (error) {
            if (error && error.name === 'AbortError') {
                return;
            }
            if (requireAuth) {
                throw error;
            }
            if (error && error.message === 'BROKER_API_NOT_AVAILABLE') {
                return this._loadBrokersFromStaticFallback();
            }

            console.error('Error loading broker config:', error);
            this.showError(error.message || 'Could not load broker config');
        } finally {
            this.showLoading(false);
        }
    }

    async _loadBrokersFromStaticFallback() {
        var fallbackPaths = [
            '/config/config.json',
            '/config/brokers.json'
        ];

        for (var i = 0; i < fallbackPaths.length; i++) {
            try {
                var path = fallbackPaths[i];
                var fallbackResponse = await fetch(path, {
                    cache: 'no-store',
                    signal: this._abortController.signal
                });
                if (!fallbackResponse.ok) continue;

                var text = await fallbackResponse.text();
                var payload = text ? JSON.parse(text) : null;
                var fallbackBrokers = Array.isArray(payload?.brokers) ? payload.brokers : (Array.isArray(payload) ? payload : []);
                if (!Array.isArray(fallbackBrokers) || !fallbackBrokers.length) continue;

                this._setSaveCapability(false);
                this.renderBrokers(fallbackBrokers);
                if (window.brokerManager && typeof window.brokerManager.setBrokers === 'function') {
                    window.brokerManager.setBrokers(fallbackBrokers);
                }
                this.showStatus('Loaded brokers from static config. Save is disabled in this deployment.', 'warning');
                return;
            } catch (error) {
                if (error && error.name === 'AbortError') {
                    return;
                }
            }
        }

        throw new Error('Broker API is not available, and static config fallback could not be loaded.');
    }

    _setSaveCapability(canWrite) {
        var saveBtn = document.getElementById('save-brokers-btn');
        if (!saveBtn) return;

        if (canWrite === null) {
            this._apiSupportsWrite = null;
            saveBtn.removeAttribute('disabled');
            saveBtn.textContent = 'Save Changes';
            saveBtn.title = '';
            this._refreshTokenState();
            return;
        }

        this._apiSupportsWrite = canWrite;
        if (canWrite) {
            saveBtn.removeAttribute('disabled');
            saveBtn.textContent = 'Save Changes';
            saveBtn.title = '';
            this._refreshTokenState();
            return;
        }

        saveBtn.setAttribute('disabled', 'disabled');
        saveBtn.textContent = 'Save Changes (Unavailable)';
        saveBtn.title = 'Save is not available in this deployment. This dashboard bundle is served without a writable broker endpoint.';
    }

    renderBrokers(brokers) {
        var list = document.getElementById('broker-list');
        if (!list) return;

        var rows = brokers.map((broker) => this._rowTemplate(broker)).join('');
        list.innerHTML = rows;
        this._syncDefaultState();
    }

    _rowTemplate(broker) {
        var name = this.escapeHtml(broker.name || '');
        var host = this.escapeHtml(broker.host || '');
        var port = broker.port || 4000;
        var tls = broker.tls ? 'checked' : '';
        var endpoint = this.escapeHtml(broker.endpoint || '/graphql');
        var defaultFlag = broker.default ? 'checked' : '';

        return `
            <div class="broker-row">
                <div class="broker-grid">
                    <label class="broker-field">Name
                        <input class="form-control broker-name" type="text" value="${name}" placeholder="e.g. Local">
                    </label>
                    <label class="broker-field">Host
                        <input class="form-control broker-host" type="text" value="${host}" placeholder="hostname or IP">
                    </label>
                    <label class="broker-field">Port
                        <input class="form-control broker-port" type="number" min="0" step="1" value="${port}" placeholder="1883">
                    </label>
                    <label class="broker-field">TLS
                        <input class="broker-tls" type="checkbox" ${tls}>
                    </label>
                    <label class="broker-field broker-endpoint-wrap">Endpoint
                        <input class="form-control broker-endpoint" type="text" value="${endpoint}" placeholder="/graphql">
                    </label>
                    <label class="broker-field broker-default-wrap">Default
                        <input type="checkbox" class="broker-default" ${defaultFlag} disabled>
                    </label>
                </div>

                <div class="broker-actions">
                    <button type="button" class="btn btn-secondary broker-set-default" title="Set as default">Set as default</button>
                    <button type="button" class="btn btn-secondary broker-remove" title="Remove">Remove</button>
                </div>
            </div>`;
    }

    addBrokerRow() {
        var list = document.getElementById('broker-list');
        if (!list) return;
        var temp = document.createElement('div');
        temp.innerHTML = this._rowTemplate({
            name: '',
            host: '',
            port: 4000,
            tls: false,
            endpoint: '/graphql',
            default: false
        });
        list.appendChild(temp.firstElementChild);
        this._syncDefaultState();
    }

    removeBrokerRow(row) {
        if (!row) return;
        row.remove();
        this._syncDefaultState();
    }

    setDefaultBroker(row) {
        if (!row) return;
        var defaultInputs = this._getRows().map(function(r) { return r.querySelector('.broker-default'); });
        defaultInputs.forEach(function(input) {
            if (!input) return;
            input.checked = false;
        });
        var target = row.querySelector('.broker-default');
        if (target) target.checked = true;
    }

    _syncDefaultState() {
        var rows = this._getRows();
        if (rows.length === 0) return;

        var hasDefault = rows.some(function(row) {
            var defaultInput = row.querySelector('.broker-default');
            return defaultInput && defaultInput.checked;
        });

        if (!hasDefault) {
            var firstDefaultInput = rows[0].querySelector('.broker-default');
            if (firstDefaultInput) firstDefaultInput.checked = true;
        }

        var removeButtons = rows.map(function(row) { return row.querySelector('.broker-remove'); });
        var onlyOne = rows.length === 1;
        removeButtons.forEach(function(button) {
            if (button) button.disabled = onlyOne;
        });

        var setDefaultButtons = rows.map(function(row) { return row.querySelector('.broker-set-default'); });
        setDefaultButtons.forEach(function(button) {
            if (button) button.disabled = onlyOne;
        });
    }

    _getRows() {
        return Array.from(document.querySelectorAll('.broker-row'));
    }

    _collectBrokers() {
        var rows = this._getRows();
        if (rows.length === 0) {
            throw new Error('At least one broker is required');
        }

        var seenNames = {};
        var brokers = [];

        for (var i = 0; i < rows.length; i++) {
            var row = rows[i];
            var nameInput = row.querySelector('.broker-name');
            var hostInput = row.querySelector('.broker-host');
            var portInput = row.querySelector('.broker-port');
            var tlsInput = row.querySelector('.broker-tls');
            var endpointInput = row.querySelector('.broker-endpoint');
            var defaultInput = row.querySelector('.broker-default');

            if (!nameInput || !hostInput || !portInput || !tlsInput || !endpointInput || !defaultInput) {
                throw new Error('Malformed broker row. Reload the page and try again.');
            }

            var name = nameInput.value.trim();
            if (!name) {
                throw new Error('Broker name is required on every row');
            }
            if (seenNames[name]) {
                throw new Error('Duplicate broker name: ' + name);
            }
            seenNames[name] = true;

            var host = hostInput.value.trim();
            var rawPort = Number(portInput.value);
            var port = Number.isFinite(rawPort) && Number.isInteger(rawPort) ? rawPort : 0;

            if (host && !port) {
                throw new Error('Port must be a positive integer for broker "' + name + '".');
            }

            var endpoint = endpointInput.value.trim() || '/graphql';

            brokers.push({
                name: name,
                host: host,
                port: port,
                tls: tlsInput.checked,
                endpoint: endpoint,
                default: !!defaultInput.checked
            });
        }

        if (!brokers.some(function(b) { return b.default; }) && brokers.length) {
            brokers[0].default = true;
        }

        return brokers;
    }

    async saveBrokers() {
        var isLocal = window.isElectron || (window.brokerManager && window.brokerManager.isLocalMode);
        if (isLocal) {
            var saveBtn = document.getElementById('save-brokers-btn');
            if (saveBtn) {
                saveBtn.setAttribute('disabled', 'disabled');
            }
            try {
                var payload = this._collectBrokers();
                if (window.isElectron && window.ElectronAPI) {
                    var currentConfig = await window.ElectronAPI.readConfig() || {};
                    currentConfig.brokers = payload;
                    // Ensure active broker is still in the list, otherwise update it
                    if (currentConfig.activeBroker && !payload.some(function(b) { return b.name === currentConfig.activeBroker; })) {
                        var defB = payload.find(function(b) { return b.default; }) || payload[0];
                        currentConfig.activeBroker = defB ? defB.name : 'Local';
                    }
                    await window.ElectronAPI.writeConfig(currentConfig);
                } else {
                    safeStorage.setItem('monstermq_desktop_brokers', JSON.stringify(payload));
                }
                this.renderBrokers(payload);
                if (window.brokerManager && typeof window.brokerManager.setBrokers === 'function') {
                    window.brokerManager.setBrokers(payload);
                } else if (window.brokerManager && typeof window.brokerManager.refreshConfig === 'function') {
                    await window.brokerManager.refreshConfig();
                }
                this.showStatus('Broker configuration saved locally.', 'success');
            } catch (error) {
                console.error('Error saving local broker config:', error);
                this.showError(error.message || 'Failed to save local broker configuration.');
            } finally {
                if (saveBtn) {
                    saveBtn.removeAttribute('disabled');
                }
                this._syncDefaultState();
            }
            return;
        }

        var tokenInput = document.getElementById('broker-admin-token');
        if (tokenInput) {
            this.currentToken = tokenInput.value.trim();
        }

        if (this._apiSupportsWrite === false) {
            this.showError('Saving brokers is not available in this deployment. Edit brokers.json or the served config directly.');
            return;
        }

        if (this._apiSupportsWrite === null) {
            this.showError('Save capability is not known yet. Load broker configuration first, then try again.');
            return;
        }

        if (!this.currentToken) {
            this.showError('A save token is required. Fill in the token field and click Save token.');
            return;
        }

        var saveBtn = document.getElementById('save-brokers-btn');
        if (saveBtn) {
            saveBtn.setAttribute('disabled', 'disabled');
        }

        try {
            var payload = this._collectBrokers();

            var response = await fetch('/api/brokers', {
                method: 'POST',
                headers: Object.assign({
                    'Content-Type': 'application/json'
                }, this.getAuthHeader()),
                body: JSON.stringify({ brokers: payload }),
                signal: this._abortController.signal
            });

            var result = {};
            try {
                var responseText = await response.text();
                result = responseText ? JSON.parse(responseText) : {};
            } catch (error) {
                result = {};
            }

            if (!response.ok) {
                throw new Error(result && result.error || ('HTTP ' + response.status));
            }

            var saved = Array.isArray(result && result.brokers) ? result.brokers : payload;
            this.renderBrokers(saved);
            if (window.brokerManager && typeof window.brokerManager.setBrokers === 'function') {
                window.brokerManager.setBrokers(saved);
            } else if (window.brokerManager && typeof window.brokerManager.refreshConfig === 'function') {
                await window.brokerManager.refreshConfig();
            }
            this.showStatus('Broker configuration saved.', 'success');
        } catch (error) {
            if (error && error.name === 'AbortError') {
                return;
            }
            console.error('Error saving broker config:', error);
            this.showError(error.message || 'Failed to save broker configuration.');
        } finally {
            if (saveBtn) {
                if (this._authenticated) {
                    saveBtn.removeAttribute('disabled');
                }
            }
            this._syncDefaultState();
        }
    }

    escapeHtml(value) {
        return String(value)
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;')
            .replace(/'/g, '&#039;');
    }
}

let brokerConfigManager;
document.addEventListener('DOMContentLoaded', async function() {
    if (window.brokerManager && typeof window.brokerManager.ready === 'function') {
        await window.brokerManager.ready();
    }
    brokerConfigManager = new BrokerConfigManager();
});
