/**
 * BrokerManager — manages broker endpoint selection for the dashboard.
 *
 * Brokers are defined in config/brokers.json (shipped with the dashboard).
 * The active broker is stored in localStorage.
 * Auth credentials (token, username, etc.) are stored per broker so
 * switching back to a previously authenticated broker re-uses the session.
 *
 * All requests go through the proxy:
 *   - Local broker (host=""): relative /graphql path
 *   - Remote brokers: /broker-api/<name>/graphql (proxied by Vite or the serving broker)
 */

var AUTH_KEYS = ['token', 'username', 'isAdmin', 'guest', 'userManagementEnabled'];

class BrokerManager {
    constructor() {
        this.STORAGE_KEY_ACTIVE = 'monstermq_active_broker';
        this.configBrokers = [];
        this.loaded = false;
        this._loadPromise = this._loadConfig();
    }

    async _loadConfig() {
        // Always add a "Local" broker that uses relative URLs (same origin)
        var localBroker = { name: 'Local', host: '', port: 0, tls: false, default: true };
        try {
            var resp = await fetch('/config/brokers.json', { cache: 'no-store' });
            if (resp.ok) {
                var remote = await resp.json();
                // Filter out any "Local" entries from config — the auto-injected one takes precedence
                this.configBrokers = [localBroker].concat(remote.filter(function(b) { return b.name !== 'Local'; }));
            } else {
                this.configBrokers = [localBroker];
            }
        } catch (e) {
            console.warn('BrokerManager: could not load brokers.json', e);
            this.configBrokers = [localBroker];
        }
        this.loaded = true;
    }

    /** Wait until config is loaded */
    async ready() {
        await this._loadPromise;
    }

    /** Get all configured brokers */
    getAllBrokers() {
        return this.configBrokers;
    }

    /** Get the stored active broker identifier (name) */
    getActiveBrokerId() {
        return safeStorage.getItem(this.STORAGE_KEY_ACTIVE);
    }

    /** Set the active broker by name */
    setActiveBroker(name) {
        safeStorage.setItem(this.STORAGE_KEY_ACTIVE, name);
    }

    /** Resolve the active broker object. Falls back to the default or first broker. */
    getActiveBroker() {
        var all = this.getAllBrokers();
        var activeId = this.getActiveBrokerId();

        if (activeId) {
            var found = all.find(function(b) { return b.name === activeId; });
            if (found) return found;
        }

        var defaultBroker = all.find(function(b) { return b.default; });
        return defaultBroker || all[0] || { name: 'Local', host: '', port: 4000, tls: false };
    }

    /** Build the GraphQL endpoint path for a broker (always proxied) */
    getEndpoint(broker) {
        if (!broker) broker = this.getActiveBroker();
        if (!broker.host) return '/graphql';
        return '/broker-api/' + encodeURIComponent(broker.name) + '/graphql';
    }

    /** Build the WebSocket endpoint path for a broker (always proxied) */
    getWsEndpoint(broker) {
        if (!broker) broker = this.getActiveBroker();
        if (!broker.host) return '/graphqlws';
        return '/broker-ws/' + encodeURIComponent(broker.name) + '/graphqlws';
    }

    /** Get a display label for a broker */
    getDisplayName(broker) {
        if (!broker) broker = this.getActiveBroker();
        return broker.name || 'Local';
    }

    /** Per-broker storage key */
    _brokerKey(brokerName, key) {
        return 'monstermq_' + brokerName + '_' + key;
    }

    /** Save current global auth state to the active broker's slot */
    saveAuthForBroker(brokerName) {
        if (!brokerName) brokerName = this.getActiveBrokerId();
        if (!brokerName) return;
        for (var i = 0; i < AUTH_KEYS.length; i++) {
            var val = safeStorage.getItem('monstermq_' + AUTH_KEYS[i]);
            if (val !== null) {
                safeStorage.setItem(this._brokerKey(brokerName, AUTH_KEYS[i]), val);
            } else {
                safeStorage.removeItem(this._brokerKey(brokerName, AUTH_KEYS[i]));
            }
        }
    }

    /** Restore a broker's saved auth into the global keys */
    restoreAuthForBroker(brokerName) {
        for (var i = 0; i < AUTH_KEYS.length; i++) {
            var val = safeStorage.getItem(this._brokerKey(brokerName, AUTH_KEYS[i]));
            if (val !== null) {
                safeStorage.setItem('monstermq_' + AUTH_KEYS[i], val);
            } else {
                safeStorage.removeItem('monstermq_' + AUTH_KEYS[i]);
            }
        }
    }

    /** Check if a broker has a saved (and still valid) session */
    hasSavedSession(brokerName) {
        var token = safeStorage.getItem(this._brokerKey(brokerName, 'token'));
        if (!token) {
            // Check for guest mode
            return safeStorage.getItem(this._brokerKey(brokerName, 'guest')) === 'true';
        }
        if (token === 'null') return true; // auth disabled
        try {
            var decoded = JSON.parse(atob(token.split('.')[1]));
            return decoded.exp > Date.now() / 1000;
        } catch {
            return false;
        }
    }

    /**
     * Switch to a different broker.
     * Saves current auth, restores target broker's auth.
     * Returns true if broker changed.
     */
    switchBroker(name) {
        var current = this.getActiveBrokerId();
        if (current === name) return false;

        // Save current broker's auth before switching
        this.saveAuthForBroker(current);

        this.setActiveBroker(name);

        // Restore target broker's auth
        this.restoreAuthForBroker(name);

        sessionStorage.clear();
        return true;
    }
}

// Global instance
window.brokerManager = new BrokerManager();
