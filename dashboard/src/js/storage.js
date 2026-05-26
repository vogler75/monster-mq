/**
 * Safe wrapper for localStorage to handle cases where it is unavailable
 * (e.g., Brave browser with Shields UP, or disabled cookies).
 * 
 * Falls back to in-memory storage if localStorage access fails.
 */
class StorageManager {
    constructor() {
        this.memoryStorage = {};
        this.sessionMemoryStorage = {};
        this.tabScopedKeys = new Set([
            'monstermq_active_broker',
            'monstermq_token',
            'monstermq_username',
            'monstermq_isAdmin',
            'monstermq_guest',
            'monstermq_userManagementEnabled'
        ]);
        this.isSupported = this.checkSupport();
        this.isSessionSupported = this.checkSessionSupport();

        if (!this.isSupported) {
            console.warn('localStorage is not available. Using in-memory fallback.');
        }
        if (!this.isSessionSupported) {
            console.warn('sessionStorage is not available. Using in-memory fallback for tab-scoped state.');
        }
    }

    checkSupport() {
        try {
            const testKey = '__monstermq_test__';
            localStorage.setItem(testKey, testKey);
            localStorage.removeItem(testKey);
            return true;
        } catch (e) {
            return false;
        }
    }

    checkSessionSupport() {
        try {
            const testKey = '__monstermq_session_test__';
            sessionStorage.setItem(testKey, testKey);
            sessionStorage.removeItem(testKey);
            return true;
        } catch (e) {
            return false;
        }
    }

    isTabScopedKey(key) {
        return this.tabScopedKeys.has(key);
    }

    setItem(key, value) {
        if (this.isTabScopedKey(key)) {
            if (this.isSessionSupported) {
                try {
                    sessionStorage.setItem(key, value);
                    return;
                } catch (e) {
                    console.warn(`Failed to save to sessionStorage: ${e.message}`);
                }
            }
            this.sessionMemoryStorage[key] = value;
            return;
        }

        if (this.isSupported) {
            try {
                localStorage.setItem(key, value);
            } catch (e) {
                console.warn(`Failed to save to localStorage: ${e.message}`);
                this.memoryStorage[key] = value;
            }
        } else {
            this.memoryStorage[key] = value;
        }
    }

    getItem(key) {
        if (this.isTabScopedKey(key)) {
            if (this.isSessionSupported) {
                try {
                    return sessionStorage.getItem(key);
                } catch (e) {
                    return this.sessionMemoryStorage[key] || null;
                }
            }
            return this.sessionMemoryStorage[key] || null;
        }

        if (this.isSupported) {
            try {
                return localStorage.getItem(key);
            } catch (e) {
                return this.memoryStorage[key] || null;
            }
        }
        return this.memoryStorage[key] || null;
    }

    removeItem(key) {
        if (this.isTabScopedKey(key)) {
            if (this.isSessionSupported) {
                try {
                    sessionStorage.removeItem(key);
                    return;
                } catch (e) {
                    delete this.sessionMemoryStorage[key];
                    return;
                }
            }
            delete this.sessionMemoryStorage[key];
            return;
        }

        if (this.isSupported) {
            try {
                localStorage.removeItem(key);
            } catch (e) {
                delete this.memoryStorage[key];
            }
        } else {
            delete this.memoryStorage[key];
        }
    }

    clear() {
        if (this.isSupported) {
            try {
                localStorage.clear();
            } catch (e) {
                this.memoryStorage = {};
            }
        } else {
            this.memoryStorage = {};
        }
    }
}

// Expose a global instance
window.safeStorage = new StorageManager();

window.clearPageSessionState = function() {
    const preserved = {};
    const keys = window.safeStorage ? Array.from(window.safeStorage.tabScopedKeys) : [];

    keys.forEach(function(key) {
        const value = safeStorage.getItem(key);
        if (value !== null) preserved[key] = value;
    });

    try {
        sessionStorage.clear();
    } catch (e) {
        if (window.safeStorage) {
            window.safeStorage.sessionMemoryStorage = {};
        }
    }

    Object.keys(preserved).forEach(function(key) {
        safeStorage.setItem(key, preserved[key]);
    });
};

/**
 * SPA-aware location proxy.
 * Assign spaLocation.href exactly like window.location.href.
 * Uses window.navigateTo (SPA) when available, except for login page
 * which always needs a full reload to clear auth state.
 */
window.spaLocation = {
    set href(url) {
        if (window.navigateTo && !url.includes('login.html')) {
            window.navigateTo(url);
        } else {
            window.location.href = url;
        }
    }
};

window.redirectToLogin = function(reason) {
    if (reason) {
        try {
            sessionStorage.setItem('monstermq_login_message', reason);
        } catch (e) {
            console.warn('Failed to store login redirect reason:', e.message);
        }
    }
    window.location.href = '/pages/login.html';
};

window.decodeJwtPayload = function(token) {
    const parts = token ? token.split('.') : [];
    if (parts.length !== 3) {
        throw new Error('JWT must have three parts');
    }

    let payload = parts[1].replace(/-/g, '+').replace(/_/g, '/');
    const padding = payload.length % 4;
    if (padding) {
        payload += '='.repeat(4 - padding);
    }

    return JSON.parse(atob(payload));
};

window.isJwtToken = function(token) {
    return !!token && token.split('.').length === 3;
};

/**
 * Shared auth check used by all dashboard pages.
 * Returns true if the user has a valid JWT, auth is disabled (token === 'null'),
 * or they are in guest (read-only) mode.
 */
window.isLoggedIn = function() {
    const token = safeStorage.getItem('monstermq_token');

    // Guest mode: no token but guest flag set — allow through (read-only)
    if (!token) {
        return safeStorage.getItem('monstermq_guest') === 'true';
    }

    // Auth disabled
    if (token === 'null') return true;

    // Opaque server-side session token. The backend validates it on API calls.
    if (!window.isJwtToken(token)) return true;

    // Validate JWT expiry when the broker returns a JWT.
    try {
        const decoded = window.decodeJwtPayload(token);
        return decoded.exp > Date.now() / 1000;
    } catch (e) {
        try {
            sessionStorage.setItem('monstermq_login_message', 'Saved login token could not be decoded: ' + e.message);
        } catch (_) {}
        return false;
    }
};
