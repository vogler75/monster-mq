/**
 * Safe wrapper for localStorage to handle cases where it is unavailable
 * (e.g., Brave browser with Shields UP, or disabled cookies).
 * 
 * Falls back to in-memory storage if localStorage access fails.
 */
class StorageManager {
    constructor() {
        this.memoryStorage = {};
        this.isSupported = this.checkSupport();

        if (!this.isSupported) {
            console.warn('localStorage is not available. Using in-memory fallback.');
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

    setItem(key, value) {
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
