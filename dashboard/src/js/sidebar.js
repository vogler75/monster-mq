// Shared sidebar functionality for all dashboard pages
//
// HOW TO ADD A NEW MENU ITEM:
// 1. Add the menu item to the menuConfig array in the getMenuConfig() method below
// 2. Specify: section, sectionIcon, href, icon (iX icon name), text, and optionally id/adminOnly
// 3. The menu will automatically appear in all pages

class SidebarManager {
    constructor() {
        this.init();
    }

    init() {
        this.injectFavicons();
        this.renderMenu();
        this.setupUI();
        this.setActiveNavItem();
        this.setupClientNavigation();
    }

    injectFavicons() {
        const favicons = [
            { rel: 'icon', type: 'image/x-icon', href: '/favicon.ico' },
            { rel: 'icon', type: 'image/png', sizes: '32x32', href: '/favicon-32x32.png' },
            { rel: 'icon', type: 'image/png', sizes: '16x16', href: '/favicon-16x16.png' },
            { rel: 'apple-touch-icon', sizes: '180x180', href: '/apple-touch-icon.png' }
        ];
        favicons.forEach(f => {
            if (!document.querySelector(`link[href="${f.href}"]`)) {
                const link = document.createElement('link');
                Object.entries(f).forEach(([k, v]) => link.setAttribute(k, v));
                document.head.appendChild(link);
            }
        });
    }

    getMenuConfig() {
        return [
            {
                section: 'Monitoring',
                sectionIcon: 'capacity-filled',
                items: [
                    { href: '/pages/dashboard.html', icon: 'capacity-filled', text: 'Dashboard' },
                    { href: '/pages/sessions.html', icon: 'user-management', text: 'Sessions' },
                    { href: '/pages/topic-browser.html', icon: 'search', text: 'Browser' },
                    { href: '/pages/topic-chart.html', icon: 'barchart', text: 'Visualizer' }
                ]
            },
            {
                section: 'Configuration',
                sectionIcon: 'cogwheel',
                items: [
                    { href: '/pages/archive-groups.html', icon: 'health', text: 'Archives' },
                    { href: '/pages/jdbc-loggers.html', icon: 'database', text: 'Loggers' },
                    { href: '/pages/workflows.html', icon: 'ontology-filled', text: 'Workflows' },
                    { href: '/pages/device-config-export-import.html', icon: 'upload', text: 'Import/Export' }
                ]
            },
            {
                section: 'Governance',
                sectionIcon: 'shield',
                items: [
                    { href: '/pages/topic-schema-policies.html', icon: 'shield-check', text: 'Schema Policies' },
                    { href: '/pages/topic-namespaces.html', icon: 'folder', text: 'Topic Namespaces' }
                ]
            },
            {
                section: 'Bridging',
                sectionIcon: 'link',
                items: [
                    { href: '/pages/opcua-devices.html', icon: 'screen', text: 'OPC UA Clients' },
                    { href: '/pages/opcua-servers.html', icon: 'project-server', text: 'OPC UA Servers' },
                    { href: '/pages/mqtt-clients.html', icon: 'link', text: 'MQTT Clients' },
                    { href: '/pages/kafka-clients.html', icon: 'link', text: 'Kafka Clients' },
                    { href: '/pages/nats-clients.html', icon: 'link', text: 'NATS Clients' },
                    { href: '/pages/winccoa-clients.html', icon: 'rack-ipc', text: 'WinCC OA Clients' },
                    { href: '/pages/winccua-clients.html', icon: 'rack-ipc', text: 'WinCC Unified' },
                    { href: '/pages/plc4x-clients.html', icon: 'solid-state-drive', text: 'PLC4X Clients' },
                    { href: '/pages/neo4j-clients.html', icon: 'distribution', text: 'Neo4j Clients' },
                    { href: '/pages/sparkplugb-decoders.html', icon: 'electrical-energy', text: 'SparkplugB Decoders' }
                ]
            },
            {
                section: 'System',
                sectionIcon: 'cogwheel',
                items: [
                    { href: '/pages/broker-config.html', icon: 'cogwheel', text: 'Configuration' },
                    { href: '/pages/users.html', icon: 'user-settings', text: 'Users', id: 'users-nav-link', adminOnly: true },
                    { isUserItem: true }
                ]
            }
        ];
    }

    renderMenu() {
        const ixMenu = document.querySelector('ix-menu');
        if (!ixMenu) return;

        const menuConfig = this.getMenuConfig();

        menuConfig.forEach(section => {
            const category = document.createElement('ix-menu-category');
            category.setAttribute('label', section.section);
            category.setAttribute('icon', section.sectionIcon);

            section.items.forEach(item => {
                if (item.isUserItem) return;

                const menuItem = document.createElement('ix-menu-item');
                menuItem.setAttribute('label', item.text);
                menuItem.setAttribute('icon', item.icon);
                if (item.id) menuItem.id = item.id;
                if (item.adminOnly) menuItem.style.display = 'none';

                menuItem.addEventListener('click', (e) => {
                    e.preventDefault();
                    e.stopPropagation();
                    this.navigateTo(item.href);
                });

                menuItem.dataset.href = item.href;

                category.appendChild(menuItem);
            });

            ixMenu.appendChild(category);
        });

        this._addBottomItems(ixMenu);
    }

    _addBottomItems(ixMenu) {
        const token = safeStorage.getItem('monstermq_token');
        const isGuest = !token && safeStorage.getItem('monstermq_guest') === 'true';
        const hasRealToken = token && token !== 'null';
        const userManagementEnabled = hasRealToken ||
            safeStorage.getItem('monstermq_userManagementEnabled') === 'true';
        const username = isGuest ? 'Anonymous' : (safeStorage.getItem('monstermq_username') || 'User');

        if (isGuest) {
            const guestItem = document.createElement('ix-menu-item');
            guestItem.setAttribute('slot', 'bottom');
            guestItem.setAttribute('label', 'Sign in');
            guestItem.setAttribute('icon', 'log-out');
            guestItem.addEventListener('click', () => {
                window.location.href = '/pages/login.html';
            });
            ixMenu.appendChild(guestItem);
        } else if (userManagementEnabled) {
            const logoutItem = document.createElement('ix-menu-item');
            logoutItem.setAttribute('slot', 'bottom');
            logoutItem.setAttribute('label', 'Logout (' + username + ')');
            logoutItem.setAttribute('icon', 'log-out');
            logoutItem.id = 'user-menu-item';
            logoutItem.addEventListener('click', () => {
                this.logout();
            });
            ixMenu.appendChild(logoutItem);
        }
    }

    setupUI() {
        const isAdmin = safeStorage.getItem('monstermq_isAdmin') === 'true';
        const token = safeStorage.getItem('monstermq_token');
        const isGuest = !token && safeStorage.getItem('monstermq_guest') === 'true';

        if (isGuest) {
            document.body.classList.add('read-only-mode');
        }

        if (isAdmin) {
            const usersNavLink = document.getElementById('users-nav-link');
            if (usersNavLink) usersNavLink.style.display = '';
        }
    }

    setActiveNavItem() {
        const currentPath = window.location.pathname;
        requestAnimationFrame(() => {
            const menuItems = document.querySelectorAll('ix-menu-item[data-href]');
            menuItems.forEach(item => {
                if (item.dataset.href === currentPath) {
                    item.setAttribute('active', '');
                } else {
                    item.removeAttribute('active');
                }
            });
        });
    }

    // ===================== Client-Side Navigation =====================

    setupClientNavigation() {
        // Handle browser back/forward
        window.addEventListener('popstate', (e) => {
            if (e.state && e.state.href) {
                this._loadPage(e.state.href, false);
            }
        });

        // Intercept <a> links within main-content that point to /pages/
        document.addEventListener('click', (e) => {
            const link = e.target.closest('a[href]');
            if (!link) return;

            const href = link.getAttribute('href');
            if (!href) return;

            // Only intercept internal /pages/ links
            if (href.startsWith('/pages/') && href.endsWith('.html')) {
                e.preventDefault();
                // Preserve query string
                const fullHref = link.href;
                const url = new URL(fullHref, window.location.origin);
                this.navigateTo(url.pathname + url.search);
            }
        });

        // Store initial state
        history.replaceState({ href: window.location.pathname + window.location.search }, '', window.location.href);
    }

    navigateTo(href) {
        if (href === window.location.pathname + window.location.search) return;
        this._loadPage(href, true);
    }

    async _loadPage(href, pushState) {
        try {
            const response = await fetch(href);
            if (!response.ok) {
                window.location.href = href; // fallback to full navigation
                return;
            }
            const html = await response.text();
            const parser = new DOMParser();
            const doc = parser.parseFromString(html, 'text/html');

            // Extract main content
            const newMainContent = doc.querySelector('#main-content');
            if (!newMainContent) {
                window.location.href = href; // fallback
                return;
            }

            // Extract page-specific <style> blocks from <head>
            const newStyles = doc.querySelectorAll('head style');

            // Extract page-specific scripts (the ones after main-content, not shared ones)
            const sharedScripts = new Set([
                '/js/storage.js', '/js/graphql-client.js', '/js/sidebar.js',
                '/js/log-viewer.js', '/js/ix-init.js'
            ]);
            const newScripts = [];
            doc.querySelectorAll('head script[src], body script[src]').forEach(s => {
                const src = s.getAttribute('src');
                if (src && !sharedScripts.has(src) && !src.includes('cdn.jsdelivr') && !src.startsWith('http')) {
                    newScripts.push(src);
                }
            });

            // Also collect CDN scripts needed by the page (e.g., Chart.js)
            const cdnScripts = [];
            doc.querySelectorAll('head script[src]').forEach(s => {
                const src = s.getAttribute('src');
                if (src && src.startsWith('http') && !document.querySelector(`script[src="${src}"]`)) {
                    cdnScripts.push(src);
                }
            });

            // Extract all extra elements outside main-content but inside ix-application
            // (modals, side panels, floating buttons, overlays, etc.)
            const newExtras = [];
            const ixApp = doc.querySelector('ix-application');
            if (ixApp) {
                for (const child of ixApp.children) {
                    const tag = child.tagName.toLowerCase();
                    if (tag === 'ix-menu' || tag === 'script' || child.id === 'main-content') continue;
                    newExtras.push(child);
                }
            }

            // Extract body data attributes for log viewer
            const newBody = doc.querySelector('body');
            const bodyDataAttrs = {};
            if (newBody) {
                for (const attr of newBody.attributes) {
                    if (attr.name.startsWith('data-')) {
                        bodyDataAttrs[attr.name] = attr.value;
                    }
                }
            }

            // Update title
            const newTitle = doc.querySelector('title');
            if (newTitle) document.title = newTitle.textContent;

            // Remove old page-specific styles
            document.querySelectorAll('style[data-page-style]').forEach(s => s.remove());

            // Add new page-specific styles
            newStyles.forEach(style => {
                const s = document.createElement('style');
                s.setAttribute('data-page-style', 'true');
                s.textContent = style.textContent;
                document.head.appendChild(s);
            });

            // Suppress errors from stale in-flight async callbacks during transition
            const _origError = console.error;
            const suppressHandler = (e) => { e.preventDefault(); };
            console.error = function() {};
            window.addEventListener('error', suppressHandler);
            window.addEventListener('unhandledrejection', suppressHandler);

            // Clean up previous page FIRST (stop intervals before swapping DOM)
            this._cleanupPage();

            // Swap main content
            const mainContent = document.getElementById('main-content');
            mainContent.innerHTML = newMainContent.innerHTML;
            mainContent.scrollTop = 0;

            // Remove old page-specific extra elements from ix-application
            const ixAppLocal = document.querySelector('ix-application');
            document.querySelectorAll('[data-page-extra]').forEach(el => el.remove());

            // Add new extra elements
            newExtras.forEach(el => {
                el.setAttribute('data-page-extra', 'true');
                ixAppLocal.appendChild(el);
            });

            // Update body data attributes
            for (const attr of [...document.body.attributes]) {
                if (attr.name.startsWith('data-log')) {
                    document.body.removeAttribute(attr.name);
                }
            }
            for (const [name, value] of Object.entries(bodyDataAttrs)) {
                document.body.setAttribute(name, value);
            }

            // Update URL
            if (pushState) {
                history.pushState({ href }, '', href);
            }

            // Update active menu item
            this.setActiveNavItem();

            // Load CDN scripts (only if not already loaded)
            for (const src of cdnScripts) {
                await this._loadCdnScript(src);
            }

            // Load and execute page scripts with DOMContentLoaded shim
            for (const src of newScripts) {
                await this._loadPageScript(src);
            }

            // Restore error reporting after new page is loaded
            setTimeout(() => {
                console.error = _origError;
                window.removeEventListener('error', suppressHandler);
                window.removeEventListener('unhandledrejection', suppressHandler);
            }, 500);

        } catch (error) {
            console.error = _origError;
            window.removeEventListener('error', suppressHandler);
            window.removeEventListener('unhandledrejection', suppressHandler);
            console.error('Client navigation failed, falling back:', error);
            window.location.href = href;
        }
    }

    _cleanupPage() {
        // Clear tracked intervals/timeouts from previous page
        if (window._pageIntervals) {
            window._pageIntervals.forEach(id => clearInterval(id));
        }
        if (window._pageTimeouts) {
            window._pageTimeouts.forEach(id => clearTimeout(id));
        }
        window._pageIntervals = [];
        window._pageTimeouts = [];
    }

    async _loadPageScript(src) {
        try {
            const resp = await fetch(src);
            if (!resp.ok) return;
            const code = await resp.text();

            // Patch addEventListener BEFORE executing the script so DOMContentLoaded
            // callbacks fire immediately. Do NOT wrap in IIFE — page scripts use
            // top-level let/const/function that inline onclick handlers need in global scope.
            const origAdd = document.addEventListener;
            document.addEventListener = function(type, fn, opts) {
                if (type === 'DOMContentLoaded') {
                    try { fn(); } catch(e) { console.error(e); }
                    return;
                }
                return origAdd.call(document, type, fn, opts);
            };

            try {
                const script = document.createElement('script');
                script.textContent = code;
                document.body.appendChild(script);
                script.remove();
            } finally {
                document.addEventListener = origAdd;
            }
        } catch (e) {
            console.warn('Failed to load page script:', src, e);
        }
    }

    _loadCdnScript(src) {
        if (document.querySelector(`script[src="${src}"]`)) {
            return Promise.resolve();
        }
        return new Promise((resolve) => {
            const script = document.createElement('script');
            script.src = src;
            script.onload = resolve;
            script.onerror = () => { console.warn('Failed to load CDN script:', src); resolve(); };
            document.head.appendChild(script);
        });
    }

    // ===================== Auth =====================

    logout() {
        if (window.graphqlClient) {
            window.graphqlClient.stopTokenExpirationCheck();
        }
        safeStorage.removeItem('monstermq_token');
        safeStorage.removeItem('monstermq_username');
        safeStorage.removeItem('monstermq_isAdmin');
        safeStorage.removeItem('monstermq_guest');
        sessionStorage.clear();
        window.location.href = '/pages/login.html';
    }
}

// Global interval/timeout tracking for client-side navigation cleanup
(function() {
    window._pageIntervals = [];
    window._pageTimeouts = [];
    const _origSetInterval = window.setInterval.bind(window);
    const _origSetTimeout = window.setTimeout.bind(window);
    const _origClearInterval = window.clearInterval.bind(window);
    const _origClearTimeout = window.clearTimeout.bind(window);
    window.setInterval = function() {
        const id = _origSetInterval.apply(window, arguments);
        window._pageIntervals.push(id);
        return id;
    };
    window.setTimeout = function() {
        const id = _origSetTimeout.apply(window, arguments);
        window._pageTimeouts.push(id);
        return id;
    };
    window.clearInterval = function(id) {
        _origClearInterval(id);
        const idx = window._pageIntervals.indexOf(id);
        if (idx !== -1) window._pageIntervals.splice(idx, 1);
    };
    window.clearTimeout = function(id) {
        _origClearTimeout(id);
        const idx = window._pageTimeouts.indexOf(id);
        if (idx !== -1) window._pageTimeouts.splice(idx, 1);
    };
})();

// Initialize sidebar when DOM is loaded
document.addEventListener('DOMContentLoaded', () => {
    new SidebarManager();

    // Inject global Log Viewer on all authenticated pages (exclude login page)
    if (!window.location.pathname.endsWith('/pages/login.html')) {
        let lvContainer = document.getElementById('log-viewer-container');
        if (!lvContainer) {
            lvContainer = document.createElement('div');
            lvContainer.id = 'log-viewer-container';
            document.body.appendChild(lvContainer);
        }
        if (!window.__monsterMQLogViewer) {
            if (typeof LogViewer === 'function') {
                const body = document.body;
                const filters = {};
                if (body.dataset.logSourceClass) filters.sourceClassRegex = body.dataset.logSourceClass;
                if (body.dataset.logMessageRegex) filters.messageRegex = body.dataset.logMessageRegex;
                if (body.dataset.logLoggerRegex) filters.loggerRegex = body.dataset.logLoggerRegex;
                const title = body.dataset.logTitle || 'System Logs';
                const startCollapsed = body.dataset.logCollapsed === 'false' ? false : true;
                window.__monsterMQLogViewer = new LogViewer('log-viewer-container', {
                    collapsed: startCollapsed,
                    maxLines: 1000,
                    autoScroll: true,
                    title,
                    filters,
                    deferConnection: true
                });
                window.updateLogViewerFilters = (newFilters) => {
                    if (window.__monsterMQLogViewer) {
                        window.__monsterMQLogViewer.setFilters(newFilters);
                    }
                };
                if (window.__monsterMQLogViewerReadyCallbacks) {
                    window.__monsterMQLogViewerReadyCallbacks.forEach(cb => cb(window.__monsterMQLogViewer));
                    window.__monsterMQLogViewerReadyCallbacks = [];
                }
                if (!window.__monsterMQLogViewer.isConnected && !window.__monsterMQLogViewer.ws) {
                    window.__monsterMQLogViewer.connect();
                }
            }
        }
    }
});

// Helper to run code when log viewer is ready
window.onLogViewerReady = function (callback) {
    if (window.__monsterMQLogViewer) {
        callback(window.__monsterMQLogViewer);
    } else {
        if (!window.__monsterMQLogViewerReadyCallbacks) {
            window.__monsterMQLogViewerReadyCallbacks = [];
        }
        window.__monsterMQLogViewerReadyCallbacks.push(callback);
    }
};
