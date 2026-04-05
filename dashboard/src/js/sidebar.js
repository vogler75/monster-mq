// MonsterMQ Dashboard — Sidebar & SPA Navigation
//
// HOW TO ADD A NEW MENU ITEM:
// 1. Add the item to getMenuConfig() below
// 2. Specify: section, sectionIcon, href, icon, text, and optionally id/adminOnly

class SidebarManager {
    constructor() {
        this._currentHref = null;
        this._pageCleanups = [];
        this.init();
    }

    async init() {
        this.injectFavicons();
        if (window.brokerManager) await window.brokerManager.ready();
        // Fetch enabled features; null = unknown (show all), [] = all disabled
        this._enabledFeatures = null;
        try {
            const client = window.graphqlClient || null;
            if (client) this._enabledFeatures = await client.getEnabledFeatures();
        } catch (e) { /* feature hiding is best-effort */ }
        this.renderMenu();
        this.setupUI();
        this.initLogViewer();

        // Expose navigateTo globally for page scripts
        window.navigateTo = (href) => this.navigateTo(href);

        // Intercept local <a> link clicks so they use SPA navigation instead of full reloads
        document.addEventListener('click', (e) => {
            const link = e.target.closest('a[href]');
            if (!link) return;
            const href = link.getAttribute('href');
            if (!href || href.startsWith('http') || href.startsWith('//') ||
                href.startsWith('mailto:') || href.startsWith('#')) return;
            if (href.startsWith('/pages/') || href.startsWith('pages/')) {
                e.preventDefault();
                this.navigateTo(href.startsWith('/') ? href : '/' + href);
            }
        });

        // Handle browser back/forward
        window.addEventListener('popstate', (e) => {
            const page = e.state?.page || '/pages/dashboard.html';
            this.navigateTo(page, false);
        });

        // Load initial page based on current URL
        this.loadInitialPage();
    }

    loadInitialPage() {
        // Check for ?page= redirect from standalone pages
        const params = new URLSearchParams(window.location.search);
        const redirectPage = params.get('page');
        if (redirectPage) {
            this.navigateTo(redirectPage, true);
            return;
        }
        // Otherwise check the URL path
        const path = window.location.pathname;
        if (path.includes('/pages/') && path.endsWith('.html')) {
            this.navigateTo(path + window.location.search, true);
        } else {
            this.navigateTo('/pages/dashboard.html', true);
        }
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

    // ===================== Menu Configuration =====================

    getMenuConfig() {
        return [
            {
                section: 'Monitoring', sectionIcon: 'capacity-filled',
                items: [
                    { href: '/pages/dashboard.html', icon: 'capacity-filled', text: 'Dashboard' },
                    { href: '/pages/sessions.html', icon: 'user-management', text: 'Sessions' },
                    { href: '/pages/topic-browser.html', icon: 'search', text: 'Browser' },
                    { href: '/pages/topic-chart.html', icon: 'barchart', text: 'Visualizer' },
                    { href: '/pages/archive-explorer.html', icon: 'list', text: 'Explorer' }
                ]
            },
            {
                section: 'Configuration', sectionIcon: 'cogwheel',
                items: [
                    { href: '/pages/archive-groups.html', icon: 'health', text: 'Archives' },
                    { href: '/pages/jdbc-loggers.html', icon: 'database', text: 'JDBC Loggers', feature: 'JdbcLogger' },
                    { href: '/pages/influxdb-loggers.html', icon: 'database', text: 'InfluxDB Loggers', feature: 'InfluxDBLogger' },
                    { href: '/pages/timebase-loggers.html', icon: 'database', text: 'TimeBase Loggers', feature: 'TimeBaseLogger' },
                    { href: '/pages/workflows.html', icon: 'ontology-filled', text: 'Workflows', feature: 'FlowEngine' },
                    { href: '/pages/device-config-export-import.html', icon: 'upload', text: 'Import/Export' }
                ]
            },
            {
                section: 'Governance', sectionIcon: 'shield',
                items: [
                    { href: '/pages/topic-schema-policies.html', icon: 'shield-check', text: 'Schema Policies' },
                    { href: '/pages/topic-namespaces.html', icon: 'folder', text: 'Topic Namespaces' }
                ]
            },
            {
                section: 'Agents', sectionIcon: 'rocket',
                items: [
                    { href: '/pages/agent-monitor.html', icon: 'capacity-filled', text: 'Agent Monitor' },
                    { href: '/pages/agent-online.html', icon: 'distribution', text: 'Agent Graph' },
                    { href: '/pages/agents.html', icon: 'rocket', text: 'AI Agents' },
                    { href: '/pages/genai-providers.html', icon: 'hierarchy', text: 'AI Providers' },
                    { href: '/pages/mcp-servers.html', icon: 'connector', text: 'MCP Servers' }
                ]
            },
            {
                section: 'Bridging', sectionIcon: 'link',
                items: [
                    { href: '/pages/opcua-devices.html', icon: 'screen', text: 'OPC UA Clients', feature: 'OpcUa' },
                    { href: '/pages/opcua-servers.html', icon: 'project-server', text: 'OPC UA Servers', feature: 'OpcUaServer' },
                    { href: '/pages/mqtt-clients.html', icon: 'link', text: 'MQTT Clients', feature: 'MqttClient' },
                    { href: '/pages/kafka-clients.html', icon: 'link', text: 'Kafka Clients', feature: 'Kafka' },
                    { href: '/pages/nats-clients.html', icon: 'link', text: 'NATS Clients', feature: 'Nats' },
                    { href: '/pages/redis-clients.html', icon: 'link', text: 'Redis Clients', feature: 'Redis' },
                    { href: '/pages/telegram-clients.html', icon: 'send-top-right', text: 'Telegram Clients', feature: 'Telegram' },
                    { href: '/pages/winccoa-clients.html', icon: 'rack-ipc', text: 'WinCC OA Clients', feature: 'WinCCOa' },
                    { href: '/pages/winccua-clients.html', icon: 'rack-ipc', text: 'WinCC Unified Clients', feature: 'WinCCUa' },
                    { href: '/pages/plc4x-clients.html', icon: 'solid-state-drive', text: 'PLC4X Clients', feature: 'Plc4x' },
                    { href: '/pages/neo4j-clients.html', icon: 'distribution', text: 'Neo4j Clients', feature: 'Neo4j' },
                    { href: '/pages/sparkplugb-decoders.html', icon: 'electrical-energy', text: 'SparkplugB Decoders', feature: 'SparkplugB' }
                ]
            },
            {
                section: 'System', sectionIcon: 'maintenance',
                items: [
                    { href: '/pages/broker-config.html', icon: 'cogwheel', text: 'Configuration' },
                    { href: '/pages/users.html', icon: 'user-settings', text: 'Users', id: 'users-nav-link', adminOnly: true },
                    { isUserItem: true }
                ]
            }
        ];
    }

    // ===================== Menu Rendering =====================

    renderMenu() {
        const ixMenu = document.querySelector('ix-menu');
        if (!ixMenu) return;

        this.getMenuConfig().forEach(section => {
            const category = document.createElement('ix-menu-category');
            category.setAttribute('label', section.section);
            category.setAttribute('icon', section.sectionIcon);

            // Prevent auto-closing other categories
            category.addEventListener('closeOtherCategories', (e) => e.stopPropagation());

            let itemCount = 0;
            section.items.forEach(item => {
                if (item.isUserItem) return;
                // Hide if feature is specified and not in the broker's enabled set
                if (item.feature && Array.isArray(this._enabledFeatures) &&
                    !this._enabledFeatures.includes(item.feature)) return;

                itemCount++;
                const menuItem = document.createElement('ix-menu-item');
                menuItem.setAttribute('label', item.text);
                menuItem.setAttribute('icon', item.icon);
                if (item.id) menuItem.id = item.id;
                if (item.adminOnly) menuItem.style.display = 'none';
                menuItem.dataset.href = item.href;

                menuItem.addEventListener('click', () => this.navigateTo(item.href));

                category.appendChild(menuItem);
            });

            // Skip empty categories (all items were feature-gated out)
            if (itemCount > 0) ixMenu.appendChild(category);
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

        // Log viewer toggle
        const logToggle = document.createElement('ix-menu-item');
        logToggle.setAttribute('slot', 'bottom');
        logToggle.setAttribute('label', 'Logs');
        logToggle.setAttribute('icon', 'tag-logging');
        logToggle.addEventListener('click', () => {
            if (window.__monsterMQLogViewer) {
                window.__monsterMQLogViewer.toggleVisibility();
                logToggle.setAttribute('icon',
                    window.__monsterMQLogViewer.isHidden ? 'tag-logging' : 'tag-logging-filled');
            }
        });
        ixMenu.appendChild(logToggle);
        this._logToggleItem = logToggle;

        // Broker switcher item
        if (window.brokerManager) {
            const brokerName = window.brokerManager.getDisplayName();
            const brokerItem = document.createElement('ix-menu-item');
            brokerItem.setAttribute('slot', 'bottom');
            brokerItem.setAttribute('label', brokerName);
            brokerItem.setAttribute('icon', 'distribution');
            brokerItem.addEventListener('click', () => this.showBrokerSwitcher());
            ixMenu.appendChild(brokerItem);
        }

        if (isGuest) {
            const guestItem = document.createElement('ix-menu-item');
            guestItem.setAttribute('slot', 'bottom');
            guestItem.setAttribute('label', 'Sign in');
            guestItem.setAttribute('icon', 'log-out');
            guestItem.addEventListener('click', () => { window.location.href = '/pages/login.html'; });
            ixMenu.appendChild(guestItem);
        } else if (userManagementEnabled) {
            const logoutItem = document.createElement('ix-menu-item');
            logoutItem.setAttribute('slot', 'bottom');
            logoutItem.setAttribute('label', 'Logout (' + username + ')');
            logoutItem.setAttribute('icon', 'log-out');
            logoutItem.addEventListener('click', () => this.logout());
            ixMenu.appendChild(logoutItem);
        }
    }

    showBrokerSwitcher() {
        // Remove existing overlay if any
        const existing = document.getElementById('broker-switcher-overlay');
        if (existing) { existing.remove(); return; }

        const brokers = window.brokerManager.getAllBrokers();
        const activeBroker = window.brokerManager.getActiveBroker();
        const activeId = activeBroker ? activeBroker.name : null;

        const overlay = document.createElement('div');
        overlay.id = 'broker-switcher-overlay';
        overlay.style.cssText = 'position:fixed;top:0;left:0;width:100%;height:100%;background:rgba(0,0,0,0.6);z-index:10000;display:flex;align-items:center;justify-content:center;';

        let brokerListHtml = brokers.map(b => {
            const isActive = b.name === activeId;
            const detail = b.host ? (b.tls ? 'https://' : 'http://') + b.host + ':' + b.port : 'This server';
            return '<div class="broker-switcher-item' + (isActive ? ' active' : '') + '" data-broker-id="' + b.name + '">' +
                '<div class="broker-item-info">' +
                '<div class="broker-item-name">' + b.name + (isActive ? ' <span class="broker-active-badge">active</span>' : '') + '</div>' +
                '<div class="broker-item-detail">' + detail + '</div>' +
                '</div>' +
                '</div>';
        }).join('');

        overlay.innerHTML =
            '<div class="broker-switcher-card">' +
            '<div class="broker-switcher-header">' +
            '<h3>Switch Broker</h3>' +
            '<button class="broker-switcher-close">&times;</button>' +
            '</div>' +
            '<div class="broker-switcher-list">' + brokerListHtml + '</div>' +
            '</div>';

        // Inject styles
        const style = document.createElement('style');
        style.id = 'broker-switcher-styles';
        style.textContent = [
            '.broker-switcher-card { background:var(--dark-surface,#1E293B);border:1px solid var(--dark-border,#475569);border-radius:12px;width:400px;max-width:90vw;max-height:70vh;display:flex;flex-direction:column;box-shadow:0 8px 32px rgba(0,0,0,0.5); }',
            '.broker-switcher-header { display:flex;justify-content:space-between;align-items:center;padding:1.25rem 1.5rem;border-bottom:1px solid var(--dark-border,#475569); }',
            '.broker-switcher-header h3 { margin:0;color:var(--text-primary,#F1F5F9);font-size:1.1rem; }',
            '.broker-switcher-close { background:none;border:none;color:var(--text-muted,#94A3B8);font-size:1.5rem;cursor:pointer;padding:0;line-height:1; }',
            '.broker-switcher-close:hover { color:var(--text-primary,#F1F5F9); }',
            '.broker-switcher-list { overflow-y:auto;padding:0.5rem; }',
            '.broker-switcher-item { display:flex;align-items:center;padding:0.75rem 1rem;border-radius:8px;cursor:pointer;transition:background 0.15s; }',
            '.broker-switcher-item:hover { background:var(--dark-surface-2,#334155); }',
            '.broker-switcher-item.active { background:var(--dark-surface-2,#334155);border:1px solid var(--monster-purple,#7C3AED); }',
            '.broker-item-info { flex:1;min-width:0; }',
            '.broker-item-name { color:var(--text-primary,#F1F5F9);font-weight:500;font-size:0.95rem; }',
            '.broker-item-detail { color:var(--text-muted,#94A3B8);font-size:0.8rem;margin-top:2px; }',
            '.broker-active-badge { background:var(--monster-purple,#7C3AED);color:white;font-size:0.65rem;padding:2px 6px;border-radius:4px;margin-left:0.5rem;vertical-align:middle;text-transform:uppercase;letter-spacing:0.03em; }'
        ].join('\n');
        document.head.appendChild(style);

        document.body.appendChild(overlay);

        // Close on backdrop click
        overlay.addEventListener('click', (e) => {
            if (e.target === overlay) { overlay.remove(); style.remove(); }
        });

        // Close button
        overlay.querySelector('.broker-switcher-close').addEventListener('click', () => {
            overlay.remove(); style.remove();
        });

        // Broker selection
        overlay.querySelectorAll('.broker-switcher-item').forEach(item => {
            item.addEventListener('click', () => {
                const brokerId = item.dataset.brokerId;
                if (brokerId === activeId) { overlay.remove(); style.remove(); return; }
                if (window.brokerManager.switchBroker(brokerId)) {
                    if (window.isLoggedIn()) {
                        // Valid session restored — reload dashboard
                        window.location.href = '/';
                    } else {
                        window.location.href = '/pages/login.html';
                    }
                }
            });
        });
    }

    setupUI() {
        const isAdmin = safeStorage.getItem('monstermq_isAdmin') === 'true';
        const token = safeStorage.getItem('monstermq_token');
        const isGuest = !token && safeStorage.getItem('monstermq_guest') === 'true';

        if (isGuest) document.body.classList.add('read-only-mode');
        if (isAdmin) {
            const usersNavLink = document.getElementById('users-nav-link');
            if (usersNavLink) usersNavLink.style.display = '';
        }
    }

    setActiveNavItem() {
        const currentPath = this._currentHref?.split('?')[0];
        requestAnimationFrame(() => {
            document.querySelectorAll('ix-menu-item[data-href]').forEach(item => {
                if (item.dataset.href === currentPath) {
                    item.setAttribute('active', '');
                } else {
                    item.removeAttribute('active');
                }
            });
        });
    }

    // ===================== SPA Navigation =====================

    async navigateTo(href, push = true) {
        const hrefNoQuery = href.split('?')[0];
        const currentNoQuery = this._currentHref?.split('?')[0];

        // Skip if same page (but allow query string changes)
        if (href === this._currentHref) return;

        try {
            // Fetch the target page
            const response = await fetch(href, { cache: 'no-store' });
            if (!response.ok) throw new Error(`HTTP ${response.status}`);
            const html = await response.text();
            const doc = new DOMParser().parseFromString(html, 'text/html');

            // Extract main content
            const newMainContent = doc.querySelector('#main-content');
            if (!newMainContent) throw new Error('No #main-content found');

            // Extract page-specific styles from <head>
            const newStyles = doc.querySelectorAll('head style');

            // Extract page-specific stylesheets (not monster-theme or ix-app)
            const sharedCSS = new Set(['/assets/monster-theme.css', '/assets/ix-app.css']);
            const newStylesheets = [];
            doc.querySelectorAll('head link[rel="stylesheet"]').forEach(link => {
                const h = link.getAttribute('href');
                if (h && !sharedCSS.has(h)) newStylesheets.push(h);
            });

            // Extract page-specific scripts (skip shared ones)
            const sharedScripts = new Set([
                '/js/storage.js', '/js/broker-manager.js', '/js/graphql-client.js',
                '/js/sidebar.js', '/js/log-viewer.js', '/js/ix-init.js'
            ]);
            const newScripts = [];
            doc.querySelectorAll('head script[src], body script[src]').forEach(s => {
                const src = s.getAttribute('src');
                if (!src) return;
                if (s.getAttribute('type') === 'module') return;
                if (src.includes('cdn.jsdelivr') || src.startsWith('http')) return;
                const srcPath = src.split('?')[0];
                if (sharedScripts.has(srcPath)) return;
                newScripts.push(srcPath);
            });

            // Extract CDN scripts
            const cdnScripts = [];
            doc.querySelectorAll('head script[src]').forEach(s => {
                const src = s.getAttribute('src');
                if (src && (src.includes('cdn.jsdelivr') || src.startsWith('http'))) {
                    if (!document.querySelector(`script[src="${src}"]`)) cdnScripts.push(src);
                }
            });

            // Extract extra elements outside #main-content (modals, panels, etc.)
            const newExtras = [];
            const ixApp = doc.querySelector('ix-application');
            if (ixApp) {
                for (const child of ixApp.children) {
                    const tag = child.tagName.toLowerCase();
                    if (tag === 'ix-menu' || tag === 'ix-application-header' ||
                        tag === 'script' || child.id === 'main-content') continue;
                    newExtras.push(child);
                }
            }

            // Update title
            const newTitle = doc.querySelector('title');
            if (newTitle) document.title = newTitle.textContent;

            // --- Cleanup previous page ---
            this._cleanupPage();

            // --- Apply new page ---

            // Swap styles
            document.querySelectorAll('style[data-page-style]').forEach(s => s.remove());
            newStyles.forEach(style => {
                const s = document.createElement('style');
                s.setAttribute('data-page-style', 'true');
                s.textContent = style.textContent;
                document.head.appendChild(s);
            });

            // Swap stylesheets
            document.querySelectorAll('link[data-page-css]').forEach(l => l.remove());
            newStylesheets.forEach(href => {
                const link = document.createElement('link');
                link.rel = 'stylesheet';
                link.href = href;
                link.setAttribute('data-page-css', 'true');
                document.head.appendChild(link);
            });

            // Swap main content
            const mainContent = document.getElementById('main-content');
            mainContent.innerHTML = newMainContent.innerHTML;
            mainContent.scrollTop = 0;

            // Swap extra elements
            const ixAppLocal = document.querySelector('ix-application');
            document.querySelectorAll('[data-page-extra]').forEach(el => el.remove());
            newExtras.forEach(el => {
                el.setAttribute('data-page-extra', 'true');
                ixAppLocal.appendChild(el);
            });

            // Update URL
            if (push) {
                history.pushState({ page: href }, '', href);
            }
            this._currentHref = href;

            // Update active menu item
            this.setActiveNavItem();

            // Load scripts
            for (const src of cdnScripts) await this._loadCdnScript(src);
            for (const src of newScripts) await this._loadPageScript(src);

        } catch (error) {
            console.error('SPA navigation failed:', error);
            // Fallback: full page load
            window.location.replace(href);
        }
    }

    _cleanupPage() {
        // Run registered cleanup callbacks
        this._pageCleanups.forEach(fn => { try { fn(); } catch(e) {} });
        this._pageCleanups = [];

        // Clear tracked intervals/timeouts
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
            const resp = await fetch(src, { cache: 'no-store' });
            if (!resp.ok) return;
            let code = await resp.text();

            // Rewrite let/const/class to var so they can be re-declared across navigations.
            // function/async function are fine in sloppy mode IIFEs.
            code = code.replace(/^class\s+(\w+)/gm, 'var $1 = class $1');
            code = code.replace(/^(let |const )/gm, 'var ');

            // Collect top-level identifiers for window export
            const names = new Set();
            code.replace(/^var\s+(\w+)/gm, (_, n) => names.add(n));
            code.replace(/^(?:async\s+)?function\s+(\w+)/gm, (_, n) => names.add(n));
            const exports = [...names].map(n =>
                `if(typeof ${n}!=='undefined')window.${n}=${n};`
            ).join('\n');

            // IIFE wrapper: fresh scope + DOMContentLoaded shim
            const wrapped = `(function(){
var _orig=document.addEventListener;
var _cbs=[];
document.addEventListener=function(t,fn,o){
  if(t==='DOMContentLoaded'){_cbs.push(fn);return;}
  return _orig.call(document,t,fn,o);
};
try{
${code}
_cbs.forEach(function(fn){try{fn();}catch(e){console.error(e);}});
${exports}
}finally{document.addEventListener=_orig;}
})();`;

            const script = document.createElement('script');
            script.textContent = wrapped;
            document.body.appendChild(script);
            script.remove();
        } catch (e) {
            console.warn('Failed to load page script:', src, e);
        }
    }

    _loadCdnScript(src) {
        if (document.querySelector(`script[src="${src}"]`)) return Promise.resolve();
        return new Promise(resolve => {
            const script = document.createElement('script');
            script.src = src;
            script.onload = resolve;
            script.onerror = () => { console.warn('Failed to load CDN script:', src); resolve(); };
            document.head.appendChild(script);
        });
    }

    // ===================== Log Viewer =====================

    initLogViewer() {
        if (typeof LogViewer !== 'function') return;
        let container = document.getElementById('log-viewer-container');
        if (!container) {
            container = document.createElement('div');
            container.id = 'log-viewer-container';
            document.body.appendChild(container);
        }
        window.__monsterMQLogViewer = new LogViewer('log-viewer-container', {
            collapsed: true,
            maxLines: 1000,
            autoScroll: true,
            title: 'System Logs',
            deferConnection: false
        });
        // Update sidebar toggle icon to match saved state
        if (this._logToggleItem && window.__monsterMQLogViewer.isHidden) {
            this._logToggleItem.setAttribute('icon', 'tag-logging');
        } else if (this._logToggleItem) {
            this._logToggleItem.setAttribute('icon', 'tag-logging-filled');
        }
    }

    // ===================== Auth =====================

    logout() {
        if (window.graphqlClient) window.graphqlClient.stopTokenExpirationCheck();
        safeStorage.removeItem('monstermq_token');
        safeStorage.removeItem('monstermq_username');
        safeStorage.removeItem('monstermq_isAdmin');
        safeStorage.removeItem('monstermq_guest');
        sessionStorage.clear();
        window.location.href = '/pages/login.html';
    }
}

// ===================== Global interval/timeout tracking =====================
// Wraps setInterval/setTimeout so page navigations can clean up stale timers.
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

// ===================== Page cleanup registration =====================
window.registerPageCleanup = function(fn) {
    if (window._sidebarManager) {
        window._sidebarManager._pageCleanups.push(fn);
    }
};

// ===================== Init =====================
document.addEventListener('DOMContentLoaded', () => {
    // Skip on login page
    if (window.location.pathname.endsWith('/pages/login.html')) return;

    if (window.__SPA_MODE) {
        // SPA mode (index.html): check auth, then init full SPA with navigation
        if (!window.isLoggedIn || !window.isLoggedIn()) {
            window.location.href = '/pages/login.html';
            return;
        }
        window._sidebarManager = new SidebarManager();
    } else {
        // Standalone page mode: redirect to SPA shell so sidebar works properly.
        // The SPA shell will load this page's content via navigateTo().
        const target = window.location.pathname + window.location.search;
        window.location.replace('/?page=' + encodeURIComponent(target));
    }
});

// Helper for log viewer (backward compat)
window.onLogViewerReady = function(callback) {
    if (window.__monsterMQLogViewer) callback(window.__monsterMQLogViewer);
};
