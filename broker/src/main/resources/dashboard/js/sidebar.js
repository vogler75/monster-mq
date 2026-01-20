// Shared sidebar functionality for all dashboard pages
//
// HOW TO ADD A NEW MENU ITEM:
// 1. Add the menu item to the menuConfig array in the renderMenu() method below
// 2. Specify: section, href, icon (SVG path), text, and optionally id/adminOnly
// 3. The menu will automatically appear in all pages
//
// Example:
//   {
//       href: '/pages/my-new-page.html',
//       icon: '<circle cx="12" cy="12" r="10"></circle>',
//       text: 'My New Page'
//   }

class SidebarManager {
    constructor() {
        this.init();
    }

    init() {
        this.injectFavicons();
        this.renderMenu();
        this.setupUI();
        this.restoreSidebarState();
        this.restoreSidebarScroll();
        this.setActiveNavItem();
        this.setupEventListeners();
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

    renderMenu() {
        const menuConfig = [
            {
                section: 'Monitoring',
                items: [
                    {
                        href: '/pages/dashboard.html',
                        icon: '<rect x="3" y="3" width="7" height="7"></rect><rect x="14" y="3" width="7" height="7"></rect><rect x="14" y="14" width="7" height="7"></rect><rect x="3" y="14" width="7" height="7"></rect>',
                        text: 'Dashboard'
                    },
                    {
                        href: '/pages/sessions.html',
                        icon: '<path d="M16 21v-2a4 4 0 0 0-4-4H6a4 4 0 0 0-4 4v2"></path><circle cx="9" cy="7" r="4"></circle><path d="M22 21v-2a4 4 0 0 0-3-3.87"></path><path d="M16 3.13a4 4 0 0 1 0 7.75"></path>',
                        text: 'Sessions'
                    },
                    {
                        href: '/pages/topic-browser.html',
                        icon: '<circle cx="11" cy="11" r="8"></circle><path d="M21 21l-4.35-4.35"></path>',
                        text: 'Topics'
                    }
                ]
            },
            {
                section: 'Configuration',
                items: [
                    {
                        href: '/pages/archive-groups.html',
                        icon: '<polyline points="22,12 18,12 15,21 9,3 6,12 2,12"></polyline>',
                        text: 'Archives'
                    },
                    {
                        href: '/pages/jdbc-loggers.html',
                        icon: '<ellipse cx="12" cy="5" rx="9" ry="3"></ellipse><path d="M21 12c0 1.66-4 3-9 3s-9-1.34-9-3"></path><path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5"></path>',
                        text: 'Loggers'
                    },
                    {
                        href: '/pages/workflows.html',
                        icon: '<path d="M21 16V8a2 2 0 0 0-1-1.73l-7-4a2 2 0 0 0-2 0l-7 4A2 2 0 0 0 3 8v8a2 2 0 0 0 1 1.73l7 4a2 2 0 0 0 2 0l7-4A2 2 0 0 0 21 16z"></path><polyline points="7.5 4.21 12 6.81 16.5 4.21"></polyline><polyline points="7.5 19.79 7.5 14.6 3 12"></polyline><polyline points="21 12 16.5 14.6 16.5 19.79"></polyline><polyline points="3.27 6.96 12 12.01 20.73 6.96"></polyline><line x1="12" y1="22.08" x2="12" y2="12"></line>',
                        text: 'Workflows'
                    },
                    {
                        href: '/pages/device-config-export-import.html',
                        icon: '<path d="M19 12v7H5v-7H3v7c0 1.1.9 2 2 2h14c1.1 0 2-.9 2-2v-7h-2z"/><path d="M11 3L5.5 8.5l1.42 1.42L11 5.84V15h2V5.84l4.08 4.08L18.5 8.5z"/>',
                        text: 'Import/Export'
                    }
                ]
            },
            {
                section: 'Bridging',
                items: [
                    {
                        href: '/pages/opcua-devices.html',
                        icon: '<rect x="2" y="3" width="20" height="14" rx="2" ry="2"></rect><line x1="8" y1="21" x2="16" y2="21"></line><line x1="12" y1="17" x2="12" y2="21"></line>',
                        text: 'OPC UA Clients'
                    },
                    {
                        href: '/pages/opcua-servers.html',
                        icon: '<rect x="2" y="2" width="20" height="20" rx="2.18" ry="2.18"></rect><line x1="7" y1="2" x2="7" y2="22"></line><line x1="17" y1="2" x2="17" y2="22"></line><line x1="2" y1="12" x2="22" y2="12"></line><line x1="2" y1="7" x2="7" y2="7"></line><line x1="2" y1="17" x2="7" y2="17"></line><line x1="17" y1="17" x2="22" y2="17"></line><line x1="17" y1="7" x2="22" y2="7"></line>',
                        text: 'OPC UA Servers'
                    },
                    {
                        href: '/pages/mqtt-clients.html',
                        icon: '<path d="M16 18l2-2m0 0l2-2m-2 2l-2-2m2 2l2 2"/><path d="M12 5v14M5 12h14"/>',
                        text: 'MQTT Clients'
                    },
                    {
                        href: '/pages/kafka-clients.html',
                        icon: '<path d="M6 3v18" /><circle cx="6" cy="6" r="2" /><circle cx="6" cy="12" r="2" /><circle cx="6" cy="18" r="2" /><circle cx="14" cy="6" r="3" /><circle cx="14" cy="18" r="3" /><path d="M14 9v6" />',
                        text: 'Kafka Clients'
                    },
                    {
                        href: '/pages/winccoa-clients.html',
                        icon: '<rect x="4" y="4" width="16" height="16" rx="2" fill="none"></rect><text x="12" y="17" text-anchor="middle" font-size="14" font-weight="bold" fill="currentColor" font-family="Arial, sans-serif">S</text>',
                        text: 'WinCC OA Clients'
                    },
                    {
                        href: '/pages/winccua-clients.html',
                        icon: '<rect x="4" y="4" width="16" height="16" rx="2" fill="none"></rect><text x="12" y="17" text-anchor="middle" font-size="14" font-weight="bold" fill="currentColor" font-family="Arial, sans-serif">S</text>',
                        text: 'WinCC Unified Clients'
                    },
                    {
                        href: '/pages/plc4x-clients.html',
                        icon: '<rect x="2" y="2" width="20" height="20" rx="2" fill="none"></rect><circle cx="12" cy="12" r="3" fill="currentColor"></circle><line x1="12" y1="2" x2="12" y2="9"></line><line x1="12" y1="15" x2="12" y2="22"></line><line x1="2" y1="12" x2="9" y2="12"></line><line x1="15" y1="12" x2="22" y2="12"></line>',
                        text: 'PLC4X Clients'
                    },
                    {
                        href: '/pages/neo4j-clients.html',
                        icon: '<circle cx="6" cy="12" r="2" fill="currentColor"></circle><circle cx="12" cy="6" r="2" fill="currentColor"></circle><circle cx="12" cy="18" r="2" fill="currentColor"></circle><circle cx="18" cy="12" r="2" fill="currentColor"></circle><line x1="7.5" y1="10.5" x2="10.5" y2="7.5"></line><line x1="7.5" y1="13.5" x2="10.5" y2="16.5"></line><line x1="13.5" y1="7.5" x2="16.5" y2="10.5"></line><line x1="13.5" y1="16.5" x2="16.5" y2="13.5"></line>',
                        text: 'Neo4j Clients'
                    },
                    {
                        href: '/pages/sparkplugb-decoders.html',
                        icon: '<path d="M13 2L3 14h9l-1 8 10-12h-9l1-8z"></path>',
                        text: 'SparkplugB Decoders'
                    }
                ]
            },
            {
                section: 'System',
                items: [
                    {
                        href: '/pages/users.html',
                        icon: '<path d="M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2"></path><circle cx="9" cy="7" r="4"></circle><path d="M23 21v-2a4 4 0 0 0-3-3.87"></path><path d="M16 3.13a4 4 0 0 1 0 7.75"></path>',
                        text: 'Users',
                        id: 'users-nav-link',
                        adminOnly: true
                    },
                    {
                        isUserItem: true
                    }
                ]
            }
        ];

        const sidebarNav = document.getElementById('sidebar-nav');
        if (!sidebarNav) return;

        let html = '';
        menuConfig.forEach(section => {
            html += `<div class="nav-section">`;
            html += `<div class="nav-section-title">${section.section}</div>`;
            section.items.forEach(item => {
                if (item.isUserItem) {
                    html += `
                        <div class="nav-item user-menu-item" id="user-menu-item" title="Logout">
                            <svg class="nav-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                                <path d="M9 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h4"></path>
                                <polyline points="16 17 21 12 16 7"></polyline>
                                <line x1="21" y1="12" x2="9" y2="12"></line>
                            </svg>
                            <span class="nav-text">Logout</span>
                        </div>
                    `;
                } else {
                    const style = item.adminOnly ? ' style="display: none;"' : '';
                    const id = item.id ? ` id="${item.id}"` : '';
                    html += `
                        <a href="${item.href}" class="nav-item"${id}${style}>
                            <svg class="nav-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                                ${item.icon}
                            </svg>
                            <span class="nav-text">${item.text}</span>
                        </a>
                    `;
                }
            });
            html += `</div>`;
        });

        sidebarNav.innerHTML = html;
    }

    setupUI() {
        // Check authentication and show/hide admin features
        const isAdmin = safeStorage.getItem('monstermq_isAdmin') === 'true';
        const userManagementEnabled = safeStorage.getItem('monstermq_userManagementEnabled') === 'true';

        // Show Users menu for admins (regardless of user management enabled status)
        const usersNavLink = document.getElementById('users-nav-link');
        if (isAdmin && usersNavLink) {
            usersNavLink.style.display = 'flex';
        }

        // Set up logout functionality on user menu item
        const userMenuItem = document.getElementById('user-menu-item');
        if (userMenuItem) {
            if (userManagementEnabled) {
                // Show logout only if user management is enabled
                userMenuItem.style.display = 'flex';
                userMenuItem.addEventListener('click', () => {
                    this.logout();
                });
            } else {
                // Hide logout if user management is disabled
                userMenuItem.style.display = 'none';
            }
        }
    }

    restoreSidebarState() {
        const isCollapsed = safeStorage.getItem('monstermq_sidebar_collapsed') === 'true';
        if (isCollapsed) {
            const sidebar = document.getElementById('sidebar');
            const mainContent = document.getElementById('main-content');
            if (sidebar && mainContent && window.innerWidth > 768) {
                sidebar.classList.add('collapsed');
                mainContent.classList.add('sidebar-collapsed');
            }
        }
    }

    restoreSidebarScroll() {
        const savedScroll = safeStorage.getItem('monstermq_sidebar_scroll');
        if (savedScroll) {
            const sidebarNav = document.getElementById('sidebar-nav');
            if (sidebarNav) {
                // Use requestAnimationFrame to ensure DOM is fully rendered
                requestAnimationFrame(() => {
                    sidebarNav.scrollTop = parseInt(savedScroll, 10);
                });
            }
        }
    }

    setupEventListeners() {
        // Make logo clickable to expand sidebar when collapsed
        const sidebar = document.getElementById('sidebar');
        const sidebarBrand = sidebar ? sidebar.querySelector('.sidebar-brand') : null;
        if (sidebarBrand) {
            sidebarBrand.addEventListener('click', (e) => {
                // Only toggle if clicking directly on the brand, not on nav items
                if (e.target === sidebarBrand || e.target.closest('.sidebar-brand')) {
                    if (sidebar && sidebar.classList.contains('collapsed')) {
                        window.toggleSidebar();
                    }
                }
            });
        }

        // Save sidebar scroll position before navigating (exclude user menu item)
        const navItems = document.querySelectorAll('.nav-item:not(.user-menu-item)');
        navItems.forEach(item => {
            item.addEventListener('click', (e) => {
                // Stop propagation to prevent brand click handler from toggling
                e.stopPropagation();
                const sidebarNav = document.getElementById('sidebar-nav');
                if (sidebarNav) {
                    safeStorage.setItem('monstermq_sidebar_scroll', sidebarNav.scrollTop.toString());
                }
            });
        });

        // Handle responsive behavior on window resize
        window.addEventListener('resize', () => {
            const sidebar = document.getElementById('sidebar');
            const mainContent = document.getElementById('main-content');

            if (window.innerWidth > 768) {
                sidebar.classList.remove('open');
                // If it was collapsed on desktop, keep it collapsed
                if (sidebar.classList.contains('collapsed')) {
                    mainContent.classList.add('sidebar-collapsed');
                } else {
                    mainContent.classList.remove('sidebar-collapsed');
                }
            } else {
                sidebar.classList.remove('collapsed');
                mainContent.classList.remove('sidebar-collapsed');
            }
        });
    }

    setActiveNavItem() {
        const currentPath = window.location.pathname;
        const navItems = document.querySelectorAll('.nav-item');

        navItems.forEach(item => {
            item.classList.remove('active');
            if (item.getAttribute('href') === currentPath) {
                item.classList.add('active');
            }
        });
    }

    logout() {
        // Stop token expiration check
        if (window.graphqlClient) {
            window.graphqlClient.stopTokenExpirationCheck();
        }

        // Clear all auth data
        safeStorage.removeItem('monstermq_token');
        safeStorage.removeItem('monstermq_username');
        safeStorage.removeItem('monstermq_isAdmin');
        sessionStorage.clear();

        window.location.href = '/pages/login.html';
    }
}

// Global sidebar toggle function
window.toggleSidebar = function () {
    const sidebar = document.getElementById('sidebar');
    const mainContent = document.getElementById('main-content');

    if (window.innerWidth <= 768) {
        // Mobile behavior - show/hide sidebar
        sidebar.classList.toggle('open');
    } else {
        // Desktop behavior - collapse/expand sidebar
        sidebar.classList.toggle('collapsed');
        mainContent.classList.toggle('sidebar-collapsed');

        // Save the collapsed state to localStorage
        const isCollapsed = sidebar.classList.contains('collapsed');
        safeStorage.setItem('monstermq_sidebar_collapsed', isCollapsed.toString());
    }

    // Dispatch event so log viewer can update its margin
    window.dispatchEvent(new CustomEvent('sidebarToggled'));
};

// Initialize sidebar when DOM is loaded
document.addEventListener('DOMContentLoaded', () => {
    new SidebarManager();

    // Inject global Log Viewer on all authenticated pages (exclude login page)
    if (!window.location.pathname.endsWith('/pages/login.html')) {
        // Ensure container exists
        let lvContainer = document.getElementById('log-viewer-container');
        if (!lvContainer) {
            lvContainer = document.createElement('div');
            lvContainer.id = 'log-viewer-container';
            document.body.appendChild(lvContainer);
        }
        // Avoid duplicate initialization
        if (!window.__monsterMQLogViewer) {
            if (typeof LogViewer === 'function') {
                // Allow per-page default filters via body data attributes
                // e.g., <body data-log-source-class="at.rocworks.flowengine" data-log-title="Flow Engine Logs">
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
                    deferConnection: true // Don't connect yet, let callbacks set filters first
                });
                // Expose helper for dynamic filter updates from pages
                window.updateLogViewerFilters = (newFilters) => {
                    if (window.__monsterMQLogViewer) {
                        window.__monsterMQLogViewer.setFilters(newFilters);
                    }
                };
                // Trigger ready callbacks if any
                if (window.__monsterMQLogViewerReadyCallbacks) {
                    window.__monsterMQLogViewerReadyCallbacks.forEach(cb => cb(window.__monsterMQLogViewer));
                    window.__monsterMQLogViewerReadyCallbacks = [];
                }
                // Now connect after callbacks have had a chance to set filters
                if (!window.__monsterMQLogViewer.isConnected && !window.__monsterMQLogViewer.ws) {
                    window.__monsterMQLogViewer.connect();
                }
            }
            // LogViewer is optional - pages can load without it
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