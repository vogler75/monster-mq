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
        this.renderMenu();
        this.setupUI();
        this.restoreSidebarState();
        this.setActiveNavItem();
        this.setupEventListeners();
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
            });
            html += `</div>`;
        });

        sidebarNav.innerHTML = html;
    }

    setupUI() {
        // Check authentication and show/hide admin features
        const isAdmin = localStorage.getItem('monstermq_isAdmin') === 'true';
        const usersNavLink = document.getElementById('users-nav-link');
        if (isAdmin && usersNavLink) {
            usersNavLink.style.display = 'flex';
        }

        // Set up user display
        const username = localStorage.getItem('monstermq_username');
        if (username) {
            const userNameEl = document.getElementById('user-name');
            const userAvatarEl = document.getElementById('user-avatar');
            if (userNameEl) {
                userNameEl.textContent = username;
            }
            if (userAvatarEl) {
                userAvatarEl.textContent = username.charAt(0).toUpperCase();
            }
        }

        // Set up logout functionality in sidebar footer
        const userInfo = document.querySelector('.user-info');
        if (userInfo) {
            userInfo.addEventListener('click', () => {
                this.logout();
            });
            userInfo.style.cursor = 'pointer';
            userInfo.title = 'Click to logout';
        }
    }

    restoreSidebarState() {
        const isCollapsed = localStorage.getItem('monstermq_sidebar_collapsed') === 'true';
        if (isCollapsed) {
            const sidebar = document.getElementById('sidebar');
            const mainContent = document.getElementById('main-content');
            if (sidebar && mainContent && window.innerWidth > 768) {
                sidebar.classList.add('collapsed');
                mainContent.classList.add('sidebar-collapsed');
            }
        }
    }

    setupEventListeners() {
        // Make logo clickable to expand sidebar when collapsed
        const sidebar = document.getElementById('sidebar');
        const sidebarBrand = sidebar ? sidebar.querySelector('.sidebar-brand') : null;
        if (sidebarBrand) {
            sidebarBrand.addEventListener('click', () => {
                if (sidebar && sidebar.classList.contains('collapsed')) {
                    window.toggleSidebar();
                }
            });
        }

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
        localStorage.removeItem('monstermq_token');
        localStorage.removeItem('monstermq_username');
        localStorage.removeItem('monstermq_isAdmin');
        window.location.href = '/pages/login.html';
    }
}

// Global sidebar toggle function
window.toggleSidebar = function() {
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
        localStorage.setItem('monstermq_sidebar_collapsed', isCollapsed.toString());
    }
};

// Initialize sidebar when DOM is loaded
document.addEventListener('DOMContentLoaded', () => {
    new SidebarManager();
});