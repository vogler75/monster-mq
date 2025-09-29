// Shared sidebar functionality for all dashboard pages

class SidebarManager {
    constructor() {
        this.init();
    }

    init() {
        this.setupUI();
        this.restoreSidebarState();
        this.setActiveNavItem();
        this.setupEventListeners();
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