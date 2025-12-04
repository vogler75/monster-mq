class LoginManager {
    constructor() {
        this.form = document.getElementById('login-form');
        this.usernameInput = document.getElementById('username');
        this.passwordInput = document.getElementById('password');
        this.loginBtn = document.getElementById('login-btn');
        this.loginText = document.getElementById('login-text');
        this.loginSpinner = document.getElementById('login-spinner');
        this.alertContainer = document.getElementById('alert-container');

        this.init();
    }

    init() {
        if (this.isLoggedIn()) {
            window.location.href = '/pages/dashboard.html';
            return;
        }

        // Check if user management is enabled
        this.checkUserManagementEnabled();

        this.form.addEventListener('submit', (e) => this.handleLogin(e));
    }

    async checkUserManagementEnabled() {
        try {
            const response = await fetch('/graphql', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({
                    query: `
                        query GetBroker {
                            broker {
                                userManagementEnabled
                            }
                        }
                    `
                })
            });

            const result = await response.json();
            const userManagementEnabled = result.data?.broker?.userManagementEnabled ?? false;

            if (!userManagementEnabled) {
                // User management is disabled, auto-login with empty credentials
                console.log('User management is disabled, auto-logging in...');
                this.autoLoginDisabled();
            }
        } catch (error) {
            console.error('Error checking user management status:', error);
            // Continue with normal login flow if there's an error
        }
    }

    autoLoginDisabled() {
        // Auto-login when user management is disabled
        safeStorage.setItem('monstermq_token', 'null');
        safeStorage.setItem('monstermq_username', 'Anonymous');
        safeStorage.setItem('monstermq_isAdmin', 'false');
        safeStorage.setItem('monstermq_userManagementEnabled', 'false');

        // Show success message and redirect
        this.showAlert('Authentication disabled - accessing dashboard...', 'success');

        setTimeout(() => {
            window.location.href = '/pages/dashboard.html';
        }, 500);
    }

    isLoggedIn() {
        const token = safeStorage.getItem('monstermq_token');
        if (!token) return false;

        // If token is 'null', authentication is disabled
        if (token === 'null') return true;

        try {
            const decoded = JSON.parse(atob(token.split('.')[1]));
            const now = Date.now() / 1000;
            return decoded.exp > now;
        } catch {
            return false;
        }
    }

    showAlert(message, type = 'error') {
        this.alertContainer.innerHTML = `
            <div class="alert alert-${type}">
                ${message}
            </div>
        `;

        setTimeout(() => {
            this.alertContainer.innerHTML = '';
        }, 5000);
    }

    setLoading(loading) {
        this.loginBtn.disabled = loading;

        if (loading) {
            this.loginText.style.display = 'none';
            this.loginSpinner.style.display = 'inline-block';
        } else {
            this.loginText.style.display = 'inline';
            this.loginSpinner.style.display = 'none';
        }
    }

    async handleLogin(e) {
        e.preventDefault();

        const username = this.usernameInput.value.trim();
        const password = this.passwordInput.value;

        console.log('Login attempt:', { username: username || '(empty)', password: password ? '***' : '(empty)' });

        // Allow empty credentials to test if authentication is disabled
        // Also allow if both are provided
        // Only reject if one is empty and the other is not
        if ((!username && password) || (username && !password)) {
            console.log('Validation failed: partial credentials');
            this.showAlert('Please enter both username and password, or leave both empty if authentication is disabled');
            return;
        }

        console.log('Validation passed, proceeding with login...');

        this.setLoading(true);
        this.alertContainer.innerHTML = '';

        try {
            // Use GraphQL directly for login
            const graphqlEndpoint = '/graphql';
            const response = await fetch(graphqlEndpoint, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({
                    query: `
                        mutation Login($username: String!, $password: String!) {
                            login(username: $username, password: $password) {
                                success
                                message
                                token
                                username
                                isAdmin
                            }
                        }
                    `,
                    variables: {
                        username: username || '',
                        password: password || ''
                    }
                })
            });

            const graphqlResult = await response.json();

            if (graphqlResult.errors) {
                throw new Error(graphqlResult.errors[0].message);
            }

            const result = graphqlResult.data?.login || {};

            if (result.success) {
                // Handle case where authentication is disabled (token is null)
                const token = result.token || 'null';
                safeStorage.setItem('monstermq_token', token);
                safeStorage.setItem('monstermq_username', result.username);
                safeStorage.setItem('monstermq_isAdmin', result.isAdmin);
                safeStorage.setItem('monstermq_userManagementEnabled', 'true');

                if (result.token === null) {
                    this.showAlert('Authentication disabled - accessing dashboard...', 'success');
                } else {
                    this.showAlert('Login successful! Redirecting...', 'success');
                }

                setTimeout(() => {
                    window.location.href = '/pages/dashboard.html';
                }, 1000);
            } else {
                this.showAlert(result.message || 'Login failed');
            }
        } catch (error) {
            console.error('Login error:', error);
            this.showAlert('Network error. Please check if the MonsterMQ broker is running.');
        } finally {
            this.setLoading(false);
        }
    }
}

document.addEventListener('DOMContentLoaded', () => {
    new LoginManager();
});