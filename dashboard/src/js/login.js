(function() {
    class LoginManager {
        constructor() {
            this.form = document.getElementById('login-form');
            this.usernameInput = document.getElementById('username');
            this.passwordInput = document.getElementById('password');
            this.loginBtn = document.getElementById('login-btn');
            this.alertContainer = document.getElementById('alert-container');
            this.brokerSelect = document.getElementById('broker-select');
            this.guestAccess = document.getElementById('guest-access');
            this.guestLink = document.getElementById('guest-link');
            this.pendingLogin = false;

            this.bindEvents();
            this.init();
        }

        bindEvents() {
            this.form.setAttribute('novalidate', 'novalidate');
            this.form.addEventListener('submit', (event) => {
                event.preventDefault();
                event.stopPropagation();
                this.handleLogin();
            });

            this.loginBtn.addEventListener('click', (event) => {
                event.preventDefault();
                this.handleLogin();
            });

            [this.usernameInput, this.passwordInput].forEach((input) => {
                input.addEventListener('keydown', (event) => {
                    if (event.key === 'Enter') {
                        event.preventDefault();
                        event.stopPropagation();
                        this.handleLogin();
                    }
                });
            });

            this.brokerSelect.addEventListener('change', () => this.onBrokerSelectChange());

            if (this.guestLink) {
                this.guestLink.addEventListener('click', (event) => {
                    event.preventDefault();
                    this.enterGuestMode();
                });
            }
        }

        async init() {
            var loginMessage = null;
            try {
                loginMessage = sessionStorage.getItem('monstermq_login_message');
                sessionStorage.removeItem('monstermq_login_message');
            } catch (e) {
                console.warn('Failed to read login redirect reason:', e.message);
            }

            sessionStorage.clear();
            safeStorage.removeItem('monstermq_guest');

            if (loginMessage) {
                this.showAlert(loginMessage);
            }

            try {
                await window.brokerManager.ready();
                this.populateBrokerSelect();
                await this.checkUserManagementEnabled();
                if (loginMessage) {
                    this.showAlert(loginMessage);
                }
            } catch (error) {
                console.error('Login page initialization failed:', error);
                this.showBrokerStatus('error', 'Cannot initialize broker selector');
                this.showAlert('Cannot initialize login page: ' + error.message);
            }
        }

        populateBrokerSelect() {
            var brokers = window.brokerManager.getAllBrokers();
            var activeBroker = window.brokerManager.getActiveBroker();
            var activeId = activeBroker ? activeBroker.name : null;

            this.brokerSelect.innerHTML = '';

            brokers.forEach(function(broker) {
                var option = document.createElement('option');
                option.value = broker.name;
                option.textContent = broker.host ? broker.name : broker.name + ' (this server)';
                if (broker.name === activeId) option.selected = true;
                this.brokerSelect.appendChild(option);
            }.bind(this));
        }

        onBrokerSelectChange() {
            window.brokerManager.switchBroker(this.brokerSelect.value);
            this.hideGuestAccess();
            this.clearAlert();
            this.usernameInput.value = '';
            this.passwordInput.value = '';
            this.checkUserManagementEnabled();
        }

        getGraphqlEndpoint() {
            return window.brokerManager.getEndpoint();
        }

        showBrokerStatus(state, message) {
            var container = document.getElementById('broker-status');
            var dot = document.getElementById('broker-status-dot');
            var text = document.getElementById('broker-status-text');

            if (state === 'hidden') {
                container.style.display = 'none';
                return;
            }

            container.style.display = 'flex';
            dot.className = 'dot';

            if (state === 'checking') {
                text.textContent = message || 'Checking broker...';
            } else if (state === 'connected') {
                dot.classList.add('connected');
                text.textContent = message || 'Broker reachable';
            } else if (state === 'error') {
                dot.classList.add('error');
                text.textContent = message || 'Cannot reach broker';
            }
        }

        async checkUserManagementEnabled() {
            this.showBrokerStatus('checking');

            try {
                var result = await this.graphqlRequest(
                    '{ broker { userManagementEnabled anonymousEnabled } }',
                    {}
                );

                var broker = result.data?.broker || {};
                var userManagementEnabled = broker.userManagementEnabled ?? false;
                var anonymousEnabled = broker.anonymousEnabled ?? false;

                this.showBrokerStatus('connected');
                this.clearCachedAuthDisabled();

                if (!userManagementEnabled) {
                    this.showAlert('Authentication is disabled on this broker. Sign in with empty credentials to continue.', 'success', false);
                } else if (anonymousEnabled) {
                    this.showGuestAccess();
                }

                return {
                    userManagementEnabled: userManagementEnabled,
                    anonymousEnabled: anonymousEnabled
                };
            } catch (error) {
                console.error('Error checking broker authentication status:', error);
                this.showBrokerStatus('error', 'Cannot reach broker');
                this.showAlert('Cannot reach broker: ' + error.message);
                return null;
            }
        }

        async graphqlRequest(query, variables) {
            var response = await fetch(this.getGraphqlEndpoint(), {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ query: query, variables: variables || {} })
            });

            var responseText = await response.text();

            if (!response.ok) {
                throw new Error('HTTP ' + response.status + ' ' + response.statusText + this.formatResponseDetail(responseText));
            }

            var result;
            try {
                result = JSON.parse(responseText);
            } catch (error) {
                throw new Error('Invalid JSON response from GraphQL server' + this.formatResponseDetail(responseText));
            }

            if (result.errors && result.errors.length > 0) {
                throw new Error(result.errors[0].message || 'GraphQL request failed');
            }

            return result;
        }

        formatResponseDetail(responseText) {
            if (!responseText) return '';
            var trimmed = responseText.trim();
            if (trimmed.length > 500) {
                trimmed = trimmed.substring(0, 500) + '...';
            }
            return ': ' + trimmed;
        }

        clearCachedAuthDisabled() {
            if (safeStorage.getItem('monstermq_token') === 'null') {
                safeStorage.removeItem('monstermq_token');
                safeStorage.removeItem('monstermq_username');
                safeStorage.removeItem('monstermq_isAdmin');
                safeStorage.removeItem('monstermq_userManagementEnabled');
                window.brokerManager.saveAuthForBroker();
            }
        }

        showGuestAccess() {
            if (this.guestAccess) this.guestAccess.style.display = 'block';
        }

        hideGuestAccess() {
            if (this.guestAccess) this.guestAccess.style.display = 'none';
        }

        enterGuestMode() {
            safeStorage.removeItem('monstermq_token');
            safeStorage.removeItem('monstermq_username');
            safeStorage.removeItem('monstermq_isAdmin');
            safeStorage.setItem('monstermq_guest', 'true');
            safeStorage.setItem('monstermq_userManagementEnabled', 'true');
            window.brokerManager.saveAuthForBroker();
            window.location.assign('/pages/dashboard.html');
        }

        showAlert(message, type, autoDismiss) {
            type = type || 'error';
            autoDismiss = autoDismiss === undefined ? type === 'success' : autoDismiss;

            this.clearAlert();

            var alert = document.createElement('div');
            alert.className = 'alert alert-' + type;
            alert.setAttribute('role', type === 'error' ? 'alert' : 'status');
            alert.textContent = message;
            this.alertContainer.appendChild(alert);

            if (autoDismiss) {
                setTimeout(function() {
                    if (alert.parentNode === this.alertContainer) {
                        this.clearAlert();
                    }
                }.bind(this), 5000);
            }
        }

        clearAlert() {
            this.alertContainer.innerHTML = '';
        }

        setLoading(loading) {
            this.pendingLogin = loading;
            this.loginBtn.disabled = loading;
            this.loginBtn.textContent = loading ? 'Signing in...' : 'Sign In';
        }

        async handleLogin() {
            if (this.pendingLogin) return;

            var username = this.usernameInput.value.trim();
            var password = this.passwordInput.value;

            if ((!username && password) || (username && !password)) {
                this.showAlert('Enter both username and password, or leave both empty if authentication is disabled.');
                return;
            }

            this.setLoading(true);
            this.clearAlert();

            try {
                try {
                    sessionStorage.removeItem('monstermq_login_message');
                } catch (_) {}

                var graphqlResult = await this.graphqlRequest(
                    'mutation Login($username: String!, $password: String!) { login(username: $username, password: $password) { success message token username isAdmin } }',
                    { username: username || '', password: password || '' }
                );

                var result = graphqlResult.data?.login || {};

                if (!result.success) {
                    this.showAlert(result.message || 'Login failed.');
                    return;
                }

                safeStorage.removeItem('monstermq_guest');

                var token = result.token || 'null';
                safeStorage.setItem('monstermq_token', token);
                safeStorage.setItem('monstermq_username', result.token === null ? 'Anonymous' : result.username);
                safeStorage.setItem('monstermq_isAdmin', result.token === null ? 'false' : String(result.isAdmin));
                safeStorage.setItem('monstermq_userManagementEnabled', result.token === null ? 'false' : 'true');
                window.brokerManager.saveAuthForBroker();

                this.showAlert(result.token === null ? 'Authentication disabled. Opening dashboard...' : 'Login successful. Opening dashboard...', 'success');
                setTimeout(function() {
                    window.location.assign('/pages/dashboard.html');
                }, 500);
            } catch (error) {
                console.error('Login failed:', error);
                this.showAlert('Login failed: ' + error.message);
            } finally {
                this.setLoading(false);
            }
        }
    }

    function startLoginPage() {
        new LoginManager();
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', startLoginPage, { once: true });
    } else {
        startLoginPage();
    }
})();
