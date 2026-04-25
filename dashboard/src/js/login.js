class LoginManager {
    constructor() {
        this.form = document.getElementById('login-form');
        this.usernameInput = document.getElementById('username');
        this.passwordInput = document.getElementById('password');
        this.loginBtn = document.getElementById('login-btn');
        this.loginText = document.getElementById('login-text');
        this.loginSpinner = document.getElementById('login-spinner');
        this.alertContainer = document.getElementById('alert-container');
        this.brokerSelect = document.getElementById('broker-select');

        this.init();
    }

    async init() {
        sessionStorage.clear();
        safeStorage.removeItem('monstermq_guest');

        await window.brokerManager.ready();
        this.populateBrokerSelect();

        var authStatus = await this.checkUserManagementEnabled();

        if (authStatus?.userManagementEnabled && this.isLoggedIn()) {
            window.location.href = '/pages/dashboard.html';
            return;
        }

        this.form.addEventListener('submit', (e) => this.handleLogin(e));
        this.brokerSelect.addEventListener('change', () => this.onBrokerSelectChange());

        var guestLink = document.getElementById('guest-link');
        if (guestLink) {
            guestLink.addEventListener('click', (e) => {
                e.preventDefault();
                this.enterGuestMode();
            });
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
            var label = broker.name;
            if (!broker.host) label += ' (this server)';
            option.textContent = label;
            if (broker.name === activeId) option.selected = true;
            this.brokerSelect.appendChild(option);
        }.bind(this));
    }

    onBrokerSelectChange() {
        window.brokerManager.setActiveBroker(this.brokerSelect.value);
        // Hide guest access until re-checked
        var guestAccess = document.getElementById('guest-access');
        if (guestAccess) guestAccess.style.display = 'none';
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
            text.textContent = 'Checking broker...';
        } else if (state === 'connected') {
            dot.classList.add('connected');
            text.textContent = message || 'Broker reachable';
        } else if (state === 'error') {
            dot.classList.add('error');
            text.textContent = message || 'Cannot reach broker';
        }
    }

    async checkUserManagementEnabled() {
        var endpoint = this.getGraphqlEndpoint();
        this.showBrokerStatus('checking');

        try {
            var response = await fetch(endpoint, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    query: '{ broker { userManagementEnabled anonymousEnabled } }'
                })
            });

            var result = await response.json();
            var userManagementEnabled = result.data?.broker?.userManagementEnabled ?? false;
            var anonymousEnabled = result.data?.broker?.anonymousEnabled ?? false;

            this.showBrokerStatus('connected');

            if (!userManagementEnabled) {
                this.clearCachedAuthDisabled();
                this.showAlert('Authentication is disabled on this broker. Submit with empty credentials to continue.', 'success');
            } else if (anonymousEnabled) {
                this.clearCachedAuthDisabled();
                var guestAccess = document.getElementById('guest-access');
                if (guestAccess) guestAccess.style.display = 'block';
            } else {
                this.clearCachedAuthDisabled();
            }

            return { userManagementEnabled: userManagementEnabled, anonymousEnabled: anonymousEnabled };
        } catch (error) {
            console.error('Error checking user management status:', error);
            this.showBrokerStatus('error', 'Cannot reach broker');
            return null;
        }
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

    enterGuestMode() {
        safeStorage.removeItem('monstermq_token');
        safeStorage.removeItem('monstermq_username');
        safeStorage.removeItem('monstermq_isAdmin');
        safeStorage.setItem('monstermq_guest', 'true');
        safeStorage.setItem('monstermq_userManagementEnabled', 'true');
        window.brokerManager.saveAuthForBroker();
        window.location.href = '/pages/dashboard.html';
    }

    isLoggedIn() {
        var token = safeStorage.getItem('monstermq_token');
        if (!token) return false;
        if (token === 'null') return true;
        try {
            var decoded = JSON.parse(atob(token.split('.')[1]));
            return decoded.exp > Date.now() / 1000;
        } catch {
            return false;
        }
    }

    showAlert(message, type) {
        type = type || 'error';
        this.alertContainer.innerHTML = '<div class="alert alert-' + type + '">' + message + '</div>';
        setTimeout(function() {
            this.alertContainer.innerHTML = '';
        }.bind(this), 5000);
    }

    setLoading(loading) {
        if (loading) {
            this.loginBtn.setAttribute('loading', '');
            this.loginBtn.setAttribute('disabled', '');
        } else {
            this.loginBtn.removeAttribute('loading');
            this.loginBtn.removeAttribute('disabled');
        }
    }

    async handleLogin(e) {
        e.preventDefault();

        var username = this.usernameInput.value.trim();
        var password = this.passwordInput.value;

        if ((!username && password) || (username && !password)) {
            this.showAlert('Please enter both username and password, or leave both empty if authentication is disabled');
            return;
        }

        this.setLoading(true);
        this.alertContainer.innerHTML = '';

        var graphqlEndpoint = this.getGraphqlEndpoint();

        try {
            var response = await fetch(graphqlEndpoint, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    query: 'mutation Login($username: String!, $password: String!) { login(username: $username, password: $password) { success message token username isAdmin } }',
                    variables: { username: username || '', password: password || '' }
                })
            });

            var graphqlResult = await response.json();

            if (graphqlResult.errors) {
                throw new Error(graphqlResult.errors[0].message);
            }

            var result = graphqlResult.data?.login || {};

            if (result.success) {
                safeStorage.removeItem('monstermq_guest');

                var token = result.token || 'null';
                safeStorage.setItem('monstermq_token', token);
                safeStorage.setItem('monstermq_username', result.token === null ? 'Anonymous' : result.username);
                safeStorage.setItem('monstermq_isAdmin', result.token === null ? 'false' : result.isAdmin);
                safeStorage.setItem('monstermq_userManagementEnabled', result.token === null ? 'false' : 'true');
                window.brokerManager.saveAuthForBroker();

                if (result.token === null) {
                    this.showAlert('Authentication disabled - accessing dashboard...', 'success');
                } else {
                    this.showAlert('Login successful! Redirecting...', 'success');
                }

                setTimeout(function() {
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

document.addEventListener('DOMContentLoaded', function() {
    new LoginManager();
});
