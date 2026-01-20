class GraphQLDashboardClient {
    constructor(endpoint = '/graphql') {
        this.endpoint = endpoint;
        this.tokenCheckInterval = null;
        this.WARNING_THRESHOLD_SECONDS = 5 * 60; // 5 minutes
        this.CHECK_INTERVAL_MS = 30 * 1000; // 30 seconds
    }

    // ===================== SESSION EXPIRY HANDLING =====================

    /**
     * Start periodic token expiration check
     */
    startTokenExpirationCheck() {
        // Don't start if already running
        if (this.tokenCheckInterval) {
            return;
        }

        // Skip if on login page
        if (window.location.pathname.includes('login')) {
            return;
        }

        // Initial check after 1 second delay
        setTimeout(() => {
            this.checkTokenExpiration();
        }, 1000);

        // Periodic check every 30 seconds
        this.tokenCheckInterval = setInterval(() => {
            this.checkTokenExpiration();
        }, this.CHECK_INTERVAL_MS);

        console.log('Token expiration check started');
    }

    /**
     * Stop the token expiration check interval
     */
    stopTokenExpirationCheck() {
        if (this.tokenCheckInterval) {
            clearInterval(this.tokenCheckInterval);
            this.tokenCheckInterval = null;
            console.log('Token expiration check stopped');
        }
    }

    /**
     * Check if the token is expired or about to expire
     */
    checkTokenExpiration() {
        const token = safeStorage.getItem('monstermq_token');

        // Skip if no token or auth is disabled
        if (!token || token === 'null') {
            return;
        }

        try {
            // Decode JWT payload
            const parts = token.split('.');
            if (parts.length !== 3) {
                return;
            }

            const payload = JSON.parse(atob(parts[1]));
            const expiryTime = payload.exp; // Unix timestamp in seconds
            const currentTime = Math.floor(Date.now() / 1000);
            const remainingSeconds = expiryTime - currentTime;

            console.log(`Token expiry check: ${remainingSeconds} seconds remaining`);

            if (remainingSeconds <= 0) {
                // Token has expired
                this.handleTokenExpiration();
            } else if (remainingSeconds <= this.WARNING_THRESHOLD_SECONDS) {
                // Token is about to expire - show warning (only once)
                this.showExpirationWarning(remainingSeconds);
            }
        } catch (e) {
            console.warn('Failed to check token expiration:', e.message);
        }
    }

    /**
     * Show a warning notification before session expires
     */
    showExpirationWarning(remainingSeconds) {
        // Only show warning once per session
        if (sessionStorage.getItem('monstermq_expiry_warning_shown')) {
            return;
        }
        sessionStorage.setItem('monstermq_expiry_warning_shown', 'true');

        const minutes = Math.floor(remainingSeconds / 60);
        const seconds = remainingSeconds % 60;
        const timeStr = minutes > 0 ? `${minutes} minute${minutes > 1 ? 's' : ''}` : `${seconds} seconds`;

        // Create toast notification
        const toast = document.createElement('div');
        toast.id = 'session-expiry-warning';
        toast.style.cssText = `
            position: fixed;
            top: 20px;
            right: 20px;
            background: #ff9800;
            color: white;
            padding: 16px 24px;
            border-radius: 8px;
            box-shadow: 0 4px 12px rgba(0,0,0,0.3);
            z-index: 10000;
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            font-size: 14px;
            max-width: 350px;
            animation: slideIn 0.3s ease-out;
        `;
        toast.innerHTML = `
            <div style="display: flex; align-items: center; gap: 12px;">
                <span style="font-size: 24px;">⚠️</span>
                <div>
                    <div style="font-weight: 600; margin-bottom: 4px;">Session Expiring Soon</div>
                    <div>Your session will expire in ${timeStr}. Please save your work.</div>
                </div>
            </div>
        `;

        // Add animation styles
        const style = document.createElement('style');
        style.textContent = `
            @keyframes slideIn {
                from { transform: translateX(100%); opacity: 0; }
                to { transform: translateX(0); opacity: 1; }
            }
            @keyframes slideOut {
                from { transform: translateX(0); opacity: 1; }
                to { transform: translateX(100%); opacity: 0; }
            }
        `;
        document.head.appendChild(style);
        document.body.appendChild(toast);

        // Auto-dismiss after 10 seconds
        setTimeout(() => {
            toast.style.animation = 'slideOut 0.3s ease-in forwards';
            setTimeout(() => toast.remove(), 300);
        }, 10000);
    }

    /**
     * Handle expired token - show modal and redirect to login
     */
    handleTokenExpiration() {
        // Stop checking
        this.stopTokenExpirationCheck();

        // Clear all auth data
        safeStorage.removeItem('monstermq_token');
        safeStorage.removeItem('monstermq_username');
        safeStorage.removeItem('monstermq_isAdmin');
        sessionStorage.clear();

        // Create modal overlay
        const modal = document.createElement('div');
        modal.id = 'session-expired-modal';
        modal.style.cssText = `
            position: fixed;
            top: 0;
            left: 0;
            width: 100%;
            height: 100%;
            background: rgba(0, 0, 0, 0.7);
            display: flex;
            align-items: center;
            justify-content: center;
            z-index: 10001;
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
        `;

        let countdown = 5;
        modal.innerHTML = `
            <div style="background: white; padding: 32px 48px; border-radius: 12px; text-align: center; max-width: 400px; box-shadow: 0 8px 32px rgba(0,0,0,0.3);">
                <div style="font-size: 48px; margin-bottom: 16px;">🔒</div>
                <h2 style="margin: 0 0 12px 0; color: #333; font-size: 24px;">Session Expired</h2>
                <p style="color: #666; margin: 0 0 24px 0; font-size: 14px;">Your authentication session has expired. Please log in again to continue.</p>
                <button id="session-expired-login-btn" style="
                    background: #1976d2;
                    color: white;
                    border: none;
                    padding: 12px 32px;
                    border-radius: 6px;
                    font-size: 16px;
                    cursor: pointer;
                    transition: background 0.2s;
                ">Return to Login</button>
                <p id="session-expired-countdown" style="color: #999; margin: 16px 0 0 0; font-size: 12px;">Redirecting in ${countdown} seconds...</p>
            </div>
        `;

        document.body.appendChild(modal);

        // Button click handler
        document.getElementById('session-expired-login-btn').addEventListener('click', () => {
            window.location.href = '/pages/login.html';
        });

        // Auto-redirect countdown
        const countdownEl = document.getElementById('session-expired-countdown');
        const countdownInterval = setInterval(() => {
            countdown--;
            if (countdown <= 0) {
                clearInterval(countdownInterval);
                window.location.href = '/pages/login.html';
            } else {
                countdownEl.textContent = `Redirecting in ${countdown} seconds...`;
            }
        }, 1000);
    }

    // ===================== GRAPHQL QUERY =====================

    async query(query, variables = {}) {
        const token = safeStorage.getItem('monstermq_token');
        const headers = {
            'Content-Type': 'application/json',
        };

        // Only add Authorization header if token exists and is not 'null' string
        if (token && token !== 'null') {
            headers.Authorization = `Bearer ${token}`;
        }

        // Extract query/mutation name for better debugging
        const queryMatch = query.match(/(?:query|mutation|subscription)\s+(\w+)/);
        const queryName = queryMatch ? queryMatch[1] : query.substring(0, 50) + '...';

        // Clean up query for display (remove extra whitespace)
        const cleanQuery = query.trim().replace(/\s+/g, ' ').replace(/\n/g, ' ');

        // Debug logging
        console.log('=== GraphQL Request ===');
        console.log('Endpoint:', this.endpoint);
        console.log('Operation:', queryName);
        console.log('Query:', cleanQuery);
        console.log('Variables:', JSON.stringify(variables));
        console.log('Headers:', {
            'Content-Type': headers['Content-Type'],
            'Authorization': headers.Authorization ? '[Token Present]' : '[No Token]'
        });
        console.log('Token Status:', token === 'null' ? 'null (auth disabled)' : token ? `JWT token present (${token.substring(0, 20)}...)` : 'no token');

        const requestBody = JSON.stringify({ query, variables });
        console.log('Request Body:', requestBody);

        try {
            const response = await fetch(this.endpoint, {
                method: 'POST',
                headers,
                body: requestBody
            });

            console.log('=== GraphQL Response ===');
            console.log('Status:', response.status, response.statusText);
            console.log('Headers:', Object.fromEntries(response.headers.entries()));

            // Handle authentication errors (401/403)
            if (response.status === 401 || response.status === 403) {
                console.error('=== Authentication Error ===');
                console.error('Status:', response.status, response.statusText);
                console.error('============================');
                this.handleTokenExpiration();
                throw new Error(`Authentication failed: ${response.status} ${response.statusText}`);
            }

            if (!response.ok) {
                const errorText = await response.text();
                console.error('=== GraphQL HTTP Error ===');
                console.error('Status:', response.status, response.statusText);
                console.error('Response Body:', errorText);
                console.error('========================');
                throw new Error(`HTTP ${response.status}: ${response.statusText} - ${errorText}`);
            }

            const responseText = await response.text();
            console.log('Raw Response:', responseText);

            let result;
            try {
                result = JSON.parse(responseText);
            } catch (parseError) {
                console.error('Failed to parse GraphQL response as JSON:', parseError);
                console.error('Response was:', responseText);
                throw new Error('Invalid JSON response from GraphQL server');
            }

            console.log('Parsed Response:', result);

            if (result.errors) {
                console.error('=== GraphQL Query Errors ===');
                result.errors.forEach((error, index) => {
                    console.error(`Error ${index + 1}:`, error);
                });
                console.error('===========================');

                // Check for authentication-related errors in GraphQL response
                const errorMessage = result.errors[0].message.toLowerCase();
                if (errorMessage.includes('unauthorized') ||
                    errorMessage.includes('authentication') ||
                    errorMessage.includes('token expired') ||
                    errorMessage.includes('not authenticated') ||
                    errorMessage.includes('invalid token')) {
                    this.handleTokenExpiration();
                }

                throw new Error(result.errors[0].message);
            }

            console.log('=== GraphQL Success ===');
            console.log('Data:', result.data);
            console.log('=======================');

            return result.data;
        } catch (error) {
            console.error('=== GraphQL Exception ===');
            console.error('Error:', error.message);
            console.error('Stack:', error.stack);
            console.error('========================');
            throw error;
        }
    }

    async getBrokers() {
        const query = `
            query GetBrokers {
                brokers {
                    nodeId
                    version
                    userManagementEnabled
                    metrics {
                        messagesIn
                        messagesOut
                        nodeSessionCount
                        clusterSessionCount
                        queuedMessagesCount
                        subscriptionCount
                        clientNodeMappingSize
                        topicNodeMappingSize
                        messageBusIn
                        messageBusOut
                        mqttClientIn
                        mqttClientOut
                        kafkaClientIn
                        kafkaClientOut
                        opcUaClientIn
                        opcUaClientOut
                        winCCOaClientIn
                        winCCUaClientIn
                        timestamp
                    }
                }
            }
        `;

        const result = await this.query(query);
        return result.brokers;
    }

    async isUserManagementEnabled() {
        const query = `
            query GetBroker {
                broker {
                    userManagementEnabled
                }
            }
        `;

        try {
            const result = await this.query(query);
            return result.broker?.userManagementEnabled ?? false;
        } catch (error) {
            console.warn('Failed to check user management status:', error);
            // Default to false if we can't reach the broker
            return false;
        }
    }

    async getBrokersWithHistory(lastMinutes = null, from = null, to = null) {
        const query = `
            query GetBrokersWithHistory($lastMinutes: Int, $from: String, $to: String) {
                brokers {
                    nodeId
                    version
                    metrics {
                        messagesIn
                        messagesOut
                        nodeSessionCount
                        clusterSessionCount
                        queuedMessagesCount
                        subscriptionCount
                        clientNodeMappingSize
                        topicNodeMappingSize
                        messageBusIn
                        messageBusOut
                        mqttClientIn
                        mqttClientOut
                        kafkaClientIn
                        kafkaClientOut
                        opcUaClientIn
                        opcUaClientOut
                        winCCOaClientIn
                        winCCUaClientIn
                        timestamp
                    }
                    metricsHistory(lastMinutes: $lastMinutes, from: $from, to: $to) {
                        messagesIn
                        messagesOut
                        nodeSessionCount
                        clusterSessionCount
                        queuedMessagesCount
                        subscriptionCount
                        clientNodeMappingSize
                        topicNodeMappingSize
                        messageBusIn
                        messageBusOut
                        mqttClientIn
                        mqttClientOut
                        kafkaClientIn
                        kafkaClientOut
                        opcUaClientIn
                        opcUaClientOut
                        winCCOaClientIn
                        winCCUaClientIn
                        timestamp
                    }
                }
            }
        `;

        const result = await this.query(query, { lastMinutes, from, to });
        return result.brokers;
    }

    async getSessions(nodeId = null, connected = null) {
        const query = `
            query GetSessions($nodeId: String, $connected: Boolean) {
                sessions(nodeId: $nodeId, connected: $connected) {
                    clientId
                    nodeId
                    metrics {
                        messagesIn
                        messagesOut
                        timestamp
                    }
                    cleanSession
                    clientAddress
                    connected
                    queuedMessageCount
                }
            }
        `;

        const result = await this.query(query, { nodeId, connected });
        return result.sessions;
    }

    async getSession(clientId, nodeId = null) {
        const query = `
            query GetSession($clientId: String!, $nodeId: String) {
                session(clientId: $clientId, nodeId: $nodeId) {
                    clientId
                    nodeId
                    metrics {
                        messagesIn
                        messagesOut
                        timestamp
                    }
                    subscriptions {
                        topicFilter
                        qos
                    }
                    cleanSession
                    sessionExpiryInterval
                    clientAddress
                    connected
                    queuedMessageCount
                    information
                }
            }
        `;

        const result = await this.query(query, { clientId, nodeId });
        return result.session;
    }

    async getUsers() {
        const query = `
            query GetUsers {
                users {
                    username
                    enabled
                    canSubscribe
                    canPublish
                    isAdmin
                    createdAt
                    updatedAt
                    aclRules {
                        id
                        topicPattern
                        canSubscribe
                        canPublish
                        priority
                        createdAt
                    }
                }
            }
        `;

        const result = await this.query(query);
        return result.users;
    }

    async createUser(userData) {
        const mutation = `
            mutation CreateUser($input: CreateUserInput!) {
                user {
                    createUser(input: $input) {
                        success
                        message
                        user {
                            username
                            enabled
                            canSubscribe
                            canPublish
                            isAdmin
                            createdAt
                        }
                    }
                }
            }
        `;

        const result = await this.query(mutation, { input: userData });
        return result.user.createUser;
    }

    async updateUser(userData) {
        const mutation = `
            mutation UpdateUser($input: UpdateUserInput!) {
                user {
                    updateUser(input: $input) {
                        success
                        message
                        user {
                            username
                            enabled
                            canSubscribe
                            canPublish
                            isAdmin
                            updatedAt
                        }
                    }
                }
            }
        `;

        const result = await this.query(mutation, { input: userData });
        return result.user.updateUser;
    }

    async deleteUser(username) {
        const mutation = `
            mutation DeleteUser($username: String!) {
                user {
                    deleteUser(username: $username) {
                        success
                        message
                    }
                }
            }
        `;

        const result = await this.query(mutation, { username });
        return result.user.deleteUser;
    }

    async setPassword(input) {
        const mutation = `
            mutation SetPassword($input: SetPasswordInput!) {
                user {
                    setPassword(input: $input) {
                        success
                        message
                    }
                }
            }
        `;

        const result = await this.query(mutation, { input });
        return result.user.setPassword;
    }

    // ===================== ACL RULES =====================
    async createAclRule(input) {
        const mutation = `
            mutation CreateAclRule($input: CreateAclRuleInput!) {
                user {
                    createAclRule(input: $input) {
                        success
                        message
                        aclRule {
                            id
                            username
                            topicPattern
                            canSubscribe
                            canPublish
                            priority
                            createdAt
                        }
                        user {
                            username
                            aclRules {
                                id
                                topicPattern
                                canSubscribe
                                canPublish
                                priority
                                createdAt
                            }
                        }
                    }
                }
            }
        `;
        const result = await this.query(mutation, { input });
        return result.user.createAclRule;
    }

    async updateAclRule(input) {
        const mutation = `
            mutation UpdateAclRule($input: UpdateAclRuleInput!) {
                user {
                    updateAclRule(input: $input) {
                        success
                        message
                        aclRule {
                            id
                            username
                            topicPattern
                            canSubscribe
                            canPublish
                            priority
                            createdAt
                        }
                    }
                }
            }
        `;
        const result = await this.query(mutation, { input });
        return result.user.updateAclRule;
    }

    async deleteAclRule(id) {
        const mutation = `
            mutation DeleteAclRule($id: String!) {
                user {
                    deleteAclRule(id: $id) {
                        success
                        message
                        aclRule { id }
                    }
                }
            }
        `;
        const result = await this.query(mutation, { id });
        return result.user.deleteAclRule;
    }
}

// Global instance
window.graphqlClient = new GraphQLDashboardClient();

// Debug function to check localStorage
window.debugAuth = function () {
    const token = localStorage.getItem('monstermq_token');
    const username = localStorage.getItem('monstermq_username');
    const isAdmin = localStorage.getItem('monstermq_isAdmin');

    console.log('=== Authentication Debug ===');
    console.log('Token:', token);
    console.log('Token type:', typeof token);
    console.log('Token === "null":', token === 'null');
    console.log('Token === null:', token === null);
    console.log('Username:', username);
    console.log('Is Admin:', isAdmin);
    console.log('==========================');

    if (token && token !== 'null') {
        try {
            // Try to decode JWT
            const parts = token.split('.');
            if (parts.length === 3) {
                const decoded = JSON.parse(atob(parts[1]));
                console.log('JWT Decoded:', decoded);
                const now = Date.now() / 1000;
                console.log('Token expired:', decoded.exp < now);
            }
        } catch (e) {
            console.log('Failed to decode token as JWT:', e.message);
        }
    }
};

// Auto-run debug on load
console.log('GraphQL Client loaded. Run debugAuth() to check authentication status.');
window.debugAuth();

// Auto-start token expiration check when page loads (if authenticated)
document.addEventListener('DOMContentLoaded', () => {
    if (window.graphqlClient) {
        window.graphqlClient.startTokenExpirationCheck();
    }
});