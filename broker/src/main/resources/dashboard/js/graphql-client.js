class GraphQLDashboardClient {
    constructor(endpoint = '/graphql') {
        this.endpoint = endpoint;
    }

    async query(query, variables = {}) {
        const token = localStorage.getItem('monstermq_token');
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
window.debugAuth = function() {
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