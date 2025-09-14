const axios = require('axios');

class GraphQLClient {
    constructor(endpoint = process.env.GRAPHQL_ENDPOINT || 'http://localhost:4000/graphql') {
        this.endpoint = endpoint;
        console.log(`GraphQL Client initialized with endpoint: ${this.endpoint}`);
    }

    async query(query, variables = {}, token = null) {
        const headers = {
            'Content-Type': 'application/json',
        };

        // Only add Authorization header if token is provided and not null
        if (token && token !== 'null') {
            headers.Authorization = `Bearer ${token}`;
        }

        try {
            const response = await axios.post(this.endpoint, {
                query,
                variables
            }, { headers });

            if (response.data.errors) {
                throw new Error(response.data.errors[0].message);
            }

            return response.data.data;
        } catch (error) {
            console.error('GraphQL Error:', error.message);
            throw error;
        }
    }

    async login(username, password) {
        const mutation = `
            mutation Login($username: String!, $password: String!) {
                login(username: $username, password: $password) {
                    success
                    token
                    message
                    username
                    isAdmin
                }
            }
        `;

        try {
            const result = await this.query(mutation, { username, password });

            // If login returns null, authentication is disabled
            if (result.login === null) {
                return {
                    success: true,
                    token: null,
                    message: "Authentication disabled - no login required",
                    username: username || "anonymous",
                    isAdmin: true // Grant admin access when auth is disabled
                };
            }

            // Check if authentication is disabled message
            if (result.login && !result.login.success &&
                result.login.message === "Authentication is not enabled") {
                return {
                    success: true,
                    token: null,
                    message: "Authentication disabled - no login required",
                    username: username || "anonymous",
                    isAdmin: true // Grant admin access when auth is disabled
                };
            }

            return result.login;
        } catch (error) {
            // If mutation fails, might indicate auth is disabled
            if (error.message.includes('Unknown field') || error.message.includes('login')) {
                return {
                    success: true,
                    token: null,
                    message: "Authentication disabled - no login required",
                    username: username || "anonymous",
                    isAdmin: true
                };
            }
            throw error;
        }
    }

    async getBrokers(token) {
        const query = `
            query GetBrokers {
                brokers {
                    nodeId
                    metrics {
                        messagesIn
                        messagesOut
                        nodeSessionCount
                        clusterSessionCount
                        queuedMessagesCount
                        topicIndexSize
                        clientNodeMappingSize
                        topicNodeMappingSize
                        messageBusIn
                        messageBusOut
                    }
                }
            }
        `;

        const result = await this.query(query, {}, token);
        return result.brokers;
    }

    async getBrokerMetrics(nodeId, lastMinutes, token) {
        const query = `
            query GetBrokerMetrics($nodeId: String, $lastMinutes: Int) {
                broker(nodeId: $nodeId) {
                    nodeId
                    metrics(lastMinutes: $lastMinutes) {
                        messagesIn
                        messagesOut
                        nodeSessionCount
                        clusterSessionCount
                        queuedMessagesCount
                        topicIndexSize
                        clientNodeMappingSize
                        topicNodeMappingSize
                        messageBusIn
                        messageBusOut
                    }
                }
            }
        `;

        const result = await this.query(query, { nodeId, lastMinutes }, token);
        return result.broker;
    }

    async getSessions(nodeId, connected, token) {
        const query = `
            query GetSessions($nodeId: String, $connected: Boolean) {
                sessions(nodeId: $nodeId, connected: $connected) {
                    clientId
                    nodeId
                    metrics {
                        messagesIn
                        messagesOut
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
                }
            }
        `;

        const result = await this.query(query, { nodeId, connected }, token);
        return result.sessions;
    }

    async getSession(clientId, nodeId, token) {
        const query = `
            query GetSession($clientId: String!, $nodeId: String) {
                session(clientId: $clientId, nodeId: $nodeId) {
                    clientId
                    nodeId
                    metrics {
                        messagesIn
                        messagesOut
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
                }
            }
        `;

        const result = await this.query(query, { clientId, nodeId }, token);
        return result.session;
    }

    async getUsers(token) {
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

        const result = await this.query(query, {}, token);
        return result.users;
    }

    async createUser(userData, token) {
        const mutation = `
            mutation CreateUser($input: CreateUserInput!) {
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
        `;

        const result = await this.query(mutation, { input: userData }, token);
        return result.createUser;
    }

    async updateUser(userData, token) {
        const mutation = `
            mutation UpdateUser($input: UpdateUserInput!) {
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
        `;

        const result = await this.query(mutation, { input: userData }, token);
        return result.updateUser;
    }

    async deleteUser(username, token) {
        const mutation = `
            mutation DeleteUser($username: String!) {
                deleteUser(username: $username) {
                    success
                    message
                }
            }
        `;

        const result = await this.query(mutation, { username }, token);
        return result.deleteUser;
    }
}

module.exports = GraphQLClient;