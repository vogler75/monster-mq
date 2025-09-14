const express = require('express');
const GraphQLClient = require('../auth/graphql-client');
const { authenticateToken, requireAdmin } = require('../auth/middleware');

const router = express.Router();
const graphqlClient = new GraphQLClient();

router.post('/login', async (req, res) => {
    try {
        const { username, password } = req.body;

        // Allow empty credentials to test if authentication is disabled
        // The GraphQL client will handle this appropriately

        const result = await graphqlClient.login(username, password);

        if (result.success) {
            res.json({
                success: true,
                token: result.token,
                username: result.username,
                isAdmin: result.isAdmin
            });
        } else {
            res.status(401).json({
                success: false,
                message: result.message
            });
        }
    } catch (error) {
        console.error('Login error:', error);
        res.status(500).json({ error: 'Login failed' });
    }
});

router.get('/brokers', authenticateToken, async (req, res) => {
    try {
        const brokers = await graphqlClient.getBrokers(req.token);
        res.json(brokers);
    } catch (error) {
        console.error('Brokers fetch error:', error);
        res.status(500).json({ error: 'Failed to fetch brokers' });
    }
});

router.get('/broker/:nodeId/metrics', authenticateToken, async (req, res) => {
    try {
        const { nodeId } = req.params;
        const { lastMinutes } = req.query;

        const broker = await graphqlClient.getBrokerMetrics(
            nodeId,
            lastMinutes ? parseInt(lastMinutes) : undefined,
            req.token
        );
        res.json(broker);
    } catch (error) {
        console.error('Broker metrics fetch error:', error);
        res.status(500).json({ error: 'Failed to fetch broker metrics' });
    }
});

router.get('/sessions', authenticateToken, async (req, res) => {
    try {
        const { nodeId, connected } = req.query;
        const sessions = await graphqlClient.getSessions(
            nodeId,
            connected === 'true' ? true : connected === 'false' ? false : undefined,
            req.token
        );
        res.json(sessions);
    } catch (error) {
        console.error('Sessions fetch error:', error);
        res.status(500).json({ error: 'Failed to fetch sessions' });
    }
});

router.get('/session/:clientId', authenticateToken, async (req, res) => {
    try {
        const { clientId } = req.params;
        const { nodeId } = req.query;

        const session = await graphqlClient.getSession(clientId, nodeId, req.token);
        res.json(session);
    } catch (error) {
        console.error('Session fetch error:', error);
        res.status(500).json({ error: 'Failed to fetch session' });
    }
});

router.get('/users', authenticateToken, requireAdmin, async (req, res) => {
    try {
        const users = await graphqlClient.getUsers(req.token);
        res.json(users);
    } catch (error) {
        console.error('Users fetch error:', error);
        res.status(500).json({ error: 'Failed to fetch users' });
    }
});

router.post('/users/create', authenticateToken, requireAdmin, async (req, res) => {
    try {
        const result = await graphqlClient.createUser(req.body, req.token);
        res.json(result);
    } catch (error) {
        console.error('User creation error:', error);
        res.status(500).json({ error: 'Failed to create user' });
    }
});

router.post('/users/update', authenticateToken, requireAdmin, async (req, res) => {
    try {
        const result = await graphqlClient.updateUser(req.body, req.token);
        res.json(result);
    } catch (error) {
        console.error('User update error:', error);
        res.status(500).json({ error: 'Failed to update user' });
    }
});

router.post('/users/delete', authenticateToken, requireAdmin, async (req, res) => {
    try {
        const { username } = req.body;
        const result = await graphqlClient.deleteUser(username, req.token);
        res.json(result);
    } catch (error) {
        console.error('User deletion error:', error);
        res.status(500).json({ error: 'Failed to delete user' });
    }
});

module.exports = router;