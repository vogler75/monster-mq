const express = require('express');
const http = require('http');
const socketIo = require('socket.io');
const cors = require('cors');
const path = require('path');
const apiRouter = require('./routes/api');
const GraphQLClient = require('./auth/graphql-client');

// Parse command line arguments
const args = process.argv.slice(2);
let graphqlEndpoint = process.env.GRAPHQL_ENDPOINT || 'http://localhost:4000/graphql';

// Check for --graphql argument
const graphqlArgIndex = args.findIndex(arg => arg === '--graphql' || arg === '-g');
if (graphqlArgIndex !== -1 && args[graphqlArgIndex + 1]) {
    graphqlEndpoint = args[graphqlArgIndex + 1];
}

// Check for --endpoint argument (alternative)
const endpointArgIndex = args.findIndex(arg => arg === '--endpoint' || arg === '-e');
if (endpointArgIndex !== -1 && args[endpointArgIndex + 1]) {
    graphqlEndpoint = args[endpointArgIndex + 1];
}

const app = express();
const server = http.createServer(app);
const io = socketIo(server, {
    cors: {
        origin: "*",
        methods: ["GET", "POST"]
    }
});

const PORT = process.env.PORT || 3001;
const graphqlClient = new GraphQLClient(graphqlEndpoint);

app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, '../public')));

app.use('/api', apiRouter);

app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, '../public/pages/login.html'));
});

app.get('/dashboard', (req, res) => {
    res.sendFile(path.join(__dirname, '../public/pages/dashboard.html'));
});

app.get('/sessions', (req, res) => {
    res.sendFile(path.join(__dirname, '../public/pages/sessions.html'));
});

app.get('/users', (req, res) => {
    res.sendFile(path.join(__dirname, '../public/pages/users.html'));
});

const metricsCache = new Map();
const CACHE_DURATION = 5000;

async function broadcastMetrics() {
    try {
        const now = Date.now();
        const cacheKey = 'brokers';

        let brokers = metricsCache.get(cacheKey);

        if (!brokers || now - brokers.timestamp > CACHE_DURATION) {
            try {
                const freshBrokers = await graphqlClient.getBrokers();
                brokers = { data: freshBrokers, timestamp: now };
                metricsCache.set(cacheKey, brokers);
            } catch (error) {
                console.error('Failed to fetch broker metrics:', error.message);
                return;
            }
        }

        io.emit('metrics-update', brokers.data);
    } catch (error) {
        console.error('Error broadcasting metrics:', error);
    }
}

io.on('connection', (socket) => {
    console.log('Client connected:', socket.id);

    socket.on('subscribe-metrics', () => {
        broadcastMetrics();
    });

    socket.on('disconnect', () => {
        console.log('Client disconnected:', socket.id);
    });
});

setInterval(broadcastMetrics, CACHE_DURATION);

server.listen(PORT, () => {
    console.log(`MonsterMQ Dashboard running on http://localhost:${PORT}`);
    console.log(`GraphQL endpoint: ${graphqlEndpoint}`);
    console.log(`To change GraphQL endpoint, use: --graphql <endpoint> or set GRAPHQL_ENDPOINT environment variable`);
});

module.exports = app;