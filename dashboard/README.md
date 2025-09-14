# MonsterMQ Dashboard

A dark monster-themed dashboard for monitoring and managing your MonsterMQ MQTT broker cluster. Built with Node.js, Express, and Chart.js.

![MonsterMQ Dashboard](https://img.shields.io/badge/MonsterMQ-Dashboard-7C3AED)

## Features

### 🎨 Dark Monster Theme
- Consistent styling with MonsterMQ homepage
- Purple/green gradient accents and dark surfaces
- Responsive design for desktop and mobile

### 📊 Real-time Broker Monitoring
- **Cluster Overview**: Total messages in/out, active sessions, queued messages
- **Live Charts**: Message traffic trends and inter-node communication
- **Node Health**: Status indicators and detailed metrics per broker node
- **Auto-refresh**: Real-time updates via WebSocket

### 👥 Session Management
- **Session List**: Filter by node, connection status (connected/disconnected)
- **Session Details**: Individual client metrics, subscription lists, queue status
- **Visual Analytics**: Session distribution charts across nodes

### 🔐 User Management (Admin Only)
- **User CRUD**: Create, edit, delete MQTT users
- **Permission Management**: Subscribe/publish permissions, admin access
- **ACL Rules**: View access control rules per user
- **Security**: JWT-based authentication with role-based access

## Installation

### Prerequisites
- Node.js 16+
- MonsterMQ broker running with GraphQL enabled (port 4000)
- Admin user configured in MonsterMQ for full dashboard access

### Setup

1. **Install Dependencies**
```bash
cd dashboard
npm install
```

2. **Configure MonsterMQ GraphQL**

   Ensure your MonsterMQ `config.yaml` includes:
```yaml
GraphQL:
  Enabled: true
  Port: 4000  # Dashboard expects GraphQL on port 4000
  Path: /graphql
  CorsEnabled: true
```

3. **Start Dashboard**
```bash
# Default: GraphQL on localhost:4000
npm start

# Custom GraphQL endpoint via command line
npm start -- --graphql http://your-broker:8080/graphql
npm start -- -g http://192.168.1.100:4000/graphql

# Alternative syntax
npm start -- --endpoint http://your-broker:4000/graphql
npm start -- -e http://remote-broker:4000/graphql

# Via environment variable
GRAPHQL_ENDPOINT=http://your-broker:4000/graphql npm start

# Development with auto-reload
npm run dev -- --graphql http://your-broker:4000/graphql
```

The dashboard will be available at `http://localhost:3001`

## Usage

### Login
1. Navigate to `http://localhost:3001`
2. Login with your MonsterMQ username and password
3. The dashboard will authenticate via GraphQL and store a JWT token

### Dashboard Pages

#### Main Dashboard (`/dashboard`)
- **Cluster Metrics Cards**: Messages in/out, sessions, queue depths, bus traffic
- **Traffic Trends Chart**: Real-time line chart of message throughput
- **Message Bus Chart**: Inter-node communication visualization
- **Broker Table**: Detailed view of each cluster node

#### Sessions (`/sessions`)
- **Session Metrics**: Total/connected/disconnected counts
- **Distribution Chart**: Sessions per node (doughnut chart)
- **Session Table**: Filterable list with client details
- **Session Details Modal**: Click any session for detailed view

#### User Management (`/users`) - Admin Only
- **User Metrics**: Total users, active, admin counts, ACL rules
- **User Table**: Full CRUD operations
- **ACL Rules Modal**: View access control rules per user

### Real-time Updates
- Dashboard auto-refreshes broker metrics every 5 seconds
- WebSocket connection provides live data streams
- Charts update smoothly with new data points
- Connection status indicators show cluster health

## API Endpoints

The dashboard server provides these REST endpoints:

### Authentication
- `POST /api/login` - Login with username/password

### Broker Metrics
- `GET /api/brokers` - Get all broker nodes with metrics
- `GET /api/broker/:nodeId/metrics` - Get specific node metrics

### Sessions
- `GET /api/sessions` - Get all sessions (supports filtering)
- `GET /api/session/:clientId` - Get specific session details

### User Management (Admin Required)
- `GET /api/users` - List all users with ACL rules
- `POST /api/users/create` - Create new user
- `POST /api/users/update` - Update existing user
- `POST /api/users/delete` - Delete user

## Configuration

### Environment Variables
```bash
PORT=3001                                        # Dashboard server port (default: 3001)
GRAPHQL_ENDPOINT=http://localhost:4000/graphql   # MonsterMQ GraphQL endpoint (default: localhost:4000)
```

### Command Line Arguments
```bash
# GraphQL endpoint options (all equivalent)
--graphql <endpoint>     # or -g <endpoint>
--endpoint <endpoint>    # or -e <endpoint>

# Examples:
npm start -- --graphql http://192.168.1.100:4000/graphql
npm start -- -g http://monstermq.example.com:8080/graphql
npm start -- --endpoint http://broker:4000/graphql
```

### MonsterMQ Requirements
The dashboard requires these GraphQL schema features:
- `login` mutation for authentication
- `brokers` query for cluster metrics
- `sessions` query for session data
- `users` query for user management (admin)
- User management mutations (`createUser`, `updateUser`, `deleteUser`)

## Architecture

```
dashboard/
├── server/                  # Express.js backend
│   ├── server.js           # Main server with WebSocket
│   ├── auth/               # Authentication & GraphQL client
│   └── routes/             # API endpoints
├── public/                 # Frontend assets
│   ├── assets/             # CSS, images
│   ├── js/                 # Client-side JavaScript
│   └── pages/              # HTML pages
└── package.json
```

### Technology Stack
- **Backend**: Node.js, Express, Socket.io
- **Frontend**: Vanilla JavaScript, Chart.js
- **Styling**: Custom CSS with CSS variables
- **Authentication**: JWT tokens
- **Communication**: REST API + WebSocket for real-time data

## Security

- **JWT Authentication**: All API calls require valid JWT tokens
- **Role-based Access**: Admin functions restricted to admin users
- **Token Expiration**: Automatic logout on token expiry
- **CORS Enabled**: Cross-origin requests supported
- **Input Validation**: XSS protection via HTML escaping

## Development

### File Structure
- **Login**: `login.html` + `login.js` - Authentication page
- **Dashboard**: `dashboard.html` + `dashboard.js` - Main metrics page
- **Sessions**: `sessions.html` + `sessions.js` - Session management
- **Users**: `users.html` + `users.js` - User administration
- **Styling**: `monster-theme.css` - Dark monster theme

### GraphQL Integration
The dashboard communicates with MonsterMQ exclusively through GraphQL:
- Queries for read operations (metrics, sessions, users)
- Mutations for write operations (login, user management)
- Real-time subscriptions possible for future enhancements

## Troubleshooting

### Common Issues

**Dashboard shows "Network error"**
- Verify MonsterMQ is running with GraphQL enabled on port 4000
- Check if user has correct permissions

**Users page not accessible**
- Ensure logged-in user has `isAdmin: true` in MonsterMQ
- Admin users can access user management functions

**Charts not updating**
- Check browser console for WebSocket connection errors
- Verify MonsterMQ broker is responding to GraphQL queries

**Authentication fails**
- Confirm username/password are correct in MonsterMQ
- Check MonsterMQ logs for authentication errors

### Logs
Monitor the dashboard console output:
```bash
npm run dev
# Watch for GraphQL connection errors
# WebSocket connection status
# API request/response logs
```

## License

This dashboard is part of the MonsterMQ project and follows the same license terms.