# MonsterMQ Development Notes

## Dashboard Implementation

### Overview
A complete dark monster-themed dashboard has been created for monitoring and managing MonsterMQ MQTT broker clusters. The dashboard is implemented as a separate Node.js application that communicates exclusively through the MonsterMQ GraphQL API.

### Directory Structure
```
dashboard/
├── server/                  # Express.js backend
│   ├── server.js           # Main server with WebSocket support
│   ├── auth/               # Authentication & GraphQL client
│   │   ├── graphql-client.js
│   │   └── middleware.js
│   └── routes/             # API endpoints
│       └── api.js
├── public/                 # Frontend assets
│   ├── assets/             # CSS, images
│   │   ├── monster-theme.css
│   │   └── logo.png        # Updated from Logo-v3-transparent.png
│   ├── js/                 # Client-side JavaScript
│   │   ├── login.js
│   │   ├── dashboard.js
│   │   ├── sessions.js
│   │   └── users.js
│   └── pages/              # HTML pages
│       ├── login.html
│       ├── dashboard.html
│       ├── sessions.html
│       └── users.html
├── package.json
└── README.md
```

### Key Features Implemented

#### 🎨 Dark Monster Theme
- Consistent styling with MonsterMQ homepage
- Purple/green gradients (#7C3AED, #10B981)
- Dark surfaces (#0F172A, #1E293B) with proper contrast
- Responsive design for all screen sizes
- Monster logo integration

#### 📊 Main Dashboard
- **Real-time Metrics Cards**: Messages in/out, sessions, queue depths, message bus traffic
- **Live Charts**: Traffic trends (Chart.js line charts) and message bus communication
- **Broker Table**: Detailed view of each cluster node with health indicators
- **Auto-refresh**: WebSocket-based real-time updates every 5 seconds

#### 👥 Session Management
- **Filterable Session List**: By node and connection status
- **Session Details Modal**: Individual client metrics, subscriptions, queue status
- **Visual Analytics**: Doughnut chart showing session distribution across nodes
- **Comprehensive Data**: Client address, clean sessions, message I/O per client

#### 🔐 User Management (Admin Only)
- **Full CRUD Operations**: Create, edit, delete users
- **Permission Control**: Subscribe/publish permissions, admin access
- **ACL Rules Viewer**: Display access control rules per user
- **Security**: Admin-only access with JWT validation

### Technical Implementation

#### Backend (Node.js/Express)
- **GraphQL Integration**: Complete client for MonsterMQ GraphQL API
- **Authentication**: JWT-based login with role-based access control
- **Real-time Updates**: Socket.io WebSocket connection for live metrics
- **API Abstraction**: REST endpoints that proxy to GraphQL
- **Security**: CORS enabled, input validation, XSS protection

#### Frontend (Vanilla JavaScript)
- **Modular Architecture**: Separate JS files for each page functionality
- **Chart.js Integration**: Real-time charts with smooth animations
- **WebSocket Client**: Live data streaming from server
- **Responsive UI**: CSS Grid and Flexbox layouts
- **Error Handling**: User-friendly error messages and loading states

### Configuration Options

#### GraphQL Endpoint Configuration
Multiple ways to specify the MonsterMQ GraphQL endpoint:

1. **Command Line Arguments**:
   ```bash
   npm start -- --graphql http://broker:4000/graphql
   npm start -- -g http://192.168.1.100:4000/graphql
   npm start -- --endpoint http://remote:8080/graphql
   npm start -- -e http://cluster:4000/graphql
   ```

2. **Environment Variables**:
   ```bash
   GRAPHQL_ENDPOINT=http://broker:4000/graphql npm start
   ```

3. **Default**: `http://localhost:4000/graphql`

#### Server Configuration
- **PORT**: Dashboard server port (default: 3001)
- **GRAPHQL_ENDPOINT**: MonsterMQ GraphQL endpoint

### Installation & Usage

#### Prerequisites
- Node.js 16+
- MonsterMQ broker with GraphQL enabled
- Admin user in MonsterMQ for full dashboard access

#### Quick Start
```bash
cd dashboard
npm install
npm start -- --graphql http://your-broker:4000/graphql
```

Dashboard available at: `http://localhost:3001`

### GraphQL API Requirements
The dashboard requires these MonsterMQ GraphQL features:
- Authentication: `login` mutation
- Broker metrics: `brokers`, `broker` queries
- Session data: `sessions`, `session` queries
- User management: `users` query, `createUser`/`updateUser`/`deleteUser` mutations
- Real-time capabilities: Ready for GraphQL subscriptions

### File Changes Made
The following files were updated during development:

#### HTML Pages
- All HTML pages updated to use `/assets/logo.png` instead of `/assets/Logo-v3-transparent.png`
- Consistent navigation and styling across all pages

#### Asset References
- Logo file copied and referenced as `logo.png`
- All image references updated in HTML files

#### Server Configuration
- Added command-line argument parsing for GraphQL endpoint
- Environment variable support for flexible deployment
- Startup logging shows active configuration

### Development Notes

#### Real-time Data Flow
1. **Server**: Express server fetches data from MonsterMQ GraphQL
2. **WebSocket**: Socket.io broadcasts metrics updates every 5 seconds
3. **Client**: JavaScript receives updates and refreshes charts/tables
4. **Caching**: 5-second cache on server to prevent GraphQL overload

#### Authentication Flow
1. **Login**: User submits credentials via REST API
2. **GraphQL**: Server calls MonsterMQ login mutation
3. **JWT**: Token stored in localStorage, included in all API calls
4. **Validation**: Server validates JWT for protected endpoints
5. **Role-based**: Admin-only features check `isAdmin` claim

#### Chart Integration
- Chart.js configured with dark theme matching monster design
- Real-time data updates without full chart re-render
- Smooth animations and responsive layouts
- Color scheme matches MonsterMQ branding

#### Security Considerations
- JWT tokens with expiration handling
- Role-based access control for admin features
- Input sanitization and XSS protection
- CORS configuration for cross-origin requests
- No direct database access - all via GraphQL

### Future Enhancements
- GraphQL subscriptions for true real-time updates
- Historical data with time-range selection
- Advanced filtering and search capabilities
- Export functionality for metrics and logs
- Mobile app companion
- Alerting and notification system
- Custom dashboard layouts
- Multi-tenant support

### Deployment Considerations
- Docker containerization ready
- Environment-based configuration
- Reverse proxy compatible
- Load balancer friendly
- Monitoring and logging integration ready
- Production security hardening implemented

The dashboard is production-ready and provides a comprehensive management interface for MonsterMQ deployments of any size, from single-node development to large-scale cluster operations.