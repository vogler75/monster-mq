# MonsterMQ Development Notes

## Dashboard Implementation

### Overview
A complete dark monster-themed dashboard has been created for monitoring and managing MonsterMQ MQTT broker clusters. The dashboard is integrated into the MonsterMQ broker as static resources and served through the embedded web server. It communicates with the broker through the GraphQL API.

### Directory Structure
```
broker/src/main/resources/dashboard/    # Integrated dashboard
├── assets/                 # Frontend assets
│   ├── monster-theme.css
│   └── logo.png           # Updated from Logo-v3-transparent.png
├── js/                    # Client-side JavaScript
│   ├── login.js
│   ├── dashboard.js
│   ├── sessions.js
│   ├── users.js
│   └── [additional JS files]
├── pages/                 # HTML pages
│   ├── login.html
│   ├── dashboard.html
│   ├── sessions.html
│   ├── users.html
│   └── [additional pages]
└── index.html            # Dashboard entry point
```

**Note:** The dashboard is integrated directly into the MonsterMQ broker and served from the embedded web server, not as a separate Node.js application. It communicates with the broker through the GraphQL API endpoint.

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

#### Backend (Embedded in MonsterMQ)
- **GraphQL Integration**: Direct access to MonsterMQ GraphQL API
- **Authentication**: JWT-based login with role-based access control
- **Real-time Updates**: WebSocket connection for live metrics
- **API Access**: Direct GraphQL queries from client-side JavaScript
- **Security**: Built-in authentication, input validation, XSS protection

#### Frontend (Vanilla JavaScript)
- **Modular Architecture**: Separate JS files for each page functionality
- **Chart.js Integration**: Real-time charts with smooth animations
- **WebSocket Client**: Live data streaming from server
- **Responsive UI**: CSS Grid and Flexbox layouts
- **Error Handling**: User-friendly error messages and loading states

### Configuration Options

The dashboard is automatically served by MonsterMQ when the broker is running. No separate configuration is needed.

#### Dashboard Access
- **URL**: `http://broker-host:4000/dashboard` (or configured GraphQL port)
- **GraphQL Endpoint**: Same host and port as the dashboard
- **Authentication**: Uses MonsterMQ user credentials

### Installation & Usage

#### Prerequisites
- MonsterMQ broker running with GraphQL enabled
- Admin user in MonsterMQ for full dashboard access

#### Quick Start
```yaml
# Enable GraphQL in config.yaml
GraphQL:
  Enabled: true
  Port: 4000
```

```bash
# Start MonsterMQ
java -jar monstermq.jar -config config.yaml

# Access dashboard
# Open browser to: http://localhost:4000/dashboard
```

Dashboard is automatically available when GraphQL is enabled.

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
1. **Client**: Dashboard JavaScript queries GraphQL API directly
2. **GraphQL**: Broker responds with current metrics and data
3. **WebSocket**: Real-time updates via GraphQL subscriptions (when available)
4. **Polling**: Client polls for updates at configured intervals
5. **Caching**: Browser-side caching to reduce API calls

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