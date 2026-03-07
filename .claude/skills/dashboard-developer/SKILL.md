---
name: dashboard-developer
description: >
  Guide for developing the MonsterMQ web dashboard. Use this skill whenever the user wants to
  create, modify, or fix dashboard pages, UI components, styles, or JavaScript for the MonsterMQ
  broker's web interface. This includes adding new pages, modifying existing dashboard views,
  working with the GraphQL client, updating the sidebar menu, fixing CSS/styling issues, or
  adding interactive features. Trigger on mentions of "dashboard", "web UI", "frontend page",
  "sidebar", "CSS", "theme", "add a page", "fix the UI", "GraphQL client", "table view",
  "detail page", or any work on files in the dashboard directory.
---

# MonsterMQ Dashboard Development Skill

You are helping a developer work on the MonsterMQ broker's web dashboard — a dark-themed,
single-page-per-view admin interface built with vanilla HTML/CSS/JavaScript (no frameworks).

## Development Setup

For fast iteration, run the broker with the `-dashboardPath` option:
```bash
cd broker
./run.sh -dashboardPath src/main/resources/dashboard
```
This serves files directly from disk — edit and refresh the browser without rebuilding.

## Project Structure

```
broker/src/main/resources/dashboard/
  index.html                    # Entry point (redirects to login or dashboard)
  assets/
    monster-theme.css           # Global theme (CSS variables, layouts, components)
    logo.png                    # Brand logo
  css/
    script-editor-modal.css     # Component-specific styles
  js/
    storage.js                  # SafeStorage wrapper (localStorage with fallback)
    graphql-client.js           # GraphQLDashboardClient class
    sidebar.js                  # SidebarManager with menu config
    log-viewer.js               # Global log viewer component
    login.js                    # Authentication
    dashboard.js                # Main dashboard page
    sessions.js                 # Sessions page
    topic-browser.js            # Topic browser
    [device]-clients.js         # Device list pages
    [device]-client-detail.js   # Device detail pages
    ...
  pages/
    login.html                  # Login page
    dashboard.html              # Main dashboard
    sessions.html               # Active sessions
    topic-browser.html          # MQTT topic browser
    [device]-clients.html       # Device list pages
    [device]-client-detail.html # Device detail/edit pages
    ...
  includes/
    sidebar-template.html       # Reference (sidebar is now rendered by JS)
  config/
    ai-prompts.json             # AI prompt configurations
```

## Page Template

Every dashboard page follows this structure:

```html
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>MonsterMQ - Page Title</title>
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap" rel="stylesheet">
    <link rel="stylesheet" href="/assets/monster-theme.css">
    <script src="/js/storage.js"></script>
    <script src="/js/graphql-client.js"></script>
    <script src="/js/log-viewer.js"></script>
    <script src="/js/sidebar.js"></script>
    <style>
        /* Page-specific styles here */
    </style>
</head>
<body>
    <!-- Mobile Top Bar -->
    <div class="top-bar">
        <div class="sidebar-brand">
            <img src="/assets/logo.png" alt="MonsterMQ Logo">
            <span class="brand-name">MonsterMQ</span>
        </div>
        <button class="mobile-menu-toggle" onclick="toggleSidebar()">
            <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                <line x1="3" y1="6" x2="21" y2="6"></line>
                <line x1="3" y1="12" x2="21" y2="12"></line>
                <line x1="3" y1="18" x2="21" y2="18"></line>
            </svg>
        </button>
    </div>

    <!-- Sidebar (rendered by sidebar.js) -->
    <aside class="sidebar" id="sidebar">
        <div class="sidebar-header">
            <div class="sidebar-brand">
                <img src="/assets/logo.png" alt="MonsterMQ Logo">
                <span class="brand-name">MonsterMQ</span>
            </div>
            <button class="sidebar-toggle" onclick="toggleSidebar()">
                <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                    <polyline points="15,18 9,12 15,6"></polyline>
                </svg>
            </button>
        </div>
        <nav class="sidebar-nav" id="sidebar-nav"></nav>
    </aside>

    <!-- Main Content -->
    <div class="main-content" id="main-content">
        <!-- Your page content here -->
    </div>

    <script src="/js/your-page.js"></script>
    <script>
        if (!isLoggedIn()) window.location.href = '/pages/login.html';
    </script>
</body>
</html>
```

## Theme & CSS Variables

The theme is defined in `assets/monster-theme.css`. Key CSS variables:

```css
--monster-purple: #7C3AED;     /* Primary brand color */
--monster-dark: #5B21B6;       /* Darker purple */
--monster-light: #A78BFA;      /* Lighter purple */
--monster-green: #10B981;      /* Success / connected */
--monster-teal: #14B8A6;       /* Secondary accent */
--monster-red: #EF4444;        /* Error / disconnected */
--monster-orange: #F97316;     /* Warning */
--accent-blue: #3B82F6;        /* Action buttons */
--dark-bg: #0F172A;            /* Page background */
--dark-surface: #1E293B;       /* Card/sidebar background */
--dark-surface-2: #334155;     /* Elevated surface */
--dark-border: #475569;        /* Borders */
--text-primary: #F1F5F9;       /* Main text */
--text-secondary: #CBD5E1;     /* Secondary text */
--text-muted: #94A3B8;         /* Muted text */
--gradient-primary: linear-gradient(135deg, #7C3AED 0%, #10B981 100%);
```

## GraphQL Client

Use `window.graphqlClient` (instance of `GraphQLDashboardClient` from `graphql-client.js`):

```javascript
// Query
const result = await window.graphqlClient.query(`
    query GetDevices {
        devices(type: "MyDevice") {
            id name namespace enabled config
        }
    }
`);

// Mutation with variables
const result = await window.graphqlClient.query(`
    mutation CreateDevice($input: CreateDeviceInput!) {
        createDevice(input: $input) { id name }
    }
`, { input: { name: "test", ... } });
```

The client handles auth tokens, error display, and session expiry automatically.

## Adding a New Page to the Sidebar

Edit `js/sidebar.js` — find the `renderMenu()` method and add to `menuConfig`:

```javascript
{
    href: '/pages/your-page.html',
    icon: '<circle cx="12" cy="12" r="10"></circle>',  // SVG inner content
    text: 'Your Page'
}
```

Place it in the appropriate section: Monitoring, Configuration, Bridging, or System.

## Common UI Patterns

### List/Table Pages
- Class-based manager (e.g., `MqttClientManager`)
- Constructor calls `init()` which loads data
- `loadClients()` fetches via GraphQL, renders table rows
- Auto-refresh with `setInterval(() => this.loadClients(), 30000)`
- Status indicators: green circle for connected, red for disconnected
- Action buttons: view/edit (navigates to detail page), delete (with confirm dialog)

### Detail/Edit Pages
- Read `id` from URL query params: `new URLSearchParams(window.location.search).get('id')`
- If id present: load existing config and populate form (edit mode)
- If no id: show empty form (create mode)
- Dynamic lists (addresses): add/remove rows with JavaScript
- On submit: call appropriate create or update mutation
- Navigate back to list page on success

### Modals/Dialogs
- Create overlay div with `position: fixed; z-index: 10000`
- Dark backdrop: `rgba(0, 0, 0, 0.7)`
- Centered card with border-radius and shadow
- Close on backdrop click or close button

### Toast Notifications
- Fixed position, top-right
- Auto-dismiss after timeout
- Color-coded: green for success, red for error, orange for warning

## Auth & Storage

- `window.safeStorage` — localStorage wrapper with in-memory fallback
- `window.isLoggedIn()` — checks JWT validity or guest mode
- Auth data keys: `monstermq_token`, `monstermq_username`, `monstermq_isAdmin`, `monstermq_guest`
- Guest mode: read-only, mutations show toast instead of redirecting

## Key Rules

1. **No frameworks** — vanilla HTML/CSS/JS only. No React, Vue, build tools.
2. **Dark theme** — always use CSS variables, never hardcode light colors.
3. **Responsive** — sidebar collapses on mobile (handled by sidebar.js).
4. **Script load order matters**: storage.js -> graphql-client.js -> log-viewer.js -> sidebar.js -> page-specific.js
5. **Auth check** at page bottom: `if (!isLoggedIn()) window.location.href = '/pages/login.html';`
6. **GraphQL schema** is in `broker/src/main/resources/schema-*.graphqls` files — check these to understand available queries/mutations.
