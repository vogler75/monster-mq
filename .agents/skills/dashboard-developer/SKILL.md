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

You are helping a developer work on the MonsterMQ broker's web dashboard — a dark-themed SPA
built with vanilla HTML/CSS/JavaScript and the **Siemens iX design system** (web components).
The dashboard lives in `dashboard/` and is built with Vite.

## Development Setup

```bash
cd dashboard
npm install
npm run dev          # Vite dev server on http://localhost:5173, proxies /graphql to broker
npm run build        # Outputs to dashboard/dist/
```

The Vite dev server proxies `/graphql` and `/graphqlws` to the broker backend (configured in `vite.config.js`).

## Project Structure

```
dashboard/
  package.json                   # Deps: @siemens/ix, @siemens/ix-icons, @siemens/ix-echarts, echarts, vite
  vite.config.js                 # Build config: root=src, output=dist, only ix-init.js is bundled
  src/
    index.html                   # SPA shell — sets __SPA_MODE, loads sidebar + ix-menu
    js/
      ix-init.js                 # ES module: loads iX CSS, registers custom elements, sets classic-dark theme
      storage.js                 # StorageManager → window.safeStorage, window.isLoggedIn()
      graphql-client.js          # GraphQLDashboardClient → window.graphqlClient
      log-viewer.js              # LogViewer class → window.__monsterMQLogViewer
      sidebar.js                 # SidebarManager: menu config, SPA navigation, page script loading
      dashboard.js               # Dashboard page logic
      sessions.js                # Sessions page logic
      topic-browser.js           # Topic browser page logic
      [device]-clients.js        # Device list pages
      [device]-client-detail.js  # Device detail/edit pages
      ...
    pages/
      login.html                 # Login page (standalone, not loaded via SPA)
      dashboard.html             # Main dashboard
      sessions.html              # Active sessions
      topic-browser.html         # MQTT topic browser
      [device]-clients.html      # Device list pages
      [device]-client-detail.html # Device detail/edit pages
      ...
    assets/
      monster-theme.css          # Global theme: CSS variables, layouts, component styles
      ix-app.css                 # iX layout overrides (viewport, sidebar height, table header)
      logo.png                   # Brand logo
    css/
      script-editor-modal.css    # Component-specific styles
    config/
      ai-prompts.json            # AI prompt configurations
    includes/
      sidebar-template.html      # Reference (sidebar rendered by JS)
    svg/                         # Symlink to iX icon SVGs (copied from node_modules at build)
```

## Build Architecture

- **Only `ix-init.js` is bundled by Vite/Rollup** — it imports iX CSS and registers custom elements
- **All other JS/HTML/CSS files are copied as-is** via a custom `copy-static` Vite plugin
- **iX icon SVGs** are copied from `node_modules/@siemens/ix-icons/dist/ix-icons/svg` to `dist/svg/`
- Page HTML files are NOT processed by Vite — they're plain HTML

## SPA Architecture

The dashboard is a custom SPA built around `index.html`:

1. **`index.html`** is the shell — it sets `window.__SPA_MODE = true`, loads shared scripts, and contains the `<ix-application>` + `<ix-menu>` layout
2. **`sidebar.js`** (`SidebarManager`) handles all navigation:
   - Renders menu items from `getMenuConfig()` using `<ix-menu-category>` and `<ix-menu-item>`
   - Intercepts link clicks and fetches page HTML via `fetch()`
   - Extracts `#main-content`, page-specific styles, and scripts from fetched HTML
   - Wraps page scripts in IIFEs for scope isolation (rewrites `let`/`const`/`class` to `var`)
   - Shims `DOMContentLoaded` so page scripts' init callbacks fire after injection
   - Manages browser history (`pushState` / `popstate`)
   - Cleans up previous page's timers and registered cleanup callbacks
3. **Standalone page access** redirects to the SPA shell: `/?page=/pages/foo.html`

### Timer and Cleanup Tracking

- `setInterval`/`setTimeout` are globally wrapped to track IDs in `window._pageIntervals`/`window._pageTimeouts`
- On navigation, all tracked timers are cleared automatically
- Pages can register custom cleanup via `window.registerPageCleanup(fn)`

## Page Template

Every page follows this structure (the SPA shell extracts `#main-content` and scripts):

```html
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>MonsterMQ - Page Title</title>
    <script type="module" src="/js/ix-init.js"></script>
    <link rel="stylesheet" href="/assets/monster-theme.css">
    <!-- Optional: CDN scripts like Chart.js -->
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <script src="/js/storage.js"></script>
    <script src="/js/graphql-client.js"></script>
    <script src="/js/log-viewer.js"></script>
    <script src="/js/sidebar.js"></script>
    <style>
        /* Page-specific styles here */
    </style>
</head>
<body>
    <ix-application force-breakpoint="lg">
    <ix-menu application-name="MonsterMQ" pinned start-expanded>
    </ix-menu>
    <div class="main-content" id="main-content">
        <div class="container">
            <div class="page-header">
                <h1 class="page-title">Page Title</h1>
                <p class="page-subtitle">Description of this page</p>
            </div>

            <!-- Page content here -->
        </div>
    </div>

    <script src="/js/your-page.js"></script>
    </ix-application>
</body>
</html>
```

**Key points:**
- The `<ix-application>`, `<ix-menu>`, and shared scripts are present but only used when the page is loaded standalone (which redirects to SPA mode)
- The SPA extracts only `#main-content` innerHTML and page-specific `<script>`/`<style>` tags
- Shared scripts (`storage.js`, `graphql-client.js`, `sidebar.js`, `log-viewer.js`, `ix-init.js`) are skipped during SPA navigation — they're already loaded
- CDN scripts (e.g., Chart.js) are loaded once and reused across navigations

## Siemens iX Design System

The dashboard uses **Siemens iX web components** (`@siemens/ix` v4.3+). Key components:

- **Layout**: `<ix-application>`, `<ix-application-header>`, `<ix-menu>`, `<ix-menu-category>`, `<ix-menu-item>`
- **Inputs**: `<ix-input>`, `<ix-select>`, `<ix-toggle>`, `<ix-checkbox>`
- **Buttons**: `<ix-button>`, `<ix-icon-button>`
- **Containers**: `<ix-card>`, `<ix-group>`, `<ix-tile>`
- **Feedback**: `<ix-toast>`, `<ix-modal>`, `<ix-spinner>`
- **Data**: `<ix-event-list>`, `<ix-event-list-item>`
- **Icons**: Use `icon="icon-name"` attribute — icons come from `@siemens/ix-icons` (1400+ SVGs served from `/svg/`)
- **Theme**: `theme-classic-dark` is applied to `<body>` by `ix-init.js`

To search for available iX icons, use the `ix-icon-search` MCP tool if available, or browse the icon library.

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
--card-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.4), 0 2px 4px -1px rgba(0, 0, 0, 0.2);
```

Layout overrides for iX components are in `assets/ix-app.css`:
- Full-height `ix-application` and `ix-menu`
- Scrollable `.main-content` area
- Standardized `.table-header` with `.table-title` and `.table-actions`
- Per-row `.action-buttons` sizing for `ix-icon-button`

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

The client handles auth tokens, error display, session expiry warnings, and auto-logout automatically.

## Adding a New Page

### 1. Add the menu item in `js/sidebar.js`

Find `getMenuConfig()` and add your item to the appropriate section:

```javascript
{
    section: 'Bridging', sectionIcon: 'link',
    items: [
        // ... existing items ...
        { href: '/pages/your-page.html', icon: 'icon-name', text: 'Your Page' }
    ]
}
```

Available sections: Monitoring, Configuration, Governance, Bridging, System.

### 2. Create the HTML page

Create `src/pages/your-page.html` following the page template above.

### 3. Create the JavaScript file

Create `src/js/your-page.js` with the page logic:

```javascript
class YourPageManager {
    constructor() {
        this.init();
    }

    async init() {
        if (!window.isLoggedIn || !window.isLoggedIn()) return;
        this.setupEventListeners();
        await this.loadData();
    }

    setupEventListeners() {
        // Set up UI event listeners
    }

    async loadData() {
        const result = await window.graphqlClient.query(`
            query { ... }
        `);
        if (result?.data) {
            this.renderData(result.data);
        }
    }

    renderData(data) {
        // Populate DOM elements
    }
}

// Auto-init — the SPA wraps this in an IIFE with a DOMContentLoaded shim
new YourPageManager();
```

## Common UI Patterns

### List/Table Pages
- Class-based manager (e.g., `MqttClientManager`)
- Constructor calls `init()` which loads data
- `loadClients()` fetches via GraphQL, renders table rows
- Auto-refresh with `setInterval(() => this.loadClients(), 30000)`
- Status indicators: green circle for connected, red for disconnected
- Action buttons using `<ix-icon-button>` in `.action-buttons` container
- Table header bar using `.table-header` > `.table-title` + `.table-actions`

### Detail/Edit Pages
- Read `id` from URL query params: `new URLSearchParams(window.location.search).get('id')`
- If id present: load existing config and populate form (edit mode)
- If no id: show empty form (create mode)
- Dynamic lists (addresses): add/remove rows with JavaScript
- On submit: call appropriate create or update mutation
- Navigate back to list page on success using `window.navigateTo('/pages/list.html')`

### Modals/Dialogs
- Create overlay div with `position: fixed; z-index: 10000`
- Dark backdrop: `rgba(0, 0, 0, 0.7)`
- Centered card with border-radius and shadow
- Close on backdrop click or close button
- Or use iX components: `<ix-modal>` for structured dialogs

### Toast Notifications
- Fixed position, top-right
- Auto-dismiss after timeout
- Color-coded: green for success, red for error, orange for warning
- Or use iX toast: `window.showToast({ message: 'Done', type: 'success' })`

### Navigation Between Pages
- Use `window.navigateTo('/pages/target.html')` for SPA navigation
- Use `window.navigateTo('/pages/detail.html?id=' + itemId)` for detail pages
- For `<a>` tags: `<a href="/pages/target.html">` — clicks are intercepted by SPA automatically

## Auth & Storage

- `window.safeStorage` — localStorage wrapper with in-memory fallback (handles Brave shields, private browsing)
- `window.isLoggedIn()` — checks JWT validity or guest mode
- Auth data keys: `monstermq_token`, `monstermq_username`, `monstermq_isAdmin`, `monstermq_guest`
- Guest mode: read-only, body gets `.read-only-mode` class
- Hide write controls from guests: add `data-requires-auth` attribute to any element

## Key Rules

1. **No frameworks** — vanilla HTML/CSS/JS + iX web components. No React, Vue, Angular.
2. **Dark theme** — always use CSS variables from `monster-theme.css`, never hardcode light colors.
3. **iX components** — prefer iX web components (`<ix-button>`, `<ix-icon-button>`, `<ix-toggle>`, etc.) over custom HTML elements where appropriate.
4. **iX icons** — use `icon="icon-name"` on iX components. Browse available icons at the iX icon library or use the `ix-icon-search` MCP tool.
5. **SPA-aware scripts** — page scripts are wrapped in IIFEs by the SPA. Use `var` or top-level declarations (the SPA rewrites `let`/`const` to `var`). Don't rely on module scope.
6. **Cleanup** — register cleanup callbacks with `window.registerPageCleanup(fn)` for event listeners, WebSocket connections, or other resources that should be torn down on navigation.
7. **Script load order**: `ix-init.js` (module) → `storage.js` → `graphql-client.js` → `log-viewer.js` → `sidebar.js` → page-specific JS.
8. **GraphQL schema** is in `broker/src/main/resources/schema-*.graphqls` files — check these to understand available queries/mutations.
9. **File naming**: page HTML in `src/pages/`, matching JS in `src/js/` — e.g., `pages/foo.html` ↔ `js/foo.js`.
10. **No build step for pages** — HTML/JS/CSS files are copied as-is to `dist/`. Only `ix-init.js` goes through Vite bundling.
