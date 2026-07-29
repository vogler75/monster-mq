---
name: monstermq-dashboard-developer
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

This skill provides instructions, architecture guidelines, design rules, and code patterns for developing the MonsterMQ web dashboard (`dashboard/`).

---

## 1. Core Architecture & Development Setup

- **Technology Stack**: Vanilla HTML5, CSS3, JavaScript (ES6+), Siemens iX Web Components (`@siemens/ix` v4.3+), Vite bundler.
- **Root Directory**: `dashboard/`
- **Output Directory**: `dashboard/dist/` (embedded into broker JAR at `broker/src/main/resources/dashboard/`).

```bash
cd dashboard
npm install
npm run dev          # Starts Vite dev server on http://localhost:5173 (proxies /graphql to :4000)
npm run build        # Builds production bundle to dashboard/dist/
```

> [!IMPORTANT]
> Always edit dashboard source files in `dashboard/src/`, **NEVER** in `broker/src/main/resources/dashboard/`. Direct edits to `broker/src/main/resources/dashboard/` will be overwritten on build.

---

## 2. Design System & Design Concept (`dashboard/DESIGN.md`)

### The One Cardinal Rule
**Pages MUST NOT redefine shared components in a local `<style>` block.**

The SPA router (`js/sidebar.js`) hoists each page's `<style>` block into `<head>` *after* shared stylesheets. Local overrides of `.data-table`, `.btn`, `.form-control`, `.card`, etc., cause visual regressions across navigations.

### File Ownership Map

| File | Responsibilities |
|------|------------------|
| `assets/components.css` | **The Design System.** Every recurring UI element (`.data-table`, `.metric-card`, `.section-card`, `.status-badge`, `.btn`, `.form-control`, `.loading-indicator`, `.error-message`). |
| `assets/monster-theme.css` | Brand color tokens, CSS variables, scrollbars, auth indicator, standalone login page. |
| `assets/ix-app.css` | Layout overrides only — viewport height, `.main-content` container, `ix-menu` height. |
| `js/ui.js` | Modals, confirmations, toasts, loading/error/empty states (`window.ui`). |
| `js/sidebar.js` | Menu configuration (`getMenuConfig()`) and SPA router navigation. |
| `js/graphql-client.js` | GraphQL HTTP client (`window.graphqlClient`). |

---

## 3. The Two Page Shapes

Every page in the dashboard follows one of two canonical shapes:

### 1. List Page Shape (e.g. `pages/redis-clients.html` + `js/redis-clients.js`)
- **Structure**:
  ```
  page-header          # Title + subtitle left; page-level actions right
  error-message        # Driven by window.ui.showError()
  metrics-grid         # 4-6 metric-cards with Siemens iX icons
  data-table           # Data container
    table-header       # Table title left, table-actions right (search, refresh, create)
    table-responsive   # Table container with horizontal scroll
  loading-indicator    # Driven by window.ui.setLoading()
  ```
- **Actions Placement**: List page "Create" actions live in `table-actions` inside `table-header` (next to filters/refresh).

### 2. Detail Page Shape (e.g. `pages/nats-client-detail.html` + `js/nats-client-detail.js`)
- **Structure**:
  ```
  page-header
    breadcrumb         # Parent list › Entity Name (No back button!)
    title + subtitle
    page-header-actions # Destructive action first, primary save action last
  error-message
  section-card         # One per logical form section
    section-header     # Heading left, status badge / section action right
    section-content    # Form grid or sub-resource table
  loading-indicator
  ```
- **Save Behavior**: Never navigate back to list page on save. Show a success toast and update the page state (or redirect `window.spaLocation.href` to the newly created entity URL in edit mode).

---

## 4. Siemens iX Web Components

The dashboard uses **Siemens iX** web components:

- **Layout**: `<ix-application>`, `<ix-menu>`, `<ix-menu-category>`, `<ix-menu-item>`
- **Inputs**: `<ix-input>`, `<ix-select>`, `<ix-toggle>`, `<ix-checkbox>`
- **Buttons**: `<ix-button>`, `<ix-icon-button>`
- **Feedback**: `<ix-toast>`, `<ix-modal>`, `<ix-spinner>`
- **Icons**: Use `icon="icon-name"` attribute (1400+ SVGs from `@siemens/ix-icons` served from `/svg/`).

---

## 5. UI Helpers (`window.ui`)

Never use raw `alert()`, `confirm()`, or custom inline toast HTML. Always use `window.ui` from `js/ui.js`:

```javascript
// Toasts
window.ui.success("Configuration saved successfully");
window.ui.error("Failed to connect to broker");

// Confirmation Dialogs
const confirmed = await window.ui.showConfirm({
    title: "Delete Connector",
    message: "Are you sure you want to delete this device?",
    confirmText: "Delete",
    type: "danger"
});

// Loading & Error states
window.ui.setLoading(true);
window.ui.showError("Unable to fetch topic list");
```

---

## 6. GraphQL Client (`window.graphqlClient`)

Interact with the backend via `window.graphqlClient` (`GraphQLDashboardClient`):

```javascript
// Query
const data = await window.graphqlClient.query(`
    query GetClients {
        mqttClientConfigs {
            name
            clientHost
            enabled
        }
    }
`);

// Mutation
const result = await window.graphqlClient.query(`
    mutation CreateMqttClient($input: MqttClientConfigInput!) {
        createMqttClientConfig(input: $input) {
            name
        }
    }
`, { input: { name: "remote-broker-1", clientHost: "192.168.1.50" } });
```

---

## 7. Adding a New Page Step-by-Step

1. **Add Sidebar Menu Item**: In `js/sidebar.js`, add your page to `getMenuConfig()` under the appropriate category (`Bridging`, `Governance`, `Configuration`, `Monitoring`).
2. **Create HTML File**: Create `src/pages/your-page.html` using the standard `<ix-application>` + `#main-content` shell.
3. **Create JS File**: Create `src/js/your-page.js` with a manager class that loads data via `window.graphqlClient` and attaches handlers.
4. **Register Cleanup**: Call `window.registerPageCleanup(fn)` for any timers or event listeners.

---

## 8. Summary Checklist for Dashboard Code Changes

- [ ] Edits made in `dashboard/src/`, NOT in `broker/src/main/resources/dashboard/`.
- [ ] No local CSS redefinitions of shared components (`.data-table`, `.btn`, `.card`).
- [ ] Used `window.ui` for alerts, confirmations, toasts, and loading indicators.
- [ ] Used Siemens iX components (`<ix-button>`, `<ix-toggle>`, etc.) with valid iX icons.
- [ ] Registered timers/listeners with `window.registerPageCleanup()`.
