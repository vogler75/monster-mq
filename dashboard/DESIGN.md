# Dashboard design concept

Every page in the dashboard is one of two shapes. Build new pages by copying an
existing migrated example rather than starting from scratch.

- **List page** — `pages/redis-clients.html` + `js/redis-clients.js`
- **Detail page** — `pages/nats-client-detail.html` + `js/nats-client-detail.js`

## The one rule

**Pages must not redefine shared components in a local `<style>` block.**

The SPA router (`js/sidebar.js`) hoists each page's `<style>` into `<head>`
*after* the shared stylesheets, so a local copy of `.data-table` or `.btn`
silently wins. That is how the dashboard ended up with the same component
defined 37 times at slightly different sizes, visibly changing as you navigate.

A page `<style>` block is for things genuinely unique to that page — a topic
tree, a workflow canvas, a drag-and-drop zone. If you are typing `.btn`,
`.modal`, `.form-control`, `.data-table`, `.status-badge`, `.metric-card`,
`.section-card`, `.loading-indicator` or `.error-message`, stop: it already
exists in `assets/components.css`. If it needs a variant, add the variant there.

## Where things live

| File | Owns |
|---|---|
| `assets/components.css` | Every recurring UI element. The design system. |
| `assets/monster-theme.css` | Brand tokens, scrollbars, auth indicator, the standalone login page. |
| `assets/ix-app.css` | Layout only — viewport, `.main-content`, `ix-menu` height. |
| `js/ui.js` | Modals, confirms, toasts, loading/error/empty states. |
| `js/sidebar.js` | Menu config and SPA navigation. Add new pages to `getMenuConfig()`. |

Loaded once from `index.html`. Pages do **not** link them — the router skips
duplicates anyway.

## Page anatomy

### List page

```
page-header          title + subtitle left, primary action right
error-message        hidden banner, driven by ui.showError()
metrics-grid         4–6 metric-cards, iX icons (never emoji)
data-table
  table-header       table title left, filters + refresh right
  table-responsive   the table scrolls here, never the page body
loading-indicator    hidden, driven by ui.setLoading()
```

### Detail page

```
page-header
  breadcrumb         Parent list › this entity. Replaces per-page Back buttons.
  title + subtitle
  page-header-actions  destructive first, primary last — always that order
error-message
section-card         one per logical group
  section-header     heading left, status badge or section action right
  section-content    form-grid / info-grid / table
loading-indicator
```

Detail pages have no Back button. The breadcrumb is the way back, and its last
segment is set from the loaded entity (`setText('breadcrumb-name', …)`).

## Interaction

Use `window.ui` — never `alert()`, `confirm()`, or a hand-rolled toast:

```js
if (await ui.confirmDelete(name, { title: 'Delete Redis client' })) { … }
ui.success('Client saved');           // auto-hides
ui.error('Failed: ' + e.message);     // stays until dismissed
ui.setLoading(true);                  // toggles #loading-indicator
ui.showError(msg); ui.clearError();   // the page's #error-message banner
tbody.innerHTML = ui.emptyRow(8, 'No clients configured', 'Hint text.');
ui.statusBadge('Enabled', 'ok');
```

Write actions carry `data-requires-auth` so guest mode hides them.

## Icons

iX icons only: `<ix-icon name="database" size="12">`. Names come from
`node_modules/@siemens/ix-icons/dist/ix-icons/svg/` — check the file exists
before using it; a wrong name renders as nothing.

Metric card icons take a tone class: `is-ok`, `is-warn`, `is-err`, `is-info`,
or none for the default accent. Green must always mean the same thing.

## Status colours

Four semantic states, defined once. Any `enabled` / `connected` / `online` /
`running` class resolves to the same green; `disabled` / `disconnected` /
`offline` / `error` to the same red.

## Third-party scripts

Never load from a CDN. The broker is deployed into air-gapped plant networks.
Add the package to `package.json`, register it in `VENDOR_BUNDLES` in
`vite.config.js`, and reference it as `/js/vendor/<name>.js`.

## Migration status

All 70 pages now draw their components from `components.css`; the ~5,600 lines
of duplicated component CSS that lived in page `<style>` blocks are gone. What
remains in those blocks is genuinely page-specific (topic trees, the workflow
canvas, the archive explorer grid, help-page typography).

Fully rebuilt on the concept — copy one of these when adding a page:

- list: `redis-clients`
- detail: `nats-client-detail`
- overview: `dashboard`, `sessions`

27 of the 43 hand-written delete-confirmation modals are gone, replaced by
`ui.confirmDelete()`. Two shapes were converted mechanically:

- list pages — `deleteX(name)` → `showConfirmDeleteModal()` → `confirmDeleteX()`
- detail pages — `showDeleteModal()` → `hideDeleteModal()` / `confirmDeleteX()`

Still outstanding:

- **16 delete modals remain**, on pages whose flow differs from those two shapes
  (`archive-groups`, `jdbc-loggers`, `influxdb-loggers`, `timebase-loggers`,
  `topic-namespaces`, `topic-schema-policies`, `opcua-servers`,
  `kafka-server-detail`, and friends). They share styling and get
  Escape/backdrop close from `ui`'s legacy-modal handler, so they behave
  consistently — they are just more code than they need to be. Convert
  opportunistically and exercise the delete path when you do.
- A handful of pages still build key/value displays as two-column tables where
  `.info-grid` would read better.

## Checking your work

Two scripts guard the wiring these refactors can silently break. Both should
report zero:

- every inline `onclick="foo()"` resolves to something the page's scripts define
- every `getElementById('…-modal')` / `e.target.id === '…-modal'` refers to a
  modal that still exists in the page

`node --check` on each file in `js/` is also worth running: `package.json` sets
`"type": "module"`, so Node parses them strictly and catches duplicate
declarations that the browser silently tolerates (that is how a shadowed
`showSuccessMessage` stub on the OPC UA servers page was found).
