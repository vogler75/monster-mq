/* =============================================================================
   MonsterMQ Dashboard — Shared UI interactions
   =============================================================================

   One implementation of the interaction patterns that were previously
   re-invented per page: 42 hand-written confirm modals, 17 files calling the
   native alert()/confirm(), and a single page using a toast.

   Loaded once from index.html; available everywhere as `window.ui`.

       await ui.confirm({ title: 'Delete client', message: '…', danger: true })
       ui.success('Client saved')
       ui.error('Failed to save: ' + e.message)
       const m = ui.modal({ title: 'Add mapping', body: formEl, footer: [...] })
       ui.setLoading(true, 'Loading Redis clients…')
       ui.showError('…')            // renders into the page's #error-message
       tbody.innerHTML = ui.emptyRow(8, 'No Redis clients configured')

   Styling for all of it lives in assets/components.css.
   ========================================================================== */

(function () {
    'use strict';

    // Re-running this file (the SPA re-evaluates page scripts) must not stack
    // duplicate listeners or lose the open-modal registry.
    if (window.ui && window.ui.__version === 1) return;

    var ICONS = {
        success: '✓',
        error: '✕',
        warning: '⚠',
        info: 'ℹ'
    };

    // ----------------------------------------------------------------- utils

    function el(tag, className, text) {
        var node = document.createElement(tag);
        if (className) node.className = className;
        if (text != null) node.textContent = text;
        return node;
    }

    // Everything user-supplied goes in as text, never as HTML.
    function setContent(target, content) {
        if (content == null) return;
        if (typeof content === 'string') {
            content.split('\n').forEach(function (line) {
                target.appendChild(el('p', null, line));
            });
        } else if (content instanceof Node) {
            target.appendChild(content);
        }
    }

    function escapeHtml(s) {
        return String(s == null ? '' : s)
            .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;').replace(/'/g, '&#39;');
    }

    // ---------------------------------------------------------------- toasts

    var toastStack = null;

    function getToastStack() {
        if (toastStack && toastStack.isConnected) return toastStack;
        toastStack = document.getElementById('mmq-toast-stack');
        if (!toastStack) {
            toastStack = el('div', 'toast-stack');
            toastStack.id = 'mmq-toast-stack';
            toastStack.setAttribute('role', 'status');
            toastStack.setAttribute('aria-live', 'polite');
            document.body.appendChild(toastStack);
        }
        return toastStack;
    }

    function dismissToast(node) {
        if (!node || node.classList.contains('is-leaving')) return;
        node.classList.add('is-leaving');
        // Fall back to a timer in case the animation never fires (reduced motion).
        var done = function () { if (node.parentNode) node.remove(); };
        node.addEventListener('animationend', done, { once: true });
        setTimeout(done, 400);
    }

    /**
     * Show a transient message. Errors stay until dismissed; everything else
     * auto-hides, because a success toast you have to click is just friction.
     */
    function toast(message, type, options) {
        type = type || 'info';
        options = options || {};
        var duration = options.duration != null
            ? options.duration
            : (type === 'error' ? 0 : 4000);

        var node = el('div', 'toast toast-' + type);
        node.appendChild(el('span', 'toast-icon', ICONS[type] || ICONS.info));
        node.appendChild(el('span', 'toast-text', String(message)));

        var close = el('button', 'toast-close', '×');
        close.type = 'button';
        close.setAttribute('aria-label', 'Dismiss');
        close.addEventListener('click', function () { dismissToast(node); });
        node.appendChild(close);

        getToastStack().appendChild(node);
        if (duration > 0) setTimeout(function () { dismissToast(node); }, duration);

        return { dismiss: function () { dismissToast(node); } };
    }

    // ---------------------------------------------------------------- modals

    var openModals = [];

    function trapFocus(container, event) {
        var focusable = container.querySelectorAll(
            'a[href], button:not([disabled]), input:not([disabled]):not([type="hidden"]),' +
            ' select:not([disabled]), textarea:not([disabled]), [tabindex]:not([tabindex="-1"])'
        );
        if (!focusable.length) return;
        var first = focusable[0];
        var last = focusable[focusable.length - 1];
        if (event.shiftKey && document.activeElement === first) {
            event.preventDefault();
            last.focus();
        } else if (!event.shiftKey && document.activeElement === last) {
            event.preventDefault();
            first.focus();
        }
    }

    /**
     * Build a modal dialog.
     *
     * @param {object}  opts
     * @param {string}  opts.title
     * @param {string|Node} opts.body
     * @param {Array}   [opts.footer]      [{ label, variant, onClick, autofocus }]
     * @param {string}  [opts.size]        'sm' | 'md' | 'lg'
     * @param {boolean} [opts.dismissable] backdrop click + Escape close (default true)
     * @param {Function}[opts.onClose]
     * @returns {{ el: HTMLElement, body: HTMLElement, close: Function }}
     */
    function modal(opts) {
        opts = opts || {};
        var dismissable = opts.dismissable !== false;
        var previouslyFocused = document.activeElement;

        var overlay = el('div', 'modal');
        overlay.setAttribute('role', 'dialog');
        overlay.setAttribute('aria-modal', 'true');

        var content = el('div', 'modal-content' + (opts.size ? ' modal-' + opts.size : ''));

        // Header
        var header = el('div', 'modal-header');
        var heading = el('h3', null, opts.title || '');
        var headingId = 'mmq-modal-title-' + Math.random().toString(36).slice(2, 9);
        heading.id = headingId;
        overlay.setAttribute('aria-labelledby', headingId);
        header.appendChild(heading);

        if (dismissable) {
            var closeBtn = el('button', 'modal-close', '×');
            closeBtn.type = 'button';
            closeBtn.setAttribute('aria-label', 'Close');
            closeBtn.addEventListener('click', function () { close(); });
            header.appendChild(closeBtn);
        }
        content.appendChild(header);

        // Body
        var body = el('div', 'modal-body');
        setContent(body, opts.body);
        content.appendChild(body);

        // Footer
        var autofocusTarget = null;
        if (opts.footer && opts.footer.length) {
            var footer = el('div', 'modal-footer');
            opts.footer.forEach(function (spec) {
                var btn = el('button', 'btn btn-' + (spec.variant || 'secondary'), spec.label);
                btn.type = 'button';
                if (spec.id) btn.id = spec.id;
                btn.addEventListener('click', function () { spec.onClick && spec.onClick(close); });
                if (spec.autofocus) autofocusTarget = btn;
                footer.appendChild(btn);
            });
            content.appendChild(footer);
        }

        overlay.appendChild(content);

        function onKeydown(e) {
            if (openModals[openModals.length - 1] !== handle) return;
            if (e.key === 'Escape' && dismissable) {
                e.stopPropagation();
                close();
            } else if (e.key === 'Tab') {
                trapFocus(content, e);
            }
        }

        function close() {
            if (!overlay.isConnected) return;
            document.removeEventListener('keydown', onKeydown, true);
            overlay.remove();
            var idx = openModals.indexOf(handle);
            if (idx !== -1) openModals.splice(idx, 1);
            if (previouslyFocused && previouslyFocused.focus) {
                try { previouslyFocused.focus(); } catch (e) { /* element gone */ }
            }
            opts.onClose && opts.onClose();
        }

        if (dismissable) {
            overlay.addEventListener('click', function (e) {
                if (e.target === overlay) close();
            });
        }
        document.addEventListener('keydown', onKeydown, true);

        var handle = { el: overlay, content: content, body: body, close: close };
        openModals.push(handle);
        document.body.appendChild(overlay);

        var focusTarget = autofocusTarget ||
            content.querySelector('input:not([type="hidden"]):not([disabled]), select, textarea') ||
            content.querySelector('.modal-footer .btn');
        if (focusTarget) {
            try { focusTarget.focus(); } catch (e) { /* not focusable */ }
        }

        return handle;
    }

    /**
     * Ask before doing something irreversible. Replaces window.confirm() and
     * the per-page delete modals.
     *
     * @returns {Promise<boolean>}
     */
    function confirm(opts) {
        if (typeof opts === 'string') opts = { message: opts };
        opts = opts || {};

        return new Promise(function (resolve) {
            var settled = false;
            function settle(value, close) {
                if (settled) return;
                settled = true;
                resolve(value);
                if (close) close();
            }

            modal({
                title: opts.title || 'Are you sure?',
                body: opts.message || '',
                size: 'sm',
                onClose: function () { settle(false); },
                footer: [
                    {
                        label: opts.cancelLabel || 'Cancel',
                        variant: 'secondary',
                        autofocus: true,
                        onClick: function (close) { settle(false, close); }
                    },
                    {
                        label: opts.confirmLabel || (opts.danger ? 'Delete' : 'Confirm'),
                        variant: opts.danger ? 'danger-solid' : 'primary',
                        onClick: function (close) { settle(true, close); }
                    }
                ]
            });
        });
    }

    /** Confirm a destructive action on a named entity. */
    function confirmDelete(name, opts) {
        opts = opts || {};
        return confirm({
            title: opts.title || 'Confirm delete',
            message: opts.message ||
                ('Delete "' + name + '"?\nThis action cannot be undone.'),
            confirmLabel: opts.confirmLabel || 'Delete',
            danger: true
        });
    }

    // ------------------------------------------------------- page-level state

    /**
     * Toggle the page's #loading-indicator. Pages keep their existing markup:
     *   <div id="loading-indicator" class="loading-indicator" style="display:none">
     */
    function setLoading(show, message) {
        var node = document.getElementById('loading-indicator');
        if (!node) return;
        if (message) {
            var label = node.querySelector('span');
            if (label) label.textContent = message;
        }
        node.style.display = show ? 'flex' : 'none';
    }

    /**
     * Render into the page's #error-message banner.
     * Pass autoHideMs = 0 to keep it until cleared.
     */
    function showError(message, autoHideMs) {
        var node = document.getElementById('error-message');
        if (!node) {
            toast(message, 'error');
            return;
        }
        var text = node.querySelector('.error-text');
        if (text) text.textContent = message;
        else node.textContent = message;
        node.style.display = 'flex';

        if (node.__mmqHideTimer) clearTimeout(node.__mmqHideTimer);
        var delay = autoHideMs == null ? 8000 : autoHideMs;
        if (delay > 0) {
            node.__mmqHideTimer = setTimeout(clearError, delay);
        }
    }

    function clearError() {
        var node = document.getElementById('error-message');
        if (!node) return;
        if (node.__mmqHideTimer) clearTimeout(node.__mmqHideTimer);
        node.style.display = 'none';
    }

    /**
     * Markup for the "no rows" state inside a table body.
     * Keeps the empty state identical everywhere instead of 20 variations of
     * <td colspan=7 style="text-align:center">No x found</td>.
     */
    function emptyRow(colspan, title, hint) {
        return '<tr><td class="empty-cell" colspan="' + Number(colspan) + '">' +
            '<div class="empty-state">' +
            '<div class="empty-state-title">' + escapeHtml(title) + '</div>' +
            (hint ? '<div class="empty-state-hint">' + escapeHtml(hint) + '</div>' : '') +
            '</div></td></tr>';
    }

    /** Markup for a status badge, so colour and casing never drift per page. */
    function statusBadge(label, state) {
        var cls = {
            ok: 'status-enabled', enabled: 'status-enabled', online: 'status-enabled',
            err: 'status-disabled', error: 'status-disabled', disabled: 'status-disabled',
            warn: 'status-pending', pending: 'status-pending',
            info: 'status-info', neutral: 'badge-neutral'
        }[state] || 'badge-neutral';
        return '<span class="status-badge ' + cls + '">' + escapeHtml(label) + '</span>';
    }

    /**
     * Breadcrumb markup for detail pages.
     *   ui.breadcrumb([{ label: 'Redis Clients', href: '/pages/redis-clients.html' },
     *                  { label: 'edge-01' }])
     */
    function breadcrumb(trail) {
        return '<nav class="breadcrumb" aria-label="Breadcrumb">' + trail.map(function (item, i) {
            var sep = i > 0 ? '<span class="breadcrumb-sep" aria-hidden="true">/</span>' : '';
            return sep + (item.href
                ? '<a href="' + escapeHtml(item.href) + '">' + escapeHtml(item.label) + '</a>'
                : '<span class="breadcrumb-current" aria-current="page">' +
                  escapeHtml(item.label) + '</span>');
        }).join('') + '</nav>';
    }

    // ------------------------------------------- legacy page-authored modals

    /**
     * Give Escape-to-close and backdrop-click-to-close to the ~40 pages that
     * still declare their own <div class="modal"> markup and toggle it with
     * style.display. Those dialogs already share their styling with ui.modal()
     * via components.css; this closes the behaviour gap too, without each page
     * having to be rewritten.
     *
     * Delegated and idempotent, so it survives SPA navigation.
     */
    function initLegacyModals() {
        if (window.__mmqLegacyModals) return;
        window.__mmqLegacyModals = true;

        var isOpen = function (el) {
            return el && el.classList.contains('modal') &&
                el.style.display !== 'none' && el.offsetParent !== null;
        };

        var openLegacy = function () {
            var found = null;
            document.querySelectorAll('.modal').forEach(function (m) {
                if (isOpen(m)) found = m;   // last one wins == topmost
            });
            return found;
        };

        var close = function (el) {
            el.style.display = 'none';
            // Let page code react if it tracks open state itself
            el.dispatchEvent(new CustomEvent('mmq:closed', { bubbles: true }));
        };

        // Backdrop click. Pages that already bind this get a harmless no-op,
        // because by then display is 'none' and isOpen() is false.
        document.addEventListener('click', function (e) {
            if (e.target.classList && e.target.classList.contains('modal') && isOpen(e.target)) {
                close(e.target);
            }
        });

        document.addEventListener('keydown', function (e) {
            if (e.key !== 'Escape') return;
            if (openModals.length) return;      // a ui.modal() dialog owns Escape
            var el = openLegacy();
            if (el) {
                e.stopPropagation();
                close(el);
            }
        });
    }

    // ------------------------------------------------------- breadcrumb sync

    var crumbObserver = null;

    /**
     * Keep a detail page's breadcrumb tail in step with its <h1>.
     *
     * Page scripts set #page-title once their entity has loaded, which is
     * asynchronous and happens in ~25 different places, so the breadcrumb
     * mirrors the heading instead of every page wiring it up by hand.
     * A leading "Type: " prefix is dropped, since the breadcrumb's parent
     * segment already says which type we are looking at.
     */
    function syncBreadcrumb() {
        if (crumbObserver) { crumbObserver.disconnect(); crumbObserver = null; }

        var crumb = document.getElementById('breadcrumb-name');
        var title = document.getElementById('page-title');
        if (!crumb || !title) return;

        var apply = function () {
            var text = (title.textContent || '').trim();
            var colon = text.indexOf(': ');
            crumb.textContent = colon > -1 ? text.slice(colon + 2) : text;
        };
        apply();

        crumbObserver = new MutationObserver(apply);
        crumbObserver.observe(title, { childList: true, characterData: true, subtree: true });
    }

    // ------------------------------------------------------------------ close

    /** Close every open modal — called by the SPA router before a page swap. */
    function closeAllModals() {
        openModals.slice().forEach(function (m) { m.close(); });
        if (crumbObserver) { crumbObserver.disconnect(); crumbObserver = null; }
    }

    window.ui = {
        __version: 1,
        toast: toast,
        success: function (m, o) { return toast(m, 'success', o); },
        error:   function (m, o) { return toast(m, 'error', o); },
        warning: function (m, o) { return toast(m, 'warning', o); },
        info:    function (m, o) { return toast(m, 'info', o); },
        modal: modal,
        confirm: confirm,
        confirmDelete: confirmDelete,
        closeAllModals: closeAllModals,
        setLoading: setLoading,
        showError: showError,
        clearError: clearError,
        emptyRow: emptyRow,
        statusBadge: statusBadge,
        breadcrumb: breadcrumb,
        syncBreadcrumb: syncBreadcrumb,
        escapeHtml: escapeHtml
    };

    initLegacyModals();
})();
