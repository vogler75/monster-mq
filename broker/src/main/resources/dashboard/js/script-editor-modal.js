/**
 * Shared Script Editor Modal
 *
 * Usage:
 *   ScriptEditorModal.open({
 *     title: 'Edit Script',
 *     subtitle: 'Node: MyNode',
 *     initialScript: '// your code here',
 *     onSave: (script) => { console.log('Saved:', script); }
 *   });
 */

const ScriptEditorModal = (() => {
  let overlay = null;
  let textarea = null;
  let validationEl = null;
  let onSaveCallback = null;

  function init() {
    // Create modal HTML structure
    const html = `
      <div class="script-editor-overlay" id="script-editor-overlay">
        <div class="script-editor-modal">
          <div class="script-editor-header">
            <div class="script-editor-title-area">
              <h2 class="script-editor-title" id="script-editor-title">Edit Script</h2>
              <div class="script-editor-subtitle" id="script-editor-subtitle"></div>
            </div>
            <div class="script-editor-actions">
              <button class="script-editor-btn script-editor-btn-validate" id="script-editor-validate">
                <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                  <polyline points="20 6 9 17 4 12"></polyline>
                </svg>
                Validate
              </button>
              <button class="script-editor-btn script-editor-btn-save" id="script-editor-save">
                <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                  <path d="M19 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h11l5 5v11a2 2 0 0 1-2 2z"></path>
                  <polyline points="17 21 17 13 7 13 7 21"></polyline>
                  <polyline points="7 3 7 8 15 8"></polyline>
                </svg>
                Save & Close
              </button>
              <button class="script-editor-btn script-editor-btn-close" id="script-editor-close">
                <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                  <line x1="18" y1="6" x2="6" y2="18"></line>
                  <line x1="6" y1="6" x2="18" y2="18"></line>
                </svg>
                Close
              </button>
            </div>
          </div>
          <div class="script-editor-body">
            <textarea id="script-editor-textarea" class="script-editor-textarea" spellcheck="false" placeholder="// Write your script here..."></textarea>
            <div id="script-editor-validation" class="script-editor-validation"></div>
          </div>
        </div>
      </div>
    `;

    // Add to body if not already present
    if (!document.getElementById('script-editor-overlay')) {
      document.body.insertAdjacentHTML('beforeend', html);
    }

    // Get references
    overlay = document.getElementById('script-editor-overlay');
    textarea = document.getElementById('script-editor-textarea');
    validationEl = document.getElementById('script-editor-validation');

    // Setup event listeners
    document.getElementById('script-editor-validate').addEventListener('click', validate);
    document.getElementById('script-editor-save').addEventListener('click', saveAndClose);
    document.getElementById('script-editor-close').addEventListener('click', close);

    // Close on overlay click (but not on modal click)
    overlay.addEventListener('click', (e) => {
      if (e.target === overlay) {
        close();
      }
    });

    // Close on Escape key
    document.addEventListener('keydown', (e) => {
      if (e.key === 'Escape' && overlay && overlay.classList.contains('active')) {
        close();
      }
    });

    // Tab key support in textarea
    textarea.addEventListener('keydown', (e) => {
      if (e.key === 'Tab') {
        e.preventDefault();
        const start = textarea.selectionStart;
        const end = textarea.selectionEnd;
        textarea.value = textarea.value.substring(0, start) + '  ' + textarea.value.substring(end);
        textarea.selectionStart = textarea.selectionEnd = start + 2;
      }
    });

    // Auto-validate on blur
    textarea.addEventListener('blur', () => {
      if (textarea.value.trim()) {
        validate();
      }
    });

    // Clear validation on input
    textarea.addEventListener('input', () => {
      validationEl.style.display = 'none';
    });
  }

  function open(options = {}) {
    if (!overlay) {
      init();
    }

    const {
      title = 'Edit Script',
      subtitle = '',
      initialScript = '',
      onSave = null
    } = options;

    // Set content
    document.getElementById('script-editor-title').textContent = title;
    document.getElementById('script-editor-subtitle').textContent = subtitle;
    textarea.value = initialScript;
    onSaveCallback = onSave;

    // Clear validation
    validationEl.style.display = 'none';

    // Show overlay
    overlay.classList.add('active');
    textarea.focus();
  }

  function close() {
    if (overlay) {
      overlay.classList.remove('active');
      onSaveCallback = null;
    }
  }

  function validate() {
    const script = textarea.value.trim();

    if (!script) {
      validationEl.className = 'script-editor-validation warning';
      validationEl.style.display = 'block';
      validationEl.textContent = '⚠ Script is empty';
      return false;
    }

    try {
      // Validate JavaScript syntax using Function constructor
      new Function(script);
      validationEl.className = 'script-editor-validation success';
      validationEl.style.display = 'block';
      validationEl.textContent = '✓ JavaScript syntax is valid';
      return true;
    } catch (e) {
      validationEl.className = 'script-editor-validation error';
      validationEl.style.display = 'block';
      validationEl.textContent = `✗ Syntax error: ${e.message}`;
      return false;
    }
  }

  function saveAndClose() {
    const script = textarea.value;

    if (onSaveCallback) {
      onSaveCallback(script);
    }

    close();
  }

  function getScript() {
    return textarea ? textarea.value : '';
  }

  // Initialize when DOM is ready
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }

  return {
    open,
    close,
    validate,
    getScript
  };
})();

// Make globally available
window.ScriptEditorModal = ScriptEditorModal;
