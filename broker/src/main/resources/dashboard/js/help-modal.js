/**
 * Help Modal System
 * Displays workflow help documentation in an overlay modal
 */

const HelpModal = (() => {
  let overlay = null;
  let iframe = null;

  function init() {
    // Create modal HTML structure
    const html = `
      <div class="help-modal-overlay" id="help-modal-overlay">
        <div class="help-modal-container">
          <div class="help-modal-header">
            <h2 class="help-modal-title">Workflow Help</h2>
            <button class="help-modal-close" id="help-modal-close" title="Close Help">
              <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                <line x1="18" y1="6" x2="6" y2="18"></line>
                <line x1="6" y1="6" x2="18" y2="18"></line>
              </svg>
            </button>
          </div>
          <iframe id="help-modal-iframe" class="help-modal-iframe"></iframe>
        </div>
      </div>
    `;

    // Add to body if not already present
    if (!document.getElementById('help-modal-overlay')) {
      document.body.insertAdjacentHTML('beforeend', html);
    }

    // Get references
    overlay = document.getElementById('help-modal-overlay');
    iframe = document.getElementById('help-modal-iframe');

    // Setup event listeners
    document.getElementById('help-modal-close').addEventListener('click', close);

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

    // Add CSS styles
    addStyles();
  }

  function addStyles() {
    if (document.getElementById('help-modal-styles')) {
      return;
    }

    const style = document.createElement('style');
    style.id = 'help-modal-styles';
    style.textContent = `
      .help-modal-overlay {
        display: none;
        position: fixed;
        top: 0;
        left: 0;
        right: 0;
        bottom: 0;
        background: rgba(0, 0, 0, 0.75);
        z-index: 10001;
        backdrop-filter: blur(4px);
      }

      .help-modal-overlay.active {
        display: flex;
        align-items: center;
        justify-content: center;
        animation: helpFadeIn 0.2s ease;
      }

      @keyframes helpFadeIn {
        from { opacity: 0; }
        to { opacity: 1; }
      }

      .help-modal-container {
        background: #0f1117;
        border: 1px solid #2d3139;
        border-radius: 12px;
        width: 95%;
        max-width: 1400px;
        height: 90vh;
        display: flex;
        flex-direction: column;
        box-shadow: 0 20px 60px rgba(0, 0, 0, 0.5);
        animation: helpSlideUp 0.3s ease;
      }

      @keyframes helpSlideUp {
        from {
          transform: translateY(30px);
          opacity: 0;
        }
        to {
          transform: translateY(0);
          opacity: 1;
        }
      }

      .help-modal-header {
        background: #1a1d28;
        border-bottom: 1px solid #2d3139;
        padding: 1rem 1.5rem;
        display: flex;
        justify-content: space-between;
        align-items: center;
        border-top-left-radius: 12px;
        border-top-right-radius: 12px;
      }

      .help-modal-title {
        font-size: 1.1rem;
        font-weight: 600;
        color: #e8eaed;
        margin: 0;
      }

      .help-modal-close {
        background: transparent;
        border: none;
        color: #9ca3af;
        cursor: pointer;
        padding: 0.5rem;
        border-radius: 6px;
        display: flex;
        align-items: center;
        justify-content: center;
        transition: all 0.2s;
      }

      .help-modal-close:hover {
        background: #2d3139;
        color: #e8eaed;
      }

      .help-modal-iframe {
        flex: 1;
        width: 100%;
        border: none;
        border-bottom-left-radius: 12px;
        border-bottom-right-radius: 12px;
        background: var(--background-primary, #1a1d28);
      }
    `;
    document.head.appendChild(style);
  }

  function open(url, section) {
    if (!overlay) {
      init();
    }

    // Build URL with section anchor if provided
    const fullUrl = section ? `${url}#${section}` : url;

    // Set iframe source
    iframe.src = fullUrl;

    // Show overlay
    overlay.classList.add('active');
  }

  function close() {
    if (overlay) {
      overlay.classList.remove('active');
      // Clear iframe after animation
      setTimeout(() => {
        if (iframe) {
          iframe.src = '';
        }
      }, 300);
    }
  }

  // Initialize when DOM is ready
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }

  return {
    open,
    close
  };
})();

// Make globally available
window.HelpModal = HelpModal;

// Global helper function to open help
window.openHelp = function(section) {
  HelpModal.open('/pages/workflow-help.html', section);
};
