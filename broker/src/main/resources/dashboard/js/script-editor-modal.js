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
  let nodeInputs = '';
  let nodeOutputs = '';
  let aiPanel = null;
  let aiInput = null;
  let aiFeedback = null;

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
              <button class="script-editor-btn script-editor-btn-help" onclick="openHelp('script-api')" title="Help: Script API Reference">
                <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                  <circle cx="12" cy="12" r="10"></circle>
                  <path d="M9.09 9a3 3 0 0 1 5.83 1c0 2-3 3-3 3"></path>
                  <line x1="12" y1="17" x2="12.01" y2="17"></line>
                </svg>
                Help
              </button>
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
          <div class="script-editor-ai-panel" id="script-editor-ai-panel">
            <div class="script-editor-ai-header">
              <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                <path d="M12 2L2 7l10 5 10-5-10-5z"></path>
                <path d="M2 17l10 5 10-5M2 12l10 5 10-5"></path>
              </svg>
              AI Assistant
            </div>
            <div class="script-editor-ai-input-area">
              <input type="text" id="script-editor-ai-input" class="script-editor-ai-input" placeholder="Ask AI to help with your code... (select code to focus on specific lines)" />
              <button id="script-editor-ai-send" class="script-editor-ai-send" title="Send to AI">
                <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                  <line x1="22" y1="2" x2="11" y2="13"></line>
                  <polygon points="22 2 15 22 11 13 2 9 22 2"></polygon>
                </svg>
              </button>
            </div>
            <div id="script-editor-ai-feedback" class="script-editor-ai-feedback" style="display: none;"></div>
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
    aiPanel = document.getElementById('script-editor-ai-panel');
    aiInput = document.getElementById('script-editor-ai-input');
    aiFeedback = document.getElementById('script-editor-ai-feedback');

    // Setup event listeners
    document.getElementById('script-editor-validate').addEventListener('click', validate);
    document.getElementById('script-editor-save').addEventListener('click', saveAndClose);
    document.getElementById('script-editor-close').addEventListener('click', close);

    // AI Assistant event listeners
    document.getElementById('script-editor-ai-send').addEventListener('click', askAI);
    aiInput.addEventListener('keydown', (e) => {
      if (e.key === 'Enter') {
        e.preventDefault();
        askAI();
      }
    });

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
      onSave = null,
      inputs = '',
      outputs = ''
    } = options;

    // Set content
    document.getElementById('script-editor-title').textContent = title;
    document.getElementById('script-editor-subtitle').textContent = subtitle;
    textarea.value = initialScript;
    onSaveCallback = onSave;
    nodeInputs = inputs;
    nodeOutputs = outputs;

    // Clear validation and AI feedback
    validationEl.style.display = 'none';
    aiFeedback.style.display = 'none';
    aiFeedback.innerHTML = '';
    aiInput.value = '';

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

  async function askAI() {
    const question = aiInput.value.trim();
    if (!question) {
      return;
    }

    const sendBtn = document.getElementById('script-editor-ai-send');
    sendBtn.disabled = true;
    aiFeedback.style.display = 'block';
    aiFeedback.className = 'script-editor-ai-feedback loading';
    aiFeedback.innerHTML = '<div class="ai-loading">⏳ AI is thinking...</div>';

    try {
      // Get current script
      const fullScript = textarea.value;

      // Get selected text
      const selectionStart = textarea.selectionStart;
      const selectionEnd = textarea.selectionEnd;
      const selectedText = textarea.value.substring(selectionStart, selectionEnd);
      const hasSelection = selectedText.length > 0;

      // Build context
      const contextParts = [];
      contextParts.push(`Node Configuration:`);
      contextParts.push(`- Inputs: ${nodeInputs || 'none'}`);
      contextParts.push(`- Outputs: ${nodeOutputs || 'none'}`);

      if (hasSelection) {
        contextParts.push(`\nUser has selected these lines to modify:`);
        contextParts.push('```javascript');
        contextParts.push(selectedText);
        contextParts.push('```');
      }

      const context = contextParts.join('\n');

      // Build optimized prompt
      const promptParts = [];
      promptParts.push('You are a code assistant for MonsterMQ workflow nodes.');
      promptParts.push('');

      if (hasSelection) {
        promptParts.push('**IMPORTANT**: The user has selected specific lines in their code.');
        promptParts.push('Focus ONLY on modifying those selected lines.');
        promptParts.push('Return the COMPLETE script with ONLY the selected lines changed.');
        promptParts.push('Keep all other code unchanged.');
        promptParts.push('');
      }

      promptParts.push(`User's question: ${question}`);
      promptParts.push('');
      promptParts.push('Current script:');
      promptParts.push('```javascript');
      promptParts.push(fullScript || '// Empty script');
      promptParts.push('```');
      promptParts.push('');
      promptParts.push('Return:');
      promptParts.push('1. The modified JavaScript code in a code block');
      promptParts.push('2. A brief explanation of what changed');
      promptParts.push('');
      promptParts.push('Format your response as:');
      promptParts.push('```javascript');
      promptParts.push('// ... complete updated code ...');
      promptParts.push('```');
      promptParts.push('');
      promptParts.push('**Explanation:** Your explanation here');

      const prompt = promptParts.join('\n');

      // Call GraphQL API
      const response = await fetch('/graphql', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          query: `
            query AskAI($prompt: String!, $context: String, $docs: [String!]) {
              genai {
                ask(prompt: $prompt, context: $context, docs: $docs) {
                  response
                  model
                  error
                }
              }
            }
          `,
          variables: {
            prompt: prompt,
            context: context,
            docs: ['script-assistant.md', 'workflow-reference.md']
          }
        })
      });

      const result = await response.json();

      if (result.errors) {
        throw new Error(result.errors[0].message);
      }

      const aiResponse = result.data?.genai?.ask;

      if (aiResponse.error) {
        throw new Error(aiResponse.error);
      }

      const responseText = aiResponse.response;

      // Extract code from response (look for ```javascript code blocks)
      const codeBlockRegex = /```(?:javascript|js)?\s*\n([\s\S]*?)\n```/i;
      const codeMatch = responseText.match(codeBlockRegex);

      let extractedCode = null;
      if (codeMatch && codeMatch[1]) {
        extractedCode = codeMatch[1].trim();
      }

      // Extract explanation (look for text after code block or after **Explanation:**)
      let explanation = '';
      if (codeMatch) {
        const afterCode = responseText.substring(codeMatch.index + codeMatch[0].length).trim();
        const explMatch = afterCode.match(/\*\*Explanation:\*\*\s*([\s\S]+)/i);
        explanation = explMatch ? explMatch[1].trim() : afterCode;
      } else {
        explanation = responseText;
      }

      // Update editor if code was found
      if (extractedCode) {
        textarea.value = extractedCode;
        aiFeedback.className = 'script-editor-ai-feedback success';
        aiFeedback.innerHTML = `
          <div class="ai-success-header">✓ Code updated by AI (${aiResponse.model})</div>
          ${explanation ? `<div class="ai-explanation">${escapeHtml(explanation)}</div>` : ''}
        `;
      } else {
        aiFeedback.className = 'script-editor-ai-feedback info';
        aiFeedback.innerHTML = `
          <div class="ai-info-header">💡 AI Response (${aiResponse.model})</div>
          <div class="ai-explanation">${escapeHtml(responseText)}</div>
        `;
      }

    } catch (error) {
      console.error('AI Assistant error:', error);
      aiFeedback.className = 'script-editor-ai-feedback error';
      aiFeedback.innerHTML = `
        <div class="ai-error-header">✗ Error</div>
        <div class="ai-explanation">${escapeHtml(error.message)}</div>
      `;
    } finally {
      sendBtn.disabled = false;
    }
  }

  function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
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
