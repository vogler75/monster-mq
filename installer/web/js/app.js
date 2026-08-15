// MonsterMQ Setup & Interactive Schema-Driven Config Editor

const state = {
  currentStep: 1,
  systemInfo: null,
  releases: [],
  selectedRelease: null,
  schema: null,
  targetDir: '',
  config: {},
  activeCategory: 'network',
  viewMode: 'form', // 'form' or 'yaml'
  installDone: false
};

// Default Configuration Preset (SQLite standalone)
const defaultSQLiteConfig = {
  TCP: 1883,
  WS: 1884,
  NodeName: 'local',
  DefaultStoreType: 'SQLITE',
  QueuedMessagesEnabled: true,
  AllowRootWildcardSubscription: true,
  SQLite: {
    Path: 'sqlite',
    EnableWAL: true
  },
  GraphQL: {
    Enabled: true,
    Port: 4000,
    Path: '/graphql'
  },
  MCP: {
    Enabled: true,
    Port: 3000
  },
  UserManagement: {
    Enabled: false
  },
  Metrics: {
    Enabled: true
  },
  Logging: {
    Memory: {
      Enabled: true,
      Entries: 1000
    }
  },
  Features: {
    OpcUa: true,
    OpcUaServer: true,
    MqttClient: true,
    Kafka: true,
    Nats: true,
    Redis: true,
    RedisServer: true,
    KafkaServer: true,
    Telegram: true,
    WinCCOa: true,
    WinCCUa: true,
    Plc4x: true,
    Neo4j: true,
    JdbcLogger: true,
    InfluxDBLogger: true,
    TimeBaseLogger: true,
    SparkplugB: true,
    FlowEngine: true,
    Agents: true,
    GenAi: true,
    Mcp: true,
    SchemaPolicy: true,
    TopicNamespace: true,
    DeviceImportExport: true,
    Zenoh: true,
    Hmi: true,
    I3xClient: true
  }
};

// Category Mapping for Schema Properties
const categoryMapping = {
  network: ['TCP', 'WS', 'TCPS', 'WSS', 'NATS', 'GraphQL', 'MCP', 'Prometheus', 'I3x', 'RedisServer', 'KafkaServer'],
  storage: ['DefaultStoreType', 'SessionStoreType', 'QueueStoreType', 'RetainedStoreType', 'ConfigStoreType', 'SQLite', 'Postgres', 'MongoDB', 'CrateDB'],
  features: ['Features'],
  security: ['UserManagement', 'SSL', 'AllowRootWildcardSubscription'],
  extensions: ['RestApi', 'Dashboard', 'GenAI', 'Kafka', 'Zenoh'],
  tuning: ['MqttTcpServer', 'Queues', 'BulkMessaging', 'BulkProcessing', 'Metrics', 'Logging', 'MaxPublishRate', 'MaxSubscribeRate', 'QueueVisibilityTimeoutSeconds', 'MaxQueuedMessagesPerClient']
};

document.addEventListener('DOMContentLoaded', async () => {
  state.config = JSON.parse(JSON.stringify(defaultSQLiteConfig));
  setupNavigation();
  setupEditorControls();
  setupPresets();
  setupLaunchActions();

  await loadSystemInfo();
  await loadReleases();
  await loadSchema();

  renderActiveCategory();
});

// -------------------------------------------------------------
// Initialization & API Calls
// -------------------------------------------------------------
async function loadSystemInfo() {
  try {
    const res = await fetch('/api/system');
    state.systemInfo = await res.json();
    
    document.getElementById('sys-os').textContent = state.systemInfo.os || '-';
    document.getElementById('sys-arch').textContent = state.systemInfo.arch || '-';
    
    const javaText = state.systemInfo.javaInstalled 
      ? `Java ${state.systemInfo.javaVersion} (Major: ${state.systemInfo.javaMajor})` 
      : 'Not Detected';
    document.getElementById('sys-java').textContent = javaText;

    const pathRow = document.getElementById('java-path-row');
    const pathValue = document.getElementById('sys-java-path');
    if (state.systemInfo.javaPath) {
      pathRow.classList.remove('hidden');
      pathValue.textContent = state.systemInfo.javaPath;
    } else {
      pathRow.classList.add('hidden');
    }

    const javaPill = document.getElementById('java-status-pill');
    const javaWarning = document.getElementById('java-warning-box');
    const javaOk = document.getElementById('java-ok-box');

    if (state.systemInfo.javaSupported) {
      javaPill.className = 'status-pill success';
      javaPill.innerHTML = '<span class="dot"></span><span class="text">Java 21+ Ready</span>';
      javaOk.classList.remove('hidden');
      javaWarning.classList.add('hidden');
    } else {
      javaPill.className = 'status-pill warning';
      javaPill.innerHTML = '<span class="dot"></span><span class="text">Java 21+ Missing</span>';
      javaWarning.classList.remove('hidden');
      javaOk.classList.add('hidden');

      const warningHtml = (state.systemInfo.javaHelp || []).map(h => `<p style="margin-bottom: 4px;">${escapeHtml(h)}</p>`).join('');
      document.getElementById('java-warning-text').innerHTML = warningHtml;
    }

    if (state.systemInfo.javaDownload) {
      const downloadLink = document.getElementById('java-download-link');
      if (downloadLink) {
        downloadLink.href = state.systemInfo.javaDownload;
      }
    }

    // Set default target directory
    const dirInput = document.getElementById('install-dir-input');
    if (!state.targetDir) {
      dirInput.value = state.systemInfo.defaultDir || '';
      state.targetDir = dirInput.value;
      validateDirectory(state.targetDir);
    }

    dirInput.addEventListener('input', () => {
      state.targetDir = dirInput.value;
      validateDirectory(state.targetDir);
    });

    const btnRecheck = document.getElementById('btn-recheck-java');
    if (btnRecheck && !btnRecheck.dataset.wired) {
      btnRecheck.dataset.wired = 'true';
      btnRecheck.addEventListener('click', async () => {
        btnRecheck.textContent = 'Checking...';
        btnRecheck.disabled = true;
        await loadSystemInfo();
        btnRecheck.textContent = 'Re-check Java';
        btnRecheck.disabled = false;
      });
    }

  } catch (err) {
    console.error('Failed to load system info:', err);
  }
}

async function loadReleases() {
  try {
    const res = await fetch('/api/releases');
    state.releases = await res.json();

    const select = document.getElementById('release-select');
    select.innerHTML = '';

    if (!state.releases || state.releases.length === 0) {
      select.innerHTML = '<option value="">No releases found</option>';
      return;
    }

    state.releases.forEach((rel, index) => {
      const opt = document.createElement('option');
      opt.value = rel.tag_name;
      const dateStr = rel.published_at ? new Date(rel.published_at).toLocaleDateString() : '';
      const sizeMB = rel.broker_zip ? ` (${(rel.broker_zip.size / (1024 * 1024)).toFixed(1)} MB)` : '';
      opt.textContent = `${rel.name || rel.tag_name} ${sizeMB} — ${dateStr}`;
      if (index === 0) {
        opt.selected = true;
      }
      select.appendChild(opt);
    });

    state.selectedRelease = state.releases[0];
    updateReleaseDisplay(state.selectedRelease);

    select.addEventListener('change', () => {
      const found = state.releases.find(r => r.tag_name === select.value);
      if (found) {
        state.selectedRelease = found;
        updateReleaseDisplay(found);
      }
    });

  } catch (err) {
    console.error('Failed to load releases:', err);
    document.getElementById('release-notes-content').textContent = 'Could not load release notes from GitHub.';
  }
}

function updateReleaseDisplay(rel) {
  if (!rel) return;
  document.getElementById('version-tag-pill').textContent = rel.tag_name;
  document.getElementById('release-details').textContent = `Asset: ${rel.broker_zip ? rel.broker_zip.name : 'monstermq-broker.zip'}`;
  document.getElementById('release-notes-content').textContent = rel.body || 'No release notes provided.';
}

async function loadSchema() {
  try {
    const res = await fetch('/api/schema');
    state.schema = await res.json();
  } catch (err) {
    console.error('Failed to load schema:', err);
  }
}

async function validateDirectory(dirPath) {
  const statusHint = document.getElementById('dir-status');
  if (!dirPath || dirPath.trim() === '') {
    statusHint.textContent = 'Please enter an installation path.';
    statusHint.className = 'field-hint error';
    return false;
  }
  try {
    const res = await fetch('/api/validate-dir', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ path: dirPath })
    });
    const data = await res.json();
    if (data.valid) {
      statusHint.textContent = `✓ Target folder ready: ${data.absPath}`;
      statusHint.className = 'field-hint success';
      return true;
    } else {
      statusHint.textContent = `Error: ${data.error || 'Cannot write to target directory'}`;
      statusHint.className = 'field-hint error';
      return false;
    }
  } catch (err) {
    statusHint.textContent = 'Error validating path.';
    statusHint.className = 'field-hint error';
    return false;
  }
}

// -------------------------------------------------------------
// Wizard Stepper & Navigation
// -------------------------------------------------------------
function setupNavigation() {
  const btnNext = document.getElementById('btn-next-step');
  const btnPrev = document.getElementById('btn-prev-step');
  const btnExit = document.getElementById('btn-exit');

  btnNext.addEventListener('click', () => {
    if (state.currentStep === 1) {
      goToStep(2);
    } else if (state.currentStep === 2) {
      goToStep(3);
    } else if (state.currentStep === 3) {
      syncConfigFromActiveView();
      goToStep(4);
      startInstallation();
    } else if (state.currentStep === 4) {
      goToStep(5);
    }
  });

  btnPrev.addEventListener('click', () => {
    if (state.currentStep > 1 && state.currentStep !== 4) {
      goToStep(state.currentStep - 1);
    }
  });

  btnExit.addEventListener('click', () => {
    fetch('/api/exit', { method: 'POST' });
    window.close();
  });

  // Category sidebar clicks
  document.querySelectorAll('.category-item').forEach(item => {
    item.addEventListener('click', () => {
      document.querySelectorAll('.category-item').forEach(i => i.classList.remove('active'));
      item.classList.add('active');
      state.activeCategory = item.dataset.category;
      renderActiveCategory();
    });
  });

  // Search filter
  const searchInput = document.getElementById('config-search');
  if (searchInput) {
    searchInput.addEventListener('input', (e) => {
      const q = e.target.value.toLowerCase().trim();
      renderActiveCategory(q);
    });
  }
}

function goToStep(stepNumber) {
  state.currentStep = stepNumber;

  document.querySelectorAll('.wizard-step').forEach(step => step.classList.remove('active'));
  const current = document.getElementById(`step-${stepNumber}`);
  if (current) current.classList.add('active');

  // Update Stepper
  document.querySelectorAll('.step-item').forEach(item => {
    const s = parseInt(item.dataset.step, 10);
    item.classList.remove('active', 'completed');
    if (s === stepNumber) {
      item.classList.add('active');
    } else if (s < stepNumber) {
      item.classList.add('completed');
    }
  });

  // Update Footer buttons
  const btnNext = document.getElementById('btn-next-step');
  const btnPrev = document.getElementById('btn-prev-step');
  const btnExit = document.getElementById('btn-exit');

  if (stepNumber === 1) {
    btnPrev.classList.add('hidden');
    btnNext.textContent = 'Continue';
    btnNext.classList.remove('hidden');
    btnExit.classList.add('hidden');
  } else if (stepNumber === 2) {
    btnPrev.classList.remove('hidden');
    btnNext.textContent = 'Configure MonsterMQ';
    btnNext.classList.remove('hidden');
    btnExit.classList.add('hidden');
  } else if (stepNumber === 3) {
    btnPrev.classList.remove('hidden');
    btnNext.textContent = 'Install Now';
    btnNext.classList.remove('hidden');
    btnExit.classList.add('hidden');
  } else if (stepNumber === 4) {
    btnPrev.classList.add('hidden');
    btnNext.classList.add('hidden');
    btnExit.classList.add('hidden');
  } else if (stepNumber === 5) {
    btnPrev.classList.add('hidden');
    btnNext.classList.add('hidden');
    btnExit.classList.remove('hidden');
  }
}

// -------------------------------------------------------------
// Schema-Driven Config Editor
// -------------------------------------------------------------
function setupEditorControls() {
  const btnViewForm = document.getElementById('btn-view-form');
  const btnViewYaml = document.getElementById('btn-view-yaml');
  const formLayout = document.getElementById('config-form-view');
  const yamlLayout = document.getElementById('config-yaml-view');
  const yamlTextarea = document.getElementById('raw-yaml-input');

  btnViewForm.addEventListener('click', () => {
    if (state.viewMode === 'yaml') {
      try {
        state.config = parseYamlToObject(yamlTextarea.value);
        state.viewMode = 'form';
        btnViewForm.classList.add('active');
        btnViewYaml.classList.remove('active');
        formLayout.classList.remove('hidden');
        yamlLayout.classList.add('hidden');
        renderActiveCategory();
      } catch (err) {
        alert('YAML Syntax Error: ' + err.message);
      }
    }
  });

  btnViewYaml.addEventListener('click', () => {
    if (state.viewMode === 'form') {
      state.viewMode = 'yaml';
      btnViewYaml.classList.add('active');
      btnViewForm.classList.remove('active');
      yamlLayout.classList.remove('hidden');
      formLayout.classList.add('hidden');
      yamlTextarea.value = dumpObjectToYaml(state.config);
    }
  });
}

function setupPresets() {
  document.querySelectorAll('.preset-chip').forEach(chip => {
    chip.addEventListener('click', () => {
      document.querySelectorAll('.preset-chip').forEach(c => c.classList.remove('active'));
      chip.classList.add('active');
      applyPreset(chip.dataset.preset);
    });
  });
}

function applyPreset(presetName) {
  if (presetName === 'sqlite') {
    state.config = JSON.parse(JSON.stringify(defaultSQLiteConfig));
  } else if (presetName === 'postgres') {
    state.config = JSON.parse(JSON.stringify(defaultSQLiteConfig));
    state.config.DefaultStoreType = 'POSTGRES';
    state.config.Postgres = {
      Url: 'jdbc:postgresql://localhost:5432/monstermq',
      User: 'postgres',
      Pass: 'password',
      Schema: 'public'
    };
    delete state.config.SQLite;
  } else if (presetName === 'minimal') {
    state.config = {
      TCP: 1883,
      WS: 0,
      DefaultStoreType: 'SQLITE',
      SQLite: { Path: 'sqlite', EnableWAL: true },
      GraphQL: { Enabled: true, Port: 4000, Path: '/graphql' },
      Features: {
        MqttClient: true,
        WinCCUa: true,
        FlowEngine: true
      }
    };
  }

  if (state.viewMode === 'form') {
    renderActiveCategory();
  } else {
    document.getElementById('raw-yaml-input').value = dumpObjectToYaml(state.config);
  }
}

function syncConfigFromActiveView() {
  if (state.viewMode === 'yaml') {
    const yamlStr = document.getElementById('raw-yaml-input').value;
    state.config = parseYamlToObject(yamlStr);
  }
}

// -------------------------------------------------------------
// Rendering Schema Fields & Categories
// -------------------------------------------------------------
function renderActiveCategory(filterQuery = '') {
  const container = document.getElementById('category-content');
  container.innerHTML = '';

  if (!state.schema || !state.schema.properties) {
    container.innerHTML = '<div class="field-hint">Loading schema properties...</div>';
    return;
  }

  const propsToRender = categoryMapping[state.activeCategory] || [];

  if (state.activeCategory === 'features') {
    renderFeaturesSection(container, filterQuery);
    return;
  }

  propsToRender.forEach(propKey => {
    const propSchema = state.schema.properties[propKey];
    if (!propSchema) return;

    if (filterQuery) {
      const matchName = propKey.toLowerCase().includes(filterQuery);
      const matchTitle = (propSchema.title || '').toLowerCase().includes(filterQuery);
      const matchDesc = (propSchema.description || '').toLowerCase().includes(filterQuery);
      if (!matchName && !matchTitle && !matchDesc) return;
    }

    const card = document.createElement('div');
    card.className = 'config-field-card';

    const header = document.createElement('div');
    header.className = 'field-header-row';

    const titleGroup = document.createElement('div');
    titleGroup.className = 'field-title-group';

    const nameSpan = document.createElement('span');
    nameSpan.className = 'field-name';
    nameSpan.textContent = propSchema.title || propKey;

    const typeBadge = document.createElement('span');
    typeBadge.className = 'field-type-badge';
    typeBadge.textContent = propSchema.type || 'any';

    titleGroup.appendChild(nameSpan);
    titleGroup.appendChild(typeBadge);
    header.appendChild(titleGroup);
    card.appendChild(header);

    if (propSchema.description) {
      const doc = document.createElement('div');
      doc.className = 'field-doc';
      doc.textContent = propSchema.description;
      card.appendChild(doc);
    }

    const controlRow = document.createElement('div');
    controlRow.className = 'field-control-row';

    const currentValue = state.config[propKey];
    const inputElement = buildFieldControl(propKey, propSchema, currentValue, (newVal) => {
      if (newVal === undefined || newVal === '') {
        delete state.config[propKey];
      } else {
        state.config[propKey] = newVal;
      }
    });

    controlRow.appendChild(inputElement);
    card.appendChild(controlRow);
    container.appendChild(card);
  });
}

function renderFeaturesSection(container, filterQuery = '') {
  const featuresSchema = state.schema.properties.Features;
  if (!featuresSchema || !featuresSchema.properties) return;

  const card = document.createElement('div');
  card.className = 'config-field-card';

  const titleGroup = document.createElement('div');
  titleGroup.className = 'field-title-group';
  titleGroup.innerHTML = '<span class="field-name">Broker Subsystems & Bridges</span><span class="field-type-badge">27 Features</span>';
  card.appendChild(titleGroup);

  const doc = document.createElement('div');
  doc.className = 'field-doc';
  doc.textContent = featuresSchema.description || 'Enable or disable optional broker protocols and subsystems.';
  card.appendChild(doc);

  const grid = document.createElement('div');
  grid.className = 'features-grid';

  if (!state.config.Features) {
    state.config.Features = {};
  }

  Object.keys(featuresSchema.properties).forEach(flagKey => {
    const flagSchema = featuresSchema.properties[flagKey];
    if (filterQuery) {
      const match = flagKey.toLowerCase().includes(filterQuery) || 
                    (flagSchema.title || '').toLowerCase().includes(filterQuery) ||
                    (flagSchema.description || '').toLowerCase().includes(filterQuery);
      if (!match) return;
    }

    const isChecked = state.config.Features[flagKey] !== false;

    const featureCard = document.createElement('div');
    featureCard.className = 'feature-toggle-card';

    const switchLabel = document.createElement('label');
    switchLabel.className = 'switch-label';

    const checkbox = document.createElement('input');
    checkbox.type = 'checkbox';
    checkbox.className = 'switch-input';
    checkbox.checked = isChecked;

    checkbox.addEventListener('change', () => {
      state.config.Features[flagKey] = checkbox.checked;
    });

    const slider = document.createElement('span');
    slider.className = 'switch-slider';

    switchLabel.appendChild(checkbox);
    switchLabel.appendChild(slider);

    const info = document.createElement('div');
    info.className = 'feature-info';

    const name = document.createElement('div');
    name.className = 'feature-name';
    name.textContent = flagSchema.title || flagKey;

    const desc = document.createElement('div');
    desc.className = 'feature-desc';
    desc.textContent = flagSchema.description || '';

    info.appendChild(name);
    info.appendChild(desc);

    featureCard.appendChild(switchLabel);
    featureCard.appendChild(info);
    grid.appendChild(featureCard);
  });

  card.appendChild(grid);
  container.appendChild(card);
}

function buildFieldControl(propKey, schema, value, onChange) {
  const type = schema.type;

  // Boolean Switch
  if (type === 'boolean') {
    const wrapper = document.createElement('div');
    const switchLabel = document.createElement('label');
    switchLabel.className = 'switch-label';

    const input = document.createElement('input');
    input.type = 'checkbox';
    input.className = 'switch-input';
    input.checked = value !== undefined ? Boolean(value) : Boolean(schema.default);

    input.addEventListener('change', () => {
      onChange(input.checked);
    });

    const slider = document.createElement('span');
    slider.className = 'switch-slider';

    const text = document.createElement('span');
    text.className = 'switch-text';
    text.textContent = input.checked ? 'Enabled' : 'Disabled';

    input.addEventListener('change', () => {
      text.textContent = input.checked ? 'Enabled' : 'Disabled';
    });

    switchLabel.appendChild(input);
    switchLabel.appendChild(slider);
    switchLabel.appendChild(text);
    wrapper.appendChild(switchLabel);
    return wrapper;
  }

  // Enum Dropdown
  if (schema.enum && Array.isArray(schema.enum)) {
    const wrapper = document.createElement('div');
    wrapper.style.display = 'flex';
    wrapper.style.flexDirection = 'column';
    wrapper.style.gap = '4px';

    const select = document.createElement('select');
    const isStoreTypeField = propKey.toLowerCase().includes('store') || propKey.toLowerCase().includes('type');
    const currentDefault = state.config.DefaultStoreType || 'SQLITE';

    schema.enum.forEach(optionVal => {
      const opt = document.createElement('option');
      opt.value = optionVal;

      if (optionVal === 'DEFAULT') {
        opt.textContent = `DEFAULT (Inherit from DefaultStoreType: ${currentDefault})`;
      } else if (optionVal === 'NONE') {
        opt.textContent = 'NONE (Disabled / No persistence)';
      } else {
        opt.textContent = optionVal;
      }

      if (value === optionVal || (value === undefined && schema.default === optionVal)) {
        opt.selected = true;
      }
      select.appendChild(opt);
    });

    select.addEventListener('change', () => {
      const chosen = select.value;
      if (chosen === 'DEFAULT') {
        onChange(undefined);
      } else {
        onChange(chosen);
      }

      if (propKey === 'DefaultStoreType') {
        renderActiveCategory();
      }
    });

    wrapper.appendChild(select);

    if (isStoreTypeField && (value === undefined || value === 'DEFAULT')) {
      const inheritHint = document.createElement('div');
      inheritHint.className = 'field-hint';
      inheritHint.style.fontSize = '11px';
      inheritHint.textContent = `↳ Currently inherits '${currentDefault}' from DefaultStoreType.`;
      wrapper.appendChild(inheritHint);
    }

    return wrapper;
  }

  // Integer / Number Input
  if (type === 'integer' || type === 'number') {
    const input = document.createElement('input');
    input.type = 'number';
    input.className = 'text-input';
    if (schema.minimum !== undefined) input.min = schema.minimum;
    if (schema.maximum !== undefined) input.max = schema.maximum;
    input.value = value !== undefined ? value : (schema.default !== undefined ? schema.default : '');

    input.addEventListener('input', () => {
      const num = parseInt(input.value, 10);
      onChange(isNaN(num) ? undefined : num);
    });
    return input;
  }

  // Object / Sub-schema
  if (type === 'object' && schema.properties) {
    const objContainer = document.createElement('div');
    objContainer.style.display = 'flex';
    objContainer.style.flexDirection = 'column';
    objContainer.style.gap = '10px';
    objContainer.style.marginTop = '6px';

    const currentObj = (typeof value === 'object' && value !== null) ? value : {};

    Object.keys(schema.properties).forEach(subKey => {
      const subSchema = schema.properties[subKey];
      const subCard = document.createElement('div');
      subCard.style.padding = '8px 12px';
      subCard.style.background = 'rgba(0,0,0,0.2)';
      subCard.style.borderRadius = '4px';
      subCard.style.border = '1px solid #374151';

      const subHeader = document.createElement('div');
      subHeader.style.display = 'flex';
      subHeader.style.justifyContent = 'space-between';
      subHeader.style.marginBottom = '4px';

      const subTitle = document.createElement('span');
      subTitle.style.fontWeight = '600';
      subTitle.style.fontSize = '12px';
      subTitle.textContent = subSchema.title || subKey;

      subHeader.appendChild(subTitle);
      subCard.appendChild(subHeader);

      if (subSchema.description) {
        const subDoc = document.createElement('div');
        subDoc.style.fontSize = '11px';
        subDoc.style.color = '#9ca3af';
        subDoc.style.marginBottom = '6px';
        subDoc.textContent = subSchema.description;
        subCard.appendChild(subDoc);
      }

      const subCtrl = buildFieldControl(subKey, subSchema, currentObj[subKey], (newSubVal) => {
        if (newSubVal === undefined || newSubVal === '') {
          delete currentObj[subKey];
        } else {
          currentObj[subKey] = newSubVal;
        }
        onChange(Object.keys(currentObj).length > 0 ? currentObj : undefined);
      });

      subCard.appendChild(subCtrl);
      objContainer.appendChild(subCard);
    });

    return objContainer;
  }

  // Generic String Text Input
  const input = document.createElement('input');
  input.type = 'text';
  input.className = 'text-input';
  input.value = value !== undefined ? value : (schema.default !== undefined ? schema.default : '');
  if (schema.examples && schema.examples.length > 0) {
    input.placeholder = schema.examples[0];
  }

  input.addEventListener('input', () => {
    onChange(input.value.trim());
  });
  return input;
}

// -------------------------------------------------------------
// Installation Execution & SSE Stream
// -------------------------------------------------------------
function startInstallation() {
  const stageTitle = document.getElementById('install-stage-title');
  const stageMsg = document.getElementById('install-stage-msg');
  const progressBar = document.getElementById('install-progress-bar');

  const downloadUrl = (state.selectedRelease && state.selectedRelease.broker_zip)
    ? state.selectedRelease.broker_zip.browser_download_url
    : '';

  const version = state.selectedRelease ? state.selectedRelease.tag_name : 'latest';

  const rawConfig = dumpObjectToYaml(state.config);

  const payload = {
    targetDir: state.targetDir,
    downloadUrl: downloadUrl,
    version: version,
    rawConfig: rawConfig,
    configValues: state.config
  };

  fetch('/api/install', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload)
  }).then(response => {
    const reader = response.body.getReader();
    const decoder = new TextDecoder('utf-8');
    let buffer = '';

    function readStream() {
      reader.read().then(({ done, value }) => {
        if (done) return;
        buffer += decoder.decode(value, { stream: true });
        const lines = buffer.split('\n');
        buffer = lines.pop(); // keep unfinished line

        let currentEvent = 'message';
        for (const line of lines) {
          if (line.startsWith('event: ')) {
            currentEvent = line.substring(7).trim();
          } else if (line.startsWith('data: ')) {
            const dataStr = line.substring(6).trim();
            if (!dataStr) continue;
            try {
              const data = JSON.parse(dataStr);
              handleInstallEvent(currentEvent, data);
            } catch (e) {
              console.error('SSE JSON error:', e);
            }
          }
        }
        readStream();
      });
    }
    readStream();
  }).catch(err => {
    stageTitle.textContent = 'Installation Error';
    stageMsg.textContent = err.message;
  });
}

function handleInstallEvent(event, data) {
  const stageTitle = document.getElementById('install-stage-title');
  const stageMsg = document.getElementById('install-stage-msg');
  const progressBar = document.getElementById('install-progress-bar');

  if (event === 'progress') {
    progressBar.style.width = `${Math.min(data.percent, 100)}%`;
    stageMsg.textContent = data.message;

    // Update steps checklist
    const stageId = `pstep-${data.stage}`;
    const stepEl = document.getElementById(stageId);
    if (stepEl) {
      stepEl.className = 'pstep-item active';
      stepEl.querySelector('.pstep-icon').textContent = '▶';
    }

    if (data.stage === 'download') {
      markStepDone('pstep-prepare');
      stageTitle.textContent = 'Downloading Package';
    } else if (data.stage === 'extract') {
      markStepDone('pstep-download');
      stageTitle.textContent = 'Extracting Files';
    } else if (data.stage === 'configure') {
      markStepDone('pstep-extract');
      stageTitle.textContent = 'Writing Configuration';
    } else if (data.stage === 'finalize') {
      markStepDone('pstep-configure');
      stageTitle.textContent = 'Finalizing Setup';
    }
  } else if (event === 'done') {
    markStepDone('pstep-finalize');
    progressBar.style.width = '100%';
    stageTitle.textContent = 'Installation Complete!';
    stageMsg.textContent = data.message || 'MonsterMQ is installed and ready.';
    state.installDone = true;

    setTimeout(() => {
      goToStep(5);
    }, 800);
  } else if (event === 'error') {
    stageTitle.textContent = 'Installation Failed';
    stageMsg.textContent = data.error || 'An unexpected error occurred.';
    progressBar.style.background = '#ef4444';
  }
}

function markStepDone(id) {
  const el = document.getElementById(id);
  if (el) {
    el.className = 'pstep-item done';
    el.querySelector('.pstep-icon').textContent = '✓';
  }
}

// -------------------------------------------------------------
// Launch Actions (Step 5)
// -------------------------------------------------------------
function setupLaunchActions() {
  const btnStart = document.getElementById('btn-start-broker');
  const btnDashboard = document.getElementById('btn-open-dashboard');
  const btnFolder = document.getElementById('btn-open-folder');
  const statusBadge = document.getElementById('broker-status-badge');
  const consoleBox = document.getElementById('console-logs');

  btnStart.addEventListener('click', () => {
    btnStart.disabled = true;
    btnStart.textContent = 'Starting...';
    consoleBox.innerHTML = '<div class="log-line" style="color:#38bdf8">Launching MonsterMQ broker process...</div>';

    fetch('/api/start-broker', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ targetDir: state.targetDir })
    }).then(response => {
      const reader = response.body.getReader();
      const decoder = new TextDecoder('utf-8');
      let buffer = '';

      function readLogs() {
        reader.read().then(({ done, value }) => {
          if (done) return;
          buffer += decoder.decode(value, { stream: true });
          const lines = buffer.split('\n');
          buffer = lines.pop();

          let currentEvent = 'message';
          for (const line of lines) {
            if (line.startsWith('event: ')) {
              currentEvent = line.substring(7).trim();
            } else if (line.startsWith('data: ')) {
              const dataStr = line.substring(6).trim();
              if (!dataStr) continue;
              try {
                const data = JSON.parse(dataStr);
                if (currentEvent === 'log' && data.line) {
                  appendConsoleLog(data.line);
                } else if (currentEvent === 'started') {
                  statusBadge.className = 'badge-pill success';
                  statusBadge.textContent = 'Running';
                  btnStart.textContent = 'Running';
                  btnDashboard.disabled = false;
                } else if (currentEvent === 'error') {
                  appendConsoleLog(`[ERROR] ${data.error}`);
                  btnStart.disabled = false;
                  btnStart.textContent = 'Start MonsterMQ';
                }
              } catch (e) {}
            }
          }
          readLogs();
        });
      }
      readLogs();
    }).catch(err => {
      appendConsoleLog(`[ERROR] ${err.message}`);
      btnStart.disabled = false;
      btnStart.textContent = 'Start MonsterMQ';
    });
  });

  btnDashboard.addEventListener('click', () => {
    fetch('/api/open-dashboard');
  });

  btnFolder.addEventListener('click', () => {
    fetch('/api/open-folder', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ path: state.targetDir })
    });
  });
}

function appendConsoleLog(line) {
  const consoleBox = document.getElementById('console-logs');
  const div = document.createElement('div');
  div.className = 'log-line';
  div.textContent = line;
  consoleBox.appendChild(div);
  consoleBox.scrollTop = consoleBox.scrollHeight;
}

// -------------------------------------------------------------
// YAML Serializer / Deserializer Helpers
// -------------------------------------------------------------
function dumpObjectToYaml(obj, indent = 0) {
  let yaml = '';
  const spaces = '  '.repeat(indent);

  for (const key of Object.keys(obj)) {
    const val = obj[key];
    if (val === undefined) continue;

    if (val === null) {
      yaml += `${spaces}${key}: null\n`;
    } else if (typeof val === 'boolean') {
      yaml += `${spaces}${key}: ${val}\n`;
    } else if (typeof val === 'number') {
      yaml += `${spaces}${key}: ${val}\n`;
    } else if (typeof val === 'string') {
      if (val.includes('\n') || val.includes(':') || val.includes('#')) {
        yaml += `${spaces}${key}: "${val.replace(/"/g, '\\"')}"\n`;
      } else {
        yaml += `${spaces}${key}: ${val}\n`;
      }
    } else if (Array.isArray(val)) {
      if (val.length === 0) {
        yaml += `${spaces}${key}: []\n`;
      } else if (val.every(item => typeof item === 'string' || typeof item === 'number')) {
        yaml += `${spaces}${key}: [${val.map(v => typeof v === 'string' ? `"${v}"` : v).join(', ')}]\n`;
      } else {
        yaml += `${spaces}${key}:\n`;
        for (const item of val) {
          if (typeof item === 'object') {
            const nested = dumpObjectToYaml(item, indent + 2);
            const firstLine = nested.split('\n')[0];
            const rest = nested.split('\n').slice(1).join('\n');
            yaml += `${spaces}  - ${firstLine.trim()}\n${rest}`;
          } else {
            yaml += `${spaces}  - ${item}\n`;
          }
        }
      }
    } else if (typeof val === 'object') {
      const inner = dumpObjectToYaml(val, indent + 1);
      if (inner.trim()) {
        yaml += `${spaces}${key}:\n${inner}`;
      } else {
        yaml += `${spaces}${key}: {}\n`;
      }
    }
  }
  return yaml;
}

function parseYamlToObject(yamlStr) {
  // Simple YAML line-based parser suitable for standard broker configs
  const lines = yamlStr.split('\n');
  const root = {};
  const stack = [{ obj: root, indent: -1 }];

  for (let rawLine of lines) {
    const commentIdx = rawLine.indexOf('#');
    let line = commentIdx >= 0 ? rawLine.substring(0, commentIdx) : rawLine;
    if (!line.trim()) continue;

    const indent = line.search(/\S/);
    line = line.trim();

    while (stack.length > 1 && stack[stack.length - 1].indent >= indent) {
      stack.pop();
    }

    const currentContext = stack[stack.length - 1].obj;

    const colonIdx = line.indexOf(':');
    if (colonIdx === -1) continue;

    const key = line.substring(0, colonIdx).trim();
    let valStr = line.substring(colonIdx + 1).trim();

    if (valStr === '') {
      const newObj = {};
      currentContext[key] = newObj;
      stack.push({ obj: newObj, indent: indent });
    } else {
      currentContext[key] = parsePrimitive(valStr);
    }
  }
  return root;
}

function parsePrimitive(valStr) {
  if (valStr === 'true') return true;
  if (valStr === 'false') return false;
  if (valStr === 'null') return null;
  if (!isNaN(valStr) && valStr !== '') return Number(valStr);
  if (valStr.startsWith('[') && valStr.endsWith(']')) {
    const inside = valStr.substring(1, valStr.length - 1).trim();
    if (!inside) return [];
    return inside.split(',').map(s => parsePrimitive(s.trim()));
  }
  if ((valStr.startsWith('"') && valStr.endsWith('"')) || (valStr.startsWith("'") && valStr.endsWith("'"))) {
    return valStr.substring(1, valStr.length - 1);
  }
  return valStr;
}

function escapeHtml(str) {
  if (!str) return '';
  return str.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
}
