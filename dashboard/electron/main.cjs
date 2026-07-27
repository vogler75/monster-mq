const { app, BrowserWindow, protocol, ipcMain, safeStorage } = require('electron');
const path = require('path');
const fs = require('fs');

const isDev = process.env.NODE_ENV === 'development';

const getConfigPath = () => {
  return path.join(app.getPath('userData'), 'config.json');
};

const getCredentialsPath = () => {
  return path.join(app.getPath('userData'), 'credentials.json');
};

const isTrustedRendererUrl = (rawUrl) => {
  try {
    const url = new URL(rawUrl);
    if (url.protocol === 'app:' && url.hostname === 'dist') {
      return true;
    }
    return isDev &&
      url.protocol === 'http:' &&
      (url.hostname === 'localhost' || url.hostname === '127.0.0.1') &&
      url.port === '5173';
  } catch (_) {
    return false;
  }
};

const assertTrustedIpcSender = (event) => {
  const senderUrl = event.senderFrame?.url || event.sender?.getURL() || '';
  if (!isTrustedRendererUrl(senderUrl)) {
    throw new Error('IPC request rejected from an untrusted renderer');
  }
};

const assertLoginIpcSender = (event) => {
  assertTrustedIpcSender(event);
  const senderUrl = event.senderFrame?.url || event.sender?.getURL() || '';
  if (new URL(senderUrl).pathname !== '/pages/login.html') {
    throw new Error('Credential request rejected outside the login page');
  }
};

const validateBrokerName = (name) => {
  if (typeof name !== 'string' || !name.trim() || name.length > 128) {
    throw new Error('Invalid broker name');
  }
  return name.trim();
};

const readConfig = () => {
  const filePath = getConfigPath();
  try {
    if (fs.existsSync(filePath)) {
      const data = fs.readFileSync(filePath, 'utf8');
      return JSON.parse(data);
    }
  } catch (e) {
    console.error('Error reading desktop config:', e);
  }
  return {
    brokers: [
      { name: 'Local', host: 'localhost', port: 4000, tls: false, default: true, endpoint: '/graphql' }
    ],
    activeBroker: 'Local'
  };
};

const writeConfig = (config) => {
  const filePath = getConfigPath();
  try {
    const dir = path.dirname(filePath);
    if (!fs.existsSync(dir)) {
      fs.mkdirSync(dir, { recursive: true });
    }
    fs.writeFileSync(filePath, JSON.stringify(config, null, 2), 'utf8');
    return true;
  } catch (e) {
    console.error('Error writing desktop config:', e);
    return false;
  }
};

const getCredentialStorageStatus = () => {
  if (!safeStorage.isEncryptionAvailable()) {
    return {
      available: false,
      message: 'Operating-system credential encryption is not available.'
    };
  }

  const backend = process.platform === 'linux' &&
    typeof safeStorage.getSelectedStorageBackend === 'function'
    ? safeStorage.getSelectedStorageBackend()
    : null;

  if (backend === 'basic_text') {
    return {
      available: false,
      message: 'No Linux secret store is available; plaintext fallback is disabled.'
    };
  }

  return { available: true, backend };
};

const readCredentialStore = () => {
  const filePath = getCredentialsPath();
  try {
    if (!fs.existsSync(filePath)) {
      return { version: 1, entries: [] };
    }
    const parsed = JSON.parse(fs.readFileSync(filePath, 'utf8'));
    if (parsed?.version !== 1 || !Array.isArray(parsed.entries)) {
      throw new Error('Unsupported credential store format');
    }
    return {
      version: 1,
      entries: parsed.entries.filter((entry) =>
        entry &&
        typeof entry.broker === 'string' &&
        typeof entry.encrypted === 'string'
      )
    };
  } catch (error) {
    console.error('Error reading encrypted desktop credentials:', error);
    return { version: 1, entries: [] };
  }
};

const writeCredentialStore = (store) => {
  const filePath = getCredentialsPath();
  const dir = path.dirname(filePath);
  const tempPath = `${filePath}.${process.pid}.tmp`;

  if (!fs.existsSync(dir)) {
    fs.mkdirSync(dir, { recursive: true, mode: 0o700 });
  }

  try {
    fs.writeFileSync(tempPath, JSON.stringify(store, null, 2), {
      encoding: 'utf8',
      mode: 0o600
    });
    fs.renameSync(tempPath, filePath);
    fs.chmodSync(filePath, 0o600);
  } finally {
    if (fs.existsSync(tempPath)) {
      fs.unlinkSync(tempPath);
    }
  }
};

const readCredential = (brokerName) => {
  const status = getCredentialStorageStatus();
  if (!status.available) return null;

  const broker = validateBrokerName(brokerName);
  const entry = readCredentialStore().entries.find((item) => item.broker === broker);
  if (!entry) return null;

  try {
    const decrypted = safeStorage.decryptString(Buffer.from(entry.encrypted, 'base64'));
    const credentials = JSON.parse(decrypted);
    if (
      typeof credentials.username !== 'string' ||
      typeof credentials.password !== 'string'
    ) {
      throw new Error('Invalid credential payload');
    }
    return credentials;
  } catch (error) {
    console.error(`Could not decrypt credentials for broker "${broker}":`, error);
    return null;
  }
};

const saveCredential = (brokerName, credentials) => {
  const status = getCredentialStorageStatus();
  if (!status.available) {
    throw new Error(status.message);
  }

  const broker = validateBrokerName(brokerName);
  if (
    !credentials ||
    typeof credentials.username !== 'string' ||
    typeof credentials.password !== 'string' ||
    credentials.username.length > 1024 ||
    credentials.password.length > 4096
  ) {
    throw new Error('Invalid credentials');
  }

  const encrypted = safeStorage
    .encryptString(JSON.stringify({
      username: credentials.username,
      password: credentials.password
    }))
    .toString('base64');
  const store = readCredentialStore();
  const existingIndex = store.entries.findIndex((entry) => entry.broker === broker);
  const entry = { broker, encrypted };

  if (existingIndex >= 0) {
    store.entries[existingIndex] = entry;
  } else {
    store.entries.push(entry);
  }
  writeCredentialStore(store);
  return true;
};

const removeCredential = (brokerName) => {
  const broker = validateBrokerName(brokerName);
  const store = readCredentialStore();
  const entries = store.entries.filter((entry) => entry.broker !== broker);
  if (entries.length !== store.entries.length) {
    store.entries = entries;
    writeCredentialStore(store);
  }
  return true;
};

// Register IPC handlers for configuration management
ipcMain.handle('desktop-config:read', (event) => {
  assertTrustedIpcSender(event);
  return readConfig();
});

ipcMain.handle('desktop-config:write', (event, config) => {
  assertTrustedIpcSender(event);
  return writeConfig(config);
});

ipcMain.handle('desktop-config:set-active-broker', (event, name) => {
  assertTrustedIpcSender(event);
  const config = readConfig();
  config.activeBroker = validateBrokerName(name);
  return writeConfig(config);
});

ipcMain.handle('desktop-credentials:status', (event) => {
  assertLoginIpcSender(event);
  return getCredentialStorageStatus();
});

ipcMain.handle('desktop-credentials:read', (event, brokerName) => {
  assertLoginIpcSender(event);
  return readCredential(brokerName);
});

ipcMain.handle('desktop-credentials:save', (event, brokerName, credentials) => {
  assertLoginIpcSender(event);
  return saveCredential(brokerName, credentials);
});

ipcMain.handle('desktop-credentials:remove', (event, brokerName) => {
  assertLoginIpcSender(event);
  return removeCredential(brokerName);
});

// Register 'app' as a standard and secure scheme
protocol.registerSchemesAsPrivileged([
  { scheme: 'app', privileges: { standard: true, secure: true, supportFetchAPI: true } }
]);

let mainWindow;

function createWindow() {
  mainWindow = new BrowserWindow({
    width: 1280,
    height: 800,
    webPreferences: {
      preload: path.join(__dirname, 'preload.cjs'),
      contextIsolation: true,
      nodeIntegration: false,
      sandbox: true,
      webSecurity: true,
      allowRunningInsecureContent: false
    }
  });

  mainWindow.webContents.setWindowOpenHandler(() => ({ action: 'deny' }));
  mainWindow.webContents.on('will-attach-webview', (event) => event.preventDefault());
  mainWindow.webContents.on('will-navigate', (event, targetUrl) => {
    if (!isTrustedRendererUrl(targetUrl)) {
      event.preventDefault();
    }
  });

  if (isDev) {
    mainWindow.loadURL('http://localhost:5173');
    mainWindow.webContents.openDevTools();
  } else {
    mainWindow.loadURL('app://dist/index.html');
  }

  mainWindow.on('closed', () => {
    mainWindow = null;
  });
}

app.whenReady().then(() => {
  // Set up custom protocol handler for app:// to serve local files correctly
  protocol.handle('app', (request) => {
    const url = request.url;
    let relativePath = '';
    try {
      const parsedUrl = new URL(url);
      relativePath = parsedUrl.pathname;
      console.log('App protocol requesting:', url, '-> parsed relativePath:', relativePath);
    } catch (e) {
      relativePath = url.replace(/^app:\/\//, '').split('?')[0].split('#')[0];
      console.log('App protocol requesting (fallback):', url, '-> parsed relativePath:', relativePath);
    }

    // Strip leading slash if present
    if (relativePath.startsWith('/')) {
      relativePath = relativePath.substring(1);
    }
    
    // Strip leading 'dist/' if present
    if (relativePath.startsWith('dist/')) {
      relativePath = relativePath.substring(5);
    }
    
    // Default to index.html if empty
    if (!relativePath || relativePath === '/') {
      relativePath = 'index.html';
    }

    // Resolve full path relative to the dashboard directory
    const distDir = path.resolve(__dirname, '../dist');
    const filePath = path.resolve(distDir, relativePath);
    const pathWithinDist = path.relative(distDir, filePath);

    // Prevent directory traversal attacks
    if (pathWithinDist.startsWith('..') || path.isAbsolute(pathWithinDist)) {
      return new Response('Access Denied', { status: 403 });
    }

    try {
      const data = fs.readFileSync(filePath);
      const ext = path.extname(filePath).toLowerCase();
      let mimeType = 'text/html';
      
      if (ext === '.js') mimeType = 'application/javascript';
      else if (ext === '.css') mimeType = 'text/css';
      else if (ext === '.svg') mimeType = 'image/svg+xml';
      else if (ext === '.png') mimeType = 'image/png';
      else if (ext === '.jpg' || ext === '.jpeg') mimeType = 'image/jpeg';
      else if (ext === '.json') mimeType = 'application/json';
      else if (ext === '.woff2') mimeType = 'font/woff2';
      else if (ext === '.woff') mimeType = 'font/woff';
      else if (ext === '.ttf') mimeType = 'font/ttf';

      return new Response(data, {
        headers: { 'content-type': mimeType }
      });
    } catch (e) {
      console.error(`Error serving path ${relativePath}:`, e);
      return new Response('Not Found', { status: 404 });
    }
  });

  createWindow();

  app.on('activate', () => {
    if (BrowserWindow.getAllWindows().length === 0) {
      createWindow();
    }
  });
});

app.on('window-all-closed', () => {
  if (process.platform !== 'darwin') {
    app.quit();
  }
});
