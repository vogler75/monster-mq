const { app, BrowserWindow, protocol, ipcMain } = require('electron');
const path = require('path');
const fs = require('fs');

const getConfigPath = () => {
  return path.join(app.getPath('userData'), 'config.json');
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

// Register IPC handlers for configuration management
ipcMain.handle('desktop-config:read', () => {
  return readConfig();
});

ipcMain.handle('desktop-config:write', (event, config) => {
  return writeConfig(config);
});

ipcMain.handle('desktop-config:set-active-broker', (event, name) => {
  const config = readConfig();
  config.activeBroker = name;
  return writeConfig(config);
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
      nodeIntegration: false
    }
  });

  const isDev = process.env.NODE_ENV === 'development';

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
    const filePath = path.normalize(path.join(__dirname, '../dist', relativePath));
    const distDir = path.normalize(path.join(__dirname, '../dist'));

    // Prevent directory traversal attacks
    if (!filePath.startsWith(distDir)) {
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
