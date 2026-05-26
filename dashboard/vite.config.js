import { defineConfig } from 'vite';
import { resolve, join } from 'path';
import { readdirSync, readFileSync, writeFileSync, copyFileSync, mkdirSync, existsSync, statSync } from 'fs';

// Recursively copy files, skipping those already produced by rollup
function copyDir(src, dest) {
  if (!existsSync(dest)) mkdirSync(dest, { recursive: true });
  for (const entry of readdirSync(src, { withFileTypes: true })) {
    if (entry.name === '.DS_Store' || entry.name === 'node_modules') continue;
    const srcPath = join(src, entry.name);
    const destPath = join(dest, entry.name);
    const realStat = statSync(srcPath, { throwIfNoEntry: false });
    if (!realStat) continue;
    if (realStat.isDirectory()) {
      copyDir(srcPath, destPath);
    } else if (!existsSync(destPath)) {
      copyFileSync(srcPath, destPath);
    }
  }
}

const BROKERS_JSON_PATH = resolve(__dirname, 'src/config/brokers.json');
const BROKER_DASHBOARD_CONFIG_PATH = resolve(__dirname, 'src/config/config.json');
const BROKER_DASHBOARD_INSTANCE_CONFIG_PATH = resolve(__dirname, 'src/config/config-instance.json');

function normalizeBrokerEntry(rawBroker, index) {
  if (!rawBroker || typeof rawBroker !== 'object') return null;

  const name = typeof rawBroker.name === 'string' ? rawBroker.name.trim() : '';
  if (!name) return null;

  const host = typeof rawBroker.host === 'string' ? rawBroker.host.trim() : '';
  const rawPort = rawBroker.port;
  const parsedPort = Number(rawPort);
  const port = Number.isFinite(parsedPort) && parsedPort > 0 && Number.isInteger(parsedPort)
    ? parsedPort
    : 0;
  const tls = rawBroker.tls === true;
  const endpoint = typeof rawBroker.endpoint === 'string' && rawBroker.endpoint.trim()
    ? rawBroker.endpoint.trim()
    : '/graphql';

  return {
    name,
    host,
    port,
    tls,
    endpoint,
    default: false,
    __sourceIndex: index
  };
}

function normalizeBrokersList(rawBrokers) {
  if (!Array.isArray(rawBrokers)) return [];

  const normalized = [];
  const seenNames = new Set();

  for (let i = 0; i < rawBrokers.length; i++) {
    const broker = normalizeBrokerEntry(rawBrokers[i], i);
    if (!broker) continue;
    if (seenNames.has(broker.name)) continue;
    seenNames.add(broker.name);
    normalized.push(broker);
  }

  const requestedDefault = normalized.findIndex((entry) => rawBrokers[entry.__sourceIndex]?.default === true);
  if (requestedDefault >= 0) {
    normalized.forEach((entry, idx) => {
      entry.default = idx === requestedDefault;
    });
  } else if (normalized.length > 0) {
    normalized[0].default = true;
  }

  return normalized.map(({ __sourceIndex, ...entry }) => entry);
}

function readJsonConfig(configPath) {
  try {
    const content = readFileSync(configPath, 'utf-8');
    return JSON.parse(content);
  } catch (e) {
    return null;
  }
}

function readDashboardConfig() {
  const brokersFileConfig = (() => {
    const parsedBrokersFile = readJsonConfig(BROKERS_JSON_PATH);
    return Array.isArray(parsedBrokersFile) ? parsedBrokersFile : [];
  })();

  const fallbackConfig = {
    adminToken: '',
    brokers: brokersFileConfig
  };

  const fileConfig = readJsonConfig(BROKER_DASHBOARD_INSTANCE_CONFIG_PATH) || readJsonConfig(BROKER_DASHBOARD_CONFIG_PATH) || fallbackConfig;
  return {
    adminToken: typeof fileConfig.adminToken === 'string' ? fileConfig.adminToken : '',
    brokers: Array.isArray(fileConfig.brokers) ? fileConfig.brokers : fallbackConfig.brokers
  };
}

function getDashboardAdminToken(config) {
  return (typeof config.adminToken === 'string' ? config.adminToken : '').trim();
}

function writeDashboardConfig(updatedBrokers, existingConfig) {
  const normalized = normalizeBrokersList(updatedBrokers);
  const source = {
    ...(typeof existingConfig === 'object' && existingConfig ? existingConfig : {}),
    brokers: normalized,
    version: Date.now()
  };

  writeFileSync(BROKER_DASHBOARD_INSTANCE_CONFIG_PATH, JSON.stringify(source, null, 2));

  return normalized;
}

function getAuthToken(req) {
  const header = req.headers?.authorization || '';
  if (typeof header === 'string' && header.startsWith('Bearer ')) {
    return header.slice('Bearer '.length).trim();
  }
  if (typeof header === 'string' && header.trim()) return header.trim();

  const dashboardToken = req.headers?.['x-dashboard-token'];
  return typeof dashboardToken === 'string' ? dashboardToken.trim() : '';
}

async function readJsonBody(req) {
  return await new Promise((resolve, reject) => {
    let data = '';

    req.on('data', (chunk) => {
      data += chunk;
      if (data.length > 5_000_000) {
        reject(new Error('Request body too large'));
        req.destroy();
      }
    });

    req.on('end', () => {
      if (!data) {
        resolve({});
        return;
      }

      try {
        resolve(JSON.parse(data));
      } catch (e) {
        reject(new Error('Invalid JSON body'));
      }
    });

    req.on('error', (error) => {
      reject(error);
    });
  });
}

function sendJson(res, statusCode, payload) {
  res.statusCode = statusCode;
  res.setHeader('Content-Type', 'application/json; charset=utf-8');
  res.end(JSON.stringify(payload));
}

// Build proxy entries from brokers.json
function buildProxyConfig() {
  const proxy = {};
  try {
    const config = readDashboardConfig();
    const brokers = normalizeBrokersList(config.brokers);
    for (const broker of brokers) {
      if (!broker.host) continue; // Local broker only works in production (served by broker)

      const target = `${broker.tls ? 'https' : 'http'}://${broker.host}:${broker.port}`;
      const name = encodeURIComponent(broker.name);
      proxy[`/broker-api/${name}`] = {
        target,
        changeOrigin: true,
        secure: false,
        rewrite: (path) => path.replace(new RegExp(`^/broker-api/${name}`), '')
      };
      proxy[`/broker-ws/${name}`] = {
        target: target.replace(/^http/, 'ws'),
        changeOrigin: true,
        secure: false,
        ws: true,
        rewrite: (path) => path.replace(new RegExp(`^/broker-ws/${name}`), '')
      };
    }
  } catch (e) {
    console.warn('brokers.json not found — using local broker only. Copy src/config/brokers.json.example to src/config/brokers.json to add remote brokers.');
  }
  const localTarget = process.env.VITE_LOCAL_GRAPHQL_TARGET;
  if (localTarget) {
    const target = localTarget.replace(/\/$/, '');
    proxy['/graphql'] = {
      target,
      changeOrigin: true,
      secure: false
    };
    proxy['/graphqlws'] = {
      target: target.replace(/^http/, 'ws'),
      changeOrigin: true,
      secure: false,
      ws: true
    };
  }
  return proxy;
}

export default defineConfig({
  root: 'src',
  publicDir: false,
  build: {
    outDir: '../dist',
    emptyOutDir: true,
    rollupOptions: {
      input: {
        'ix-init': resolve(__dirname, 'src/js/ix-init.js')
      },
      output: {
        entryFileNames: 'js/[name].js',
        chunkFileNames: 'js/[name]-[hash].js',
        assetFileNames: (assetInfo) => {
          // Give the main CSS a stable name so index.html can reference it
          if (assetInfo.names && assetInfo.names.includes('ix-init.css')) {
            return 'assets/ix-init.css';
          }
          return 'assets/[name]-[hash][extname]';
        }
      }
    }
  },
  plugins: [{
    name: 'copy-static',
    configureServer(server) {
      const svgRoot = resolve(__dirname, 'node_modules/@siemens/ix-icons/dist/ix-icons/svg');
      const svgRootPrefix = `${svgRoot}/`;
      const configRoot = resolve(__dirname, 'src/config');

      server.middlewares.use(async (req, res, next) => {
        const url = req.url?.split('?')[0] || '';

        if (url === '/api/brokers') {
          try {
            const method = req.method || 'GET';
            const config = readDashboardConfig();

            if (method === 'GET') {
              const providedToken = getAuthToken(req);
              if (providedToken) {
                const expectedToken = getDashboardAdminToken(config);
                if (!expectedToken) {
                  sendJson(res, 403, {
                    error: 'Broker admin token is not configured. Set config.json:adminToken or config-instance.json:adminToken to enable broker list updates.'
                  });
                  return;
                }
                if (providedToken !== expectedToken) {
                  sendJson(res, 401, { error: 'Invalid broker admin token.' });
                  return;
                }
              }
              const brokers = normalizeBrokersList(config.brokers);
              sendJson(res, 200, { brokers });
              return;
            }

            if (method !== 'PUT' && method !== 'POST') {
              res.statusCode = 405;
              res.setHeader('Allow', 'GET, POST, PUT');
              res.setHeader('Content-Type', 'application/json; charset=utf-8');
              res.end(JSON.stringify({ error: 'Method not allowed' }));
              return;
            }

            const expectedToken = getDashboardAdminToken(config);
            if (!expectedToken) {
              sendJson(res, 403, {
                error: 'Broker admin token is not configured. Set config.json:adminToken or config-instance.json:adminToken to enable broker list updates.'
              });
              return;
            }

            const providedToken = getAuthToken(req);
            if (providedToken !== expectedToken) {
              sendJson(res, 401, { error: 'Invalid broker admin token.' });
              return;
            }

            const body = await readJsonBody(req);
            const nextBrokers = body?.brokers ?? body;
            const normalized = normalizeBrokersList(nextBrokers);
            if (!Array.isArray(nextBrokers) || normalized.length === 0) {
              sendJson(res, 400, { error: 'Payload must contain a non-empty brokers array.' });
              return;
            }

            const savedBrokers = writeDashboardConfig(normalized, config);
            sendJson(res, 200, { brokers: savedBrokers });
            return;
          } catch (e) {
            sendJson(res, 500, { error: e.message || 'Failed to process /api/brokers request.' });
            return;
          }
        }

        if (url.startsWith('/svg/') && url.endsWith('.svg')) {
          const fileName = decodeURIComponent(url.slice('/svg/'.length));
          const svgPath = resolve(svgRoot, fileName);
          if (!svgPath.startsWith(svgRootPrefix) || !existsSync(svgPath)) {
            res.statusCode = 404;
            res.end();
            return;
          }
          res.setHeader('Content-Type', 'image/svg+xml');
          res.end(readFileSync(svgPath));
          return;
        }

        if (url === '/config/brokers.json') {
          const brokersPath = resolve(configRoot, 'brokers.json');
          if (!existsSync(brokersPath)) {
            res.statusCode = 404;
            res.end();
            return;
          }
        }

        if (url === '/graphql' || url === '/graphqlws') {
          res.statusCode = 503;
          res.setHeader('Content-Type', 'text/plain');
          res.end('No local GraphQL proxy is configured. Select a broker from brokers.json or set VITE_LOCAL_GRAPHQL_TARGET.');
          return;
        }

        next();
      });
    },
    closeBundle() {
      const dest = resolve(__dirname, 'dist');
      copyDir(resolve(__dirname, 'src'), dest);
      const svgSrc = resolve(__dirname, 'node_modules/@siemens/ix-icons/dist/ix-icons/svg');
      const svgDest = join(dest, 'svg');
      copyDir(svgSrc, svgDest);
    }
  }],
  server: {
    port: 5173,
    proxy: buildProxyConfig()
  }
});
