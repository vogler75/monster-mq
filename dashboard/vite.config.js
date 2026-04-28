import { defineConfig } from 'vite';
import { resolve, join } from 'path';
import { readdirSync, readFileSync, copyFileSync, mkdirSync, existsSync, statSync } from 'fs';

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

// Build proxy entries from brokers.json
function buildProxyConfig() {
  const proxy = {};
  try {
    const brokers = JSON.parse(readFileSync(resolve(__dirname, 'src/config/brokers.json'), 'utf-8'));
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
  // Local broker: proxy /graphql and /graphqlws to localhost:4000
  proxy['/graphql'] = {
    target: 'http://localhost:4000',
    changeOrigin: true,
    secure: false
  };
  proxy['/graphqlws'] = {
    target: 'ws://localhost:4000',
    changeOrigin: true,
    secure: false,
    ws: true
  };
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
