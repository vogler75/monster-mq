import { defineConfig } from 'vite';
import { resolve, join } from 'path';
import { readdirSync, copyFileSync, mkdirSync, existsSync, statSync } from 'fs';

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
        assetFileNames: 'assets/[name]-[hash][extname]'
      }
    }
  },
  plugins: [{
    name: 'copy-static',
    closeBundle() {
      const dest = resolve(__dirname, 'dist');

      // Copy dashboard source files
      copyDir(resolve(__dirname, 'src'), dest);

      // Copy iX icon SVGs
      const svgSrc = resolve(__dirname, 'node_modules/@siemens/ix-icons/dist/ix-icons/svg');
      const svgDest = join(dest, 'svg');
      copyDir(svgSrc, svgDest);
    }
  }],
  server: {
    port: 5173,
    proxy: {
      '/graphql': {
        target: 'http://localhost:4000',
        changeOrigin: true
      },
      '/graphqlws': {
        target: 'ws://localhost:4000',
        ws: true
      }
    }
  }
});
