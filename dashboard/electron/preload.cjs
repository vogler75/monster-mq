const { contextBridge, ipcRenderer } = require('electron');

// Expose isElectron to the main window's renderer context
contextBridge.exposeInMainWorld('isElectron', true);

// Expose Electron configuration store API
contextBridge.exposeInMainWorld('ElectronAPI', {
  readConfig: () => ipcRenderer.invoke('desktop-config:read'),
  writeConfig: (config) => ipcRenderer.invoke('desktop-config:write', config),
  setActiveBroker: (name) => ipcRenderer.invoke('desktop-config:set-active-broker', name)
});
