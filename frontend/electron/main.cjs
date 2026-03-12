const { app, BrowserWindow, shell, session } = require('electron')
const fs = require('node:fs')
const http = require('node:http')
const path = require('node:path')

const isDev = Boolean(process.env.ELECTRON_DEV_SERVER_URL)
const preloadPath = path.join(__dirname, 'preload.cjs')
const distPath = path.join(__dirname, '..', 'dist')

let mainWindow = null
let staticServer = null

function contentTypeFor(filePath) {
  if (filePath.endsWith('.html')) return 'text/html; charset=utf-8'
  if (filePath.endsWith('.js')) return 'application/javascript; charset=utf-8'
  if (filePath.endsWith('.css')) return 'text/css; charset=utf-8'
  if (filePath.endsWith('.json')) return 'application/json; charset=utf-8'
  if (filePath.endsWith('.svg')) return 'image/svg+xml'
  if (filePath.endsWith('.png')) return 'image/png'
  if (filePath.endsWith('.jpg') || filePath.endsWith('.jpeg')) return 'image/jpeg'
  if (filePath.endsWith('.woff2')) return 'font/woff2'
  if (filePath.endsWith('.woff')) return 'font/woff'
  return 'application/octet-stream'
}

function applySecurityHeaders(res) {
  res.setHeader('X-Content-Type-Options', 'nosniff')
  res.setHeader('X-Frame-Options', 'DENY')
  res.setHeader('Referrer-Policy', 'strict-origin-when-cross-origin')
  res.setHeader('Permissions-Policy', 'geolocation=(), microphone=(), camera=()')
  res.setHeader('Cross-Origin-Opener-Policy', 'same-origin')
  res.setHeader('Cross-Origin-Resource-Policy', 'same-origin')
  res.setHeader('X-Permitted-Cross-Domain-Policies', 'none')
  res.setHeader(
    'Content-Security-Policy',
    "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data: blob:; font-src 'self' data:; connect-src 'self' http: https: ws: wss:; frame-ancestors 'none'; base-uri 'self'; object-src 'none'; form-action 'self';",
  )
}

function sanitizePath(urlPath) {
  const normalized = path.normalize(urlPath.replace(/^\/+/, ''))
  return normalized.startsWith('..') ? '' : normalized
}

function resolveStaticFile(staticRoot, requestPath) {
  const safePath = sanitizePath(requestPath)
  const preferred = safePath ? path.join(staticRoot, safePath) : path.join(staticRoot, 'index.html')

  if (!preferred.startsWith(staticRoot)) {
    return null
  }

  if (fs.existsSync(preferred) && fs.statSync(preferred).isFile()) {
    return preferred
  }

  return path.join(staticRoot, 'index.html')
}

function startStaticServer(staticRoot) {
  return new Promise((resolve, reject) => {
    if (!fs.existsSync(path.join(staticRoot, 'index.html'))) {
      reject(new Error('Build não encontrado. Execute npm run build antes de iniciar o Electron em produção.'))
      return
    }

    const server = http.createServer((req, res) => {
      const urlPath = (() => {
        try {
          return new URL(req.url || '/', 'http://localhost').pathname
        } catch {
          return '/'
        }
      })()

      const filePath = resolveStaticFile(staticRoot, urlPath)
      if (!filePath) {
        res.statusCode = 403
        res.end('Forbidden')
        return
      }

      fs.readFile(filePath, (error, data) => {
        if (error) {
          res.statusCode = 404
          res.end('Not found')
          return
        }
        applySecurityHeaders(res)
        res.setHeader('Content-Type', contentTypeFor(filePath))
        res.end(data)
      })
    })

    server.on('error', reject)
    server.listen(0, 'localhost', () => {
      resolve(server)
    })
  })
}

function getServerUrl(server) {
  const address = server.address()
  if (!address || typeof address === 'string') {
    throw new Error('Não foi possível resolver endereço do servidor local do Electron.')
  }
  return `http://localhost:${address.port}`
}

function wireSecurityGuards(window, allowedOrigin) {
  const normalizedAllowed = allowedOrigin.replace(/\/$/, '')

  window.webContents.setWindowOpenHandler(({ url }) => {
    if (url.startsWith(normalizedAllowed)) {
      return { action: 'allow' }
    }

    void shell.openExternal(url)
    return { action: 'deny' }
  })

  window.webContents.on('will-navigate', (event, nextUrl) => {
    if (!nextUrl.startsWith(normalizedAllowed)) {
      event.preventDefault()
    }
  })
}

async function createWindow() {
  mainWindow = new BrowserWindow({
    width: 1560,
    height: 940,
    minWidth: 1200,
    minHeight: 760,
    title: 'Compliance Gate',
    backgroundColor: '#020202',
    autoHideMenuBar: true,
    webPreferences: {
      preload: preloadPath,
      contextIsolation: true,
      nodeIntegration: false,
      sandbox: true,
      spellcheck: false,
    },
  })

  const appUrl = isDev
    ? process.env.ELECTRON_DEV_SERVER_URL
    : getServerUrl(staticServer)

  wireSecurityGuards(mainWindow, appUrl)

  await mainWindow.loadURL(appUrl)

  if (isDev) {
    mainWindow.webContents.openDevTools({ mode: 'detach' })
  }
}

app.whenReady().then(async () => {
  session.defaultSession.setPermissionRequestHandler((_wc, _permission, callback) => {
    callback(false)
  })

  if (!isDev) {
    staticServer = await startStaticServer(distPath)
  }

  await createWindow()

  app.on('activate', async () => {
    if (BrowserWindow.getAllWindows().length === 0) {
      await createWindow()
    }
  })
})

app.on('window-all-closed', () => {
  if (process.platform !== 'darwin') {
    app.quit()
  }
})

app.on('before-quit', () => {
  if (staticServer) {
    staticServer.close()
    staticServer = null
  }
})
