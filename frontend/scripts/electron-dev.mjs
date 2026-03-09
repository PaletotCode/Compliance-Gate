import { spawn } from 'node:child_process'

const DEV_PORT = process.env.ELECTRON_VITE_PORT || '5174'
const DEV_URL = process.env.ELECTRON_DEV_SERVER_URL || `http://localhost:${DEV_PORT}`
const WAIT_TIMEOUT_MS = 120_000

function wait(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

async function waitForServer(url, timeoutMs) {
  const startedAt = Date.now()

  while (Date.now() - startedAt < timeoutMs) {
    try {
      const response = await fetch(url, { method: 'GET' })
      if (response.ok || response.status === 404) {
        return
      }
    } catch {
      // noop while server still starts
    }
    await wait(400)
  }

  throw new Error(`Timeout aguardando Vite em ${url}`)
}

function run() {
  const vite = spawn(
    process.platform === 'win32' ? 'npm.cmd' : 'npm',
    ['run', 'dev', '--', '--host', 'localhost', '--port', DEV_PORT],
    {
      stdio: 'inherit',
      env: {
        ...process.env,
        BROWSER: 'none',
      },
    },
  )

  const shutdown = () => {
    if (!vite.killed) {
      vite.kill('SIGTERM')
    }
  }

  process.on('SIGINT', () => {
    shutdown()
    process.exit(130)
  })

  process.on('SIGTERM', () => {
    shutdown()
    process.exit(143)
  })

  waitForServer(DEV_URL, WAIT_TIMEOUT_MS)
    .then(() => {
      const electron = spawn(
        process.platform === 'win32' ? 'npx.cmd' : 'npx',
        ['electron', '.'],
        {
          stdio: 'inherit',
          env: {
            ...process.env,
            ELECTRON_DEV_SERVER_URL: DEV_URL,
          },
        },
      )

      electron.on('exit', (code) => {
        shutdown()
        process.exit(code ?? 0)
      })
    })
    .catch((error) => {
      shutdown()
      console.error(`[electron:dev] ${error instanceof Error ? error.message : String(error)}`)
      process.exit(1)
    })

  vite.on('exit', (code) => {
    if (code && code !== 0) {
      process.exit(code)
    }
  })
}

run()
