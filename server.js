import express from 'express'
import multer from 'multer'
import archiver from 'archiver'
import WebTorrent from 'webtorrent'
import path from 'node:path'
import fs from 'node:fs'
import os from 'node:os'
import crypto from 'node:crypto'

const PORT = process.env.PORT || 3000
const TMP_ROOT = path.join(os.tmpdir(), 'torrent2zip')
const requestedMaxConns = Number.parseInt(process.env.TORRENT_MAX_CONNS ?? '200', 10)
const maxConns = Number.isInteger(requestedMaxConns)
  ? Math.min(Math.max(requestedMaxConns, 1), 500)
  : 200
const JOB_TTL = 30 * 60 * 1000
fs.mkdirSync(TMP_ROOT, { recursive: true })

const app = express()
const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 5 * 1024 * 1024 } // un .torrent fait quelques Ko
})

const client = new WebTorrent({ maxConns })
client.on('error', (err) => console.error('[webtorrent]', err))
const jobs = new Map()

app.use(express.static('public'))

/* ---------- 1) Upload du .torrent -> démarre le téléchargement ---------- */
app.post('/api/upload', upload.single('torrent'), async (req, res) => {
  if (!req.file) return res.status(400).json({ error: 'Aucun fichier .torrent reçu' })

  const existingTorrent = await client.get(req.file.buffer)
  if (existingTorrent) {
    const existingJob = [...jobs.values()].find(job => job.torrent === existingTorrent)
    if (existingJob) return res.json({ id: existingJob.id })
    return res.status(409).json({ error: 'Ce torrent est déjà en cours de traitement.' })
  }

  const id = crypto.randomUUID()
  const dir = path.join(TMP_ROOT, id)
  fs.mkdirSync(dir, { recursive: true })

  let torrent
  try {
    torrent = client.add(req.file.buffer, { path: dir })
  } catch (err) {
    fs.rm(dir, { recursive: true, force: true }, () => {})
    return res.status(400).json({ error: 'Torrent invalide : ' + err.message })
  }

  const job = { id, torrent, dir, status: 'downloading', progress: 0, speed: 0, error: null }
  jobs.set(id, job)

  torrent.on('download', () => {
    job.progress = torrent.progress
    job.speed = torrent.downloadSpeed
  })
  torrent.on('done', () => {
    job.progress = 1
    job.status = 'ready'
    scheduleCleanup(job)
  })
  torrent.on('error', (err) => {
    job.status = 'error'
    job.error = err.message
    scheduleCleanup(job)
  })

  res.json({ id })
})

/* ---------- 2) Progression en temps réel (SSE) ---------- */
app.get('/api/progress/:id', (req, res) => {
  const job = jobs.get(req.params.id)
  if (!job) return res.status(404).end()

  res.setHeader('Content-Type', 'text/event-stream')
  res.setHeader('Cache-Control', 'no-cache')
  res.setHeader('Connection', 'keep-alive')
  res.flushHeaders()

  const send = () => {
    const t = job.torrent
    res.write(`data: ${JSON.stringify({
      status: job.status,
      progress: job.progress,
      speed: job.speed,
      peers: t.numPeers,
      downloaded: t.downloaded,
      length: t.length,
      name: t.name,
      files: t.files.length,
      error: job.error
    })}\n\n`)
  }

  send()
  const iv = setInterval(send, 500)
  req.on('close', () => clearInterval(iv))
})

/* ---------- 3) Génération + streaming du ZIP ---------- */
app.get('/api/download/:id', (req, res) => {
  const job = jobs.get(req.params.id)
  if (!job || job.status !== 'ready') {
    return res.status(409).send('Le téléchargement du torrent n’est pas terminé.')
  }

  const base = (job.torrent.name || 'torrent').replace(/[^\w.\- ]+/g, '_').trim() || 'torrent'

  res.setHeader('Content-Type', 'application/zip')
  res.setHeader('Content-Disposition', `attachment; filename="${base}.zip"`)

  // store: true => aucune compression => ZIP généré quasi instantanément
  const archive = archiver('zip', { store: true })
  archive.on('warning', (e) => console.warn('[zip]', e.message))
  archive.on('error', (err) => res.destroy(err))
  archive.pipe(res)

  for (const file of job.torrent.files) {
    const abs = path.join(job.dir, file.path)
    if (fs.existsSync(abs)) archive.file(abs, { name: file.path })
  }

  archive.finalize()
})

function cleanup(job) {
  jobs.delete(job.id)
  try { job.torrent.destroy({ destroyStore: true }) } catch {}
  fs.rm(job.dir, { recursive: true, force: true }, () => {})
}

function scheduleCleanup(job) {
  setTimeout(() => cleanup(job), JOB_TTL).unref()
}

process.on('SIGINT', () => {
  for (const job of jobs.values()) { try { job.torrent.destroy() } catch {} }
  process.exit(0)
})

app.listen(PORT, () => console.log(`http://localhost:${PORT}`))
