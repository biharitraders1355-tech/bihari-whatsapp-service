/**
 * Bihari Traders — Baileys WhatsApp Automation Service
 *
 * Architecture:
 *   - Connects to WhatsApp via Baileys (WebSocket, no browser)
 *   - Polls MongoDB `wa_queue` collection every POLL_INTERVAL_MS
 *   - Sends pending messages automatically, marks them sent
 *   - Exposes a tiny HTTP server for:
 *       GET /health          → status + connection state
 *       GET /qr              → base64 PNG of current QR (if not yet connected)
 *       POST /send           → manually enqueue a message (for testing)
 *
 * First run: scan the QR printed to terminal (or hit GET /qr from admin panel).
 * Subsequent runs: reconnects automatically using saved session in AUTH_DIR.
 */
import 'dotenv/config';
import * as baileys from '@whiskeysockets/baileys';

const {
  default: makeWASocket,
  DisconnectReason,
  useMultiFileAuthState,
  fetchLatestBaileysVersion,
  makeCacheableSignalKeyStore,
} = baileys;
import { Boom } from '@hapi/boom';
import { MongoClient, ObjectId } from 'mongodb';
import QRCode from 'qrcode-terminal';
import pino from 'pino';
import { createServer } from 'http';
import { readFileSync, existsSync, mkdirSync } from 'fs';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';

// ── Config ────────────────────────────────────────────────────────────────────
const MONGO_URL      = process.env.MONGO_URL      || 'mongodb://localhost:27017';
const MONGO_DB       = process.env.MONGO_DB       || 'biharitraders';
const AUTH_DIR       = process.env.AUTH_DIR        || './auth_session';
const SEND_DELAY_MS  = parseInt(process.env.SEND_DELAY_MS  || '3000', 10);
const POLL_INTERVAL  = parseInt(process.env.POLL_INTERVAL_MS || '5000', 10);
const HTTP_PORT      = parseInt(process.env.HTTP_PORT || '3001', 10);
const LOG_LEVEL      = process.env.LOG_LEVEL || 'info';
const SELF_PING_URL  = process.env.SELF_PING_URL || '';          // e.g. https://your-app.onrender.com
const PING_INTERVAL  = parseInt(process.env.PING_INTERVAL_MS || '840000', 10); // 14 min default

const logger = pino({
  level: LOG_LEVEL,
  transport: { target: 'pino-pretty', options: { colorize: true } },
});

// Ensure auth dir exists
if (!existsSync(AUTH_DIR)) mkdirSync(AUTH_DIR, { recursive: true });

// ── State ─────────────────────────────────────────────────────────────────────
let sock = null;
let isConnected = false;
let currentQR = null;         // raw QR string for HTTP endpoint
let mongoClient = null;
let db = null;
let pollTimer = null;
let isSending = false;        // mutex — prevent overlapping send loops

// ── MongoDB ───────────────────────────────────────────────────────────────────
async function connectMongo() {
  mongoClient = new MongoClient(MONGO_URL);
  await mongoClient.connect();
  db = mongoClient.db(MONGO_DB);
  // Ensure indexes exist
  await db.collection('wa_queue').createIndex({ sent: 1, created_at: 1 });
  logger.info({ db: MONGO_DB }, 'MongoDB connected');
}

// ── Phone normalisation ───────────────────────────────────────────────────────
function toJid(phone) {
  // Strip everything except digits
  let digits = phone.replace(/\D/g, '');
  // Add India country code if 10-digit number
  if (digits.length === 10) digits = '91' + digits;
  return `${digits}@s.whatsapp.net`;
}

// ── Delay helper ─────────────────────────────────────────────────────────────
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// ── Send a single message ─────────────────────────────────────────────────────
async function sendMessage(jid, text) {
  if (!isConnected || !sock) throw new Error('WhatsApp not connected');
  await sock.sendMessage(jid, { text });
}

// ── Drain the wa_queue ────────────────────────────────────────────────────────
async function drainQueue() {
  if (!isConnected || !db || isSending) return;
  isSending = true;

  try {
    const pending = await db
      .collection('wa_queue')
      .find({ sent: false })
      .sort({ created_at: 1 })
      .limit(20)
      .toArray();

    if (pending.length === 0) { isSending = false; return; }

    logger.info(`Processing ${pending.length} pending WhatsApp message(s)`);

    for (const item of pending) {
      const jid = toJid(item.phone);
      try {
        await sendMessage(jid, item.message);
        await db.collection('wa_queue').updateOne(
          { _id: item._id },
          { $set: { sent: true, sent_at: new Date(), delivered_via: 'baileys' } },
        );
        logger.info({ to: item.phone, event: item.event }, 'WA message sent ✓');
      } catch (err) {
        // Increment retry count; after 5 failures mark as failed so it stops retrying
        const retries = (item.retries || 0) + 1;
        const update = retries >= 5
          ? { $set: { sent: false, failed: true, last_error: err.message } }
          : { $set: { retries, last_error: err.message, last_attempt: new Date() } };
        await db.collection('wa_queue').updateOne({ _id: item._id }, update);
        logger.warn({ to: item.phone, err: err.message, retries }, 'WA send failed');
      }
      // Throttle to avoid spam detection
      await sleep(SEND_DELAY_MS);
    }
  } finally {
    isSending = false;
  }
}

// ── Build the Baileys socket ──────────────────────────────────────────────────
async function startBaileys() {
  const { state, saveCreds } = await useMultiFileAuthState(AUTH_DIR);
  const { version } = await fetchLatestBaileysVersion();
  logger.info({ version }, 'Using Baileys WA version');

  sock = makeWASocket({
    version,
    auth: {
      creds: state.creds,
      keys: makeCacheableSignalKeyStore(state.keys, logger),
    },
    printQRInTerminal: false,   // we handle QR ourselves
    logger: logger.child({ name: 'baileys' }),
    browser: ['Bihari Traders', 'Chrome', '120.0.0'],
    syncFullHistory: false,
    markOnlineOnConnect: false, // don't show "online" on every reconnect
    generateHighQualityLinkPreview: false,
  });

  // ── Connection state ────────────────────────────────────────────────────────
  sock.ev.on('connection.update', async (update) => {
    const { connection, lastDisconnect, qr } = update;

    if (qr) {
      currentQR = qr;
      logger.info('Scan the QR code below (or hit GET /qr from the admin panel):');
      QRCode.generate(qr, { small: true });
    }

    if (connection === 'open') {
      currentQR = null;
      isConnected = true;
      logger.info('✅ WhatsApp connected! Starting queue polling...');
      startPolling();
    }

    if (connection === 'close') {
      isConnected = false;
      stopPolling();
      const code = (lastDisconnect?.error instanceof Boom)
        ? lastDisconnect.error.output?.statusCode
        : undefined;
      const reason = Object.entries(DisconnectReason).find(([, v]) => v === code)?.[0] || code;

      logger.warn({ reason }, 'WA connection closed');

      // Don't reconnect if logged out — needs a new QR scan
      if (code === DisconnectReason.loggedOut) {
        logger.error('Logged out from WhatsApp. Delete ./auth_session and restart to re-scan QR.');
        return;
      }
      // Reconnect for all other reasons (restartRequired, connectionReplaced, etc.)
      logger.info('Reconnecting in 5s...');
      setTimeout(startBaileys, 5000);
    }
  });

  // ── Save creds on every update ──────────────────────────────────────────────
  sock.ev.on('creds.update', saveCreds);

  // ── Ignore all incoming messages (we only send) ─────────────────────────────
  sock.ev.on('messages.upsert', () => {});
}

// ── Polling ───────────────────────────────────────────────────────────────────
function startPolling() {
  if (pollTimer) return;
  pollTimer = setInterval(drainQueue, POLL_INTERVAL);
  // Drain immediately on connect
  drainQueue();
}

function stopPolling() {
  if (pollTimer) { clearInterval(pollTimer); pollTimer = null; }
}

// ── Tiny HTTP server ──────────────────────────────────────────────────────────
function startHttp() {
  const server = createServer(async (req, res) => {
    // CORS — allow requests from the admin frontend
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, PATCH, DELETE, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');

  // Handle preflight
  if (req.method === 'OPTIONS') {
    res.writeHead(204);
    res.end();
    return;
  }

  const url = new URL(req.url, `http://localhost:${HTTP_PORT}`);

    // ── GET /health ────────────────────────────────────────────────────────────
    if (req.method === 'GET' && url.pathname === '/health') {
      const pending = db
        ? await db.collection('wa_queue').countDocuments({ sent: false })
        : -1;
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({
        status: isConnected ? 'connected' : 'disconnected',
        qr_pending: !!currentQR,
        pending_messages: pending,
      }));
      return;
    }

    // ── GET /qr ────────────────────────────────────────────────────────────────
    // Returns a JSON with a base64 QR image — admin panel can display this
    if (req.method === 'GET' && url.pathname === '/qr') {
      if (isConnected) {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ connected: true, qr: null }));
        return;
      }
      if (!currentQR) {
        res.writeHead(202, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ connected: false, qr: null, message: 'QR not yet generated. Wait a moment.' }));
        return;
      }
      // Dynamically import qrcode (ESM)
      const { default: QRCodeLib } = await import('qrcode');
      const dataUrl = await QRCodeLib.toDataURL(currentQR, { width: 300, margin: 2 });
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ connected: false, qr: dataUrl }));
      return;
    }

    // ── POST /send ─────────────────────────────────────────────────────────────
    // Body: { phone: "919876543210", message: "Hello!" }
    if (req.method === 'POST' && url.pathname === '/send') {
      let body = '';
      req.on('data', (c) => (body += c));
      req.on('end', async () => {
        try {
          const { phone, message } = JSON.parse(body);
          if (!phone || !message) throw new Error('phone and message are required');
          const jid = toJid(phone);
          await sendMessage(jid, message);
          res.writeHead(200, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ success: true, to: jid }));
        } catch (err) {
          res.writeHead(400, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ success: false, error: err.message }));
        }
      });
      return;
    }

    res.writeHead(404);
    res.end('Not found');
  });

  server.listen(HTTP_PORT, () => {
    logger.info(`HTTP API listening on port ${HTTP_PORT}`);
    logger.info(`  GET  http://localhost:${HTTP_PORT}/health`);
    logger.info(`  GET  http://localhost:${HTTP_PORT}/qr     ← scan from admin panel`);
    logger.info(`  POST http://localhost:${HTTP_PORT}/send   ← manual test send`);
  });
}

// ── Self-ping (prevents Render free-tier spin-down) ───────────────────────────
// Render spins down services after ~15 min of inactivity on the free plan.
// This pings our own /health endpoint every 14 min to keep the instance alive.
// Set SELF_PING_URL=https://your-app.onrender.com in .env / Render env vars.
// Leave it empty to disable (e.g. if you're on a paid plan).
function startSelfPing() {
  if (!SELF_PING_URL) {
    logger.info('SELF_PING_URL not set — self-ping disabled');
    return;
  }

  const url = `${SELF_PING_URL.replace(/\/$/, '')}/health`;

  const ping = async () => {
    try {
      const { default: httpLib } = await import(url.startsWith('https') ? 'https' : 'http');
      await new Promise((resolve, reject) => {
        const req = httpLib.get(url, (res) => {
          res.resume(); // drain response body
          logger.debug({ status: res.statusCode }, 'Self-ping ✓');
          resolve();
        });
        req.on('error', reject);
        req.setTimeout(10_000, () => { req.destroy(); reject(new Error('timeout')); });
      });
    } catch (err) {
      logger.warn({ err: err.message }, 'Self-ping failed (non-fatal)');
    }
  };

  // First ping shortly after startup, then on interval
  setTimeout(ping, 10_000);
  setInterval(ping, PING_INTERVAL);
  logger.info({ url, intervalMs: PING_INTERVAL }, 'Self-ping enabled');
}

// ── Graceful shutdown ─────────────────────────────────────────────────────────
async function shutdown() {
  logger.info('Shutting down...');
  stopPolling();
  if (sock) sock.end();
  if (mongoClient) await mongoClient.close();
  process.exit(0);
}
process.on('SIGTERM', shutdown);
process.on('SIGINT',  shutdown);

// ── Bootstrap ─────────────────────────────────────────────────────────────────
(async () => {
  try {
    await connectMongo();
    startHttp();
    startSelfPing();
    await startBaileys();
  } catch (err) {
    logger.error({ err }, 'Fatal startup error');
    process.exit(1);
  }
})();
