# Bihari Traders — WhatsApp Automation Service

Automated WhatsApp message sender powered by **Baileys** (free, no API key needed).

---

## How it works

1. When an order is placed / paid / shipped / delivered / cancelled, the Python backend writes a message to the `wa_queue` MongoDB collection.
2. This Node.js service polls that collection every 5 seconds.
3. Messages are sent automatically via WhatsApp — no human clicks needed.
4. The admin panel at `/admin/whatsapp` shows real-time status, QR code, and message history.

---

## Requirements

- Node.js 17 or higher
- Same MongoDB instance as your Python backend
- A WhatsApp account (personal or business) on a phone

> ⚠️ **Risk notice**: Baileys uses the unofficial WhatsApp Web protocol. Meta can ban the phone number. Use a dedicated/secondary number, not your primary business number.

---

## Setup

### 1. Install dependencies

```bash
cd whatsapp-service
npm install
```

### 2. Configure environment

```bash
cp .env.example .env
```

Edit `.env`:

```env
MONGO_URL=mongodb://localhost:27017      # your MongoDB URL
MONGO_DB=biharitraders                  # your DB name
AUTH_DIR=./auth_session                 # where session files are saved
SEND_DELAY_MS=3000                      # delay between messages (ms)
POLL_INTERVAL_MS=5000                   # how often to check for new messages
HTTP_PORT=3001                          # port for health/QR API
```

### 3. Start the service

```bash
npm start
```

### 4. Scan the QR code

**First run only.** The QR code will appear in the terminal. You can also:
- Open `http://localhost:3001/qr` to get a base64 image
- Or go to `/admin/whatsapp` in the admin panel — it displays the QR there

Scan the QR using:
- WhatsApp → ⋮ Menu → Linked Devices → Link a Device

After scanning, the service saves the session in `./auth_session/`. Subsequent restarts reconnect automatically — no re-scan needed.

---

## Running in production (with PM2)

```bash
npm install -g pm2
pm2 start src/index.js --name bihari-whatsapp
pm2 save
pm2 startup
```

---

## HTTP API

| Endpoint | Method | Description |
|---|---|---|
| `/health` | GET | Connection status + pending message count |
| `/qr` | GET | Base64 QR image (if not yet connected) |
| `/send` | POST | Manually send a test message |

### Test send

```bash
curl -X POST http://localhost:3001/send \
  -H "Content-Type: application/json" \
  -d '{"phone": "919876543210", "message": "Test from Bihari Traders!"}'
```

---

## Messages triggered automatically

| Event | Trigger |
|---|---|
| Order Placed (COD) | Customer places a COD order |
| Payment Confirmed | Online payment webhook fires |
| Order Shipped | Admin marks order as shipped |
| Order Delivered | Admin marks order as delivered |
| Order Cancelled | Admin or customer cancels order |
