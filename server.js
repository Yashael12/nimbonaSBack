const express = require('express');
const { AccessToken, RoomServiceClient } = require('livekit-server-sdk');
const { createClient } = require('redis');  // ✅ fixed typo
// const { v4: uuidv4 } = require('uuid');
// Remove this line:
// const { v4: uuidv4 } = require('uuid');

// Add this instead:
const { randomUUID } = require('crypto'); // built into Node.js, no install needed

const app = express();

// ─────────────────────────────────────────────
// CONFIG
// ─────────────────────────────────────────────
const API_KEY     = "APIPX7aADrTbSYv";
const API_SECRET  = "9e7pjI9Nd3XieaCAPDrBovReC7seJDeWWYhnCJkitd0D";
const LIVEKIT_URL = "https://tapay-i6uqe3a6.livekit.cloud";

const roomService = new RoomServiceClient(LIVEKIT_URL, API_KEY, API_SECRET);

// ─────────────────────────────────────────────
// REDIS
// ─────────────────────────────────────────────
const redis = createClient({
  url: process.env.REDIS_URL || 'redis://localhost:6379'
});

redis.on('error', e => console.error('❌ Redis error:', e));

// Connect Redis before starting server
redis.connect()
  .then(() => console.log('✅ Redis connected'))
  .catch(e => console.error('❌ Redis connect failed:', e));

// ─────────────────────────────────────────────
// REDIS KEY HELPERS
// ─────────────────────────────────────────────
const QUEUE_VIDEO = 'queue:video';
const QUEUE_AUDIO = 'queue:audio';
const queueKey = (audioOnly) => audioOnly ? QUEUE_AUDIO : QUEUE_VIDEO;
const matchKey = (identity)  => `match:${identity}`;

// ─────────────────────────────────────────────
// TOKEN HELPER
// ─────────────────────────────────────────────
async function createToken({ identity, room, canPublish = false, canSubscribe = true }) {
  const at = new AccessToken(API_KEY, API_SECRET, { identity });
  at.addGrant({ roomJoin: true, room, canPublish, canSubscribe });
  return await at.toJwt();
}

// ─────────────────────────────────────────────
// MATCH — random 1-on-1 call
// GET /match?identity=xxx&audioOnly=false
// ─────────────────────────────────────────────
app.get('/match', async (req, res) => {
  const identity  = req.query.identity  || `user_${randomUUID()}`;
  const audioOnly = req.query.audioOnly === 'true';
  const key       = queueKey(audioOnly);

  try {
    // ── 1. Try to pop a waiting user from queue ──
    const waitingRaw = await redis.lPop(key);

    if (waitingRaw) {
      const waiting = JSON.parse(waitingRaw);

      // Don't match with yourself (edge case on reconnect)
      if (waiting.identity === identity) {
        await redis.lPush(key, waitingRaw); // put back
      } else {
        const room   = `call_${randomUUID()}`;
        const token1 = await createToken({ identity: waiting.identity, room, canPublish: true, canSubscribe: true });
        const token2 = await createToken({ identity,                   room, canPublish: true, canSubscribe: true });

        // Write result for the waiting user to pick up
        await redis.set(
          matchKey(waiting.identity),
          JSON.stringify({ token: token1, room, matched: true }),
          { EX: 60 }  // auto-expire after 60s if not picked up
        );

        console.log(`✅ Matched: ${waiting.identity} ↔ ${identity} → ${room}`);
        return res.json({ token: token2, room, matched: true });
      }
    }

    // ── 2. No match yet — push self to queue ──
    await redis.rPush(key, JSON.stringify({
      identity,
      audioOnly,
      joinedAt: Date.now()
    }));

    // ── 3. Poll Redis every 600ms for up to 30s ──
    const TIMEOUT  = 30_000;
    const INTERVAL = 600;
    const deadline = Date.now() + TIMEOUT;
    let   resolved = false;

    // If client disconnects early, remove from queue
    req.on('close', async () => {
      if (resolved) return;
      resolved = true;
      const items = await redis.lRange(key, 0, -1);
      for (const item of items) {
        if (JSON.parse(item).identity === identity) {
          await redis.lRem(key, 1, item);
          break;
        }
      }
      console.log(`📴 Client disconnected: ${identity} removed from queue`);
    });

    while (Date.now() < deadline) {
      await new Promise(r => setTimeout(r, INTERVAL));

      if (resolved) return; // client already disconnected

      const resultRaw = await redis.get(matchKey(identity));
      if (resultRaw) {
        resolved = true;
        await redis.del(matchKey(identity));
        console.log(`📬 Delivered match result to: ${identity}`);
        return res.json(JSON.parse(resultRaw));
      }
    }

    // ── 4. Timed out — remove from queue ──
    if (!resolved) {
      resolved = true;
      const items = await redis.lRange(key, 0, -1);
      for (const item of items) {
        if (JSON.parse(item).identity === identity) {
          await redis.lRem(key, 1, item);
          break;
        }
      }
      console.log(`⏰ Match timeout: ${identity}`);
      return res.status(408).json({ error: 'No match found, try again' });
    }

  } catch (e) {
    console.error('❌ /match error:', e);
    res.status(500).json({ error: e.message });
  }
});

// ─────────────────────────────────────────────
// CANCEL MATCH
// GET /cancelMatch?identity=xxx&audioOnly=false
// ─────────────────────────────────────────────
app.get('/cancelMatch', async (req, res) => {
  const { identity, audioOnly } = req.query;
  if (!identity) return res.status(400).json({ error: 'identity required' });

  const key = queueKey(audioOnly === 'true');

  try {
    const items = await redis.lRange(key, 0, -1);
    for (const item of items) {
      if (JSON.parse(item).identity === identity) {
        await redis.lRem(key, 1, item);
        console.log(`🚫 Cancelled: ${identity}`);
        break;
      }
    }
    res.json({ cancelled: true });
  } catch (e) {
    console.error('❌ /cancelMatch error:', e);
    res.status(500).json({ error: e.message });
  }
});

// ─────────────────────────────────────────────
// QUEUE STATUS  (debug)
// GET /queueStatus
// ─────────────────────────────────────────────
app.get('/queueStatus', async (req, res) => {
  try {
    const [video, audio] = await Promise.all([
      redis.lRange(QUEUE_VIDEO, 0, -1),
      redis.lRange(QUEUE_AUDIO, 0, -1),
    ]);
    res.json({
      totalWaiting: video.length + audio.length,
      video: video.map(x => JSON.parse(x)),
      audio: audio.map(x => JSON.parse(x)),
    });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ─────────────────────────────────────────────
// LIVESTREAM HOST TOKEN
// GET /getToken?identity=xxx&room=live_xxx
// ─────────────────────────────────────────────
app.get('/getToken', async (req, res) => {
  try {
    const identity = req.query.identity || `host_${Date.now()}`;
    let room       = req.query.room     || `live_${Date.now()}`;

    if (!room.startsWith("live_")) room = `live_${room}`;

    const token = await createToken({ identity, room, canPublish: true, canSubscribe: true });
    res.json({ token, room });

  } catch (e) {
    console.error("❌ HOST TOKEN ERROR:", e);
    res.status(500).json({ error: e.message });
  }
});

// ─────────────────────────────────────────────
// LIVESTREAM VIEWER TOKEN
// GET /getViewerToken?identity=xxx&room=live_xxx
// ─────────────────────────────────────────────
app.get('/getViewerToken', async (req, res) => {
  try {
    const identity = req.query.identity || `viewer_${Date.now()}`;
    const room     = req.query.room;

    if (!room)                     return res.status(400).json({ error: "room is required" });
    if (!room.startsWith("live_")) return res.status(403).json({ error: "Not a livestream room" });

    const token = await createToken({ identity, room, canPublish: false, canSubscribe: true });
    res.json({ token });

  } catch (e) {
    console.error("❌ VIEWER TOKEN ERROR:", e);
    res.status(500).json({ error: e.message });
  }
});

// ─────────────────────────────────────────────
// PRIVATE CALL TOKEN
// GET /getCallToken?identity=xxx&room=call_xxx
// ─────────────────────────────────────────────
app.get('/getCallToken', async (req, res) => {
  try {
    const identity = req.query.identity || `user_${Date.now()}`;
    let room       = req.query.room;

    if (!room)                     return res.status(400).json({ error: "room is required" });
    if (!room.startsWith("call_")) room = `call_${room}`;

    const token = await createToken({ identity, room, canPublish: true, canSubscribe: true });
    res.json({ token, room });

  } catch (e) {
    console.error("❌ CALL TOKEN ERROR:", e);
    res.status(500).json({ error: e.message });
  }
});

// ─────────────────────────────────────────────
// ACTIVE PUBLIC STREAMS
// GET /activeStreams
// ─────────────────────────────────────────────
app.get('/activeStreams', async (req, res) => {
  try {
    const rooms   = await roomService.listRooms();
    const streams = rooms
      .filter(r => r.name.startsWith("live_") && r.numParticipants > 0)
      .map(r => ({
        room:         r.name,
        participants: r.numParticipants,
        createdAt:    r.creationTime || null
      }));
    res.json({ streams });

  } catch (e) {
    console.error("❌ ACTIVE STREAMS ERROR:", e);
    res.status(500).json({ error: e.message });
  }
});

// ─────────────────────────────────────────────
// HEALTH CHECK
// ─────────────────────────────────────────────
app.get('/', (req, res) => {
  res.json({ status: "LiveKit + Redis server running ✅" });
});

// ─────────────────────────────────────────────
// START SERVER
// ─────────────────────────────────────────────
const PORT = process.env.PORT || 3000;

app.listen(PORT, "0.0.0.0", () => {
  console.log(`
🚀 Server running on port ${PORT}

Endpoints:
  GET /match              → random 1-on-1 matching
  GET /cancelMatch        → leave the queue
  GET /queueStatus        → debug: who is waiting
  GET /getToken           → livestream host token
  GET /getViewerToken     → livestream viewer token
  GET /getCallToken       → private call token
  GET /activeStreams       → list live public streams
  GET /                   → health check
  `);
});