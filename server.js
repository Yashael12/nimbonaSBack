const express = require('express');
const { AccessToken, RoomServiceClient } = require('livekit-server-sdk');
const { createClient } = require('redis');
const { randomUUID } = require('crypto');
const multer = require('multer');
const path   = require('path');
const fs     = require('fs');

const app = express();
app.use(express.json());
// ─────────────────────────────────────────────
// FIREBASE ADMIN (for FCM notifications)
// ─────────────────────────────────────────────
// ─────────────────────────────────────────────
// FIREBASE ADMIN (for FCM notifications)
// ─────────────────────────────────────────────
const { initializeApp, cert } = require('firebase-admin/app');
const { getMessaging } = require('firebase-admin/messaging');

let firebaseReady = false;
let messaging = null;

let serviceAccount = {};
try {
  serviceAccount = JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT || '{}');
} catch (e) {
  console.error('❌ Failed to parse FIREBASE_SERVICE_ACCOUNT:', e.message);
}

if (serviceAccount.project_id) {
  try {
    initializeApp({ credential: cert(serviceAccount) });
    messaging = getMessaging();
    firebaseReady = true;
    console.log('✅ Firebase Admin initialized');
  } catch (e) {
    console.error('❌ Firebase Admin init failed:', e.message);
  }
} else {
  console.error('❌ FIREBASE_SERVICE_ACCOUNT missing or invalid');
}
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
let redis = null;

try {
  redis = createClient({ url: process.env.REDIS_URL || 'redis://localhost:6379' });
  redis.on('error', (e) => console.error('❌ Redis error:', e.message));
  redis.on('connect', () => console.log('✅ Redis connected'));
  redis.connect().catch(e => {
    console.error('❌ Redis connect failed:', e.message);
    redis = null;
  });
} catch(e) {
  console.error('❌ Redis initialisation failed:', e.message);
  redis = null;
}

function isRedisReady() {
  return redis && redis.isOpen;
}

// ─────────────────────────────────────────────
// REDIS KEY HELPERS
// ─────────────────────────────────────────────
const QUEUE_VIDEO = 'queue:video';
const QUEUE_AUDIO = 'queue:audio';
const queueKey   = (audioOnly) => audioOnly ? QUEUE_AUDIO : QUEUE_VIDEO;
const matchKey   = (identity)  => `match:${identity}`;
const roomKey    = (room)      => `room:${room}`;
const productKey = (room)      => `product:${room}`;   // ← NEW

// ─────────────────────────────────────────────
// TOKEN HELPER
// ─────────────────────────────────────────────
async function createToken({ identity, room, canPublish = false, canSubscribe = true }) {
  const at = new AccessToken(API_KEY, API_SECRET, { identity });
  at.addGrant({ roomJoin: true, room, canPublish, canSubscribe });
  return await at.toJwt();
}

// ─────────────────────────────────────────────
// MATCH
// GET /match?identity=xxx&audioOnly=false
// ─────────────────────────────────────────────
// ─────────────────────────────────────────────
// MATCH
// GET /match?identity=xxx&audioOnly=false
// ─────────────────────────────────────────────
app.get('/match', async (req, res) => {
  if (!isRedisReady()) {
    return res.status(503).json({
      error: 'Matchmaking temporarily unavailable (Redis down)'
    });
  }

  const identity = req.query.identity || `user_${randomUUID()}`;
  const displayName = req.query.displayName || identity;
  const audioOnly = req.query.audioOnly === 'true';
  const key = queueKey(audioOnly);

  try {
    const waitingRaw = await redis.lPop(key);

    if (waitingRaw) {
      const waiting = JSON.parse(waitingRaw);

      // Prevent self-match
      if (waiting.identity === identity) {
        await redis.rPush(key, waitingRaw);
      } else {

        const room = `call_${randomUUID()}`;

        const token1 = await createToken({
          identity: waiting.identity,
          room,
          canPublish: true,
          canSubscribe: true
        });

        const token2 = await createToken({
          identity,
          room,
          canPublish: true,
          canSubscribe: true
        });

        await redis.set(roomKey(room), '2', { EX: 600 });

        const waitingPartnerName =
          waiting.displayName || waiting.identity;

        const currentPartnerName =
          displayName || identity;

        // Result for waiting user
        await redis.set(
          matchKey(waiting.identity),
          JSON.stringify({
            token: token1,
            room,
            matched: true,
            partnerName: currentPartnerName
          }),
          { EX: 120 }
        );

        console.log(
          `✅ Matched: ${waitingPartnerName} ↔ ${currentPartnerName}`
        );

        // Result for current user
        return res.json({
          token: token2,
          room,
          matched: true,
          partnerName: waitingPartnerName
        });
      }
    }

    // Check if already queued
    const alreadyQueued = await redis
      .lRange(key, 0, -1)
      .then(list =>
        list.some(item => JSON.parse(item).identity === identity)
      );

    if (!alreadyQueued) {
      await redis.rPush(
        key,
        JSON.stringify({
          identity,
          displayName,
          audioOnly,
          joinedAt: Date.now()
        })
      );
    }

    const TIMEOUT = 30000;
    const INTERVAL = 600;
    const deadline = Date.now() + TIMEOUT;

    let resolved = false;

    req.on('close', async () => {
      if (resolved) return;

      resolved = true;

      try {
        const items = await redis.lRange(key, 0, -1);

        for (const item of items) {
          if (JSON.parse(item).identity === identity) {
            await redis.lRem(key, 1, item);
            break;
          }
        }
      } catch (e) {
        console.error("cleanup error:", e.message);
      }
    });

    while (Date.now() < deadline) {
      await new Promise(r => setTimeout(r, INTERVAL));

      if (resolved) return;

      const resultRaw = await redis.get(matchKey(identity));

      if (resultRaw) {
        resolved = true;

        await redis.del(matchKey(identity));

        const result = JSON.parse(resultRaw);

        console.log(
          `📬 ${identity} matched with ${result.partnerName}`
        );

        return res.json(result);
      }
    }

    // Timeout
    if (!resolved) {
      resolved = true;

      const items = await redis.lRange(key, 0, -1);

      for (const item of items) {
        if (JSON.parse(item).identity === identity) {
          await redis.lRem(key, 1, item);
          break;
        }
      }

      return res.status(408).json({
        error: 'No match found, try again'
      });
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
  if (!isRedisReady()) {
    return res.status(503).json({ error: 'Matchmaking temporarily unavailable (Redis down)' });
  }

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
// QUEUE STATUS
// GET /queueStatus
// ─────────────────────────────────────────────
app.get('/queueStatus', async (req, res) => {
  if (!isRedisReady()) {
    return res.json({ totalWaiting: 0, video: [], audio: [], redis: false });
  }

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
    const identity   = req.query.identity   || `host_${Date.now()}`;
    const title      = req.query.title      || "";
    const coverImage = req.query.coverImage || "";
    let room         = req.query.room       || `live_${Date.now()}`;

    if (!room.startsWith("live_")) room = `live_${room}`;

    if (isRedisReady()) {
      await redis.set(`title:${room}`, title, { EX: 86400 });
      console.log(`✅ Title saved: title:${room} = "${title}"`);

      if (coverImage) {
        await redis.set(`cover:${room}`, coverImage, { EX: 86400 });
        console.log(`✅ Cover saved: cover:${room} = "${coverImage}"`);
      } else {
        console.log(`ℹ️  No coverImage in getToken — keeping existing Redis value`);
      }
    }

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
// ACTIVE STREAMS
// GET /activeStreams
// ─────────────────────────────────────────────
app.get('/activeStreams', async (req, res) => {
  try {
    const rooms = await roomService.listRooms();

    const liveRooms = rooms.filter(r =>
      r.name.startsWith("live_") && r.numParticipants > 0
    );

    console.log(`📡 Live rooms: ${liveRooms.map(r => r.name)}`);

    const streams = await Promise.all(
      liveRooms.map(async (r) => {
        const [title, cover] = isRedisReady()
          ? await Promise.all([
              redis.get(`title:${r.name}`),
              redis.get(`cover:${r.name}`)
            ])
          : ["", ""];

        console.log(`🏷️  Room: ${r.name}, Title: "${title}"`);

        return {
          room:         r.name,
          participants: r.numParticipants,
          title:        title  || "",
          cover:        cover  || "",
          createdAt:    r.creationTime ? Number(r.creationTime) : null
        };
      })
    );

    console.log(`📤 Sending streams:`, JSON.stringify(streams));
    res.json({ streams });

  } catch (e) {
    console.error("❌ ACTIVE STREAMS ERROR:", e);
    res.status(500).json({ error: e.message });
  }
});

// ─────────────────────────────────────────────
// PIN PRODUCT  ← NEW
// Host calls this to feature a product in the live chat card.
// Pass an empty name to unpin / hide the card for viewers.
//
// POST /pinProduct
// Body: { room, name, price, originalPrice, discount, emoji }
// ─────────────────────────────────────────────
app.post('/pinProduct', async (req, res) => {
  try {
    const { room, name = "", price = "", originalPrice = "", discount = "", emoji = "🛍️" } = req.body;

    if (!room)                     return res.status(400).json({ error: "room is required" });
    if (!room.startsWith("live_")) return res.status(403).json({ error: "Not a livestream room" });

    if (!isRedisReady()) {
      return res.status(503).json({ error: "Redis unavailable — cannot pin product" });
    }

    if (!name.trim()) {
      // Empty name → unpin (delete the key so getProduct returns nothing)
      await redis.del(productKey(room));
      console.log(`📦 Product unpinned for ${room}`);
      return res.json({ pinned: false, room });
    }

    const payload = { name, price, originalPrice, discount, emoji };
    await redis.set(productKey(room), JSON.stringify(payload), { EX: 86400 });
    console.log(`📦 Product pinned for ${room}:`, payload);
    res.json({ pinned: true, room, product: payload });

  } catch (e) {
    console.error("❌ /pinProduct error:", e);
    res.status(500).json({ error: e.message });
  }
});

// ─────────────────────────────────────────────
// GET PRODUCT  ← NEW
// Viewers poll / fetch this once after joining to hydrate the product card.
//
// GET /getProduct?room=live_xxx
// Response: { product: { name, price, originalPrice, discount, emoji } }
//           or { product: null } when nothing is pinned
// ─────────────────────────────────────────────
app.get('/getProduct', async (req, res) => {
  try {
    const room = req.query.room;

    if (!room)                     return res.status(400).json({ error: "room is required" });
    if (!room.startsWith("live_")) return res.status(403).json({ error: "Not a livestream room" });

    if (!isRedisReady()) {
      return res.json({ product: null });
    }

    const raw = await redis.get(productKey(room));
    res.json({ product: raw ? JSON.parse(raw) : null });

  } catch (e) {
    console.error("❌ /getProduct error:", e);
    res.status(500).json({ error: e.message });
  }
});

// ─────────────────────────────────────────────
// DELETE ROOM
// POST /deleteRoom?room=call_xxx
// ─────────────────────────────────────────────
app.post('/deleteRoom', async (req, res) => {
  const room = req.query.room || req.body?.room;
  if (!room) return res.status(400).json({ error: 'room required' });

  if (!room.startsWith('call_') && !room.startsWith('live_')) {
    return res.status(403).json({ error: 'Not allowed' });
  }

  try {
    await roomService.deleteRoom(room);
    // Clean up product pin when room is explicitly deleted
    if (isRedisReady()) await redis.del(productKey(room));
    console.log(`🗑️  Room deleted on app exit: ${room}`);
    res.json({ deleted: true, room });
  } catch (e) {
    console.warn(`⚠️  deleteRoom (already gone?): ${room} — ${e.message}`);
    res.json({ deleted: true, room, note: 'already gone' });
  }
});

// ─────────────────────────────────────────────
// REMOVE PARTICIPANT
// POST /removeParticipant?room=call_xxx&identity=yyy
// ─────────────────────────────────────────────
app.post('/removeParticipant', async (req, res) => {
  const room     = req.query.room     || req.body?.room;
  const identity = req.query.identity || req.body?.identity;
  if (!room || !identity) return res.status(400).json({ error: 'room and identity required' });

  try {
    await roomService.removeParticipant(room, identity);
    console.log(`👟 Kicked ${identity} from ${room}`);
    res.json({ removed: true });
  } catch (e) {
    console.warn(`⚠️  removeParticipant: ${e.message}`);
    res.json({ removed: true, note: 'already gone' });
  }
});

// ─────────────────────────────────────────────
// GHOST ROOM SWEEPER
// ─────────────────────────────────────────────
async function sweepGhostRooms() {
  try {
    const rooms  = await roomService.listRooms();
    const ghosts = rooms.filter(r =>
      (r.name.startsWith('call_') || r.name.startsWith('live_')) &&
      Number(r.numParticipants) === 0
    );
    for (const r of ghosts) {
      await roomService.deleteRoom(r.name);
      if (isRedisReady()) await redis.del(`product:${r.name}`);
      console.log(`🧹 Swept ghost room: ${r.name}`);
    }
    if (ghosts.length > 0) {
      console.log(`🧹 Swept ${ghosts.length} ghost room(s)`);
    }
  } catch (e) {
    console.error('❌ sweepGhostRooms error:', e.message);
  }
}
// ─────────────────────────────────────────────
// COVER UPLOAD
// POST /uploadCover?room=live_xxx
// ─────────────────────────────────────────────
const storage = multer.diskStorage({
  destination: (req, file, cb) => {
    const dir = path.join(__dirname, 'uploads');
    if (!fs.existsSync(dir)) fs.mkdirSync(dir);
    cb(null, dir);
  },
  filename: (req, file, cb) => {
    const room = req.query.room || `cover_${Date.now()}`;
    cb(null, `${room}.jpg`);
  }
});

const upload = multer({ storage, limits: { fileSize: 5 * 1024 * 1024 } });

app.use('/uploads', express.static(path.join(__dirname, 'uploads')));

app.post('/uploadCover', upload.single('cover'), async (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ error: 'No file' });

    const room = req.query.room;
    const host = `${req.protocol}://${req.get('host')}`;
    const url  = `${host}/uploads/${req.file.filename}`;

    if (room && isRedisReady()) {
      await redis.set(`cover:${room}`, url, { EX: 86400 });
      console.log(`✅ Cover saved to Redis: cover:${room} = "${url}"`);
    }

    console.log(`🖼️  Cover uploaded for ${room}: ${url}`);
    res.json({ url });

  } catch (e) {
    console.error('❌ uploadCover error:', e);
    res.status(500).json({ error: e.message });
  }
});
// ─────────────────────────────────────────────
// SAVE FCM TOKEN
// POST /saveFcmToken
// Body: { userId, token }
// ─────────────────────────────────────────────
app.post('/saveFcmToken', async (req, res) => {
  try {
    const { userId, token } = req.body;
    if (!userId || !token) {
      return res.status(400).json({ error: 'userId and token required' });
    }

    // Save token to Redis with 30-day expiry
    if (isRedisReady()) {
      await redis.set(`fcm:${userId}`, token, { EX: 30 * 24 * 3600 });
      console.log(`✅ FCM token saved for user: ${userId}`);
    }

    res.json({ saved: true });
  } catch (e) {
    console.error('❌ /saveFcmToken error:', e);
    res.status(500).json({ error: e.message });
  }
});

// ─────────────────────────────────────────────
// SEND CHAT NOTIFICATION
// POST /sendChatNotification
// Body: { toUserId, fromName, message, roomId }
// ─────────────────────────────────────────────
app.post('/sendChatNotification', async (req, res) => {
  try {
    const { toUserId, fromName, message, roomId, soundEnabled = true, 
      vibrationEnabled = true } = req.body;
    if (!toUserId || !fromName || !message || !roomId) {
      return res.status(400).json({ error: 'toUserId, fromName, message, roomId required' });
    }

    // Get FCM token from Redis
    if (!isRedisReady()) {
      return res.status(503).json({ error: 'Redis unavailable' });
    }

    const fcmToken = await redis.get(`fcm:${toUserId}`);
    if (!fcmToken) {
      console.log(`⚠️  No FCM token found for user: ${toUserId}`);
      return res.json({ sent: false, reason: 'No FCM token for this user' });
    }

    // Send FCM notification
    const fcmMessage = {
      token: fcmToken,
      notification: {
        title: `💬 ${fromName}`,
        body: message,
      },
      data: {
        roomId:  roomId,
        type:    'chat',
        fromName: fromName
      },
      android: {
        priority: 'high',
        notification: {
         sound:     soundEnabled     ? 'default' : null,
          channelId: 'chat_messages',
             defaultVibrateTimings: vibrationEnabled,   
          clickAction: 'OPEN_CHAT'
        }
      },
      apns: {
        payload: {
          aps: {
            sound: 'default',
            badge: 1
          }
        }
      }
    };

  // NEW
   if (!firebaseReady || !messaging) {
  return res.status(503).json({ error: 'Firebase messaging not available' });
}
const result = await messaging.send(fcmMessage);
    console.log(`📲 Notification sent to ${toUserId}: ${result}`);
    res.json({ sent: true, messageId: result });

  } catch (e) {
    // Token expired or invalid — clean it up
    if (e.code === 'messaging/registration-token-not-registered') {
      if (isRedisReady()) {
        await redis.del(`fcm:${req.body.toUserId}`);
        console.log(`🗑️  Removed expired FCM token for: ${req.body.toUserId}`);
      }
      return res.json({ sent: false, reason: 'Token expired, removed' });
    }
    console.error('❌ /sendChatNotification error:', e);
    res.status(500).json({ error: e.message });
  }
});
sweepGhostRooms();
setInterval(sweepGhostRooms, 2 * 60 * 1000);

// ─────────────────────────────────────────────
// PING / HEALTH
// ─────────────────────────────────────────────
app.get('/ping', (req, res) => res.json({ ok: true }));
app.get('/', (req, res) => res.json({ status: "LiveKit + Redis server running ✅" }));
// ─────────────────────────────────────────────
// KEEP ALIVE
// ─────────────────────────────────────────────
const https = require('https');
if (process.env.RENDER_EXTERNAL_URL) {
  setInterval(() => {
    https.get(`${process.env.RENDER_EXTERNAL_URL}/ping`, (res) => {
      console.log(`🏓 Self-ping: ${res.statusCode}`);
    }).on('error', (e) => {
      console.error('❌ Self-ping failed:', e.message);
    });
  }, 10 * 60 * 1000);
}
// ─────────────────────────────────────────────
// ONLINE COUNT
// GET /onlineCount
// Returns total unique participants across all active rooms
// ─────────────────────────────────────────────
app.get('/onlineCount', async (req, res) => {
  try {
    const [rooms, queueVideo, queueAudio] = await Promise.all([
      roomService.listRooms(),
      isRedisReady() ? redis.lLen(QUEUE_VIDEO) : Promise.resolve(0),
      isRedisReady() ? redis.lLen(QUEUE_AUDIO) : Promise.resolve(0),
    ]);

    // Sum participants in all active rooms
    const inCall = rooms.reduce((sum, r) => sum + Number(r.numParticipants), 0);

    // People waiting in queue are also "online"
    const inQueue = queueVideo + queueAudio;

    const total = inCall + inQueue;

    res.json({
      total,
      inCall,
      inQueue,
      rooms: rooms.length,
    });
  } catch (e) {
    console.error('❌ /onlineCount error:', e);
    res.status(500).json({ error: e.message });
  }
});
// ─────────────────────────────────────────────
// SEND CALL INVITE
// POST /sendCallInvite
// Body: { toUserId, fromUid, fromName, room }
// ─────────────────────────────────────────────
app.post('/sendCallInvite', async (req, res) => {
  try {
    const { toUserId, fromUid, fromName, room } = req.body;
    if (!toUserId || !fromUid || !fromName || !room) {
      return res.status(400).json({ error: 'toUserId, fromUid, fromName, room required' });
    }

    if (!isRedisReady()) {
      return res.status(503).json({ error: 'Redis unavailable' });
    }

    const fcmToken = await redis.get(`fcm:${toUserId}`);
    if (!fcmToken) {
      return res.json({ sent: false, reason: 'No FCM token for this user' });
    }

    if (!firebaseReady || !messaging) {
      return res.status(503).json({ error: 'Firebase messaging not available' });
    }

    const fcmMessage = {
      token: fcmToken,
      data: {
        type: 'call_invite',
        room: room,
        fromUid: fromUid,
        fromName: fromName
      },
      android: {
        priority: 'high'
      },
      apns: {
        headers: { 'apns-priority': '10' },
        payload: { aps: { 'content-available': 1 } }
      }
    };

    const result = await messaging.send(fcmMessage);
    console.log(`📞 Call invite sent to ${toUserId} from ${fromName} for room ${room}`);
    res.json({ sent: true, messageId: result });

  } catch (e) {
    if (e.code === 'messaging/registration-token-not-registered') {
      if (isRedisReady()) await redis.del(`fcm:${req.body.toUserId}`);
      return res.json({ sent: false, reason: 'Token expired, removed' });
    }
    console.error('❌ /sendCallInvite error:', e);
    res.status(500).json({ error: e.message });
  }
});
// ─────────────────────────────────────────────
// START
// ─────────────────────────────────────────────
const PORT = process.env.PORT || 3000;

app.listen(PORT, "0.0.0.0", () => {
  console.log(`
🚀 Server running on port ${PORT}

Endpoints:
  GET  /match              → random 1-on-1 matching
  GET  /cancelMatch        → leave the queue
  GET  /queueStatus        → debug: who is waiting
  GET  /getToken           → livestream host token
  GET  /getViewerToken     → livestream viewer token
  GET  /getCallToken       → private call token
  GET  /activeStreams       → list live public streams
  POST /pinProduct          → host pins a product card  ← NEW
  GET  /getProduct          → viewer fetches pinned product  ← NEW
  GET  /ping               → wake-up ping
  GET  /                   → health check
  `);
});