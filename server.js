const express = require('express');
const { AccessToken, RoomServiceClient } = require('livekit-server-sdk');

const app = express();

const API_KEY = "APIPX7aADrTbSYv";
const API_SECRET = "9e7pjI9Nd3XieaCAPDrBovReC7seJDeWWYhnCJkitd0D";

const LIVEKIT_URL = "https://tapay-i6uqe3a6.livekit.cloud";

// ─────────────────────────────────────────────
// HOST TOKEN
// ─────────────────────────────────────────────

app.get('/getToken', async (req, res) => {

  try {

    const identity = req.query.identity || "user1";
    const room = req.query.room || "test-room";

    const at = new AccessToken(
      API_KEY,
      API_SECRET,
      { identity }
    );

    at.addGrant({
      roomJoin: true,
      room,
      canPublish: true,
      canSubscribe: true,
    });

    const token = await at.toJwt();

    res.json({ token });

  } catch (e) {

    console.error("❌ TOKEN ERROR:", e);

    res.status(500).json({
      error: e.message
    });
  }
});

// ─────────────────────────────────────────────
// VIEWER TOKEN
// ─────────────────────────────────────────────

app.get('/getViewerToken', async (req, res) => {

  try {

    const identity =
      req.query.identity || "viewer_" + Date.now();

    const room =
      req.query.room || "test-room";

    const at = new AccessToken(
      API_KEY,
      API_SECRET,
      { identity }
    );

    at.addGrant({
      roomJoin: true,
      room,
      canPublish: false,
      canSubscribe: true,
    });

    const token = await at.toJwt();

    res.json({ token });

  } catch (e) {

    console.error("❌ VIEWER TOKEN ERROR:", e);

    res.status(500).json({
      error: e.message
    });
  }
});

// ─────────────────────────────────────────────
// ACTIVE STREAMS
// ─────────────────────────────────────────────

app.get('/activeStreams', async (req, res) => {
  try {
    const svc = new RoomServiceClient(
      "https://tapay-i6uqe3a6.livekit.cloud",
      API_KEY,
      API_SECRET
    );

    const rooms = await svc.listRooms();

    const streams = rooms.map(room => ({
      room: room.name,
      participants: room.numParticipants
    }));

    res.json({ streams });

  } catch (e) {
    console.error(e);
    res.status(500).json({ error: e.message });
  }
});

// ─────────────────────────────────────────────

app.listen(3000, "0.0.0.0", () => {

  console.log(
    "🚀 Server running on http://0.0.0.0:3000"
  );

});