const express = require('express');
const { AccessToken } = require('livekit-server-sdk');

const app = express();

const API_KEY = "APIPX7aADrTbSYv";
const API_SECRET = "9e7pjI9Nd3XieaCAPDrBovReC7seJDeWWYhnCJkitd0D";

// 1. Make this function async
app.get('/getToken', async (req, res) => { 
  try {
    const identity = req.query.identity || "user1";
    const room = req.query.room || "test-room";

    const at = new AccessToken(API_KEY, API_SECRET, {
      identity,
    });

    at.addGrant({
      roomJoin: true,
      room,
      canPublish: true,
      canSubscribe: true,
    });

    // 2. Await the token!
    const token = await at.toJwt(); 

    console.log("👉 TOKEN VALUE:", token);

    // This check will now pass because token is a string
    if (!token || typeof token !== "string") {
      throw new Error("Token generation failed - invalid output");
    }

    res.json({ token });

  } catch (e) {
    console.error("❌ TOKEN ERROR:", e);
    res.status(500).json({ error: e.message });
  }
});

app.listen(3000, "0.0.0.0", () => {
  console.log("Server running on http://0.0.0.0:3000");
});