require('dotenv').config();
const express = require('express');
const compression = require('compression');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');
const { MongoClient, ObjectId } = require('mongodb');
const cloudinary = require('cloudinary').v2;
const { S3Client, PutObjectCommand } = require('@aws-sdk/client-s3');

const app = express();
app.disable('x-powered-by');

/* ===== PERFORMANCE MIDDLEWARE (must be first) ===== */
app.use(compression({ threshold: 512, level: 6 }));          // gzip responses
app.use(cors());
app.use(express.json({ limit: '25mb' }));
app.use(express.static(__dirname, { maxAge: '1h', etag: true, lastModified: true }));

const server = http.createServer(app);
const io = new Server(server, {
    cors: { origin: '*' },
    maxHttpBufferSize: 2.5e7,
    pingInterval: 25000,
    pingTimeout: 20000,
    transports: ['websocket', 'polling'],
    perMessageDeflate: false,                 // cheaper CPU
    httpCompression: false,
    allowEIO3: true
});

const MONGODB_URI = process.env.MONGODB_URI;
if (!MONGODB_URI) { console.error('❌ MONGODB_URI not set'); process.exit(1); }

const GOOGLE_CLIENT_ID = process.env.GOOGLE_CLIENT_ID || '973713902857-fnbbiifd2n9mor4moljdoijds7dq9olh.apps.googleusercontent.com';
const ADMIN_PASSWORD = process.env.ADMIN_PASSWORD || 'system@12';

/* ===== STORAGE ===== */
let cloudinaryReady = false;
if (process.env.CLOUDINARY_CLOUD_NAME && process.env.CLOUDINARY_API_KEY && process.env.CLOUDINARY_API_SECRET) {
    try {
        cloudinary.config({ cloud_name: process.env.CLOUDINARY_CLOUD_NAME, api_key: process.env.CLOUDINARY_API_KEY, api_secret: process.env.CLOUDINARY_API_SECRET, secure: true });
        cloudinaryReady = true;
        console.log('✅ Cloudinary configured');
    } catch (e) { console.error('Cloudinary init:', e.message); }
}

let r2Ready = false, r2Client = null, R2_BUCKET = '', R2_PUBLIC_URL = '';
const STORAGE_PROVIDER = (process.env.STORAGE_PROVIDER || 'auto').toLowerCase();
if (process.env.R2_ACCOUNT_ID && process.env.R2_ACCESS_KEY_ID && process.env.R2_SECRET_ACCESS_KEY && process.env.R2_BUCKET_NAME) {
    try {
        r2Client = new S3Client({
            region: 'auto',
            endpoint: `https://${process.env.R2_ACCOUNT_ID}.r2.cloudflarestorage.com`,
            credentials: { accessKeyId: process.env.R2_ACCESS_KEY_ID, secretAccessKey: process.env.R2_SECRET_ACCESS_KEY },
        });
        R2_BUCKET = process.env.R2_BUCKET_NAME;
        R2_PUBLIC_URL = (process.env.R2_PUBLIC_URL || '').replace(/\/$/, '');
        r2Ready = true;
        console.log('✅ R2 configured');
    } catch (e) { console.error('R2 init:', e.message); }
}

const YOUTUBE_API_KEY = process.env.YOUTUBE_API_KEY || '';
const youtubeApiReady = !!YOUTUBE_API_KEY;
if (youtubeApiReady) console.log('✅ YouTube API configured');

/* ===== DB COLLECTIONS ===== */
let usersCol, requestsCol, friendshipsCol, messagesCol, storiesCol;
let reelsCol, reelLikesCol, reelCommentsCol, musicCol;
let shortsCol, shortLikesCol, starredCol, archivedCol;
const onlineUsers = new Map();

let dbReady = false;
let dbError = null;

/* ===== DB READY GUARD ===== */
app.use((req, res, next) => {
    if (dbReady) return next();
    // Allow health checks + static + service-worker always
    if (req.path === '/api/ping' || req.path === '/api/config') return next();
    if (req.path.startsWith('/socket.io') || req.path.startsWith('/service-worker') || req.path === '/favicon.ico' || req.path === '/manifest.json') return next();
    return res.status(503).json({ error: dbError ? 'DB error, please retry' : 'Server warming up, please retry in a moment' });
});

async function connectDB() {
    const t0 = Date.now();
    const client = new MongoClient(MONGODB_URI, {
        maxPoolSize: 20, minPoolSize: 5,
        serverSelectionTimeoutMS: 3000,
        socketTimeoutMS: 30000,
        connectTimeoutMS: 3000,
        heartbeatFrequencyMS: 10000,
        retryWrites: true
    });
    await client.connect();
    const db = client.db('encrypted_chat');
    usersCol = db.collection('users');
    requestsCol = db.collection('friend_requests');
    friendshipsCol = db.collection('friendships');
    messagesCol = db.collection('messages');
    storiesCol = db.collection('stories');
    reelsCol = db.collection('reels');
    reelLikesCol = db.collection('reel_likes');
    reelCommentsCol = db.collection('reel_comments');
    musicCol = db.collection('music');
    shortsCol = db.collection('shorts_cache');
    shortLikesCol = db.collection('short_likes');
    starredCol = db.collection('starred_messages');
    archivedCol = db.collection('archived_chats');

    // Indexes (createIndex is idempotent & fast)
    await Promise.all([
        usersCol.createIndex({ username: 1 }, { unique: true }),
        usersCol.createIndex({ email: 1 }, { unique: true }),
        messagesCol.createIndex({ from: 1, to: 1, timestamp: -1 }),
        messagesCol.createIndex({ to: 1, from: 1, timestamp: -1 }),
        requestsCol.createIndex({ to: 1 }),
        friendshipsCol.createIndex({ user1: 1 }),
        friendshipsCol.createIndex({ user2: 1 }),
        storiesCol.createIndex({ userId: 1, createdAt: -1 }),
        reelsCol.createIndex({ createdAt: -1 }),
        reelLikesCol.createIndex({ reelId: 1, userId: 1 }, { unique: true }),
        reelCommentsCol.createIndex({ reelId: 1, createdAt: -1 }),
        musicCol.createIndex({ userId: 1, addedAt: -1 }),
        shortsCol.createIndex({ videoId: 1, category: 1 }, { unique: true }),
        shortsCol.createIndex({ category: 1, fetchedAt: -1 }),
        shortLikesCol.createIndex({ videoId: 1, userId: 1 }, { unique: true }),
        starredCol.createIndex({ userId: 1, messageId: 1 }, { unique: true }),
        archivedCol.createIndex({ userId: 1, friendId: 1 }, { unique: true })
    ]);

    const VERSION = 3;
    const metaCol = db.collection('shorts_meta');
    const meta = await metaCol.findOne({ key: 'version' });
    if (!meta || meta.value !== VERSION) {
        await shortsCol.deleteMany({});
        await metaCol.updateOne({ key: 'version' }, { $set: { value: VERSION } }, { upsert: true });
    }

    dbReady = true;
    console.log(`✅ MongoDB connected in ${Date.now() - t0}ms`);
}

/* ===== LIGHT HEALTH ROUTES ===== */
app.get('/api/ping', (req, res) => res.json({ ok: true, t: Date.now(), db: dbReady, storage: { r2: r2Ready, cloudinary: cloudinaryReady }, youtube: youtubeApiReady }));
app.get('/api/config', (req, res) => res.json({ googleClientId: GOOGLE_CLIENT_ID }));

/* ===== UPLOAD ===== */
function buildFileName(n) {
    const ext = (n || '').split('.').pop() || 'bin';
    const safeExt = ext.length > 5 ? 'bin' : ext;
    return `${Date.now()}_${Math.random().toString(36).slice(2, 10)}.${safeExt}`;
}

async function uploadToR2(buf, mime, fileName) {
    if (!r2Ready) throw new Error('R2 not configured');
    const key = `uploads/${buildFileName(fileName)}`;
    await r2Client.send(new PutObjectCommand({
        Bucket: R2_BUCKET, Key: key, Body: buf,
        ContentType: mime || 'application/octet-stream',
        CacheControl: 'public, max-age=31536000, immutable',
    }));
    return { url: R2_PUBLIC_URL ? `${R2_PUBLIC_URL}/${key}` : `https://${process.env.R2_ACCOUNT_ID}.r2.cloudflarestorage.com/${R2_BUCKET}/${key}`, publicId: key, provider: 'r2' };
}

async function uploadToCloudinary(buf, mime) {
    if (!cloudinaryReady) throw new Error('Cloudinary not configured');
    let rt = 'raw';
    if (mime.startsWith('image/')) rt = 'image';
    else if (mime.startsWith('video/') || mime.startsWith('audio/')) rt = 'video';
    const result = await new Promise((resolve, reject) => {
        const stream = cloudinary.uploader.upload_stream({ folder: 'encrypted_chat', resource_type: rt, unique_filename: true }, (err, r) => err ? reject(err) : resolve(r));
        stream.end(buf);
    });
    return { url: result.secure_url, publicId: result.public_id, provider: 'cloudinary' };
}

app.post('/api/upload', async (req, res) => {
    try {
        const { base64, fileName } = req.body;
        if (!base64) return res.status(400).json({ error: 'No data' });
        const match = base64.match(/^data:([^;]+);base64,(.+)$/);
        if (!match) return res.status(400).json({ error: 'Invalid base64' });
        const mime = match[1] || 'application/octet-stream';
        const buf = Buffer.from(match[2], 'base64');
        if (buf.length > 20 * 1024 * 1024) return res.status(400).json({ error: 'File too large (max 20MB)' });
        let result = null;
        const isAudio = mime.startsWith('audio/');
        if (isAudio) {
            if (r2Ready) { try { result = await uploadToR2(buf, mime, fileName); } catch (e) {} }
            if (!result && cloudinaryReady) { try { result = await uploadToCloudinary(buf, mime); } catch (e) {} }
        } else {
            if (STORAGE_PROVIDER === 'r2') result = await uploadToR2(buf, mime, fileName);
            else if (STORAGE_PROVIDER === 'cloudinary') result = await uploadToCloudinary(buf, mime);
            else {
                try { if (r2Ready) result = await uploadToR2(buf, mime, fileName); else throw new Error('R2 not ready'); }
                catch (e) { result = await uploadToCloudinary(buf, mime); }
            }
        }
        if (!result) return res.status(500).json({ error: 'No storage available' });
        res.json({ url: result.url, publicId: result.publicId, provider: result.provider });
    } catch (err) { console.error('Upload error:', err); res.status(500).json({ error: 'Upload failed: ' + (err.message || 'unknown') }); }
});

/* ===== TIKTOK ===== */
app.post('/api/tiktok/resolve', async (req, res) => {
    try {
        const { url } = req.body;
        if (!url) return res.status(400).json({ error: 'No URL' });
        let finalUrl = url.trim();
        if (/vm\.tiktok\.com|vt\.tiktok\.com/i.test(finalUrl)) {
            try { const r = await fetch(finalUrl, { redirect: 'follow', headers: { 'User-Agent': 'Mozilla/5.0' } }); finalUrl = r.url || finalUrl; } catch (e) {}
        }
        const patterns = [/tiktok\.com\/@[^/?#]+\/video\/(\d+)/i, /tiktok\.com\/v\/(\d+)/i, /tiktok\.com\/embed\/v2\/(\d+)/i];
        for (const p of patterns) { const m = finalUrl.match(p); if (m) return res.json({ videoId: m[1] }); }
        res.status(400).json({ error: 'Invalid TikTok URL' });
    } catch (err) { res.status(500).json({ error: 'Failed' }); }
});

/* ===== YOUTUBE ===== */
app.post('/api/youtube/oembed', async (req, res) => {
    try {
        const { videoId } = req.body;
        if (!videoId) return res.status(400).json({ error: 'Missing videoId' });
        const r = await fetch(`https://www.youtube.com/oembed?url=https://www.youtube.com/watch?v=${videoId}&format=json`);
        if (!r.ok) return res.status(400).json({ error: 'Video not found' });
        const data = await r.json();
        res.json({ videoId, title: data.title, thumbnail: data.thumbnail_url, author: data.author_name });
    } catch (err) { res.status(500).json({ error: 'Failed' }); }
});

app.get('/api/youtube/search', async (req, res) => {
    try {
        if (!youtubeApiReady) return res.status(500).json({ error: 'YouTube not configured' });
        const { q, maxResults = 20 } = req.query;
        if (!q || q.trim().length < 2) return res.json({ items: [] });
        const url = `https://www.googleapis.com/youtube/v3/search?part=snippet&type=video&videoEmbeddable=true&maxResults=${Math.min(parseInt(maxResults), 50)}&q=${encodeURIComponent(q)}&key=${YOUTUBE_API_KEY}`;
        const r = await fetch(url);
        const data = await r.json();
        if (!r.ok) return res.status(r.status).json({ error: (data.error && data.error.message) || 'Search failed' });
        res.json({ items: (data.items || []).map(it => ({ videoId: it.id.videoId, title: it.snippet.title, thumbnail: it.snippet.thumbnails.medium?.url, author: it.snippet.channelTitle })) });
    } catch (err) { res.status(500).json({ error: 'Failed' }); }
});

/* ===== MUSIC ===== */
app.post('/api/music', async (req, res) => {
    try {
        const { userId, videoId, title, thumbnail, author } = req.body;
        if (!userId || !videoId) return res.status(400).json({ error: 'Missing' });
        const doc = { userId, videoId, title: title || 'Unknown', thumbnail, author: author || 'Unknown', addedAt: Date.now() };
        const result = await musicCol.insertOne(doc);
        res.json({ song: { ...doc, _id: result.insertedId.toString() } });
    } catch (err) { res.status(500).json({ error: 'Failed' }); }
});

app.get('/api/music', async (req, res) => {
    try {
        const { userId } = req.query;
        const songs = await musicCol.find({ userId }).sort({ addedAt: -1 }).toArray();
        res.json(songs.map(s => ({ _id: s._id.toString(), videoId: s.videoId, title: s.title, thumbnail: s.thumbnail, author: s.author, addedAt: s.addedAt })));
    } catch (err) { res.status(500).json({ error: 'Failed' }); }
});

app.delete('/api/music/:id', async (req, res) => {
    try {
        const { userId } = req.query;
        const song = await musicCol.findOne({ _id: new ObjectId(req.params.id) });
        if (!song || song.userId !== userId) return res.status(403).json({ error: 'Not allowed' });
        await musicCol.deleteOne({ _id: song._id });
        res.json({ success: true });
    } catch (err) { res.status(500).json({ error: 'Failed' }); }
});

/* ===== HELPERS ===== */
async function getFriendIds(userId) {
    const fs = await friendshipsCol.find({ $or: [{ user1: userId }, { user2: userId }] }).toArray();
    return fs.map(f => f.user1 === userId ? f.user2 : f.user1);
}

function formatUser(u) {
    const id = u._id.toString();
    return {
        id, username: u.username, fullName: u.fullName,
        avatar: u.avatar, avatarType: u.avatarType || 'letter',
        verified: !!u.verified,
        isOnline: onlineUsers.has(id),
        lastSeen: u.lastSeen || null,
        nowPlaying: (u.nowPlaying && (Date.now() - u.nowPlaying.startedAt < 30 * 60 * 1000)) ? u.nowPlaying : null
    };
}

async function notifyFriendsStatus(userId, isOnline) {
    try {
        const now = isOnline ? null : Date.now();
        if (!isOnline) usersCol.updateOne({ _id: new ObjectId(userId) }, { $set: { lastSeen: now } }).catch(() => {});
        const fids = await getFriendIds(userId);
        fids.forEach(fid => { const sid = onlineUsers.get(fid); if (sid) io.to(sid).emit('user_status', { userId, isOnline, lastSeen: now }); });
    } catch (e) {}
}

async function notifyFriendsVerified(userId, verified) {
    try {
        const fids = await getFriendIds(userId);
        fids.forEach(fid => { const sid = onlineUsers.get(fid); if (sid) io.to(sid).emit('user_verified', { userId, verified }); });
    } catch (e) {}
}

async function notifyFriendsProfileUpdate(userId) {
    try {
        const user = await usersCol.findOne({ _id: new ObjectId(userId) });
        if (!user) return;
        const payload = { userId, username: user.username, fullName: user.fullName, avatar: user.avatar, avatarType: user.avatarType || 'letter', verified: !!user.verified };
        const fids = await getFriendIds(userId);
        fids.forEach(fid => { const sid = onlineUsers.get(fid); if (sid) io.to(sid).emit('user_profile_updated', payload); });
    } catch (e) {}
}

/* ============ AUTH ============ */

app.post('/api/register', async (req, res) => {
    try {
        const { fullName, username, email, password, avatar } = req.body;
        if (!fullName || !username || !email || !password) return res.status(400).json({ error: 'Fill all fields' });
        if (username.length < 3) return res.status(400).json({ error: 'Username too short' });
        if (password.length < 6) return res.status(400).json({ error: 'Password too short' });

        const dup = await usersCol.findOne({ $or: [{ username }, { email }] }, { projection: { username: 1, email: 1 } });
        if (dup) return res.status(400).json({ error: dup.username === username ? 'Username taken' : 'Email registered' });

        let avatarUrl = avatar, avatarType = 'letter';
        if (avatar && avatar.startsWith('data:')) {
            const match = avatar.match(/^data:([^;]+);base64,(.+)$/);
            if (match) {
                const mime = match[1]; const buf = Buffer.from(match[2], 'base64');
                try {
                    let r = r2Ready ? await uploadToR2(buf, mime, 'avatar_' + Date.now()) : (cloudinaryReady ? await uploadToCloudinary(buf, mime) : null);
                    if (r) { avatarUrl = r.url; avatarType = mime === 'image/gif' ? 'gif' : 'image'; }
                } catch (e) {}
            }
        } else if (avatar) { avatarType = 'image'; }
        if (!avatarUrl) avatarUrl = fullName[0].toUpperCase();

        const newUser = { fullName, username, email, password, avatar: avatarUrl, avatarType, verified: false, lastSeen: Date.now(), createdAt: Date.now(), blockedUsers: [], blockedBy: [] };
        const result = await usersCol.insertOne(newUser);
        res.json({ user: { id: result.insertedId.toString(), username, fullName, email, avatar: newUser.avatar, avatarType: newUser.avatarType, verified: false, hasPassword: true } });
    } catch (err) { console.error('Register error:', err); res.status(500).json({ error: 'Server error' }); }
});

/* ⚡ OPTIMIZED LOGIN — the main fix */
app.post('/api/login', async (req, res) => {
    const t0 = Date.now();
    try {
        const { username, password } = req.body;
        if (!username || !password) return res.status(400).json({ error: 'Please enter username and password' });

        // Query only by username OR email — let the index do the work
        const user = await usersCol.findOne(
            { $or: [{ username }, { email: username }] },
            { projection: { username: 1, fullName: 1, email: 1, avatar: 1, avatarType: 1, verified: 1, password: 1 } }
        );

        if (!user || user.password !== password) {
            return res.status(400).json({ error: 'Incorrect username or password' });
        }

        // Fire-and-forget lastSeen (does NOT block response)
        usersCol.updateOne({ _id: user._id }, { $set: { lastSeen: Date.now() } }).catch(() => {});

        res.json({ user: {
            id: user._id.toString(),
            username: user.username,
            fullName: user.fullName,
            email: user.email || '',
            avatar: user.avatar,
            avatarType: user.avatarType || 'letter',
            verified: !!user.verified,
            hasPassword: !(user.password || '').startsWith('google-oauth:')
        }});
    } catch (err) {
        console.error('Login error:', err);
        res.status(500).json({ error: 'Server error' });
    }
});

app.post('/api/auth/google', async (req, res) => {
    try {
        const { credential } = req.body;
        if (!credential) return res.status(400).json({ error: 'No credential' });
        const verifyRes = await fetch('https://oauth2.googleapis.com/tokeninfo?id_token=' + encodeURIComponent(credential));
        if (!verifyRes.ok) return res.status(400).json({ error: 'Invalid token' });
        const info = await verifyRes.json();
        if (GOOGLE_CLIENT_ID && info.aud !== GOOGLE_CLIENT_ID) return res.status(400).json({ error: 'Audience mismatch' });
        const email = info.email, name = info.name || email.split('@')[0], picture = info.picture || '', googleId = info.sub;
        let user = await usersCol.findOne({ email });
        if (!user) {
            let base = email.split('@')[0].replace(/[^a-zA-Z0-9_.]/g, '').toLowerCase();
            if (base.length < 3) base = 'user' + Date.now().toString().slice(-4);
            let username = base, counter = 1;
            while (await usersCol.findOne({ username })) { username = base + '_' + counter; counter++; if (counter > 999) break; }
            const newUser = { fullName: name, username, email, password: 'google-oauth:' + googleId, avatar: picture || name[0].toUpperCase(), avatarType: picture ? 'image' : 'letter', verified: false, googleId, lastSeen: Date.now(), createdAt: Date.now(), blockedUsers: [], blockedBy: [] };
            const r = await usersCol.insertOne(newUser);
            user = { ...newUser, _id: r.insertedId };
        }
        res.json({ user: { id: user._id.toString(), username: user.username, fullName: user.fullName, email: user.email, avatar: user.avatar, avatarType: user.avatarType || 'letter', verified: !!user.verified, hasPassword: !(user.password || '').startsWith('google-oauth:') } });
    } catch (err) { res.status(500).json({ error: 'Google failed' }); }
});

app.post('/api/users/avatar', async (req, res) => {
    try {
        const { userId, avatar } = req.body;
        let avatarUrl = avatar, avatarType = 'image';
        if (avatar.startsWith('data:')) {
            const match = avatar.match(/^data:([^;]+);base64,(.+)$/);
            if (match) {
                const mime = match[1]; const buf = Buffer.from(match[2], 'base64');
                if (buf.length > 5 * 1024 * 1024) return res.status(400).json({ error: 'Too large (max 5MB)' });
                try {
                    let r = r2Ready ? await uploadToR2(buf, mime, 'avatar_' + Date.now()) : (cloudinaryReady ? await uploadToCloudinary(buf, mime) : null);
                    if (r) { avatarUrl = r.url; avatarType = mime === 'image/gif' ? 'gif' : 'image'; }
                } catch (e) {}
            }
        }
        await usersCol.updateOne({ _id: new ObjectId(userId) }, { $set: { avatar: avatarUrl, avatarType } });
        notifyFriendsProfileUpdate(userId).catch(() => {});
        res.json({ success: true, avatar: avatarUrl, avatarType });
    } catch (err) { res.status(500).json({ error: 'Server error' }); }
});

app.post('/api/users/verify', async (req, res) => {
    try {
        const { userId, verified } = req.body;
        const v = !!verified;
        await usersCol.updateOne({ _id: new ObjectId(userId) }, { $set: { verified: v } });
        notifyFriendsVerified(userId, v).catch(() => {});
        notifyFriendsProfileUpdate(userId).catch(() => {});
        res.json({ success: true, verified: v });
    } catch (err) { res.status(500).json({ error: 'Failed' }); }
});

app.post('/api/users/username', async (req, res) => {
    try {
        const { userId, newUsername } = req.body;
        if (!newUsername || newUsername.length < 3) return res.status(400).json({ error: 'Too short' });
        if (!/^[a-zA-Z0-9_.]+$/.test(newUsername)) return res.status(400).json({ error: 'Invalid chars' });
        const existing = await usersCol.findOne({ username: newUsername }, { projection: { _id: 1 } });
        if (existing && existing._id.toString() !== userId) return res.status(400).json({ error: 'Taken' });
        await usersCol.updateOne({ _id: new ObjectId(userId) }, { $set: { username: newUsername } });
        const user = await usersCol.findOne({ _id: new ObjectId(userId) }, { projection: { username: 1, email: 1, avatar: 1, avatarType: 1, verified: 1 } });
        notifyFriendsProfileUpdate(userId).catch(() => {});
        res.json({ success: true, username: user.username, email: user.email || '', avatar: user.avatar, avatarType: user.avatarType || 'letter', verified: !!user.verified });
    } catch (err) { res.status(500).json({ error: 'Failed' }); }
});

app.post('/api/users/password', async (req, res) => {
    try {
        const { userId, oldPassword, newPassword } = req.body;
        if (newPassword.length < 6) return res.status(400).json({ error: 'Too short' });
        const user = await usersCol.findOne({ _id: new ObjectId(userId) }, { projection: { password: 1 } });
        if (!user) return res.status(404).json({ error: 'Not found' });
        if (user.password.startsWith('google-oauth:')) return res.status(400).json({ error: 'Google account' });
        if (user.password !== oldPassword) return res.status(400).json({ error: 'Wrong password' });
        await usersCol.updateOne({ _id: new ObjectId(userId) }, { $set: { password: newPassword } });
        res.json({ success: true });
    } catch (err) { res.status(500).json({ error: 'Failed' }); }
});

app.get('/api/users/profile/:username', async (req, res) => {
    try {
        const user = await usersCol.findOne({ username: req.params.username });
        if (!user) return res.status(404).json({ error: 'Not found' });
        res.json(formatUser(user));
    } catch (err) { res.status(500).json({ error: 'Failed' }); }
});

app.get('/api/users/search', async (req, res) => {
    try {
        const { q, myId } = req.query;
        if (!q || q.length < 2) return res.json([]);
        const regex = new RegExp('^' + q.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'), 'i');
        const users = await usersCol.find({ username: regex, _id: { $ne: new ObjectId(myId) } }).project({ username: 1, fullName: 1, avatar: 1, avatarType: 1, lastSeen: 1, verified: 1 }).limit(20).toArray();
        res.json(users.map(formatUser));
    } catch (err) { res.status(500).json({ error: 'Failed' }); }
});

/* ===== BLOCK ===== */
app.post('/api/users/block', async (req, res) => {
    try {
        const { userId, targetId } = req.body;
        if (userId === targetId) return res.status(400).json({ error: 'Cannot block yourself' });
        await Promise.all([
            usersCol.updateOne({ _id: new ObjectId(userId) }, { $addToSet: { blockedUsers: targetId } }),
            usersCol.updateOne({ _id: new ObjectId(targetId) }, { $addToSet: { blockedBy: userId } })
        ]);
        const s = onlineUsers.get(userId); if (s) io.to(s).emit('user_blocked', { userId, targetId });
        const t = onlineUsers.get(targetId); if (t) io.to(t).emit('you_were_blocked', { by: userId });
        res.json({ success: true });
    } catch (err) { res.status(500).json({ error: 'Failed' }); }
});

app.post('/api/users/unblock', async (req, res) => {
    try {
        const { userId, targetId } = req.body;
        await Promise.all([
            usersCol.updateOne({ _id: new ObjectId(userId) }, { $pull: { blockedUsers: targetId } }),
            usersCol.updateOne({ _id: new ObjectId(targetId) }, { $pull: { blockedBy: userId } })
        ]);
        const s = onlineUsers.get(userId); if (s) io.to(s).emit('user_unblocked', { userId, targetId });
        res.json({ success: true });
    } catch (err) { res.status(500).json({ error: 'Failed' }); }
});

app.get('/api/users/blocked', async (req, res) => {
    try {
        const { userId } = req.query;
        const user = await usersCol.findOne({ _id: new ObjectId(userId) }, { projection: { blockedUsers: 1 } });
        if (!user) return res.status(404).json({ error: 'Not found' });
        const ids = (user.blockedUsers || []).map(id => { try { return new ObjectId(id); } catch (e) { return null; } }).filter(Boolean);
        const blocked = await usersCol.find({ _id: { $in: ids } }).project({ username: 1, fullName: 1, avatar: 1, avatarType: 1, verified: 1 }).toArray();
        res.json(blocked.map(u => ({ id: u._id.toString(), username: u.username, fullName: u.fullName, avatar: u.avatar, avatarType: u.avatarType || 'letter', verified: !!u.verified })));
    } catch (err) { res.status(500).json({ error: 'Failed' }); }
});

/* ===== FRIENDS ===== */
app.post('/api/friend-request', async (req, res) => {
    try {
        const { from, to } = req.body;
        if (from === to) return res.status(400).json({ error: 'Cannot add self' });
        const targetDoc = await usersCol.findOne({ _id: new ObjectId(to) }, { projection: { blockedUsers: 1 } });
        if (targetDoc?.blockedUsers?.includes(from)) return res.status(400).json({ error: 'Cannot send request' });
        const existing = await friendshipsCol.findOne({ $or: [{ user1: from, user2: to }, { user1: to, user2: from }] }, { projection: { _id: 1 } });
        if (existing) return res.status(400).json({ error: 'Already friends' });
        const reqExist = await requestsCol.findOne({ from, to }, { projection: { _id: 1 } });
        if (reqExist) return res.status(400).json({ error: 'Request already sent' });
        const reverse = await requestsCol.findOne({ from: to, to: from });
        if (reverse) {
            await requestsCol.deleteOne({ _id: reverse._id });
            await friendshipsCol.insertOne({ user1: from, user2: to, createdAt: Date.now() });
            const s1 = onlineUsers.get(to), s2 = onlineUsers.get(from);
            if (s1) io.to(s1).emit('friend_added');
            if (s2) io.to(s2).emit('friend_added');
            return res.json({ success: true, autoAccepted: true });
        }
        await requestsCol.insertOne({ from, to, createdAt: Date.now() });
        const rs = onlineUsers.get(to); if (rs) io.to(rs).emit('new_friend_request', { from });
        res.json({ success: true });
    } catch (err) { res.status(500).json({ error: 'Failed' }); }
});

app.get('/api/friend-requests', async (req, res) => {
    try {
        const { userId } = req.query;
        const reqs = await requestsCol.find({ to: userId }).toArray();
        if (!reqs.length) return res.json([]);
        const ids = reqs.map(r => { try { return new ObjectId(r.from); } catch (e) { return null; } }).filter(Boolean);
        const senders = await usersCol.find({ _id: { $in: ids } }).project({ username: 1, fullName: 1, avatar: 1, avatarType: 1, lastSeen: 1, verified: 1 }).toArray();
        const map = new Map(senders.map(s => [s._id.toString(), s]));
        res.json(reqs.map(r => { const s = map.get(r.from); return s ? formatUser(s) : null; }).filter(Boolean));
    } catch (err) { res.status(500).json({ error: 'Failed' }); }
});

app.post('/api/friend-request/accept', async (req, res) => {
    try {
        const { userId, friendId } = req.body;
        await requestsCol.deleteMany({ from: friendId, to: userId });
        await friendshipsCol.insertOne({ user1: userId, user2: friendId, createdAt: Date.now() });
        const ss = onlineUsers.get(friendId), ms = onlineUsers.get(userId);
        if (ss) io.to(ss).emit('friend_added');
        if (ms) io.to(ms).emit('friend_added');
        res.json({ success: true });
    } catch (err) { res.status(500).json({ error: 'Failed' }); }
});

app.get('/api/friends', async (req, res) => {
    try {
        const { userId } = req.query;
        const friendships = await friendshipsCol.find({ $or: [{ user1: userId }, { user2: userId }] }).toArray();
        if (!friendships.length) return res.json([]);
        const ids = friendships.map(f => f.user1 === userId ? f.user2 : f.user1).filter(id => { try { new ObjectId(id); return true; } catch (e) { return false; } }).map(id => new ObjectId(id));
        const friends = await usersCol.find({ _id: { $in: ids } }).project({ username: 1, fullName: 1, avatar: 1, avatarType: 1, lastSeen: 1, verified: 1 }).toArray();
        res.json(friends.map(formatUser));
    } catch (err) { res.status(500).json({ error: 'Failed' }); }
});

app.delete('/api/friends/:friendId', async (req, res) => {
    try {
        const { friendId } = req.params;
        const { userId, deleteMessages } = req.query;
        await friendshipsCol.deleteMany({ $or: [{ user1: userId, user2: friendId }, { user1: friendId, user2: userId }] });
        await requestsCol.deleteMany({ $or: [{ from: userId, to: friendId }, { from: friendId, to: userId }] });
        if (deleteMessages === 'true') await messagesCol.deleteMany({ $or: [{ from: userId, to: friendId }, { from: friendId, to: userId }] });
        const o = onlineUsers.get(friendId), s = onlineUsers.get(userId);
        if (o) io.to(o).emit('friend_removed', { by: userId });
        if (s) io.to(s).emit('friend_removed', { by: userId });
        res.json({ success: true });
    } catch (err) { res.status(500).json({ error: 'Failed' }); }
});

/* ===== CONVERSATIONS ===== */
app.get('/api/conversations', async (req, res) => {
    try {
        const { userId } = req.query;
        const friendships = await friendshipsCol.find({ $or: [{ user1: userId }, { user2: userId }] }).toArray();
        if (!friendships.length) return res.json([]);
        const meDoc = await usersCol.findOne({ _id: new ObjectId(userId) }, { projection: { blockedUsers: 1, blockedBy: 1 } });
        const bM = meDoc?.blockedUsers || [], bMe = meDoc?.blockedBy || [];
        const friendIds = friendships.map(f => f.user1 === userId ? f.user2 : f.user1).filter(fid => !bM.includes(fid) && !bMe.includes(fid));
        if (!friendIds.length) return res.json([]);
        const fObjIds = friendIds.map(id => { try { return new ObjectId(id); } catch (e) { return null; } }).filter(Boolean);

        // Run queries in parallel
        const [friends, lastMsgs, unreadAgg] = await Promise.all([
            usersCol.find({ _id: { $in: fObjIds } }).project({ username: 1, fullName: 1, avatar: 1, avatarType: 1, lastSeen: 1, verified: 1, nowPlaying: 1 }).toArray(),
            messagesCol.aggregate([
                { $match: { $or: [{ from: userId, to: { $in: friendIds } }, { from: { $in: friendIds }, to: userId }] } },
                { $sort: { timestamp: -1 } },
                { $group: { _id: { $cond: [{ $eq: ['$from', userId] }, '$to', '$from'] }, lastMsg: { $first: '$$ROOT' } } }
            ]).toArray(),
            messagesCol.aggregate([
                { $match: { to: userId, from: { $in: friendIds }, read: false } },
                { $group: { _id: '$from', count: { $sum: 1 } } }
            ]).toArray()
        ]);

        const lastMsgMap = new Map();
        lastMsgs.forEach(i => { if (i.lastMsg) lastMsgMap.set(i._id, i.lastMsg); });
        const unreadMap = new Map(unreadAgg.map(u => [u._id, u.count]));

        const results = friends.map(f => {
            const fid = f._id.toString();
            const lm = lastMsgMap.get(fid);
            return { friend: formatUser(f), lastMessage: lm ? { text: lm.text || '', type: lm.type || 'text', timestamp: lm.timestamp, fromMe: lm.from === userId } : null, unreadCount: unreadMap.get(fid) || 0 };
        });
        results.sort((a, b) => ((b.lastMessage?.timestamp || 0) - (a.lastMessage?.timestamp || 0)));
        res.json(results);
    } catch (err) { console.error(err); res.status(500).json({ error: 'Failed' }); }
});

/* ===== MESSAGES ===== */
app.get('/api/messages', async (req, res) => {
    try {
        const { userId, friendId, before, limit = 50 } = req.query;
        if (!userId || !friendId) return res.status(400).json({ error: 'Missing' });
        const lim = Math.min(parseInt(limit), 100);
        const query = { $or: [{ from: userId, to: friendId }, { from: friendId, to: userId }] };
        if (before) query.timestamp = { $lt: parseInt(before) };
        const msgs = await messagesCol.find(query).sort({ timestamp: -1 }).limit(lim + 1).toArray();
        const hasMore = msgs.length > lim;
        if (hasMore) msgs.pop();
        msgs.reverse();
        res.json({
            messages: msgs.map(m => ({ _id: m._id.toString(), from: m.from, to: m.to, text: m.text || '', type: m.type || 'text', media: m.media || null, timestamp: m.timestamp, delivered: !!m.delivered, read: !!m.read, isEdited: !!m.isEdited, reactions: m.reactions || {}, replyTo: m.replyTo || null, replyText: m.replyText || null, isStarred: !!m.isStarred, isPinned: !!m.isPinned })),
            hasMore
        });
    } catch (err) { res.status(500).json({ error: 'Failed' }); }
});

app.get('/api/messages/search', async (req, res) => {
    try {
        const { userId, friendId, q } = req.query;
        if (!userId || !friendId || !q || q.length < 2) return res.json([]);
        const regex = new RegExp(q.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'), 'i');
        const msgs = await messagesCol.find({ $or: [{ from: userId, to: friendId }, { from: friendId, to: userId }], text: regex, type: 'text' }).sort({ timestamp: -1 }).limit(50).toArray();
        res.json(msgs.map(m => ({ _id: m._id.toString(), from: m.from, to: m.to, text: m.text, timestamp: m.timestamp })));
    } catch (err) { res.status(500).json({ error: 'Failed' }); }
});

app.post('/api/messages/:id/star', async (req, res) => {
    try {
        const { userId } = req.body;
        const msgId = req.params.id;
        const existing = await starredCol.findOne({ userId, messageId: msgId });
        if (existing) {
            await starredCol.deleteOne({ _id: existing._id });
            messagesCol.updateOne({ _id: new ObjectId(msgId) }, { $set: { isStarred: false } }).catch(() => {});
            res.json({ starred: false });
        } else {
            await starredCol.insertOne({ userId, messageId: msgId, createdAt: Date.now() });
            messagesCol.updateOne({ _id: new ObjectId(msgId) }, { $set: { isStarred: true } }).catch(() => {});
            res.json({ starred: true });
        }
    } catch (err) { res.status(500).json({ error: 'Failed' }); }
});

app.post('/api/messages/:id/pin', async (req, res) => {
    try {
        const { userId, friendId } = req.body;
        await messagesCol.updateMany({ $or: [{ from: userId, to: friendId }, { from: friendId, to: userId }], isPinned: true }, { $set: { isPinned: false } });
        await messagesCol.updateOne({ _id: new ObjectId(req.params.id) }, { $set: { isPinned: true } });
        const o = onlineUsers.get(friendId), s = onlineUsers.get(userId);
        if (o) io.to(o).emit('message_pinned', { messageId: req.params.id, by: userId });
        if (s) io.to(s).emit('message_pinned', { messageId: req.params.id, by: userId });
        res.json({ success: true });
    } catch (err) { res.status(500).json({ error: 'Failed' }); }
});

app.get('/api/messages/pinned/:friendId', async (req, res) => {
    try {
        const { userId } = req.query;
        const msg = await messagesCol.findOne({ $or: [{ from: userId, to: req.params.friendId }, { from: req.params.friendId, to: userId }], isPinned: true });
        if (!msg) return res.json(null);
        res.json({ _id: msg._id.toString(), text: msg.text, type: msg.type, media: msg.media, timestamp: msg.timestamp });
    } catch (err) { res.json(null); }
});

app.delete('/api/messages/:id', async (req, res) => {
    try {
        const { userId } = req.query;
        const msg = await messagesCol.findOne({ _id: new ObjectId(req.params.id) });
        if (!msg || msg.from !== userId) return res.status(403).json({ error: 'Not allowed' });
        await messagesCol.deleteOne({ _id: msg._id });
        const o = onlineUsers.get(msg.to), s = onlineUsers.get(userId);
        if (o) io.to(o).emit('message_deleted', { id: req.params.id, by: userId });
        if (s) io.to(s).emit('message_deleted', { id: req.params.id, by: userId });
        res.json({ success: true });
    } catch (err) { res.status(500).json({ error: 'Failed' }); }
});

app.delete('/api/conversations/:friendId', async (req, res) => {
    try {
        const { userId } = req.query;
        await messagesCol.deleteMany({ $or: [{ from: userId, to: req.params.friendId }, { from: req.params.friendId, to: userId }] });
        const o = onlineUsers.get(req.params.friendId), s = onlineUsers.get(userId);
        if (o) io.to(o).emit('conversation_cleared', { by: userId });
        if (s) io.to(s).emit('conversation_cleared', { by: userId });
        res.json({ success: true });
    } catch (err) { res.status(500).json({ error: 'Failed' }); }
});

/* ===== STORIES ===== */
app.post('/api/stories', async (req, res) => {
    try {
        const { userId, media, type, caption } = req.body;
        if (!media) return res.status(400).json({ error: 'No media' });
        await storiesCol.insertOne({ userId, media, type: type || 'image', caption: caption || '', createdAt: Date.now() });
        const fids = await getFriendIds(userId);
        fids.forEach(fid => { const s = onlineUsers.get(fid); if (s) io.to(s).emit('story_updated', { userId }); });
        res.json({ success: true });
    } catch (err) { res.status(500).json({ error: 'Failed' }); }
});

app.delete('/api/stories/:id', async (req, res) => {
    try {
        const { userId } = req.query;
        const story = await storiesCol.findOne({ _id: new ObjectId(req.params.id) });
        if (!story || story.userId !== userId) return res.status(403).json({ error: 'Not allowed' });
        await storiesCol.deleteOne({ _id: story._id });
        const fids = await getFriendIds(userId);
        fids.forEach(fid => { const s = onlineUsers.get(fid); if (s) io.to(s).emit('story_updated', { userId }); });
        res.json({ success: true });
    } catch (err) { res.status(500).json({ error: 'Failed' }); }
});

app.get('/api/stories', async (req, res) => {
    try {
        const { userId } = req.query;
        const cutoff = Date.now() - 24 * 60 * 60 * 1000;
        const fids = await getFriendIds(userId);
        const ids = [userId, ...fids];
        const stories = await storiesCol.find({ userId: { $in: ids }, createdAt: { $gt: cutoff } }).sort({ createdAt: 1 }).toArray();
        if (!stories.length) return res.json([]);
        const uids = [...new Set(stories.map(s => s.userId))].map(id => { try { return new ObjectId(id); } catch (e) { return null; } }).filter(Boolean);
        const users = await usersCol.find({ _id: { $in: uids } }).project({ username: 1, fullName: 1, avatar: 1, avatarType: 1, verified: 1 }).toArray();
        const map = new Map(users.map(u => [u._id.toString(), u]));
        const grouped = new Map();
        stories.forEach(s => { if (!grouped.has(s.userId)) grouped.set(s.userId, []); grouped.get(s.userId).push({ id: s._id.toString(), media: s.media, type: s.type, caption: s.caption, createdAt: s.createdAt }); });
        const result = [];
        const order = [userId, ...fids.filter(f => f !== userId)];
        order.forEach(uid => {
            if (!grouped.has(uid)) return;
            const u = map.get(uid);
            if (!u) return;
            result.push({ userId: uid, username: u.username, fullName: u.fullName, avatar: u.avatar, avatarType: u.avatarType || 'letter', verified: !!u.verified, isOwn: uid === userId, stories: grouped.get(uid) });
        });
        res.json(result);
    } catch (err) { res.status(500).json({ error: 'Failed' }); }
});

/* ===== REELS ===== */
async function buildReel(r, currentUserId) {
    let user = null;
    try { user = await usersCol.findOne({ _id: new ObjectId(r.userId) }, { projection: { username: 1, fullName: 1, avatar: 1, avatarType: 1, verified: 1 } }); } catch (e) {}
    const [likesCount, commentsCount] = await Promise.all([
        reelLikesCol.countDocuments({ reelId: r._id.toString() }),
        reelCommentsCol.countDocuments({ reelId: r._id.toString() })
    ]);
    let likedByMe = false;
    if (currentUserId) likedByMe = !!(await reelLikesCol.findOne({ reelId: r._id.toString(), userId: currentUserId }, { projection: { _id: 1 } }));
    return {
        _id: r._id.toString(), userId: r.userId,
        username: user ? user.username : 'unknown', fullName: user ? user.fullName : '',
        avatar: user ? user.avatar : '', avatarType: user ? (user.avatarType || 'letter') : 'letter',
        verified: user ? !!user.verified : false,
        type: r.type, media: r.media, caption: r.caption || '',
        createdAt: r.createdAt, likesCount, commentsCount, likedByMe
    };
}

app.post('/api/reels', async (req, res) => {
    try {
        const { userId, type, media, caption } = req.body;
        if (!userId || !type || !media) return res.status(400).json({ error: 'Missing' });
        const doc = { userId, type, media, caption: (caption || '').slice(0, 500), createdAt: Date.now() };
        const result = await reelsCol.insertOne(doc);
        const reel = await buildReel({ ...doc, _id: result.insertedId }, userId);
        onlineUsers.forEach(sid => io.to(sid).emit('reel:new', reel));
        res.json({ reel });
    } catch (err) { res.status(500).json({ error: 'Failed' }); }
});

app.get('/api/reels', async (req, res) => {
    try {
        const { userId, limit = 30, before } = req.query;
        const q = {};
        if (before) q.createdAt = { $lt: parseInt(before) };
        const reels = await reelsCol.find(q).sort({ createdAt: -1 }).limit(parseInt(limit)).toArray();
        const result = await Promise.all(reels.map(r => buildReel(r, userId)));
        res.json(result);
    } catch (err) { res.status(500).json({ error: 'Failed' }); }
});

app.get('/api/reels/mine', async (req, res) => {
    try {
        const { userId } = req.query;
        const reels = await reelsCol.find({ userId }).sort({ createdAt: -1 }).limit(100).toArray();
        const result = await Promise.all(reels.map(r => buildReel(r, userId)));
        res.json(result);
    } catch (err) { res.status(500).json({ error: 'Failed' }); }
});

app.post('/api/reels/:id/like', async (req, res) => {
    try {
        const { userId } = req.body;
        const existing = await reelLikesCol.findOne({ reelId: req.params.id, userId });
        let liked;
        if (existing) { await reelLikesCol.deleteOne({ _id: existing._id }); liked = false; }
        else { await reelLikesCol.insertOne({ reelId: req.params.id, userId, createdAt: Date.now() }); liked = true; }
        const likesCount = await reelLikesCol.countDocuments({ reelId: req.params.id });
        onlineUsers.forEach(sid => io.to(sid).emit('reel:like', { reelId: req.params.id, likesCount, likedBy: userId, liked }));
        res.json({ liked, likesCount });
    } catch (err) { res.status(500).json({ error: 'Failed' }); }
});

app.get('/api/reels/:id/comments', async (req, res) => {
    try {
        const cmts = await reelCommentsCol.find({ reelId: req.params.id }).sort({ createdAt: -1 }).limit(100).toArray();
        if (!cmts.length) return res.json([]);
        const uids = [...new Set(cmts.map(c => c.userId))].map(x => { try { return new ObjectId(x); } catch (e) { return null; } }).filter(Boolean);
        const users = await usersCol.find({ _id: { $in: uids } }).project({ username: 1, fullName: 1, avatar: 1, avatarType: 1, verified: 1 }).toArray();
        const umap = new Map(users.map(u => [u._id.toString(), u]));
        res.json(cmts.map(c => {
            const u = umap.get(c.userId);
            return { _id: c._id.toString(), userId: c.userId, username: u ? u.username : 'unknown', fullName: u ? u.fullName : '', avatar: u ? u.avatar : '', avatarType: u ? (u.avatarType || 'letter') : 'letter', verified: u ? !!u.verified : false, text: c.text, createdAt: c.createdAt };
        }));
    } catch (err) { res.status(500).json({ error: 'Failed' }); }
});

app.post('/api/reels/:id/comments', async (req, res) => {
    try {
        const { userId, text } = req.body;
        if (!userId || !text) return res.status(400).json({ error: 'Missing' });
        const doc = { reelId: req.params.id, userId, text: text.slice(0, 500), createdAt: Date.now() };
        const result = await reelCommentsCol.insertOne(doc);
        const user = await usersCol.findOne({ _id: new ObjectId(userId) }, { projection: { username: 1, fullName: 1, avatar: 1, avatarType: 1, verified: 1 } });
        const payload = { reelId: req.params.id, comment: { _id: result.insertedId.toString(), userId, text: doc.text, createdAt: doc.createdAt, username: user ? user.username : 'unknown', fullName: user ? user.fullName : '', avatar: user ? user.avatar : '', avatarType: user ? (user.avatarType || 'letter') : 'letter', verified: user ? !!user.verified : false } };
        onlineUsers.forEach(sid => io.to(sid).emit('reel:comment', payload));
        res.json({ comment: payload.comment });
    } catch (err) { res.status(500).json({ error: 'Failed' }); }
});

app.delete('/api/reels/comments/:id', async (req, res) => {
    try {
        const { userId } = req.query;
        const c = await reelCommentsCol.findOne({ _id: new ObjectId(req.params.id) });
        if (!c || c.userId !== userId) return res.status(403).json({ error: 'Not allowed' });
        await reelCommentsCol.deleteOne({ _id: c._id });
        onlineUsers.forEach(sid => io.to(sid).emit('reel:comment_deleted', { reelId: c.reelId, commentId: req.params.id }));
        res.json({ success: true });
    } catch (err) { res.status(500).json({ error: 'Failed' }); }
});

app.delete('/api/reels/:id', async (req, res) => {
    try {
        const { userId } = req.query;
        const r = await reelsCol.findOne({ _id: new ObjectId(req.params.id) });
        if (!r || r.userId !== userId) return res.status(403).json({ error: 'Not allowed' });
        await reelsCol.deleteOne({ _id: r._id });
        await reelLikesCol.deleteMany({ reelId: req.params.id });
        await reelCommentsCol.deleteMany({ reelId: req.params.id });
        onlineUsers.forEach(sid => io.to(sid).emit('reel:deleted', { reelId: req.params.id }));
        res.json({ success: true });
    } catch (err) { res.status(500).json({ error: 'Failed' }); }
});

/* ===== SHORTS ===== */
const SHORT_CATEGORIES = {
    trending: { q: 'shorts viral trending aesthetic edit' },
    aesthetic: { q: 'shorts aesthetic video edit vibes' },
    love: { q: 'shorts love status couple goals romantic' },
    sad: { q: 'shorts sad status emotional broken heart' },
    attitude: { q: 'shorts attitude status boy stylish' },
    dance: { q: 'shorts dance trending viral reel' },
    meme: { q: 'shorts funny meme comedy trending' },
    indian: { q: 'shorts indian love status video song' },
    bangla: { q: 'shorts bangla status video song romantic' },
    korean: { q: 'shorts korean aesthetic edit bts' },
    romantic: { q: 'shorts romantic couple love edit' },
    whatsapp: { q: 'shorts whatsapp status video love' },
    edit: { q: 'shorts edit video transition smooth' },
    girl: { q: 'shorts girl aesthetic outfit style' },
    boy: { q: 'shorts boy aesthetic style attitude' },
    music: { q: 'shorts song music lyrical edit' }
};

async function fetchShortsFromYouTube(query, maxResults = 50) {
    if (!youtubeApiReady) throw new Error('YouTube not configured');
    const url = `https://www.googleapis.com/youtube/v3/search?part=snippet&type=video&videoDuration=short&maxResults=${maxResults}&q=${encodeURIComponent(query)}&key=${YOUTUBE_API_KEY}`;
    const r = await fetch(url);
    const data = await r.json();
    if (!r.ok) throw new Error((data.error && data.error.message) || 'Failed');
    return (data.items || []).map(it => ({
        videoId: it.id.videoId, title: it.snippet.title,
        thumbnail: it.snippet.thumbnails.high?.url || it.snippet.thumbnails.medium?.url,
        author: it.snippet.channelTitle, publishedAt: it.snippet.publishedAt
    })).filter(v => v.videoId);
}

async function refreshShortsCache(category) {
    const cat = SHORT_CATEGORIES[category];
    if (!cat) throw new Error('Invalid');
    const items = await fetchShortsFromYouTube(cat.q, 50);
    const now = Date.now();
    if (items.length) {
        const ops = items.map(item => ({ updateOne: { filter: { videoId: item.videoId, category }, update: { $set: { ...item, category, fetchedAt: now } }, upsert: true } }));
        await shortsCol.bulkWrite(ops, { ordered: false });
    }
    return items.length;
}

app.get('/api/shorts/feed', async (req, res) => {
    try {
        const { limit = 20, userId } = req.query;
        const lim = parseInt(limit);
        let shorts = await shortsCol.aggregate([{ $sample: { size: lim } }]).toArray();
        if (!shorts.length) {
            await refreshShortsCache('trending').catch(() => {});
            shorts = await shortsCol.aggregate([{ $sample: { size: lim } }]).toArray();
        }
        let likedIds = new Set();
        if (userId && shorts.length) {
            const likes = await shortLikesCol.find({ userId, videoId: { $in: shorts.map(s => s.videoId) } }).toArray();
            likedIds = new Set(likes.map(l => l.videoId));
        }
        const likeCounts = shorts.length ? await shortLikesCol.aggregate([{ $match: { videoId: { $in: shorts.map(s => s.videoId) } } }, { $group: { _id: '$videoId', count: { $sum: 1 } } }]).toArray() : [];
        const countMap = new Map(likeCounts.map(c => [c._id, c.count]));
        res.json(shorts.map(s => ({ videoId: s.videoId, title: s.title, thumbnail: s.thumbnail, author: s.author, category: s.category, liked: likedIds.has(s.videoId), likesCount: countMap.get(s.videoId) || 0 })));
    } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/shorts/:videoId/like', async (req, res) => {
    try {
        const { userId } = req.body;
        const existing = await shortLikesCol.findOne({ videoId: req.params.videoId, userId });
        let liked;
        if (existing) { await shortLikesCol.deleteOne({ _id: existing._id }); liked = false; }
        else { await shortLikesCol.insertOne({ videoId: req.params.videoId, userId, likedAt: Date.now() }); liked = true; }
        const count = await shortLikesCol.countDocuments({ videoId: req.params.videoId });
        res.json({ liked, likesCount: count });
    } catch (e) { res.status(500).json({ error: 'Failed' }); }
});

/* ===== NOW PLAYING ===== */
app.post('/api/users/now-playing', async (req, res) => {
    try {
        const { userId, song, action } = req.body;
        const nowPlaying = (action === 'play' && song) ? { title: song.title, author: song.author, videoId: song.videoId, thumbnail: song.thumbnail, startedAt: Date.now() } : null;
        await usersCol.updateOne({ _id: new ObjectId(userId) }, { $set: { nowPlaying } });
        const fids = await getFriendIds(userId);
        fids.forEach(fid => { const sid = onlineUsers.get(fid); if (sid) io.to(sid).emit('friend_now_playing', { userId, nowPlaying }); });
        res.json({ success: true });
    } catch (err) { res.status(500).json({ error: 'Failed' }); }
});

/* ===== ADMIN ===== */
function checkAdmin(req, res) {
    if (req.headers['x-admin-pass'] !== ADMIN_PASSWORD) { res.status(401).json({ error: 'Unauthorized' }); return false; }
    return true;
}

app.post('/api/admin/verify', (req, res) => {
    const { password } = req.body || {};
    if (password === ADMIN_PASSWORD) return res.json({ ok: true });
    res.status(401).json({ error: 'Invalid password' });
});

app.get('/api/admin/stats', async (req, res) => {
    if (!checkAdmin(req, res)) return;
    try {
        const today = Date.now() - 24 * 60 * 60 * 1000;
        const [totalUsers, totalMsgs, totalStories, totalReels, totalFriendships, totalMusic, totalShorts, msgsToday, newUsersToday] = await Promise.all([
            usersCol.countDocuments({}), messagesCol.countDocuments({}), storiesCol.countDocuments({}),
            reelsCol.countDocuments({}), friendshipsCol.countDocuments({}), musicCol.countDocuments({}), shortsCol.countDocuments({}),
            messagesCol.countDocuments({ timestamp: { $gt: today } }), usersCol.countDocuments({ createdAt: { $gt: today } })
        ]);
        res.json({ totalUsers, onlineCount: onlineUsers.size, totalMsgs, totalStories, totalReels, totalFriendships, totalMusic, totalShorts, msgsToday, newUsersToday });
    } catch (e) { res.status(500).json({ error: 'Failed' }); }
});

app.get('/api/admin/users', async (req, res) => {
    if (!checkAdmin(req, res)) return;
    try {
        const users = await usersCol.find({}).toArray();
        const result = await Promise.all(users.map(async u => {
            const id = u._id.toString();
            const [sent, received, friends, stories, reels, music] = await Promise.all([
                messagesCol.countDocuments({ from: id }), messagesCol.countDocuments({ to: id }),
                friendshipsCol.countDocuments({ $or: [{ user1: id }, { user2: id }] }),
                storiesCol.countDocuments({ userId: id }), reelsCol.countDocuments({ userId: id }), musicCol.countDocuments({ userId: id })
            ]);
            const pw = u.password || '(none)';
            return { id, username: u.username, fullName: u.fullName || '', email: u.email || '', password: pw, isGoogle: pw.startsWith('google-oauth:'), avatar: u.avatar, avatarType: u.avatarType || 'letter', verified: !!u.verified, isOnline: onlineUsers.has(id), lastSeen: u.lastSeen || null, createdAt: u.createdAt || null, stats: { sent, received, total: sent + received, friends, stories, reels, music } };
        }));
        result.sort((a, b) => (b.lastSeen || 0) - (a.lastSeen || 0));
        res.json(result);
    } catch (e) { res.status(500).json({ error: 'Failed' }); }
});

app.get('/api/admin/activities', async (req, res) => {
    if (!checkAdmin(req, res)) return;
    try {
        const msgs = await messagesCol.find({}).sort({ timestamp: -1 }).limit(200).toArray();
        const ids = new Set();
        msgs.forEach(m => { ids.add(m.from); ids.add(m.to); });
        const validIds = [...ids].map(x => { try { return new ObjectId(x); } catch (e) { return null; } }).filter(Boolean);
        const us = await usersCol.find({ _id: { $in: validIds } }).project({ username: 1 }).toArray();
        const umap = new Map(us.map(u => [u._id.toString(), u.username]));
        res.json(msgs.map(m => ({ _id: m._id.toString(), from: m.from, fromName: umap.get(m.from) || 'unknown', to: m.to, toName: umap.get(m.to) || 'unknown', text: (m.text || '').slice(0, 200), type: m.type || 'text', timestamp: m.timestamp, read: !!m.read })));
    } catch (e) { res.status(500).json({ error: 'Failed' }); }
});

app.post('/api/admin/logout-all', async (req, res) => {
    if (!checkAdmin(req, res)) return;
    try {
        const { message } = req.body || {};
        const notification = message || 'New update available! Please login again to get the latest features.';
        onlineUsers.forEach(sid => io.to(sid).emit('force_logout', { message: notification, timestamp: Date.now() }));
        res.json({ success: true, count: onlineUsers.size });
    } catch (err) { res.status(500).json({ error: 'Failed' }); }
});

app.post('/api/admin/broadcast', async (req, res) => {
    if (!checkAdmin(req, res)) return;
    try {
        const { message } = req.body || {};
        if (!message) return res.status(400).json({ error: 'No message' });
        onlineUsers.forEach(sid => io.to(sid).emit('server_announcement', { message, timestamp: Date.now() }));
        res.json({ success: true, count: onlineUsers.size });
    } catch (err) { res.status(500).json({ error: 'Failed' }); }
});

app.delete('/api/admin/users/:userId', async (req, res) => {
    if (!checkAdmin(req, res)) return;
    try {
        const { userId } = req.params;
        let oid; try { oid = new ObjectId(userId); } catch (e) { return res.status(400).json({ error: 'bad id' }); }
        await Promise.all([
            usersCol.deleteOne({ _id: oid }),
            messagesCol.deleteMany({ $or: [{ from: userId }, { to: userId }] }),
            friendshipsCol.deleteMany({ $or: [{ user1: userId }, { user2: userId }] }),
            requestsCol.deleteMany({ $or: [{ from: userId }, { to: userId }] }),
            storiesCol.deleteMany({ userId }), reelsCol.deleteMany({ userId }),
            reelLikesCol.deleteMany({ userId }), reelCommentsCol.deleteMany({ userId }),
            musicCol.deleteMany({ userId }), starredCol.deleteMany({ userId }), archivedCol.deleteMany({ userId })
        ]);
        const sid = onlineUsers.get(userId);
        if (sid) { io.to(sid).emit('force_logout', { message: 'Your account has been deleted.', timestamp: Date.now() }); onlineUsers.delete(userId); }
        res.json({ success: true });
    } catch (err) { res.status(500).json({ error: 'Failed' }); }
});

/* ===== SOCKET ===== */
io.on('connection', (socket) => {
    socket.on('register', (userId) => {
        onlineUsers.set(userId, socket.id);
        socket.userId = userId;
        notifyFriendsStatus(userId, true).catch(() => {});
    });

    socket.on('send_message', async ({ from, to, text, type, media, fromName, replyTo, replyText }) => {
        try {
            const [senderDoc, recipientDoc] = await Promise.all([
                usersCol.findOne({ _id: new ObjectId(from) }, { projection: { blockedUsers: 1 } }),
                usersCol.findOne({ _id: new ObjectId(to) }, { projection: { blockedUsers: 1 } })
            ]);
            if (senderDoc?.blockedUsers?.includes(to)) return socket.emit('message_error', { error: 'You have blocked this user' });
            if (recipientDoc?.blockedUsers?.includes(from)) return socket.emit('message_error', { error: 'You are blocked' });
            const toOnline = onlineUsers.has(to);
            const msg = { from, to, text: text || '', type: type || 'text', media: media || null, timestamp: Date.now(), delivered: toOnline, read: false, isEdited: false, reactions: {}, replyTo: replyTo || null, replyText: replyText || null, isStarred: false, isPinned: false };
            const result = await messagesCol.insertOne(msg);
            const payload = { ...msg, _id: result.insertedId.toString(), fromName };
            socket.emit('message_sent', payload);
            const rs = onlineUsers.get(to);
            if (rs) io.to(rs).emit('receive_message', payload);
        } catch (err) { socket.emit('message_error', { error: 'Message save failed', details: err.message }); }
    });

    socket.on('edit_message', async ({ messageId, from, to, newText }) => {
        try {
            await messagesCol.updateOne({ _id: new ObjectId(messageId) }, { $set: { text: newText, isEdited: true, editedAt: Date.now() } });
            const payload = { messageId, newText, isEdited: true };
            const rs = onlineUsers.get(to), ss = onlineUsers.get(from);
            if (rs) io.to(rs).emit('message_edited', payload);
            if (ss) io.to(ss).emit('message_edited', payload);
        } catch (err) {}
    });

    socket.on('react_message', async ({ messageId, userId, reaction, from, to }) => {
        try {
            const msg = await messagesCol.findOne({ _id: new ObjectId(messageId) });
            if (!msg) return;
            let reactions = msg.reactions || {};
            let userPrev = Object.keys(reactions).filter(k => (reactions[k] || []).includes(userId));
            if (userPrev.includes(reaction)) {
                await messagesCol.updateOne({ _id: new ObjectId(messageId) }, { $pull: { [`reactions.${reaction}`]: userId } });
            } else {
                for (let r of userPrev) await messagesCol.updateOne({ _id: new ObjectId(messageId) }, { $pull: { [`reactions.${r}`]: userId } });
                await messagesCol.updateOne({ _id: new ObjectId(messageId) }, { $addToSet: { [`reactions.${reaction}`]: userId } });
            }
            const updated = await messagesCol.findOne({ _id: new ObjectId(messageId) });
            let clean = {};
            if (updated.reactions) Object.keys(updated.reactions).forEach(k => { if (updated.reactions[k].length > 0) clean[k] = updated.reactions[k]; });
            await messagesCol.updateOne({ _id: new ObjectId(messageId) }, { $set: { reactions: clean } });
            const payload = { messageId, reactions: clean };
            const rs = onlineUsers.get(to), ss = onlineUsers.get(from);
            if (rs) io.to(rs).emit('message_reacted', payload);
            if (ss) io.to(ss).emit('message_reacted', payload);
            socket.emit('message_reacted', payload);
        } catch (err) {}
    });

    socket.on('message:read', async ({ fromUserId, byUserId }) => {
        try {
            await messagesCol.updateMany({ from: fromUserId, to: byUserId, read: false }, { $set: { read: true, readAt: Date.now() } });
            const senderSocket = onlineUsers.get(fromUserId);
            if (senderSocket) io.to(senderSocket).emit('messages_read', { by: byUserId });
        } catch (err) {}
    });

    socket.on('typing:start', ({ from, to }) => { const rs = onlineUsers.get(to); if (rs) io.to(rs).emit('typing:start', { from }); });
    socket.on('typing:stop', ({ from, to }) => { const rs = onlineUsers.get(to); if (rs) io.to(rs).emit('typing:stop', { from }); });

    socket.on('now_playing_update', async ({ userId, nowPlaying }) => {
        try {
            await usersCol.updateOne({ _id: new ObjectId(userId) }, { $set: { nowPlaying } });
            const fids = await getFriendIds(userId);
            fids.forEach(fid => { const sid = onlineUsers.get(fid); if (sid) io.to(sid).emit('friend_now_playing', { userId, nowPlaying }); });
        } catch (err) {}
    });

    socket.on('admin_logout_user', ({ userId, message }) => {
        const sid = onlineUsers.get(userId);
        if (sid) { io.to(sid).emit('force_logout', { message: message || 'Session ended by admin', timestamp: Date.now() }); onlineUsers.delete(userId); }
    });

    socket.on('call:initiate', ({ fromId, toId, fromName, fromAvatar }) => { const rs = onlineUsers.get(toId); if (!rs) return socket.emit('call:unavailable', { reason: 'User is offline' }); io.to(rs).emit('call:incoming', { fromId, fromName, fromAvatar }); });
    socket.on('call:accept', ({ fromId, toId }) => { const cs = onlineUsers.get(toId); if (cs) io.to(cs).emit('call:accepted', { fromId }); });
    socket.on('call:reject', ({ fromId, toId }) => { const cs = onlineUsers.get(toId); if (cs) io.to(cs).emit('call:rejected', { fromId }); });
    socket.on('call:end', ({ fromId, toId }) => { const os = onlineUsers.get(toId); if (os) io.to(os).emit('call:ended', { fromId }); });
    socket.on('webrtc:offer', ({ toId, offer }) => { const ts = onlineUsers.get(toId); if (ts) io.to(ts).emit('webrtc:offer', { fromId: socket.userId, offer }); });
    socket.on('webrtc:answer', ({ toId, answer }) => { const ts = onlineUsers.get(toId); if (ts) io.to(ts).emit('webrtc:answer', { fromId: socket.userId, answer }); });
    socket.on('webrtc:ice', ({ toId, candidate }) => { const ts = onlineUsers.get(toId); if (ts) io.to(ts).emit('webrtc:ice', { fromId: socket.userId, candidate }); });

    socket.on('disconnect', async () => {
        for (const [uid, sid] of onlineUsers.entries()) {
            if (sid === socket.id) { onlineUsers.delete(uid); await notifyFriendsStatus(uid, false); break; }
        }
    });
});

/* ===== BACKGROUND TASKS (fully detached) ===== */
async function runShortsWarmup() {
    for (const cat of Object.keys(SHORT_CATEGORIES)) {
        try { await refreshShortsCache(cat); } catch (e) {}
        await new Promise(r => setTimeout(r, 2000));
    }
    console.log('✅ Shorts cache warmed');
}

/* ===== START SERVER IMMEDIATELY (before DB) ===== */
const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`✅ Server listening on port ${PORT}`));

connectDB().then(() => {
    // Background: warm shorts cache ONLY after 20s so first login isn't disturbed
    setTimeout(() => {
        runShortsWarmup().catch(() => {});
    }, 20000);
    // Refresh every 6 hours
    setInterval(() => { runShortsWarmup().catch(() => {}); }, 6 * 60 * 60 * 1000);
}).catch(err => {
    console.error('❌ MongoDB error:', err);
    dbError = err.message;
    // Don't exit — keep server up so it can recover
    setTimeout(() => { connectDB().then(() => { dbError = null; }).catch(() => {}); }, 5000);
});
