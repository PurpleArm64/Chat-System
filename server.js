const express = require('express');
const compression = require('compression');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');
const { MongoClient, ObjectId } = require('mongodb');
const { S3Client, PutObjectCommand } = require('@aws-sdk/client-s3');

const app = express();
app.disable('x-powered-by');
app.use(compression({ threshold: 512, level: 6 }));
app.use(cors());
app.use(express.json({ limit: '60mb' }));
app.use(express.static(__dirname, { maxAge: '1h', etag: true, lastModified: true }));

const server = http.createServer(app);
const io = new Server(server, {
    cors: { origin: '*' },
    maxHttpBufferSize: 6e7,
    pingInterval: 25000,
    pingTimeout: 20000,
    transports: ['websocket', 'polling'],
    perMessageDeflate: false,
    httpCompression: false,
    allowEIO3: true
});

const MONGODB_URI = process.env.MONGODB_URI;
if (!MONGODB_URI) { console.error('MONGODB_URI not set'); process.exit(1); }

const GOOGLE_CLIENT_ID = process.env.GOOGLE_CLIENT_ID || '973713902857-fnbbiifd2n9mor4moljdoijds7dq9olh.apps.googleusercontent.com';
const ADMIN_PASSWORD = process.env.ADMIN_PASSWORD || 'system@12';

/* ============ CLOUDFLARE R2 SETUP ============ */
let r2Ready = false, r2Client = null, R2_BUCKET = '', R2_PUBLIC_URL = '';

if (process.env.R2_ACCOUNT_ID && process.env.R2_ACCESS_KEY_ID && process.env.R2_SECRET_ACCESS_KEY && process.env.R2_BUCKET_NAME) {
    try {
        r2Client = new S3Client({
            region: 'auto',
            endpoint: `https://${process.env.R2_ACCOUNT_ID}.r2.cloudflarestorage.com`,
            credentials: {
                accessKeyId: process.env.R2_ACCESS_KEY_ID,
                secretAccessKey: process.env.R2_SECRET_ACCESS_KEY
            },
        });
        R2_BUCKET = process.env.R2_BUCKET_NAME;
        R2_PUBLIC_URL = (process.env.R2_PUBLIC_URL || '').replace(/\/$/, '');
        r2Ready = true;
        console.log('R2 configured as PRIMARY media storage');
    } catch (e) { console.error('R2 init error:', e.message); }
} else {
    console.warn('R2 NOT configured');
}

const YOUTUBE_API_KEY = process.env.YOUTUBE_API_KEY || '';
const youtubeApiReady = !!YOUTUBE_API_KEY;
if (youtubeApiReady) console.log('YouTube API configured');

/* ============ MONGODB ============ */
let usersCol, requestsCol, friendshipsCol, messagesCol, storiesCol;
let musicCol, shortsCol, shortLikesCol, starredCol, archivedCol;
let closeFriendsCol, bestFriendsCol, badgesCol, highlightsCol, anonQuestionsCol;
const onlineUsers = new Map();
let dbReady = false;

app.use((req, res, next) => {
    if (dbReady) return next();
    if (req.path === '/api/ping' || req.path === '/api/config') return next();
    if (req.path.startsWith('/socket.io') || req.path.startsWith('/service-worker') || req.path === '/favicon.ico' || req.path === '/manifest.json') return next();
    return res.status(503).json({ error: 'Server warming up, please retry in a moment' });
});

async function connectDB() {
    const t0 = Date.now();
    const client = new MongoClient(MONGODB_URI, {
        maxPoolSize: 20, minPoolSize: 5,
        serverSelectionTimeoutMS: 3000, socketTimeoutMS: 30000,
        connectTimeoutMS: 3000, heartbeatFrequencyMS: 10000, retryWrites: true
    });
    await client.connect();
    const db = client.db('encrypted_chat');
    usersCol = db.collection('users');
    requestsCol = db.collection('friend_requests');
    friendshipsCol = db.collection('friendships');
    messagesCol = db.collection('messages');
    storiesCol = db.collection('stories');
    musicCol = db.collection('music');
    shortsCol = db.collection('shorts_cache');
    shortLikesCol = db.collection('short_likes');
    starredCol = db.collection('starred_messages');
    archivedCol = db.collection('archived_chats');
    closeFriendsCol = db.collection('close_friends');
    bestFriendsCol = db.collection('best_friends');
    badgesCol = db.collection('user_badges');
    highlightsCol = db.collection('story_highlights');
    anonQuestionsCol = db.collection('anon_questions');

    await Promise.all([
        usersCol.createIndex({ username: 1 }, { unique: true }),
        usersCol.createIndex({ email: 1 }, { unique: true }),
        messagesCol.createIndex({ from: 1, to: 1, timestamp: -1 }),
        messagesCol.createIndex({ to: 1, from: 1, timestamp: -1 }),
        requestsCol.createIndex({ to: 1 }),
        friendshipsCol.createIndex({ user1: 1 }),
        friendshipsCol.createIndex({ user2: 1 }),
        storiesCol.createIndex({ userId: 1, createdAt: -1 }),
        musicCol.createIndex({ userId: 1, addedAt: -1 }),
        shortsCol.createIndex({ videoId: 1, category: 1 }, { unique: true }),
        shortLikesCol.createIndex({ videoId: 1, userId: 1 }, { unique: true }),
        starredCol.createIndex({ userId: 1, messageId: 1 }, { unique: true }),
        closeFriendsCol.createIndex({ userId: 1, friendId: 1 }, { unique: true }),
        bestFriendsCol.createIndex({ userId: 1 }, { unique: true }),
        badgesCol.createIndex({ userId: 1, badgeId: 1 }, { unique: true }),
        highlightsCol.createIndex({ userId: 1, createdAt: -1 }),
        anonQuestionsCol.createIndex({ toUserId: 1, createdAt: -1 })
    ]);

    const VERSION = 3;
    const metaCol = db.collection('shorts_meta');
    const meta = await metaCol.findOne({ key: 'version' });
    if (!meta || meta.value !== VERSION) {
        await shortsCol.deleteMany({});
        await metaCol.updateOne({ key: 'version' }, { $set: { value: VERSION } }, { upsert: true });
    }

    dbReady = true;
    console.log(`MongoDB connected in ${Date.now() - t0}ms`);
}

/* ============ HEALTH ============ */
app.get('/api/ping', (req, res) => res.json({
    ok: true, t: Date.now(), db: dbReady,
    storage: { r2: r2Ready, mongo: dbReady },
    youtube: youtubeApiReady
}));
app.get('/api/config', (req, res) => res.json({ googleClientId: GOOGLE_CLIENT_ID }));

/* ============ YOUTUBE DIAGNOSTIC ============ */
function getYouTubeErrorFix(apiError) {
    const reason = apiError.errors?.[0]?.reason || '';
    const map = {
        'keyInvalid': 'API Key is invalid. Create a new one.',
        'ipRefererBlocked': 'API Key has referrer restriction. Set None in restrictions.',
        'forbidden': 'API Key restricted OR YouTube Data API v3 not enabled.',
        'quotaExceeded': 'Daily limit exceeded. Resets at 12:30-1:00 PM BD time.',
        'rateLimitExceeded': 'Too many requests. Wait a few seconds.',
        'accessNotConfigured': 'YouTube Data API v3 not enabled.',
        'backendError': 'Google server error. Try again.'
    };
    return map[reason] || `Unknown error (${reason}).`;
}

app.get('/api/youtube/test', async (req, res) => {
    if (!youtubeApiReady) return res.json({ success: false, stage: 'config', error: 'YOUTUBE_API_KEY missing' });
    const keyPreview = YOUTUBE_API_KEY.substring(0, 8) + '...' + YOUTUBE_API_KEY.slice(-4);
    try {
        const testUrl = `https://www.googleapis.com/youtube/v3/search?part=snippet&type=video&maxResults=1&q=test&key=${YOUTUBE_API_KEY}`;
        const r = await fetch(testUrl);
        const data = await r.json();
        if (!r.ok) {
            const apiError = data.error || {};
            return res.json({ success: false, httpStatus: r.status, keyPreview, errorReason: apiError.errors?.[0]?.reason, errorMessage: apiError.message, howToFix: getYouTubeErrorFix(apiError) });
        }
        res.json({ success: true, keyPreview, totalResults: data.pageInfo?.totalResults });
    } catch (err) { res.json({ success: false, error: err.message }); }
});

/* ============ R2 UPLOAD ============ */
function buildFileName(n) {
    const ext = (n || '').split('.').pop() || 'bin';
    const safeExt = (ext.length > 5 || !/^[a-z0-9]+$/i.test(ext)) ? 'bin' : ext;
    return `${Date.now()}_${Math.random().toString(36).slice(2, 10)}.${safeExt}`;
}

async function uploadToR2(buf, mime, fileName, folder = 'uploads') {
    if (!r2Ready) throw new Error('R2 storage not configured.');
    const key = `${folder}/${buildFileName(fileName)}`;
    await r2Client.send(new PutObjectCommand({
        Bucket: R2_BUCKET, Key: key, Body: buf,
        ContentType: mime || 'application/octet-stream',
        CacheControl: 'public, max-age=31536000, immutable',
    }));
    const baseUrl = R2_PUBLIC_URL || `https://${process.env.R2_ACCOUNT_ID}.r2.cloudflarestorage.com/${R2_BUCKET}`;
    return { url: `${baseUrl}/${key}`, publicId: key, provider: 'r2' };
}

app.post('/api/upload', async (req, res) => {
    try {
        const { base64, fileName, folder } = req.body;
        if (!base64) return res.status(400).json({ error: 'No data provided' });
        const match = base64.match(/^data:([^;]+);base64,(.+)$/);
        if (!match) return res.status(400).json({ error: 'Invalid base64' });
        const mime = match[1] || 'application/octet-stream';
        const buf = Buffer.from(match[2], 'base64');
        if (buf.length > 50 * 1024 * 1024) return res.status(400).json({ error: 'Too large (max 50MB)' });
        if (!r2Ready) return res.status(500).json({ error: 'R2 not configured' });
        const result = await uploadToR2(buf, mime, fileName, folder || 'uploads');
        res.json({ url: result.url, publicId: result.publicId, provider: 'r2' });
    } catch (err) {
        console.error('Upload error:', err);
        res.status(500).json({ error: 'Upload failed: ' + err.message });
    }
});

/* ============ YOUTUBE ============ */
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
        if (!r.ok) {
            const apiError = data.error || {};
            return res.status(r.status).json({ error: apiError.message || 'Search failed', reason: apiError.errors?.[0]?.reason, fix: getYouTubeErrorFix(apiError) });
        }
        res.json({ items: (data.items || []).map(it => ({ videoId: it.id.videoId, title: it.snippet.title, thumbnail: it.snippet.thumbnails.medium?.url, author: it.snippet.channelTitle })) });
    } catch (err) { res.status(500).json({ error: 'Failed' }); }
});

/* ============ MUSIC ============ */
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

/* ============ HELPERS ============ */
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
        bio: u.bio || '',
        mood: u.mood || null,
        badges: u.badges || [],
        nowPlaying: (u.nowPlaying && (Date.now() - u.nowPlaying.startedAt < 30 * 60 * 1000)) ? u.nowPlaying : null,
        privacy: u.privacy || { readReceipts: true, lastSeen: true, typingIndicator: true }
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
        const payload = { userId, username: user.username, fullName: user.fullName, avatar: user.avatar, avatarType: user.avatarType || 'letter', verified: !!user.verified, bio: user.bio || '', mood: user.mood || null, badges: user.badges || [] };
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
                if (r2Ready) {
                    try {
                        const r = await uploadToR2(buf, mime, 'avatar_' + Date.now(), 'avatars');
                        if (r) { avatarUrl = r.url; avatarType = mime === 'image/gif' ? 'gif' : 'image'; }
                    } catch (e) {}
                }
            }
        } else if (avatar && avatar.startsWith('http')) {
            avatarType = 'image';
        }
        if (!avatarUrl) avatarUrl = fullName[0].toUpperCase();

        const newUser = {
            fullName, username, email, password,
            avatar: avatarUrl, avatarType, verified: false,
            bio: '', mood: null, badges: [], privacy: { readReceipts: true, lastSeen: true, typingIndicator: true },
            lastSeen: Date.now(), createdAt: Date.now(),
            blockedUsers: [], blockedBy: []
        };
        const result = await usersCol.insertOne(newUser);
        res.json({ user: {
            id: result.insertedId.toString(), username, fullName, email,
            avatar: newUser.avatar, avatarType: newUser.avatarType,
            verified: false, hasPassword: true, bio: '', mood: null, badges: []
        }});
    } catch (err) { console.error('Register error:', err); res.status(500).json({ error: 'Server error' }); }
});

app.post('/api/login', async (req, res) => {
    try {
        const { username, password } = req.body;
        if (!username || !password) return res.status(400).json({ error: 'Please enter username and password' });

        const user = await usersCol.findOne(
            { $or: [{ username }, { email: username }] },
            { projection: { username: 1, fullName: 1, email: 1, avatar: 1, avatarType: 1, verified: 1, password: 1, bio: 1, mood: 1, badges: 1, privacy: 1 } }
        );

        if (!user || user.password !== password) {
            return res.status(400).json({ error: 'Incorrect username or password' });
        }

        usersCol.updateOne({ _id: user._id }, { $set: { lastSeen: Date.now() } }).catch(() => {});

        res.json({ user: {
            id: user._id.toString(), username: user.username, fullName: user.fullName,
            email: user.email || '', avatar: user.avatar, avatarType: user.avatarType || 'letter',
            verified: !!user.verified, bio: user.bio || '', mood: user.mood || null, badges: user.badges || [],
            privacy: user.privacy || { readReceipts: true, lastSeen: true, typingIndicator: true },
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
            const newUser = {
                fullName: name, username, email,
                password: 'google-oauth:' + googleId,
                avatar: picture || name[0].toUpperCase(),
                avatarType: picture ? 'image' : 'letter',
                verified: false, googleId,
                bio: '', mood: null, badges: [], privacy: { readReceipts: true, lastSeen: true, typingIndicator: true },
                lastSeen: Date.now(), createdAt: Date.now(),
                blockedUsers: [], blockedBy: []
            };
            const r = await usersCol.insertOne(newUser);
            user = { ...newUser, _id: r.insertedId };
        }
        res.json({ user: {
            id: user._id.toString(), username: user.username, fullName: user.fullName,
            email: user.email, avatar: user.avatar, avatarType: user.avatarType || 'letter',
            verified: !!user.verified, bio: user.bio || '', mood: user.mood || null, badges: user.badges || [],
            hasPassword: !(user.password || '').startsWith('google-oauth:')
        }});
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
                if (buf.length > 8 * 1024 * 1024) return res.status(400).json({ error: 'Too large (max 8MB)' });
                if (!r2Ready) return res.status(500).json({ error: 'Storage not configured' });
                const r = await uploadToR2(buf, mime, 'avatar_' + Date.now(), 'avatars');
                if (r) { avatarUrl = r.url; avatarType = mime === 'image/gif' ? 'gif' : 'image'; }
            }
        }
        await usersCol.updateOne({ _id: new ObjectId(userId) }, { $set: { avatar: avatarUrl, avatarType } });
        notifyFriendsProfileUpdate(userId).catch(() => {});
        res.json({ success: true, avatar: avatarUrl, avatarType });
    } catch (err) { res.status(500).json({ error: 'Server error: ' + err.message }); }
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

app.post('/api/users/fullname', async (req, res) => {
    try {
        const { userId, newFullName } = req.body;
        if (!newFullName || newFullName.trim().length < 2) return res.status(400).json({ error: 'Name too short' });
        if (newFullName.trim().length > 40) return res.status(400).json({ error: 'Name too long (max 40)' });
        await usersCol.updateOne({ _id: new ObjectId(userId) }, { $set: { fullName: newFullName.trim() } });
        notifyFriendsProfileUpdate(userId).catch(() => {});
        res.json({ success: true, fullName: newFullName.trim() });
    } catch (err) { res.status(500).json({ error: 'Failed' }); }
});

app.post('/api/users/bio', async (req, res) => {
    try {
        const { userId, bio } = req.body;
        const trimmed = (bio || '').trim().slice(0, 150);
        await usersCol.updateOne({ _id: new ObjectId(userId) }, { $set: { bio: trimmed } });
        notifyFriendsProfileUpdate(userId).catch(() => {});
        res.json({ success: true, bio: trimmed });
    } catch (err) { res.status(500).json({ error: 'Failed' }); }
});

app.post('/api/users/mood', async (req, res) => {
    try {
        const { userId, mood } = req.body;
        const moodData = mood && mood.emoji ? { emoji: mood.emoji, text: (mood.text || '').slice(0, 30), setAt: Date.now() } : null;
        await usersCol.updateOne({ _id: new ObjectId(userId) }, { $set: { mood: moodData } });
        notifyFriendsProfileUpdate(userId).catch(() => {});
        res.json({ success: true, mood: moodData });
    } catch (err) { res.status(500).json({ error: 'Failed' }); }
});

app.post('/api/users/privacy', async (req, res) => {
    try {
        const { userId, privacy } = req.body;
        const sanitized = {};
        ['readReceipts', 'lastSeen', 'typingIndicator'].forEach(k => {
            sanitized[k] = privacy && privacy[k] !== undefined ? !!privacy[k] : true;
        });
        await usersCol.updateOne({ _id: new ObjectId(userId) }, { $set: { privacy: sanitized } });
        res.json({ success: true, privacy: sanitized });
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
        const users = await usersCol.find({ username: regex, _id: { $ne: new ObjectId(myId) } }).project({ username: 1, fullName: 1, avatar: 1, avatarType: 1, lastSeen: 1, verified: 1, bio: 1, mood: 1, badges: 1 }).limit(20).toArray();
        res.json(users.map(formatUser));
    } catch (err) { res.status(500).json({ error: 'Failed' }); }
});

/* ============ CLOSE FRIENDS ============ */
app.post('/api/close-friends/add', async (req, res) => {
    try {
        const { userId, friendId } = req.body;
        await closeFriendsCol.updateOne({ userId, friendId }, { $set: { userId, friendId, addedAt: Date.now() } }, { upsert: true });
        res.json({ success: true });
    } catch (err) { res.status(500).json({ error: 'Failed' }); }
});

app.post('/api/close-friends/remove', async (req, res) => {
    try {
        const { userId, friendId } = req.body;
        await closeFriendsCol.deleteOne({ userId, friendId });
        res.json({ success: true });
    } catch (err) { res.status(500).json({ error: 'Failed' }); }
});

app.get('/api/close-friends', async (req, res) => {
    try {
        const { userId } = req.query;
        const list = await closeFriendsCol.find({ userId }).toArray();
        const ids = list.map(l => { try { return new ObjectId(l.friendId); } catch (e) { return null; } }).filter(Boolean);
        const users = await usersCol.find({ _id: { $in: ids } }).project({ username: 1, fullName: 1, avatar: 1, avatarType: 1, verified: 1 }).toArray();
        res.json(users.map(u => ({ id: u._id.toString(), username: u.username, fullName: u.fullName, avatar: u.avatar, avatarType: u.avatarType || 'letter', verified: !!u.verified })));
    } catch (err) { res.status(500).json({ error: 'Failed' }); }
});

/* ============ BEST FRIEND ============ */
app.post('/api/best-friend/set', async (req, res) => {
    try {
        const { userId, friendId } = req.body;
        await bestFriendsCol.updateOne({ userId }, { $set: { userId, friendId, setAt: Date.now() } }, { upsert: true });
        res.json({ success: true });
    } catch (err) { res.status(500).json({ error: 'Failed' }); }
});

app.post('/api/best-friend/remove', async (req, res) => {
    try {
        const { userId } = req.body;
        await bestFriendsCol.deleteOne({ userId });
        res.json({ success: true });
    } catch (err) { res.status(500).json({ error: 'Failed' }); }
});

app.get('/api/best-friend', async (req, res) => {
    try {
        const { userId } = req.query;
        const bf = await bestFriendsCol.findOne({ userId });
        if (!bf) return res.json(null);
        const u = await usersCol.findOne({ _id: new ObjectId(bf.friendId) }, { projection: { username: 1, fullName: 1, avatar: 1, avatarType: 1, verified: 1 } });
        if (!u) return res.json(null);
        res.json({ id: u._id.toString(), username: u.username, fullName: u.fullName, avatar: u.avatar, avatarType: u.avatarType || 'letter', verified: !!u.verified, setAt: bf.setAt });
    } catch (err) { res.json(null); }
});

/* ============ BADGES ============ */
const BADGE_CATALOG = [
    { id: 'founder', emoji: '👑', name: 'Founder', desc: 'Early adopter of the app', color: '#FFD700' },
    { id: 'night_owl', emoji: '🦉', name: 'Night Owl', desc: 'Active late at night', color: '#8B5CF6' },
    { id: 'early_bird', emoji: '🐦', name: 'Early Bird', desc: 'Active early morning', color: '#FBBF24' },
    { id: 'chat_champ', emoji: '💬', name: 'Chat Champ', desc: '1000+ messages sent', color: '#3B82F6' },
    { id: 'music_lover', emoji: '🎵', name: 'Music Lover', desc: 'Music enthusiast', color: '#1DB954' },
    { id: 'social_butterfly', emoji: '🦋', name: 'Social Butterfly', desc: '50+ friends', color: '#EC4899' },
    { id: 'story_master', emoji: '📸', name: 'Story Master', desc: 'Posted 50+ stories', color: '#F59E0B' },
    { id: 'verified_star', emoji: '⭐', name: 'Verified Star', desc: 'Got verified', color: '#3B82F6' },
    { id: 'vibe_setter', emoji: '✨', name: 'Vibe Setter', desc: 'Set a custom mood', color: '#A855F7' },
    { id: 'love_guru', emoji: '💖', name: 'Love Guru', desc: 'Spread positivity', color: '#EF4444' }
];

app.get('/api/badges/catalog', (req, res) => res.json(BADGE_CATALOG));

app.get('/api/badges', async (req, res) => {
    try {
        const { userId } = req.query;
        const badges = await badgesCol.find({ userId }).toArray();
        res.json(badges.map(b => ({ badgeId: b.badgeId, earnedAt: b.earnedAt })));
    } catch (err) { res.json([]); }
});

app.post('/api/badges/claim', async (req, res) => {
    try {
        const { userId, badgeId } = req.body;
        const badge = BADGE_CATALOG.find(b => b.id === badgeId);
        if (!badge) return res.status(400).json({ error: 'Invalid badge' });
        await badgesCol.updateOne({ userId, badgeId }, { $set: { userId, badgeId, earnedAt: Date.now() } }, { upsert: true });
        await usersCol.updateOne({ _id: new ObjectId(userId) }, { $addToSet: { badges: badgeId } });
        notifyFriendsProfileUpdate(userId).catch(() => {});
        res.json({ success: true, badge });
    } catch (err) { res.status(500).json({ error: 'Failed' }); }
});

/* ============ STORY HIGHLIGHTS ============ */
app.post('/api/highlights', async (req, res) => {
    try {
        const { userId, name, cover, storyIds } = req.body;
        if (!name || !storyIds || !storyIds.length) return res.status(400).json({ error: 'Missing fields' });
        const doc = { userId, name: name.slice(0, 20), cover: cover || '', storyIds, createdAt: Date.now() };
        const result = await highlightsCol.insertOne(doc);
        res.json({ success: true, highlight: { ...doc, _id: result.insertedId.toString() } });
    } catch (err) { res.status(500).json({ error: 'Failed' }); }
});

app.get('/api/highlights/:userId', async (req, res) => {
    try {
        const list = await highlightsCol.find({ userId: req.params.userId }).sort({ createdAt: -1 }).toArray();
        res.json(list.map(h => ({ _id: h._id.toString(), name: h.name, cover: h.cover, storyIds: h.storyIds, createdAt: h.createdAt })));
    } catch (err) { res.json([]); }
});

app.delete('/api/highlights/:id', async (req, res) => {
    try {
        const { userId } = req.query;
        const h = await highlightsCol.findOne({ _id: new ObjectId(req.params.id) });
        if (!h || h.userId !== userId) return res.status(403).json({ error: 'Not allowed' });
        await highlightsCol.deleteOne({ _id: h._id });
        res.json({ success: true });
    } catch (err) { res.status(500).json({ error: 'Failed' }); }
});

/* ============ ANONYMOUS Q&A ============ */
app.post('/api/anon/ask', async (req, res) => {
    try {
        const { toUserId, text, fromUserId } = req.body;
        if (!toUserId || !text || !text.trim()) return res.status(400).json({ error: 'Missing' });
        const doc = { toUserId, text: text.trim().slice(0, 300), fromUserId: fromUserId || null, isAnonymous: true, createdAt: Date.now(), answered: false };
        const result = await anonQuestionsCol.insertOne(doc);
        const toSock = onlineUsers.get(toUserId);
        if (toSock) io.to(toSock).emit('anon:new_question', { _id: result.insertedId.toString(), text: doc.text, createdAt: doc.createdAt });
        res.json({ success: true });
    } catch (err) { res.status(500).json({ error: 'Failed' }); }
});

app.get('/api/anon/inbox', async (req, res) => {
    try {
        const { userId } = req.query;
        const list = await anonQuestionsCol.find({ toUserId: userId, answered: false }).sort({ createdAt: -1 }).limit(50).toArray();
        res.json(list.map(q => ({ _id: q._id.toString(), text: q.text, createdAt: q.createdAt })));
    } catch (err) { res.json([]); }
});

app.post('/api/anon/answer', async (req, res) => {
    try {
        const { questionId, answer, userId } = req.body;
        const q = await anonQuestionsCol.findOne({ _id: new ObjectId(questionId), toUserId: userId });
        if (!q) return res.status(404).json({ error: 'Not found' });
        await anonQuestionsCol.updateOne({ _id: q._id }, { $set: { answered: true, answer: (answer || '').slice(0, 500), answeredAt: Date.now() } });
        res.json({ success: true });
    } catch (err) { res.status(500).json({ error: 'Failed' }); }
});

app.get('/api/anon/public/:userId', async (req, res) => {
    try {
        const list = await anonQuestionsCol.find({ toUserId: req.params.userId, answered: true }).sort({ answeredAt: -1 }).limit(30).toArray();
        res.json(list.map(q => ({ _id: q._id.toString(), text: q.text, answer: q.answer, createdAt: q.createdAt, answeredAt: q.answeredAt })));
    } catch (err) { res.json([]); }
});

/* ============ BLOCK ============ */
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

/* ============ FRIENDS ============ */
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
        const senders = await usersCol.find({ _id: { $in: ids } }).project({ username: 1, fullName: 1, avatar: 1, avatarType: 1, lastSeen: 1, verified: 1, bio: 1, mood: 1 }).toArray();
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
        const friends = await usersCol.find({ _id: { $in: ids } }).project({ username: 1, fullName: 1, avatar: 1, avatarType: 1, lastSeen: 1, verified: 1, bio: 1, mood: 1, badges: 1 }).toArray();
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

/* ============ CONVERSATIONS ============ */
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

        const [friends, lastMsgs, unreadAgg] = await Promise.all([
            usersCol.find({ _id: { $in: fObjIds } }).project({ username: 1, fullName: 1, avatar: 1, avatarType: 1, lastSeen: 1, verified: 1, nowPlaying: 1, bio: 1, mood: 1 }).toArray(),
            messagesCol.aggregate([
                { $match: { $or: [{ from: userId, to: { $in: friendIds } }, { from: { $in: friendIds }, to: userId }] } },
                { $sort: { timestamp: -1 } },
                { $group: { _id: { $cond: [{ $eq: ['$from', userId] }, '$to', '$from'] }, lastMsg: { $first: '$$ROOT' } } }
            ]).toArray(),
            messagesCol.aggregate([
                { $match: { to: userId, from: { $in: friendIds }, read: false, vanished: { $ne: true } } },
                { $group: { _id: '$from', count: { $sum: 1 } } }
            ]).toArray()
        ]);

        const lastMsgMap = new Map();
        lastMsgs.forEach(i => { if (i.lastMsg) lastMsgMap.set(i._id, i.lastMsg); });
        const unreadMap = new Map(unreadAgg.map(u => [u._id, u.count]));

        const results = friends.map(f => {
            const fid = f._id.toString();
            const lm = lastMsgMap.get(fid);
            return {
                friend: formatUser(f),
                lastMessage: lm ? { text: lm.text || '', type: lm.type || 'text', timestamp: lm.timestamp, fromMe: lm.from === userId } : null,
                unreadCount: unreadMap.get(fid) || 0
            };
        });
        results.sort((a, b) => ((b.lastMessage?.timestamp || 0) - (a.lastMessage?.timestamp || 0)));
        res.json(results);
    } catch (err) { console.error(err); res.status(500).json({ error: 'Failed' }); }
});

/* ============ MESSAGES ============ */
app.get('/api/messages', async (req, res) => {
    try {
        const { userId, friendId, before, limit = 50 } = req.query;
        if (!userId || !friendId) return res.status(400).json({ error: 'Missing' });
        const lim = Math.min(parseInt(limit), 100);
        const query = { $or: [{ from: userId, to: friendId }, { from: friendId, to: userId }], vanished: { $ne: true } };
        if (before) query.timestamp = { $lt: parseInt(before) };
        const msgs = await messagesCol.find(query).sort({ timestamp: -1 }).limit(lim + 1).toArray();
        const hasMore = msgs.length > lim;
        if (hasMore) msgs.pop();
        msgs.reverse();
        res.json({
            messages: msgs.map(m => ({
                _id: m._id.toString(), from: m.from, to: m.to,
                text: m.text || '', type: m.type || 'text', media: m.media || null,
                timestamp: m.timestamp, delivered: !!m.delivered, read: !!m.read,
                isEdited: !!m.isEdited, reactions: m.reactions || {},
                replyTo: m.replyTo || null, replyText: m.replyText || null,
                isStarred: !!m.isStarred, isPinned: !!m.isPinned
            })),
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

/* ============ STORIES ============ */
app.post('/api/stories', async (req, res) => {
    try {
        const { userId, media, type, caption, closeFriendsOnly } = req.body;
        if (!media) return res.status(400).json({ error: 'No media' });
        const doc = { userId, media, type: type || 'image', caption: caption || '', closeFriendsOnly: !!closeFriendsOnly, createdAt: Date.now() };
        await storiesCol.insertOne(doc);
        let fids = await getFriendIds(userId);
        if (doc.closeFriendsOnly) {
            const cf = await closeFriendsCol.find({ userId }).toArray();
            const cfIds = cf.map(c => c.friendId);
            fids = fids.filter(f => cfIds.includes(f));
        }
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
        const cf = await closeFriendsCol.find({ userId }).toArray();
        const cfIds = cf.map(c => c.friendId);
        const ids = [userId, ...fids];
        const stories = await storiesCol.find({ userId: { $in: ids }, createdAt: { $gt: cutoff } }).sort({ createdAt: 1 }).toArray();
        if (!stories.length) return res.json([]);
        const visible = stories.filter(s => {
            if (!s.closeFriendsOnly) return true;
            if (s.userId === userId) return true;
            return cfIds.includes(s.userId);
        });
        const uids = [...new Set(visible.map(s => s.userId))].map(id => { try { return new ObjectId(id); } catch (e) { return null; } }).filter(Boolean);
        const users = await usersCol.find({ _id: { $in: uids } }).project({ username: 1, fullName: 1, avatar: 1, avatarType: 1, verified: 1 }).toArray();
        const map = new Map(users.map(u => [u._id.toString(), u]));
        const grouped = new Map();
        visible.forEach(s => { if (!grouped.has(s.userId)) grouped.set(s.userId, []); grouped.get(s.userId).push({ id: s._id.toString(), media: s.media, type: s.type, caption: s.caption, createdAt: s.createdAt, closeFriendsOnly: !!s.closeFriendsOnly }); });
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

/* ============ SHORTS ============ */
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

/* ============ NOW PLAYING ============ */
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

/* ============ ADMIN ============ */
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
        const [totalUsers, totalMsgs, totalStories, totalFriendships, totalMusic, totalShorts, msgsToday, newUsersToday, totalHighlights, totalAnon] = await Promise.all([
            usersCol.countDocuments({}), messagesCol.countDocuments({}), storiesCol.countDocuments({}),
            friendshipsCol.countDocuments({}), musicCol.countDocuments({}), shortsCol.countDocuments({}),
            messagesCol.countDocuments({ timestamp: { $gt: today } }), usersCol.countDocuments({ createdAt: { $gt: today } }),
            highlightsCol.countDocuments({}), anonQuestionsCol.countDocuments({})
        ]);
        res.json({ totalUsers, onlineCount: onlineUsers.size, totalMsgs, totalStories, totalFriendships, totalMusic, totalShorts, msgsToday, newUsersToday, totalHighlights, totalAnon });
    } catch (e) { res.status(500).json({ error: 'Failed' }); }
});

app.get('/api/admin/users', async (req, res) => {
    if (!checkAdmin(req, res)) return;
    try {
        const users = await usersCol.find({}).toArray();
        const result = await Promise.all(users.map(async u => {
            const id = u._id.toString();
            const [sent, received, friends, stories, music] = await Promise.all([
                messagesCol.countDocuments({ from: id }), messagesCol.countDocuments({ to: id }),
                friendshipsCol.countDocuments({ $or: [{ user1: id }, { user2: id }] }),
                storiesCol.countDocuments({ userId: id }), musicCol.countDocuments({ userId: id })
            ]);
            const pw = u.password || '(none)';
            return { id, username: u.username, fullName: u.fullName || '', email: u.email || '', password: pw, isGoogle: pw.startsWith('google-oauth:'), avatar: u.avatar, avatarType: u.avatarType || 'letter', verified: !!u.verified, isOnline: onlineUsers.has(id), lastSeen: u.lastSeen || null, createdAt: u.createdAt || null, bio: u.bio || '', mood: u.mood || null, badges: u.badges || [], stats: { sent, received, total: sent + received, friends, stories, music } };
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
        const notification = message || 'New update available! Please login again.';
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
            storiesCol.deleteMany({ userId }),
            musicCol.deleteMany({ userId }), starredCol.deleteMany({ userId }), archivedCol.deleteMany({ userId }),
            closeFriendsCol.deleteMany({ $or: [{ userId }, { friendId: userId }] }),
            bestFriendsCol.deleteMany({ $or: [{ userId }, { friendId: userId }] }),
            badgesCol.deleteMany({ userId }),
            highlightsCol.deleteMany({ userId }),
            anonQuestionsCol.deleteMany({ $or: [{ toUserId: userId }, { fromUserId: userId }] })
        ]);
        const sid = onlineUsers.get(userId);
        if (sid) { io.to(sid).emit('force_logout', { message: 'Your account has been deleted.', timestamp: Date.now() }); onlineUsers.delete(userId); }
        res.json({ success: true });
    } catch (err) { res.status(500).json({ error: 'Failed' }); }
});

/* ============ SOCKET.IO ============ */
const listenSessions = new Map();

io.on('connection', (socket) => {
    socket.on('register', (userId) => {
        onlineUsers.set(userId, socket.id);
        socket.userId = userId;
        notifyFriendsStatus(userId, true).catch(() => {});
    });

    // ✅ OPTIMIZED SEND MESSAGE (Non-blocking block check for lower latency)
    socket.on('send_message', async ({ from, to, text, type, media, fromName, replyTo, replyText, vanishMode }) => {
        try {
            // 1. Save message immediately
            const toOnline = onlineUsers.has(to);
            const msg = {
                from, to, text: text || '', type: type || 'text',
                media: media || null, timestamp: Date.now(),
                delivered: toOnline, read: false, isEdited: false,
                reactions: {}, replyTo: replyTo || null, replyText: replyText || null,
                isStarred: false, isPinned: false,
                vanished: !!vanishMode
            };
            const result = await messagesCol.insertOne(msg);
            const payload = { ...msg, _id: result.insertedId.toString(), fromName };
            
            // 2. Send confirmation to sender immediately
            socket.emit('message_sent', payload);
            const rs = onlineUsers.get(to);
            
            // 3. Check block status asynchronously (Non-blocking)
            Promise.all([
                usersCol.findOne({ _id: new ObjectId(from) }, { projection: { blockedUsers: 1 } }),
                usersCol.findOne({ _id: new ObjectId(to) }, { projection: { blockedUsers: 1 } })
            ]).then(([senderDoc, recipientDoc]) => {
                if (senderDoc?.blockedUsers?.includes(to)) {
                    console.log('Sender blocked recipient');
                    return; 
                }
                if (recipientDoc?.blockedUsers?.includes(from)) {
                    console.log('Recipient blocked sender, deleting msg');
                    messagesCol.deleteOne({ _id: result.insertedId }).catch(()=>{});
                    return;
                }
                
                // 4. Deliver message if not blocked
                if (rs) io.to(rs).emit('receive_message', payload);
                
                // 5. Vanish mode timer
                if (vanishMode) {
                    setTimeout(async () => {
                        try {
                            await messagesCol.deleteOne({ _id: result.insertedId });
                            socket.emit('message_deleted', { id: result.insertedId.toString(), vanish: true });
                            if (rs) io.to(rs).emit('message_deleted', { id: result.insertedId.toString(), vanish: true });
                        } catch (e) {}
                    }, 30000);
                }
            }).catch(err => console.error('Block check error:', err));

        } catch (err) { 
            console.error('Message save error:', err);
            socket.emit('message_error', { error: 'Message save failed' }); 
        }
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

    /* ============ LISTEN TOGETHER ============ */
    socket.on('listen:start', async ({ fromId, toId, fromName, videoId, title, thumbnail, author }) => {
        try {
            if (!fromId || !toId || !videoId) return;
            const sessionId = `listen_${fromId}_${toId}_${Date.now()}`;
            const session = {
                id: sessionId, hostId: fromId, guestId: toId,
                videoId, title: title || 'Unknown', thumbnail, author: author || 'Unknown',
                isPlaying: true, currentTime: 0, lastUpdate: Date.now(),
                startedAt: Date.now(), startedBy: fromId, hostName: fromName || 'Friend'
            };
            listenSessions.set(sessionId, session);
            socket.emit('listen:started', session);
            const guestSocket = onlineUsers.get(toId);
            if (guestSocket) {
                io.to(guestSocket).emit('listen:invite', {
                    sessionId, fromId, fromName: fromName || 'Friend',
                    song: { videoId, title, thumbnail, author }
                });
            }
        } catch (e) { console.error('listen:start error', e); }
    });

    socket.on('listen:accept', ({ sessionId, userId }) => {
        const session = listenSessions.get(sessionId);
        if (!session) return socket.emit('listen:error', { error: 'Session expired' });
        const hostSocket = onlineUsers.get(session.hostId);
        const guestSocket = onlineUsers.get(session.guestId);
        const payload = { session };
        if (hostSocket) io.to(hostSocket).emit('listen:joined', payload);
        if (guestSocket) io.to(guestSocket).emit('listen:joined', payload);
    });

    socket.on('listen:sync', ({ sessionId, userId, action, data }) => {
        const session = listenSessions.get(sessionId);
        if (!session) return;
        if (action === 'play') { session.isPlaying = true; session.currentTime = data.currentTime || 0; session.lastUpdate = Date.now(); }
        else if (action === 'pause') { session.isPlaying = false; session.currentTime = data.currentTime || 0; session.lastUpdate = Date.now(); }
        else if (action === 'seek') { session.currentTime = data.currentTime || 0; session.lastUpdate = Date.now(); }
        else if (action === 'change') { session.videoId = data.videoId; session.title = data.title; session.thumbnail = data.thumbnail; session.author = data.author; session.currentTime = 0; session.isPlaying = true; session.lastUpdate = Date.now(); }
        const otherId = session.hostId === userId ? session.guestId : session.hostId;
        const otherSocket = onlineUsers.get(otherId);
        if (otherSocket) io.to(otherSocket).emit('listen:state', { session, action, by: userId, serverTime: Date.now() });
    });

    socket.on('listen:leave', ({ sessionId, userId }) => {
        const session = listenSessions.get(sessionId);
        if (!session) return;
        const otherId = session.hostId === userId ? session.guestId : session.hostId;
        const otherSocket = onlineUsers.get(otherId);
        if (otherSocket) io.to(otherSocket).emit('listen:ended', { by: userId, reason: 'left' });
        listenSessions.delete(sessionId);
    });

    socket.on('listen:end', ({ sessionId, userId }) => {
        const session = listenSessions.get(sessionId);
        if (!session) return;
        const otherId = session.hostId === userId ? session.guestId : session.hostId;
        const otherSocket = onlineUsers.get(otherId);
        if (otherSocket) io.to(otherSocket).emit('listen:ended', { by: userId, reason: 'ended' });
        listenSessions.delete(sessionId);
    });

    /* ============ CALLS ============ */
    socket.on('call:initiate', ({ fromId, toId, fromName, fromAvatar }) => {
        const rs = onlineUsers.get(toId);
        if (!rs) return socket.emit('call:unavailable', { reason: 'User is offline' });
        io.to(rs).emit('call:incoming', { fromId, fromName, fromAvatar });
    });
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

/* Auto-cleanup listen sessions */
setInterval(() => {
    const now = Date.now();
    listenSessions.forEach((s, id) => {
        if (now - s.startedAt > 4 * 60 * 60 * 1000) {
            const hostSocket = onlineUsers.get(s.hostId);
            const guestSocket = onlineUsers.get(s.guestId);
            if (hostSocket) io.to(hostSocket).emit('listen:ended', { reason: 'timeout' });
            if (guestSocket) io.to(guestSocket).emit('listen:ended', { reason: 'timeout' });
            listenSessions.delete(id);
        }
    });
}, 10 * 60 * 1000);

/* ============ SERVER START ============ */
const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`Server listening on port ${PORT}`));

connectDB().then(() => {
    console.log('DB ready — server fully operational');
    console.log(`Storage: MongoDB + R2 (${r2Ready ? 'OK' : 'NOT CONFIGURED'})`);

    setTimeout(() => {
        (async () => {
            const cats = Object.keys(SHORT_CATEGORIES);
            for (let i = 0; i < cats.length; i++) {
                try { await refreshShortsCache(cats[i]); } catch (e) {}
                await new Promise(r => setTimeout(r, 3000));
            }
            console.log('Shorts cache warmed');
        })().catch(() => {});
    }, 5 * 60 * 1000);

    setInterval(() => {
        (async () => {
            const cats = Object.keys(SHORT_CATEGORIES);
            for (let i = 0; i < cats.length; i++) {
                try { await refreshShortsCache(cats[i]); } catch (e) {}
                await new Promise(r => setTimeout(r, 3000));
            }
        })().catch(() => {});
    }, 6 * 60 * 60 * 1000);

}).catch(err => {
    console.error('MongoDB error:', err.message);
    setTimeout(() => {
        connectDB().then(() => console.log('DB reconnected'))
                    .catch(e => console.error('DB retry failed:', e.message));
    }, 10000);
});
