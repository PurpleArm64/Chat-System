const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');
const { MongoClient, ObjectId } = require('mongodb');
const cloudinary = require('cloudinary').v2;
const { S3Client, PutObjectCommand } = require('@aws-sdk/client-s3');

const app = express();
app.use(cors());
app.use(express.json({ limit: '20mb' }));
app.use(express.static(__dirname, { maxAge: '1h', etag: true }));

const server = http.createServer(app);
const io = new Server(server, {
    cors: { origin: '*' },
    maxHttpBufferSize: 2.5e7,
    pingInterval: 25000, pingTimeout: 20000,
    transports: ['websocket', 'polling']
});

const MONGODB_URI = process.env.MONGODB_URI;
if (!MONGODB_URI) { console.error('❌ MONGODB_URI not set'); process.exit(1); }

const GOOGLE_CLIENT_ID = process.env.GOOGLE_CLIENT_ID || '973713902857-fnbbiifd2n9mor4moljdoijds7dq9olh.apps.googleusercontent.com';
const ADMIN_PASSWORD = process.env.ADMIN_PASSWORD || 'system@12';

/* ===== Cloudinary ===== */
let cloudinaryReady = false;
if (process.env.CLOUDINARY_CLOUD_NAME && process.env.CLOUDINARY_API_KEY && process.env.CLOUDINARY_API_SECRET) {
    try {
        cloudinary.config({
            cloud_name: process.env.CLOUDINARY_CLOUD_NAME,
            api_key: process.env.CLOUDINARY_API_KEY,
            api_secret: process.env.CLOUDINARY_API_SECRET,
            secure: true
        });
        cloudinaryReady = true;
        console.log('✅ Cloudinary configured');
    } catch (e) { console.error('Cloudinary error:', e.message); }
}

/* ===== R2 ===== */
let r2Ready = false, r2Client = null, R2_BUCKET = '', R2_PUBLIC_URL = '';
const STORAGE_PROVIDER = (process.env.STORAGE_PROVIDER || 'auto').toLowerCase();
if (process.env.R2_ACCOUNT_ID && process.env.R2_ACCESS_KEY_ID && process.env.R2_SECRET_ACCESS_KEY && process.env.R2_BUCKET_NAME) {
    try {
        r2Client = new S3Client({
            region: 'auto',
            endpoint: `https://${process.env.R2_ACCOUNT_ID}.r2.cloudflarestorage.com`,
            credentials: { accessKeyId: process.env.R2_ACCESS_KEY_ID, secretAccessKey: process.env.R2_SECRET_ACCESS_KEY }
        });
        R2_BUCKET = process.env.R2_BUCKET_NAME;
        R2_PUBLIC_URL = (process.env.R2_PUBLIC_URL || '').replace(/\/$/, '');
        r2Ready = true;
        console.log('✅ R2 configured');
    } catch (e) { console.error('R2 error:', e.message); }
}

const YOUTUBE_API_KEY = process.env.YOUTUBE_API_KEY || '';
const youtubeApiReady = !!YOUTUBE_API_KEY;
if (youtubeApiReady) console.log('✅ YouTube API configured');

let usersCol, requestsCol, friendshipsCol, messagesCol, storiesCol, reelsCol, reelLikesCol, reelCommentsCol, musicCol, blocksCol;
const onlineUsers = new Map();

async function connectDB() {
    const client = new MongoClient(MONGODB_URI, {
        maxPoolSize: 10, minPoolSize: 2,
        serverSelectionTimeoutMS: 5000, socketTimeoutMS: 45000
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
    blocksCol = db.collection('blocks');

    await usersCol.createIndex({ username: 1 }, { unique: true });
    await usersCol.createIndex({ email: 1 }, { unique: true });
    await messagesCol.createIndex({ from: 1, to: 1, timestamp: 1 });
    await messagesCol.createIndex({ to: 1, from: 1, timestamp: 1 });
    await requestsCol.createIndex({ to: 1 });
    await requestsCol.createIndex({ from: 1, to: 1 }, { unique: true });
    await friendshipsCol.createIndex({ user1: 1 });
    await friendshipsCol.createIndex({ user2: 1 });
    await storiesCol.createIndex({ userId: 1, createdAt: 1 });
    await reelsCol.createIndex({ createdAt: -1 });
    await reelLikesCol.createIndex({ reelId: 1, userId: 1 }, { unique: true });
    await reelCommentsCol.createIndex({ reelId: 1, createdAt: -1 });
    await musicCol.createIndex({ userId: 1, addedAt: -1 });
    await blocksCol.createIndex({ userId: 1 });
    await blocksCol.createIndex({ blockedId: 1 });
    console.log('✅ MongoDB connected');
}

app.get('/api/ping', (req, res) => res.json({
    ok: true, t: Date.now(),
    storage: { r2: r2Ready, cloudinary: cloudinaryReady, provider: STORAGE_PROVIDER },
    youtube: youtubeApiReady
}));

/* ===== UPLOAD ===== */
function buildFileName(n) {
    const ext = (n || '').split('.').pop() || 'bin';
    return `${Date.now()}_${Math.random().toString(36).slice(2, 10)}.${ext.length > 5 ? 'bin' : ext}`;
}
async function uploadToR2(buf, mime, fileName) {
    if (!r2Ready) throw new Error('R2 not configured');
    const key = `uploads/${buildFileName(fileName)}`;
    await r2Client.send(new PutObjectCommand({
        Bucket: R2_BUCKET, Key: key, Body: buf,
        ContentType: mime || 'application/octet-stream',
        CacheControl: 'public, max-age=31536000, immutable'
    }));
    return { url: `${R2_PUBLIC_URL}/${key}`, publicId: key, provider: 'r2' };
}
async function uploadToCloudinary(buf, mime) {
    if (!cloudinaryReady) throw new Error('Cloudinary not configured');
    let rt = 'raw';
    if (mime.startsWith('image/')) rt = 'image';
    else if (mime.startsWith('video/') || mime.startsWith('audio/')) rt = 'video';
    const result = await new Promise((resolve, reject) => {
        const stream = cloudinary.uploader.upload_stream({ folder: 'encrypted_chat', resource_type: rt, unique_filename: true }, (e, r) => e ? reject(e) : resolve(r));
        stream.end(buf);
    });
    return { url: result.secure_url, publicId: result.public_id, provider: 'cloudinary' };
}
app.post('/api/upload', async (req, res) => {
    try {
        const { base64, fileName } = req.body;
        if (!base64) return res.status(400).json({ error: 'No file data' });
        const m = base64.match(/^data:([^;]+);base64,(.+)$/);
        if (!m) return res.status(400).json({ error: 'Invalid format' });
        const mime = m[1], buf = Buffer.from(m[2], 'base64');
        if (buf.length > 15 * 1024 * 1024) return res.status(400).json({ error: 'File too large (max 15MB)' });
        let result;
        if (STORAGE_PROVIDER === 'r2') result = await uploadToR2(buf, mime, fileName);
        else if (STORAGE_PROVIDER === 'cloudinary') result = await uploadToCloudinary(buf, mime);
        else {
            try { result = r2Ready ? await uploadToR2(buf, mime, fileName) : (() => { throw new Error(); })(); }
            catch (e) { result = await uploadToCloudinary(buf, mime); }
        }
        res.json({ url: result.url, publicId: result.publicId, provider: result.provider });
    } catch (err) { res.status(500).json({ error: 'Upload failed: ' + err.message }); }
});

/* ===== TIKTOK ===== */
app.post('/api/tiktok/resolve', async (req, res) => {
    try {
        const { url } = req.body;
        if (!url) return res.status(400).json({ error: 'No URL' });
        let finalUrl = url.trim();
        if (/vm\.tiktok\.com|vt\.tiktok\.com|m\.tiktok\.com\/v\//i.test(finalUrl)) {
            try { const r = await fetch(finalUrl, { redirect: 'follow', headers: { 'User-Agent': 'Mozilla/5.0' } }); finalUrl = r.url || finalUrl; } catch (e) {}
        }
        const patterns = [/tiktok\.com\/@[^/?#]+\/video\/(\d+)/i, /tiktok\.com\/v\/(\d+)/i, /tiktok\.com\/embed\/v2\/(\d+)/i, /tiktok\.com\/embed\/(\d+)/i];
        for (const p of patterns) { const m = finalUrl.match(p); if (m) return res.json({ videoId: m[1], url: finalUrl }); }
        res.status(400).json({ error: 'Invalid TikTok URL' });
    } catch (err) { res.status(500).json({ error: 'Failed to resolve' }); }
});

/* ===== YOUTUBE ===== */
app.post('/api/youtube/oembed', async (req, res) => {
    try {
        const { videoId } = req.body;
        if (!videoId) return res.status(400).json({ error: 'Missing videoId' });
        const r = await fetch(`https://www.youtube.com/oembed?url=https://www.youtube.com/watch?v=${videoId}&format=json`);
        if (!r.ok) return res.status(400).json({ error: 'Video not found' });
        const d = await r.json();
        res.json({ videoId, title: d.title, thumbnail: d.thumbnail_url, author: d.author_name });
    } catch (err) { res.status(500).json({ error: 'Failed' }); }
});
app.get('/api/youtube/search', async (req, res) => {
    try {
        if (!youtubeApiReady) return res.status(500).json({ error: 'Search not configured' });
        const { q, maxResults = 20 } = req.query;
        if (!q || q.trim().length < 2) return res.json({ items: [] });
        const url = `https://www.googleapis.com/youtube/v3/search?part=snippet&type=video&videoEmbeddable=true&videoSyndicated=true&maxResults=${Math.min(parseInt(maxResults), 50)}&q=${encodeURIComponent(q)}&key=${YOUTUBE_API_KEY}`;
        const r = await fetch(url);
        const data = await r.json();
        if (!r.ok) return res.status(r.status).json({ error: (data.error && data.error.message) || 'Search failed' });
        const items = (data.items || []).map(it => ({
            videoId: it.id.videoId, title: it.snippet.title,
            thumbnail: it.snippet.thumbnails.medium?.url || it.snippet.thumbnails.default?.url,
            author: it.snippet.channelTitle
        }));
        res.json({ items });
    } catch (err) { res.status(500).json({ error: 'Search failed' }); }
});

/* ===== MUSIC ===== */
app.post('/api/music', async (req, res) => {
    try {
        const { userId, videoId, title, thumbnail, author } = req.body;
        if (!userId || !videoId) return res.status(400).json({ error: 'Missing fields' });
        const doc = { userId, videoId, title: title || 'Unknown', thumbnail: thumbnail || `https://img.youtube.com/vi/${videoId}/mqdefault.jpg`, author: author || 'Unknown', addedAt: Date.now() };
        const result = await musicCol.insertOne(doc);
        res.json({ song: { ...doc, _id: result.insertedId.toString() } });
    } catch (err) { res.status(500).json({ error: 'Server error' }); }
});
app.get('/api/music', async (req, res) => {
    try {
        const { userId } = req.query;
        if (!userId) return res.status(400).json({ error: 'Missing userId' });
        const songs = await musicCol.find({ userId }).sort({ addedAt: -1 }).toArray();
        res.json(songs.map(s => ({ _id: s._id.toString(), videoId: s.videoId, title: s.title, thumbnail: s.thumbnail, author: s.author, addedAt: s.addedAt })));
    } catch (err) { res.status(500).json({ error: 'Server error' }); }
});
app.delete('/api/music/:id', async (req, res) => {
    try {
        const { id } = req.params;
        const { userId } = req.query;
        let oid; try { oid = new ObjectId(id); } catch(e) { return res.status(400).json({error:'bad id'}); }
        const song = await musicCol.findOne({ _id: oid });
        if (!song || song.userId !== userId) return res.status(403).json({ error: 'Not allowed' });
        await musicCol.deleteOne({ _id: oid });
        res.json({ success: true });
    } catch (err) { res.status(500).json({ error: 'Server error' }); }
});

/* ===== HELPERS ===== */
async function getFriendIds(userId) {
    const fs = await friendshipsCol.find({ $or: [{ user1: userId }, { user2: userId }] }).toArray();
    return fs.map(f => f.user1 === userId ? f.user2 : f.user1);
}
async function getBlockedIds(userId) {
    const b = await blocksCol.find({ userId }).toArray();
    return b.map(x => x.blockedId);
}
function formatUser(u) {
    const id = u._id.toString();
    return {
        id, username: u.username, fullName: u.fullName,
        avatar: u.avatar, avatarType: u.avatarType || 'letter',
        verified: !!u.verified,
        isOnline: onlineUsers.has(id),
        lastSeen: u.lastSeen || null
    };
}
async function notifyFriendsStatus(userId, isOnline) {
    try {
        const now = isOnline ? null : Date.now();
        if (!isOnline) await usersCol.updateOne({ _id: new ObjectId(userId) }, { $set: { lastSeen: now } });
        const friendIds = await getFriendIds(userId);
        friendIds.forEach(fid => { const sid = onlineUsers.get(fid); if (sid) io.to(sid).emit('user_status', { userId, isOnline, lastSeen: now }); });
    } catch (e) {}
}
async function notifyFriendsVerified(userId, verified) {
    try {
        const friendIds = await getFriendIds(userId);
        friendIds.forEach(fid => { const sid = onlineUsers.get(fid); if (sid) io.to(sid).emit('user_verified', { userId, verified }); });
    } catch (e) {}
}
async function notifyFriendsProfileUpdate(userId) {
    try {
        const user = await usersCol.findOne({ _id: new ObjectId(userId) });
        if (!user) return;
        const payload = { userId, username: user.username, fullName: user.fullName, avatar: user.avatar, avatarType: user.avatarType || 'letter', verified: !!user.verified };
        const friendIds = await getFriendIds(userId);
        friendIds.forEach(fid => { const sid = onlineUsers.get(fid); if (sid) io.to(sid).emit('user_profile_updated', payload); });
        const selfSid = onlineUsers.get(userId);
        if (selfSid) io.to(selfSid).emit('user_profile_updated', payload);
    } catch (e) {}
}

/* ===== AUTH ===== */
app.post('/api/register', async (req, res) => {
    try {
        const { fullName, username, email, password, avatar } = req.body;
        if (!fullName || !username || !email || !password) return res.status(400).json({ error: 'Please fill in all fields' });
        if (username.length < 3) return res.status(400).json({ error: 'Username must be at least 3 characters' });
        if (password.length < 6) return res.status(400).json({ error: 'Password must be at least 6 characters' });
        const dup = await usersCol.findOne({ $or: [{ username }, { email }] });
        if (dup) {
            if (dup.username === username) return res.status(400).json({ error: 'Username is already taken' });
            return res.status(400).json({ error: 'Email is already registered' });
        }
        const newUser = {
            fullName, username, email, password,
            avatar: avatar || fullName[0].toUpperCase(),
            avatarType: avatar ? 'image' : 'letter',
            verified: false, lastSeen: Date.now(), createdAt: Date.now()
        };
        const result = await usersCol.insertOne(newUser);
        res.json({ user: { id: result.insertedId.toString(), username, fullName, email, avatar: newUser.avatar, avatarType: newUser.avatarType, verified: false } });
    } catch (err) { res.status(500).json({ error: 'Server error' }); }
});
app.post('/api/login', async (req, res) => {
    try {
        const { username, password } = req.body;
        if (!username || !password) return res.status(400).json({ error: 'Please enter credentials' });
        const user = await usersCol.findOne({ $or: [{ username }, { email: username }], password });
        if (!user) return res.status(400).json({ error: 'Incorrect username or password' });
        res.json({ user: {
            id: user._id.toString(), username: user.username, fullName: user.fullName,
            email: user.email || '', avatar: user.avatar,
            avatarType: user.avatarType || 'letter', verified: !!user.verified,
            hasPassword: !(user.password || '').startsWith('google-oauth:')
        }});
    } catch (err) { res.status(500).json({ error: 'Server error' }); }
});
app.post('/api/auth/google', async (req, res) => {
    try {
        const { credential } = req.body;
        if (!credential) return res.status(400).json({ error: 'No credential' });
        const vr = await fetch('https://oauth2.googleapis.com/tokeninfo?id_token=' + encodeURIComponent(credential));
        if (!vr.ok) return res.status(400).json({ error: 'Invalid Google token' });
        const info = await vr.json();
        const email = info.email, name = info.name || email.split('@')[0], picture = info.picture || '', googleId = info.sub;
        if (!email) return res.status(400).json({ error: 'No email' });
        let user = await usersCol.findOne({ email });
        if (!user) {
            let base = (email.split('@')[0] || 'user').replace(/[^a-zA-Z0-9_.]/g, '').toLowerCase();
            if (base.length < 3) base = 'user' + Date.now().toString().slice(-4);
            let username = base, c = 1;
            while (await usersCol.findOne({ username })) { username = base + '_' + c; c++; if (c > 999) { username = base + '_' + Date.now().toString().slice(-6); break; } }
            const newUser = { fullName: name, username, email, password: 'google-oauth:' + googleId, avatar: picture || name[0].toUpperCase(), avatarType: picture ? 'image' : 'letter', verified: false, googleId, lastSeen: Date.now(), createdAt: Date.now() };
            const r = await usersCol.insertOne(newUser);
            user = { ...newUser, _id: r.insertedId };
        } else if (!user.googleId) {
            await usersCol.updateOne({ _id: user._id }, { $set: { googleId, lastSeen: Date.now() } });
        }
        res.json({ user: { id: user._id.toString(), username: user.username, fullName: user.fullName, email: user.email, avatar: user.avatar, avatarType: user.avatarType || 'letter', verified: !!user.verified, hasPassword: !(user.password || '').startsWith('google-oauth:') } });
    } catch (err) { res.status(500).json({ error: 'Google sign-in failed' }); }
});
app.post('/api/users/avatar', async (req, res) => {
    try {
        const { userId, avatar } = req.body;
        if (!avatar) return res.status(400).json({ error: 'No avatar' });
        await usersCol.updateOne({ _id: new ObjectId(userId) }, { $set: { avatar, avatarType: 'image' } });
        await notifyFriendsProfileUpdate(userId);
        res.json({ success: true });
    } catch (err) { res.status(500).json({ error: 'Server error' }); }
});
app.post('/api/users/verify', async (req, res) => {
    try {
        const { userId, verified } = req.body;
        const v = !!verified;
        await usersCol.updateOne({ _id: new ObjectId(userId) }, { $set: { verified: v } });
        await notifyFriendsVerified(userId, v);
        await notifyFriendsProfileUpdate(userId);
        res.json({ success: true, verified: v });
    } catch (err) { res.status(500).json({ error: 'Server error' }); }
});
app.post('/api/users/username', async (req, res) => {
    try {
        const { userId, newUsername } = req.body;
        if (!newUsername || newUsername.length < 3) return res.status(400).json({ error: 'Too short' });
        if (!/^[a-zA-Z0-9_.]+$/.test(newUsername)) return res.status(400).json({ error: 'Invalid chars' });
        const existing = await usersCol.findOne({ username: newUsername });
        if (existing && existing._id.toString() !== userId) return res.status(400).json({ error: 'Username already taken' });
        await usersCol.updateOne({ _id: new ObjectId(userId) }, { $set: { username: newUsername } });
        const user = await usersCol.findOne({ _id: new ObjectId(userId) });
        await notifyFriendsProfileUpdate(userId);
        res.json({ success: true, username: user.username, email: user.email || '', avatar: user.avatar, avatarType: user.avatarType || 'letter', verified: !!user.verified });
    } catch (err) { res.status(500).json({ error: 'Server error' }); }
});
app.post('/api/users/password', async (req, res) => {
    try {
        const { userId, oldPassword, newPassword } = req.body;
        if (!oldPassword || !newPassword) return res.status(400).json({ error: 'Fill all fields' });
        if (newPassword.length < 6) return res.status(400).json({ error: 'New password 6+ chars' });
        if (oldPassword === newPassword) return res.status(400).json({ error: 'Must be different' });
        const user = await usersCol.findOne({ _id: new ObjectId(userId) });
        if (!user) return res.status(404).json({ error: 'User not found' });
        if ((user.password || '').startsWith('google-oauth:')) return res.status(400).json({ error: 'Google account' });
        if (user.password !== oldPassword) return res.status(400).json({ error: 'Current password incorrect' });
        await usersCol.updateOne({ _id: new ObjectId(userId) }, { $set: { password: newPassword } });
        res.json({ success: true });
    } catch (err) { res.status(500).json({ error: 'Server error' }); }
});
app.get('/api/users/profile/:username', async (req, res) => {
    try {
        const user = await usersCol.findOne({ username: req.params.username });
        if (!user) return res.status(404).json({ error: 'User not found' });
        res.json(formatUser(user));
    } catch (err) { res.status(500).json({ error: 'Server error' }); }
});
app.get('/api/users/search', async (req, res) => {
    try {
        const { q, myId } = req.query;
        if (!q || q.length < 2) return res.json([]);
        const regex = new RegExp('^' + q.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'), 'i');
        const users = await usersCol.find({ username: regex, _id: { $ne: new ObjectId(myId) } })
            .project({ username: 1, fullName: 1, avatar: 1, avatarType: 1, lastSeen: 1, verified: 1 }).limit(20).toArray();
        res.json(users.map(formatUser));
    } catch (err) { res.status(500).json({ error: 'Server error' }); }
});

/* ===== FRIENDS ===== */
app.post('/api/friend-request', async (req, res) => {
    try {
        const { from, to } = req.body;
        if (from === to) return res.status(400).json({ error: 'Cannot add yourself' });
        // Block check
        const blocked = await blocksCol.findOne({ $or: [{ userId: from, blockedId: to }, { userId: to, blockedId: from }] });
        if (blocked) return res.status(400).json({ error: 'Cannot send request' });
        const existing = await friendshipsCol.findOne({ $or: [{ user1: from, user2: to }, { user1: to, user2: from }] });
        if (existing) return res.status(400).json({ error: 'Already friends' });
        const existingReq = await requestsCol.findOne({ from, to });
        if (existingReq) return res.status(400).json({ error: 'Request already sent' });
        const reverse = await requestsCol.findOne({ from: to, to: from });
        if (reverse) {
            await requestsCol.deleteOne({ _id: reverse._id });
            await friendshipsCol.insertOne({ user1: from, user2: to, createdAt: Date.now() });
            [from, to].forEach(uid => { const s = onlineUsers.get(uid); if (s) io.to(s).emit('friend_added'); });
            return res.json({ success: true, autoAccepted: true });
        }
        try {
            await requestsCol.insertOne({ from, to, createdAt: Date.now() });
        } catch (e) {
            if (e.code === 11000) return res.status(400).json({ error: 'Request already sent' });
            throw e;
        }
        const rs = onlineUsers.get(to);
        if (rs) io.to(rs).emit('new_friend_request', { from });
        res.json({ success: true });
    } catch (err) { res.status(500).json({ error: 'Server error' }); }
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
    } catch (err) { res.status(500).json({ error: 'Server error' }); }
});
app.post('/api/friend-request/accept', async (req, res) => {
    try {
        const { userId, friendId } = req.body;
        await requestsCol.deleteMany({ from: friendId, to: userId });
        await friendshipsCol.insertOne({ user1: userId, user2: friendId, createdAt: Date.now() });
        [userId, friendId].forEach(uid => { const s = onlineUsers.get(uid); if (s) io.to(s).emit('friend_added'); });
        res.json({ success: true });
    } catch (err) { res.status(500).json({ error: 'Server error' }); }
});
app.post('/api/friend-request/reject', async (req, res) => {
    try {
        const { userId, friendId } = req.body;
        await requestsCol.deleteMany({ from: friendId, to: userId });
        const ss = onlineUsers.get(friendId);
        if (ss) io.to(ss).emit('friend_request_rejected', { by: userId });
        res.json({ success: true });
    } catch (err) { res.status(500).json({ error: 'Server error' }); }
});
app.get('/api/friends', async (req, res) => {
    try {
        const { userId } = req.query;
        const friendships = await friendshipsCol.find({ $or: [{ user1: userId }, { user2: userId }] }).toArray();
        if (!friendships.length) return res.json([]);
        const ids = friendships.map(f => f.user1 === userId ? f.user2 : f.user1)
            .filter(id => { try { new ObjectId(id); return true; } catch (e) { return false; } })
            .map(id => new ObjectId(id));
        const friends = await usersCol.find({ _id: { $in: ids } }).project({ username: 1, fullName: 1, avatar: 1, avatarType: 1, lastSeen: 1, verified: 1 }).toArray();
        res.json(friends.map(formatUser));
    } catch (err) { res.status(500).json({ error: 'Server error' }); }
});
app.delete('/api/friends/:friendId', async (req, res) => {
    try {
        const { friendId } = req.params;
        const { userId, deleteMessages } = req.query;
        if (!userId || !friendId) return res.status(400).json({ error: 'Missing params' });
        if (userId === friendId) return res.status(400).json({ error: 'Invalid' });
        await friendshipsCol.deleteMany({ $or: [{ user1: userId, user2: friendId }, { user1: friendId, user2: userId }] });
        await requestsCol.deleteMany({ $or: [{ from: userId, to: friendId }, { from: friendId, to: userId }] });
        if (deleteMessages === 'true') {
            await messagesCol.deleteMany({ $or: [{ from: userId, to: friendId }, { from: friendId, to: userId }] });
        }
        const otherSid = onlineUsers.get(friendId);
        if (otherSid) io.to(otherSid).emit('friend_removed', { by: userId });
        const selfSid = onlineUsers.get(userId);
        if (selfSid) io.to(selfSid).emit('friend_removed', { by: userId });
        res.json({ success: true });
    } catch (err) { res.status(500).json({ error: 'Server error' }); }
});

/* ===== BLOCK SYSTEM ===== */
app.post('/api/blocks', async (req, res) => {
    try {
        const { userId, blockedId } = req.body;
        if (!userId || !blockedId) return res.status(400).json({ error: 'Missing fields' });
        if (userId === blockedId) return res.status(400).json({ error: 'Cannot block yourself' });
        const existing = await blocksCol.findOne({ userId, blockedId });
        if (existing) return res.status(400).json({ error: 'Already blocked' });
        await blocksCol.insertOne({ userId, blockedId, createdAt: Date.now() });
        // Remove friendship if exists
        await friendshipsCol.deleteMany({ $or: [{ user1: userId, user2: blockedId }, { user1: blockedId, user2: userId }] });
        // Remove friend requests both directions
        await requestsCol.deleteMany({ $or: [{ from: userId, to: blockedId }, { from: blockedId, to: userId }] });
        const otherSid = onlineUsers.get(blockedId);
        if (otherSid) io.to(otherSid).emit('friend_removed', { by: userId });
        const selfSid = onlineUsers.get(userId);
        if (selfSid) io.to(selfSid).emit('friend_removed', { by: userId });
        res.json({ success: true });
    } catch (err) { res.status(500).json({ error: 'Server error' }); }
});
app.get('/api/blocks', async (req, res) => {
    try {
        const { userId } = req.query;
        if (!userId) return res.status(400).json({ error: 'Missing userId' });
        const blocks = await blocksCol.find({ userId }).toArray();
        if (!blocks.length) return res.json([]);
        const ids = blocks.map(b => { try { return new ObjectId(b.blockedId); } catch (e) { return null; } }).filter(Boolean);
        const users = await usersCol.find({ _id: { $in: ids } }).project({ username: 1, fullName: 1, avatar: 1, avatarType: 1, lastSeen: 1, verified: 1 }).toArray();
        const umap = new Map(users.map(u => [u._id.toString(), u]));
        res.json(blocks.map(b => {
            const u = umap.get(b.blockedId);
            if (!u) return null;
            return { ...formatUser(u), blockedAt: b.createdAt };
        }).filter(Boolean));
    } catch (err) { res.status(500).json({ error: 'Server error' }); }
});
app.delete('/api/blocks/:blockedId', async (req, res) => {
    try {
        const { blockedId } = req.params;
        const { userId } = req.query;
        if (!userId || !blockedId) return res.status(400).json({ error: 'Missing params' });
        await blocksCol.deleteOne({ userId, blockedId });
        res.json({ success: true });
    } catch (err) { res.status(500).json({ error: 'Server error' }); }
});

/* ===== CONVERSATIONS ===== */
app.get('/api/conversations', async (req, res) => {
    try {
        const { userId } = req.query;
        const friendships = await friendshipsCol.find({ $or: [{ user1: userId }, { user2: userId }] }).toArray();
        if (!friendships.length) return res.json([]);
        const friendIds = friendships.map(f => f.user1 === userId ? f.user2 : f.user1);
        const results = [];
        for (const fid of friendIds) {
            let friendObjId; try { friendObjId = new ObjectId(fid); } catch (e) { continue; }
            const friend = await usersCol.findOne({ _id: friendObjId });
            if (!friend) continue;
            const lastMsgs = await messagesCol.find({ $or: [{ from: userId, to: fid }, { from: fid, to: userId }] }).sort({ timestamp: -1 }).limit(1).toArray();
            const unreadCount = await messagesCol.countDocuments({ from: fid, to: userId, read: false });
            results.push({
                friend: formatUser(friend),
                lastMessage: lastMsgs[0] ? { text: lastMsgs[0].text || '', type: lastMsgs[0].type || 'text', timestamp: lastMsgs[0].timestamp, fromMe: lastMsgs[0].from === userId } : null,
                unreadCount
            });
        }
        results.sort((a, b) => ((b.lastMessage?.timestamp) || 0) - ((a.lastMessage?.timestamp) || 0));
        res.json(results);
    } catch (err) { res.status(500).json({ error: 'Server error' }); }
});
app.get('/api/messages', async (req, res) => {
    try {
        const { userId, friendId } = req.query;
        const msgs = await messagesCol.find({ $or: [{ from: userId, to: friendId }, { from: friendId, to: userId }] }).sort({ timestamp: -1 }).limit(100).toArray();
        msgs.reverse();
        res.json(msgs.map(m => ({
            _id: m._id.toString(), from: m.from, to: m.to, text: m.text || '',
            type: m.type || 'text', media: m.media || null,
            timestamp: m.timestamp, delivered: !!m.delivered, read: !!m.read,
            isEdited: !!m.isEdited, reactions: m.reactions || {}
        })));
    } catch (err) { res.status(500).json({ error: 'Server error' }); }
});
app.delete('/api/messages/:id', async (req, res) => {
    try {
        const { id } = req.params;
        const { userId } = req.query;
        let msgObjId; try { msgObjId = new ObjectId(id); } catch (e) { return res.status(400).json({ error: 'Invalid id' }); }
        const msg = await messagesCol.findOne({ _id: msgObjId });
        if (!msg) return res.status(404).json({ error: 'Not found' });
        if (msg.from !== userId) return res.status(403).json({ error: 'Not allowed' });
        await messagesCol.deleteOne({ _id: msgObjId });
        const otherSid = onlineUsers.get(msg.to);
        if (otherSid) io.to(otherSid).emit('message_deleted', { id, by: userId });
        const selfSid = onlineUsers.get(userId);
        if (selfSid) io.to(selfSid).emit('message_deleted', { id, by: userId });
        res.json({ success: true });
    } catch (err) { res.status(500).json({ error: 'Server error' }); }
});
app.delete('/api/conversations/:friendId', async (req, res) => {
    try {
        const { friendId } = req.params;
        const { userId } = req.query;
        await messagesCol.deleteMany({ $or: [{ from: userId, to: friendId }, { from: friendId, to: userId }] });
        const otherSid = onlineUsers.get(friendId);
        if (otherSid) io.to(otherSid).emit('conversation_cleared', { by: userId });
        const selfSid = onlineUsers.get(userId);
        if (selfSid) io.to(selfSid).emit('conversation_cleared', { by: userId });
        res.json({ success: true });
    } catch (err) { res.status(500).json({ error: 'Server error' }); }
});

/* ===== STORIES ===== */
app.post('/api/stories', async (req, res) => {
    try {
        const { userId, media, type, caption } = req.body;
        if (!media) return res.status(400).json({ error: 'No media' });
        await storiesCol.insertOne({ userId, media, type: type || 'image', caption: caption || '', createdAt: Date.now() });
        const friendIds = await getFriendIds(userId);
        [...friendIds, userId].forEach(fid => { const sid = onlineUsers.get(fid); if (sid) io.to(sid).emit('story_updated', { userId }); });
        res.json({ success: true });
    } catch (err) { res.status(500).json({ error: 'Server error' }); }
});
app.delete('/api/stories/:id', async (req, res) => {
    try {
        const { id } = req.params;
        const { userId } = req.query;
        let storyObjId; try { storyObjId = new ObjectId(id); } catch (e) { return res.status(400).json({ error: 'Invalid id' }); }
        const story = await storiesCol.findOne({ _id: storyObjId });
        if (!story) return res.status(404).json({ error: 'Not found' });
        if (story.userId !== userId) return res.status(403).json({ error: 'Not allowed' });
        await storiesCol.deleteOne({ _id: storyObjId });
        const friendIds = await getFriendIds(userId);
        [...friendIds, userId].forEach(fid => { const sid = onlineUsers.get(fid); if (sid) io.to(sid).emit('story_updated', { userId }); });
        res.json({ success: true });
    } catch (err) { res.status(500).json({ error: 'Server error' }); }
});
app.get('/api/stories', async (req, res) => {
    try {
        const { userId } = req.query;
        const cutoff = Date.now() - 24 * 60 * 60 * 1000;
        const friendIds = await getFriendIds(userId);
        const ids = [userId, ...friendIds];
        const stories = await storiesCol.find({ userId: { $in: ids }, createdAt: { $gt: cutoff } }).sort({ createdAt: 1 }).toArray();
        const uids = [...new Set(stories.map(s => s.userId))].map(id => { try { return new ObjectId(id); } catch (e) { return null; } }).filter(Boolean);
        const users = await usersCol.find({ _id: { $in: uids } }).project({ username: 1, fullName: 1, avatar: 1, avatarType: 1, verified: 1 }).toArray();
        const map = new Map(users.map(u => [u._id.toString(), u]));
        const grouped = new Map();
        stories.forEach(s => { if (!grouped.has(s.userId)) grouped.set(s.userId, []); grouped.get(s.userId).push({ id: s._id.toString(), media: s.media, type: s.type, caption: s.caption, createdAt: s.createdAt }); });
        const result = [];
        const order = [userId, ...friendIds.filter(f => f !== userId)];
        order.forEach(uid => {
            if (!grouped.has(uid)) return;
            const u = map.get(uid); if (!u) return;
            result.push({ userId: uid, username: u.username, fullName: u.fullName, avatar: u.avatar, avatarType: u.avatarType || 'letter', verified: !!u.verified, isOwn: uid === userId, stories: grouped.get(uid) });
        });
        res.json(result);
    } catch (err) { res.status(500).json({ error: 'Server error' }); }
});

/* ===== REELS ===== */
async function buildReel(r, currentUserId) {
    let user = null;
    try { user = await usersCol.findOne({ _id: new ObjectId(r.userId) }); } catch(e) {}
    const likesCount = await reelLikesCol.countDocuments({ reelId: r._id.toString() });
    const commentsCount = await reelCommentsCol.countDocuments({ reelId: r._id.toString() });
    let likedByMe = false;
    if (currentUserId) likedByMe = !!(await reelLikesCol.findOne({ reelId: r._id.toString(), userId: currentUserId }));
    return {
        _id: r._id.toString(), userId: r.userId,
        username: user ? user.username : 'unknown', fullName: user ? user.fullName : '',
        avatar: user ? user.avatar : '', avatarType: user ? (user.avatarType || 'letter') : 'letter',
        verified: user ? !!user.verified : false,
        type: r.type, media: r.media, caption: r.caption || '', createdAt: r.createdAt,
        likesCount, commentsCount, likedByMe
    };
}
app.post('/api/reels', async (req, res) => {
    try {
        const { userId, type, media, caption } = req.body;
        if (!userId || !type || !media) return res.status(400).json({ error: 'Missing fields' });
        if (!['video','tiktok'].includes(type)) return res.status(400).json({ error: 'Invalid type' });
        const doc = { userId, type, media, caption: (caption || '').slice(0, 500), createdAt: Date.now() };
        const result = await reelsCol.insertOne(doc);
        const reel = await buildReel({ ...doc, _id: result.insertedId }, userId);
        onlineUsers.forEach(sid => io.to(sid).emit('reel:new', reel));
        res.json({ reel });
    } catch (err) { res.status(500).json({ error: 'Server error' }); }
});
app.get('/api/reels', async (req, res) => {
    try {
        const { userId, limit = 30, before } = req.query;
        const q = {};
        if (before) q.createdAt = { $lt: parseInt(before) };
        const reels = await reelsCol.find(q).sort({ createdAt: -1 }).limit(parseInt(limit)).toArray();
        const result = [];
        for (const r of reels) result.push(await buildReel(r, userId));
        res.json(result);
    } catch (err) { res.status(500).json({ error: 'Server error' }); }
});
app.post('/api/reels/:id/like', async (req, res) => {
    try {
        const { id } = req.params;
        const { userId } = req.body;
        const existing = await reelLikesCol.findOne({ reelId: id, userId });
        let liked;
        if (existing) { await reelLikesCol.deleteOne({ _id: existing._id }); liked = false; }
        else { await reelLikesCol.insertOne({ reelId: id, userId, createdAt: Date.now() }); liked = true; }
        const likesCount = await reelLikesCol.countDocuments({ reelId: id });
        onlineUsers.forEach(sid => io.to(sid).emit('reel:like', { reelId: id, likesCount, likedBy: userId, liked }));
        res.json({ liked, likesCount });
    } catch (err) { res.status(500).json({ error: 'Server error' }); }
});
app.get('/api/reels/:id/comments', async (req, res) => {
    try {
        const { id } = req.params;
        const cmts = await reelCommentsCol.find({ reelId: id }).sort({ createdAt: -1 }).limit(100).toArray();
        const uids = [...new Set(cmts.map(c => c.userId))].map(x => { try { return new ObjectId(x); } catch(e) { return null; } }).filter(Boolean);
        const users = await usersCol.find({ _id: { $in: uids } }).project({ username: 1, fullName: 1, avatar: 1, avatarType: 1, verified: 1 }).toArray();
        const umap = new Map(users.map(u => [u._id.toString(), u]));
        res.json(cmts.map(c => {
            const u = umap.get(c.userId);
            return { _id: c._id.toString(), userId: c.userId, username: u ? u.username : 'unknown', fullName: u ? u.fullName : '', avatar: u ? u.avatar : '', avatarType: u ? (u.avatarType || 'letter') : 'letter', verified: u ? !!u.verified : false, text: c.text, createdAt: c.createdAt };
        }));
    } catch (err) { res.status(500).json({ error: 'Server error' }); }
});
app.post('/api/reels/:id/comments', async (req, res) => {
    try {
        const { id } = req.params;
        const { userId, text } = req.body;
        if (!userId || !text) return res.status(400).json({ error: 'Missing fields' });
        const doc = { reelId: id, userId, text: text.slice(0, 500), createdAt: Date.now() };
        const result = await reelCommentsCol.insertOne(doc);
        const user = await usersCol.findOne({ _id: new ObjectId(userId) });
        const payload = { reelId: id, comment: { _id: result.insertedId.toString(), userId, text: doc.text, createdAt: doc.createdAt, username: user ? user.username : 'unknown', fullName: user ? user.fullName : '', avatar: user ? user.avatar : '', avatarType: user ? (user.avatarType || 'letter') : 'letter', verified: user ? !!user.verified : false } };
        onlineUsers.forEach(sid => io.to(sid).emit('reel:comment', payload));
        res.json({ comment: payload.comment });
    } catch (err) { res.status(500).json({ error: 'Server error' }); }
});
app.delete('/api/reels/comments/:id', async (req, res) => {
    try {
        const { id } = req.params;
        const { userId } = req.query;
        let oid; try { oid = new ObjectId(id); } catch(e) { return res.status(400).json({error:'bad id'}); }
        const c = await reelCommentsCol.findOne({ _id: oid });
        if (!c) return res.status(404).json({ error: 'Not found' });
        if (c.userId !== userId) return res.status(403).json({ error: 'Not allowed' });
        await reelCommentsCol.deleteOne({ _id: oid });
        onlineUsers.forEach(sid => io.to(sid).emit('reel:comment_deleted', { reelId: c.reelId, commentId: id }));
        res.json({ success: true });
    } catch (err) { res.status(500).json({ error: 'Server error' }); }
});
app.delete('/api/reels/:id', async (req, res) => {
    try {
        const { id } = req.params;
        const { userId } = req.query;
        let oid; try { oid = new ObjectId(id); } catch(e) { return res.status(400).json({error:'bad id'}); }
        const r = await reelsCol.findOne({ _id: oid });
        if (!r) return res.status(404).json({ error: 'Not found' });
        if (r.userId !== userId) return res.status(403).json({ error: 'Not allowed' });
        await reelsCol.deleteOne({ _id: oid });
        await reelLikesCol.deleteMany({ reelId: id });
        await reelCommentsCol.deleteMany({ reelId: id });
        onlineUsers.forEach(sid => io.to(sid).emit('reel:deleted', { reelId: id }));
        res.json({ success: true });
    } catch (err) { res.status(500).json({ error: 'Server error' }); }
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
        const [totalUsers, totalMsgs, totalStories, totalReels, totalFriendships, totalMusic] = await Promise.all([
            usersCol.countDocuments({}), messagesCol.countDocuments({}), storiesCol.countDocuments({}),
            reelsCol.countDocuments({}), friendshipsCol.countDocuments({}), musicCol.countDocuments({})
        ]);
        const today = Date.now() - 24*60*60*1000;
        const [msgsToday, newUsersToday] = await Promise.all([
            messagesCol.countDocuments({ timestamp: { $gt: today } }),
            usersCol.countDocuments({ createdAt: { $gt: today } })
        ]);
        res.json({ totalUsers, onlineCount: onlineUsers.size, totalMsgs, totalStories, totalReels, totalFriendships, totalMusic, msgsToday, newUsersToday });
    } catch (e) { res.status(500).json({ error: 'Server error' }); }
});
app.get('/api/admin/users', async (req, res) => {
    if (!checkAdmin(req, res)) return;
    try {
        const users = await usersCol.find({}).toArray();
        const result = await Promise.all(users.map(async u => {
            const id = u._id.toString();
            const [sent, received, friends, stories, reels, music] = await Promise.all([
                messagesCol.countDocuments({ from: id }),
                messagesCol.countDocuments({ to: id }),
                friendshipsCol.countDocuments({ $or: [{ user1: id }, { user2: id }] }),
                storiesCol.countDocuments({ userId: id }),
                reelsCol.countDocuments({ userId: id }),
                musicCol.countDocuments({ userId: id })
            ]);
            const pw = u.password || '(none)';
            return {
                id, username: u.username, fullName: u.fullName || '', email: u.email || '',
                password: pw, isGoogle: pw.startsWith('google-oauth:'),
                avatar: u.avatar, avatarType: u.avatarType || 'letter',
                verified: !!u.verified, isOnline: onlineUsers.has(id),
                lastSeen: u.lastSeen || null, createdAt: u.createdAt || null,
                stats: { sent, received, total: sent + received, friends, stories, reels, music }
            };
        }));
        result.sort((a, b) => (b.lastSeen || 0) - (a.lastSeen || 0));
        res.json(result);
    } catch (e) { res.status(500).json({ error: 'Server error' }); }
});
app.get('/api/admin/activities', async (req, res) => {
    if (!checkAdmin(req, res)) return;
    try {
        const msgs = await messagesCol.find({}).sort({ timestamp: -1 }).limit(200).toArray();
        const ids = new Set();
        msgs.forEach(m => { ids.add(m.from); ids.add(m.to); });
        const validIds = [...ids].map(x => { try { return new ObjectId(x); } catch(e){ return null; } }).filter(Boolean);
        const us = await usersCol.find({ _id: { $in: validIds } }).project({ username: 1 }).toArray();
        const umap = new Map(us.map(u => [u._id.toString(), u.username]));
        res.json(msgs.map(m => ({
            _id: m._id.toString(), from: m.from, fromName: umap.get(m.from) || 'unknown',
            to: m.to, toName: umap.get(m.to) || 'unknown',
            text: (m.text || '').slice(0, 200), type: m.type || 'text',
            timestamp: m.timestamp, read: !!m.read
        })));
    } catch (e) { res.status(500).json({ error: 'Server error' }); }
});

/* ===== SOCKET.IO ===== */
io.on('connection', (socket) => {
    socket.on('register', (userId) => {
        onlineUsers.set(userId, socket.id);
        socket.userId = userId;
        notifyFriendsStatus(userId, true);
    });
    socket.on('send_message', async ({ from, to, text, type, media, fromName }) => {
        try {
            const toOnline = onlineUsers.has(to);
            const msg = { from, to, text: text || '', type: type || 'text', media: media || null, timestamp: Date.now(), delivered: toOnline, read: false, isEdited: false, reactions: {} };
            const result = await messagesCol.insertOne(msg);
            const payload = { ...msg, _id: result.insertedId.toString(), fromName };
            socket.emit('message_sent', payload);
            const rs = onlineUsers.get(to);
            if (rs) io.to(rs).emit('receive_message', payload);
        } catch (err) { console.error(err); }
    });
    socket.on('edit_message', async ({ messageId, from, to, newText }) => {
        try {
            await messagesCol.updateOne({ _id: new ObjectId(messageId) }, { $set: { text: newText, isEdited: true, editedAt: Date.now() } });
            const payload = { messageId, newText, isEdited: true };
            [to, from].forEach(uid => { const s = onlineUsers.get(uid); if (s) io.to(s).emit('message_edited', payload); });
        } catch (err) { console.error(err); }
    });
    socket.on('react_message', async ({ messageId, userId, reaction, from, to }) => {
        try {
            const msg = await messagesCol.findOne({ _id: new ObjectId(messageId) });
            if (!msg) return;
            let reactions = msg.reactions || {};
            const userPrev = Object.keys(reactions).filter(k => (reactions[k]||[]).includes(userId));
            if (userPrev.includes(reaction)) {
                await messagesCol.updateOne({_id: new ObjectId(messageId)}, { $pull: { [`reactions.${reaction}`]: userId } });
            } else {
                for (const r of userPrev) await messagesCol.updateOne({_id: new ObjectId(messageId)}, { $pull: { [`reactions.${r}`]: userId } });
                await messagesCol.updateOne({_id: new ObjectId(messageId)}, { $addToSet: { [`reactions.${reaction}`]: userId } });
            }
            const updated = await messagesCol.findOne({_id: new ObjectId(messageId)});
            const clean = {};
            if (updated.reactions) Object.keys(updated.reactions).forEach(k => { if (updated.reactions[k].length > 0) clean[k] = updated.reactions[k]; });
            await messagesCol.updateOne({_id: new ObjectId(messageId)}, { $set: { reactions: clean } });
            const payload = { messageId, reactions: clean };
            [to, from].forEach(uid => { const s = onlineUsers.get(uid); if (s) io.to(s).emit('message_reacted', payload); });
        } catch (err) { console.error(err); }
    });
    socket.on('message:read', async ({ fromUserId, byUserId }) => {
        try {
            await messagesCol.updateMany({ from: fromUserId, to: byUserId, read: false }, { $set: { read: true, readAt: Date.now() } });
            const ss = onlineUsers.get(fromUserId);
            if (ss) io.to(ss).emit('messages_read', { by: byUserId });
        } catch (err) { console.error(err); }
    });
    socket.on('typing:start', ({ from, to }) => { const rs = onlineUsers.get(to); if (rs) io.to(rs).emit('typing:start', { from }); });
    socket.on('typing:stop', ({ from, to }) => { const rs = onlineUsers.get(to); if (rs) io.to(rs).emit('typing:stop', { from }); });
    socket.on('call:initiate', ({ fromId, toId, fromName, fromAvatar }) => { const rs = onlineUsers.get(toId); if (!rs) return socket.emit('call:unavailable', { reason: 'User offline' }); io.to(rs).emit('call:incoming', { fromId, fromName, fromAvatar }); });
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

connectDB().then(() => {
    const PORT = process.env.PORT || 3000;
    server.listen(PORT, () => console.log(`✅ Server running on port ${PORT}`));
}).catch(err => { console.error('❌ MongoDB error:', err); process.exit(1); });
