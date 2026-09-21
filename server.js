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
    } catch (e) { console.error('Cloudinary init error:', e.message); }
} else { console.warn('⚠️ Cloudinary env vars not set'); }

let r2Ready = false;
let r2Client = null;
let R2_BUCKET = '';
let R2_PUBLIC_URL = '';
const STORAGE_PROVIDER = (process.env.STORAGE_PROVIDER || 'auto').toLowerCase();

if (process.env.R2_ACCOUNT_ID && process.env.R2_ACCESS_KEY_ID && process.env.R2_SECRET_ACCESS_KEY && process.env.R2_BUCKET_NAME) {
    try {
        r2Client = new S3Client({
            region: 'auto',
            endpoint: `https://${process.env.R2_ACCOUNT_ID}.r2.cloudflarestorage.com`,
            credentials: {
                accessKeyId: process.env.R2_ACCESS_KEY_ID,
                secretAccessKey: process.env.R2_SECRET_ACCESS_KEY,
            },
        });
        R2_BUCKET = process.env.R2_BUCKET_NAME;
        R2_PUBLIC_URL = (process.env.R2_PUBLIC_URL || '').replace(/\/$/, '');
        r2Ready = true;
        console.log('✅ Cloudflare R2 configured:', R2_BUCKET);
    } catch (e) { console.error('R2 init error:', e.message); }
} else { console.warn('⚠️ Cloudflare R2 env vars not set'); }

const YOUTUBE_API_KEY = process.env.YOUTUBE_API_KEY || '';
const youtubeApiReady = !!YOUTUBE_API_KEY;
if (youtubeApiReady) console.log('✅ YouTube Data API configured');
else console.warn('⚠️ YOUTUBE_API_KEY not set');

console.log('✅ Admin portal password configured');

let usersCol, requestsCol, friendshipsCol, messagesCol, storiesCol;
let reelsCol, reelLikesCol, reelCommentsCol, musicCol;
let shortsCol, shortLikesCol;
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
    shortsCol = db.collection('shorts_cache');
    shortLikesCol = db.collection('short_likes');

    await usersCol.createIndex({ username: 1 }, { unique: true });
    await usersCol.createIndex({ email: 1 }, { unique: true });
    await messagesCol.createIndex({ from: 1, to: 1, timestamp: 1 });
    await messagesCol.createIndex({ to: 1, from: 1, timestamp: 1 });
    await messagesCol.createIndex({ timestamp: -1 });
    await requestsCol.createIndex({ to: 1 });
    await friendshipsCol.createIndex({ user1: 1 });
    await friendshipsCol.createIndex({ user2: 1 });
    await storiesCol.createIndex({ userId: 1, createdAt: 1 });
    await reelsCol.createIndex({ createdAt: -1 });
    await reelsCol.createIndex({ userId: 1, createdAt: -1 });
    await reelLikesCol.createIndex({ reelId: 1, userId: 1 }, { unique: true });
    await reelCommentsCol.createIndex({ reelId: 1, createdAt: -1 });
    await musicCol.createIndex({ userId: 1, addedAt: -1 });
    await shortsCol.createIndex({ videoId: 1, category: 1 }, { unique: true });
    await shortsCol.createIndex({ category: 1, fetchedAt: -1 });
    await shortLikesCol.createIndex({ videoId: 1, userId: 1 }, { unique: true });

    const SHORTS_CACHE_VERSION = 3;
    const metaCol = db.collection('shorts_meta');
    const meta = await metaCol.findOne({ key: 'version' });
    if (!meta || meta.value !== SHORTS_CACHE_VERSION) {
        await shortsCol.deleteMany({});
        await metaCol.updateOne({ key: 'version' }, { $set: { value: SHORTS_CACHE_VERSION } }, { upsert: true });
        console.log('🗑️ Shorts cache cleared (version bump)');
    }

    console.log('✅ MongoDB connected with indexes');
}

app.get('/api/ping', (req, res) => res.json({
    ok: true, t: Date.now(),
    storage: { r2: r2Ready, cloudinary: cloudinaryReady, provider: STORAGE_PROVIDER },
    youtube: youtubeApiReady
}));

app.get('/api/config', (req, res) => res.json({ googleClientId: GOOGLE_CLIENT_ID }));

function buildFileName(originalName) {
    const ext = (originalName || '').split('.').pop() || 'bin';
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
    const publicUrl = R2_PUBLIC_URL ? `${R2_PUBLIC_URL}/${key}` : `${process.env.R2_ACCOUNT_ID}.r2.cloudflarestorage.com/${R2_BUCKET}/${key}`;
    return { url: publicUrl, publicId: key, provider: 'r2' };
}

async function uploadToCloudinary(buf, mime) {
    if (!cloudinaryReady) throw new Error('Cloudinary not configured');
    let resourceType = 'raw';
    if (mime.startsWith('image/')) resourceType = 'image';
    else if (mime.startsWith('video/') || mime.startsWith('audio/')) resourceType = 'video';
    const result = await new Promise((resolve, reject) => {
        const stream = cloudinary.uploader.upload_stream(
            { folder: 'encrypted_chat', resource_type: resourceType, unique_filename: true },
            (error, result) => { if (error) reject(error); else resolve(result); }
        );
        stream.end(buf);
    });
    return { url: result.secure_url, publicId: result.public_id, provider: 'cloudinary' };
}

app.post('/api/upload', async (req, res) => {
    try {
        const { base64, fileName } = req.body;
        if (!base64) return res.status(400).json({ error: 'No file data' });
        const match = base64.match(/^data:([^;]+);base64,(.+)$/);
        if (!match) return res.status(400).json({ error: 'Invalid base64 format' });
        const mime = match[1] || 'application/octet-stream';
        const buf = Buffer.from(match[2], 'base64');
        if (buf.length > 15 * 1024 * 1024) return res.status(400).json({ error: 'File too large (max 15MB)' });
        let result = null;
        if (STORAGE_PROVIDER === 'r2') result = await uploadToR2(buf, mime, fileName);
        else if (STORAGE_PROVIDER === 'cloudinary') result = await uploadToCloudinary(buf, mime);
        else {
            try {
                if (r2Ready) result = await uploadToR2(buf, mime, fileName);
                else throw new Error('R2 not ready');
            } catch (e) {
                console.warn('R2 upload failed, trying Cloudinary:', e.message);
                result = await uploadToCloudinary(buf, mime);
            }
        }
        if (!result) return res.status(500).json({ error: 'No storage available' });
        res.json({ url: result.url, publicId: result.publicId, provider: result.provider });
    } catch (err) {
        console.error('Upload error:', err);
        res.status(500).json({ error: 'Upload failed: ' + (err.message || 'unknown') });
    }
});

app.post('/api/tiktok/resolve', async (req, res) => {
    try {
        const { url } = req.body;
        if (!url || typeof url !== 'string') return res.status(400).json({ error: 'No URL provided' });
        let finalUrl = url.trim();
        if (/vm\.tiktok\.com|vt\.tiktok\.com|m\.tiktok\.com\/v\//i.test(finalUrl)) {
            try {
                const r = await fetch(finalUrl, {
                    redirect: 'follow',
                    headers: { 'User-Agent': 'Mozilla/5.0 (compatible; ChatBot/1.0)' }
                });
                finalUrl = r.url || finalUrl;
            } catch (e) {}
        }
        const patterns = [
            /tiktok\.com\/@[^/?#]+\/video\/(\d+)/i,
            /tiktok\.com\/v\/(\d+)/i,
            /tiktok\.com\/embed\/v2\/(\d+)/i,
            /tiktok\.com\/embed\/(\d+)/i
        ];
        for (const p of patterns) {
            const m = finalUrl.match(p);
            if (m) return res.json({ videoId: m[1], url: finalUrl });
        }
        res.status(400).json({ error: 'Invalid TikTok URL. Please paste a valid video link.' });
    } catch (err) {
        res.status(500).json({ error: 'Failed to resolve TikTok URL' });
    }
});

app.post('/api/youtube/oembed', async (req, res) => {
    try {
        const { videoId } = req.body;
        if (!videoId) return res.status(400).json({ error: 'Missing videoId' });
        const url = `https://www.youtube.com/oembed?url=https://www.youtube.com/watch?v=${videoId}&format=json`;
        const r = await fetch(url);
        if (!r.ok) return res.status(400).json({ error: 'Video not found or not embeddable' });
        const data = await r.json();
        res.json({ videoId, title: data.title, thumbnail: data.thumbnail_url, author: data.author_name });
    } catch (err) {
        res.status(500).json({ error: 'Failed to fetch video info' });
    }
});

app.get('/api/youtube/search', async (req, res) => {
    try {
        if (!youtubeApiReady) return res.status(500).json({ error: 'YouTube search is not configured' });
        const { q, maxResults = 20 } = req.query;
        if (!q || q.trim().length < 2) return res.json({ items: [] });
        const url = `https://www.googleapis.com/youtube/v3/search?part=snippet&type=video&videoEmbeddable=true&videoSyndicated=true&maxResults=${Math.min(parseInt(maxResults), 50)}&q=${encodeURIComponent(q)}&key=${YOUTUBE_API_KEY}`;
        const r = await fetch(url);
        const data = await r.json();
        if (!r.ok) return res.status(r.status).json({ error: (data.error && data.error.message) || 'Search failed' });
        const items = (data.items || []).map(it => ({
            videoId: it.id.videoId,
            title: it.snippet.title,
            thumbnail: it.snippet.thumbnails.medium?.url || it.snippet.thumbnails.default?.url,
            author: it.snippet.channelTitle,
            publishedAt: it.snippet.publishedAt
        }));
        res.json({ items });
    } catch (err) { res.status(500).json({ error: 'Search failed' }); }
});

app.post('/api/music', async (req, res) => {
    try {
        const { userId, videoId, title, thumbnail, author } = req.body;
        if (!userId || !videoId) return res.status(400).json({ error: 'Missing fields' });
        const doc = {
            userId, videoId,
            title: title || 'Unknown Title',
            thumbnail: thumbnail || `https://img.youtube.com/vi/${videoId}/mqdefault.jpg`,
            author: author || 'Unknown Artist',
            addedAt: Date.now()
        };
        const result = await musicCol.insertOne(doc);
        res.json({ song: { ...doc, _id: result.insertedId.toString() } });
    } catch (err) { res.status(500).json({ error: 'Server error' }); }
});

app.get('/api/music', async (req, res) => {
    try {
        const { userId } = req.query;
        if (!userId) return res.status(400).json({ error: 'Missing userId' });
        const songs = await musicCol.find({ userId }).sort({ addedAt: -1 }).toArray();
        res.json(songs.map(s => ({
            _id: s._id.toString(), videoId: s.videoId, title: s.title,
            thumbnail: s.thumbnail, author: s.author, addedAt: s.addedAt
        })));
    } catch (err) { res.status(500).json({ error: 'Server error' }); }
});

app.delete('/api/music/:id', async (req, res) => {
    try {
        const { id } = req.params;
        const { userId } = req.query;
        if (!userId) return res.status(400).json({ error: 'Missing userId' });
        let oid;
        try { oid = new ObjectId(id); } catch(e) { return res.status(400).json({error:'bad id'}); }
        const song = await musicCol.findOne({ _id: oid });
        if (!song) return res.status(404).json({ error: 'Not found' });
        if (song.userId !== userId) return res.status(403).json({ error: 'Not allowed' });
        await musicCol.deleteOne({ _id: oid });
        res.json({ success: true });
    } catch (err) { res.status(500).json({ error: 'Server error' }); }
});

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
        lastSeen: u.lastSeen || null
    };
}

async function notifyFriendsStatus(userId, isOnline) {
    try {
        const now = isOnline ? null : Date.now();
        if (!isOnline) await usersCol.updateOne({ _id: new ObjectId(userId) }, { $set: { lastSeen: now } });
        const friendIds = await getFriendIds(userId);
        friendIds.forEach(fid => {
            const sid = onlineUsers.get(fid);
            if (sid) io.to(sid).emit('user_status', { userId, isOnline, lastSeen: now });
        });
    } catch (e) { console.error(e); }
}

async function notifyFriendsVerified(userId, verified) {
    try {
        const friendIds = await getFriendIds(userId);
        friendIds.forEach(fid => {
            const sid = onlineUsers.get(fid);
            if (sid) io.to(sid).emit('user_verified', { userId, verified });
        });
    } catch (e) { console.error(e); }
}

async function notifyFriendsProfileUpdate(userId) {
    try {
        const user = await usersCol.findOne({ _id: new ObjectId(userId) });
        if (!user) return;
        const payload = {
            userId, username: user.username, fullName: user.fullName,
            avatar: user.avatar, avatarType: user.avatarType || 'letter',
            verified: !!user.verified
        };
        const friendIds = await getFriendIds(userId);
        friendIds.forEach(fid => {
            const sid = onlineUsers.get(fid);
            if (sid) io.to(sid).emit('user_profile_updated', payload);
        });
        const selfSid = onlineUsers.get(userId);
        if (selfSid) io.to(selfSid).emit('user_profile_updated', payload);
    } catch (e) { console.error(e); }
}

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
        if (!username || !password) return res.status(400).json({ error: 'Please enter username and password' });
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
        if (!credential) return res.status(400).json({ error: 'No credential provided' });
        const verifyRes = await fetch('https://oauth2.googleapis.com/tokeninfo?id_token=' + encodeURIComponent(credential));
        if (!verifyRes.ok) return res.status(400).json({ error: 'Invalid Google token' });
        const info = await verifyRes.json();
        if (GOOGLE_CLIENT_ID && info.aud !== GOOGLE_CLIENT_ID) return res.status(400).json({ error: 'Token audience mismatch' });
        const email = info.email;
        const name = info.name || (email ? email.split('@')[0] : 'User');
        const picture = info.picture || '';
        const googleId = info.sub;
        if (!email) return res.status(400).json({ error: 'No email in Google account' });
        let user = await usersCol.findOne({ email });
        if (!user) {
            let base = (email.split('@')[0] || 'user').replace(/[^a-zA-Z0-9_.]/g, '').toLowerCase();
            if (base.length < 3) base = 'user' + Date.now().toString().slice(-4);
            let username = base;
            let counter = 1;
            while (await usersCol.findOne({ username })) {
                username = base + '_' + counter; counter++;
                if (counter > 999) { username = base + '_' + Date.now().toString().slice(-6); break; }
            }
            const newUser = {
                fullName: name, username, email,
                password: 'google-oauth:' + googleId,
                avatar: picture || (name ? name[0].toUpperCase() : 'U'),
                avatarType: picture ? 'image' : 'letter',
                verified: false, googleId, lastSeen: Date.now(), createdAt: Date.now()
            };
            const result = await usersCol.insertOne(newUser);
            user = { ...newUser, _id: result.insertedId };
        } else if (!user.googleId) {
            await usersCol.updateOne({ _id: user._id }, { $set: { googleId, lastSeen: Date.now() } });
        }
        res.json({ user: {
            id: user._id.toString(), username: user.username, fullName: user.fullName,
            email: user.email, avatar: user.avatar,
            avatarType: user.avatarType || 'letter', verified: !!user.verified,
            hasPassword: !(user.password || '').startsWith('google-oauth:')
        }});
    } catch (err) { res.status(500).json({ error: 'Google sign-in failed' }); }
});

app.post('/api/users/avatar', async (req, res) => {
    try {
        const { userId, avatar } = req.body;
        if (!avatar) return res.status(400).json({ error: 'No avatar provided' });
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
        if (!newUsername || newUsername.length < 3) return res.status(400).json({ error: 'Username must be at least 3 characters' });
        if (!/^[a-zA-Z0-9_.]+$/.test(newUsername)) return res.status(400).json({ error: 'Only letters, numbers, . and _ allowed' });
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
        if (!oldPassword || !newPassword) return res.status(400).json({ error: 'Please fill in all fields' });
        if (newPassword.length < 6) return res.status(400).json({ error: 'New password must be at least 6 characters' });
        if (oldPassword === newPassword) return res.status(400).json({ error: 'New password must be different' });
        const user = await usersCol.findOne({ _id: new ObjectId(userId) });
        if (!user) return res.status(404).json({ error: 'User not found' });
        if (user.password && user.password.startsWith('google-oauth:')) return res.status(400).json({ error: 'This account uses Google Sign-In.' });
        if (user.password !== oldPassword) return res.status(400).json({ error: 'Current password is incorrect' });
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

app.post('/api/friend-request', async (req, res) => {
    try {
        const { from, to } = req.body;
        if (from === to) return res.status(400).json({ error: 'You cannot send a request to yourself' });
        const existing = await friendshipsCol.findOne({ $or: [{ user1: from, user2: to }, { user1: to, user2: from }] });
        if (existing) return res.status(400).json({ error: 'You are already friends' });
        const existingReq = await requestsCol.findOne({ from, to });
        if (existingReq) return res.status(400).json({ error: 'Request already sent' });
        const reverse = await requestsCol.findOne({ from: to, to: from });
        if (reverse) {
            await requestsCol.deleteOne({ _id: reverse._id });
            await friendshipsCol.insertOne({ user1: from, user2: to, createdAt: Date.now() });
            const senderSocket = onlineUsers.get(to);
            if (senderSocket) io.to(senderSocket).emit('friend_added');
            const receiverSocket = onlineUsers.get(from);
            if (receiverSocket) io.to(receiverSocket).emit('friend_added');
            return res.json({ success: true, autoAccepted: true });
        }
        await requestsCol.insertOne({ from, to, createdAt: Date.now() });
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
        const senders = await usersCol.find({ _id: { $in: ids } })
            .project({ username: 1, fullName: 1, avatar: 1, avatarType: 1, lastSeen: 1, verified: 1 }).toArray();
        const map = new Map(senders.map(s => [s._id.toString(), s]));
        res.json(reqs.map(r => { const s = map.get(r.from); return s ? formatUser(s) : null; }).filter(Boolean));
    } catch (err) { res.status(500).json({ error: 'Server error' }); }
});

app.post('/api/friend-request/accept', async (req, res) => {
    try {
        const { userId, friendId } = req.body;
        await requestsCol.deleteMany({ from: friendId, to: userId });
        await friendshipsCol.insertOne({ user1: userId, user2: friendId, createdAt: Date.now() });
        const ss = onlineUsers.get(friendId);
        if (ss) io.to(ss).emit('friend_added');
        const ms = onlineUsers.get(userId);
        if (ms) io.to(ms).emit('friend_added');
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
        const friends = await usersCol.find({ _id: { $in: ids } })
            .project({ username: 1, fullName: 1, avatar: 1, avatarType: 1, lastSeen: 1, verified: 1 }).toArray();
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

app.get('/api/conversations', async (req, res) => {
    try {
        const { userId } = req.query;
        const friendships = await friendshipsCol.find({ $or: [{ user1: userId }, { user2: userId }] }).toArray();
        if (!friendships.length) return res.json([]);
        const friendIds = friendships.map(f => f.user1 === userId ? f.user2 : f.user1);
        const results = [];
        for (const fid of friendIds) {
            let friendObjId;
            try { friendObjId = new ObjectId(fid); } catch (e) { continue; }
            const friend = await usersCol.findOne({ _id: friendObjId });
            if (!friend) continue;
            const lastMsgs = await messagesCol.find({ $or: [{ from: userId, to: fid }, { from: fid, to: userId }] }).sort({ timestamp: -1 }).limit(1).toArray();
            const unreadCount = await messagesCol.countDocuments({ from: fid, to: userId, read: false });
            results.push({
                friend: formatUser(friend),
                lastMessage: lastMsgs[0] ? {
                    text: lastMsgs[0].text || '', type: lastMsgs[0].type || 'text',
                    timestamp: lastMsgs[0].timestamp, fromMe: lastMsgs[0].from === userId
                } : null,
                unreadCount
            });
        }
        results.sort((a, b) => {
            const ta = a.lastMessage ? a.lastMessage.timestamp : 0;
            const tb = b.lastMessage ? b.lastMessage.timestamp : 0;
            return tb - ta;
        });
        res.json(results);
    } catch (err) { res.status(500).json({ error: 'Server error' }); }
});

app.get('/api/messages', async (req, res) => {
    try {
        const { userId, friendId } = req.query;
        if (!userId || !friendId) return res.status(400).json({ error: 'Missing params' });
        const msgs = await messagesCol.find({
            $or: [{ from: userId, to: friendId }, { from: friendId, to: userId }]
        }).sort({ timestamp: 1 }).toArray();
        res.json(msgs.map(m => ({
            _id: m._id.toString(), from: m.from, to: m.to, text: m.text || '',
            type: m.type || 'text', media: m.media || null,
            timestamp: m.timestamp, delivered: !!m.delivered, read: !!m.read,
            isEdited: !!m.isEdited, reactions: m.reactions || {}
        })));
    } catch (err) {
        console.error('Messages fetch error:', err);
        res.status(500).json({ error: 'Server error' });
    }
});

app.delete('/api/messages/:id', async (req, res) => {
    try {
        const { id } = req.params;
        const { userId } = req.query;
        if (!userId) return res.status(400).json({ error: 'Missing userId' });
        let msgObjId;
        try { msgObjId = new ObjectId(id); } catch (e) { return res.status(400).json({ error: 'Invalid message id' }); }
        const msg = await messagesCol.findOne({ _id: msgObjId });
        if (!msg) return res.status(404).json({ error: 'Message not found' });
        if (msg.from !== userId) return res.status(403).json({ error: 'You can only delete your own messages' });
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
        if (!userId || !friendId) return res.status(400).json({ error: 'Missing params' });
        await messagesCol.deleteMany({ $or: [{ from: userId, to: friendId }, { from: friendId, to: userId }] });
        const otherSid = onlineUsers.get(friendId);
        if (otherSid) io.to(otherSid).emit('conversation_cleared', { by: userId });
        const selfSid = onlineUsers.get(userId);
        if (selfSid) io.to(selfSid).emit('conversation_cleared', { by: userId });
        res.json({ success: true });
    } catch (err) { res.status(500).json({ error: 'Server error' }); }
});

app.post('/api/stories', async (req, res) => {
    try {
        const { userId, media, type, caption } = req.body;
        if (!media) return res.status(400).json({ error: 'No media' });
        await storiesCol.insertOne({ userId, media, type: type || 'image', caption: caption || '', createdAt: Date.now() });
        const friendIds = await getFriendIds(userId);
        friendIds.forEach(fid => {
            const sid = onlineUsers.get(fid);
            if (sid) io.to(sid).emit('story_updated', { userId });
        });
        const selfSid = onlineUsers.get(userId);
        if (selfSid) io.to(selfSid).emit('story_updated', { userId });
        res.json({ success: true });
    } catch (err) { res.status(500).json({ error: 'Server error' }); }
});

app.delete('/api/stories/:id', async (req, res) => {
    try {
        const { id } = req.params;
        const { userId } = req.query;
        if (!userId) return res.status(400).json({ error: 'Missing userId' });
        let storyObjId;
        try { storyObjId = new ObjectId(id); } catch (e) { return res.status(400).json({ error: 'Invalid story id' }); }
        const story = await storiesCol.findOne({ _id: storyObjId });
        if (!story) return res.status(404).json({ error: 'Story not found' });
        if (story.userId !== userId) return res.status(403).json({ error: 'Not authorized' });
        await storiesCol.deleteOne({ _id: storyObjId });
        const friendIds = await getFriendIds(userId);
        friendIds.forEach(fid => {
            const sid = onlineUsers.get(fid);
            if (sid) io.to(sid).emit('story_updated', { userId });
        });
        const selfSid = onlineUsers.get(userId);
        if (selfSid) io.to(selfSid).emit('story_updated', { userId });
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
        stories.forEach(s => {
            if (!grouped.has(s.userId)) grouped.set(s.userId, []);
            grouped.get(s.userId).push({ id: s._id.toString(), media: s.media, type: s.type, caption: s.caption, createdAt: s.createdAt });
        });
        const result = [];
        const order = [userId, ...friendIds.filter(f => f !== userId)];
        order.forEach(uid => {
            if (!grouped.has(uid)) return;
            const u = map.get(uid);
            if (!u) return;
            result.push({
                userId: uid, username: u.username, fullName: u.fullName,
                avatar: u.avatar, avatarType: u.avatarType || 'letter',
                verified: !!u.verified, isOwn: uid === userId,
                stories: grouped.get(uid)
            });
        });
        res.json(result);
    } catch (err) { res.status(500).json({ error: 'Server error' }); }
});

async function buildReel(r, currentUserId) {
    let user = null;
    try { user = await usersCol.findOne({ _id: new ObjectId(r.userId) }); } catch(e) {}
    const likesCount = await reelLikesCol.countDocuments({ reelId: r._id.toString() });
    const commentsCount = await reelCommentsCol.countDocuments({ reelId: r._id.toString() });
    let likedByMe = false;
    if (currentUserId) likedByMe = !!(await reelLikesCol.findOne({ reelId: r._id.toString(), userId: currentUserId }));
    return {
        _id: r._id.toString(), userId: r.userId,
        username: user ? user.username : 'unknown',
        fullName: user ? user.fullName : '',
        avatar: user ? user.avatar : '',
        avatarType: user ? (user.avatarType || 'letter') : 'letter',
        verified: user ? !!user.verified : false,
        type: r.type, media: r.media, caption: r.caption || '',
        createdAt: r.createdAt, likesCount, commentsCount, likedByMe
    };
}

app.post('/api/reels', async (req, res) => {
    try {
        const { userId, type, media, caption } = req.body;
        if (!userId || !type || !media) return res.status(400).json({ error: 'Missing fields' });
        if (!['video','tiktok'].includes(type)) return res.status(400).json({ error: 'Invalid reel type' });
        const doc = { userId, type, media, caption: (caption || '').slice(0, 500), createdAt: Date.now() };
        const result = await reelsCol.insertOne(doc);
        const reel = await buildReel({ ...doc, _id: result.insertedId }, userId);
        onlineUsers.forEach((sid) => { io.to(sid).emit('reel:new', reel); });
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

app.get('/api/reels/mine', async (req, res) => {
    try {
        const { userId } = req.query;
        if (!userId) return res.status(400).json({ error: 'Missing userId' });
        const reels = await reelsCol.find({ userId }).sort({ createdAt: -1 }).limit(100).toArray();
        const result = [];
        for (const r of reels) result.push(await buildReel(r, userId));
        res.json(result);
    } catch (err) { res.status(500).json({ error: 'Server error' }); }
});

app.post('/api/reels/:id/like', async (req, res) => {
    try {
        const { id } = req.params;
        const { userId } = req.body;
        if (!userId) return res.status(400).json({ error: 'Missing userId' });
        const existing = await reelLikesCol.findOne({ reelId: id, userId });
        let liked;
        if (existing) { await reelLikesCol.deleteOne({ _id: existing._id }); liked = false; }
        else { await reelLikesCol.insertOne({ reelId: id, userId, createdAt: Date.now() }); liked = true; }
        const likesCount = await reelLikesCol.countDocuments({ reelId: id });
        onlineUsers.forEach((sid) => { io.to(sid).emit('reel:like', { reelId: id, likesCount, likedBy: userId, liked }); });
        res.json({ liked, likesCount });
    } catch (err) { res.status(500).json({ error: 'Server error' }); }
});

app.get('/api/reels/:id/comments', async (req, res) => {
    try {
        const { id } = req.params;
        const cmts = await reelCommentsCol.find({ reelId: id }).sort({ createdAt: -1 }).limit(100).toArray();
        const uids = [...new Set(cmts.map(c => c.userId))].map(x => { try { return new ObjectId(x); } catch(e){ return null; } }).filter(Boolean);
        const users = await usersCol.find({ _id: { $in: uids } }).project({ username: 1, fullName: 1, avatar: 1, avatarType: 1, verified: 1 }).toArray();
        const umap = new Map(users.map(u => [u._id.toString(), u]));
        res.json(cmts.map(c => {
            const u = umap.get(c.userId);
            return {
                _id: c._id.toString(), userId: c.userId,
                username: u ? u.username : 'unknown',
                fullName: u ? u.fullName : '',
                avatar: u ? u.avatar : '',
                avatarType: u ? (u.avatarType || 'letter') : 'letter',
                verified: u ? !!u.verified : false,
                text: c.text, createdAt: c.createdAt
            };
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
        const payload = {
            reelId: id,
            comment: {
                _id: result.insertedId.toString(), userId, text: doc.text, createdAt: doc.createdAt,
                username: user ? user.username : 'unknown',
                fullName: user ? user.fullName : '',
                avatar: user ? user.avatar : '',
                avatarType: user ? (user.avatarType || 'letter') : 'letter',
                verified: user ? !!user.verified : false
            }
        };
        onlineUsers.forEach((sid) => { io.to(sid).emit('reel:comment', payload); });
        res.json({ comment: payload.comment });
    } catch (err) { res.status(500).json({ error: 'Server error' }); }
});

app.delete('/api/reels/comments/:id', async (req, res) => {
    try {
        const { id } = req.params;
        const { userId } = req.query;
        if (!userId) return res.status(400).json({ error: 'Missing userId' });
        let oid; try { oid = new ObjectId(id); } catch(e) { return res.status(400).json({error:'bad id'}); }
        const c = await reelCommentsCol.findOne({ _id: oid });
        if (!c) return res.status(404).json({ error: 'Not found' });
        if (c.userId !== userId) return res.status(403).json({ error: 'Not allowed' });
        await reelCommentsCol.deleteOne({ _id: oid });
        onlineUsers.forEach((sid) => { io.to(sid).emit('reel:comment_deleted', { reelId: c.reelId, commentId: id }); });
        res.json({ success: true });
    } catch (err) { res.status(500).json({ error: 'Server error' }); }
});

app.delete('/api/reels/:id', async (req, res) => {
    try {
        const { id } = req.params;
        const { userId } = req.query;
        if (!userId) return res.status(400).json({ error: 'Missing userId' });
        let oid; try { oid = new ObjectId(id); } catch(e) { return res.status(400).json({error:'bad id'}); }
        const r = await reelsCol.findOne({ _id: oid });
        if (!r) return res.status(404).json({ error: 'Not found' });
        if (r.userId !== userId) return res.status(403).json({ error: 'Not allowed' });
        await reelsCol.deleteOne({ _id: oid });
        await reelLikesCol.deleteMany({ reelId: id });
        await reelCommentsCol.deleteMany({ reelId: id });
        onlineUsers.forEach((sid) => { io.to(sid).emit('reel:deleted', { reelId: id }); });
        res.json({ success: true });
    } catch (err) { res.status(500).json({ error: 'Server error' }); }
});

/* SHORTS — Mixed feed, no categories */
const SHORT_CATEGORIES = {
    trending:    { q: 'shorts viral trending aesthetic edit' },
    aesthetic:   { q: 'shorts aesthetic video edit vibes' },
    love:        { q: 'shorts love status couple goals romantic' },
    sad:         { q: 'shorts sad status emotional broken heart' },
    attitude:    { q: 'shorts attitude status boy stylish' },
    dance:       { q: 'shorts dance trending viral reel' },
    meme:        { q: 'shorts funny meme comedy trending' },
    indian:      { q: 'shorts indian love status video song' },
    bangla:      { q: 'shorts bangla status video song romantic' },
    korean:      { q: 'shorts korean aesthetic edit bts' },
    romantic:    { q: 'shorts romantic couple love edit' },
    whatsapp:    { q: 'shorts whatsapp status video love' },
    edit:        { q: 'shorts edit video transition smooth' },
    girl:        { q: 'shorts girl aesthetic outfit style' },
    boy:         { q: 'shorts boy aesthetic style attitude' },
    music:       { q: 'shorts song music lyrical edit' }
};

async function fetchShortsFromYouTube(query, maxResults = 50) {
    if (!youtubeApiReady) throw new Error('YouTube API not configured');
    const url = `https://www.googleapis.com/youtube/v3/search?part=snippet&type=video&videoDuration=short&maxResults=${maxResults}&q=${encodeURIComponent(query)}&key=${YOUTUBE_API_KEY}`;
    const r = await fetch(url);
    const data = await r.json();
    if (!r.ok) throw new Error((data.error && data.error.message) || 'YouTube search failed');
    return (data.items || []).map(it => ({
        videoId: it.id.videoId,
        title: it.snippet.title,
        thumbnail: it.snippet.thumbnails.high?.url || it.snippet.thumbnails.medium?.url || it.snippet.thumbnails.default?.url,
        author: it.snippet.channelTitle,
        publishedAt: it.snippet.publishedAt
    })).filter(v => v.videoId);
}

async function refreshShortsCache(category) {
    const cat = SHORT_CATEGORIES[category];
    if (!cat) throw new Error('Invalid category');
    const items = await fetchShortsFromYouTube(cat.q, 50);
    const now = Date.now();
    if (items.length) {
        const ops = items.map(item => ({
            updateOne: {
                filter: { videoId: item.videoId, category },
                update: { $set: { ...item, category, fetchedAt: now } },
                upsert: true
            }
        }));
        await shortsCol.bulkWrite(ops);
    }
    console.log(`✅ Shorts cache [${category}]: ${items.length} videos`);
    return items.length;
}

app.get('/api/shorts/feed', async (req, res) => {
    try {
        const { limit = 20, userId } = req.query;
        const lim = parseInt(limit);

        const cats = Object.keys(SHORT_CATEGORIES);
        for (const cat of cats) {
            const sample = await shortsCol.findOne({ category: cat }, { sort: { fetchedAt: -1 } });
            if (!sample || (Date.now() - sample.fetchedAt > 6 * 60 * 60 * 1000)) {
                refreshShortsCache(cat).catch(e => console.error(`Refresh [${cat}]:`, e.message));
            }
        }

        let shorts = await shortsCol.aggregate([
            { $sample: { size: lim } }
        ]).toArray();

        if (!shorts.length) {
            await refreshShortsCache('trending').catch(()=>{});
            await refreshShortsCache('aesthetic').catch(()=>{});
            shorts = await shortsCol.aggregate([
                { $sample: { size: lim } }
            ]).toArray();
        }

        let likedIds = new Set();
        if (userId && shorts.length) {
            const likes = await shortLikesCol.find({ userId, videoId: { $in: shorts.map(s => s.videoId) } }).toArray();
            likedIds = new Set(likes.map(l => l.videoId));
        }
        const likeCounts = shorts.length ? await shortLikesCol.aggregate([
            { $match: { videoId: { $in: shorts.map(s => s.videoId) } } },
            { $group: { _id: '$videoId', count: { $sum: 1 } } }
        ]).toArray() : [];
        const countMap = new Map(likeCounts.map(c => [c._id, c.count]));

        res.json(shorts.map(s => ({
            videoId: s.videoId,
            title: s.title,
            thumbnail: s.thumbnail,
            author: s.author,
            category: s.category,
            liked: likedIds.has(s.videoId),
            likesCount: countMap.get(s.videoId) || 0
        })));
    } catch (e) {
        console.error('Shorts feed error:', e);
        res.status(500).json({ error: e.message });
    }
});

app.post('/api/shorts/:videoId/like', async (req, res) => {
    try {
        const { videoId } = req.params;
        const { userId } = req.body;
        if (!userId) return res.status(400).json({ error: 'Missing userId' });
        const existing = await shortLikesCol.findOne({ videoId, userId });
        let liked;
        if (existing) { await shortLikesCol.deleteOne({ _id: existing._id }); liked = false; }
        else { await shortLikesCol.insertOne({ videoId, userId, likedAt: Date.now() }); liked = true; }
        const count = await shortLikesCol.countDocuments({ videoId });
        res.json({ liked, likesCount: count });
    } catch (e) { res.status(500).json({ error: 'Server error' }); }
});

/* ADMIN */
function checkAdmin(req, res) {
    if (req.headers['x-admin-pass'] !== ADMIN_PASSWORD) {
        res.status(401).json({ error: 'Unauthorized' });
        return false;
    }
    return true;
}

app.post('/api/admin/verify', (req, res) => {
    const { password } = req.body || {};
    if (!password) return res.status(400).json({ error: 'Password required' });
    if (password === ADMIN_PASSWORD) return res.json({ ok: true });
    res.status(401).json({ error: 'Invalid password' });
});

app.get('/api/admin/stats', async (req, res) => {
    if (!checkAdmin(req, res)) return;
    try {
        const totalUsers = await usersCol.countDocuments({});
        const onlineCount = onlineUsers.size;
        const totalMsgs = await messagesCol.countDocuments({});
        const totalStories = await storiesCol.countDocuments({});
        const totalReels = await reelsCol.countDocuments({});
        const totalFriendships = await friendshipsCol.countDocuments({});
        const totalMusic = await musicCol.countDocuments({});
        const totalShorts = await shortsCol.countDocuments({});
        const today = Date.now() - 24*60*60*1000;
        const msgsToday = await messagesCol.countDocuments({ timestamp: { $gt: today } });
        const newUsersToday = await usersCol.countDocuments({ createdAt: { $gt: today } });
        res.json({ totalUsers, onlineCount, totalMsgs, totalStories, totalReels, totalFriendships, totalMusic, totalShorts, msgsToday, newUsersToday });
    } catch (e) { res.status(500).json({ error: 'Server error' }); }
});

app.get('/api/admin/users', async (req, res) => {
    if (!checkAdmin(req, res)) return;
    try {
        const users = await usersCol.find({}).toArray();
        const result = [];
        for (const u of users) {
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
            result.push({
                id, username: u.username, fullName: u.fullName || '', email: u.email || '',
                password: pw, isGoogle: pw.startsWith('google-oauth:'),
                avatar: u.avatar, avatarType: u.avatarType || 'letter',
                verified: !!u.verified, isOnline: onlineUsers.has(id),
                lastSeen: u.lastSeen || null, createdAt: u.createdAt || null,
                stats: { sent, received, total: sent + received, friends, stories, reels, music }
            });
        }
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
            _id: m._id.toString(),
            from: m.from, fromName: umap.get(m.from) || 'unknown',
            to: m.to, toName: umap.get(m.to) || 'unknown',
            text: (m.text || '').slice(0, 200),
            type: m.type || 'text', timestamp: m.timestamp, read: !!m.read
        })));
    } catch (e) { res.status(500).json({ error: 'Server error' }); }
});

/* SOCKET.IO */
io.on('connection', (socket) => {
    socket.on('register', (userId) => {
        onlineUsers.set(userId, socket.id);
        socket.userId = userId;
        notifyFriendsStatus(userId, true);
    });

    socket.on('send_message', async ({ from, to, text, type, media, fromName }) => {
        try {
            const toOnline = onlineUsers.has(to);
            const msg = {
                from, to, text: text || '', type: type || 'text', media: media || null,
                timestamp: Date.now(), delivered: toOnline, read: false,
                isEdited: false, reactions: {}
            };
            const result = await messagesCol.insertOne(msg);
            const payload = { ...msg, _id: result.insertedId.toString(), fromName };
            socket.emit('message_sent', payload);
            const rs = onlineUsers.get(to);
            if (rs) io.to(rs).emit('receive_message', payload);
        } catch (err) {
            console.error('❌ Message save failed:', err);
            socket.emit('message_error', { error: 'Message could not be saved. Please try again.', details: err.message });
        }
    });

    socket.on('edit_message', async ({ messageId, from, to, newText }) => {
        try {
            await messagesCol.updateOne(
                { _id: new ObjectId(messageId) },
                { $set: { text: newText, isEdited: true, editedAt: Date.now() } }
            );
            const payload = { messageId, newText, isEdited: true };
            const rs = onlineUsers.get(to);
            if (rs) io.to(rs).emit('message_edited', payload);
            const ss = onlineUsers.get(from);
            if (ss) io.to(ss).emit('message_edited', payload);
        } catch (err) { console.error(err); }
    });

    socket.on('react_message', async ({ messageId, userId, reaction, from, to }) => {
        try {
            const msg = await messagesCol.findOne({ _id: new ObjectId(messageId) });
            if (!msg) return;
            let reactions = msg.reactions || {};
            let userPreviousReactions = Object.keys(reactions).filter(k => (reactions[k]||[]).includes(userId));
            if (userPreviousReactions.includes(reaction)) {
                await messagesCol.updateOne({_id: new ObjectId(messageId)}, { $pull: { [`reactions.${reaction}`]: userId } });
            } else {
                for (let r of userPreviousReactions) {
                    await messagesCol.updateOne({_id: new ObjectId(messageId)}, { $pull: { [`reactions.${r}`]: userId } });
                }
                await messagesCol.updateOne({_id: new ObjectId(messageId)}, { $addToSet: { [`reactions.${reaction}`]: userId } });
            }
            const updatedMsg = await messagesCol.findOne({_id: new ObjectId(messageId)});
            let cleanReactions = {};
            if(updatedMsg.reactions){
               Object.keys(updatedMsg.reactions).forEach(k => {
                   if(updatedMsg.reactions[k].length > 0) cleanReactions[k] = updatedMsg.reactions[k];
               });
            }
            await messagesCol.updateOne({_id: new ObjectId(messageId)}, { $set: { reactions: cleanReactions } });
            const payload = { messageId, reactions: cleanReactions };
            const rs = onlineUsers.get(to);
            if (rs) io.to(rs).emit('message_reacted', payload);
            const ss = onlineUsers.get(from);
            if (ss) io.to(ss).emit('message_reacted', payload);
            socket.emit('message_reacted', payload);
        } catch (err) { console.error(err); }
    });

    socket.on('message:read', async ({ fromUserId, byUserId }) => {
        try {
            await messagesCol.updateMany({ from: fromUserId, to: byUserId, read: false }, { $set: { read: true, readAt: Date.now() } });
            const senderSocket = onlineUsers.get(fromUserId);
            if (senderSocket) io.to(senderSocket).emit('messages_read', { by: byUserId });
        } catch (err) { console.error(err); }
    });

    socket.on('typing:start', ({ from, to }) => {
        const rs = onlineUsers.get(to);
        if (rs) io.to(rs).emit('typing:start', { from });
    });
    socket.on('typing:stop', ({ from, to }) => {
        const rs = onlineUsers.get(to);
        if (rs) io.to(rs).emit('typing:stop', { from });
    });

    socket.on('call:initiate', ({ fromId, toId, fromName, fromAvatar }) => {
        const rs = onlineUsers.get(toId);
        if (!rs) return socket.emit('call:unavailable', { reason: 'User is offline' });
        io.to(rs).emit('call:incoming', { fromId, fromName, fromAvatar });
    });
    socket.on('call:accept', ({ fromId, toId }) => {
        const cs = onlineUsers.get(toId);
        if (cs) io.to(cs).emit('call:accepted', { fromId });
    });
    socket.on('call:reject', ({ fromId, toId }) => {
        const cs = onlineUsers.get(toId);
        if (cs) io.to(cs).emit('call:rejected', { fromId });
    });
    socket.on('call:end', ({ fromId, toId }) => {
        const os = onlineUsers.get(toId);
        if (os) io.to(os).emit('call:ended', { fromId });
    });
    socket.on('webrtc:offer', ({ toId, offer }) => {
        const ts = onlineUsers.get(toId);
        if (ts) io.to(ts).emit('webrtc:offer', { fromId: socket.userId, offer });
    });
    socket.on('webrtc:answer', ({ toId, answer }) => {
        const ts = onlineUsers.get(toId);
        if (ts) io.to(ts).emit('webrtc:answer', { fromId: socket.userId, answer });
    });
    socket.on('webrtc:ice', ({ toId, candidate }) => {
        const ts = onlineUsers.get(toId);
        if (ts) io.to(ts).emit('webrtc:ice', { fromId: socket.userId, candidate });
    });

    socket.on('disconnect', async () => {
        for (const [uid, sid] of onlineUsers.entries()) {
            if (sid === socket.id) {
                onlineUsers.delete(uid);
                await notifyFriendsStatus(uid, false);
                break;
            }
        }
    });
});

connectDB().then(() => {
    const PORT = process.env.PORT || 3000;
    server.listen(PORT, () => console.log(`✅ Server running on port ${PORT}`));

    setTimeout(async () => {
        console.log('🔄 Pre-warming Shorts cache...');
        for (const cat of Object.keys(SHORT_CATEGORIES)) {
            try { await refreshShortsCache(cat); }
            catch (e) { console.error(`Prewarm [${cat}] failed:`, e.message); }
            await new Promise(r => setTimeout(r, 1500));
        }
        console.log('✅ Shorts cache ready');
        setInterval(async () => {
            for (const cat of Object.keys(SHORT_CATEGORIES)) {
                try { await refreshShortsCache(cat); } catch (e) {}
                await new Promise(r => setTimeout(r, 2000));
            }
        }, 6 * 60 * 60 * 1000);
    }, 5000);

}).catch(err => { console.error('❌ MongoDB error:', err); process.exit(1); });
