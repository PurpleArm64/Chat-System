const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');
const { MongoClient, ObjectId } = require('mongodb');
const cloudinary = require('cloudinary').v2;

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
        console.log('✅ Cloudinary configured:', process.env.CLOUDINARY_CLOUD_NAME);
    } catch (e) { console.error('Cloudinary init error:', e.message); }
} else { console.warn('⚠️ Cloudinary env vars not set'); }

let usersCol, requestsCol, friendshipsCol, messagesCol, storiesCol;
const onlineUsers = new Map();

async function connectDB() {
    // FIXED: MongoClient spelling
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

    await usersCol.createIndex({ username: 1 }, { unique: true });
    await usersCol.createIndex({ email: 1 }, { unique: true });
    await messagesCol.createIndex({ from: 1, to: 1, timestamp: 1 });
    await messagesCol.createIndex({ to: 1, from: 1, timestamp: 1 });
    await requestsCol.createIndex({ to: 1 });
    await friendshipsCol.createIndex({ user1: 1 });
    await friendshipsCol.createIndex({ user2: 1 });
    await storiesCol.createIndex({ userId: 1, createdAt: 1 });
    console.log('✅ MongoDB connected with indexes');
}

app.get('/api/ping', (req, res) => res.json({ ok: true, t: Date.now(), storage: cloudinaryReady }));

app.get('/api/config', (req, res) => res.json({ googleClientId: GOOGLE_CLIENT_ID }));

/* ===== UPLOAD ===== */
app.post('/api/upload', async (req, res) => {
    try {
        if (!cloudinaryReady) return res.status(500).json({ error: 'Storage not configured' });
        const { base64, fileName } = req.body;
        if (!base64) return res.status(400).json({ error: 'No file data' });
        const match = base64.match(/^data:([^;]+);base64,(.+)$/);
        if (!match) return res.status(400).json({ error: 'Invalid base64 format' });
        const mime = match[1] || 'application/octet-stream';
        const buf = Buffer.from(match[2], 'base64');
        if (buf.length > 15 * 1024 * 1024) return res.status(400).json({ error: 'File too large (max 15MB)' });
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
        res.json({ url: result.secure_url, publicId: result.public_id });
    } catch (err) {
        console.error('Upload error:', err);
        res.status(500).json({ error: 'Upload failed: ' + (err.message || 'unknown') });
    }
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
    } catch (err) { console.error(err); res.status(500).json({ error: 'Server error' }); }
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
    } catch (err) { console.error(err); res.status(500).json({ error: 'Server error' }); }
});

/* ===== GOOGLE SIGN-IN ===== */
app.post('/api/auth/google', async (req, res) => {
    try {
        const { credential } = req.body;
        if (!credential) return res.status(400).json({ error: 'No credential provided' });

        const verifyRes = await fetch('https://oauth2.googleapis.com/tokeninfo?id_token=' + encodeURIComponent(credential));
        if (!verifyRes.ok) return res.status(400).json({ error: 'Invalid Google token' });
        const info = await verifyRes.json();

        if (GOOGLE_CLIENT_ID && !GOOGLE_CLIENT_ID.startsWith('YOUR_') && info.aud !== GOOGLE_CLIENT_ID) {
            return res.status(400).json({ error: 'Token audience mismatch' });
        }

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
                username = base + '_' + counter;
                counter++;
                if (counter > 999) { username = base + '_' + Date.now().toString().slice(-6); break; }
            }
            const newUser = {
                fullName: name, username, email,
                password: 'google-oauth:' + googleId,
                avatar: picture || (name ? name[0].toUpperCase() : 'U'),
                avatarType: picture ? 'image' : 'letter',
                verified: false, googleId,
                lastSeen: Date.now(), createdAt: Date.now()
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
    } catch (err) { console.error('Google auth error:', err); res.status(500).json({ error: 'Google sign-in failed' }); }
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
    } catch (err) { console.error(err); res.status(500).json({ error: 'Server error' }); }
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
    } catch (err) { console.error(err); res.status(500).json({ error: 'Server error' }); }
});

app.post('/api/users/password', async (req, res) => {
    try {
        const { userId, oldPassword, newPassword } = req.body;
        if (!oldPassword || !newPassword) return res.status(400).json({ error: 'Please fill in all fields' });
        if (newPassword.length < 6) return res.status(400).json({ error: 'New password must be at least 6 characters' });
        if (oldPassword === newPassword) return res.status(400).json({ error: 'New password must be different' });
        const user = await usersCol.findOne({ _id: new ObjectId(userId) });
        if (!user) return res.status(404).json({ error: 'User not found' });
        if (user.password && user.password.startsWith('google-oauth:')) {
            return res.status(400).json({ error: 'This account uses Google Sign-In. Password cannot be changed.' });
        }
        if (user.password !== oldPassword) return res.status(400).json({ error: 'Current password is incorrect' });
        await usersCol.updateOne({ _id: new ObjectId(userId) }, { $set: { password: newPassword } });
        res.json({ success: true });
    } catch (err) { console.error(err); res.status(500).json({ error: 'Server error' }); }
});

app.get('/api/users/profile/:username', async (req, res) => {
    try {
        const { username } = req.params;
        const user = await usersCol.findOne({ username });
        if (!user) return res.status(404).json({ error: 'User not found' });
        res.json(formatUser(user));
    } catch (err) { console.error(err); res.status(500).json({ error: 'Server error' }); }
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
    } catch (err) { console.error(err); res.status(500).json({ error: 'Server error' }); }
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

/* ===== REMOVE FRIEND ===== */
app.delete('/api/friends/:friendId', async (req, res) => {
    try {
        const { friendId } = req.params;
        const { userId, deleteMessages } = req.query;
        if (!userId || !friendId) return res.status(400).json({ error: 'Missing params' });
        if (userId === friendId) return res.status(400).json({ error: 'Invalid' });
        await friendshipsCol.deleteMany({
            $or: [{ user1: userId, user2: friendId }, { user1: friendId, user2: userId }]
        });
        await requestsCol.deleteMany({
            $or: [{ from: userId, to: friendId }, { from: friendId, to: userId }]
        });
        if (deleteMessages === 'true') {
            await messagesCol.deleteMany({
                $or: [{ from: userId, to: friendId }, { from: friendId, to: userId }]
            });
        }
        const otherSid = onlineUsers.get(friendId);
        if (otherSid) io.to(otherSid).emit('friend_removed', { by: userId });
        const selfSid = onlineUsers.get(userId);
        if (selfSid) io.to(selfSid).emit('friend_removed', { by: userId });
        res.json({ success: true });
    } catch (err) { console.error(err); res.status(500).json({ error: 'Server error' }); }
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
            let friendObjId;
            try { friendObjId = new ObjectId(fid); } catch (e) { continue; }
            const friend = await usersCol.findOne({ _id: friendObjId });
            if (!friend) continue;
            const lastMsgs = await messagesCol.find({
                $or: [{ from: userId, to: fid }, { from: fid, to: userId }]
            }).sort({ timestamp: -1 }).limit(1).toArray();
            const unreadCount = await messagesCol.countDocuments({ from: fid, to: userId, read: false });
            results.push({
                friend: formatUser(friend),
                lastMessage: lastMsgs[0] ? {
                    text: lastMsgs[0].text || '',
                    type: lastMsgs[0].type || 'text',
                    timestamp: lastMsgs[0].timestamp,
                    fromMe: lastMsgs[0].from === userId
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
    } catch (err) { console.error(err); res.status(500).json({ error: 'Server error' }); }
});

app.get('/api/messages', async (req, res) => {
    try {
        const { userId, friendId } = req.query;
        const msgs = await messagesCol.find({ $or: [{ from: userId, to: friendId }, { from: friendId, to: userId }] })
            .sort({ timestamp: -1 }).limit(100).toArray();
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
    } catch (err) { console.error(err); res.status(500).json({ error: 'Server error' }); }
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
    } catch (err) { console.error(err); res.status(500).json({ error: 'Server error' }); }
});

/* ===== STORIES ===== */
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
    } catch (err) { console.error(err); res.status(500).json({ error: 'Server error' }); }
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
    } catch (err) { console.error(err); res.status(500).json({ error: 'Server error' }); }
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
    } catch (err) { console.error(err); res.status(500).json({ error: 'Server error' }); }
});

/* ===== SOCKET.IO ===== */
io.on('connection', (socket) => {
    socket.on('register', (userId) => {
        onlineUsers.set(userId, socket.id);
        socket.userId = userId;
        notifyFriendsStatus(userId, true);
    });

    socket.on('send_message', async ({ from, to, text, type, media }) => {
        try {
            const toOnline = onlineUsers.has(to);
            const msg = { 
                from, to, text: text || '', type: type || 'text', media: media || null, 
                timestamp: Date.now(), delivered: toOnline, read: false,
                isEdited: false, reactions: {}
            };
            const result = await messagesCol.insertOne(msg);
            socket.emit('message_sent', { ...msg, _id: result.insertedId.toString() });
            const rs = onlineUsers.get(to);
            if (rs) io.to(rs).emit('receive_message', { ...msg, _id: result.insertedId.toString() });
        } catch (err) { console.error(err); }
    });

    // NEW: Edit Message
    socket.on('edit_message', async ({ messageId, from, to, newText }) => {
        try {
            await messagesCol.updateOne(
                { _id: new ObjectId(messageId) },
                { $set: { text: newText, isEdited: true, editedAt: Date.now() } }
            );
            const payload = { messageId, newText, isEdited: true };
            socket.emit('message_edited', payload);
            const rs = onlineUsers.get(to);
            if (rs) io.to(rs).emit('message_edited', payload);
            const ss = onlineUsers.get(from);
            if (ss) io.to(ss).emit('message_edited', payload);
        } catch (err) { console.error(err); }
    });

    // NEW: React to Message
    socket.on('react_message', async ({ messageId, userId, reaction, from, to }) => {
        try {
            const msg = await messagesCol.findOne({ _id: new ObjectId(messageId) });
            if (!msg) return;
            
            let reactions = msg.reactions || {};
            let userPreviousReactions = Object.keys(reactions).filter(k => reactions[k].includes(userId));
            
            // If user already reacted with this exact reaction, remove it (toggle off)
            if (userPreviousReactions.includes(reaction)) {
                await messagesCol.updateOne({_id: new ObjectId(messageId)}, { $pull: { [`reactions.${reaction}`]: userId } });
            } else {
                // Remove previous reaction from this user
                for (let r of userPreviousReactions) {
                    await messagesCol.updateOne({_id: new ObjectId(messageId)}, { $pull: { [`reactions.${r}`]: userId } });
                }
                // Add new reaction
                await messagesCol.updateOne({_id: new ObjectId(messageId)}, { $addToSet: { [`reactions.${reaction}`]: userId } });
            }
            
            // Clean up empty arrays
            const updatedMsg = await messagesCol.findOne({_id: new ObjectId(messageId)});
            let cleanReactions = {};
            if(updatedMsg.reactions){
               Object.keys(updatedMsg.reactions).forEach(k => {
                   if(updatedMsg.reactions[k].length > 0) cleanReactions[k] = updatedMsg.reactions[k];
               });
            }
            await messagesCol.updateOne({_id: new ObjectId(messageId)}, { $set: { reactions: cleanReactions } });

            const payload = { messageId, reactions: cleanReactions };
            socket.emit('message_reacted', payload);
            const rs = onlineUsers.get(to);
            if (rs) io.to(rs).emit('message_reacted', payload);
            const ss = onlineUsers.get(from);
            if (ss) io.to(ss).emit('message_reacted', payload);
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
}).catch(err => { console.error('❌ MongoDB error:', err); process.exit(1); });
