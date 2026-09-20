const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');
const { MongoClient, ObjectId } = require('mongodb');

const app = express();
app.use(cors());
app.use(express.json({ limit: '15mb' }));
// Static files cache 1 hour → faster loads
app.use(express.static(__dirname, { maxAge: '1h', etag: true }));

const server = http.createServer(app);
const io = new Server(server, {
    cors: { origin: '*' },
    maxHttpBufferSize: 1.5e7,
    pingInterval: 25000,
    pingTimeout: 20000,
    transports: ['websocket', 'polling']
});

const MONGODB_URI = process.env.MONGODB_URI;
if (!MONGODB_URI) { console.error('❌ MONGODB_URI not set'); process.exit(1); }

let usersCol, requestsCol, friendshipsCol, messagesCol;
const onlineUsers = new Map();

async function connectDB() {
    const client = new MongoClient(MONGODB_URI, {
        maxPoolSize: 10,
        minPoolSize: 2,
        serverSelectionTimeoutMS: 5000,
        socketTimeoutMS: 45000
    });
    await client.connect();
    const db = client.db('encrypted_chat');
    usersCol = db.collection('users');
    requestsCol = db.collection('friend_requests');
    friendshipsCol = db.collection('friendships');
    messagesCol = db.collection('messages');

    // 🚀 Indexes — এই লাইনগুলো query 5-10x দ্রুত করে
    await usersCol.createIndex({ username: 1 }, { unique: true });
    await usersCol.createIndex({ email: 1 }, { unique: true });
    await usersCol.createIndex({ username: 'text' });
    await messagesCol.createIndex({ from: 1, to: 1, timestamp: 1 });
    await messagesCol.createIndex({ to: 1, from: 1, timestamp: 1 });
    await requestsCol.createIndex({ to: 1 });
    await friendshipsCol.createIndex({ user1: 1 });
    await friendshipsCol.createIndex({ user2: 1 });

    console.log('✅ MongoDB connected with indexes');
}

/* ========== KEEP-ALIVE PING (lag fix) ========== */
app.get('/api/ping', (req, res) => res.json({ ok: true, t: Date.now() }));

/* ========== AUTH ========== */
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
            createdAt: Date.now()
        };
        const result = await usersCol.insertOne(newUser);
        res.json({ user: { id: result.insertedId.toString(), username, fullName, avatar: newUser.avatar, avatarType: newUser.avatarType } });
    } catch (err) { console.error(err); res.status(500).json({ error: 'Server error' }); }
});

app.post('/api/login', async (req, res) => {
    try {
        const { username, password } = req.body;
        if (!username || !password) return res.status(400).json({ error: 'Please enter username and password' });
        const user = await usersCol.findOne({ $or: [{ username }, { email: username }], password });
        if (!user) return res.status(400).json({ error: 'Incorrect username or password' });
        res.json({ user: { id: user._id.toString(), username: user.username, fullName: user.fullName, avatar: user.avatar, avatarType: user.avatarType || 'letter' } });
    } catch (err) { console.error(err); res.status(500).json({ error: 'Server error' }); }
});

app.post('/api/users/avatar', async (req, res) => {
    try {
        const { userId, avatar } = req.body;
        if (!avatar) return res.status(400).json({ error: 'No avatar provided' });
        await usersCol.updateOne({ _id: new ObjectId(userId) }, { $set: { avatar, avatarType: 'image' } });
        res.json({ success: true });
    } catch (err) { res.status(500).json({ error: 'Server error' }); }
});

app.get('/api/users/search', async (req, res) => {
    try {
        const { q, myId } = req.query;
        if (!q || q.length < 2) return res.json([]);
        const regex = new RegExp('^' + q.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'), 'i');
        const users = await usersCol.find({ username: regex, _id: { $ne: new ObjectId(myId) } })
            .project({ username: 1, fullName: 1, avatar: 1, avatarType: 1 })
            .limit(20).toArray();
        res.json(users.map(u => ({ id: u._id.toString(), username: u.username, fullName: u.fullName, avatar: u.avatar, avatarType: u.avatarType || 'letter' })));
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
        const senders = await usersCol.find({ _id: { $in: ids } }).project({ username: 1, fullName: 1, avatar: 1, avatarType: 1 }).toArray();
        const map = new Map(senders.map(s => [s._id.toString(), s]));
        const result = reqs.map(r => {
            const s = map.get(r.from);
            return s ? { id: r.from, username: s.username, fullName: s.fullName, avatar: s.avatar, avatarType: s.avatarType || 'letter' } : null;
        }).filter(Boolean);
        res.json(result);
    } catch (err) { res.status(500).json({ error: 'Server error' }); }
});

app.post('/api/friend-request/accept', async (req, res) => {
    try {
        const { userId, friendId } = req.body;
        await requestsCol.deleteMany({ from: friendId, to: userId });
        await friendshipsCol.insertOne({ user1: userId, user2: friendId, createdAt: Date.now() });
        const ss = onlineUsers.get(friendId);
        if (ss) io.to(ss).emit('friend_added');
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
            .project({ username: 1, fullName: 1, avatar: 1, avatarType: 1 }).toArray();
        res.json(friends.map(u => ({ id: u._id.toString(), username: u.username, fullName: u.fullName, avatar: u.avatar, avatarType: u.avatarType || 'letter' })));
    } catch (err) { res.status(500).json({ error: 'Server error' }); }
});

app.get('/api/messages', async (req, res) => {
    try {
        const { userId, friendId } = req.query;
        const msgs = await messagesCol.find({ $or: [{ from: userId, to: friendId }, { from: friendId, to: userId }] })
            .sort({ timestamp: -1 }).limit(100).toArray();
        msgs.reverse(); // পুরনো আগে
        res.json(msgs.map(m => ({ from: m.from, to: m.to, text: m.text || '', type: m.type || 'text', media: m.media || null, timestamp: m.timestamp })));
    } catch (err) { res.status(500).json({ error: 'Server error' }); }
});

/* ========== SOCKET.IO ========== */
io.on('connection', (socket) => {
    socket.on('register', (userId) => { onlineUsers.set(userId, socket.id); });

    socket.on('send_message', async ({ from, to, text, type, media }) => {
        try {
            const msg = { from, to, text: text || '', type: type || 'text', media: media || null, timestamp: Date.now() };
            await messagesCol.insertOne(msg);
            socket.emit('message_sent', msg);
            const rs = onlineUsers.get(to);
            if (rs) io.to(rs).emit('receive_message', msg);
        } catch (err) { console.error(err); }
    });

    socket.on('disconnect', () => {
        for (const [uid, sid] of onlineUsers.entries()) {
            if (sid === socket.id) { onlineUsers.delete(uid); break; }
        }
    });
});

/* ========== START ========== */
connectDB().then(() => {
    const PORT = process.env.PORT || 3000;
    server.listen(PORT, () => console.log(`✅ Server running on port ${PORT}`));
}).catch(err => { console.error('❌ MongoDB error:', err); process.exit(1); });
