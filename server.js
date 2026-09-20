const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');
const { MongoClient, ObjectId } = require('mongodb');

const app = express();
app.use(cors());
app.use(express.json({ limit: '15mb' }));
app.use(express.static(__dirname));

const server = http.createServer(app);
const io = new Server(server, {
    cors: { origin: '*' },
    maxHttpBufferSize: 1.5e7
});

const MONGODB_URI = process.env.MONGODB_URI;
if (!MONGODB_URI) { console.error('❌ MONGODB_URI not set'); process.exit(1); }

let usersCol, requestsCol, friendshipsCol, messagesCol;
const onlineUsers = new Map();

async function connectDB() {
    const client = new MongoClient(MONGODB_URI);
    await client.connect();
    const db = client.db('encrypted_chat');
    usersCol = db.collection('users');
    requestsCol = db.collection('friend_requests');
    friendshipsCol = db.collection('friendships');
    messagesCol = db.collection('messages');
    await usersCol.createIndex({ username: 1 }, { unique: true });
    await usersCol.createIndex({ email: 1 }, { unique: true });
    console.log('✅ MongoDB connected');
}

app.post('/api/register', async (req, res) => {
    try {
        const { fullName, username, email, password, avatar } = req.body;
        if (!fullName || !username || !email || !password)
            return res.status(400).json({ error: 'Please fill in all fields' });
        if (username.length < 3) return res.status(400).json({ error: 'Username must be at least 3 characters' });
        if (password.length < 6) return res.status(400).json({ error: 'Password must be at least 6 characters' });
        if (await usersCol.findOne({ username })) return res.status(400).json({ error: 'Username is already taken' });
        if (await usersCol.findOne({ email })) return res.status(400).json({ error: 'Email is already registered' });

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
    } catch (err) { console.error(err); res.status(500).json({ error: 'Server error' }); }
});

app.get('/api/users/search', async (req, res) => {
    try {
        const { q, myId } = req.query;
        if (!q) return res.json([]);
        const users = await usersCol.find({ username: new RegExp(q, 'i'), _id: { $ne: new ObjectId(myId) } }).limit(20).toArray();
        res.json(users.map(u => ({ id: u._id.toString(), username: u.username, fullName: u.fullName, avatar: u.avatar, avatarType: u.avatarType || 'letter' })));
    } catch (err) { res.status(500).json({ error: 'Server error' }); }
});

app.post('/api/friend-request', async (req, res) => {
    try {
        const { from, to } = req.body;
        if (from === to) return res.status(400).json({ error: 'You cannot send a request to yourself' });
        if (await friendshipsCol.findOne({ $or: [{ user1: from, user2: to }, { user1: to, user2: from }] }))
            return res.status(400).json({ error: 'You are already friends' });
        if (await requestsCol.findOne({ from, to })) return res.status(400).json({ error: 'Request already sent' });
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
        const result = [];
        for (const r of reqs) {
            try {
                const s = await usersCol.findOne({ _id: new ObjectId(r.from) });
                if (s) result.push({ id: r.from, username: s.username, fullName: s.fullName, avatar: s.avatar, avatarType: s.avatarType || 'letter' });
            } catch (e) { }
        }
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
        const ids = friendships.map(f => f.user1 === userId ? f.user2 : f.user1)
            .filter(id => { try { new ObjectId(id); return true; } catch (e) { return false; } })
            .map(id => new ObjectId(id));
        const friends = await usersCol.find({ _id: { $in: ids } }).toArray();
        res.json(friends.map(u => ({ id: u._id.toString(), username: u.username, fullName: u.fullName, avatar: u.avatar, avatarType: u.avatarType || 'letter' })));
    } catch (err) { res.status(500).json({ error: 'Server error' }); }
});

app.get('/api/messages', async (req, res) => {
    try {
        const { userId, friendId } = req.query;
        const msgs = await messagesCol.find({ $or: [{ from: userId, to: friendId }, { from: friendId, to: userId }] })
            .sort({ timestamp: 1 }).limit(200).toArray();
        res.json(msgs.map(m => ({ from: m.from, to: m.to, text: m.text || '', type: m.type || 'text', media: m.media || null, timestamp: m.timestamp })));
    } catch (err) { res.status(500).json({ error: 'Server error' }); }
});

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

connectDB().then(() => {
    const PORT = process.env.PORT || 3000;
    server.listen(PORT, () => console.log(`✅ Server running on port ${PORT}`));
}).catch(err => { console.error('❌ MongoDB error:', err); process.exit(1); });
