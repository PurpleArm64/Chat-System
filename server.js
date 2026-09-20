const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');
const { MongoClient, ObjectId } = require('mongodb');

const app = express();
app.use(cors());
app.use(express.json());
app.use(express.static(__dirname));

const server = http.createServer(app);
const io = new Server(server, { cors: { origin: '*' } });

const MONGODB_URI = process.env.MONGODB_URI;
if (!MONGODB_URI) {
    console.error('❌ MONGODB_URI environment variable not set');
    process.exit(1);
}

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
        const { fullName, username, email, password } = req.body;
        if (!fullName || !username || !email || !password)
            return res.status(400).json({ error: 'Please fill in all fields' });
        if (username.length < 3)
            return res.status(400).json({ error: 'Username must be at least 3 characters' });
        if (password.length < 6)
            return res.status(400).json({ error: 'Password must be at least 6 characters' });

        const existingUsername = await usersCol.findOne({ username });
        if (existingUsername) return res.status(400).json({ error: 'Username is already taken' });

        const existingEmail = await usersCol.findOne({ email });
        if (existingEmail) return res.status(400).json({ error: 'Email is already registered' });

        const newUser = {
            fullName, username, email, password,
            avatar: fullName[0].toUpperCase(),
            createdAt: Date.now()
        };
        const result = await usersCol.insertOne(newUser);
        res.json({ user: { id: result.insertedId.toString(), username, fullName, avatar: newUser.avatar } });
    } catch (err) {
        console.error(err);
        res.status(500).json({ error: 'Server error' });
    }
});

app.post('/api/login', async (req, res) => {
    try {
        const { username, password } = req.body;
        if (!username || !password)
            return res.status(400).json({ error: 'Please enter username and password' });

        const user = await usersCol.findOne({
            $or: [{ username: username }, { email: username }],
            password: password
        });
        if (!user) return res.status(400).json({ error: 'Incorrect username or password' });

        res.json({ user: { id: user._id.toString(), username: user.username, fullName: user.fullName, avatar: user.avatar } });
    } catch (err) {
        console.error(err);
        res.status(500).json({ error: 'Server error' });
    }
});

app.get('/api/users/search', async (req, res) => {
    try {
        const { q, myId } = req.query;
        if (!q) return res.json([]);
        const regex = new RegExp(q, 'i');
        const users = await usersCol.find({
            username: regex,
            _id: { $ne: new ObjectId(myId) }
        }).limit(20).toArray();
        res.json(users.map(u => ({ id: u._id.toString(), username: u.username, fullName: u.fullName, avatar: u.avatar })));
    } catch (err) {
        console.error(err);
        res.status(500).json({ error: 'Server error' });
    }
});

app.post('/api/friend-request', async (req, res) => {
    try {
        const { from, to } = req.body;
        if (from === to) return res.status(400).json({ error: 'You cannot send a request to yourself' });

        const existingFriendship = await friendshipsCol.findOne({
            $or: [
                { user1: from, user2: to },
                { user1: to, user2: from }
            ]
        });
        if (existingFriendship) return res.status(400).json({ error: 'You are already friends' });

        const existingRequest = await requestsCol.findOne({ from, to });
        if (existingRequest) return res.status(400).json({ error: 'Request already sent' });

        await requestsCol.insertOne({ from, to, createdAt: Date.now() });

        const receiverSocket = onlineUsers.get(to);
        if (receiverSocket) io.to(receiverSocket).emit('new_friend_request', { from });

        res.json({ success: true });
    } catch (err) {
        console.error(err);
        res.status(500).json({ error: 'Server error' });
    }
});

app.get('/api/friend-requests', async (req, res) => {
    try {
        const { userId } = req.query;
        const reqs = await requestsCol.find({ to: userId }).toArray();
        const result = [];
        for (const r of reqs) {
            try {
                const sender = await usersCol.findOne({ _id: new ObjectId(r.from) });
                result.push({
                    id: r.from,
                    username: sender ? sender.username : 'Unknown',
                    fullName: sender ? sender.fullName : '',
                    avatar: sender ? sender.avatar : '?'
                });
            } catch (e) { }
        }
        res.json(result);
    } catch (err) {
        console.error(err);
        res.status(500).json({ error: 'Server error' });
    }
});

app.post('/api/friend-request/accept', async (req, res) => {
    try {
        const { userId, friendId } = req.body;
        await requestsCol.deleteMany({ from: friendId, to: userId });
        await friendshipsCol.insertOne({ user1: userId, user2: friendId, createdAt: Date.now() });

        const senderSocket = onlineUsers.get(friendId);
        if (senderSocket) io.to(senderSocket).emit('friend_added');

        res.json({ success: true });
    } catch (err) {
        console.error(err);
        res.status(500).json({ error: 'Server error' });
    }
});

app.get('/api/friends', async (req, res) => {
    try {
        const { userId } = req.query;
        const friendships = await friendshipsCol.find({
            $or: [{ user1: userId }, { user2: userId }]
        }).toArray();
        const friendIds = friendships.map(f => f.user1 === userId ? f.user2 : f.user1);
        const validIds = friendIds.filter(id => { try { new ObjectId(id); return true; } catch (e) { return false; } })
                                 .map(id => new ObjectId(id));
        const friends = await usersCol.find({ _id: { $in: validIds } }).toArray();
        res.json(friends.map(u => ({ id: u._id.toString(), username: u.username, fullName: u.fullName, avatar: u.avatar })));
    } catch (err) {
        console.error(err);
        res.status(500).json({ error: 'Server error' });
    }
});

app.get('/api/messages', async (req, res) => {
    try {
        const { userId, friendId } = req.query;
        const msgs = await messagesCol.find({
            $or: [
                { from: userId, to: friendId },
                { from: friendId, to: userId }
            ]
        }).sort({ timestamp: 1 }).toArray();
        res.json(msgs.map(m => ({ from: m.from, to: m.to, text: m.text, timestamp: m.timestamp })));
    } catch (err) {
        console.error(err);
        res.status(500).json({ error: 'Server error' });
    }
});

io.on('connection', (socket) => {
    socket.on('register', (userId) => {
        onlineUsers.set(userId, socket.id);
    });

    socket.on('send_message', async ({ from, to, text }) => {
        try {
            const msg = { from, to, text, timestamp: Date.now() };
            await messagesCol.insertOne(msg);
            socket.emit('message_sent', msg);
            const recipientSocket = onlineUsers.get(to);
            if (recipientSocket) io.to(recipientSocket).emit('receive_message', msg);
        } catch (err) {
            console.error(err);
        }
    });

    socket.on('disconnect', () => {
        for (const [userId, socketId] of onlineUsers.entries()) {
            if (socketId === socket.id) {
                onlineUsers.delete(userId);
                break;
            }
        }
    });
});

connectDB().then(() => {
    const PORT = process.env.PORT || 3000;
    server.listen(PORT, () => console.log(`✅ Server running on port ${PORT}`));
}).catch(err => {
    console.error('❌ Failed to connect to MongoDB:', err);
    process.exit(1);
});
