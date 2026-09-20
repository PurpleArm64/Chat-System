const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');

const app = express();
app.use(cors());
app.use(express.json());
app.use(express.static(__dirname));

const server = http.createServer(app);
const io = new Server(server, { cors: { origin: '*' } });

let users = [], friendRequests = [], friendships = [], messages = [];

app.post('/api/register', (req, res) => {
    const { fullName, username, email, password } = req.body;
    if (!fullName || !username || !email || !password) return res.status(400).json({ error: 'Please fill in all fields' });
    if (username.length < 3) return res.status(400).json({ error: 'Username must be at least 3 characters' });
    if (password.length < 6) return res.status(400).json({ error: 'Password must be at least 6 characters' });
    if (users.find(u => u.username === username)) return res.status(400).json({ error: 'Username is already taken' });
    if (users.find(u => u.email === email)) return res.status(400).json({ error: 'Email is already registered' });
    const newUser = { id: Date.now().toString(), fullName, username, email, password, socketId: null, avatar: fullName[0].toUpperCase(), createdAt: Date.now() };
    users.push(newUser);
    res.json({ user: { id: newUser.id, username: newUser.username, fullName: newUser.fullName, avatar: newUser.avatar } });
});

app.post('/api/login', (req, res) => {
    const { username, password } = req.body;
    if (!username || !password) return res.status(400).json({ error: 'Please enter username and password' });
    const user = users.find(u => (u.username === username || u.email === username) && u.password === password);
    if (!user) return res.status(400).json({ error: 'Incorrect username or password' });
    res.json({ user: { id: user.id, username: user.username, fullName: user.fullName, avatar: user.avatar } });
});

app.get('/api/users/search', (req, res) => {
    const { q, myId } = req.query;
    if (!q) return res.json([]);
    const results = users.filter(u => u.username.toLowerCase().includes(q.toLowerCase()) && u.id !== myId)
        .map(u => ({ id: u.id, username: u.username, fullName: u.fullName, avatar: u.avatar }));
    res.json(results);
});

app.post('/api/friend-request', (req, res) => {
    const { from, to } = req.body;
    if (from === to) return res.status(400).json({ error: 'You cannot send a request to yourself' });
    if (friendships.find(f => (f.user1 === from && f.user2 === to) || (f.user1 === to && f.user2 === from))) return res.status(400).json({ error: 'You are already friends' });
    if (friendRequests.find(r => r.from === from && r.to === to)) return res.status(400).json({ error: 'Request already sent' });
    friendRequests.push({ from, to });
    const receiver = users.find(u => u.id === to);
    if (receiver && receiver.socketId) io.to(receiver.socketId).emit('new_friend_request', { from });
    res.json({ success: true });
});

app.get('/api/friend-requests', (req, res) => {
    const { userId } = req.query;
    const reqs = friendRequests.filter(r => r.to === userId).map(r => {
        const sender = users.find(u => u.id === r.from);
        return { id: r.from, username: sender?.username || 'Unknown', fullName: sender?.fullName || '', avatar: sender?.avatar || '?' };
    });
    res.json(reqs);
});

app.post('/api/friend-request/accept', (req, res) => {
    const { userId, friendId } = req.body;
    friendRequests = friendRequests.filter(r => !(r.from === friendId && r.to === userId));
    friendships.push({ user1: userId, user2: friendId });
    const sender = users.find(u => u.id === friendId);
    if (sender && sender.socketId) io.to(sender.socketId).emit('friend_added');
    res.json({ success: true });
});

app.get('/api/friends', (req, res) => {
    const { userId } = req.query;
    const myFriendships = friendships.filter(f => f.user1 === userId || f.user2 === userId);
    const friendIds = myFriendships.map(f => f.user1 === userId ? f.user2 : f.user1);
    const friends = users.filter(u => friendIds.includes(u.id)).map(u => ({ id: u.id, username: u.username, fullName: u.fullName, avatar: u.avatar }));
    res.json(friends);
});

app.get('/api/messages', (req, res) => {
    const { userId, friendId } = req.query;
    const chat = messages.filter(m => (m.from === userId && m.to === friendId) || (m.from === friendId && m.to === userId)).sort((a, b) => a.timestamp - b.timestamp);
    res.json(chat);
});

io.on('connection', (socket) => {
    socket.on('register', (userId) => {
        const user = users.find(u => u.id === userId);
        if (user) user.socketId = socket.id;
    });
    socket.on('send_message', ({ from, to, text }) => {
        const msg = { from, to, text, timestamp: Date.now() };
        messages.push(msg);
        socket.emit('message_sent', msg);
        const recipient = users.find(u => u.id === to);
        if (recipient && recipient.socketId) io.to(recipient.socketId).emit('receive_message', msg);
    });
    socket.on('disconnect', () => {
        const user = users.find(u => u.socketId === socket.id);
        if (user) user.socketId = null;
    });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`Server running on port ${PORT}`));
