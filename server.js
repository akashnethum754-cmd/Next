require('dotenv').config();
const express = require('express'), mongoose = require('mongoose'), path = require('path');
const E = process.env, app = express();
app.use(express.json());

mongoose.connect(E.MONGODB_URI).then(() => console.log('MongoDB connected')).catch(e => console.error('MongoDB error:', e.message));

const Movie = mongoose.model('Movie', new mongoose.Schema({
  sourceUrl: { type: String, unique: true }, type: { type: String, default: 'movie' },
  title: String, year: String, poster: String, overview: String, rating: String, runtime: String,
  director: String, country: String, cast: [String], genres: [String], featured: { type: Boolean, default: false },
  downloads: [{ label: String, size: String, url: String }], linksAt: Date,
  episodes: [{ season: Number, number: String, title: String, url: String }]
}, { timestamps: true }));

// ---- cinesubz API (key stays on the server) ----
const BASE = E.CINE_API_URL || 'https://apis.laksidu.site';
async function cine(p, params, tries = 2) {
  const u = new URL(BASE + p);
  Object.entries(params).forEach(([k, v]) => u.searchParams.set(k, v));
  u.searchParams.set('api_key', E.CINE_API_KEY);
  for (let i = 1; ; i++) {
    try {
      const r = await fetch(u); if (!r.ok) throw new Error('API error ' + r.status);
      const j = await r.json(); if (!j.status) throw new Error('API returned no data');
      return j;
    } catch (e) { if (i >= tries) throw e; await new Promise(r => setTimeout(r, 800)); }
  }
}
const isTv = l => (l || '').includes('/tvshows/');

// ---- chamindu API: movies (search + info with resolved download links) ----
const CH = 'https://api.chamindu.site/api/v1/movies/cinesubz';
async function ch(p, params) {
  const u = new URL(CH + p);
  Object.entries(params).forEach(([k, v]) => u.searchParams.set(k, v));
  u.searchParams.set('api_key', E.CHAMINDU_API_KEY);
  const r = await fetch(u, { signal: AbortSignal.timeout(25000) }); if (!r.ok) throw new Error('API error ' + r.status);
  const j = await r.json(); if (!j.status || !j.data) throw new Error('API returned no data');
  return j.data;
}
const linkCache = new Map(); // direct links carry tokens, so keep them fresh (10 min)
async function infodl(link) {
  const c = linkCache.get(link); if (c && Date.now() - c.t < 6e5) return c.d;
  const d = await ch('/infodl', { q: link }); linkCache.set(link, { t: Date.now(), d }); return d;
}
const toLinks = d => (d.downloads || []).filter(x => x.link && !/telegram/i.test(x.quality)).map(x => ({ label: x.quality, size: x.size, url: x.link }));
const inflight = new Map();
function freshLinks(m) { // fetch new links, save them in MongoDB (shared by simultaneous requests)
  const k = String(m._id);
  if (!inflight.has(k)) inflight.set(k, (async () => {
    linkCache.delete(m.sourceUrl);
    const downloads = toLinks(await infodl(m.sourceUrl));
    await Movie.findByIdAndUpdate(m._id, { downloads, linksAt: new Date() });
    return downloads;
  })().finally(() => inflight.delete(k)));
  return inflight.get(k);
}

async function build(link) {
  if (isTv(link)) {
    const d = (await cine('/cinesubz/tvshow', { url: link })).data;
    const episodes = (d.episodes?.list || []).map(ep => {
      const parts = String(ep.number || '1').split(/\s*-\s*/);
      return { season: parts.length > 1 ? parseInt(parts[0]) || 1 : 1, number: parts[parts.length - 1], title: ep.title || 'Episode', url: ep.url };
    }).filter(e => e.url);
    return { sourceUrl: link, type: 'tv', title: d.title, year: d.year, poster: d.poster, overview: d.description, rating: d.rating?.score ? String(d.rating.score) : '', episodes };
  }
  const d = await infodl(link);
  return {
    sourceUrl: link, type: 'movie', title: d.title, year: d.year, poster: d.image,
    overview: (d.story || '').split('\n\n')[0].slice(0, 700), rating: d.rating && d.rating !== 'N/A' ? d.rating : '',
    runtime: d.duration, director: d.director, country: [...new Set((d.country || '').split(',').map(x => x.trim()).filter(Boolean))].join(', '),
    cast: (d.cast || []).slice(0, 8).map(c => c.name), genres: (d.genres || []).filter(g => !/^[#.]/.test(g) && !/^(hdcam|cam)$/i.test(g)),
    downloads: toLinks(d), linksAt: new Date()
  };
}
const allowed = new Set(); // episode links we handed out
const wrap = fn => (req, res) => fn(req, res).catch(e => res.status(500).json({ error: e.message }));

// ---------- public ----------
const rx = s => new RegExp(String(s).replace(/[.*+?^${}()|[\]\\]/g, '\\$&'), 'i');
app.get('/api/movies', wrap(async (req, res) => {
  const q = req.query, page = Math.max(1, +q.page || 1), c = [];
  if (q.q) c.push({ title: rx(q.q) });
  if (q.type === 'movie') c.push({ type: { $ne: 'tv' } });
  if (q.type === 'tv') c.push({ type: 'tv' });
  if (q.status === 'complete') c.push({ title: /complete/i });
  if (q.status === 'incomplete') c.push({ title: { $not: /complete/i } });
  if (q.genre) c.push({ genres: String(q.genre) });
  if (q.featured) c.push({ featured: true });
  const f = c.length ? { $and: c } : {};
  const [items, total] = await Promise.all([
    Movie.find(f, '-downloads -cast -episodes').sort({ featured: -1, createdAt: -1 }).skip((page - 1) * 24).limit(24), Movie.countDocuments(f)]);
  res.json({ items, total, pages: Math.ceil(total / 24) });
}));
app.get('/api/genres', wrap(async (req, res) => res.json((await Movie.distinct('genres')).filter(Boolean).sort())));
app.get('/api/movies/:id', wrap(async (req, res) => {
  const m = await Movie.findById(req.params.id);
  m ? res.json(m) : res.status(404).json({ error: 'Not found' });
}));
// episode -> quality list
app.get('/api/episode', wrap(async (req, res) => {
  const url = req.query.url;
  if (!await Movie.exists({ 'episodes.url': url })) return res.status(403).json({ error: 'Not allowed' });
  const d = (await cine('/api/episode', { url })).data;
  const links = (d?.download_links || []).map((l, i) => ({ label: l.meta || l.type || 'Quality ' + (i + 1), url: l.url })).filter(l => l.url);
  links.forEach(l => allowed.add(l.url));
  res.json({ links });
}));
// quality page link -> final direct link
app.get('/api/resolve', wrap(async (req, res) => {
  const url = req.query.url;
  if (!allowed.has(url) && !await Movie.exists({ 'downloads.url': url })) return res.status(403).json({ error: 'Not allowed' });
  const d = (await cine('/dl/cinesubz', { url }, 3)).data;
  const ok = (d?.download || []).filter(l => l.name && l.name.toLowerCase() !== 'telegram' && l.url);
  const pick = ok.find(l => l.name === 'unknown') || ok[0];
  pick ? res.json({ url: pick.url }) : res.status(404).json({ error: 'No download link available right now' });
}));

app.get('/api/links/:id', wrap(async (req, res) => {
  const m = await Movie.findById(req.params.id, 'sourceUrl downloads linksAt');
  if (!m) return res.status(404).json({ error: 'Not found' });
  if (m.downloads.length) { // saved links open instantly; old ones refresh in the background
    const stale = Date.now() - (m.linksAt?.getTime() || 0) > 20 * 60e3;
    if (stale) freshLinks(m).catch(e => console.error('links refresh:', e.message));
    return res.json({ stale, links: m.downloads.map(x => ({ label: x.label, size: x.size, url: x.url })) });
  }
  res.json({ links: await freshLinks(m) });
}));

app.get('*', (req, res) => res.sendFile(path.join(__dirname, 'index.html')));
app.listen(E.PORT || 3000, () => console.log('SHAGGY MOVIES running'));

// ---------- Telegram admin bot ----------
// Level 1: only Telegram IDs listed in ADMIN_TG_IDS get any answer (everyone else is ignored).
// Level 2: those admins must unlock with /login PIN (30 min inactivity session, 3 wrong tries = 15 min lock).
const crypto = require('crypto');
const TG = `https://api.telegram.org/bot${E.TG_BOT_TOKEN}`;
const IDS = (E.ADMIN_TG_IDS || '').split(',').map(s => s.trim()).filter(Boolean);
const tg = (m, b = {}) => fetch(`${TG}/${m}`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(b) }).then(r => r.json()).catch(e => ({ ok: false, description: e.message }));
const SES = new Map();
const ses = id => { if (!SES.has(id)) SES.set(id, { until: 0, fails: 0, lock: 0, res: [] }); return SES.get(id); };
const same = (a, b) => { const x = Buffer.from(String(a)), y = Buffer.from(String(b)); return x.length === y.length && crypto.timingSafeEqual(x, y); };
const H = s => String(s ?? '').replace(/[&<>]/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;' }[c]));
const clean = t => String(t || '').replace(/\s*Sinhala Subtitles.*$/i, '').replace(/\s*\|.*$/, '').trim();
const say = (chat, text, kb) => tg('sendMessage', { chat_id: chat, text, parse_mode: 'HTML', disable_web_page_preview: true, reply_markup: kb ? { inline_keyboard: kb } : undefined });
const show = (c, text, kb) => c.mid ? tg('editMessageText', { chat_id: c.chat, message_id: c.mid, text, parse_mode: 'HTML', reply_markup: { inline_keyboard: kb || [] } }) : say(c.chat, text, kb);
const BACK = [{ text: '« Menu', callback_data: 'menu' }];
const MENU = [[{ text: '🔍 Search & add', callback_data: 's' }], [{ text: '🎬 Movies', callback_data: 'l:movie:0' }, { text: '📺 TV series', callback_data: 'l:tv:0' }], [{ text: '⭐ Featured', callback_data: 'l:feat:0' }, { text: '📊 Stats', callback_data: 'st' }], [{ text: '🔒 Log out', callback_data: 'out' }]];
const HOME = '🎬 <b>SHAGGY MOVIES admin</b>\nSend a movie or series name to search and add it.';
const q4 = t => t === 'tv' ? { type: 'tv' } : t === 'feat' ? { featured: true } : { type: { $ne: 'tv' } };

async function searchAll(q) {
  const seen = new Set(), out = []; let err;
  const push = arr => arr.forEach(x => {
    if (x.link && !seen.has(x.link)) { seen.add(x.link); out.push({ title: x.title, link: x.link, type: x.type === 'tvshows' || isTv(x.link) ? 'tv' : 'movie' }); }
  });
  try { push(await ch('/search', { q })); } catch (e) { err = e; console.error('chamindu search:', e.message); }
  if (!out.length) { try { push((await cine('/cinesubz/search', { query: q })).results || []); } catch (e) { err = err || e; console.error('laksidu search:', e.message); } }
  if (!out.length && err) throw err;
  return out;
}
async function doSearch(chat, s, q) {
  tg('sendChatAction', { chat_id: chat, action: 'typing' });
  const w = await say(chat, `🔍 Searching <b>${H(q)}</b>…`), c = { chat, mid: w.result?.message_id };
  try {
    const r = (await searchAll(q)).slice(0, 10); s.res = r; s.q = q;
    if (!r.length) return show(c, 'No results. Try another spelling, for example <code>salaar</code>.', [BACK]);
    const have = new Set((await Movie.find({ sourceUrl: { $in: r.map(x => x.link) } }, 'sourceUrl')).map(x => x.sourceUrl));
    s.kb = [...r.map((x, i) => [{ text: `${have.has(x.link) ? '✓ ' : ''}${x.type === 'tv' ? '📺' : '🎬'} ${clean(x.title).slice(0, 46)}`, callback_data: 'a:' + i }]), BACK];
    show(c, `Results for <b>${H(q)}</b>. Tap one to add it:`, s.kb);
  } catch (e) { show(c, '⚠️ ' + H(e.message), [BACK]); }
}
async function itemView(c, id) {
  const m = await Movie.findById(id); if (!m) return show(c, 'Not found.', [BACK]);
  show(c, `<b>${H(clean(m.title))}</b>\n${H(m.year || '')} · ${m.type === 'tv' ? 'TV series, ' + m.episodes.length + ' episodes' : 'Movie'}${m.rating ? ' · ★ ' + H(m.rating) : ''}${m.featured ? '\n⭐ Featured' : ''}`,
    [[{ text: m.featured ? '☆ Unfeature' : '⭐ Feature', callback_data: 'f:' + id }, { text: '🔄 Refresh', callback_data: 'r:' + id }], [{ text: '🗑 Delete', callback_data: 'd:' + id }], [{ text: '« Back', callback_data: `l:${m.type === 'tv' ? 'tv' : 'movie'}:0` }]]);
}
async function listView(c, t, p) {
  const f = q4(t), [items, n] = await Promise.all([Movie.find(f, 'title featured').sort({ createdAt: -1 }).skip(p * 8).limit(8), Movie.countDocuments(f)]);
  const nav = [];
  if (p > 0) nav.push({ text: '‹ Prev', callback_data: `l:${t}:${p - 1}` });
  if ((p + 1) * 8 < n) nav.push({ text: 'Next ›', callback_data: `l:${t}:${p + 1}` });
  show(c, `<b>${{ movie: '🎬 Movies', tv: '📺 TV series', feat: '⭐ Featured' }[t] || 'List'}</b> (${n})${n ? '' : '\nNothing here yet.'}`,
    [...items.map(m => [{ text: (m.featured ? '⭐ ' : '') + clean(m.title).slice(0, 48), callback_data: 'm:' + m._id }]), ...(nav.length ? [nav] : []), BACK]);
}
async function act(c, s, d) {
  const [k, a, b] = d.split(':');
  if (k === 'menu') return show(c, HOME, MENU);
  if (k === 's') return show(c, '🔍 Send the movie or series name.', [BACK]);
  if (k === 'st') {
    const [m, t, f] = await Promise.all([Movie.countDocuments({ type: { $ne: 'tv' } }), Movie.countDocuments({ type: 'tv' }), Movie.countDocuments({ featured: true })]);
    return show(c, `📊 <b>Stats</b>\n🎬 Movies: ${m}\n📺 TV series: ${t}\n⭐ Featured: ${f}`, [BACK]);
  }
  if (k === 'out') { s.until = 0; return show(c, '🔒 Logged out.'); }
  if (k === 'l') return listView(c, a, +b || 0);
  if (k === 'm') return itemView(c, a);
  if (k === 'f') { const m = await Movie.findById(a); await Movie.findByIdAndUpdate(a, { featured: !m.featured }); return itemView(c, a); }
  if (k === 'r') { await show(c, '⏳ Refreshing…'); const m = await Movie.findById(a); linkCache.delete(m.sourceUrl); await Movie.findByIdAndUpdate(a, await build(m.sourceUrl)); return itemView(c, a); }
  if (k === 'rs') return show(c, `Results for <b>${H(s.q || '')}</b>. Tap one to add it:`, s.kb || [BACK]);
  if (k === 'd') return show(c, '⚠️ Delete this title?', [[{ text: '✅ Yes, delete', callback_data: 'D:' + a }, { text: 'Cancel', callback_data: 'm:' + a }]]);
  if (k === 'D') { await Movie.findByIdAndDelete(a); return show(c, '🗑 Deleted.', [BACK]); }
  if (k === 'a') {
    const x = s.res[+a]; if (!x) return show(c, 'Search again.', [BACK]);
    await show(c, `⏳ Adding <b>${H(clean(x.title))}</b>… a few seconds`);
    const doc = await build(x.link);
    await Movie.findOneAndUpdate({ sourceUrl: doc.sourceUrl }, doc, { upsert: true, new: true });
    const row = s.kb?.[+a]?.[0]; if (row) row.text = '✓ ' + row.text.replace(/^✓ /, '');
    return show(c, `✅ Added: <b>${H(clean(doc.title))}</b>`, [[{ text: '📋 Back to results', callback_data: 'rs' }], [{ text: '🔍 New search', callback_data: 's' }, ...BACK]]);
  }
}
async function handle(u) {
  const cb = u.callback_query, msg = u.message, from = (cb || msg)?.from; if (!from) return;
  const chat = cb ? cb.message.chat.id : msg.chat.id, text = (msg?.text || '').trim();
  if (!cb && /^\/id\b/.test(text)) return say(chat, `Your Telegram ID: <code>${from.id}</code>`);
  if (!IDS.includes(String(from.id))) { if (cb) tg('answerCallbackQuery', { callback_query_id: cb.id }); return; } // level 1
  if ((cb ? cb.message.chat.type : msg.chat.type) !== 'private') return;
  const s = ses(from.id), now = Date.now();
  if (!cb && /^\/login\b/.test(text)) { // level 2
    tg('deleteMessage', { chat_id: chat, message_id: msg.message_id });
    if (now < s.lock) return say(chat, '⛔ Too many wrong tries. Try again later.');
    if (E.ADMIN_PIN && same(text.replace(/^\/login\s*/, ''), E.ADMIN_PIN)) { s.until = now + 30 * 60e3; s.fails = 0; return say(chat, '✅ Logged in. Session ends after 30 minutes of inactivity.\n\n' + HOME, MENU); }
    if (++s.fails >= 3) { s.lock = now + 15 * 60e3; s.fails = 0; }
    return say(chat, '❌ Wrong PIN.');
  }
  if (now > s.until) {
    if (cb) return tg('answerCallbackQuery', { callback_query_id: cb.id, text: 'Session ended. Send /login PIN', show_alert: true });
    return say(chat, '🔒 Send <code>/login YOUR_PIN</code> to unlock.');
  }
  s.until = now + 30 * 60e3;
  if (cb) { tg('answerCallbackQuery', { callback_query_id: cb.id }); return act({ chat, mid: cb.message.message_id }, s, cb.data).catch(e => say(chat, '⚠️ ' + H(e.message))); }
  if (/^\/(start|menu)\b/.test(text)) return act({ chat }, s, 'menu');
  if (text && !text.startsWith('/')) return doSearch(chat, s, text).catch(e => say(chat, '⚠️ ' + H(e.message)));
}
async function poll() {
  if (!E.TG_BOT_TOKEN) return console.log('Telegram bot off (no TG_BOT_TOKEN)');
  await tg('deleteWebhook'); console.log('Telegram bot running'); let off = 0;
  for (;;) {
    const r = await tg('getUpdates', { offset: off, timeout: 30, allowed_updates: ['message', 'callback_query'] });
    if (!r.ok) { await new Promise(x => setTimeout(x, 5000)); continue; }
    for (const u of r.result) { off = u.update_id + 1; handle(u).catch(e => console.error('bot:', e.message)); }
  }
}
poll();
