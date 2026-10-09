require('dotenv').config();
const express = require('express'), mongoose = require('mongoose'), path = require('path');
const E = process.env, app = express();
const ERRS = []; // recent server errors (the AI site doctor reads these)
{ const log = console.error; console.error = (...a) => { try { ERRS.push({ t: Date.now(), m: a.map(x => x?.message || String(x)).join(' ').slice(0, 200) }); if (ERRS.length > 80) ERRS.shift(); } catch {} log(...a); }; }
process.on('unhandledRejection', e => console.error('unhandled:', e?.message || e));
process.on('uncaughtException', e => console.error('uncaught:', e?.message || e));
app.set('trust proxy', 1);
app.use(express.json());

// ---------- databases: one or more MongoDB clusters ----------
// MONGODB_URI = main database (also keeps settings and cache). Add MONGODB_URI_2, MONGODB_URI_3 ... as the first one fills up:
// new titles and posters go to the first database that still has room, and the site reads from all of them.
const LIMIT = +E.DB_LIMIT_MB || 512, KEEP = 0.93; // a database is treated as full at 93% so it keeps room for settings
const MovieSchema = new mongoose.Schema({
  sourceUrl: { type: String, unique: true }, type: { type: String, default: 'movie' },
  title: String, year: String, poster: String, overview: String, rating: String, runtime: String,
  director: String, country: String, cast: [String], genres: [String], featured: { type: Boolean, default: false },
  downloads: [{ label: String, size: String, url: String }], pages: [{ label: String, url: String }], linksAt: Date, syncAt: Date,
  episodes: [{ season: Number, number: String, title: String, url: String }]
}, { timestamps: true });
const PosterSchema = new mongoose.Schema({ movie: { type: mongoose.Schema.Types.ObjectId, unique: true }, type: String, data: Buffer });
const URIS = [E.MONGODB_URI, ...(E.MONGODB_URIS || '').split(','), ...Array.from({ length: 8 }, (_, i) => E['MONGODB_URI_' + (i + 2)])].map(x => (x || '').trim()).filter((x, i, a) => x && a.indexOf(x) === i);
if (!URIS.length) { console.error('MONGODB_URI is missing'); URIS.push('mongodb://127.0.0.1:27017/shaggy'); }
const fixUri = u => u.replace(/:<([^>@]*)>@/, ':$1@').replace(/(mongodb(?:\+srv)?:\/\/[^/?]+)\/?(\?|$)/, '$1/shaggy$2'); // removes < > around the password, adds a database name if missing
const OPTS = { serverSelectionTimeoutMS: 8000, connectTimeoutMS: 8000 };
const SH = URIS.map((raw, i) => {
  const uri = fixUri(raw), conn = i === 0 ? mongoose.connection : mongoose.createConnection();
  const sh = { i, name: 'DB' + (i + 1), conn, Movie: conn.model('Movie', MovieSchema), Poster: conn.model('Poster', PosterSchema), full: false, mb: 0, err: '' };
  (async () => { // keeps trying every 30 s until connected
    for (;;) {
      try { await (i === 0 ? mongoose.connect(uri, OPTS) : conn.openUri(uri, OPTS)); sh.err = ''; console.log(`MongoDB ${i + 1} connected`); return; }
      catch (e) { sh.err = e.message; console.error(`MongoDB ${i + 1} error:`, e.message); await new Promise(r => setTimeout(r, 30000)); }
    }
  })();
  return sh;
});
const live = () => SH.filter(x => x.conn.readyState === 1); // databases that are connected right now
const QUOTA = e => /quota/i.test(String(e?.message || e));
async function dbSize() {
  const out = [];
  for (const sh of SH) {
    if (sh.conn.readyState !== 1) { out.push({ name: sh.name, mb: null, down: true }); continue; }
    try { const st = await sh.conn.db.stats(); sh.mb = Math.round((st.storageSize + st.indexSize) / 1048576); sh.full = sh.mb > LIMIT * KEEP; out.push({ name: sh.name, mb: sh.mb }); }
    catch { out.push({ name: sh.name, mb: null }); }
  }
  return out;
}
const dbLine = a => a.map(x => x.down ? `${x.name} ❌ offline` : `${x.name} ${x.mb ?? '?'}/${LIMIT} MB`).join(' · ');
// Serverless hosts (Vercel) start cold: wait until the databases are connected before answering, and say so clearly if they are not.
const waitDb = () => new Promise(done => { const t0 = Date.now(), chk = () => { const ms = Date.now() - t0; if ((SH[0].conn.readyState === 1 && (SH.every(x => x.conn.readyState === 1) || ms > 3500)) || ms > 9000) return done(); setTimeout(chk, 100); }; chk(); });
app.use(async (req, res, next) => {
  if (!/^\/(api|img|dl|m|sitemap)/.test(req.path)) return next();
  if (SH[0].conn.readyState !== 1 || !SH.every(x => x.conn.readyState === 1)) await waitDb();
  if (!live().length) return res.status(503).json({ error: 'Database is not connected. Check the MONGODB_URI setting on this host.' });
  next();
});
const allFull = () => !live().some(x => !x.full);
const dropPosters = async () => { let n = 0; for (const sh of live()) { try { await sh.conn.collection('posters').drop(); n++; } catch {} } return n > 0; };

// Movie = one model over every database (same calls as a normal model, results are merged)
const val = v => v instanceof Date ? v.getTime() : typeof v === 'boolean' ? +v : v ?? 0;
const cmpBy = sort => (a, b) => { for (const [k, d] of Object.entries(sort)) { const x = val(a[k]), y = val(b[k]); if (x < y) return -d; if (x > y) return d; } return 0; };
class Q {
  constructor(f, proj) { this.f = f || {}; this.proj = proj; this._skip = 0; }
  sort(s) { this._sort = s; return this; } skip(n) { this._skip = n; return this; } limit(n) { this._limit = n; return this; } lean() { this._lean = true; return this; }
  async run() {
    const { f, _sort: sort, _skip: skip, _limit: limit } = this; let proj = this.proj;
    if (sort && typeof proj === 'string' && proj && !/(^|\s)-/.test(proj)) proj += ' ' + Object.keys(sort).join(' ');
    const rows = (await Promise.all(live().map(sh => {
      let q = sh.Movie.find(f, proj); if (sort) q = q.sort(sort); if (limit) q = q.limit(skip + limit); if (this._lean) q = q.lean(); return q;
    }))).flat();
    if (sort) rows.sort(cmpBy(sort));
    return limit ? rows.slice(skip, skip + limit) : skip ? rows.slice(skip) : rows;
  }
  then(a, b) { return this.run().then(a, b); }
  catch(b) { return this.run().catch(b); }
}
const first = async list => { for (const r of await Promise.all(list)) if (r) return r; return null; };
const Movie = {
  find: (f, proj) => new Q(f, proj),
  findById: (id, proj) => /^[0-9a-f]{24}$/i.test(String(id)) ? first(live().map(s => s.Movie.findById(id, proj))) : Promise.resolve(null),
  findOne: (f, proj) => first(live().map(s => s.Movie.findOne(f, proj))),
  exists: f => first(live().map(s => s.Movie.exists(f))),
  async countDocuments(f) { return (await Promise.all(live().map(s => s.Movie.countDocuments(f)))).reduce((a, b) => a + b, 0); },
  async distinct(k, f) { return [...new Set((await Promise.all(live().map(s => s.Movie.distinct(k, f)))).flat())]; },
  async findByIdAndUpdate(id, u, o) { for (const s of live()) { const r = await s.Movie.findByIdAndUpdate(id, u, o); if (r) return r; } return null; },
  async findByIdAndDelete(id) { for (const s of live()) { const r = await s.Movie.findByIdAndDelete(id); if (r) return r; } return null; },
  async deleteMany(f) { let n = 0; for (const s of live()) n += (await s.Movie.deleteMany(f)).deletedCount || 0; return { deletedCount: n }; },
  async updateMany(f, u) { let n = 0; for (const s of live()) n += (await s.Movie.updateMany(f, u)).modifiedCount || 0; return { modifiedCount: n }; },
};

// ---- posters: saved in MongoDB as small WebP copies (about 20 KB each) and shown from our own site ----
const imgType = b => !b || b.length < 200 ? null : (b[0] === 0xFF && b[1] === 0xD8) ? 'image/jpeg' : (b[0] === 0x89 && b[1] === 0x50) ? 'image/png'
  : (b.slice(0, 4).toString() === 'RIFF' && b.slice(8, 12).toString() === 'WEBP') ? 'image/webp' : b.slice(0, 3).toString() === 'GIF' ? 'image/gif' : null;
const IMGC = new Map(); // recently shown posters, kept in memory
let sharp = null; try { sharp = require('sharp'); sharp.cache({ memory: 32, files: 0, items: 40 }); sharp.concurrency(1); } catch {}
async function shrink(buf, w = 480, q = 72) {
  if (sharp) { try { const out = await sharp(buf).resize({ width: w, withoutEnlargement: true }).webp({ quality: q }).toBuffer(); if (out.length < buf.length) return out; } catch {} }
  return buf;
}
async function readPoster(id) {
  for (const r of await Promise.all(live().map(s => s.Poster.findOne({ movie: id }).catch(() => null)))) { const t = r && imgType(r.data); if (t) return { data: Buffer.from(r.data), type: t }; }
  return null;
}
async function storePoster(movieId, data, type) {
  if (!sharp && data.length > 90000) return false; // without the shrinker only small images are saved
  for (const sh of live().filter(s => !s.full)) {
    try { await sh.Poster.findOneAndUpdate({ movie: movieId }, { type, data }, { upsert: true }); return true; }
    catch (e) { if (QUOTA(e)) sh.full = true; else return false; }
  }
  return false;
}
async function fetchPoster(url) {
  const r = await fetch(url, { signal: AbortSignal.timeout(15000), headers: { 'User-Agent': 'Mozilla/5.0', Referer: new URL(url).origin + '/' } });
  if (!r.ok) return null;
  const raw = Buffer.from(await r.arrayBuffer()); if (!imgType(raw) || raw.length > 8e6) return null;
  const data = await shrink(raw); return { data, type: imgType(data) };
}
async function warmOne(m) { if (!m?.poster || await readPoster(m._id)) return false; const p = await fetchPoster(m.poster); return p ? storePoster(m._id, p.data, p.type) : false; }
async function warmPosters() {
  const have = new Set(); for (const sh of live()) for (const x of await sh.Poster.find({}, 'movie').catch(() => [])) have.add(String(x.movie));
  const todo = (await Movie.find({}, 'poster')).filter(m => m.poster && !have.has(String(m._id)));
  let n = 0;
  for (let i = 0; i < todo.length && !allFull(); i += 3) n += (await Promise.allSettled(todo.slice(i, i + 3).map(warmOne))).filter(r => r.value).length;
  return n;
}
async function delPosters(ids) { for (const sh of live()) await sh.Poster.deleteMany({ movie: { $in: ids } }).catch(() => {}); }

// short-lived cache in MongoDB (resolved links, episode lists, cooldowns) so the paid APIs are hit as little as possible
const Cache = mongoose.model('Cache', new mongoose.Schema({ k: { type: String, unique: true }, v: String, at: { type: Date, default: Date.now, expires: 10800 } }));
const Fail = mongoose.model('Fail', new mongoose.Schema({ link: { type: String, unique: true }, title: String, reason: String, at: { type: Date, default: Date.now } }));
const NoticeImg = mongoose.model('NoticeImg', new mongoose.Schema({ k: { type: String, unique: true }, type: String, data: Buffer }));
const Setting = mongoose.model('Setting', new mongoose.Schema({ k: { type: String, unique: true }, v: String }));
let CFG = { ads: true, t: 0 }, BL = { a: [], t: 0 };
async function adsOn() {
  if (Date.now() - CFG.t > 3e4) { const x = await Setting.findOne({ k: 'ads' }).catch(() => null); CFG = { ads: x ? x.v !== 'off' : true, t: Date.now() }; }
  return CFG.ads;
}
async function blocked() { // download servers hidden from the site (label or link contains the word)
  if (Date.now() - BL.t > 3e4) { const x = await Setting.findOne({ k: 'block' }).catch(() => null); let a = ['1ditfile']; try { if (x) a = JSON.parse(x.v); } catch {} BL = { a, t: Date.now() }; }
  return BL.a;
}
const isBlocked = (x, a) => a.some(w => `${x.label || ''} ${x.url || ''}`.toLowerCase().includes(w));
async function purgeBlocked(words) {
  let n = 0;
  for (const w of words) { const r = new RegExp(w.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'), 'i');
    for (const f of ['label', 'url']) n += (await Movie.updateMany({ ['downloads.' + f]: r }, { $pull: { downloads: { [f]: r } } })).modifiedCount || 0; }
  return n;
}
const cget = async k => { const c = await Cache.findOne({ k }); return c ? JSON.parse(c.v) : null; };
const cset = (k, v) => Cache.findOneAndUpdate({ k }, { v: JSON.stringify(v), at: new Date() }, { upsert: true }).catch(() => {});

// ---- cinesubz API: laksidu = main API (key stays on the server) ----
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

// ---- chamindu API: FALLBACK only (costs more) ----
const CH = E.CHAMINDU_BASE || 'https://api.chamindu.site/api/v1/movies/cinesubz'; // paid fallback only
async function ch(p, params, base = CH) {
  const u = new URL(base + p);
  Object.entries(params).forEach(([k, v]) => u.searchParams.set(k, v));
  u.searchParams.set('api_key', E.CHAMINDU_API_KEY);
  const r = await fetch(u, { signal: AbortSignal.timeout(25000) }); if (!r.ok) throw new Error('API error ' + r.status);
  const j = await r.json(); if (!j.status || !j.data) throw new Error('API returned no data');
  return j.data;
}
const linkCache = new Map(); // fallback answers kept 10 min in memory
async function infodl(link) {
  const c = linkCache.get(link); if (c && Date.now() - c.t < 6e5) return c.d;
  const d = await ch('/infodl', { q: link }); linkCache.set(link, { t: Date.now(), d }); if (linkCache.size > 60) linkCache.delete(linkCache.keys().next().value); return d;
}
// cinesubz.* pages and Telegram links are not real downloads, so they are never shown
const bad = u => { try { return /(^|\.)(cinesubz\.[a-z]+|t\.me|telegram\.me)$/i.test(new URL(u).hostname); } catch { return true; } };
const toLinks = d => (d.downloads || []).filter(x => x.link && !/telegram/i.test(x.quality) && !bad(x.link)).map(x => ({ label: x.quality, size: x.size, url: x.link }));
const toPages = d => (d.downloads || []).filter(x => x && x.quality && x.url).map(x => ({ label: x.quality, url: x.url }));
const yearOf = (y, t) => /^(19|20)\d{2}$/.test(String(y || '').trim()) ? String(y).trim() : (String(t || '').match(/\((\d{4})\)/) || [])[1] || '';
const uniq = s => [...new Set(String(s || '').split(',').map(x => x.trim()).filter(Boolean))];
const lakDetails = async link => (await cine('/cinesubz/details', { url: link })).data;

// Never overwrite saved data with empty values: only non-empty fields are written
const nonEmpty = v => !(v == null || v === '' || (Array.isArray(v) && !v.length));
const save = async doc => {
  const set = {}; for (const [k, v] of Object.entries(doc)) if (nonEmpty(v)) set[k] = v;
  for (const sh of live()) { // already saved in any database? update it there
    let old; try { old = await sh.Movie.findOneAndUpdate({ sourceUrl: doc.sourceUrl }, { $set: set }, { new: true }); }
    catch (e) { if (QUOTA(e)) old = await sh.Movie.findOne({ sourceUrl: doc.sourceUrl }); else throw e; }
    if (old) return old;
  }
  for (const sh of live().filter(s => !s.full)) { // new title: first database with room
    try { return await sh.Movie.create(set); } catch (e) { if (QUOTA(e)) { sh.full = true; continue; } throw e; }
  }
  throw new Error('all databases are over their space quota - add another MongoDB link (MONGODB_URI_2)');
};
const inflight = new Map();
function freshLinks(m) { // laksidu first; chamindu only if laksidu fails. Result is saved in MongoDB.
  const k = String(m._id);
  if (!inflight.has(k)) inflight.set(k, (async () => {
    if (isSS(m.sourceUrl)) { const dl = (await buildSS(m.sourceUrl)).downloads; if (dl.length) await Movie.findByIdAndUpdate(m._id, { downloads: dl, linksAt: new Date() }); return { pages: [], downloads: dl }; }
    let pages = [], downloads = [];
    try { pages = toPages(await lakDetails(m.sourceUrl)); } catch (e) { console.error('laksidu details:', e.message); }
    if (!pages.length) { try { linkCache.delete(m.sourceUrl); downloads = toLinks(await infodl(m.sourceUrl)); } catch (e) { console.error('fallback infodl:', e.message); } }
    if (pages.length) await Movie.findByIdAndUpdate(m._id, { pages, linksAt: new Date() });
    else if (downloads.length) await Movie.findByIdAndUpdate(m._id, { downloads, linksAt: new Date() });
    return { pages, downloads };
  })().finally(() => inflight.delete(k)));
  return inflight.get(k);
}

async function build(link) {
  if (isSS(link)) return buildSS(link);
  if (isAnime(link)) return buildAnime(link);
  if (isTv(link)) {
    const d = (await cine('/cinesubz/tvshow', { url: link })).data;
    const episodes = (d.episodes?.list || []).map(ep => {
      const parts = String(ep.number || '1').split(/\s*-\s*/);
      return { season: parts.length > 1 ? parseInt(parts[0]) || 1 : 1, number: parts[parts.length - 1], title: ep.title || 'Episode', url: ep.url };
    }).filter(e => e.url);
    return { sourceUrl: link, type: 'tv', title: d.title, year: yearOf(d.year, d.title), poster: d.poster, overview: d.description, rating: d.rating?.score ? String(d.rating.score) : '', episodes };
  }
  let d; try { d = await lakDetails(link); } catch (e) { console.error('laksidu details failed, using fallback:', e.message); }
  if (d?.title) {
    const arr = v => Array.isArray(v) ? v.map(x => typeof x === 'string' ? x : x?.name) : uniq(v);
    return {
      sourceUrl: link, type: 'movie', title: d.title, year: yearOf(d.year, d.title), poster: d.poster,
      overview: String(d.description || '').slice(0, 700), rating: d.imdb_rating ? d.imdb_rating + '/10' : '',
      runtime: d.runtime, director: d.director, country: uniq(d.country).join(', '),
      cast: arr(d.cast).filter(Boolean).slice(0, 8), genres: arr(d.genres || d.genre).filter(g => g && !/^[#.]/.test(g)),
      pages: toPages(d), linksAt: new Date()
    };
  }
  const c = await infodl(link); // paid fallback
  return {
    sourceUrl: link, type: 'movie', title: c.title, year: yearOf(c.year, c.title), poster: c.image,
    overview: (c.story || '').split('\n\n')[0].slice(0, 700), rating: c.rating && c.rating !== 'N/A' ? c.rating : '',
    runtime: c.duration, director: c.director, country: uniq(c.country).join(', '),
    cast: (c.cast || []).slice(0, 8).map(x => x.name), genres: (c.genres || []).filter(g => !/^[#.]/.test(g) && !/^(hdcam|cam)$/i.test(g)),
    downloads: toLinks(c), linksAt: new Date()
  };
}
// ---- anime (animeheaven via chamindu: paid API, so everything is saved in MongoDB when added) ----
const CHA = 'https://api.chamindu.site/api/v1/anime/animeheaven';
const isAnime = l => /animeheaven\./i.test(l || '');
const aCache = new Map();
async function animeInfo(link) {
  const c = aCache.get(link); if (c && Date.now() - c.t < 12e5) return c.d;
  const d = await ch('/info', { q: link }, CHA); aCache.set(link, { t: Date.now(), d }); if (aCache.size > 60) aCache.delete(aCache.keys().next().value); return d;
}
const animeList = d => d.episodes?.length ? d.episodes : d.downloads || [];
async function buildAnime(link) {
  const d = await animeInfo(link);
  return {
    sourceUrl: link, type: 'anime', title: d.title, year: d.year, poster: d.image, overview: d.story,
    rating: d.imdb && d.imdb !== 'N/A' ? d.imdb : '', runtime: d.duration, genres: uniq(d.genres).slice(0, 10),
    episodes: animeList(d).map((e, i) => ({
      season: 1, number: String(String(e.title || e.name || '').match(/\d+/)?.[0] || i + 1), title: e.title || e.name || 'Episode ' + (i + 1),
      url: e.id || (String(e.link || e.direct_link || '').match(/\?([0-9a-f]{16,})/) || [])[1] || ''
    }))
  };
}
// ---- SinhalaSub (chamindu, paid): everything incl. download links is saved in MongoDB when the title is added ----
const SS = 'https://api.chamindu.site/api/v1/movies/sinhalasub';
const isSS = l => /sinhalasub\./i.test(l || '');
async function buildSS(link) {
  const d = await ch('/infodl', { q: link }, SS), bl = await blocked();
  if (!d.title) throw new Error('no title returned');
  const downloads = (d.downloads || []).filter(x => x.link && !/telegram/i.test((x.name || '') + x.link) && !bad(x.link)).map(x => ({
    label: `${String(x.quality || '').replace(/^(FHD|HD|SD)\s*/i, '')} [${((x.name || '').match(/\[Movie File\]\s*(.+?)\s+-\s+/) || [0, 'Server'])[1]}]`, size: x.size, url: x.link })).filter(x => !isBlocked(x, bl));
  if (!downloads.length) throw new Error('no usable download links');
  return {
    sourceUrl: link, type: 'movie', title: d.title, year: yearOf('', d.title), poster: String(d.image || '').replace('/w154/', '/w500/'),
    overview: d.story || '', rating: d.imdb && d.imdb !== 'N/A' ? d.imdb + '/10' : '', director: String(d.director || '').split(',').slice(0, 3).join(',').trim(),
    cast: (d.cast || []).slice(0, 8).map(c => c.name), genres: d.genres || [], downloads, linksAt: new Date()
  };
}
const permitted = async u => !!(await Movie.exists({ $or: [{ 'pages.url': u }, { 'downloads.url': u }] })) || !!(await Cache.exists({ k: 'ok:' + u }));
const wrap = fn => (req, res) => fn(req, res).catch(e => { console.error(req.path + ': ' + e.message); res.status(500).json({ error: e.message }); });

// ---------- public ----------
const rx = s => new RegExp(String(s).replace(/[.*+?^${}()|[\]\\]/g, '\\$&'), 'i');
app.get('/api/movies', wrap(async (req, res) => {
  const q = req.query, page = Math.max(1, +q.page || 1), c = [];
  if (q.q) c.push({ title: rx(q.q) });
  if (q.type === 'movie') c.push({ type: { $nin: ['tv', 'anime'] } });
  if (q.type === 'anime') c.push({ type: 'anime' });
  if (q.type === 'tv') c.push({ type: 'tv' });
  if (q.status === 'complete') c.push({ title: /complete/i });
  if (q.status === 'incomplete') c.push({ title: { $not: /complete/i } });
  if (q.genre) c.push({ genres: String(q.genre) });
  if (q.featured) c.push({ featured: true });
  const f = c.length ? { $and: c } : {};
  const [items, total] = await Promise.all([
    Movie.find(f, '-downloads -pages -cast -episodes').sort({ featured: -1, createdAt: -1 }).skip((page - 1) * 24).limit(24), Movie.countDocuments(f)]);
  res.set('Cache-Control', 'public, max-age=20').json({ items, total, pages: Math.ceil(total / 24) });
}));
app.get('/api/genres', wrap(async (req, res) => res.json((await Movie.distinct('genres')).filter(Boolean).sort())));
app.get('/api/movies/:id', wrap(async (req, res) => {
  const m = await Movie.findById(req.params.id);
  m ? res.json(m) : res.status(404).json({ error: 'Not found' });
}));
// episode -> quality list (saved 3h, so repeat clicks cost nothing)
app.get('/api/episode', wrap(async (req, res) => {
  const url = String(req.query.url || '');
  if (!await Movie.exists({ 'episodes.url': url })) return res.status(403).json({ error: 'Not allowed' });
  let links = await cget('e:' + url);
  if (!links) {
    const d = (await cine('/api/episode', { url })).data;
    links = (d?.download_links || []).map((l, i) => ({ label: l.meta || l.type || 'Quality ' + (i + 1), url: l.url })).filter(l => l.url);
    if (links.length) { cset('e:' + url, links); await Promise.all(links.map(l => cset('ok:' + l.url, 1))); }
  }
  res.json({ links });
}));
// does this link really serve a file? (checks the first byte only, max 6s)
const probe = async u => {
  try {
    const r = await fetch(u, { headers: { Range: 'bytes=0-0', 'User-Agent': 'Mozilla/5.0' }, signal: AbortSignal.timeout(6000) });
    r.body?.cancel?.().catch(() => {});
    return (r.status === 200 || r.status === 206) && !/text\/html/i.test(r.headers.get('content-type') || '');
  } catch { return false; }
};
async function fallbackOpts(mid, pageUrl) { // paid fallback, only when the main download API gives nothing
  try {
    const m = await Movie.findById(mid, 'sourceUrl pages'); if (!m) return [];
    const res = ((m.pages.find(p => p.url === pageUrl)?.label || '').match(/\d{3,4}p/) || [])[0];
    return toLinks(await infodl(m.sourceUrl)).filter(l => !res || l.label.includes(res)).map(l => ({ name: (l.label.match(/\[(.*?)\]/) || [0, 'Server'])[1], url: l.url }));
  } catch (e) { console.error('fallback links:', e.message); return []; }
}
// quality page link -> working download links. Every link is checked; the working one goes first. Saved 30 min.
app.get('/api/resolve', wrap(async (req, res) => {
  const url = String(req.query.url || '');
  if (!await permitted(url)) return res.status(403).json({ error: 'Not allowed' });
  const hit = await cget('r:' + url);
  if (hit?.v && Date.now() - hit.t < 30 * 60e3) return res.json(hit.v);
  let opts = [];
  try {
    const d = (await cine('/dl/cinesubz', { url }, 3)).data;
    opts = (d?.download || []).filter(l => l.name && l.name.toLowerCase() !== 'telegram' && l.url && !bad(l.url))
      .map(l => ({ name: l.name === 'unknown' ? 'Direct' : l.name, url: l.url })).sort((x, y) => (x.name === 'Direct' ? 0 : 1) - (y.name === 'Direct' ? 0 : 1));
  } catch (e) { console.error('download api:', e.message); }
  if (!opts.length && req.query.m) opts = await fallbackOpts(String(req.query.m), url);
  if (!opts.length) return res.status(404).json({ error: 'No working download link right now. Please try again in a few minutes.' });
  const ok = await Promise.all(opts.map(o => probe(o.url)));
  const ordered = [...opts.filter((_, i) => ok[i]), ...opts.filter((_, i) => !ok[i])];
  const out = { url: ordered[0].url, verified: ok.some(Boolean), alts: ordered };
  cset('r:' + url, { t: Date.now(), v: out }); res.json(out);
}));

const { Readable } = require('stream');
// anime episode download: streamed through our server (the source needs a Referer header). Saved id first = no API call.
app.get('/dl/anime/:id/:n', async (req, res) => {
  try {
    const m = await Movie.findById(req.params.id, 'sourceUrl title type episodes'), n = +req.params.n, ep = m?.episodes?.[n];
    if (!m || m.type !== 'anime' || !ep) return res.status(404).send('Not found');
    if (E.VERCEL) { // serverless cannot stream big files: use the always-on host (STREAM_HOST) or send the visitor straight to the file
      if (E.STREAM_HOST) return res.redirect(`${E.STREAM_HOST.replace(/\/$/, '')}/dl/anime/${m.id}/${n}`);
      const e = ep.url ? {} : animeList(await animeInfo(m.sourceUrl))[n] || {}, link = ep.url ? `https://cz.animeheaven.me/video.mp4?${ep.url}&d` : e.direct_link;
      return link ? res.redirect(link) : res.status(502).send('Download source is not available right now.');
    }
    const ac = new AbortController(); res.on('close', () => ac.abort());
    const hdr = { 'User-Agent': 'Mozilla/5.0', Referer: 'https://animeheaven.me/', ...(req.headers.range ? { Range: req.headers.range } : {}) };
    const get = async u => { try { const r = await fetch(u, { signal: ac.signal, headers: hdr }); return r.ok && !/text\/html/i.test(r.headers.get('content-type') || '') ? r : null; } catch { return null; } };
    let r = ep.url ? await get(`https://cz.animeheaven.me/video.mp4?${ep.url}&d`) : null;
    if (!r) { // fresh link from the API (kept 20 min)
      const e = animeList(await animeInfo(m.sourceUrl))[n] || {};
      r = (e.direct_link && await get(e.direct_link)) || (e.proxy_link && await get(e.proxy_link)) || null;
    }
    if (!r) return res.status(502).send('Download source is not available right now. Please try again later.');
    ['content-length', 'content-range', 'accept-ranges'].forEach(h => r.headers.get(h) && res.set(h, r.headers.get(h)));
    const name = `${m.title} - ${ep.title}`.replace(/\s*Sinhala Subtitles.*$/i, '').replace(/[^\w .()-]/g, '') + '.mp4';
    res.status(r.status).set({ 'Content-Type': 'video/mp4', 'Content-Disposition': `attachment; filename="${name}"` });
    Readable.fromWeb(r.body).on('error', () => res.destroy()).pipe(res);
  } catch (e) { res.headersSent ? res.destroy() : res.status(500).send('Download error: ' + e.message); }
});
app.get('/img/:id', async (req, res) => {
  const id = req.params.id;
  try {
    let hit = IMGC.get(id) || await readPoster(id);
    if (!hit) {
      const m = await Movie.findById(id, 'poster'); if (!m?.poster) return res.status(404).end();
      hit = await fetchPoster(m.poster);
      if (!hit) return res.set('Referrer-Policy', 'no-referrer').redirect(m.poster);
      storePoster(m._id, hit.data, hit.type).catch(() => {});
    }
    IMGC.set(id, hit); if (IMGC.size > 80) IMGC.delete(IMGC.keys().next().value);
    res.set({ 'Content-Type': hit.type, 'Cache-Control': 'public, max-age=31536000, immutable' }).send(hit.data);
  } catch {
    const m = await Movie.findById(id, 'poster').catch(() => null);
    m?.poster ? res.set('Referrer-Policy', 'no-referrer').redirect(m.poster) : res.status(404).end();
  }
});
// Saved links are returned from MongoDB. No API call unless a movie has nothing saved.
app.get('/api/links/:id', wrap(async (req, res) => {
  const m = await Movie.findById(req.params.id, 'sourceUrl pages downloads');
  if (!m) return res.status(404).json({ error: 'Not found' });
  if (m.pages.length) return res.json({ pages: m.pages.map(x => ({ label: x.label, url: x.url })) });
  const bl = await blocked(), good = m.downloads.filter(x => !bad(x.url) && !isBlocked(x, bl));
  if (good.length) return res.json({ links: good.map(x => ({ label: x.label, size: x.size, url: x.url })) });
  if (await cget('miss:' + m.id)) return res.json({ links: [] }); // recently tried, nothing found
  const r = await freshLinks(m);
  if (!r.pages.length && !r.downloads.length) cset('miss:' + m.id, 1);
  res.json(r.pages.length ? { pages: r.pages } : { links: r.downloads });
}));
// "Links not working?" button: one refresh per movie every 3 hours
app.post('/api/links/:id/refresh', wrap(async (req, res) => {
  const m = await Movie.findById(req.params.id, 'sourceUrl type');
  if (!m || m.type === 'tv') return res.status(404).json({ error: 'Not found' });
  if (await cget('cool:' + m.id)) return res.status(429).json({ error: 'Links were refreshed a short while ago. Please try again later.' });
  await cset('cool:' + m.id, 1);
  const r = await freshLinks(m);
  res.json(r.pages.length ? { pages: r.pages } : { links: r.downloads });
}));

let NC = { t: 0, v: null };
async function getNotice() {
  if (Date.now() - NC.t > 3e4) { const x = await Setting.findOne({ k: 'notice' }).catch(() => null); let v = null; try { v = x ? JSON.parse(x.v) : null; } catch {} NC = { t: Date.now(), v }; }
  return NC.v;
}
app.get('/api/notice', wrap(async (req, res) => res.set('Cache-Control', 'public, max-age=30').json({ notice: await getNotice() })));
app.get('/api/notice/img', wrap(async (req, res) => {
  const b = await NoticeImg.findOne({ k: 'notice' }); if (!b) return res.status(404).end();
  res.set({ 'Content-Type': b.type, 'Cache-Control': 'public, max-age=86400' }).send(Buffer.from(b.data));
}));
app.get('/health', (req, res) => res.json({ ok: true, up: Math.round(process.uptime()), db: SH.map(x => x.conn.readyState === 1) })); // for an uptime pinger (keeps the dyno awake)
app.get('/api/config', wrap(async (req, res) => res.set('Cache-Control', 'public, max-age=30').json({ ads: await adsOn() })));

// ---------- SEO: real page titles/descriptions/social cards per title, sitemap, robots ----------
const fs = require('fs');
const HTML = fs.readFileSync(path.join(__dirname, 'index.html'), 'utf8');
const X = v => String(v ?? '').replace(/[&<>"]/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]));
const baseUrl = req => (E.SITE_URL || `${req.protocol}://${req.get('host')}`).replace(/\/$/, '');
const slug = t => clean(t).toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-+|-+$/g, '').slice(0, 80) || 'movie';
function sendPage(req, res, o, status = 200) {
  const b = baseUrl(req), url = b + (o.path || '/');
  const tags = `<title>${X(o.title)}</title>
<meta name="description" content="${X(o.desc)}">
<link rel="canonical" href="${X(url)}">
${o.noindex ? '<meta name="robots" content="noindex">\n' : ''}<meta property="og:site_name" content="SHAGGY MOVIES">
<meta property="og:type" content="${o.type || 'website'}">
<meta property="og:title" content="${X(o.title)}">
<meta property="og:description" content="${X(o.desc)}">
<meta property="og:url" content="${X(url)}">
${o.img ? `<meta property="og:image" content="${X(b + o.img)}">\n<meta name="twitter:card" content="summary_large_image">\n<meta name="twitter:image" content="${X(b + o.img)}">` : '<meta name="twitter:card" content="summary">'}
${o.ld ? `<script type="application/ld+json">${JSON.stringify(o.ld).replace(/</g, '\\u003c')}</script>` : ''}`;
  res.status(status).type('html').send(HTML.replace('<!--SEO-->', () => tags));
}
app.get('/m/:id/:slug?', async (req, res) => {
  const m = await Movie.findById(req.params.id, '-downloads -pages -episodes').catch(() => null);
  if (!m) return sendPage(req, res, { title: 'Not found | SHAGGY MOVIES', desc: 'This title could not be found.', noindex: true }, 404);
  const name = clean(m.title), kind = m.type === 'anime' ? 'anime' : m.type === 'tv' ? 'TV series' : 'movie', b = baseUrl(req);
  const desc = (m.overview || '').replace(/\s+/g, ' ').trim().slice(0, 155) || `Download the ${kind} ${name} with Sinhala subtitles in 480p, 720p and 1080p.${m.genres?.length ? ' ' + m.genres.slice(0, 3).join(', ') + '.' : ''}`;
  const ld = { '@context': 'https://schema.org', '@type': m.type === 'movie' || !m.type ? 'Movie' : 'TVSeries', name, image: b + '/img/' + m.id, description: desc, genre: (m.genres || []).filter(g => !/^[#.]/.test(g)) };
  if (/^\d{4}$/.test(m.year || '')) ld.datePublished = m.year;
  if (m.director) ld.director = { '@type': 'Person', name: m.director.split(',')[0].trim() };
  if (m.cast?.length) ld.actor = m.cast.slice(0, 5).map(n => ({ '@type': 'Person', name: n }));
  sendPage(req, res, { title: `${name} ${kind === 'movie' ? 'Sinhala Subtitles – Download' : '– Download All Episodes'} | SHAGGY MOVIES`, desc, path: `/m/${m.id}/${slug(m.title)}`, img: '/img/' + m.id, type: kind === 'movie' ? 'video.movie' : 'video.tv_show', ld });
});
let SM = { t: 0, x: '' };
app.get('/sitemap.xml', wrap(async (req, res) => {
  if (Date.now() - SM.t > 36e5) {
    const b = baseUrl(req), list = await Movie.find({}, 'title updatedAt').sort({ updatedAt: -1 }).limit(50000);
    SM = { t: Date.now(), x: `<?xml version="1.0" encoding="UTF-8"?>\n<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n<url><loc>${b}/</loc></url>\n` + list.map(m => `<url><loc>${b}/m/${m.id}/${slug(m.title)}</loc><lastmod>${(m.updatedAt || new Date()).toISOString().slice(0, 10)}</lastmod></url>`).join('\n') + '\n</urlset>' };
  }
  res.type('application/xml').send(SM.x);
}));
app.get('/robots.txt', (req, res) => res.type('text/plain').send(`User-agent: *\nAllow: /\nDisallow: /api/\nDisallow: /dl/\nSitemap: ${baseUrl(req)}/sitemap.xml\n`));
app.get('*', (req, res) => sendPage(req, res, { title: 'SHAGGY MOVIES – Download Movies, TV Series & Anime with Sinhala Subtitles', desc: 'Download the latest movies, TV series and anime with Sinhala subtitles in 480p, 720p and 1080p. Fast, simple and free.', path: '/' }));
if (!E.VERCEL) app.listen(E.PORT || 3000, () => console.log('SHAGGY MOVIES running'));
module.exports = app; // Vercel uses this

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
const MENU = [[{ text: '🔍 Search & add', callback_data: 's' }], [{ text: '🎬 Movies', callback_data: 'l:movie:0' }, { text: '📺 TV series', callback_data: 'l:tv:0' }], [{ text: '🎌 Anime', callback_data: 'l:anime:0' }, { text: '🎌 Search anime', callback_data: 'sa' }], [{ text: '🇱🇰 Search SinhalaSub', callback_data: 'sx' }], [{ text: '⭐ Featured', callback_data: 'l:feat:0' }, { text: '📊 Stats', callback_data: 'st' }], [{ text: '🔒 Log out', callback_data: 'out' }]];
const HOME = '🎬 <b>SHAGGY MOVIES admin</b>\nSend a movie or series name to search and add it.\n\n<b>Commands</b>\n/bulk word : add every result of a search\n/bulkall : add everything from all 3 sources (or /bulkall anime)\n/links : save download links for all movies\n/anime name : search anime\n/ss name : search SinhalaSub\n/go 100,ANIME : add 100 new titles (CINESUBZ, SINHALASUB or ANIME)\n/stats : numbers   /db : database status\n/notice : photo + message on the site\n/ai : AI site check (/ai on|off)\n/auto : auto-add new titles and episodes (/auto on)\n/recent : last 10 added\n/find word : search your library\n/missing : movies without links\n/dupes : duplicate titles\n/ads on|off : show or hide ads\n/freespace : drop saved posters (re-saved smaller)\n/posters : save missing posters in MongoDB\n/dedupe : remove duplicate titles\n/failed : failed adds   /retry : retry them\n/tv 100 : add 100 new TV series\n/blocklink name : hide a download server everywhere\n/clearcache : clear link caches\n/stop : stop a running job';
const q4 = t => t === 'tv' ? { type: 'tv' } : t === 'anime' ? { type: 'anime' } : t === 'feat' ? { featured: true } : { type: { $nin: ['tv', 'anime'] } };

async function searchAll(q, page = 1) { // laksidu first; chamindu only if laksidu fails
  const seen = new Set(), out = []; let err;
  const push = arr => arr.forEach(x => {
    if (x.link && !seen.has(x.link)) { seen.add(x.link); out.push({ title: x.title, link: x.link, type: x.type === 'tvshows' || isTv(x.link) ? 'tv' : 'movie' }); }
  });
  try { push((await cine('/cinesubz/search', page > 1 ? { query: q, page } : { query: q })).results || []); } catch (e) { err = e; console.error('laksidu search:', e.message); }
  if (!out.length && err && page === 1) { try { push(await ch('/search', { q })); } catch (e) { console.error('fallback search:', e.message); } }
  if (!out.length && err && page === 1) throw err;
  return out;
}
async function searchAnime(q) { // paid API: only used when you ask for anime
  const d = await ch('/search', { q }, CHA);
  return d.map(x => ({ title: x.title, link: x.link, type: 'anime' })).filter(x => x.link);
}
async function doSearch(chat, s, q, src = 'cine') {
  tg('sendChatAction', { chat_id: chat, action: 'typing' });
  const w = await say(chat, `🔍 Searching <b>${H(q)}</b>…`), c = { chat, mid: w.result?.message_id };
  try {
    const r = (await ({ anime: searchAnime, ss: searchSS, cine: searchAll }[src] || searchAll)(q)).slice(0, 10); s.res = r; s.q = q;
    if (!r.length) return show(c, 'No results. Try another spelling, for example <code>salaar</code>.', [BACK]);
    const have = new Set((await Movie.find({ sourceUrl: { $in: r.map(x => x.link) } }, 'sourceUrl')).map(x => x.sourceUrl));
    s.kb = [...r.map((x, i) => [{ text: `${have.has(x.link) ? '✓ ' : ''}${x.type === 'tv' ? '📺' : x.type === 'anime' ? '🎌' : '🎬'} ${clean(x.title).slice(0, 46)}`, callback_data: 'a:' + i }]), BACK];
    show(c, `Results for <b>${H(q)}</b>. Tap one to add it:`, s.kb);
  } catch (e) { show(c, '⚠️ ' + H(e.message), [BACK]); }
}
async function itemView(c, id) {
  const m = await Movie.findById(id); if (!m) return show(c, 'Not found.', [BACK]);
  show(c, `<b>${H(clean(m.title))}</b>\n${H(m.year || '')} · ${m.type === 'tv' || m.type === 'anime' ? (m.type === 'tv' ? 'TV series' : 'Anime') + ', ' + m.episodes.length + ' episodes' : 'Movie'}${m.rating ? ' · ★ ' + H(m.rating) : ''}${m.featured ? '\n⭐ Featured' : ''}`,
    [[{ text: m.featured ? '☆ Unfeature' : '⭐ Feature', callback_data: 'f:' + id }, { text: '🔄 Refresh', callback_data: 'r:' + id }], [{ text: '🗑 Delete', callback_data: 'd:' + id }], [{ text: '« Back', callback_data: `l:${m.type === 'tv' ? 'tv' : m.type === 'anime' ? 'anime' : 'movie'}:0` }]]);
}
async function listView(c, t, p) {
  const f = q4(t), [items, n] = await Promise.all([Movie.find(f, 'title featured').sort({ createdAt: -1 }).skip(p * 8).limit(8), Movie.countDocuments(f)]);
  const nav = [];
  if (p > 0) nav.push({ text: '‹ Prev', callback_data: `l:${t}:${p - 1}` });
  if ((p + 1) * 8 < n) nav.push({ text: 'Next ›', callback_data: `l:${t}:${p + 1}` });
  show(c, `<b>${{ movie: '🎬 Movies', tv: '📺 TV series', anime: '🎌 Anime', feat: '⭐ Featured' }[t] || 'List'}</b> (${n})${n ? '' : '\nNothing here yet.'}`,
    [...items.map(m => [{ text: (m.featured ? '⭐ ' : '') + clean(m.title).slice(0, 48), callback_data: 'm:' + m._id }]), ...(nav.length ? [nav] : []), BACK]);
}
async function act(c, s, d) {
  const [k, a, b] = d.split(':');
  if (k === 'menu') { s.mode = ''; return show(c, HOME, MENU); }
  if (k === 'stop') { JOB.stop = true; return show(c, '🛑 Stopping after the current step…'); }
  if (k === 'sx') { s.mode = 'ss'; return show(c, '🇱🇰 Send the movie name (SinhalaSub).', [BACK]); }
  if (k === 'sa') { s.mode = 'anime'; return show(c, '🎌 Send the anime name.', [BACK]); }
  if (k === 's') { s.mode = ''; return show(c, '🔍 Send the movie or series name.', [BACK]); }
  if (k === 'st') {
    const day = new Date(Date.now() - 864e5), nl = { type: { $nin: ['tv', 'anime'] }, 'downloads.0': { $exists: false }, 'pages.0': { $exists: false } };
    const [m, t, an, f, today, nolinks, db, ads] = await Promise.all([Movie.countDocuments({ type: { $nin: ['tv', 'anime'] } }), Movie.countDocuments({ type: 'tv' }), Movie.countDocuments({ type: 'anime' }), Movie.countDocuments({ featured: true }),
      Movie.countDocuments({ createdAt: { $gt: day } }), Movie.countDocuments(nl), dbSize(), adsOn()]);
    return show(c, `📊 <b>Stats</b>\n🎬 Movies: ${m}\n📺 TV series: ${t}\n🎌 Anime: ${an}\n⭐ Featured: ${f}\n🆕 Added in 24h: ${today}\n🔗 Movies without links: ${nolinks}\n💾 ${dbLine(db)}\n📢 Ads: ${ads ? 'ON' : 'OFF'}\n⚙️ Job: ${JOB.run ? 'running' : 'idle'}\n⏱ Uptime: ${Math.round(process.uptime() / 3600)}h`, [BACK]);
  }
  if (k === 'dd') { const { del } = await dupeInfo(); await Movie.deleteMany({ _id: { $in: del } }); await delPosters(del); return show(c, `🧹 Removed ${del.length} duplicate copies.`, [BACK]); }
  if (k === 'rt') { show(c, '🔁 Starting retry…'); return retryFailed(c.chat); }
  if (k === 'out') { s.until = 0; return show(c, '🔒 Logged out.'); }
  if (k === 'l') return listView(c, a, +b || 0);
  if (k === 'm') return itemView(c, a);
  if (k === 'f') { const m = await Movie.findById(a); await Movie.findByIdAndUpdate(a, { featured: !m.featured }); return itemView(c, a); }
  if (k === 'r') { await show(c, '⏳ Refreshing…'); const m = await Movie.findById(a); linkCache.delete(m.sourceUrl); await save(await build(m.sourceUrl)); return itemView(c, a); }
  if (k === 'rs') return show(c, `Results for <b>${H(s.q || '')}</b>. Tap one to add it:`, s.kb || [BACK]);
  if (k === 'd') return show(c, '⚠️ Delete this title?', [[{ text: '✅ Yes, delete', callback_data: 'D:' + a }, { text: 'Cancel', callback_data: 'm:' + a }]]);
  if (k === 'D') { await Movie.findByIdAndDelete(a); await delPosters([a]); return show(c, '🗑 Deleted.', [BACK]); }
  if (k === 'a') {
    const x = s.res[+a]; if (!x) return show(c, 'Search again.', [BACK]);
    await show(c, `⏳ Adding <b>${H(clean(x.title))}</b>… a few seconds`);
    const have = await Movie.findOne({ sourceUrl: x.link }, 'title'); // already saved = no API call
    const doc = have ? { title: have.title } : await build(x.link);
    const saved = have || await save(doc);
    if (!have) warmOne(saved).catch(() => {});
    const row = s.kb?.[+a]?.[0]; if (row) row.text = '✓ ' + row.text.replace(/^✓ /, '');
    return show(c, `✅ Added: <b>${H(clean(doc.title))}</b>`, [[{ text: '📋 Back to results', callback_data: 'rs' }], [{ text: '🔍 New search', callback_data: 's' }, ...BACK]]);
  }
}
const JOB = { run: false, stop: false };
const STOPKB = [[{ text: '🛑 Stop', callback_data: 'stop' }]];
const ALLKW = [...Array.from({ length: 47 }, (_, i) => String(2026 - i)), ...'abcdefghijklmnopqrstuvwxyz0123456789'.split(''),
  'action', 'comedy', 'horror', 'thriller', 'drama', 'romance', 'animation', 'crime', 'war', 'adventure', 'fantasy', 'family', 'mystery', 'korean', 'hindi', 'tamil', 'anime', 'sinhala', 'complete'];
const ANIMEKW = [...'abcdefghijklmnopqrstuvwxyz'.split(''), 'naruto', 'one piece', 'dragon ball', 'solo leveling', 'attack on titan', 'demon slayer', 'jujutsu', 'bleach', 'hunter', 'boruto', 'hero academia', 'tokyo', 'sword art', 'death note', 'season 2', 'season 3', 'movie', 'ova'];
const WORDS = ['the', 'man', 'love', 'war', 'king', 'dark', 'night', 'last', 'dead', 'blood', 'girl', 'boy', 'home', 'city', 'life', 'world', 'black', 'white', 'red', 'big', 'little', 'house', 'power', 'secret', 'return', 'rise', 'day', 'fire', 'ice', 'dragon', 'star', 'killer', 'game', 'story', 'dream', 'road', 'island', 'crime', 'lost', 'young', 'new', 'great', 'mission', 'agent', 'spider', 'super', 'iron', 'fast', 'dual audio', 'bluray', 'hdrip', 'tamil', 'telugu', 'malayalam', 'kannada', 'chinese', 'japanese', 'thai', 'turkish', 'french', 'spanish', 'german', 'russian', 'bollywood', 'hollywood', 'part 2', 'chapter', 'origins', 'legend', 'revenge', 'escape', 'hunt', 'empire', 'kingdom', 'shadow', 'ghost', 'demon', 'zombie', 'alien', 'space', 'time', 'future', 'robot', 'cop', 'police', 'prison', 'gangster', 'heist', 'soldier', 'army', 'pirate', 'vampire', 'witch', 'magic', 'school', 'family', 'wedding', 'baby', 'sister', 'brother', 'mother', 'father', 'friends', 'summer', 'winter', 'christmas'];
const UA = { 'User-Agent': 'Mozilla/5.0 (Linux; Android 13) AppleWebKit/537.36 Chrome/120 Mobile Safari/537.36', Accept: 'application/json,text/xml,*/*' };
const unent = t => String(t || '').replace(/&#(\d+);/g, (_, n) => String.fromCharCode(+n)).replace(/&amp;/g, '&').replace(/&lt;/g, '<').replace(/&gt;/g, '>').replace(/&quot;/g, '"').replace(/<[^>]+>/g, '').trim();
// whole catalogue of a WordPress movie site, page by page: REST API first, sitemap files as a backup
const NOREST = new Set();
async function wpList(site, kinds, p, note) {
  const out = [];
  for (const [kind, type] of kinds) {
    let got = null;
    if (!NOREST.has(site + kind)) try {
      const r = await fetch(`${site}/wp-json/wp/v2/${kind}?per_page=100&page=${p}&_fields=link,title`, { headers: UA, signal: AbortSignal.timeout(25000) });
      if (r.status === 400) continue; // past the last page
      if (r.status === 404 || r.status === 401 || r.status === 403) { NOREST.add(site + kind); note(`${kind} REST ${r.status}`); }
      else if (r.ok) got = (await r.json()).map(x => ({ title: unent(x.title?.rendered), link: x.link, type })); else note(`${kind} REST ${r.status}`);
    } catch (e) { note(`${kind} REST ${e.message.slice(0, 30)}`); }
    if (!got) for (const u of [`${site}/${kind}-sitemap${p > 1 ? p : ''}.xml`, `${site}/wp-sitemap-posts-${kind}-${p}.xml`]) {
      try {
        const r = await fetch(u, { headers: UA, signal: AbortSignal.timeout(25000) });
        if (!r.ok) { note(`${kind} sitemap ${r.status}`); continue; }
        got = [...(await r.text()).matchAll(/<loc>\s*([^<\s]+)\s*<\/loc>/g)].map(m => m[1]).filter(l => l.includes(`/${kind}/`)).map(l => ({ title: l.split('/').filter(Boolean).pop().replace(/-/g, ' '), link: l, type }));
        break;
      } catch (e) { note(`${kind} sitemap ${e.message.slice(0, 30)}`); }
    }
    if (got) out.push(...got);
  }
  return out;
}
const cineList = (p, note) => wpList((E.CINE_SITE || 'https://cinesubz.net').replace(/\/$/, ''), [['movies', 'movie'], ['tvshows', 'tv']], p, note);
const ssList = (p, note) => wpList((E.SS_SITE || 'https://sinhalasub.lk').replace(/\/$/, ''), [['movies', 'movie']], p, note);
const norm = t => clean(t).toLowerCase().replace(/[^a-z0-9\u0d80-\u0dff]+/g, '');
async function searchSS(q, page = 1) { // SinhalaSub (movies only)
  if (page > 1) return [];
  const d = await ch('/search', { q }, SS);
  return d.map(x => ({ title: x.title, link: x.link, type: x.type === 'tvshows' || /\/tvshows\//.test(x.link) ? 'tv' : 'movie' })).filter(x => x.link && x.type === 'movie');
}
const tvOnly = async (q, p) => (await searchAll(q, p)).filter(x => x.type === 'tv');
const SRC_TV = { name: 'CINESUBZ TV', find: tvOnly, kw: [...ALLKW, ...WORDS], list: (p, note) => wpList((E.CINE_SITE || 'https://cinesubz.net').replace(/\/$/, ''), [['tvshows', 'tv']], p, note) };
const SRC = { CINESUBZ: { find: searchAll, kw: [...ALLKW, ...WORDS], list: cineList }, SINHALASUB: { find: searchSS, kw: [...ALLKW, ...WORDS], list: ssList }, ANIME: { find: (q, p) => p > 1 ? [] : searchAnime(q), kw: ANIMEKW } };
const SRC_ALIAS = { CINE: 'CINESUBZ', CINESUBZ: 'CINESUBZ', SS: 'SINHALASUB', SINHALA: 'SINHALASUB', SINHALASUB: 'SINHALASUB', ANIME: 'ANIME' };

// save download links for every movie that has none yet
async function backfill(chat) {
  if (JOB.run) return chat && say(chat, '⏳ Another job is running. Send /stop to cancel it.');
  JOB.run = true; JOB.stop = false;
  let ok = 0, bad = 0;
  try {
    const list = await Movie.find({ type: { $nin: ['tv', 'anime'] }, 'downloads.0': { $exists: false }, 'pages.0': { $exists: false } }, 'sourceUrl');
    if (!list.length) return chat && say(chat, '✅ Every movie already has saved download links.');
    const w = chat && await say(chat, `🔗 Saving links for ${list.length} movies…`, STOPKB), c = { chat, mid: w?.result?.message_id };
    let last = 0;
    for (let i = 0; i < list.length && !JOB.stop; i += 3) {
      const r = await Promise.allSettled(list.slice(i, i + 3).map(m => freshLinks(m)));
      r.forEach(x => x.status === 'fulfilled' && (x.value.pages.length || x.value.downloads.length) ? ok++ : bad++);
      if (chat && Date.now() - last > 4000) { last = Date.now(); show(c, `🔗 Saving links… ${ok + bad}/${list.length}\n✅ ${ok}   ⚠️ ${bad}`, STOPKB); }
    }
    if (chat) say(chat, `✅ Links finished${JOB.stop ? ' (stopped)' : ''}\nSaved: ${ok}\nNo links found: ${bad}`);
  } finally { JOB.run = false; }
}
// add one title. Never saves empty titles or movies without any download link.
async function addItem(x) {
  const doc = await build(x.link);
  if (!doc.title) throw new Error('no title');
  if (doc.type === 'movie' && !doc.pages?.length && !doc.downloads?.length) throw new Error('no download links');
  if (doc.type !== 'movie' && !doc.episodes?.length) throw new Error('no episodes');
  const v = await save(doc); warmOne(v).catch(() => {}); Fail.deleteOne({ link: x.link }).catch(() => {});
}
const failed = (x, e) => Fail.findOneAndUpdate({ link: x.link }, { title: x.title, reason: String(e?.message || e).slice(0, 80), at: new Date() }, { upsert: true }).catch(() => {});
const FULL = e => /quota/i.test(String(e?.message || e));
// add many NEW titles. plan = [{ name, find, kw, list? }], limit = how many new titles to add. Titles already on the site are skipped.
const pk = l => { try { const u = new URL(l); return 'p:' + u.pathname + u.search; } catch { return 'l:' + l; } };
const nk = t => { const k = norm(t); return k.length > 3 ? 'n:' + k : null; };
async function bulk(chat, plan, limit = Infinity) {
  if (JOB.run) return say(chat, '⏳ Another job is running. Send /stop to cancel it.');
  const sz = await dbSize(); if (allFull()) return say(chat, `⛔ All databases are almost full (${dbLine(sz)}). Add another MongoDB link (MONGODB_URI_2) or send /freespace.`);
  JOB.run = true; JOB.stop = false;
  const w = await say(chat, '📥 Starting…', STOPKB), c = { chat, mid: w.result?.message_id };
  let added = 0, bad = 0, found = 0, scanned = 0, errs = 0, last = 0, cur = plan[0]?.name, step = '';
  const notes = new Set(), why = {};
  const info = n => [...Object.entries(why).sort((x, y) => y[1] - x[1]).slice(0, 3).map(([k, v]) => `${k} ×${v}`), ...[...notes].slice(0, 3)].slice(0, n).map(H).join('\n');
  const tick = () => { if (Date.now() - last > 4000) { last = Date.now(); show(c, `📥 Adding from <b>${cur}</b>\n${H(step)}\n🔍 Scanned: ${scanned}\n🆕 New found: ${found}\n✅ Added: ${added}${limit < Infinity ? ' / ' + limit : ''}\n⚠️ Failed: ${bad}   Search errors: ${errs}${info(4) ? '\nℹ️ ' + info(4) : ''}`, STOPKB); } };
  try {
    const have = new Set();
    for (const m of await Movie.find({}, 'title sourceUrl')) { have.add(pk(m.sourceUrl)); const k = nk(m.title); if (k) have.add(k); }
    const take = async items => {
      scanned += items.length;
      const todo = items.filter(x => x.link && !have.has(pk(x.link)) && !(nk(x.title) && have.has(nk(x.title))));
      todo.forEach(x => { have.add(pk(x.link)); const k = nk(x.title); if (k) have.add(k); });
      found += todo.length;
      for (let i = 0; i < todo.length && !JOB.stop && added < limit; i += 3) {
        const chunk = todo.slice(i, i + Math.min(3, limit - added)), r = await Promise.allSettled(chunk.map(addItem));
        r.forEach((x, j) => {
          if (x.status === 'fulfilled') return added++;
          bad++; const k = String(x.reason?.message || 'error').slice(0, 40); why[k] = (why[k] || 0) + 1; failed(chunk[j], x.reason);
          if (FULL(x.reason)) { JOB.stop = true; notes.add('⛔ ALL DATABASES FULL: add another MongoDB link (MONGODB_URI_2) and redeploy, then run the command again'); }
        }); tick();
      }
    };
    for (const src of plan) {
      cur = src.name; let listed = false;
      if (src.list) { // 1) the site's full catalogue
        for (let p = 1; p <= 500 && !JOB.stop && added < limit; p++) {
          step = `📚 Catalogue page ${p}`; let items = [];
          try { items = await src.list(p, n => notes.add(n)); } catch (e) { errs++; notes.add(e.message.slice(0, 40)); }
          if (!items.length) break;
          listed = true; await take(items); tick();
        }
        if (!listed) notes.add('catalogue unavailable, using keyword search');
      }
      if (!listed) for (let i = 0; i < src.kw.length && !JOB.stop && added < limit; i++) { // 2) keyword search
        const kw = src.kw[i], seen = new Set(); step = `🔎 Keyword ${i + 1}/${src.kw.length}: ${kw}`;
        for (let p = 1; p <= 5 && !JOB.stop && added < limit; p++) {
          let res = []; try { res = await src.find(kw, p); } catch (e) { errs++; notes.add(('search: ' + e.message).slice(0, 40)); break; }
          const fresh = res.filter(x => !seen.has(x.link)); fresh.forEach(x => seen.add(x.link));
          if (!fresh.length) break;
          await take(fresh);
        }
        tick();
      }
      if (JOB.stop || added >= limit) break;
    }
  } finally { JOB.run = false; }
  say(chat, `✅ Finished${JOB.stop ? ' (stopped)' : ''}\nScanned: ${scanned}\nNew found: ${found}\nAdded: ${added}\nFailed: ${bad}\nSearch errors: ${errs}${info(5) ? '\nℹ️ ' + info(5) : ''}`);
}
async function retryFailed(chat) {
  if (JOB.run) return say(chat, '⏳ Another job is running. Send /stop to cancel it.');
  const list = await Fail.find().limit(5000);
  if (!list.length) return say(chat, '✅ No failed items to retry.');
  JOB.run = true; JOB.stop = false;
  const w = await say(chat, `🔁 Retrying ${list.length} failed titles…`, STOPKB), c = { chat, mid: w.result?.message_id };
  let ok = 0, bad = 0, last = 0; const why = {};
  try {
    for (let i = 0; i < list.length && !JOB.stop; i += 3) {
      const chunk = list.slice(i, i + 3), r = await Promise.allSettled(chunk.map(addItem));
      r.forEach((x, j) => {
        if (x.status === 'fulfilled') return ok++;
        bad++; const k = String(x.reason?.message || 'error').slice(0, 40); why[k] = (why[k] || 0) + 1; failed(chunk[j], x.reason);
        if (FULL(x.reason)) { JOB.stop = true; why['⛔ ALL DATABASES FULL: add MONGODB_URI_2'] = 1; }
      });
      if (Date.now() - last > 4000) { last = Date.now(); show(c, `🔁 Retrying… ${ok + bad}/${list.length}\n✅ ${ok}   ⚠️ ${bad}`, STOPKB); }
    }
  } finally { JOB.run = false; }
  say(chat, `✅ Retry finished${JOB.stop ? ' (stopped)' : ''}\nAdded: ${ok}\nStill failing: ${bad}${bad ? '\nℹ️ ' + Object.entries(why).slice(0, 4).map(([k, v]) => H(k) + ' ×' + v).join('\n') : ''}`);
}
// duplicates = same title (and year) in the same kind. Keep the best copy: featured, then most links, then the oldest.
async function dupeInfo() {
  const G = new Map();
  for (const m of await Movie.find({}, 'title type featured createdAt downloads.url pages.url').lean()) {
    const k = nk(m.title); if (!k) continue;
    const g = (m.type === 'tv' || m.type === 'anime' ? m.type : 'movie') + k; if (!G.has(g)) G.set(g, []); G.get(g).push(m);
  }
  const del = []; let groups = 0;
  for (const g of G.values()) if (g.length > 1) {
    groups++;
    g.sort((a, b) => (!!b.featured - !!a.featured) || ((b.pages?.length || 0) + (b.downloads?.length || 0) - (a.pages?.length || 0) - (a.downloads?.length || 0)) || (new Date(a.createdAt) - new Date(b.createdAt)));
    del.push(...g.slice(1).map(m => m._id));
  }
  return { groups, del };
}
// ---------- /notice: photo + message shown to every visitor ----------
async function setNotice(chat, msg, body) {
  const cur = await getNotice();
  if (/^off$/i.test(body)) { await Setting.deleteOne({ k: 'notice' }); await NoticeImg.deleteOne({ k: 'notice' }); NC.t = 0; return say(chat, '🗑 Notice removed from the site.'); }
  const ph = msg.photo || msg.reply_to_message?.photo;
  if (!body && !ph) return say(chat, `📣 <b>Site notice</b>\n${cur ? `Showing now: ${H(cur.text || '(photo only)')}${cur.img ? ' + photo' : ''}` : 'None right now.'}\n\nWith a photo: send the photo with the caption <code>/notice your message</code>\nText only: <code>/notice your message</code>\nRemove: <code>/notice off</code>`);
  let img = false;
  if (ph) {
    const f = await tg('getFile', { file_id: ph[ph.length - 1].file_id });
    if (!f.ok) return say(chat, '⚠️ Could not get the photo from Telegram.');
    const r = await fetch(`https://api.telegram.org/file/bot${E.TG_BOT_TOKEN}/${f.result.file_path}`);
    const buf = await shrink(Buffer.from(await r.arrayBuffer()), 900, 78);
    if (buf.length > 450000) return say(chat, '⚠️ The photo is too big. Send a smaller one.');
    await NoticeImg.findOneAndUpdate({ k: 'notice' }, { type: imgType(buf) || 'image/jpeg', data: buf }, { upsert: true }); img = true;
  } else await NoticeImg.deleteOne({ k: 'notice' });
  await Setting.findOneAndUpdate({ k: 'notice' }, { v: JSON.stringify({ id: Date.now().toString(36), text: body, img }) }, { upsert: true }); NC.t = 0;
  return say(chat, `✅ Notice is live on the site${img ? ' (with photo)' : ''}.\nEvery visitor sees it once. Remove it with <code>/notice off</code>.`);
}

// ---------- 24/7: AI site doctor (Gemini) ----------
const autoCfg = async () => { const x = await Setting.findOne({ k: 'auto' }).catch(() => null); let v = { on: false, anime: false }; try { if (x) v = { ...v, ...JSON.parse(x.v) }; } catch {} return v; };
const setAuto = async p => { const v = { ...(await autoCfg()), ...p }; await Setting.findOneAndUpdate({ k: 'auto' }, { v: JSON.stringify(v) }, { upsert: true }); return v; };
const aiOn = async () => { const x = await Setting.findOne({ k: 'ai' }).catch(() => null); return x ? x.v !== 'off' : !!E.GEMINI_API_KEY; };
const notifyAdmins = t => IDS.forEach(id => say(id, t));
// Gemini: the model is chosen automatically from what your API key can use (set GEMINI_MODEL only to force one).
// If Google retires a model, the next call switches to the model Google suggests or the newest "flash" model.
const GAPI = 'https://generativelanguage.googleapis.com/v1beta', GM = { name: '', t: 0, bad: new Set() };
async function geminiModel(force) {
  if (E.GEMINI_MODEL && !force) return E.GEMINI_MODEL;
  if (GM.name && !force && Date.now() - GM.t < 6 * 36e5) return GM.name;
  const r = await fetch(`${GAPI}/models?pageSize=200`, { headers: { 'x-goog-api-key': E.GEMINI_API_KEY }, signal: AbortSignal.timeout(20000) });
  const j = await r.json().catch(() => ({})); if (!r.ok) throw new Error(j.error?.message || 'Gemini HTTP ' + r.status);
  const names = (j.models || []).filter(m => (m.supportedGenerationMethods || []).includes('generateContent')).map(m => m.name.replace(/^models\//, ''))
    .filter(n => /^gemini/.test(n) && !/(image|tts|live|audio|embed|vision|robotics|computer|-exp)/i.test(n) && !GM.bad.has(n));
  const rank = n => parseFloat((n.match(/^gemini-(\d+(?:\.\d+)?)/) || [])[1] || 0) * 100 + (/flash/.test(n) ? 150 : /pro/.test(n) ? 5 : 0) - (/lite/.test(n) ? 8 : 0) - (/preview/.test(n) ? 3 : 0);
  const pick = names.sort((x, y) => rank(y) - rank(x))[0];
  if (!pick) throw new Error('no Gemini model is available for this API key');
  GM.name = pick; GM.t = Date.now(); return pick;
}
async function gemini(prompt) {
  if (!E.GEMINI_API_KEY) return null;
  let model = await geminiModel();
  for (let tries = 0; tries < 3; tries++) {
    const r = await fetch(`${GAPI}/models/${model}:generateContent`, {
      method: 'POST', headers: { 'Content-Type': 'application/json', 'x-goog-api-key': E.GEMINI_API_KEY }, signal: AbortSignal.timeout(40000),
      body: JSON.stringify({ contents: [{ parts: [{ text: prompt }] }], generationConfig: { temperature: 0.2, maxOutputTokens: 1000 } })
    });
    const j = await r.json().catch(() => ({})), msg = j.error?.message || 'Gemini HTTP ' + r.status;
    if (r.ok) { GM.name = model; return (j.candidates?.[0]?.content?.parts || []).map(p => p.text || '').join('').trim() || null; }
    if (r.status === 404 || /no longer available|not found|not supported|deprecated|retired/i.test(msg)) { // retired model: use the suggested one, or look for another
      GM.bad.add(model);
      const hint = [...msg.matchAll(/models\/(gemini[\w.\-]*)/g)].map(m => m[1]).filter(n => n !== model && !GM.bad.has(n)).pop();
      model = hint || await geminiModel(true); GM.name = model; GM.t = Date.now(); continue;
    }
    throw new Error(msg);
  }
  throw new Error('no working Gemini model found');
}
async function scanSite() {
  const R = { at: new Date().toISOString(), issues: [], checks: {}, fix: { deadMovies: [] } }, flag = m => R.issues.push(m), base = `http://127.0.0.1:${E.PORT || 3000}`;
  const hit = async (name, p) => {
    const t = Date.now();
    try { const r = await fetch(base + p, { signal: AbortSignal.timeout(25000) }), ms = Date.now() - t; R.checks[name] = `${r.status} in ${ms}ms`; if (!r.ok) flag(`${name}: HTTP ${r.status}`); else if (ms > 6000) flag(`${name} is slow (${ms} ms)`); return r; }
    catch (e) { R.checks[name] = 'failed'; flag(`${name}: ${e.message}`); return null; }
  };
  for (const [n, p] of [['home list', '/api/movies?page=1'], ['genres', '/api/genres'], ['config', '/api/config'], ['sitemap', '/sitemap.xml']]) await hit(n, p);
  const sz = await dbSize(); R.checks.databases = dbLine(sz); sz.filter(x => x.down).forEach(x => flag(`${x.name} is offline`)); if (allFull()) flag('all databases are almost full');
  const sample = (await Promise.all(live().map(x => x.Movie.aggregate([{ $sample: { size: 4 } }, { $project: { title: 1, type: 1, downloads: 1, pages: 1, poster: 1, episodes: { $slice: ['$episodes', 1] } } }]).catch(() => [])))).flat();
  let posters = 0, nolinks = 0, dead = 0; const bl = await blocked();
  for (const m of sample) {
    const pr = await fetch(`${base}/img/${m._id}`, { signal: AbortSignal.timeout(20000) }).catch(() => null);
    if (!pr || !pr.ok || !/^image\//.test(pr.headers.get('content-type') || '')) posters++;
    if (m.type === 'tv' || m.type === 'anime') { if (!m.episodes?.length) flag(`"${clean(m.title)}" has no episodes`); continue; }
    const good = (m.downloads || []).filter(x => !bad(x.url) && !isBlocked(x, bl));
    if (!good.length && !m.pages?.length) { nolinks++; continue; }
    if (good.length && !m.pages?.length && !(await Promise.all(good.slice(0, 3).map(x => probe(x.url)))).some(Boolean)) { dead++; R.fix.deadMovies.push(String(m._id)); }
  }
  R.checks.sample = `${sample.length} random titles: ${posters} poster problems, ${nolinks} without links, ${dead} with dead links`;
  if (sample.length && posters > sample.length / 2) flag('most sampled posters do not load');
  if (dead) flag(`${dead} sampled movies have dead download links`);
  const [nl, fails] = await Promise.all([Movie.countDocuments({ type: { $nin: ['tv', 'anime'] }, 'downloads.0': { $exists: false }, 'pages.0': { $exists: false } }), Fail.countDocuments()]);
  R.checks.moviesWithoutLinks = nl; R.checks.failedAdds = fails;
  if (nl) flag(`${nl} movies have no download links`); if (fails) flag(`${fails} titles failed to add earlier`);
  try { const d = await cine('/cinesubz/search', { query: 'avatar' }, 1); if (!(d.results || []).length) flag('main search API returned nothing'); } catch (e) { flag('main API (laksidu): ' + e.message); }
  const rec = ERRS.filter(e => Date.now() - e.t < 36e5); R.recentErrors = [...new Set(rec.map(e => e.m))].slice(-8);
  if (rec.length) flag(`${rec.length} server errors in the last hour`);
  R.checks.memoryMB = Math.round(process.memoryUsage().rss / 1048576); if (R.checks.memoryMB > (+E.MEM_LIMIT_MB || 512) * 0.9) flag(`high memory use (${R.checks.memoryMB} MB)`);
  return R;
}
async function fixLinks(limit = 30) { // safe repair: fetch links for movies that have none (main API only)
  const list = (await Movie.find({ type: { $nin: ['tv', 'anime'] }, 'downloads.0': { $exists: false }, 'pages.0': { $exists: false } }, 'sourceUrl').limit(limit)).filter(m => !isSS(m.sourceUrl));
  let ok = 0;
  for (let i = 0; i < list.length; i += 3) for (const r of await Promise.allSettled(list.slice(i, i + 3).map(m => freshLinks(m)))) if (r.status === 'fulfilled' && (r.value.pages.length || r.value.downloads.length)) ok++;
  return ok;
}
const DOCTOR = `You are the site doctor for SHAGGY MOVIES, a movie download website (Node/Express, several MongoDB databases, a Telegram admin bot). Below is a JSON health report. Explain in simple Sinhala (Sinhala script, technical words stay in English) what is wrong, most important first, at most 8 short lines. For each real problem give the likely cause and ONE fix, using only these admin bot commands when relevant: /links, /retry, /dedupe, /freespace, /clearcache, /db, /stats, /blocklink name, /go N,SOURCE, /auto now - or say to check the Heroku logs or config. Never invent problems that are not in the report. If everything is fine reply exactly: ✅ OK\n\nREPORT:\n`;
const LASTDOC = { sig: '', t: 0 };
async function runDoctor() {
  const R = await scanSite(), fixed = {};
  try {
    if (R.checks.moviesWithoutLinks) fixed.linksFetched = await fixLinks(30);
    let n = 0; for (const id of R.fix.deadMovies.slice(0, 5)) { const m = await Movie.findById(id, 'sourceUrl'); if (m && !isSS(m.sourceUrl) && !(await cget('cool:' + m.id))) { await cset('cool:' + m.id, 1); await freshLinks(m); n++; } }
    if (n) fixed.deadLinkSetsRefreshed = n;
  } catch (e) { console.error('autofix:', e.message); }
  let ai = null, aiErr = ''; try { ai = await gemini(DOCTOR + JSON.stringify({ ...R, fix: undefined, autoFixed: fixed })); } catch (e) { aiErr = e.message; console.error('gemini:', e.message); }
  const lines = Object.entries(R.checks).map(([k, v]) => `${k}: ${v}`).join('\n');
  const problems = R.issues.length > 0 && !(ai && /^✅\s*OK/.test(ai));
  const text = `🩺 <b>Site doctor</b>${problems ? ' ⚠️' : ' ✅'}\n${H(lines)}\n\n${ai ? H(ai.slice(0, 2500)) : R.issues.length ? R.issues.map(i => '• ' + H(i)).join('\n') : 'Everything looks fine.'}${Object.keys(fixed).length ? '\n\n🔧 Auto-fixed: ' + H(JSON.stringify(fixed)) : ''}${ai ? '\n\n🤖 ' + H(GM.name) : aiErr ? '\n\n🤖 AI unavailable: ' + H(aiErr.slice(0, 200)) : '\n\nℹ️ Add GEMINI_API_KEY for AI explanations.'}`;
  return { problems, text, sig: R.issues.join('|') };
}
// ---------- 24/7: auto sync (new titles + new episodes, no commands needed) ----------
let LASTAUTO = { t: 0, text: '' };
async function autoSync() {
  const cfg = await autoCfg(), max = +E.AUTO_ADD_MAX || 15, epMax = +E.AUTO_EP_MAX || 12, year = new Date().getFullYear(), L = [];
  let added = 0, bad2 = 0, eps = 0, full = false;
  const have = new Set(); for (const m of await Movie.find({}, 'title sourceUrl')) { have.add(pk(m.sourceUrl)); const k = nk(m.title); if (k) have.add(k); }
  const cand = [];
  const grab = async fn => { try { cand.push(...await fn()); } catch (e) { console.error('autosync search:', e.message); } };
  await grab(() => searchAll(String(year))); await grab(() => searchAll(`${year} tv series`)); await grab(() => searchSS(String(year)));
  if (cfg.anime) { const x = await Setting.findOne({ k: 'auto_kw' }).catch(() => null), i = +(x?.v || 0); await grab(() => searchAnime(ANIMEKW[i % ANIMEKW.length])); await Setting.findOneAndUpdate({ k: 'auto_kw' }, { v: String(i + 1) }, { upsert: true }).catch(() => {}); }
  const todo = []; for (const x of cand) { if (!x.link || have.has(pk(x.link)) || (nk(x.title) && have.has(nk(x.title)))) continue; have.add(pk(x.link)); if (nk(x.title)) have.add(nk(x.title)); todo.push(x); }
  const names = [];
  for (let i = 0; i < todo.length && added < max && !JOB.stop && !full; i += 3) {
    const chunk = todo.slice(i, i + Math.min(3, max - added)), r = await Promise.allSettled(chunk.map(addItem));
    r.forEach((x, j) => { if (x.status === 'fulfilled') { added++; names.push(clean(chunk[j].title)); } else { bad2++; failed(chunk[j], x.reason); if (FULL(x.reason)) full = true; } });
  }
  if (added) L.push(`🆕 New titles: <b>${added}</b>\n${names.slice(0, 8).map(n => '• ' + H(n)).join('\n')}`);
  const now = Date.now();
  const series = (await Movie.find({ type: { $in: ['tv', 'anime'] } }, 'title type sourceUrl syncAt episodes.url')).filter(m => !(m.type === 'tv' && /complete/i.test(m.title)) && !(m.type === 'anime' && !cfg.anime)
    && now - (m.syncAt?.getTime() || 0) > (m.type === 'anime' ? 6 : 12) * 36e5).sort((a, b) => (a.syncAt?.getTime() || 0) - (b.syncAt?.getTime() || 0)).slice(0, epMax);
  const upd = [];
  for (let i = 0; i < series.length && !JOB.stop && !full; i += 2) await Promise.allSettled(series.slice(i, i + 2).map(async m => {
    try {
      const doc = await build(m.sourceUrl);
      if (doc.episodes?.length > m.episodes.length) { await save(doc); eps += doc.episodes.length - m.episodes.length; upd.push(`${clean(m.title)} +${doc.episodes.length - m.episodes.length}`); }
      await Movie.findByIdAndUpdate(m._id, { syncAt: new Date() });
    } catch (e) { bad2++; if (FULL(e)) full = true; console.error('autosync episodes:', e.message); }
  }));
  if (eps) L.push(`📺 New episodes: <b>${eps}</b>\n${upd.slice(0, 8).map(n => '• ' + H(n)).join('\n')}`);
  if (full) L.push('⛔ Database full: add MONGODB_URI_2 (see /db).');
  if (bad2) L.push(`⚠️ Failed: ${bad2} (see /failed)`);
  const text = L.length ? '🔄 <b>Auto-sync</b>\n' + L.join('\n\n') : '';
  LASTAUTO = { t: Date.now(), text: text || 'nothing new' };
  return text;
}
async function runAuto(chat) {
  if (JOB.run) return chat ? say(chat, '⏳ Another job is running. Send /stop to cancel it.') : null;
  JOB.run = true; JOB.stop = false;
  try { const t = await autoSync(); if (chat) say(chat, t || '🔄 Auto-sync: nothing new right now.'); else if (t) notifyAdmins(t); }
  catch (e) { console.error('autosync:', e.message); if (chat) say(chat, '⚠️ ' + H(e.message)); }
  finally { JOB.run = false; }
}
const SCHED = { sync: Date.now() - ((+E.AUTO_SYNC_MIN || 60) - 5) * 60e3, ai: Date.now() - ((+E.AI_SCAN_MIN || 120) - 6) * 60e3 }; // first runs ~5 minutes after start
const SCH = async () => {
  try {
    if (JOB.run || !IDS.length) return;
    const now = Date.now();
    if (now - SCHED.sync > (+E.AUTO_SYNC_MIN || 60) * 60e3 && (await autoCfg()).on) { SCHED.sync = now; await runAuto(null); }
    if (now - SCHED.ai > (+E.AI_SCAN_MIN || 120) * 60e3 && await aiOn()) {
      SCHED.ai = now; const r = await runDoctor();
      if (r.problems && (r.sig !== LASTDOC.sig || now - LASTDOC.t > 6 * 36e5)) { LASTDOC.sig = r.sig; LASTDOC.t = now; notifyAdmins(r.text); }
      else if (!r.problems) LASTDOC.sig = '';
    }
  } catch (e) { console.error('scheduler:', e.message); }
};
if (!E.VERCEL) setInterval(SCH, 60e3);

async function handle(u) {
  const cb = u.callback_query, msg = u.message, from = (cb || msg)?.from; if (!from) return;
  const chat = cb ? cb.message.chat.id : msg.chat.id, text = (msg?.text || msg?.caption || '').trim();
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
  if (/^\/stop\b/.test(text)) { JOB.stop = true; return say(chat, '🛑 Stopping after the current step…'); }
  if (/^\/links\b/.test(text)) return backfill(chat).catch(e => say(chat, '⚠️ ' + H(e.message)));
  const row = m => [{ text: (m.type === 'tv' ? '📺 ' : m.type === 'anime' ? '🎌 ' : '🎬 ') + clean(m.title).slice(0, 46), callback_data: 'm:' + m._id }];
  if (/^\/dbs?\b/.test(text)) {
    const sz = await dbSize();
    return say(chat, '🗄 <b>Databases</b>\n' + SH.map((x, i) => `${x.name}: ${x.conn.readyState === 1 ? `✅ connected · ${sz[i].mb ?? '?'}/${LIMIT} MB${x.full ? ' (full)' : ''}` : '❌ offline' + (x.err ? ' – ' + H(x.err.slice(0, 140)) : '')}`).join('\n') + '\n\nAdd more with MONGODB_URI_2, MONGODB_URI_3 …');
  }
  if (/^\/notice\b/.test(text)) return setNotice(chat, msg, text.replace(/^\/notice\s*/, '').trim()).catch(e => say(chat, '⚠️ ' + H(e.message)));
  if (/^\/ai\b/.test(text)) {
    const a = (text.split(/\s+/)[1] || '').toLowerCase();
    if (a === 'on' || a === 'off') { await Setting.findOneAndUpdate({ k: 'ai' }, { v: a }, { upsert: true }); return say(chat, `🤖 AI site doctor <b>${a.toUpperCase()}</b>. It checks the whole site every ${+E.AI_SCAN_MIN || 120} min and messages you only when something is wrong.${a === 'on' && !E.GEMINI_API_KEY ? '\n⚠️ GEMINI_API_KEY is not set, so reports will be plain text (no AI).' : ''}`); }
    say(chat, '🩺 Checking the whole site… about 1 minute.');
    return runDoctor().then(r => say(chat, r.text)).catch(e => say(chat, '⚠️ ' + H(e.message)));
  }
  if (/^\/auto\b/.test(text)) {
    const [, a, b] = text.toLowerCase().split(/\s+/);
    if (a === 'on' || a === 'off') { await setAuto({ on: a === 'on' }); SCHED.sync = Date.now() - ((+E.AUTO_SYNC_MIN || 60) - 2) * 60e3; return say(chat, `🔄 Auto-sync <b>${a.toUpperCase()}</b>${a === 'on' ? '. First run in about 2 minutes.' : '.'}`); }
    if (a === 'anime' && (b === 'on' || b === 'off')) { await setAuto({ anime: b === 'on' }); return say(chat, `🎌 Auto-sync for anime <b>${b.toUpperCase()}</b> (anime uses the paid API).`); }
    if (a === 'now') return runAuto(chat);
    const c = await autoCfg();
    return say(chat, `🔄 <b>Auto-sync</b>: ${c.on ? 'ON' : 'OFF'}\n🎌 Anime: ${c.anime ? 'ON' : 'OFF'}\n⏱ Every ${+E.AUTO_SYNC_MIN || 60} min · max ${+E.AUTO_ADD_MAX || 15} new titles and ${+E.AUTO_EP_MAX || 12} series checked per run\n🕒 Last run: ${LASTAUTO.t ? Math.round((Date.now() - LASTAUTO.t) / 60000) + ' min ago' : 'not yet'}\n\n/auto on · /auto off · /auto now · /auto anime on|off`);
  }
  if (/^\/stats\b/.test(text)) return act({ chat }, s, 'st').catch(e => say(chat, '⚠️ ' + H(e.message)));
  if (/^\/recent\b/.test(text)) return say(chat, '🆕 <b>Recently added</b>', [...(await Movie.find({}, 'title type').sort({ createdAt: -1 }).limit(10)).map(row), BACK]);
  if (/^\/find\b/.test(text)) {
    const w = text.replace(/^\/find\s*/, '').trim(); if (!w) return say(chat, 'Use: <code>/find avatar</code>');
    const L = await Movie.find({ title: rx(w) }, 'title type').sort({ createdAt: -1 }).limit(10);
    return say(chat, L.length ? `🔎 In your library: <b>${H(w)}</b>` : 'Nothing in your library matches that.', [...L.map(row), BACK]);
  }
  if (/^\/missing\b/.test(text)) {
    const f = { type: { $nin: ['tv', 'anime'] }, 'downloads.0': { $exists: false }, 'pages.0': { $exists: false } };
    const [n, L] = await Promise.all([Movie.countDocuments(f), Movie.find(f, 'title type').limit(10)]);
    return say(chat, `🔗 Movies without download links: <b>${n}</b>${n ? '\nSend /links to fetch them.' : ''}`, [...L.map(row), BACK]);
  }
  if (/^\/dupes\b/.test(text)) {
    const G = new Map(); for (const m of await Movie.find({}, 'title type').sort({ createdAt: 1 })) { const k = nk(m.title); if (k) G.set(k, [...(G.get(k) || []), m]); }
    const D = [...G.values()].filter(g => g.length > 1);
    return say(chat, `♊ Duplicate titles: <b>${D.length}</b>\nThe newer copy of each is listed. Tap it to open and delete.`, [...D.slice(0, 12).map(g => row(g[g.length - 1])), BACK]);
  }
  if (/^\/ads\b/.test(text)) {
    const a = (text.split(/\s+/)[1] || '').toLowerCase();
    if (a !== 'on' && a !== 'off') return say(chat, `📢 Ads are <b>${await adsOn() ? 'ON' : 'OFF'}</b>.\nUse <code>/ads on</code> or <code>/ads off</code>`);
    await Setting.findOneAndUpdate({ k: 'ads' }, { v: a }, { upsert: true }); CFG.t = 0;
    return say(chat, `📢 Ads turned <b>${a.toUpperCase()}</b>. The site picks it up within a minute.`);
  }
  if (/^\/clearcache\b/.test(text)) { await Cache.deleteMany({}); linkCache.clear(); aCache.clear(); SM.t = 0; CFG.t = 0; BL.t = 0; return say(chat, '🧹 Cache cleared.'); }
  if (/^\/freespace\b/.test(text)) {
    const before = await dbSize(), dropped = await dropPosters(); await Cache.deleteMany({}).catch(() => {}); IMGC.clear(); const after = await dbSize();
    warmPosters().catch(() => {});
    return say(chat, `💾 ${dbLine(before)}\n→ ${dbLine(after)}\n${dropped ? '🗑 Saved posters removed. They are being saved again as small copies.' : 'No saved posters found.'}\nNeed more room? Add MONGODB_URI_2 (another free cluster).`);
  }
  if (/^\/dedupe\b/.test(text)) {
    const { groups, del } = await dupeInfo();
    if (!del.length) return say(chat, '✅ No duplicate titles found.');
    return say(chat, `♊ Found <b>${groups}</b> duplicated titles (<b>${del.length}</b> extra copies).\nThe best copy of each (featured, most links, oldest) is kept. Remove the rest?`, [[{ text: '✅ Remove duplicates', callback_data: 'dd' }, { text: 'Cancel', callback_data: 'menu' }]]);
  }
  if (/^\/failed\b/.test(text)) {
    const L = await Fail.find().limit(5000), why = {}; L.forEach(x => { const k = x.reason || 'error'; why[k] = (why[k] || 0) + 1; });
    return say(chat, L.length ? `⚠️ Failed titles saved: <b>${L.length}</b>\n${Object.entries(why).sort((a, b) => b[1] - a[1]).slice(0, 5).map(([k, v]) => H(k) + ' ×' + v).join('\n')}` : '✅ No failed titles.', L.length ? [[{ text: '🔁 Retry all', callback_data: 'rt' }], BACK] : [BACK]);
  }
  if (/^\/retry\b/.test(text)) return retryFailed(chat).catch(e => say(chat, '⚠️ ' + H(e.message)));
  if (/^\/tv\b/.test(text)) {
    const n = +((text.match(/^\/tv\s+(\d+)/i) || [])[1]);
    if (!n) return say(chat, 'Use: <code>/tv 100</code> (adds 100 new TV series)');
    return bulk(chat, [SRC_TV], n).catch(e => say(chat, '⚠️ ' + H(e.message)));
  }
  if (/^\/posters\b/.test(text)) { say(chat, '🖼 Saving missing posters into MongoDB (small copies)…'); warmPosters().then(n => say(chat, `🖼 Done. Saved ${n} new posters.`)).catch(e => say(chat, '⚠️ ' + H(e.message))); return; }
  if (/^\/blocklink\b/.test(text)) {
    const w = text.replace(/^\/blocklink\s*/, '').trim().toLowerCase(), cur = await blocked();
    if (!w) return say(chat, `🚫 Blocked download servers: ${cur.length ? cur.map(x => '<code>' + H(x) + '</code>').join(', ') : 'none'}\nAdd: <code>/blocklink name</code>\nRemove: <code>/unblock name</code>`);
    await Setting.findOneAndUpdate({ k: 'block' }, { v: JSON.stringify([...new Set([...cur, w])]) }, { upsert: true }); BL.t = 0;
    return say(chat, `🚫 Blocked <code>${H(w)}</code>. Removed from ${await purgeBlocked([w])} movies. It is also skipped when adding new titles.`);
  }
  if (/^\/unblock\b/.test(text)) {
    const w = text.replace(/^\/unblock\s*/, '').trim().toLowerCase(), cur = await blocked();
    await Setting.findOneAndUpdate({ k: 'block' }, { v: JSON.stringify(cur.filter(x => x !== w)) }, { upsert: true }); BL.t = 0;
    return say(chat, `✅ <code>${H(w)}</code> is no longer blocked (removed links are not restored; use Refresh on a movie).`);
  }
  if (/^\/go\b/.test(text)) {
    const g = text.match(/^\/go\s+(\d+)\s*[, ]\s*([a-z]+)/i), key = g && SRC_ALIAS[g[2].toUpperCase()];
    if (!g || !key || +g[1] < 1) return say(chat, 'Use: <code>/go 100,ANIME</code>\nSources: CINESUBZ, SINHALASUB, ANIME');
    return bulk(chat, [{ name: key, ...SRC[key] }], +g[1]).catch(e => say(chat, '⚠️ ' + H(e.message)));
  }
  if (/^\/bulkall\b/.test(text)) {
    const k = SRC_ALIAS[(text.split(/\s+/)[1] || '').toUpperCase()];
    return bulk(chat, (k ? [k] : Object.keys(SRC)).map(n => ({ name: n, ...SRC[n] }))).catch(e => say(chat, '⚠️ ' + H(e.message)));
  }
  if (/^\/bulk\b/.test(text)) {
    const w = text.replace(/^\/bulk\s*/, '').trim();
    return w ? bulk(chat, ['CINESUBZ', 'SINHALASUB'].map(n => ({ name: n, find: SRC[n].find, kw: [w] }))).catch(e => say(chat, '⚠️ ' + H(e.message))) : say(chat, 'Use: <code>/bulk avatar</code>');
  }
  if (/^\/ss\b/.test(text)) {
    const w = text.replace(/^\/ss\s*/, '').trim();
    if (!w) { s.mode = 'ss'; return say(chat, '🇱🇰 Send the movie name (SinhalaSub).'); }
    return doSearch(chat, s, w, 'ss').catch(e => say(chat, '⚠️ ' + H(e.message)));
  }
  if (/^\/(start|menu)\b/.test(text)) return act({ chat }, s, 'menu');
  if (/^\/anime\b/.test(text)) {
    const w = text.replace(/^\/anime\s*/, '').trim();
    if (!w) { s.mode = 'anime'; return say(chat, '🎌 Send the anime name.'); }
    return doSearch(chat, s, w, 'anime').catch(e => say(chat, '⚠️ ' + H(e.message)));
  }
  if (text && !text.startsWith('/') && !msg.photo) { const md = s.mode || 'cine'; s.mode = ''; return doSearch(chat, s, text, md).catch(e => say(chat, '⚠️ ' + H(e.message))); }
}
async function poll() {
  if (!E.TG_BOT_TOKEN) return console.log('Telegram bot off (no TG_BOT_TOKEN)');
  await tg('deleteWebhook');
  tg('setMyCommands', { commands: [{ command: 'menu', description: 'Open menu' }, { command: 'login', description: 'Unlock with PIN' }, { command: 'bulk', description: 'Add all results of a search' }, { command: 'bulkall', description: 'Add as many movies as possible' }, { command: 'links', description: 'Save links for all movies' }, { command: 'anime', description: 'Search and add anime' }, { command: 'ss', description: 'Search SinhalaSub' }, { command: 'go', description: 'Add N new titles, e.g. /go 100,ANIME' }, { command: 'stats', description: 'Site numbers' }, { command: 'notice', description: 'Show a notice on the site' }, { command: 'ai', description: 'AI site doctor' }, { command: 'auto', description: 'Auto-add new titles/episodes' }, { command: 'db', description: 'Database status' }, { command: 'recent', description: 'Last 10 added' }, { command: 'find', description: 'Search your library' }, { command: 'missing', description: 'Movies without links' }, { command: 'dupes', description: 'Duplicate titles' }, { command: 'ads', description: 'Ads on or off' }, { command: 'freespace', description: 'Free database space' }, { command: 'posters', description: 'Save posters in MongoDB' }, { command: 'dedupe', description: 'Remove duplicate titles' }, { command: 'tv', description: 'Add N new TV series' }, { command: 'failed', description: 'Failed adds' }, { command: 'retry', description: 'Retry failed adds' }, { command: 'blocklink', description: 'Hide a download server' }, { command: 'clearcache', description: 'Clear caches' }, { command: 'stop', description: 'Stop running job' }] }); console.log('Telegram bot running'); let off = 0;
  for (;;) {
    const r = await tg('getUpdates', { offset: off, timeout: 30, allowed_updates: ['message', 'callback_query'] });
    if (!r.ok) { await new Promise(x => setTimeout(x, 5000)); continue; }
    for (const u of r.result) { off = u.update_id + 1; handle(u).catch(e => console.error('bot:', e.message)); }
  }
}
if (!E.VERCEL) poll(); // the Telegram bot only runs on an always-on host
if (!E.VERCEL) setTimeout(async () => {
  await dbSize();
  if (!(await Setting.findOne({ k: 'posters_v2' }).catch(() => null))) { // one time: the first full-size poster copies filled the database
    await dropPosters(); await dbSize(); await Setting.findOneAndUpdate({ k: 'posters_v2' }, { v: '1' }, { upsert: true }).catch(() => {});
  }
  await purgeBlocked(await blocked()).catch(() => {});
  warmPosters().catch(e => console.error('posters:', e.message));
}, 20000); // after start: check database sizes, remove blocked servers, save missing posters
if (!E.VERCEL) setInterval(() => dbSize().catch(() => {}), 6e4);
