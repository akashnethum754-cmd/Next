require('dotenv').config();
const express = require('express'), mongoose = require('mongoose'), jwt = require('jsonwebtoken'), path = require('path');
const E = process.env, app = express();
app.use(express.json());

mongoose.connect(E.MONGODB_URI).then(() => console.log('MongoDB connected')).catch(e => console.error('MongoDB error:', e.message));

const Movie = mongoose.model('Movie', new mongoose.Schema({
  sourceUrl: { type: String, unique: true }, type: { type: String, default: 'movie' },
  title: String, year: String, poster: String, overview: String, rating: String, runtime: String,
  director: String, country: String, cast: [String], featured: { type: Boolean, default: false },
  downloads: [{ label: String, url: String }],
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

async function build(link) {
  if (isTv(link)) {
    const d = (await cine('/cinesubz/tvshow', { url: link })).data;
    const episodes = (d.episodes?.list || []).map(ep => {
      const parts = String(ep.number || '1').split(/\s*-\s*/);
      return { season: parts.length > 1 ? parseInt(parts[0]) || 1 : 1, number: parts[parts.length - 1], title: ep.title || 'Episode', url: ep.url };
    }).filter(e => e.url);
    return { sourceUrl: link, type: 'tv', title: d.title, year: d.year, poster: d.poster, overview: d.description, rating: d.rating?.score ? String(d.rating.score) : '', episodes };
  }
  const d = (await cine('/cinesubz/details', { url: link })).data;
  return {
    sourceUrl: link, type: 'movie', title: d.title, year: d.year, poster: d.poster, overview: d.description,
    rating: d.imdb_rating ? d.imdb_rating + '/10' : '', runtime: d.runtime, director: d.director, country: d.country,
    cast: Array.isArray(d.cast) ? d.cast : d.cast ? [d.cast] : [],
    downloads: (d.downloads || []).filter(x => x && x.quality && x.url).map(x => ({ label: x.quality, url: x.url }))
  };
}
const allowed = new Set(); // episode links we handed out
const auth = (req, res, next) => {
  try { jwt.verify((req.headers.authorization || '').replace('Bearer ', ''), E.JWT_SECRET); next(); }
  catch { res.status(401).json({ error: 'Login required' }); }
};
const wrap = fn => (req, res) => fn(req, res).catch(e => res.status(500).json({ error: e.message }));

// ---------- public ----------
app.get('/api/movies', wrap(async (req, res) => {
  const q = (req.query.q || '').trim(), page = Math.max(1, +req.query.page || 1);
  const f = q ? { title: new RegExp(q.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'), 'i') } : {};
  const [items, total] = await Promise.all([
    Movie.find(f, '-downloads -cast -episodes').sort({ featured: -1, createdAt: -1 }).skip((page - 1) * 24).limit(24), Movie.countDocuments(f)]);
  res.json({ items, total, pages: Math.ceil(total / 24) });
}));
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

// ---------- admin ----------
app.post('/api/login', (req, res) => {
  const { user, pass } = req.body || {};
  if (user === E.ADMIN_USER && pass === E.ADMIN_PASS) return res.json({ token: jwt.sign({ a: 1 }, E.JWT_SECRET, { expiresIn: '12h' }) });
  res.status(401).json({ error: 'Wrong username or password' });
});
app.get('/api/admin/search', auth, wrap(async (req, res) => {
  const d = await cine('/cinesubz/search', { query: req.query.q || '' });
  res.json((d.results || []).slice(0, 25).map(x => ({ title: x.title, link: x.link, type: isTv(x.link) ? 'tv' : 'movie', poster: x.poster || x.image || x.thumbnail || x.img || '' })));
}));
app.get('/api/admin/movies', auth, wrap(async (req, res) => res.json(await Movie.find().sort({ createdAt: -1 }))));
app.post('/api/admin/import', auth, wrap(async (req, res) => {
  const doc = await build(req.body.link);
  res.json(await Movie.findOneAndUpdate({ sourceUrl: doc.sourceUrl }, doc, { upsert: true, new: true }));
}));
app.post('/api/admin/movies/:id/refresh', auth, wrap(async (req, res) => {
  const m = await Movie.findById(req.params.id);
  res.json(await Movie.findByIdAndUpdate(m._id, await build(m.sourceUrl), { new: true }));
}));
app.put('/api/admin/movies/:id', auth, wrap(async (req, res) => res.json(await Movie.findByIdAndUpdate(req.params.id, { featured: !!req.body.featured }, { new: true }))));
app.delete('/api/admin/movies/:id', auth, wrap(async (req, res) => { await Movie.findByIdAndDelete(req.params.id); res.json({ ok: 1 }); }));

app.get('*', (req, res) => res.sendFile(path.join(__dirname, 'index.html')));
app.listen(E.PORT || 3000, () => console.log('SHAGGY MOVIES running'));
