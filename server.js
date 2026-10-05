require('dotenv').config();
const express = require('express'), mongoose = require('mongoose'), jwt = require('jsonwebtoken'), path = require('path');
const E = process.env, app = express();
app.use(express.json());

mongoose.connect(E.MONGODB_URI).then(() => console.log('MongoDB connected')).catch(e => console.error('MongoDB error:', e.message));

const Movie = mongoose.model('Movie', new mongoose.Schema({
  tmdbId: { type: Number, unique: true }, title: String, year: String, poster: String, backdrop: String,
  overview: String, rating: Number, runtime: Number, imdbId: String, genres: [String], cast: [String],
  trailer: String, featured: { type: Boolean, default: false }, downloads: [{ label: String, url: String }]
}, { timestamps: true }));

const IMG = 'https://image.tmdb.org/t/p/';
async function tmdb(p, q = '') {
  const r = await fetch(`https://api.themoviedb.org/3${p}?api_key=${E.TMDB_API_KEY}&language=en-US${q}`);
  if (!r.ok) throw new Error('TMDB error ' + r.status);
  return r.json();
}
// Download API: sends title+year, accepts many JSON shapes
async function findLinks(title, year, imdbId) {
  if (!E.DOWNLOAD_API_URL) return [];
  try {
    const u = new URL(E.DOWNLOAD_API_URL);
    u.searchParams.set('query', title); u.searchParams.set('q', title);
    if (year) u.searchParams.set('year', year);
    if (imdbId) u.searchParams.set('imdb', imdbId);
    const r = await fetch(u, { headers: { Authorization: 'Bearer ' + E.DOWNLOAD_API_KEY, 'x-api-key': E.DOWNLOAD_API_KEY || '' } });
    const j = await r.json();
    const arr = Array.isArray(j) ? j : j.links || j.downloads || j.data || j.results || [];
    return arr.map(x => ({ label: x.label || x.quality || x.name || 'Download', url: x.url || x.link || x.href })).filter(x => x.url);
  } catch (e) { console.error('Download API:', e.message); return []; }
}
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
    Movie.find(f, '-downloads -cast').sort({ featured: -1, createdAt: -1 }).skip((page - 1) * 24).limit(24), Movie.countDocuments(f)]);
  res.json({ items, total, pages: Math.ceil(total / 24) });
}));
app.get('/api/movies/:id', wrap(async (req, res) => {
  const m = await Movie.findById(req.params.id).lean();
  if (!m) return res.status(404).json({ error: 'Not found' });
  if (E.OMDB_API_KEY && m.imdbId) {
    try {
      const o = await (await fetch(`https://www.omdbapi.com/?apikey=${E.OMDB_API_KEY}&i=${m.imdbId}`)).json();
      m.extra = { imdb: o.imdbRating, rated: o.Rated, director: o.Director, awards: o.Awards };
    } catch {}
  }
  res.json(m);
}));

// ---------- admin ----------
app.post('/api/login', (req, res) => {
  const { user, pass } = req.body || {};
  if (user === E.ADMIN_USER && pass === E.ADMIN_PASS) return res.json({ token: jwt.sign({ a: 1 }, E.JWT_SECRET, { expiresIn: '12h' }) });
  res.status(401).json({ error: 'Wrong username or password' });
});
app.get('/api/admin/search', auth, wrap(async (req, res) => {
  const d = await tmdb('/search/movie', '&query=' + encodeURIComponent(req.query.q || ''));
  res.json(d.results.slice(0, 12).map(x => ({ id: x.id, title: x.title, year: (x.release_date || '').slice(0, 4), poster: x.poster_path ? IMG + 'w185' + x.poster_path : '' })));
}));
app.get('/api/admin/movies', auth, wrap(async (req, res) => res.json(await Movie.find().sort({ createdAt: -1 }))));
app.post('/api/admin/import', auth, wrap(async (req, res) => {
  const d = await tmdb(`/movie/${+req.body.tmdbId}`, '&append_to_response=credits,videos');
  const year = (d.release_date || '').slice(0, 4), yt = (d.videos?.results || []).find(v => v.site === 'YouTube' && v.type === 'Trailer');
  const doc = {
    tmdbId: d.id, title: d.title, year, overview: d.overview, rating: Math.round(d.vote_average * 10) / 10, runtime: d.runtime,
    imdbId: d.imdb_id, genres: d.genres.map(g => g.name), cast: (d.credits?.cast || []).slice(0, 8).map(c => c.name),
    poster: d.poster_path ? IMG + 'w500' + d.poster_path : '', backdrop: d.backdrop_path ? IMG + 'w1280' + d.backdrop_path : '',
    trailer: yt ? 'https://www.youtube.com/watch?v=' + yt.key : ''
  };
  const old = await Movie.findOne({ tmdbId: d.id });
  doc.downloads = old?.downloads?.length ? old.downloads : await findLinks(d.title, year, d.imdb_id);
  res.json(await Movie.findOneAndUpdate({ tmdbId: d.id }, doc, { upsert: true, new: true }));
}));
app.put('/api/admin/movies/:id', auth, wrap(async (req, res) => {
  const { downloads, featured } = req.body, u = {};
  if (downloads) u.downloads = downloads.filter(x => x.url);
  if (typeof featured === 'boolean') u.featured = featured;
  res.json(await Movie.findByIdAndUpdate(req.params.id, u, { new: true }));
}));
app.post('/api/admin/movies/:id/links', auth, wrap(async (req, res) => {
  const m = await Movie.findById(req.params.id);
  m.downloads = await findLinks(m.title, m.year, m.imdbId); await m.save(); res.json(m);
}));
app.delete('/api/admin/movies/:id', auth, wrap(async (req, res) => { await Movie.findByIdAndDelete(req.params.id); res.json({ ok: 1 }); }));

app.get('*', (req, res) => res.sendFile(path.join(__dirname, 'index.html')));
app.listen(E.PORT || 3000, () => console.log('SHAGGY MOVIES running'));
