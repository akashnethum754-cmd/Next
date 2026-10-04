import express from 'express';
import { MongoClient, ObjectId } from 'mongodb';
import crypto from 'node:crypto';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const { MONGODB_URI, ADMIN_USER, ADMIN_PASSWORD, SESSION_SECRET, DB_NAME = 'shaggymovies', PORT = 3000 } = process.env;
for (const [k, v] of Object.entries({ MONGODB_URI, ADMIN_USER, ADMIN_PASSWORD, SESSION_SECRET })) {
  if (!v) { console.error(`Missing environment variable: ${k}`); process.exit(1); }
}

/* ---------- demo data (inserted once when the database is empty) ---------- */
const T = 'https://image.tmdb.org/t/p/w500/';
const SEED = [
  ['Shadow Protocol', 2024, 8.7, 'Action', '8cdWjvZQUExUUTzyp4t6EDMubfO.jpg', 'An elite agent uncovers a conspiracy that threatens the world.', true],
  ['Midnight Echo', 2023, 7.9, 'Thriller', 'q719jXXEzOoYaps6babgKnONONX.jpg', 'A detective races against time to solve a string of murders.'],
  ['Neon Skyline', 2025, 8.2, 'Sci-Fi', '1E5baAaEse26fej7uHcjOgEE2t2.jpg', 'In a cyberpunk future, a hacker fights a mega-corporation.'],
  ['The Last Ronin', 2024, 9.1, 'Anime', 'rktDFPbfHfUbArZ6OOOKsXcv0Bm.jpg', 'A lone warrior seeks vengeance in a ravaged land.'],
  ['Crimson Tide Rising', 2023, 7.5, 'Series', '1XS1oqL89opfnbLl8WnZY1O1uJx.jpg', 'A naval crew faces a deadly storm and an unseen enemy.'],
  ['Ghost Frequency', 2025, 8.4, 'Horror', 'u3bZgnGQ9T01sWNhyveQz0wH0Hl.jpg', 'A radio host discovers a broadcast from beyond the grave.'],
  ['Broken Compass', 2022, 7.2, 'Adventure', '6DrHO1jr3qVrViUO6s6kFiAGM7.jpg', 'Explorers search for a lost city in the Amazon.'],
  ['Silent Vengeance', 2024, 8.0, 'Action', 'pFlaoHTZeyNkG83vxsAJiGzfSsa.jpg', 'A retired assassin is pulled back for one last job.'],
  ['Frozen Horizon', 2023, 7.7, 'Sci-Fi', '1E5baAaEse26fej7uHcjOgEE2t2.jpg', 'A research team in Antarctica makes a chilling discovery.'],
  ['Tokyo Drift Kings', 2024, 8.1, 'Action', '8cdWjvZQUExUUTzyp4t6EDMubfO.jpg', 'Street racers battle for glory in the neon streets of Tokyo.'],
  ['Whispers in the Dark', 2022, 7.4, 'Horror', 'u3bZgnGQ9T01sWNhyveQz0wH0Hl.jpg', 'A family moves into a house with a terrifying secret.'],
  ["Samurai's Path", 2025, 8.9, 'Anime', 'rktDFPbfHfUbArZ6OOOKsXcv0Bm.jpg', 'A young samurai must choose between honor and survival.'],
].map(([title, year, rating, category, img, desc, featured = false], i) =>
  ({ title, year, rating, category, poster: T + img, videoUrl: '', desc, featured, createdAt: new Date(Date.now() - i * 1000) }));

/* ---------- auth: signed, expiring token (no extra packages) ---------- */
const sign = (d) => crypto.createHmac('sha256', SESSION_SECRET).update(d).digest('base64url');
const makeToken = () => { const p = Buffer.from(JSON.stringify({ exp: Date.now() + 12 * 3600e3 })).toString('base64url'); return `${p}.${sign(p)}`; };
function validToken(t = '') {
  const [p, s] = t.split('.');
  if (!p || !s) return false;
  const a = Buffer.from(s), b = Buffer.from(sign(p));
  if (a.length !== b.length || !crypto.timingSafeEqual(a, b)) return false;
  try { return JSON.parse(Buffer.from(p, 'base64url').toString()).exp > Date.now(); } catch { return false; }
}
const hash = (s) => crypto.createHash('sha256').update(String(s)).digest();
const same = (a, b) => crypto.timingSafeEqual(hash(a), hash(b));
const requireAdmin = (req, res, next) =>
  validToken((req.headers.authorization || '').replace(/^Bearer /, '')) ? next() : res.status(401).json({ error: 'Please log in again' });

const attempts = new Map(); // ip -> { n, reset }
function limited(ip) {
  const now = Date.now(), e = attempts.get(ip);
  if (!e || e.reset < now) { attempts.set(ip, { n: 1, reset: now + 15 * 60e3 }); return false; }
  return ++e.n > 10;
}

/* ---------- validation ---------- */
const str = (v, max) => String(v ?? '').trim().slice(0, max);
const url = (v) => { const s = str(v, 500); return /^https?:\/\//i.test(s) ? s : ''; };
function clean(b = {}) {
  const title = str(b.title, 120);
  if (!title) return { error: 'Title is required' };
  const year = Number(b.year), rating = Number(b.rating);
  if (!Number.isInteger(year) || year < 1888 || year > 2100) return { error: 'Invalid year' };
  if (!(rating >= 0 && rating <= 10)) return { error: 'Rating must be between 0 and 10' };
  const poster = url(b.poster);
  if (!poster) return { error: 'Poster must be a valid http(s) link' };
  return { doc: { title, year, rating, category: str(b.category, 40) || 'Movie', poster, videoUrl: url(b.videoUrl), desc: str(b.desc, 1000), featured: b.featured === true } };
}
const out = ({ _id, createdAt, ...rest }) => ({ id: _id.toString(), ...rest });

/* ---------- app ---------- */
const client = new MongoClient(MONGODB_URI);
await client.connect();
const movies = client.db(DB_NAME).collection('movies');
if ((await movies.countDocuments()) === 0) await movies.insertMany(SEED);

const app = express();
app.set('trust proxy', 1);
app.use(express.json({ limit: '100kb' }));
app.use((_, res, next) => {
  res.set({ 'X-Content-Type-Options': 'nosniff', 'X-Frame-Options': 'DENY', 'Referrer-Policy': 'strict-origin-when-cross-origin' });
  next();
});
const wrap = (fn) => (req, res, next) => fn(req, res).catch(next);
const oid = (id) => (ObjectId.isValid(id) ? new ObjectId(id) : null);

app.get('/api/movies', wrap(async (_, res) => {
  res.json((await movies.find().sort({ createdAt: -1 }).toArray()).map(out));
}));

app.post('/api/login', (req, res) => {
  if (limited(req.ip)) return res.status(429).json({ error: 'Too many attempts. Try again in 15 minutes.' });
  const { username = '', password = '' } = req.body ?? {};
  const ok = same(username, ADMIN_USER) & same(password, ADMIN_PASSWORD);
  return ok ? res.json({ token: makeToken() }) : res.status(401).json({ error: 'Wrong username or password' });
});

async function save(req, res, id) {
  const { doc, error } = clean(req.body);
  if (error) return res.status(400).json({ error });
  if (doc.featured) await movies.updateMany(id ? { _id: { $ne: id } } : {}, { $set: { featured: false } });
  if (!id) { const r = await movies.insertOne({ ...doc, createdAt: new Date() }); return res.status(201).json({ id: r.insertedId.toString() }); }
  const r = await movies.updateOne({ _id: id }, { $set: doc });
  return r.matchedCount ? res.json({ ok: true }) : res.status(404).json({ error: 'Not found' });
}
app.post('/api/movies', requireAdmin, wrap((req, res) => save(req, res, null)));
app.put('/api/movies/:id', requireAdmin, wrap((req, res) => {
  const id = oid(req.params.id);
  return id ? save(req, res, id) : res.status(404).json({ error: 'Not found' });
}));
app.delete('/api/movies/:id', requireAdmin, wrap(async (req, res) => {
  const id = oid(req.params.id);
  if (!id) return res.status(404).json({ error: 'Not found' });
  await movies.deleteOne({ _id: id });
  res.json({ ok: true });
}));

const here = dirname(fileURLToPath(import.meta.url));
app.get('/favicon.ico', (_, res) => res.status(204).end());
app.get('/', (_, res) => res.sendFile(join(here, 'index.html')));
app.use((_, res) => res.status(404).json({ error: 'Not found' }));
app.use((err, _req, res, _next) => { console.error(err); res.status(500).json({ error: 'Server error' }); });

app.listen(PORT, () => console.log(`SHAGGY MOVIES running on ${PORT}`));
