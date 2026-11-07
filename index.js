/* Creator Rivo — live-worker (multi-user)
   Every signed-in user can press "Start" in the Live Stream page. This worker watches the Firestore
   collection userStreams and, for each user who wants to be live, downloads THEIR video once and runs
   ffmpeg in an endless loop, pushing it to THEIR YouTube channel with THEIR stream key (userStreamSecrets/{uid}).
   It restarts a dropped stream automatically and stops it when the user (or admin) says so. */
import admin from 'firebase-admin';
import { spawn } from 'node:child_process';
import { createWriteStream, existsSync, mkdirSync, readFileSync, renameSync, rmSync, readdirSync, statSync } from 'node:fs';
import { createHash } from 'node:crypto';
import { pipeline } from 'node:stream/promises';
import { Readable } from 'node:stream';
import path from 'node:path';
import { createServer } from 'node:http';

/* Tiny HTTP server: Render "Web Service" needs an open port, and UptimeRobot pings it to keep the free plan awake. */
createServer((req, res) => { res.writeHead(200, { 'Content-Type': 'text/plain' }); res.end('ok'); })
  .listen(process.env.PORT || 10000, () => console.log('health server on port', process.env.PORT || 10000));

const CREDS = process.env.GOOGLE_APPLICATION_CREDENTIALS || './serviceAccount.json';
if(!existsSync(CREDS)){
  console.error(`Service account file not found: ${CREDS}\nSee README.md (step 2).`);
  process.exit(1);
}
let sa;
try{ sa = JSON.parse(readFileSync(CREDS, 'utf8')); }
catch(e){ console.error('serviceAccount.json is NOT valid JSON (bad copy/paste?):', e.message); process.exit(1); }
if(sa.type !== 'service_account' || !sa.project_id || !sa.private_key){
  console.error('WRONG KEY FILE. Top-level keys found:', Object.keys(sa).join(', '));
  console.error('A correct Firebase service-account key has: type, project_id, private_key_id, private_key, client_email ...');
  process.exit(1);
}
console.log('service account OK for project:', sa.project_id);
admin.initializeApp({ credential: admin.credential.cert(sa) });
const db = admin.firestore();
const FieldValue = admin.firestore.FieldValue;

const RTMP_BASE    = process.env.RTMP_URL || 'rtmp://a.rtmp.youtube.com/live2';
const CACHE_DIR    = process.env.CACHE_DIR || './cache';
const VIDEO_KBPS   = Number(process.env.VIDEO_KBPS || 2500);
const PRESET       = process.env.X264_PRESET || 'veryfast';
const COPY_MODE    = process.env.COPY_MODE === '1';           // 1 = no re-encode (light CPU; video must already be H.264 + AAC)
const MAX_STREAMS  = Number(process.env.MAX_STREAMS || 2);    // how many users can be live at the same time on this server
const MAX_VIDEO_MB = Number(process.env.MAX_VIDEO_MB || 500); // refuse bigger uploads
const KEY_RE = /^[A-Za-z0-9_-]{4}(?:-[A-Za-z0-9_-]{4}){3,5}$/; // YouTube stream-key shape (also keeps odd characters out of the ffmpeg URL)
mkdirSync(CACHE_DIR, { recursive: true });

const log = (...a) => console.log(new Date().toISOString(), ...a);
const mask = s => String(s).replace(/[A-Za-z0-9_-]{4}(?:-[A-Za-z0-9_-]{4}){3,5}/g, '****-****-****-****-****');

/* one state object per user */
const S = new Map();
function st(uid){
  if(!S.has(uid)) S.set(uid, { ffmpeg: null, want: false, token: null, retryTimer: null, retry: 0, busy: false, cfg: null });
  return S.get(uid);
}
const userRef = uid => db.doc(`userStreams/${uid}`);
const activeCount = () => [...S.values()].filter(s => s.ffmpeg || s.busy).length;

async function setStatus(uid, status, statusMessage = ''){
  try{ await userRef(uid).set({ status, statusMessage, statusAt: FieldValue.serverTimestamp() }, { merge: true }); }
  catch(e){ log('status write failed:', e.message); }
}

/* Only accept videos the user uploaded to their OWN folder in our Storage (stops people pointing the server at other URLs). */
function validVideoUrl(uid, url){
  try{
    const u = new URL(url);
    return u.protocol === 'https:' && u.hostname === 'firebasestorage.googleapis.com'
      && decodeURIComponent(u.pathname).includes(`/o/live/${uid}/`);
  }catch{ return false; }
}

function purgeCache(uid, keep = ''){
  try{
    for(const f of readdirSync(CACHE_DIR)){
      if(f.startsWith(uid + '_') && f !== keep) rmSync(path.join(CACHE_DIR, f), { force: true });
    }
  }catch{}
}

async function ensureLocalVideo(uid, url){
  const hash = createHash('sha1').update(url).digest('hex').slice(0, 16);
  const name = `${uid}_${hash}.mp4`;
  const file = path.join(CACHE_DIR, name);
  if(existsSync(file) && statSync(file).size > 0) return file;
  log(uid, 'downloading video...');
  await setStatus(uid, 'starting', 'Downloading your video to the server...');
  const res = await fetch(url);
  if(!res.ok || !res.body) throw new Error(`Video download failed (HTTP ${res.status})`);
  const len = Number(res.headers.get('content-length') || 0);
  if(len > MAX_VIDEO_MB * 1048576) throw new Error(`Video is too large (max ${MAX_VIDEO_MB} MB).`);
  const tmp = file + '.part';
  let n = 0;
  try{
    await pipeline(Readable.fromWeb(res.body), async function*(src){
      for await(const chunk of src){
        n += chunk.length;
        if(n > MAX_VIDEO_MB * 1048576) throw new Error(`Video is too large (max ${MAX_VIDEO_MB} MB).`);
        yield chunk;
      }
    }, createWriteStream(tmp));
  }catch(e){ rmSync(tmp, { force: true }); throw e; }
  renameSync(tmp, file);
  purgeCache(uid, name);
  return file;
}

function ffmpegArgs(file, key){
  const base = ['-hide_banner', '-loglevel', 'warning', '-re', '-stream_loop', '-1', '-i', file];
  const enc = COPY_MODE
    ? ['-c', 'copy']
    : ['-c:v', 'libx264', '-preset', PRESET, '-tune', 'zerolatency', '-pix_fmt', 'yuv420p',
       '-b:v', `${VIDEO_KBPS}k`, '-maxrate', `${VIDEO_KBPS}k`, '-bufsize', `${VIDEO_KBPS * 2}k`,
       '-g', '60', '-keyint_min', '60', '-sc_threshold', '0',
       '-c:a', 'aac', '-b:a', '128k', '-ar', '44100', '-ac', '2'];
  return [...base, ...enc, '-f', 'flv', `${RTMP_BASE}/${key}`];
}

function stopFfmpeg(s){
  return new Promise(resolve => {
    if(!s.ffmpeg) return resolve();
    const p = s.ffmpeg; s.ffmpeg = null;
    const t = setTimeout(() => p.kill('SIGKILL'), 5000);
    p.once('exit', () => { clearTimeout(t); resolve(); });
    p.kill('SIGTERM');
  });
}

async function startStream(uid){
  const s = st(uid);
  if(s.busy) return;
  if(!s.ffmpeg && activeCount() >= MAX_STREAMS){
    s.want = false;
    await setStatus(uid, 'error', 'The live server is full right now. Please try again in a few minutes.');
    return;
  }
  s.busy = true;
  clearTimeout(s.retryTimer);
  try{
    await stopFfmpeg(s);
    const settings = await db.doc('settings/liveStream').get();
    if(settings.exists && settings.data().enabled === false) throw new Error('Live streaming is turned off by the admin right now.');
    const cfg = s.cfg || {};
    if(cfg.blocked) throw new Error('Blocked by admin');
    if(!cfg.videoUrl) throw new Error('Please upload a video first.');
    if(!validVideoUrl(uid, cfg.videoUrl)) throw new Error('Video link is not valid. Please upload the video again.');
    const sec = await db.doc(`userStreamSecrets/${uid}`).get();
    const key = sec.exists ? String(sec.data().streamKey || '').trim() : '';
    if(!key) throw new Error('Please enter your YouTube stream key.');
    if(!KEY_RE.test(key)) throw new Error('That does not look like a YouTube stream key (it looks like xxxx-xxxx-xxxx-xxxx-xxxx).');
    const file = await ensureLocalVideo(uid, cfg.videoUrl);
    if(!s.want) return;                       // user pressed Stop while we were downloading

    log(uid, 'starting ffmpeg (loop, ' + (COPY_MODE ? 'copy' : 're-encode') + ')');
    await setStatus(uid, 'starting', 'Connecting to YouTube...');
    const child = spawn('ffmpeg', ffmpegArgs(file, key), { stdio: ['ignore', 'ignore', 'pipe'] });
    s.ffmpeg = child;
    let lastErr = '';
    child.stderr.on('data', d => { lastErr = mask(d.toString()).trim().split('\n').pop(); });
    setTimeout(() => { if(s.ffmpeg === child && child.exitCode === null){ s.retry = 0; setStatus(uid, 'live', 'You are live'); userRef(uid).set({ startedAt: FieldValue.serverTimestamp() }, { merge: true }).catch(() => {}); } }, 8000);
    child.on('error', e => log(uid, 'ffmpeg spawn error:', e.message));
    child.on('exit', async (code, sig) => {
      if(s.ffmpeg !== child) return;           // we stopped it ourselves
      s.ffmpeg = null;
      log(uid, `ffmpeg exited code=${code} signal=${sig} ${lastErr}`);
      if(!s.want) return;
      s.retry++;
      const wait = Math.min(60, 5 * s.retry);
      await setStatus(uid, 'starting', `Stream dropped — retrying in ${wait}s${lastErr ? ' (' + lastErr.slice(0, 120) + ')' : ''}`);
      s.retryTimer = setTimeout(() => { if(s.want) startStream(uid); }, wait * 1000);
    });
  }catch(e){
    log(uid, 'start failed:', mask(e.message));
    s.want = false;
    await setStatus(uid, 'error', mask(e.message));
  }finally{
    s.busy = false;
  }
}

async function handle(uid, d){
  const s = st(uid);
  s.cfg = d;
  if(d.blocked){
    if(s.want || s.ffmpeg){ s.want = false; s.token = null; clearTimeout(s.retryTimer); await stopFfmpeg(s); purgeCache(uid); }
    if(d.status !== 'error' || d.statusMessage !== 'Blocked by admin') await setStatus(uid, 'error', 'Blocked by admin');
    return;
  }
  if(d.desired === 'on'){
    // Act only on a NEW "Start" press (new restartToken) or the first snapshot after the worker boots.
    // Our own status/heartbeat writes also trigger snapshots; those carry the same token and are ignored.
    const token = String(d.restartToken || '');
    if(token !== s.token){ s.token = token; s.want = true; s.retry = 0; startStream(uid); }
  }else if(s.want || s.ffmpeg){
    s.want = false; s.token = null;
    clearTimeout(s.retryTimer);
    log(uid, 'stop requested');
    await stopFfmpeg(s);
    purgeCache(uid);
    await setStatus(uid, 'idle', 'Stopped');
  }else if(d.status && d.status !== 'idle'){
    s.token = null;
    await setStatus(uid, 'idle', 'Stopped');
  }
}

db.collection('userStreams').onSnapshot(snap => {
  for(const ch of snap.docChanges()){
    const uid = ch.doc.id;
    if(ch.type === 'removed'){
      const s = st(uid); s.want = false; clearTimeout(s.retryTimer);
      stopFfmpeg(s).then(() => purgeCache(uid)); S.delete(uid);
      continue;
    }
    handle(uid, ch.doc.data()).catch(e => log(uid, 'handle error:', e.message));
  }
}, err => log('snapshot error:', err.message));

/* heartbeats: per running stream (user page shows "server online") + one global doc for the admin panel */
setInterval(() => {
  let running = 0;
  for(const [uid, s] of S){
    if(s.ffmpeg){ running++; userRef(uid).set({ heartbeatAt: FieldValue.serverTimestamp() }, { merge: true }).catch(() => {}); }
  }
  db.doc('workerStatus/main').set({ heartbeatAt: FieldValue.serverTimestamp(), running, max: MAX_STREAMS }, { merge: true }).catch(() => {});
}, 20000);
db.doc('workerStatus/main').set({ heartbeatAt: FieldValue.serverTimestamp(), running: 0, max: MAX_STREAMS }, { merge: true }).catch(() => {});

for(const sig of ['SIGINT', 'SIGTERM']){
  process.on(sig, async () => {
    for(const [uid, s] of S){
      if(s.ffmpeg){ s.want = false; await setStatus(uid, 'starting', 'Server restarting — your stream will resume shortly.'); await stopFfmpeg(s); }
    }
    process.exit(0);
  });
}
log(`live-worker running (max ${MAX_STREAMS} streams). Waiting for users to press Start...`);
