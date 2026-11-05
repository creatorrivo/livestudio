/* Creator Rivo — live-worker
   Watches Firestore liveStreams/main. When the admin presses "Start", it downloads the
   video once and runs ffmpeg in an endless loop, pushing it to YouTube over RTMP.
   It restarts ffmpeg automatically if it crashes, and stops when the admin presses "Stop". */
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

const RTMP_BASE   = process.env.RTMP_URL || 'rtmp://a.rtmp.youtube.com/live2';
const CACHE_DIR   = process.env.CACHE_DIR || './cache';
const VIDEO_KBPS  = Number(process.env.VIDEO_KBPS || 2500);   // 720p/1080p friendly for YouTube
const PRESET      = process.env.X264_PRESET || 'veryfast';
const COPY_MODE   = process.env.COPY_MODE === '1';             // 1 = no re-encode (very light CPU, needs YouTube-ready H.264/AAC file)
mkdirSync(CACHE_DIR, { recursive: true });

const streamRef = db.doc('liveStreams/main');
const secretRef = db.doc('liveSecrets/main');

let ffmpeg = null;          // current child process
let wantRunning = false;    // what the admin wants
let lastToken = null;       // restartToken we last acted on
let retryTimer = null;
let retryCount = 0;
let busy = false;           // guards start/stop races
let currentCfg = null;

const log = (...a) => console.log(new Date().toISOString(), ...a);
const mask = s => String(s).replace(/[A-Za-z0-9_-]{4}-[A-Za-z0-9_-]{4}-[A-Za-z0-9_-]{4}-[A-Za-z0-9_-]{4}-[A-Za-z0-9_-]{4}/g, '****-****-****-****-****');

async function setStatus(status, statusMessage = ''){
  try{ await streamRef.set({ status, statusMessage, statusAt: FieldValue.serverTimestamp() }, { merge: true }); }
  catch(e){ log('status write failed:', e.message); }
}

/* ---- download the video once and reuse it (looping from local disk is far more reliable than HTTP) ---- */
async function ensureLocalVideo(url){
  const hash = createHash('sha1').update(url).digest('hex').slice(0, 16);
  const file = path.join(CACHE_DIR, `${hash}.mp4`);
  if(existsSync(file) && statSync(file).size > 0) return file;
  log('downloading video...');
  await setStatus('starting', 'Downloading video to the server...');
  const res = await fetch(url);
  if(!res.ok || !res.body) throw new Error(`Video download failed (HTTP ${res.status})`);
  const tmp = file + '.part';
  await pipeline(Readable.fromWeb(res.body), createWriteStream(tmp));
  renameSync(tmp, file);
  // keep the cache small: delete other cached videos
  for(const f of readdirSync(CACHE_DIR)){
    if(f !== path.basename(file)) rmSync(path.join(CACHE_DIR, f), { force: true });
  }
  return file;
}

function ffmpegArgs(file, key){
  const out = `${RTMP_BASE}/${key}`;
  const base = ['-hide_banner', '-loglevel', 'warning', '-re', '-stream_loop', '-1', '-i', file];
  const enc = COPY_MODE
    ? ['-c', 'copy']
    : ['-c:v', 'libx264', '-preset', PRESET, '-tune', 'zerolatency', '-pix_fmt', 'yuv420p',
       '-b:v', `${VIDEO_KBPS}k`, '-maxrate', `${VIDEO_KBPS}k`, '-bufsize', `${VIDEO_KBPS * 2}k`,
       '-g', '60', '-keyint_min', '60', '-sc_threshold', '0',
       '-c:a', 'aac', '-b:a', '128k', '-ar', '44100', '-ac', '2'];
  return [...base, ...enc, '-f', 'flv', out];
}

async function startStream(cfg){
  if(busy) return;
  busy = true;
  clearTimeout(retryTimer);
  try{
    await stopFfmpeg();
    const sec = await secretRef.get();
    const key = sec.exists ? String(sec.data().streamKey || '').trim() : '';
    if(!key) throw new Error('No stream key saved.');
    if(!cfg.videoUrl) throw new Error('No video uploaded.');
    const file = await ensureLocalVideo(cfg.videoUrl);

    log('starting ffmpeg (loop, ' + (COPY_MODE ? 'copy' : 're-encode') + ')');
    await setStatus('starting', 'Connecting to YouTube...');
    ffmpeg = spawn('ffmpeg', ffmpegArgs(file, key), { stdio: ['ignore', 'ignore', 'pipe'] });
    const me = ffmpeg;
    let lastErr = '';
    me.stderr.on('data', d => { lastErr = mask(d.toString()).trim().split('\n').pop(); });
    // If ffmpeg is still alive after 8s, the RTMP connection was accepted.
    setTimeout(() => { if(ffmpeg === me && me.exitCode === null){ retryCount = 0; setStatus('live', 'Streaming'); } }, 8000);
    me.on('error', e => log('ffmpeg spawn error:', e.message));
    me.on('exit', async (code, sig) => {
      if(ffmpeg !== me) return;          // we stopped it ourselves
      ffmpeg = null;
      log(`ffmpeg exited code=${code} signal=${sig} ${lastErr}`);
      if(!wantRunning) return;
      retryCount++;
      const wait = Math.min(60, 5 * retryCount);
      await setStatus('starting', `Stream dropped — retrying in ${wait}s${lastErr ? ' (' + lastErr.slice(0, 120) + ')' : ''}`);
      retryTimer = setTimeout(() => { if(wantRunning && currentCfg) startStream(currentCfg); }, wait * 1000);
    });
  }catch(e){
    log('start failed:', mask(e.message));
    await setStatus('error', mask(e.message));
    wantRunning = false;
  }finally{
    busy = false;
  }
}

function stopFfmpeg(){
  return new Promise(resolve => {
    if(!ffmpeg) return resolve();
    const p = ffmpeg; ffmpeg = null;
    const t = setTimeout(() => p.kill('SIGKILL'), 5000);
    p.once('exit', () => { clearTimeout(t); resolve(); });
    p.kill('SIGTERM');
  });
}

streamRef.onSnapshot(async snap => {
  if(!snap.exists) return;
  const d = snap.data();
  currentCfg = d;
  if(d.desired === 'on'){
    // Act only on a NEW "Start" press (new restartToken), or on the first snapshot after the worker boots.
    // Our own status/heartbeat writes also trigger snapshots, so same-token snapshots are ignored.
    const token = String(d.restartToken || '');
    if(token !== lastToken){
      lastToken = token; wantRunning = true; retryCount = 0;
      startStream(d);
    }
  }else{
    if(wantRunning || ffmpeg){
      wantRunning = false; lastToken = null;
      clearTimeout(retryTimer);
      log('stop requested');
      await stopFfmpeg();
      await setStatus('idle', 'Stopped');
    }else if(d.status && d.status !== 'idle'){
      lastToken = null;
      await setStatus('idle', 'Stopped');
    }
  }
}, err => log('snapshot error:', err.message));

// heartbeat so the admin panel can tell the worker is alive
setInterval(() => { streamRef.set({ heartbeatAt: FieldValue.serverTimestamp() }, { merge: true }).catch(() => {}); }, 20000);
streamRef.set({ heartbeatAt: FieldValue.serverTimestamp() }, { merge: true }).catch(() => {});

for(const sig of ['SIGINT', 'SIGTERM']){
  process.on(sig, async () => {
    wantRunning = false;
    await stopFfmpeg();
    await setStatus('idle', 'Worker stopped').catch(() => {});
    process.exit(0);
  });
}
log('live-worker running. Waiting for commands from the admin panel...');
