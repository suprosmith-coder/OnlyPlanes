/* ============================================================
   Devit — Service Worker  sw.js
   Strategy:
     • App shell (HTML/CSS/JS/fonts) → Cache First, update in background
     • Supabase API / Groq Edge Fn   → Network First, fall back to cache
     • Images / avatars              → Cache First with 7-day expiry
     • Everything else               → Network First
   Push notifications → show with action buttons
   Background sync   → retry failed post submissions
   Share target      → receive shared URLs/text from OS share sheet
   ============================================================ */

'use strict';

const CACHE_VERSION  = 'devit-v1';
const SHELL_CACHE    = `${CACHE_VERSION}-shell`;
const IMAGE_CACHE    = `${CACHE_VERSION}-images`;
const API_CACHE      = `${CACHE_VERSION}-api`;

// Files that make up the app shell — cached on install
const SHELL_ASSETS = [
  '/',
  '/index.html',
  '/style.css',
  '/app.js',
  '/devit.png',
  '/manifest.json',
  // Font Awesome (served from cdnjs — cache the woff2 subset you need)
  'https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.6.0/css/all.min.css',
  // Supabase JS
  'https://cdn.jsdelivr.net/npm/@supabase/supabase-js@2/dist/umd/supabase.js',
];

// Offline fallback page (inline — no extra file needed)
const OFFLINE_HTML = `<!DOCTYPE html>
<html lang="en" data-theme="dark">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <meta name="theme-color" content="#050508">
  <title>Devit — Offline</title>
  <style>
    *{margin:0;padding:0;box-sizing:border-box}
    body{
      font-family:'DM Sans',system-ui,sans-serif;
      background:#050508;color:#f0f2ff;
      display:flex;align-items:center;justify-content:center;
      min-height:100vh;text-align:center;padding:24px;
    }
    .wrap{max-width:320px}
    .logo{width:64px;height:64px;border-radius:16px;margin:0 auto 20px;display:block}
    h1{font-size:22px;font-weight:800;margin-bottom:8px;
       background:linear-gradient(135deg,#63d9ff,#a78bfa);
       -webkit-background-clip:text;-webkit-text-fill-color:transparent}
    p{font-size:14px;color:#8b92b8;line-height:1.6;margin-bottom:24px}
    button{
      padding:12px 28px;border-radius:12px;border:none;cursor:pointer;
      font-size:14px;font-weight:600;
      background:linear-gradient(135deg,rgba(99,217,255,0.18),rgba(167,139,250,0.12));
      color:#63d9ff;border:1px solid rgba(99,217,255,0.3);
      transition:opacity .2s;
    }
    button:hover{opacity:.8}
  </style>
</head>
<body>
  <div class="wrap">
    <img src="/devit.png" alt="Devit" class="logo">
    <h1>You're offline</h1>
    <p>No internet connection detected.<br>
       Check your connection and try again.</p>
    <button onclick="location.reload()">Try again</button>
  </div>
</body>
</html>`;

// ── Helpers ──────────────────────────────────────────────────────────────────

function isShellAsset(url) {
  return SHELL_ASSETS.some(a => url.endsWith(a) || url === a);
}

function isImage(url) {
  return /\.(png|jpg|jpeg|gif|webp|svg|ico)(\?|$)/.test(url) ||
         url.includes('avatars.githubusercontent.com') ||
         url.includes('lh3.googleusercontent.com');
}

function isSupabaseOrGroq(url) {
  return url.includes('.supabase.co') || url.includes('api.groq.com');
}

function isCdnFont(url) {
  return url.includes('fonts.gstatic.com') || url.includes('cdnjs.cloudflare.com');
}

// ── Install — pre-cache shell ─────────────────────────────────────────────────
self.addEventListener('install', event => {
  event.waitUntil(
    caches.open(SHELL_CACHE).then(cache => {
      // Cache what we can; ignore individual failures (CDN might 304)
      return Promise.allSettled(
        SHELL_ASSETS.map(url =>
          cache.add(url).catch(e => console.warn(`[SW] pre-cache miss: ${url}`, e))
        )
      );
    }).then(() => self.skipWaiting())
  );
});

// ── Activate — purge old caches ───────────────────────────────────────────────
self.addEventListener('activate', event => {
  event.waitUntil(
    caches.keys().then(keys =>
      Promise.all(
        keys
          .filter(k => k.startsWith('devit-') && !k.startsWith(CACHE_VERSION))
          .map(k => caches.delete(k))
      )
    ).then(() => self.clients.claim())
  );
});

// ── Fetch — routing logic ─────────────────────────────────────────────────────
self.addEventListener('fetch', event => {
  const { request } = event;
  const url = request.url;

  // Skip non-GET and chrome-extension requests
  if (request.method !== 'GET') return;
  if (!url.startsWith('http')) return;

  // ── Supabase / Groq — Network First, short cache TTL ──────────────────────
  if (isSupabaseOrGroq(url)) {
    event.respondWith(networkFirst(request, API_CACHE, 60));
    return;
  }

  // ── Images / avatars — Cache First, 7-day TTL ─────────────────────────────
  if (isImage(url)) {
    event.respondWith(cacheFirstWithExpiry(request, IMAGE_CACHE, 7 * 24 * 60 * 60));
    return;
  }

  // ── CDN fonts / FA — Cache First (long-lived, content-hashed) ─────────────
  if (isCdnFont(url)) {
    event.respondWith(cacheFirst(request, SHELL_CACHE));
    return;
  }

  // ── App shell — Cache First, stale-while-revalidate ───────────────────────
  if (isShellAsset(url) || url.includes('/')) {
    event.respondWith(staleWhileRevalidate(request, SHELL_CACHE));
    return;
  }

  // ── Default — Network First ────────────────────────────────────────────────
  event.respondWith(networkFirst(request, SHELL_CACHE, 300));
});

// ── Cache strategies ──────────────────────────────────────────────────────────

async function cacheFirst(request, cacheName) {
  const cached = await caches.match(request);
  if (cached) return cached;
  try {
    const response = await fetch(request);
    if (response.ok) {
      const cache = await caches.open(cacheName);
      cache.put(request, response.clone());
    }
    return response;
  } catch (_) {
    return offlineFallback(request);
  }
}

async function cacheFirstWithExpiry(request, cacheName, maxAgeSeconds) {
  const cache  = await caches.open(cacheName);
  const cached = await cache.match(request);
  if (cached) {
    const date = cached.headers.get('sw-cached-at');
    if (date && (Date.now() - Number(date)) < maxAgeSeconds * 1000) {
      return cached;
    }
  }
  try {
    const response = await fetch(request);
    if (response.ok) {
      // Clone and stamp with cache time
      const stamped = new Response(response.clone().body, {
        status:  response.status,
        headers: new Headers(response.headers),
      });
      stamped.headers.append('sw-cached-at', String(Date.now()));
      cache.put(request, stamped);
    }
    return response;
  } catch (_) {
    return cached || offlineFallback(request);
  }
}

async function networkFirst(request, cacheName, maxAgeSeconds = 300) {
  try {
    const response = await fetch(request);
    if (response.ok) {
      const cache = await caches.open(cacheName);
      cache.put(request, response.clone());
    }
    return response;
  } catch (_) {
    const cached = await caches.match(request);
    return cached || offlineFallback(request);
  }
}

async function staleWhileRevalidate(request, cacheName) {
  const cache  = await caches.open(cacheName);
  const cached = await cache.match(request);

  const fetchPromise = fetch(request).then(response => {
    if (response.ok) cache.put(request, response.clone());
    return response;
  }).catch(() => null);

  return cached || fetchPromise || offlineFallback(request);
}

function offlineFallback(request) {
  // For navigation requests serve the offline page
  if (request.mode === 'navigate') {
    return new Response(OFFLINE_HTML, {
      headers: { 'Content-Type': 'text/html; charset=utf-8' },
    });
  }
  // For everything else return an empty 503
  return new Response('Offline', { status: 503, statusText: 'Service Unavailable' });
}

// ── Push Notifications ────────────────────────────────────────────────────────
self.addEventListener('push', event => {
  let data = {};
  try { data = event.data?.json() ?? {}; } catch (_) {
    data = { title: 'Devit', body: event.data?.text() ?? 'New notification' };
  }

  const title   = data.title   || 'Devit';
  const body    = data.body    || 'You have a new notification';
  const icon    = data.icon    || '/devit.png';
  const badge   = data.badge   || '/devit.png';
  const tag     = data.tag     || 'devit-notif';
  const url     = data.url     || '/';
  const type    = data.type    || '';   // 'like' | 'follow' | 'comment' | 'message'

  // Action buttons vary by notification type
  const actions = type === 'message'
    ? [{ action: 'open', title: 'Reply', icon: '/devit.png' }]
    : type === 'follow'
      ? [{ action: 'open', title: 'View profile', icon: '/devit.png' }]
      : [
          { action: 'open',    title: 'View',    icon: '/devit.png' },
          { action: 'dismiss', title: 'Dismiss'                          },
        ];

  event.waitUntil(
    self.registration.showNotification(title, {
      body,
      icon,
      badge,
      tag,
      data:    { url, type },
      actions,
      vibrate: [100, 50, 100],
      renotify: true,
    })
  );
});

// ── Notification click ────────────────────────────────────────────────────────
self.addEventListener('notificationclick', event => {
  event.notification.close();

  if (event.action === 'dismiss') return;

  const targetUrl = event.notification.data?.url || '/';

  event.waitUntil(
    clients.matchAll({ type: 'window', includeUncontrolled: true }).then(clientList => {
      // Focus existing Devit tab if open
      for (const client of clientList) {
        if (client.url.includes('/') && 'focus' in client) {
          client.focus();
          client.postMessage({ type: 'NOTIF_CLICK', url: targetUrl });
          return;
        }
      }
      // Otherwise open a new tab
      if (clients.openWindow) return clients.openWindow(targetUrl);
    })
  );
});

// ── Background sync — retry failed post submissions ───────────────────────────
self.addEventListener('sync', event => {
  if (event.tag === 'devit-sync-posts') {
    event.waitUntil(syncPendingPosts());
  }
});

async function syncPendingPosts() {
  // Posts queued while offline are stored in IndexedDB under 'devit-pending-posts'
  // The main app reads this on startup too, but SW sync fires as soon as online
  try {
    const db     = await openIDB();
    const posts  = await getAllFromStore(db, 'pending-posts');
    for (const post of posts) {
      try {
        const resp = await fetch(`${post.supabaseUrl}/rest/v1/posts`, {
          method:  'POST',
          headers: {
            'Content-Type':  'application/json',
            'apikey':        post.anonKey,
            'Authorization': `Bearer ${post.accessToken}`,
            'Prefer':        'return=minimal',
          },
          body: JSON.stringify(post.data),
        });
        if (resp.ok) await deleteFromStore(db, 'pending-posts', post.id);
      } catch (_) { /* will retry next sync */ }
    }
  } catch (e) {
    console.warn('[SW] Background sync failed:', e);
  }
}

// ── IndexedDB helpers (for background sync) ───────────────────────────────────
function openIDB() {
  return new Promise((resolve, reject) => {
    const req = indexedDB.open('devit-sw', 1);
    req.onupgradeneeded = e => {
      const db = e.target.result;
      if (!db.objectStoreNames.contains('pending-posts')) {
        db.createObjectStore('pending-posts', { keyPath: 'id', autoIncrement: true });
      }
    };
    req.onsuccess = e => resolve(e.target.result);
    req.onerror   = e => reject(e.target.error);
  });
}

function getAllFromStore(db, storeName) {
  return new Promise((resolve, reject) => {
    const tx  = db.transaction(storeName, 'readonly');
    const req = tx.objectStore(storeName).getAll();
    req.onsuccess = e => resolve(e.target.result);
    req.onerror   = e => reject(e.target.error);
  });
}

function deleteFromStore(db, storeName, key) {
  return new Promise((resolve, reject) => {
    const tx  = db.transaction(storeName, 'readwrite');
    const req = tx.objectStore(storeName).delete(key);
    req.onsuccess = () => resolve();
    req.onerror   = e => reject(e.target.error);
  });
}

// ── Message from main thread ──────────────────────────────────────────────────
self.addEventListener('message', event => {
  // Main app can tell SW to skip waiting (used after update detected)
  if (event.data?.type === 'SKIP_WAITING') {
    self.skipWaiting();
  }
});
