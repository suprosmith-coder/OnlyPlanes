/* ============================================================
   DEVIT — Code. Connect. Ship.
   app.js  —  Full Supabase integration
   ============================================================ */

'use strict';

/* ── Supabase Init ──────────────────────────────────────────── */
if (!window.supabase || !window.supabase.createClient) {
  console.error('[Devit] Supabase CDN failed to load. Check your internet connection or CDN URL.');
}
if (!window.DEVIT_CONFIG || !window.DEVIT_CONFIG.SUPABASE_URL) {
  console.error('[Devit] window.DEVIT_CONFIG is missing. Make sure the config <script> block runs before app.js.');
}
const { createClient } = supabase;
const sb = createClient(
  window.DEVIT_CONFIG.SUPABASE_URL,
  window.DEVIT_CONFIG.SUPABASE_ANON_KEY,
  {
    auth: {
      detectSessionInUrl: true,
      persistSession: true,
      autoRefreshToken: true,
      flowType: 'pkce',
    }
  }
);
window.sb = sb;

/* ── Realtime Manager ───────────────────────────────────────── */
class RealtimeManager {
  constructor(sb) {
    this.sb = sb;
    this.subscriptions = new Map(); // key → channel
    this.listeners = new Map();     // key → handler[]
  }

  /**
   * Subscribe to a table/event combo.
   * For postgres_changes: channel is a logical name, table and event are used.
   * For presence/broadcast channels pass a pre-built channel via subscribeRaw().
   * Returns an unsubscribe function.
   */
  subscribe(channelName, table, event = '*', handler, filterStr = null) {
    const key = `${table}:${event}${filterStr ? ':' + filterStr : ''}`;
    if (!this.subscriptions.has(key)) {
      const opts = { event, schema: 'public', table };
      if (filterStr) opts.filter = filterStr;
      const ch = this.sb
        .channel(channelName)
        .on('postgres_changes', opts, payload => this._broadcast(key, payload))
        .subscribe();
      this.subscriptions.set(key, ch);
    }
    if (!this.listeners.has(key)) this.listeners.set(key, []);
    this.listeners.get(key).push(handler);
    return () => this._unsubscribe(key, handler);
  }

  /**
   * Register a pre-built channel (presence, broadcast, etc.) under a key.
   * The caller is responsible for calling .subscribe() on the channel before
   * passing it here. Returns an unsubscribe function.
   */
  subscribeRaw(key, channel, handler) {
    this.subscriptions.set(key, channel);
    if (!this.listeners.has(key)) this.listeners.set(key, []);
    this.listeners.get(key).push(handler);
    return () => this._unsubscribe(key, handler);
  }

  _broadcast(key, payload) {
    this.listeners.get(key)?.forEach(h => h(payload));
  }

  _unsubscribe(key, handler) {
    const list = this.listeners.get(key);
    if (list) {
      const idx = list.indexOf(handler);
      if (idx !== -1) list.splice(idx, 1);
    }
    if (list?.length === 0) {
      const ch = this.subscriptions.get(key);
      if (ch) { try { this.sb.removeChannel(ch); } catch (_) {} }
      this.subscriptions.delete(key);
      this.listeners.delete(key);
    }
  }

  /** Remove all subscriptions whose key starts with prefix (e.g. view-scoped). */
  cleanupByPrefix(prefix) {
    for (const key of [...this.subscriptions.keys()]) {
      if (key.startsWith(prefix)) {
        const ch = this.subscriptions.get(key);
        if (ch) { try { this.sb.removeChannel(ch); } catch (_) {} }
        this.subscriptions.delete(key);
        this.listeners.delete(key);
      }
    }
  }

  /** Tear down every subscription (call on sign-out). */
  cleanup() {
    for (const ch of this.subscriptions.values()) {
      try { this.sb.removeChannel(ch); } catch (_) {}
    }
    this.subscriptions.clear();
    this.listeners.clear();
  }
}

/* ── State ──────────────────────────────────────────────────── */
const State = {
  user: null,          // Supabase Auth user
  profile: null,       // profiles row
  currentView: 'feed',
  currentCommunity: null,
  currentChannel: null,
  currentDM: null,
  feedTab: 'for-you',
  isGuest: false,
  posts: [],
  notifications: [],
  onlineUsers: new Set(),
  unreadNotifs: 0,
  unreadMessages: 0,
};

/* ── Realtime Manager instance ──────────────────────────────── */
const realtimeManager = new RealtimeManager(sb);
window.State = State;

/* ── Helpers ────────────────────────────────────────────────── */
const $ = (sel, root = document) => root.querySelector(sel);
const $$ = (sel, root = document) => [...root.querySelectorAll(sel)];
const el = (tag, cls, html = '') => {
  const e = document.createElement(tag);
  if (cls) e.className = cls;
  if (html) e.innerHTML = html;
  return e;
};
const fmtNum = n => n >= 1000 ? (n / 1000).toFixed(1) + 'K' : String(n);
const timeAgo = ts => {
  const s = Math.floor((Date.now() - new Date(ts)) / 1000);
  if (s < 60) return 'just now';
  if (s < 3600) return Math.floor(s / 60) + 'm ago';
  if (s < 86400) return Math.floor(s / 3600) + 'h ago';
  return Math.floor(s / 86400) + 'd ago';
};

/* ── Follow count helpers ────────────────────────────────────────
   Direct UPDATE instead of RPC — no custom DB functions needed.
   Uses Supabase's atomic SQL so counts stay accurate under concurrency.
   ─────────────────────────────────────────────────────────────── */
async function incrementFollowCounts(targetId, followerId) {
  // target gets +1 follower, follower gets +1 following
  await Promise.all([
    sb.rpc('increment_col', { row_id: targetId,   col: 'followers_count' })
      .then(({ error }) => {
        if (error) {
          // RPC not set up — fall back to read-then-write
          return sb.from('op_profiles').select('followers_count').eq('id', targetId).single()
            .then(({ data }) => sb.from('op_profiles').update({ followers_count: (data?.followers_count || 0) + 1 }).eq('id', targetId));
        }
      }),
    sb.rpc('increment_col', { row_id: followerId, col: 'following_count' })
      .then(({ error }) => {
        if (error) {
          return sb.from('op_profiles').select('following_count').eq('id', followerId).single()
            .then(({ data }) => sb.from('op_profiles').update({ following_count: (data?.following_count || 0) + 1 }).eq('id', followerId));
        }
      }),
  ]);
  // Sync own State.profile following_count so our bio stays accurate
  if (followerId === State.user?.id && State.profile) {
    State.profile.following_count = (State.profile.following_count || 0) + 1;
    _syncProfileStatDOM();
  }
}

async function decrementFollowCounts(targetId, followerId) {
  await Promise.all([
    sb.rpc('decrement_col', { row_id: targetId,   col: 'followers_count' })
      .then(({ error }) => {
        if (error) {
          return sb.from('op_profiles').select('followers_count').eq('id', targetId).single()
            .then(({ data }) => sb.from('op_profiles').update({ followers_count: Math.max(0, (data?.followers_count || 1) - 1) }).eq('id', targetId));
        }
      }),
    sb.rpc('decrement_col', { row_id: followerId, col: 'following_count' })
      .then(({ error }) => {
        if (error) {
          return sb.from('op_profiles').select('following_count').eq('id', followerId).single()
            .then(({ data }) => sb.from('op_profiles').update({ following_count: Math.max(0, (data?.following_count || 1) - 1) }).eq('id', followerId));
        }
      }),
  ]);
  if (followerId === State.user?.id && State.profile) {
    State.profile.following_count = Math.max(0, (State.profile.following_count || 1) - 1);
    _syncProfileStatDOM();
  }
}

/* Fetch real follower/following counts from DB and stamp them into the DOM */
async function _refreshFollowCountsInDOM(main, targetId) {
  // Fetch target profile counts
  const [{ data: target }, { data: self }] = await Promise.all([
    sb.from('op_profiles').select('followers_count, following_count').eq('id', targetId).single(),
    State.user?.id
      ? sb.from('op_profiles').select('following_count').eq('id', State.user.id).single()
      : Promise.resolve({ data: null }),
  ]);

  if (target) {
    // Profile page stat pills: Following[0], Followers[1], Posts[2]
    const statEls = main.querySelectorAll('.profile-stat strong');
    if (statEls[0]) statEls[0].textContent = fmtNum(target.following_count || 0);
    if (statEls[1]) statEls[1].textContent = fmtNum(target.followers_count || 0);
  }

  if (self && State.profile) {
    State.profile.following_count = self.following_count || 0;
    _syncProfileStatDOM();
  }
}

/* Sync the current user's stat numbers anywhere they appear in the DOM
   (topbar mini-profile, own profile page stats, quick-view card) */
function _syncProfileStatDOM() {
  if (!State.profile) return;
  // Own profile page stat pills (Following[0], Followers[1], Posts[2])
  const main = document.getElementById('main');
  if (main) {
    const statEls = main.querySelectorAll('.profile-stat strong');
    if (statEls[0]) statEls[0].textContent = fmtNum(State.profile.following_count || 0);
    if (statEls[1]) statEls[1].textContent = fmtNum(State.profile.followers_count || 0);
    if (statEls[2]) statEls[2].textContent = fmtNum(State.profile.posts_count || 0);
  }
  // Quick-view overlay — update own following count if own card is open
  const pqvOverlay = document.getElementById('profile-quick-view-overlay');
  if (pqvOverlay && pqvOverlay.style.display !== 'none') {
    // The quick view shows target's stats; if target === self, sync both
    const strongs = pqvOverlay.querySelectorAll('strong');
    if (strongs.length >= 2 && pqvOverlay.dataset.uid === State.user?.id) {
      strongs[0].textContent = fmtNum(State.profile.followers_count || 0);
      strongs[1].textContent = fmtNum(State.profile.following_count || 0);
    }
  }
}

/* Realtime: update our own follower count when someone follows/unfollows us */
function _subscribeToOwnFollowCount() {
  if (!State.user?.id) return;
  realtimeManager.subscribe(
    'own-follow-count',
    'follows',
    'INSERT',
    payload => {
      if (payload.new?.following_id === State.user.id && State.profile) {
        State.profile.followers_count = (State.profile.followers_count || 0) + 1;
        _syncProfileStatDOM();
      }
    },
    `following_id=eq.${State.user.id}`
  );
  realtimeManager.subscribe(
    'own-unfollow-count',
    'follows',
    'DELETE',
    payload => {
      if (payload.old?.following_id === State.user.id && State.profile) {
        State.profile.followers_count = Math.max(0, (State.profile.followers_count || 1) - 1);
        _syncProfileStatDOM();
      }
    },
    `following_id=eq.${State.user.id}`
  );
}
const avatarColor = str => {
  let h = 0;
  for (let i = 0; i < str.length; i++) h = str.charCodeAt(i) + ((h << 5) - h);
  const colors = ['#ff2d6e','#a78bfa','#34d399','#fb7185','#fbbf24','#f97316','#38bdf8','#f472b6'];
  return colors[Math.abs(h) % colors.length];
};
const avatarInitials = name => {
  const parts = (name || 'U').trim().split(' ');
  return parts.length > 1 ? parts[0][0] + parts[1][0] : parts[0].slice(0,2);
};

/* ── Badge Definitions ──────────────────────────────────────── */
const BADGE_DEFS = [
  { id: 'first_ship',     icon: '🚀', label: 'First Ship',      tier: 'cyan',    desc: 'Published your first post on Devit',                 check: p => (p.posts_count || 0) >= 1 },
  { id: 'serial_shipper', icon: '⚡', label: 'Serial Shipper',   tier: 'violet',  desc: '10+ posts shipped',                                  check: p => (p.posts_count || 0) >= 10 },
  { id: 'hundred_posts',  icon: '💯', label: 'Century',          tier: 'gold',    desc: '100 posts — you never stop shipping',                check: p => (p.posts_count || 0) >= 100 },
  { id: 'popular',        icon: '🌟', label: 'Rising Star',      tier: 'gold',    desc: 'Reached 50+ followers',                              check: p => (p.followers_count || 0) >= 50 },
  { id: 'influencer',     icon: '👑', label: 'Influencer',       tier: 'gold',    desc: 'Reached 500+ followers',                             check: p => (p.followers_count || 0) >= 500 },
  { id: 'connector',      icon: '🔗', label: 'Connector',        tier: 'emerald', desc: 'Following 25+ developers',                           check: p => (p.following_count || 0) >= 25 },
  { id: 'early_adopter',  icon: '🌱', label: 'Early Adopter',    tier: 'emerald', desc: 'Joined Devit in its early days',                     check: p => p.created_at && new Date(p.created_at) < new Date('2025-12-31') },
];

function computeBadges(profile) {
  return BADGE_DEFS.filter(b => b.check(profile));
}

/* ── Navigate to Tag Feed ────────────────────────────────────── */
function navigateToTag(tag) {
  const normalizedTag = tag.startsWith('#') ? tag : '#' + tag;
  State.currentView = 'tag:' + normalizedTag;
  showPresence();
  updateSidebarActive();
  updateBottomNavActive(null);
  updatePageMeta({ title: normalizedTag, description: `Posts tagged with ${normalizedTag} on Devit` });
  const main = $('#main');
  main.style.cssText = '';
  main.style.overflow = '';  // restore from community view
  main.innerHTML = '';
  closeSearch?.();
  realtimeManager.cleanupByPrefix('view:');
  while (ViewUnsubFns.length) { try { ViewUnsubFns.pop()(); } catch (_) {} }
  renderTagFeed(main, normalizedTag);
  main.classList.remove('page-enter');
  requestAnimationFrame(() => requestAnimationFrame(() => { main.classList.add('page-enter'); main.focus(); }));
}
window.navigateToTag = navigateToTag;

/* ── Tag Feed Renderer ───────────────────────────────────────── */
async function renderTagFeed(main, tag) {
  const rawTag = tag.replace(/^#/, '');
  main.innerHTML = `
    <div class="tag-feed-header">
      <div class="tag-feed-title">${escapeHtml(tag)}</div>
      <div class="tag-feed-meta" id="tag-feed-meta">Loading posts…</div>
    </div>
    <div id="tag-posts-feed" style="margin-top:0">
      <div style="padding:32px;text-align:center;color:var(--text-muted)">Loading…</div>
    </div>
  `;

  const { data: posts, error } = await sb
    .from('op_posts')
    .select(`id, content, code_block, code_lang, image_url, file_url, file_name, likes_count, comments_count, reposts_count, created_at, poll,
      profiles:op_profiles!author_id(id, username, display_name, avatar_url)`)
    .ilike('content', `%#${rawTag}%`)
    .order('created_at', { ascending: false })
    .limit(40);

  const container = $('#tag-posts-feed', main);
  const metaEl    = $('#tag-feed-meta', main);
  if (error || !posts?.length) {
    if (metaEl) metaEl.textContent = 'No posts yet — be the first to post!';
    if (container) container.innerHTML = `<div style="padding:48px;text-align:center;color:var(--text-muted)"><div style="font-size:36px;margin-bottom:12px">#️⃣</div><div>No posts tagged <strong>${escapeHtml(tag)}</strong> yet</div></div>`;
    return;
  }

  if (metaEl) metaEl.textContent = `${posts.length} post${posts.length !== 1 ? 's' : ''}`;

  const postIds = posts.map(p => p.id);
  const { data: likes } = State.user?.id
    ? await sb.from('op_post_likes').select('post_id').eq('user_id', State.user.id).in('post_id', postIds)
    : { data: [] };
  const likedIds = new Set((likes || []).map(l => l.post_id));

  container.innerHTML = '';
  posts.forEach(p => container.appendChild(buildPostCard(p, p.profiles, likedIds.has(p.id), false)));
}


function toast(msg, icon = 'check') {
  const c = $('#toast-container');
  if (!c) { console.warn('[Devit] #toast-container not found:', msg); return; }
  // icon can be an FA icon name (like 'rocket') or a legacy emoji
  const isEmoji = /\p{Emoji}/u.test(icon);
  const iconHtml = isEmoji
    ? `<span class="toast-icon">${icon}</span>`
    : `<span class="toast-icon"><i class="fa-solid fa-${icon}"></i></span>`;
  const t = el('div', 'toast', `${iconHtml}<span>${msg}</span>`);
  c.appendChild(t);
  setTimeout(() => { t.classList.add('exit'); setTimeout(() => t.remove(), 300); }, 3000);
}

/* ── Force-download helper (bypasses browser's open-in-tab for text files) ── */
window.devitDownloadFile = async function(url, filename) {
  try {
    const res = await fetch(url);
    if (!res.ok) throw new Error('fetch failed');
    const blob = await res.blob();
    const a = document.createElement('a');
    a.href = URL.createObjectURL(blob);
    a.download = filename || 'download';
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    setTimeout(() => URL.revokeObjectURL(a.href), 15000);
  } catch (e) {
    // Cross-origin fallback: open in new tab
    window.open(url, '_blank', 'noopener');
  }
};

function showPresence() {
  const b = $('#presence-bar');
  if (!b) return;
  b.classList.add('loading');
  setTimeout(() => { b.classList.remove('loading'); b.classList.add('done'); setTimeout(() => b.classList.remove('done'), 400); }, 600);
}

function setAuthStatus(msg, isError = false) {
  const el = $('#auth-status');
  if (!el) { console.warn('[Devit] #auth-status not found:', msg); return; }
  el.style.display = 'block';
  el.style.color = isError ? 'var(--rose)' : 'var(--text-secondary)';
  el.innerHTML = msg;
}

/* ── Avatar HTML ─────────────────────────────────────────────── */
function avatarHtml(profile, size = 36, cls = '') {
  if (!profile) return `<div class="profile-avatar-circle" style="width:${size}px;height:${size}px;font-size:${size*0.4}px;background:#444;${cls ? '' : ''}">?</div>`;
  const name = profile.display_name || profile.username || 'U';
  const color = avatarColor(name);
  const badgeSize = Math.max(14, Math.round(size * 0.38));
  const iconSize  = Math.max(8, Math.round(size * 0.22));
  const ghBadge   = profile.is_github
    ? `<div class="avatar-gh-badge" style="position:absolute;bottom:-2px;right:-2px;width:${badgeSize}px;height:${badgeSize}px;background:#24292e;border-radius:50%;border:2px solid var(--bg-surface,#10121a);display:flex;align-items:center;justify-content:center;pointer-events:none;z-index:1;"><i class="fa-brands fa-github" style="color:#fff;font-size:${iconSize}px;line-height:1"></i></div>`
    : '';
  function wrap(inner) {
    return profile.is_github
      ? `<div style="position:relative;display:inline-flex;flex-shrink:0;">${inner}${ghBadge}</div>`
      : inner;
  }
  if (profile.avatar_url) {
    return wrap(`<img src="${profile.avatar_url}" class="profile-avatar-circle${cls ? ' '+cls:''}" style="width:${size}px;height:${size}px;object-fit:cover;flex-shrink:0;" data-fallback-initials="${avatarInitials(name).replace(/"/g,'')}" data-fallback-color="${color}" data-fallback-size="${size}" onload="" onerror="devitAvatarFallback(this)">`);
  }
  return wrap(`<div class="profile-avatar-circle${cls ? ' '+cls:''}" style="width:${size}px;height:${size}px;font-size:${size*0.4}px;background:${color};flex-shrink:0;">${avatarInitials(name)}</div>`);
}


/* ── Safe avatar image fallback ─────────────────────────────
   Called via onerror data attribute instead of inline script
   interpolation, preventing XSS through malformed names.    */
function devitAvatarFallback(img) {
  const initials = img.dataset.fallbackInitials || '?';
  const color    = img.dataset.fallbackColor    || '#444';
  const size     = img.dataset.fallbackSize     || '36';
  const div = document.createElement('div');
  div.className = img.className;
  div.style.cssText = `width:${size}px;height:${size}px;font-size:${parseFloat(size)*0.4}px;background:${color};display:flex;align-items:center;justify-content:center;border-radius:50%;flex-shrink:0;font-weight:600;color:#fff;`;
  div.textContent = initials;
  img.replaceWith(div);
}
window.devitAvatarFallback = devitAvatarFallback;

/* ── SQL Bootstrap (run once) ───────────────────────────────── */
async function bootstrapSchema() {
  // We attempt to query key tables; if they fail we surface a helpful message
  // Actual table creation should be done via Supabase Dashboard SQL editor
  // This function checks readiness and primes any missing profile for current user
  try {
    const { data, error } = await sb.from('op_profiles').select('id').limit(1);
    if (error && error.code === '42P01') {
      console.warn('[Devit] Tables not found. Run the SQL setup in Supabase Dashboard.');
      toast('DB tables missing — see console for setup SQL', 'triangle-exclamation');
      logSetupSQL();
      return false;
    }
    return true;
  } catch (e) {
    console.error('[Devit] Schema check failed', e);
    return false;
  }
}

function logSetupSQL() {
  // SQL schema is maintained in migrations/schema.sql
}

/* ── Auth ───────────────────────────────────────────────────── */

// ── Sign-in rate limiting (ported from Cyanix AI) ────────────
// Prevents brute-force: locks out for 30 s after 5 failed attempts.
let _signInAttempts  = 0;
let _signInLockUntil = 0;
const _SIGNIN_MAX    = 5;
const _SIGNIN_LOCK   = 30_000;

function checkSignInRateLimit() {
  // Only check if currently locked — do NOT increment here.
  // Increment happens in recordSignInFailure() after a confirmed failure.
  if (Date.now() < _signInLockUntil) {
    const secs = Math.ceil((_signInLockUntil - Date.now()) / 1000);
    setAuthStatus(`Too many attempts — wait ${secs}s before trying again.`, true);
    return false;
  }
  return true;
}
function recordSignInFailure() {
  _signInAttempts++;
  if (_signInAttempts >= _SIGNIN_MAX) {
    _signInLockUntil = Date.now() + _SIGNIN_LOCK;
    _signInAttempts  = 0;
    setAuthStatus('Too many failed attempts. Locked for 30 seconds.', true);
  }
}
function resetSignInRateLimit() { _signInAttempts = 0; _signInLockUntil = 0; }

// ── Session expiry warning (ported from Cyanix AI) ───────────
// Warns the user 5 min before their JWT expires and offers
// a silent refresh. Supabase auto-refreshes every ~55 min but
// this catches backgrounded-tab / network-outage edge cases.
let _expiryWarningTimer = null;

function scheduleSessionExpiryWarning(session) {
  if (_expiryWarningTimer) clearTimeout(_expiryWarningTimer);
  if (!session?.expires_at) return;
  const warnInMs = (session.expires_at * 1000) - Date.now() - 5 * 60 * 1000;
  if (warnInMs <= 0) return;
  _expiryWarningTimer = setTimeout(() => showSessionExpiryBanner(), warnInMs);
}

function showSessionExpiryBanner() {
  // Reuse the auth-status element as a non-blocking in-app banner
  const banner = document.getElementById('session-expiry-banner');
  if (banner) { banner.style.display = 'flex'; return; }
  // Fallback: inject a minimal banner if the element doesn't exist in HTML
  const b = document.createElement('div');
  b.id = 'session-expiry-banner';
  b.style.cssText = 'position:fixed;bottom:72px;left:50%;transform:translateX(-50%);background:var(--bg-surface,#1e1e2e);border:1px solid var(--border,#333);border-radius:12px;padding:10px 16px;display:flex;align-items:center;gap:10px;font-size:13px;color:var(--text-primary,#fff);z-index:9999;box-shadow:0 4px 20px #0006';
  b.innerHTML = '<i class="fa-solid fa-clock" style="color:var(--amber,#fbbf24)"></i><span>Your session expires soon.</span><button id="session-refresh-btn" style="margin-left:8px;padding:4px 10px;border-radius:6px;background:var(--brand,#ff2d6e);color:#000;border:none;cursor:pointer;font-size:12px;font-weight:600">Stay signed in</button><button id="session-expiry-dismiss-btn" style="background:none;border:none;color:var(--text-muted,#888);cursor:pointer;font-size:16px;line-height:1;margin-left:4px">×</button>';
  document.body.appendChild(b);
  document.getElementById('session-expiry-dismiss-btn').addEventListener('click', () => {
    document.getElementById('session-expiry-banner').style.display = 'none';
  });
  b.querySelector('#session-refresh-btn').addEventListener('click', async () => {
    try {
      const { data, error } = await sb.auth.refreshSession();
      if (error) throw error;
      b.style.display = 'none';
      scheduleSessionExpiryWarning(data.session);
      toast('Session refreshed!', 'check');
    } catch (e) {
      toast('Could not refresh — please sign in again.', 'circle-exclamation');
    }
  });
}

async function initAuth() {
  const screen     = $('#auth-screen');
  const app        = $('#app');
  const discordBtn = $('#discord-login-btn');

  function showOAuthRedirectOverlay(providerName) {
    let ov = document.getElementById('oauth-redirect-overlay');
    if (!ov) {
      ov = document.createElement('div');
      ov.id = 'oauth-redirect-overlay';
      ov.style.cssText = 'position:fixed;inset:0;z-index:9999;background:var(--bg-void,#040c1a);display:flex;flex-direction:column;align-items:center;justify-content:center;gap:16px;font-family:var(--font-body,sans-serif)';
      ov.innerHTML = `
        <svg width="36" height="36" viewBox="0 0 36 36" style="animation:spin 0.9s linear infinite"><circle cx="18" cy="18" r="14" fill="none" stroke="#5865F2" stroke-width="3" stroke-dasharray="60 30" stroke-linecap="round"/></svg>
        <style>@keyframes spin{to{transform:rotate(360deg)}}</style>
        <div style="font-size:15px;font-weight:600;color:var(--text-primary,#e8f4ff)">Redirecting to ${providerName}…</div>
        <div style="font-size:12px;color:var(--text-muted,#3d5a78)">You'll be brought back automatically</div>`;
      document.body.appendChild(ov);
    }
    ov.style.display = 'flex';
  }

  // ── Discord OAuth ─────────────────────────────────────────────
  try { discordBtn?.addEventListener('click', async () => {
    discordBtn.disabled = true;
    const span = discordBtn.querySelector('span');
    if (span) span.textContent = 'Connecting…';
    showOAuthRedirectOverlay('Discord');
    const { error } = await sb.auth.signInWithOAuth({
      provider: 'discord',
      options: {
        redirectTo: window.DEVIT_CONFIG?.SITE_URL || window.location.origin,
        skipBrowserRedirect: false,
      }
    });
    if (error) {
      const ov = document.getElementById('oauth-redirect-overlay');
      if (ov) ov.style.display = 'none';
      setAuthStatus('Discord sign-in failed: ' + error.message, true);
      discordBtn.disabled = false;
      if (span) span.textContent = 'Continue with Discord';
    }
    // On success the browser navigates away
  }); } catch (e) { console.error('[Planebook] Discord btn bind error:', e); }
  // ── Guest / Browse mode ───────────────────────────────────────
  try {
    const guestBtn = document.getElementById('guest-browse-btn');
    guestBtn?.addEventListener('click', () => {
      State.isGuest = true;
      document.body.classList.add('guest-mode');
      State.user    = null;
      State.profile = { username: 'guest', display_name: 'Guest', id: null };
      screen.style.opacity    = '0';
      screen.style.transform  = 'scale(1.02)';
      screen.style.transition = '0.4s ease';
      setTimeout(async () => {
        screen.style.display = 'none'; screen.style.visibility = 'hidden'; screen.style.pointerEvents = 'none';
        app.classList.add('visible');
        await buildApp();
        toast('Browsing as guest — sign in to post & interact ✈️', 'eye');
      }, 400);
    });
  } catch(e) { console.error('[Planebook] Guest btn bind error:', e); }



  // ── onAuthStateChange — single source of truth ───────────────
  //
  // Ported from Cyanix AI with the following improvements:
  //   • _syncPending + user-ID dedup guard (more robust than appBuilt bool)
  //   • TOKEN_REFRESHED: reschedules expiry warning instead of ignoring
  //   • PASSWORD_RECOVERY: surfaces the password-reset UI
  //   • SIGNED_OUT: full state cleanup including expiry timer
  //   • URL cleanup: covers both PKCE (?code=) and implicit (#access_token)
  //   • Google OAuth: access_type=offline + prompt=consent for refresh token
  let _syncPending  = false;
  let _signedInUser = null;  // tracks user ID to prevent double-boot
  let appBuilt      = false;

  sb.auth.onAuthStateChange(async (event, session) => {

    // ── TOKEN_REFRESHED ────────────────────────────────────────
    // Supabase auto-refreshes the JWT every ~55 min. When it does,
    // reschedule the expiry warning with the new expiry time.
    // Do NOT re-run the sign-in flow — user is already in the app.
    if (event === 'TOKEN_REFRESHED') {
      if (session) scheduleSessionExpiryWarning(session);
      return;
    }

    // ── PASSWORD_RECOVERY ──────────────────────────────────────
    // User clicked a password-reset link. session.access_token is
    // valid and scoped to updateUser() only. Surface the reset UI.
    if (event === 'PASSWORD_RECOVERY') {
      // If the app is already visible (user was signed in), open a modal
      // instead of silently showing the auth screen behind the app.
      if (appBuilt) {
        openChangePasswordModal();
      } else {
        setAuthStatus('<i class="fa-solid fa-key" style="margin-right:6px"></i>Enter your new password below to reset it.');
        screen.style.display = 'flex';
        app.classList.remove('visible');
        document.getElementById('auth-password-si')?.focus();
      }
      return;
    }

    // ── SIGNED_OUT ─────────────────────────────────────────────
    if (!session?.user) {
      if (event === 'SIGNED_OUT') {
        // Cancel expiry warning
        if (_expiryWarningTimer) { clearTimeout(_expiryWarningTimer); _expiryWarningTimer = null; }
        const expiryBanner = document.getElementById('session-expiry-banner');
        if (expiryBanner) expiryBanner.style.display = 'none';

        // Tear down all realtime subscriptions
        realtimeManager.cleanup();
        while (ViewUnsubFns.length) { try { ViewUnsubFns.pop()(); } catch (_) {} }

        // Reset all state
        _signedInUser = null;
        _syncPending  = false;
        appBuilt      = false;
        State.user    = null;
        State.profile = null;
        State.isGuest = false;
        document.body.classList.remove('guest-mode');

        // Return to auth screen
        screen.style.display   = 'flex';
        screen.style.opacity   = '1';
        screen.style.transform = '';
        screen.style.transition = '';
        app.classList.remove('visible');
      }
      // All other null-session noise (INITIAL_SESSION before hash is parsed, etc.) — ignore
      return;
    }

    // ── SIGNED_IN / INITIAL_SESSION ────────────────────────────
    // Guard against double-invocation:
    //   • _syncPending: blocks re-entry while async boot is running
    //   • _signedInUser: blocks re-run if the same user is already booted
    // This handles the race between getSession() and onAuthStateChange
    // that can fire both INITIAL_SESSION and SIGNED_IN for the same session.
    if (_syncPending) return;
    if (_signedInUser && _signedInUser === session.user.id && appBuilt) return;

    _syncPending  = true;
    State.user    = session.user;

    try {
    // Clean OAuth tokens from the URL bar after Supabase parses them.
    // Implicit flow tokens arrive in the hash (#access_token=...).
    try {
      const url = new URL(window.location.href);
      const hasOAuthHash = url.hash && (url.hash.includes('access_token') || url.hash.includes('refresh_token'));
      if (hasOAuthHash) {
        history.replaceState(null, '', url.pathname + (url.search && url.search !== '?' ? url.search : ''));
      }
    } catch (e) { /* non-critical */ }

    // Schedule a session expiry warning 5 min before the JWT expires
    scheduleSessionExpiryWarning(session);

    // Cyanix AI OAuth flow — ensureProfile is non-blocking:
    //   • Phase 1 (sync): sets State.profile instantly from JWT metadata
    //   • Phase 2 (async): syncs real DB profile in background, patches DOM when done
    // The app transition starts immediately after Phase 1 — no network wait on mobile.
    ensureProfile(session.user);   // intentionally NOT awaited

    _signedInUser = session.user.id;

    if (!appBuilt) {
      screen.style.opacity    = '0';
      screen.style.transform  = 'scale(1.02)';
      screen.style.transition = '0.4s ease';
      setTimeout(async () => {
        appBuilt = true;  // set inside callback so double-fire can't sneak through
        screen.style.display = 'none'; screen.style.visibility = 'hidden'; screen.style.pointerEvents = 'none';
        app.classList.add('visible');
        await buildApp();
        if (!State.isGuest) _subscribeToOwnFollowCount(); // realtime follower count sync
        document.dispatchEvent(new CustomEvent('devit:signed-in', { detail: { user: State.user } }));
        // Re-render any polls that loaded before auth resolved (they showed as unvoted)
        refreshAllPollsForUser(State.user.id);
        const firstName = State.profile?.display_name?.split(' ')[0] || State.profile?.username || 'pilot';
        toast(`Welcome${event === 'SIGNED_IN' ? '' : ' back'}, ${firstName}! ✈️`, 'plane-departure');
      }, 400);
    }

    } catch (bootErr) {
      // Ensure _syncPending never stays true, which would permanently lock the auth flow
      console.error('[Devit] Auth boot error:', bootErr);
      setAuthStatus('Sign-in error — please refresh and try again.', true);
    } finally {
      _syncPending = false;
    }

  }); // end onAuthStateChange

  // ── getSession() on page load ─────────────────────────────────
  // For returning users: resolves instantly from localStorage.
  // For OAuth redirects (implicit): Supabase parses #access_token from the hash.
  //
  // MOBILE FIX: Mobile browsers (iOS Safari, Chrome Android) sometimes deliver
  // the hash *after* JS has already executed, or restore the page from bfcache
  // (back-forward cache) skipping onAuthStateChange entirely.
  // Strategy:
  //   1. If hash token detected → retry getSession up to 5× with 200ms gaps
  //   2. pageshow listener → catches bfcache restore (iOS Safari back button)
  //   3. hashchange listener → catches late hash delivery on Android WebViews

  const _hash = window.location.hash;
  const _hasTokenHash = _hash && (_hash.includes('access_token') || _hash.includes('refresh_token'));

  if (_hasTokenHash) {
    // Hash is present — retry until Supabase parses it (mobile may be slow)
    let _gotSession = false;
    for (let i = 0; i < 5; i++) {
      const { data } = await sb.auth.getSession();
      if (data?.session) { _gotSession = true; break; }
      await new Promise(r => setTimeout(r, 200));
    }
    // If still nothing, Supabase may not have seen the hash yet — wait one more tick
    if (!_gotSession) await sb.auth.getSession();
  } else {
    await sb.auth.getSession();
  }

  // Catch hash arriving after page load (Android WebViews)
  window.addEventListener('hashchange', async () => {
    const h = window.location.hash;
    if (h && (h.includes('access_token') || h.includes('refresh_token'))) {
      await sb.auth.getSession();
    }
  });

  // Catch bfcache restore (iOS Safari back-button after OAuth redirect)
  // bfcache restores don't re-fire DOMContentLoaded or onAuthStateChange,
  // so we need to recheck the session manually here.
  window.addEventListener('pageshow', async (e) => {
    if (e.persisted && !State.user) {
      const { data } = await sb.auth.getSession();
      // If we now have a session but the app isn't built, boot it
      if (data?.session && !appBuilt) {
        // onAuthStateChange won't fire from bfcache — trigger manually
        const user = data.session.user;
        State.user = user;
        // Cyanix AI OAuth flow — non-blocking; Phase 1 sets optimistic profile instantly
        ensureProfile(user);       // intentionally NOT awaited
        _signedInUser = user.id;
        appBuilt = true;
        const screen = $('#auth-screen');
        const app    = $('#app');
        screen.style.display = 'none'; screen.style.visibility = 'hidden'; screen.style.pointerEvents = 'none';
        app.classList.add('visible');
        await buildApp();
        const firstName = State.profile?.display_name?.split(' ')[0] || 'dev';
        toast(`Welcome back, ${firstName}!`, 'rocket');
      }
    }
  });
  // If no session exists, auth screen stays visible (shown by default in HTML).
}

// ── Email verification banner (ported from Cyanix AI) ────────
// Shown after email sign-up until the user confirms their email.
// OAuth users (Google/GitHub) are always auto-confirmed — skip them.
function showEmailVerifyBanner(email) {
  if (document.getElementById('email-verify-banner')) return;
  const b = document.createElement('div');
  b.id = 'email-verify-banner';
  b.style.cssText = 'position:fixed;top:0;left:0;right:0;padding:10px 16px;background:var(--bg-surface,#1e1e2e);border-bottom:1px solid var(--border,#333);display:flex;align-items:center;justify-content:center;gap:10px;font-size:13px;z-index:9999;flex-wrap:wrap';
  b.innerHTML = `<i class="fa-solid fa-envelope-circle-check" style="color:var(--brand,#ff2d6e)"></i><span>Confirmation sent to <strong>${email}</strong> — check your inbox.</span><button id="verify-resend-btn" style="padding:3px 10px;border-radius:6px;background:var(--brand,#ff2d6e);color:#000;border:none;cursor:pointer;font-size:12px;font-weight:600">Resend</button><button onclick="document.getElementById('email-verify-banner').remove()" style="background:none;border:none;color:var(--text-muted,#888);cursor:pointer;font-size:18px;line-height:1;margin-left:4px">×</button>`;
  document.body.appendChild(b);
  document.getElementById('verify-resend-btn').addEventListener('click', async function() {
    this.textContent = 'Sending…';
    this.disabled = true;
    const { error } = await sb.auth.resend({ type: 'signup', email });
    if (error) toast('Failed to resend: ' + error.message, 'circle-exclamation');
    else       toast('Confirmation email resent!', 'envelope');
    setTimeout(() => { this.textContent = 'Resend'; this.disabled = false; }, 30_000);
  });
}

// ── Change Password Modal (shown on PASSWORD_RECOVERY when app is visible) ──
function openChangePasswordModal() {
  const modal = $('#modal-overlay');
  const body  = $('#modal-body');
  if (!modal || !body) return;
  $('#modal-title-text').textContent = 'Set New Password';
  modal.classList.add('open');
  body.innerHTML = `
    <div style="padding:16px;display:flex;flex-direction:column;gap:12px">
      <div class="auth-input-group">
        <label>New Password</label>
        <input type="password" id="recovery-pw" class="auth-input" placeholder="At least 6 characters" minlength="6" autocomplete="new-password">
      </div>
      <div class="auth-input-group">
        <label>Confirm New Password</label>
        <input type="password" id="recovery-pw2" class="auth-input" placeholder="Repeat password" autocomplete="new-password">
      </div>
      <div id="recovery-status" style="font-size:12px;color:var(--text-muted);display:none"></div>
      <button class="auth-btn-primary" id="recovery-save-btn"><i class="fa-solid fa-key"></i> Set Password</button>
    </div>
  `;
  $('#recovery-save-btn').addEventListener('click', async () => {
    const pw  = $('#recovery-pw').value;
    const pw2 = $('#recovery-pw2').value;
    const statusEl = $('#recovery-status');
    statusEl.style.display = 'block';
    if (pw.length < 6) { statusEl.style.color = 'var(--rose)'; statusEl.textContent = 'Password must be at least 6 characters.'; return; }
    if (pw !== pw2)   { statusEl.style.color = 'var(--rose)'; statusEl.textContent = 'Passwords do not match.'; return; }
    const btn = $('#recovery-save-btn');
    btn.disabled = true; btn.innerHTML = '<i class="fa-solid fa-spinner fa-spin"></i> Saving…';
    const { error } = await sb.auth.updateUser({ password: pw });
    if (error) {
      statusEl.style.color = 'var(--rose)';
      statusEl.textContent = 'Failed: ' + error.message;
      btn.disabled = false; btn.innerHTML = '<i class="fa-solid fa-key"></i> Set Password';
    } else {
      modal.classList.remove('open');
      toast('Password updated!', 'check');
    }
  });
}

/* ── Ensure Profile ─────────────────────────────────────────── */
//
// Cyanix AI OAuth flow — two-phase approach for fast mobile sign-in:
//
//  Phase 1 (INSTANT — sync): Build an optimistic profile from JWT metadata
//           so the app can render immediately without waiting for Supabase.
//           State.profile is set before any network call.
//
//  Phase 2 (BACKGROUND — async): Fetch or create the real DB profile and
//           upsert presence in parallel. Once resolved, patch State.profile
//           and refresh any rendered avatars/names silently.
//
// This eliminates the 1–3 s mobile auth delay caused by sequential DB calls
// blocking the auth → app transition.

function _buildOptimisticProfile(authUser) {
  const meta     = authUser.user_metadata || {};
  const provider = authUser.app_metadata?.provider || '';
  const isDiscord = provider === 'discord';
  // Discord OAuth supplies: user_name (their Discord username), full_name, avatar_url
  // NEVER use email — it would leak into the public username
  const baseUsername = (
    meta.user_name || meta.preferred_username || meta.custom_claims?.global_name ||
    'pilot_' + Math.random().toString(36).slice(2, 8)
  ).toLowerCase().replace(/[^a-z0-9_]/g, '_').slice(0, 30);
  // Display name: prefer Discord global name, fall back to username — never email
  const displayName = meta.full_name || meta.custom_claims?.global_name || meta.name || baseUsername;
  return {
    id: authUser.id,
    username: baseUsername,
    display_name: displayName,
    avatar_url: meta.avatar_url || meta.picture || null,
    bio: '', location: '', website: '',
    tech_stack: [], followers_count: 0, following_count: 0, posts_count: 0,
    is_github: false,
    _optimistic: true,
  };
}

function _patchRenderedProfile(profile) {
  // Silently refresh any avatar/display-name nodes already in the DOM
  // so the UI reflects the real DB profile without a full re-render.
  try {
    const name  = profile.display_name || profile.username || '';
    const color = avatarColor(name);

    // Topbar avatar
    const tbAvatar = document.getElementById('topbar-avatar');
    if (tbAvatar) {
      if (profile.avatar_url) {
        tbAvatar.outerHTML = `<img id="topbar-avatar" src="${profile.avatar_url}" style="width:32px;height:32px;border-radius:50%;object-fit:cover;cursor:pointer" onerror="devitAvatarFallback(this)" data-fallback-initials="${avatarInitials(name)}" data-fallback-color="${color}" data-fallback-size="32">`;
      } else {
        tbAvatar.style.background = color;
        tbAvatar.textContent = avatarInitials(name);
      }
    }

    // Sidebar display name / username
    const sbName = document.getElementById('sidebar-display-name');
    if (sbName) sbName.textContent = name;
    const sbUser = document.getElementById('sidebar-username');
    if (sbUser) sbUser.textContent = '@' + (profile.username || '');
  } catch (e) { /* non-critical */ }
}

async function _syncProfileInBackground(authUser) {
  const meta     = authUser.user_metadata || {};
  const email    = authUser.email || '';
  const provider = authUser.app_metadata?.provider || '';
  const isGitHub = provider === 'github';

  // Run DB fetch and presence upsert in parallel for max speed on mobile
  const [profileResult] = await Promise.all([
    sb.from('op_profiles').select('*').eq('id', authUser.id).single(),
    sb.from('op_presence').upsert(
      { id: authUser.id, online: true, last_seen: new Date().toISOString() },
      { onConflict: 'id' }
    ),
  ]);

  let { data: profile, error } = profileResult;

  if (!error && profile) {
    State.profile = profile;
    _patchRenderedProfile(profile);
    return;
  }

  // Profile doesn't exist yet — create it (new user via OAuth)
  // IMPORTANT: never use email or email prefix as username — it leaks private info publicly
  const baseUsername = (
    meta.user_name || meta.preferred_username || meta.custom_claims?.global_name ||
    'pilot_' + Math.random().toString(36).slice(2, 8)
  ).toLowerCase().replace(/[^a-z0-9_]/g, '_').slice(0, 30);
  // Display name: use Discord global name or username — never email
  const display_name = meta.full_name || meta.custom_claims?.global_name || meta.name || baseUsername;
  const avatar_url   = meta.avatar_url || meta.picture || null;

  let newProfile = null;
  let createErr  = null;
  for (let attempt = 0; attempt < 3; attempt++) {
    const username = attempt === 0
      ? baseUsername
      : baseUsername.slice(0, 25) + '_' + Math.random().toString(36).slice(2, 6);
    const res = await sb
      .from('op_profiles')
      .upsert({
        id: authUser.id, username, display_name, avatar_url,
        bio: '', location: '', website: '',
        tech_stack: [], followers_count: 0, following_count: 0, posts_count: 0,
        is_github: isGitHub,
      }, { onConflict: 'id' })
      .select().single();
    newProfile = res.data;
    createErr  = res.error;
    if (!createErr || createErr.code !== '23505') break;
  }

  if (createErr) {
    console.error('[Devit] Failed to create profile:', createErr);
    // State.profile already has the optimistic version — keep it
    // (minus the internal flag) so the app stays functional
    if (State.profile?._optimistic) delete State.profile._optimistic;
  } else {
    State.profile = newProfile;
    _patchRenderedProfile(newProfile);
  }
}

async function ensureProfile(authUser) {
  // Phase 1 — instant: give State.profile an optimistic value from JWT metadata.
  // The app can render immediately without any network round-trip.
  const cached = State.profile?.id === authUser.id && !State.profile?._optimistic;
  if (!cached) {
    State.profile = _buildOptimisticProfile(authUser);
  }

  // Phase 2 — background: fetch/create real DB profile + presence in parallel.
  // We deliberately do NOT await this — the caller (onAuthStateChange) proceeds
  // to buildApp() immediately while this resolves in the background.
  _syncProfileInBackground(authUser).catch(e =>
    console.error('[Devit] Background profile sync failed:', e)
  );
}

/* ── MutationGuard — client-side rate limiting for DB mutations ── */
window.MutationGuard = (() => {
  const _cooldowns = {}; // { action: expiresAtMs }

  return {
    /**
     * Wrap a DB insert with a cooldown check.
     * @param {string} action  e.g. 'post', 'like', 'message', 'comment'
     * @param {Function} fn    async () => supabase insert/upsert call
     * @returns {Promise<{data, error}>}
     */
    async wrapInsert(action, fn) {
      const now = Date.now();
      const expires = _cooldowns[action] || 0;
      if (now < expires) {
        const secsLeft = Math.ceil((expires - now) / 1000);
        return {
          data: null,
          error: { blocked: true, message: `Please wait ${secsLeft}s before doing that again.` },
        };
      }
      const result = await fn();
      if (!result.error) {
        // Default cooldowns per action type (ms)
        const COOLDOWNS = { post: 10_000, like: 1_000, message: 2_000, comment: 3_000 };
        _cooldowns[action] = now + (COOLDOWNS[action] ?? 3_000);
      }
      return result;
    },

    /** Sync cooldown state from DB — survives page refresh if you store to a table */
    async syncCooldowns() {
      // No-op stub — extend to read from a `user_cooldowns` Supabase table if needed
    },
  };
})();

/* ── Deep Link Handler ──────────────────────────────────────── */
async function handleDeepLink() {
  const match = window.location.pathname.match(/^\/post\/([0-9a-f-]{36})$/i);
  if (!match) return;

  const postId = match[1];

  // Clean the URL back to / so Back button works naturally
  history.replaceState(null, '', '/');

  const { data: post, error } = await sb
    .from('op_posts')
    .select(`
      *,
      profiles:op_profiles!author_id(id, username, display_name, avatar_url)
    `)
    .eq('id', postId)
    .single();

  if (error || !post) {
    toast('Post not found or may have been deleted.', 'circle-exclamation');
    return;
  }

  openPostThread(post, post.profiles);
}

/* ── Build App ──────────────────────────────────────────────── */
async function buildApp() {
  // Restore any active rate-limit cooldowns from the DB (survives page refresh)
  if (window.MutationGuard) await MutationGuard.syncCooldowns();

  // Build UI immediately — don't let DB schema check block nav from rendering
  buildTopbar();
  buildSidebar();
  // Restore sidebar collapsed state from Supabase (fall back to localStorage for instant paint)
  if (localStorage.getItem('devit-sidebar-collapsed') === '1') {
    document.getElementById('sidebar')?.classList.add('sidebar-collapsed');
  }
  if (State.user?.id && !State.isGuest) {
    sb.from('op_user_preferences').select('sidebar_collapsed').eq('user_id', State.user.id).single()
      .then(({ data }) => {
        if (data != null) {
          const sidebar = document.getElementById('sidebar');
          if (sidebar) sidebar.classList.toggle('sidebar-collapsed', !!data.sidebar_collapsed);
        }
      }).catch(() => {});
  }
  buildRightbar();
  initBottomNav();
  navigateTo('feed');
  handleDeepLink(); // Handle direct /post/:id URL entry

  // Remove aria-hidden now that app is active
  const appEl = document.getElementById('app');
  if (appEl) appEl.removeAttribute('aria-hidden');

  // Non-blocking background tasks
  bootstrapSchema(); // fire and forget — only logs warnings
  initPresenceRealtime();
  initGlobalNotifSub();
  loadUnreadCounts();
  registerServiceWorker();
  handleInviteOnLoad();
  // Push notifications: opt-in only via settings — not auto-prompted on login
  // Enable swipe left/right to switch tabs on mobile
  setTimeout(() => initMainSwipeNavigation(), 500);
  // Enable swipe on the topbar tabs as well
  setTimeout(() => initTopbarSwipe(), 500);
}

/* ── Invite Link System ─────────────────────────────────────── */

const INVITE_EDGE_URL = `${window.DEVIT_CONFIG.SUPABASE_URL}/functions/v1/invite`;
const SITE_URL = window.DEVIT_CONFIG.SITE_URL;

/** Generate a DEVIT-XXXXXX style code */
function generateInviteCode() {
  const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789'; // no I,O,0,1 (ambiguous)
  let code = 'DEVIT-';
  for (let i = 0; i < 6; i++) code += chars[Math.floor(Math.random() * chars.length)];
  return code;
}

/**
 * Get or create a permanent invite link for the current user.
 * Re-uses existing codes — one per user, stored in invite_links.
 */
async function getOrCreateInviteCode() {
  // Check for existing code
  const { data: existing } = await sb
    .from('op_invite_links')
    .select('code')
    .eq('inviter_id', State.user.id)
    .limit(1)
    .single();

  if (existing?.code) return existing.code;

  // Create a new one (retry on collision)
  for (let attempt = 0; attempt < 5; attempt++) {
    const code = generateInviteCode();
    const { data, error } = await sb
      .from('op_invite_links')
      .insert({ code, inviter_id: State.user.id })
      .select('code')
      .single();
    if (!error && data) return data.code;
    // 23505 = unique_violation — code already taken, retry
    if (error?.code !== '23505') {
      console.error('[Devit] Failed to create invite link:', error);
      return null;
    }
  }
  return null;
}

/**
 * Build the shareable invite URL that routes through the frontend.
 * Format: https://devit-six.vercel.app/invite?code=DEVIT-XXXXXX
 */
function buildInviteUrl(code) {
  return `${SITE_URL.replace(/\/$/, '')}/invite?code=${encodeURIComponent(code)}`;
}

/**
 * Called on app boot — checks if the current URL has ?invite=CODE.
 * If so, records the usage and shows a welcome modal to the new user.
 */
async function handleInviteOnLoad() {
  const params = new URLSearchParams(window.location.search);
  const code = params.get('code');
  if (!code || !window.location.pathname.startsWith('/invite')) return;

  // Clean the invite param from the URL immediately (don't leave it in history)
  try {
    const clean = new URL(window.location.href);
    clean.searchParams.delete('invite');
    history.replaceState(null, '', clean.pathname + (clean.search !== '?' ? clean.search : ''));
  } catch (_) {}

  // Record usage via RPC (server-side atomic increment)
  const { data: valid } = await sb.rpc('use_invite', { invite_code: code });

  if (!valid) {
    // Expired or non-existent — show a quiet toast, don't make a big deal
    toast('Invite link expired or invalid', 'circle-exclamation');
    return;
  }

  // Save code to profile so we can track attribution
  await sb
    .from('op_profiles')
    .update({ invited_by_code: code })
    .eq('id', State.user.id);

  // Fetch the inviter profile to show in the welcome card
  const { data: invite } = await sb
    .from('op_invite_links')
    .select('inviter_id, profiles(id, username, display_name, avatar_url, bio)')
    .eq('code', code)
    .single();

  if (!invite) return;
  showInviteWelcomeModal(invite.profiles);
}

/**
 * Show a branded welcome modal when a user arrives via an invite link.
 */
function showInviteWelcomeModal(inviter) {
  const modal = $('#modal-overlay');
  const body  = $('#modal-body');
  if (!modal || !body) return;

  $('#modal-title-text').textContent = 'Welcome to Devit!';
  modal.classList.add('open');

  body.innerHTML = `
    <div style="padding:24px;text-align:center;display:flex;flex-direction:column;align-items:center;gap:16px">

      <!-- Devit logo -->
      <img src="devit.png" alt="Devit" style="width:52px;height:52px;border-radius:14px;border:1px solid rgba(255,45,110,0.2);background:rgba(255,45,110,0.08);padding:6px;object-fit:contain;">

      <!-- Inviter avatar -->
      <div style="position:relative">
        ${avatarHtml(inviter, 68)}
        <div style="position:absolute;bottom:-4px;right:-4px;width:22px;height:22px;background:var(--emerald);border-radius:50%;border:2px solid var(--bg-surface);display:flex;align-items:center;justify-content:center;">
          <i class="fa-solid fa-check" style="font-size:10px;color:#050508"></i>
        </div>
      </div>

      <div>
        <div style="font-size:18px;font-weight:800;font-family:var(--font-display);margin-bottom:4px">
          ${escapeHtml(inviter?.display_name || inviter?.username || 'A developer')} invited you
        </div>
        <div style="font-size:13px;color:var(--cyan)">@${escapeHtml(inviter?.username || '')}</div>
      </div>

      <p style="font-size:13px;color:var(--text-secondary);line-height:1.6;max-width:280px">
        You're joining Devit — the social platform built for developers.<br>
        Code. Connect. Ship.
      </p>

      <div style="display:flex;flex-direction:column;gap:8px;width:100%;max-width:280px">
        <button id="invite-welcome-profile" class="auth-btn-primary" style="width:100%;padding:12px">
          <i class="fa-solid fa-user-plus"></i> View @${escapeHtml(inviter?.username || '')}'s profile
        </button>
        <button id="invite-welcome-dismiss" class="auth-btn-magic" style="width:100%;padding:12px">
          Explore Devit
        </button>
      </div>
    </div>
  `;

  $('#invite-welcome-profile').addEventListener('click', () => {
    modal.classList.remove('open');
    if (inviter?.id) renderProfile($('#main'), inviter.id);
  });
  $('#invite-welcome-dismiss').addEventListener('click', () => {
    modal.classList.remove('open');
  });
}

/**
 * Open the share invite modal — called from the profile share button.
 */
async function openShareInviteModal(profile) {
  const modal = $('#modal-overlay');
  const body  = $('#modal-body');
  if (!modal || !body) return;

  $('#modal-title-text').textContent = 'Invite to Devit';
  modal.classList.add('open');

  // Loading state
  body.innerHTML = `
    <div style="padding:32px;text-align:center;color:var(--text-muted)">
      <i class="fa-solid fa-spinner fa-spin" style="font-size:20px;color:var(--cyan)"></i>
      <div style="margin-top:10px;font-size:13px">Generating invite link…</div>
    </div>
  `;

  const code = await getOrCreateInviteCode();

  if (!code) {
    body.innerHTML = `<div style="padding:32px;text-align:center;color:var(--rose)">Failed to generate invite link. Try again.</div>`;
    return;
  }

  const inviteUrl = buildInviteUrl(code);

  body.innerHTML = `
    <div style="padding:20px;display:flex;flex-direction:column;gap:16px">

      <!-- Preview card -->
      <div style="
        background:var(--bg-elevated);border:1px solid var(--border);border-radius:var(--radius-md);
        overflow:hidden;
      ">
        <!-- OG image strip -->
        <div style="
          height:72px;
          background:linear-gradient(135deg, rgba(255,45,110,0.15), rgba(255,107,53,0.15));
          display:flex;align-items:center;justify-content:center;border-bottom:1px solid var(--border);
          gap:12px;padding:0 16px;
        ">
          <img src="devit.png" alt="Devit" style="width:28px;height:28px;border-radius:6px;object-fit:contain;">
          <div style="font-size:11px;font-weight:600;color:var(--text-muted);letter-spacing:0.08em;text-transform:uppercase">Devit · Code. Connect. Ship.</div>
        </div>

        <!-- Inviter info -->
        <div style="padding:14px 16px;display:flex;align-items:center;gap:12px">
          ${avatarHtml(profile, 44)}
          <div style="flex:1;min-width:0">
            <div style="font-size:14px;font-weight:700">${escapeHtml(profile?.display_name || profile?.username || 'You')} invited you to Devit</div>
            <div style="font-size:12px;color:var(--text-muted);margin-top:2px">Join the developer social platform</div>
          </div>
        </div>
      </div>

      <!-- Invite code display -->
      <div style="
        display:flex;align-items:center;justify-content:space-between;
        background:var(--bg-elevated);border:1px solid var(--border-active);border-radius:var(--radius-md);
        padding:10px 14px;
      ">
        <div>
          <div style="font-size:10px;color:var(--text-muted);letter-spacing:0.06em;text-transform:uppercase;margin-bottom:2px">Invite code</div>
          <div style="font-size:16px;font-weight:800;font-family:var(--font-mono);color:var(--cyan);letter-spacing:0.08em">${escapeHtml(code)}</div>
        </div>
        <button id="copy-code-btn" style="
          background:var(--bg-float);border:1px solid var(--border);border-radius:var(--radius-sm);
          color:var(--text-secondary);padding:6px 10px;font-size:12px;font-weight:600;
          transition:all 0.15s;
        ">Copy code</button>
      </div>

      <!-- Full URL display -->
      <div style="
        background:var(--bg-void);border:1px solid var(--border);border-radius:var(--radius-sm);
        padding:8px 12px;font-family:var(--font-mono);font-size:10px;color:var(--text-muted);
        word-break:break-all;line-height:1.5;
      ">${escapeHtml(inviteUrl)}</div>

      <!-- Action buttons -->
      <div style="display:flex;flex-direction:column;gap:8px">
        <button id="share-invite-copy" class="auth-btn-primary" style="width:100%;padding:12px">
          <i class="fa-solid fa-link"></i> Copy invite link
        </button>
        <button id="share-invite-native" class="auth-btn-magic" style="width:100%;padding:12px;display:${navigator.share ? 'block' : 'none'}">
          <i class="fa-solid fa-share-nodes"></i> Share via…
        </button>
      </div>

      <!-- Fine print -->
      <div style="font-size:11px;color:var(--text-muted);text-align:center;line-height:1.5">
        Link is permanent · No expiry · Unlimited uses
      </div>
    </div>
  `;

  // Copy full link
  $('#share-invite-copy').addEventListener('click', async () => {
    try {
      await navigator.clipboard.writeText(inviteUrl);
      const btn = $('#share-invite-copy');
      btn.innerHTML = '<i class="fa-solid fa-check"></i> Copied!';
      setTimeout(() => { btn.innerHTML = '<i class="fa-solid fa-link"></i> Copy invite link'; }, 2000);
      toast('Invite link copied!', 'link');
    } catch (_) {
      toast('Could not copy — try manually', 'circle-exclamation');
    }
  });

  // Copy code only
  $('#copy-code-btn').addEventListener('click', async () => {
    try {
      await navigator.clipboard.writeText(code);
      const btn = $('#copy-code-btn');
      btn.textContent = 'Copied!';
      btn.style.color = 'var(--emerald)';
      setTimeout(() => { btn.textContent = 'Copy code'; btn.style.color = ''; }, 2000);
    } catch (_) {}
  });

  // Native share sheet (mobile)
  const nativeBtn = $('#share-invite-native');
  if (nativeBtn) {
    nativeBtn.addEventListener('click', async () => {
      try {
        await navigator.share({
          title: `Join me on Devit!`,
          text: `${profile?.display_name || profile?.username || 'A dev'} invited you to Devit — Code. Connect. Ship.`,
          url: inviteUrl,
        });
      } catch (_) {} // user dismissed — no-op
    });
  }
}

function initGlobalNotifSub() {
  realtimeManager.subscribe(
    `global_notifs_${State.user.id}`,
    'notifications',
    'INSERT',
    _payload => {
      State.unreadNotifs++;
      updateBadges();
      if (State.currentView === 'notifications') loadNotifications();
    },
    `user_id=eq.${State.user.id}`
  );
  // Note: global — intentionally never unsubscribed until sign-out
}

function initBottomNav() {
  // Force pointer-events on all nav buttons (overrides any CSS that may block clicks)
  document.querySelectorAll('.bnav-btn').forEach(btn => {
    btn.style.pointerEvents = 'auto';
    btn.style.cursor = 'pointer';
  });

  // Direct listeners
  document.querySelectorAll('.bnav-btn[data-nav]').forEach(btn => {
    btn.addEventListener('click', () => navigateTo(btn.dataset.nav));
  });
  const bnavPost = document.getElementById('bnav-post-btn');
  if (bnavPost) bnavPost.addEventListener('click', openNewPostModal);

  // Delegated fallback on the nav element — catches any missed clicks
  const bottomNav = document.getElementById('bottom-nav');
  if (bottomNav) {
    bottomNav.addEventListener('click', e => {
      const btn = e.target.closest('.bnav-btn');
      if (!btn) return;
      if (btn.id === 'bnav-post-btn') { openNewPostModal(); return; }
      const navTarget = btn.dataset.nav;
      if (navTarget === 'messages' && guestGuard('send messages')) return;
      if ((navTarget === 'profile' || navTarget === 'bookmarks' || navTarget === 'notifications') && guestGuard('access that')) return;
      const nav = btn.dataset.nav;
      if (nav) navigateTo(nav);
    });
  }
}

/* ── Presence Realtime ──────────────────────────────────────── */
function initPresenceRealtime() {
  const channel = sb.channel('presence_global', {
    config: { presence: { key: State.user.id } }
  });

  channel
    .on('presence', { event: 'sync' }, () => {
      const state = channel.presenceState();
      State.onlineUsers = new Set(Object.keys(state));
      updatePresenceDots();
    })
    .on('presence', { event: 'join' }, ({ newPresences }) => {
      newPresences.forEach(p => State.onlineUsers.add(p.key));
      updatePresenceDots();
    })
    .on('presence', { event: 'leave' }, ({ leftPresences }) => {
      leftPresences.forEach(p => State.onlineUsers.delete(p.key));
      updatePresenceDots();
    })
    .subscribe(async status => {
      if (status === 'SUBSCRIBED') {
        await channel.track({ online: true, user_id: State.user.id });
      }
    });

  // Register with realtimeManager (global — never removed on view change)
  realtimeManager.subscribeRaw('global:presence', channel, () => {});

  // Heartbeat to keep presence alive — uses realtime events, not polling
  setInterval(async () => {
    await channel.track({ online: true, user_id: State.user.id });
  }, 30000);

  // Mark offline on page unload
  window.addEventListener('beforeunload', () => {
    sb.from('op_presence').update({ online: false }).eq('id', State.user.id);
  });
}

function updatePresenceDots() {
  // Update any visible online dots
  document.querySelectorAll('[data-presence-uid]').forEach(dot => {
    const uid = dot.dataset.presenceUid;
    dot.classList.toggle('online', State.onlineUsers.has(uid));
    dot.classList.toggle('offline', !State.onlineUsers.has(uid));
  });
}

async function loadUnreadCounts() {
  const { count: notifCount } = await sb
    .from('op_notifications')
    .select('*', { count: 'exact', head: true })
    .eq('user_id', State.user.id)
    .eq('read', false);
  State.unreadNotifs = notifCount || 0;

  // Count unread DMs
  const { data: convos } = await sb
    .from('op_conversations')
    .select('id')
    .or(`participant_a.eq.${State.user.id},participant_b.eq.${State.user.id}`);

  if (convos?.length) {
    const convoIds = convos.map(c => c.id);
    const { count: msgCount } = await sb
      .from('op_messages')
      .select('*', { count: 'exact', head: true })
      .in('conversation_id', convoIds)
      .neq('sender_id', State.user.id)
      .eq('read', false);
    State.unreadMessages = msgCount || 0;
  }

  updateBadges();
}

function updateBadges() {
  const notifBadge = $('#nav-notifs .badge');
  const msgBadge   = $('#nav-messages-btn .badge');
  if (notifBadge) notifBadge.style.display = State.unreadNotifs > 0 ? '' : 'none';
  if (msgBadge)   msgBadge.style.display   = State.unreadMessages > 0 ? '' : 'none';
  // Bottom nav badges
  const bnavNotifs = document.getElementById('bnav-badge-notifs');
  const bnavMsgs   = document.getElementById('bnav-badge-messages');
  if (bnavNotifs)  bnavNotifs.classList.toggle('visible', State.unreadNotifs > 0);
  if (bnavMsgs)    bnavMsgs.classList.toggle('visible', State.unreadMessages > 0);
  // bnav-badge-links removed (links view removed)
}

/* ── Topbar ─────────────────────────────────────────────────── */
function buildTopbar() {
  const tb = $('#topbar');
  tb.innerHTML = `
    <div class="topbar-logo">
      <img src="devit.png" alt="Devit" style="width:30px;height:30px;border-radius:8px;object-fit:cover">
      <span>Devit</span>
    </div>
    <div class="topbar-search">
      <span class="topbar-search-icon">
        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5"><circle cx="11" cy="11" r="8"/><path d="m21 21-4.35-4.35"/></svg>
      </span>
      <input type="search" id="search-input" placeholder="Search people, posts, communities…" autocomplete="off" autocorrect="off" autocapitalize="off" spellcheck="false" inputmode="search">
    </div>
    <div class="topbar-actions">
      <button class="topbar-action-btn" id="nav-notifs" title="Notifications">
        <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M18 8A6 6 0 0 0 6 8c0 7-3 9-3 9h18s-3-2-3-9"/><path d="M13.73 21a2 2 0 0 1-3.46 0"/></svg>
        <span class="badge" style="${State.unreadNotifs > 0 ? '' : 'display:none'}"></span>
      </button>
      <button class="topbar-action-btn" id="nav-messages-btn" title="Messages">
        <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z"/></svg>
        <span class="badge" style="${State.unreadMessages > 0 ? '' : 'display:none'}"></span>
      </button>

      <button class="topbar-action-btn" id="topbar-signout-btn" title="Sign out"><i class="fa-solid fa-power-off"></i></button>
      <button id="theme-toggle" title="Toggle theme" aria-label="Toggle light/dark mode">
        <i class="fa-solid fa-moon"></i>
      </button>
      <div class="topbar-avatar" id="topbar-avatar-btn">
        ${State.profile?.avatar_url
          ? `<img src="${State.profile.avatar_url}" style="width:100%;height:100%;object-fit:cover;border-radius:50%">`
          : avatarInitials(State.profile?.display_name || 'U')}
      </div>
    </div>
  `;

  $('#nav-notifs').addEventListener('click', () => navigateTo('notifications'));
  $('#nav-messages-btn').addEventListener('click', () => navigateTo('messages'));
  $('#topbar-avatar-btn').addEventListener('click', () => navigateTo('profile'));
  $('#topbar-signout-btn').addEventListener('click', async () => {
    await sb.from('op_presence').update({ online: false }).eq('id', State.user.id);
    await sb.auth.signOut();
    toast('Signed out. See you soon!', 'right-from-bracket');
  });

  // Theme toggle
  const themeToggleBtn = $('#theme-toggle');
  if (themeToggleBtn) {
    const applyTheme = (theme, persist = true) => {
      document.documentElement.setAttribute('data-theme', theme);
      const icon = themeToggleBtn.querySelector('i');
      if (icon) icon.className = theme === 'dark' ? 'fa-solid fa-moon' : 'fa-solid fa-sun';
      themeToggleBtn.title = theme === 'dark' ? 'Switch to light mode' : 'Switch to dark mode';
      if (persist && State.user?.id && !State.isGuest) {
        // fire-and-forget upsert into op_user_preferences
        sb.rpc('set_user_preference', { p_theme: theme }).catch(() => {});
      }
    };
    // Init from Supabase (fall back to localStorage for instant paint, then patch)
    const localTheme = localStorage.getItem('devit-theme') || 'dark';
    applyTheme(localTheme, false); // instant paint — no persist
    if (State.user?.id && !State.isGuest) {
      sb.from('op_user_preferences').select('theme').eq('user_id', State.user.id).single()
        .then(({ data }) => {
          if (data?.theme) applyTheme(data.theme, false);
        }).catch(() => {});
    }
    themeToggleBtn.addEventListener('click', () => {
      const current = document.documentElement.getAttribute('data-theme') || 'dark';
      applyTheme(current === 'dark' ? 'light' : 'dark');
    });
  }

  // Search with debounce
  let searchTimeout;
  $('#search-input').addEventListener('input', e => {
    clearTimeout(searchTimeout);
    const q = e.target.value.trim();
    if (q.length > 1) {
      searchTimeout = setTimeout(() => runSearch(q), 350);
    }
  });
  $('#search-input').addEventListener('keydown', e => {
    if (e.key === 'Escape') { e.target.value = ''; closeSearch(); }
    if (e.key === 'Enter') { const q = e.target.value.trim(); if (q) runSearch(q); }
  });
  $('#search-input').addEventListener('focus', () => {
    if ($('#search-input').value.trim().length > 1) runSearch($('#search-input').value.trim());
  });
}

/* ── Search ─────────────────────────────────────────────────── */
async function runSearch(query) {
  const existingOverlay = document.getElementById('search-overlay');
  if (existingOverlay) existingOverlay.remove();

  const overlay = el('div', '', '');
  overlay.id = 'search-overlay';
  overlay.style.cssText = `position:fixed;top:56px;left:50%;transform:translateX(-50%);width:560px;max-width:90vw;background:var(--bg-surface);border:1px solid var(--border);border-radius:var(--radius-lg);z-index:900;box-shadow:0 20px 60px rgba(0,0,0,0.6);overflow:hidden;max-height:70vh;overflow-y:auto`;

  overlay.innerHTML = `<div style="padding:12px 16px;font-size:12px;color:var(--text-muted);border-bottom:1px solid var(--border)">Searching for "${escapeHtml(query)}"…</div>`;
  document.body.appendChild(overlay);

  const closeOnClick = e => { if (!overlay.contains(e.target) && e.target !== $('#search-input')) { overlay.remove(); document.removeEventListener('click', closeOnClick); } };
  setTimeout(() => document.addEventListener('click', closeOnClick), 100);

  // Full-text search via Postgres tsvector RPC (falls back gracefully to ilike)
  const tsQuery = query.trim().split(/\s+/).filter(Boolean).join(' & ');

  // Run profiles + FTS in parallel — one call each, no duplication
  const [profilesRes, ftsRes] = await Promise.all([
    sb.from('op_profiles').select('id, username, display_name, avatar_url, bio')
      .or(`username.ilike.%${query}%,display_name.ilike.%${query}%`).limit(5),
    sb.rpc('search_posts_fts', { query_text: tsQuery, max_results: 5 }),
  ]);

  // Fall back to ilike if FTS RPC unavailable
  let posts;
  if (!ftsRes.error && ftsRes.data) {
    posts = ftsRes.data;
  } else {
    const { data: ilikePosts } = await sb.from('op_posts')
      .select('id, content, created_at, author_id, profiles(username, display_name, avatar_url)')
      .ilike('content', `%${query}%`).limit(5);
    posts = ilikePosts;
  }

  const profiles = profilesRes?.data;

  let html = '';
  if (profiles?.length) {
    html += `<div style="padding:8px 16px 4px;font-size:11px;font-weight:700;color:var(--text-muted);text-transform:uppercase;letter-spacing:0.08em">People</div>`;
    profiles.forEach(p => {
      html += `<div class="search-result-item" data-uid="${p.id}" style="display:flex;align-items:center;gap:10px;padding:10px 16px;cursor:pointer">
        ${avatarHtml(p, 32)}
        <div><div style="font-weight:600;font-size:13px">${escapeHtml(p.display_name || p.username)}</div><div style="font-size:12px;color:var(--text-muted)">@${escapeHtml(p.username)}</div></div>
      </div>`;
    });
  }
  if (posts?.length) {
    html += `<div style="padding:8px 16px 4px;font-size:11px;font-weight:700;color:var(--text-muted);text-transform:uppercase;letter-spacing:0.08em;border-top:1px solid var(--border);margin-top:4px">Posts</div>`;
    posts.forEach(p => {
      html += `<div class="search-result-item" data-pid="${p.id}" style="padding:10px 16px;cursor:pointer">
        <div style="font-size:12px;color:var(--text-muted)">@${escapeHtml(p.profiles?.username || '?')}</div>
        <div style="font-size:13px;margin-top:2px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis">${escapeHtml(p.content)}</div>
      </div>`;
    });
  }
  if (!html) html = `<div style="padding:20px;text-align:center;color:var(--text-muted);font-size:13px">No results for "${escapeHtml(query)}"</div>`;

  overlay.innerHTML = html;

  overlay.querySelectorAll('.search-result-item[data-uid]').forEach(item => {
    item.addEventListener('click', () => { overlay.remove(); renderProfile($('#main'), item.dataset.uid); });
  });
  overlay.querySelectorAll('.search-result-item[data-pid]').forEach(item => {
    item.addEventListener('click', () => { overlay.remove(); navigateTo('feed'); });
  });
}

function closeSearch() {
  const overlay = document.getElementById('search-overlay');
  if (overlay) overlay.remove();
}

/* ── Sidebar ────────────────────────────────────────────────── */
function toggleSidebar() {
  const sidebar = document.getElementById('sidebar');
  const collapsed = sidebar.classList.toggle('sidebar-collapsed');
  // Update aria-label for accessibility
  const hamBtn = sidebar.querySelector('#sidebar-ham-btn');
  if (hamBtn) hamBtn.setAttribute('aria-label', collapsed ? 'Expand sidebar' : 'Collapse sidebar');
  // Persist to Supabase (fire-and-forget)
  if (State.user?.id && !State.isGuest) {
    sb.rpc('set_user_preference', { p_sidebar_collapsed: collapsed }).catch(() => {});
  }
}

function buildSidebar() {
  const sb_el = $('#sidebar');
  const allLinks = [
    { id: 'feed',          icon: '<i class="fa-solid fa-house"></i>',        label: 'Activity',     guestOk: true },
    { id: 'explore',       icon: '<i class="fa-solid fa-compass"></i>',       label: 'Discover',     guestOk: true },
    { id: 'notifications', icon: '<i class="fa-solid fa-bell"></i>',          label: 'Alerts',       guestOk: false, badge: State.unreadNotifs },
    { id: 'messages',      icon: '<i class="fa-solid fa-message"></i>',       label: 'DMs',          guestOk: false, badge: State.unreadMessages },
    { id: 'profile',       icon: '<i class="fa-solid fa-user"></i>',          label: 'Profile',      guestOk: false },
    { id: 'bookmarks',     icon: '<i class="fa-solid fa-bookmark"></i>',      label: 'Saved',        guestOk: false },
    { id: 'leaderboard',   icon: '<i class="fa-solid fa-trophy"></i>',        label: 'Leaderboard',  guestOk: true },
    { id: 'settings',      icon: '<i class="fa-solid fa-gear"></i>',          label: 'Settings',     guestOk: false },
  ];
  const links = State.isGuest ? allLinks.filter(l => l.guestOk) : allLinks;

  let html = `<button class="sidebar-ham-btn" aria-label="Toggle sidebar" id="sidebar-ham-btn">
    <i class="fa-solid fa-bars"></i>
  </button>`;
  if (!State.isGuest) html += `<button class="sidebar-new-post-btn" id="sidebar-new-post-btn" aria-label="New Post">
    <i class="fa-solid fa-plus"></i>
    <span>New Post</span>
  </button>`;
  html += `<div class="sidebar-section-label">navigate</div>`;
  links.forEach(l => {
    html += `<div class="sidebar-link${l.id === State.currentView ? ' active' : ''}" data-nav="${l.id}">
      <span class="icon">${l.icon}</span>
      <span>${l.label}</span>
      ${l.badge ? `<span class="badge-count">${l.badge}</span>` : ''}
    </div>`;
  });

  if (State.isGuest) {
    html += `<div style="margin-top:16px;padding:0 12px">
      <button onclick="showGuestPrompt('sign in')" style="width:100%;padding:11px;border-radius:12px;background:rgba(255,45,110,0.1);border:1px solid rgba(255,45,110,0.3);color:var(--cyan);font-size:13px;font-weight:700;cursor:pointer;display:flex;align-items:center;justify-content:center;gap:8px;font-family:var(--font-body)">
        <i class="fa-brands fa-discord"></i> Sign in
      </button>
    </div>`;
  }
  sb_el.innerHTML = html;

  // Re-attach hamburger listener after innerHTML reset
  const ham = sb_el.querySelector('#sidebar-ham-btn');
  if (ham) ham.addEventListener('click', toggleSidebar);

  // New Post button
  const sidebarPostBtn = sb_el.querySelector('#sidebar-new-post-btn');
  if (sidebarPostBtn) sidebarPostBtn.addEventListener('click', openNewPostModal);

  $$('.sidebar-link[data-nav]', sb_el).forEach(link => {
    link.addEventListener('click', () => {
      if (link.dataset.nav) {
          if (link.dataset.nav === 'messages' && guestGuard('send messages')) return;
          navigateTo(link.dataset.nav);
        }
    });
  });
}

async function loadSidebarCommunities() {
  if (State.isGuest) return;
  const { data } = await sb
    .from('op_community_members')
    .select('community_id, communities(id, name, icon, color)')
    .eq('user_id', State.user.id)
    .limit(10);

  const container = $('#sidebar-communities');
  if (!container) return;

  if (!data?.length) {
    container.innerHTML = `<div style="padding:8px 16px;font-size:12px;color:var(--text-muted)">No communities yet — explore!</div>`;
    return;
  }

  container.innerHTML = data.map(m => `
    <div class="sidebar-link sidebar-community-link" data-cid="${m.communities.id}" style="gap:8px">
      <span style="font-size:16px">${m.communities.icon}</span>
      <span style="font-size:13px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${m.communities.name}</span>
    </div>
  `).join('');

  $$('.sidebar-community-link', container).forEach(link => {
    link.addEventListener('click', () => openCommunity(link.dataset.cid));
  });
}

function updateSidebarActive() {
  $$('.sidebar-link[data-nav]').forEach(l => {
    l.classList.toggle('active', l.dataset.nav === State.currentView);
  });
}

/* ── Rightbar ───────────────────────────────────────────────── */
async function buildRightbar() {
  const rb = $('#rightbar');
  rb.innerHTML = `
    <div class="rightbar-section">
      <div class="rightbar-title">Who to Follow</div>
      <div id="who-to-follow"><div style="color:var(--text-muted);font-size:13px">Loading…</div></div>
    </div>
    <div class="rightbar-section">
      <div class="rightbar-title">Trending Topics</div>
      <div id="trending-tags"><div style="color:var(--text-muted);font-size:13px">Loading…</div></div>
    </div>
  `;

  loadWhoToFollow();
  loadTrendingTags();
}

async function loadWhoToFollow() {
  // Get people the user follows
  const { data: followingData } = await sb
    .from('op_follows')
    .select('following_id')
    .eq('follower_id', State.user.id);

  const followingIds = (followingData || []).map(f => f.following_id);
  followingIds.push(State.user.id); // exclude self

  const { data: suggestions } = await sb
    .from('op_profiles')
    .select('id, username, display_name, avatar_url, bio, followers_count')
    .not('id', 'in', `(${followingIds.join(',') || State.user.id})`)
    .order('followers_count', { ascending: false })
    .limit(4);

  const container = $('#who-to-follow');
  if (!container) return;

  if (!suggestions?.length) {
    container.innerHTML = `<div style="color:var(--text-muted);font-size:12px">You're following everyone! 🎉</div>`;
    return;
  }

  container.innerHTML = suggestions.map(p => `
    <div class="follow-suggestion">
      ${avatarHtml(p, 36)}
      <div class="follow-suggestion-info">
        <div class="follow-suggestion-name">${p.display_name || p.username}</div>
        <div class="follow-suggestion-handle">@${p.username}</div>
      </div>
      <button class="follow-btn" data-uid="${p.id}">Follow</button>
    </div>
  `).join('');

  $$('.follow-btn', container).forEach(btn => {
    btn.addEventListener('click', async () => {
      const uid = btn.dataset.uid;
      btn.disabled = true;
      btn.textContent = '…';
      const { error } = await sb.from('op_follows').insert({ follower_id: State.user.id, following_id: uid });
      if (!error) {
        btn.innerHTML = '<i class="fa-solid fa-check"></i> Following';
        btn.style.opacity = '0.5';
        // Increment counts directly — no RPC needed
        await incrementFollowCounts(uid, State.user.id);
        // Notify
        await sb.from('op_notifications').insert({ user_id: uid, actor_id: State.user.id, type: 'follow' });
        toast('Followed!', 'user-check');
      } else if (error.code === '23505') {
        // Already following (duplicate key) — treat as success
        btn.innerHTML = '<i class="fa-solid fa-check"></i> Following';
        btn.style.opacity = '0.5';
      } else {
        btn.textContent = 'Follow';
        btn.disabled = false;
      }
    });
  });
}

let _trendingCache = null, _trendingExpires = 0;
async function loadTrendingTags() {
  const container = $('#trending-tags');
  if (!container) return;
  if (_trendingCache && Date.now() < _trendingExpires) {
    container.innerHTML = _trendingCache;
    _bindTrendingClicks(container);
    return;
  }
  // Extract hashtags from recent posts
  const { data: posts } = await sb.from('op_posts').select('content').order('created_at', { ascending: false }).limit(100);
  const tagCounts = {};
  (posts || []).forEach(p => {
    const matches = p.content.match(/#\w+/g) || [];
    matches.forEach(tag => { tagCounts[tag] = (tagCounts[tag] || 0) + 1; });
  });

  const sorted = Object.entries(tagCounts).sort((a, b) => b[1] - a[1]).slice(0, 6);

  if (!sorted.length) {
    container.innerHTML = `<div class="trending-item"><div class="trending-tag-name">#developers</div><div class="trending-tag-count">Be the first to post!</div></div>`;
    return;
  }

  const maxCount = sorted[0]?.[1] || 1;
  const html = sorted.map(([tag, count]) => {
    const pct = Math.max(8, Math.round((count / maxCount) * 100));
    return `
    <div class="trending-item" data-tag="${escapeHtml(tag.replace('#',''))}">
      <div class="trending-tag-row">
        <div class="trending-tag-name">${escapeHtml(tag)}</div>
        <div class="trending-tag-count">${count} post${count !== 1 ? 's' : ''}</div>
      </div>
      <div class="trending-tag-bar" style="width:${pct}%"></div>
    </div>`;
  }).join('');
  _trendingCache = html;
  _trendingExpires = Date.now() + 5 * 60 * 1000;
  container.innerHTML = html;
  _bindTrendingClicks(container);
}

function _bindTrendingClicks(container) {
  container.querySelectorAll('.trending-item[data-tag]').forEach(item => {
    item.addEventListener('click', () => navigateToTag(item.dataset.tag));
  });
}

/* ── Navigation ─────────────────────────────────────────────── */
// ── Dynamic meta tags for SPA ─────────────────────────────────
function updatePageMeta({ title, description } = {}) {
  const siteName = 'Devit';
  const baseDesc = 'The social platform built by developers, for developers.';
  const pageTitle = title ? `${title} · ${siteName}` : `${siteName} — Code. Connect. Ship.`;
  const pageDesc  = description || baseDesc;

  document.title = pageTitle;
  const metas = {
    'description':          pageDesc,
    'og:title':             pageTitle,
    'og:description':       pageDesc,
    'twitter:title':        pageTitle,
    'twitter:description':  pageDesc,
  };
  Object.entries(metas).forEach(([key, val]) => {
    const el = document.querySelector(`meta[name="${key}"], meta[property="${key}"]`);
    if (el) el.setAttribute('content', val);
  });
}

const viewMeta = {
  feed:          { title: 'Home Feed' },
  explore:       { title: 'Explore', description: 'Discover developers, communities, and trending topics on Devit.' },
  snippets:      { title: 'Snippets', description: 'Short-form code videos from the developer community.' },
  notifications:  { title: 'Notifications' },
  messages:       { title: 'Messages' },
  profile:        { title: 'Profile' },
  bookmarks:      { title: 'Bookmarks' },
  settings:       { title: 'Settings' },
  'edit-profile': { title: 'Edit Profile' },
};

// View-scoped subscription unsub functions (non-global)
const ViewUnsubFns = [];

function navigateTo(view) {
  // Clean up snippets full-screen overlay if leaving snippets view
  const existingSnippetsContainer = document.getElementById('snippets-container');
  if (existingSnippetsContainer) {
    document.querySelectorAll('.snip-video').forEach(v => v.pause());
    existingSnippetsContainer.remove();
  }

  State.currentView = view;
  showPresence();
  updateSidebarActive();
  updateBottomNavActive(view);
  updatePageMeta(viewMeta[view] || {});
  const main = $('#main');
  main.style.cssText = ''; // reset any inline styles set by snippets view
  main.innerHTML = '';
  closeSearch();

  // Clean up view-specific realtime subs via realtimeManager
  realtimeManager.cleanupByPrefix('view:');
  while (ViewUnsubFns.length) { try { ViewUnsubFns.pop()(); } catch (_) {} }

  const renderers = {
    feed:           renderFeed,
    explore:        renderExplore,
    notifications:  renderNotifications,
    messages:       renderMessages,
    profile:        renderProfile,
    bookmarks:      renderBookmarks,
    settings:       renderSettings,
    'edit-profile': renderEditProfile,
    leaderboard:    renderLeaderboard,
  };

  (renderers[view] || renderFeed)(main);

  // Page enter animation
  main.classList.remove('page-enter');
  requestAnimationFrame(() => {
    requestAnimationFrame(() => {
      main.classList.add('page-enter');
      // Focus management: move focus to main for screen readers
      main.focus();
    });
  });
}

function updateBottomNavActive(view) {
  const btns = document.querySelectorAll('.bnav-btn');
  btns.forEach(btn => {
    btn.classList.toggle('active', btn.dataset.nav === view);
  });
}

/* ── Community sidebar swipe drawer (Discord-style) ─────────── */
function initMainSwipeNavigation() {}
function initTopbarSwipe() {}
function initSwipeNavigation() {}

/**
 * Called by openCommunity() after the view is rendered.
 * Adds a backdrop + swipe-right-to-open / swipe-left-to-close
 * behaviour for the .disc-sidebar on mobile.
 */
function initCommunitySidebarSwipe(view) {
  const sidebar = view.querySelector('.disc-sidebar');
  if (!sidebar) return;

  const SIDEBAR_W  = 280;   // must match CSS width
  const EDGE_ZONE  = 80;    // px from left edge that can start a swipe-open
  const THRESHOLD  = 80;    // px travel to commit open/close
  const VELOCITY_T = 0.25;  // px/ms fast-flick threshold

  // ── Backdrop (shared across re-renders) ──────────────────
  let backdrop = document.getElementById('disc-sidebar-backdrop');
  if (backdrop) {
    // Remove old listeners by replacing node
    const fresh = backdrop.cloneNode(false);
    backdrop.replaceWith(fresh);
    backdrop = fresh;
  } else {
    backdrop = document.createElement('div');
    backdrop.id = 'disc-sidebar-backdrop';
    backdrop.className = 'disc-sidebar-backdrop';
    document.body.appendChild(backdrop);
  }

  function openSidebar() {
    sidebar.style.transition = '';
    sidebar.style.transform  = '';
    sidebar.classList.add('sidebar-open');
    backdrop.style.display   = 'block';
    // force reflow so CSS transition fires
    backdrop.getBoundingClientRect();
    backdrop.style.opacity   = '1';
    // Do NOT lock body scroll — it freezes the whole page on mobile
  }
  function closeSidebar() {
    sidebar.style.transition = '';
    sidebar.style.transform  = '';
    sidebar.classList.remove('sidebar-open');
    backdrop.style.opacity   = '0';
    setTimeout(() => {
      if (!sidebar.classList.contains('sidebar-open')) {
        backdrop.style.display = 'none';
      }
    }, 280);
  }

  backdrop.addEventListener('click', closeSidebar);

  // ── Touch state ───────────────────────────────────────────
  let startX = 0, startY = 0, startT = 0;
  let dragging = false, axis = null;

  // Always attach to document so edge swipes are never missed
  function onTouchStart(e) {
    const t = e.touches[0];
    startX = t.clientX;
    startY = t.clientY;
    startT = Date.now();
    axis   = null;

    const isOpen = sidebar.classList.contains('sidebar-open');
    // Activate: swipe right from edge (open) OR anywhere when already open (close)
    dragging = isOpen || startX <= EDGE_ZONE;
  }

  function onTouchMove(e) {
    if (!dragging) return;
    const t  = e.touches[0];
    const dx = t.clientX - startX;
    const dy = t.clientY - startY;

    if (!axis && (Math.abs(dx) > 5 || Math.abs(dy) > 5)) {
      axis = Math.abs(dx) >= Math.abs(dy) ? 'h' : 'v';
    }
    if (axis !== 'h') return;

    const isOpen = sidebar.classList.contains('sidebar-open');

    if (!isOpen && dx > 0) {
      const travel = Math.min(dx, SIDEBAR_W);
      sidebar.style.transition = 'none';
      sidebar.style.transform  = `translateX(calc(-100% + ${travel}px))`;
      backdrop.style.display   = 'block';
      backdrop.style.opacity   = String((travel / SIDEBAR_W) * 0.7);
    } else if (isOpen && dx < 0) {
      const travel = Math.max(dx, -SIDEBAR_W);
      sidebar.style.transition = 'none';
      sidebar.style.transform  = `translateX(${travel}px)`;
      backdrop.style.opacity   = String((1 + travel / SIDEBAR_W) * 0.7);
    }
  }

  function onTouchEnd(e) {
    if (!dragging) return;
    dragging = false;

    if (axis !== 'h') { axis = null; return; }
    axis = null;

    const t  = e.changedTouches[0];
    const dx = t.clientX - startX;
    const dt = Math.max(Date.now() - startT, 1);
    const vx = Math.abs(dx) / dt;
    const isOpen = sidebar.classList.contains('sidebar-open');

    if (!isOpen) {
      if (dx > THRESHOLD || (dx > 20 && vx > VELOCITY_T)) {
        openSidebar();
      } else {
        // Snap back closed
        sidebar.style.transition = '';
        sidebar.style.transform  = '';
        backdrop.style.opacity   = '0';
        setTimeout(() => { backdrop.style.display = 'none'; }, 280);
      }
    } else {
      if (dx < -THRESHOLD || (dx < -20 && vx > VELOCITY_T)) {
        closeSidebar();
      } else {
        openSidebar(); // snap back open
      }
    }
  }

  document.addEventListener('touchstart', onTouchStart, { passive: true });
  document.addEventListener('touchmove',  onTouchMove,  { passive: true });
  document.addEventListener('touchend',   onTouchEnd,   { passive: true });

  // Clean up old listeners when community is navigated away
  view._destroySwipe = () => {
    document.removeEventListener('touchstart', onTouchStart);
    document.removeEventListener('touchmove',  onTouchMove);
    document.removeEventListener('touchend',   onTouchEnd);
    closeSidebar();
  };

  view._closeSidebar = closeSidebar;
  view._openSidebar  = openSidebar;
}

/* ── Feed ───────────────────────────────────────────────────── */


/* ── Guest mode styles ────────────────────────────────────── */
(function injectGuestStyles() {
  const s = document.createElement('style');
  s.textContent = `
    body.guest-mode #bnav-post-btn { opacity: 0.4; pointer-events: none; }
    body.guest-mode .composer-inner { display: none !important; }
  `;
  document.head.appendChild(s);
})();

/* ── Guest Mode Guard ───────────────────────────────────────── */
function guestGuard(action) {
  if (!State.isGuest) return false;
  showGuestPrompt(action);
  return true;
}

function showGuestPrompt(action = 'do that') {
  const modal = document.getElementById('modal-overlay');
  const body  = document.getElementById('modal-body');
  const title = document.getElementById('modal-title-text');
  if (!modal || !body) return;
  title.textContent = 'Sign in to continue';
  modal.classList.add('open');
  body.innerHTML = `
    <div style="padding:32px;text-align:center;display:flex;flex-direction:column;align-items:center;gap:16px">
      <div style="width:60px;height:60px;border-radius:50%;background:rgba(255,45,110,0.1);border:2px solid rgba(255,45,110,0.3);display:flex;align-items:center;justify-content:center;font-size:24px">✈️</div>
      <div>
        <div style="font-size:17px;font-weight:800;margin-bottom:6px">You need an account to ${escapeHtml(action)}</div>
        <div style="font-size:13px;color:var(--text-muted)">Join Planebook — it's free. Sign in with Discord to post, like, and connect with fellow aviators.</div>
      </div>
      <button class="auth-btn-discord" id="guest-prompt-discord-btn" style="width:100%;max-width:280px">
        <svg class="discord-icon" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg">
          <path d="M20.317 4.37a19.791 19.791 0 0 0-4.885-1.515.074.074 0 0 0-.079.037c-.21.375-.444.864-.608 1.25a18.27 18.27 0 0 0-5.487 0 12.64 12.64 0 0 0-.617-1.25.077.077 0 0 0-.079-.037A19.736 19.736 0 0 0 3.677 4.37a.07.07 0 0 0-.032.027C.533 9.046-.32 13.58.099 18.057a.082.082 0 0 0 .031.057 19.9 19.9 0 0 0 5.993 3.03.078.078 0 0 0 .084-.028 14.09 14.09 0 0 0 1.226-1.994.076.076 0 0 0-.041-.106 13.107 13.107 0 0 1-1.872-.892.077.077 0 0 1-.008-.128 10.2 10.2 0 0 0 .372-.292.074.074 0 0 1 .077-.01c3.928 1.793 8.18 1.793 12.062 0a.074.074 0 0 1 .078.01c.12.098.246.198.373.292a.077.077 0 0 1-.006.127 12.299 12.299 0 0 1-1.873.892.077.077 0 0 0-.041.107c.36.698.772 1.362 1.225 1.993a.076.076 0 0 0 .084.028 19.839 19.839 0 0 0 6.002-3.03.077.077 0 0 0 .032-.054c.5-5.177-.838-9.674-3.549-13.66a.061.061 0 0 0-.031-.03zM8.02 15.33c-1.183 0-2.157-1.085-2.157-2.419 0-1.333.956-2.419 2.157-2.419 1.21 0 2.176 1.096 2.157 2.42 0 1.333-.956 2.418-2.157 2.418zm7.975 0c-1.183 0-2.157-1.085-2.157-2.419 0-1.333.955-2.419 2.157-2.419 1.21 0 2.176 1.096 2.157 2.42 0 1.333-.946 2.418-2.157 2.418z" fill="#fff"/>
        </svg>
        <span>Sign in with Discord</span>
      </button>
      <button style="font-size:12px;color:var(--text-muted);background:none;border:none;cursor:pointer" onclick="document.getElementById('modal-overlay').classList.remove('open')">Continue browsing as guest</button>
    </div>`;
  document.getElementById('guest-prompt-discord-btn')?.addEventListener('click', async () => {
    modal.classList.remove('open');
    // Bring back auth screen
    State.isGuest = false;
    const screen = document.getElementById('auth-screen');
    const app    = document.getElementById('app');
    if (screen && app) {
      screen.style.display = 'flex'; screen.style.visibility = ''; screen.style.pointerEvents = '';
      screen.style.opacity = '1'; screen.style.transform = '';
      app.classList.remove('visible');
    }
    // Trigger Discord OAuth
    await sb.auth.signInWithOAuth({
      provider: 'discord',
      options: { redirectTo: window.DEVIT_CONFIG?.SITE_URL || window.location.origin }
    });
  });
}
window.guestGuard = guestGuard;

function renderFeed(main) {
  // Build unique stacks from user's own stack for filtering
  const userStack = State.profile?.tech_stack || [];

  main.innerHTML = `
    <div class="view-tabs" role="tablist" aria-label="Feed tabs">
      <div class="view-tab ${State.feedTab === 'for-you' ? 'active' : ''}" data-tab="for-you" role="tab" aria-selected="${State.feedTab === 'for-you'}" tabindex="0">main feed</div>
      <div class="view-tab ${State.feedTab === 'following' ? 'active' : ''}" data-tab="following" role="tab" aria-selected="${State.feedTab === 'following'}" tabindex="-1">following</div>
    </div>
    ${State.isGuest ? `<div style="margin:8px 12px 0;padding:10px 14px;background:rgba(255,45,110,0.07);border:1px solid rgba(255,45,110,0.2);border-radius:12px;display:flex;align-items:center;gap:10px;font-size:12px;color:var(--text-secondary)">
      <i class="fa-solid fa-eye" style="color:var(--cyan);font-size:14px;flex-shrink:0"></i>
      <span>You're browsing as a guest. <button onclick="showGuestPrompt('sign in')" style="background:none;border:none;cursor:pointer;color:var(--cyan);font-weight:700;font-size:12px;padding:0">Sign in with Discord</button> to post & interact.</span>
    </div>` : ''}
    <div id="feed" role="feed" aria-label="Aviation posts" aria-busy="true"><div style="padding:32px;text-align:center;color:var(--text-muted)">Loading posts…</div></div>
  `;

  // Active stack filter state
  let activeStack = '';


  $$('.view-tab[data-tab]', main).forEach(tab => {
    tab.addEventListener('click', () => {
      State.feedTab = tab.dataset.tab;
      $$('.view-tab', main).forEach(t => { t.classList.remove('active'); t.setAttribute('aria-selected','false'); t.setAttribute('tabindex','-1'); });
      tab.classList.add('active');
      tab.setAttribute('aria-selected','true');
      tab.setAttribute('tabindex','0');
      loadPosts($('#feed'), activeStack);
    });
    tab.addEventListener('keydown', e => {
      if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); tab.click(); }
    });
  });

  loadPosts($('#feed'), activeStack);
  subscribeToNewPosts($('#feed'));
}

async function loadPosts(container, stackFilter = '') {
  container.setAttribute('aria-busy', 'true');
  container.innerHTML = `<div style="padding:32px;text-align:center;color:var(--text-muted)">Loading…</div>`;

  let query = sb
    .from('op_posts')
    .select(`
      id, content, code_block, code_lang, image_url, file_url, file_name, likes_count, comments_count, reposts_count, created_at, poll, author_id,
      profiles:op_profiles!author_id(id, username, display_name, avatar_url, tech_stack)
    `)
    .order('created_at', { ascending: false })
    .limit(30);

  if (State.feedTab === 'following') {
    const { data: following } = await sb.from('op_follows').select('following_id').eq('follower_id', State.user.id);
    const ids = (following || []).map(f => f.following_id);
    if (!ids.length) {
      container.innerHTML = `<div style="padding:40px;text-align:center;color:var(--text-muted)">Follow some aviators to see their posts here ✈️</div>`;
      return;
    }
    query = query.in('author_id', ids);
  }

  const { data: posts, error } = await query;
  if (error) { container.innerHTML = `<div style="padding:32px;text-align:center;color:var(--rose)">Failed to load posts</div>`; return; }


  // Apply stack filter client-side (filter by author's tech_stack)
  let filteredPosts = posts || [];
  if (stackFilter) {
    filteredPosts = filteredPosts.filter(p =>
      (p.profiles?.tech_stack || []).some(t => t.toLowerCase() === stackFilter.toLowerCase()) ||
      (p.content || '').toLowerCase().includes(stackFilter.toLowerCase())
    );
  }

  // Get which posts user liked/bookmarked — also fetch vote direction for up/down vote UI
  const postIds = filteredPosts.map(p => p.id);
  let likedIds = new Set(), bookmarkedIds = new Set(), voteMap = {};
  if (postIds.length && State.user?.id) {
    const { data: likes } = await sb.from('op_post_likes').select('post_id, vote').eq('user_id', State.user.id).in('post_id', postIds);
    const { data: bookmarks } = await sb.from('op_bookmarks').select('post_id').eq('user_id', State.user.id).in('post_id', postIds);
    likedIds = new Set((likes || []).map(l => l.post_id));
    // voteMap: { [post_id]: 1 | -1 }  — used by the UpVote/DownVote patch
    (likes || []).forEach(l => { if (l.vote != null) voteMap[l.post_id] = l.vote; });
    bookmarkedIds = new Set((bookmarks || []).map(b => b.post_id));
  }

  container.innerHTML = '';
  container.setAttribute('aria-busy', 'false');
  if (!filteredPosts.length) {
    const msg = stackFilter
      ? `<div style="padding:40px;text-align:center;color:var(--text-muted)">No posts yet 🛫</div>`
      : `<div style="padding:40px;text-align:center;color:var(--text-muted)">No posts yet — be the first to share! ✈️</div>`;
    container.innerHTML = msg;
    return;
  }

  filteredPosts.forEach(post => {
    const card = buildPostCard(post, post.profiles, likedIds.has(post.id), bookmarkedIds.has(post.id), voteMap);
    container.appendChild(card);
  });
}

function subscribeToNewPosts(container) {
  const unsub = realtimeManager.subscribe(
    'view:posts_realtime',
    'posts',
    'INSERT',
    async payload => {
      const newPost = payload.new;
      const { data: profile } = await sb.from('op_profiles').select('id, username, display_name, avatar_url').eq('id', newPost.author_id).single();
      if (profile && newPost.author_id !== State.user.id) {
        const card = buildPostCard(newPost, profile, false, false);
        card.style.opacity = '0';
        card.style.transform = 'translateY(-10px)';
        container.prepend(card);
        requestAnimationFrame(() => {
          card.style.transition = '0.4s ease';
          card.style.opacity = '1';
          card.style.transform = '';
        });
        // toast muted — realtime toasts interrupt reading
      }
    }
  );
  ViewUnsubFns.push(unsub);
}

/* ── Dev Activity Feed (GitHub stars, follows, contributions) ── */
async function renderDevActivityFeed(container) {
  container.setAttribute('aria-busy', 'true');
  container.innerHTML = `<div style="padding:32px;text-align:center;color:var(--text-muted)">Loading activity…</div>`;

  const profile = State.profile;
  const ghUsername = profile?.github_username || (profile?.is_github ? profile?.username : null);

  if (!ghUsername) {
    container.innerHTML = `
      <div style="padding:48px 24px;text-align:center;display:flex;flex-direction:column;align-items:center;gap:14px">
        <div style="width:64px;height:64px;background:#24292e;border-radius:50%;display:flex;align-items:center;justify-content:center;font-size:28px">
          <i class="fa-brands fa-github" style="color:#fff"></i>
        </div>
        <div style="font-size:15px;font-weight:700">Connect GitHub to see Dev Activity</div>
        <div style="font-size:13px;color:var(--text-muted);max-width:280px;line-height:1.6">
          Opt in to see stars, follows, and contributions from developers you follow. Sign in with GitHub to enable.
        </div>
        <button class="auth-btn-github" style="max-width:240px" onclick="window.sb?.auth.signInWithOAuth({provider:'github',options:{redirectTo:window.location.href}})">
          <i class="fa-brands fa-github"></i> Connect GitHub
        </button>
      </div>`;
    return;
  }

  // Fetch GitHub events for the user + who they follow
  const token = await (async () => {
    try {
      const { data } = await sb.from('op_github_tokens').select('access_token').eq('user_id', State.user.id).single();
      return data?.access_token || null;
    } catch { return null; }
  })();

  const ghFetchLocal = async (path) => {
    const headers = token ? { Authorization: `Bearer ${token}` } : {};
    const r = await fetch(`https://api.github.com${path}`, { headers });
    if (!r.ok) throw new Error(`GH ${r.status}`);
    return r.json();
  };

  let events = [];
  try {
    events = await ghFetchLocal(`/users/${encodeURIComponent(ghUsername)}/received_events?per_page=30`);
    if (!Array.isArray(events)) events = [];
  } catch (_) {
    try {
      events = await ghFetchLocal(`/users/${encodeURIComponent(ghUsername)}/events/public?per_page=30`);
      if (!Array.isArray(events)) events = [];
    } catch (_2) { events = []; }
  }

  container.setAttribute('aria-busy', 'false');

  if (!events.length) {
    container.innerHTML = `<div style="padding:40px;text-align:center;color:var(--text-muted)">No recent GitHub activity found for <strong>@${escapeHtml(ghUsername)}</strong>.</div>`;
    return;
  }

  const typeMap = {
    WatchEvent:   { icon: 'fa-regular fa-star',        color: 'var(--amber)',   label: (e) => `starred <a href="${escapeHtml(e.repo?.url?.replace('api.github.com/repos','github.com') || '#')}" target="_blank" rel="noopener" style="color:var(--cyan)">${escapeHtml(e.repo?.name || '')}</a>` },
    ForkEvent:    { icon: 'fa-solid fa-code-fork',     color: 'var(--emerald)', label: (e) => `forked <a href="${escapeHtml(e.repo?.url?.replace('api.github.com/repos','github.com') || '#')}" target="_blank" rel="noopener" style="color:var(--cyan)">${escapeHtml(e.repo?.name || '')}</a>` },
    FollowEvent:  { icon: 'fa-solid fa-user-plus',     color: 'var(--violet)',  label: (e) => `followed <strong>${escapeHtml(e.payload?.target?.login || '')}</strong>` },
    PushEvent:    { icon: 'fa-solid fa-code-commit',   color: 'var(--cyan)',    label: (e) => `pushed ${e.payload?.commits?.length || 1} commit${(e.payload?.commits?.length||1)!==1?'s':''} to <a href="${escapeHtml(e.repo?.url?.replace('api.github.com/repos','github.com') || '#')}" target="_blank" rel="noopener" style="color:var(--cyan)">${escapeHtml(e.repo?.name || '')}</a>` },
    CreateEvent:  { icon: 'fa-solid fa-code-branch',   color: 'var(--sky)',     label: (e) => `created ${escapeHtml(e.payload?.ref_type || 'branch')} in <a href="${escapeHtml(e.repo?.url?.replace('api.github.com/repos','github.com') || '#')}" target="_blank" rel="noopener" style="color:var(--cyan)">${escapeHtml(e.repo?.name || '')}</a>` },
    PullRequestEvent: { icon: 'fa-solid fa-code-pull-request', color: 'var(--violet)', label: (e) => `${escapeHtml(e.payload?.action || 'opened')} PR in <a href="${escapeHtml(e.repo?.url?.replace('api.github.com/repos','github.com') || '#')}" target="_blank" rel="noopener" style="color:var(--cyan)">${escapeHtml(e.repo?.name || '')}</a>` },
    IssuesEvent:  { icon: 'fa-solid fa-circle-dot',    color: 'var(--rose)',    label: (e) => `${escapeHtml(e.payload?.action || 'opened')} issue in <a href="${escapeHtml(e.repo?.url?.replace('api.github.com/repos','github.com') || '#')}" target="_blank" rel="noopener" style="color:var(--cyan)">${escapeHtml(e.repo?.name || '')}</a>` },
    ReleaseEvent: { icon: 'fa-solid fa-rocket',        color: 'var(--cyan)',    label: (e) => `released <strong>${escapeHtml(e.payload?.release?.tag_name || '')}</strong> in <a href="${escapeHtml(e.repo?.url?.replace('api.github.com/repos','github.com') || '#')}" target="_blank" rel="noopener" style="color:var(--cyan)">${escapeHtml(e.repo?.name || '')}</a>` },
  };

  const html = events.slice(0, 25).map(ev => {
    const def = typeMap[ev.type];
    if (!def) return '';
    const actor = ev.actor?.login || ghUsername;
    const avatarUrl = ev.actor?.avatar_url || '';
    const time = timeAgo(ev.created_at);
    return `
      <div class="dev-activity-item">
        <div class="dev-activity-avatar">
          ${avatarUrl ? `<img src="${escapeHtml(avatarUrl)}" style="width:36px;height:36px;border-radius:50%;object-fit:cover">` : `<div style="width:36px;height:36px;border-radius:50%;background:${avatarColor(actor)};display:flex;align-items:center;justify-content:center;font-weight:700;font-size:13px;color:#fff">${(actor[0]||'?').toUpperCase()}</div>`}
        </div>
        <div class="dev-activity-body">
          <div class="dev-activity-text">
            <span class="dev-activity-icon" style="color:${def.color}"><i class="${def.icon}"></i></span>
            <strong>@${escapeHtml(actor)}</strong> ${def.label(ev)}
          </div>
          <div class="dev-activity-time">${escapeHtml(time)}</div>
        </div>
      </div>`;
  }).filter(Boolean).join('');

  container.innerHTML = html || `<div style="padding:40px;text-align:center;color:var(--text-muted)">No recognisable activity events found.</div>`;
}


const FILE_MAX_BYTES = 600 * 1024; // 600 KB

const FILE_ICONS = {
  'pdf':  'fa-file-pdf',
  'doc':  'fa-file-word',  'docx': 'fa-file-word',
  'xls':  'fa-file-excel', 'xlsx': 'fa-file-excel',
  'ppt':  'fa-file-powerpoint', 'pptx': 'fa-file-powerpoint',
  'zip':  'fa-file-zipper','rar':  'fa-file-zipper', '7z': 'fa-file-zipper',
  'mp3':  'fa-file-audio', 'wav':  'fa-file-audio', 'ogg': 'fa-file-audio',
  'mp4':  'fa-file-video', 'mov':  'fa-file-video', 'webm':'fa-file-video',
  'txt':  'fa-file-lines', 'md':   'fa-file-lines',
  'js':   'fa-file-code',  'ts':   'fa-file-code',  'py': 'fa-file-code',
  'html': 'fa-file-code',  'css':  'fa-file-code',  'json':'fa-file-code',
};

function fileIcon(name) {
  const ext = (name.split('.').pop() || '').toLowerCase();
  return FILE_ICONS[ext] || 'fa-file';
}

function fmtBytes(bytes) {
  if (bytes < 1024) return bytes + ' B';
  if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + ' KB';
  return (bytes / (1024 * 1024)).toFixed(1) + ' MB';
}

function buildComposer(container) {
  const profile = State.profile;
  container.innerHTML = `
    <div class="composer-inner">
      <div class="composer-row">
        <div class="composer-avatar">${avatarHtml(profile, 38)}</div>
        <textarea class="composer-textarea" id="post-textarea" placeholder="What did you spot today? ✈️" rows="2"></textarea>
      </div>
      <div id="composer-attach-preview" style="display:none;padding:0 0 8px 0"></div>
      <div class="composer-toolbar">
        <button class="composer-tool" title="Add image" id="composer-img-btn">
          <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="3" y="3" width="18" height="18" rx="2"/><circle cx="8.5" cy="8.5" r="1.5"/><polyline points="21 15 16 10 5 21"/></svg>
        </button>
        <button class="composer-tool" title="Attach file (max 600 KB)" id="composer-file-btn">
          <i class="fa-solid fa-paperclip" style="font-size:14px"></i>
        </button>
        <input type="file" id="composer-img-input" accept="image/*" style="display:none">
        <input type="file" id="composer-file-input" accept=".pdf,.doc,.docx,.xls,.xlsx,.ppt,.pptx,.txt,.md,.js,.ts,.py,.html,.css,.json,.zip,.rar,.7z,.mp3,.wav,.ogg,.mp4,.mov,.webm" style="display:none">
        <div class="composer-actions">
          <span class="char-count" id="char-count">280</span>
          <button class="post-btn" id="post-submit-btn" disabled>Post</button>
        </div>
      </div>
    </div>
  `;

  // Inject poll button once composer is ready (avoids MutationObserver on body)
  setTimeout(() => { if (typeof injectPollButtonIntoComposer === 'function') injectPollButtonIntoComposer(); }, 0);

  const textarea    = $('#post-textarea');
  const charCount   = $('#char-count');
  const submitBtn   = $('#post-submit-btn');
  const imgBtn      = $('#composer-img-btn');
  const imgInput    = $('#composer-img-input');
  const fileBtn     = $('#composer-file-btn');
  const fileInput   = $('#composer-file-input');
  const preview     = $('#composer-attach-preview');
  let selectedImageFile = null;
  let selectedAttachFile = null;

  const canPost = () => textarea.value.trim().length > 0 || selectedImageFile || selectedAttachFile;

  // ── Hashtag autocomplete ────────────────────────────────────
  // Injects a dropdown when user types # and shows tag chips for confirmed tags.
  const POPULAR_TAGS = ['react','typescript','javascript','rust','python','webdev',
    'css','nodejs','nextjs','svelte','go','ai','web3','opensource','devops'];

  // Tag chips strip below textarea
  const tagStrip = document.createElement('div');
  tagStrip.id = 'composer-tag-strip';
  tagStrip.style.cssText = 'display:none;flex-wrap:wrap;gap:5px;padding:6px 0 2px;margin-top:-4px;';
  textarea.parentNode.insertBefore(tagStrip, textarea.nextSibling);

  // Dropdown
  const tagDropdown = document.createElement('div');
  tagDropdown.id = 'composer-tag-dropdown';
  tagDropdown.style.cssText = `
    display:none;position:absolute;z-index:999;
    background:var(--bg-surface);border:1px solid var(--border-mid);
    border-radius:var(--radius-md);box-shadow:0 8px 28px rgba(0,0,0,0.5);
    min-width:180px;max-width:260px;overflow:hidden;
    font-family:var(--font-mono);font-size:12px;
  `;
  // Anchor dropdown to the composer
  const composerInner = container.querySelector('.composer-inner') || container;
  composerInner.style.position = 'relative';
  composerInner.appendChild(tagDropdown);

  let _tagQuery = '';
  let _tagSuggestions = [];
  let _activeSuggIdx = -1;

  function getExistingTags() {
    // Pull from trending cache + popular fallbacks
    const fromPosts = Object.keys(_tagCache || {});
    const combined  = [...new Set([...fromPosts, ...POPULAR_TAGS])];
    return combined;
  }

  // Maintain a small live tag cache from recent posts (populated on first open)
  let _tagCache = null;
  async function ensureTagCache() {
    if (_tagCache) return;
    const { data: posts } = await sb.from('op_posts').select('content').order('created_at', { ascending: false }).limit(150);
    _tagCache = {};
    (posts || []).forEach(p => {
      (p.content.match(/#(\w+)/g) || []).forEach(t => {
        const k = t.slice(1).toLowerCase();
        _tagCache[k] = (_tagCache[k] || 0) + 1;
      });
    });
  }
  ensureTagCache();

  function showTagDropdown(query, caretRect) {
    _tagQuery = query;
    const all = getExistingTags().filter(t =>
      t.toLowerCase().startsWith(query.toLowerCase()) && t.length > 0
    ).sort((a, b) => ((_tagCache?.[b] || 0) - (_tagCache?.[a] || 0))).slice(0, 7);

    if (!all.length) { hideTagDropdown(); return; }
    _tagSuggestions = all;
    _activeSuggIdx = 0;

    tagDropdown.innerHTML = all.map((t, i) => `
      <div class="tag-sugg-item ${i === 0 ? 'active' : ''}" data-tag="${t}" style="
        padding:9px 14px;cursor:pointer;display:flex;align-items:center;gap:8px;
        ${i === 0 ? 'background:rgba(255,45,110,0.08);' : ''}
        transition:background 0.12s;
      ">
        <span style="color:var(--violet);font-weight:700">#</span>
        <span style="color:var(--text-primary);font-weight:600">${t}</span>
        ${_tagCache?.[t] ? `<span style="margin-left:auto;font-size:10px;color:var(--text-muted)">${_tagCache[t]}</span>` : ''}
      </div>
    `).join('');

    // Position: below the textarea
    const taEl = textarea;
    tagDropdown.style.display = 'block';
    tagDropdown.style.top = (taEl.offsetTop + taEl.offsetHeight + 4) + 'px';
    tagDropdown.style.left = '0px';

    tagDropdown.querySelectorAll('.tag-sugg-item').forEach((item, i) => {
      item.addEventListener('mousedown', e => {
        e.preventDefault();
        commitTag(item.dataset.tag);
      });
      item.addEventListener('mouseenter', () => {
        _activeSuggIdx = i;
        updateActiveSugg();
      });
    });
  }

  function hideTagDropdown() {
    tagDropdown.style.display = 'none';
    _tagSuggestions = [];
    _activeSuggIdx = -1;
  }

  function updateActiveSugg() {
    tagDropdown.querySelectorAll('.tag-sugg-item').forEach((item, i) => {
      const active = i === _activeSuggIdx;
      item.classList.toggle('active', active);
      item.style.background = active ? 'rgba(255,45,110,0.08)' : '';
    });
  }

  function commitTag(tag) {
    const val = textarea.value;
    const pos = textarea.selectionStart;
    // Find the # that triggered the dropdown
    const before = val.slice(0, pos);
    const hashIdx = before.lastIndexOf('#');
    if (hashIdx === -1) { hideTagDropdown(); return; }
    const after = val.slice(pos);
    const newVal = val.slice(0, hashIdx) + '#' + tag + ' ' + after;
    textarea.value = newVal;
    // Move cursor after the inserted tag
    const newPos = hashIdx + tag.length + 2;
    textarea.setSelectionRange(newPos, newPos);
    textarea.focus();
    hideTagDropdown();
    // Update char count
    const left = 280 - textarea.value.length;
    charCount.textContent = left;
    charCount.style.color = left < 20 ? 'var(--rose)' : left < 60 ? 'var(--amber)' : 'var(--text-muted)';
    submitBtn.disabled = !canPost();
    updateTagStrip();
  }

  function updateTagStrip() {
    const tags = [...new Set((textarea.value.match(/#(\w+)/g) || []).map(t => t.toLowerCase()))];
    if (!tags.length) { tagStrip.style.display = 'none'; return; }
    tagStrip.style.display = 'flex';
    tagStrip.innerHTML = `
      <span style="font-size:10px;font-weight:700;color:var(--text-muted);letter-spacing:0.05em;text-transform:uppercase;align-self:center;margin-right:2px;">Sections:</span>
      ${tags.map(t => `
        <span style="
          display:inline-flex;align-items:center;gap:4px;
          padding:3px 9px;border-radius:var(--radius-full);
          font-size:11px;font-weight:700;font-family:var(--font-mono);
          background:rgba(255,107,53,0.10);border:1px solid rgba(255,107,53,0.25);
          color:var(--violet);
        ">${t}</span>
      `).join('')}
    `;
  }

  textarea.addEventListener('input', () => {
    const left = 280 - textarea.value.length;
    charCount.textContent = left;
    charCount.style.color = left < 20 ? 'var(--rose)' : left < 60 ? 'var(--amber)' : 'var(--text-muted)';
    submitBtn.disabled = !canPost();

    // Hashtag autocomplete trigger
    const val = textarea.value;
    const pos = textarea.selectionStart;
    const before = val.slice(0, pos);
    const hashMatch = before.match(/#(\w*)$/);
    if (hashMatch) {
      showTagDropdown(hashMatch[1]);
    } else {
      hideTagDropdown();
    }
    updateTagStrip();
  });

  textarea.addEventListener('keydown', e => {
    if (tagDropdown.style.display === 'none') return;
    if (e.key === 'ArrowDown') {
      e.preventDefault();
      _activeSuggIdx = Math.min(_activeSuggIdx + 1, _tagSuggestions.length - 1);
      updateActiveSugg();
    } else if (e.key === 'ArrowUp') {
      e.preventDefault();
      _activeSuggIdx = Math.max(_activeSuggIdx - 1, 0);
      updateActiveSugg();
    } else if (e.key === 'Enter' || e.key === 'Tab') {
      if (_tagSuggestions.length && _activeSuggIdx >= 0) {
        e.preventDefault();
        commitTag(_tagSuggestions[_activeSuggIdx]);
      }
    } else if (e.key === 'Escape') {
      hideTagDropdown();
    }
  });

  textarea.addEventListener('blur', () => setTimeout(hideTagDropdown, 150));

  // ── Image picker ──
  imgBtn.addEventListener('click', () => { selectedAttachFile = null; imgInput.click(); });

  imgInput.addEventListener('change', () => {
    const file = imgInput.files[0];
    if (!file) return;
    if (file.size > FILE_MAX_BYTES) {
      toast(`Image must be under ${fmtBytes(FILE_MAX_BYTES)}`, 'circle-exclamation');
      imgInput.value = '';
      return;
    }
    selectedImageFile = file;
    selectedAttachFile = null;
    const reader = new FileReader();
    reader.onload = e => {
      preview.style.display = 'block';
      preview.innerHTML = `
        <div style="position:relative;display:inline-block">
          <img src="${e.target.result}" style="max-height:180px;max-width:100%;border-radius:10px;border:1px solid var(--border);object-fit:cover">
          <button id="composer-attach-remove" style="position:absolute;top:4px;right:4px;background:rgba(0,0,0,0.65);border:none;border-radius:50%;width:22px;height:22px;cursor:pointer;color:#fff;font-size:12px;display:flex;align-items:center;justify-content:center"><i class="fa-solid fa-xmark"></i></button>
        </div>`;
      bindRemove();
      submitBtn.disabled = !canPost();
    };
    reader.readAsDataURL(file);
  });

  // ── File picker ──
  fileBtn.addEventListener('click', () => { selectedImageFile = null; fileInput.click(); });

  fileInput.addEventListener('change', () => {
    const file = fileInput.files[0];
    if (!file) return;
    if (file.size > FILE_MAX_BYTES) {
      toast(`File must be under ${fmtBytes(FILE_MAX_BYTES)}`, 'circle-exclamation');
      fileInput.value = '';
      return;
    }
    selectedAttachFile = file;
    selectedImageFile = null;
    const icon = fileIcon(file.name);
    preview.style.display = 'block';
    preview.innerHTML = `
      <div style="display:flex;align-items:center;gap:10px;padding:10px 12px;background:var(--bg-elevated);border:1px solid var(--border);border-radius:10px;max-width:320px">
        <i class="fa-solid ${icon}" style="font-size:22px;color:var(--cyan);flex-shrink:0"></i>
        <div style="min-width:0;flex:1">
          <div style="font-size:13px;font-weight:600;white-space:nowrap;overflow:hidden;text-overflow:ellipsis">${escapeHtml(file.name)}</div>
          <div style="font-size:11px;color:var(--text-muted)">${fmtBytes(file.size)}</div>
        </div>
        <button id="composer-attach-remove" style="background:none;border:none;color:var(--text-muted);cursor:pointer;font-size:16px;padding:4px;flex-shrink:0"><i class="fa-solid fa-xmark"></i></button>
      </div>`;
    bindRemove();
    submitBtn.disabled = !canPost();
  });

  function bindRemove() {
    $('#composer-attach-remove').addEventListener('click', () => {
      selectedImageFile = null;
      selectedAttachFile = null;
      imgInput.value = '';
      fileInput.value = '';
      preview.style.display = 'none';
      preview.innerHTML = '';
      submitBtn.disabled = !canPost();
    });
  }

  // ── Submit ──
  submitBtn.addEventListener('click', async () => {
    if (!canPost()) return;
    submitBtn.disabled = true;
    submitBtn.textContent = 'Posting…';

    let imageUrl = null;
    let fileUrl  = null;
    let fileName = null;

    if (selectedImageFile) {
      const ext  = selectedImageFile.name.split('.').pop();
      const path = `posts/${State.user.id}/${Date.now()}.${ext}`;
      const { error: uploadErr } = await sb.storage.from('post-images').upload(path, selectedImageFile, { contentType: selectedImageFile.type });
      if (uploadErr) {
        toast('Image upload failed: ' + uploadErr.message, 'circle-exclamation');
        submitBtn.disabled = false; submitBtn.textContent = 'Post'; return;
      }
      imageUrl = sb.storage.from('post-images').getPublicUrl(path).data.publicUrl;
    }

    if (selectedAttachFile) {
      const ext  = selectedAttachFile.name.split('.').pop();
      const path = `posts/${State.user.id}/${Date.now()}_${selectedAttachFile.name.replace(/[^a-zA-Z0-9._-]/g, '_')}`;
      const { error: uploadErr } = await sb.storage.from('post-files').upload(path, selectedAttachFile, { contentType: selectedAttachFile.type });
      if (uploadErr) {
        toast('File upload failed: ' + uploadErr.message, 'circle-exclamation');
        submitBtn.disabled = false; submitBtn.textContent = 'Post'; return;
      }
      fileUrl  = sb.storage.from('post-files').getPublicUrl(path).data.publicUrl;
      fileName = selectedAttachFile.name;
    }

    const text = textarea.value.trim();
    const postData = { author_id: State.user.id, content: text || '' };
    if (imageUrl)  postData.image_url  = imageUrl;
    if (fileUrl)   postData.file_url   = fileUrl;
    if (fileName)  postData.file_name  = fileName;


    // Attach poll if active
    if (typeof PollState !== 'undefined' && PollState.active) {
      const pollData = (typeof getPollData === 'function') ? getPollData() : null;
      if (pollData) {
        postData.poll = pollData;
      } else if (PollState.active) {
        toast('Add at least 2 poll options', 'circle-exclamation');
        submitBtn.disabled = false; submitBtn.textContent = 'Post'; return;
      }
    }

    const { data: newPost, error } = await MutationGuard.wrapInsert(
      'post',
      () => sb.from('op_posts').insert(postData).select().single()
    );
    if (error) {
      if (!error.blocked) toast('Failed to post: ' + error.message, 'circle-exclamation');
    } else {
      // Increment posts_count on the author's profile
      const currentPostsCount = State.profile?.posts_count || 0;
      await sb.from('op_profiles')
        .update({ posts_count: currentPostsCount + 1 })
        .eq('id', State.user.id);
      if (State.profile) {
        State.profile.posts_count = currentPostsCount + 1;
        _syncProfileStatDOM();
      }

      // Close modal first, then reload the feed so the new post is visible
      document.getElementById('modal-overlay')?.classList.remove('open');
      setTimeout(() => {
        const feed = document.getElementById('feed');
        if (feed) loadPosts(feed);
      }, 100);
      textarea.value = '';
      charCount.textContent = '280';

      selectedImageFile = null;
      selectedAttachFile = null;
      imgInput.value = '';
      fileInput.value = '';
      preview.style.display = 'none';
      preview.innerHTML = '';

      toast('Posted!', 'paper-plane');

      // Check for new milestone badges after posting
      setTimeout(async () => {
        const { data: freshProfile } = await sb.from('op_profiles').select('*').eq('id', State.user.id).single();
        if (!freshProfile) return;
        const earned = computeBadges(freshProfile);
        const prevBadges = computeBadges({ ...freshProfile, posts_count: (freshProfile.posts_count || 1) - 1 });
        const newBadges = earned.filter(b => !prevBadges.find(p => p.id === b.id));
        newBadges.forEach(b => {
          toast(`${b.icon} Badge unlocked: ${b.label}!`, 'trophy');
        });
      }, 1500);

      // Reset poll state
      if (typeof PollState !== 'undefined') {
        PollState.active = false;
        PollState.options = ['', ''];
        document.getElementById('poll-builder-ui')?.remove();
        const pollBtn = document.getElementById('poll-toggle-btn');
        if (pollBtn) { pollBtn.style.color = ''; pollBtn.style.background = ''; }
      }

      // Notify all followers about the new post (fire and forget)
      if (newPost?.id) {
        const postSnippet = (text || '').slice(0, 100) || 'New post';
        sb.from('op_follows').select('follower_id').eq('following_id', State.user.id).then(({ data: followers }) => {
          if (!followers?.length) return;
          const notifications = followers.map(f => ({
            user_id: f.follower_id,
            actor_id: State.user.id,
            type: 'new_post',
            post_id: newPost.id,
            post_title: postSnippet,
          }));
          sb.from('op_notifications').insert(notifications).then(() => {});
        });
      }
    }
    submitBtn.disabled = false;
    submitBtn.textContent = 'Post';
  });
}

/* ── Post Card ──────────────────────────────────────────────── */
function buildPostCard(post, profile, isLiked = false, isBookmarked = false) {
  const card = el('div', 'post-card');
  if (post?.id) card.dataset.postId = post.id;
  const color = avatarColor(profile?.display_name || profile?.username || '?');

  let contentHtml = `<div class="post-content">${escapeHtml(post.content).replace(/#(\w+)/g, '<span class="hashtag" data-tag="$1">#$1</span>').replace(/@(\w+)/g, '<span class="mention">@$1</span>')}</div>`;

  // OG preview placeholder — filled async after card is in DOM
  const _ogUrl = extractFirstUrl(post.content);
  if (_ogUrl && !post.image_url) {
    contentHtml += `<div class="og-preview-slot" data-url="${escapeHtml(_ogUrl)}"></div>`;
  }
  if (post.image_url) {
    contentHtml += `<div class="post-image-wrap"><img src="${escapeHtml(post.image_url)}" class="post-image" alt="Post image" loading="lazy" style="max-width:100%;border-radius:12px;margin-top:8px;border:1px solid var(--border);display:block"></div>`;
  }
  if (post.file_url && post.file_name) {
    const icon = fileIcon(post.file_name);
    contentHtml += `
      <div role="button" tabindex="0" onclick="devitDownloadFile('${escapeHtml(post.file_url)}','${escapeHtml(post.file_name)}')" onkeydown="if(event.key==='Enter')devitDownloadFile('${escapeHtml(post.file_url)}','${escapeHtml(post.file_name)}')" style="cursor:pointer;text-decoration:none;display:inline-flex;align-items:center;gap:10px;padding:10px 14px;background:var(--bg-elevated);border:1px solid var(--border);border-radius:10px;margin-top:8px;max-width:100%;min-width:0;transition:border-color 0.15s" onmouseover="this.style.borderColor='var(--cyan)'" onmouseout="this.style.borderColor='var(--border)'">
        <i class="fa-solid ${icon}" style="font-size:20px;color:var(--cyan);flex-shrink:0"></i>
        <div style="min-width:0;flex:1">
          <div style="font-size:13px;font-weight:600;color:var(--text-primary);white-space:nowrap;overflow:hidden;text-overflow:ellipsis">${escapeHtml(post.file_name)}</div>
          <div style="font-size:11px;color:var(--text-muted)">Click to download</div>
        </div>
        <i class="fa-solid fa-download" style="font-size:13px;color:var(--text-muted);flex-shrink:0"></i>
      </div>`;
  }

  // Render poll if present
  if (post.poll && post.poll.options?.length) {
    const currentUserId = State.user?.id || '';
    contentHtml += renderPollInPost(post.poll, post.id, currentUserId);
  }

  card.innerHTML = `
    <div class="post-header">
      <div class="post-avatar pfp-clickable" data-uid="${profile?.id || ''}" style="background:${color};cursor:pointer" title="View profile">${profile?.avatar_url ? `<img src="${escapeHtml(profile.avatar_url)}" style="width:100%;height:100%;object-fit:cover;border-radius:50%">` : avatarInitials(profile?.display_name || profile?.username || '?')}</div>
      <div class="post-meta">
        <div class="post-author">
          <span class="pfp-clickable" data-uid="${profile?.id || ''}" style="cursor:pointer">${profile?.display_name || profile?.username || 'Unknown'}</span>
          
          <span class="post-author-handle">@${profile?.username || '?'}</span>
        </div>
        <div class="post-time">${timeAgo(post.created_at)}</div>
      </div>
      <button class="post-more-btn" data-pid="${post.id}" data-uid="${profile?.id}" title="${post.author_id === State.user?.id ? 'Post options' : 'More options'}" style="margin-left:auto;color:var(--text-muted);font-size:14px;padding:4px 8px;border-radius:6px;transition:color 0.15s"><i class="fa-solid fa-ellipsis"></i></button>
    </div>
    ${contentHtml}
    <div class="post-actions">
      <button class="post-action comment-btn" title="Comment">
        <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z"/></svg>
        <span class="comment-count">${fmtNum(post.comments_count || 0)}</span>
      </button>
      <button class="post-action like-btn ${isLiked ? 'liked' : ''}" title="Like">
        <svg width="15" height="15" viewBox="0 0 24 24" fill="${isLiked ? 'currentColor' : 'none'}" stroke="currentColor" stroke-width="2"><path d="M20.84 4.61a5.5 5.5 0 0 0-7.78 0L12 5.67l-1.06-1.06a5.5 5.5 0 0 0-7.78 7.78l1.06 1.06L12 21.23l7.78-7.78 1.06-1.06a5.5 5.5 0 0 0 0-7.78z"/></svg>
        <span class="like-count">${fmtNum(post.likes_count || 0)}</span>
      </button>
      <button class="post-action bookmark-btn ${isBookmarked ? 'bookmarked' : ''}" title="Bookmark">
        <svg width="15" height="15" viewBox="0 0 24 24" fill="${isBookmarked ? 'currentColor' : 'none'}" stroke="currentColor" stroke-width="2"><path d="M19 21l-7-5-7 5V5a2 2 0 0 1 2-2h10a2 2 0 0 1 2 2z"/></svg>
      </button>
      <button class="post-action share-btn" title="Share" style="margin-left:auto">
        <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="18" cy="5" r="3"/><circle cx="6" cy="12" r="3"/><circle cx="18" cy="19" r="3"/><line x1="8.59" y1="13.51" x2="15.42" y2="17.49"/><line x1="15.41" y1="6.51" x2="8.59" y2="10.49"/></svg>
      </button>
    </div>
  `;

  // Like toggle
  let likedState = isLiked;
  const likeBtn = $('.like-btn', card);
  likeBtn.addEventListener('click', async e => {
    e.stopPropagation();
    if (guestGuard('like posts')) return;
    likedState = !likedState;
    const countEl = likeBtn.querySelector('.like-count');
    const svg = likeBtn.querySelector('svg');
    const currentCount = parseInt(countEl.textContent) || 0;
    likeBtn.classList.toggle('liked', likedState);
    svg.setAttribute('fill', likedState ? 'currentColor' : 'none');
    countEl.textContent = fmtNum(likedState ? currentCount + 1 : currentCount - 1);
    if (likedState) { likeBtn.style.transform = 'scale(1.3)'; setTimeout(() => likeBtn.style.transform = '', 200); }

    if (likedState) {
      const { error: likeErr } = await MutationGuard.wrapInsert(
        'like',
        () => sb.from('op_post_likes').insert({ post_id: post.id, user_id: State.user.id })
      );
      if (likeErr?.blocked) {
        // Undo optimistic UI update — insert was blocked by rate limit
        likedState = false;
        likeBtn.classList.remove('liked');
        svg.setAttribute('fill', 'none');
        countEl.textContent = fmtNum(currentCount);
      } else if (!likeErr) {
        // Notify author if not self
        if (post.author_id !== State.user.id) {
          await sb.from('op_notifications').insert({ user_id: post.author_id, actor_id: State.user.id, type: 'like', post_id: post.id });
        }
      }
    } else {
      await sb.from('op_post_likes').delete().eq('post_id', post.id).eq('user_id', State.user.id);
    }
  });

  // Bookmark
  let bookmarkedState = isBookmarked;
  const bookmarkBtn = $('.bookmark-btn', card);
  bookmarkBtn.addEventListener('click', async e => {
    e.stopPropagation();
    if (guestGuard('save bookmarks')) return;
    bookmarkedState = !bookmarkedState;
    bookmarkBtn.classList.toggle('bookmarked', bookmarkedState);
    bookmarkBtn.querySelector('svg').setAttribute('fill', bookmarkedState ? 'currentColor' : 'none');
    if (bookmarkedState) {
      await sb.from('op_bookmarks').insert({ post_id: post.id, user_id: State.user.id });
      toast('Saved to bookmarks', 'bookmark');
    } else {
      await sb.from('op_bookmarks').delete().eq('post_id', post.id).eq('user_id', State.user.id);
      toast('Removed from bookmarks', 'bookmark');
    }
  });

  // Comment
  $('.comment-btn', card).addEventListener('click', e => { e.stopPropagation(); if (guestGuard('comment')) return; openPostThread(post, profile); });

  // Hashtag clicks → tag feed
  card.querySelectorAll('.hashtag[data-tag]').forEach(span => {
    span.addEventListener('click', e => { e.stopPropagation(); navigateToTag(span.dataset.tag); });
  });

  // Share
  $('.share-btn', card).addEventListener('click', e => {
    e.stopPropagation();
    navigator.clipboard?.writeText(window.location.origin + '/post/' + post.id).then(() => toast('Link copied!', 'link'));
  });

  // AI Summary (Groq)
  const aiSummaryBtn = card.querySelector('.ai-summary-btn');
  if (aiSummaryBtn) {
    aiSummaryBtn.addEventListener('click', async e => {
      e.stopPropagation();
      const resultEl = aiSummaryBtn.nextElementSibling;
      if (resultEl.style.display !== 'none') { resultEl.style.display = 'none'; aiSummaryBtn.innerHTML = '<i class="fa-solid fa-wand-magic-sparkles"></i> AI Summary'; return; }
      aiSummaryBtn.innerHTML = '<i class="fa-solid fa-spinner fa-spin"></i> Analyzing…';
      aiSummaryBtn.disabled = true;
      const code = decodeURIComponent(aiSummaryBtn.dataset.code);
      const lang = aiSummaryBtn.dataset.lang;
      try {
        const resp = await fetch('https://api.groq.com/openai/v1/chat/completions', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${window.DEVIT_CONFIG?.GROQ_API_KEY || ''}` },
          body: JSON.stringify({
            model: 'llama3-8b-8192',
            max_tokens: 120,
            messages: [{ role: 'user', content: `Summarize this ${lang} code snippet in 2-3 sentences. Be concise and developer-friendly. Code:\n${code}` }]
          })
        });
        const data = await resp.json();
        const summary = data.choices?.[0]?.message?.content?.trim() || 'Unable to summarize.';
        resultEl.textContent = summary;
        resultEl.style.display = 'block';
        aiSummaryBtn.innerHTML = '<i class="fa-solid fa-wand-magic-sparkles"></i> Hide Summary';
      } catch {
        resultEl.textContent = 'AI summary failed. Check your Groq API key.';
        resultEl.style.display = 'block';
        aiSummaryBtn.innerHTML = '<i class="fa-solid fa-wand-magic-sparkles"></i> AI Summary';
      }
      aiSummaryBtn.disabled = false;
    });
  }

  // Unified more-btn: own posts → edit/delete menu; others → report/block menu
  const moreBtn = $('.post-more-btn', card);
  if (moreBtn) {
    moreBtn.addEventListener('click', e => {
      e.stopPropagation();
      if (post.author_id === State.user?.id) {
        openOwnPostMenu(moreBtn, post, profile, card);
      } else {
        openPostMoreMenu(moreBtn, post.id, profile?.id);
      }
    });
  }

  // PFP / author click → quick profile view
  card.querySelectorAll('.pfp-clickable[data-uid]').forEach(el => {
    el.addEventListener('click', e => {
      e.stopPropagation();
      const uid = el.dataset.uid;
      if (uid) openProfileQuickView(uid);
    });
  });

  // Trigger OG preview async after card is ready
  const ogSlot = card.querySelector('.og-preview-slot');
  if (ogSlot) {
    const ogUrl = ogSlot.dataset.url;
    requestAnimationFrame(() => injectOGPreview(post.content, ogSlot));
  }

  return card;
}

function escapeHtml(str) {
  return String(str == null ? '' : str).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
}
window.escapeHtml = escapeHtml;

/* ── Post Thread / Comments ─────────────────────────────────── */
function openPostThread(post, profile) {
  const modal = $('#modal-overlay');
  const body  = $('#modal-body');
  $('#modal-title-text').textContent = 'Post';
  modal.classList.add('open');

  body.innerHTML = `
    <div style="padding:16px;border-bottom:1px solid var(--border)">
      <div style="display:flex;align-items:center;gap:10px;margin-bottom:8px">
        ${avatarHtml(profile, 36)}
        <div>
          <div style="font-weight:700">${profile?.display_name || profile?.username || 'User'}</div>
          <div style="font-size:12px;color:var(--text-muted)">@${profile?.username || '?'} · ${timeAgo(post.created_at)}</div>
        </div>
      </div>
      <div style="font-size:15px;line-height:1.6">${escapeHtml(post.content)}</div>
    </div>
    <div id="comment-list" style="max-height:min(300px,45vh);overflow-y:auto;padding:8px 0;-webkit-overflow-scrolling:touch">
      <div style="padding:16px;text-align:center;color:var(--text-muted);font-size:13px">Loading comments…</div>
    </div>
    <div style="padding:12px 16px;border-top:1px solid var(--border);display:flex;gap:10px;align-items:center">
      ${avatarHtml(State.profile, 32)}
      <input id="comment-input" class="chat-input" placeholder="Write a comment…" style="flex:1">
      <button class="chat-send-btn" id="comment-send-btn">
        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><line x1="22" y1="2" x2="11" y2="13"/><polygon points="22 2 15 22 11 13 2 9 22 2"/></svg>
      </button>
    </div>
  `;

  loadComments(post.id);

  const sendComment = async () => {
    const input = $('#comment-input');
    const text = input.value.trim();
    if (!text) return;
    input.value = '';
    const { error } = await MutationGuard.wrapInsert(
      'comment',
      () => sb.from('op_comments').insert({ post_id: post.id, author_id: State.user.id, content: text })
    );
    if (error?.blocked) {
      toast(error.message, 'clock');
      input.value = text; // restore text so user doesn't lose it
      return;
    }
    if (!error) {
      loadComments(post.id);
      // Increment comment count on all matching post cards in the feed
      document.querySelectorAll(`.post-card[data-post-id="${post.id}"] .comment-count`).forEach(el => {
        el.textContent = fmtNum((parseInt(el.textContent) || 0) + 1);
      });
      // Increment in DB
      await sb.from('op_posts')
        .update({ comments_count: (post.comments_count || 0) + 1 })
        .eq('id', post.id);
      post.comments_count = (post.comments_count || 0) + 1; // keep local ref in sync
      if (post.author_id !== State.user.id) {
        await sb.from('op_notifications').insert({ user_id: post.author_id, actor_id: State.user.id, type: 'comment', post_id: post.id });
      }
    }
  };

  // Clone send button to wipe any pre-existing listeners (modal reuse guard)
  const rawSendBtn = $('#comment-send-btn');
  const freshSendBtn = rawSendBtn.cloneNode(true);
  rawSendBtn.replaceWith(freshSendBtn);
  freshSendBtn.addEventListener('click', sendComment);

  const commentInput = $('#comment-input');
  // Remove old keydown listeners by replacing the element too
  const freshInput = commentInput.cloneNode(true);
  commentInput.replaceWith(freshInput);
  freshInput.addEventListener('keydown', e => { if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); sendComment(); } });
  freshInput.focus();
}

async function loadComments(postId) {
  const container = $('#comment-list');
  if (!container) return;
  const { data: comments } = await sb
    .from('op_comments')
    .select('id, content, created_at, profiles:op_profiles!author_id(id, username, display_name, avatar_url)')
    .eq('post_id', postId)
    .order('created_at', { ascending: true });

  if (!comments?.length) {
    container.innerHTML = `<div style="padding:16px;text-align:center;color:var(--text-muted);font-size:13px">No comments yet — start the conversation!</div>`;
    return;
  }

  // Fetch comment likes for current user
  const commentIds = comments.map(c => c.id);
  const { data: myLikes } = await sb.from('op_comment_likes').select('comment_id, vote').eq('user_id', State.user.id).in('comment_id', commentIds);
  const likeMap = {};
  (myLikes || []).forEach(l => { likeMap[l.comment_id] = l.vote; });

  container.innerHTML = '';
  comments.forEach(c => {
    const div = document.createElement('div');
    div.style.cssText = 'display:flex;gap:10px;padding:10px 16px;border-bottom:1px solid var(--border)';
    div.innerHTML = `
      ${avatarHtml(c.profiles, 30)}
      <div style="flex:1;min-width:0">
        <div style="font-size:13px;font-weight:600">${escapeHtml(c.profiles?.display_name||c.profiles?.username||'User')} <span style="font-size:11px;color:var(--text-muted);font-weight:400">${timeAgo(c.created_at)}</span></div>
        <div style="font-size:13px;margin-top:2px">${escapeHtml(c.content)}</div>
        <div style="display:flex;gap:8px;margin-top:6px">
          <button class="cmt-vote-btn" data-cid="${c.id}" data-vote="1" style="background:none;border:none;cursor:pointer;color:${likeMap[c.id]===1?'var(--cyan)':'var(--text-muted)'};font-size:12px;display:flex;align-items:center;gap:3px;padding:0">
            <i class="fa-${likeMap[c.id]===1?'solid':'regular'} fa-thumbs-up"></i>
          </button>
          <button class="cmt-vote-btn" data-cid="${c.id}" data-vote="-1" style="background:none;border:none;cursor:pointer;color:${likeMap[c.id]===-1?'var(--rose)':'var(--text-muted)'};font-size:12px;display:flex;align-items:center;gap:3px;padding:0">
            <i class="fa-${likeMap[c.id]===-1?'solid':'regular'} fa-thumbs-down"></i>
          </button>
        </div>
      </div>
    `;
    container.appendChild(div);
  });

  // Vote handler
  container.querySelectorAll('.cmt-vote-btn').forEach(btn => {
    btn.addEventListener('click', async () => {
      const cid = btn.dataset.cid;
      const vote = parseInt(btn.dataset.vote);
      const current = likeMap[cid];
      if (current === vote) {
        // toggle off
        await sb.from('op_comment_likes').delete().eq('comment_id', cid).eq('user_id', State.user.id);
        delete likeMap[cid];
      } else {
        await sb.from('op_comment_likes').upsert({ comment_id: cid, user_id: State.user.id, vote }, { onConflict: 'comment_id,user_id' });
        likeMap[cid] = vote;
      }
      // Re-render votes in-place
      const row = btn.closest('[style*="border-bottom"]');
      if (row) {
        row.querySelectorAll('.cmt-vote-btn').forEach(b => {
          const v = parseInt(b.dataset.vote);
          const active = likeMap[cid] === v;
          b.style.color = active ? (v === 1 ? 'var(--cyan)' : 'var(--rose)') : 'var(--text-muted)';
          b.querySelector('i').className = `fa-${active?'solid':'regular'} fa-thumbs-${v===1?'up':'down'}`;
        });
      }
    });
  });
}

/* ── New Post Modal ─────────────────────────────────────────── */
function openNewPostModal() {
  if (guestGuard('post')) return;
  const modal = $('#modal-overlay');
  const body  = $('#modal-body');
  $('#modal-title-text').textContent = 'New Post';
  modal.classList.add('open');
  body.innerHTML = `<div id="modal-composer"></div>`;
  buildComposer($('#modal-composer'));
  const submitBtn = $('#post-submit-btn');
  submitBtn.addEventListener('click', () => {
    setTimeout(() => { if (!$('#post-textarea')?.value?.trim()) modal.classList.remove('open'); }, 500);
  });
}

/* ── Explore ────────────────────────────────────────────────── */
async function renderExplore(main) {
  const EXPLORE_TABS = [
    { id: 'discover',  label: 'Discover',  icon: 'fa-solid fa-compass' },
    { id: 'trending',  label: 'Trending',  icon: 'fa-solid fa-fire' },
  ];
  let activeExploreTab = 'discover';

  main.innerHTML = `
    <div class="explore-header">
      <h2>Explore</h2>
      <p>Discover fellow aviators from around the world</p>
    </div>
    <div class="view-tabs" role="tablist" style="padding:0 4px">
      ${EXPLORE_TABS.map(t => `<div class="view-tab ${t.id === activeExploreTab ? 'active' : ''}" data-etab="${t.id}" role="tab"><i class="${t.icon}" style="margin-right:6px;font-size:11px"></i>${t.label}</div>`).join('')}
    </div>
    <div id="explore-content" style="min-height:200px"></div>
  `;

  async function loadExploreTab(tabId) {
    activeExploreTab = tabId;
    const content = $('#explore-content', main);
    content.innerHTML = `<div style="padding:32px;text-align:center;color:var(--text-muted)"><i class="fa-solid fa-spinner fa-spin" style="margin-right:8px"></i>Loading…</div>`;

    if (tabId === 'discover') {
      const [peopleRes, communityRes] = await Promise.all([
        sb.from('op_profiles').select('id, username, display_name, avatar_url, bio, followers_count').neq('id', State.user.id).order('followers_count', { ascending: false }).limit(8),
        sb.from('op_communities').select('id, name, icon, color, description, members_count').order('members_count', { ascending: false }).limit(6),
      ]);
      const people = peopleRes.data || [];
      const communities = communityRes.data || [];

      content.innerHTML = `
        <div style="padding:16px 16px 4px;font-family:var(--font-display);font-size:14px;font-weight:800;color:var(--text-primary);display:flex;align-items:center;gap:8px">
          <i class="fa-solid fa-users" style="color:var(--cyan)"></i> Aviators to follow
        </div>
        <div id="explore-people" style="display:grid;grid-template-columns:repeat(auto-fill,minmax(190px,1fr));gap:10px;padding:8px 16px 20px"></div>
        <div style="padding:4px 16px 4px;font-family:var(--font-display);font-size:14px;font-weight:800;color:var(--text-primary);display:flex;align-items:center;gap:8px;border-top:1px solid var(--border);padding-top:16px">
          <i class="fa-solid fa-hashtag" style="color:var(--violet)"></i> Communities
          <button id="create-community-btn" style="margin-left:auto;font-size:11px;color:var(--cyan);background:var(--cyan-dim);border:1px solid rgba(255,45,110,0.2);border-radius:8px;padding:3px 10px;font-weight:700"><i class="fa-solid fa-plus"></i> New</button>
        </div>
        <div id="explore-communities" style="display:grid;grid-template-columns:repeat(auto-fill,minmax(175px,1fr));gap:10px;padding:8px 16px 24px"></div>
      `;

      const peopleEl = $('#explore-people', content);
      if (people.length) {
        peopleEl.innerHTML = people.map(p => `
          <div class="explore-person-card" data-uid="${p.id}">
            <div style="display:flex;align-items:center;gap:8px;margin-bottom:8px">
              ${avatarHtml(p, 36)}
              <div style="flex:1;min-width:0">
                <div style="font-weight:700;font-size:13px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${escapeHtml(p.display_name || p.username)}</div>
                <div style="font-size:11px;color:var(--text-muted)">@${escapeHtml(p.username)}</div>
              </div>
            </div>
            <div style="font-size:11px;color:var(--text-secondary);margin-bottom:8px;line-height:1.45;min-height:32px">${escapeHtml((p.bio || '').slice(0, 80) || 'Aviator on Planebook')}</div>
            
            <button class="follow-btn" data-uid="${p.id}" style="width:100%;font-size:12px">Follow</button>
          </div>`).join('');
      } else {
        peopleEl.innerHTML = `<div style="padding:20px;text-align:center;color:var(--text-muted);font-size:13px;grid-column:1/-1">No other aviators yet 🌱</div>`;
      }

      const commEl = $('#explore-communities', content);
      if (communities.length) {
        commEl.innerHTML = communities.map(c => buildCommunityCard(c)).join('');
      } else {
        commEl.innerHTML = `<div style="padding:20px;text-align:center;color:var(--text-muted);font-size:13px;grid-column:1/-1">No communities yet — create one!</div>`;
      }

      $$('.follow-btn', content).forEach(btn => {
        btn.addEventListener('click', async e => {
          e.stopPropagation();
          const uid = btn.dataset.uid;
          if (guestGuard('follow people')) { btn.disabled = false; return; }
          btn.disabled = true; btn.textContent = '…';
          const { error } = await sb.from('op_follows').insert({ follower_id: State.user.id, following_id: uid });
          if (!error || error.code === '23505') {
            await incrementFollowCounts(uid, State.user.id);
            await sb.from('op_notifications').insert({ user_id: uid, actor_id: State.user.id, type: 'follow' });
            btn.innerHTML = '<i class="fa-solid fa-check"></i> Following'; btn.style.opacity = '0.6';
          } else { btn.textContent = 'Follow'; btn.disabled = false; }
        });
      });

      $$('.community-card', content).forEach(card => {
        card.addEventListener('click', () => openCommunity(card.dataset.cid));
      });
      $$('.join-btn', content).forEach(btn => {
        btn.addEventListener('click', async e => {
          e.stopPropagation();
          if (btn.classList.contains('joined')) return;
          await sb.from('op_community_members').insert({ community_id: btn.dataset.cid, user_id: State.user.id });
          btn.innerHTML = '<i class="fa-solid fa-check"></i> Joined'; btn.classList.add('joined');
          toast('Joined!', 'circle-check'); loadSidebarCommunities();
        });
      });
      $('#create-community-btn', content)?.addEventListener('click', openCreateCommunityModal);

    } else if (tabId === 'trending') {
      const { data: posts } = await sb.from('op_posts')
        .select('id, content, code_block, code_lang, image_url, likes_count, comments_count, reposts_count, created_at, author_id, profiles:op_profiles!author_id(id, username, display_name, avatar_url)')
        .order('likes_count', { ascending: false })
        .limit(20);
      content.innerHTML = `<div style="padding:12px 16px 4px;font-family:var(--font-display);font-size:14px;font-weight:800;color:var(--text-primary);display:flex;align-items:center;gap:8px"><i class="fa-solid fa-fire" style="color:var(--violet)"></i> Trending Posts</div><div id="trending-posts-feed"></div>`;
      const feed = $('#trending-posts-feed', content);
      if (!posts?.length) { feed.innerHTML = `<div style="padding:40px;text-align:center;color:var(--text-muted)">No trending posts yet 🌱</div>`; return; }
      const postIds = posts.map(p => p.id);
      const { data: likes } = await sb.from('op_post_likes').select('post_id').eq('user_id', State.user.id).in('post_id', postIds);
      const likedIds = new Set((likes || []).map(l => l.post_id));
      posts.forEach((p, i) => {
        const rankEl = document.createElement('div');
        rankEl.style.cssText = 'display:flex;align-items:center;padding:4px 16px 0;gap:8px';
        rankEl.innerHTML = `<span style="font-family:var(--font-mono);font-size:11px;font-weight:800;color:${i<3?'var(--amber)':'var(--text-muted)'};min-width:20px">#${i+1}</span><span style="font-size:11px;color:var(--text-muted)"><i class="fa-solid fa-heart" style="color:var(--rose)"></i> ${fmtNum(p.likes_count||0)}</span>`;
        feed.appendChild(rankEl);
        feed.appendChild(buildPostCard(p, p.profiles, likedIds.has(p.id), false));
      });

    }
  }

  $$('.view-tab[data-etab]', main).forEach(tab => {
    tab.addEventListener('click', () => {
      $$('.view-tab[data-etab]', main).forEach(t => t.classList.remove('active'));
      tab.classList.add('active');
      loadExploreTab(tab.dataset.etab);
    });
  });

  loadExploreTab('discover');
}

function buildCommunityCard(c, isJoined = false) {
  return `<div class="community-card" data-cid="${c.id}">
    <div class="community-card-icon" style="background:rgba(${hexToRgb(c.color)},0.15);color:${c.color}">${c.icon}</div>
    <div class="community-card-name">${c.name}</div>
    <div class="community-card-desc">${c.description || ''}</div>
    <div class="community-card-meta">
      <span class="community-card-members">👥 ${fmtNum(c.members_count || 0)}</span>
    </div>
    <button class="join-btn ${isJoined ? 'joined' : ''}" data-cid="${c.id}">${isJoined ? '<i class="fa-solid fa-check"></i> Joined' : 'Join'}</button>
  </div>`;
}

function hexToRgb(hex) {
  const result = /^#?([a-f\d]{2})([a-f\d]{2})([a-f\d]{2})$/i.exec(hex);
  return result ? `${parseInt(result[1],16)},${parseInt(result[2],16)},${parseInt(result[3],16)}` : '99,217,255';
}

/* ── Create Community Modal ─────────────────────────────────── */
function openCreateCommunityModal() {
  const modal = $('#modal-overlay');
  const body  = $('#modal-body');
  $('#modal-title-text').textContent = 'Create Community';
  modal.classList.add('open');

  const icons = ['🌐','🦀','⚛️','🧠','☁️','🎨','🔬','🌍','🔥','💻','🤖','🎵'];

  body.innerHTML = `
    <div style="padding:16px;display:flex;flex-direction:column;gap:14px">
      <div class="auth-input-group">
        <label>Community Name</label>
        <input type="text" id="comm-name" class="auth-input" placeholder="e.g. Rust & Systems" maxlength="50">
      </div>
      <div class="auth-input-group">
        <label>Description</label>
        <textarea id="comm-desc" class="auth-input" placeholder="What's this community about?" rows="3" style="resize:vertical"></textarea>
      </div>
      <div class="auth-input-group">
        <label>Icon</label>
        <div style="display:flex;gap:8px;flex-wrap:wrap">
          ${icons.map(i => `<button class="comm-icon-btn" data-icon="${i}" style="font-size:22px;padding:8px;border-radius:8px;background:var(--bg-elevated);border:2px solid transparent;transition:all 0.15s">${i}</button>`).join('')}
        </div>
      </div>
      <div class="auth-input-group">
        <label>Color</label>
        <div style="display:flex;gap:8px;flex-wrap:wrap">
          ${['#ff2d6e','#a78bfa','#34d399','#fb7185','#fbbf24','#f97316','#38bdf8','#f472b6'].map(c =>
            `<button class="comm-color-btn" data-color="${c}" style="width:28px;height:28px;border-radius:50%;background:${c};border:3px solid transparent;transition:all 0.15s"></button>`
          ).join('')}
        </div>
      </div>
      <button class="auth-btn-primary" id="create-comm-btn">Create Community</button>
    </div>
  `;

  let selectedIcon = '🌐', selectedColor = '#ff2d6e';
  $$('.comm-icon-btn', body).forEach(btn => {
    btn.addEventListener('click', () => {
      $$('.comm-icon-btn', body).forEach(b => b.style.borderColor = 'transparent');
      btn.style.borderColor = 'var(--cyan)';
      selectedIcon = btn.dataset.icon;
    });
  });
  $$('.comm-color-btn', body).forEach(btn => {
    btn.addEventListener('click', () => {
      $$('.comm-color-btn', body).forEach(b => b.style.borderColor = 'transparent');
      btn.style.borderColor = 'white';
      selectedColor = btn.dataset.color;
    });
  });

  $('#create-comm-btn').addEventListener('click', async () => {
    const name = $('#comm-name').value.trim();
    const desc = $('#comm-desc').value.trim();
    if (!name) { toast('Enter a community name', 'triangle-exclamation'); return; }

    const btn = $('#create-comm-btn');
    btn.disabled = true;
    btn.textContent = 'Creating…';

    const { data: community, error } = await sb.from('op_communities').insert({
      name, description: desc, icon: selectedIcon, color: selectedColor,
      owner_id: State.user.id, members_count: 1
    }).select().single();

    if (error) {
      toast('Failed: ' + error.message, 'circle-exclamation');
      btn.disabled = false;
      btn.textContent = 'Create Community';
      return;
    }

    // Create default channels
    await sb.from('op_channels').insert([
      { community_id: community.id, name: 'general', type: 'text' },
      { community_id: community.id, name: 'showcase', type: 'text' },
      { community_id: community.id, name: 'help', type: 'text' },
    ]);

    // Join as owner
    await sb.from('op_community_members').insert({ community_id: community.id, user_id: State.user.id, role: 'owner' });

    modal.classList.remove('open');
    toast(`${selectedIcon} ${name} created!`, '🎉');
    loadSidebarCommunities();
    openCommunity(community.id);
  });
}

/* ── Community View ─────────────────────────────────────────── */
async function openCommunity(communityId) {
  const { data: community } = await sb.from('op_communities').select('*').eq('id', communityId).single();
  if (!community) return;
  State.currentCommunity = community;
  State.currentView = 'community';
  showPresence();
  updateSidebarActive();

  const { data: channels } = await sb.from('op_channels').select('*').eq('community_id', communityId).order('created_at');
  const { count: memberCount } = await sb.from('op_community_members').select('*', { count: 'exact', head: true }).eq('community_id', communityId);
  const isJoined = !!(await sb.from('op_community_members').select('id').eq('community_id', communityId).eq('user_id', State.user.id).single()).data;

  const main = $('#main');
  main.innerHTML = '';
  main.style.overflow = 'hidden';  // community view manages its own scroll

  const textChannels = (channels || []).filter(c => c.type === 'text');
  const firstChannel = textChannels[0];

  const userProfile = State.profile;
  const userColor = avatarColor(userProfile?.display_name || userProfile?.username || '?');
  const userInitials = avatarInitials(userProfile?.display_name || userProfile?.username || '?');

  const view = el('div', 'community-view');
  view.innerHTML = `
    <div class="disc-sidebar">
      <!-- Server Header -->
      <div class="disc-server-header" style="--server-color:${community.color || '#5865f2'}">
        <div class="disc-server-banner">
          <div class="disc-server-banner-icon">${community.icon}</div>
          <div class="disc-server-banner-glow" style="background:radial-gradient(ellipse at 30% 50%, ${community.color || '#5865f2'}44 0%, transparent 70%)"></div>
        </div>
        <div class="disc-server-name-row">
          <span class="disc-server-name">${community.name}</span>
          <i class="fa-solid fa-chevron-down disc-server-chevron"></i>
        </div>
        <div class="disc-server-meta">${fmtNum(community.members_count || 0)} members</div>
        ${!isJoined ? `<button id="join-community-btn" class="disc-join-btn"><i class="fa-solid fa-user-plus"></i> Join Server</button>` : ''}
      </div>

      <!-- Channels scroll area -->
      <div class="disc-channels-scroll">
        <div class="disc-category-block">
          <div class="disc-category-header" id="disc-cat-text">
            <i class="fa-solid fa-chevron-down disc-cat-arrow"></i>
            <span>Text Channels</span>
            ${community.owner_id === State.user.id ? `<button class="disc-add-channel-btn" id="add-channel-btn" title="Add channel"><i class="fa-solid fa-plus"></i></button>` : ''}
          </div>
          <div class="disc-channel-list" id="disc-channel-list-text">
            ${textChannels.map(ch => `
              <div class="disc-channel-item ${firstChannel?.id === ch.id ? 'active' : ''}" data-chid="${ch.id}">
                <span class="disc-hash">#</span>
                <span class="disc-channel-name">${ch.name}</span>
                <span class="disc-channel-unread" style="display:none"></span>
              </div>
            `).join('')}
          </div>
        </div>
      </div>

      <!-- User Panel -->
      <div class="disc-user-panel">
        <div class="disc-user-avatar-wrap">
          <div class="disc-user-avatar" style="background:${userColor}">${userInitials}</div>
          <div class="disc-user-status-dot online"></div>
        </div>
        <div class="disc-user-info">
          <div class="disc-user-name">${userProfile?.display_name || userProfile?.username || 'User'}</div>
          <div class="disc-user-tag">#${(userProfile?.username || 'user').slice(0,8)}</div>
        </div>
        <div class="disc-user-controls">
          <button class="disc-icon-btn" title="Mute"><i class="fa-solid fa-microphone-slash"></i></button>
          <button class="disc-icon-btn" title="Settings"><i class="fa-solid fa-gear"></i></button>
        </div>
      </div>
    </div>

    <div class="community-chat" id="community-chat-area"></div>
    <div class="disc-members-panel" id="members-panel"></div>
  `;

  main.appendChild(view);

  $$('.disc-channel-item[data-chid]', view).forEach(item => {
    item.addEventListener('click', () => {
      $$('.disc-channel-item', view).forEach(c => c.classList.remove('active'));
      item.classList.add('active');
      const ch = (channels || []).find(c => c.id === item.dataset.chid);
      // Close sidebar first (instant, no animation delay blocking the tap)
      if (view._closeSidebar) view._closeSidebar();
      // Then switch channel on next frame so the close doesn't eat the event
      if (ch) requestAnimationFrame(() => renderChannelChat($('#community-chat-area'), ch));
    });
  });

  // Category collapse toggle
  $('#disc-cat-text', view)?.addEventListener('click', e => {
    if (e.target.closest('#add-channel-btn')) return;
    const list = $('#disc-channel-list-text', view);
    const arrow = $('#disc-cat-text .disc-cat-arrow', view);
    list?.classList.toggle('collapsed');
    arrow?.classList.toggle('collapsed');
  });

  const joinBtn = $('#join-community-btn', view);
  if (joinBtn) {
    joinBtn.addEventListener('click', async () => {
      joinBtn.disabled = true;
      const { error } = await sb.from('op_community_members').insert({ community_id: communityId, user_id: State.user.id });
      if (error && error.code !== '23505') {
        toast('Failed to join', 'circle-exclamation');
        joinBtn.disabled = false;
        return;
      }
      // Increment count in DB
      const newCount = (community.members_count || 0) + 1;
      await sb.from('op_communities').update({ members_count: newCount }).eq('id', communityId);
      community.members_count = newCount;
      // Update DOM count — sidebar meta and channel header
      const metaEl = view.querySelector('.disc-server-meta');
      if (metaEl) metaEl.textContent = fmtNum(newCount) + ' members';
      const headerCountEl = document.getElementById('channel-member-count');
      if (headerCountEl) headerCountEl.textContent = fmtNum(newCount) + ' members';
      joinBtn.remove();
      toast('Joined!', 'circle-check');
      loadSidebarCommunities();
    });
  }

  const addChannelBtn = $('#add-channel-btn', view);
  if (addChannelBtn) {
    addChannelBtn.addEventListener('click', () => {
      const modal = $('#modal-overlay');
      const body  = $('#modal-body');
      $('#modal-title-text').textContent = 'Add Channel';
      modal.classList.add('open');
      body.innerHTML = `
        <div style="padding:16px;display:flex;flex-direction:column;gap:14px">
          <div class="auth-input-group">
            <label>Channel Name</label>
            <input type="text" id="new-channel-name" class="auth-input" placeholder="e.g. announcements" maxlength="50" autocomplete="off">
          </div>
          <button class="auth-btn-primary" id="confirm-add-channel-btn">Create Channel</button>
        </div>
      `;
      setTimeout(() => $('#new-channel-name')?.focus(), 50);
      const confirmBtn = $('#confirm-add-channel-btn');
      const doCreate = async () => {
        const name = $('#new-channel-name').value.trim();
        if (!name) { toast('Enter a channel name', 'triangle-exclamation'); return; }
        const cleanName = name.toLowerCase().replace(/[^a-z0-9-]/g, '-');
        confirmBtn.disabled = true;
        confirmBtn.textContent = 'Creating…';
        const { data: newCh } = await sb.from('op_channels').insert({ community_id: communityId, name: cleanName, type: 'text' }).select().single();
        modal.classList.remove('open');
        if (newCh) openCommunity(communityId);
      };
      confirmBtn.addEventListener('click', doCreate);
      $('#new-channel-name').addEventListener('keydown', e => { if (e.key === 'Enter') doCreate(); });
    });
  }

  if (firstChannel) renderChannelChat($('#community-chat-area'), firstChannel);
  renderCommunityMembers($('#members-panel'), communityId);
  // Wire up swipe-to-reveal sidebar on mobile
  initCommunitySidebarSwipe(view);
}

async function renderChannelChat(container, channel) {
  State.currentChannel = channel;
  container.innerHTML = `
    <div class="disc-chat-header">
      <div class="disc-chat-header-left">
        <button id="open-sidebar-btn" class="disc-channel-name-btn" title="Show channels">
          <span class="disc-chat-hash">#</span>
          <h3 class="disc-chat-title">${channel.name}</h3>
        </button>
        ${channel.topic ? `<div class="disc-chat-divider"></div><span class="disc-chat-topic">${escapeHtml(channel.topic)}</span>` : ''}
      </div>
      <div class="disc-chat-header-right">
        <button id="pin-resource-btn" class="disc-header-btn" title="Pin a resource"><i class="fa-solid fa-thumbtack"></i></button>
        <span class="disc-header-member-count" id="channel-member-count"></span>
        <button class="disc-header-btn disc-header-btn--active" id="toggle-members-btn" title="Toggle Members"><i class="fa-solid fa-users"></i></button>
        <button class="disc-header-btn" title="Search"><i class="fa-solid fa-magnifying-glass"></i></button>
      </div>
    </div>
    <div id="pinned-resources-bar" style="display:none;padding:8px 16px;background:rgba(251,191,36,0.05);border-bottom:1px solid rgba(251,191,36,0.12)"></div>
    <div class="disc-chat-messages" id="chat-messages-list"></div>
    <div class="disc-chat-typing-bar" id="disc-chat-typing-bar" style="display:none;padding:2px 16px 0;min-height:18px"></div>
    <div class="disc-chat-input-area">
      <div class="disc-chat-input-wrap">
        <button class="disc-attach-btn" title="Attach file"><i class="fa-solid fa-plus"></i></button>
        <input class="disc-chat-input" id="channel-chat-input" type="text" placeholder="Message #${escapeHtml(channel.name)}" maxlength="2000" autocomplete="off">
        <div class="disc-input-right">
          <button class="disc-emoji-btn" title="Emoji"><i class="fa-regular fa-face-smile"></i></button>
          <button class="disc-send-btn" id="channel-send-btn" title="Send">
            <svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor"><path d="M2.01 21L23 12 2.01 3 2 10l15 2-15 2z"/></svg>
          </button>
        </div>
      </div>
    </div>
  `;

  // ── Grab DOM refs early — mirrors DM's top-of-function refs ──
  const msgList  = $('#chat-messages-list', container);
  const input    = $('#channel-chat-input', container);
  const sendBtn  = $('#channel-send-btn', container);
  const typingBar = container.querySelector('#disc-chat-typing-bar');

  // ── Populate header member count ──
  const memberCountEl = container.querySelector('#channel-member-count');
  if (memberCountEl && State.currentCommunity) {
    const mc = State.currentCommunity.members_count || 0;
    memberCountEl.textContent = fmtNum(mc) + ' members';
  }

  // ── Reply state — declared before message render so the loop can reference it ──
  const replyState = { msg: null };

  // ── Typing indicator helpers (mirrors DM's showTypingIndicator / hideTypingIndicator) ──
  let _chTypingTimeout = null;
  function showChTypingIndicator(name) {
    if (!typingBar) return;
    typingBar.style.display = 'flex';
    typingBar.style.alignItems = 'center';
    typingBar.style.gap = '6px';
    typingBar.innerHTML = `
      <span class="typing-indicator">
        <span class="typing-dot"></span><span class="typing-dot"></span><span class="typing-dot"></span>
      </span>
      <span style="font-size:12px;color:var(--text-muted)">${escapeHtml(name)} is typing…</span>`;
    clearTimeout(_chTypingTimeout);
    _chTypingTimeout = setTimeout(() => { if (typingBar) typingBar.style.display = 'none'; }, 3000);
  }
  function hideChTypingIndicator() {
    clearTimeout(_chTypingTimeout);
    if (typingBar) typingBar.style.display = 'none';
  }

  // ── Load pinned resources ──
  async function loadPinnedResources() {
    const { data: pins } = await sb.from('op_channel_pins').select('*').eq('channel_id', channel.id).order('created_at', { ascending: false }).limit(5);
    const bar = container.querySelector('#pinned-resources-bar');
    if (!bar) return;
    if (!pins?.length) { bar.style.display = 'none'; return; }
    bar.style.cssText += ';display:flex;flex-wrap:wrap;gap:6px;align-items:center;';
    bar.innerHTML = `<span style="font-size:10px;font-weight:800;color:var(--amber);text-transform:uppercase;letter-spacing:0.05em;margin-right:2px"><i class="fa-solid fa-thumbtack" style="font-size:9px"></i> Pinned</span>` +
      pins.map(p => `<a href="${escapeHtml(p.url || '#')}" target="_blank" rel="noopener noreferrer" onclick="event.stopPropagation()" class="pinned-resource-chip">${escapeHtml(p.emoji || '📌')} ${escapeHtml(p.title || p.url || 'Resource')}</a>`).join('');
  }
  loadPinnedResources();

  // ── Pin Resource modal ──
  container.querySelector('#pin-resource-btn')?.addEventListener('click', () => {
    const modal = document.getElementById('modal-overlay');
    const body  = document.getElementById('modal-body');
    if (!modal || !body) return;
    document.getElementById('modal-title-text').textContent = '📌 Pin a Resource';
    modal.classList.add('open');
    body.innerHTML = `
      <div style="padding:16px;display:flex;flex-direction:column;gap:12px">
        <div style="font-size:12px;color:var(--text-muted)">Share a useful link, tutorial, or documentation with everyone in #${escapeHtml(channel.name)}.</div>
        <div class="auth-input-group"><label>Title</label><input type="text" id="pin-title" class="auth-input" placeholder="e.g. Official React Docs" maxlength="80"></div>
        <div class="auth-input-group"><label>URL</label><input type="url" id="pin-url" class="auth-input" placeholder="https://…" style="font-family:var(--font-mono);font-size:13px"></div>
        <div style="display:flex;align-items:center;gap:8px">
          <label style="font-size:12px;color:var(--text-secondary);flex-shrink:0">Emoji</label>
          <div style="display:flex;gap:6px;flex-wrap:wrap">
            ${['📌','📖','🛠️','🎓','🚀','🔗','💡','🧪','📝','⚡'].map(e => `<button class="pin-emoji-btn" data-emoji="${e}" style="font-size:18px;padding:4px;border-radius:6px;border:2px solid transparent;background:var(--bg-elevated);cursor:pointer;transition:all 0.12s">${e}</button>`).join('')}
          </div>
        </div>
        <button class="auth-btn-primary" id="pin-confirm-btn"><i class="fa-solid fa-thumbtack"></i> Pin Resource</button>
      </div>`;
    let emoji = '📌';
    body.querySelectorAll('.pin-emoji-btn').forEach(b => {
      b.addEventListener('click', () => {
        body.querySelectorAll('.pin-emoji-btn').forEach(x => x.style.borderColor = 'transparent');
        b.style.borderColor = 'var(--amber)';
        emoji = b.dataset.emoji;
      });
    });
    document.getElementById('pin-confirm-btn').addEventListener('click', async () => {
      const title = document.getElementById('pin-title').value.trim();
      const url   = document.getElementById('pin-url').value.trim();
      if (!title || !url) { toast('Enter title and URL', 'triangle-exclamation'); return; }
      const { error } = await sb.from('op_channel_pins').insert({ channel_id: channel.id, author_id: State.user.id, title, url, emoji });
      if (!error) { modal.classList.remove('open'); loadPinnedResources(); toast('Resource pinned!', 'thumbtack'); }
      else toast('Failed: ' + error.message, 'circle-exclamation');
    });
  });

  // ── Tap channel name → open sidebar drawer ──
  container.querySelector('#open-sidebar-btn')?.addEventListener('click', () => {
    const communityView = container.closest('.community-view') || document.querySelector('.community-view');
    if (communityView?._openSidebar) {
      communityView._openSidebar();
    }
  });

  // ── Toggle members panel ──
  container.querySelector('#toggle-members-btn')?.addEventListener('click', () => {
    const panel = document.getElementById('members-panel');
    if (panel) {
      panel.classList.toggle('hidden');
      container.querySelector('#toggle-members-btn')?.classList.toggle('disc-header-btn--active');
    }
  });

  // ── ch:reply event — fired by reply buttons inside buildChannelMessage ──
  msgList.addEventListener('ch:reply', e => {
    showReplyBar(e.detail.msg);
  });

  // ── Load messages — no FK join (mirrors DM: fetch messages then profiles separately) ──
  const { data: messages, error: msgLoadErr } = await sb
    .from('op_channel_messages')
    .select('id, content, created_at, author_id, reply_to_id, reply_to_content, reply_to_author')
    .eq('channel_id', channel.id)
    .order('created_at', { ascending: true })
    .limit(80);

  if (msgLoadErr) console.error('[OnlyPlanes] Failed to load channel messages:', msgLoadErr);

  const msgsToRender = messages || [];

  // ── Welcome banner — only rendered when no messages (mirrors DM's welcome banner) ──
  if (!msgsToRender.length) {
    const welcomeEl = document.createElement('div');
    welcomeEl.id = 'ch-welcome-banner';
    welcomeEl.className = 'disc-channel-welcome';
    welcomeEl.innerHTML = `
      <div class="disc-welcome-hash">#</div>
      <h3 class="disc-welcome-title">Welcome to #${escapeHtml(channel.name)}!</h3>
      <p class="disc-welcome-sub">This is the start of the <strong>#${escapeHtml(channel.name)}</strong> channel.</p>`;
    msgList.appendChild(welcomeEl);
  }

  // ── Batch-fetch all author profiles (mirrors DM's single up-front profile fetch) ──
  const authorIds = [...new Set(msgsToRender.map(m => m.author_id).filter(Boolean))];
  const profileMap = {};
  if (authorIds.length) {
    const { data: authorProfiles } = await sb
      .from('op_profiles')
      .select('id, username, display_name, avatar_url')
      .in('id', authorIds);
    (authorProfiles || []).forEach(p => { profileMap[p.id] = p; });
  }
  // Always seed own profile so optimistic sends render immediately
  if (State.profile?.id) profileMap[State.profile.id] = State.profile;

  msgsToRender.forEach((msg, i) => {
    msg.profiles = profileMap[msg.author_id] || null;
    const prev = msgsToRender[i - 1];
    const isCont = prev && prev.author_id === msg.author_id;
    msgList.appendChild(buildChannelMessage(msg, isCont, profileMap, msgList, input, replyState));
  });
  msgList.scrollTop = msgList.scrollHeight;

  // ── Broadcast channel — mirrors DM's realtimeCh exactly ──
  const broadcastCh = sb.channel(`channel_realtime_${channel.id}`, {
    config: { broadcast: { self: false } },
  });

  broadcastCh
    .on('broadcast', { event: 'new_message' }, ({ payload }) => {
      const msg = payload;
      if (!msg || msg.author_id === State.user.id) return;
      const listEl = document.getElementById('chat-messages-list');
      if (!listEl) return;
      if (msg.id && listEl.querySelector(`[data-msgid="${msg.id}"]`)) return;
      // Remove welcome banner on first incoming message
      listEl.querySelector('#ch-welcome-banner')?.remove();
      hideChTypingIndicator();
      // Hydrate profile from map (sender broadcast their profile in payload)
      if (msg.profiles) profileMap[msg.author_id] = msg.profiles;
      const prev = listEl.lastElementChild?.dataset?.uid ? listEl.lastElementChild : null;
      const isCont = prev && prev.dataset.uid === msg.author_id && !prev.classList.contains('disc-channel-welcome');
      const msgEl = buildChannelMessage(msg, isCont, profileMap, listEl, input, replyState);
      msgEl.dataset.msgid = msg.id || '';
      listEl.appendChild(msgEl);
      listEl.scrollTop = listEl.scrollHeight;
    })
    .on('broadcast', { event: 'typing' }, ({ payload }) => {
      // mirrors DM's typing broadcast handler
      if (payload?.userId === State.user.id) return;
      const senderProfile = profileMap[payload?.userId];
      const name = senderProfile?.display_name || senderProfile?.username || 'Someone';
      showChTypingIndicator(name);
    })
    .subscribe();

  const unsubBroadcast = realtimeManager.subscribeRaw(`view:channel_bc_${channel.id}`, broadcastCh, () => {});
  ViewUnsubFns.push(unsubBroadcast);

  // ── Postgres changes fallback (other devices / missed broadcasts) ──
  const unsubPg = realtimeManager.subscribe(
    `view:channel_${channel.id}`,
    'op_channel_messages',
    'INSERT',
    async payload => {
      const msg = payload.new;
      if (msg.author_id === State.user.id) return;
      const listEl = document.getElementById('chat-messages-list');
      if (!listEl) return;
      if (msg.id && listEl.querySelector(`[data-msgid="${msg.id}"]`)) return;
      // Fetch profile if not already in map
      if (!profileMap[msg.author_id]) {
        const { data: profile } = await sb.from('op_profiles').select('id, username, display_name, avatar_url').eq('id', msg.author_id).single();
        if (profile) profileMap[profile.id] = profile;
      }
      msg.profiles = profileMap[msg.author_id] || null;
      listEl.querySelector('#ch-welcome-banner')?.remove();
      const prev = listEl.lastElementChild?.dataset?.uid ? listEl.lastElementChild : null;
      const isCont = prev && prev.dataset.uid === msg.author_id && !prev.classList.contains('disc-channel-welcome');
      const msgEl = buildChannelMessage(msg, isCont, profileMap, listEl, input, replyState);
      msgEl.dataset.msgid = msg.id || '';
      listEl.appendChild(msgEl);
      listEl.scrollTop = listEl.scrollHeight;
    },
    `channel_id=eq.${channel.id}`
  );
  ViewUnsubFns.push(unsubPg);

  // ── Reply bar (mirrors DM pattern, inserted above input area) ──
  const replyBar = document.createElement('div');
  replyBar.id = 'channel-reply-bar';
  replyBar.className = 'ch-reply-bar';
  replyBar.style.display = 'none';
  replyBar.innerHTML = `
    <div class="ch-reply-bar-inner">
      <i class="fa-solid fa-reply" style="color:var(--cyan);font-size:11px"></i>
      <span class="ch-reply-bar-label">Replying to <strong class="ch-reply-bar-name"></strong></span>
      <span class="ch-reply-bar-preview"></span>
      <button class="ch-reply-bar-close" title="Cancel reply"><i class="fa-solid fa-xmark"></i></button>
    </div>`;
  const inputArea = container.querySelector('.disc-chat-input-area');
  if (inputArea) inputArea.insertBefore(replyBar, inputArea.firstChild);

  function showReplyBar(msg) {
    replyState.msg = msg;
    const authorName = msg.profiles?.display_name || msg.profiles?.username || 'User';
    replyBar.querySelector('.ch-reply-bar-name').textContent = authorName;
    replyBar.querySelector('.ch-reply-bar-preview').textContent = (msg.content || '').slice(0, 60) + ((msg.content || '').length > 60 ? '…' : '');
    replyBar.style.display = 'block';
    input.focus();
  }

  function clearReplyBar() {
    replyState.msg = null;
    replyBar.style.display = 'none';
  }

  replyBar.querySelector('.ch-reply-bar-close').addEventListener('click', clearReplyBar);

  // ── Send message — mirrors DM's sendDM exactly ──
  async function sendMsg() {
    const text = input.value.trim();
    if (!text) return;
    input.value = '';
    const replyRef = replyState.msg ? {
      reply_to_id:      replyState.msg.id,
      reply_to_content: replyState.msg.content,
      reply_to_author:  replyState.msg.profiles?.display_name || replyState.msg.profiles?.username || 'User',
    } : {};
    clearReplyBar();
    const msgData = { channel_id: channel.id, author_id: State.user.id, content: text, ...replyRef };
    const { data: msg, error: msgErr } = await MutationGuard.wrapInsert(
      'channel_msg',
      () => sb.from('op_channel_messages').insert(msgData).select().single()
    );
    if (msgErr?.blocked) { toast(msgErr.message, 'triangle-exclamation'); input.value = text; return; }
    if (msgErr)          { toast('Failed to send message', 'circle-exclamation'); input.value = text; return; }
    // Optimistic render — use returned row, fall back to local data (mirrors DM)
    const displayMsg = msg || { ...msgData, created_at: new Date().toISOString() };
    displayMsg.profiles = State.profile;
    // Remove welcome banner on first sent message
    msgList.querySelector('#ch-welcome-banner')?.remove();
    const lastMsg = msgList.lastElementChild?.dataset?.uid ? msgList.lastElementChild : null;
    const isCont = lastMsg && lastMsg.dataset.uid === State.user.id && !replyRef.reply_to_id;
    const msgEl = buildChannelMessage(displayMsg, isCont, profileMap, msgList, input, replyState);
    if (displayMsg.id) msgEl.dataset.msgid = displayMsg.id;
    msgList.appendChild(msgEl);
    msgList.scrollTop = msgList.scrollHeight;
    // Broadcast for instant delivery to other members
    if (msg) {
      await broadcastCh.send({
        type: 'broadcast',
        event: 'new_message',
        payload: { ...msg, profiles: State.profile },
      });
    }
  }

  // ── Broadcast typing event with debounce (mirrors DM's typing broadcast) ──
  let _chTypingBroadcastTimeout = null;
  input.addEventListener('input', () => {
    if (_chTypingBroadcastTimeout) return;
    broadcastCh.send({
      type: 'broadcast',
      event: 'typing',
      payload: { userId: State.user.id },
    }).catch(() => {});
    _chTypingBroadcastTimeout = setTimeout(() => { _chTypingBroadcastTimeout = null; }, 2000);
  });

  sendBtn.addEventListener('click', sendMsg);
  input.addEventListener('keydown', e => { if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); sendMsg(); } });
  input.focus();
}

function buildChannelMessage(msg, isContinuation, profileMap = {}, msgList = null, input = null, replyState = null) {
  const profile = msg.profiles;
  const color = avatarColor(profile?.display_name || profile?.username || '?');
  const name = profile?.display_name || profile?.username || 'User';
  const isOwn = msg.author_id === State.user?.id;

  const msgEl = el('div', `disc-msg${isContinuation ? ' disc-msg--cont' : ''}`);
  msgEl.dataset.uid = profile?.id || '';
  if (msg.id) msgEl.dataset.msgid = msg.id;

  const avatarHtml = profile?.avatar_url
    ? `<img src="${escapeHtml(profile.avatar_url)}" alt="${escapeHtml(name)}" class="disc-msg-avatar-img">`
    : `<div class="disc-msg-avatar" style="background:${color}">${avatarInitials(name)}</div>`;

  // Reply quote block
  const replyHtml = msg.reply_to_id
    ? `<div class="disc-msg-reply-quote" data-jump="${escapeHtml(msg.reply_to_id)}">
        <i class="fa-solid fa-reply" style="font-size:10px;color:var(--cyan);opacity:0.8"></i>
        <span class="disc-msg-reply-author">${escapeHtml(msg.reply_to_author || 'User')}</span>
        <span class="disc-msg-reply-text">${escapeHtml((msg.reply_to_content || '').slice(0, 80))}${(msg.reply_to_content || '').length > 80 ? '…' : ''}</span>
       </div>`
    : '';

  // Action buttons (reply always, edit/delete only for own)
  const actionsHtml = `
    <div class="disc-msg-actions">
      <button class="disc-msg-action-btn ch-reply-btn" title="Reply"><i class="fa-solid fa-reply"></i></button>
      ${isOwn ? `<button class="disc-msg-action-btn ch-edit-btn" title="Edit"><i class="fa-solid fa-pencil"></i></button>` : ''}
      ${isOwn ? `<button class="disc-msg-action-btn ch-delete-btn" title="Delete" style="color:var(--rose)"><i class="fa-solid fa-trash"></i></button>` : ''}
    </div>`;

  msgEl.innerHTML = `
    <div class="disc-msg-avatar-col">
      ${isContinuation
        ? `<span class="disc-msg-hover-time">${timeAgo(msg.created_at)}</span>`
        : avatarHtml}
    </div>
    <div class="disc-msg-body">
      ${!isContinuation ? `<div class="disc-msg-header"><span class="disc-msg-author" style="color:${color}">${escapeHtml(name)}</span><span class="disc-msg-time">${timeAgo(msg.created_at)}</span></div>` : ''}
      ${replyHtml}
      <div class="disc-msg-text">${escapeHtml(msg.content)}</div>
    </div>
    ${actionsHtml}
  `;

  // Jump to quoted message on quote click
  if (msg.reply_to_id) {
    msgEl.querySelector('.disc-msg-reply-quote')?.addEventListener('click', () => {
      const target = msgList?.querySelector(`[data-msgid="${msg.reply_to_id}"]`);
      if (target) {
        target.scrollIntoView({ behavior: 'smooth', block: 'center' });
        target.classList.add('disc-msg-highlight');
        setTimeout(() => target.classList.remove('disc-msg-highlight'), 1500);
      }
    });
  }

  // Reply button
  msgEl.querySelector('.ch-reply-btn')?.addEventListener('click', () => {
    if (replyState && input) {
      const showReplyBar = input.closest('.disc-chat-input-area')?.parentElement
        ?.querySelector?.('#channel-reply-bar');
      // Trigger via replyState + custom event so the bar in closure updates
      msgEl.dispatchEvent(new CustomEvent('ch:reply', { bubbles: true, detail: { msg: { ...msg, profiles: profile } } }));
    }
  });

  // Edit button
  msgEl.querySelector('.ch-edit-btn')?.addEventListener('click', () => {
    const textEl = msgEl.querySelector('.disc-msg-text');
    if (!textEl || msgEl.querySelector('.ch-edit-input')) return;
    const original = msg.content;
    textEl.style.display = 'none';
    const editWrap = document.createElement('div');
    editWrap.className = 'ch-edit-wrap';
    editWrap.innerHTML = `
      <textarea class="ch-edit-input" rows="2">${escapeHtml(original)}</textarea>
      <div class="ch-edit-actions">
        <span style="font-size:11px;color:var(--text-muted)">Enter to save · Esc to cancel</span>
        <button class="ch-edit-save disc-msg-action-btn" style="color:var(--emerald)"><i class="fa-solid fa-check"></i> Save</button>
        <button class="ch-edit-cancel disc-msg-action-btn"><i class="fa-solid fa-xmark"></i> Cancel</button>
      </div>`;
    textEl.insertAdjacentElement('afterend', editWrap);
    const ta = editWrap.querySelector('.ch-edit-input');
    ta.focus();
    ta.setSelectionRange(ta.value.length, ta.value.length);

    async function saveEdit() {
      const newText = ta.value.trim();
      if (!newText || newText === original) { cancelEdit(); return; }
      const { error } = await sb.from('op_channel_messages').update({ content: newText }).eq('id', msg.id);
      if (error) { toast('Edit failed: ' + error.message, 'circle-exclamation'); return; }
      msg.content = newText;
      textEl.textContent = newText;
      textEl.style.display = '';
      editWrap.remove();
    }

    function cancelEdit() {
      textEl.style.display = '';
      editWrap.remove();
    }

    editWrap.querySelector('.ch-edit-save').addEventListener('click', saveEdit);
    editWrap.querySelector('.ch-edit-cancel').addEventListener('click', cancelEdit);
    ta.addEventListener('keydown', e => {
      if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); saveEdit(); }
      if (e.key === 'Escape') cancelEdit();
    });
  });

  // Delete button
  msgEl.querySelector('.ch-delete-btn')?.addEventListener('click', async () => {
    if (!confirm('Delete this message?')) return;
    const { error } = await sb.from('op_channel_messages').delete().eq('id', msg.id);
    if (error) { toast('Delete failed: ' + error.message, 'circle-exclamation'); return; }
    msgEl.style.transition = 'opacity 0.2s';
    msgEl.style.opacity = '0';
    setTimeout(() => msgEl.remove(), 200);
  });

  return msgEl;
}

async function renderCommunityMembers(container, communityId) {
  const { data: members } = await sb
    .from('op_community_members')
    .select('user_id, role, profiles:op_profiles!user_id(id, username, display_name, avatar_url)')
    .eq('community_id', communityId)
    .limit(20);

  if (!container) return;
  const online = (members || []).filter(m => State.onlineUsers.has(m.user_id));
  const offline = (members || []).filter(m => !State.onlineUsers.has(m.user_id));

  function memberHtml(m, isOnline) {
    const p = m.profiles;
    const color = avatarColor(p?.display_name || p?.username || '?');
    const name = p?.display_name || p?.username || 'User';
    const avatarContent = p?.avatar_url
      ? `<img src="${escapeHtml(p.avatar_url)}" class="disc-mem-avatar-img" alt="">`
      : `<span>${avatarInitials(name)}</span>`;
    return `
      <div class="disc-member-item${!isOnline ? ' disc-member-item--offline' : ''}">
        <div class="disc-mem-avatar-wrap">
          <div class="disc-mem-avatar" style="background:${color}">${avatarContent}</div>
          <div class="disc-mem-status ${isOnline ? 'online' : 'offline'}"></div>
        </div>
        <div class="disc-mem-info">
          <div class="disc-mem-name">${escapeHtml(name)}</div>
          ${m.role === 'owner' ? `<div class="disc-mem-role"><i class="fa-solid fa-crown" style="font-size:8px;color:var(--amber)"></i> Owner</div>` : ''}
        </div>
      </div>`;
  }

  container.innerHTML = `
    <div class="disc-members-header">Members — ${(members || []).length}</div>
    ${online.length ? `
      <div class="disc-mem-section-label">ONLINE — ${online.length}</div>
      ${online.map(m => memberHtml(m, true)).join('')}
    ` : ''}
    ${offline.length ? `
      <div class="disc-mem-section-label" style="margin-top:16px">OFFLINE — ${offline.length}</div>
      ${offline.slice(0, 10).map(m => memberHtml(m, false)).join('')}
    ` : ''}
  `;
}

/* ── Notifications ──────────────────────────────────────────── */
async function renderNotifications(main) {
  main.innerHTML = `
    <div class="view-tabs">
      <div class="view-tab active" style="cursor:default">All Notifications</div>
      <button class="view-tab" id="mark-all-read" style="margin-left:auto;font-size:12px;color:var(--cyan)">Mark all read</button>
    </div>
    <div class="notif-list" id="notif-list">
      <div style="padding:32px;text-align:center;color:var(--text-muted)">Loading…</div>
    </div>
  `;

  $('#mark-all-read').addEventListener('click', async () => {
    await sb.from('op_notifications').update({ read: true }).eq('user_id', State.user.id);
    State.unreadNotifs = 0;
    updateBadges();
    loadNotifications();
  });

  loadNotifications();
}

async function loadNotifications() {
  const container = $('#notif-list');
  if (!container) return;

  container.innerHTML = `<div style="padding:32px;text-align:center;color:var(--text-muted)">Loading…</div>`;

  // Try full query first
  let notifs = null;
  let queryError = null;

  const { data: d1, error: e1 } = await sb
    .from('op_notifications')
    .select('id, type, read, created_at, post_id, post_title, actor_id')
    .eq('user_id', State.user.id)
    .order('created_at', { ascending: false })
    .limit(40);

  if (e1) {
    console.warn('[Devit] notifications full query failed:', e1.message, e1.code);
    // Fallback: try without actor_id (in case column doesn't exist yet)
    const { data: d2, error: e2 } = await sb
      .from('op_notifications')
      .select('id, type, read, created_at, post_id, post_title')
      .eq('user_id', State.user.id)
      .order('created_at', { ascending: false })
      .limit(40);

    if (e2) {
      console.error('[Devit] notifications fallback query also failed:', e2.message, e2.code, e2.details);
      container.innerHTML = `<div style="padding:40px;text-align:center;color:var(--rose);font-size:13px">
        Could not load notifications<br>
        <span style="color:var(--text-muted);font-size:11px;font-family:monospace">${e2.message}</span>
      </div>`;
      return;
    }
    notifs = d2;
  } else {
    notifs = d1;
  }

  if (!notifs?.length) {
    container.innerHTML = `<div style="padding:40px;text-align:center;color:var(--text-muted)">No notifications yet 🔕</div>`;
    return;
  }

  // Batch-fetch all unique actor profiles in one query
  const actorIds = [...new Set(notifs.map(n => n.actor_id).filter(Boolean))];
  let actorMap = {};
  if (actorIds.length) {
    const { data: actors } = await sb
      .from('op_profiles')
      .select('id, username, display_name, avatar_url')
      .in('id', actorIds);
    (actors || []).forEach(a => { actorMap[a.id] = a; });
  }

  const iconMap = {
    like:         '<i class="fa-solid fa-heart" style="color:var(--rose)"></i>',
    follow:       '<i class="fa-solid fa-user-plus" style="color:var(--violet)"></i>',
    comment:      '<i class="fa-solid fa-comment" style="color:var(--sky)"></i>',
    mention:      '<i class="fa-solid fa-at" style="color:var(--cyan)"></i>',
    reply:        '<i class="fa-solid fa-reply" style="color:var(--emerald)"></i>',
    link_request: '<i class="fa-solid fa-link" style="color:var(--amber)"></i>',
    link_accepted:'<i class="fa-solid fa-handshake" style="color:var(--emerald)"></i>',
    new_post:     '<i class="fa-solid fa-pen-to-square" style="color:var(--cyan)"></i>',
  };
  const textMap = {
    like:         actor => `<strong>${actor}</strong> liked your post`,
    follow:       actor => `<strong>${actor}</strong> started following you`,
    comment:      actor => `<strong>${actor}</strong> commented on your post`,
    mention:      actor => `<strong>${actor}</strong> mentioned you in a post`,
    reply:        actor => `<strong>${actor}</strong> replied to your comment`,
    link_request: actor => `<strong>${actor}</strong> wants to link with you`,
    link_accepted:actor => `<strong>${actor}</strong> accepted your link request`,
    new_post:     actor => `<strong>${actor}</strong> published a new post`,
  };

  container.innerHTML = notifs.map(n => {
    const profile = actorMap[n.actor_id] || null;
    const actor = profile?.display_name || profile?.username || 'Someone';
    const color = avatarColor(actor);
    const icon  = iconMap[n.type] || '<i class="fa-solid fa-bell"></i>';
    const text  = (textMap[n.type] || (() => 'New notification'))(escapeHtml(actor));
    // Show post snippet if available
    const postPreview = n.post_title
      ? `<div style="margin-top:4px;padding:6px 10px;background:var(--bg-elevated);border-radius:8px;font-size:12px;color:var(--text-muted);white-space:nowrap;overflow:hidden;text-overflow:ellipsis;max-width:100%">${escapeHtml(n.post_title)}</div>`
      : '';

    return `<div class="notif-item ${n.read ? '' : 'unread'}" data-nid="${n.id}" data-post-id="${n.post_id || ''}" data-actor-id="${profile?.id || ''}" style="display:flex;align-items:flex-start;gap:12px;padding:14px 16px;cursor:pointer;border-bottom:1px solid var(--border);transition:background 0.15s">
      <div style="position:relative;flex-shrink:0">
        <div class="notif-avatar" style="background:${color};width:40px;height:40px;border-radius:50%;display:flex;align-items:center;justify-content:center;font-weight:700;font-size:14px;color:#fff;overflow:hidden;flex-shrink:0;">${profile?.avatar_url ? `<img src="${escapeHtml(profile.avatar_url)}" style="width:100%;height:100%;object-fit:cover;" onerror="this.parentElement.innerHTML='${avatarInitials(actor)}'">` : avatarInitials(actor)}</div>
        <div class="notif-icon notif-${n.type}" style="position:absolute;bottom:-2px;right:-2px;width:18px;height:18px;border-radius:50%;background:var(--bg-float);border:2px solid var(--bg-surface);display:flex;align-items:center;justify-content:center;font-size:8px;">${icon}</div>
      </div>
      <div style="flex:1;min-width:0">
        <div class="notif-text" style="font-size:13px;line-height:1.5;color:var(--text-secondary)">${text}</div>
        ${postPreview}
        <div class="notif-time" style="font-size:11px;color:var(--text-muted);margin-top:3px">${timeAgo(n.created_at)}</div>
      </div>
      ${!n.read ? '<div style="width:8px;height:8px;border-radius:50%;background:var(--cyan);flex-shrink:0;margin-top:4px"></div>' : ''}
    </div>`;
  }).join('');

  $$('.notif-item', container).forEach(item => {
    item.addEventListener('click', async () => {
      item.classList.remove('unread');
      // Remove the unread dot
      const dot = item.querySelector('div[style*="background:var(--cyan)"]');
      if (dot) dot.remove();
      await sb.from('op_notifications').update({ read: true }).eq('id', item.dataset.nid);
      State.unreadNotifs = Math.max(0, State.unreadNotifs - 1);
      updateBadges();
      // Navigate to post if applicable
      if (item.dataset.postId && item.dataset.postId !== 'null') {
        const { data: post } = await sb.from('op_posts').select('*, profiles:op_profiles!author_id(id,username,display_name,avatar_url)').eq('id', item.dataset.postId).single();
        if (post) openPostThread(post, post.profiles);
      } else if (item.dataset.actorId && item.dataset.actorId !== 'null') {
        // For follows, link requests etc. — go to their profile
        const notifType = item.querySelector('.notif-text')?.textContent;
        if (notifType && (notifType.includes('following') || notifType.includes('link'))) {
          renderProfile($('#main'), item.dataset.actorId);
        }
      }
    });

    // Hover effect
    item.addEventListener('mouseenter', () => { item.style.background = 'var(--bg-elevated)'; });
    item.addEventListener('mouseleave', () => { item.style.background = ''; });
  });
}

/* ── Messages / DMs ─────────────────────────────────────────── */
async function renderMessages(main) {
  main.innerHTML = `
    <div class="messages-layout">
      <div class="conversations-list">
        <div class="conversations-header">
          Messages
          <button id="new-dm-btn" style="color:var(--cyan);font-size:16px;font-weight:700" title="New message"><i class="fa-solid fa-plus"></i></button>
        </div>
        <div id="conversations-container">
          <div style="padding:16px;text-align:center;color:var(--text-muted);font-size:13px">Loading…</div>
        </div>
      </div>
      <div class="dm-view" id="dm-view">
        <div style="flex:1;display:flex;align-items:center;justify-content:center;flex-direction:column;gap:10px;color:var(--text-muted)">
          <div style="font-size:32px;color:var(--text-muted)"><i class="fa-solid fa-message"></i></div>
          <div style="font-size:14px;font-weight:600">Select a conversation</div>
          <div style="font-size:12px">or start a new one</div>
        </div>
      </div>
    </div>
  `;

  $('#new-dm-btn').addEventListener('click', openNewDMModal);
  loadConversations();

  // Subscribe to new messages — reload list but preserve the currently open convo
  const unsubMsgs = realtimeManager.subscribe(
    `view:messages_${State.user.id}`,
    'messages',
    'INSERT',
    () => {
      const activeItem = $('#conversations-container .conversation-item.active');
      loadConversations(activeItem?.dataset.cid || null);
    }
  );
  ViewUnsubFns.push(unsubMsgs);
}

async function loadConversations(preserveActiveConvoId = null) {
  const container = $('#conversations-container');
  if (!container) return;

  // Remember which convo is currently open so we don't hijack it on reload
  const activeItem = container.querySelector('.conversation-item.active');
  const currentlyOpenId = preserveActiveConvoId || activeItem?.dataset.cid || null;

  const { data: convos } = await sb
    .from('op_conversations')
    .select('id, last_message, last_message_at, participant_a, participant_b, hidden_for')
    .or(`participant_a.eq.${State.user.id},participant_b.eq.${State.user.id}`)
    .order('last_message_at', { ascending: false });

  // Filter out conversations the user has hidden
  const visibleConvos = (convos || []).filter(c => {
    const hidden = c.hidden_for || [];
    return !hidden.includes(State.user.id);
  });

  if (!visibleConvos.length) {
    container.innerHTML = `<div style="padding:16px;font-size:13px;color:var(--text-muted)">No conversations yet</div>`;
    return;
  }

  // Fetch other participant profiles
  const otherIds = visibleConvos.map(c => c.participant_a === State.user.id ? c.participant_b : c.participant_a);
  const { data: profiles } = await sb.from('op_profiles').select('id, username, display_name, avatar_url').in('id', otherIds);
  const profileMap = Object.fromEntries((profiles || []).map(p => [p.id, p]));

  container.innerHTML = visibleConvos.map(c => {
    const otherId = c.participant_a === State.user.id ? c.participant_b : c.participant_a;
    const other = profileMap[otherId] || { username: 'Unknown', display_name: 'Unknown' };
    const isOnline = State.onlineUsers.has(otherId);
    const isActive = c.id === currentlyOpenId;
    return `<div class="conversation-item${isActive ? ' active' : ''}" data-cid="${c.id}" data-otherid="${otherId}">
      <div style="position:relative;flex-shrink:0">
        ${avatarHtml(other, 38)}
        ${isOnline ? '<div class="conv-online"></div>' : ''}
      </div>
      <div class="conv-info">
        <div class="conv-name">${escapeHtml(other.display_name || other.username)}<span class="conv-time">${c.last_message_at ? timeAgo(c.last_message_at) : ''}</span></div>
        <div class="conv-preview">${escapeHtml(c.last_message || '')}</div>
      </div>
      <button class="conv-remove-btn" data-cid="${c.id}" title="Remove conversation" aria-label="Remove conversation">
        <i class="fa-solid fa-xmark"></i>
      </button>
    </div>`;
  }).join('');

  $$('.conversation-item', container).forEach(item => {
    item.addEventListener('click', (e) => {
      // Don't open DM if the remove button was clicked
      if (e.target.closest('.conv-remove-btn')) return;
      $$('.conversation-item', container).forEach(i => i.classList.remove('active'));
      item.classList.add('active');
      openDM(item.dataset.cid, item.dataset.otherid, $('#dm-view'));
    });
  });

  // ── Remove / hide conversation ────────────────────────────────
  $$('.conv-remove-btn', container).forEach(btn => {
    btn.addEventListener('click', async (e) => {
      e.stopPropagation();
      const convoId = btn.dataset.cid;
      const item = btn.closest('.conversation-item');

      // Optimistically remove from list
      item.style.transition = 'opacity 0.2s, transform 0.2s';
      item.style.opacity = '0';
      item.style.transform = 'translateX(12px)';
      setTimeout(() => item.remove(), 200);

      // If this was the open convo, clear the dm-view
      if (convoId === currentlyOpenId) {
        const dmView = $('#dm-view');
        if (dmView) dmView.innerHTML = `
          <div style="flex:1;display:flex;align-items:center;justify-content:center;flex-direction:column;gap:10px;color:var(--text-muted)">
            <div style="font-size:32px"><i class="fa-solid fa-message"></i></div>
            <div style="font-size:14px;font-weight:600">Select a conversation</div>
            <div style="font-size:12px">or start a new one</div>
          </div>`;
      }

      // Hide for this user — uses a hidden_for array column.
      // If your DB doesn't have this column yet, add it:
      // ALTER TABLE conversations ADD COLUMN IF NOT EXISTS hidden_for uuid[] DEFAULT '{}';
      const { data: convo } = await sb.from('op_conversations').select('hidden_for').eq('id', convoId).single();
      const existing = convo?.hidden_for || [];
      if (!existing.includes(State.user.id)) {
        await sb.from('op_conversations')
          .update({ hidden_for: [...existing, State.user.id] })
          .eq('id', convoId);
      }
    });
  });

  // Auto-open first only on initial load (no active convo yet)
  if (!currentlyOpenId) {
    const first = $('.conversation-item', container);
    if (first) first.click();
  }
}

// ── DM permission helper ──────────────────────────────────────
// Returns true if the current user is allowed to DM targetId.
// dmPrivacy is the target's dm_privacy value; isFollowing is whether
// the current user already follows the target.
async function canDM(targetId, dmPrivacy, isFollowing) {
  if (targetId === State.user.id) return true; // always DM yourself (notes)
  const setting = dmPrivacy || 'everyone';
  if (setting === 'nobody') return false;
  if (setting === 'everyone') return true;
  // 'followers' — check if current user follows target
  if (isFollowing !== undefined) return !!isFollowing;
  const { data } = await sb.from('op_follows').select('id').eq('follower_id', State.user.id).eq('following_id', targetId).single();
  return !!data;
}

// ── Open DM conversation view ─────────────────────────────────
async function openDM(convoId, otherUserId, container) {
  const { data: other } = await sb.from('op_profiles').select('id, username, display_name, avatar_url').eq('id', otherUserId).single();
  const isOnline = State.onlineUsers.has(otherUserId);
  const color = avatarColor(other?.display_name || other?.username || '?');

  // Mobile: slide the dm-view panel into view
  const isMobile = window.innerWidth <= 640;
  if (isMobile) {
    container.classList.add('dm-view-open');
    document.body.classList.add('dm-open');
  }

  const otherName = other?.display_name || other?.username || 'User';
  const otherHandle = other?.username || '';
  const joinedDate = other?.created_at
    ? new Date(other.created_at).toLocaleDateString('en-US', { month: 'long', year: 'numeric' })
    : null;

  container.innerHTML = `
    <div class="dm-header">
      <button class="dm-back-btn" id="dm-back-btn" aria-label="Back to conversations"><i class="fa-solid fa-arrow-left"></i></button>
      <div style="position:relative">
        ${avatarHtml(other, 36)}
        <div class="conv-online" style="display:${isOnline ? 'block' : 'none'}"></div>
      </div>
      <div>
        <div style="font-weight:700;font-size:14px">${escapeHtml(otherName)}</div>
        <div style="font-size:11px;color:var(--${isOnline ? 'emerald' : 'text-muted'})">${isOnline ? '● Online' : 'Offline'}</div>
      </div>
    </div>
    <div class="dm-messages" id="active-dm-messages"></div>
    <div class="dm-typing-bar" id="dm-typing-bar" style="display:none;padding:4px 16px 0;min-height:20px"></div>
    <div class="chat-input-area">
      <div class="chat-input-wrap">
        <input class="chat-input" id="dm-input" type="text" placeholder="Message ${escapeHtml(otherName)}…">
        <button class="chat-send-btn" id="dm-send-btn">
          <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><line x1="22" y1="2" x2="11" y2="13"/><polygon points="22 2 15 22 11 13 2 9 22 2"/></svg>
        </button>
      </div>
    </div>
  `;

  const msgList = $('#active-dm-messages', container);

  // Load messages
  const { data: messages } = await sb
    .from('op_messages')
    .select('id, content, sender_id, created_at')
    .eq('conversation_id', convoId)
    .order('created_at', { ascending: true });

  // ── Welcome banner (shown only when no messages yet) ─────────
  if (!messages || messages.length === 0) {
    const welcomeEl = document.createElement('div');
    welcomeEl.id = 'dm-welcome-banner';
    welcomeEl.className = 'dm-welcome-banner';
    welcomeEl.innerHTML = `
      <div class="dm-welcome-avatar">
        ${avatarHtml(other, 80)}
      </div>
      <div class="dm-welcome-name">${escapeHtml(otherName)}</div>
      <div class="dm-welcome-handle">@${escapeHtml(otherHandle)}</div>
      ${joinedDate ? `<div class="dm-welcome-meta"><i class="fa-solid fa-calendar-days"></i> Joined ${joinedDate}</div>` : ''}
      <div class="dm-welcome-desc">
        This is the beginning of your direct message history with <strong>@${escapeHtml(otherHandle)}</strong>.
      </div>
    `;
    msgList.appendChild(welcomeEl);
  }

  (messages || []).forEach(m => {
    msgList.appendChild(buildDMMessage(m, other, color));
  });
  msgList.scrollTop = msgList.scrollHeight;

  // Mark unread as read
  await sb.from('op_messages').update({ read: true }).eq('conversation_id', convoId).neq('sender_id', State.user.id);

  // ── Typing indicator helpers ──────────────────────────────────
  const typingBar = document.getElementById('dm-typing-bar');
  let _typingTimeout = null;

  function showTypingIndicator(name) {
    if (!typingBar) return;
    typingBar.style.display = 'flex';
    typingBar.innerHTML = `
      <span class="dm-typing-dots">
        <span></span><span></span><span></span>
      </span>
      <span class="dm-typing-label">${escapeHtml(name)} is typing…</span>
    `;
    clearTimeout(_typingTimeout);
    _typingTimeout = setTimeout(() => {
      if (typingBar) typingBar.style.display = 'none';
    }, 3000);
  }

  function hideTypingIndicator() {
    clearTimeout(_typingTimeout);
    if (typingBar) typingBar.style.display = 'none';
  }

  // ── Supabase Realtime broadcast channel for instant DMs ──
  const realtimeCh = sb.channel(`dm_realtime_${convoId}`, {
    config: { broadcast: { self: false } },
  });

  realtimeCh
    .on('broadcast', { event: 'new_message' }, ({ payload }) => {
      const msg = payload;
      if (!msg || msg.sender_id === State.user.id) return;
      const listEl = document.getElementById('active-dm-messages');
      if (!listEl) return;
      // Avoid duplicate if postgres_changes already added it
      if (msg.id && listEl.querySelector(`[data-msgid="${msg.id}"]`)) return;
      // Remove welcome banner once first message arrives
      listEl.querySelector('#dm-welcome-banner')?.remove();
      hideTypingIndicator();
      listEl.appendChild(buildDMMessage(msg, other, color));
      listEl.scrollTop = listEl.scrollHeight;
      // Mark as read instantly since the chat is open
      sb.from('op_messages').update({ read: true }).eq('id', msg.id);
    })
    .on('broadcast', { event: 'typing' }, ({ payload }) => {
      if (payload?.userId === State.user.id) return;
      showTypingIndicator(otherName);
    })
    .subscribe();

  const unsubBroadcast = realtimeManager.subscribeRaw(`view:dm_realtime_${convoId}`, realtimeCh, () => {});
  ViewUnsubFns.push(unsubBroadcast);

  // Also keep a postgres_changes sub as fallback (for messages sent from other devices)
  const unsubPg = realtimeManager.subscribe(
    `view:dm_pg_${convoId}`,
    'messages',
    'INSERT',
    payload => {
      const msg = payload.new;
      if (msg.sender_id === State.user.id) return;
      const listEl = document.getElementById('active-dm-messages');
      if (!listEl) return;
      // Avoid duplicate if realtime broadcast already added it
      if (listEl.querySelector(`[data-msgid="${msg.id}"]`)) return;
      listEl.appendChild(buildDMMessage(msg, other, color));
      listEl.scrollTop = listEl.scrollHeight;
    },
    `conversation_id=eq.${convoId}`
  );
  ViewUnsubFns.push(unsubPg);

  // Send
  async function sendDM() {
    const inputEl = document.getElementById('dm-input');
    if (!inputEl) return;
    const text = inputEl.value.trim();
    if (!text) return;
    inputEl.value = '';

    const now = new Date().toISOString();
    const { data: msg, error } = await sb.from('op_messages').insert({
      conversation_id: convoId,
      sender_id: State.user.id,
      content: text,
    }).select().single();

    if (error) {
      toast('Failed to send message', 'circle-exclamation');
      inputEl.value = text; // restore
      return;
    }

    if (msg) {
      const listEl = document.getElementById('active-dm-messages');
      if (listEl) {
        // Remove welcome banner on first message sent
        listEl.querySelector('#dm-welcome-banner')?.remove();
        const msgEl = buildDMMessage(msg, other, color, true);
        msgEl.dataset.msgid = msg.id;
        listEl.appendChild(msgEl);
        listEl.scrollTop = listEl.scrollHeight;
      }
      // Broadcast to the other participant via realtime
      await realtimeCh.send({
        type: 'broadcast',
        event: 'new_message',
        payload: { ...msg },
      });
      // Update convo preview
      await sb.from('op_conversations').update({ last_message: text, last_message_at: msg.created_at }).eq('id', convoId);
    }
  }

  const sendBtn = document.getElementById('dm-send-btn');
  const dmInputEl = document.getElementById('dm-input');

  // ── Broadcast "typing" event with debounce ────────────────────
  let _typingBroadcastTimeout = null;
  if (dmInputEl) {
    dmInputEl.addEventListener('input', () => {
      if (_typingBroadcastTimeout) return; // already throttled
      realtimeCh.send({
        type: 'broadcast',
        event: 'typing',
        payload: { userId: State.user.id },
      }).catch(() => {});
      _typingBroadcastTimeout = setTimeout(() => {
        _typingBroadcastTimeout = null;
      }, 2000); // throttle to once every 2s
    });
  }

  if (sendBtn) sendBtn.addEventListener('click', (e) => { e.preventDefault(); sendDM(); });
  if (dmInputEl) dmInputEl.addEventListener('keydown', e => { if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); sendDM(); } });

  // Mobile back button
  const backBtn = $('#dm-back-btn', container);
  if (backBtn) {
    backBtn.addEventListener('click', () => {
      container.classList.remove('dm-view-open');
      document.body.classList.remove('dm-open');
    });
  }
}

function buildDMMessage(msg, other, color, isSelf = null) {
  const isOwn = isSelf !== null ? isSelf : msg.sender_id === State.user.id;
  const msgEl = el('div', `msg ${isOwn ? 'dm-own' : 'dm-other'}`);
  if (msg.id) msgEl.dataset.msgid = msg.id;
  if (isOwn) {
    msgEl.innerHTML = `<div class="msg-body"><div class="msg-text dm-own-text">${escapeHtml(msg.content)}</div><div style="font-size:10px;color:var(--text-muted);text-align:right;margin-top:2px">${timeAgo(msg.created_at)}</div></div>`;
  } else {
    msgEl.innerHTML = `
      <div class="msg-avatar" style="background:${color}">${avatarInitials(other?.display_name || other?.username || '?')}</div>
      <div class="msg-body">
        <div class="msg-text" style="background:var(--bg-elevated);padding:8px 12px;border-radius:16px 16px 16px 4px">${escapeHtml(msg.content)}</div>
        <div style="font-size:10px;color:var(--text-muted);margin-top:2px">${timeAgo(msg.created_at)}</div>
      </div>
    `;
  }
  return msgEl;
}

function openNewDMModal() {
  const modal = $('#modal-overlay');
  const body  = $('#modal-body');
  $('#modal-title-text').textContent = 'New Message';
  modal.classList.add('open');

  body.innerHTML = `
    <div style="padding:16px">
      <input type="text" id="dm-search-input" class="auth-input" placeholder="Search for a user…">
      <div id="dm-search-results" style="margin-top:12px"></div>
    </div>
  `;

  let searchTimeout;
  $('#dm-search-input').addEventListener('input', async e => {
    clearTimeout(searchTimeout);
    const q = e.target.value.trim();
    if (q.length < 1) return;
    searchTimeout = setTimeout(async () => {
      const { data: people } = await sb.from('op_profiles').select('id, username, display_name, avatar_url').or(`username.ilike.%${q}%,display_name.ilike.%${q}%`).neq('id', State.user.id).limit(6);
      const results = $('#dm-search-results');
      if (!results) return;
      results.innerHTML = (people || []).map(p => `
        <div class="search-result-item dm-search-person" data-uid="${p.id}" style="cursor:pointer;display:flex;align-items:center;gap:10px;padding:10px;border-radius:8px">
          ${avatarHtml(p, 34)}
          <div><div style="font-weight:600;font-size:13px">${escapeHtml(p.display_name || p.username)}</div><div style="font-size:12px;color:var(--text-muted)">@${escapeHtml(p.username)}</div></div>
        </div>
      `).join('');
      $$('[data-uid]', results).forEach(item => {
        item.addEventListener('click', async () => {
          const uid = item.dataset.uid;
          // Get or create conversation
          const a = State.user.id < uid ? State.user.id : uid;
          const b = State.user.id < uid ? uid : State.user.id;
          let { data: convo } = await sb.from('op_conversations').select('id').eq('participant_a', a).eq('participant_b', b).single();
          if (!convo) {
            const { data: newConvo } = await sb.from('op_conversations').insert({ participant_a: a, participant_b: b }).select().single();
            convo = newConvo;
          }
          modal.classList.remove('open');
          navigateTo('messages');
          setTimeout(() => {
            const item2 = $(`[data-cid="${convo.id}"]`);
            if (item2) item2.click();
            else openDM(convo.id, uid, $('#dm-view'));
          }, 200);
        });
      });
    }, 300);
  });
}

/* ── Profile ────────────────────────────────────────────────── */
async function renderProfile(main, userId = null) {
  const targetId = userId || State.user.id;
  const isOwn = targetId === State.user.id;

  const { data: profile } = await sb.from('op_profiles').select('*').eq('id', targetId).single();
  if (!profile) { main.innerHTML = `<div style="padding:40px;text-align:center;color:var(--text-muted)">Profile not found</div>`; return; }

  // ── Private profile gate ──────────────────────────────────────
  if (!isOwn && profile.profile_visibility === 'private') {
    const isFollowing_check = !!(await sb.from('op_follows').select('id').eq('follower_id', State.user.id).eq('following_id', targetId).single()).data;
    if (!isFollowing_check) {
      const color2 = avatarColor(profile.display_name || profile.username || '?');
      main.innerHTML = `
        <div class="profile-cover" style="${profile.banner_url ? `background-image:url('${escapeHtml(profile.banner_url)}');background-size:cover;background-position:center` : ''}">
          <div class="profile-cover-art" style="background:${profile.banner_url ? 'rgba(0,0,0,0.3)' : `linear-gradient(135deg,${color2}22,var(--bg-void))`}"></div>
        </div>
        <div class="profile-info-section">
          <div class="profile-avatar-center-wrap">
            <div class="profile-avatar-wrap">
              <div class="profile-avatar" style="background:${color2};font-size:32px;font-weight:800;color:white;display:flex;align-items:center;justify-content:center;filter:blur(4px)">
                ${profile.avatar_url ? `<img src="${escapeHtml(profile.avatar_url)}" style="width:100%;height:100%;object-fit:cover;border-radius:50%">` : avatarInitials(profile.display_name || profile.username || 'U')}
              </div>
            </div>
          </div>
          <div class="profile-identity">
            <div class="profile-name">${escapeHtml(profile.display_name || profile.username || 'Unknown')}</div>
            <div class="profile-handle">@${escapeHtml(profile.username || '')}</div>
          </div>
          <div style="text-align:center;padding:32px 20px;color:var(--text-muted)">
            <i class="fa-solid fa-lock" style="font-size:32px;margin-bottom:12px;display:block;color:var(--border-active)"></i>
            <div style="font-size:15px;font-weight:700;color:var(--text-primary);margin-bottom:6px">This profile is private</div>
            <div style="font-size:13px;margin-bottom:20px">Follow this account to see their posts and activity.</div>
            <button class="profile-action-btn primary" id="private-follow-btn">Follow</button>
          </div>
        </div>
      `;
      document.getElementById('private-follow-btn')?.addEventListener('click', async () => {
        const btn = document.getElementById('private-follow-btn');
        btn.disabled = true; btn.textContent = 'Following…';
        const { error } = await sb.from('op_follows').insert({ follower_id: State.user.id, following_id: targetId });
        if (!error || error.code === '23505') {
          await incrementFollowCounts(targetId, State.user.id);
          await sb.from('op_notifications').insert({ user_id: targetId, actor_id: State.user.id, type: 'follow' });
          btn.textContent = 'Following ✓';
          toast('Followed!', 'user-check');
        } else {
          btn.disabled = false; btn.textContent = 'Follow';
        }
      });
      return;
    }
  }

  const isFollowing = !isOwn ? !!(await sb.from('op_follows').select('id').eq('follower_id', State.user.id).eq('following_id', targetId).single()).data : false;
  const color = avatarColor(profile.display_name || profile.username || '?');

  const safeName     = escapeHtml(profile.display_name || profile.username || 'Unknown');
  const safeHandle   = escapeHtml(profile.username || '');
  const safeBio      = escapeHtml(profile.bio || '');
  const safeLocation = escapeHtml(profile.location || '');
  const safeWebsite  = escapeHtml(profile.website || '');

  main.innerHTML = `
    <div class="profile-cover" style="${profile.banner_url ? `background-image:url('${escapeHtml(profile.banner_url)}');background-size:cover;background-position:center` : ''}">
      <div class="profile-cover-art" style="background:${profile.banner_url ? 'rgba(0,0,0,0.3)' : `linear-gradient(135deg,${color}22,var(--bg-void))`};${profile.banner_color && !profile.banner_url ? `background:${profile.banner_color}` : ''}"></div>
    </div>
    <div class="profile-info-section">
      <div class="profile-avatar-center-wrap">
        <div class="profile-avatar-wrap">
          <div class="profile-avatar" style="background:${color};font-size:32px;font-weight:800;color:white;display:flex;align-items:center;justify-content:center">
            ${profile.avatar_url ? `<img src="${escapeHtml(profile.avatar_url)}" style="width:100%;height:100%;object-fit:cover;border-radius:50%">` : avatarInitials(profile.display_name || profile.username || 'U')}
          </div>
          <div class="profile-online-dot ${State.onlineUsers.has(targetId) ? 'online' : ''}" data-presence-uid="${targetId}"></div>
        </div>
      </div>
      <div class="profile-identity">
        <div class="profile-name">${safeName}</div>
        <div class="profile-handle">@${safeHandle} ${State.onlineUsers.has(targetId) ? '<span style="color:var(--emerald);font-size:12px">● Online</span>' : ''}</div>
      </div>
      <div class="profile-actions-center">
        ${isOwn
          ? `<button class="profile-action-btn secondary" id="edit-profile-btn">Edit Profile</button>`
          : `<button class="profile-action-btn ${isFollowing ? 'secondary' : 'primary'}" id="follow-profile-btn">${isFollowing ? 'Unfollow' : 'Follow'}</button>
             <button class="profile-action-btn secondary" id="dm-profile-btn">Message</button>`
        }
        <button class="profile-action-btn secondary" id="share-profile-btn"><i class="fa-solid fa-link"></i></button>
      </div>
      <div class="profile-bio">${safeBio}</div>
      <div class="profile-meta">
        ${safeLocation ? `<div class="profile-meta-item">📍 <span>${safeLocation}</span></div>` : ''}
        ${safeWebsite ? `<div class="profile-meta-item"><i class="fa-solid fa-link"></i> <a href="${safeWebsite}" target="_blank" rel="noopener noreferrer" style="color:var(--cyan)">${safeWebsite}</a></div>` : ''}
        <div class="profile-meta-item">📅 <span>Joined ${new Date(profile.created_at).toLocaleDateString('en-US', {month:'short',year:'numeric'})}</span></div>
      </div>
      <div class="profile-stats">
        <div class="profile-stat"><strong>${fmtNum(profile.following_count || 0)}</strong> <span>Following</span></div>
        <div class="profile-stat"><strong>${fmtNum(profile.followers_count || 0)}</strong> <span>Followers</span></div>
        <div class="profile-stat"><strong>${fmtNum(profile.posts_count || 0)}</strong> <span>Posts</span></div>
      </div>

      ${(() => {
        const badges = computeBadges(profile);
        return badges.length ? `
          <div class="profile-badges-label"><i class="fa-solid fa-trophy"></i> Badges</div>
          <div class="profile-badges">${badges.map(b => `<span class="profile-badge badge-${b.tier}" title="${escapeHtml(b.desc)}"><span class="badge-icon">${b.icon}</span>${escapeHtml(b.label)}</span>`).join('')}</div>
        ` : '';
      })()}
    </div>
    <div class="profile-tabs">
      <div class="profile-tab-list">
        ${['Posts'].map((t,i) => `<div class="profile-tab ${i===0?'active':''}" data-ptab="${t}">${t}</div>`).join('')}
      </div>
    </div>
    <div id="profile-content"></div>
  `;

  // Follow button
  const followBtn = $('#follow-profile-btn', main);
  if (followBtn) {
    let followState = isFollowing;
    followBtn.addEventListener('click', async () => {
      followBtn.disabled = true;
      if (followState) {
        const { error } = await sb.from('op_follows').delete().eq('follower_id', State.user.id).eq('following_id', targetId);
        if (!error) {
          followState = false;
          followBtn.textContent = 'Follow';
          followBtn.className = 'profile-action-btn primary';
          toast('Unfollowed', 'user-minus');
          await decrementFollowCounts(targetId, State.user.id);
          await _refreshFollowCountsInDOM(main, targetId);
        }
      } else {
        const { error } = await sb.from('op_follows').insert({ follower_id: State.user.id, following_id: targetId });
        if (!error || error.code === '23505') {
          followState = true;
          followBtn.textContent = 'Unfollow';
          followBtn.className = 'profile-action-btn secondary';
          toast('Followed!', 'user-check');
          await incrementFollowCounts(targetId, State.user.id);
          await sb.from('op_notifications').insert({ user_id: targetId, actor_id: State.user.id, type: 'follow' });
          await _refreshFollowCountsInDOM(main, targetId);
        } else {
          toast('Something went wrong', 'circle-exclamation');
        }
      }
      followBtn.disabled = false;
    });
  }

  // DM button
  const dmBtn = $('#dm-profile-btn', main);
  if (dmBtn) {
    // Check if target allows DMs from this user
    const dmAllowed = await canDM(targetId, profile.dm_privacy, isFollowing);
    if (!dmAllowed) {
      dmBtn.disabled = true;
      dmBtn.title = profile.dm_privacy === 'nobody' ? "This user doesn't accept DMs" : "You must follow this user to DM them";
      dmBtn.style.opacity = '0.45';
      dmBtn.style.cursor = 'not-allowed';
    } else {
      dmBtn.addEventListener('click', async () => {
        const a = State.user.id < targetId ? State.user.id : targetId;
        const b = State.user.id < targetId ? targetId : State.user.id;
        let { data: convo } = await sb.from('op_conversations').select('id').eq('participant_a', a).eq('participant_b', b).single();
        if (!convo) {
          const { data: newConvo } = await sb.from('op_conversations').insert({ participant_a: a, participant_b: b }).select().single();
          convo = newConvo;
        }
        navigateTo('messages');
        setTimeout(() => {
          openDM(convo.id, targetId, $('#dm-view'));
        }, 300);
      });
    }
  }

  // Share profile button → opens invite modal
  const shareProfileBtn = $('#share-profile-btn', main);
  if (shareProfileBtn) {
    shareProfileBtn.addEventListener('click', () => {
      openShareInviteModal(profile);
    });
  }

  // Edit profile button
  const editBtn = $('#edit-profile-btn', main);
  if (editBtn) editBtn.addEventListener('click', () => navigateTo('edit-profile'));

  // Profile tabs
  $$('.profile-tab', main).forEach(tab => {
    tab.addEventListener('click', () => {
      $$('.profile-tab', main).forEach(t => t.classList.remove('active'));
      tab.classList.add('active');
      const content = $('#profile-content');
      if (tab.dataset.ptab === 'Posts') loadProfilePosts(content, targetId);

      else content.innerHTML = `<div style="padding:40px;text-align:center;color:var(--text-muted)">Coming soon 🔜</div>`;
    });
  });

  loadProfilePosts($('#profile-content'), targetId);

  // Tech stack chips → tag feed
  $$('.tech-badge[data-tag]', main).forEach(chip => {
    chip.addEventListener('click', () => navigateToTag(chip.dataset.tag));
  });

  // Stack endorsements — load counts then wire up click handlers
  const techStackEl = $('#profile-tech-stack', main);
  if (techStackEl && profile.tech_stack?.length) {
    const skills = profile.tech_stack;
    // Load endorsement counts
    sb.from('op_stack_endorsements')
      .select('skill')
      .eq('profile_id', targetId)
      .then(({ data: endorsements }) => {
        const counts = {};
        (endorsements || []).forEach(e => { counts[e.skill] = (counts[e.skill] || 0) + 1; });
        skills.forEach(skill => {
          const countEl = techStackEl.querySelector(`.endorse-count[data-skill="${CSS.escape(skill)}"]`);
          if (countEl && counts[skill]) countEl.textContent = ` +${counts[skill]}`;
        });
      });

    // Endorsement click (only on other people's profiles)
    if (targetId !== State.user.id) {
      $$('.tech-badge-endorsable', techStackEl).forEach(badge => {
        badge.style.cursor = 'pointer';
        badge.addEventListener('click', async e => {
          e.stopPropagation();
          const skill = badge.dataset.skill;
          const { error } = await sb.from('op_stack_endorsements').upsert(
            { profile_id: targetId, endorser_id: State.user.id, skill },
            { onConflict: 'profile_id,endorser_id,skill' }
          );
          if (!error) {
            badge.classList.add('endorsed');
            const countEl = badge.querySelector('.endorse-count');
            if (countEl) {
              const n = parseInt((countEl.textContent || '+0').replace(/\D/g,'')) || 0;
              countEl.textContent = ` +${n + 1}`;
            }
            toast(`Endorsed ${skill}!`, 'thumbs-up');
          }
        });
      });
    }
  }
}

async function loadProfilePosts(container, userId) {
  if (!container) return;
  container.innerHTML = `<div style="padding:32px;text-align:center;color:var(--text-muted)">Loading…</div>`;
  const { data: posts } = await sb
    .from('op_posts')
    .select('id, content, code_block, code_lang, image_url, file_url, file_name, likes_count, comments_count, reposts_count, created_at, author_id, profiles:op_profiles!author_id(id, username, display_name, avatar_url)')
    .eq('author_id', userId)
    .order('created_at', { ascending: false });

  if (!posts?.length) { container.innerHTML = `<div style="padding:40px;text-align:center;color:var(--text-muted)">No posts yet</div>`; return; }

  const postIds = posts.map(p => p.id);
  const { data: likes } = await sb.from('op_post_likes').select('post_id').eq('user_id', State.user.id).in('post_id', postIds);
  const likedIds = new Set((likes || []).map(l => l.post_id));

  container.innerHTML = '';
  posts.forEach(p => container.appendChild(buildPostCard(p, p.profiles, likedIds.has(p.id), false)));
}

/* ── Profile Repos ──────────────────────────────────────────── */
async function loadProfileRepos(container, profile) {
  if (!container) return;

  // Use cached repos from tech_stack if no GitHub token available
  const username = profile?.username;
  const isGitHub = profile?.is_github;

  container.innerHTML = `<div style="padding:32px;text-align:center;color:var(--text-muted)">Loading repos…</div>`;

  // Try fetching from GitHub public API (works without a token for public repos)
  let repos = [];
  if (isGitHub && username) {
    try {
      const res = await fetch(`https://api.github.com/users/${encodeURIComponent(username)}/repos?sort=updated&per_page=12&type=owner`);
      if (res.ok) {
        const data = await res.json();
        repos = data.sort((a, b) => (b.stargazers_count - a.stargazers_count));
      }
    } catch (e) { /* fall through to tech_stack fallback */ }
  }

  // Fallback: show tech_stack chips as "pinned repos" if no GH data
  if (!repos.length) {
    const techStack = profile?.tech_stack || [];
    if (!techStack.length) {
      container.innerHTML = `
        <div style="padding:48px;text-align:center;color:var(--text-muted)">
          <div style="font-size:36px;margin-bottom:12px">📦</div>
          <div style="font-size:14px;font-weight:600">No repos linked</div>
          <div style="font-size:12px;margin-top:6px">${isGitHub ? 'No public repos found on GitHub.' : 'Connect with GitHub to show your repositories here.'}</div>
        </div>`;
      return;
    }
    // Render tech_stack as repo chips
    container.innerHTML = `
      <div style="padding:16px">
        <div style="font-size:11px;font-weight:700;color:var(--text-muted);text-transform:uppercase;letter-spacing:0.08em;margin-bottom:12px">
          <i class="fa-solid fa-code-branch" style="color:var(--cyan);margin-right:6px"></i>Tech Stack
        </div>
        <div style="display:flex;flex-wrap:wrap;gap:8px">
          ${techStack.map(t => `
            <span style="display:inline-flex;align-items:center;gap:6px;padding:6px 12px;border-radius:20px;background:rgba(255,45,110,0.07);border:1px solid rgba(255,45,110,0.15);font-size:13px;font-weight:600;color:var(--text-secondary)">
              <i class="fa-solid fa-code" style="font-size:11px;color:var(--cyan)"></i>${escapeHtml(t)}
            </span>`).join('')}
        </div>
      </div>`;
    return;
  }

  // Render full GitHub repo cards
  container.innerHTML = `
    <div style="padding:16px;display:grid;grid-template-columns:repeat(auto-fill,minmax(260px,1fr));gap:12px">
      ${repos.map(r => `
        <a href="${escapeHtml(r.html_url)}" target="_blank" rel="noopener noreferrer" style="text-decoration:none">
          <div style="
            background:var(--bg-surface);border:1px solid var(--border);border-radius:14px;
            padding:14px 16px;transition:border-color 0.2s,transform 0.2s;cursor:pointer;
            display:flex;flex-direction:column;gap:8px;min-height:110px;
          " onmouseenter="this.style.borderColor='rgba(255,45,110,0.35)';this.style.transform='translateY(-2px)'"
             onmouseleave="this.style.borderColor='var(--border)';this.style.transform=''">
            <div style="display:flex;align-items:center;gap:8px">
              <i class="fa-solid fa-code-branch" style="color:var(--cyan);font-size:13px"></i>
              <span style="font-size:14px;font-weight:700;color:var(--cyan);overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${escapeHtml(r.name)}</span>
              ${r.private ? '<span style="font-size:9px;padding:2px 6px;background:rgba(251,191,36,0.1);border:1px solid rgba(251,191,36,0.2);border-radius:4px;color:var(--amber);font-weight:700">PRIVATE</span>' : ''}
            </div>
            ${r.description ? `<div style="font-size:12px;color:var(--text-secondary);line-height:1.4;display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;overflow:hidden">${escapeHtml(r.description)}</div>` : ''}
            <div style="display:flex;align-items:center;gap:12px;margin-top:auto;font-size:11px;color:var(--text-muted)">
              ${r.language ? `<span style="display:flex;align-items:center;gap:4px"><span style="width:8px;height:8px;border-radius:50%;background:var(--cyan);display:inline-block"></span>${escapeHtml(r.language)}</span>` : ''}
              <span><i class="fa-solid fa-star" style="color:var(--amber);font-size:10px"></i> ${r.stargazers_count || 0}</span>
              <span><i class="fa-solid fa-code-fork" style="font-size:10px"></i> ${r.forks_count || 0}</span>
              <span style="margin-left:auto">${timeAgo(r.updated_at)}</span>
            </div>
          </div>
        </a>
      `).join('')}
    </div>
    <div style="padding:8px 16px 16px;text-align:center">
      <a href="https://github.com/${escapeHtml(username)}" target="_blank" rel="noopener noreferrer"
         style="font-size:12px;color:var(--cyan);text-decoration:none;display:inline-flex;align-items:center;gap:6px">
        <i class="fa-brands fa-github"></i> View all on GitHub
      </a>
    </div>`;
}

/* ── Bookmarks ──────────────────────────────────────────────── */

/* ── Leaderboard ─────────────────────────────────────────────── */
async function renderLeaderboard(main) {
  const TABS = [
    { id: 'posts',    label: 'Top Posters',    icon: 'fa-solid fa-file-code',   col: 'posts_count',    unit: 'posts' },
    { id: 'likes',    label: 'Most Liked',     icon: 'fa-solid fa-heart',       col: 'likes_count',    unit: 'likes' },
    { id: 'followers',label: 'Most Followed',  icon: 'fa-solid fa-users',       col: 'followers_count',unit: 'followers' },
  ];
  let activeTab = 'posts';

  main.innerHTML = `
    <div style="padding: 24px 24px 0">
      <div class="leaderboard-page-title"><i class="fa-solid fa-trophy" style="margin-right:10px;font-size:22px"></i>Leaderboard</div>
      <div class="leaderboard-page-sub">Top developers ranked by activity</div>
      <div class="leaderboard-page-tabs" id="lb-tabs" style="margin-top:16px">
        ${TABS.map(t => `<button class="leaderboard-page-tab${t.id === activeTab ? ' active' : ''}" data-tab="${t.id}">
          <i class="${t.icon}"></i>${t.label}
        </button>`).join('')}
      </div>
    </div>
    <div id="lb-content" style="padding:0 24px 32px">
      <div style="padding:32px;text-align:center;color:var(--text-muted)"><i class="fa-solid fa-spinner fa-spin" style="margin-right:8px"></i>Loading…</div>
    </div>
  `;

  async function loadTab(tabId) {
    const tab = TABS.find(t => t.id === tabId);
    const lbContent = document.getElementById('lb-content');
    lbContent.innerHTML = `<div style="padding:32px;text-align:center;color:var(--text-muted)"><i class="fa-solid fa-spinner fa-spin" style="margin-right:8px"></i>Loading…</div>`;

    try {
      const { data: users, error } = await sb
        .from('op_profiles')
        .select('id, username, display_name, avatar_url, ' + tab.col)
        .order(tab.col, { ascending: false })
        .limit(50);

      if (error || !users?.length) {
        lbContent.innerHTML = `<div style="padding:40px;text-align:center;color:var(--text-muted);font-size:14px"><i class="fa-solid fa-ghost" style="display:block;font-size:32px;margin-bottom:12px;opacity:0.4"></i>No data yet</div>`;
        return;
      }

      const top3 = users.slice(0, 3);
      const rest = users.slice(3);

      const rankColors = ['#facc15','#94a3b8','#cd7c2f'];
      const rankEmoji  = ['👑','🥈','🥉'];

      function avatarHtml(u, size = 42) {
        const initials = (u.display_name || u.username || '?').slice(0, 2).toUpperCase();
        const hue = ((u.username || '').charCodeAt(0) * 37) % 360;
        if (u.avatar_url) {
          return `<img src="${u.avatar_url}" alt="" loading="lazy" style="width:${size}px;height:${size}px;border-radius:50%;object-fit:cover">`;
        }
        return `<div class="leaderboard-full-avatar" style="width:${size}px;height:${size}px;background:hsl(${hue},60%,35%);color:#fff">${initials}</div>`;
      }

      // Podium (top 3)
      let podiumHtml = `<div class="leaderboard-podium">`;
      [top3[1], top3[0], top3[2]].forEach((u, i) => {
        if (!u) return;
        const realIdx = i === 0 ? 1 : i === 1 ? 0 : 2;
        const slotClass = ['second','first','third'][i];
        const barH = [40, 60, 28][i];
        const score = u[tab.col] ?? 0;
        podiumHtml += `
          <div class="podium-slot ${slotClass}" data-userid="${u.id}" style="cursor:pointer">
            ${realIdx === 0 ? '<div class="podium-crown">👑</div>' : ''}
            ${avatarHtml(u, realIdx === 0 ? 64 : 52)}
            <div class="podium-name" style="color:${rankColors[realIdx]}">${u.display_name || u.username}</div>
            <div class="podium-score">${score.toLocaleString()} ${tab.unit}</div>
            <div class="podium-bar" style="height:${barH}px"></div>
          </div>`;
      });
      podiumHtml += `</div>`;

      // Full ranked list
      let listHtml = `<div class="leaderboard-full-list">`;
      users.forEach((u, i) => {
        const rank = i + 1;
        const score = u[tab.col] ?? 0;
        const isTop3 = rank <= 3;
        const rankStr = isTop3 ? rankEmoji[rank - 1] : `#${rank}`;
        const hue = ((u.username || '').charCodeAt(0) * 37) % 360;
        const avatarInner = u.avatar_url
          ? `<img src="${u.avatar_url}" alt="" loading="lazy" style="width:42px;height:42px;border-radius:50%;object-fit:cover">`
          : `<div class="leaderboard-full-avatar" style="background:hsl(${hue},60%,35%);color:#fff">${(u.display_name || u.username || '?').slice(0,2).toUpperCase()}</div>`;

        listHtml += `
          <div class="leaderboard-full-row${isTop3 ? ' top-three' : ''}${rank === 1 ? ' rank-pos-1' : ''}" data-userid="${u.id}" style="cursor:pointer">
            <div class="leaderboard-rank-big" style="${isTop3 ? 'color:' + rankColors[rank-1] : 'color:var(--text-muted)'}">${rankStr}</div>
            ${avatarInner}
            <div class="leaderboard-full-info">
              <div class="leaderboard-full-name">${u.display_name || u.username}</div>
              <div class="leaderboard-full-handle">@${u.username}</div>
            </div>
            <div>
              <div class="leaderboard-full-score">${score.toLocaleString()}</div>
              <div class="leaderboard-full-score-label">${tab.unit}</div>
            </div>
          </div>`;
      });
      listHtml += `</div>`;

      lbContent.innerHTML = podiumHtml + listHtml;

      // Click to view profile
      lbContent.querySelectorAll('[data-userid]').forEach(el => {
        el.addEventListener('click', () => renderProfile($('#main'), el.dataset.userid));
      });

    } catch (err) {
      lbContent.innerHTML = `<div style="padding:32px;text-align:center;color:var(--text-muted)">Failed to load leaderboard</div>`;
    }
  }

  // Tab switching
  document.getElementById('lb-tabs')?.querySelectorAll('.leaderboard-page-tab').forEach(btn => {
    btn.addEventListener('click', () => {
      document.querySelectorAll('.leaderboard-page-tab').forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      activeTab = btn.dataset.tab;
      loadTab(activeTab);
    });
  });

  loadTab(activeTab);
}

async function renderBookmarks(main) {
  main.innerHTML = `
    <div class="view-tabs"><div class="view-tab active" style="cursor:default">Bookmarks</div></div>
    <div id="bookmarks-list"><div style="padding:32px;text-align:center;color:var(--text-muted)">Loading…</div></div>
  `;

  const { data: bookmarks } = await sb
    .from('op_bookmarks')
    .select('post_id, posts(id, content, code_block, code_lang, image_url, file_url, file_name, likes_count, comments_count, created_at, profiles:op_profiles!author_id(id, username, display_name, avatar_url))')
    .eq('user_id', State.user.id)
    .order('created_at', { ascending: false });

  const container = $('#bookmarks-list');
  if (!bookmarks?.length) {
    container.innerHTML = `<div style="padding:40px;text-align:center;color:var(--text-muted)">No bookmarks yet — save posts for later! 🔖</div>`;
    return;
  }

  container.innerHTML = '';
  bookmarks.forEach(b => {
    if (b.posts) container.appendChild(buildPostCard(b.posts, b.posts.profiles, false, true));
  });
}

/* ── Settings ───────────────────────────────────────────────── */
function renderSettings(main) {
  main.style.overflowY = 'auto';
  main.style.webkitOverflowScrolling = 'touch';
  main.innerHTML = `
    <div class="settings-shell">
      <h2 class="settings-title">
        <i class="fa-solid fa-gear" style="color:var(--cyan);margin-right:10px"></i>Settings
      </h2>

      <div class="settings-card">
        <div class="settings-card-header">Account</div>
        <div class="settings-row" id="settings-edit-profile">
          <div style="display:flex;align-items:center;gap:10px">
            <i class="fa-solid fa-user-pen" style="color:var(--cyan);width:18px;text-align:center"></i>
            <span>Edit Profile</span>
          </div>
          <i class="fa-solid fa-chevron-right" style="color:var(--text-muted);font-size:12px"></i>
        </div>
      </div>

      <div class="settings-card">
        <div class="settings-card-header" style="color:var(--rose)">Danger Zone</div>
        <div class="settings-row" id="settings-signout" style="color:var(--rose)">
          <div style="display:flex;align-items:center;gap:10px">
            <i class="fa-solid fa-right-from-bracket" style="width:18px;text-align:center"></i>
            <span>Sign Out</span>
          </div>
          <i class="fa-solid fa-chevron-right" style="font-size:12px"></i>
        </div>
      </div>

      <div class="settings-footer">Devit v1.0 · Built with Supabase ⚡</div>
    </div>
  `;

  $('#settings-edit-profile').addEventListener('click', () => navigateTo('edit-profile'));
  $('#settings-signout').addEventListener('click', async () => {
    await sb.from('op_presence').update({ online: false }).eq('id', State.user.id);
    await sb.auth.signOut();
  });
}

/* ── Edit Profile (full page) ───────────────────────────────── */
function renderEditProfile(main) {
  const profile = State.profile || {};
  const BANNER_COLORS = ['#0d1b2e', '#1a0d2e', '#0d2e1a', '#2e1a0d', '#1a1a2e', '#2e0d1a', '#0d2e2e'];

  main.style.overflowY = 'auto';
  main.style.webkitOverflowScrolling = 'touch';
  main.innerHTML = `
    <div class="settings-shell">
      <div style="display:flex;align-items:center;gap:12px;margin-bottom:20px">
        <button id="ep-back-btn" style="background:none;border:none;color:var(--text-secondary);cursor:pointer;font-size:18px;padding:4px 8px 4px 0"><i class="fa-solid fa-arrow-left"></i></button>
        <h2 class="settings-title" style="margin:0">Edit Profile</h2>
      </div>

      <div class="settings-card" style="padding:16px;display:flex;flex-direction:column;gap:14px">

        <!-- Banner -->
        <div>
          <label style="font-size:11px;font-weight:700;color:var(--text-secondary);text-transform:uppercase;letter-spacing:0.05em;margin-bottom:6px;display:block">Profile Banner</label>
          <div id="ep-banner-preview" style="height:90px;border-radius:12px;background:${profile.banner_color||'#0d1b2e'};position:relative;overflow:hidden;margin-bottom:8px;border:1px solid var(--border)">
            ${profile.banner_url ? `<img src="${escapeHtml(profile.banner_url)}" style="width:100%;height:100%;object-fit:cover;position:absolute;inset:0">` : ''}
            <button id="ep-banner-img-btn" style="position:absolute;right:8px;bottom:8px;background:rgba(0,0,0,0.6);border:none;border-radius:8px;color:#fff;padding:5px 10px;font-size:11px;font-weight:600;cursor:pointer"><i class="fa-solid fa-image"></i> Change Image</button>
            <input type="file" id="ep-banner-img-input" accept="image/*" style="display:none">
          </div>
          <div style="display:flex;gap:6px;flex-wrap:wrap">
            ${BANNER_COLORS.map(c => `<button class="ep-banner-color-btn" data-color="${c}" style="width:28px;height:28px;border-radius:8px;background:${c};border:2px solid ${(profile.banner_color||'#0d1b2e')===c?'var(--cyan)':'transparent'};cursor:pointer;transition:0.15s"></button>`).join('')}
            <input type="color" id="ep-banner-custom-color" value="${profile.banner_color||'#0d1b2e'}" style="width:28px;height:28px;border-radius:8px;border:2px solid var(--border);cursor:pointer;padding:0;background:none">
          </div>
        </div>

        <!-- Avatar -->
        <div class="edit-avatar-section">
          <div id="ep-avatar-preview" class="edit-avatar-preview" style="background:linear-gradient(135deg,${profile.banner_color||'#ff2d6e'},'#ff6b35')">
            ${profile.avatar_url ? `<img src="${escapeHtml(profile.avatar_url)}" style="width:100%;height:100%;object-fit:cover;border-radius:50%">` : avatarInitials(profile.display_name||profile.username||'U')}
          </div>
          <div class="edit-avatar-actions">
            <button class="edit-avatar-btn" id="ep-change-avatar-btn"><i class="fa-solid fa-camera"></i> Change Photo</button>
            <input type="file" id="ep-avatar-img-input" accept="image/*" style="display:none">
            <div style="font-size:11px;color:var(--text-muted)">Max 2MB · JPG, PNG, GIF</div>
          </div>
        </div>

        <div class="auth-input-group"><label>Display Name</label><input type="text" id="ep-display-name" class="auth-input" value="${profile.display_name||''}" placeholder="Your name" maxlength="50"></div>
        <div class="auth-input-group"><label>Username</label><input type="text" id="ep-username" class="auth-input" value="${profile.username||''}" placeholder="username" maxlength="30"></div>
        <div class="auth-input-group"><label>Bio</label><textarea id="ep-bio" class="auth-input" placeholder="Tell the world about yourself" rows="3" style="resize:vertical">${profile.bio||''}</textarea></div>
        <div class="auth-input-group"><label>Location</label><input type="text" id="ep-location" class="auth-input" value="${profile.location||''}" placeholder="City, Country"></div>
        <div class="auth-input-group"><label>Website</label><input type="url" id="ep-website" class="auth-input" value="${profile.website||''}" placeholder="https://yoursite.dev"></div>
        

        <div id="ep-save-status" style="font-size:12px;color:var(--text-muted);display:none"></div>
        <button class="auth-btn-primary" id="ep-save-btn">Save Changes</button>
      </div>

      <!-- Account section (from Settings) -->
      <div class="settings-card" style="margin-top:16px">
        <div class="settings-card-header">Account</div>
        <div class="settings-row" id="ep-change-email">
          <div style="display:flex;align-items:center;gap:10px">
            <i class="fa-solid fa-envelope" style="color:var(--cyan);width:18px;text-align:center"></i>
            <span>Email</span>
          </div>
          <span class="settings-row-value">${State.user.email}</span>
        </div>
        <div class="settings-row" id="ep-change-pass">
          <div style="display:flex;align-items:center;gap:10px">
            <i class="fa-solid fa-lock" style="color:var(--cyan);width:18px;text-align:center"></i>
            <span>Change Password</span>
          </div>
          <i class="fa-solid fa-chevron-right" style="color:var(--text-muted);font-size:12px"></i>
        </div>
      </div>

      <!-- Privacy section (from Settings) -->
      <div class="settings-card" style="margin-top:16px">
        <div class="settings-card-header">Privacy</div>
        <div class="settings-row" style="flex-wrap:wrap;gap:8px">
          <div style="display:flex;align-items:center;gap:10px">
            <i class="fa-solid fa-message" style="color:var(--violet);width:18px;text-align:center"></i>
            <span>Who can DM me</span>
          </div>
          <select id="ep-dm-privacy" class="privacy-select">
            <option value="everyone"  ${(profile.dm_privacy||'everyone')==='everyone'  ? 'selected' : ''}>Everyone</option>
            <option value="followers" ${(profile.dm_privacy||'everyone')==='followers' ? 'selected' : ''}>Followers only</option>
            <option value="nobody"    ${(profile.dm_privacy||'everyone')==='nobody'    ? 'selected' : ''}>Nobody</option>
          </select>
        </div>
        <div class="settings-row" style="flex-wrap:wrap;gap:8px">
          <div style="display:flex;align-items:center;gap:10px">
            <i class="fa-solid fa-eye" style="color:var(--violet);width:18px;text-align:center"></i>
            <span>Profile visibility</span>
          </div>
          <select id="ep-profile-visibility" class="privacy-select">
            <option value="public"  ${(profile.profile_visibility||'public')==='public'  ? 'selected' : ''}>Public</option>
            <option value="private" ${(profile.profile_visibility||'public')==='private' ? 'selected' : ''}>Private</option>
          </select>
        </div>
        <div id="ep-privacy-status" style="font-size:12px;color:var(--text-muted);padding:4px 16px 12px;display:none"></div>
      </div>

      <div class="settings-footer">Devit v1.0 · Built with Supabase ⚡</div>
    </div>
  `;

  document.getElementById('ep-back-btn').addEventListener('click', () => navigateTo('settings'));

  // ── Privacy dropdowns — auto-save on change ───────────────────
  async function savePrivacySetting(field, value) {
    const statusEl = document.getElementById('ep-privacy-status');
    if (statusEl) { statusEl.textContent = 'Saving…'; statusEl.style.display = 'block'; statusEl.style.color = 'var(--text-muted)'; }
    const { error } = await sb.from('op_profiles').update({ [field]: value }).eq('id', State.user.id);
    if (error) {
      if (statusEl) { statusEl.textContent = 'Failed to save.'; statusEl.style.color = 'var(--rose)'; }
    } else {
      if (State.profile) State.profile[field] = value;
      if (statusEl) {
        statusEl.textContent = 'Saved ✓';
        statusEl.style.color = 'var(--emerald)';
        setTimeout(() => { statusEl.style.display = 'none'; }, 2000);
      }
    }
  }

  document.getElementById('ep-dm-privacy')?.addEventListener('change', e => savePrivacySetting('dm_privacy', e.target.value));
  document.getElementById('ep-profile-visibility')?.addEventListener('change', e => savePrivacySetting('profile_visibility', e.target.value));

  let newBannerColor = profile.banner_color || '#0d1b2e';
  let newAvatarFile = null;
  let newBannerFile = null;

  main.querySelectorAll('.ep-banner-color-btn').forEach(btn => {
    btn.addEventListener('click', () => {
      newBannerColor = btn.dataset.color;
      main.querySelectorAll('.ep-banner-color-btn').forEach(b => b.style.borderColor = 'transparent');
      btn.style.borderColor = 'var(--cyan)';
      if (!newBannerFile) document.getElementById('ep-banner-preview').style.background = newBannerColor;
      document.getElementById('ep-banner-custom-color').value = newBannerColor;
    });
  });

  document.getElementById('ep-banner-custom-color').addEventListener('input', e => {
    newBannerColor = e.target.value;
    document.getElementById('ep-banner-preview').style.background = newBannerColor;
  });

  document.getElementById('ep-banner-img-btn').addEventListener('click', () => document.getElementById('ep-banner-img-input').click());
  document.getElementById('ep-banner-img-input').addEventListener('change', e => {
    const file = e.target.files[0]; if (!file) return;
    newBannerFile = file;
    const url = URL.createObjectURL(file);
    const preview = document.getElementById('ep-banner-preview');
    let img = preview.querySelector('img');
    if (!img) { img = document.createElement('img'); img.style.cssText = 'width:100%;height:100%;object-fit:cover;position:absolute;inset:0'; preview.insertBefore(img, preview.firstChild); }
    img.src = url;
  });

  document.getElementById('ep-change-avatar-btn').addEventListener('click', () => document.getElementById('ep-avatar-img-input').click());
  document.getElementById('ep-avatar-img-input').addEventListener('change', e => {
    const file = e.target.files[0]; if (!file) return;
    if (file.size > 2 * 1024 * 1024) { toast('Avatar must be under 2MB', 'circle-exclamation'); return; }
    newAvatarFile = file;
    const url = URL.createObjectURL(file);
    document.getElementById('ep-avatar-preview').innerHTML = `<img src="${url}" style="width:100%;height:100%;object-fit:cover;border-radius:50%">`;
  });

  document.getElementById('ep-change-pass').addEventListener('click', async () => {
    const { error } = await sb.auth.resetPasswordForEmail(State.user.email, { redirectTo: 'https://devit-six.vercel.app/' });
    if (!error) toast('Password reset email sent!', 'envelope');
    else toast('Error: ' + error.message, 'circle-exclamation');
  });

  document.getElementById('ep-save-btn').addEventListener('click', async () => {
    const btn = document.getElementById('ep-save-btn');
    const statusEl = document.getElementById('ep-save-status');
    btn.disabled = true; btn.textContent = 'Saving…';
    statusEl.style.display = 'block'; statusEl.textContent = 'Saving…';

    let avatarUrl = profile.avatar_url;
    let bannerUrl = profile.banner_url;

    async function storageUpload(bucket, path, file) {
      await sb.storage.from(bucket).remove([path]);
      const { error } = await sb.storage.from(bucket).upload(path, file, { contentType: file.type, cacheControl: '3600' });
      return error;
    }

    if (newAvatarFile) {
      const ext = (newAvatarFile.name.split('.').pop() || 'jpg').toLowerCase();
      const path = `${State.user.id}/avatar.${ext}`;
      const avErr = await storageUpload('post-images', path, newAvatarFile);
      if (avErr) { btn.disabled = false; btn.textContent = 'Save Changes'; statusEl.textContent = 'Avatar upload failed: ' + avErr.message; return; }
      const { data: { publicUrl } } = sb.storage.from('post-images').getPublicUrl(path);
      avatarUrl = publicUrl + '?t=' + Date.now();
    }

    if (newBannerFile) {
      const ext = (newBannerFile.name.split('.').pop() || 'jpg').toLowerCase();
      const path = `${State.user.id}/banner.${ext}`;
      const bnErr = await storageUpload('post-images', path, newBannerFile);
      if (!bnErr) {
        const { data: { publicUrl } } = sb.storage.from('post-images').getPublicUrl(path);
        bannerUrl = publicUrl + '?t=' + Date.now();
      }
    }

    const updates = {
      display_name: document.getElementById('ep-display-name').value.trim(),
      username: document.getElementById('ep-username').value.trim().toLowerCase(),
      bio: document.getElementById('ep-bio').value.trim(),
      location: document.getElementById('ep-location').value.trim(),
      website: document.getElementById('ep-website').value.trim(),
      avatar_url: avatarUrl,
      banner_url: bannerUrl,
      banner_color: newBannerColor,
    };

    const { error } = await sb.from('op_profiles').update(updates).eq('id', State.user.id);
    if (error) {
      statusEl.textContent = 'Error: ' + error.message;
      btn.disabled = false; btn.textContent = 'Save Changes';
    } else {
      State.profile = { ...State.profile, ...updates };
      statusEl.style.color = 'var(--cyan)';
      statusEl.textContent = 'Saved!';
      toast('Profile updated!', 'check');
      setTimeout(() => navigateTo('profile'), 800);
    }
  });
}

/* ── Profile Quick View (tap PFP) ───────────────────────────── */
function openProfileQuickView(userId) {
  const overlay = document.getElementById('profile-quick-overlay');
  const card    = document.getElementById('profile-quick-card');
  if (!overlay || !card) return;

  overlay.style.display = 'flex';
  card.innerHTML = `<div style="padding:32px;text-align:center;color:var(--text-muted)">Loading…</div>`;

  // Close on overlay click
  overlay.onclick = e => { if (e.target === overlay) { overlay.style.display = 'none'; } };

  sb.from('op_profiles').select('*').eq('id', userId).single().then(async ({ data: p }) => {
    if (!p) { card.innerHTML = `<div style="padding:24px;text-align:center;color:var(--rose)">Profile not found</div>`; return; }

    const isOwn = userId === State.user.id;
    const color = avatarColor(p.display_name || p.username || '?');
    const { data: followRow } = !isOwn ? await sb.from('op_follows').select('id').eq('follower_id', State.user.id).eq('following_id', userId).single() : { data: null };
    const isFollowing = !!followRow;

    // Check if linked
    const { data: linkRow } = !isOwn ? await sb.from('op_links').select('id').eq('requester_id', State.user.id).eq('target_id', userId).eq('status', 'accepted').single() : { data: null };
    const isLinked = !!linkRow;

    card.innerHTML = `
      <div style="height:80px;background:linear-gradient(135deg,${color}33,var(--bg-void));position:relative;">
        <button id="pqv-close" style="position:absolute;top:10px;right:12px;color:var(--text-muted);font-size:20px;background:none;border:none;cursor:pointer;line-height:1"><i class="fa-solid fa-xmark"></i></button>
      </div>
      <div style="padding:0 20px 20px;margin-top:-36px">
        <div style="width:72px;height:72px;border-radius:50%;background:${color};border:4px solid var(--bg-surface);display:flex;align-items:center;justify-content:center;font-size:26px;font-weight:800;color:#fff;overflow:hidden;margin-bottom:10px">
          ${p.avatar_url ? `<img src="${escapeHtml(p.avatar_url)}" style="width:100%;height:100%;object-fit:cover">` : avatarInitials(p.display_name || p.username || 'U')}
        </div>
        <div style="font-family:var(--font-display);font-size:18px;font-weight:800">${escapeHtml(p.display_name || p.username || 'Unknown')}</div>
        <div style="font-size:13px;color:var(--text-muted);margin-bottom:6px">@${escapeHtml(p.username || '')}</div>
        ${p.bio ? `<div style="font-size:13px;color:var(--text-secondary);margin-bottom:10px;line-height:1.5">${escapeHtml(p.bio)}</div>` : ''}
        <div style="display:flex;gap:16px;margin-bottom:14px">
          <div style="font-size:13px"><strong>${fmtNum(p.followers_count||0)}</strong> <span style="color:var(--text-muted)">Followers</span></div>
          <div style="font-size:13px"><strong>${fmtNum(p.following_count||0)}</strong> <span style="color:var(--text-muted)">Following</span></div>
        </div>
        ${!isOwn ? `<div style="display:flex;gap:8px">
          <button class="profile-action-btn ${isFollowing?'secondary':'primary'}" id="pqv-follow" style="flex:1">${isFollowing ? 'Unfollow' : 'Follow'}</button>
          <button class="profile-action-btn secondary" id="pqv-dm" style="flex:1"><i class="fa-solid fa-message"></i> DM</button>
          <button class="profile-action-btn secondary" id="pqv-link" title="${isLinked?'Linked':'Link'}">${isLinked ? '<i class="fa-solid fa-link" style="color:var(--cyan)"></i>' : '<i class="fa-solid fa-user-plus"></i>'}</button>
        </div>` : ''}
        <button class="profile-action-btn secondary" id="pqv-view-profile" style="width:100%;margin-top:8px">View Full Profile</button>
      </div>
    `;

    document.getElementById('pqv-close').onclick = () => overlay.style.display = 'none';
    document.getElementById('pqv-view-profile').onclick = () => { overlay.style.display = 'none'; renderProfile($('#main'), userId); };

    const followBtn = document.getElementById('pqv-follow');
    if (followBtn) {
      let fState = isFollowing;
      followBtn.onclick = async () => {
        followBtn.disabled = true;
        // strongs[0] = Followers count of target, strongs[1] = Following count of target
        const strongs = card.querySelectorAll('strong');
        const follEl  = strongs[0]; // target's Followers
        if (fState) {
          const { error } = await sb.from('op_follows').delete().eq('follower_id', State.user.id).eq('following_id', userId);
          if (!error) {
            await decrementFollowCounts(userId, State.user.id);
            fState = false; followBtn.textContent = 'Follow'; followBtn.className = 'profile-action-btn primary';
            toast('Unfollowed', 'user-minus');
            if (follEl) follEl.textContent = fmtNum(Math.max(0, (parseInt(follEl.textContent.replace(/[^\d]/g,'')) || 1) - 1));
          }
        } else {
          const { error } = await sb.from('op_follows').insert({ follower_id: State.user.id, following_id: userId });
          if (!error || error.code === '23505') {
            await incrementFollowCounts(userId, State.user.id);
            await sb.from('op_notifications').insert({ user_id: userId, actor_id: State.user.id, type: 'follow' });
            fState = true; followBtn.textContent = 'Unfollow'; followBtn.className = 'profile-action-btn secondary';
            toast('Followed!', 'user-check');
            if (follEl) follEl.textContent = fmtNum((parseInt(follEl.textContent.replace(/[^\d]/g,'')) || 0) + 1);
          }
        }
        followBtn.disabled = false;
      };
    }

    const dmBtn = document.getElementById('pqv-dm');
    if (dmBtn) {
      dmBtn.onclick = async () => {
        const { data: targetProfile } = await sb.from('op_profiles').select('dm_privacy').eq('id', userId).single();
        const isFollowingTarget = !!(await sb.from('op_follows').select('id').eq('follower_id', State.user.id).eq('following_id', userId).single()).data;
        const dmAllowed = await canDM(userId, targetProfile?.dm_privacy, isFollowingTarget);
        if (!dmAllowed) {
          toast(targetProfile?.dm_privacy === 'nobody' ? "This user doesn't accept DMs" : 'You must follow this user to DM them', 'lock');
          return;
        }
        overlay.style.display = 'none';
        const a = State.user.id < userId ? State.user.id : userId;
        const b = State.user.id < userId ? userId : State.user.id;
        let { data: convo } = await sb.from('op_conversations').select('id').eq('participant_a', a).eq('participant_b', b).single();
        if (!convo) { const { data: nc } = await sb.from('op_conversations').insert({ participant_a: a, participant_b: b }).select().single(); convo = nc; }
        navigateTo('messages');
        setTimeout(() => openDM(convo.id, userId, $('#dm-view')), 300);
      };
    }

    const linkBtn = document.getElementById('pqv-link');
    if (linkBtn) {
      linkBtn.onclick = async () => {
        if (isLinked) { toast('Already linked!', 'link'); return; }
        const { error } = await sb.from('op_links').insert({ requester_id: State.user.id, target_id: userId, status: 'pending' });
        if (!error) {
          await sb.from('op_notifications').insert({ user_id: userId, actor_id: State.user.id, type: 'link_request' });
          toast('Link request sent!', 'link');
          linkBtn.innerHTML = '<i class="fa-solid fa-clock"></i>';
        }
      };
    }
  });
}

/* ── Snippets — TikTok/Shorts style full-screen snap feed ────── */

// Global mute state shared across all snippet cards
let _snippetsMuted = true;

function renderSnippets(main) {
  // Full-screen takeover: hide topbar/sidebar while in snippets view
  main.style.cssText = 'padding:0;max-width:none;';

  main.innerHTML = `
    <div id="snippets-container" style="
      position:fixed;inset:0;z-index:200;background:#000;
      overflow-y:scroll;scroll-snap-type:y mandatory;
      scrollbar-width:none;-ms-overflow-style:none;
    " role="region" aria-label="Snippets feed" aria-roledescription="Video feed — swipe up or down to navigate">
      <style>#snippets-container::-webkit-scrollbar{display:none}</style>
      <div id="snippets-feed" style="width:100%;"></div>
    </div>

    <!-- Top bar overlay -->
    <div style="position:fixed;top:0;left:0;right:0;z-index:210;
      display:flex;align-items:center;justify-content:space-between;
      padding:12px 16px;
      background:linear-gradient(to bottom,rgba(0,0,0,0.6) 0%,transparent 100%);
      pointer-events:none;">
      <div style="pointer-events:auto">
        <button id="snippets-back-btn" aria-label="Back to feed" style="background:rgba(0,0,0,0.4);border:none;color:#fff;width:36px;height:36px;border-radius:50%;cursor:pointer;font-size:16px;display:flex;align-items:center;justify-content:center;backdrop-filter:blur(8px)">
          <i class="fa-solid fa-arrow-left" aria-hidden="true"></i>
        </button>
      </div>
      <div style="font-size:15px;font-weight:700;color:#fff;letter-spacing:0.02em" aria-hidden="true">Snippets</div>
      <div style="pointer-events:auto;display:flex;gap:8px;align-items:center">
        <button id="snippets-mute-btn" aria-label="Toggle mute" style="background:rgba(0,0,0,0.4);border:none;color:#fff;width:36px;height:36px;border-radius:50%;cursor:pointer;font-size:14px;display:flex;align-items:center;justify-content:center;backdrop-filter:blur(8px)">
          <i class="fa-solid fa-volume-xmark" aria-hidden="true"></i>
        </button>
        <button id="snippets-post-btn" aria-label="Post a snippet" style="background:var(--cyan,#ff2d6e);border:none;color:#000;height:32px;padding:0 14px;border-radius:20px;cursor:pointer;font-size:12px;font-weight:700;display:flex;align-items:center;gap:6px">
          <i class="fa-solid fa-plus" aria-hidden="true"></i> Post
        </button>
      </div>
    </div>

    <!-- Swipe hint (fades out after first interaction) -->
    <div id="snippets-swipe-hint" aria-hidden="true" style="
      position:fixed;bottom:100px;left:50%;transform:translateX(-50%);
      z-index:211;display:flex;flex-direction:column;align-items:center;gap:6px;
      color:rgba(255,255,255,0.7);font-size:12px;font-weight:600;
      pointer-events:none;animation:swipeHintFade 3s 1.5s forwards;
    ">
      <i class="fa-solid fa-angles-up" style="font-size:20px;animation:bounceUp 1s ease-in-out infinite alternate"></i>
      Swipe up for next
    </div>
  `;

  // Back button exits snippets view
  document.getElementById('snippets-back-btn').addEventListener('click', () => {
    main.style.cssText = '';
    document.querySelectorAll('.snip-video').forEach(v => v.pause());
    navigateTo('feed');
  });

  // Global mute toggle
  const muteBtn = document.getElementById('snippets-mute-btn');
  muteBtn.addEventListener('click', () => {
    _snippetsMuted = !_snippetsMuted;
    muteBtn.setAttribute('aria-label', _snippetsMuted ? 'Unmute' : 'Mute');
    muteBtn.innerHTML = _snippetsMuted
      ? '<i class="fa-solid fa-volume-xmark" aria-hidden="true"></i>'
      : '<i class="fa-solid fa-volume-high" aria-hidden="true"></i>';
    document.querySelectorAll('.snip-video').forEach(v => { v.muted = _snippetsMuted; });
  });

  document.getElementById('snippets-post-btn').addEventListener('click', openSnippetUploadModal);

  loadSnippets(document.getElementById('snippets-feed'));

  // Keyboard navigation (arrow keys / j/k)
  const snippetsContainer = document.getElementById('snippets-container');
  const onKeydown = e => {
    if (!document.getElementById('snippets-container')) { document.removeEventListener('keydown', onKeydown); return; }
    const h = window.innerHeight;
    if (e.key === 'ArrowDown' || e.key === 'j') snippetsContainer.scrollBy({ top: h, behavior: 'smooth' });
    if (e.key === 'ArrowUp'   || e.key === 'k') snippetsContainer.scrollBy({ top: -h, behavior: 'smooth' });
    if (e.key === 'ArrowLeft' || e.key === 'Escape') { main.style.cssText = ''; document.querySelectorAll('.snip-video').forEach(v => v.pause()); navigateTo('feed'); }
  };
  document.addEventListener('keydown', onKeydown);

  // Hide swipe hint on first scroll
  snippetsContainer.addEventListener('scroll', () => {
    const hint = document.getElementById('snippets-swipe-hint');
    if (hint) hint.style.display = 'none';
  }, { once: true });
}

async function loadSnippets(container) {
  const { data: snippets } = await sb
    .from('op_snippets')
    .select('*, profiles:op_profiles!author_id(id, username, display_name, avatar_url)')
    .order('created_at', { ascending: false })
    .limit(20);

  if (!snippets?.length) {
    container.innerHTML = `
      <div style="height:100vh;display:flex;flex-direction:column;align-items:center;justify-content:center;gap:16px;color:#fff">
        <div style="font-size:56px">🎬</div>
        <div style="font-size:20px;font-weight:800">No Snippets Yet</div>
        <div style="font-size:14px;opacity:0.6">Be the first to post one!</div>
      </div>`;
    return;
  }

  container.innerHTML = '';
  snippets.forEach(s => container.appendChild(buildSnippetCard(s)));
}

function buildSnippetCard(snippet) {
  const card = document.createElement('div');
  card.style.cssText = `
    position:relative;width:100%;height:100vh;
    scroll-snap-align:start;scroll-snap-stop:always;
    overflow:hidden;background:#000;flex-shrink:0;
  `;

  const color = avatarColor(snippet.profiles?.display_name || snippet.profiles?.username || '?');
  const username = escapeHtml(snippet.profiles?.username || '?');
  const displayName = escapeHtml(snippet.profiles?.display_name || snippet.profiles?.username || '?');
  const avatarContent = snippet.profiles?.avatar_url
    ? `<img src="${escapeHtml(snippet.profiles.avatar_url)}" style="width:100%;height:100%;object-fit:cover">`
    : avatarInitials(snippet.profiles?.display_name || snippet.profiles?.username || 'U');

  card.innerHTML = `
    <!-- Video -->
    <video class="snip-video" src="${escapeHtml(snippet.video_url || '')}"
      loop playsinline preload="metadata"
      style="position:absolute;inset:0;width:100%;height:100%;object-fit:cover;">
    </video>

    <!-- Gradient overlays -->
    <div style="position:absolute;inset:0;background:linear-gradient(to top,rgba(0,0,0,0.75) 0%,transparent 50%,transparent 80%,rgba(0,0,0,0.2) 100%);pointer-events:none"></div>

    <!-- Tap to play/pause hit zone -->
    <div class="snip-tap-zone" style="position:absolute;inset:0;z-index:1"></div>

    <!-- Play/pause icon (center flash) -->
    <div class="snip-playpause-flash" style="
      position:absolute;top:50%;left:50%;transform:translate(-50%,-50%);
      width:72px;height:72px;border-radius:50%;
      background:rgba(0,0,0,0.5);backdrop-filter:blur(4px);
      display:flex;align-items:center;justify-content:center;
      font-size:28px;color:#fff;opacity:0;z-index:2;
      transition:opacity 0.15s;pointer-events:none;">
      <i class="fa-solid fa-play"></i>
    </div>

    <!-- Progress bar -->
    <div style="position:absolute;top:0;left:0;right:0;height:2px;background:rgba(255,255,255,0.2);z-index:3">
      <div class="snip-progress" style="height:100%;background:var(--cyan,#ff2d6e);width:0%;transition:width 0.1s linear"></div>
    </div>

    <!-- Right action column -->
    <div style="position:absolute;right:12px;bottom:90px;z-index:4;display:flex;flex-direction:column;align-items:center;gap:20px;">

      <!-- Avatar with follow ring -->
      <div style="position:relative;margin-bottom:4px">
        <div class="snip-avatar-btn" data-uid="${snippet.profiles?.id || ''}" style="
          width:48px;height:48px;border-radius:50%;background:${color};
          display:flex;align-items:center;justify-content:center;
          font-weight:700;font-size:16px;color:#fff;overflow:hidden;
          border:2px solid #fff;cursor:pointer;flex-shrink:0;">
          ${avatarContent}
        </div>
        <div style="
          position:absolute;bottom:-8px;left:50%;transform:translateX(-50%);
          width:20px;height:20px;border-radius:50%;background:var(--rose,#fb7185);
          display:flex;align-items:center;justify-content:center;
          font-size:10px;color:#fff;border:2px solid #000;cursor:pointer;"
          class="snip-follow-dot" data-uid="${snippet.profiles?.id || ''}">
          <i class="fa-solid fa-plus"></i>
        </div>
      </div>

      <!-- Heart -->
      <div style="display:flex;flex-direction:column;align-items:center;gap:4px">
        <button class="snip-heart-btn" style="
          background:none;border:none;color:#fff;font-size:28px;cursor:pointer;
          padding:6px;transition:transform 0.2s;">
          <i class="fa-solid fa-heart"></i>
        </button>
        <span class="snip-heart-count" style="color:#fff;font-size:12px;font-weight:700">${fmtNum(snippet.hearts_count || 0)}</span>
      </div>

      <!-- Comment -->
      <div style="display:flex;flex-direction:column;align-items:center;gap:4px">
        <button class="snip-comment-btn" data-sid="${snippet.id}" style="background:none;border:none;color:#fff;font-size:26px;cursor:pointer;padding:6px">
          <i class="fa-solid fa-comment-dots"></i>
        </button>
        <span class="snip-comment-count" style="color:#fff;font-size:12px;font-weight:700">${fmtNum(snippet.comments_count || 0)}</span>
      </div>

      <!-- Bookmark -->
      <div style="display:flex;flex-direction:column;align-items:center;gap:4px">
        <button class="snip-bookmark-btn" style="background:none;border:none;color:#fff;font-size:24px;cursor:pointer;padding:6px">
          <i class="fa-solid fa-bookmark"></i>
        </button>
      </div>

      <!-- Share -->
      <div style="display:flex;flex-direction:column;align-items:center;gap:4px">
        <button class="snip-share-btn" style="background:none;border:none;color:#fff;font-size:24px;cursor:pointer;padding:6px">
          <i class="fa-solid fa-share-nodes"></i>
        </button>
      </div>
    </div>

    <!-- Bottom info -->
    <div style="position:absolute;left:0;right:72px;bottom:16px;z-index:4;padding:0 16px">
      <div class="snip-author-info" data-uid="${snippet.profiles?.id || ''}" style="display:flex;align-items:center;gap:8px;margin-bottom:8px;cursor:pointer">
        <div style="font-size:14px;font-weight:700;color:#fff">@${username}</div>
        
      </div>
      ${snippet.caption ? `
        <div style="font-size:13px;color:rgba(255,255,255,0.9);line-height:1.5;
          display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;overflow:hidden;">
          ${escapeHtml(snippet.caption).replace(/#(\w+)/g,'<span style="color:var(--cyan,#ff2d6e)">#$1</span>')}
        </div>` : ''}
      <div style="margin-top:6px;display:flex;align-items:center;gap:6px">
        <i class="fa-solid fa-music" style="font-size:11px;color:rgba(255,255,255,0.6)"></i>
        <div style="font-size:11px;color:rgba(255,255,255,0.6)">${displayName} · Original audio</div>
      </div>
    </div>
  `;

  const video     = card.querySelector('.snip-video');
  const flash     = card.querySelector('.snip-playpause-flash');
  const progress  = card.querySelector('.snip-progress');
  const tapZone   = card.querySelector('.snip-tap-zone');

  // Sync video mute state with global mute
  video.muted = _snippetsMuted;

  // Progress bar
  video.addEventListener('timeupdate', () => {
    if (video.duration) progress.style.width = ((video.currentTime / video.duration) * 100) + '%';
  });

  // Tap to play/pause with flash animation
  let _flashTimer;
  tapZone.addEventListener('click', () => {
    if (video.paused) {
      video.play();
      flash.querySelector('i').className = 'fa-solid fa-play';
    } else {
      video.pause();
      flash.querySelector('i').className = 'fa-solid fa-pause';
    }
    flash.style.opacity = '1';
    clearTimeout(_flashTimer);
    _flashTimer = setTimeout(() => { flash.style.opacity = '0'; }, 600);
  });

  // Double-tap to heart (TikTok style)
  let _lastTap = 0;
  tapZone.addEventListener('click', () => {
    const now = Date.now();
    if (now - _lastTap < 300) {
      // Double tap — trigger heart
      card.querySelector('.snip-heart-btn').click();
      // Show heart burst
      const burst = document.createElement('div');
      burst.innerHTML = '<i class="fa-solid fa-heart" style="color:var(--rose,#fb7185);font-size:80px"></i>';
      burst.style.cssText = 'position:absolute;top:50%;left:50%;transform:translate(-50%,-50%) scale(0);z-index:10;pointer-events:none;transition:transform 0.3s,opacity 0.3s';
      card.appendChild(burst);
      requestAnimationFrame(() => { burst.style.transform = 'translate(-50%,-50%) scale(1)'; burst.style.opacity = '1'; });
      setTimeout(() => { burst.style.transform = 'translate(-50%,-50%) scale(1.4)'; burst.style.opacity = '0'; }, 300);
      setTimeout(() => burst.remove(), 700);
    }
    _lastTap = now;
  });

  // Intersection observer — autoplay when in view, pause when not
  const obs = new IntersectionObserver(entries => {
    entries.forEach(entry => {
      if (entry.isIntersecting) {
        video.muted = _snippetsMuted; // sync mute on each entry
        video.play().catch(() => {
          // Autoplay blocked — show play icon
          flash.querySelector('i').className = 'fa-solid fa-play';
          flash.style.opacity = '1';
        });
      } else {
        video.pause();
        video.currentTime = 0;
        progress.style.width = '0%';
      }
    });
  }, { threshold: 0.7 });
  obs.observe(card);

  // Avatar / author click
  card.querySelectorAll('.snip-avatar-btn, .snip-author-info').forEach(el => {
    el.addEventListener('click', e => {
      e.stopPropagation();
      const uid = el.dataset.uid;
      if (uid) openProfileQuickView(uid);
    });
  });

  // Follow dot
  card.querySelector('.snip-follow-dot').addEventListener('click', async e => {
    e.stopPropagation();
    const uid = e.currentTarget.dataset.uid;
    if (!uid || uid === State.user.id) return;
    const { error } = await sb.from('op_follows').insert({ follower_id: State.user.id, following_id: uid });
    if (!error) {
      e.currentTarget.innerHTML = '<i class="fa-solid fa-check"></i>';
      e.currentTarget.style.background = 'var(--emerald,#34d399)';
      await sb.rpc('increment_followers', { target_user_id: uid });
      await sb.rpc('increment_following', { target_user_id: State.user.id });
      await sb.from('op_notifications').insert({ user_id: uid, actor_id: State.user.id, type: 'follow' });
      toast('Followed!', 'user-check');
    }
  });

  // Heart
  let _hearted = false;
  const heartBtn   = card.querySelector('.snip-heart-btn');
  const heartCount = card.querySelector('.snip-heart-count');
  heartBtn.addEventListener('click', async e => {
    e.stopPropagation();
    _hearted = !_hearted;
    heartBtn.querySelector('i').style.color = _hearted ? 'var(--rose,#fb7185)' : '#fff';
    heartBtn.style.transform = 'scale(1.3)';
    setTimeout(() => { heartBtn.style.transform = ''; }, 200);
    const cur = parseInt(heartCount.textContent) || 0;
    heartCount.textContent = fmtNum(_hearted ? cur + 1 : Math.max(0, cur - 1));
    if (_hearted) {
      await sb.from('op_snippet_hearts').insert({ snippet_id: snippet.id, user_id: State.user.id });
    } else {
      const { data: existing } = await sb.from('op_snippet_hearts').select('id').eq('snippet_id', snippet.id).eq('user_id', State.user.id).single();
      if (existing) await sb.from('op_snippet_hearts').delete().eq('id', existing.id);
    }
  });

  // Check if already hearted
  sb.from('op_snippet_hearts').select('id').eq('snippet_id', snippet.id).eq('user_id', State.user.id).single().then(({ data }) => {
    if (data) { _hearted = true; heartBtn.querySelector('i').style.color = 'var(--rose,#fb7185)'; }
  });

  // Bookmark
  card.querySelector('.snip-bookmark-btn').addEventListener('click', async e => {
    e.stopPropagation();
    const btn = e.currentTarget;
    const { data: existing } = await sb.from('op_snippet_bookmarks').select('id').eq('snippet_id', snippet.id).eq('user_id', State.user.id).single();
    if (existing) {
      await sb.from('op_snippet_bookmarks').delete().eq('id', existing.id);
      btn.querySelector('i').style.color = '#fff';
      toast('Removed from bookmarks', 'bookmark');
    } else {
      await sb.from('op_snippet_bookmarks').insert({ snippet_id: snippet.id, user_id: State.user.id });
      btn.querySelector('i').style.color = 'var(--cyan,#ff2d6e)';
      toast('Snippet bookmarked!', 'bookmark');
    }
  });

  // Share
  card.querySelector('.snip-share-btn').addEventListener('click', async e => {
    e.stopPropagation();
    const url = snippet.video_url;
    if (navigator.share) {
      try { await navigator.share({ title: `@${username} on Devit`, url }); } catch (_) {}
    } else {
      navigator.clipboard?.writeText(url).then(() => toast('Video link copied!', 'link'));
    }
  });

  // Comment — open sliding panel
  card.querySelector('.snip-comment-btn').addEventListener('click', e => {
    e.stopPropagation();
    openSnippetComments(snippet.id, card);
  });

  // Disable long-press context menu / download on the video
  const vid = card.querySelector('.snip-video');
  if (vid) {
    vid.addEventListener('contextmenu', e => e.preventDefault());
    vid.addEventListener('touchstart', e => { vid._lpTimer = setTimeout(() => e.preventDefault(), 400); }, { passive: false });
    vid.addEventListener('touchend', () => clearTimeout(vid._lpTimer));
    vid.addEventListener('touchmove', () => clearTimeout(vid._lpTimer));
    vid.setAttribute('controlsList', 'nodownload');
    vid.setAttribute('disablePictureInPicture', '');
  }

  return card;
}

/* ── Snippet Comments Panel ──────────────────────────────────── */
async function openSnippetComments(snippetId, card) {
  // Remove any existing panel
  document.getElementById('snip-comments-panel')?.remove();
  document.getElementById('snip-comments-overlay')?.remove();

  const overlay = document.createElement('div');
  overlay.id = 'snip-comments-overlay';
  overlay.style.cssText = 'position:fixed;inset:0;z-index:300;background:rgba(0,0,0,0.5);';
  document.body.appendChild(overlay);

  const panel = document.createElement('div');
  panel.id = 'snip-comments-panel';
  panel.style.cssText = `
    position:fixed;bottom:0;left:0;right:0;z-index:301;
    background:var(--bg-surface,#10121a);
    border-radius:24px 24px 0 0;
    border-top:1px solid rgba(255,255,255,0.08);
    max-height:70vh;display:flex;flex-direction:column;
    animation:slideUpPanel 0.3s cubic-bezier(0.16,1,0.3,1) forwards;
  `;

  if (!document.getElementById('snip-panel-anim')) {
    const s = document.createElement('style');
    s.id = 'snip-panel-anim';
    s.textContent = `
      @keyframes slideUpPanel { from{transform:translateY(100%)} to{transform:translateY(0)} }
    `;
    document.head.appendChild(s);
  }

  panel.innerHTML = `
    <div style="padding:12px 16px;border-bottom:1px solid rgba(255,255,255,0.06);display:flex;align-items:center;justify-content:space-between;">
      <div style="font-size:14px;font-weight:700;color:var(--text-primary)">Comments</div>
      <button id="snip-comments-close" style="color:var(--text-muted);font-size:18px;background:none;border:none;cursor:pointer;"><i class="fa-solid fa-xmark"></i></button>
    </div>
    <div id="snip-comments-list" style="flex:1;overflow-y:auto;padding:8px 0;min-height:120px;">
      <div style="padding:24px;text-align:center;color:var(--text-muted);font-size:13px;">Loading…</div>
    </div>
    <div style="padding:10px 12px;border-top:1px solid rgba(255,255,255,0.06);display:flex;gap:8px;align-items:center;">
      ${avatarHtml(State.profile, 32)}
      <input id="snip-comment-input" style="flex:1;background:var(--bg-elevated,#181c27);border:1px solid rgba(255,255,255,0.08);border-radius:999px;padding:9px 14px;color:var(--text-primary);font-size:13px;outline:none;font-family:inherit;" placeholder="Add a comment…">
      <button id="snip-comment-send" style="background:var(--cyan,#ff2d6e);color:#050508;border:none;border-radius:999px;padding:9px 16px;font-size:13px;font-weight:700;cursor:pointer;">Send</button>
    </div>
  `;
  document.body.appendChild(panel);

  const close = () => {
    panel.style.transform = 'translateY(100%)';
    panel.style.transition = '0.25s ease';
    setTimeout(() => { panel.remove(); overlay.remove(); }, 250);
  };
  overlay.addEventListener('click', close);
  panel.querySelector('#snip-comments-close').addEventListener('click', close);

  // Load comments
  await loadSnippetComments(snippetId, panel.querySelector('#snip-comments-list'));

  // Send
  const input = panel.querySelector('#snip-comment-input');
  const sendBtn = panel.querySelector('#snip-comment-send');
  const doSend = async () => {
    const text = input.value.trim();
    if (!text) return;
    input.value = '';
    const { error } = await sb.from('op_snippet_comments').insert({
      snippet_id: snippetId,
      author_id: State.user.id,
      content: text,
    });
    if (!error) {
      await loadSnippetComments(snippetId, panel.querySelector('#snip-comments-list'));
      // Update count on the card button
      const countEl = card?.querySelector('.snip-comment-count');
      if (countEl) {
        const cur = parseInt(countEl.textContent.replace('K','000')) || 0;
        countEl.textContent = fmtNum(cur + 1);
      }
      // Increment in DB
      await sb.from('op_snippets').update({ comments_count: (parseInt(card?.querySelector('.snip-comment-count')?.textContent)||0) }).eq('id', snippetId);
    } else {
      toast('Failed to post comment', 'circle-exclamation');
    }
  };
  sendBtn.addEventListener('click', doSend);
  input.addEventListener('keydown', e => { if (e.key === 'Enter') doSend(); });
}

async function loadSnippetComments(snippetId, container) {
  if (!container) return;
  const { data: comments } = await sb
    .from('op_snippet_comments')
    .select('id, content, created_at, profiles:op_profiles!author_id(id, username, display_name, avatar_url, is_github)')
    .eq('snippet_id', snippetId)
    .order('created_at', { ascending: true })
    .limit(50);

  if (!comments?.length) {
    container.innerHTML = `<div style="padding:24px;text-align:center;color:var(--text-muted);font-size:13px;">No comments yet — be the first!</div>`;
    return;
  }

  container.innerHTML = '';
  comments.forEach(c => {
    const p = c.profiles;
    const div = document.createElement('div');
    div.style.cssText = 'display:flex;gap:10px;padding:10px 16px;align-items:flex-start;';
    div.innerHTML = `
      ${avatarHtml(p, 32)}
      <div style="flex:1;min-width:0;">
        <div style="display:flex;align-items:center;gap:6px;flex-wrap:wrap;">
          <span style="font-size:13px;font-weight:700;color:var(--text-primary);">${escapeHtml(p?.display_name || p?.username || 'User')}</span>
          
          <span style="font-size:11px;color:var(--text-muted);">${timeAgo(c.created_at)}</span>
        </div>
        <div style="font-size:13px;color:var(--text-secondary);margin-top:2px;line-height:1.4;">${escapeHtml(c.content)}</div>
      </div>
    `;
    container.appendChild(div);
  });
  container.scrollTop = container.scrollHeight;
}

function openSnippetUploadModal() {
  const modal = $('#modal-overlay');
  const body  = $('#modal-body');
  $('#modal-title-text').textContent = '📸 Post a Snippet';
  modal.classList.add('open');

  body.innerHTML = `
    <div style="padding:20px;display:flex;flex-direction:column;gap:14px">
      <div class="drop-zone" id="snippet-drop-zone" style="border:2px dashed var(--border);border-radius:16px;padding:32px;text-align:center;cursor:pointer;transition:0.2s">
        <div style="font-size:40px;margin-bottom:10px">🎬</div>
        <div style="font-size:14px;font-weight:600;color:var(--text-secondary)">Drop your video here</div>
        <div style="font-size:12px;color:var(--text-muted);margin-top:4px">Max 30 seconds · Will be compressed to ~599 KB</div>
        <input type="file" id="snippet-file-input" accept="video/*" style="display:none">
      </div>
      <div id="snippet-preview-area" style="display:none">
        <video id="snippet-preview-video" style="width:100%;border-radius:12px;max-height:300px;background:#000" controls></video>
        <div id="snippet-duration-warn" style="display:none;color:var(--rose);font-size:12px;margin-top:6px"><i class="fa-solid fa-triangle-exclamation"></i> Video exceeds 30 seconds — please trim it</div>
      </div>
      <div class="auth-input-group">
        <label>Caption (optional)</label>
        <textarea id="snippet-caption" class="auth-input" placeholder="What's this about? #hashtags @mentions" rows="2" style="resize:none"></textarea>
      </div>
      <div id="snippet-compress-status" style="display:none;font-size:12px;color:var(--cyan)"><i class="fa-solid fa-spinner fa-spin"></i> Compressing video…</div>
      <button class="auth-btn-primary" id="snippet-post-btn" disabled><i class="fa-solid fa-film"></i> Post Snippet</button>
    </div>
  `;

  const dropZone   = document.getElementById('snippet-drop-zone');
  const fileInput  = document.getElementById('snippet-file-input');
  const previewArea= document.getElementById('snippet-preview-area');
  const previewVid = document.getElementById('snippet-preview-video');
  const durationWarn = document.getElementById('snippet-duration-warn');
  const postBtn    = document.getElementById('snippet-post-btn');
  let selectedFile = null;

  const ALLOWED_VIDEO_TYPES = ['video/mp4', 'video/webm', 'video/ogg', 'video/quicktime', 'video/x-msvideo', 'video/x-matroska'];

  function validateVideoFile(file) {
    if (!file) return false;
    if (!file.type.startsWith('video/') || !ALLOWED_VIDEO_TYPES.includes(file.type)) {
      toast(`Unsupported file type: ${file.type || 'unknown'}. Please upload a video (MP4, WebM, MOV).`, 'circle-exclamation');
      return false;
    }
    return true;
  }

  dropZone.addEventListener('click', () => fileInput.click());
  dropZone.addEventListener('dragover', e => { e.preventDefault(); dropZone.style.borderColor = 'var(--cyan)'; });
  dropZone.addEventListener('dragleave', () => { dropZone.style.borderColor = 'var(--border)'; });
  dropZone.addEventListener('drop', e => {
    e.preventDefault();
    dropZone.style.borderColor = 'var(--border)';
    const f = e.dataTransfer.files[0];
    if (f && validateVideoFile(f)) handleSnippetFile(f);
  });

  fileInput.addEventListener('change', () => {
    const f = fileInput.files[0];
    if (f && validateVideoFile(f)) handleSnippetFile(f);
    fileInput.value = '';
  });

  function handleSnippetFile(file) {
    selectedFile = file;
    const url = URL.createObjectURL(file);
    previewVid.src = url;
    previewArea.style.display = 'block';
    previewVid.onloadedmetadata = () => {
      if (previewVid.duration > 31) {
        durationWarn.style.display = 'block';
        postBtn.disabled = true;
      } else {
        durationWarn.style.display = 'none';
        postBtn.disabled = false;
      }
    };
  }

  postBtn.addEventListener('click', async () => {
    if (!selectedFile) return;
    postBtn.disabled = true;
    postBtn.innerHTML = '<i class="fa-solid fa-spinner fa-spin"></i> Uploading…';
    const status = document.getElementById('snippet-compress-status');
    status.style.display = 'block';

    // Simulate compression (actual FFmpeg compression would require a server/edge function)
    // Here we upload directly — in production, route through a Supabase Edge Function
    const caption = document.getElementById('snippet-caption').value.trim();
    const ext = selectedFile.name.split('.').pop() || 'mp4';
    const path = `snippets/${State.user.id}/${Date.now()}.${ext}`;

    // Guarantee a video contentType — never let audio/mpeg or unknown types through
    const safeContentType = selectedFile.type.startsWith('video/') ? selectedFile.type : 'video/mp4';
    const { error: uploadErr } = await sb.storage.from('snippets').upload(path, selectedFile, { contentType: safeContentType });
    status.style.display = 'none';

    if (uploadErr) {
      toast('Upload failed: ' + uploadErr.message, 'circle-exclamation');
      postBtn.disabled = false;
      postBtn.innerHTML = '<i class="fa-solid fa-film"></i> Post Snippet';
      return;
    }

    const videoUrl = sb.storage.from('snippets').getPublicUrl(path).data.publicUrl;
    const { error: insertErr } = await sb.from('op_snippets').insert({
      author_id: State.user.id,
      video_url: videoUrl,
      caption,
      hearts_count: 0,
      duration: Math.round(previewVid.duration || 0),
    });

    if (insertErr) {
      toast('Failed to post: ' + insertErr.message, 'circle-exclamation');
    } else {
      modal.classList.remove('open');
      toast('Snippet posted!', 'film');
      if (State.currentView === 'snippets') navigateTo('snippets');
    }
    postBtn.disabled = false;
    postBtn.innerHTML = '<i class="fa-solid fa-film"></i> Post Snippet';
  });
}

/* ── Links (like friends but with DM + Discord-style perks) ─── */
function openProfileEditModal(profile) {
  const modal = $('#modal-overlay');
  const body  = $('#modal-body');
  $('#modal-title-text').textContent = 'Edit Profile';
  modal.classList.add('open');

  const BANNER_COLORS = ['#0d1b2e', '#1a0d2e', '#0d2e1a', '#2e1a0d', '#1a1a2e', '#2e0d1a', '#0d2e2e'];

  body.innerHTML = `
    <div style="padding:16px;display:flex;flex-direction:column;gap:12px">

      <!-- Banner customizer -->
      <div>
        <label style="font-size:11px;font-weight:700;color:var(--text-secondary);text-transform:uppercase;letter-spacing:0.05em;margin-bottom:6px;display:block">Profile Banner</label>
        <div id="banner-preview" style="height:80px;border-radius:12px;background:${profile.banner_color || '#0d1b2e'};position:relative;overflow:hidden;margin-bottom:8px;border:1px solid var(--border)">
          ${profile.banner_url ? `<img src="${escapeHtml(profile.banner_url)}" style="width:100%;height:100%;object-fit:cover;position:absolute;inset:0">` : ''}
          <button id="banner-img-btn" style="position:absolute;right:8px;bottom:8px;background:rgba(0,0,0,0.6);border:none;border-radius:8px;color:#fff;padding:5px 10px;font-size:11px;font-weight:600;cursor:pointer"><i class="fa-solid fa-image"></i> Change Image</button>
          <input type="file" id="banner-img-input" accept="image/*" style="display:none">
        </div>
        <div style="display:flex;gap:6px;flex-wrap:wrap">
          ${BANNER_COLORS.map(c => `<button class="banner-color-btn" data-color="${c}" style="width:28px;height:28px;border-radius:8px;background:${c};border:2px solid ${(profile.banner_color||'#0d1b2e')===c?'var(--cyan)':'transparent'};cursor:pointer;transition:0.15s"></button>`).join('')}
          <input type="color" id="banner-custom-color" value="${profile.banner_color || '#0d1b2e'}" style="width:28px;height:28px;border-radius:8px;border:2px solid var(--border);cursor:pointer;padding:0;background:none">
        </div>
      </div>

      <!-- Avatar -->
      <div class="edit-avatar-section">
        <div id="edit-avatar-preview" class="edit-avatar-preview" style="background:linear-gradient(135deg,${profile.banner_color||'#ff2d6e'},'#ff6b35')">
          ${profile.avatar_url ? `<img src="${escapeHtml(profile.avatar_url)}" style="width:100%;height:100%;object-fit:cover;border-radius:50%">` : avatarInitials(profile.display_name || profile.username || 'U')}
        </div>
        <div class="edit-avatar-actions">
          <button class="edit-avatar-btn" id="change-avatar-btn"><i class="fa-solid fa-camera"></i> Change Photo</button>
          <input type="file" id="avatar-img-input" accept="image/*" style="display:none">
          <div style="font-size:11px;color:var(--text-muted)">Max 2MB · JPG, PNG, GIF</div>
        </div>
      </div>

      <div class="auth-input-group">
        <label>Display Name</label>
        <input type="text" id="edit-display-name" class="auth-input" value="${profile.display_name || ''}" placeholder="Your name" maxlength="50" autocomplete="name">
      </div>
      <div class="auth-input-group">
        <label>Username</label>
        <input type="text" id="edit-username" class="auth-input" value="${profile.username || ''}" placeholder="username" maxlength="30" autocomplete="username">
      </div>
      <div class="auth-input-group">
        <label>Bio</label>
        <textarea id="edit-bio" class="auth-input" placeholder="Tell the world about yourself" rows="3" style="resize:vertical" autocomplete="off">${profile.bio || ''}</textarea>
      </div>
      <div class="auth-input-group">
        <label>Location</label>
        <input type="text" id="edit-location" class="auth-input" value="${profile.location || ''}" placeholder="City, Country" autocomplete="address-level2">
      </div>
      <div class="auth-input-group">
        <label>Website</label>
        <input type="url" id="edit-website" class="auth-input" value="${profile.website || ''}" placeholder="https://yoursite.dev" autocomplete="url">
      </div>
      <div class="auth-input-group">

      </div>
      <div id="edit-status" style="font-size:12px;color:var(--text-muted);display:none"></div>
      <button class="auth-btn-primary" id="save-profile-btn">Save Changes</button>
    </div>
  `;

  let newBannerColor = profile.banner_color || '#0d1b2e';
  let newAvatarFile  = null;
  let newBannerFile  = null;

  // Banner color swatches
  document.querySelectorAll('.banner-color-btn').forEach(btn => {
    btn.addEventListener('click', () => {
      newBannerColor = btn.dataset.color;
      document.querySelectorAll('.banner-color-btn').forEach(b => b.style.borderColor = 'transparent');
      btn.style.borderColor = 'var(--cyan)';
      const preview = document.getElementById('banner-preview');
      if (!newBannerFile) preview.style.background = newBannerColor;
      document.getElementById('banner-custom-color').value = newBannerColor;
    });
  });

  document.getElementById('banner-custom-color').addEventListener('input', e => {
    newBannerColor = e.target.value;
    document.getElementById('banner-preview').style.background = newBannerColor;
  });

  // Banner image
  document.getElementById('banner-img-btn').addEventListener('click', () => document.getElementById('banner-img-input').click());
  document.getElementById('banner-img-input').addEventListener('change', e => {
    const file = e.target.files[0];
    if (!file) return;
    newBannerFile = file;
    const url = URL.createObjectURL(file);
    const preview = document.getElementById('banner-preview');
    let img = preview.querySelector('img');
    if (!img) { img = document.createElement('img'); img.style.cssText = 'width:100%;height:100%;object-fit:cover;position:absolute;inset:0'; preview.insertBefore(img, preview.firstChild); }
    img.src = url;
  });

  // Avatar
  document.getElementById('change-avatar-btn').addEventListener('click', () => document.getElementById('avatar-img-input').click());
  document.getElementById('avatar-img-input').addEventListener('change', e => {
    const file = e.target.files[0];
    if (!file) return;
    if (file.size > 2 * 1024 * 1024) { toast('Avatar must be under 2MB', 'circle-exclamation'); return; }
    newAvatarFile = file;
    const url = URL.createObjectURL(file);
    const preview = document.getElementById('edit-avatar-preview');
    preview.innerHTML = `<img src="${url}" style="width:100%;height:100%;object-fit:cover;border-radius:50%">`;
  });

  document.getElementById('save-profile-btn').addEventListener('click', async () => {
    const btn = document.getElementById('save-profile-btn');
    btn.disabled = true; btn.textContent = 'Saving…';
    const statusEl = document.getElementById('edit-status');
    statusEl.style.display = 'block'; statusEl.textContent = 'Saving…';

    let avatarUrl = profile.avatar_url;
    let bannerUrl = profile.banner_url;

    // Helper: delete-then-upload so upsert quirks don't block us
    async function storageUpload(bucket, path, file) {
      // Remove existing file first (ignore error if it doesn't exist)
      await sb.storage.from(bucket).remove([path]);
      const { data, error } = await sb.storage
        .from(bucket)
        .upload(path, file, { contentType: file.type, cacheControl: '3600' });
      if (error) {
        console.error(`[Devit] storage upload failed bucket=${bucket} path=${path}`, JSON.stringify(error));
      }
      return error;
    }

    // Upload new avatar
    if (newAvatarFile) {
      const ext = (newAvatarFile.name.split('.').pop() || 'jpg').toLowerCase();
      // Keep path flat: USER_ID/avatar.ext — matches RLS policy foldername[1] = auth.uid()
      const path = `${State.user.id}/avatar.${ext}`;
      const avErr = await storageUpload('post-images', path, newAvatarFile);
      if (avErr) {
        btn.disabled = false; btn.textContent = 'Save Changes';
        statusEl.style.display = 'block';
        statusEl.style.color = 'var(--rose)';
        statusEl.textContent = 'Avatar upload failed: ' + (avErr.message || avErr.error || JSON.stringify(avErr));
        return;
      }
      avatarUrl = sb.storage.from('post-images').getPublicUrl(path).data.publicUrl + '?t=' + Date.now();
    }

    // Upload new banner — path: USER_ID/banner.ext (still under user folder, RLS matches)
    if (newBannerFile) {
      const ext = (newBannerFile.name.split('.').pop() || 'jpg').toLowerCase();
      const path = `${State.user.id}/banner.${ext}`;
      const bnErr = await storageUpload('post-images', path, newBannerFile);
      if (bnErr) {
        btn.disabled = false; btn.textContent = 'Save Changes';
        statusEl.style.display = 'block';
        statusEl.style.color = 'var(--rose)';
        statusEl.textContent = 'Banner upload failed: ' + (bnErr.message || bnErr.error || JSON.stringify(bnErr));
        return;
      }
      bannerUrl = sb.storage.from('post-images').getPublicUrl(path).data.publicUrl + '?t=' + Date.now();
    }

    const { error } = await sb.from('op_profiles').update({
      display_name: document.getElementById('edit-display-name').value.trim(),
      username: document.getElementById('edit-username').value.trim().toLowerCase().replace(/[^a-z0-9_]/g, '_'),
      bio: document.getElementById('edit-bio').value.trim(),
      location: document.getElementById('edit-location').value.trim(),
      website: document.getElementById('edit-website').value.trim(),
      avatar_url: avatarUrl,
      banner_url: bannerUrl,
      banner_color: newBannerColor,
    }).eq('id', State.user.id);

    if (error) {
      statusEl.style.color = 'var(--rose)'; statusEl.textContent = 'Failed: ' + error.message;
      btn.disabled = false; btn.textContent = 'Save Changes';
    } else {
      const { data: updated } = await sb.from('op_profiles').select('*').eq('id', State.user.id).single();
      State.profile = updated;
      modal.classList.remove('open');
      toast('Profile updated!', 'pen');
      navigateTo('profile');
    }
  });
}

/* ── Content Moderation ─────────────────────────────────────── */

function openOwnPostMenu(anchorBtn, post, profile, card) {
  document.getElementById('own-post-menu')?.remove();

  const menu = document.createElement('div');
  menu.id = 'own-post-menu';
  menu.style.cssText = 'position:fixed;z-index:1000;background:var(--bg-surface);border:1px solid var(--border);border-radius:12px;box-shadow:0 16px 48px rgba(0,0,0,0.5);min-width:180px;overflow:hidden;font-size:13px';

  const items = [
    { id: 'opm-edit',   icon: 'fa-pen',        label: 'Edit Post',       color: 'var(--text-primary)' },
    { id: 'opm-pin',    icon: 'fa-thumbtack',   label: 'Pin to Profile',  color: 'var(--cyan)' },
    { id: 'opm-delete', icon: 'fa-trash',       label: 'Delete Post',     color: 'var(--rose)' },
  ];

  menu.innerHTML = items.map(it => `
    <button id="${it.id}" style="display:flex;align-items:center;gap:10px;width:100%;padding:12px 16px;background:none;border:none;color:${it.color};cursor:pointer;font-size:13px;transition:background 0.15s;text-align:left">
      <i class="fa-solid ${it.icon}" style="width:14px;text-align:center"></i> ${it.label}
    </button>`).join('');

  menu.querySelectorAll('button').forEach(b => {
    b.addEventListener('mouseenter', () => b.style.background = 'var(--bg-elevated)');
    b.addEventListener('mouseleave', () => b.style.background = '');
  });

  document.body.appendChild(menu);

  // Position near button
  const rect = anchorBtn.getBoundingClientRect();
  const mw = 180;
  let left = rect.right - mw;
  if (left < 8) left = 8;
  menu.style.top  = (rect.bottom + 4) + 'px';
  menu.style.left = left + 'px';

  const dismiss = e => {
    if (!menu.contains(e.target) && e.target !== anchorBtn) {
      menu.remove();
      document.removeEventListener('pointerdown', dismiss);
    }
  };
  setTimeout(() => document.addEventListener('pointerdown', dismiss), 50);

  // ── Edit ──
  menu.querySelector('#opm-edit').addEventListener('click', () => {
    menu.remove();
    openEditPostModal(post, card);
  });

  // ── Pin ──
  menu.querySelector('#opm-pin').addEventListener('click', async () => {
    menu.remove();
    if (typeof pinPost === 'function') {
      await pinPost(post.id);
    } else {
      toast('Pin feature loading…', 'thumbtack');
    }
  });

  // ── Delete ──
  menu.querySelector('#opm-delete').addEventListener('click', () => {
    menu.remove();
    const modal  = document.getElementById('modal-overlay');
    const body   = document.getElementById('modal-body');
    const title  = document.getElementById('modal-title-text');
    if (!modal || !body) return;
    title.textContent = 'Delete Post';
    modal.classList.add('open');
    body.innerHTML = `
      <div style="padding:24px;display:flex;flex-direction:column;gap:16px">
        <p style="font-size:14px;color:var(--text-secondary);margin:0">Are you sure you want to delete this post? This cannot be undone.</p>
        <div style="display:flex;gap:10px">
          <button id="confirm-delete-post" style="flex:1;padding:10px;border-radius:8px;background:var(--rose);border:none;color:#fff;font-weight:700;cursor:pointer;font-size:13px">Delete</button>
          <button id="cancel-delete-post" style="flex:1;padding:10px;border-radius:8px;background:var(--bg-elevated);border:1px solid var(--border);color:var(--text-primary);font-weight:600;cursor:pointer;font-size:13px">Cancel</button>
        </div>
      </div>`;
    document.getElementById('confirm-delete-post').addEventListener('click', async () => {
      modal.classList.remove('open');
      const { error } = await sb.from('op_posts').delete().eq('id', post.id);
      if (error) { toast('Failed to delete: ' + error.message, 'circle-exclamation'); return; }
      card.style.transition = 'opacity 0.3s, transform 0.3s';
      card.style.opacity = '0';
      card.style.transform = 'translateY(-8px)';
      setTimeout(() => card.remove(), 300);
      toast('Post deleted', 'trash');
    });
    document.getElementById('cancel-delete-post').addEventListener('click', () => modal.classList.remove('open'));
  });
}

function openEditPostModal(post, card) {
  const modal = document.getElementById('modal-overlay');
  const body  = document.getElementById('modal-body');
  const title = document.getElementById('modal-title-text');
  if (!modal || !body) return;
  title.textContent = 'Edit Post';
  modal.classList.add('open');

  body.innerHTML = `
    <div style="padding:16px;display:flex;flex-direction:column;gap:12px">
      <textarea id="edit-post-textarea" class="composer-textarea" rows="5" style="width:100%;resize:vertical">${(post.content || '').replace(/</g,'&lt;').replace(/>/g,'&gt;')}</textarea>
      <div id="edit-post-status" style="font-size:12px;color:var(--text-muted);display:none"></div>
      <div style="display:flex;gap:10px">
        <button id="save-edit-post" class="auth-btn-primary" style="flex:1">Save Changes</button>
        <button id="cancel-edit-post" style="flex:1;padding:10px;border-radius:8px;background:var(--bg-elevated);border:1px solid var(--border);color:var(--text-primary);font-weight:600;cursor:pointer;font-size:13px">Cancel</button>
      </div>
    </div>`;

  document.getElementById('cancel-edit-post').addEventListener('click', () => modal.classList.remove('open'));

  document.getElementById('save-edit-post').addEventListener('click', async () => {
    const newContent = document.getElementById('edit-post-textarea').value.trim();
    if (!newContent) { toast('Post cannot be empty', 'circle-exclamation'); return; }
    const btn = document.getElementById('save-edit-post');
    btn.disabled = true; btn.textContent = 'Saving…';
    const { error } = await sb.from('op_posts').update({ content: newContent }).eq('id', post.id);
    if (error) {
      btn.disabled = false; btn.textContent = 'Save Changes';
      toast('Failed: ' + error.message, 'circle-exclamation');
      return;
    }
    modal.classList.remove('open');
    // Update the card text in place
    const contentEl = card?.querySelector('.post-content');
    if (contentEl) {
      contentEl.innerHTML = newContent
        .replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;')
        .replace(/#(\w+)/g, '<span class="hashtag" data-tag="$1">#$1</span>')
        .replace(/@(\w+)/g, '<span class="mention">@$1</span>');
    }
    post.content = newContent;
    toast('Post updated!', 'pen');
  });
}

function openPostMoreMenu(anchorBtn, postId, authorId) {
  // Remove any existing menu
  document.getElementById('post-more-menu')?.remove();
  const menu = document.createElement('div');
  menu.id = 'post-more-menu';
  menu.style.cssText = `position:fixed;z-index:1000;background:var(--bg-surface);border:1px solid var(--border);border-radius:12px;box-shadow:0 16px 48px rgba(0,0,0,0.5);min-width:170px;overflow:hidden;font-size:13px`;
  menu.innerHTML = `
    <button class="post-more-item" id="pmi-report-post" style="display:flex;align-items:center;gap:10px;width:100%;padding:12px 16px;background:none;border:none;color:var(--text-primary);cursor:pointer;transition:background 0.15s">
      <i class="fa-solid fa-flag" style="color:var(--amber,#fb923c)"></i> Report post
    </button>
    <button class="post-more-item" id="pmi-block-user" style="display:flex;align-items:center;gap:10px;width:100%;padding:12px 16px;background:none;border:none;color:var(--rose,#f87171);cursor:pointer;transition:background 0.15s">
      <i class="fa-solid fa-ban"></i> Block user
    </button>
  `;
  menu.querySelectorAll('.post-more-item').forEach(b => {
    b.addEventListener('mouseenter', () => b.style.background = 'var(--bg-elevated)');
    b.addEventListener('mouseleave', () => b.style.background = '');
  });
  document.body.appendChild(menu);

  // Position near button
  const rect = anchorBtn.getBoundingClientRect();
  const mw = 170;
  let left = rect.right - mw;
  if (left < 8) left = 8;
  menu.style.top = (rect.bottom + 4) + 'px';
  menu.style.left = left + 'px';

  menu.querySelector('#pmi-report-post').addEventListener('click', () => { menu.remove(); openReportModal('post', postId); });
  menu.querySelector('#pmi-block-user').addEventListener('click', () => { menu.remove(); confirmBlockUser(authorId); });

  const dismiss = e => { if (!menu.contains(e.target) && e.target !== anchorBtn) { menu.remove(); document.removeEventListener('pointerdown', dismiss); } };
  setTimeout(() => document.addEventListener('pointerdown', dismiss), 50);
}

function openReportModal(type, targetId) {
  const modal = $('#modal-overlay');
  $('#modal-title-text').textContent = 'Report ' + (type === 'post' ? 'Post' : 'User');
  modal.classList.add('open');

  const reasons = ['Spam or misleading', 'Harassment or bullying', 'Hate speech', 'Violent or harmful content', 'Misinformation', 'Other'];
  $('#modal-body').innerHTML = `
    <div style="padding:16px;display:flex;flex-direction:column;gap:12px">
      <p style="font-size:13px;color:var(--text-secondary);margin:0">Why are you reporting this ${type}?</p>
      <div id="report-reasons" style="display:flex;flex-direction:column;gap:6px">
        ${reasons.map((r, i) => `<label style="display:flex;align-items:center;gap:10px;padding:10px 12px;border-radius:8px;cursor:pointer;border:1px solid var(--border);transition:border-color 0.15s" class="report-reason-label">
          <input type="radio" name="report-reason" value="${escapeHtml(r)}" style="accent-color:var(--cyan)"> <span style="font-size:13px">${escapeHtml(r)}</span>
        </label>`).join('')}
      </div>
      <textarea id="report-extra" class="auth-input" placeholder="Additional details (optional)" rows="3" style="resize:vertical;font-size:13px"></textarea>
      <button id="submit-report-btn" class="auth-btn-primary" style="margin-top:4px"><i class="fa-solid fa-flag"></i> Submit Report</button>
      <div id="report-status" style="display:none;font-size:12px;text-align:center;color:var(--text-muted)"></div>
    </div>
  `;

  $$('.report-reason-label').forEach(l => {
    l.querySelector('input').addEventListener('change', () => {
      $$('.report-reason-label').forEach(x => x.style.borderColor = 'var(--border)');
      l.style.borderColor = 'var(--cyan)';
    });
  });

  $('#submit-report-btn').addEventListener('click', async () => {
    const reason = document.querySelector('input[name="report-reason"]:checked')?.value;
    if (!reason) { toast('Please select a reason', 'circle-exclamation'); return; }
    const extra = $('#report-extra').value.trim();
    const btn = $('#submit-report-btn');
    btn.disabled = true; btn.textContent = 'Submitting…';
    const { error } = await sb.from('op_reports').insert({
      reporter_id: State.user.id,
      target_type: type,
      target_id: targetId,
      reason,
      details: extra || null,
    });
    if (error) {
      btn.disabled = false; btn.textContent = 'Submit Report';
      toast('Failed: ' + error.message, 'circle-exclamation');
    } else {
      modal.classList.remove('open');
      toast('Report submitted. Thank you.', 'flag');
    }
  });
}

async function confirmBlockUser(userId) {
  if (!userId) return;
  const { data: profile } = await sb.from('op_profiles').select('display_name, username').eq('id', userId).single();
  const name = profile?.display_name || profile?.username || 'this user';
  const modal = $('#modal-overlay');
  $('#modal-title-text').textContent = 'Block User';
  modal.classList.add('open');
  $('#modal-body').innerHTML = `
    <div style="padding:16px;display:flex;flex-direction:column;gap:16px">
      <p style="font-size:14px;color:var(--text-primary);margin:0">Block <strong>${escapeHtml(name)}</strong>?</p>
      <p style="font-size:13px;color:var(--text-secondary);margin:0">They won't be able to see your posts or DM you. Their content will be hidden from your feed.</p>
      <div style="display:flex;gap:10px">
        <button id="confirm-block-btn" style="flex:1;padding:10px;border-radius:8px;background:var(--rose,#f87171);border:none;color:#fff;font-weight:700;cursor:pointer;font-size:13px">Block</button>
        <button id="cancel-block-btn" style="flex:1;padding:10px;border-radius:8px;background:var(--bg-elevated);border:1px solid var(--border);color:var(--text-primary);font-weight:600;cursor:pointer;font-size:13px">Cancel</button>
      </div>
    </div>
  `;
  $('#confirm-block-btn').addEventListener('click', async () => {
    const { error } = await sb.from('op_blocks').insert({ blocker_id: State.user.id, blocked_id: userId });
    modal.classList.remove('open');
    if (error && error.code !== '23505') { toast('Error: ' + error.message, 'circle-exclamation'); return; }
    toast(`${name} blocked.`, 'ban');
    // Remove their cards from current view
    document.querySelectorAll(`.post-card [data-uid="${userId}"]`).forEach(el => el.closest('.post-card')?.remove());
  });
  $('#cancel-block-btn').addEventListener('click', () => modal.classList.remove('open'));
}

/* ── Web Push Notifications ──────────────────────────────────── */
// VAPID public key — replace with your own from: npx web-push generate-vapid-keys
const VAPID_PUBLIC_KEY = 'BEl62iUYgUivxIkv69yViEuiBIa-Ib9-SkvMeAtA3LFgDzkrxZJjSgSnfckjBJuBkr3qBkYIL55lLpurs1A';

async function registerPushNotifications() {
  if (!('serviceWorker' in navigator) || !('PushManager' in window)) return;
  try {
    const reg = await navigator.serviceWorker.ready;
    const existing = await reg.pushManager.getSubscription();
    if (existing) return; // already subscribed

    const permission = await Notification.requestPermission();
    if (permission !== 'granted') return;

    const sub = await reg.pushManager.subscribe({
      userVisibleOnly: true,
      applicationServerKey: urlBase64ToUint8Array(VAPID_PUBLIC_KEY),
    });
    // Save subscription to Supabase
    await sb.from('op_push_subscriptions').upsert({
      user_id: State.user.id,
      endpoint: sub.endpoint,
      keys: JSON.stringify({ p256dh: btoa(String.fromCharCode(...new Uint8Array(sub.getKey('p256dh')))), auth: btoa(String.fromCharCode(...new Uint8Array(sub.getKey('auth')))) }),
    }, { onConflict: 'user_id,endpoint' });
  } catch (err) { console.warn('Push registration failed:', err); }
}

function urlBase64ToUint8Array(base64String) {
  const padding = '='.repeat((4 - base64String.length % 4) % 4);
  const base64 = (base64String + padding).replace(/-/g, '+').replace(/_/g, '/');
  const raw = atob(base64);
  return Uint8Array.from([...raw].map(c => c.charCodeAt(0)));
}

async function registerServiceWorker() {
  if (!('serviceWorker' in navigator)) return;
  try {
    await navigator.serviceWorker.register('./sw.js');
  } catch (err) { console.warn('SW registration failed:', err); }
}

/* ── Boot ───────────────────────────────────────────────────── */
document.addEventListener('DOMContentLoaded', () => {
  initAuth();

  document.addEventListener('keydown', e => {
    if (e.key === 'Escape') {
      document.getElementById('modal-overlay')?.classList.remove('open');
      closeSearch();
    }
  });
});


/* ============================================================
   DEVIT — Features Patch v2 (merged)
   GitHub autofill · Polls · Digest widget · Read time/Views · Pinned posts
   + Soft UI overhaul styles (injected)
   ============================================================ */
/* ============================================================
   DEVIT — Features Patch v2
   devit-features.patch.js

   Adds:
   6. GitHub OAuth → auto-fill profile (repos, bio, location)
   7. Polls in posts
   8. Weekly digest / dev newsletter widget
   9. Reading time + post views counter
  10. Pinned posts on profile (up to 3)
   + Full UI softness overhaul (injected CSS)
   ============================================================ */

// (strict mode inherited from app.js)

/* ── 0. Inject soft UI overhaul styles ──────────────────────── */
(function injectSoftUI() {
  const style = document.createElement('style');
  style.id = 'devit-soft-ui';
  style.textContent = `
    /* ── Soft UI Overhaul ─────────────────────────────────── */
    :root {
      --soft-radius:    20px;
      --soft-shadow:    0 2px 16px rgba(0,0,0,0.18), 0 1px 4px rgba(0,0,0,0.12);
      --soft-shadow-lg: 0 8px 40px rgba(0,0,0,0.28), 0 2px 8px rgba(0,0,0,0.14);
      --soft-blur:      blur(18px);
      --ease-out-expo:  cubic-bezier(0.16,1,0.3,1);
      --ease-spring:    cubic-bezier(0.34,1.4,0.64,1);
      --transition-soft: 0.22s var(--ease-out-expo);
    }

    /* Topbar — softer, frosted */
    #topbar {
      border-bottom: 1px solid rgba(255,255,255,0.055) !important;
      box-shadow: 0 1px 0 rgba(255,255,255,0.03), 0 4px 24px rgba(0,0,0,0.2) !important;
    }

    /* Post cards — softer corners, breathing room */
    .post-card {
      border-radius: 18px !important;
      border: 1px solid rgba(255,255,255,0.055) !important;
      box-shadow: var(--soft-shadow) !important;
      transition: transform var(--transition-soft), box-shadow var(--transition-soft), border-color var(--transition-soft) !important;
      will-change: transform;
    }
    .post-card:hover {
      border-color: rgba(255,45,110,0.1) !important;
    }

    /* Sidebar — softer links */
    .sidebar-link {
      border-radius: 12px !important;
      transition: background var(--transition-soft), color var(--transition-soft) !important;
    }
    .sidebar-link.active {
      background: linear-gradient(135deg, rgba(255,45,110,0.12), rgba(255,107,53,0.08)) !important;
      box-shadow: inset 0 1px 0 rgba(255,45,110,0.08) !important;
    }

    /* Right sidebar widgets */
    .widget {
      border-radius: 18px !important;
      border: 1px solid rgba(255,255,255,0.055) !important;
      box-shadow: var(--soft-shadow) !important;
      backdrop-filter: blur(8px);
    }

    /* Composer */
    .composer {
      border-radius: 20px !important;
      border: 1px solid rgba(255,255,255,0.06) !important;
      box-shadow: var(--soft-shadow) !important;
    }
    .composer-inner {
      border-radius: 14px !important;
    }

    /* Auth card */
    .auth-card {
      background: rgba(16,18,26,0.85) !important;
      backdrop-filter: var(--soft-blur) !important;
    }
    .auth-input {
      border-radius: 14px !important;
      transition: border-color var(--transition-soft), box-shadow var(--transition-soft) !important;
    }
    .auth-btn-primary {
      border-radius: 14px !important;
      transition: transform var(--transition-soft), box-shadow var(--transition-soft) !important;
    }
    .auth-btn-primary:hover {
      transform: translateY(-2px) !important;
      box-shadow: 0 8px 24px rgba(255,45,110,0.25) !important;
    }
    .auth-btn-github, .auth-btn-google {
      border-radius: 14px !important;
      transition: transform var(--transition-soft), box-shadow var(--transition-soft) !important;
    }
    .auth-btn-github:hover, .auth-btn-google:hover {
      transform: translateY(-2px) !important;
    }

    /* Modal */
    .modal {
      border-radius: 24px !important;
      box-shadow: var(--soft-shadow-lg) !important;
      border: 1px solid rgba(255,255,255,0.07) !important;
    }

    /* Bottom nav */
    #bottom-nav {
      border-top: 1px solid rgba(255,255,255,0.055) !important;
      backdrop-filter: var(--soft-blur) !important;
      box-shadow: 0 -4px 24px rgba(0,0,0,0.2) !important;
    }
    .bnav-btn {
      border-radius: 14px !important;
      transition: background var(--transition-soft), color var(--transition-soft), transform var(--transition-soft) !important;
    }
    .bnav-btn.active { transform: scale(1.08) !important; }

    /* Mobile FAB */
    #mobile-fab {
      border-radius: 20px !important;
      box-shadow: 0 8px 32px rgba(255,45,110,0.35), 0 2px 8px rgba(0,0,0,0.3) !important;
      transition: transform var(--ease-spring) 0.1s, box-shadow var(--transition-soft) !important;
    }
    #mobile-fab:hover { transform: scale(1.08) rotate(8deg) !important; }
    #mobile-fab:active { transform: scale(0.94) !important; }

    /* Toast */
    .toast {
      border-radius: 14px !important;
      box-shadow: var(--soft-shadow-lg) !important;
      backdrop-filter: blur(12px) !important;
    }

    /* Buttons generally */
    .btn, button[class*="auth-btn"] {
      border-radius: 12px !important;
    }

    /* Action buttons on posts */
    .post-action {
      border-radius: 10px !important;
      transition: background var(--transition-soft), color var(--transition-soft), transform var(--transition-soft) !important;
    }
    .post-action:hover { transform: scale(1.05) !important; }

    /* Tags/chips */
    .post-tag {
      border-radius: 8px !important;
    }

    /* View tabs */
    .view-tabs {
      border-radius: 14px !important;
      padding: 4px !important;
      gap: 2px !important;
    }
    .view-tab {
      border-radius: 10px !important;
      transition: background var(--transition-soft), color var(--transition-soft) !important;
    }

    /* Profile avatar circle */
    .profile-avatar-circle {
      box-shadow: 0 0 0 2px rgba(255,255,255,0.07) !important;
      transition: box-shadow var(--transition-soft) !important;
    }
    .profile-avatar-circle:hover {
      box-shadow: 0 0 0 3px rgba(255,45,110,0.3) !important;
    }

    /* ── Polls ─────────────────────────────────────────────── */
    .poll-container {
      background: linear-gradient(135deg, rgba(255,45,110,0.05), rgba(255,255,255,0.015));
      border: 1px solid rgba(255,45,110,0.18);
      border-radius: 16px;
      padding: 14px 16px;
      margin-top: 12px;
    }
    .poll-question {
      font-weight: 600;
      font-size: 15px;
      color: var(--text-primary);
      margin-bottom: 12px;
    }
    .poll-option {
      position: relative;
      display: flex;
      align-items: center;
      gap: 10px;
      padding: 10px 14px;
      border-radius: 12px;
      border: 1px solid rgba(255,255,255,0.08);
      margin-bottom: 8px;
      cursor: pointer;
      overflow: hidden;
      transition: border-color 0.2s, transform 0.15s;
      background: rgba(255,255,255,0.02);
    }
    .poll-option:hover { border-color: rgba(255,45,110,0.3); transform: translateX(2px); }
    .poll-option.voted { cursor: default; pointer-events: none; }
    .poll-option.voted.winner { border-color: rgba(255,45,110,0.4); }
    .poll-fill {
      position: absolute;
      left: 0; top: 0; bottom: 0;
      border-radius: 11px;
      transition: width 0.7s var(--ease-out-expo);
      pointer-events: none;
    }
    .poll-option:not(.voted) .poll-fill { display: none; }
    .poll-option-text {
      position: relative; z-index: 1;
      font-size: 14px; font-weight: 500;
      color: var(--text-primary); flex: 1;
    }
    .poll-option-pct {
      position: relative; z-index: 1;
      font-size: 12px; font-weight: 700;
      color: var(--text-secondary);
      min-width: 36px; text-align: right;
    }
    .poll-meta {
      font-size: 11px; color: var(--text-muted);
      margin-top: 8px; display: flex; gap: 12px;
    }
    .poll-meta i { margin-right: 4px; }
    /* Composer poll builder */
    .poll-builder {
      margin-top: 12px;
      background: rgba(255,45,110,0.04);
      border: 1px solid rgba(255,45,110,0.12);
      border-radius: 16px;
      padding: 14px;
    }
    .poll-builder-title {
      font-size: 12px; font-weight: 700;
      color: var(--cyan); text-transform: uppercase; letter-spacing: 0.06em;
      margin-bottom: 10px;
    }
    .poll-option-input {
      width: 100%;
      padding: 9px 12px;
      background: var(--bg-surface);
      border: 1px solid var(--border);
      border-radius: 10px;
      color: var(--text-primary);
      font-size: 13px;
      margin-bottom: 7px;
      outline: none;
      transition: border-color 0.2s;
    }
    .poll-option-input:focus { border-color: var(--cyan); }
    .poll-add-option-btn {
      font-size: 12px; font-weight: 600;
      color: var(--cyan); padding: 6px 10px;
      border-radius: 8px;
      transition: background 0.15s;
    }
    .poll-add-option-btn:hover { background: var(--cyan-dim); }
    .poll-duration-row {
      display: flex; align-items: center; gap: 8px;
      margin-top: 10px; font-size: 12px; color: var(--text-secondary);
    }
    .poll-duration-row select {
      background: var(--bg-surface); border: 1px solid var(--border);
      border-radius: 8px; color: var(--text-primary); font-size: 12px;
      padding: 5px 8px; outline: none;
    }

    /* ── Reading time + views ──────────────────────────────── */
    .post-read-meta {
      display: flex; align-items: center; gap: 10px;
      font-size: 11px; color: var(--text-muted);
      margin-top: 6px;
    }
    .post-read-meta i { font-size: 10px; }
    .post-views-badge {
      display: inline-flex; align-items: center; gap: 4px;
      font-size: 11px; color: var(--text-muted);
    }

    /* ── Pinned posts ──────────────────────────────────────── */
    .pin-badge {
      display: inline-flex; align-items: center; gap: 5px;
      font-size: 10px; font-weight: 700; color: var(--amber);
      text-transform: uppercase; letter-spacing: 0.07em;
      background: rgba(251,191,36,0.1);
      border: 1px solid rgba(251,191,36,0.2);
      border-radius: 6px; padding: 2px 7px;
      margin-bottom: 6px;
    }
    .pin-badge i { font-size: 9px; }
    .pinned-section {
      margin-bottom: 20px;
    }
    .pinned-section-header {
      font-size: 11px; font-weight: 700;
      color: var(--amber); text-transform: uppercase;
      letter-spacing: 0.07em; margin-bottom: 10px;
      display: flex; align-items: center; gap: 6px;
    }

    /* ── Weekly digest widget ──────────────────────────────── */
    .digest-widget {
      border-radius: 18px !important;
    }
    .digest-header {
      display: flex; align-items: center; justify-content: space-between;
      margin-bottom: 14px;
    }
    .digest-title {
      font-size: 13px; font-weight: 800;
      color: var(--text-primary);
      display: flex; align-items: center; gap: 7px;
    }
    .digest-title i { color: var(--cyan); }
    .digest-badge {
      font-size: 10px; font-weight: 700;
      background: linear-gradient(90deg, #ff2d6e, #ff6b35);
      color: var(--bg-void);
      padding: 2px 8px; border-radius: 20px;
    }
    .digest-section-label {
      font-size: 10px; font-weight: 700;
      text-transform: uppercase; letter-spacing: 0.08em;
      color: var(--text-muted); margin: 10px 0 6px;
    }
    .digest-post-item {
      display: flex; align-items: flex-start; gap: 10px;
      padding: 8px 0; border-bottom: 1px solid rgba(255,255,255,0.04);
      cursor: pointer; transition: opacity 0.15s;
    }
    .digest-post-item:last-child { border-bottom: none; }
    .digest-post-item:hover { opacity: 0.8; }
    .digest-rank {
      font-size: 11px; font-weight: 800;
      color: var(--text-muted); min-width: 18px;
      line-height: 1.6;
    }
    .digest-post-title {
      font-size: 12px; font-weight: 600;
      color: var(--text-primary); line-height: 1.4; flex: 1;
    }
    .digest-tag-item {
      display: inline-flex; align-items: center; gap: 5px;
      padding: 4px 10px; border-radius: 20px;
      background: rgba(255,45,110,0.07);
      border: 1px solid rgba(255,45,110,0.12);
      font-size: 11px; font-weight: 600;
      color: var(--cyan); margin: 0 4px 4px 0;
      cursor: pointer; transition: background 0.15s;
    }
    .digest-tag-item:hover { background: rgba(255,45,110,0.14); }
    .digest-new-member {
      display: flex; align-items: center; gap: 8px;
      padding: 6px 0;
    }
    .digest-member-name {
      font-size: 12px; font-weight: 600; color: var(--text-primary);
    }
    .digest-member-handle {
      font-size: 11px; color: var(--text-muted);
    }

    /* ── GitHub profile banner ─────────────────────────────── */
    .github-profile-banner {
      background: linear-gradient(135deg, rgba(255,45,110,0.08), rgba(255,107,53,0.06));
      border: 1px solid rgba(255,45,110,0.15);
      border-radius: 16px;
      padding: 14px 16px;
      margin-top: 12px;
      display: flex; align-items: flex-start; gap: 12px;
    }
    .github-banner-icon {
      width: 36px; height: 36px;
      background: rgba(255,255,255,0.08);
      border-radius: 10px;
      display: flex; align-items: center; justify-content: center;
      font-size: 18px; color: var(--text-primary); flex-shrink: 0;
    }
    .github-banner-body { flex: 1; min-width: 0; }
    .github-banner-title {
      font-size: 13px; font-weight: 700;
      color: var(--text-primary); margin-bottom: 4px;
    }
    .github-banner-desc {
      font-size: 12px; color: var(--text-secondary); line-height: 1.5;
    }
    .github-repo-chip {
      display: inline-flex; align-items: center; gap: 5px;
      padding: 3px 9px; border-radius: 20px;
      background: rgba(255,255,255,0.06);
      border: 1px solid rgba(255,255,255,0.08);
      font-size: 11px; font-weight: 600;
      color: var(--text-secondary);
      margin: 4px 4px 0 0;
    }
    .github-repo-chip i { font-size: 10px; color: var(--cyan); }
  `;
  document.head.appendChild(style);
})();


/* ── Utility ────────────────────────────────────────────────── */
function readingTime(text) {
  const words = (text || '').trim().split(/\s+/).length;
  const mins = Math.max(1, Math.round(words / 200));
  return mins === 1 ? '1 min read' : `${mins} min read`;
}

function fmtViews(n) {
  if (!n) return '0';
  if (n >= 1_000_000) return (n / 1_000_000).toFixed(1) + 'M';
  if (n >= 1000) return (n / 1000).toFixed(1) + 'K';
  return String(n);
}

function escHtml(s) {
  return String(s || '').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
}
window.escHtml = window.escHtml || escHtml;
const _esc = escHtml;


/* ══════════════════════════════════════════════════════════════
   FEATURE 6 — GitHub OAuth → auto-fill profile
   ══════════════════════════════════════════════════════════════ */

async function handleGitHubProfileAutofill(session) {
  if (!session?.provider_token) return;
  const token = session.provider_token;
  try {
    // Fetch GitHub user
    const ghRes = await fetch('https://api.github.com/user', {
      headers: { Authorization: `Bearer ${token}` }
    });
    if (!ghRes.ok) return;
    const ghUser = await ghRes.json();

    // Fetch repos (top 6 by stars)
    const reposRes = await fetch('https://api.github.com/user/repos?sort=updated&per_page=6&type=owner', {
      headers: { Authorization: `Bearer ${token}` }
    });
    const repos = reposRes.ok ? await reposRes.json() : [];
    const topRepos = repos
      .sort((a,b) => (b.stargazers_count - a.stargazers_count))
      .slice(0, 6)
      .map(r => r.name);

    // Build update payload
    const update = {};
    if (ghUser.bio && !State?.profile?.bio) update.bio = ghUser.bio;
    if (ghUser.location && !State?.profile?.location) update.location = ghUser.location;
    if (ghUser.avatar_url && !State?.profile?.avatar_url) update.avatar_url = ghUser.avatar_url;
    if (ghUser.blog && !State?.profile?.website) update.website = ghUser.blog;
    if (topRepos.length > 0) {
      const existing = State?.profile?.tech_stack || [];
      update.tech_stack = [...new Set([...existing, ...topRepos])].slice(0, 12);
    }
    if (ghUser.name && !State?.profile?.display_name) update.display_name = ghUser.name;
    // Always mark as GitHub user so the badge shows throughout the app
    update.is_github = true;

    if (Object.keys(update).length === 0) return;

    // Apply to Supabase profile
    await sb.from('op_profiles').update(update).eq('id', session.user.id);

    // Show banner with auto-filled info
    showGitHubAutofillBanner(ghUser, topRepos);
  } catch(e) {
    console.warn('[Devit] GitHub autofill failed:', e);
  }
}

function showGitHubAutofillBanner(ghUser, repos) {
  const existing = document.getElementById('gh-autofill-banner');
  if (existing) existing.remove();

  const banner = document.createElement('div');
  banner.id = 'gh-autofill-banner';
  banner.className = 'github-profile-banner';
  banner.style.cssText = 'position:fixed;bottom:80px;right:20px;z-index:900;max-width:340px;animation:slideInRight 0.4s cubic-bezier(0.16,1,0.3,1)';

  const repoChips = repos.slice(0, 4).map(r =>
    `<span class="github-repo-chip"><i class="fa-solid fa-code-branch"></i>${escHtml(r)}</span>`
  ).join('');

  banner.innerHTML = `
    <div class="github-banner-icon"><i class="fa-brands fa-github"></i></div>
    <div class="github-banner-body">
      <div class="github-banner-title">Profile auto-filled from GitHub ✓</div>
      <div class="github-banner-desc">
        ${ghUser.bio ? `<em>${escHtml(ghUser.bio.slice(0, 80))}</em><br>` : ''}
        ${ghUser.location ? `<i class="fa-solid fa-location-dot" style="margin-right:4px;color:var(--cyan)"></i>${escHtml(ghUser.location)}<br>` : ''}
      </div>
      <div style="margin-top:6px">${repoChips}</div>
    </div>
    <button onclick="this.closest('#gh-autofill-banner').remove()" style="color:var(--text-muted);font-size:14px;padding:4px;flex-shrink:0">
      <i class="fa-solid fa-xmark"></i>
    </button>
  `;

  if (!document.getElementById('gh-autofill-anim')) {
    const s = document.createElement('style');
    s.id = 'gh-autofill-anim';
    s.textContent = `
      @keyframes slideInRight {
        from { opacity:0; transform:translateX(30px); }
        to   { opacity:1; transform:translateX(0); }
      }
    `;
    document.head.appendChild(s);
  }

  document.body.appendChild(banner);
  setTimeout(() => banner.remove(), 8000);
}

// Hook into Supabase auth state changes
if (typeof sb !== 'undefined') {
  sb.auth.onAuthStateChange(async (event, session) => {
    if (event === 'SIGNED_IN' && session?.user?.app_metadata?.provider === 'github') {
      // Small delay to let app.js handle user init first
      setTimeout(() => handleGitHubProfileAutofill(session), 1500);
    }
  });
}


/* ══════════════════════════════════════════════════════════════
   FEATURE 7 — Polls in posts
   ══════════════════════════════════════════════════════════════ */

const PollState = {
  active: false,
  options: ['', ''],
  durationDays: 7,
};

function renderPollBuilder() {
  const existing = document.getElementById('poll-builder-ui');
  if (existing) existing.remove();

  const builder = document.createElement('div');
  builder.id = 'poll-builder-ui';
  builder.className = 'poll-builder';
  builder.innerHTML = `
    <div class="poll-builder-title"><i class="fa-solid fa-chart-bar" style="margin-right:5px"></i>Poll options</div>
    <div id="poll-options-list">
      ${PollState.options.map((v, i) => `
        <div class="poll-option-row" style="display:flex;gap:6px;margin-bottom:7px">
          <input class="poll-option-input" type="text" placeholder="Option ${i+1}" value="${escHtml(v)}" data-poll-idx="${i}" maxlength="80" style="flex:1">
          ${i >= 2 ? `<button class="poll-rm-btn" data-poll-idx="${i}" style="color:var(--rose);font-size:13px;padding:0 8px">×</button>` : ''}
        </div>
      `).join('')}
    </div>
    <button class="poll-add-option-btn" id="poll-add-opt-btn" ${PollState.options.length >= 4 ? 'disabled style="opacity:0.4"' : ''}>
      <i class="fa-solid fa-plus" style="margin-right:4px"></i>Add option
    </button>
    <div class="poll-duration-row">
      <i class="fa-regular fa-clock" style="color:var(--cyan)"></i>
      <span>Duration:</span>
      <select id="poll-duration-sel">
        <option value="1" ${PollState.durationDays===1?'selected':''}>1 day</option>
        <option value="3" ${PollState.durationDays===3?'selected':''}>3 days</option>
        <option value="7" ${PollState.durationDays===7?'selected':''}>7 days</option>
        <option value="14" ${PollState.durationDays===14?'selected':''}>2 weeks</option>
      </select>
    </div>
  `;

  // Wire events
  builder.querySelectorAll('.poll-option-input').forEach(inp => {
    inp.addEventListener('input', () => {
      PollState.options[+inp.dataset.pollIdx] = inp.value;
    });
  });
  builder.querySelectorAll('.poll-rm-btn').forEach(btn => {
    btn.addEventListener('click', () => {
      const idx = +btn.dataset.pollIdx;
      PollState.options.splice(idx, 1);
      renderPollBuilder();
    });
  });
  builder.querySelector('#poll-add-opt-btn')?.addEventListener('click', () => {
    if (PollState.options.length < 4) {
      PollState.options.push('');
      renderPollBuilder();
    }
  });
  builder.querySelector('#poll-duration-sel').addEventListener('change', e => {
    PollState.durationDays = +e.target.value;
  });

  // Inject after composer toolbar
  const composerToolbar = document.querySelector('.composer-toolbar');
  const composerInner   = document.querySelector('.composer-inner');
  const composerEl      = composerToolbar?.parentElement || composerInner;
  if (composerEl) {
    // Insert after the toolbar if possible
    if (composerToolbar) {
      composerToolbar.insertAdjacentElement('afterend', builder);
    } else {
      composerEl.appendChild(builder);
    }
  }
}

function getPollData() {
  if (!PollState.active) return null;
  const opts = PollState.options.map(o => o.trim()).filter(Boolean);
  if (opts.length < 2) return null;
  return {
    options: opts,
    duration_days: PollState.durationDays,
    ends_at: new Date(Date.now() + PollState.durationDays * 86400000).toISOString(),
    votes: Object.fromEntries(opts.map(o => [o, 0])),
    voted_by: {},
  };
}

function timeRelative(date) {
  const diff = date - Date.now();
  if (diff <= 0) return 'now';
  const h = Math.floor(diff / 3600000);
  if (h < 24) return `in ${h}h`;
  return `in ${Math.floor(h/24)}d`;
}

// ── Poll rendering helper (pure — takes resolved counts/myVote) ──
function _renderPollHtml(poll, postId, counts, myVote, total) {
  const now = new Date();
  const endsAt = poll.ends_at ? new Date(poll.ends_at) : null;
  const isExpired = endsAt && now > endsAt;
  const hasVoted = !!myVote || isExpired;
  const maxVotes = counts ? Math.max(...Object.values(counts), 1) : 1;

  const optionHtml = poll.options.map(opt => {
    const votes = counts?.[opt] || 0;
    const pct = total > 0 ? Math.round((votes / total) * 100) : 0;
    const isWinner = hasVoted && votes === maxVotes && votes > 0;
    const isMyVote = myVote === opt;
    const fillColor = isMyVote
      ? 'linear-gradient(90deg,rgba(255,45,110,0.28),rgba(255,45,110,0.12))'
      : isWinner
        ? 'linear-gradient(90deg,rgba(255,107,53,0.22),rgba(255,107,53,0.08))'
        : 'linear-gradient(90deg,rgba(255,255,255,0.07),rgba(255,255,255,0.03))';
    const borderColor = isMyVote
      ? 'rgba(255,45,110,0.5)'
      : isWinner ? 'rgba(255,107,53,0.4)' : 'rgba(255,255,255,0.08)';

    return `
      <div class="poll-option${hasVoted ? ' voted' : ''}${isWinner && hasVoted ? ' winner' : ''}"
           data-poll-opt="${escapeHtml(opt)}" data-post-id="${escapeHtml(postId)}"
           style="border-color:${borderColor}">
        <div class="poll-fill" style="width:${hasVoted ? pct : 0}%;background:${fillColor}"></div>
        <span class="poll-option-text">${escapeHtml(opt)}${isMyVote ? ' <i class="fa-solid fa-check-circle" style="color:var(--cyan);font-size:11px"></i>' : ''}${isWinner && hasVoted && !isMyVote ? ' <i class="fa-solid fa-crown" style="color:var(--amber);font-size:10px"></i>' : ''}</span>
        ${hasVoted
          ? `<span class="poll-option-pct" style="${isMyVote ? 'color:var(--cyan);font-weight:800' : ''}">${pct}%</span>`
          : `<span class="poll-vote-cta">Vote</span>`}
      </div>`;
  }).join('');

  const timeLeft = endsAt && !isExpired
    ? `Ends ${timeRelative(endsAt)}`
    : isExpired ? 'Poll ended' : '';

  return `
    <div class="poll-container" data-poll-post="${escapeHtml(postId)}">
      <div class="poll-header-row">
        <span class="poll-tag"><i class="fa-solid fa-chart-bar"></i> Poll</span>
        ${!hasVoted ? `<span class="poll-tap-hint">Tap to vote</span>` : ''}
      </div>
      ${optionHtml}
      <div class="poll-meta">
        <span><i class="fa-solid fa-users"></i> ${fmtViews(total)} vote${total !== 1 ? 's' : ''}</span>
        ${timeLeft ? `<span><i class="fa-regular fa-clock"></i> ${escapeHtml(timeLeft)}</span>` : ''}
        ${!hasVoted ? `<span style="color:var(--cyan);font-weight:600"><i class="fa-solid fa-arrow-pointer"></i> vote to see results</span>` : ''}
      </div>
    </div>`;
}

// renderPollInPost: fetches live counts from poll_votes then renders.
// Called from buildPostCard — starts a fetch in background and patches DOM when ready.
function renderPollInPost(poll, postId, currentUserId) {
  if (!poll?.options?.length) return '';

  // Render immediately with skeleton counts (0), then patch asynchronously
  const placeholder = _renderPollHtml(poll, postId, {}, null, 0);

  // Kick off async hydration without blocking card render
  if (postId && currentUserId) {
    _hydratePollCounts(postId, currentUserId);
  }

  return placeholder;
}

// Fetch real counts + user's vote from poll_votes and patch DOM in place
async function _hydratePollCounts(postId, userId) {
  try {
    let counts = {}, myVote = null, total = 0;

    // Try RPC first
    const { data: rpcData, error: rpcErr } = await sb.rpc('get_poll_state', {
      p_post_ids: [postId],
      p_user_id: userId || null,
    });

    if (!rpcErr && rpcData?.length) {
      const row = rpcData[0];
      counts  = row.counts  || {};
      myVote  = row.my_vote || null;
      total   = Number(row.total) || 0;
    } else {
      // RPC not available — read votes directly from the poll JSON on the post
      const { data: postRow } = await sb.from('op_posts').select('poll').eq('id', postId).single();
      if (postRow?.poll) {
        const poll = postRow.poll;
        counts = poll.votes || {};
        total  = Object.values(counts).reduce((s, v) => s + v, 0);
        myVote = (poll.voted_by || {})[userId] || null;
      }
    }

    // Fetch poll options from post (needed for full render)
    const { data: post } = await sb.from('op_posts').select('poll').eq('id', postId).single();
    if (!post?.poll?.options?.length) return;

    const container = document.querySelector(`.poll-container[data-poll-post="${postId}"]`);
    if (!container) return;

    const temp = document.createElement('div');
    temp.innerHTML = _renderPollHtml(post.poll, postId, counts, myVote, total);
    const newNode = temp.firstElementChild;
    if (newNode) container.replaceWith(newNode);
  } catch (_) { /* non-critical */ }
}

// Delegated poll vote handler — uses cast_poll_vote RPC (atomic, race-safe)
document.addEventListener('click', async e => {
  const opt = e.target.closest('.poll-option:not(.voted)');
  if (!opt) return;
  const chosen = opt.dataset.pollOpt;
  const postId = opt.dataset.postId;
  if (!chosen || !postId) return;
  if (!window.State?.user) {
    toast('Sign in to vote', 'lock');
    return;
  }

  const userId = State.user.id;

  // Optimistic: mark this option as voted immediately
  const container = opt.closest('.poll-container');
  if (container) {
    container.querySelectorAll('.poll-option').forEach(o => {
      o.classList.add('voted');
      o.style.pointerEvents = 'none';
    });
    opt.querySelector('.poll-vote-cta')?.remove();
  }

  try {
    const { data, error } = await sb.rpc('cast_poll_vote', {
      p_post_id: postId,
      p_user_id: userId,
      p_option:  chosen,
    });

    if (error) {
      // RPC not set up — fall back to client-side vote via direct post update
      const { data: postData } = await sb.from('op_posts').select('poll').eq('id', postId).single();
      if (!postData?.poll?.options?.length) {
        toast('Vote failed: poll data not found', 'circle-exclamation');
        container?.querySelectorAll('.poll-option').forEach(o => { o.classList.remove('voted'); o.style.pointerEvents = ''; });
        return;
      }

      const poll = postData.poll;
      // Prevent double-voting client-side
      const votedBy = poll.voted_by || {};
      if (votedBy[userId]) {
        toast('You already voted!', 'circle-exclamation');
        container?.querySelectorAll('.poll-option').forEach(o => { o.classList.remove('voted'); o.style.pointerEvents = ''; });
        return;
      }

      const votes = { ...(poll.votes || {}) };
      votes[chosen] = (votes[chosen] || 0) + 1;
      votedBy[userId] = chosen;
      const updatedPoll = { ...poll, votes, voted_by: votedBy };

      const { error: updateErr } = await sb.from('op_posts').update({ poll: updatedPoll }).eq('id', postId);
      if (updateErr) {
        toast('Vote failed: ' + updateErr.message, 'circle-exclamation');
        container?.querySelectorAll('.poll-option').forEach(o => { o.classList.remove('voted'); o.style.pointerEvents = ''; });
        return;
      }

      // Build counts from the updated votes object
      const counts = votes;
      const total  = Object.values(votes).reduce((s, v) => s + v, 0);

      const fresh = document.querySelector(`.poll-container[data-poll-post="${postId}"]`);
      if (fresh) {
        const temp = document.createElement('div');
        temp.innerHTML = _renderPollHtml(updatedPoll, postId, counts, chosen, total);
        const newNode = temp.firstElementChild;
        if (newNode) fresh.replaceWith(newNode);
      }
      toast(`Voted: ${chosen}`, 'chart-bar');
      return;
    }

    // Re-render with confirmed counts from the server
    const counts = data.counts || {};
    const myVote = data.my_vote;
    const total  = Number(data.total) || 0;

    const { data: post } = await sb.from('op_posts').select('poll').eq('id', postId).single();
    if (!post?.poll?.options?.length) return;

    const fresh = document.querySelector(`.poll-container[data-poll-post="${postId}"]`);
    if (fresh) {
      const temp = document.createElement('div');
      temp.innerHTML = _renderPollHtml(post.poll, postId, counts, myVote, total);
      const newNode = temp.firstElementChild;
      if (newNode) fresh.replaceWith(newNode);
    }

    toast(`Voted: ${chosen}`, 'chart-bar');
  } catch (err) {
    toast('Vote failed', 'circle-exclamation');
  }
});

// Re-render all visible polls once user auth is confirmed.
async function refreshAllPollsForUser(userId) {
  if (!userId) return;
  const containers = document.querySelectorAll('.poll-container[data-poll-post]');
  if (!containers.length) return;
  const postIds = [...containers].map(c => c.dataset.pollPost).filter(Boolean);
  if (!postIds.length) return;

  try {
    // Fetch all vote states in one RPC call
    let stateMap = {};
    const { data: states, error: rpcErr } = await sb.rpc('get_poll_state', {
      p_post_ids: postIds,
      p_user_id: userId,
    });

    if (!rpcErr && states) {
      stateMap = Object.fromEntries((states || []).map(s => [s.post_id, s]));
    }
    // (If RPC fails, stateMap stays empty — we'll fall back per-post via the post's poll JSON)

    const { data: posts } = await sb.from('op_posts').select('id, poll').in('id', postIds);
    if (!posts) return;

    const postMap = Object.fromEntries((posts || []).map(p => [p.id, p]));

    for (const postId of postIds) {
      const post  = postMap[postId];
      if (!post?.poll?.options?.length) continue;

      const container = document.querySelector(`.poll-container[data-poll-post="${postId}"]`);
      if (!container) continue;

      let counts = {}, myVote = null, total = 0;
      if (stateMap[postId]) {
        counts = stateMap[postId]?.counts  || {};
        myVote = stateMap[postId]?.my_vote || null;
        total  = Number(stateMap[postId]?.total) || 0;
      } else {
        // Fallback: read from the poll JSON directly
        counts = post.poll.votes || {};
        total  = Object.values(counts).reduce((s, v) => s + v, 0);
        myVote = (post.poll.voted_by || {})[userId] || null;
      }

      const temp = document.createElement('div');
      temp.innerHTML = _renderPollHtml(post.poll, postId, counts, myVote, total);
      const newNode = temp.firstElementChild;
      if (newNode) container.replaceWith(newNode);
    }
  } catch (_) { /* non-critical */ }
}

// Inject poll toggle button into composer when it renders
function injectPollButtonIntoComposer() {
  // Target the composer toolbar specifically
  const toolbar = document.querySelector('.composer-toolbar');
  if (!toolbar || document.getElementById('poll-toggle-btn')) return;

  const pollBtn = document.createElement('button');
  pollBtn.id = 'poll-toggle-btn';
  pollBtn.title = 'Add a poll';
  pollBtn.className = 'composer-tool';
  pollBtn.innerHTML = '<i class="fa-solid fa-chart-bar"></i>';
  pollBtn.addEventListener('click', (e) => {
    e.preventDefault();
    e.stopPropagation();
    PollState.active = !PollState.active;
    pollBtn.style.color = PollState.active ? 'var(--cyan)' : '';
    pollBtn.style.background = PollState.active ? 'var(--cyan-dim)' : '';
    if (PollState.active) {
      PollState.options = ['', ''];
      renderPollBuilder();
    } else {
      document.getElementById('poll-builder-ui')?.remove();
    }
  });

  // Insert before the actions group
  const actionsGroup = toolbar.querySelector('.composer-actions');
  if (actionsGroup) {
    toolbar.insertBefore(pollBtn, actionsGroup);
  } else {
    toolbar.appendChild(pollBtn);
  }
}

// Poll button is injected when buildComposer() is called — see buildComposer()


/* ══════════════════════════════════════════════════════════════
   FEATURE 8 — Weekly digest / dev newsletter widget
   ══════════════════════════════════════════════════════════════ */

async function buildWeeklyDigestWidget() {
  const rightbar = document.getElementById('rightbar');
  if (!rightbar || document.getElementById('digest-widget')) return;

  // Fetch top posts from last 7 days
  const since = new Date(Date.now() - 7 * 86400000).toISOString();

  let topPosts = [], trendingTags = [], newMembers = [];

  try {
    const { data: posts } = await sb.from('op_posts')
      .select('id, content, tags, likes_count, comments_count, created_at, author_id')
      .gte('created_at', since)
      .order('likes_count', { ascending: false })
      .limit(5);
    topPosts = posts || [];

    // Trending tags (aggregate from posts)
    const { data: allPosts } = await sb.from('op_posts')
      .select('tags')
      .gte('created_at', since)
      .not('tags', 'is', null)
      .limit(100);
    const tagCounts = {};
    (allPosts || []).forEach(p => {
      (p.tags || []).forEach(t => { tagCounts[t] = (tagCounts[t] || 0) + 1; });
    });
    trendingTags = Object.entries(tagCounts)
      .sort((a,b) => b[1]-a[1])
      .slice(0, 6)
      .map(([t]) => t);

    // New members this week
    const { data: members } = await sb.from('op_profiles')
      .select('id, username, display_name, avatar_url')
      .gte('created_at', since)
      .order('created_at', { ascending: false })
      .limit(4);
    newMembers = members || [];
  } catch(e) {
    console.warn('[Devit] Digest fetch failed:', e);
  }

  const topPostsHtml = topPosts.length > 0
    ? topPosts.map((p, i) => `
        <div class="digest-post-item" data-post-id="${p.id}">
          <span class="digest-rank">${i+1}</span>
          <div class="digest-post-title">${escHtml((p.content || '').slice(0, 70))}${(p.content||'').length > 70 ? '…' : ''}</div>
          <span style="font-size:10px;color:var(--text-muted);white-space:nowrap">
            <i class="fa-solid fa-heart" style="color:var(--rose)"></i> ${p.likes_count || 0}
          </span>
        </div>
      `).join('')
    : `<div style="font-size:12px;color:var(--text-muted);padding:8px 0">No posts this week yet — be the first!</div>`;

  const tagsHtml = trendingTags.length > 0
    ? `<div style="display:flex;flex-wrap:wrap;gap:4px">${trendingTags.map(t =>
        `<span class="digest-tag-item" data-tag="${escHtml(t)}">#${escHtml(t)}</span>`
      ).join('')}</div>`
    : `<div style="font-size:12px;color:var(--text-muted)">No trending tags yet</div>`;

  const membersHtml = newMembers.length > 0
    ? newMembers.map(m => {
        const name = m.display_name || m.username || 'Dev';
        const color = ['#ff2d6e','#a78bfa','#34d399','#fb7185','#fbbf24'][name.charCodeAt(0) % 5];
        const avatar = m.avatar_url
          ? `<img src="${m.avatar_url}" style="width:28px;height:28px;border-radius:8px;object-fit:cover" onerror="this.style.display='none'">`
          : `<div style="width:28px;height:28px;border-radius:8px;background:${color};display:flex;align-items:center;justify-content:center;font-size:11px;font-weight:700;color:var(--bg-void)">${name[0].toUpperCase()}</div>`;
        return `
          <div class="digest-new-member">
            ${avatar}
            <div>
              <div class="digest-member-name">${escHtml(name)}</div>
              <div class="digest-member-handle">@${escHtml(m.username || '?')}</div>
            </div>
          </div>
        `;
      }).join('')
    : `<div style="font-size:12px;color:var(--text-muted)">No new members this week</div>`;

  const widget = document.createElement('div');
  widget.id = 'digest-widget';
  widget.className = 'widget digest-widget';
  widget.style.cssText = `
    background: var(--bg-surface);
    border: 1px solid var(--border);
    padding: 16px;
    margin-bottom: 12px;
  `;
  widget.innerHTML = `
    <div class="digest-header">
      <div class="digest-title">
        <i class="fa-solid fa-newspaper"></i>
        This Week on Devit
      </div>
      <span class="digest-badge">Weekly</span>
    </div>

    <div class="digest-section-label"><i class="fa-solid fa-fire" style="margin-right:4px;color:var(--rose)"></i>Top Posts</div>
    <div id="digest-top-posts">${topPostsHtml}</div>

    <div class="digest-section-label" style="margin-top:14px"><i class="fa-solid fa-hashtag" style="margin-right:4px;color:var(--cyan)"></i>Trending Tags</div>
    <div id="digest-tags">${tagsHtml}</div>

    ${newMembers.length > 0 ? `
      <div class="digest-section-label" style="margin-top:14px"><i class="fa-solid fa-user-plus" style="margin-right:4px;color:var(--emerald)"></i>New Members</div>
      <div id="digest-members">${membersHtml}</div>
    ` : ''}
  `;

  // Insert at top of rightbar
  rightbar.insertBefore(widget, rightbar.firstChild);

  // Wire tag clicks
  widget.querySelectorAll('.digest-tag-item').forEach(tag => {
    tag.addEventListener('click', () => {
      const t = tag.dataset.tag;
      if (window.navigateTo) navigateTo('feed');
      setTimeout(() => {
        const searchInput = document.querySelector('.topbar-search input, #search-input');
        if (searchInput) {
          searchInput.value = '#' + t;
          searchInput.dispatchEvent(new Event('input'));
        }
      }, 300);
    });
  });

  // Refresh weekly (poll every hour)
  setInterval(() => {
    widget.remove();
    buildWeeklyDigestWidget();
  }, 3600000);
}

// Boot digest widget after login
function tryInitDigest() {
  if (document.getElementById('rightbar')) {
    buildWeeklyDigestWidget();
  } else {
    const obs = new MutationObserver(() => {
      if (document.getElementById('rightbar')) {
        obs.disconnect();
        buildWeeklyDigestWidget();
      }
    });
    obs.observe(document.body, { childList: true, subtree: true });
  }
}

// Weekly digest: available via tryInitDigest() but not auto-booted on login


/* ══════════════════════════════════════════════════════════════
   FEATURE 9 — Reading time + post views counter
   ══════════════════════════════════════════════════════════════ */

// In-memory dedup set — seeded from Supabase on first use (see _ensureViewedPostsLoaded)
const viewedPosts = new Set();
let _viewedPostsLoaded = false;

async function _ensureViewedPostsLoaded() {
  if (_viewedPostsLoaded || !State.user?.id || State.isGuest) return;
  _viewedPostsLoaded = true;
  try {
    const { data } = await sb.from('op_post_views').select('post_id').eq('user_id', State.user.id);
    (data || []).forEach(r => viewedPosts.add(r.post_id));
  } catch(_) {}
}

async function recordPostView(postId) {
  await _ensureViewedPostsLoaded();
  if (viewedPosts.has(postId)) return;
  viewedPosts.add(postId);
  // Persist view to Supabase (fire-and-forget)
  if (State.user?.id && !State.isGuest) {
    sb.from('op_post_views').upsert(
      { user_id: State.user.id, post_id: postId },
      { onConflict: 'user_id,post_id' }
    ).catch(() => {});
  }
  // Increment view counter on the post
  try {
    await sb.rpc('increment_post_views', { post_id: postId });
  } catch(_) {
    await sb.from('op_posts')
      .update({ views_count: sb.raw('views_count + 1') })
      .eq('id', postId);
  }
}

// Inject reading time + views meta into post cards
function injectReadMetaIntoCard(card) {
  if (card.dataset.readMetaInjected) return;
  card.dataset.readMetaInjected = '1';

  const content = card.querySelector('.post-body, .post-content, .post-text, p');
  const text = content?.textContent || '';
  const rt = readingTime(text);

  const postId = card.dataset.postId || card.getAttribute('data-id') ||
    card.querySelector('[data-post-id]')?.dataset.postId;

  const viewsStr = card.dataset.views ? fmtViews(+card.dataset.views) : null;

  const meta = document.createElement('div');
  meta.className = 'post-read-meta';
  meta.innerHTML = `
    <span><i class="fa-regular fa-clock"></i>${escHtml(rt)}</span>
    ${viewsStr ? `<span class="post-views-badge"><i class="fa-regular fa-eye"></i>${escHtml(viewsStr)} views</span>` : ''}
  `;

  const footer = card.querySelector('.post-footer, .post-actions, .post-meta-row');
  if (footer) footer.parentElement.insertBefore(meta, footer);
  else if (content) content.parentElement.insertBefore(meta, content.nextSibling);

  // Record view via IntersectionObserver
  if (postId) {
    const io = new IntersectionObserver(entries => {
      if (entries[0].isIntersecting) {
        io.disconnect();
        recordPostView(postId);
      }
    }, { threshold: 0.5 });
    io.observe(card);
  }
}

// Observe feed for post cards
const readMetaObserver = new MutationObserver(mutations => {
  mutations.forEach(m => {
    m.addedNodes.forEach(node => {
      if (node.nodeType !== 1) return;
      if (node.classList?.contains('post-card')) injectReadMetaIntoCard(node);
      node.querySelectorAll?.('.post-card').forEach(c => injectReadMetaIntoCard(c));
    });
  });
});
readMetaObserver.observe(document.body, { childList: true, subtree: true });
// Also handle already-rendered cards
document.querySelectorAll('.post-card').forEach(c => injectReadMetaIntoCard(c));


/* ══════════════════════════════════════════════════════════════
   FEATURE 10 — Pinned posts on profile (up to 3)
   ══════════════════════════════════════════════════════════════ */

async function getPinnedPosts(userId) {
  const { data } = await sb.from('op_profiles')
    .select('pinned_posts')
    .eq('id', userId)
    .single();
  return data?.pinned_posts || [];
}

async function setPinnedPosts(userId, pinnedIds) {
  await sb.from('op_profiles')
    .update({ pinned_posts: pinnedIds })
    .eq('id', userId);
}

async function pinPost(postId) {
  const userId = window.State?.user?.id;
  if (!userId) return;
  const current = await getPinnedPosts(userId);
  if (current.includes(postId)) {
    toast('Already pinned', 'thumbtack');
    return;
  }
  if (current.length >= 3) {
    toast('Max 3 pinned posts. Unpin one first.', 'circle-exclamation');
    return;
  }
  await setPinnedPosts(userId, [...current, postId]);
  toast('Post pinned to your profile!', 'thumbtack');
}

async function unpinPost(postId) {
  const userId = window.State?.user?.id;
  if (!userId) return;
  const current = await getPinnedPosts(userId);
  await setPinnedPosts(userId, current.filter(id => id !== postId));
  toast('Post unpinned', 'thumbtack');
  // Remove pin badge from card
  document.querySelectorAll(`.post-card[data-post-id="${postId}"] .pin-badge`).forEach(b => b.remove());
}

async function renderPinnedSection(profileUserId, containerEl) {
  const pinnedIds = await getPinnedPosts(profileUserId);
  if (!pinnedIds.length) return;

  const existing = document.getElementById('pinned-posts-section');
  if (existing) existing.remove();

  const { data: posts } = await sb.from('op_posts')
    .select('*, profiles(username, display_name, avatar_url)')
    .in('id', pinnedIds)
    .order('created_at', { ascending: false });

  if (!posts?.length) return;

  const section = document.createElement('div');
  section.id = 'pinned-posts-section';
  section.className = 'pinned-section';
  section.innerHTML = `
    <div class="pinned-section-header">
      <i class="fa-solid fa-thumbtack"></i> Pinned posts
    </div>
  `;

  posts.forEach(post => {
    const card = document.createElement('div');
    card.className = 'post-card';
    card.dataset.postId = post.id;
    card.innerHTML = `
      <div class="pin-badge"><i class="fa-solid fa-thumbtack"></i> Pinned</div>
      <div class="post-content">${escHtml((post.content || '').slice(0, 200))}${(post.content||'').length>200?'…':''}</div>
      <div class="post-read-meta" style="margin-top:8px">
        <span><i class="fa-regular fa-clock"></i>${readingTime(post.content)}</span>
        ${post.views_count ? `<span><i class="fa-regular fa-eye"></i>${fmtViews(post.views_count)} views</span>` : ''}
        <span style="margin-left:auto"><i class="fa-solid fa-heart" style="color:var(--rose)"></i> ${post.likes_count||0}</span>
      </div>
    `;
    section.appendChild(card);
  });

  containerEl.insertBefore(section, containerEl.firstChild);
}

// Inject pin/unpin into post more menu



// When profile view renders, inject pinned posts section
const profileObserver = new MutationObserver(() => {
  const profileFeed = document.getElementById('profile-posts-feed') ||
    document.querySelector('.profile-feed, [data-view="profile"] .feed-col');
  if (profileFeed && !document.getElementById('pinned-posts-section')) {
    const profileUserId = profileFeed.dataset.profileUserId ||
      document.querySelector('[data-profile-user-id]')?.dataset.profileUserId;
    if (profileUserId) {
      renderPinnedSection(profileUserId, profileFeed);
    }
  }
});
profileObserver.observe(document.body, { childList: true, subtree: true });


/* ══════════════════════════════════════════════════════════════
   POST COMPOSER — integrate poll data into post creation
   ══════════════════════════════════════════════════════════════ */

// Monkey-patch submitPost to attach poll data if active
(function patchSubmitPost() {
  // Direct call — submitPost is defined in this file
  if (typeof window.submitPost === 'function' && !window._pollPatchApplied) {
    window._pollPatchApplied = true;
    // Poll attachment is handled via event delegation below
  }

  // Hook into the post insert call via Supabase middleware pattern
  // We intercept the composer's submit button click
  document.addEventListener('click', async e => {
    const submitBtn = e.target.closest('#composer-submit, .composer-submit, [data-action="submit-post"]');
    if (!submitBtn) return;
    if (!PollState.active) return;
    const pollData = getPollData();
    if (!pollData) {
      toast('Add at least 2 poll options', 'circle-exclamation');
      e.stopImmediatePropagation();
      return;
    }
    // Attach poll to window for the main submit handler to pick up
    window._pendingPoll = pollData;
    // Reset after submit
    setTimeout(() => {
      window._pendingPoll = null;
      PollState.active = false;
      PollState.options = ['', ''];
      document.getElementById('poll-builder-ui')?.remove();
      document.getElementById('poll-toggle-btn').style.color = '';
      document.getElementById('poll-toggle-btn').style.background = '';
    }, 500);
  }, true);
})();

// Patch renderPostCard to show polls
(function patchRenderPostCard() {
  if (typeof window.renderPostCard === 'function' && !window._pollCardPatchApplied) {
    window._pollCardPatchApplied = true;
    const original = window.renderPostCard;
    window.renderPostCard = function(post, ...rest) {
      const card = original.call(this, post, ...rest);
      if (post.poll && card) {
        const userId = window.State?.user?.id || '';
        const pollHtml = renderPollInPost(post.poll, post.id, userId);
        if (pollHtml) {
          const contentEl = card.querySelector('.post-body, .post-content, .post-text');
          if (contentEl) {
            const wrapper = document.createElement('div');
            wrapper.innerHTML = pollHtml;
            contentEl.parentElement.insertBefore(wrapper.firstElementChild, contentEl.nextSibling);
          }
        }
      }
      return card;
    };
  }
})();


/* ══════════════════════════════════════════════════════════════
   SUPABASE SQL ADDITIONS (log to console for setup)
   ══════════════════════════════════════════════════════════════ */
// SQL migrations moved to migrations/ directory

console.log('[Devit Features Patch v2] ✓ Loaded: GitHub autofill, Polls, Digest widget, Read time/Views, Pinned posts + Soft UI');

/* ── DM Welcome Banner + Typing Indicator Styles ─────────────── */
(function injectDMStyles() {
  if (document.getElementById('devit-dm-styles')) return;
  const s = document.createElement('style');
  s.id = 'devit-dm-styles';
  s.textContent = `
    /* ── Remove conversation button ─────────────────────── */
    .conversation-item {
      position: relative;
    }
    .conv-remove-btn {
      display: none;
      position: absolute;
      right: 8px;
      top: 50%;
      transform: translateY(-50%);
      background: var(--bg-elevated, #1e1e2e);
      border: 1px solid var(--border, #333);
      border-radius: 6px;
      color: var(--text-muted);
      width: 24px;
      height: 24px;
      align-items: center;
      justify-content: center;
      cursor: pointer;
      font-size: 11px;
      transition: background 0.15s, color 0.15s;
      z-index: 2;
    }
    .conversation-item:hover .conv-remove-btn {
      display: flex;
    }
    .conv-remove-btn:hover {
      background: var(--rose, #f43f5e);
      border-color: var(--rose, #f43f5e);
      color: #fff;
    }

    /* ── DM Welcome Banner ──────────────────────────────── */
    .dm-welcome-banner {
      display: flex;
      flex-direction: column;
      align-items: flex-start;
      padding: 32px 20px 24px;
      border-bottom: 1px solid var(--border);
      margin-bottom: 8px;
    }
    .dm-welcome-avatar {
      margin-bottom: 16px;
    }
    .dm-welcome-avatar .profile-avatar-circle,
    .dm-welcome-avatar img {
      width: 80px !important;
      height: 80px !important;
      font-size: 32px !important;
      border-radius: 50%;
    }
    .dm-welcome-name {
      font-family: var(--font-display);
      font-size: 22px;
      font-weight: 800;
      color: var(--text-primary);
      margin-bottom: 2px;
    }
    .dm-welcome-handle {
      font-size: 14px;
      color: var(--text-muted);
      margin-bottom: 10px;
    }
    .dm-welcome-meta {
      display: flex;
      align-items: center;
      gap: 6px;
      font-size: 13px;
      color: var(--text-muted);
      margin-bottom: 12px;
    }
    .dm-welcome-meta i {
      color: var(--cyan);
      font-size: 12px;
    }
    .dm-welcome-desc {
      font-size: 14px;
      color: var(--text-secondary);
      line-height: 1.55;
      max-width: 420px;
    }
    .dm-welcome-desc strong {
      color: var(--text-primary);
      font-weight: 700;
    }

    /* ── Typing Indicator ───────────────────────────────── */
    .dm-typing-bar {
      display: flex;
      align-items: center;
      gap: 6px;
      padding: 2px 16px 4px;
      min-height: 22px;
    }
    .dm-typing-label {
      font-size: 12px;
      color: var(--text-muted);
      font-style: italic;
    }
    .dm-typing-dots {
      display: inline-flex;
      align-items: center;
      gap: 3px;
    }
    .dm-typing-dots span {
      display: inline-block;
      width: 5px;
      height: 5px;
      border-radius: 50%;
      background: var(--text-muted);
      animation: dm-bounce 1.2s ease-in-out infinite;
    }
    .dm-typing-dots span:nth-child(2) { animation-delay: 0.2s; }
    .dm-typing-dots span:nth-child(3) { animation-delay: 0.4s; }
    @keyframes dm-bounce {
      0%, 60%, 100% { transform: translateY(0); opacity: 0.4; }
      30%            { transform: translateY(-4px); opacity: 1; }
    }
  `;
  document.head.appendChild(s);
})();

(function injectChannelMessageStyles() {
  if (document.getElementById('ch-msg-styles')) return;
  const s = document.createElement('style');
  s.id = 'ch-msg-styles';
  s.textContent = `
    /* ── Message hover actions ─────────────────────────── */
    .disc-msg {
      position: relative;
    }
    .disc-msg-actions {
      display: none;
      position: absolute;
      top: 2px;
      right: 12px;
      background: var(--bg-elevated, #1e1e2e);
      border: 1px solid var(--border, #2a2a3a);
      border-radius: 8px;
      padding: 3px 6px;
      gap: 2px;
      align-items: center;
      box-shadow: 0 4px 16px rgba(0,0,0,0.4);
      z-index: 10;
    }
    .disc-msg:hover .disc-msg-actions {
      display: flex;
    }
    .disc-msg-action-btn {
      background: none;
      border: none;
      color: var(--text-muted, #888);
      cursor: pointer;
      padding: 4px 7px;
      border-radius: 6px;
      font-size: 12px;
      transition: background 0.12s, color 0.12s;
      display: flex;
      align-items: center;
      gap: 4px;
    }
    .disc-msg-action-btn:hover {
      background: var(--bg-surface, #141420);
      color: var(--text-primary, #fff);
    }

    /* ── Reply quote inside message ────────────────────── */
    .disc-msg-reply-quote {
      display: flex;
      align-items: center;
      gap: 6px;
      background: var(--bg-void, #0a0a14);
      border-left: 3px solid var(--cyan, #22d3ee);
      border-radius: 0 6px 6px 0;
      padding: 4px 10px;
      margin-bottom: 4px;
      cursor: pointer;
      max-width: 100%;
      overflow: hidden;
      transition: background 0.12s;
    }
    .disc-msg-reply-quote:hover {
      background: rgba(34,211,238,0.07);
    }
    .disc-msg-reply-author {
      font-size: 11px;
      font-weight: 700;
      color: var(--cyan, #22d3ee);
      white-space: nowrap;
      flex-shrink: 0;
    }
    .disc-msg-reply-text {
      font-size: 11px;
      color: var(--text-muted, #888);
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
    }

    /* ── Jump-to highlight flash ───────────────────────── */
    .disc-msg-highlight {
      animation: ch-highlight-flash 1.5s ease forwards;
    }
    @keyframes ch-highlight-flash {
      0%   { background: rgba(34,211,238,0.18); border-radius: 8px; }
      100% { background: transparent; }
    }

    /* ── Reply bar above input ─────────────────────────── */
    #channel-reply-bar {
      padding: 6px 14px 4px;
      border-top: 1px solid var(--border, #2a2a3a);
      background: var(--bg-surface, #141420);
    }
    .ch-reply-bar-inner {
      display: flex;
      align-items: center;
      gap: 7px;
      font-size: 12px;
    }
    .ch-reply-bar-label {
      color: var(--text-muted, #888);
      white-space: nowrap;
      flex-shrink: 0;
    }
    .ch-reply-bar-label strong {
      color: var(--cyan, #22d3ee);
    }
    .ch-reply-bar-preview {
      color: var(--text-muted, #888);
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
      flex: 1;
      opacity: 0.7;
    }
    .ch-reply-bar-close {
      background: none;
      border: none;
      color: var(--text-muted, #888);
      cursor: pointer;
      padding: 2px 6px;
      border-radius: 4px;
      font-size: 13px;
      flex-shrink: 0;
      transition: color 0.12s;
    }
    .ch-reply-bar-close:hover { color: var(--rose, #f43f5e); }

    /* ── Inline edit ───────────────────────────────────── */
    .ch-edit-wrap {
      display: flex;
      flex-direction: column;
      gap: 5px;
      margin-top: 2px;
    }
    .ch-edit-input {
      width: 100%;
      background: var(--bg-void, #0a0a14);
      border: 1px solid var(--cyan, #22d3ee);
      border-radius: 8px;
      color: var(--text-primary, #fff);
      font-size: 14px;
      font-family: inherit;
      padding: 7px 10px;
      resize: none;
      outline: none;
      line-height: 1.5;
      box-sizing: border-box;
    }
    .ch-edit-actions {
      display: flex;
      align-items: center;
      gap: 6px;
    }
  `;
  document.head.appendChild(s);
})();

/* ══════════════════════════════════════════════════════════════
   DEVIT FIXES — SQL additions (run in Supabase Dashboard)
   ══════════════════════════════════════════════════════════════ */
// SQL migrations moved to migrations/ directory


/* ══════════════════════════════════════════════════════════════
   SQL — run in Supabase Dashboard > SQL Editor
   ══════════════════════════════════════════════════════════════ */
// SQL for GitHub integration is in migrations/ directory

/* ── Helpers (self-contained, no dependency on outer scope) ── */
function _timeAgo(ts) {
  if (!ts) return '';
  const s = Math.floor((Date.now() - new Date(ts)) / 1000);
  if (s < 60)    return 'just now';
  if (s < 3600)  return Math.floor(s/60)   + 'm ago';
  if (s < 86400) return Math.floor(s/3600) + 'h ago';
  return Math.floor(s/86400) + 'd ago';
}


/* ================================================================
   § 5 — PATCH buildPostCard → UpVote / DownVote + Edit / Delete
   ================================================================ */

const _origBuildPostCard = window.buildPostCard;

window.buildPostCard = function(post, profile, isLiked = false, isBookmarked = false, voteMap = {}) {
  // Call original to get the card
  const card = _origBuildPostCard.call(this, post, profile, isLiked, isBookmarked);
  if (!card) return card;

  // ── Replace heart with UpVote / DownVote ─────────────────
  const likeBtn = card.querySelector('.like-btn');
  if (likeBtn) {
    const actionsRow = likeBtn.closest('.post-actions');
    const likeCount  = parseInt(likeBtn.querySelector('.like-count')?.textContent || '0') || 0;

    // Resolve vote state from DB-backed voteMap (1 = up, -1 = down, absent = none)
    const dbVote = voteMap[post.id];
    const storedVote  = dbVote === 1 ? 'up' : dbVote === -1 ? 'down' : 'none';
    const displayScore = likeCount; // score already reflects DB state

    const voteGroup = document.createElement('span');
    voteGroup.className = 'vote-group';
    voteGroup.setAttribute('role', 'group');
    voteGroup.setAttribute('aria-label', 'Vote on post');
    voteGroup.innerHTML = `
      <button class="vote-btn upvote-btn ${storedVote === 'up' ? 'upvoted' : ''}" aria-label="Upvote" title="Upvote">
        <svg width="13" height="13" viewBox="0 0 24 24" fill="${storedVote === 'up' ? 'currentColor' : 'none'}" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round">
          <polyline points="18 15 12 9 6 15"></polyline>
        </svg>
        <span class="upvote-label">Up</span>
      </button>
      <span class="vote-score ${displayScore > 0 ? 'positive' : displayScore < 0 ? 'negative' : ''}" aria-live="polite">${fmtNum(displayScore)}</span>
      <div class="vote-divider" aria-hidden="true"></div>
      <button class="vote-btn downvote-btn ${storedVote === 'down' ? 'downvoted' : ''}" aria-label="Downvote" title="Downvote">
        <svg width="13" height="13" viewBox="0 0 24 24" fill="${storedVote === 'down' ? 'currentColor' : 'none'}" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round">
          <polyline points="6 9 12 15 18 9"></polyline>
        </svg>
        <span class="downvote-label">Down</span>
      </button>
    `;

    likeBtn.replaceWith(voteGroup);

    // Vote logic
    let voteState  = storedVote;
    let voteScore  = displayScore;
    const scoreEl  = voteGroup.querySelector('.vote-score');
    const upBtn    = voteGroup.querySelector('.upvote-btn');
    const downBtn  = voteGroup.querySelector('.downvote-btn');

    function updateVoteUI() {
      upBtn.classList.toggle('upvoted', voteState === 'up');
      upBtn.querySelector('svg').setAttribute('fill', voteState === 'up' ? 'currentColor' : 'none');
      downBtn.classList.toggle('downvoted', voteState === 'down');
      downBtn.querySelector('svg').setAttribute('fill', voteState === 'down' ? 'currentColor' : 'none');
      scoreEl.textContent = fmtNum(voteScore);
      scoreEl.className = `vote-score ${voteScore > 0 ? 'positive' : voteScore < 0 ? 'negative' : ''}`;
      // vote state is persisted directly in op_post_likes — no sessionStorage needed
    }

    upBtn.addEventListener('click', async e => {
      e.stopPropagation();
      const prev = voteState;
      if (voteState === 'up') {
        voteState = 'none'; voteScore--;
        await sb.from('op_post_likes').delete().eq('post_id', post.id).eq('user_id', State.user?.id);
      } else {
        if (voteState === 'down') voteScore++; // undo downvote
        voteState = 'up'; voteScore++;
        await sb.from('op_post_likes').upsert({ post_id: post.id, user_id: State.user?.id, vote: 1 }, { onConflict: 'post_id,user_id' });
        if (post.author_id !== State.user?.id) {
          sb.from('op_notifications').insert({ user_id: post.author_id, actor_id: State.user?.id, type: 'like', post_id: post.id });
        }
      }
      upBtn.style.transform = 'scale(1.35)';
      setTimeout(() => upBtn.style.transform = '', 200);
      updateVoteUI();
    });

    downBtn.addEventListener('click', async e => {
      e.stopPropagation();
      if (voteState === 'down') {
        voteState = 'none'; voteScore++;
        await sb.from('op_post_likes').delete().eq('post_id', post.id).eq('user_id', State.user?.id);
      } else {
        if (voteState === 'up') voteScore--; // undo upvote
        voteState = 'down'; voteScore--;
        await sb.from('op_post_likes').upsert({ post_id: post.id, user_id: State.user?.id, vote: -1 }, { onConflict: 'post_id,user_id' });
      }
      downBtn.style.transform = 'scale(1.3)';
      setTimeout(() => downBtn.style.transform = '', 200);
      updateVoteUI();
    });
  }


  return card;
};




/* ══════════════════════════════════════════════════════════════
   FEATURE: OPENGRAPH LINK PREVIEWS
   Detects URLs in post content and renders rich embed cards.
   Supports YouTube (inline player), Twitter/X, GitHub, Discord,
   and any site that serves OG meta tags.
   ══════════════════════════════════════════════════════════════ */

const OG_CACHE = new Map(); // url → og data, avoids duplicate fetches

/** Extract the first URL from a string */
function extractFirstUrl(text) {
  const match = text?.match(/https?:\/\/[^\s<>"']+/);
  return match ? match[0].replace(/[.,;!?)]+$/, '') : null; // trim trailing punctuation
}

/** YouTube video ID from any yt URL format */
function getYouTubeId(url) {
  try {
    const u = new URL(url);
    if (u.hostname.includes('youtu.be')) return u.pathname.slice(1).split('?')[0];
    if (u.hostname.includes('youtube.com')) return u.searchParams.get('v');
  } catch (_) {}
  return null;
}

/** Site brand config — icon + accent color */
function ogSiteBrand(url) {
  try {
    const host = new URL(url).hostname.replace('www.', '');
    const brands = {
      'youtube.com':   { icon: 'fa-brands fa-youtube',   color: '#ff0000' },
      'youtu.be':      { icon: 'fa-brands fa-youtube',   color: '#ff0000' },
      'github.com':    { icon: 'fa-brands fa-github',    color: '#58a6ff' },
      'twitter.com':   { icon: 'fa-brands fa-x-twitter', color: '#1d9bf0' },
      'x.com':         { icon: 'fa-brands fa-x-twitter', color: '#1d9bf0' },
      'discord.com':   { icon: 'fa-brands fa-discord',   color: '#5865f2' },
      'discord.gg':    { icon: 'fa-brands fa-discord',   color: '#5865f2' },
      'twitch.tv':     { icon: 'fa-brands fa-twitch',    color: '#9146ff' },
      'reddit.com':    { icon: 'fa-brands fa-reddit',    color: '#ff4500' },
      'medium.com':    { icon: 'fa-brands fa-medium',    color: '#00ab6c' },
      'dev.to':        { icon: 'fa-brands fa-dev',       color: '#08090a' },
      'producthunt.com':{ icon: 'fa-solid fa-p',         color: '#da552f' },
      'npmjs.com':     { icon: 'fa-brands fa-npm',       color: '#cb3837' },
      'stackoverflow.com':{ icon: 'fa-brands fa-stack-overflow', color: '#f48024' },
    };
    for (const [domain, brand] of Object.entries(brands)) {
      if (host === domain || host.endsWith('.' + domain)) return { ...brand, host };
    }
    return { icon: 'fa-solid fa-globe', color: 'var(--text-muted)', host };
  } catch (_) {
    return { icon: 'fa-solid fa-globe', color: 'var(--text-muted)', host: url };
  }
}

/** Fetch OG tags via allorigins proxy (CORS-safe) */
async function fetchOGData(url) {
  if (OG_CACHE.has(url)) return OG_CACHE.get(url);
  try {
    const proxyUrl = `https://api.allorigins.win/get?url=${encodeURIComponent(url)}`;
    const res = await fetch(proxyUrl, { signal: AbortSignal.timeout(6000) });
    if (!res.ok) throw new Error('proxy error');
    const { contents } = await res.json();
    if (!contents) throw new Error('empty');

    const doc = new DOMParser().parseFromString(contents, 'text/html');
    const meta = (prop) =>
      doc.querySelector(`meta[property="${prop}"]`)?.content ||
      doc.querySelector(`meta[name="${prop}"]`)?.content || '';

    const og = {
      title:       meta('og:title')       || meta('twitter:title')       || doc.title || '',
      description: meta('og:description') || meta('twitter:description') || meta('description') || '',
      image:       meta('og:image')       || meta('twitter:image')       || '',
      siteName:    meta('og:site_name')   || '',
      url,
    };

    // Clean up
    og.title = og.title.slice(0, 120);
    og.description = og.description.slice(0, 200);

    OG_CACHE.set(url, og);
    return og;
  } catch (_) {
    OG_CACHE.set(url, null);
    return null;
  }
}

/** Render a YouTube embed player */
function renderYouTubeEmbed(videoId, url) {
  const wrap = document.createElement('div');
  wrap.className = 'og-embed og-embed-youtube';
  wrap.innerHTML = `
    <div class="og-yt-thumb" data-vid="${videoId}" style="position:relative;padding-bottom:56.25%;border-radius:12px;overflow:hidden;background:#000;cursor:pointer">
      <img src="https://img.youtube.com/vi/${videoId}/hqdefault.jpg"
           style="position:absolute;inset:0;width:100%;height:100%;object-fit:cover;opacity:0.85" loading="lazy">
      <div style="position:absolute;inset:0;display:flex;align-items:center;justify-content:center">
        <div style="width:56px;height:56px;background:#ff0000;border-radius:50%;display:flex;align-items:center;justify-content:center;box-shadow:0 4px 20px rgba(0,0,0,0.5)">
          <i class="fa-solid fa-play" style="color:#fff;font-size:18px;margin-left:3px"></i>
        </div>
      </div>
      <a href="${url}" target="_blank" rel="noopener noreferrer"
         style="position:absolute;inset:0" aria-label="Play on YouTube"></a>
    </div>
    <div class="og-footer">
      <i class="fa-brands fa-youtube" style="color:#ff0000;font-size:12px"></i>
      <span class="og-site-name">YouTube</span>
      <a href="${url}" target="_blank" rel="noopener noreferrer" class="og-url">${url.slice(0,60)}${url.length>60?'…':''}</a>
    </div>`;

  // Click thumb → swap to iframe embed
  wrap.querySelector('.og-yt-thumb').addEventListener('click', e => {
    if (e.target.tagName === 'A') return;
    e.preventDefault();
    const iframe = document.createElement('iframe');
    iframe.src = `https://www.youtube.com/embed/${videoId}?autoplay=1`;
    iframe.allow = 'autoplay; encrypted-media; picture-in-picture';
    iframe.allowFullscreen = true;
    iframe.style.cssText = 'position:absolute;inset:0;width:100%;height:100%;border:none';
    const thumb = wrap.querySelector('.og-yt-thumb');
    thumb.innerHTML = '';
    thumb.appendChild(iframe);
  });

  return wrap;
}

/** Render a standard OG card */
function renderOGCard(og, url) {
  const brand = ogSiteBrand(url);
  const wrap = document.createElement('div');
  wrap.className = 'og-embed';

  const imgHtml = og.image
    ? `<div class="og-image-wrap"><img src="${escapeHtml(og.image)}" class="og-image" loading="lazy" onerror="this.closest('.og-image-wrap').remove()"></div>`
    : '';

  wrap.innerHTML = `
    ${imgHtml}
    <div class="og-body">
      <div class="og-title">${escapeHtml(og.title || url)}</div>
      ${og.description ? `<div class="og-desc">${escapeHtml(og.description)}</div>` : ''}
    </div>
    <div class="og-footer">
      <i class="${brand.icon}" style="color:${brand.color};font-size:12px"></i>
      <span class="og-site-name">${escapeHtml(og.siteName || brand.host)}</span>
      <a href="${escapeHtml(url)}" target="_blank" rel="noopener noreferrer" class="og-url">${escapeHtml(brand.host)}</a>
    </div>`;

  wrap.addEventListener('click', e => {
    if (e.target.tagName === 'A') return;
    window.open(url, '_blank', 'noopener,noreferrer');
  });

  return wrap;
}

/**
 * Main entry — find first URL in post content, render embed into container.
 * Call this after the card is in the DOM.
 */
async function injectOGPreview(postContent, container) {
  const url = extractFirstUrl(postContent);
  if (!url) return;

  // Skeleton placeholder
  const skeleton = document.createElement('div');
  skeleton.className = 'og-skeleton';
  container.appendChild(skeleton);

  try {
    // YouTube gets special inline player
    const ytId = getYouTubeId(url);
    if (ytId) {
      skeleton.replaceWith(renderYouTubeEmbed(ytId, url));
      return;
    }

    const og = await fetchOGData(url);
    if (!og || !og.title) { skeleton.remove(); return; }
    skeleton.replaceWith(renderOGCard(og, url));
  } catch (_) {
    skeleton.remove();
  }
}
