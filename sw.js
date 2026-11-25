/* Morning Briefing shell worker: network-first for HTML so editions and the reader JS always update. */
const VERSION = 'briefing-shell-20261125-1825';
const SHELL_ASSETS = [
  '/morning-briefing/manifest.webmanifest',
  '/morning-briefing/img/mach33-wordmark-black.svg?v=3',
  '/morning-briefing/img/mach33-wordmark-white.svg?v=3',
  '/morning-briefing/img/icon-192.png',
  '/morning-briefing/img/icon-512.png',
  '/morning-briefing/img/apple-touch-icon.png'
];
const SHELL_HTML = [
  '/morning-briefing/',
  '/morning-briefing/news/',
  '/morning-briefing/scitech/',
  '/morning-briefing/papers/'
];

self.addEventListener('install', (event) => {
  event.waitUntil(
    caches.open(VERSION).then((cache) => cache.addAll(SHELL_ASSETS.concat(SHELL_HTML))).then(() => self.skipWaiting())
  );
});

self.addEventListener('activate', (event) => {
  event.waitUntil(
    caches.keys().then((keys) =>
      Promise.all(keys.filter((k) => k !== VERSION).map((k) => caches.delete(k)))
    ).then(() => self.clients.claim())
  );
});

self.addEventListener('message', (event) => {
  if (event.data === 'skipWaiting') self.skipWaiting();
});

function isHtml(request, url) {
  if (request.mode === 'navigate') return true;
  const accept = request.headers.get('accept') || '';
  if (accept.includes('text/html')) return true;
  const p = url.pathname;
  if (p.endsWith('.html') || p.endsWith('/')) return true;
  const last = (p.split('/').pop() || '');
  if (last && !/\.[a-zA-Z0-9]{1,8}$/.test(last)) return true;
  return /\/morning-briefing\/(news\/|scitech\/|papers\/)?$/.test(p);
}

self.addEventListener('fetch', (event) => {
  const request = event.request;
  if (request.method !== 'GET') return;
  const url = new URL(request.url);
  if (url.origin !== self.location.origin) return;
  if (!url.pathname.startsWith('/morning-briefing/')) return;

  // Intraday + article JSON + HTML documents: always network-first.
  if (url.pathname.endsWith('/latest.json') || url.pathname.endsWith('latest.json')
      || url.pathname.includes('/articles/') || isHtml(request, url)) {
    event.respondWith(
      fetch(request)
        .then((res) => {
          if (res && res.ok && isHtml(request, url)) {
            const copy = res.clone();
            caches.open(VERSION).then((cache) => cache.put(request, copy));
          }
          return res;
        })
        .catch(() => caches.match(request).then((cached) => cached || caches.match('/morning-briefing/')))
    );
    return;
  }

  const pathWithQuery = url.pathname + url.search;
  if (SHELL_ASSETS.includes(url.pathname) || SHELL_ASSETS.includes(pathWithQuery)) {
    event.respondWith(
      caches.match(request).then((cached) => {
        const net = fetch(request).then((res) => {
          if (res && res.ok) {
            const copy = res.clone();
            caches.open(VERSION).then((cache) => cache.put(request, copy));
          }
          return res;
        });
        return cached || net;
      })
    );
  }
});
