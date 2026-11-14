/* Morning Briefing shell worker: network-first for HTML so daily editions update; cache the app shell for fast standalone launch. */
const VERSION = 'briefing-shell-v6';
const SHELL = [
  '/morning-briefing/',
  '/morning-briefing/news/',
  '/morning-briefing/scitech/',
  '/morning-briefing/manifest.webmanifest',
  '/morning-briefing/img/mach33-wordmark-black.svg?v=3',
  '/morning-briefing/img/mach33-wordmark-white.svg?v=3',
  '/morning-briefing/img/icon-192.png',
  '/morning-briefing/img/icon-512.png',
  '/morning-briefing/img/apple-touch-icon.png'
];

self.addEventListener('install', (event) => {
  event.waitUntil(
    caches.open(VERSION).then((cache) => cache.addAll(SHELL)).then(() => self.skipWaiting())
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
  return url.pathname.endsWith('.html') || /\/morning-briefing\/(news\/|scitech\/)?$/.test(url.pathname);
}

self.addEventListener('fetch', (event) => {
  const request = event.request;
  if (request.method !== 'GET') return;
  const url = new URL(request.url);
  if (url.origin !== self.location.origin) return;
  if (!url.pathname.startsWith('/morning-briefing/')) return;

  // Intraday endpoints must always hit the network so refresh sees new stories.
  if (url.pathname.endsWith('/latest.json') || url.pathname.endsWith('latest.json')
      || url.pathname.includes('/articles/')) {
    event.respondWith(fetch(request).catch(() => caches.match(request)));
    return;
  }

  if (isHtml(request, url)) {
    event.respondWith(
      fetch(request)
        .then((res) => {
          if (res && res.ok) {
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
  if (SHELL.includes(url.pathname) || SHELL.includes(pathWithQuery)) {
    event.respondWith(
      caches.match(request).then((cached) => {
        const net = fetch(request).then((res) => {
          if (res && res.ok) {
            const copy = res.clone();
            caches.open(VERSION).then((cache) => cache.put(request, copy));
          }
          return res;
        });
        // prefer network for versioned assets too if cache miss; stale-while-revalidate when cached
        return cached || net;
      })
    );
  }
});
