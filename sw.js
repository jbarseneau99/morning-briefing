/* Morning Briefing shell worker: network-first for HTML so daily editions update; cache the app shell for fast standalone launch. */
const VERSION = 'briefing-shell-v1';
const SHELL = [
  '/morning-briefing/',
  '/morning-briefing/news/',
  '/morning-briefing/scitech/',
  '/morning-briefing/manifest.webmanifest',
  '/morning-briefing/img/mach33-wordmark-black.svg',
  '/morning-briefing/img/mach33-wordmark-white.svg',
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

  if (SHELL.includes(url.pathname)) {
    event.respondWith(
      caches.match(request).then((cached) => {
        if (cached) return cached;
        return fetch(request).then((res) => {
          if (res && res.ok) {
            const copy = res.clone();
            caches.open(VERSION).then((cache) => cache.put(request, copy));
          }
          return res;
        });
      })
    );
  }
});
