// Cloudflare Worker for OOR Offline Connect redirects.
// Route example: https://covaxracing.org/oor-connect/9277
// Redirects to: acc-connect://103.212.227.132:9277/

const DEFAULT_OOR_IP = '103.212.227.132';

export default {
  async fetch(request) {
    const url = new URL(request.url);
    const port = url.pathname.split('/').filter(Boolean).pop();

    if (!/^\d{2,5}$/.test(port || '')) {
      return new Response('Missing or invalid ACC TCP port.', { status: 400 });
    }

    return Response.redirect(`acc-connect://${DEFAULT_OOR_IP}:${port}/`, 302);
  },
};
