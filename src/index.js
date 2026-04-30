require('dotenv').config();

const fs = require('fs');
const path = require('path');
const { chromium } = require('playwright');
const {
  Client,
  GatewayIntentBits,
  REST,
  Routes,
  SlashCommandBuilder,
  PermissionFlagsBits,
  EmbedBuilder,
  MessageFlags,
  ActionRowBuilder,
  ButtonBuilder,
  ButtonStyle,
} = require('discord.js');

const ROOT = path.resolve(__dirname, '..');
const CONFIG_PATH = path.join(ROOT, 'data', 'config.json');

const TOKEN = process.env.DISCORD_TOKEN;
const CLIENT_ID = process.env.CLIENT_ID;
const ACC_STATUS_URL = process.env.ACC_STATUS_URL || 'https://acc-status.jonatan.net/servers';
const SEARCH_TERM = process.env.SEARCH_TERM || 'OORAP';
const VISIBILITY = (process.env.SERVER_VISIBILITY || 'private').toLowerCase();
const REFRESH_SECONDS = Number(process.env.REFRESH_SECONDS || 30);
const CONNECT_BASE_URL = (process.env.CONNECT_BASE_URL || 'https://covaxracing.org/oor-connect').replace(/\/$/, '');
const OFFLINE_HELP_URL = process.env.OFFLINE_HELP_URL || 'https://oor-offline-help.covaxracing.org/';
const ACC_LOBBY_STATUS_URL = process.env.ACC_LOBBY_STATUS_URL || 'https://acc-status.jonatan.net/';
const LAST_GOOD_CACHE_PATH = path.join(ROOT, 'data', 'last-good-scrape.json');
const LAST_CHECKED_HEARTBEAT_SECONDS = Number(process.env.LAST_CHECKED_HEARTBEAT_SECONDS || 300);
const BROWSER_RECYCLE_MINUTES = Number(process.env.BROWSER_RECYCLE_MINUTES || 120);
const BLOCKED_BROWSER_RESOURCE_TYPES = new Set(
  String(process.env.BLOCKED_BROWSER_RESOURCE_TYPES || 'image,font,media')
    .split(',')
    .map(x => x.trim().toLowerCase())
    .filter(Boolean)
);
function normaliseDiscordEmoji(raw, fallbackName = 'ooricon') {
  const value = String(raw || '').trim();
  if (!value) return '🏁';
  if (/^<a?:[A-Za-z0-9_~]+:\d+>$/.test(value)) return value;
  const shorthandMatch = value.match(/^:([A-Za-z0-9_~]+):(\d+)$/);
  if (shorthandMatch) return `<:${shorthandMatch[1]}:${shorthandMatch[2]}>`;
  if (/^\d+$/.test(value)) return `<:${fallbackName}:${value}>`;
  if (/^:[A-Za-z0-9_~]+:$/.test(value)) return value;
  return '🏁';
}

const OOR_ICON_EMOJI = normaliseDiscordEmoji(process.env.OOR_ICON_EMOJI, 'ooricon');
const MAX_FIELD_CHARS = 1024;
const MAX_EMBEDS = 10;
const PANEL_WIDTH_PAD = '⠀'.repeat(88);
const DEBUG = process.env.DEBUG_OOR === '1';
const SCRAPE_TIMEOUT_MS = Number(process.env.SCRAPE_TIMEOUT_MS || 60000);
const CONNECT_SCRAPE_TIMEOUT_MS = Number(process.env.CONNECT_SCRAPE_TIMEOUT_MS || 1200);
const ENABLE_CONNECT_LINKS = process.env.ENABLE_CONNECT_LINKS !== '0';
const POST_PROTECTION_ENABLED = process.env.POST_PROTECTION_ENABLED !== '0';
const USER_POST_DELETE_SECONDS = Math.max(1, Number(process.env.USER_POST_DELETE_SECONDS || 10));
const USER_POST_NOTICE_SECONDS = Math.max(1, Number(process.env.USER_POST_NOTICE_SECONDS || 10));

function debugLog(...args) {
  if (DEBUG) console.log('[Debug]', ...args);
}

function withTimeout(promise, ms, label) {
  let timer;
  const timeout = new Promise((_, reject) => {
    timer = setTimeout(() => reject(new Error(`${label} timed out after ${ms}ms`)), ms);
  });
  return Promise.race([promise, timeout]).finally(() => clearTimeout(timer));
}

if (!TOKEN || !CLIENT_ID) {
  console.error('Missing DISCORD_TOKEN or CLIENT_ID in .env');
  process.exit(1);
}

function ensureData() {
  const dataDir = path.dirname(CONFIG_PATH);
  if (!fs.existsSync(dataDir)) fs.mkdirSync(dataDir, { recursive: true });
  if (!fs.existsSync(CONFIG_PATH)) {
    fs.writeFileSync(CONFIG_PATH, JSON.stringify({ guilds: {}, global: {} }, null, 2));
  }
}

function loadConfig() {
  ensureData();
  try {
    const parsed = JSON.parse(fs.readFileSync(CONFIG_PATH, 'utf8'));
    parsed.guilds ||= {};
    parsed.global ||= {};
    return parsed;
  } catch (err) {
    console.error('Unable to read config.json:', err);
    return { guilds: {}, global: {} };
  }
}

function saveConfig(config) {
  fs.writeFileSync(CONFIG_PATH, JSON.stringify(config, null, 2));
}

function loadLastGoodScrape() {
  try {
    if (!fs.existsSync(LAST_GOOD_CACHE_PATH)) return null;
    const parsed = JSON.parse(fs.readFileSync(LAST_GOOD_CACHE_PATH, 'utf8'));
    return Array.isArray(parsed?.servers) ? parsed : null;
  } catch { return null; }
}

function saveLastGoodScrape(scrape) {
  try { fs.writeFileSync(LAST_GOOD_CACHE_PATH, JSON.stringify(scrape, null, 2)); }
  catch (err) { console.error('[Cache] Unable to save last good scrape:', err.message || err); }
}

function scrapeSignature(scrape) {
  return JSON.stringify({
    accOnline: scrape?.accOnline === false ? false : true,
    servers: (Array.isArray(scrape?.servers) ? scrape.servers : []).map(s => ({
      track: s.track, name: s.name, drivers: Number(s.drivers || 0), maxDrivers: Number(s.maxDrivers || 0),
      variability: Number(s.variability || 0), sessions: Array.isArray(s.sessions) ? s.sessions : [], tcpPort: Number(s.tcpPort || 0),
    })),
  });
}

function getRefreshSeconds(config = loadConfig()) {
  return Math.max(15, Number(config?.global?.refreshSeconds || REFRESH_SECONDS || 30));
}

function nowString() {
  return new Intl.DateTimeFormat('en-AU', {
    timeZone: 'Australia/Brisbane',
    day: '2-digit',
    month: '2-digit',
    year: 'numeric',
    hour: 'numeric',
    minute: '2-digit',
    hour12: true,
  }).format(new Date());
}

function statusEmoji(ok) {
  return ok ? '🟢' : '🔴';
}

function driverBar(current, max) {
  const slots = 25;
  const safeCurrent = Number(current || 0);
  const safeMax = Number(max || 0);
  const ratio = safeMax > 0 ? Math.min(1, Math.max(0, safeCurrent / safeMax)) : 0;
  const filled = ratio > 0 ? Math.max(1, Math.round(ratio * slots)) : 0;

  if (ratio >= 1) return '🟥'.repeat(slots);
  if (ratio >= 0.75) return '🟨'.repeat(filled) + '⬛'.repeat(slots - filled);
  if (ratio > 0) return '🟩'.repeat(filled) + '⬛'.repeat(slots - filled);
  return '⬛'.repeat(slots);
}

function cleanServerName(serverName) {
  return String(serverName || '')
    .replace(/^\[OORAP\]\s*Octane Online Racing\s*\|\s*/i, '')
    .trim();
}

function normaliseScrapedServers(servers) {
  const search = SEARCH_TERM.toUpperCase();
  const byKey = new Map();

  for (const server of Array.isArray(servers) ? servers : []) {
    const name = String(server?.name || '').trim();
    const track = String(server?.track || '').trim();
    const maxDrivers = Number(server?.maxDrivers || 0);

    // Drop partial rows created by expanded/list wrapper elements.
    if (!name.toUpperCase().includes(search)) continue;
    if (!track || /^unknown track$/i.test(track)) continue;
    if (!maxDrivers || maxDrivers <= 0) continue;

    const cleanName = cleanServerName(name);
    if (!cleanName) continue;

    // Track + cleaned server name is stable while still allowing Wet/Day/Open Practice variants.
    const key = `${track.toUpperCase()}|${cleanName.toUpperCase()}`;
    const candidate = {
      ...server,
      track,
      name,
      drivers: Number(server.drivers || 0),
      maxDrivers,
      variability: Number(server.variability || 0),
      sessions: Array.isArray(server.sessions) ? server.sessions.filter(Boolean) : [],
      tcpPort: server.tcpPort ? Number(server.tcpPort) : null,
      udpPort: server.udpPort ? Number(server.udpPort) : null,
    };

    const existing = byKey.get(key);
    if (!existing) {
      byKey.set(key, candidate);
      continue;
    }

    // Prefer the richer row if the same server was seen through multiple DOM wrappers.
    const existingScore = (existing.tcpPort ? 4 : 0) + (existing.sessions?.length || 0) + (existing.drivers > 0 ? 2 : 0);
    const candidateScore = (candidate.tcpPort ? 4 : 0) + (candidate.sessions?.length || 0) + (candidate.drivers > 0 ? 2 : 0);
    if (candidateScore > existingScore) byKey.set(key, candidate);
  }

  return [...byKey.values()].sort((a, b) => {
    const driverDiff = Number(b.drivers || 0) - Number(a.drivers || 0);
    if (driverDiff) return driverDiff;
    const seriesDiff = getSeriesType(a.name).localeCompare(getSeriesType(b.name));
    if (seriesDiff) return seriesDiff;
    return String(a.track || '').localeCompare(String(b.track || ''));
  });
}

function connectLink(server) {
  const tcpPort = Number(server?.tcpPort || 0);
  if (!tcpPort) return null;
  return `[Offline Connect ↗](${CONNECT_BASE_URL}/${tcpPort})`;
}

function formatSessions(sessions) {
  const list = Array.isArray(sessions) ? sessions.filter(Boolean) : [];
  if (!list.length) return 'Session info unavailable';
  return list.map((session, index) => index === 0 ? `🟢 ${session}` : session).join(' | ');
}

function weatherEmoji(text) {
  const t = String(text || '').toLowerCase();
  if (t.includes('storm')) return '⛈️';
  if (t.includes('rain')) return '🌧️';
  if (t.includes('cloud')) return '☁️';
  if (t.includes('clear') || t.includes('sun')) return '☀️';
  return '☀️';
}

function truncate(str, max) {
  const s = String(str || '');
  return s.length <= max ? s : `${s.slice(0, max - 1)}…`;
}

let browserPromise = null;
let browserStartedAt = 0;
let scrapeInFlight = false;
let shuttingDown = false;

async function resetBrowser(reason = 'manual reset') {
  const oldBrowserPromise = browserPromise;
  browserPromise = null;
  browserStartedAt = 0;
  const oldBrowser = await oldBrowserPromise?.catch(() => null);
  await oldBrowser?.close().catch(() => {});
  console.log(`[Browser] closed shared Chromium browser (${reason})`);
  debugLog('browser reset', reason);
}

async function getBrowser() {
  const now = Date.now();
  const recycleMs = BROWSER_RECYCLE_MINUTES > 0 ? BROWSER_RECYCLE_MINUTES * 60 * 1000 : 0;
  if (browserPromise && recycleMs && browserStartedAt && now - browserStartedAt > recycleMs) {
    await resetBrowser(`recycle after ${BROWSER_RECYCLE_MINUTES} minutes`);
  }
  if (!browserPromise) {
    browserStartedAt = now;
    console.log('[Browser] launching shared Chromium browser');
    browserPromise = chromium.launch({
      headless: true,
      args: [
        '--disable-dev-shm-usage',
        '--no-sandbox',
        '--disable-background-networking',
        '--disable-background-timer-throttling',
        '--disable-renderer-backgrounding',
        '--disable-features=Translate,BackForwardCache,AcceptCHFrame',
        '--disable-extensions',
        '--mute-audio',
      ],
    });
  }
  const browser = await browserPromise;
  if (!browser.isConnected()) {
    await resetBrowser('browser disconnected');
    browserStartedAt = Date.now();
    console.log('[Browser] relaunching shared Chromium browser');
    browserPromise = chromium.launch({
      headless: true,
      args: [
        '--disable-dev-shm-usage',
        '--no-sandbox',
        '--disable-background-networking',
        '--disable-background-timer-throttling',
        '--disable-renderer-backgrounding',
        '--disable-features=Translate,BackForwardCache,AcceptCHFrame',
        '--disable-extensions',
        '--mute-audio',
      ],
    });
    return browserPromise;
  }
  return browser;
}

async function checkAccLobbyStatus(page) {
  try {
    await page.goto(ACC_LOBBY_STATUS_URL, { waitUntil: 'domcontentloaded', timeout: 12000 });
    await page.waitForTimeout(700);
    const text = await page.locator('body').innerText({ timeout: 3000 }).catch(() => '');
    const compact = String(text || '').replace(/\s+/g, ' ').toLowerCase();

    // The ACC status homepage includes generic explanatory words like "down" in help text,
    // so avoid broad keyword matching. Prefer the explicit status card wording.
    if (/servers\s+are\s+up/.test(compact)) return true;
    if (/servers\s+are\s+down|server\s+browser\s+is\s+down|service\s+is\s+down/.test(compact)) return false;

    const statusCards = await page.locator('text=/Servers are (up|down)/i').allInnerTexts().catch(() => []);
    const statusText = statusCards.join(' ').replace(/\s+/g, ' ').toLowerCase();
    if (/servers\s+are\s+up/.test(statusText)) return true;
    if (/servers\s+are\s+down/.test(statusText)) return false;

    return null;
  } catch (err) {
    debugLog('acc lobby status check skipped', err.message || err);
    return null;
  }
}

async function enrichTcpPorts(page, servers) {
  if (!ENABLE_CONNECT_LINKS || !Array.isArray(servers) || !servers.length) return servers;

  const enriched = [];

  for (const server of servers) {
    const next = { ...server };

    if (Number(next.tcpPort || 0) > 0) {
      enriched.push(next);
      continue;
    }

    const label = String(server.name || '').trim();
    if (!label) {
      enriched.push(next);
      continue;
    }

    try {
      debugLog('expand server for TCP', label);

      const exactLocator = page.getByText(label, { exact: true }).first();
      await exactLocator.click({ timeout: CONNECT_SCRAPE_TIMEOUT_MS }).catch(async () => {
        const partial = label.slice(0, Math.min(60, label.length));
        await page.locator(`text=${partial}`).first().click({ timeout: CONNECT_SCRAPE_TIMEOUT_MS });
      });

      await page.waitForTimeout(250);

      const details = await page.evaluate(({ name, track }) => {
        const clean = (value) => String(value || '').replace(/\s+/g, ' ').trim();
        const wantedName = clean(name);
        const wantedTrack = clean(track);

        const candidates = Array.from(document.querySelectorAll('body *'))
          .map(el => clean(el.innerText || ''))
          .filter(txt =>
            txt.includes(wantedName) &&
            txt.includes('TCP') &&
            (!wantedTrack || txt.includes(wantedTrack)) &&
            txt.length < 2200
          )
          .sort((a, b) => a.length - b.length);

        const txt = candidates[0] || '';
        const ipMatch = txt.match(/\bIP\s+((?:\d{1,3}\.){3}\d{1,3})\b/i);
        const tcpMatch = txt.match(/\bTCP\s+(\d+)\b/i);
        const udpMatch = txt.match(/\bUDP\s+(\d+)\b/i);

        return {
          ip: ipMatch ? ipMatch[1] : null,
          tcpPort: tcpMatch ? Number(tcpMatch[1]) : null,
          udpPort: udpMatch ? Number(udpMatch[1]) : null,
        };
      }, { name: server.name, track: server.track });

      if (Number(details?.tcpPort || 0) > 0) {
        next.ip = details.ip || next.ip || null;
        next.tcpPort = Number(details.tcpPort);
        next.udpPort = details.udpPort ? Number(details.udpPort) : next.udpPort || null;
        debugLog('tcp found', label, next.tcpPort);
      } else {
        debugLog('tcp unavailable', label);
      }
    } catch (err) {
      debugLog('tcp expansion skipped', label, err.message || err);
    }

    enriched.push(next);
  }

  return enriched;
}

async function scrapeServersCore() {
  debugLog('scrape start');
  const browser = await getBrowser();
  debugLog('browser ready');
  const context = await browser.newContext({ viewport: { width: 1280, height: 1600 } });
  await context.route('**/*', route => {
    const type = route.request().resourceType();
    if (BLOCKED_BROWSER_RESOURCE_TYPES.has(type)) return route.abort().catch(() => {});
    return route.continue().catch(() => {});
  });
  const page = await context.newPage();

  try {
    const accOnline = await checkAccLobbyStatus(page);

    debugLog('goto', ACC_STATUS_URL);
    await page.goto(ACC_STATUS_URL, { waitUntil: 'domcontentloaded', timeout: 30000 });

    debugLog('fill search', SEARCH_TERM);
    await page.locator('input').first().fill(SEARCH_TERM, { timeout: 10000 });

    if (VISIBILITY === 'private') {
      debugLog('click private filter');
      await page.getByText(/^Private$/).first().click({ timeout: 8000 }).catch(err => debugLog('private click skipped', err.message));
    }

    debugLog('click search');
    await page.getByRole('button', { name: /search/i }).first().click({ timeout: 8000 }).catch(err => debugLog('search click skipped', err.message));

    await page.waitForTimeout(2500);
    await page.waitForSelector('text=Found', { timeout: 12000 }).catch(err => debugLog('found selector skipped', err.message));

    debugLog('extract visible server cards');
    const data = await page.evaluate((searchTerm) => {
      const parseCard = (fullText) => {
        const text = (fullText || '').replace(/\s+/g, ' ').trim();
        const lines = (fullText || '').split('\n').map(x => x.trim()).filter(Boolean);
        const nameLine = lines.find(x => x.includes(searchTerm)) || '';
        if (!nameLine) return null;

        const trackLine = lines.find(x =>
          !x.includes(searchTerm) &&
          !/^Connect/i.test(x) &&
          !/^(Mixed|GT3|GT4|GT2|TCX|GTC)$/i.test(x) &&
          !/^IP\b/i.test(x) &&
          !/^TCP\b/i.test(x) &&
          !/^UDP\b/i.test(x) &&
          !/^More servers/i.test(x) &&
          !/^Found\s+/i.test(x) &&
          !/^\d+\s*\/\s*\d+/.test(x)
        ) || 'Unknown track';

        const driverMatch = text.match(/(\d+)\s*\/\s*(\d+)/);
        const variabilityMatch = text.match(/(\d+)%/);
        const rawSessionMatches = [...text.matchAll(/\b([PQR])\s+(\d+)\s*min\b/gi)];
        const sessionByType = new Map();
        for (const m of rawSessionMatches) {
          const type = m[1].toUpperCase();
          if (!sessionByType.has(type)) sessionByType.set(type, type + ' ' + m[2]);
        }
        const sessionMatches = ['P', 'Q', 'R'].map(type => sessionByType.get(type)).filter(Boolean);
        const classMatch = text.match(/\b(Mixed|GT3|GT4|GT2|TCX|GTC)\b/i);
        const ipMatch = text.match(/\bIP\s+((?:\d{1,3}\.){3}\d{1,3})\b/i);
        const tcpMatch = text.match(/\bTCP\s+(\d+)\b/i);
        const udpMatch = text.match(/\bUDP\s+(\d+)\b/i);

        return {
          track: trackLine,
          name: nameLine,
          carClass: classMatch ? classMatch[1].toUpperCase() : 'Mixed',
          drivers: driverMatch ? Number(driverMatch[1]) : 0,
          maxDrivers: driverMatch ? Number(driverMatch[2]) : 0,
          variability: variabilityMatch ? Number(variabilityMatch[1]) : 0,
          sessions: sessionMatches,
          ip: ipMatch ? ipMatch[1] : null,
          tcpPort: tcpMatch ? Number(tcpMatch[1]) : null,
          udpPort: udpMatch ? Number(udpMatch[1]) : null,
        };
      };

      const candidates = Array.from(document.querySelectorAll('body *'))
        .filter(el => {
          const txt = el.innerText || '';
          return txt.includes(searchTerm) && txt.length < 1600;
        });

      const unique = [];
      const seen = new Set();

      for (const card of candidates) {
        const parsed = parseCard(card.innerText || '');
        if (!parsed) continue;
        const key = parsed.track + '|' + parsed.name + '|' + parsed.drivers + '/' + parsed.maxDrivers;
        if (seen.has(key)) continue;
        seen.add(key);
        unique.push(parsed);
      }

      return unique;
    }, SEARCH_TERM);

    debugLog('visible cards found before cleanup', data.length);
    const filtered = normaliseScrapedServers(data);
    debugLog('visible cards after cleanup', filtered.length);
    const enriched = await enrichTcpPorts(page, filtered);
    const cleaned = normaliseScrapedServers(enriched);
    debugLog('visible cards after tcp enrichment', cleaned.length, 'with tcp', cleaned.filter(s => s.tcpPort).length);
    return {
      ok: true,
      servers: cleaned,
      checkedAt: new Date().toISOString(),
      error: null,
      accOnline,
    };
  } finally {
    await context.close().catch(() => {});
  }
}

async function scrapeServers() {
  try {
    return await withTimeout(scrapeServersCore(), SCRAPE_TIMEOUT_MS, 'scrapeServers');
  } catch (err) {
    if (/Target page|browser has been closed|context or browser|timed out/i.test(String(err?.message || err))) {
      await resetBrowser('scrape failure');
    }
    return {
      ok: false,
      servers: [],
      checkedAt: new Date().toISOString(),
      error: err.message || String(err),
      accOnline: null,
    };
  }
}

function getSeriesType(serverName) {
  const parts = String(serverName || '').split('|').map(p => p.trim()).filter(Boolean);
  return parts.length >= 2 ? parts[1] : 'Other Servers';
}

function buildEmbeds(scrape) {
  const embed = new EmbedBuilder()
    .setTitle('🏁 OOR Server Status - slightly delayed from live')
    .setDescription([
      `**Welcome to OOR Server Status**`,
      `ACC Server Status – ${statusEmoji(scrape.accOnline !== false)} ${scrape.accOnline === false ? 'Connect through Offline Connect Links' : 'Connect through ACC'}`,
      '',
      `Persistent status monitor • Last changed • ${scrape.lastChangedAt || nowString()}`,
    ].join('\n').trimEnd())
    .setColor(scrape.accOnline === false ? 0xed4245 : 0x57f287);

  if (!scrape.servers.length) {
    embed.addFields({ name: `${OOR_ICON_EMOJI} Live Drivers – 0 Drivers`, value: 'No private OORAP servers found right now.' });
    return [embed];
  }

  const totalDrivers = scrape.servers.reduce((sum, server) => sum + Number(server.drivers || 0), 0);

  embed.addFields({
    name: `${OOR_ICON_EMOJI} Live Drivers – ${totalDrivers} Driver${totalDrivers === 1 ? '' : 's'}`,
    value: `**${scrape.servers.length}** [OORAP] Octane Online Racing Servers Live ${PANEL_WIDTH_PAD}`,
  });

  const groups = new Map();
  for (const server of scrape.servers) {
    const seriesType = getSeriesType(server.name);
    if (!groups.has(seriesType)) groups.set(seriesType, []);
    groups.get(seriesType).push(server);
  }

  const embeds = [embed];
  const sortedGroups = [...groups.entries()].sort((a, b) => a[0].localeCompare(b[0]));

  for (const [seriesType, servers] of sortedGroups) {
    if (embeds.length >= MAX_EMBEDS - 1) break;

    const seriesEmbed = new EmbedBuilder()
      .setTitle(`🏁 ${truncate(seriesType, 90)}`)
      .setColor(scrape.accOnline === false ? 0xed4245 : 0x57f287);

    const lines = servers.map(server => {
      const drivers = `${server.drivers}/${server.maxDrivers}`;
      const bar = driverBar(server.drivers, server.maxDrivers);
      const sessions = formatSessions(server.sessions);
      const offlineLink = connectLink(server);
      const line3Parts = [`${weatherEmoji()}  🔀 ${server.variability}%`, sessions];
      if (offlineLink) line3Parts.push(offlineLink);
      return [
        `**${truncate(server.track.toUpperCase(), 48)}**`,
        `${truncate(cleanServerName(server.name), 90)}`,
        `${bar} **${drivers}**`,
        line3Parts.join('  •  '),
      ].join('\n');
    });

    let chunk = '';
    let part = 1;
    for (const line of lines) {
      if ((chunk + '\n\n' + line).length > MAX_FIELD_CHARS) {
        seriesEmbed.addFields({ name: part === 1 ? `${servers.length} server(s)` : `More ${seriesType} ${part}`, value: `${chunk || '-'}` });
        chunk = line;
        part += 1;
      } else {
        chunk = chunk ? `${chunk}\n\n${line}` : line;
      }
    }
    if (chunk) seriesEmbed.addFields({ name: part === 1 ? `${servers.length} server(s)` : `More ${seriesType} ${part}`, value: `${chunk}` });
    embeds.push(seriesEmbed);
  }

  if (embeds.length < MAX_EMBEDS) {
    embeds.push(buildOfflineHelpEmbed());
  }

  if (sortedGroups.length > MAX_EMBEDS - 2) {
    embeds[0].addFields({
      name: '⚠️ Display limit',
      value: `Discord allows ${MAX_EMBEDS} embeds per message, so some series groups were not shown.`,
    });
  }

  return embeds;
}

async function findExistingPanelMessage(channel, clientUserId) {
  const messages = await channel.messages.fetch({ limit: 20 }).catch(() => null);
  if (!messages) return null;
  return messages.find(msg =>
    msg.author?.id === clientUserId &&
    msg.embeds?.some(embed => String(embed.title || '').includes('OOR Server Status'))
  ) || null;
}

async function pruneDuplicatePanelMessages(channel, keepMessageId, clientUserId) {
  const messages = await channel.messages.fetch({ limit: 30 }).catch(() => null);
  if (!messages) return;

  const panelMessages = messages.filter(msg =>
    msg.author?.id === clientUserId &&
    msg.id !== keepMessageId &&
    msg.embeds?.some(embed => String(embed.title || '').includes('OOR Server Status'))
  );

  for (const msg of panelMessages.values()) {
    await msg.delete().catch(() => {});
  }
}

function isUnknownMessageError(err) {
  return err?.code === 10008 || err?.rawError?.code === 10008;
}

function saveGuildMessageId(config, guildId, guildCfg, messageId) {
  guildCfg.messageId = messageId;
  config.guilds[guildId] = guildCfg;
  saveConfig(config);
}


function buildComponents() {
  return [
    new ActionRowBuilder().addComponents(
      new ButtonBuilder()
        .setLabel('Offline Help')
        .setEmoji('🌐')
        .setStyle(ButtonStyle.Link)
        .setURL(OFFLINE_HELP_URL)
    ),
  ];
}

function buildOfflineHelpEmbed() {
  return new EmbedBuilder()
    .setTitle(`${OOR_ICON_EMOJI} OOR Offline Help`)
    .setDescription([
      'If the ACC server browser is down, use the **Offline Connect ↗** links beside each OOR server.',
      `Need setup instructions? Use the **Offline Help** button below. ${PANEL_WIDTH_PAD}`,
    ].join('\n').trimEnd())
    .setColor(0x57f287);
}

async function postOrEditPanel(client, guildId, scrape, options = {}) {
  const config = loadConfig();
  const guildCfg = config.guilds[guildId];
  if (!guildCfg?.channelId) return;

  const channel = await client.channels.fetch(guildCfg.channelId).catch(err => {
    console.error(`[SelfHeal] Unable to fetch channel for guild ${guildId}:`, err.message || err);
    return null;
  });
  if (!channel) return;

  const payload = { embeds: buildEmbeds(scrape), components: buildComponents() };

  if (guildCfg.messageId) {
    try {
      const message = await channel.messages.fetch(guildCfg.messageId);
      await message.edit(payload);
      if (options.pruneDuplicates) await pruneDuplicatePanelMessages(channel, message.id, client.user.id);
      return message;
    } catch (err) {
      if (isUnknownMessageError(err)) {
        console.log(`[SelfHeal] Saved panel message missing for guild ${guildId}; recreating.`);
        delete guildCfg.messageId;
        config.guilds[guildId] = guildCfg;
        saveConfig(config);
      } else {
        console.error(`[SelfHeal] Unable to edit saved panel for guild ${guildId}:`, err);
      }
    }
  }

  const existing = await findExistingPanelMessage(channel, client.user.id);
  if (existing) {
    try {
      await existing.edit(payload);
      saveGuildMessageId(config, guildId, guildCfg, existing.id);
      if (options.pruneDuplicates) await pruneDuplicatePanelMessages(channel, existing.id, client.user.id);
      return existing;
    } catch (err) {
      if (isUnknownMessageError(err)) {
        console.log(`[SelfHeal] Existing panel disappeared before edit for guild ${guildId}; sending a new one.`);
      } else {
        console.error(`[SelfHeal] Unable to edit discovered panel for guild ${guildId}:`, err);
      }
    }
  }

  try {
    const sent = await channel.send(payload);
    saveGuildMessageId(config, guildId, guildCfg, sent.id);
    if (options.pruneDuplicates) await pruneDuplicatePanelMessages(channel, sent.id, client.user.id);
    return sent;
  } catch (err) {
    console.error(`[SelfHeal] Unable to send replacement panel for guild ${guildId}:`, err);
    return null;
  }
}

async function refreshAll(client, options = {}) {
  if (shuttingDown) return;
  if (scrapeInFlight && !options.force) {
    console.log('[Refresh] skipped because previous scrape is still running.');
    return;
  }
  scrapeInFlight = true;
  try {
    const config = loadConfig();
    const guildIds = Object.keys(config.guilds).filter(gid => config.guilds[gid]?.channelId);
    if (!guildIds.length) return;

    let scrape = await scrapeServers();
    const currentTime = nowString();
    let shouldPost = Boolean(options.force);

    if (scrape.ok) {
      scrape.servers = normaliseScrapedServers(scrape.servers);
      scrape.lastChangedAt = config.global.lastChangedAt || currentTime;
      const signature = scrapeSignature(scrape);
      if (signature !== config.global.lastSignature) {
        config.global.lastSignature = signature;
        config.global.lastChangedAt = currentTime;
        scrape.lastChangedAt = currentTime;
        shouldPost = true;
      }
      config.global.lastGoodScrape = scrape;
      saveConfig(config);
      saveLastGoodScrape(scrape);
    } else {
      const cached = config.global.lastGoodScrape || loadLastGoodScrape();
      if (!cached) {
        console.log(`[Refresh] ok=false servers=0 guilds=${guildIds.length} at=${currentTime} error=${scrape.error}`);
        return;
      }
      scrape = { ...cached, cachedFallback: true, error: scrape.error, lastChangedAt: cached.lastChangedAt || config.global.lastChangedAt || currentTime };
      shouldPost = Boolean(options.force);
    }

    const heartbeatSeconds = Math.max(0, Number(config.global.heartbeatSeconds || LAST_CHECKED_HEARTBEAT_SECONDS || 0));
    const lastPostAt = Number(config.global.lastPanelPostAt || 0);
    if (!shouldPost && heartbeatSeconds > 0 && Date.now() - lastPostAt > heartbeatSeconds * 1000) shouldPost = true;

    console.log(`[Refresh] ok=${scrape.ok !== false} servers=${scrape.servers.length} guilds=${guildIds.length} changed=${shouldPost ? 'yes' : 'no'} at=${currentTime}${scrape.error && !scrape.cachedFallback ? ` error=${scrape.error}` : ''}`);
    if (!shouldPost) return;

    config.global.lastPanelPostAt = Date.now();
    saveConfig(config);
    for (const guildId of guildIds) {
      await postOrEditPanel(client, guildId, scrape, { pruneDuplicates: Boolean(options.pruneDuplicates) }).catch(err => console.error(`[Refresh] guild ${guildId}:`, err));
    }
  } finally {
    scrapeInFlight = false;
  }
}
const commands = [
  new SlashCommandBuilder()
    .setName('statussetup')
    .setDescription('Bind this channel as the OOR Server Status channel and post the persistent panel.')
    .setDefaultMemberPermissions(PermissionFlagsBits.ManageGuild),
  new SlashCommandBuilder()
    .setName('statusrefresh')
    .setDescription('Force refresh the OOR Server Status panel now.')
    .setDefaultMemberPermissions(PermissionFlagsBits.ManageGuild),
  new SlashCommandBuilder()
    .setName('healthcheck')
    .setDescription('Show OOR Server Status Bot health.'),
  new SlashCommandBuilder()
    .setName('statusremove')
    .setDescription('Remove this guild status panel binding from config.')
    .setDefaultMemberPermissions(PermissionFlagsBits.ManageGuild),
  new SlashCommandBuilder()
    .setName('statusconfig')
    .setDescription('Show the current OOR Server Status configuration.')
    .setDefaultMemberPermissions(PermissionFlagsBits.ManageGuild),
  new SlashCommandBuilder()
    .setName('setrefresh')
    .setDescription('Set the global refresh interval for this bot instance.')
    .addIntegerOption(option => option.setName('seconds').setDescription('Refresh interval in seconds. Minimum 15.').setMinValue(15).setMaxValue(3600).setRequired(true))
    .setDefaultMemberPermissions(PermissionFlagsBits.ManageGuild),
  new SlashCommandBuilder()
    .setName('statusdebug')
    .setDescription('Run a debug scrape and show a compact result summary.')
    .setDefaultMemberPermissions(PermissionFlagsBits.ManageGuild),
].map(cmd => cmd.toJSON());
async function registerCommands() {
  const rest = new REST({ version: '10' }).setToken(TOKEN);
  await rest.put(Routes.applicationCommands(CLIENT_ID), { body: commands });
  console.log('Global slash commands registered. They may take a few minutes to appear.');
}

async function main() {
  ensureData();
  await registerCommands();

  const client = new Client({ intents: [GatewayIntentBits.Guilds, GatewayIntentBits.GuildMessages] });
  let refreshTimer = null;

  function startRefreshLoop() {
    if (refreshTimer) return;
    const seconds = getRefreshSeconds();
    refreshTimer = setInterval(() => refreshAll(client).catch(console.error), seconds * 1000);
    console.log(`Refresh loop active every ${seconds}s`);
  }

  function restartRefreshLoop() {
    if (refreshTimer) clearInterval(refreshTimer);
    refreshTimer = null;
    startRefreshLoop();
  }

  client.once('clientReady', async () => {
    console.log(`Logged in as ${client.user.tag}`);
    console.log(`Search=${SEARCH_TERM} visibility=${VISIBILITY} refresh=${REFRESH_SECONDS}s`);
    console.log(`OOR icon emoji=${OOR_ICON_EMOJI}`);
    console.log(`Connect base=${CONNECT_BASE_URL}`);
    console.log(`Connect links=${ENABLE_CONNECT_LINKS ? 'on' : 'off'} tcp timeout=${CONNECT_SCRAPE_TIMEOUT_MS}ms`);
    console.log(`Scrape timeout=${SCRAPE_TIMEOUT_MS}ms debug=${DEBUG ? 'on' : 'off'}`);
    await refreshAll(client, { force: true, pruneDuplicates: true }).catch(console.error);
    startRefreshLoop();
  });

  client.on('messageCreate', async message => {
    if (!POST_PROTECTION_ENABLED) return;
    if (!message.guildId || message.author?.bot) return;

    const config = loadConfig();
    const guildCfg = config.guilds?.[message.guildId];
    if (!guildCfg?.channelId || message.channelId !== guildCfg.channelId) return;

    setTimeout(async () => {
      try {
        const fresh = await message.channel.messages.fetch(message.id).catch(() => null);
        if (!fresh || fresh.author?.bot || fresh.pinned) return;

        await fresh.delete().catch(err => {
          if (!isUnknownMessageError(err)) throw err;
        });

        const notice = await message.channel.send({
          content: `${message.author}, this channel is reserved for live OOR Server Status posts. Your message has been removed to keep the status panel clear.`,
          allowedMentions: { users: [message.author.id], roles: [], repliedUser: false },
        }).catch(err => {
          console.error('[PostProtect] Unable to send removal notice:', err.message || err);
          return null;
        });

        if (notice) {
          setTimeout(() => notice.delete().catch(() => {}), USER_POST_NOTICE_SECONDS * 1000);
        }
      } catch (err) {
        console.error('[PostProtect] Unable to remove user post:', err.message || err);
      }
    }, USER_POST_DELETE_SECONDS * 1000);
  });

  client.on('interactionCreate', async interaction => {
    if (!interaction.isChatInputCommand()) return;

    if (interaction.commandName === 'statussetup') {
      const config = loadConfig();
      config.guilds[interaction.guildId] = {
        ...(config.guilds[interaction.guildId] || {}),
        channelId: interaction.channelId,
      };
      saveConfig(config);
      await interaction.reply({ content: 'Setting up OOR Server Status panel in this channel...', flags: MessageFlags.Ephemeral });
      await refreshAll(client, { force: true, pruneDuplicates: true });
      const latest = loadConfig().global.lastGoodScrape;
      await interaction.editReply(`Done. Found ${latest?.servers?.length || 0} private OORAP server(s).`);
      return;
    }

    if (interaction.commandName === 'statusrefresh') {
      await interaction.reply({ content: 'Refreshing OOR Server Status now...', flags: MessageFlags.Ephemeral });
      await refreshAll(client, { force: true, pruneDuplicates: true });
      const latest = loadConfig().global.lastGoodScrape;
      await interaction.editReply(`Refresh complete. Found ${latest?.servers?.length || 0} private OORAP server(s).`);
      return;
    }

    if (interaction.commandName === 'statusremove') {
      const config = loadConfig();
      delete config.guilds[interaction.guildId];
      saveConfig(config);
      await interaction.reply({ content: 'Removed this guild from OOR Server Status config.', flags: MessageFlags.Ephemeral });
      return;
    }

    if (interaction.commandName === 'statusconfig') {
      const config = loadConfig();
      const guildCfg = config.guilds[interaction.guildId] || {};
      await interaction.reply({ flags: MessageFlags.Ephemeral, content: [
        '**OOR Server Status Config**',
        `Configured channel: ${guildCfg.channelId || '-'}`,
        `Panel message: ${guildCfg.messageId || '-'}`,
        `Search: ${SEARCH_TERM}`,
        `Visibility: ${VISIBILITY}`,
        `Refresh: ${getRefreshSeconds(config)}s`,
        `Heartbeat edit: ${config.global.heartbeatSeconds || LAST_CHECKED_HEARTBEAT_SECONDS}s`,
        `Connect base: ${CONNECT_BASE_URL}`,
        `Offline help: ${OFFLINE_HELP_URL}`,
        `ACC lobby status URL: ${ACC_LOBBY_STATUS_URL}`,
        `Configured guilds: ${Object.keys(config.guilds).length}`,
      ].join('\n') });
      return;
    }

    if (interaction.commandName === 'setrefresh') {
      const seconds = interaction.options.getInteger('seconds', true);
      const config = loadConfig();
      config.global.refreshSeconds = Math.max(15, Number(seconds));
      saveConfig(config);
      restartRefreshLoop();
      await interaction.reply({ content: `Refresh interval set to ${config.global.refreshSeconds}s.`, flags: MessageFlags.Ephemeral });
      return;
    }

    if (interaction.commandName === 'statusdebug') {
      await interaction.reply({ content: 'Running debug scrape...', flags: MessageFlags.Ephemeral });
      const scrape = await scrapeServers();
      const servers = Array.isArray(scrape.servers) ? scrape.servers : [];
      const withPorts = servers.filter(s => s.tcpPort).length;
      const totalDrivers = servers.reduce((sum, s) => sum + Number(s.drivers || 0), 0);
      const sample = servers.slice(0, 5).map(s => `- ${s.track}: ${cleanServerName(s.name)} (${s.drivers}/${s.maxDrivers}) TCP=${s.tcpPort || '-'}`).join('\n') || '-';
      await interaction.editReply(['**OOR Server Status Debug**', `Scrape ok: ${scrape.ok ? 'yes' : 'no'}`, `ACC lobby online: ${scrape.accOnline === false ? 'no' : scrape.accOnline === true ? 'yes' : 'unknown'}`, `Servers: ${servers.length}`, `Drivers: ${totalDrivers}`, `TCP ports found: ${withPorts}/${servers.length}`, scrape.error ? `Error: ${truncate(scrape.error, 400)}` : '', '**Sample**', sample].filter(Boolean).join('\n'));
      return;
    }

    if (interaction.commandName === 'healthcheck') {
      const config = loadConfig();
      const guildCfg = config.guilds[interaction.guildId];
      await interaction.reply({
        flags: MessageFlags.Ephemeral,
        content: [
          '**OOR Server Status Bot Health**',
          `Bot: ${client.user.tag}`,
          `Configured here: ${guildCfg?.channelId ? 'yes' : 'no'}`,
          `Channel ID: ${guildCfg?.channelId || '-'}`,
          `Message ID: ${guildCfg?.messageId || '-'}`,
          `Search: ${SEARCH_TERM}`,
          `Visibility: ${VISIBILITY}`,
          `OOR icon emoji: ${OOR_ICON_EMOJI}`,
          `Connect base: ${CONNECT_BASE_URL}`,
          `Connect links: ${ENABLE_CONNECT_LINKS ? 'on' : 'off'}`,
          `TCP scrape timeout: ${CONNECT_SCRAPE_TIMEOUT_MS}ms`,
          `Offline help: ${OFFLINE_HELP_URL}`,
          `Refresh: ${getRefreshSeconds(config)}s`,
          `ACC lobby status URL: ${ACC_LOBBY_STATUS_URL}`,
          `Browser recycle: ${BROWSER_RECYCLE_MINUTES} min`,
          `Blocked browser resources: ${[...BLOCKED_BROWSER_RESOURCE_TYPES].join(', ') || 'none'}`,
          `Last good cache: ${config.global.lastGoodScrape?.servers?.length || 0} server(s)`,
          `Configured guilds: ${Object.keys(config.guilds).length}`,
        ].join('\n'),
      });
    }
  });

  async function shutdown(signal) {
    if (shuttingDown) return;
    shuttingDown = true;
    console.log(`[Shutdown] ${signal} received; closing browser and Discord client`);
    await resetBrowser('shutdown').catch(() => {});
    await client.destroy().catch(() => {});
    process.exit(0);
  }

  process.on('SIGINT', () => shutdown('SIGINT'));
  process.on('SIGTERM', () => shutdown('SIGTERM'));

  await client.login(TOKEN);
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
