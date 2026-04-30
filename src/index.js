require('dotenv').config();

const fs = require('fs');
const path = require('path');
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
let scrapeInFlight = false;

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


function httpTimeoutMs(value, fallback) {
  const n = Number(value || fallback);
  return Number.isFinite(n) && n > 0 ? n : fallback;
}

const HTTP_HEADERS = {
  'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36',
  accept: 'text/html,application/xhtml+xml,application/xml;q=0.9,application/json;q=0.8,text/plain;q=0.7,*/*;q=0.6',
  'accept-language': 'en-GB,en;q=0.9,en-US;q=0.8',
  'cache-control': 'no-cache',
  pragma: 'no-cache',
  connection: 'keep-alive',
};

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function fetchTextOnce(url, timeoutMs, label = 'fetch') {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const res = await fetch(url, {
      signal: controller.signal,
      headers: HTTP_HEADERS,
      redirect: 'follow',
    });
    if (!res.ok) throw new Error(`${label} returned HTTP ${res.status}`);
    return await res.text();
  } finally {
    clearTimeout(timer);
  }
}

async function fetchText(url, timeoutMs, label = 'fetch', retries = 1) {
  let lastError = null;
  for (let attempt = 0; attempt <= retries; attempt += 1) {
    try {
      const text = await fetchTextOnce(url, timeoutMs, label);
      if (text && text.trim().length > 0) return text;
      throw new Error(`${label} returned an empty response`);
    } catch (err) {
      lastError = err;
      if (attempt >= retries) break;
      debugLog(`${label} attempt ${attempt + 1} failed; retrying`, err.message || err);
      await sleep(750);
    }
  }
  throw lastError;
}

async function fetchJson(url, timeoutMs, label = 'fetchJson') {
  const text = await fetchText(url, timeoutMs, label, 1);
  try { return JSON.parse(text); }
  catch (err) { throw new Error(`${label} did not return JSON: ${err.message || err}`); }
}

function htmlToLines(html) {
  return String(html || '')
    .replace(/<script[\s\S]*?<\/script>/gi, '\n')
    .replace(/<style[\s\S]*?<\/style>/gi, '\n')
    .replace(/<br\s*\/?\s*>/gi, '\n')
    .replace(/<\/(p|div|section|article|li|tr|td|th|h[1-6]|button|span)>/gi, '\n')
    .replace(/<[^>]+>/g, ' ')
    .replace(/&nbsp;/g, ' ')
    .replace(/&amp;/g, '&')
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&#x27;/g, "'")
    .replace(/&quot;/g, '"')
    .split(/\n+/)
    .map(line => line.replace(/\s+/g, ' ').trim())
    .filter(Boolean);
}

function buildServerListUrls() {
  const base = new URL(ACC_STATUS_URL);
  const variants = [];
  const add = (params = {}) => {
    const u = new URL(base.toString());
    for (const [k, v] of Object.entries(params)) {
      if (v !== undefined && v !== null && String(v) !== '') u.searchParams.set(k, String(v));
    }
    const txt = u.toString();
    if (!variants.includes(txt)) variants.push(txt);
  };

  add({ search: SEARCH_TERM, visibility: VISIBILITY });
  add({ q: SEARCH_TERM, visibility: VISIBILITY });
  add({ search: SEARCH_TERM, private: VISIBILITY === 'private' ? 'true' : undefined });
  add({ query: SEARCH_TERM, private: VISIBILITY === 'private' ? 'true' : undefined });
  add();
  return variants;
}

function looksLikeTrack(line) {
  return /^(Barcelona|Brands Hatch|Circuit|Donington|Hungaroring|Imola|Indianapolis|Kyalami|Laguna Seca|Misano|Monza|Mount Panorama|Nürburgring|Nurburgring|Oulton Park|Paul Ricard|Red Bull Ring|Silverstone|Snetterton|Spa|Suzuka|Valencia|Watkins Glen|Zandvoort|Zolder|Autodromo)/i.test(String(line || ''));
}

function parseServersFromHtml(html) {
  const lines = htmlToLines(html);
  const search = SEARCH_TERM.toUpperCase();
  const servers = [];

  for (let i = 0; i < lines.length; i += 1) {
    const line = lines[i];
    if (!String(line).toUpperCase().includes(search)) continue;

    let track = 'Unknown track';
    let carClass = 'Mixed';

    for (let j = i - 1; j >= Math.max(0, i - 6); j -= 1) {
      if (/^(Mixed|GT3|GT4|GT2|TCX|GTC)$/i.test(lines[j])) { carClass = lines[j].toUpperCase(); break; }
    }
    for (let j = i - 1; j >= Math.max(0, i - 8); j -= 1) {
      if (looksLikeTrack(lines[j])) { track = lines[j]; break; }
    }

    const window = lines.slice(i + 1, i + 16);
    const joined = window.join(' ');
    const driverMatch = joined.match(/(\d+)\s*\/\s*(\d+)/);
    if (!driverMatch) continue;

    const sessions = [];
    for (let k = 0; k < window.length; k += 1) {
      const combined = `${window[k]} ${window[k + 1] || ''}`;
      const m = combined.match(/\b([PQR])\s+(\d+)\s*min\b/i);
      if (m) {
        const session = `${m[1].toUpperCase()} ${m[2]}`;
        if (!sessions.includes(session)) sessions.push(session);
      }
    }

    const variabilityMatch = joined.match(/(\d+)%/);
    const ipMatch = joined.match(/\bIP\s+((?:\d{1,3}\.){3}\d{1,3})\b/i);
    const tcpMatch = joined.match(/\bTCP\s+(\d+)\b/i);
    const udpMatch = joined.match(/\bUDP\s+(\d+)\b/i);

    servers.push({
      track,
      name: line,
      carClass,
      drivers: Number(driverMatch[1]),
      maxDrivers: Number(driverMatch[2]),
      variability: variabilityMatch ? Number(variabilityMatch[1]) : 0,
      sessions,
      ip: ipMatch ? ipMatch[1] : null,
      tcpPort: tcpMatch ? Number(tcpMatch[1]) : null,
      udpPort: udpMatch ? Number(udpMatch[1]) : null,
    });
  }

  return normaliseScrapedServers(servers);
}

function enrichFromLastGood(servers) {
  const cached = loadLastGoodScrape()?.servers || loadConfig()?.global?.lastGoodScrape?.servers || [];
  if (!Array.isArray(cached) || !cached.length) return servers;
  const byName = new Map(cached.map(srv => [String(srv.name || '').trim().toUpperCase(), srv]));
  return servers.map(server => {
    const cachedServer = byName.get(String(server.name || '').trim().toUpperCase());
    if (!cachedServer) return server;
    return {
      ...server,
      ip: server.ip || cachedServer.ip || null,
      tcpPort: server.tcpPort || cachedServer.tcpPort || null,
      udpPort: server.udpPort || cachedServer.udpPort || null,
      sessions: server.sessions?.length ? server.sessions : cachedServer.sessions || [],
    };
  });
}

async function checkAccLobbyStatus() {
  try {
    const apiUrl = new URL('/api/v2/acc/status', ACC_LOBBY_STATUS_URL).toString();
    const data = await fetchJson(apiUrl, httpTimeoutMs(CONNECT_SCRAPE_TIMEOUT_MS, 1200), 'ACC status API');
    if (Number(data?.status) === 1) return true;
    if (Number(data?.status) === 0) return false;
    return null;
  } catch (err) {
    debugLog('acc status API check skipped', err.message || err);
    return null;
  }
}

async function scrapeServersCore() {
  debugLog('http scrape start');
  const accOnline = await checkAccLobbyStatus();
  const urls = buildServerListUrls();
  let lastError = null;

  for (const url of urls) {
    try {
      debugLog('fetch server list', url);
      const html = await fetchText(url, httpTimeoutMs(SCRAPE_TIMEOUT_MS, 60000), 'server list', 1);
      const cleaned = normaliseScrapedServers(enrichFromLastGood(parseServersFromHtml(html)));
      debugLog('http scrape parsed', cleaned.length, 'servers from', url, 'with tcp', cleaned.filter(s => s.tcpPort).length);
      if (cleaned.length) {
        return { ok: true, servers: cleaned, checkedAt: new Date().toISOString(), error: null, accOnline };
      }
    } catch (err) {
      lastError = err;
      debugLog('server list fetch failed', url, err.message || err);
    }
  }

  const cached = loadLastGoodScrape() || loadConfig()?.global?.lastGoodScrape || null;
  if (cached?.servers?.length) {
    debugLog('using cached last-good server list after empty HTTP scrape');
    return {
      ok: true,
      servers: cached.servers,
      checkedAt: new Date().toISOString(),
      error: 'Live HTTP scrape returned no OOR servers; reused last-good server list.',
      accOnline,
    };
  }

  throw lastError || new Error('Live HTTP scrape returned no OOR servers and no last-good cache is available.');
}

async function scrapeServers() {
  try {
    return await withTimeout(scrapeServersCore(), SCRAPE_TIMEOUT_MS + 2000, 'scrapeServers');
  } catch (err) {
    return { ok: false, servers: [], checkedAt: new Date().toISOString(), error: err.message || String(err), accOnline: null };
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
    console.log(`Connect links=${ENABLE_CONNECT_LINKS ? 'on' : 'off'} mode=http-cache-enriched`);
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
              `Last good cache: ${config.global.lastGoodScrape?.servers?.length || 0} server(s)`,
          `Configured guilds: ${Object.keys(config.guilds).length}`,
        ].join('\n'),
      });
    }
  });

  process.on('SIGINT', async () => {
    process.exit(0);
  });

  await client.login(TOKEN);
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
