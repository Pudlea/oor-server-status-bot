# OOR Server Status Bot

Multi-guild Discord bot for monitoring private OOR ACC servers from acc-status.jonatan.net.

## Setup

```bash
npm install
cp .env.example .env
npm start
```

Required `.env` values:

```env
DISCORD_TOKEN=...
CLIENT_ID=...
```

Then run `/statussetup` in the Discord channel where the persistent panel should live.

## Main commands

- `/statussetup` - bind the current channel and post/self-heal the panel.
- `/statusrefresh` - force an immediate scrape and panel update.
- `/statusremove` - remove this guild binding.
- `/statusconfig` - show current config.
- `/setrefresh seconds:<n>` - change refresh interval without editing `.env`.
- `/statusdebug` - run a compact debug scrape summary.
- `/healthcheck` - show bot health.

## Production notes

- Keeps one Playwright browser alive and recycles it periodically.
- Skips overlapping scrapes if a refresh is still running.
- Stores the last successful scrape in `data/last-good-scrape.json`.
- If a scrape fails, the bot keeps the last good panel instead of posting an error.
- Discord panels are only edited when data changes or on the heartbeat interval.

