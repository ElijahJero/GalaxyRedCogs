# GalaxyRedCogs

A collection of cogs for [Red-DiscordBot](https://github.com/Cog-Creators/Red-DiscordBot).

## Installation

Add this repository to your Red instance:

```
[p]repo add GalaxyRedCogs https://github.com/ElijahJero/GalaxyRedCogs
```

Then install and load individual cogs:

```
[p]cog install GalaxyRedCogs <cog_name>
[p]load <cog_name>
```

Use `[p]cog list GalaxyRedCogs` to see every package in this repo.

## Available Cogs

### AprilFools

Owner-only April Fools prank: DMs the bot a rickroll URL and channel IDs, then posts a fake scam giveaway embed.

**Commands:**
- `[p]fool <rickroll-url> <channel_id> [channel_id...]` — Deploy the prank (owner, DMs only)

```
[p]cog install GalaxyRedCogs AprilFools
[p]load AprilFools
```

### BotEngine

Relays messages from enabled Discord channels to an Ollama-based Bot Engine HTTP server and posts the replies.

**Features:**
- Per-channel enable/disable
- Global and per-channel pauses
- Response cooldowns
- User block list
- Conversation reset and basic stats

**Commands:**
- `[p]botengine setapi <url>` — Set the Bot Engine server URL (owner)
- `[p]botengine enable [channel]` — Enable the bot in a channel
- `[p]botengine disable [channel]` — Disable the bot in a channel
- `[p]botengine status` — Show current configuration

Requires a separately running Bot Engine server.

```
[p]cog install GalaxyRedCogs BotEngine
[p]load BotEngine
```

### BotSheild

Bot protection for Discord servers.

**Features:**
- Captcha verification for new members
- Scam detection for newer members' messages
- Configurable warning ping roles

**Commands:**
- `[p]botsheild protect` — Enable captcha protection
- `[p]botsheild unprotect` — Disable protection
- `[p]botsheild scam` — Configure scam detection
- `[p]botsheild pingroles` — Manage roles pinged on warnings

```
[p]cog install GalaxyRedCogs BotSheild
[p]load BotSheild
```

### CMLink

Tournament integration with Challenger Mode.

**Features:**
- Monitors Challenger Mode tournaments
- Creates Discord channels for active matches
- Manages linked tournament participants
- Optional Grafana Loki logging

**Commands:**
- `[p]cmlink settings setrefreshtoken <token>` — Store the Challenger Mode refresh token (owner)
- `[p]cmlink tournament settings setupdatechannel <channel>` — Tournament update channel
- `[p]cmlink tournament settings setlobbyvoice <channel>` — Lobby voice channel
- `[p]cmlink tournament settings setcategory <category>` — Category for match channels
- `[p]cmlink tournament add <id>` — Track a tournament
- `[p]cmlink connect` — Link a Discord user to Challenger Mode

```
[p]cog install GalaxyRedCogs CMLink
[p]load CMLink
```

### Elections

Anonymous plurality elections. Voter identity and ballot choices are stored separately so they cannot be correlated.

**Commands:**
- `[p]election start <#channel>` — Start an election from an attached JSON config
- `[p]election join <id>` — Open a DM ballot (if automatic DMs failed)
- `[p]election end <id>` — End an election early and post results
- `[p]election list` — List active elections
- `[p]election info <id>` — Show positions and candidates (no vote counts)
- `[p]election example` — Example JSON config

```
[p]cog install GalaxyRedCogs Elections
[p]load Elections
```

### fluent

In-channel auto-translation between two languages, plus a manual translate command. By Vertyco.

**Commands:**
- `[p]fluent add <language1> <language2> [channel]` — Translate between two languages in a channel
- `[p]fluent only <language> [channel]` — Translate all messages to one language
- `[p]translate <language> [message]` — Manual translation
- `[p]fluent openai` / `[p]fluent deepl` — Optional API keys (owner)

See [fluent/README.md](fluent/README.md) for the full command list. Translation API logic is from [vrt-cogs](https://github.com/vertyco/vrt-cogs) by Vertyco (MIT License).

```
[p]cog install GalaxyRedCogs fluent
[p]load fluent
```

### linkfixer

Rewrites social media URLs so Discord can embed them (Twitter/X, Instagram, Reddit, TikTok, Pixiv, Threads, and others). Based on [LinkFixer](https://github.com/hollowstrawberry) by hollowstrawberry.

**Commands:**
- `[p]linkfixer enable` — Enable in this server
- `[p]linkfixer disable` — Disable in this server
- `[p]linkfixer links list` — List available fixers
- `[p]linkfixer translate enable` — Prefer English embeds where supported

```
[p]cog install GalaxyRedCogs linkfixer
[p]load linkfixer
```

### ModToolsPlus

Account quarantine across all mutual servers. Useful when an account is compromised: the restriction is re-applied if they rejoin.

**Commands:**
- `[p]modtoolsplus quarantine <user> [reason]` — Quarantine a user
- `[p]modtoolsplus unquarantine <user>` — Lift quarantine
- `[p]modtoolsplus quarantinelist` — List quarantined users

```
[p]cog install GalaxyRedCogs ModToolsPlus
[p]load ModToolsPlus
```

### NOAI

Estimates whether an image looks AI-generated using OpenAI CLIP. Runs locally; downloads the CLIP model on first use.

**Note:** Installs `torch`, `transformers`, and `pillow`. First analysis is slow and memory-heavy.

**Commands:**
- `[p]noai enableimages <true/false>` — Toggle image analysis for the server
- `[p]noai setimagethreshold <percent>` — Confidence threshold
- `[p]noai analyze` — Analyze an attached, linked, or replied-to image

```
[p]cog install GalaxyRedCogs NOAI
[p]load NOAI
```

### PolyglotBridge

Cross-channel translation bridges for multilingual communities. Messages posted in one linked channel are translated and relayed to every other channel in the bridge.

**Features:**
- Link language-specific channels into a named bridge
- Optional OpenAI and DeepL keys for higher quality
- Manual `[p]translate` command

**Commands:**
- `[p]polyglot create <name>` — Create a bridge
- `[p]polyglot addchannel <bridge> <channel> <language>` — Add a channel
- `[p]polyglot view` — List configured bridges
- `[p]translate <language> [message]` — Manual translation

See [PolyglotBridge/README.md](PolyglotBridge/README.md) for setup examples. Translation API logic adapted from [Fluent](https://github.com/vertyco/vrt-cogs) by Vertyco (MIT License).

```
[p]cog install GalaxyRedCogs PolyglotBridge
[p]load PolyglotBridge
```

### SongLink

Universal music link converter via song.link.

**Features:**
- Converts Spotify, Apple Music, YouTube Music, and other supported URLs
- Optional auto-processing in registered channels
- Rate-limit aware

**Commands:**
- `[p]songlink <url>` — Convert a music URL (`[p]sl` alias)
- `[p]songchannel register <channel>` — Auto-convert in a channel
- `[p]songchannel remove <channel>` — Stop auto-converting
- `[p]songchannel list` — List registered channels

```
[p]cog install GalaxyRedCogs SongLink
[p]load SongLink
```

## Requirements

- Red-DiscordBot 3.5.0 or higher
- Python 3.8.1 or higher (some cogs need 3.9 or 3.10; Downloader will enforce `min_python_version` where set)

Extra pip packages are declared in each cog's `info.json` and installed automatically by Downloader. Notable extras:

- **fluent** / **PolyglotBridge** — translation libraries (`deepl`, `openai`, and others)
- **NOAI** — `torch`, `transformers`, `pillow` (large install)

## Support

For issues, questions, or suggestions, open an issue on the [GitHub repository](https://github.com/ElijahJero/GalaxyRedCogs/issues).

## Author

**Elijah Jero** (Galaxy Studios)

Third-party cogs in this repo: **fluent** by Vertyco, **linkfixer** by hollowstrawberry.

## License

This repository is licensed under the [GNU General Public License v3.0](LICENSE).

Translation API components in `fluent` and `PolyglotBridge` retain MIT license attribution to [vertyco/vrt-cogs](https://github.com/vertyco/vrt-cogs).
