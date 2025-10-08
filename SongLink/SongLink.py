"""
SongLink Cog for Red Discord Bot.

Generate universal SongLink (song.link) embeds for supported music service URLs.

Features
- Automatic detection in registered channels.
- Manual command to convert a single URL.
- Rate limit aware (<=10 requests/min) with intelligent retry of transient failures.

Setup
[p]load songlink (depending on your repo/cog name)
[p]help SongLink

Commands Overview
[p]songlink <url>
[p]songchannel register <channel>
[p]songchannel remove <channel>
[p]songchannel list
"""

import asyncio
import urllib.parse
import json
from typing import Any, Dict, Optional
import re
import time
import discord
from redbot.core import Config

import aiohttp
from redbot.core import commands
from redbot.core.bot import Red

API_BASE = "https://api.song.link/v1-alpha.1/links"
REQUEST_TIMEOUT = 10
MAX_JSON_BYTES = 512_000  # safety cap
# Supported music service hostnames (lowercased, no leading www.)
SUPPORTED_HOSTS = {
    "open.spotify.com", "spotify.link",
    "music.apple.com", "itunes.apple.com", "apple.com",
    "youtube.com", "youtu.be", "music.youtube.com",
    "google.com", "play.google.com", "play.google.com/store",
    "pandora.com",
    "deezer.com",
    "tidal.com",
    "music.amazon.com", "amazon.com", "amazon.co.uk", "amazon.de", "amazon.fr",
    "soundcloud.com",
    "napster.com",
    "yandex.ru", "music.yandex.ru",
    "spinrilla.com",
    "audius.co",
    "anghami.com",
    "boomplay.com",
    "audiomack.com",
    "bandcamp.com",
}
URL_REGEX = re.compile(r'https?://[^\s<>()"]+')
MIN_REQUEST_INTERVAL = 6.1  # seconds between calls (~10/min)

ERROR_MESSAGE = "an error occurred"


class SongLink(commands.Cog):
    """
    Generate SongLink universal links and manage automatic SongLink channels.

    Commands
    [p]songlink <url>
        Convert a supported music service URL to a universal SongLink embed.

    [p]songchannel register <channel>
        (Admin only) Start auto-detecting supported URLs in the given channel.

    [p]songchannel remove <channel>
        Stop auto-detection in that channel.

    [p]songchannel list
        Show all channels currently registered.

    Notes
    - "[p]" represents your bot's prefix.
    - Multiple links in one message are processed individually.
    - Only supported hostnames are queued (avoids unnecessary API calls).
    """

    def __init__(self, bot: Red):
        """
        Initialize the SongLink cog.

        Args:
            bot (Red): The Red Discord Bot instance.
        """
        self.bot = bot
        shared = getattr(bot, "session", None) or getattr(bot, "http_session", None)
        if shared:
            self.session = shared
            self._own_session = False
        else:
            self.session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT + 2),
                headers={"User-Agent": "SongLinkCog/1.3 (+github redbot)"}
            )
            self._own_session = True
        self.config = Config.get_conf(self, identifier=0x534F4E47, force_registration=True)
        self.config.register_guild(channels=[])
        # queue worker for auto-processing links
        self._link_queue: asyncio.Queue = asyncio.Queue()
        self._worker_task = asyncio.create_task(self._worker())
        self._last_request_ts = 0.0

    def cog_unload(self):
        """
        Cleanup resources when the cog is unloaded.
        Closes aiohttp session if owned and cancels the worker task.
        """
        if self._own_session and not self.session.closed:
            asyncio.create_task(self.session.close())
        if self._worker_task:
            self._worker_task.cancel()

    # ---------------- Helper / Internal Methods ----------------

    async def _fetch_songlink_data(self, url: str) -> Optional[Dict[str, Any]]:
        """
        Fetch SongLink API data for a given URL.

        Args:
            url (str): The music service URL.

        Returns:
            Optional[Dict[str, Any]]: Parsed JSON response or error dict.
        """
        api_url = f"{API_BASE}?{urllib.parse.urlencode({'url': url})}"
        try:
            async with self.session.get(api_url, timeout=REQUEST_TIMEOUT) as resp:
                status = resp.status
                if status == 429:
                    return {"__error__": "rate_limit"}
                if status >= 500:
                    return {"__error__": "server"}
                if status != 200:
                    return {"__error__": "permanent"}
                raw = await resp.read()
                if (resp.content_length and resp.content_length > MAX_JSON_BYTES) or len(raw) > MAX_JSON_BYTES:
                    return {"__error__": "permanent"}
                try:
                    data: Dict[str, Any] = json.loads(raw.decode("utf-8"))
                except json.JSONDecodeError:
                    return {"__error__": "permanent"}
                return data
        except asyncio.TimeoutError:
            return {"__error__": "server"}
        except Exception:
            return {"__error__": "server"}

    def _build_embed_from_entity(self, data: Dict[str, Any]) -> Optional[discord.Embed]:
        """
        Build a Discord embed from SongLink API entity data.

        Args:
            data (Dict[str, Any]): SongLink API response data.

        Returns:
            Optional[discord.Embed]: The constructed embed or None.
        """
        page_url: Optional[str] = data.get("pageUrl") or data.get("url")
        if not page_url:
            return None
        entities = data.get("entitiesByUniqueId") or {}
        entity_id = data.get("entityUniqueId")
        primary = entities.get(entity_id, {}) if entity_id else {}
        if not primary:
            for ent in entities.values():
                if isinstance(ent, dict) and ent.get("title") and ent.get("artistName"):
                    primary = ent
                    break
        if not isinstance(primary, dict):
            primary = {}
        title = primary.get("title") or "Unknown Title"
        artist = primary.get("artistName") or "Unknown Artist"
        thumbnail_url = primary.get("thumbnailUrl") or primary.get("thumbnailUrlRaw")

        embed = discord.Embed()
        embed.title = f"{title} by {artist}"
        embed.url = page_url
        embed.description = f"[Open in SongLink]({page_url})"
        if thumbnail_url:
            embed.set_thumbnail(url=thumbnail_url)
        else:
            embed.set_thumbnail(url="https://song.link/favicon.ico")
        embed.set_footer(text="Listen anywhere with just one link")
        return embed

    async def _process_url_for_embed(self, url: str):
        """
        Process a URL and return a SongLink embed and error status.

        Args:
            url (str): The music service URL.

        Returns:
            Tuple[Optional[discord.Embed], Optional[str]]: Embed and error status.
        """
        data = await self._fetch_songlink_data(url)
        if not data:
            return None, "server"
        err = data.get("__error__")
        if err:
            return None, err
        embed = self._build_embed_from_entity(data)
        if not embed:
            return None, "permanent"
        return embed, None

    def _is_supported_url(self, url: str) -> bool:
        """
        Check if a URL is from a supported music service.

        Args:
            url (str): The URL to check.

        Returns:
            bool: True if supported, False otherwise.
        """
        try:
            parsed = urllib.parse.urlparse(url)
            host = (parsed.hostname or "").lower()
            if host.startswith("www."):
                host = host[4:]
            return host in SUPPORTED_HOSTS
        except Exception:
            return False

    def _queue_link(self, channel_id: int, url: str):
        """
        Queue a link for SongLink processing.

        Args:
            channel_id (int): Discord channel ID.
            url (str): The music service URL.
        """
        self._link_queue.put_nowait((channel_id, url, 0))

    async def _delayed_requeue(self, channel_id: int, url: str, attempts: int, delay: float):
        """
        Requeue a link after a delay (for rate limiting or transient errors).

        Args:
            channel_id (int): Discord channel ID.
            url (str): The music service URL.
            attempts (int): Number of previous attempts.
            delay (float): Delay in seconds before requeue.
        """
        await asyncio.sleep(delay)
        self._link_queue.put_nowait((channel_id, url, attempts))

    async def _worker(self):
        """
        Background worker to process queued SongLink requests, respecting rate limits.
        """
        try:
            while True:
                channel_id, url, attempts = await self._link_queue.get()
                # passive spacing to honor 10/min (>=6s apart)
                since = time.time() - self._last_request_ts
                if since < MIN_REQUEST_INTERVAL:
                    await asyncio.sleep(MIN_REQUEST_INTERVAL - since)
                embed, error = await self._process_url_for_embed(url)
                if error is None and embed:
                    channel = self.bot.get_channel(channel_id)
                    if channel:
                        try:
                            await channel.send(embed=embed)
                        except Exception:
                            pass
                    self._last_request_ts = time.time()
                else:
                    # retry only on rate_limit or server (transient) and cap attempts
                    if error in {"rate_limit", "server"} and attempts < 3:
                        delay = 10 if error == "rate_limit" else 15
                        asyncio.create_task(self._delayed_requeue(channel_id, url, attempts + 1, delay))
                    # do not update last_request_ts for permanent errors
                    if error in {"rate_limit", "server"}:
                        self._last_request_ts = time.time()
                self._link_queue.task_done()
        except asyncio.CancelledError:
            return

    # ---------------- Commands ----------------

    @commands.group(name="songchannel")
    @commands.guild_only()
    async def songchannel(self, ctx: commands.Context):
        """
        Manage channels that auto-convert music service URLs into SongLink embeds.

        Subcommands
        [p]songchannel register <channel>
        [p]songchannel remove <channel>
        [p]songchannel list

        Example
        [p]songchannel register #music
        """
        if ctx.invoked_subcommand is None:
            await ctx.send("Subcommands: register, remove, list")

    @songchannel.command(name="register")
    @commands.has_guild_permissions(administrator=True)
    async def songchannel_register(self, ctx: commands.Context, channel: discord.TextChannel):
        """
        Register a channel for automatic SongLink conversion.

        Usage
        [p]songchannel register <channel>

        Example
        [p]songchannel register #music

        Requirements
        - Administrator permission.

        Arguments
        channel: Text channel to watch.
        """
        # Double-check (belt & suspenders) in case permissions changed after invocation
        if not ctx.author.guild_permissions.administrator:
            await ctx.send("Administrator permission required.")
            return
        channels = await self.config.guild(ctx.guild).channels()
        if channel.id in channels:
            await ctx.send("Channel already registered.")
            return
        channels.append(channel.id)
        await self.config.guild(ctx.guild).channels.set(channels)
        await ctx.send(f"Registered {channel.mention} for automatic SongLink processing.")

    @songchannel.command(name="remove")
    @commands.has_guild_permissions(manage_guild=True)
    async def songchannel_remove(self, ctx: commands.Context, channel: discord.TextChannel):
        """
        Remove a channel from automatic SongLink conversion.

        Usage
        [p]songchannel remove <channel>

        Example
        [p]songchannel remove #music
        """
        channels = await self.config.guild(ctx.guild).channels()
        if channel.id not in channels:
            await ctx.send("Channel not registered.")
            return
        channels = [c for c in channels if c != channel.id]
        await self.config.guild(ctx.guild).channels.set(channels)
        await ctx.send(f"Removed {channel.mention} from automatic processing.")

    @songchannel.command(name="list")
    async def songchannel_list(self, ctx: commands.Context):
        """
        List all channels currently registered for automatic SongLink conversion.

        Usage
        [p]songchannel list
        """
        channels = await self.config.guild(ctx.guild).channels()
        if not channels:
            await ctx.send("No song channels configured.")
            return
        mentions = []
        for cid in channels:
            ch = ctx.guild.get_channel(cid)
            if ch:
                mentions.append(ch.mention)
            else:
                mentions.append(f"(missing:{cid})")
        await ctx.send("Song channels: " + ", ".join(mentions))

    @commands.command(aliases=["sl"])
    async def songlink(self, ctx: commands.Context, url: str):
        """
        Convert a single supported music service URL into a SongLink embed.

        Usage
        [p]songlink <url>
        [p]sl <url>

        Example
        [p]songlink https://open.spotify.com/track/1234567890
        [p]sl https://music.apple.com/us/album/abcdef

        Arguments
        url: A track/album/playlist URL from a supported service.

        Notes
        - If the URL is unsupported or fails, a generic error message is sent.
        - See [p]songchannel commands to automate this per channel.
        """
        if not url.startswith(("http://", "https://")):
            await ctx.send(ERROR_MESSAGE)
            return
        embed, error = await self._process_url_for_embed(url)
        if error or not embed:
            await ctx.send(ERROR_MESSAGE)
            return
        await ctx.send(embed=embed)

    # ---------------- Listeners ----------------

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        """
        Internal listener: scans registered channels for supported music URLs and queues them.

        Behavior
        - Ignores bot messages.
        - Extracts all http(s) URLs, filters to supported hosts, enqueues each.
        - Each URL becomes its own embed (per service item).
        """
        if message.author.bot or not message.guild:
            return
        channels = await self.config.guild(message.guild).channels()
        if not channels or message.channel.id not in channels:
            return
        urls = set(URL_REGEX.findall(message.content))
        if not urls:
            return
        to_queue = []
        for url in urls:
            if self._is_supported_url(url):
                to_queue.append(url)
        for url in to_queue:
            self._queue_link(message.channel.id, url)
