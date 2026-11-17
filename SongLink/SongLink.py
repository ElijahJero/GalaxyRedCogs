"""
SongLink Cog for Red Discord Bot.

Generate universal SongLink (song.link) embeds for supported music service URLs
using the authenticated Odesli API.

Features
- Automatic detection in registered channels.
- Manual command to convert a single URL.
- Owner-set API key, sent as the ``key`` query parameter.
- Rate limits default to 1 request/minute and 60 requests/hour, and both can be changed.

Setup
[p]load songlink (depending on your repo/cog name)
[p]songlinkset apikey <key>
[p]help SongLink

Commands Overview
[p]songlink <url>
[p]songlinkset apikey <key>
[p]songlinkset clearapikey
[p]songlinkset ratelimit <per_minute> [per_hour]
[p]songlinkset status
[p]songchannel register <channel>
[p]songchannel remove <channel>
[p]songchannel list
"""

import asyncio
import json
import logging
import re
import time
import urllib.parse
from typing import Any, Dict, List, Optional, Tuple

import aiohttp
import discord
from redbot.core import Config, commands
from redbot.core.bot import Red

log = logging.getLogger("red.songlink")

API_BASE = "https://api.odesli.co/v1-alpha.1/links"
REQUEST_TIMEOUT = 10
MAX_JSON_BYTES = 512_000  # safety cap
MAX_API_KEY_LENGTH = 512
DEFAULT_PER_MINUTE = 1
DEFAULT_PER_HOUR = 60
MAX_RATE_LIMIT = 10_000
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

ERROR_MESSAGE = "an error occurred"


def _compute_rate_wait(
    timestamps: List[float],
    now: float,
    per_minute: int,
    per_hour: int,
    backoff_remaining: float,
) -> Tuple[float, str]:
    """
    Seconds to wait before the next Odesli request, and why.

    ``timestamps`` are unix times of requests in the recent window.
    ``backoff_remaining`` is an in-memory pause after an HTTP 429.
    """
    recent = [t for t in timestamps if now - t < 3600.0]
    wait = 0.0
    reason = ""
    if backoff_remaining > 0:
        wait = backoff_remaining
        reason = "backoff"
    if recent and per_minute > 0:
        interval = 60.0 / per_minute
        minute_wait = interval - (now - max(recent))
        if minute_wait > wait:
            wait = minute_wait
            reason = "minute"
    if per_hour > 0 and len(recent) >= per_hour:
        hour_wait = 3600.0 - (now - min(recent))
        if hour_wait > wait:
            wait = hour_wait
            reason = "hour"
    if wait < 0:
        return 0.0, ""
    return wait, reason


class SongLink(commands.Cog):
    """
    Generate SongLink universal links and manage automatic SongLink channels.

    Commands
    [p]songlink <url>
        Convert a supported music service URL to a universal SongLink embed.

    [p]songlinkset apikey <key>
        (Bot owner) Store the Odesli API key used to authenticate requests.

    [p]songlinkset clearapikey
        (Bot owner) Remove the stored Odesli API key.

    [p]songlinkset ratelimit <per_minute> [per_hour]
        (Bot owner) Change the request pace. Defaults are 1/minute and 60/hour.

    [p]songlinkset status
        (Bot owner) Show whether a key is set, the limits, and recent usage.

    [p]songchannel register <channel>
        (Admin only) Start auto-detecting supported URLs in the given channel.

    [p]songchannel remove <channel>
        Stop auto-detection in that channel.

    [p]songchannel list
        Show all channels currently registered.

    Notes
    - "[p]" represents your bot's prefix.
    - The API key and rate limits are global (one Odesli quota for the bot).
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
                headers={"User-Agent": "SongLinkCog/1.4 (+github redbot)"}
            )
            self._own_session = True
        self.config = Config.get_conf(self, identifier=0x534F4E47, force_registration=True)
        self.config.register_global(
            api_key=None,
            requests_per_minute=DEFAULT_PER_MINUTE,
            requests_per_hour=DEFAULT_PER_HOUR,
            recent_requests=[],
        )
        self.config.register_guild(channels=[])
        # queue worker for auto-processing links
        self._link_queue: asyncio.Queue = asyncio.Queue()
        self._worker_task = asyncio.create_task(self._worker())
        self._rate_lock = asyncio.Lock()
        self._request_times: List[float] = []
        self._times_loaded = False
        self._backoff_until = 0.0
        self._missing_key_logged = False
        self._unauthorized_logged = False

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

    async def _api_key(self) -> str:
        """Return the stored Odesli API key, or an empty string when unset."""
        raw = await self.config.api_key()
        if not isinstance(raw, str):
            return ""
        return raw.strip()

    async def _positive_limit(self, key: str, default: int) -> int:
        """Read a stored rate limit, falling back when the value is unusable."""
        raw = await getattr(self.config, key)()
        try:
            value = int(raw)
        except (TypeError, ValueError):
            return default
        if value < 1:
            return default
        return value

    async def _ensure_times_locked(self) -> None:
        """Load recent request timestamps once. Caller must hold ``_rate_lock``."""
        if self._times_loaded:
            return
        raw = await self.config.recent_requests()
        now = time.time()
        times: List[float] = []
        if isinstance(raw, list):
            for item in raw:
                if isinstance(item, (int, float)) and now - float(item) < 3600.0:
                    times.append(float(item))
        self._request_times = times
        self._times_loaded = True

    async def _slot_wait_locked(self) -> Tuple[float, str]:
        """Wait time and reason for the next request. Caller must hold ``_rate_lock``."""
        await self._ensure_times_locked()
        now = time.time()
        self._request_times = [t for t in self._request_times if now - t < 3600.0]
        per_minute = await self._positive_limit("requests_per_minute", DEFAULT_PER_MINUTE)
        per_hour = await self._positive_limit("requests_per_hour", DEFAULT_PER_HOUR)
        backoff_remaining = self._backoff_until - time.monotonic()
        return _compute_rate_wait(
            self._request_times,
            now,
            per_minute,
            per_hour,
            backoff_remaining,
        )

    async def _reserve(self, *, block: bool) -> Tuple[bool, float, str]:
        """
        Take the next API slot.

        When ``block`` is False and the slot is busy, return immediately with
        the wait time and reason (``minute``, ``hour``, or ``backoff``).
        """
        while True:
            async with self._rate_lock:
                wait, reason = await self._slot_wait_locked()
                if wait <= 0:
                    self._request_times.append(time.time())
                    await self.config.recent_requests.set(list(self._request_times))
                    return True, 0.0, ""
            if not block:
                return False, wait, reason
            await asyncio.sleep(min(max(wait, 0.05), 5.0))

    async def _apply_backoff(self, retry_after: Optional[float]) -> float:
        """Pause further requests after HTTP 429. Returns the pause in seconds."""
        async with self._rate_lock:
            await self._ensure_times_locked()
            now = time.time()
            self._request_times = [t for t in self._request_times if now - t < 3600.0]
            delay = retry_after if retry_after and retry_after > 0 else None
            if delay is None:
                # 429 is the hourly cap. Hold until the oldest request leaves the window.
                if self._request_times:
                    delay = max(60.0, 3600.0 - (now - min(self._request_times)))
                else:
                    delay = 3600.0
            self._backoff_until = max(self._backoff_until, time.monotonic() + delay)
            return delay

    @staticmethod
    def _parse_retry_after(header: Optional[str]) -> Optional[float]:
        """Parse a numeric Retry-After value. HTTP-date values are ignored."""
        if not header:
            return None
        try:
            value = float(header)
        except (TypeError, ValueError):
            return None
        if value < 0:
            return None
        return value

    def _format_wait(self, seconds: float) -> str:
        """Human-readable delay, rounded up to the next second."""
        total = max(1, int(seconds + 0.999))
        if total < 60:
            unit = "second" if total == 1 else "seconds"
            return f"{total} {unit}"
        if total < 3600:
            minutes, secs = divmod(total, 60)
            minute_unit = "minute" if minutes == 1 else "minutes"
            if secs and minutes < 5:
                second_unit = "second" if secs == 1 else "seconds"
                return f"{minutes} {minute_unit} {secs} {second_unit}"
            return f"{minutes} {minute_unit}"
        hours, rem = divmod(total, 3600)
        minutes = rem // 60
        hour_unit = "hour" if hours == 1 else "hours"
        if minutes:
            minute_unit = "minute" if minutes == 1 else "minutes"
            return f"{hours} {hour_unit} {minutes} {minute_unit}"
        return f"{hours} {hour_unit}"

    async def _rate_limit_message(self, wait: float, reason: str) -> str:
        """User-facing text for a local or remote rate limit."""
        per_minute = await self._positive_limit("requests_per_minute", DEFAULT_PER_MINUTE)
        per_hour = await self._positive_limit("requests_per_hour", DEFAULT_PER_HOUR)
        when = self._format_wait(wait)
        if reason == "hour":
            return f"Hourly Odesli cap of {per_hour} reached. Try again in {when}."
        if reason == "backoff":
            return f"Odesli rate limit reached. Try again in {when}."
        request_word = "request" if per_minute == 1 else "requests"
        return f"Limited to {per_minute} Odesli {request_word} per minute. Try again in {when}."

    async def _failure_message(
        self,
        ctx: commands.Context,
        error: Optional[str],
        retry_after: Optional[float],
    ) -> str:
        """Message for a failed lookup. Generic text covers unknown failures."""
        prefix = ctx.clean_prefix
        if error == "no_key":
            return (
                "SongLink needs an Odesli API key. "
                f"A bot owner can set it with `{prefix}songlinkset apikey`."
            )
        if error == "unauthorized":
            return (
                "The Odesli API rejected the key. "
                f"A bot owner can replace it with `{prefix}songlinkset apikey`."
            )
        if error == "gone":
            return "The Odesli links API is unavailable."
        if error == "rate_limit":
            return await self._rate_limit_message(retry_after or 60.0, "backoff")
        return ERROR_MESSAGE

    async def _delete_invocation(self, ctx: commands.Context) -> bool:
        """Delete the command message. Returns False when Discord refuses."""
        try:
            await ctx.message.delete()
        except discord.HTTPException:
            return False
        return True

    def _clear_key_warnings(self) -> None:
        """Allow a new missing-key or rejected-key warning after the key changes."""
        self._missing_key_logged = False
        self._unauthorized_logged = False

    async def _fetch_songlink_data(self, url: str) -> Dict[str, Any]:
        """
        Fetch Odesli API data for a given URL.

        The caller reserves a rate-limit slot before calling this. The API key
        is appended as the ``key`` query parameter and is not logged.

        Args:
            url (str): The music service URL.

        Returns:
            Dict[str, Any]: Parsed JSON response or an error dict.
        """
        api_key = await self._api_key()
        if not api_key:
            return {"__error__": "no_key"}
        api_url = f"{API_BASE}?{urllib.parse.urlencode({'url': url, 'key': api_key})}"
        try:
            async with self.session.get(api_url, timeout=REQUEST_TIMEOUT) as resp:
                status = resp.status
                if status == 429:
                    retry_after = self._parse_retry_after(resp.headers.get("Retry-After"))
                    await resp.read()
                    delay = await self._apply_backoff(retry_after)
                    return {"__error__": "rate_limit", "__retry_after__": delay}
                if status in {401, 403}:
                    await resp.read()
                    return {"__error__": "unauthorized"}
                if status == 410:
                    await resp.read()
                    return {"__error__": "gone"}
                if status >= 500:
                    await resp.read()
                    return {"__error__": "server"}
                if status != 200:
                    await resp.read()
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
        Process a URL and return a SongLink embed, error status, and retry delay.

        Args:
            url (str): The music service URL.

        Returns:
            Tuple[Optional[discord.Embed], Optional[str], Optional[float]]:
            Embed, error status, and seconds to wait after a rate limit.
        """
        data = await self._fetch_songlink_data(url)
        err = data.get("__error__")
        if err:
            retry_after = data.get("__retry_after__")
            if not isinstance(retry_after, (int, float)):
                retry_after = None
            return None, err, retry_after
        embed = self._build_embed_from_entity(data)
        if not embed:
            return None, "permanent", None
        return embed, None, None

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

    async def _handle_queued(self, channel_id: int, url: str, attempts: int):
        """Resolve one queued URL and post the embed, retrying transient failures."""
        if not await self._api_key():
            if not self._missing_key_logged:
                log.warning("SongLink skipped a link because no Odesli API key is set.")
                self._missing_key_logged = True
            return
        await self._reserve(block=True)
        embed, error, retry_after = await self._process_url_for_embed(url)
        if error is None and embed:
            self._clear_key_warnings()
            channel = self.bot.get_channel(channel_id)
            if channel:
                try:
                    await channel.send(embed=embed)
                except Exception:
                    pass
            return
        if error == "unauthorized":
            if not self._unauthorized_logged:
                log.warning("Odesli API rejected the SongLink API key.")
                self._unauthorized_logged = True
            return
        if error == "gone":
            log.warning("Odesli links endpoint returned 410.")
            return
        if error in {"rate_limit", "server"} and attempts < 3:
            if error == "rate_limit":
                delay = float(retry_after) if retry_after and retry_after > 0 else 60.0
            else:
                delay = 15.0
            asyncio.create_task(self._delayed_requeue(channel_id, url, attempts + 1, delay))

    async def _worker(self):
        """
        Background worker to process queued SongLink requests, respecting rate limits.
        """
        try:
            while True:
                channel_id, url, attempts = await self._link_queue.get()
                try:
                    await self._handle_queued(channel_id, url, attempts)
                finally:
                    self._link_queue.task_done()
        except asyncio.CancelledError:
            return

    # ---------------- Commands ----------------

    @commands.group(name="songlinkset", aliases=["slset"])
    @commands.is_owner()
    async def songlinkset(self, ctx: commands.Context):
        """
        Configure the Odesli API key and rate limits.

        The key and limits apply to the whole bot.

        Subcommands
        [p]songlinkset apikey <key>
        [p]songlinkset clearapikey
        [p]songlinkset ratelimit <per_minute> [per_hour]
        [p]songlinkset status
        """
        if ctx.invoked_subcommand is None:
            await ctx.send_help(ctx.command)

    @songlinkset.command(name="apikey")
    async def songlinkset_apikey(self, ctx: commands.Context, *, api_key: str):
        """
        Store the Odesli API key.

        Usage
        [p]songlinkset apikey <key>

        The key is kept in Red's config and sent as the ``key`` query parameter.
        This command tries to delete your message so the key is not left in chat.
        """
        cleaned = api_key.strip().strip("`\"'")
        deleted = await self._delete_invocation(ctx)
        if not cleaned or any(ch.isspace() for ch in cleaned) or len(cleaned) > MAX_API_KEY_LENGTH:
            text = "That key was not stored. Pass the key on its own."
            if not deleted:
                text += " Delete the command message so the key is not left in chat."
            await ctx.send(text)
            return
        await self.config.api_key.set(cleaned)
        self._clear_key_warnings()
        text = "Odesli API key stored."
        if not deleted:
            text += " Delete the command message so the key is not left in chat."
        await ctx.send(text)

    @songlinkset.command(name="clearapikey")
    async def songlinkset_clearapikey(self, ctx: commands.Context):
        """
        Remove the stored Odesli API key.

        Usage
        [p]songlinkset clearapikey

        SongLink requests stop until a new key is set.
        """
        await self.config.api_key.set(None)
        self._clear_key_warnings()
        await ctx.send("Odesli API key removed. SongLink requests stop until a new key is set.")

    @songlinkset.command(name="ratelimit")
    async def songlinkset_ratelimit(
        self,
        ctx: commands.Context,
        per_minute: int,
        per_hour: Optional[int] = None,
    ):
        """
        Set how many Odesli requests the bot may make.

        Usage
        [p]songlinkset ratelimit <per_minute>
        [p]songlinkset ratelimit <per_minute> <per_hour>

        Examples
        [p]songlinkset ratelimit 1 60
        [p]songlinkset ratelimit 5 200

        Defaults are 1 per minute and 60 per hour. Omit the hourly cap to leave it unchanged.
        """
        if not 1 <= per_minute <= MAX_RATE_LIMIT:
            await ctx.send(f"Per-minute limit has to be from 1 to {MAX_RATE_LIMIT}.")
            return
        if per_hour is None:
            per_hour = await self._positive_limit("requests_per_hour", DEFAULT_PER_HOUR)
        elif not 1 <= per_hour <= MAX_RATE_LIMIT:
            await ctx.send(f"Hourly cap has to be from 1 to {MAX_RATE_LIMIT}.")
            return
        await self.config.requests_per_minute.set(per_minute)
        await self.config.requests_per_hour.set(per_hour)
        text = f"Rate limit set to {per_minute} per minute and {per_hour} per hour."
        if per_minute * 60 > per_hour:
            text += " The hourly cap will stop requests once it is reached."
        await ctx.send(text)

    @songlinkset.command(name="status")
    async def songlinkset_status(self, ctx: commands.Context):
        """
        Show the Odesli key state, rate limits, and how many requests were used this hour.

        Usage
        [p]songlinkset status

        The key itself is never shown.
        """
        key = await self._api_key()
        per_minute = await self._positive_limit("requests_per_minute", DEFAULT_PER_MINUTE)
        per_hour = await self._positive_limit("requests_per_hour", DEFAULT_PER_HOUR)
        async with self._rate_lock:
            await self._ensure_times_locked()
            now = time.time()
            used = len([t for t in self._request_times if now - t < 3600.0])
        key_state = "set" if key else "not set"
        await ctx.send(
            f"Odesli API key: {key_state}\n"
            f"Rate limit: {per_minute}/min, {per_hour}/hour\n"
            f"Requests in the last hour: {used}/{per_hour}"
        )

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
        - A bot owner must set an Odesli API key with [p]songlinkset apikey first.
        - Requests share the bot-wide rate limit (1/minute and 60/hour by default).
        - If the URL is unsupported or fails, a generic error message is sent.
        - See [p]songchannel commands to automate this per channel.
        """
        if not url.startswith(("http://", "https://")):
            await ctx.send(ERROR_MESSAGE)
            return
        if not await self._api_key():
            await ctx.send(await self._failure_message(ctx, "no_key", None))
            return
        reserved, wait, reason = await self._reserve(block=False)
        if not reserved:
            await ctx.send(await self._rate_limit_message(wait, reason))
            return
        embed, error, retry_after = await self._process_url_for_embed(url)
        if error or not embed:
            await ctx.send(await self._failure_message(ctx, error, retry_after))
            return
        self._clear_key_warnings()
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
        - Does nothing until an Odesli API key is stored.
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
        if not to_queue:
            return
        if not await self._api_key():
            if not self._missing_key_logged:
                log.warning("SongLink ignored a music link because no Odesli API key is set.")
                self._missing_key_logged = True
            return
        for url in to_queue:
            self._queue_link(message.channel.id, url)
