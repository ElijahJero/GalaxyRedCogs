import aiohttp
import asyncio
import logging
import time
from typing import Optional, Set, Dict, Any
from datetime import datetime, timedelta

import discord
from redbot.core import Config, commands
from redbot.core.bot import Red
from redbot.core.utils.chat_formatting import humanize_timedelta

log = logging.getLogger("red.botengine")
log.disabled = True

class BotEngineCog(commands.Cog):
    """Interact with the Ollama-based Bot Engine server."""

    def __init__(self, bot: Red):
        self.bot = bot
        self.config = Config.get_conf(self, identifier=1234567890, force_registration=True)

        default_global = {
            "api_url": "http://localhost:8000/process",
            "timeout": 10,
        }
        default_guild = {
            "enabled_channels": [],          # channels where bot is active
            "disabled_channels": [],         # explicitly disabled
            "blocked_users": [],             # user IDs to ignore (guild-wide)
            "paused_until": None,            # timestamp when global pause ends (None = not paused)
            "channel_pauses": {},            # {channel_id: timestamp}
            "cooldowns": {},                 # {channel_id: seconds}
            "last_response": {},             # {channel_id: timestamp} for cooldown tracking
        }
        self.config.register_global(**default_global)
        self.config.register_guild(**default_guild)

        self._session: Optional[aiohttp.ClientSession] = None
        self._background_tasks = set()
        self._stats = {"processed": 0, "errors": 0}  # simple in-memory stats

    async def cog_load(self):
        self._session = aiohttp.ClientSession()

    async def cog_unload(self):
        if self._session:
            await self._session.close()
        for task in self._background_tasks:
            task.cancel()

    # ------------------------------------------------------------------
    # Helper methods
    # ------------------------------------------------------------------

    async def _is_paused(self, guild: discord.Guild, channel: discord.TextChannel) -> bool:
        """Check if the bot is paused globally or for this channel."""
        # Global pause
        paused_until = await self.config.guild(guild).paused_until()
        if paused_until and time.time() < paused_until:
            return True
        # Channel-specific pause
        channel_pauses = await self.config.guild(guild).channel_pauses()
        if str(channel.id) in channel_pauses:
            until = channel_pauses[str(channel.id)]
            if until and time.time() < until:
                return True
            # If expired, clean it up
            elif until:
                async with self.config.guild(guild).channel_pauses() as pauses:
                    if str(channel.id) in pauses and pauses[str(channel.id)] == until:
                        del pauses[str(channel.id)]
        return False

    async def _on_cooldown(self, guild: discord.Guild, channel: discord.TextChannel) -> bool:
        """Check if the channel is on cooldown."""
        cooldowns = await self.config.guild(guild).cooldowns()
        channel_id = str(channel.id)
        if channel_id not in cooldowns:
            return False
        last_response = (await self.config.guild(guild).last_response()).get(channel_id, 0)
        cooldown_secs = cooldowns[channel_id]
        if last_response and (time.time() - last_response) < cooldown_secs:
            return True
        return False

    async def _update_last_response(self, guild: discord.Guild, channel: discord.TextChannel):
        """Update the last response timestamp for cooldown."""
        async with self.config.guild(guild).last_response() as last:
            last[str(channel.id)] = time.time()

    # ------------------------------------------------------------------
    # Commands
    # ------------------------------------------------------------------

    @commands.group(name="botengine", aliases=["be"])
    @commands.guild_only()
    async def botengine(self, ctx: commands.Context):
        """Configure the Bot Engine integration."""
        pass

    # --- API settings ---
    @botengine.command(name="setapi")
    @commands.is_owner()
    async def set_api_url(self, ctx: commands.Context, url: str):
        """Set the URL of the Bot Engine server (global)."""
        await self.config.api_url.set(url)
        await ctx.send(f"API URL set to `{url}`")

    @botengine.command(name="timeout")
    @commands.is_owner()
    async def set_timeout(self, ctx: commands.Context, seconds: int):
        """Set the HTTP timeout in seconds (global)."""
        await self.config.timeout.set(seconds)
        await ctx.send(f"Timeout set to {seconds} seconds")

    # --- Channel enable/disable ---
    @botengine.command(name="enable")
    @commands.admin_or_permissions(manage_channels=True)
    async def enable_channel(self, ctx: commands.Context, channel: discord.TextChannel = None):
        """Enable the bot in the current or specified channel."""
        channel = channel or ctx.channel
        if not isinstance(channel, discord.TextChannel):
            await ctx.send("Only text channels are supported.")
            return

        async with self.config.guild(ctx.guild).enabled_channels() as enabled:
            if channel.id not in enabled:
                enabled.append(channel.id)
        async with self.config.guild(ctx.guild).disabled_channels() as disabled:
            if channel.id in disabled:
                disabled.remove(channel.id)

        await ctx.send(f"Bot Engine enabled in {channel.mention}")

    @botengine.command(name="disable")
    @commands.admin_or_permissions(manage_channels=True)
    async def disable_channel(self, ctx: commands.Context, channel: discord.TextChannel = None):
        """Disable the bot in the current or specified channel."""
        channel = channel or ctx.channel
        if not isinstance(channel, discord.TextChannel):
            await ctx.send("Only text channels are supported.")
            return

        async with self.config.guild(ctx.guild).disabled_channels() as disabled:
            if channel.id not in disabled:
                disabled.append(channel.id)
        async with self.config.guild(ctx.guild).enabled_channels() as enabled:
            if channel.id in enabled:
                enabled.remove(channel.id)

        await ctx.send(f"Bot Engine disabled in {channel.mention}")

    # --- Pause / Resume ---
    @botengine.group(name="pause", invoke_without_command=True)
    @commands.admin_or_permissions(manage_channels=True)
    async def pause(self, ctx: commands.Context, duration: str = None):
        """
        Pause the bot globally or in this channel.
        Provide a duration like '5m', '2h', '1d', or omit to pause indefinitely.
        Use `[p]botengine pause resume` to resume early.
        """
        if duration:
            try:
                delta = humanize_timedelta(**self._parse_duration(duration))
                if not delta:
                    raise ValueError
                until = time.time() + delta.total_seconds()
            except Exception:
                await ctx.send("Invalid duration format. Use e.g. `5m`, `2h`, `1d`.")
                return
        else:
            until = None  # indefinite

        # Global pause
        await self.config.guild(ctx.guild).paused_until.set(until)
        if until:
            resume_time = datetime.fromtimestamp(until).strftime("%Y-%m-%d %H:%M:%S UTC")
            await ctx.send(f"Bot paused globally until **{resume_time}**. Use `{ctx.prefix}botengine pause resume` to resume early.")
        else:
            await ctx.send("Bot paused globally **indefinitely**. Use `{ctx.prefix}botengine pause resume` to resume.")

    @pause.command(name="channel")
    @commands.admin_or_permissions(manage_channels=True)
    async def pause_channel(self, ctx: commands.Context, duration: str = None, channel: discord.TextChannel = None):
        """Pause the bot in a specific channel (default: current)."""
        channel = channel or ctx.channel
        if duration:
            try:
                delta = humanize_timedelta(**self._parse_duration(duration))
                if not delta:
                    raise ValueError
                until = time.time() + delta.total_seconds()
            except Exception:
                await ctx.send("Invalid duration format. Use e.g. `5m`, `2h`, `1d`.")
                return
        else:
            until = None

        async with self.config.guild(ctx.guild).channel_pauses() as pauses:
            pauses[str(channel.id)] = until

        if until:
            resume_time = datetime.fromtimestamp(until).strftime("%Y-%m-%d %H:%M:%S UTC")
            await ctx.send(f"Bot paused in {channel.mention} until **{resume_time}**.")
        else:
            await ctx.send(f"Bot paused in {channel.mention} **indefinitely**.")

    @pause.command(name="resume")
    @commands.admin_or_permissions(manage_channels=True)
    async def pause_resume(self, ctx: commands.Context, channel: discord.TextChannel = None):
        """Resume the bot globally or in a specific channel."""
        if channel:
            async with self.config.guild(ctx.guild).channel_pauses() as pauses:
                if str(channel.id) in pauses:
                    del pauses[str(channel.id)]
            await ctx.send(f"Resumed in {channel.mention}.")
        else:
            await self.config.guild(ctx.guild).paused_until.set(None)
            await ctx.send("Resumed globally.")

    def _parse_duration(self, dur: str) -> dict:
        """Convert '5m' to {'minutes': 5}, etc."""
        units = {
            "s": "seconds",
            "m": "minutes",
            "h": "hours",
            "d": "days",
            "w": "weeks",
        }
        try:
            amount = int(dur[:-1])
            unit = dur[-1].lower()
            if unit in units:
                return {units[unit]: amount}
        except:
            pass
        return {}

    # --- Block / Unblock users ---
    @botengine.command(name="block")
    @commands.admin_or_permissions(manage_messages=True)
    async def block_user(self, ctx: commands.Context, user: discord.Member):
        """Block a user so their messages are ignored by the bot."""
        async with self.config.guild(ctx.guild).blocked_users() as blocked:
            if user.id not in blocked:
                blocked.append(user.id)
        await ctx.send(f"Blocked {user.display_name}.")

    @botengine.command(name="unblock")
    @commands.admin_or_permissions(manage_messages=True)
    async def unblock_user(self, ctx: commands.Context, user: discord.Member):
        """Unblock a user so their messages are processed again."""
        async with self.config.guild(ctx.guild).blocked_users() as blocked:
            if user.id in blocked:
                blocked.remove(user.id)
        await ctx.send(f"Unblocked {user.display_name}.")

    @botengine.command(name="blocklist")
    @commands.admin_or_permissions(manage_messages=True)
    async def blocklist(self, ctx: commands.Context):
        """List all blocked users in this guild."""
        blocked = await self.config.guild(ctx.guild).blocked_users()
        if not blocked:
            await ctx.send("No blocked users.")
            return
        users = []
        for uid in blocked:
            user = ctx.guild.get_member(uid)
            if user:
                users.append(f"{user.display_name} ({user.id})")
            else:
                users.append(f"User ID {uid} (not in server)")
        await ctx.send("Blocked users:\n" + "\n".join(users))

    # --- Cooldown ---
    @botengine.command(name="cooldown")
    @commands.admin_or_permissions(manage_channels=True)
    async def set_cooldown(self, ctx: commands.Context, seconds: int, channel: discord.TextChannel = None):
        """
        Set a cooldown (in seconds) between responses in a channel.
        Use 0 to disable cooldown.
        """
        if seconds < 0:
            await ctx.send("Cooldown cannot be negative.")
            return
        channel = channel or ctx.channel
        async with self.config.guild(ctx.guild).cooldowns() as cooldowns:
            if seconds == 0:
                cooldowns.pop(str(channel.id), None)
            else:
                cooldowns[str(channel.id)] = seconds
        await ctx.send(f"Cooldown set to {seconds}s in {channel.mention}.")

    # --- Clear history ---
    @botengine.command(name="clear")
    @commands.admin_or_permissions(manage_channels=True)
    async def clear_history(self, ctx: commands.Context, channel: discord.TextChannel = None):
        """
        Clear the conversation history for a channel (reset context).
        This sends a reset request to the engine.
        """
        channel = channel or ctx.channel
        api_url = await self.config.api_url()
        timeout = await self.config.timeout()

        # We'll assume the engine has a /reset endpoint.
        # If not, we can at least inform the user.
        reset_url = api_url.replace("/process", "/reset")  # naive guess
        payload = {"channel_id": str(channel.id)}

        try:
            async with self._session.post(reset_url, json=payload, timeout=timeout) as resp:
                if resp.status == 200:
                    await ctx.send(f"History cleared for {channel.mention}.")
                else:
                    await ctx.send(f"Failed to clear history (HTTP {resp.status}). The engine may not support reset.")
        except Exception as e:
            log.error(f"Reset failed: {e}")
            await ctx.send("Could not contact engine. Please restart the engine server to clear history manually.")

    # --- Stats ---
    @botengine.command(name="stats")
    async def stats(self, ctx: commands.Context):
        """Show basic statistics for this cog."""
        processed = self._stats["processed"]
        errors = self._stats["errors"]
        await ctx.send(f"**Bot Engine Stats**\nMessages processed: {processed}\nErrors: {errors}")

    # --- Status ---
    @botengine.command(name="status")
    async def status(self, ctx: commands.Context):
        """Show current configuration and status for this guild."""
        enabled = await self.config.guild(ctx.guild).enabled_channels()
        disabled = await self.config.guild(ctx.guild).disabled_channels()
        blocked = await self.config.guild(ctx.guild).blocked_users()
        api_url = await self.config.api_url()
        timeout = await self.config.timeout()
        paused_until = await self.config.guild(ctx.guild).paused_until()
        channel_pauses = await self.config.guild(ctx.guild).channel_pauses()
        cooldowns = await self.config.guild(ctx.guild).cooldowns()

        status_msg = (
            f"**API URL:** `{api_url}`\n"
            f"**Timeout:** {timeout}s\n"
            f"**Enabled channels:** {', '.join(f'<#{c}>' for c in enabled) or 'None'}\n"
            f"**Disabled channels:** {', '.join(f'<#{c}>' for c in disabled) or 'None'}\n"
            f"**Blocked users:** {len(blocked)}\n"
        )
        if paused_until:
            resume_time = datetime.fromtimestamp(paused_until).strftime("%Y-%m-%d %H:%M:%S UTC")
            status_msg += f"**Global pause until:** {resume_time}\n"
        else:
            status_msg += "**Global pause:** None\n"

        if channel_pauses:
            pauses = []
            for cid, until in channel_pauses.items():
                ch = ctx.guild.get_channel(int(cid))
                if ch:
                    if until:
                        t = datetime.fromtimestamp(until).strftime("%Y-%m-%d %H:%M:%S UTC")
                        pauses.append(f"{ch.mention} until {t}")
                    else:
                        pauses.append(f"{ch.mention} indefinitely")
            if pauses:
                status_msg += "**Channel pauses:**\n" + "\n".join(pauses)

        if cooldowns:
            cds = []
            for cid, secs in cooldowns.items():
                ch = ctx.guild.get_channel(int(cid))
                if ch:
                    cds.append(f"{ch.mention}: {secs}s")
            if cds:
                status_msg += "**Cooldowns:**\n" + "\n".join(cds)

        await ctx.send(status_msg)

    # ------------------------------------------------------------------
    # Message listener (enhanced)
    # ------------------------------------------------------------------

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        # Ignore bots, system messages, and commands
        if message.author.bot:
            return
        ctx = await self.bot.get_context(message)
        if ctx.valid:
            return

        guild = message.guild
        if not guild:
            return  # DMs not supported

        # --- Check blocked users ---
        blocked = await self.config.guild(guild).blocked_users()
        if message.author.id in blocked:
            return

        # --- Check channel enable/disable ---
        enabled = await self.config.guild(guild).enabled_channels()
        disabled = await self.config.guild(guild).disabled_channels()
        if message.channel.id in disabled:
            return
        if enabled and message.channel.id not in enabled:
            return
        if not enabled:
            return

        # --- Pause check ---
        if await self._is_paused(guild, message.channel):
            return

        # --- Cooldown check ---
        if await self._on_cooldown(guild, message.channel):
            return

        # --- Process message ---
        api_url = await self.config.api_url()
        timeout = await self.config.timeout()

        payload = {
            "channel_id": str(message.channel.id),
            "user_message": message.content,
            "username": str(message.author),
        }

        if self._session is None:
            self._session = aiohttp.ClientSession()

        try:
            async with self._session.post(api_url, json=payload, timeout=timeout) as resp:
                if resp.status != 200:
                    error_text = await resp.text()
                    log.error(f"Engine returned {resp.status}: {error_text}")
                    self._stats["errors"] += 1
                    return
                data = await resp.json()
                self._stats["processed"] += 1
        except Exception as e:
            log.error(f"Engine request failed: {e}")
            self._stats["errors"] += 1
            return

        # --- Update last response timestamp (for cooldown) ---
        await self._update_last_response(guild, message.channel)

        # --- Process actions ---
        action = data.get("action")
        if action == "MULTI":
            for response in data.get("responses", []):
                sub_action = response.get("action")
                if sub_action == "MESSAGE":
                    reply = response.get("content")
                    if reply:
                        await message.channel.send(reply)
                elif sub_action == "GIF":
                    gif_url = response.get("gif_url")
                    if gif_url:
                        await message.channel.send(gif_url)
                    else:
                        query = response.get("content", "GIF")
                        await message.channel.send(f"GIF search: `{query}` (no URL resolved)")
        elif action == "MESSAGE":
            reply = data.get("content")
            if reply:
                await message.channel.send(reply)
        elif action == "GIF":
            gif_url = data.get("gif_url")
            if gif_url:
                await message.channel.send(gif_url)
            else:
                query = data.get("content", "GIF")
                await message.channel.send(f"GIF search: `{query}` (no URL resolved)")
        elif action == "PASS":
            pass
        elif action == "ERROR":
            error_msg = data.get("error", "Unknown error")
            log.error(f"Engine error: {error_msg}")
            self._stats["errors"] += 1