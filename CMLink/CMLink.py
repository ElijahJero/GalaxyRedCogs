from redbot.core import Config
from redbot.core import commands
from redbot.core.bot import Red
from discord import TextChannel, VoiceChannel, CategoryChannel  # added imports
from .TournamentMonitor import TournamentMonitor, Storage  # added import
from discord import Member  # new: for admin forcelink member resolution
import re  # added: for UUID-ish detection
import discord  # new: for Embeds

class CMLink(commands.Cog):
    """ Cog for tournament integration with Challenger Mode. """

    def __init__(self, bot: Red):
        self.bot = bot
        self.config = Config.get_conf(
            self,
            identifier=397498742348
        )

        default_global = {
            "API_Refresh_Token": "",         # long-lived refresh key you get from dashboard
            "API_Access_Token": "",          # cached short-lived access token obtained from refresh
            "API_Access_Expires_At": 0,      # unix epoch seconds
            "API_URL": "https://publicapi.challengermode.com/graphql",
            "API_Token_URL": "https://publicapi.challengermode.com/mk1/v1/auth/access_keys",
            "Poll_Interval": 5,  # in seconds
            "Debug_API_Logging": False,  # added: toggle API response logging
        }

        self.config.register_global(**default_global)

        # Register per-guild defaults for guild-specific settings
        default_guild = {
            "update_channel_id": None,
            "lobby_voice_id": None,
            "tournament_category_id": None,
            "tournaments": {},  # keep track of tournaments per guild if needed
            # Runtime helpers persisted for recovery/cleanup
            "active_matches": {},  # match_id -> {"channels": [ids], "created_at": ts}
        }
        self.config.register_guild(**default_guild)

        # Start the background tournament monitor
        self.monitor = TournamentMonitor(self.bot, self.config)
        self.bot.loop.create_task(self.monitor.start())

    # Embed helpers (used by many commands)
    def _make_embed(self, title: str = None, description: str = None, color: discord.Color = discord.Color.blurple()) -> discord.Embed:
        e = discord.Embed(title=title or "", description=description or "", color=color)
        e.set_footer(text="CMLink")
        return e

    def _make_error(self, title: str, description: str) -> discord.Embed:
        return self._make_embed(title=title, description=description, color=discord.Color.red())

    def _make_success(self, title: str, description: str) -> discord.Embed:
        return self._make_embed(title=title, description=description, color=discord.Color.green())

    @commands.group(name="cmlink", invoke_without_command=True)
    async def cmlink(self, ctx: commands.Context):
        """Base command for CMLink cog."""
        await ctx.send_help()

    @cmlink.group(name="settings", invoke_without_command=True)
    @commands.is_owner()
    async def settings(self, ctx: commands.Context):
        """Manage Challenger Mode settings."""
        await ctx.send_help()

    @settings.command(name="seturl")
    @commands.is_owner()
    async def set_url(self, ctx: commands.Context, url: str):
        """Set the API URL for Challenger Mode."""
        await self.config.API_URL.set(url)
        await ctx.send(embed=self._make_success("API URL Set", f"Challenger Mode API URL has been set to:\n{url}"))

    @settings.command(name="setinterval")
    @commands.is_owner()
    async def set_interval(self, ctx: commands.Context, interval: int):
        """Set the polling interval in seconds."""
        await self.config.Poll_Interval.set(interval)
        await ctx.send(embed=self._make_success("Polling Interval Updated", f"Polling interval set to {interval} seconds."))

    @settings.command(name="setapilogging")
    @commands.is_owner()
    async def set_api_logging(self, ctx: commands.Context, enabled: bool):
        """Enable/disable temporary API response logging."""
        await self.config.Debug_API_Logging.set(enabled)
        await ctx.send(embed=self._make_embed("API Logging", f"API response logging {'enabled' if enabled else 'disabled'}.\nLogs: cmlink_api.log"))

    @settings.command(name="setrefreshtoken")
    @commands.is_owner()
    async def set_refresh_token(self, ctx: commands.Context, refresh_token: str):
        """Set the Challenger Mode refresh key (long-lived). Owner only."""
        await self.config.API_Refresh_Token.set(refresh_token)
        # clear cached access token to force immediate refresh on next request
        await self.config.API_Access_Token.set("")
        await self.config.API_Access_Expires_At.set(0)
        await ctx.send(embed=self._make_success("Refresh Token Stored", "Refresh token stored and access token cache cleared.\nA new access token will be obtained automatically on next API request."))

    @settings.command(name="settokenurl")
    @commands.is_owner()
    async def set_token_url(self, ctx: commands.Context, url: str):
        """Set the token exchange endpoint (owner only)."""
        await self.config.API_Token_URL.set(url)
        await ctx.send(embed=self._make_success("Token Endpoint Updated", f"Token exchange endpoint updated to:\n{url}"))

    @settings.command(name="apitest")
    @commands.is_owner()
    async def api_test(self, ctx: commands.Context):
        """Run a small authenticated test against the API."""
        api_url = await self.config.API_URL()
        if not api_url:
            await ctx.send(embed=self._make_error("API Test Failed", "API URL not set. Please configure the API URL."))
            return
        # Ensure monitor session exists
        if not getattr(self.monitor, "session", None):
            await self.monitor.start()
        query = """
        query TestMe {
          me {
            user {
              userId
              username
            }
          }
        }
        """
        # Use monitor's raw helper which will ensure access token via refresh key if configured
        js, status = await self.monitor._graphql_raw(api_url, None, query, {})
        if js is None:
            await ctx.send(embed=self._make_error("API Test Failed", "Request error or timeout. Enable API logging and check cmlink_api.log for details."))
            return
        if isinstance(js, dict) and "errors" in js and js["errors"]:
            msgs = []
            for e in js["errors"][:5]:
                msg = e.get("message", "<no message>")
                code = (e.get("extensions") or {}).get("code") or (e.get("extensions") or {}).get("errorCode")
                msgs.append(f"{msg} (code={code})" if code else msg)
            summary = "; ".join(msgs)
            await ctx.send(embed=self._make_error("API GraphQL Errors", f"{summary}\nSee cmlink_api.log for details."))
            return
        data = js.get("data") if isinstance(js, dict) else None
        me = (data or {}).get("me") if data else None
        user = me.get("user") if isinstance(me, dict) else None
        if user and user.get("userId"):
            await ctx.send(embed=self._make_success("API OK", f"Authenticated as: **{user.get('username', 'unknown')}** ({user['userId']})\nSee cmlink_api.log for details."))
        else:
            await ctx.send(embed=self._make_error("API Test Inconclusive", "Did not return authenticated user. Enable API logging and check cmlink_api.log for details."))

    @cmlink.command(name="connect")
    @commands.dm_only()
    async def connect(self, ctx: commands.Context, cm_identifier: str):
        """Link your Discord account with your Challenger Mode user ID (UUID).
        Username -> userId lookup is not supported by the public API; provide the userId (UUID)."""
        storage = Storage("users.json")

        api_url = await self.config.API_URL()
        if not api_url:
            await ctx.send(embed=self._make_error("Link Failed", "API URL not configured. Contact the bot owner."))
            return

        # Ensure monitor session exists
        if not getattr(self.monitor, "session", None):
            await self.monitor.start()

        # simple UUID-ish check (matches the canonical GUID format)
        is_uuid = bool(re.fullmatch(r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}", cm_identifier))

        if not is_uuid:
            await ctx.send(embed=self._make_error("Invalid Identifier", "This command only accepts a Challenger Mode userId (UUID).\nUsername -> userId lookup is not available. Please provide the userId from challengermode.com/users/<userId>."))
            return

        resolved_userid = None
        resolved_username = None

        # resolve username from userId using canonical query
        query = """
        query GetUser($id: UUID!) {
          user(userId: $id) {
            userId
            username
          }
        }
        """
        js, status = await self.monitor._graphql_raw(api_url, None, query, {"id": cm_identifier})
        if js and isinstance(js, dict) and js.get("data") and js["data"].get("user"):
            u = js["data"]["user"]
            resolved_userid = u.get("userId")
            resolved_username = u.get("username")
        else:
            await ctx.send(embed=self._make_error("Lookup Failed", "Could not resolve that userId via the API. Ensure the ID is correct and the bot has a valid access token."))
            return

        # final safety check
        if not resolved_userid:
            await ctx.send(embed=self._make_error("Link Failed", "Failed to obtain a Challenger Mode userId for that identifier."))
            return
        if not resolved_username:
            resolved_username = "<unknown>"

        # persist link (cm_user_id -> discord_user_id)
        storage.save_link(resolved_userid, ctx.author.id)

        await ctx.send(embed=self._make_success("Account Linked", f"Linked Challenger Mode user **{resolved_username}** ({resolved_userid}) to your Discord account."))

    @cmlink.group(name="tournament", invoke_without_command=True)
    @commands.guild_only()
    async def tournament(self, ctx: commands.Context):
        """Manage tournament integrations."""
        await ctx.send_help()

    # Add a guild-specific tournament settings subgroup
    @tournament.group(name="settings", invoke_without_command=True)
    @commands.guild_only()
    @commands.has_guild_permissions(manage_guild=True)
    async def tournament_settings(self, ctx: commands.Context):
        """Manage guild-specific tournament settings."""
        await ctx.send_help()

    @tournament_settings.command(name="setupdatechannel")
    @commands.guild_only()
    @commands.has_guild_permissions(manage_guild=True)
    async def set_update_channel(self, ctx: commands.Context, channel: TextChannel):
        """Set the channel where tournament updates will be posted for this guild."""
        # Persist explicitly using the accessor to avoid accidental key mismatches
        await self.config.guild(ctx.guild).update_channel_id.set(channel.id)
        await ctx.send(embed=self._make_success("Update Channel Set", f"Tournament update channel set to {channel.mention}."))

    @tournament_settings.command(name="setlobbyvoice")
    @commands.guild_only()
    @commands.has_guild_permissions(manage_guild=True)
    async def set_lobby_voice(self, ctx: commands.Context, channel: VoiceChannel):
        """Set the voice channel used as the tournament lobby for this guild."""
        # Persist explicitly using the accessor
        await self.config.guild(ctx.guild).lobby_voice_id.set(channel.id)
        await ctx.send(embed=self._make_success("Lobby Voice Set", f"Tournament lobby voice channel set to **{channel.name}**."))

    @tournament_settings.command(name="setcategory")
    @commands.guild_only()
    @commands.has_guild_permissions(manage_guild=True)
    async def set_category(self, ctx: commands.Context, category: CategoryChannel):
        """Set the category under which tournament channels will be created for this guild."""
        # Persist explicitly using the accessor
        await self.config.guild(ctx.guild).tournament_category_id.set(category.id)
        await ctx.send(embed=self._make_success("Category Set", f"Tournament category set to **{category.name}**."))

    @tournament.command(name="add")
    @commands.guild_only()
    async def add_tournament(self, ctx: commands.Context, tournament_id: str):
        """Add a new tournament to this guild."""
        # Use Red config (per-guild) for storing integrations instead of users.json
        guild_tournaments = await self.config.guild(ctx.guild).tournaments()
        if tournament_id in guild_tournaments:
            await ctx.send(embed=self._make_error("Already Added", "This tournament has already been added to this server."))
            return
        cfg = {
            "channel_id": str(ctx.channel.id),
            "role_id": None,
        }
        guild_tournaments[tournament_id] = cfg
        await self.config.guild(ctx.guild).tournaments.set(guild_tournaments)
        await ctx.send(embed=self._make_success("Tournament Added", f"Tournament **{tournament_id}** has been added to this server."))

    @tournament.command(name="remove")
    @commands.guild_only()
    async def remove_tournament(self, ctx: commands.Context, tournament_id: str):
        """Remove a tournament from this guild."""
        # Use Red config (per-guild) for storing integrations instead of users.json
        guild_tournaments = await self.config.guild(ctx.guild).tournaments()
        if tournament_id not in guild_tournaments:
            await ctx.send(embed=self._make_error("Not Found", "This tournament is not integrated with this server."))
            return
        guild_tournaments.pop(tournament_id, None)
        await self.config.guild(ctx.guild).tournaments.set(guild_tournaments)
        await ctx.send(embed=self._make_success("Tournament Removed", f"Tournament **{tournament_id}** has been removed from this server."))

    @tournament.command(name="unlinked")
    @commands.guild_only()
    @commands.has_guild_permissions(manage_guild=True)
    async def list_unlinked(self, ctx: commands.Context, tournament_id: str):
        """List participants in this tournament who haven't linked their Discord accounts."""
        participants = await self.monitor.get_tournament_participants(tournament_id)
        storage = Storage("users.json")
        links = storage.all_links()
        unlinked = []
        for p in participants:
            if p["userId"] not in links:
                unlinked.append(f'{p.get("username") or "Unknown"} ({p["userId"]})')
        if not unlinked:
            await ctx.send(embed=self._make_success("All Linked", "All participants appear to be linked."))
            return
        # Show up to 20 to keep messages concise
        preview = "\n".join(unlinked[:20])
        more = f"\n... and {len(unlinked)-20} more." if len(unlinked) > 20 else ""
        embed = self._make_embed(title=f"Unlinked participants ({len(unlinked)})", description=preview + more, color=discord.Color.orange())
        await ctx.send(embed=embed)

    # Admin utilities (guild-level)
    @cmlink.group(name="admin", invoke_without_command=True)
    @commands.guild_only()
    @commands.has_guild_permissions(manage_guild=True)
    async def admin(self, ctx: commands.Context):
        """Admin utilities for this guild."""
        await ctx.send_help()

    @admin.command(name="linked")
    @commands.guild_only()
    @commands.has_guild_permissions(manage_guild=True)
    async def admin_linked(self, ctx: commands.Context, limit: int = 25):
        """Show linked users who are members of this server."""
        storage = Storage("users.json")
        links = storage.all_links()
        total = len(links)
        lines = []
        in_guild = 0
        for cm_id, d_id in links.items():
            member = ctx.guild.get_member(int(d_id))
            if not member:
                continue
            in_guild += 1
            if len(lines) < max(1, min(50, limit)):
                lines.append(f"{cm_id} -> {member.mention} ({member})")
        more = f"\n... and {in_guild - len(lines)} more." if in_guild > len(lines) else ""
        if lines:
            desc = f"Linked users in this server: **{in_guild}** (global total: {total})\n" + "\n".join(lines) + more
            await ctx.send(embed=self._make_embed(title="Linked Users", description=desc))
        else:
            await ctx.send(embed=self._make_embed(title="Linked Users", description=f"No linked users found in this server. (global total: {total})"))

    @admin.command(name="tournaments")
    @commands.guild_only()
    @commands.has_guild_permissions(manage_guild=True)
    async def admin_tournaments(self, ctx: commands.Context):
        """Show tournaments configured for this server."""
        guild_tournaments = await self.config.guild(ctx.guild).tournaments()
        if not guild_tournaments:
            await ctx.send(embed=self._make_embed("No Tournaments", "No tournaments configured for this server."))
            return
        lines = []
        for tid, cfg in guild_tournaments.items():
            ch_id = cfg.get("channel_id")
            ch = ctx.guild.get_channel(int(ch_id)) if ch_id else None
            role_id = cfg.get("role_id")
            ch_disp = ch.mention if ch and hasattr(ch, "mention") else f"<#{ch_id}>" if ch_id else "unset"
            role_disp = f"<@&{role_id}>" if role_id else "unset"
            lines.append(f"{tid} -> channel: {ch_disp}, role: {role_disp}")
        await ctx.send(embed=self._make_embed("Active tournaments", "\n".join(lines[:25]) + (f"\n... and {len(lines)-25} more." if len(lines) > 25 else "")))

    @admin.command(name="settings")
    @commands.guild_only()
    @commands.has_guild_permissions(manage_guild=True)
    async def admin_settings(self, ctx: commands.Context):
        """Show guild settings (no API secrets)."""
        # Read individual settings using accessors to ensure we use the same keys/names as the setters
        update_channel_id = await self.config.guild(ctx.guild).update_channel_id()
        lobby_voice_id = await self.config.guild(ctx.guild).lobby_voice_id()
        category_id = await self.config.guild(ctx.guild).tournament_category_id()
        poll = await self.config.Poll_Interval()

        def maybe_int(idv):
            if idv is None:
                return None
            try:
                return int(idv)
            except Exception:
                return None

        update_ch = ctx.guild.get_channel(maybe_int(update_channel_id)) if update_channel_id else None
        lobby_vc = ctx.guild.get_channel(maybe_int(lobby_voice_id)) if lobby_voice_id else None
        category = ctx.guild.get_channel(maybe_int(category_id)) if category_id else None

        desc = (
            f"- Update channel: {update_ch.mention if update_ch else 'unset'}\n"
            f"- Lobby voice: {lobby_vc.name if lobby_vc else 'unset'}\n"
            f"- Category: {category.name if category else 'unset'}\n"
            f"- Poll interval: {poll}s"
        )
        await ctx.send(embed=self._make_embed("Guild settings", desc))

    @admin.command(name="forcelink")
    @commands.guild_only()
    @commands.has_guild_permissions(manage_guild=True)
    async def admin_forcelink(self, ctx: commands.Context, member: Member, cm_user_id: str):
        """Force link a CM user ID to a Discord member."""
        storage = Storage("users.json")
        storage.save_link(cm_user_id, member.id)
        await ctx.send(embed=self._make_success("Force Link", f"Linked CM user **{cm_user_id}** to {member.mention}."))

    def cog_unload(self):
        # Stop background task and cleanup
        if hasattr(self, "monitor") and self.monitor:
            self.bot.loop.create_task(self.monitor.stop())
