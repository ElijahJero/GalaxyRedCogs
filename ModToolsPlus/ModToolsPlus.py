from redbot.core import Config, commands
from redbot.core.bot import Red
import discord
from typing import Optional

QUARANTINE_ROLE_NAME = "Quarantined"

class ModToolsPlus(commands.Cog):
    """Advanced moderation tools including account quarantine."""

    def __init__(self, bot: Red):
        self.bot = bot
        self.config = Config.get_conf(self, identifier=9876543210)
        default_global = {
            # list of user IDs (as strings) that are globally quarantined
            "quarantined_users": []
        }
        self.config.register_global(**default_global)

    async def _get_or_create_quarantine_role(self, guild: discord.Guild) -> Optional[discord.Role]:
        """Get existing quarantine role or create one that explicitly denies every permission."""
        role = discord.utils.get(guild.roles, name=QUARANTINE_ROLE_NAME)
        if role is None:
            try:
                # Build a Permissions object with every field explicitly set to False
                no_perms = discord.Permissions(
                    create_instant_invite=False,
                    kick_members=False,
                    ban_members=False,
                    administrator=False,
                    manage_channels=False,
                    manage_guild=False,
                    add_reactions=False,
                    view_audit_log=False,
                    priority_speaker=False,
                    stream=False,
                    read_messages=False,
                    view_channel=False,
                    send_messages=False,
                    send_tts_messages=False,
                    manage_messages=False,
                    embed_links=False,
                    attach_files=False,
                    read_message_history=False,
                    mention_everyone=False,
                    external_emojis=False,
                    use_external_emojis=False,
                    view_guild_insights=False,
                    connect=False,
                    speak=False,
                    mute_members=False,
                    deafen_members=False,
                    move_members=False,
                    use_voice_activation=False,
                    change_nickname=False,
                    manage_nicknames=False,
                    manage_roles=False,
                    manage_webhooks=False,
                    manage_expressions=False,
                    manage_emojis=False,
                    manage_emojis_and_stickers=False,
                    use_application_commands=False,
                    request_to_speak=False,
                    manage_events=False,
                    manage_threads=False,
                    create_public_threads=False,
                    create_private_threads=False,
                    external_stickers=False,
                    use_external_stickers=False,
                    send_messages_in_threads=False,
                    use_embedded_activities=False,
                    moderate_members=False,
                )
                role = await guild.create_role(
                    name=QUARANTINE_ROLE_NAME,
                    permissions=no_perms,
                    color=discord.Color.dark_gray(),
                    reason="ModToolsPlus: quarantine role auto-created",
                )
            except Exception:
                return None
        return role

    async def _apply_quarantine_to_guild(self, guild: discord.Guild, user_id: int) -> bool:
        """Apply quarantine role to a user in a specific guild. Returns True if applied."""
        member = guild.get_member(user_id)
        if member is None:
            return False
        role = await self._get_or_create_quarantine_role(guild)
        if role is None:
            return False
        try:
            await member.add_roles(role, reason="ModToolsPlus: account quarantined")
            return True
        except Exception:
            return False

    @commands.group(name="modtoolsplus", invoke_without_command=True)
    async def modtoolsplus(self, ctx: commands.Context):
        """ModToolsPlus command group."""
        await ctx.send_help()

    @modtoolsplus.command(name="quarantine")
    @commands.guild_only()
    @commands.has_permissions(manage_roles=True)
    async def quarantine_user(self, ctx: commands.Context, user: discord.User, *, reason: str = "No reason provided"):
        """
        Quarantine a user across all mutual servers.
        Assigns a role that removes all permissions and DMs the user.
        """
        # Fetch bot owner
        app_info = await self.bot.application_info()
        owner = app_info.owner

        # Add to global quarantine list
        quarantined = await self.config.quarantined_users()
        if str(user.id) not in quarantined:
            quarantined.append(str(user.id))
            await self.config.quarantined_users.set(quarantined)

        # Apply quarantine role in all mutual guilds
        applied_guilds = []
        failed_guilds = []
        for guild in self.bot.guilds:
            success = await self._apply_quarantine_to_guild(guild, user.id)
            if success:
                applied_guilds.append(guild.name)
            else:
                # User may not be in this guild, that's fine
                member = guild.get_member(user.id)
                if member is not None:
                    failed_guilds.append(guild.name)

        # DM the user
        dm_sent = False
        try:
            embed = discord.Embed(
                title="⚠️ Your account has been quarantined",
                color=discord.Color.red(),
            )
            embed.description = (
                "Your account has been **quarantined** across all servers managed by this bot.\n\n"
                "This action is typically taken when an account is suspected of being compromised or hacked.\n\n"
                "**What this means:**\n"
                "• You have been given a restricted role in all mutual servers.\n"
                "• You will not be able to interact normally in those servers.\n\n"
                f"**What to do:**\n"
                f"Please secure your account (change your password, enable 2FA) and then "
                f"contact the bot owner to have this removed:\n"
                f"**{owner}** (ID: `{owner.id}`)\n\n"
                f"**Reason:** {reason}"
            )
            await user.send(embed=embed)
            dm_sent = True
        except discord.Forbidden:
            dm_sent = False
        except Exception:
            dm_sent = False

        # Respond to the moderator
        response = discord.Embed(
            title="🔒 User Quarantined",
            color=discord.Color.orange(),
        )
        response.add_field(name="User", value=f"{user} (ID: `{user.id}`)", inline=False)
        response.add_field(name="Reason", value=reason, inline=False)
        response.add_field(
            name="Applied in",
            value=", ".join(applied_guilds) if applied_guilds else "No mutual servers found",
            inline=False,
        )
        if failed_guilds:
            response.add_field(name="Failed in", value=", ".join(failed_guilds), inline=False)
        response.add_field(name="DM Sent", value="✅ Yes" if dm_sent else "❌ No (user may have DMs disabled)", inline=False)
        await ctx.send(embed=response)

    @modtoolsplus.command(name="unquarantine")
    @commands.guild_only()
    @commands.has_permissions(manage_roles=True)
    async def unquarantine_user(self, ctx: commands.Context, user: discord.User):
        """Remove quarantine from a user across all mutual servers."""
        quarantined = await self.config.quarantined_users()
        if str(user.id) not in quarantined:
            await ctx.send(embed=discord.Embed(
                description=f"{user} is not currently quarantined.",
                color=discord.Color.yellow(),
            ))
            return

        quarantined.remove(str(user.id))
        await self.config.quarantined_users.set(quarantined)

        removed_guilds = []
        for guild in self.bot.guilds:
            member = guild.get_member(user.id)
            if member is None:
                continue
            role = discord.utils.get(guild.roles, name=QUARANTINE_ROLE_NAME)
            if role and role in member.roles:
                try:
                    await member.remove_roles(role, reason="ModToolsPlus: quarantine lifted")
                    removed_guilds.append(guild.name)
                except Exception:
                    pass

        embed = discord.Embed(title="🔓 Quarantine Lifted", color=discord.Color.green())
        embed.add_field(name="User", value=f"{user} (ID: `{user.id}`)", inline=False)
        embed.add_field(
            name="Removed from",
            value=", ".join(removed_guilds) if removed_guilds else "No mutual servers found",
            inline=False,
        )
        await ctx.send(embed=embed)

        # Notify the user
        try:
            await user.send(embed=discord.Embed(
                title="✅ Your quarantine has been lifted",
                description="You have been unquarantined and can now participate in servers normally.",
                color=discord.Color.green(),
            ))
        except Exception:
            pass

    @commands.Cog.listener()
    async def on_member_join(self, member: discord.Member):
        """Re-apply quarantine role if a quarantined user joins any server the bot is in."""
        quarantined = await self.config.quarantined_users()
        if str(member.id) not in quarantined:
            return

        role = await self._get_or_create_quarantine_role(member.guild)
        if role is None:
            return
        try:
            await member.add_roles(role, reason="ModToolsPlus: rejoined while quarantined")
        except Exception:
            pass

        # Remind them they are still quarantined
        try:
            app_info = await self.bot.application_info()
            owner = app_info.owner
            embed = discord.Embed(
                title="⚠️ You are still quarantined",
                color=discord.Color.red(),
            )
            embed.description = (
                f"You have joined **{member.guild.name}**, but your account is still quarantined.\n\n"
                f"Please contact the bot owner to resolve this:\n"
                f"**{owner}** (ID: `{owner.id}`)"
            )
            await member.send(embed=embed)
        except Exception:
            pass

