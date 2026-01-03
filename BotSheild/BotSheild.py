from redbot.core import Config
from redbot.core import commands
from redbot.core.bot import Red
import random
import os
import json
import asyncio
import time
from datetime import datetime, timezone
import discord
from typing import Optional
import re
from .ScamCloud import analyze_text

class BotSheild(commands.Cog):
    """A cog that provides bot protection features."""

    def __init__(self, bot: Red):
        self.bot = bot
        self.config = Config.get_conf(self, identifier=1234567890)
        default_global = {
            # map guild_id -> {"captcha_count": int, "auto_verify_days": int, "setup_time": int}
            "protected_servers": {},
            "alert_role": None,
            # scam_protection controls message analysis for newer members
            "scam_protection": {
                "enabled": True,
                "new_member_days": 30,     # default window in days to scan messages from members
                "min_score": 1.0,         # threshold to trigger a warning
                # wordlist: key -> score (float). special key "tld" matches top-level domains
                "wordlist": {
                    "tld": 0.5,
                },
            },
        }
        self.config.register_global(**default_global)

    # ----------------------------
    # Admin command group wrappers
    # ----------------------------
    @commands.group(name="botsheild", invoke_without_command=True)
    @commands.guild_only()
    async def botsheild(self, ctx: commands.Context):
        """Top-level BotSheild command. Use subcommands."""
        # Use Red's built-in help/usage display so subcommands are listed correctly
        await ctx.send_help()

    @botsheild.command(name="protect")
    @commands.guild_only()
    @commands.has_permissions(manage_guild=True)
    async def bs_protect(self, ctx: commands.Context, captcha_count: int = 1, auto_verify_days: int = -1, log_channel: Optional[discord.TextChannel] = None):
        """Alias to protect_server as a subcommand."""
        await self.protect_server(ctx, captcha_count=captcha_count, auto_verify_days=auto_verify_days, log_channel=log_channel)

    @botsheild.command(name="unprotect")
    @commands.guild_only()
    @commands.has_permissions(manage_guild=True)
    async def bs_unprotect(self, ctx: commands.Context):
        """Alias to unprotect_server as a subcommand."""
        await self.unprotect_server(ctx)

    @botsheild.command(name="addverify")
    @commands.guild_only()
    @commands.has_permissions(manage_guild=True)
    async def bs_addverify(self, ctx: commands.Context, member: discord.Member):
        """Alias to addverify as a subcommand."""
        await self.addverify(ctx, member)

    @botsheild.command(name="removeverify")
    @commands.guild_only()
    @commands.has_permissions(manage_guild=True)
    async def bs_removeverify(self, ctx: commands.Context, member: discord.Member):
        """Alias to removeverify as a subcommand."""
        await self.removeverify(ctx, member)

    @botsheild.group(name="scam", invoke_without_command=True)
    @commands.guild_only()
    @commands.has_permissions(manage_guild=True)
    async def bs_scam(self, ctx: commands.Context):
        """Scam protection configuration group."""
        await ctx.send_help()

    @bs_scam.command(name="setdays")
    @commands.guild_only()
    @commands.has_permissions(manage_guild=True)
    async def bs_scam_setdays(self, ctx: commands.Context, days: int):
        await self.scam_setdays(ctx, days)

    @bs_scam.command(name="setminscore")
    @commands.guild_only()
    @commands.has_permissions(manage_guild=True)
    async def bs_scam_setminscore(self, ctx: commands.Context, min_score: float):
        await self.scam_setminscore(ctx, min_score)

    @bs_scam.command(name="word_add")
    @commands.guild_only()
    @commands.has_permissions(manage_guild=True)
    async def bs_scam_word_add(self, ctx: commands.Context, token: str, score: float):
        await self.scam_word_add(ctx, token, score)

    @bs_scam.command(name="word_remove")
    @commands.guild_only()
    @commands.has_permissions(manage_guild=True)
    async def bs_scam_word_remove(self, ctx: commands.Context, token: str):
        await self.scam_word_remove(ctx, token)

    @bs_scam.command(name="word_list")
    @commands.guild_only()
    @commands.has_permissions(manage_guild=True)
    async def bs_scam_word_list(self, ctx: commands.Context):
        await self.scam_word_list(ctx)

    # ----------------------------
    # Warning + admin reaction UI
    # ----------------------------
    async def _resolve_log_channel(self, guild: discord.Guild) -> Optional[discord.TextChannel]:
        try:
            protected = await self.config.protected_servers()
            conf = protected.get(str(guild.id), {})
            chan_id = conf.get("log_channel_id")
            if chan_id:
                ch = self.bot.get_channel(int(chan_id)) or guild.get_channel(int(chan_id))
                if isinstance(ch, discord.TextChannel):
                    return ch
        except Exception:
            pass
        return None

    async def _send_flag_warning(
        self,
        channel: discord.TextChannel,
        target_member: discord.Member,
        *,
        score: float,
        age_str: str,
        age_seconds: Optional[int] = None,  # added param
        matches: Optional[dict],
        original_message: discord.Message,
    ) -> Optional[discord.Message]:
        """
        Send a concise, high-visibility warning embed focused on giveaway scams.
        Admin reactions remain the same: 🔨 ban+delete, 🚪 kick, ❌ remove warning.
        This function now prepends configured ping roles for the guild (if any).
        """
        # resolve per-guild ping roles (do not modify stored config)
        ping_text = ""
        try:
            protected = await self.config.protected_servers()
            conf = protected.get(str(channel.guild.id), {}) if channel.guild else {}
            pr_ids = conf.get("ping_role_ids", [])
            mentions = []
            for rid in pr_ids:
                try:
                    mentions.append(f"<@&{int(rid)}>")
                except Exception:
                    continue
            if mentions:
                ping_text = " ".join(mentions)
        except Exception:
            ping_text = ""

        try:
            desc = (
                "\nThis message matches common scam patterns (promises of free items, urgent asks to DM or move off-server).\n\n"
                "Quick safety tips:\n"
                "• Don’t DM, transfer items, or share codes/account info.\n"
                "• Don’t click unknown links or scan QR codes — they can contain malware.\n"
                "• If asked to move to another server or external site, be suspicious.\n"
                "• Nobody is giving away expensive stuff fot free. Never trust a giveaway from a random user.\n\n"
                "This may be a false positive. If you were actually intending to give something away please contact moderator to assist you."
            )
            embed = discord.Embed(
                title="🚨⚠️ POSSIBLE SCAM DETECTED ⚠️🚨",
                description=desc,
                color=discord.Color.red(),
            )
            # Display member as plain text (name#discriminator + ID) to avoid pinging them
            embed.add_field(
                name="Member",
                value=f"{target_member.name}",
                inline=True,
            )
            embed.add_field(name="Time in server", value=age_str, inline=True)

            # If the member has been in the server for less than 7 days, add a clear notice
            try:
                if age_seconds is not None and age_seconds < 7 * 86400:
                    embed.add_field(
                        name="Community status",
                        value="This user just joined the server, use extreme caution.",
                        inline=False,
                    )
            except Exception:
                # fail silently if age_seconds can't be interpreted
                pass

            embed.set_footer(text="Staff: react below to take action.")
            # Only include configured ping roles in the message content.
            # Do NOT mention the flagged member in the content to avoid pinging them.
            # If no ping roles are configured, send the embed with no content.
            warn_msg = await channel.send(content=ping_text or None, embed=embed)
        except Exception:
            return None

        # Add admin action reactions
        for e in ("🔨", "🚪", "❌"):
            try:
                await warn_msg.add_reaction(e)
            except Exception:
                pass

        # Start monitoring reactions (logs include score and full context)
        try:
            asyncio.create_task(self._monitor_admin_reactions(warn_msg, target_member, score, age_str, matches, original_message))
        except Exception:
            pass
        return warn_msg

    async def _monitor_admin_reactions(
        self,
        warn_msg: discord.Message,
        target_member: discord.Member,
        score: float,
        age_str: str,
        matches: Optional[dict],
        original_message: discord.Message,
        timeout: int = 1800,  # 30 minutes
    ):
        """Monitor reactions; restrict to admins/mods; act and log. On timeout disable mod actions but keep the warning visible."""
        guild = warn_msg.guild
        if guild is None:
            return

        actions = {"🔨": "ban", "🚪": "kick", "❌": "remove"}
        end = time.time() + timeout

        def is_privileged(member: Optional[discord.Member]) -> bool:
            if member is None:
                return False
            perms = member.guild_permissions
            return any([
                perms.administrator,
                perms.manage_guild,
                perms.ban_members,
                perms.kick_members,
                perms.manage_messages,
            ])

        while True:
            remaining = end - time.time()
            if remaining <= 0:
                # Timeout: disable admin actions but keep the warning message
                try:
                    await warn_msg.clear_reactions()
                except Exception:
                    pass
                try:
                    if warn_msg.embeds:
                        base = warn_msg.embeds[0]
                        new_emb = discord.Embed.from_dict(base.to_dict())
                        new_emb.set_footer(text="Staff action window expired — reactions disabled.")
                        await warn_msg.edit(embed=new_emb)
                    else:
                        await warn_msg.edit(content=warn_msg.content)
                except Exception:
                    pass
                return
            try:
                reaction, user = await self.bot.wait_for(
                    "reaction_add",
                    timeout=remaining,
                    check=lambda r, u: r.message.id == warn_msg.id,
                )
            except asyncio.TimeoutError:
                # Timeout while waiting: disable actions but keep message
                try:
                    await warn_msg.clear_reactions()
                except Exception:
                    pass
                try:
                    if warn_msg.embeds:
                        base = warn_msg.embeds[0]
                        new_emb = discord.Embed.from_dict(base.to_dict())
                        new_emb.set_footer(text="Staff action window expired — reactions disabled.")
                        await warn_msg.edit(embed=new_emb)
                    else:
                        await warn_msg.edit(content=warn_msg.content)
                except Exception:
                    pass
                return

            if user.bot:
                continue

            emoji = str(reaction.emoji)
            actor: Optional[discord.Member] = guild.get_member(user.id)
            if not is_privileged(actor):
                # Remove unauthorized user reaction
                try:
                    await warn_msg.remove_reaction(emoji, user)
                except Exception:
                    pass
                continue

            action = actions.get(emoji)
            if action is None:
                # Not one of our control emojis; remove it
                try:
                    await warn_msg.remove_reaction(emoji, user)
                except Exception:
                    pass
                continue

            # Perform action
            action_done = None
            reason = f"Performed via BotSheild by {actor} ({actor.id}) on warning."
            try:
                if action == "ban":
                    # Delete up to last 7 days of messages (Discord limitation)
                    await guild.ban(target_member, reason=reason, delete_message_seconds=604800)
                    action_done = "Ban + delete recent messages (up to 7 days)"
                elif action == "kick":
                    await guild.kick(target_member, reason=reason)
                    action_done = "Kick"
                else:
                    action_done = "Remove warning"
            except Exception:
                # Even if moderation fails, still remove the warning so we don't loop forever
                if action == "ban":
                    action_done = "Ban attempt failed"
                elif action == "kick":
                    action_done = "Kick attempt failed"
                else:
                    action_done = "Remove warning"

            # Remove the warning message
            try:
                await warn_msg.delete()
            except Exception:
                pass

            # Log details to configured log channel
            log_channel = await self._resolve_log_channel(guild)
            if log_channel:
                try:
                    e = discord.Embed(title="Admin Action on Warning", color=discord.Color.orange())
                    e.add_field(name="Action", value=action_done, inline=False)
                    e.add_field(name="Actor", value=f"{actor.mention if actor else user.mention} (ID: {user.id})", inline=False)
                    e.add_field(name="Target", value=f"{target_member.mention} (ID: {target_member.id})", inline=False)
                    e.add_field(name="Channel", value=f"#{original_message.channel.name} (ID: {original_message.channel.id})", inline=False)
                    # Original flagged message info
                    content = original_message.content or "[empty]"
                    e.add_field(name="Flagged Message", value=content[:1024], inline=False)
                    if original_message.attachments:
                        e.add_field(name="Attachments", value=", ".join(a.url for a in original_message.attachments), inline=False)
                    e.add_field(name="Score / Matches", value=f"{score:.2f} / {', '.join(list(matches.keys())[:5]) if matches else 'none'}", inline=False)
                    e.add_field(name="Message Link", value=original_message.jump_url, inline=False)
                    # Use timezone-aware UTC timestamp
                    e.set_footer(text=f"Time: {datetime.now(timezone.utc).isoformat()}")
                    await log_channel.send(embed=e)
                except Exception:
                    pass

            # End after the first valid admin action
            return

    def generate_captcha(self):
        # Generate a random target sum 0-9 and split into two numbers
        target_sum = random.randint(0, 9)
        number_a = random.randint(0, target_sum)
        number_b = target_sum - number_a
        return number_a, number_b

    number_emojis = ["0️⃣","1️⃣","2️⃣","3️⃣","4️⃣","5️⃣","6️⃣","7️⃣","8️⃣","9️⃣"]

    # helper to load/save users.json
    def _load_users(self):
        base_path = os.path.dirname(__file__)
        users_file = os.path.join(base_path, "users.json")
        try:
            with open(users_file, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}

    def _save_users(self, data):
        base_path = os.path.dirname(__file__)
        users_file = os.path.join(base_path, "users.json")
        try:
            with open(users_file, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)
        except Exception:
            pass

    @commands.Cog.listener()
    async def on_message(self, message):
        # If there is a message in a protected server from an unverified user, send a captcha challenge

        # ignore messages from bots and DMs
        if message.author.bot:
            return
        if message.guild is None:
            return
        if (message.type != discord.MessageType.default and
            message.type != discord.MessageType.reply and
            message.type != discord.MessageType.thread_starter_message):
            return

        protected_servers = await self.config.protected_servers()
        guild_id = str(message.guild.id)
        if guild_id not in protected_servers:
            return
        guild_conf = protected_servers.get(guild_id, {})

        # Load verification and per-user stats from users.json
        users_data = self._load_users()
        if guild_id not in users_data:
            users_data[guild_id] = {}

        member_id = str(message.author.id)
        # ensure a record exists and preserve any existing fields
        member_record = users_data[guild_id].get(member_id)
        if member_record is None:
            member_record = {"verified": False, "progress": 0}
            users_data[guild_id][member_id] = member_record
        else:
            # preserve existing fields for compatibility; do NOT track messages_sent here
            pass

        # persist any created record (do not modify verification/progress here)
        self._save_users(users_data)

        # determine verification status (may be changed below if flagged)
        verified = bool(member_record.get("verified", False))

        # Scam analysis for new-ish members (configurable)
        scam_conf = await self.config.scam_protection()
        try:
            scam_enabled = bool(scam_conf.get("enabled", True))
        except Exception:
            scam_enabled = True

        # compute member join age once
        joined_at = message.author.joined_at
        age_seconds = None
        if joined_at is None and message.guild is not None:
            # Try to fetch the member to obtain join timestamp if not cached.
            # This helps when member intents/cache don't include joined_at.
            try:
                fetched = await message.guild.fetch_member(message.author.id)
                joined_at = getattr(fetched, "joined_at", None)
            except Exception:
                joined_at = None

        if joined_at is not None:
            now_ts = int(time.time())
            joined_ts = int(joined_at.replace(tzinfo=timezone.utc).timestamp())
            age_seconds = now_ts - joined_ts

        if scam_enabled and age_seconds is not None:
            new_member_days = int(scam_conf.get("new_member_days", 30))
            # Only scan members who have been in the server less than the configured threshold
            if age_seconds < new_member_days * 86400:
                # analyze this message using ScamCloud analyzer (case-insensitive)
                wordlist = scam_conf.get("wordlist", {})
                score, matches = analyze_text(message.content or "", wordlist)

                min_score = float(scam_conf.get("min_score", 1.0))
                if score >= min_score:
                    # format how long they've been in server
                    days = age_seconds // 86400
                    hours = (age_seconds % 86400) // 3600
                    minutes = (age_seconds % 3600) // 60
                    age_str = f"{days}d {hours}h {minutes}m"

                    # If the user is currently verified, remove verification and persist
                    if verified:
                        users_data[guild_id][member_id]["verified"] = False
                        users_data[guild_id][member_id]["progress"] = 0
                        try:
                            self._save_users(users_data)
                        except Exception:
                            pass
                        verified = False
                        # send warning with admin reactions (pass age_seconds)
                        try:
                            await self._send_flag_warning(
                                message.channel,
                                message.author,
                                score=score,
                                age_str=age_str,
                                age_seconds=age_seconds,
                                matches=matches,
                                original_message=message,
                            )
                        except Exception:
                            pass
                    else:
                        # unverified: still send warning with admin reactions (pass age_seconds)
                        try:
                            await self._send_flag_warning(
                                message.channel,
                                message.author,
                                score=score,
                                age_str=age_str,
                                age_seconds=age_seconds,
                                matches=matches,
                                original_message=message,
                            )
                        except Exception:
                            pass

        # refresh member_record from possibly-updated users_data
        member_record = users_data[guild_id].get(member_id, {"verified": False, "progress": 0})
        verified = bool(member_record.get("verified", False))
        if verified:
            return

        # proceed to send a captcha challenge
        await self.handle_captcha_challenge(message, guild_conf)

    async def handle_captcha_challenge(self, message, guild_conf):
        """
        Send a captcha message, add reactions, and wait for the member to react with the correct sum.
        On success, increment progress and mark verified when reaching required count.
        On failure or timeout, delete original message and captcha message.
        Logs events to configured logging channel if set.
        """
        member = message.author
        channel = message.channel
        number_a, number_b = self.generate_captcha()
        correct_sum = number_a + number_b
        # Prepare emoji choices: correct + 3 unique wrongs
        all_digits = list(range(0, 10))
        wrong_choices = [d for d in all_digits if d != correct_sum]
        random.shuffle(wrong_choices)
        choices = [correct_sum] + wrong_choices[:3]
        random.shuffle(choices)  # randomize order

        # Build embed captcha message
        try:
            captcha_embed = discord.Embed(title="Captcha Verification", color=discord.Color.blurple())
            captcha_embed.description = f"Please react with the sum of **{number_a} and {number_b}**.\nYou have 60 seconds."
            captcha_msg = await channel.send(content=member.mention, embed=captcha_embed)
        except Exception:
            return

        # add reactions corresponding to numbers
        for num in choices:
            emoji = self.number_emojis[num]
            try:
                await captcha_msg.add_reaction(emoji)
            except Exception:
                # ignore reaction failures
                pass

        # wait for reactions
        deadline = time.time() + 60
        successful = False
        timed_out = False
        fail_reason = None
        reacted_digit = None
        start_time = time.time()

        while True:
            timeout = deadline - time.time()
            if timeout <= 0:
                timed_out = True
                fail_reason = "timeout"
                break
            try:
                reaction, user = await self.bot.wait_for(
                    "reaction_add",
                    check=lambda r, u: r.message.id == captcha_msg.id,
                    timeout=timeout,
                )
            except asyncio.TimeoutError:
                timed_out = True
                fail_reason = "timeout"
                break

            # If someone else reacted: remove their reaction and continue waiting
            if user.id != member.id:
                try:
                    await captcha_msg.remove_reaction(reaction.emoji, user)
                except Exception:
                    pass
                continue

            # user is the target member: check if correct
            reacted_emoji = reaction.emoji
            # translate emoji back to digit (safe mapping)
            try:
                reacted_digit = self.number_emojis.index(reacted_emoji)
            except ValueError:
                reacted_digit = None

            if reacted_digit is None:
                # invalid emoji, treat as wrong
                successful = False
                fail_reason = "invalid_reaction"
                break

            if reacted_digit == correct_sum:
                successful = True
                break
            else:
                successful = False
                fail_reason = f"incorrect_answer:{reacted_digit}"
                break

        users = self._load_users()
        guild_id = str(message.guild.id)
        if guild_id not in users:
            users[guild_id] = {}
        member_id = str(member.id)
        member_record = users[guild_id].get(member_id, {"verified": False, "progress": 0})

        # gather removed message content/attachments for logging
        removed_content = message.content if getattr(message, "content", None) else ""
        attachments = [a.url for a in message.attachments] if getattr(message, "attachments", None) else []

        # find configured log channel
        log_channel = None
        try:
            log_channel_id = guild_conf.get("log_channel_id")
            if log_channel_id:
                log_channel = self.bot.get_channel(int(log_channel_id))
        except Exception:
            log_channel = None

        # handle outcomes
        try:
            if not successful:
                # delete both the original message and the captcha message
                try:
                    await message.delete()
                except Exception:
                    pass
                try:
                    await captcha_msg.delete()
                except Exception:
                    pass
                # prepare log info
                if fail_reason is None:
                    fail_reason = "unknown"
                # Construct friendly reason text
                if fail_reason.startswith("incorrect_answer"):
                    parts = fail_reason.split(":")
                    chosen = parts[1] if len(parts) > 1 else "unknown"
                    reason_text = f"Incorrect answer selected ({chosen}). Expected: {correct_sum}."
                elif fail_reason == "timeout":
                    reason_text = "Timeout (no valid reaction within time limit)."
                elif fail_reason == "invalid_reaction":
                    reason_text = "Invalid reaction (not a recognized digit emoji)."
                else:
                    reason_text = f"Fail reason: {fail_reason}"

                # Log to configured channel if available (embed)
                if log_channel is not None:
                    try:
                        e = discord.Embed(title="Captcha Failed", color=discord.Color.red())
                        e.add_field(name="User", value=f"{member} (ID: {member.id})", inline=False)
                        e.add_field(name="Channel", value=f"#{channel.name} (ID: {channel.id})", inline=False)
                        e.add_field(name="Reason", value=reason_text, inline=False)
                        e.add_field(name="Original message", value=(removed_content or "[empty]"), inline=False)
                        if attachments:
                            e.add_field(name="Attachments", value=", ".join(attachments), inline=False)
                        # Use timezone-aware UTC timestamp
                        e.set_footer(text=f"Time: {datetime.now(timezone.utc).isoformat()}")
                        await log_channel.send(embed=e)
                    except Exception:
                        pass

                # no state change on failure (progress stays same)
                self._save_users(users)
                return

            # success path: compute elapsed, increment progress and possibly verify
            elapsed = time.time() - start_time
            required = int(guild_conf.get("captcha_count", 1))
            current_progress = int(member_record.get("progress", 0))
            current_progress += 1
            member_record["progress"] = current_progress
            if current_progress >= required:
                member_record["verified"] = True
                # optionally clear progress
                member_record["progress"] = 0
                users[guild_id][member_id] = member_record
                self._save_users(users)
                # delete captcha message
                try:
                    await captcha_msg.delete()
                except Exception:
                    pass
                # send verification success then delete after 10s (embed)
                try:
                    e = discord.Embed(title="Verification Complete", color=discord.Color.green())
                    e.description = f"{member.mention} You are now verified."
                    success_msg = await channel.send(embed=e)
                    await asyncio.sleep(10)
                    try:
                        await success_msg.delete()
                    except Exception:
                        pass
                except Exception:
                    pass
                # log success (embed)
                if log_channel is not None:
                    try:
                        suspicious_text = " (suspiciously fast)" if elapsed < 2.0 else ""
                        e = discord.Embed(title="Captcha Completed", color=discord.Color.green())
                        e.add_field(name="User", value=f"{member} (ID: {member.id})", inline=False)
                        e.add_field(name="Channel", value=f"#{channel.name} (ID: {channel.id})", inline=False)
                        e.add_field(name="Time taken", value=f"{elapsed:.2f}s{suspicious_text}", inline=False)
                        e.add_field(name="Status", value=f"Now verified (required {required})", inline=False)
                        # Use timezone-aware UTC timestamp
                        e.set_footer(text=f"Time: {datetime.now(timezone.utc).isoformat()}")
                        await log_channel.send(embed=e)
                    except Exception:
                        pass
            else:
                # not yet verified, save progress and delete captcha message
                users[guild_id][member_id] = member_record
                self._save_users(users)
                try:
                    await captcha_msg.delete()
                except Exception:
                    pass
                # Inform user only with a generic confirmation (no numeric progress), then delete shortly
                try:
                    e = discord.Embed(title="Captcha Passed", color=discord.Color.green())
                    e.description = f"{member.mention} Your response was accepted."
                    success_msg = await channel.send(embed=e)
                    await asyncio.sleep(5)
                    try:
                        await success_msg.delete()
                    except Exception:
                        pass
                except Exception:
                    pass
                # log progress to admin channel (still includes numeric progress for staff)
                if log_channel is not None:
                    try:
                        suspicious_text = " (suspiciously fast)" if (time.time() - start_time) < 2.0 else ""
                        e = discord.Embed(title="Captcha Completed (Progress)", color=discord.Color.green())
                        e.add_field(name="User", value=f"{member} (ID: {member.id})", inline=False)
                        e.add_field(name="Channel", value=f"#{channel.name} (ID: {channel.id})", inline=False)
                        e.add_field(name="Time taken", value=f"{(time.time() - start_time):.2f}s{suspicious_text}", inline=False)
                        e.add_field(name="Progress", value=f"{current_progress}/{required}", inline=False)
                        # Use timezone-aware UTC timestamp
                        e.set_footer(text=f"Time: {datetime.now(timezone.utc).isoformat()}")
                        await log_channel.send(embed=e)
                    except Exception:
                        pass
        finally:
            # ensure file saved
            self._save_users(users)

    async def protect_server(self, ctx: commands.Context, *, captcha_count: int = 1, auto_verify_days: int = -1, log_channel: Optional[discord.TextChannel] = None):
        """
        Helper to enable protection for the server. Not a top-level command (called by botsheild protect).
        Stores per-guild protection config in self.config.protected_servers.
        """
        if ctx.guild is None:
            try:
                await ctx.send(embed=discord.Embed(description="This command must be used in a server.", color=discord.Color.red()))
            except Exception:
                pass
            return

        gid = str(ctx.guild.id)
        protected = await self.config.protected_servers()
        conf = protected.get(gid, {}) if protected else {}

        # sanitize inputs
        conf["captcha_count"] = int(captcha_count) if captcha_count and int(captcha_count) > 0 else 1
        conf["auto_verify_days"] = int(auto_verify_days) if auto_verify_days is not None else -1
        if log_channel is not None:
            conf["log_channel_id"] = int(log_channel.id)
        else:
            # preserve existing log_channel_id if present; otherwise remove
            if "log_channel_id" in conf and log_channel is None:
                conf.pop("log_channel_id", None)

        protected[gid] = conf
        await self.config.protected_servers.set(protected)

        embed = discord.Embed(title="Server Protected", color=discord.Color.green())
        embed.add_field(name="Server", value=f"**{ctx.guild.name}** (ID: {gid})", inline=False)
        embed.add_field(name="Captcha Count", value=str(conf["captcha_count"]), inline=True)
        embed.add_field(name="Auto-verify Days", value=str(conf["auto_verify_days"]), inline=True)
        if conf.get("log_channel_id"):
            ch = ctx.guild.get_channel(conf["log_channel_id"])
            embed.add_field(name="Log Channel", value=(ch.mention if ch else f"(ID {conf['log_channel_id']})"), inline=False)
        await ctx.send(embed=embed)

    # ----------------------------
    # Scam helper implementations
    # ----------------------------
    async def scam_setdays(self, ctx: commands.Context, days: int):
        """Set how many days a member is considered 'new' for scam analysis (default 30)."""
        if days < 0:
            await ctx.send(embed=discord.Embed(description="Days must be >= 0.", color=discord.Color.red()))
            return
        conf = await self.config.scam_protection()
        conf["new_member_days"] = int(days)
        await self.config.scam_protection.set(conf)
        await ctx.send(embed=discord.Embed(description=f"Scam detection window set to {days} days.", color=discord.Color.green()))

    async def scam_setminscore(self, ctx: commands.Context, min_score: float):
        """Set the score threshold above which a warning is sent."""
        conf = await self.config.scam_protection()
        conf["min_score"] = float(min_score)
        await self.config.scam_protection.set(conf)
        await ctx.send(embed=discord.Embed(description=f"Scam detection min_score set to {min_score}.", color=discord.Color.green()))

    async def scam_word_add(self, ctx: commands.Context, token: str, score: float):
        """Add a token to the wordlist with an associated score. Use 'tld' as token to score TLDs."""
        conf = await self.config.scam_protection()
        wl = conf.get("wordlist", {})
        wl[token.lower()] = float(score)
        conf["wordlist"] = wl
        await self.config.scam_protection.set(conf)
        await ctx.send(embed=discord.Embed(description=f"Added wordlist token '{token}' with score {score}.", color=discord.Color.green()))

    async def scam_word_remove(self, ctx: commands.Context, token: str):
        """Remove a token from the wordlist."""
        conf = await self.config.scam_protection()
        wl = conf.get("wordlist", {})
        token_l = token.lower()
        if token_l in wl:
            del wl[token_l]
            conf["wordlist"] = wl
            await self.config.scam_protection.set(conf)
            await ctx.send(embed=discord.Embed(description=f"Removed wordlist token '{token}'.", color=discord.Color.orange()))
        else:
            await ctx.send(embed=discord.Embed(description=f"Token '{token}' not found in wordlist.", color=discord.Color.yellow()))

    async def scam_word_list(self, ctx: commands.Context):
        """List current wordlist tokens and their scores."""
        conf = await self.config.scam_protection()
        wl = conf.get("wordlist", {})
        if not wl:
            await ctx.send(embed=discord.Embed(description="Wordlist is empty.", color=discord.Color.yellow()))
            return
        # Build a readable embed (truncate long lists)
        embed = discord.Embed(title="Scam wordlist", color=discord.Color.blue())
        lines = []
        for k, v in sorted(wl.items()):
            lines.append(f"• {k}: {v}")
        page_text = "\n".join(lines)
        if len(page_text) > 1900:
            page_text = page_text[:1900] + "\n… (truncated)"
        embed.description = page_text
        await ctx.send(embed=embed)

    # ----------------------------
    # New pingroles subcommands
    # ----------------------------
    @botsheild.group(name="pingroles", invoke_without_command=True)
    @commands.guild_only()
    @commands.has_permissions(manage_guild=True)
    async def bs_pingroles(self, ctx: commands.Context):
        """Manage roles that are pinged when a warning is posted. Subcommands: add/remove/list/clear"""
        await ctx.send_help()

    @bs_pingroles.command(name="add")
    @commands.guild_only()
    @commands.has_permissions(manage_guild=True)
    async def bs_pingroles_add(self, ctx: commands.Context, role: discord.Role):
        """Add a role to be pinged on warnings for this server."""
        gid = str(ctx.guild.id)
        protected = await self.config.protected_servers()
        conf = protected.get(gid, {}) if protected else {}
        lst = conf.get("ping_role_ids", [])
        if role.id in lst:
            await ctx.send(embed=discord.Embed(description=f"{role.mention} is already in the ping list.", color=discord.Color.yellow()))
            return
        lst.append(role.id)
        conf["ping_role_ids"] = lst
        protected[gid] = conf
        await self.config.protected_servers.set(protected)
        await ctx.send(embed=discord.Embed(description=f"Added {role.mention} to warning pings.", color=discord.Color.green()))

    @bs_pingroles.command(name="remove")
    @commands.guild_only()
    @commands.has_permissions(manage_guild=True)
    async def bs_pingroles_remove(self, ctx: commands.Context, role: discord.Role):
        """Remove a role from the warning ping list for this server."""
        gid = str(ctx.guild.id)
        protected = await self.config.protected_servers()
        conf = protected.get(gid, {}) if protected else {}
        lst = conf.get("ping_role_ids", [])
        if role.id not in lst:
            await ctx.send(embed=discord.Embed(description=f"{role.mention} is not in the ping list.", color=discord.Color.yellow()))
            return
        lst.remove(role.id)
        conf["ping_role_ids"] = lst
        protected[gid] = conf
        await self.config.protected_servers.set(protected)
        await ctx.send(embed=discord.Embed(description=f"Removed {role.mention} from warning pings.", color=discord.Color.orange()))

    @bs_pingroles.command(name="list")
    @commands.guild_only()
    @commands.has_permissions(manage_guild=True)
    async def bs_pingroles_list(self, ctx: commands.Context):
        """List roles configured to be pinged on warnings for this server."""
        gid = str(ctx.guild.id)
        protected = await self.config.protected_servers()
        conf = protected.get(gid, {}) if protected else {}
        lst = conf.get("ping_role_ids", [])
        if not lst:
            await ctx.send(embed=discord.Embed(description="No roles configured to be pinged for warnings.", color=discord.Color.yellow()))
            return
        mentions = []
        for rid in lst:
            try:
                role = ctx.guild.get_role(int(rid))
                mentions.append(role.mention if role else f"(missing role {rid})")
            except Exception:
                mentions.append(f"(invalid id {rid})")
        await ctx.send(embed=discord.Embed(description="Configured ping roles:\n" + ", ".join(mentions), color=discord.Color.blue()))

    @bs_pingroles.command(name="clear")
    @commands.guild_only()
    @commands.has_permissions(manage_guild=True)
    async def bs_pingroles_clear(self, ctx: commands.Context):
        """Clear all roles configured to be pinged on warnings for this server."""
        gid = str(ctx.guild.id)
        protected = await self.config.protected_servers()
        conf = protected.get(gid, {}) if protected else {}
        conf["ping_role_ids"] = []
        protected[gid] = conf
        await self.config.protected_servers.set(protected)
        await ctx.send(embed=discord.Embed(description="Cleared all warning ping roles.", color=discord.Color.orange()))

    async def addverify(self, ctx: commands.Context, member: discord.Member):
        """Manually mark a user as verified (helper called by botsheild addverify)."""
        guild = ctx.guild
        if guild is None:
            try:
                await ctx.send(embed=discord.Embed(description="This command must be used in a server.", color=discord.Color.red()))
            except Exception:
                pass
            return
        users = self._load_users()
        gid = str(guild.id)
        if gid not in users:
            users[gid] = {}
        users[gid][str(member.id)] = {"verified": True, "progress": 0}
        try:
            self._save_users(users)
        except Exception:
            pass
        embed = discord.Embed(title="Verification Updated", color=discord.Color.green())
        embed.description = f"{member.mention} has been marked as verified."
        await ctx.send(embed=embed)

    async def removeverify(self, ctx: commands.Context, member: discord.Member):
        """Manually remove verification from a user (helper called by botsheild removeverify)."""
        guild = ctx.guild
        if guild is None:
            try:
                await ctx.send(embed=discord.Embed(description="This command must be used in a server.", color=discord.Color.red()))
            except Exception:
                pass
            return
        users = self._load_users()
        gid = str(guild.id)
        if gid in users and str(member.id) in users[gid]:
            users[gid][str(member.id)]["verified"] = False
            users[gid][str(member.id)]["progress"] = 0
            try:
                self._save_users(users)
            except Exception:
                pass
            embed = discord.Embed(title="Verification Updated", color=discord.Color.orange())
            embed.description = f"Verification removed for {member.mention}."
            await ctx.send(embed=embed)
        else:
            await ctx.send(embed=discord.Embed(description=f"No verification record found for {member.mention}.", color=discord.Color.yellow()))
