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

class BotSheild(commands.Cog):
    """A cog that provides bot protection features."""

    def __init__(self, bot: Red):
        self.bot = bot
        self.config = Config.get_conf(self, identifier=1234567890)
        default_global = {
            # map guild_id -> {"captcha_count": int, "auto_verify_days": int, "setup_time": int}
            "protected_servers": {},
            "alert_role": None,
        }
        self.config.register_global(**default_global)

    @commands.command()
    @commands.guild_only()
    @commands.has_permissions(manage_guild=True)
    async def protect_server(self, ctx: commands.Context, captcha_count: int = 1, auto_verify_days: int = -1, log_channel: Optional[discord.TextChannel] = None):
        """
        Register (or update) the current server as protected.
        captcha_count: how many successful captchas required to become verified (>=1)
        auto_verify_days: -1 = verify nobody; 0 = verify everyone already in server at setup; >0 = verify members who joined at least this many days ago (at setup time).
        log_channel: optional text channel to receive admin notifications about captcha events.
        New members after setup are never automatically verified.
        """
        if ctx.guild is None:
            await ctx.send(embed=discord.Embed(description="This command must be used in a server.", color=discord.Color.red()))
            return

        if captcha_count < 1:
            await ctx.send(embed=discord.Embed(description="Captcha amount must be at least 1.", color=discord.Color.red()))
            return

        guild_id = str(ctx.guild.id)
        protected = await self.config.protected_servers()
        setup_time = int(time.time())

        protected[guild_id] = {
            "captcha_count": captcha_count,
            "auto_verify_days": auto_verify_days,
            "setup_time": setup_time,
            "log_channel_id": log_channel.id if log_channel is not None else None,
        }
        await self.config.protected_servers.set(protected)

        embed = discord.Embed(title="Server Protected", color=discord.Color.green())
        embed.add_field(name="Server", value=f"{ctx.guild.name} (ID: {ctx.guild.id})", inline=False)
        embed.add_field(name="Captchas required", value=str(captcha_count), inline=True)
        embed.add_field(name="Auto-verify rule (days)", value=str(auto_verify_days), inline=True)
        embed.add_field(name="Log channel", value=(log_channel.mention if log_channel else "None"), inline=False)
        await ctx.send(embed=embed)

        # Apply auto-verification to existing members according to rule
        users_file = os.path.join(os.path.dirname(__file__), "users.json")
        try:
            with open(users_file, "r", encoding="utf-8") as f:
                users_data = json.load(f)
        except Exception:
            users_data = {}

        if guild_id not in users_data:
            users_data[guild_id] = {}

        if auto_verify_days != -1:
            now_ts = setup_time
            for member in ctx.guild.members:
                # skip bots and the bot itself
                if member.bot:
                    continue
                joined_at = member.joined_at
                if joined_at is None:
                    continue
                joined_ts = int(joined_at.replace(tzinfo=timezone.utc).timestamp())
                if auto_verify_days == 0:
                    # verify everyone who was present at setup (joined before or at setup_time)
                    if joined_ts <= now_ts:
                        users_data[guild_id][str(member.id)] = {"verified": True, "progress": 0}
                elif auto_verify_days > 0:
                    # verify members who joined at least auto_verify_days ago (relative to setup_time)
                    required_seconds = auto_verify_days * 86400
                    if now_ts - joined_ts >= required_seconds:
                        users_data[guild_id][str(member.id)] = {"verified": True, "progress": 0}
        # write back users_data
        try:
            with open(users_file, "w", encoding="utf-8") as f:
                json.dump(users_data, f, indent=2)
        except Exception:
            pass

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

        protected_servers = await self.config.protected_servers()
        guild_id = str(message.guild.id)
        if guild_id not in protected_servers:
            return

        # Load verification status from users.json
        users_data = self._load_users()
        if guild_id not in users_data:
            users_data[guild_id] = {}

        member_id = str(message.author.id)
        member_record = users_data[guild_id].get(member_id, {"verified": False, "progress": 0})
        verified = bool(member_record.get("verified", False))
        if verified:
            return

        # If for some reason the person was auto-verified at setup (but file not updated), handle now:
        # (we already applied auto-verify on protect_server; keep this as safeguard)
        guild_conf = protected_servers.get(guild_id, {})
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
                        e.set_footer(text=f"Time: {datetime.utcnow().isoformat()}Z")
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
                        e.set_footer(text=f"Time: {datetime.utcnow().isoformat()}Z")
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
                        e.set_footer(text=f"Time: {datetime.utcnow().isoformat()}Z")
                        await log_channel.send(embed=e)
                    except Exception:
                        pass
        finally:
            # ensure file saved
            self._save_users(users)

    @commands.command()
    @commands.guild_only()
    @commands.has_permissions(manage_guild=True)
    async def unprotect_server(self, ctx: commands.Context):
        """
        Remove the current server from protection.
        """
        if ctx.guild is None:
            await ctx.send(embed=discord.Embed(description="This command must be used in a server.", color=discord.Color.red()))
            return

        guild_id = str(ctx.guild.id)
        protected = await self.config.protected_servers()
        if guild_id in protected:
            del protected[guild_id]
            await self.config.protected_servers.set(protected)
            embed = discord.Embed(title="Server Unprotected", description=f"Protection removed for **{ctx.guild.name}**.", color=discord.Color.orange())
            await ctx.send(embed=embed)
        else:
            await ctx.send(embed=discord.Embed(description="This server is not protected.", color=discord.Color.yellow()))

    @commands.command()
    @commands.guild_only()
    @commands.has_permissions(manage_guild=True)
    async def addverify(self, ctx: commands.Context, member: discord.Member):
        """
        Manually mark a user as verified.
        """
        guild = ctx.guild
        if guild is None:
            await ctx.send(embed=discord.Embed(description="This command must be used in a server.", color=discord.Color.red()))
            return
        users = self._load_users()
        gid = str(guild.id)
        if gid not in users:
            users[gid] = {}
        users[gid][str(member.id)] = {"verified": True, "progress": 0}
        self._save_users(users)
        await ctx.send(embed=discord.Embed(description=f"{member.mention} has been marked as verified.", color=discord.Color.green()))

    @commands.command()
    @commands.guild_only()
    @commands.has_permissions(manage_guild=True)
    async def removeverify(self, ctx: commands.Context, member: discord.Member):
        """
        Manually remove verification from a user.
        """
        guild = ctx.guild
        if guild is None:
            await ctx.send(embed=discord.Embed(description="This command must be used in a server.", color=discord.Color.red()))
            return
        users = self._load_users()
        gid = str(guild.id)
        if gid in users and str(member.id) in users[gid]:
            users[gid][str(member.id)]["verified"] = False
            users[gid][str(member.id)]["progress"] = 0
            self._save_users(users)
            await ctx.send(embed=discord.Embed(description=f"Verification removed for {member.mention}.", color=discord.Color.orange()))
        else:
            await ctx.send(embed=discord.Embed(description=f"No verification record found for {member.mention}.", color=discord.Color.yellow()))
