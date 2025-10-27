import random
import asyncio
import time
from datetime import datetime
import discord

number_emojis = ["0️⃣","1️⃣","2️⃣","3️⃣","4️⃣","5️⃣","6️⃣","7️⃣","8️⃣","9️⃣"]

def generate_captcha():
    # Generate a random target sum 0-9 and split into two numbers
    target_sum = random.randint(0, 9)
    number_a = random.randint(0, target_sum)
    number_b = target_sum - number_a
    return number_a, number_b

async def handle_captcha_challenge(cog, message: discord.Message, guild_conf: dict):
    """
    Send a captcha message, add reactions, and wait for the member to react with the correct sum.
    On success, increment progress and mark verified when reaching required count.
    On failure or timeout, delete original message and captcha message.
    Logs events to configured logging channel if set.
    """
    member = message.author
    channel = message.channel
    number_a, number_b = generate_captcha()
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
        emoji = number_emojis[num]
        try:
            await captcha_msg.add_reaction(emoji)
        except Exception:
            pass

    # wait for reactions
    deadline = time.time() + 60
    successful = False
    fail_reason = None
    start_time = time.time()

    while True:
        timeout = deadline - time.time()
        if timeout <= 0:
            fail_reason = "timeout"
            break
        try:
            reaction, user = await cog.bot.wait_for(
                "reaction_add",
                check=lambda r, u: r.message.id == captcha_msg.id,
                timeout=timeout,
            )
        except asyncio.TimeoutError:
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
        try:
            reacted_digit = number_emojis.index(reacted_emoji)
        except ValueError:
            reacted_digit = None

        if reacted_digit is None:
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

    users = cog._load_users()
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
            log_channel = cog.bot.get_channel(int(log_channel_id))
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

            # Construct friendly reason text
            if fail_reason is None:
                fail_reason = "unknown"
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
            cog._save_users(users)
            return

        # success path: compute elapsed, increment progress and possibly verify
        elapsed = time.time() - start_time
        required = int(guild_conf.get("captcha_count", 1))
        current_progress = int(member_record.get("progress", 0))
        current_progress += 1
        member_record["progress"] = current_progress
        if current_progress >= required:
            member_record["verified"] = True
            member_record["progress"] = 0
            users[guild_id][member_id] = member_record
            cog._save_users(users)
            try:
                await captcha_msg.delete()
            except Exception:
                pass
            # send verification success then delete after 10s
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
            users[guild_id][member_id] = member_record
            cog._save_users(users)
            try:
                await captcha_msg.delete()
            except Exception:
                pass
            # Inform user briefly, then delete
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
            # log progress to admin channel
            if log_channel is not None:
                try:
                    elapsed2 = time.time() - start_time
                    suspicious_text = " (suspiciously fast)" if elapsed2 < 2.0 else ""
                    e = discord.Embed(title="Captcha Completed (Progress)", color=discord.Color.green())
                    e.add_field(name="User", value=f"{member} (ID: {member.id})", inline=False)
                    e.add_field(name="Channel", value=f"#{channel.name} (ID: {channel.id})", inline=False)
                    e.add_field(name="Time taken", value=f"{elapsed2:.2f}s{suspicious_text}", inline=False)
                    e.add_field(name="Progress", value=f"{current_progress}/{required}", inline=False)
                    e.set_footer(text=f"Time: {datetime.utcnow().isoformat()}Z")
                    await log_channel.send(embed=e)
                except Exception:
                    pass
    finally:
        # ensure file saved
        cog._save_users(users)

