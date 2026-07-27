import random
import asyncio
import time
from datetime import datetime
import discord
from .emoji_data import ALL_EMOJIS, COLOR_EMOJIS

number_emojis = ["0️⃣","1️⃣","2️⃣","3️⃣","4️⃣","5️⃣","6️⃣","7️⃣","8️⃣","9️⃣"]

def generate_captcha():
    # Generate a random target sum 0-9 and split into two numbers
    target_sum = random.randint(0, 9)
    number_a = random.randint(0, target_sum)
    number_b = target_sum - number_a
    return number_a, number_b

def generate_color_captcha():
    """
    Generate a color-based captcha.
    Returns: (target_color_name, correct_emoji, [all_emoji_choices])
    """
    # Choose a random color
    color_name = random.choice(list(COLOR_EMOJIS.keys()))
    color_emojis = COLOR_EMOJIS[color_name]
    
    # Select one emoji from the target color
    correct_emoji = random.choice(color_emojis)
    
    # Collect all emojis from other colors
    other_colors = [c for c in COLOR_EMOJIS.keys() if c != color_name]
    all_wrong_candidates = []
    for other_color in other_colors:
        all_wrong_candidates.extend(COLOR_EMOJIS[other_color])
    
    # Remove the correct emoji if it appears in other colors
    all_wrong_candidates = [e for e in all_wrong_candidates if e != correct_emoji]
    
    # Remove duplicates
    all_wrong_candidates = list(set(all_wrong_candidates))
    
    # Randomly select 3 unique wrong emojis
    if len(all_wrong_candidates) >= 3:
        wrong_emojis = random.sample(all_wrong_candidates, 3)
    else:
        # Fallback if not enough candidates (shouldn't happen)
        wrong_emojis = all_wrong_candidates
    
    # Combine and shuffle
    all_choices = [correct_emoji] + wrong_emojis
    random.shuffle(all_choices)
    
    return color_name, correct_emoji, all_choices

def generate_sequence_captcha():
    """
    Generate a sequence difference captcha.
    Returns: (sequence, different_position, different_emoji, [answer_choices])
    """
    # Generate a sequence of 4 random emojis
    sequence = random.sample(ALL_EMOJIS, 4)
    
    # Choose which position will be different (0-3)
    different_position = random.randint(0, 3)
    
    # Choose a different emoji for that position
    different_emoji = random.choice([e for e in ALL_EMOJIS if e not in sequence])
    
    # Create the answer sequence (same as original but with one different)
    answer_sequence = sequence.copy()
    answer_sequence[different_position] = different_emoji
    
    return sequence, different_position, different_emoji, answer_sequence

async def handle_captcha_challenge(cog, message: discord.Message, guild_conf: dict):
    """
    Send a captcha message, add reactions, and wait for the member to react with the correct answer.
    Randomly selects between three captcha variants:
    1. Math (sum of two numbers)
    2. Color (pick the emoji of a specific color)
    3. Sequence (identify which emoji is different)
    
    On success, increment progress and mark verified when reaching required count.
    On failure or timeout, delete original message and captcha message.
    Logs events to configured logging channel if set.
    """
    member = message.author
    channel = message.channel
    
    # Randomly choose captcha type
    captcha_type = random.choice(['math', 'color', 'sequence'])
    
    captcha_embed = discord.Embed(title="Captcha Verification", color=discord.Color.blurple())
    correct_answer = None
    answer_choices = []
    
    if captcha_type == 'math':
        # Original math captcha
        number_a, number_b = generate_captcha()
        correct_sum = number_a + number_b
        correct_answer = number_emojis[correct_sum]
        
        # Prepare emoji choices: correct + 3 unique wrongs
        all_digits = list(range(0, 10))
        wrong_choices = [d for d in all_digits if d != correct_sum]
        random.shuffle(wrong_choices)
        choices = [correct_sum] + wrong_choices[:3]
        random.shuffle(choices)
        answer_choices = [number_emojis[num] for num in choices]
        
        captcha_embed.description = f"Please react with the sum of **{number_a} and {number_b}**.\nYou have 60 seconds."
    
    elif captcha_type == 'color':
        # Color-based captcha
        color_name, correct_emoji, emoji_choices = generate_color_captcha()
        correct_answer = correct_emoji
        answer_choices = emoji_choices
        
        captcha_embed.description = f"Please react with the **{color_name}** emoji.\nYou have 60 seconds."
    
    elif captcha_type == 'sequence':
        # Sequence difference captcha
        original_sequence, diff_pos, correct_emoji, answer_sequence = generate_sequence_captcha()
        correct_answer = correct_emoji
        answer_choices = answer_sequence
        
        # Show the original sequence in the embed
        sequence_display = " ".join(original_sequence)
        captcha_embed.description = f"**Original sequence:**\n{sequence_display}\n\nReact with the emoji that is **different** in the sequence below.\nYou have 60 seconds."
    
    # Send captcha message
    try:
        captcha_msg = await channel.send(content=member.mention, embed=captcha_embed)
    except Exception:
        return

    # Add reactions corresponding to answer choices
    # Ensure we have exactly 4 unique emojis
    unique_choices = []
    seen = set()
    for emoji in answer_choices:
        if emoji not in seen:
            seen.add(emoji)
            unique_choices.append(emoji)
    
    # If we somehow don't have 4 unique choices, log it but continue
    if len(unique_choices) < 4:
        print(f"WARNING: Only {len(unique_choices)} unique emojis for {captcha_type} captcha: {unique_choices}")
    
    for emoji in unique_choices:
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
        reacted_emoji = str(reaction.emoji)
        
        if reacted_emoji == correct_answer:
            successful = True
            break
        else:
            successful = False
            fail_reason = f"incorrect_answer:{reacted_emoji}"
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
                if captcha_type == 'math':
                    reason_text = f"Incorrect answer selected ({chosen}). Expected: {correct_answer}."
                elif captcha_type == 'color':
                    reason_text = f"Incorrect emoji selected. Expected a {color_name} emoji ({correct_answer})."
                elif captcha_type == 'sequence':
                    reason_text = f"Incorrect emoji selected. Expected: {correct_answer} (the different one)."
                else:
                    reason_text = f"Incorrect answer. Expected: {correct_answer}."
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
                    e.add_field(name="Captcha Type", value=captcha_type.capitalize(), inline=False)
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
                    e.add_field(name="Captcha Type", value=captcha_type.capitalize(), inline=False)
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
                    e.add_field(name="Captcha Type", value=captcha_type.capitalize(), inline=False)
                    e.add_field(name="Time taken", value=f"{elapsed2:.2f}s{suspicious_text}", inline=False)
                    e.add_field(name="Progress", value=f"{current_progress}/{required}", inline=False)
                    e.set_footer(text=f"Time: {datetime.utcnow().isoformat()}Z")
                    await log_channel.send(embed=e)
                except Exception:
                    pass
    finally:
        # ensure file saved
        cog._save_users(users)

