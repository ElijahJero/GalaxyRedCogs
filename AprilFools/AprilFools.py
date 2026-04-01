from redbot.core import commands
from redbot.core.bot import Red
import discord
import io


class AprilFools(commands.Cog):
    """April Fools prank cog — deploys a fake scam giveaway message. 🎭"""

    def __init__(self, bot: Red):
        self.bot = bot

    @commands.command(name="fool")
    @commands.is_owner()
    @commands.dm_only()
    async def fool(
        self,
        ctx: commands.Context,
        rickroll: str,
        channel_ids: commands.Greedy[int],
    ):
        """
        Send a fake scam giveaway message to one or more channels. Owner only, DMs only.

        Usage: [p]fool <rickroll-url> <#channel1> [#channel2] ...

        The rickroll URL is hidden behind friendly link text so victims don't see it coming.
        """
        if not channel_ids:
            await ctx.send(
                "❌ You need to provide at least one channel ID.\n"
                "Usage: `[p]fool <rickroll-url> <channel_id1> [channel_id2] ...`\n"
                "Tip: right-click a channel and choose **Copy ID** (enable Developer Mode in settings)."
            )
            return

        # The prank message — @evreyone is intentionally misspelled so it doesn't actually ping.
        # Masked hyperlinks ([text](url)) only render inside Discord embeds, not plain text.
        # Using an embed also prevents Discord from auto-generating a link preview.
        embed = discord.Embed(
            description=(
                "🚨 **FREE MACBOOK GIVEAWAY — ACT FAST** 🚨\n\n"
                "@evreyone — so I just got a brand new **MacBook Pro Max Ultra Elite Plus** "
                "from my parents for Christmas and I still have my perfectly good "
                "**MacBook Pro Max Ultra Elite** from last year just sitting in a box doing nothing. "
                "I'd rather give it to someone who actually needs it than watch it collect dust.\n"
                "Comes with the original box, charger, and all accessories included.\n\n"
                "📋 **How to claim it:**\n"
                "Be the **first person** to visit my site and fill out the shipping form. "
                "It's strictly first come, first served. "
                f"👉 **[🎁 Claim Your Free MacBook Right Here 🎁]({rickroll})** 👈\n\n"
                "⏰ Once someone claims it I'm taking the page down."
            ),
        )

        sent = []
        failed = []

        # Read attachment data up front; recreate File objects per channel since they are consumed on send
        attachment_data = []
        for att in ctx.message.attachments:
            if att.content_type and att.content_type.startswith("image/"):
                data = await att.read()
                attachment_data.append((data, att.filename))

        for cid in channel_ids:
            channel = self.bot.get_channel(cid)
            if channel is None:
                failed.append(f"`{cid}` (channel not found — bot may not be in that server)")
                continue
            if not isinstance(channel, discord.TextChannel):
                failed.append(f"`{cid}` (not a text channel)")
                continue
            try:
                files = [discord.File(io.BytesIO(data), filename=fn) for data, fn in attachment_data]
                await channel.send(embed=embed, files=files)
                sent.append(f"#{channel.name} (`{cid}`)")
            except discord.Forbidden:
                failed.append(f"#{channel.name} (`{cid}`) (missing permissions)")
            except Exception as e:
                failed.append(f"#{channel.name} (`{cid}`) ({e})")


        # Report back to the owner in DMs
        lines = ["**🎭 April Fools prank deployed!**"]
        if sent:
            lines.append(f"✅ Sent to: {', '.join(sent)}")
        if failed:
            lines.append(f"❌ Failed: {', '.join(failed)}")

        await ctx.send("\n".join(lines))