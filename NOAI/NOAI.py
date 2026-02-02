import asyncio
import discord
from redbot.core import Config
from redbot.core import commands
from redbot.core.bot import Red

from .ImageTools import extract_images_from_message, value_to_hex
from .CLIPUtils import analyze_image_smart


class NOAI(commands.Cog):
    def __init__(self, bot: Red):
        self.bot = bot
        self.config = Config.get_conf(self, identifier=939355244643366179)

        default_global = {"max_image_size": 50}
        default_guild = {
            "images_enabled": False,
            "text_enabled": False,
            "image_threshold": 60
        }

        self.config.register_guild(**default_guild)
        self.config.register_global(**default_global)

        self._cache = {}

    @commands.group(name="noai", invoke_without_command=True)
    async def noai(self, ctx: commands.Context):
        await ctx.send_help()

    @noai.command(name="enableimages")
    @commands.admin_or_permissions(manage_guild=True)
    @commands.guild_only()
    async def enable_images_noai(self, ctx: commands.Context, enable: bool):
        await self.config.guild(ctx.guild).images_enabled.set(enable)
        await ctx.send(f"NOAI image analysis has been {'enabled' if enable else 'disabled'} in this server.")

    @noai.command(name="enabletext")
    @commands.admin_or_permissions(manage_guild=True)
    @commands.guild_only()
    async def enable_text_noai(self, ctx: commands.Context, enable: bool):
        await self.config.guild(ctx.guild).text_enabled.set(enable)
        await ctx.send(
            f"NOAI text analysis has been {'enabled' if enable else 'disabled'} in this server.\n"
            "However this feature has not been implemented yet."
        )

    @noai.command(name="setimagethreshold")
    @commands.admin_or_permissions(manage_guild=True)
    @commands.guild_only()
    async def set_image_threshold_noai(self, ctx: commands.Context, threshold: int):
        await self.config.guild(ctx.guild).image_threshold.set(threshold)
        await ctx.send(f"NOAI image analysis threshold has been set to {threshold}%.")

    @noai.command(name="setmaximagesize")
    @commands.is_owner()
    async def set_max_image_size_noai(self, ctx: commands.Context, size_mb: int):
        await self.config.max_image_size.set(size_mb)
        await ctx.send(f"NOAI maximum image size has been set to {size_mb} MB.")

    @noai.command(name="analyze")
    async def analyze_image_noai(self, ctx: commands.Context):
        max_image_size_mb = await self.config.max_image_size()
        source_msg = ctx.message

        await ctx.typing()
        images = await extract_images_from_message(self, source_msg, max_image_size_mb)

        if not images and ctx.message.reference:
            ref = ctx.message.reference
            try:
                if isinstance(ref.resolved, discord.Message):
                    target_msg = ref.resolved
                else:
                    target_msg = await ctx.channel.fetch_message(ref.message_id)
            except Exception:
                target_msg = None

            if target_msg:
                images = await extract_images_from_message(self, target_msg, max_image_size_mb)

        if not images:
            await ctx.send("No images could be found or loaded.")
            return

        for img in images:
            filename = img.get("filename") or img.get("url") or "image"
            try:
                report = await asyncio.wait_for(
                    analyze_image_smart(self, img["bytes"], filename, img.get("url") or "", ctx),
                    timeout=30
                )

                if not report.ok:
                    await ctx.send(f"Failed to analyze image '{filename}': {report.error}")
                    continue

                # Color always derived from ai_likelihood now (always 0..100)
                color_int = value_to_hex(report.ai_likelihood)

                lines = []
                lines.append(f"**AI likelihood:** {report.ai_likelihood}%")
                lines.append(f"**Verdict:** {report.verdict}")
                lines.append(f"**Certainty:** {report.certainty}")
                lines.append(f"**Detected type:** {report.image_type} ({int(report.image_type_confidence * 100)}% confidence)")

                if report.warning:
                    lines.append(f"\n{report.warning}")

                # Light explanation (keep it short in Discord)
                scoring = report.details.get("scoring", {})
                margin = scoring.get("margin")
                if margin is not None:
                    lines.append(f"\n**Signal margin:** {float(margin):.3f} (AI-sim − Human-sim)")

                top_ai = scoring.get("top_ai_prompt", {}).get("prompt")
                top_real = scoring.get("top_real_prompt", {}).get("prompt")
                if top_ai:
                    lines.append(f"**Top AI prompt:** {top_ai}")
                if top_real:
                    lines.append(f"**Top Human prompt:** {top_real}")

                embed = discord.Embed(
                    title="AI Image Analysis (Smart)",
                    description="\n".join(lines),
                    color=discord.Color(color_int),
                )
                embed.set_footer(
                    text="This is a best-effort guess. Non-photo types are significantly less reliable."
                )

                if img.get("url"):
                    embed.set_thumbnail(url=img["url"])
                    await ctx.send(embed=embed)
                else:
                    import io
                    file_obj = discord.File(io.BytesIO(img["bytes"]), filename=filename)
                    embed.set_thumbnail(url=f"attachment://{filename}")
                    await ctx.send(embed=embed, file=file_obj)

            except asyncio.TimeoutError:
                await ctx.send(f"Image '{filename}' analysis timed out after 30 seconds.")
            except Exception as e:
                await ctx.send(f"Error analyzing image '{filename}': {e}")
            finally:
                img["bytes"] = None

        images.clear()