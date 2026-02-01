import discord
from redbot.core import Config
from redbot.core import commands
from redbot.core.bot import Red
from .ImageTools import extract_images_from_message
from .CLIPUtils import analize_image

class NOAI(commands.Cog):

    def __init__(self, bot: Red):
        # store bot reference
        self.bot = bot

        # Config for this cog - use a large unique identifier
        # (change the number if you fork this cog to avoid conflicts)
        self.config = Config.get_conf(self, identifier=939355244643366179)

        # default settings
        default_global = {
            "max_image_size": 50                # size in MB
        }

        default_guild = {
            "images_enabled": False,            # whether to analyze images sent in the guild
            "text_enabled": False,              # whether to analyze text sent in the guild
            "image_threshold": 60               # threshold to give AI alert for images
        }

        # register defaults
        self.config.register_guild(**default_guild)
        self.config.register_global(**default_global)

        # runtime-only cache (not persisted)
        self._cache = {}

    @commands.group(name="noai", invoke_without_command=True)
    async def noai(self, ctx: commands.Context):
        """Base command for NOAI cog."""
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
        await ctx.send(f"NOAI text analysis has been {'enabled' if enable else 'disabled'} in this server.\nHowever this feature has not been implemented yet.")

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
        """
        Analyze 1 or more images provided in the message.
        Usage:
            [p]noai analyze
        The command inspects:
          - attachments in the message
          - embed.image / embed.thumbnail in the message
          - plain image URLs in the message content
        If you reply to a message and that message contains images, it will analyze that message instead.
        """

        max_image_size_mb = await self.config.max_image_size()
        source_msg = ctx.message
        images = await extract_images_from_message(self, source_msg, max_image_size_mb)

        if not images and ctx.message.reference:
            ref = ctx.message.reference
            try:
                # If resolved is already a Message object (cached), use it; else fetch
                if isinstance(ref.resolved, discord.Message):
                    target_msg = ref.resolved
                else:
                    channel = ctx.channel
                    # we use the same channel as invocation unless the reference includes channel info
                    target_msg = await channel.fetch_message(ref.message_id)
            except Exception:
                target_msg = None

            if target_msg:
                images = await extract_images_from_message(self, target_msg, max_image_size_mb)

        if not images:
            await ctx.send("No images could be found or loaded.")
            return

        await ctx.send(f"Found {len(images)} image(s) to analyze. Analyzing...")

        for img in images:
            filename = img.get('filename') or img.get('url') or "image"
            try:
                result = await analize_image(self, img['bytes'], filename, img['url'], ctx)
                if result == -1:
                    await ctx.send(f"Failed to analyze image '{filename}'.")
                else:
                    await ctx.send(f"Image '{filename}' AI likelihood: {result}%.")
            except Exception as e:
                await ctx.send(f"Error analyzing image '{filename}': {e}")
