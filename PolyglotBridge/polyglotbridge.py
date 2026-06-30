import asyncio
import logging
import re
import typing as t
from contextlib import suppress

import discord
from aiocache import cached
from discord import app_commands
from redbot.core import Config, commands
from redbot.core.bot import Red
from redbot.core.utils.chat_formatting import pagify
from redbot.core.utils.views import SetApiView

from .common import api, constants

log = logging.getLogger("red.galaxy.polyglotbridge")

BRIDGE_NAME_PATTERN = re.compile(r"^[a-z0-9][a-z0-9\-_]{0,31}$")


class PolyglotBridge(commands.Cog):
    """
    Cross-channel translation bridges for multilingual Discord communities.

    Link multiple language-specific channels into a bridge. When someone posts
    in one channel, their message is automatically translated and relayed to every
    other channel in the bridge.

    Translation API logic is adapted from [Fluent](https://github.com/vertyco/vrt-cogs)
    by Vertyco (MIT License).

    Uses Google Translate by default, with Flowery as a fallback. Optional OpenAI
    and DeepL keys improve quality — set them with `[p]polyglot openai` and
    `[p]polyglot deepl`.
    """

    __author__ = "Elijah Jero (translation API adapted from [Vertyco](https://github.com/vertyco/vrt-cogs))"
    __version__ = "1.0.0"

    def format_help_for_context(self, ctx: commands.Context):
        helpcmd = super().format_help_for_context(ctx)
        return f"{helpcmd}\nCog Version: {self.__version__}\nAuthor: {self.__author__}"

    async def red_delete_data_for_user(self, *, requester, user_id: int):
        """No data to delete"""

    def __init__(self, bot: Red):
        self.bot = bot
        self.config = Config.get_conf(self, identifier=88294731)
        self.config.register_guild(bridges={})
        logging.getLogger("hpack.hpack").setLevel(logging.INFO)
        logging.getLogger("deepl").setLevel(logging.WARNING)
        logging.getLogger("aiocache").setLevel(logging.WARNING)
        logging.getLogger("urllib3").setLevel(logging.WARNING)
        logging.getLogger("httpcore").setLevel(logging.WARNING)
        logging.getLogger("openai").setLevel(logging.WARNING)

    @staticmethod
    def _slugify(name: str) -> str:
        slug = re.sub(r"[^a-z0-9\-_]+", "-", name.strip().lower())
        slug = slug.strip("-")
        return slug[:32]

    @cached(ttl=10)
    async def get_bridges(self, guild: discord.Guild) -> dict:
        return await self.config.guild(guild).bridges()

    async def _get_channel_map(self, guild: discord.Guild) -> dict[str, tuple[str, str, dict]]:
        """Map channel_id -> (bridge_id, channel_language, bridge_data)."""
        bridges = await self.get_bridges(guild)
        mapping: dict[str, tuple[str, str, dict]] = {}
        for bridge_id, bridge in bridges.items():
            for channel_id, language in bridge.get("channels", {}).items():
                mapping[channel_id] = (bridge_id, language, bridge)
        return mapping

    async def _find_bridge_for_channel(
        self, guild: discord.Guild, channel_id: int
    ) -> t.Optional[tuple[str, str, dict]]:
        return (await self._get_channel_map(guild)).get(str(channel_id))

    async def _channel_in_any_bridge(self, guild: discord.Guild, channel_id: int) -> t.Optional[str]:
        entry = await self._find_bridge_for_channel(guild, channel_id)
        return entry[0] if entry else None

    @cached(ttl=900)
    async def translate(self, msg: str, dest: str, force: bool = False) -> t.Optional[api.Result]:
        deepl_key = await self.bot.get_shared_api_tokens("polyglotbridge_deepl")
        openai_key = await self.bot.get_shared_api_tokens("polyglotbridge_openai")
        translator = api.TranslateManager(
            deepl_key=deepl_key.get("key"),
            openai_key=openai_key.get("key"),
        )
        return await translator.translate(msg, dest, force=force)

    @commands.hybrid_command(name="translate")
    @app_commands.describe(to_language="Translate to this language")
    @commands.bot_has_permissions(embed_links=True)
    async def translate_command(self, ctx: commands.Context, to_language: str, *, message: t.Optional[str] = None):
        """Manually translate a message to another language."""
        if ctx.interaction is not None:
            await ctx.interaction.response.defer()

        translator = api.TranslateManager()
        lang = await translator.get_lang(to_language)
        if not lang:
            return await ctx.send(f"The target language `{to_language}` was not found.")

        if not message and hasattr(ctx.message, "reference"):
            with suppress(AttributeError):
                resolved = ctx.message.reference.resolved
                if isinstance(resolved, discord.Message):
                    message = resolved.content
        if not message:
            return await ctx.send("Could not find any content to translate!")

        try:
            trans = await self.translate(message, to_language)
        except Exception as e:
            await ctx.send("An error occurred while translating. Check logs for more info.")
            log.error("Translation failed", exc_info=e)
            setattr(self.bot, "_last_exception", e)
            return

        if trans is None:
            return await ctx.send("Translation failed.")

        embed = discord.Embed(description=trans.text, color=ctx.author.color)
        embed.set_footer(text=f"{trans.src} -> {trans.dest}")
        with suppress(discord.NotFound, AttributeError):
            return await ctx.reply(embed=embed, mention_author=False)
        await ctx.send(embed=embed)

    @translate_command.autocomplete("to_language")
    async def translate_autocomplete(self, interaction: discord.Interaction, current: str):
        return await self.get_langs(current)

    @cached(ttl=60)
    async def get_langs(self, current: str):
        return [
            app_commands.Choice(name=i["name"], value=i["name"])
            for i in constants.available_langs
            if current.lower() in i["name"].lower()
        ][:25]

    @commands.group(name="polyglot", aliases=["polyglotbridge", "pb"])
    @commands.mod_or_permissions(manage_channels=True)
    async def polyglot(self, ctx: commands.Context):
        """Manage cross-channel translation bridges."""

    @polyglot.command(name="create")
    async def polyglot_create(self, ctx: commands.Context, *, name: str):
        """Create a new translation bridge."""
        bridge_id = self._slugify(name)
        if not bridge_id or not BRIDGE_NAME_PATTERN.match(bridge_id):
            return await ctx.send(
                "Invalid bridge name. Use letters, numbers, hyphens, or underscores (max 32 characters)."
            )

        async with self.config.guild(ctx.guild).bridges() as bridges:
            if bridge_id in bridges:
                return await ctx.send(f"A bridge named `{bridge_id}` already exists.")
            bridges[bridge_id] = {"name": name.strip(), "channels": {}}

        await ctx.send(f"Created bridge **{name.strip()}** (`{bridge_id}`). Add channels with `{ctx.prefix}polyglot addchannel`.")

    @polyglot.command(name="addchannel")
    @commands.bot_has_permissions(send_messages=True, embed_links=True)
    async def polyglot_addchannel(
        self,
        ctx: commands.Context,
        bridge_id: str,
        channel: discord.TextChannel,
        language: str,
    ):
        """Add a channel and its language to a bridge."""
        bridge_id = bridge_id.lower()
        translator = api.TranslateManager()
        lang = await translator.get_lang(language)
        if not lang:
            return await ctx.send(f"Language `{language}` is not recognized.")

        existing_bridge = await self._channel_in_any_bridge(ctx.guild, channel.id)
        if existing_bridge:
            return await ctx.send(f"{channel.mention} is already in bridge `{existing_bridge}`.")

        async with self.config.guild(ctx.guild).bridges() as bridges:
            if bridge_id not in bridges:
                return await ctx.send(f"No bridge named `{bridge_id}`. Create one with `{ctx.prefix}polyglot create`.")
            cid = str(channel.id)
            if cid in bridges[bridge_id]["channels"]:
                return await ctx.send(f"{channel.mention} is already in this bridge.")
            bridges[bridge_id]["channels"][cid] = language

        await ctx.send(f"Added {channel.mention} as **{language}** to bridge `{bridge_id}`.")

    @polyglot.command(name="removechannel")
    async def polyglot_removechannel(
        self,
        ctx: commands.Context,
        bridge_id: str,
        channel: discord.TextChannel,
    ):
        """Remove a channel from a bridge."""
        bridge_id = bridge_id.lower()
        async with self.config.guild(ctx.guild).bridges() as bridges:
            if bridge_id not in bridges:
                return await ctx.send(f"No bridge named `{bridge_id}`.")
            cid = str(channel.id)
            if cid not in bridges[bridge_id]["channels"]:
                return await ctx.send(f"{channel.mention} is not in bridge `{bridge_id}`.")
            del bridges[bridge_id]["channels"][cid]

        await ctx.send(f"Removed {channel.mention} from bridge `{bridge_id}`.")

    @polyglot.command(name="delete", aliases=["remove", "del", "rem"])
    async def polyglot_delete(self, ctx: commands.Context, bridge_id: str):
        """Delete an entire translation bridge."""
        bridge_id = bridge_id.lower()
        async with self.config.guild(ctx.guild).bridges() as bridges:
            if bridge_id not in bridges:
                return await ctx.send(f"No bridge named `{bridge_id}`.")
            del bridges[bridge_id]

        await ctx.send(f"Deleted bridge `{bridge_id}`.")

    @polyglot.command(name="view")
    async def polyglot_view(self, ctx: commands.Context):
        """View all translation bridges and their channels."""
        bridges = await self.get_bridges(ctx.guild)
        if not bridges:
            return await ctx.send("There are no translation bridges configured.")

        msg = "**PolyglotBridge Settings**\n"
        for bridge_id, bridge in bridges.items():
            msg += f"\n**{bridge.get('name', bridge_id)}** (`{bridge_id}`)\n"
            channels = bridge.get("channels", {})
            if not channels:
                msg += "  _(no channels yet)_\n"
                continue
            for cid, language in channels.items():
                channel = ctx.guild.get_channel(int(cid))
                channel_ref = channel.mention if channel else f"#{cid} (deleted)"
                msg += f"  {channel_ref} → {language}\n"

        for page in pagify(msg, page_length=1000):
            await ctx.send(page)

    @polyglot.command(name="openai")
    @commands.is_owner()
    async def polyglot_openai(self, ctx: commands.Context):
        """Set an OpenAI key for translations."""
        tokens = await self.bot.get_shared_api_tokens("polyglotbridge_openai")
        message = (
            "1. Go to [OpenAI](https://platform.openai.com/signup) and sign up for an account.\n"
            "2. Go to the [API keys](https://platform.openai.com/account/api-keys) page.\n"
            "3. Click the `+ Create new secret key` button to create a new API key.\n"
            "4. Copy the API key and click the button below to set it."
        )
        await ctx.send(
            message,
            view=SetApiView(
                default_service="polyglotbridge_openai",
                default_keys={"key": tokens.get("key", "")},
            ),
        )

    @polyglot.command(name="deepl")
    @commands.is_owner()
    async def polyglot_deepl(self, ctx: commands.Context):
        """Set a DeepL key for translations."""
        tokens = await self.bot.get_shared_api_tokens("polyglotbridge_deepl")
        message = (
            "1. Go to [DeepL](https://www.deepl.com/pro#developer) and sign up for an account.\n"
            "2. Go to the [API keys](https://www.deepl.com/en/your-account/keys) page.\n"
            "3. Copy the API key and click the button below to set it."
        )
        await ctx.send(
            message,
            view=SetApiView(
                default_service="polyglotbridge_deepl",
                default_keys={"key": tokens.get("key", "")},
            ),
        )

    def _get_translatable_content(self, content: str) -> t.Optional[str]:
        text = content.strip()
        text = constants.URL_PATTERN.sub("", text).strip()
        if not text:
            return None
        text = constants.CUSTOM_EMOJI_PATTERN.sub("", text).strip()
        if not text:
            return None
        text = constants.DISCORD_MENTION_PATTERN.sub("", text).strip()
        if not text:
            return None
        text = constants.CODE_BLOCK_PATTERN.sub("", text).strip()
        if not text:
            return None
        if not any(c.isalpha() for c in text):
            return None
        if len([c for c in text if c.isalpha()]) < 2:
            return None
        return text

    async def _relay_to_channel(
        self,
        message: discord.Message,
        target_channel: discord.abc.Messageable,
        target_lang: str,
        source_channel_name: str,
    ) -> None:
        clean_content = self._get_translatable_content(message.content)
        if not clean_content:
            return

        try:
            trans = await self.translate(clean_content, target_lang, force=True)
        except Exception as e:
            log.error("Bridge relay translation failed", exc_info=e)
            setattr(self.bot, "_last_exception", e)
            return

        if trans is None:
            log.debug("Bridge relay returned no translation for %s -> %s", message.channel.id, target_lang)
            return

        guild = message.guild
        me = guild.me if guild else None
        can_embed = (
            isinstance(target_channel, discord.abc.GuildChannel)
            and me is not None
            and target_channel.permissions_for(me).embed_links
        )

        if can_embed:
            embed = discord.Embed(description=trans.text[:4096], color=message.author.color, url=message.jump_url)
            embed.set_author(
                name=message.author.display_name,
                icon_url=message.author.display_avatar.url,
            )
            embed.set_footer(text=f"{trans.src} → {trans.dest} · from #{source_channel_name}")
            await target_channel.send(embed=embed)
        else:
            header = f"**{message.author.display_name}** (from #{source_channel_name}):\n"
            await target_channel.send(f"{header}{trans.text[:1900]}")

    @commands.Cog.listener("on_message_without_command")
    async def message_handler(self, message: discord.Message):
        if message.author.bot or not message.guild or not message.content or not message.content.strip():
            return

        clean_content = self._get_translatable_content(message.content)
        if not clean_content:
            return

        bridge_info = await self._find_bridge_for_channel(message.guild, message.channel.id)
        if bridge_info is None:
            return

        _bridge_id, _source_lang, bridge = bridge_info
        source_channel_name = getattr(message.channel, "name", "unknown")
        targets = [
            (cid, lang)
            for cid, lang in bridge.get("channels", {}).items()
            if cid != str(message.channel.id)
        ]
        if not targets:
            return

        async def relay(cid: str, lang: str) -> None:
            channel = message.guild.get_channel(int(cid))
            if channel is None:
                return
            if not isinstance(channel, discord.abc.Messageable):
                return
            if isinstance(channel, discord.abc.GuildChannel):
                perms = channel.permissions_for(message.guild.me)
                if not perms.send_messages:
                    log.warning("Missing send_messages in %s for bridge relay", cid)
                    return
            await self._relay_to_channel(message, channel, lang, source_channel_name)

        results = await asyncio.gather(*(relay(cid, lang) for cid, lang in targets), return_exceptions=True)
        for result in results:
            if isinstance(result, Exception):
                log.error("Bridge relay task failed", exc_info=result)

    @commands.Cog.listener()
    async def on_guild_channel_delete(self, channel: discord.abc.GuildChannel):
        if not channel.guild:
            return
        cid = str(channel.id)
        async with self.config.guild(channel.guild).bridges() as bridges:
            changed = False
            for bridge_id, bridge in list(bridges.items()):
                channels = bridge.get("channels", {})
                if cid in channels:
                    del channels[cid]
                    changed = True
                if not channels:
                    del bridges[bridge_id]
                    log.info("Removed empty bridge %s after channel delete", bridge_id)
            if changed:
                log.info("Removed channel %s from bridge config", channel.id)
