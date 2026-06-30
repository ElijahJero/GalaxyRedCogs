from .botengine import BotEngineCog

async def setup(bot):
    await bot.add_cog(BotEngineCog(bot))