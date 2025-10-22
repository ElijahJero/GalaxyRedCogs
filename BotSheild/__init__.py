from .BotSheild import BotSheild

async def setup(bot):
    await bot.add_cog(BotSheild(bot))