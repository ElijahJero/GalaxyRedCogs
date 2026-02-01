from .NOAI import NOAI

async def setup(bot):
    await bot.add_cog(NOAI(bot))