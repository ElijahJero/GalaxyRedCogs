from .Elections import Elections

async def setup(bot):
    await bot.add_cog(Elections(bot))
