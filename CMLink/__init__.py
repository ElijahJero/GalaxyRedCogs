from .CMLink import CMLink

async def setup(bot):
    await bot.add_cog(CMLink(bot))