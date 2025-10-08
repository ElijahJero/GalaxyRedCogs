from .SongLink import SongLink

async def setup(bot):
    await bot.add_cog(SongLink(bot))