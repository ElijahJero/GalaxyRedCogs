from .AprilFools import AprilFools

async def setup(bot):
    await bot.add_cog(AprilFools(bot))
