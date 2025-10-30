# GalaxyRedCogs

A collection of cogs for [Red Discord Bot](https://github.com/Cog-Creators/Red-DiscordBot).

## Installation

To add this repository to your Red instance:
```
[p]repo add GalaxyRedCogs https://github.com/ElijahJero/GalaxyRedCogs
```

Then install individual cogs:
```
[p]cog install GalaxyRedCogs <cog_name>
```

## Available Cogs

### BotSheild
Bot protection features for your Discord server.

**Features:**
- Captcha verification for new members
- Scam detection for suspicious messages
- Automatic bot filtering
- Customizable protection settings

**Commands:**
- `[p]botsheild protect` - Set up server protection with captcha verification
- `[p]botsheild scamprotection` - Configure scam detection settings
- `[p]botsheild alertrole` - Set the role to mention for alerts

**Installation:**
```
[p]cog install GalaxyRedCogs BotSheild
[p]load BotSheild
```

### CMLink
Tournament integration with Challenger Mode.

**Features:**
- Monitors Challenger Mode tournaments
- Creates Discord channels for active matches
- Manages tournament participants
- Provides real-time tournament updates
- GraphQL API integration

**Commands:**
- `[p]cmlink setup` - Configure Challenger Mode API credentials
- `[p]cmlink setchannel` - Set the update channel for tournament notifications
- `[p]cmlink setlobby` - Set the lobby voice channel
- `[p]cmlink setcategory` - Set the tournament category

**Installation:**
```
[p]cog install GalaxyRedCogs CMLink
[p]load CMLink
```

### SongLink
Universal music link converter.

**Features:**
- Automatically generates universal SongLink embeds
- Supports multiple music services (Spotify, Apple Music, YouTube Music, etc.)
- Channel-based auto-processing
- Rate limit aware

**Commands:**
- `[p]songlink <url>` - Convert a music URL to a universal link
- `[p]songchannel register <channel>` - Register a channel for automatic conversion
- `[p]songchannel remove <channel>` - Remove a registered channel
- `[p]songchannel list` - List all registered channels

**Installation:**
```
[p]cog install GalaxyRedCogs SongLink
[p]load SongLink
```

## Requirements

- Red-DiscordBot version 3.5.0 or higher
- Python 3.8 or higher

## Support

For issues, questions, or suggestions, please open an issue on the [GitHub repository](https://github.com/ElijahJero/GalaxyRedCogs/issues).

## Author

**Elijah Jero**

## License

See individual cog files for license information.
