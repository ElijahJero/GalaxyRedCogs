# PolyglotBridge

Cross-channel translation bridges for multilingual Discord communities.

When a user posts in one linked channel, their message is automatically translated and relayed to every other channel in the bridge — each channel representing a different language.

## Credits

Translation API logic is adapted from **[Fluent](https://github.com/vertyco/vrt-cogs)** by [Vertyco](https://github.com/vertyco) (MIT License).

## Setup Example

```
[p]polyglot create Community Chat
[p]polyglot addchannel community-chat #english english
[p]polyglot addchannel community-chat #german german
[p]polyglot addchannel community-chat #japanese japanese
```

A message in `#english` is translated to German and Japanese and posted in `#german` and `#japanese`. The same works in reverse from any linked channel.

## Commands

| Command | Description |
|---------|-------------|
| `[p]polyglot create <name>` | Create a new bridge |
| `[p]polyglot addchannel <bridge> <channel> <language>` | Add a channel to a bridge |
| `[p]polyglot removechannel <bridge> <channel>` | Remove a channel from a bridge |
| `[p]polyglot delete <bridge>` | Delete an entire bridge |
| `[p]polyglot view` | View all bridges and channels |
| `[p]polyglot openai` | Set OpenAI API key (owner) |
| `[p]polyglot deepl` | Set DeepL API key (owner) |
| `[p]translate <language> [message]` | Manually translate text |

Aliases: `polyglotbridge`, `pb`

## Translation Providers

Fallback order (when API keys are configured):

1. OpenAI (optional)
2. DeepL (optional)
3. Google Translate
4. Flowery

## Permissions

The bot needs **Send Messages** and **Embed Links** in every channel in a bridge.

## License

This cog is part of [GalaxyRedCogs](https://github.com/ElijahJero/GalaxyRedCogs). Translation API components retain MIT license attribution to [vertyco/vrt-cogs](https://github.com/vertyco/vrt-cogs).
