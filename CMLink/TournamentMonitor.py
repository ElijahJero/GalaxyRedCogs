import asyncio
import json
import os
import threading
from typing import Dict, List, Optional, Tuple

import aiohttp
import discord
from redbot.core import commands
import logging  # added
import datetime  # new
import time
import traceback
import urllib.parse  # new: decode OTEL header env var values
import re             # new: split header pairs

# simplified module logger (no file handler by default)
_logger = logging.getLogger("CMLink.API")
if not _logger.handlers:
    _logger.setLevel(logging.DEBUG)
    _sh = logging.StreamHandler()
    _sh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    _logger.addHandler(_sh)


class Storage:
    """Simple JSON-backed storage for user links: cm_user_id -> discord_user_id."""
    def __init__(self, path: str):
        # Resolve relative paths to the cog directory so file is always predictable
        if not os.path.isabs(path):
            base_dir = os.path.dirname(__file__)
            path = os.path.join(base_dir, path)
        self.path = path
        self._lock = threading.Lock()  # use threading lock for synchronous callers

        # Ensure parent directory exists
        parent = os.path.dirname(self.path)
        if parent and not os.path.exists(parent):
            try:
                os.makedirs(parent, exist_ok=True)
            except Exception:
                _logger.exception("Failed to create storage directory for users.json")

        # Ensure file exists with initial structure
        if not os.path.exists(self.path):
            try:
                with open(self.path, "w", encoding="utf-8") as fp:
                    json.dump({"links": {}}, fp)
            except Exception:
                _logger.exception("Failed to create initial users.json")

    def _read(self) -> Dict[str, Dict[str, str]]:
        try:
            with self._lock:
                with open(self.path, "r", encoding="utf-8") as fp:
                    return json.load(fp)
        except Exception:
            _logger.exception("Failed to read users.json; returning empty structure.")
            return {"links": {}}

    def _write(self, data: Dict[str, Dict[str, str]]) -> None:
        tmp = f"{self.path}.tmp"
        try:
            with self._lock:
                with open(tmp, "w", encoding="utf-8") as fp:
                    json.dump(data, fp, indent=2)
                    fp.flush()
                    try:
                        os.fsync(fp.fileno())
                    except Exception:
                        # fsync may not be available on all platforms; ignore if it fails
                        pass
                # atomic replace
                os.replace(tmp, self.path)
        except Exception:
            _logger.exception("Failed to write users.json (atomic replace failed).")
            # best-effort cleanup of tmp
            try:
                if os.path.exists(tmp):
                    os.remove(tmp)
            except Exception:
                pass

    def save_link(self, cm_user_id: str, discord_user_id: int) -> None:
        try:
            data = self._read()
            data.setdefault("links", {})
            data["links"][str(cm_user_id)] = str(discord_user_id)
            self._write(data)
        except Exception:
            _logger.exception("Failed to save link to users.json")

    def get_discord_id(self, cm_user_id: str) -> Optional[int]:
        data = self._read()
        value = data.get("links", {}).get(str(cm_user_id))
        try:
            return int(value) if value is not None else None
        except Exception:
            return None

    def all_links(self) -> Dict[str, str]:
        # Returns mapping of cm_user_id -> discord_user_id as strings
        data = self._read()
        return data.get("links", {})


class TournamentMonitor:
    """Background worker that polls Challengermode API and orchestrates voice channels."""
    def __init__(self, bot: commands.Bot, config):
        self.bot = bot
        self.config = config
        self.session: Optional[aiohttp.ClientSession] = None
        self._task: Optional[asyncio.Task] = None
        self._running = asyncio.Event()
        # Cache match states to detect transitions
        # {guild_id: {tournament_id: {match_id: "WAITING"|"RUNNING"|...}}}
        self._state_cache: Dict[int, Dict[str, Dict[str, str]]] = {}
        # Active channels per (guild, match_id): {"channels": [ids]}
        self._active_voice: Dict[Tuple[int, str], Dict[str, List[int]]] = {}
        self.storage = Storage("users.json")

        # small cache to avoid re-sending identical API JSON payloads repeatedly
        self._last_js_by_op: Dict[str, str] = {}

    async def start(self):
        if self._task:
            return
        self.session = aiohttp.ClientSession()
        self._running.set()
        self._task = asyncio.create_task(self._loop())

    async def stop(self):
        self._running.clear()
        if self._task:
            try:
                await asyncio.wait_for(self._task, timeout=5)
            except asyncio.TimeoutError:
                self._task.cancel()
        self._task = None
        if self.session and not self.session.closed:
            await self.session.close()
        # Best-effort cleanup of any dangling voice channels
        await self._cleanup_all()

    async def _loop(self):
        while self._running.is_set():
            try:
                interval = await self.config.Poll_Interval()
                await self._tick_all_guilds()
            except Exception:
                # swallow to keep loop alive
                pass
            await asyncio.sleep(max(2, int(interval) if isinstance(interval, int) else 5))

    async def _tick_all_guilds(self):
        api_url = await self.config.API_URL()
        refresh = await self.config.API_Refresh_Token()
        if not api_url or not refresh:
            return  # not configured
        for guild in list(self.bot.guilds):
            try:
                await self._process_guild(guild, api_url)
            except Exception:
                continue

    async def _process_guild(self, guild: discord.Guild, api_url: str):
        cfg = self.config.guild(guild)
        tournaments = await cfg.tournaments()
        if not tournaments:
            return
        for tournament_id in list(tournaments.keys()):
            tdata = await self._fetch_tournament_matches(api_url, tournament_id)
            if not tdata:
                continue
            # previous per-guild cache
            guild_cache = self._state_cache.setdefault(guild.id, {})
            prev = guild_cache.setdefault(tournament_id, {})
            current = {m["id"]: m["state"] for m in tdata["matches"]}
            # detect tournament-level state change
            prev_t_state = guild_cache.get(f"{tournament_id}_state")
            new_t_state = tdata.get("state")
            if new_t_state != prev_t_state:
                try:
                    await self._on_tournament_state_change(guild, tournament_id, tdata.get("name"), prev_t_state, new_t_state, tdata)
                except Exception:
                    _logger.exception("Error handling tournament state change")
            # update stored tournament state
            guild_cache[f"{tournament_id}_state"] = new_t_state
            # Transitions
            for match in tdata["matches"]:
                mid = match["id"]
                old_state = prev.get(mid)
                new_state = match["state"]
                if new_state != old_state:
                    # pass tournament name so we can display it in embeds
                    await self._on_state_change(guild, tournament_id, tdata.get("name"), match, old_state, new_state)
            # Update cache
            self._state_cache[guild.id][tournament_id] = current

    async def _on_state_change(
        self,
        guild: discord.Guild,
        tournament_id: str,
        tournament_name: Optional[str],
        match: Dict,
        old_state: Optional[str],
        new_state: str,
    ):
        # Get guild settings
        guild_cfg = self.config.guild(guild)
        lobby_voice_id = await guild_cfg.lobby_voice_id()
        update_channel_id = await guild_cfg.update_channel_id()
        category_id = await guild_cfg.tournament_category_id()
        update_channel = guild.get_channel(update_channel_id) if update_channel_id else None
        category = guild.get_channel(category_id) if category_id else None
        lobby_vc = guild.get_channel(lobby_voice_id) if lobby_voice_id else None

        # Resolve members (Discord Member objects) per team
        teams: Dict[int, List[discord.Member]] = {}
        for lineup in match.get("lineups", []):
            num = int(lineup.get("number", 0))
            teams[num] = []
            for member in lineup.get("members", []):
                cm_uid = member.get("userId") if isinstance(member, dict) else None
                did = self.storage.get_discord_id(cm_uid) if cm_uid else None
                if did:
                    m = guild.get_member(did)
                    if m:
                        teams[num].append(m)

        if new_state == "WAITING":
            # DM participants not in lobby to join it
            if lobby_vc:
                for members in teams.values():
                    for m in members:
                        if not m.voice or m.voice.channel != lobby_vc:
                            try:
                                embed = discord.Embed(
                                    title="Match Ready",
                                    description=f"Your match is ready in **{guild.name}**.\nPlease join the lobby voice channel: **#{lobby_vc.name}**",
                                    color=discord.Color.blue(),
                                )
                                embed.set_footer(text="CMLink")
                                await m.send(embed=embed)
                                # log DM success to Loki
                                asyncio.create_task(self._push_loki("INFO", "dm_sent", {"guild": guild.id, "member_id": m.id, "match": match.get("id")}))
                            except Exception:
                                _logger.exception("failed to DM participant")
                                asyncio.create_task(self._push_loki("WARNING", "dm_failed", {"guild": guild.id, "member_id": getattr(m, "id", None), "match": match.get("id")}))

        if new_state == "RUNNING":
            # Create private VCs and move participants from lobby
            await self._create_and_move(guild, tournament_id, match, teams, lobby_vc, category)

        if new_state in {"COMPLETED", "CANCELLED", "NULLIFIED"}:
            # Announce and cleanup
            if update_channel:
                match_number = match.get("shortId") or (match.get("id") or "")[:8]
                # tournament_name may be None, fall back to id
                display_tourn = tournament_name or tournament_id
                summary = self._format_match_result(match, guild)
                try:
                    embed = discord.Embed(
                        title="Match Concluded",
                        description=f"**{display_tourn}** — Match **{match_number}** concluded.",
                        color=discord.Color.green(),
                    )
                    # include detailed results as a field to keep the description concise
                    embed.add_field(name="Results", value=summary, inline=False)
                    embed.set_footer(text="CMLink")
                    await update_channel.send(embed=embed)
                except Exception:
                    pass
            await self._cleanup_match_voice(guild, match["id"], lobby_vc)

    async def _on_tournament_state_change(
        self,
        guild: discord.Guild,
        tournament_id: str,
        tournament_name: Optional[str],
        old_state: Optional[str],
        new_state: Optional[str],
        tournament_payload: Optional[Dict] = None,
    ):
        """Handle tournament-level state transitions: announce start and end with summary/winners."""
        guild_cfg = self.config.guild(guild)
        update_channel_id = await guild_cfg.update_channel_id()
        update_channel = guild.get_channel(update_channel_id) if update_channel_id else None
        display_tourn = tournament_name or tournament_id

        if not update_channel:
            return

        try:
            if new_state == "RUNNING" and old_state != "RUNNING":
                embed = discord.Embed(
                    title="Tournament Started",
                    description=f"**{display_tourn}** has started.",
                    color=discord.Color.blue(),
                )
                embed.set_footer(text="CMLink")
                await update_channel.send(embed=embed)

            if new_state == "COMPLETED" and old_state != "COMPLETED":
                # Build winners summary from tournament_payload (best-effort)
                winners_text = "No results available."
                try:
                    # tournament_payload contains matches with results as normalized in _fetch_tournament_matches
                    matches = (tournament_payload or {}).get("matches", []) or []
                    # prefer last match that has lineupResults
                    last_with_results = None
                    for m in reversed(matches):
                        if (m.get("results") or {}).get("lineupResults"):
                            last_with_results = m
                            break
                    if last_with_results:
                        # determine winner lineup number (prefer position==0, else lowest position)
                        lr = last_with_results["results"].get("lineupResults") or []
                        winner_pos = None
                        for x in lr:
                            p = x.get("position")
                            if p == 0:
                                winner_pos = x.get("lineupNumber")
                                break
                        if winner_pos is None and lr:
                            # choose lineup with minimum position value
                            sorted_lr = sorted([x for x in lr if x.get("position") is not None], key=lambda z: z.get("position"))
                            if sorted_lr:
                                winner_pos = sorted_lr[0].get("lineupNumber")
                        # render winners and scores
                        parts = []
                        for lu in last_with_results.get("lineups", []) or []:
                            ln = lu.get("number") or 0
                            members = lu.get("members", []) or []
                            member_texts = []
                            for mem in members:
                                # mem expected {"userId": "...", "username": "..."}
                                cm_uid = mem.get("userId")
                                cm_name = mem.get("username") or "<unknown>"
                                mention_part = ""
                                if cm_uid:
                                    did = self.storage.get_discord_id(cm_uid)
                                    if did:
                                        m = guild.get_member(int(did))
                                        if m:
                                            mention_part = f" ({m.mention})"
                                member_texts.append(f"{cm_name}{mention_part}")
                            # find lineup result entry
                            entry = None
                            for x in lr:
                                if x.get("lineupNumber") == ln:
                                    entry = x
                                    break
                            score = (entry.get("score") if entry else None)
                            pos = (entry.get("position") if entry else None)
                            marker = "🏆 " if winner_pos is not None and ln == winner_pos else ""
                            parts.append(f"{marker}Team {ln+1}: {', '.join(member_texts) or 'Unknown'} — score={score if score is not None else '?'} — pos={pos if pos is not None else '?'}")
                        winners_text = "\n".join(parts) if parts else "Results unavailable."
                    else:
                        winners_text = "No match results available to determine winners."
                except Exception:
                    _logger.exception("Failed to build winners summary")

                embed = discord.Embed(
                    title="Tournament Concluded",
                    description=f"**{display_tourn}** has concluded.",
                    color=discord.Color.green(),
                )
                embed.add_field(name="Winners / Final results", value=winners_text, inline=False)
                embed.set_footer(text="CMLink")
                await update_channel.send(embed=embed)
        except Exception:
            _logger.exception("Error sending tournament state announcement")

    def _format_match_result(self, match: Dict, guild: discord.Guild) -> str:
        """
        Produce a multiline summary:
        - For each lineup: CM username and Discord mention (if linked and member of guild)
        - lineup score and position
        The result is kept reasonably short to fit embed field limits.
        """
        res = match.get("results") or {}
        lineup_results = {lr.get("lineupNumber"): lr for lr in (res.get("lineupResults") or [])}
        lineups = match.get("lineups", []) or []
        parts = []
        for lu in lineups:
            ln = lu.get("number", 0)
            members = lu.get("members", []) or []
            member_texts = []
            for mem in members:
                # mem is expected to be {"userId": "...", "username": "..."}
                cm_uid = mem.get("userId")
                cm_name = mem.get("username") or "<unknown>"
                mention_part = ""
                if cm_uid:
                    did = self.storage.get_discord_id(cm_uid)
                    if did:
                        try:
                            m = guild.get_member(int(did))
                            if m:
                                mention_part = f" ({m.mention})"
                        except Exception:
                            pass
                # append the resolved member text (this was missing previously)
                member_texts.append(f"{cm_name}{mention_part}")
            lr = lineup_results.get(ln, {})
            score = lr.get("score")
            pos = lr.get("position")
            score_part = f"score={score}" if score is not None else "score=?"
            pos_part = f"pos={pos}" if pos is not None else "pos=?"
            parts.append(f"Team {ln+1}: {', '.join(member_texts) or 'Unknown players'} — {score_part} — {pos_part}")
        return "\n".join(parts) if parts else "Results unavailable."

    async def _create_and_move(
        self,
        guild: discord.Guild,
        tournament_id: str,
        match: Dict,
        teams: Dict[int, List[discord.Member]],
        lobby_vc: Optional[discord.VoiceChannel],
        category: Optional[discord.CategoryChannel],
    ):
        key = (guild.id, match["id"])
        if key in self._active_voice:
            return  # already created

        # Determine 1v1 vs team size
        team_sizes = [len(v) for v in teams.values() if v]
        max_size = max(team_sizes) if team_sizes else 0
        channel_map: List[discord.VoiceChannel] = []

        overwrites_base = {
            guild.default_role: discord.PermissionOverwrite(connect=False, view_channel=False, speak=False),
        }

        try:
            if max_size <= 1:
                # 1v1: single shared VC
                allowed = [m for members in teams.values() for m in members]
                overwrites = dict(overwrites_base)
                for m in allowed:
                    overwrites[m] = discord.PermissionOverwrite(connect=True, view_channel=True, speak=True)
                name = f"match-{match['shortId']}"
                ch = await guild.create_voice_channel(name=name, overwrites=overwrites, category=category, reason="CMLink match voice")
                channel_map.append(ch)
                # log creation to Loki
                asyncio.create_task(self._push_loki("INFO", "voice_channel_created", {"guild": guild.id, "channel_id": ch.id, "name": ch.name, "match": match.get("id")}))
            else:
                # Per-team VC
                for team_no, members in teams.items():
                    if not members:
                        continue
                    overwrites = dict(overwrites_base)
                    for m in members:
                        overwrites[m] = discord.PermissionOverwrite(connect=True, view_channel=True, speak=True)
                    name = f"match-{match['shortId']}-team{team_no+1}"
                    ch = await guild.create_voice_channel(name=name, overwrites=overwrites, category=category, reason="CMLink match voice")
                    channel_map.append(ch)
                    asyncio.create_task(self._push_loki("INFO", "voice_channel_created", {"guild": guild.id, "channel_id": ch.id, "name": ch.name, "match": match.get("id"), "team": team_no+1}))

            # Move people from lobby to their channel(s)
            if lobby_vc:
                for team_no, members in teams.items():
                    target = channel_map[0] if len(channel_map) == 1 else next((c for c in channel_map if c.name.endswith(f"team{team_no+1}")), None)
                    if not target:
                        continue
                    for m in members:
                        try:
                            if m.voice and m.voice.channel == lobby_vc:
                                await m.move_to(target, reason="CMLink moving to match voice")
                        except Exception:
                            pass

            # Track active
            self._active_voice[key] = {"channels": [c.id for c in channel_map]}
            # Persist for recovery
            guild_cfg = self.config.guild(guild)
            active = await guild_cfg.active_matches()
            match_entry = active.get(match["id"], {"channels": []})
            match_entry["channels"] = [c.id for c in channel_map]
            active[match["id"]] = match_entry
            await guild_cfg.active_matches.set(active)
        except Exception:
            # Best-effort cleanup if partial
            for ch in channel_map:
                try:
                    await ch.delete(reason="CMLink cleanup (error)")
                    asyncio.create_task(self._push_loki("WARNING", "voice_channel_deleted_cleanup", {"guild": guild.id, "channel_id": ch.id}))
                except Exception:
                    pass

    async def _cleanup_match_voice(self, guild: discord.Guild, match_id: str, lobby_vc: Optional[discord.VoiceChannel]):
        key = (guild.id, match_id)
        channel_ids: List[int] = []
        # Read in-memory
        if key in self._active_voice:
            channel_ids = self._active_voice[key]["channels"]
            self._active_voice.pop(key, None)
        # Read persisted
        guild_cfg = self.config.guild(guild)
        active = await guild_cfg.active_matches()
        if match_id in active:
            for cid in active[match_id].get("channels", []):
                if cid not in channel_ids:
                    channel_ids.append(cid)
            active.pop(match_id, None)
            await guild_cfg.active_matches.set(active)

        # Move members back and delete channels
        for cid in channel_ids:
            ch = guild.get_channel(cid)
            if not ch or not isinstance(ch, discord.VoiceChannel):
                continue
            try:
                if lobby_vc:
                    for member in list(ch.members):
                        try:
                            await member.move_to(lobby_vc, reason="CMLink moving back to lobby")
                        except Exception:
                            pass
                await ch.delete(reason="CMLink match concluded")
            except Exception:
                pass

    async def _cleanup_all(self):
        # Cleanup all persisted active voice channels on shutdown
        for guild in list(self.bot.guilds):
            guild_cfg = self.config.guild(guild)
            active = await guild_cfg.active_matches()
            if not active:
                continue
            lobby_id = await guild_cfg.lobby_voice_id()
            lobby_vc = guild.get_channel(lobby_id) if lobby_id else None
            for match_id in list(active.keys()):
                await self._cleanup_match_voice(guild, match_id, lobby_vc)

    async def _fetch_tournament_matches(self, api_url: str, tournament_id: str) -> Optional[Dict]:
        query = """
        query TournamentMatches($id: UUID!) {
          tournament(tournamentId: $id) {
            id
            name
            state
            matchSeries {
              id
              state
              ordinal
              lineupCount
              results {
                final
                draw
                lineupResults {
                  lineupNumber
                  position
                  score
                }
              }
              matches(includeFailed: false) {
                id
                state
                lineups {
                  number
                  members {
                    user {
                      userId
                      username
                    }
                  }
                }
              }
            }
          }
        }
        """
        vars = {"id": tournament_id}
        data = await self._graphql(api_url, None, query, vars)
        t = (data or {}).get("tournament")
        if not t:
            return None
        # Normalize members to include userId directly for convenience (from the first match's lineups)
        matches = []
        for ms in t.get("matchSeries", []) or []:
            # take the first match as the source of team lineups and members
            ms_matches = ms.get("matches", []) or []
            lineups = []
            if ms_matches:
                first = ms_matches[0] or {}
                for lu in first.get("lineups", []) or []:
                    members = []
                    for mem in lu.get("members", []) or []:
                        u = mem.get("user") or {}
                        members.append({"userId": u.get("userId"), "username": u.get("username")})
                    lineups.append({"number": lu.get("number"), "members": members})
            match_obj = {
                "id": ms.get("id"),
                "shortId": str(ms.get("ordinal") or "") if ms.get("ordinal") is not None else (ms.get("id", "")[:8] or ""),
                "state": ms.get("state"),
                "lineupCount": ms.get("lineupCount"),
                "results": ms.get("results") or {},
                "lineups": lineups,
            }
            matches.append(match_obj)
        return {"id": t.get("id"), "name": t.get("name"), "matches": matches}

    async def get_tournament_participants(self, tournament_id: str) -> List[Dict[str, str]]:
        """Return list of dicts: {"userId": str, "username": Optional[str]} for signups/roster."""
        api_url = await self.config.API_URL()
        if not api_url:
            return []
        query = """
        query TournamentParticipants($id: UUID!) {
          tournament(tournamentId: $id) {
            attendance {
              signups {
                lineups {
                  members {
                    user {
                      userId
                      username
                    }
                  }
                }
              }
              roster {
                lineups {
                  members {
                    user {
                      userId
                      username
                    }
                  }
                }
              }
            }
          }
        }
        """
        vars = {"id": tournament_id}
        data = await self._graphql(api_url, None, query, vars)
        t = (data or {}).get("tournament")
        if not t:
            return []
        attendance = t.get("attendance") or {}
        users: Dict[str, str] = {}

        def ingest(block):
            for lu in (block or {}).get("lineups", []) or []:
                for mem in lu.get("members", []) or []:
                    u = mem.get("user") or {}
                    uid = u.get("userId")
                    uname = u.get("username")
                    if uid:
                        users[uid] = uname or users.get(uid)

        ingest(attendance.get("signups"))
        ingest(attendance.get("roster") or {})
        return [{"userId": k, "username": v} for k, v in users.items()]

    async def _graphql_raw(self, api_url: str, provided_refresh_token: Optional[str], query: str, variables: dict) -> \
    Tuple[Optional[dict], Optional[int]]:
        """
        Low-level GraphQL requester that returns the parsed JSON response (full) and HTTP status,
        or (None, None) on request/timeout/parse failure.
        This helper will ensure a valid BOT access token using the refresh key.
        """
        # Ensure a valid short-lived BOT token before making requests
        access = await self._ensure_access_token()
        if not access:
            _logger.warning("No BOT access token available.")
            return None, None

        base_headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {access}",
        }

        # include optional Api-Version header if present
        try:
            api_version = await self.config.API_Version()
        except Exception:
            api_version = None
        if api_version:
            base_headers["Api-Version"] = api_version

        payload = {"query": query, "variables": variables}

        def _truncate(s: str, limit: int = 4000) -> str:
            return s if len(s) <= limit else f"{s[:limit]}... (truncated, {len(s)} chars)"

        def _op_name(q: str) -> str:
            try:
                first = q.strip().splitlines()[0]
                parts = first.replace("{", " ").split()
                if len(parts) >= 2 and parts[0] in ("query", "mutation"):
                    return parts[1]
            except Exception:
                pass
            return "unknown"

        op = _op_name(query)
        req_id = os.urandom(4).hex()
        debug_log = False
        try:
            debug_log = await self.config.Debug_API_Logging()
        except Exception:
            pass

        if debug_log:
            try:
                _logger.debug(f"[{req_id}] -> POST {api_url} op={op} vars={_truncate(json.dumps(variables, ensure_ascii=False))}")
            except Exception:
                _logger.debug(f"[{req_id}] -> POST {api_url} op={op} vars=<unserializable>")

        async def _do_request(headers: Dict[str, str]) -> Tuple[Optional[int], Optional[str]]:
            try:
                async with self.session.post(api_url, headers=headers, json=payload, timeout=30) as resp:
                    text = await resp.text()
                    if debug_log:
                        _logger.debug(f"[{req_id}] <- {resp.status} body={_truncate(text)}")
                    return resp.status, text
            except asyncio.TimeoutError:
                if debug_log:
                    _logger.warning(f"[{req_id}] Request timeout for op={op} to {api_url}")
                return None, None
            except Exception as e:
                if debug_log:
                    _logger.exception(f"[{req_id}] Exception during GraphQL request op={op}: {e}")
                return None, None

        # perform primary request
        status, raw_text = await _do_request(base_headers)
        if raw_text is None:
            return None, status
        try:
            js = json.loads(raw_text)
        except Exception:
            # log full raw_text to Loki if debug enabled and it differs from last
            try:
                debug_log = await self.config.Debug_API_Logging()
            except Exception:
                debug_log = False
            if debug_log:
                prev = self._last_js_by_op.get(op)
                if raw_text != prev:
                    # send full raw body to Loki (so we don't truncate interesting failures)
                    asyncio.create_task(self._push_loki("WARNING", f"raw_nonjson_response op={op}", {"status": status, "body": raw_text, "req_id": req_id}))
                    self._last_js_by_op[op] = raw_text
                else:
                    # duplicate — send a short note
                    asyncio.create_task(self._push_loki("DEBUG", f"raw_nonjson_response_duplicate op={op}", {"status": status, "note": "same as previous"}))
            return None, status

        # log JSON responses to Loki when debug logging enabled and when they differ from last for this op
        try:
            debug_log = await self.config.Debug_API_Logging()
        except Exception:
            debug_log = False

        if debug_log:
            prev = self._last_js_by_op.get(op)
            if raw_text != prev:
                # full payload differs; push the entire response body to Loki
                try:
                    # schedule non-blocking
                    asyncio.create_task(self._push_loki("DEBUG", f"graphql_response op={op} req={req_id}", {"status": status, "body": js}))
                except Exception:
                    pass
                self._last_js_by_op[op] = raw_text
            else:
                # duplicates: send compact note
                asyncio.create_task(self._push_loki("DEBUG", f"graphql_response_duplicate op={op} req={req_id}", {"status": status}))

        # detect auth GraphQL errors and attempt one refresh+retry
        def _is_auth_error(js_obj):
            if not isinstance(js_obj, dict):
                return False
            for e in js_obj.get("errors", []) if js_obj.get("errors") else []:
                code = (e.get("extensions") or {}).get("code")
                msg = e.get("message", "")
                if code == "AUTH_NOT_AUTHENTICATED" or "not authorized" in msg.lower() or "auth" in msg.lower():
                    return True
            return False

        if status == 200 and _is_auth_error(js):
            if debug_log:
                _logger.warning(f"[{req_id}] Auth error detected; attempting token refresh and retry for op={op}")
            # clear cached access token so _ensure_access_token will exchange again
            try:
                await self.config.API_Access_Token.set("")
                await self.config.API_Access_Expires_At.set(0)
            except Exception:
                pass
            new_access = await self._ensure_access_token()
            if not new_access:
                return js, status
            headers_retry = dict(base_headers)
            headers_retry["Authorization"] = f"Bearer {new_access}"
            status2, raw_text2 = await _do_request(headers_retry)
            if raw_text2 is None:
                return None, status2
            try:
                js2 = json.loads(raw_text2)
            except Exception:
                if debug_log:
                    _logger.warning(f"[{req_id}] Failed to parse JSON response for op={op} on retry. Raw: {_truncate(raw_text2, 8000)}")
                return None, status2
            if status2 != 200:
                if debug_log:
                    _logger.warning(f"[{req_id}] Non-200 on retry: {status2} for op={op}. Body: {_truncate(raw_text2, 8000)}")
                return js2, status2
            return js2, status2

        return js, status

    async def _push_loki(self, level: str, message: str, payload: Optional[dict] = None) -> None:
        """
        Best-effort async push to Grafana Loki via simple HTTP (basic auth).
        Matches the sample usage: POST to /loki/api/v1/push with auth=(USER, API_KEY).
        Quietly returns when not configured or on error.
        """
        try:
            try:
                enabled = bool(await self.config.LOKI_Enabled())
            except Exception:
                enabled = False
            if not enabled:
                return

            try:
                loki_url = (await self.config.LOKI_URL()) or ""
                loki_user = (await self.config.LOKI_User()) or ""
                loki_key = (await self.config.LOKI_API_Key()) or ""
            except Exception:
                return

            if not loki_url or not loki_user or not loki_key:
                return

            # Build a single stream payload similar to the sample
            ts_nano = str(int(time.time() * 1e9))
            entry = {
                "level": level,
                "msg": message,
            }
            if payload is not None:
                entry["payload"] = payload

            streams = {
                "streams": [
                    {
                        "stream": {"job": "cmlink", "module": "TournamentMonitor", "level": level},
                        "values": [[ts_nano, json.dumps(entry, ensure_ascii=False)]],
                    }
                ]
            }

            # send via aiohttp with BasicAuth
            if not self.session:
                return
            post_url = loki_url.rstrip("/") + "/loki/api/v1/push"
            auth = aiohttp.BasicAuth(str(loki_user), loki_key)
            headers = {"Content-Type": "application/json"}
            try:
                async with self.session.post(post_url, json=streams, headers=headers, auth=auth, timeout=10) as resp:
                    if resp.status < 200 or resp.status >= 300:
                        text = await resp.text()
                        _logger.warning(f"Loki HTTP push returned {resp.status}: {text}")
            except Exception:
                _logger.exception("Failed to push logs to Loki HTTP endpoint")
        except Exception:
            _logger.exception("Unexpected error in _push_loki: " + traceback.format_exc())

    async def _ensure_access_token(self) -> Optional[str]:
        """Exchange the stored refresh key for a short-lived BOT access token."""
        # Check if we have a valid cached token first
        cached_token = await self.config.API_Access_Token()
        expires_at = await self.config.API_Access_Expires_At()
        now = int(time.time())

        # Use cached token if valid (with 60s buffer before expiry)
        if cached_token and expires_at and (expires_at - now) > 60:
            return cached_token

        # Token expired or not present, exchange refresh key
        refresh_key = await self.config.API_Refresh_Token()
        if not refresh_key:
            _logger.warning("No ChallengerMode refresh key configured.")
            return None

        token_url = await self.config.API_Token_URL()
        if not token_url:
            _logger.warning("No token exchange endpoint URL configured.")
            return None

        headers = {"Content-Type": "application/json"}
        body = {"refreshKey": refresh_key}

        try:
            async with self.session.post(token_url, headers=headers, json=body, timeout=10) as resp:
                text = await resp.text()
                if resp.status != 200:
                    _logger.warning(f"Token exchange failed: {resp.status} body={text}")
                    # also send to Loki
                    asyncio.create_task(self._push_loki("ERROR", "token_exchange_failed", {"status": resp.status, "body": text}))
                    return None
                js = json.loads(text)
                token = js.get("value")
                expires_at_str = js.get("expiresAt")

                # Cache the token
                await self.config.API_Access_Token.set(token)

                # Parse expires_at and store as unix timestamp
                if expires_at_str:
                    try:
                        # Parse ISO8601 timestamp like "2021-10-26T07:48:14.2830852Z"
                        dt = datetime.datetime.fromisoformat(expires_at_str.replace('Z', '+00:00'))
                        await self.config.API_Access_Expires_At.set(int(dt.timestamp()))
                    except Exception:
                        # Fallback: assume 20 minutes (1200s) minus 100s buffer
                        await self.config.API_Access_Expires_At.set(int(time.time()) + 1100)
                else:
                    # Fallback: assume 20 minutes (1200s) minus 100s buffer
                    await self.config.API_Access_Expires_At.set(int(time.time()) + 1100)

                return token
        except Exception as e:
            _logger.exception(f"Token exchange exception: {e}")
            asyncio.create_task(self._push_loki("ERROR", "token_exchange_exception", {"error": str(e)}))
            return None
