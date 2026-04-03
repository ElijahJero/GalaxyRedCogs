"""
Elections cog for Red-DiscordBot.

Anonymous plurality voting. Voter identity and ballot data are stored in
separate lists so no one can correlate who voted for whom.

Config layout (guild-scoped):
  elections: {
    "<election_id>": {
      guild_id, channel_id, message_id,
      title, description,
      end_time (unix float),
      allowed_roles, ping_role,
      allow_abstain (global default),
      positions: [{id, name, description, max_winners, allow_abstain, candidates: [{id, name, description}]}],
      ballots: {"pos_id": [candidate_index_or_null, ...]},  # anonymous list
      voters: [user_id, ...],                               # dedup only, no vote info
      active, ended
    }
  }
"""

from __future__ import annotations

import asyncio
import json
import re
import secrets
from collections import Counter
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

import discord
from discord.ext import tasks
from discord.ui import Button, Select, View
from redbot.core import Config, commands
from redbot.core.bot import Red

# ── helpers ──────────────────────────────────────────────────────────────────

def _parse_end_time(raw: str) -> float:
    """
    Accept either an ISO-8601 timestamp ("2026-04-10T18:00:00Z") or a
    duration string like "72h", "3d", "1d6h30m".  Returns a UTC unix timestamp.
    """
    raw = raw.strip()

    # Try ISO first
    for fmt in ("%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S+00:00", "%Y-%m-%dT%H:%M:%S"):
        try:
            dt = datetime.strptime(raw, fmt).replace(tzinfo=timezone.utc)
            return dt.timestamp()
        except ValueError:
            pass

    # Try duration: e.g. "3d12h30m", "72h", "45m"
    pattern = re.fullmatch(
        r"(?:(\d+)d)?(?:(\d+)h)?(?:(\d+)m)?",
        raw,
        re.IGNORECASE,
    )
    if pattern and any(pattern.groups()):
        days = int(pattern.group(1) or 0)
        hours = int(pattern.group(2) or 0)
        minutes = int(pattern.group(3) or 0)
        delta = timedelta(days=days, hours=hours, minutes=minutes)
        if delta.total_seconds() > 0:
            return (datetime.now(timezone.utc) + delta).timestamp()

    raise ValueError(f"Cannot parse end_time: {raw!r}")


def _parse_config(raw: dict, guild: discord.Guild) -> dict:
    """Validate and normalise the user-supplied JSON election config."""
    errors: List[str] = []

    title = raw.get("title", "").strip()
    if not title:
        errors.append("Missing required field: title")

    raw_end = raw.get("end_time", "")
    try:
        end_time = _parse_end_time(str(raw_end))
        if end_time <= datetime.now(timezone.utc).timestamp():
            errors.append("end_time must be in the future")
    except ValueError as e:
        end_time = 0.0
        errors.append(str(e))

    # Roles
    allowed_roles: List[int] = []
    for rid in raw.get("allowed_roles", []):
        try:
            rid = int(rid)
        except (TypeError, ValueError):
            errors.append(f"Invalid role ID: {rid!r}")
            continue
        if guild.get_role(rid) is None:
            errors.append(f"Role ID {rid} not found in this server")
        else:
            allowed_roles.append(rid)

    ping_role: Optional[int] = None
    if "ping_role" in raw:
        try:
            ping_role = int(raw["ping_role"])
            if guild.get_role(ping_role) is None:
                errors.append(f"ping_role ID {ping_role} not found in this server")
                ping_role = None
        except (TypeError, ValueError):
            errors.append(f"Invalid ping_role: {raw['ping_role']!r}")

    global_abstain: bool = bool(raw.get("allow_abstain", False))

    # Positions
    if not isinstance(raw.get("positions"), list) or len(raw["positions"]) == 0:
        errors.append("positions must be a non-empty list")

    positions: List[dict] = []
    for i, pos in enumerate(raw.get("positions") or []):
        pname = str(pos.get("name", f"Position {i+1}")).strip()
        pdesc = str(pos.get("description", "")).strip()
        try:
            max_winners = max(1, int(pos.get("max_winners", 1)))
        except (TypeError, ValueError):
            max_winners = 1

        pos_abstain: bool = bool(pos.get("allow_abstain", global_abstain))

        candidates_raw = pos.get("candidates", [])
        if not isinstance(candidates_raw, list) or len(candidates_raw) == 0:
            errors.append(f"Position {i+1} ({pname!r}) must have at least 1 candidate")
            candidates_raw = []

        cands: List[dict] = []
        for j, c in enumerate(candidates_raw):
            cands.append({
                "id": f"c{j}",
                "name": str(c.get("name", f"Candidate {j+1}")).strip(),
                "description": str(c.get("description", "")).strip(),
            })

        positions.append({
            "id": f"pos_{i}",
            "name": pname,
            "description": pdesc,
            "max_winners": max_winners,
            "allow_abstain": pos_abstain,
            "candidates": cands,
        })

    if errors:
        raise ValueError("\n".join(f"• {e}" for e in errors))

    return {
        "title": title,
        "description": str(raw.get("description", "")).strip(),
        "end_time": end_time,
        "allowed_roles": allowed_roles,
        "ping_role": ping_role,
        "allow_abstain": global_abstain,
        "positions": positions,
    }


def _discord_ts(unix: float, style: str = "R") -> str:
    return f"<t:{int(unix)}:{style}>"


def _pluralise(n: int, word: str) -> str:
    return f"{n} {word}{'s' if n != 1 else ''}"


_FIELD_LIMIT = 1024


def _candidates_field_value(cands: list, current: Optional[int], allow_abstain: bool) -> str:
    """
    Build the Candidates embed-field value.  If the full text would exceed the
    Discord 1024-char field limit, each candidate's description is trimmed
    proportionally so every candidate remains visible with at least their name.
    """
    abstain_prefix = (
        "⬜ *Abstaining from this position*\n\n"
        if (allow_abstain and current is None)
        else ""
    )

    # Name-only lines (no descriptions yet)
    name_lines = []
    for i, c in enumerate(cands):
        marker = "✅" if current == i else "◻️"
        name_lines.append(f"{marker} **{c['name']}**")

    sep = "\n\n"
    base_text = abstain_prefix + sep.join(name_lines)

    desc_indices = [i for i, c in enumerate(cands) if c.get("description")]
    if not desc_indices:
        return base_text or "No candidates."

    # Each description is attached as "\n> <text>" — prefix costs 3 chars
    per_prefix = 3
    available = _FIELD_LIMIT - len(base_text) - per_prefix * len(desc_indices)
    per_desc = (available // len(desc_indices)) if available > 0 else 0

    lines = []
    for i, c in enumerate(cands):
        marker = "✅" if current == i else "◻️"
        line = f"{marker} **{c['name']}**"
        desc = c.get("description") or ""
        if desc:
            if len(desc) > per_desc:
                desc = desc[:max(0, per_desc - 1)] + "…"
            if desc:
                line += f"\n> {desc}"
        lines.append(line)

    result = abstain_prefix + sep.join(lines)
    if len(result) > _FIELD_LIMIT:
        result = result[:_FIELD_LIMIT - 1] + "…"
    return result or "No candidates."


def _tabulate(
    position: dict, ballots: List[Optional[int]]
) -> Tuple[List[Tuple[str, int, str]], List[Tuple[str, int]], List[Tuple[str, int]]]:
    """
    Plurality tabulation.

    Returns (winners, undecided, others):
      - winners:   [(name, votes, medal_label), ...]  — clear winners in placement order
      - undecided: [(name, votes), ...]               — tied at the boundary; no winner declared
      - others:    [(name, votes), ...]               — did not win or tie at the cut-off

    If a tie group straddles the max_winners boundary (more tied candidates
    than remaining spots), all of them go into undecided and no winner is
    declared for those spots.
    """
    cands = position["candidates"]
    max_winners = position["max_winners"]
    counts: Counter = Counter()
    for ballot in ballots:
        if ballot is not None and 0 <= ballot < len(cands):
            counts[ballot] += 1

    # Sort descending, then by index for deterministic ordering within a tie
    ranked = sorted(range(len(cands)), key=lambda i: (-counts[i], i))

    medals = ["🥇", "🥈", "🥉"]
    winners: List[Tuple[str, int, str]] = []
    undecided: List[Tuple[str, int]] = []
    others: List[Tuple[str, int]] = []

    place = 0  # number of winner slots already filled
    i = 0
    while i < len(ranked):
        cur_votes = counts[ranked[i]]
        # Collect the full tie group at this vote count
        tie_end = i
        while tie_end < len(ranked) and counts[ranked[tie_end]] == cur_votes:
            tie_end += 1
        tie_group = ranked[i:tie_end]

        remaining_spots = max_winners - place

        if remaining_spots <= 0:
            # All winner slots already filled
            for idx in tie_group:
                others.append((cands[idx]["name"], counts[idx]))
        elif len(tie_group) <= remaining_spots:
            # Entire tie group fits within remaining spots — they all win
            for j, idx in enumerate(tie_group):
                slot = place + j
                label = medals[slot] if slot < 3 else f"#{slot + 1}"
                if len(tie_group) > 1:
                    label += " (tied)"
                winners.append((cands[idx]["name"], counts[idx], label))
            place += len(tie_group)
        else:
            # Tie group is larger than remaining spots — boundary tie, undecided
            for idx in tie_group:
                undecided.append((cands[idx]["name"], counts[idx]))
            # All remaining spots are consumed (but no winner declared)
            place = max_winners

        i = tie_end

    return winners, undecided, others


# ── Persistent Tracker View ───────────────────────────────────────────────────

class ElectionTrackerView(View):
    """
    Persistent view attached to the tracker message in the guild channel.
    Stays alive across bot restarts via bot.add_view().
    """

    def __init__(self, cog: "Elections", election_id: str):
        super().__init__(timeout=None)
        self.cog = cog
        self.election_id = election_id

        btn = Button(
            label="Vote Now",
            style=discord.ButtonStyle.primary,
            emoji="🗳️",
            custom_id=f"election:vote:{election_id}",
        )
        btn.callback = self._vote_callback
        self.add_item(btn)

    async def _vote_callback(self, interaction: discord.Interaction):
        cog = self.cog
        election_id = self.election_id

        guild = interaction.guild
        if guild is None:
            await interaction.response.send_message(
                "This button only works in a server.", ephemeral=True
            )
            return

        # Load election
        async with cog.config.guild(guild).elections() as elections:
            data = elections.get(election_id)

        if not data or not data.get("active") or data.get("ended"):
            await interaction.response.send_message(
                "This election is no longer active.", ephemeral=True
            )
            return

        user = interaction.user
        member = guild.get_member(user.id) or await guild.fetch_member(user.id)

        # Role check
        allowed_roles = data.get("allowed_roles", [])
        if allowed_roles:
            member_role_ids = {r.id for r in member.roles}
            if not member_role_ids.intersection(allowed_roles):
                await interaction.response.send_message(
                    "You don't have the required role to vote in this election.",
                    ephemeral=True,
                )
                return

        # Already voted?
        if user.id in data.get("voters", []):
            await interaction.response.send_message(
                "You have already submitted your ballot for this election.",
                ephemeral=True,
            )
            return

        # Already has an active session?
        if (user.id, election_id) in cog._active_sessions:
            await interaction.response.send_message(
                "You already have a voting session open. Check your DMs.",
                ephemeral=True,
            )
            return

        await interaction.response.defer(ephemeral=True)

        try:
            view = ElectionVoteView(cog=cog, election_id=election_id, election_data=data, user_id=user.id)
            embed = view.build_page_embed()
            dm_msg = await user.send(embed=embed, view=view)
            view.dm_message = dm_msg
            cog._active_sessions[(user.id, election_id)] = view
            await interaction.followup.send("Check your DMs to cast your vote! 📬", ephemeral=True)
        except discord.Forbidden:
            await interaction.followup.send(
                f"I couldn't send you a DM. Please enable DMs from server members, "
                f"or DM me directly with:\n"
                f"```\n[p]election join {election_id}\n```",
                ephemeral=True,
            )


# ── Voting View (multi-page DM) ──────────────────────────────────────────────

_ABSTAIN_VALUE = "__abstain__"


class ElectionVoteView(View):
    """
    Multi-page voting form sent via DM.
    One page per position.  Draft ballot held in memory — never persisted.
    """

    def __init__(
        self,
        cog: "Elections",
        election_id: str,
        election_data: dict,
        user_id: int,
    ):
        super().__init__(timeout=3600)  # 1 hour to complete ballot
        self.cog = cog
        self.election_id = election_id
        self.election_data = election_data
        self.user_id = user_id
        # page index: 0..len(positions)-1, or "summary", or "confirm"
        self.page: int | str = 0
        # draft: {pos_id: candidate_index (int) | None}
        self.draft: Dict[str, Optional[int]] = {
            p["id"]: None for p in election_data["positions"]
        }
        self.dm_message: Optional[discord.Message] = None
        self._desc_messages: List[discord.Message] = []
        self._desc_delete_task: Optional[asyncio.Task] = None
        self._rebuild_items()

    # ── internal builders ────────────────────────────────────────────────────

    def _rebuild_items(self):
        """Clear and re-add all UI items for the current page."""
        self.clear_items()

        if self.page == "confirm":
            self._add_confirm_items()
        elif self.page == "summary":
            self._add_summary_items()
        else:
            self._add_position_items()

    async def _clear_desc_messages(self):
        """Delete any expanded full-description messages and cancel their auto-delete task."""
        if self._desc_delete_task and not self._desc_delete_task.done():
            self._desc_delete_task.cancel()
            self._desc_delete_task = None
        for m in self._desc_messages:
            try:
                await m.delete()
            except discord.HTTPException:
                pass
        self._desc_messages.clear()

    def _add_position_items(self):
        positions = self.election_data["positions"]
        pos = positions[self.page]
        pos_id = pos["id"]
        allow_abstain = pos["allow_abstain"]
        cands = pos["candidates"]
        current = self.draft.get(pos_id)

        # Build select options
        options = []
        if allow_abstain:
            options.append(
                discord.SelectOption(
                    label="Abstain from this position",
                    value=_ABSTAIN_VALUE,
                    emoji="⬜",
                    default=(current is None),
                )
            )
        for i, c in enumerate(cands):
            # Truncate label to Discord's 100-char limit
            label = c["name"][:100]
            options.append(
                discord.SelectOption(
                    label=label,
                    value=str(i),
                    description=(c["description"][:100] if c["description"] else None),
                    default=(current == i),
                )
            )

        select = Select(
            placeholder="Select a candidate...",
            custom_id=f"vote_select_{pos_id}",
            options=options[:25],  # Discord max 25 options
            min_values=1,
            max_values=1,
        )

        async def select_callback(interaction: discord.Interaction, _s=select, _pos_id=pos_id):
            val = _s.values[0]
            if val == _ABSTAIN_VALUE:
                self.draft[_pos_id] = None
            else:
                self.draft[_pos_id] = int(val)
            self._rebuild_items()
            await interaction.response.edit_message(embed=self.build_page_embed(), view=self)

        select.callback = select_callback
        self.add_item(select)

        # Navigation
        total = len(positions)
        if self.page > 0:
            prev_btn = Button(label="← Previous", style=discord.ButtonStyle.secondary, row=1)
            async def prev_cb(interaction: discord.Interaction):
                await self._clear_desc_messages()
                self.page -= 1
                self._rebuild_items()
                await interaction.response.edit_message(embed=self.build_page_embed(), view=self)
            prev_btn.callback = prev_cb
            self.add_item(prev_btn)

        if self.page < total - 1:
            next_btn = Button(label="Next →", style=discord.ButtonStyle.secondary, row=1)
            async def next_cb(interaction: discord.Interaction):
                await self._clear_desc_messages()
                self.page += 1
                self._rebuild_items()
                await interaction.response.edit_message(embed=self.build_page_embed(), view=self)
            next_btn.callback = next_cb
            self.add_item(next_btn)

        summary_btn = Button(label="View Summary", style=discord.ButtonStyle.primary, row=1)
        async def summary_cb(interaction: discord.Interaction):
            await self._clear_desc_messages()
            self.page = "summary"
            self._rebuild_items()
            await interaction.response.edit_message(embed=self.build_page_embed(), view=self)
        summary_btn.callback = summary_cb
        self.add_item(summary_btn)

        # Show "📖 Full Descriptions" button only when descriptions overflow the field limit
        full_desc_lines = [
            f"**{c['name']}**\n{c['description']}"
            for c in cands if c["description"]
        ]
        if full_desc_lines:
            full_desc_text = f"**{pos['name']} — Candidate Descriptions**\n\n" + "\n\n".join(full_desc_lines)
            # Check using untruncated preview to decide if descriptions overflow
            preview_lines = []
            for i2, c in enumerate(cands):
                marker = "✅" if current == i2 else "◻️"
                line = f"{marker} **{c['name']}**"
                if c["description"]:
                    line += f"\n> {c['description']}"
                preview_lines.append(line)
            if len("\n\n".join(preview_lines)) > _FIELD_LIMIT:
                desc_btn = Button(
                    label="📖 Full Descriptions",
                    style=discord.ButtonStyle.secondary,
                    row=2,
                )

                async def desc_cb(
                    interaction: discord.Interaction,
                    _text: str = full_desc_text,
                ):
                    await interaction.response.defer()
                    await self._clear_desc_messages()  # replace any previous expansion
                    remaining = _text
                    while remaining:
                        chunk = remaining[:2000]
                        remaining = remaining[2000:]
                        m = await interaction.followup.send(chunk, wait=True)
                        self._desc_messages.append(m)

                    async def _delete_after():
                        await asyncio.sleep(120)
                        await self._clear_desc_messages()

                    self._desc_delete_task = asyncio.create_task(_delete_after())

                desc_btn.callback = desc_cb
                self.add_item(desc_btn)

    def _add_summary_items(self):
        positions = self.election_data["positions"]
        total = len(positions)

        # Jump to any position
        options = [
            discord.SelectOption(label=f"Edit: {pos['name'][:90]}", value=str(i))
            for i, pos in enumerate(positions)
        ]
        jump = Select(
            placeholder="Jump to a position...",
            custom_id="summary_jump",
            options=options[:25],
            min_values=1,
            max_values=1,
        )

        async def jump_cb(interaction: discord.Interaction):
            await self._clear_desc_messages()
            self.page = int(jump.values[0])
            self._rebuild_items()
            await interaction.response.edit_message(embed=self.build_page_embed(), view=self)

        jump.callback = jump_cb
        self.add_item(jump)

        # Block submit if required positions have no vote
        can_submit = all(
            self.draft.get(p["id"]) is not None or p["allow_abstain"]
            for p in positions
        )

        submit_btn = Button(
            label="Submit Ballot",
            style=discord.ButtonStyle.success if can_submit else discord.ButtonStyle.danger,
            disabled=not can_submit,
            row=1,
        )

        async def submit_cb(interaction: discord.Interaction):
            self.page = "confirm"
            self._rebuild_items()
            await interaction.response.edit_message(embed=self.build_page_embed(), view=self)

        submit_btn.callback = submit_cb
        self.add_item(submit_btn)

        back_btn = Button(label=f"← Back (position {total})", style=discord.ButtonStyle.secondary, row=1)
        async def back_cb(interaction: discord.Interaction):
            await self._clear_desc_messages()
            self.page = total - 1
            self._rebuild_items()
            await interaction.response.edit_message(embed=self.build_page_embed(), view=self)
        back_btn.callback = back_cb
        self.add_item(back_btn)

    def _add_confirm_items(self):
        positions = self.election_data["positions"]

        can_submit = all(
            self.draft.get(p["id"]) is not None or p["allow_abstain"]
            for p in positions
        )

        confirm_btn = Button(
            label="✅ Confirm & Submit",
            style=discord.ButtonStyle.success,
            disabled=not can_submit,
            row=0,
        )

        async def confirm_cb(interaction: discord.Interaction):
            await interaction.response.defer()
            await self.cog._submit_ballot(self, interaction)

        confirm_btn.callback = confirm_cb
        self.add_item(confirm_btn)

        back_btn = Button(label="← Go Back", style=discord.ButtonStyle.secondary, row=0)

        async def back_cb(interaction: discord.Interaction):
            await self._clear_desc_messages()
            self.page = "summary"
            self._rebuild_items()
            await interaction.response.edit_message(embed=self.build_page_embed(), view=self)

        back_btn.callback = back_cb
        self.add_item(back_btn)

    # ── embed builders ───────────────────────────────────────────────────────

    def build_page_embed(self) -> discord.Embed:
        if self.page == "summary":
            return self._build_summary_embed()
        elif self.page == "confirm":
            return self._build_confirm_embed()
        else:
            return self._build_position_embed()

    def _build_position_embed(self) -> discord.Embed:
        positions = self.election_data["positions"]
        pos = positions[self.page]
        total = len(positions)
        cands = pos["candidates"]
        current = self.draft.get(pos["id"])

        winners_label = _pluralise(pos["max_winners"], "winner")
        embed = discord.Embed(
            title=f"🗳️ {self.election_data['title']}",
            description=f"**Position {self.page + 1} of {total}: {pos['name']}**\n{pos['description']}",
            color=discord.Color.blurple(),
        )
        embed.add_field(
            name="ℹ️ How this works",
            value=f"Pick **one candidate**. The top {winners_label} by votes will win this position.",
            inline=False,
        )

        embed.add_field(
            name="Candidates",
            value=_candidates_field_value(cands, current, pos["allow_abstain"]),
            inline=False,
        )

        if current is not None:
            embed.set_footer(text=f"Your vote: {cands[current]['name']}")
        elif pos["allow_abstain"]:
            embed.set_footer(text="You are abstaining from this position.")
        else:
            embed.set_footer(text="⚠️ You must vote for this position.")

        return embed

    def _build_summary_embed(self) -> discord.Embed:
        positions = self.election_data["positions"]
        embed = discord.Embed(
            title=f"🗳️ {self.election_data['title']} — Ballot Summary",
            description="Review your selections below. You can jump back to any position to change your vote.",
            color=discord.Color.gold(),
        )

        has_error = False
        for pos in positions:
            pid = pos["id"]
            current = self.draft.get(pid)
            if current is not None:
                val = f"✅ {pos['candidates'][current]['name']}"
            elif pos["allow_abstain"]:
                val = "⬜ Abstaining"
            else:
                val = "❌ **No vote placed** — this position requires a vote"
                has_error = True
            embed.add_field(name=pos["name"], value=val, inline=False)

        if has_error:
            embed.set_footer(text="⚠️ You cannot submit until all required positions have a vote.")
        else:
            embed.set_footer(text="All positions filled. Press Submit Ballot to continue.")

        return embed

    def _build_confirm_embed(self) -> discord.Embed:
        positions = self.election_data["positions"]
        embed = discord.Embed(
            title=f"🗳️ {self.election_data['title']} — Confirm Ballot",
            description="**This action cannot be undone.** Once submitted your ballot cannot be changed.",
            color=discord.Color.green(),
        )

        has_warning = False
        for pos in positions:
            pid = pos["id"]
            current = self.draft.get(pid)
            if current is not None:
                val = f"✅ {pos['candidates'][current]['name']}"
            elif pos["allow_abstain"]:
                val = "⬜ Abstaining ⚠️"
                has_warning = True
            else:
                val = "❌ No vote placed — blocking submit"

            embed.add_field(name=pos["name"], value=val, inline=False)

        if has_warning:
            embed.set_footer(text="⚠️ You are abstaining from one or more positions.")
        else:
            embed.set_footer(text="Ready to submit.")

        return embed

    async def on_timeout(self):
        self.cog._active_sessions.pop((self.user_id, self.election_id), None)
        if self.dm_message:
            try:
                await self.dm_message.edit(
                    content="⏰ Your voting session timed out. Start again by pressing Vote Now.",
                    view=None,
                )
            except discord.HTTPException:
                pass


# ── Main Cog ─────────────────────────────────────────────────────────────────

class Elections(commands.Cog):
    """Anonymous elections with plurality voting, conducted via DMs."""

    def __init__(self, bot: Red):
        self.bot = bot
        self.config = Config.get_conf(self, identifier=7291836450)
        self.config.register_guild(elections={})
        # {(user_id, election_id): ElectionVoteView}
        self._active_sessions: Dict[Tuple[int, str], ElectionVoteView] = {}
        self._poller.start()

    async def cog_load(self):
        """Re-register persistent tracker views for all active elections after restart."""
        await self.bot.wait_until_ready()
        all_guilds = await self.config.all_guilds()
        for guild_id, gdata in all_guilds.items():
            for eid, edata in gdata.get("elections", {}).items():
                if edata.get("active") and not edata.get("ended"):
                    msg_id = edata.get("message_id")
                    if msg_id:
                        view = ElectionTrackerView(cog=self, election_id=eid)
                        self.bot.add_view(view, message_id=msg_id)

    def cog_unload(self):
        self._poller.cancel()

    # ── background poller ────────────────────────────────────────────────────

    @tasks.loop(minutes=1)
    async def _poller(self):
        now = datetime.now(timezone.utc).timestamp()
        all_guilds = await self.config.all_guilds()
        for guild_id, gdata in all_guilds.items():
            guild = self.bot.get_guild(guild_id)
            if guild is None:
                continue
            for eid, edata in gdata.get("elections", {}).items():
                if edata.get("active") and not edata.get("ended") and edata.get("end_time", 0) <= now:
                    await self._conclude_election(guild, eid)

    @_poller.before_loop
    async def _before_poller(self):
        await self.bot.wait_until_ready()

    # ── ballot submission ────────────────────────────────────────────────────

    async def _submit_ballot(self, vote_view: ElectionVoteView, interaction: discord.Interaction):
        eid = vote_view.election_id
        user_id = vote_view.user_id
        draft = vote_view.draft

        # Must resolve guild from election data (we stored guild_id in data)
        guild_id = vote_view.election_data.get("guild_id")
        guild = self.bot.get_guild(guild_id) if guild_id else None

        async with self.config.guild_from_id(guild_id).elections() as elections:
            edata = elections.get(eid)
            if not edata:
                await interaction.followup.send("Election no longer found.", ephemeral=True)
                self._active_sessions.pop((user_id, eid), None)
                return
            if not edata.get("active") or edata.get("ended"):
                await interaction.followup.send("This election has already ended.", ephemeral=True)
                self._active_sessions.pop((user_id, eid), None)
                return
            if user_id in edata.get("voters", []):
                await interaction.followup.send("You have already submitted a ballot.", ephemeral=True)
                self._active_sessions.pop((user_id, eid), None)
                return

            # Append ballot (anonymous)
            ballots = edata.setdefault("ballots", {})
            for pos in edata["positions"]:
                pid = pos["id"]
                choice = draft.get(pid)  # int or None
                ballots.setdefault(pid, []).append(choice)

            # Record voter (dedup only — no link to ballot data)
            edata.setdefault("voters", []).append(user_id)

        # Remove session
        self._active_sessions.pop((user_id, eid), None)

        # Disable the DM view
        vote_view.clear_items()
        try:
            await vote_view.dm_message.edit(
                embed=discord.Embed(
                    title="✅ Ballot Submitted",
                    description="Your vote has been recorded anonymously. Thank you for participating!",
                    color=discord.Color.green(),
                ),
                view=None,
            )
        except discord.HTTPException:
            pass

        # Update tracker message voter count
        await self._update_tracker(guild_id, eid)

    # ── tracker update ───────────────────────────────────────────────────────

    async def _update_tracker(self, guild_id: int, election_id: str):
        guild = self.bot.get_guild(guild_id)
        if guild is None:
            return
        edata = (await self.config.guild(guild).elections()).get(election_id)
        if not edata:
            return
        channel = guild.get_channel(edata.get("channel_id", 0))
        if channel is None:
            return
        msg_id = edata.get("message_id")
        if not msg_id:
            return
        try:
            msg = await channel.fetch_message(msg_id)
        except discord.HTTPException:
            return

        ended = edata.get("ended", False)
        voter_count = len(edata.get("voters", []))
        embed = _build_tracker_embed(edata, voter_count, ended, election_id)
        try:
            await msg.edit(embed=embed)
        except discord.HTTPException:
            pass

    # ── election conclusion ──────────────────────────────────────────────────

    async def _conclude_election(self, guild: discord.Guild, election_id: str):
        async with self.config.guild(guild).elections() as elections:
            edata = elections.get(election_id)
            if not edata or edata.get("ended"):
                return
            edata["active"] = False
            edata["ended"] = True
            snapshot = dict(edata)  # work from a copy

        await self._update_tracker(guild.id, election_id)
        await self._post_results(guild, election_id, snapshot)

    async def _post_results(self, guild: discord.Guild, election_id: str, edata: dict):
        channel = guild.get_channel(edata.get("channel_id", 0))
        if channel is None:
            return

        ballots_map = edata.get("ballots", {})
        voter_count = len(edata.get("voters", []))

        embed = discord.Embed(
            title=f"📊 Election Results: {edata['title']}",
            description=(
                f"The election has concluded. "
                f"{_pluralise(voter_count, 'voter')} participated.\n"
                f"Results are shown in placement order."
            ),
            color=discord.Color.gold(),
        )

        for pos in edata["positions"]:
            pid = pos["id"]
            pos_ballots: List[Optional[int]] = ballots_map.get(pid, [])
            winners, undecided, others = _tabulate(pos, pos_ballots)

            lines = []
            for cname, vcount, label in winners:
                lines.append(f"{label} **{cname}** — {_pluralise(vcount, 'vote')}")

            if undecided:
                tie_votes = undecided[0][1]
                tie_names = ", ".join(f"**{n}**" for n, _ in undecided)
                lines.append(
                    f"⚠️ **Undecided** — tied at {_pluralise(tie_votes, 'vote')}: {tie_names}"
                )

            if others:
                no_lines = [f"• {n} — {_pluralise(v, 'vote')}" for n, v in others]
                lines.append("__Not elected:__\n" + "\n".join(no_lines))

            field_value = "\n".join(lines) or "No votes cast."
            if len(field_value) > 1024:
                field_value = field_value[:1020] + "…"
            embed.add_field(
                name=f"**{pos['name']}**",
                value=field_value,
                inline=False,
            )

        embed.set_footer(text=f"Election ID: {election_id}")

        ping_role_id = edata.get("ping_role")
        try:
            mention = f"<@&{int(ping_role_id)}> " if ping_role_id is not None else ""
        except (TypeError, ValueError):
            mention = ""
        await channel.send(
            content=f"{mention}The election has ended!",
            embed=embed,
            allowed_mentions=discord.AllowedMentions(roles=True, everyone=False),
        )

    # ── commands ─────────────────────────────────────────────────────────────

    @commands.group(invoke_without_command=True)
    async def election(self, ctx: commands.Context):
        """Manage and participate in elections."""
        await ctx.send_help(ctx.command)

    @election.command(name="start")
    @commands.has_permissions(manage_guild=True)
    @commands.guild_only()
    async def election_start(self, ctx: commands.Context, channel: discord.TextChannel):
        """
        Start an election.  Attach the election JSON config file to this message.

        The JSON file should contain: title, end_time, allowed_roles, ping_role,
        allow_abstain, and a list of positions with candidates.
        """
        if not ctx.message.attachments:
            await ctx.send(
                "Please attach the election JSON config file to this message.\n"
                "See `[p]election example` for the config format."
            )
            return

        attachment = ctx.message.attachments[0]
        if not attachment.filename.endswith(".json"):
            await ctx.send("The attached file must be a `.json` file.")
            return

        if attachment.size > 1_000_000:  # 1 MB sanity limit
            await ctx.send("Config file is too large (max 1 MB).")
            return

        raw_bytes = await attachment.read()
        try:
            raw = json.loads(raw_bytes.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            await ctx.send(f"Failed to parse JSON: {e}")
            return

        try:
            cfg = _parse_config(raw, ctx.guild)
        except ValueError as e:
            await ctx.send(f"Config validation failed:\n{e}")
            return

        election_id = secrets.token_urlsafe(6)

        # Build initial ballot structure
        ballots = {pos["id"]: [] for pos in cfg["positions"]}

        edata = {
            "guild_id": ctx.guild.id,
            "channel_id": channel.id,
            "message_id": None,
            "title": cfg["title"],
            "description": cfg["description"],
            "end_time": cfg["end_time"],
            "allowed_roles": cfg["allowed_roles"],
            "ping_role": cfg["ping_role"],
            "allow_abstain": cfg["allow_abstain"],
            "positions": cfg["positions"],
            "ballots": ballots,
            "voters": [],
            "active": True,
            "ended": False,
        }

        # Post tracker embed
        tracker_embed = _build_tracker_embed(edata, 0, False, election_id)
        view = ElectionTrackerView(cog=self, election_id=election_id)
        tracker_msg = await channel.send(embed=tracker_embed, view=view)
        edata["message_id"] = tracker_msg.id

        async with self.config.guild(ctx.guild).elections() as elections:
            elections[election_id] = edata

        embed = discord.Embed(
            title="✅ Election Started",
            color=discord.Color.green(),
        )
        embed.add_field(name="Election ID", value=f"`{election_id}`", inline=True)
        embed.add_field(name="Tracker Channel", value=channel.mention, inline=True)
        embed.add_field(
            name="Ends",
            value=f"{_discord_ts(cfg['end_time'], 'F')} ({_discord_ts(cfg['end_time'], 'R')})",
            inline=False,
        )
        embed.add_field(name="Positions", value=str(len(cfg["positions"])), inline=True)
        await ctx.send(embed=embed)

    @election.command(name="join")
    async def election_join(self, ctx: commands.Context, election_id: str):
        """
        Join an election by its ID.  Works in DMs or server channels.
        Useful if the bot couldn't DM you automatically.
        """
        # Resolve which guild this election belongs to
        target_guild: Optional[discord.Guild] = None
        target_edata: Optional[dict] = None

        all_guilds = await self.config.all_guilds()
        for gid, gdata in all_guilds.items():
            if election_id in gdata.get("elections", {}):
                target_guild = self.bot.get_guild(gid)
                target_edata = gdata["elections"][election_id]
                break

        if target_guild is None or target_edata is None:
            await ctx.send("Election not found. Check the ID and try again.")
            return

        if not target_edata.get("active") or target_edata.get("ended"):
            await ctx.send("That election is no longer active.")
            return

        user = ctx.author

        # Role check — must be in the guild
        member = target_guild.get_member(user.id)
        if member is None:
            await ctx.send("You are not a member of the server running this election.")
            return

        allowed_roles = target_edata.get("allowed_roles", [])
        if allowed_roles:
            member_role_ids = {r.id for r in member.roles}
            if not member_role_ids.intersection(allowed_roles):
                await ctx.send("You don't have the required role to vote in this election.")
                return

        if user.id in target_edata.get("voters", []):
            await ctx.send("You have already submitted your ballot for this election.")
            return

        if (user.id, election_id) in self._active_sessions:
            await ctx.send("You already have an open voting session. Check your DMs.")
            return

        view = ElectionVoteView(
            cog=self,
            election_id=election_id,
            election_data=target_edata,
            user_id=user.id,
        )
        embed = view.build_page_embed()
        try:
            dm_msg = await user.send(embed=embed, view=view)
            view.dm_message = dm_msg
            self._active_sessions[(user.id, election_id)] = view
            if ctx.guild:
                await ctx.send("Check your DMs to cast your vote! 📬", ephemeral=True)
        except discord.Forbidden:
            await ctx.send(
                "I couldn't send you a DM. Please enable DMs from server members and try again."
            )

    @election.command(name="end")
    @commands.has_permissions(manage_guild=True)
    @commands.guild_only()
    async def election_end(self, ctx: commands.Context, election_id: str):
        """Force-end an election early and post results."""
        async with self.config.guild(ctx.guild).elections() as elections:
            edata = elections.get(election_id)
            if not edata:
                await ctx.send("Election not found.")
                return
            if edata.get("ended"):
                await ctx.send("That election has already ended.")
                return
            if edata.get("guild_id") != ctx.guild.id:
                await ctx.send("That election does not belong to this server.")
                return

        await self._conclude_election(ctx.guild, election_id)
        await ctx.send(f"Election `{election_id}` ended and results posted.")

    @election.command(name="list")
    @commands.guild_only()
    async def election_list(self, ctx: commands.Context):
        """List all active elections in this server."""
        elections = await self.config.guild(ctx.guild).elections()

        active = {eid: e for eid, e in elections.items() if e.get("active") and not e.get("ended")}
        if not active:
            await ctx.send("There are no active elections in this server right now.")
            return

        embed = discord.Embed(
            title="Active Elections",
            color=discord.Color.blurple(),
        )
        for eid, e in active.items():
            voter_count = len(e.get("voters", []))
            embed.add_field(
                name=e["title"],
                value=(
                    f"ID: `{eid}`\n"
                    f"Ends: {_discord_ts(e['end_time'], 'R')}\n"
                    f"Voters: {voter_count}\n"
                    f"Positions: {len(e['positions'])}"
                ),
                inline=True,
            )
        await ctx.send(embed=embed)

    @election.command(name="info")
    @commands.guild_only()
    async def election_info(self, ctx: commands.Context, election_id: str):
        """Show details about an election — positions, candidates, and voter count (no vote counts)."""
        elections = await self.config.guild(ctx.guild).elections()
        edata = elections.get(election_id)
        if not edata:
            await ctx.send("Election not found.")
            return

        voter_count = len(edata.get("voters", []))
        ended = edata.get("ended", False)

        embed = discord.Embed(
            title=edata["title"],
            description=edata.get("description") or "",
            color=discord.Color.green() if not ended else discord.Color.greyple(),
        )
        embed.add_field(name="Status", value="Ended" if ended else "Active", inline=True)
        embed.add_field(name="Voters", value=str(voter_count), inline=True)
        if not ended:
            embed.add_field(name="Ends", value=_discord_ts(edata["end_time"], "R"), inline=True)

        for pos in edata["positions"]:
            cand_names = ", ".join(c["name"] for c in pos["candidates"])
            winners_label = _pluralise(pos["max_winners"], "winner")
            embed.add_field(
                name=f"{pos['name']} ({winners_label})",
                value=cand_names,
                inline=False,
            )

        embed.set_footer(text=f"Election ID: {election_id}")
        await ctx.send(embed=embed)

    @election.command(name="example")
    async def election_example(self, ctx: commands.Context):
        """Send an example election config JSON."""
        example = {
            "title": "Server Staff Elections 2026",
            "description": "Vote for your server staff for the upcoming term.",
            "end_time": "72h",
            "allowed_roles": [123456789012345678],
            "ping_role": 987654321098765432,
            "allow_abstain": False,
            "positions": [
                {
                    "name": "Server President",
                    "description": "Leads the server and coordinates staff.",
                    "max_winners": 1,
                    "allow_abstain": False,
                    "candidates": [
                        {
                            "name": "Alice",
                            "description": "3 years of server experience. Wants to focus on community events."
                        },
                        {
                            "name": "Bob",
                            "description": "Moderated 5 servers. Plans to improve server rules."
                        }
                    ]
                },
                {
                    "name": "Community Managers",
                    "description": "Manage day-to-day community interaction. Top 2 are selected.",
                    "max_winners": 2,
                    "allow_abstain": True,
                    "candidates": [
                        {"name": "Charlie", "description": "Active daily. Great at conflict resolution."},
                        {"name": "Diana", "description": "Runs the weekly game nights."},
                        {"name": "Eve", "description": "Specializes in member onboarding."}
                    ]
                }
            ]
        }
        json_str = json.dumps(example, indent=2)
        # Send as a file if too long for a message
        if len(json_str) > 1900:
            await ctx.send(
                "Here's an example config:",
                file=discord.File(
                    fp=__import__("io").BytesIO(json_str.encode()),
                    filename="election_example.json",
                ),
            )
        else:
            await ctx.send(f"Example election config:\n```json\n{json_str}\n```")


# ── Shared embed builder ──────────────────────────────────────────────────────

def _build_tracker_embed(edata: dict, voter_count: int, ended: bool, election_id: str = "—") -> discord.Embed:
    color = discord.Color.greyple() if ended else discord.Color.blurple()
    status = "Ended" if ended else "Active"
    embed = discord.Embed(
        title=f"🗳️ {edata['title']} — {status}",
        description=edata.get("description") or "",
        color=color,
    )
    embed.add_field(name="Voters", value=str(voter_count), inline=True)
    if not ended:
        embed.add_field(
            name="Ends",
            value=f"{_discord_ts(edata['end_time'], 'F')}\n{_discord_ts(edata['end_time'], 'R')}",
            inline=True,
        )
    else:
        embed.add_field(name="Status", value="✅ Voting is closed. See results above.", inline=False)
    embed.add_field(
        name="Positions",
        value=", ".join(p["name"] for p in edata.get("positions", [])),
        inline=False,
    )
    embed.set_footer(text=f"Election ID: {election_id} | Votes are completely anonymous.")
    return embed
