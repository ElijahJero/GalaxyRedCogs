import re
from typing import Tuple, Dict

# Analyze a message string against a wordlist mapping token->score.
# token == "tld" is treated specially: it matches occurrences of dot + alphabetic TLDs (e.g. ".com", ".io").
# Matching is case-insensitive. For normal tokens we match whole-word boundaries.
def analyze_text(content: str, wordlist: Dict[str, float]) -> Tuple[float, Dict[str, int]]:
    """
    Returns (total_score, matches) where matches is a dict token -> count matched.

    Policy changes:
    - Each token is counted at most once per message (0/1), even if it appears multiple times.
    - Matching is made span-based to avoid overlapping detectors double-counting the same substring.
    """
    if not content or not wordlist:
        return 0.0, {}

    text = content

    _RX_FLAGS = re.IGNORECASE | re.VERBOSE

    url_re = re.compile(
        r"""
        \b(
            (?:https?://|www\.)\S+
            |
            (?:[a-z0-9-]+\.)+[a-z]{2,}(?:/[^\s]*)?
        )
        """,
        _RX_FLAGS,
    )
    email_re = re.compile(
        r"""
        \b[a-z0-9._%+-]+@(?:[a-z0-9-]+\.)+[a-z]{2,}\b
        """,
        _RX_FLAGS,
    )

    tld_re = re.compile(
        r"""
        (?i)
        (?<![@.])              # not preceded by '@' or another dot
        (?<=[a-z0-9-])         # REQUIRE at least one hostname char before the dot (avoids ".command")
        \.[a-z]{2,}            # dot + tld
        (?=(?:\b|/|:|\?|$))    # common URL boundaries
        """,
        _RX_FLAGS,
    )

    def _add_span(spans, start: int, end: int) -> bool:
        """Add span if it doesn't overlap an existing span. Returns True if added."""
        for s, e in spans:
            if start < e and end > s:
                return False
        spans.append((start, end))
        return True

    total = 0.0
    matches: Dict[str, int] = {}

    # Track accepted spans globally to prevent double-flagging the same substring via different tokens.
    accepted_spans = []

    # First, gather URL/email spans (if the caller configured tokens for them).
    # Tokens are optional; if not in wordlist, they won't contribute score.
    lowered_keys = {str(k).lower(): k for k in wordlist.keys() if k is not None}

    for special_key, regex in (("url", url_re), ("email", email_re)):
        if special_key in lowered_keys:
            for m in regex.finditer(text):
                if _add_span(accepted_spans, m.start(), m.end()):
                    # count each token at most once
                    matches[special_key] = 1
                    break

    # Now process configured tokens.
    for token, score in wordlist.items():
        if token is None:
            continue
        key = str(token).lower()
        try:
            s = float(score)
        except Exception:
            continue

        # already handled explicitly above
        if key in ("url", "email"):
            continue

        counted = False

        if key == "tld":
            for m in tld_re.finditer(text):
                if _add_span(accepted_spans, m.start(), m.end()):
                    matches[key] = 1
                    counted = True
                    break
        else:
            # Prefer whole-word match.
            pattern = re.compile(r"\b" + re.escape(key) + r"\b", flags=re.I)
            for m in pattern.finditer(text):
                if _add_span(accepted_spans, m.start(), m.end()):
                    matches[key] = 1
                    counted = True
                    break

            # Fallback: startswith/endswith on \w+ tokens, but still count at most once.
            if not counted:
                try:
                    for m in re.finditer(r"\b\w+\b", text, flags=re.I):
                        w = m.group(0)
                        lw = w.lower()
                        if lw == key:
                            continue
                        if lw.startswith(key) or lw.endswith(key):
                            if _add_span(accepted_spans, m.start(), m.end()):
                                matches[key] = 1
                                counted = True
                                break
                except Exception:
                    pass

        if counted:
            total += s  # 0/1 per token

    return float(total), matches
