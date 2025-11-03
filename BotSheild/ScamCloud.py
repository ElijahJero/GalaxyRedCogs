import re
from typing import Tuple, Dict

# Analyze a message string against a wordlist mapping token->score.
# token == "tld" is treated specially: it matches occurrences of dot + alphabetic TLDs (e.g. ".com", ".io").
# Matching is case-insensitive. For normal tokens we match whole-word boundaries.
def analyze_text(content: str, wordlist: Dict[str, float]) -> Tuple[float, Dict[str, int]]:
    """
    Returns (total_score, matches) where matches is a dict token -> count matched.
    """
    if not content or not wordlist:
        return 0.0, {}

    total = 0.0
    matches = {}

    text = content  # leave original; regex will use flags for case-insensitive

    # pre-compile tld regex
    tld_re = re.compile(r"\.[a-z]{2,}(?=$|\b|[\/:\?])", flags=re.I)

    for token, score in wordlist.items():
        if token is None:
            continue
        key = str(token).lower()
        try:
            s = float(score)
        except Exception:
            continue

        if key == "tld":
            found = tld_re.findall(text)
            count = len(found)
            if count:
                total += s * count
                matches[key] = matches.get(key, 0) + count
        else:
            # whole-word match, case-insensitive
            # allow tokens that may include punctuation/spaces by escaping
            pattern = r"\b" + re.escape(key) + r"\b"
            found = re.findall(pattern, text, flags=re.I)
            count = len(found)

            # If no whole-word matches, also check if any word in the text starts or ends with the token.
            # We extract simple word tokens (alphanumeric + underscore) and perform lower-case starts/ends checks.
            # Each word counts at most once per token (even if both starts and ends).
            if count == 0:
                extra = 0
                try:
                    words = re.findall(r"\b\w+\b", text, flags=re.I)
                    for w in words:
                        lw = w.lower()
                        if lw == key:
                            # exact match would have been caught, but guard anyway
                            continue
                        if lw.startswith(key) or lw.endswith(key):
                            extra += 1
                except Exception:
                    extra = 0
                count += extra

            if count:
                total += s * count
                matches[key] = matches.get(key, 0) + count

    return float(total), matches
