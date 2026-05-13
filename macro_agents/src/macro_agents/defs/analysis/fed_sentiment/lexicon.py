"""Monetary policy sentiment lexicon and dictionary-based scoring.

Keyword weights range from 0.0 (weak signal) to 1.0 (strong signal).
Net sentiment is computed as (hawkish - dovish) / max(hawkish + dovish, 1),
yielding a score from -1.0 (extremely dovish) to +1.0 (extremely hawkish).
"""

import re

# --- Hawkish keywords: signals of tighter monetary policy ---
HAWKISH_KEYWORDS: dict[str, float] = {
    # Inflation concern
    "inflation": 0.3,
    "inflationary": 0.5,
    "overheating": 0.8,
    "price pressures": 0.6,
    "price stability": 0.4,
    "upside risks to inflation": 0.9,
    "above target": 0.6,
    "elevated inflation": 0.7,
    # Policy tightening
    "tighten": 0.8,
    "tightening": 0.8,
    "restrictive": 0.7,
    "sufficiently restrictive": 0.8,
    "raise rates": 0.9,
    "rate increase": 0.8,
    "further firming": 0.7,
    "remove accommodation": 0.8,
    "normalize": 0.5,
    "normalization": 0.5,
    # Labor market tightness
    "tight labor market": 0.5,
    "labor shortage": 0.5,
    "wage pressures": 0.6,
    "wage growth": 0.4,
    # Growth/demand concern
    "strong demand": 0.4,
    "robust growth": 0.4,
    "overheated": 0.7,
    # Vigilance language
    "vigilant": 0.6,
    "attentive to inflation risks": 0.7,
    "prepared to adjust": 0.5,
    "data do not support": 0.5,
    "premature": 0.6,
}

# --- Dovish keywords: signals of looser monetary policy ---
DOVISH_KEYWORDS: dict[str, float] = {
    # Accommodation
    "accommodate": 0.7,
    "accommodative": 0.7,
    "supportive": 0.5,
    "support the economy": 0.6,
    "support growth": 0.5,
    "stimulus": 0.7,
    # Policy easing
    "ease": 0.8,
    "easing": 0.8,
    "cut rates": 0.9,
    "rate cut": 0.9,
    "rate reduction": 0.8,
    "lower rates": 0.7,
    "reduce the target": 0.8,
    # Inflation minimization
    "transitory": 0.6,
    "temporary": 0.4,
    "well anchored": 0.5,
    "anchored expectations": 0.5,
    "below target": 0.6,
    "subdued inflation": 0.6,
    "disinflation": 0.5,
    # Labor market slack
    "slack": 0.6,
    "underemployment": 0.5,
    "maximum employment": 0.4,
    "labor market weakness": 0.6,
    # Downside risk
    "downside risks": 0.6,
    "headwinds": 0.5,
    "uncertainty": 0.3,
    "global risks": 0.4,
    "fragile": 0.5,
    "soft landing": 0.4,
    # Patience language
    "patient": 0.6,
    "gradual": 0.5,
    "data-dependent": 0.3,
    "wait and see": 0.5,
    "appropriate": 0.2,
    "monitoring": 0.2,
}

# --- Phrases explicitly tracked for frequency reporting ---
TRACKED_PHRASES: list[str] = [
    "transitory",
    "data-dependent",
    "data dependent",
    "appropriate",
    "patient",
    "gradual",
    "substantial progress",
    "maximum employment",
    "price stability",
    "soft landing",
    "restrictive",
    "accommodative",
    "prepared to adjust",
    "inflation expectations",
    "labor market",
    "financial conditions",
]


def score_text(text: str) -> dict:
    """Score a text block for hawkish/dovish sentiment using keyword matching.

    Returns:
        dict with keys: hawkish_score, dovish_score, net_sentiment_score,
        keyword_counts, total_hawkish_keywords, total_dovish_keywords
    """
    text_lower = text.lower()

    hawkish_total = 0.0
    dovish_total = 0.0
    hawkish_hits = 0
    dovish_hits = 0
    keyword_counts: dict[str, int] = {}

    for keyword, weight in HAWKISH_KEYWORDS.items():
        count = _count_phrase(text_lower, keyword)
        if count > 0:
            hawkish_total += weight * count
            hawkish_hits += count
            keyword_counts[keyword] = count

    for keyword, weight in DOVISH_KEYWORDS.items():
        count = _count_phrase(text_lower, keyword)
        if count > 0:
            dovish_total += weight * count
            dovish_hits += count
            keyword_counts[keyword] = count

    # Track specific phrases
    for phrase in TRACKED_PHRASES:
        if phrase not in keyword_counts:
            count = _count_phrase(text_lower, phrase)
            if count > 0:
                keyword_counts[phrase] = count

    denominator = hawkish_total + dovish_total
    net = (hawkish_total - dovish_total) / denominator if denominator > 0 else 0.0

    # Normalize raw scores to 0-1 range using total keyword weight as denominator
    max_possible_h = sum(HAWKISH_KEYWORDS.values())
    max_possible_d = sum(DOVISH_KEYWORDS.values())

    return {
        "hawkish_score": round(min(hawkish_total / max_possible_h, 1.0), 3),
        "dovish_score": round(min(dovish_total / max_possible_d, 1.0), 3),
        "net_sentiment_score": round(net, 3),
        "keyword_counts": keyword_counts,
        "total_hawkish_keywords": hawkish_hits,
        "total_dovish_keywords": dovish_hits,
    }


def extract_key_phrases(text: str, max_phrases: int = 20) -> list[dict]:
    """Find keyword occurrences with surrounding context.

    Returns list of dicts: {phrase, context, sentiment, weight}
    """
    text_lower = text.lower()
    results: list[dict] = []

    all_keywords = [(kw, w, "hawkish") for kw, w in HAWKISH_KEYWORDS.items()] + [
        (kw, w, "dovish") for kw, w in DOVISH_KEYWORDS.items()
    ]

    # Sort by weight descending so we keep the strongest signals
    all_keywords.sort(key=lambda x: x[1], reverse=True)

    for keyword, weight, sentiment in all_keywords:
        if len(results) >= max_phrases:
            break
        for match in _make_pattern(keyword).finditer(text_lower):
            start = max(0, match.start() - 60)
            end = min(len(text), match.end() + 60)
            context = text[start:end].strip()
            if start > 0:
                context = "..." + context
            if end < len(text):
                context = context + "..."
            results.append(
                {
                    "phrase": keyword,
                    "context": context,
                    "sentiment": sentiment,
                    "weight": weight,
                }
            )
            if len(results) >= max_phrases:
                break

    return results


def _make_pattern(phrase: str) -> re.Pattern:
    """Build a regex pattern with word boundaries for a phrase.

    Multi-word phrases get boundaries at the start and end.
    Single-word terms get strict word boundaries to avoid
    matching substrings (e.g. 'ease' should not match 'please').
    """
    escaped = re.escape(phrase)
    return re.compile(rf"\b{escaped}\b")


def _count_phrase(text_lower: str, phrase: str) -> int:
    """Count non-overlapping occurrences of a phrase in lowercased text."""
    return len(_make_pattern(phrase).findall(text_lower))
