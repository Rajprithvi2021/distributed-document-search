"""Text analysis shared by the in-memory backend.

It mirrors the Elasticsearch analyzer defined in ``app.search.elasticsearch``
(standard tokenizer → lowercase → asciifolding → English stopwords →
``minimal_english`` stemmer), so both backends tokenise, match and highlight
the same way.
"""

from __future__ import annotations

import html
import re
import unicodedata
from collections.abc import Iterator
from functools import lru_cache

_TOKEN_RE = re.compile(r"\w+", re.UNICODE)

# Lucene's default English stopword set (ES "_english_").
STOPWORDS = frozenset(
    "a an and are as at be but by for if in into is it no not of on or such that the their then there "
    "these they this to was will with".split()
)


def _fold(token: str) -> str:
    """Lowercase and strip accents (ES ``asciifolding``)."""
    decomposed = unicodedata.normalize("NFKD", token.lower())
    return "".join(ch for ch in decomposed if not unicodedata.combining(ch))


def minimal_stem(term: str) -> str:
    """Port of Lucene ``EnglishMinimalStemmer`` (plural folding only).

    queries → query, caches → cache, systems → system; status and class are unchanged.
    """
    n = len(term)
    if n < 3 or term[-1] != "s":
        return term
    prev = term[-2]
    if prev in ("u", "s"):
        return term
    if prev == "e":
        if n > 3 and term[-3] == "i" and term[-4] not in ("a", "e"):
            return term[:-3] + "y"
        if term[-3] in ("i", "a", "o", "e"):
            return term
    return term[:-1]


@lru_cache(maxsize=131_072)  # pure function over a bounded vocabulary: memoise (≈2x faster search)
def analyze_token(raw: str) -> str | None:
    folded = _fold(raw)
    if not folded or folded in STOPWORDS:
        return None
    return minimal_stem(folded)


def analyze(text: str) -> list[str]:
    return [term for raw in _TOKEN_RE.findall(text) if (term := analyze_token(raw))]


def analyze_with_offsets(text: str) -> Iterator[tuple[str, int, int]]:
    for match in _TOKEN_RE.finditer(text):
        term = analyze_token(match.group())
        if term:
            yield term, match.start(), match.end()


def auto_fuzziness(term: str) -> int:
    """ES ``fuzziness: AUTO``: 0 edits for 1-2 chars, 1 for 3-5, 2 for longer terms."""
    if len(term) <= 2:
        return 0
    if len(term) <= 5:
        return 1
    return 2


def levenshtein(a: str, b: str, max_distance: int) -> int:
    """Edit distance with early exit; returns ``max_distance + 1`` once exceeded."""
    if abs(len(a) - len(b)) > max_distance:
        return max_distance + 1
    previous = list(range(len(b) + 1))
    for i, ca in enumerate(a, start=1):
        current = [i]
        row_min = i
        for j, cb in enumerate(b, start=1):
            value = min(previous[j] + 1, current[j - 1] + 1, previous[j - 1] + (ca != cb))
            current.append(value)
            row_min = min(row_min, value)
        if row_min > max_distance:
            return max_distance + 1
        previous = current
    return previous[-1]


def highlight_fragments(
    text: str,
    terms: set[str],
    *,
    fragment_size: int = 160,
    max_fragments: int = 3,
    pre_tag: str = "<em>",
    post_tag: str = "</em>",
) -> list[str]:
    """Return HTML-escaped fragments with matched terms wrapped in tags.

    Escaping matches ES ``encoder: html``. Document text is user-supplied, so
    highlights must be safe to render as HTML. ``max_fragments=0`` highlights
    the whole field as one fragment (ES ``number_of_fragments: 0``).
    """
    matches = [(start, end) for term, start, end in analyze_with_offsets(text) if term in terms]
    if not matches:
        return []

    def render(frag_start: int, frag_end: int) -> str:
        pieces: list[str] = []
        cursor = frag_start
        for m_start, m_end in matches:
            if m_start < frag_start or m_end > frag_end:
                continue
            pieces.append(html.escape(text[cursor:m_start]))
            pieces.append(f"{pre_tag}{html.escape(text[m_start:m_end])}{post_tag}")
            cursor = m_end
        pieces.append(html.escape(text[cursor:frag_end]))
        return "".join(pieces).strip()

    if max_fragments == 0:
        return [render(0, len(text))]

    fragments: list[str] = []
    covered_until = -1
    for start, _ in matches:
        if start < covered_until:
            continue
        frag_start = max(0, start - fragment_size // 4)
        if frag_start > 0:
            space = text.find(" ", frag_start, start)
            frag_start = space + 1 if space != -1 else frag_start
        frag_end = min(len(text), frag_start + fragment_size)
        if frag_end < len(text):
            space = text.rfind(" ", start, frag_end)
            frag_end = space if space > start else frag_end
        fragments.append(render(frag_start, frag_end))
        covered_until = frag_end
        if len(fragments) >= max_fragments:
            break
    return fragments


def leading_snippet(text: str, size: int = 160) -> str:
    """Escaped prefix of the text, cut at a word boundary (ES ``no_match_size``)."""
    if len(text) <= size:
        return html.escape(text.strip())
    cut = text.rfind(" ", 0, size)
    return html.escape(text[: cut if cut > 0 else size].strip())
