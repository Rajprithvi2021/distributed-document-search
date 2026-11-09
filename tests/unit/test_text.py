import pytest

from app.search.text import (
    analyze,
    auto_fuzziness,
    highlight_fragments,
    leading_snippet,
    levenshtein,
    minimal_stem,
)


def test_analyze_lowercases_folds_accents_drops_stopwords_and_stems() -> None:
    assert analyze("The Café QUERIES of Distributed Systems") == ["cafe", "query", "distributed", "system"]


@pytest.mark.parametrize(
    ("word", "expected"),
    [
        ("queries", "query"),
        ("caches", "cache"),
        ("systems", "system"),
        ("status", "status"),
        ("class", "class"),
        ("goes", "goes"),
        ("as", "as"),
        ("search", "search"),
    ],
)
def test_minimal_stem_matches_lucene_english_minimal(word: str, expected: str) -> None:
    assert minimal_stem(word) == expected


def test_levenshtein_with_early_exit() -> None:
    assert levenshtein("distibuted", "distributed", 2) == 1
    assert levenshtein("kitten", "sitting", 3) == 3
    assert levenshtein("abc", "xyz", 1) == 2  # exceeded -> max_distance + 1
    assert levenshtein("a", "abcdef", 2) == 3  # length gap alone exceeds


@pytest.mark.parametrize(("term", "edits"), [("ab", 0), ("redis", 1), ("elasticsearch", 2)])
def test_auto_fuzziness_follows_elasticsearch_auto(term: str, edits: int) -> None:
    assert auto_fuzziness(term) == edits


def test_highlights_are_html_escaped() -> None:
    text = "<script>alert(1)</script> distributed search & caching"
    fragments = highlight_fragments(text, {"distributed"})
    assert fragments == ["&lt;script&gt;alert(1)&lt;/script&gt; <em>distributed</em> search &amp; caching"]
    assert "<script>" not in fragments[0]


def test_highlight_matches_analyzed_forms() -> None:
    fragments = highlight_fragments("Several Queries hit the caches", {"query", "cache"})
    assert fragments == ["Several <em>Queries</em> hit the <em>caches</em>"]


def test_whole_field_highlight_and_no_match() -> None:
    title = "A very long title about distributed search engines"
    assert highlight_fragments(title, {"search"}, max_fragments=0) == [
        "A very long title about distributed <em>search</em> engines"
    ]
    assert highlight_fragments(title, {"nothing"}) == []


def test_fragments_respect_size_and_count() -> None:
    text = " ".join(["filler"] * 200 + ["needle"] + ["filler"] * 200 + ["needle"] + ["filler"] * 200)
    fragments = highlight_fragments(text, {"needle"}, fragment_size=80, max_fragments=3)
    assert len(fragments) == 2
    assert all("<em>needle</em>" in f and len(f) <= 80 + len("<em></em>") for f in fragments)


def test_leading_snippet_cuts_at_word_boundary() -> None:
    assert leading_snippet("alpha beta gamma delta", size=12) == "alpha beta"
    assert leading_snippet("short", size=100) == "short"
