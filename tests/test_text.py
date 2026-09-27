import unicodedata

import pytest

from mnp.normalize.text import clean_text


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("<p>Bitcoin <b>rises</b></p><p>Second para</p>", "Bitcoin rises Second para"),
        ("Line one<br>Line two<br/>three", "Line one Line two three"),
        ("Fees &amp; spreads &#8212; &lt;tight&gt; &nbsp;now", "Fees & spreads — <tight> now"),
        ("  lots \n\t of   space  ", "lots of space"),
        ("a<script>alert(1)</script>b<style>p{}</style>c", "abc"),
        ("<img src='x.png'>Caption", "Caption"),
        ("", None),
        ("<p> </p>", None),
        (None, None),
    ],
)
def test_clean_text(raw, expected):
    assert clean_text(raw) == expected


def test_nfc_normalization():
    decomposed = "Marchés"  # e + combining acute
    result = clean_text(decomposed)
    assert result == "Marchés"
    assert unicodedata.is_normalized("NFC", result)


def test_unit_separator_is_collapsed():
    # Keeps content_hash field boundaries unambiguous.
    assert clean_text("a\x1fb") == "a b"
