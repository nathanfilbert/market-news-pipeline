from mnp.normalize.hashing import content_hash
from mnp.normalize.text import clean_text

URL = "https://a.com/x"


def h(headline="Title", summary="Summary", body=None, url=URL):
    return content_hash(headline=headline, summary=summary, body=body, canonical_url=url)


def test_stable_and_hex():
    assert h() == h()
    assert len(h()) == 64


def test_each_field_matters():
    base = h()
    assert h(headline="Title 2") != base
    assert h(summary="Summary 2") != base
    assert h(body="Body") != base
    assert h(url="https://a.com/y") != base


def test_none_and_empty_are_equal():
    assert h(body=None) == h(body="")


def test_field_boundaries_are_unambiguous():
    assert h(headline="a b", summary="c") != h(headline="a", summary="b c")


def test_fetch_noise_does_not_change_hash():
    # The same content with different markup/whitespace normalizes to the same hash.
    a = h(headline=clean_text("<b>Bitcoin</b>  tops $100k"), summary=clean_text("<p>Up 5%</p>"))
    b = h(headline=clean_text("Bitcoin tops $100k\n"), summary=clean_text("Up&nbsp;5%"))
    assert a == b
