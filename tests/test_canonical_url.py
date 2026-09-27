import pytest

from mnp.normalize.canonical_url import canonicalize_url


@pytest.mark.parametrize(
    ("url", "expected"),
    [
        # host and scheme are case-insensitive; path is not
        ("HTTPS://WWW.CoinDesk.com/Markets/Story", "https://www.coindesk.com/Markets/Story"),
        # tracking parameters removed, others kept
        (
            "https://cointelegraph.com/news/x?utm_source=rss_feed&utm_medium=rss&utm_campaign=p",
            "https://cointelegraph.com/news/x",
        ),
        ("https://a.com/x?id=5&fbclid=abc&gclid=1&ref=twitter", "https://a.com/x?id=5"),
        ("https://a.com/x?UTM_Source=feed&page=2", "https://a.com/x?page=2"),
        # remaining parameters sorted, blanks kept
        ("https://a.com/x?b=2&a=1&empty=", "https://a.com/x?a=1&b=2&empty="),
        # fragments dropped
        ("https://a.com/x#comments", "https://a.com/x"),
        # default ports dropped, others kept
        ("https://a.com:443/x", "https://a.com/x"),
        ("http://a.com:80/x", "http://a.com/x"),
        ("https://a.com:8443/x", "https://a.com:8443/x"),
        # empty path becomes "/", trailing slashes preserved
        ("https://a.com", "https://a.com/"),
        ("https://a.com/x/", "https://a.com/x/"),
        # trailing dot in host, surrounding whitespace
        ("  https://a.com./x  ", "https://a.com/x"),
        # redirect wrappers resolved when the target is in the query string
        (
            "https://www.google.com/url?q=https://news.example.com/a%3Futm_source%3Dx&sa=D",
            "https://news.example.com/a",
        ),
        (
            "https://l.facebook.com/l.php?u=https%3A%2F%2Fnews.example.com%2Fb&h=AT0",
            "https://news.example.com/b",
        ),
        # a wrapper without a usable target is left alone (minus tracking)
        ("https://www.google.com/url?sa=D", "https://www.google.com/url?sa=D"),
    ],
)
def test_canonicalize_url(url, expected):
    assert canonicalize_url(url) == expected


def test_idempotent():
    url = "HTTPS://Example.com:443/a/b?z=1&utm_campaign=x&a=2#frag"
    once = canonicalize_url(url)
    assert canonicalize_url(once) == once
