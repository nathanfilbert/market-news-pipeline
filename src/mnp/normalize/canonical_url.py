"""Canonical URLs: the article identity key."""

from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

# Query parameters that only track the click, never select content.
TRACKING_PARAMS = {
    "fbclid", "gclid", "dclid", "gbraid", "wbraid", "msclkid", "yclid", "igshid", "twclid",
    "mc_cid", "mc_eid", "_hsenc", "_hsmi", "mkt_tok", "ref", "ref_src", "ref_url",
    "cmpid", "ncid", "sr_share", "__twitter_impression", "s_cid", "soc_src", "soc_trk",
}  # fmt: skip
TRACKING_PREFIXES = ("utm_", "__cf_", "pk_", "mtm_")

# Redirect wrappers that carry the target in a query parameter (no network needed).
REDIRECT_WRAPPERS = {
    ("www.google.com", "/url"): ("q", "url"),
    ("google.com", "/url"): ("q", "url"),
    ("l.facebook.com", "/l.php"): ("u",),
    ("lm.facebook.com", "/l.php"): ("u",),
    ("out.reddit.com", ""): ("url",),
}

DEFAULT_PORTS = {"http": 80, "https": 443}


def _is_tracking(name: str) -> bool:
    name = name.lower()
    return name in TRACKING_PARAMS or name.startswith(TRACKING_PREFIXES)


def _unwrap(url: str, max_hops: int = 3) -> str:
    for _ in range(max_hops):
        parts = urlsplit(url)
        host = (parts.hostname or "").lower()
        for (wrapper_host, path_prefix), params in REDIRECT_WRAPPERS.items():
            if host == wrapper_host and parts.path.startswith(path_prefix):
                query = dict(parse_qsl(parts.query))
                target = next((query[p] for p in params if query.get(p)), None)
                if target and urlsplit(target).scheme in ("http", "https"):
                    url = target
                    break
        else:
            return url
    return url


def canonicalize_url(url: str) -> str:
    """Lowercase scheme/host, drop default ports, tracking params and fragments.

    Remaining query parameters are sorted so their order doesn't matter. Path case and
    trailing slashes are preserved: servers may treat them as different resources.
    """
    url = _unwrap(url.strip())
    parts = urlsplit(url)
    scheme = parts.scheme.lower()
    host = (parts.hostname or "").lower().rstrip(".")
    netloc = host
    if parts.port and parts.port != DEFAULT_PORTS.get(scheme):
        netloc = f"{host}:{parts.port}"
    if parts.username:
        userinfo = parts.username + (f":{parts.password}" if parts.password else "")
        netloc = f"{userinfo}@{netloc}"
    path = parts.path or ("/" if scheme in DEFAULT_PORTS else "")
    query = sorted(
        (k, v) for k, v in parse_qsl(parts.query, keep_blank_values=True) if not _is_tracking(k)
    )
    return urlunsplit((scheme, netloc, path, urlencode(query), ""))
