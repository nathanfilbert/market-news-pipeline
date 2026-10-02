"""Script-based language detection: enough to tell CJK text from the Latin-script languages.

Sources declare one language (sources.yaml), but some feeds mix in items in another one: about
1 in 10 items in PANews's English feed is in Chinese. A share of CJK characters among the
letters separates them cleanly (English items have none, Chinese ones well over half).
"""

import re

_HAN = re.compile(r"[㐀-䶿一-鿿豈-﫿]")
_KANA = re.compile(r"[぀-ヿ]")
_HANGUL = re.compile(r"[ᄀ-ᇿ가-힯]")
# Chinese items carry tickers and names in Latin script ("Binance稳定币流入量…" is ~0.85);
# English items quoting a name in Chinese stay far below this.
CJK_SHARE = 0.2

_CJK_LANGUAGES = {"zh", "ja", "ko"}


def detect_cjk_language(text: str) -> str | None:
    """'ja', 'ko' or 'zh' if at least CJK_SHARE of the letters are CJK, else None."""
    letters = sum(c.isalpha() for c in text)
    if not letters:
        return None
    kana, hangul, han = (len(p.findall(text)) for p in (_KANA, _HANGUL, _HAN))
    if (kana + hangul + han) / letters < CJK_SHARE:
        return None
    if kana:
        return "ja"
    return "ko" if hangul > han else "zh"


def is_off_language(text: str, source_language: str) -> str | None:
    """The detected language if `text` is CJK and the source's language isn't, else None."""
    if source_language.split("-")[0].lower() in _CJK_LANGUAGES:
        return None
    return detect_cjk_language(text)
