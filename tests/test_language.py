import pytest

from mnp.normalize.language import detect_cjk_language, is_off_language


@pytest.mark.parametrize(
    "text, expected",
    [
        ("Bitcoin ETF saw net inflow of 1,383 BTC today", None),
        ("", None),
        ("1,383 / 23,436", None),
        ("现货黄金突破4200美元关口", "zh"),
        # Latin tickers and names inside a Chinese headline
        ("分析：巨鲸资金回流迹象显现，Binance稳定币流入量较低点增长超40%", "zh"),  # noqa: RUF001
        ("Polymarket上线新的用户保护及信任与安全计划", "zh"),
        ("ビットコインが最高値を更新", "ja"),
        ("비트코인 사상 최고가 경신", "ko"),
        # An English item quoting a name in Chinese stays English
        ("Jiang Zhuoer (江卓尔) says the ETH staking exit queue hit a 2026 high", None),
    ],
)
def test_detect_cjk_language(text, expected):
    assert detect_cjk_language(text) == expected


def test_is_off_language():
    assert is_off_language("现货黄金突破4200美元关口", "en") == "zh"
    assert is_off_language("现货黄金突破4200美元关口", "en-US") == "zh"
    assert is_off_language("现货黄金突破4200美元关口", "zh") is None
    assert is_off_language("现货黄金突破4200美元关口", "zh-Hans") is None
    assert is_off_language("Gold breaks $4,200", "en") is None
