"""TushareProvider 单元测试。

使用 mock 替代真实 tushare API 调用。
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch
import pytest

from dayu.market.models import (
    BarFrequency,
    MarketDataError,
    MARKET_ERROR_CONFIG,
    MARKET_ERROR_DATA,
    MARKET_ERROR_DEPENDENCY,
)
from dayu.market.providers.tushare_provider import (
    TushareProvider,
    _format_trade_date,
    _format_trade_date_short,
)


# ---------------------------------------------------------------------------
# 辅助工具
# ---------------------------------------------------------------------------

def _make_daily_df(rows: list[dict[str, object]]) -> MagicMock:
    """构造模拟 DataFrame。"""
    import pandas as pd
    return pd.DataFrame(rows)


def _make_empty_df() -> MagicMock:
    """构造空 DataFrame。"""
    import pandas as pd
    return pd.DataFrame()


# ---------------------------------------------------------------------------
# _format_trade_date 测试
# ---------------------------------------------------------------------------

class TestFormatTradeDate:
    """日期格式转换测试。"""

    def test_yyyymmdd_to_iso(self) -> None:
        """YYYYMMDD -> ISO 8601。"""
        assert _format_trade_date("20260422") == "2026-04-22T15:00:00+08:00"

    def test_passthrough(self) -> None:
        """非 8 位字符串原样返回。"""
        assert _format_trade_date("2026-04-22") == "2026-04-22"

    def test_short_format(self) -> None:
        """YYYYMMDD -> YYYY-MM-DD。"""
        assert _format_trade_date_short("20260422") == "2026-04-22"


# ---------------------------------------------------------------------------
# TushareProvider 测试
# ---------------------------------------------------------------------------

class TestTushareProvider:
    """TushareProvider 单元测试（全部 mock）。"""

    def test_missing_token_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """未设置 token 且 tushare 未安装时抛出 MarketDataError。"""
        monkeypatch.delenv("TUSHARE_TOKEN", raising=False)
        provider = TushareProvider(token="")

        with pytest.raises(MarketDataError) as exc_info:
            provider.get_realtime_quote("600519.SH")
        # 可能是 DEPENDENCY_ERROR（tushare 未安装）或 CONFIG_ERROR（token 缺失）
        assert exc_info.value.code in (MARKET_ERROR_CONFIG, MARKET_ERROR_DEPENDENCY)

    @patch("dayu.market.providers.tushare_provider._ensure_tushare_available")
    def test_get_realtime_quote_happy(
        self,
        mock_ensure: MagicMock,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """正常获取实时行情。"""
        mock_ts = MagicMock()
        mock_ensure.return_value = mock_ts

        mock_api = MagicMock()
        mock_ts.set_token = MagicMock()
        mock_ts.pro_api.return_value = mock_api

        # daily 返回
        mock_api.daily.return_value = _make_daily_df([{
            "trade_date": "20260422",
            "open": 1780.0,
            "high": 1810.0,
            "low": 1775.0,
            "close": 1800.0,
            "change": 20.0,
            "pct_chg": 1.12,
            "pre_close": 1780.0,
            "vol": 50000.0,
            "amount": 9000000.0,
        }])

        # namechange 返回
        mock_api.namechange.return_value = _make_daily_df([{
            "name": "贵州茅台",
        }])

        provider = TushareProvider(token="test_token")
        quote = provider.get_realtime_quote("600519.SH")

        assert quote.symbol == "600519.SH"
        assert quote.name == "贵州茅台"
        assert quote.price == 1800.0
        assert quote.volume == 50000.0 * 100  # 手 -> 股
        assert quote.amount == 9000000.0 * 1000  # 千元 -> 元

    @patch("dayu.market.providers.tushare_provider._ensure_tushare_available")
    def test_get_realtime_quote_empty(
        self,
        mock_ensure: MagicMock,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """空结果时抛出 MarketDataError。"""
        mock_ts = MagicMock()
        mock_ensure.return_value = mock_ts
        mock_api = MagicMock()
        mock_ts.set_token = MagicMock()
        mock_ts.pro_api.return_value = mock_api
        mock_api.daily.return_value = _make_empty_df()

        provider = TushareProvider(token="test_token")

        with pytest.raises(MarketDataError) as exc_info:
            provider.get_realtime_quote("600519.SH")
        assert exc_info.value.code == MARKET_ERROR_DATA

    @patch("dayu.market.providers.tushare_provider._ensure_tushare_available")
    def test_get_realtime_quote_with_none_values(
        self,
        mock_ensure: MagicMock,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """DataFrame 含 None 值时不崩溃，安全降级为 0。"""
        mock_ts = MagicMock()
        mock_ensure.return_value = mock_ts
        mock_api = MagicMock()
        mock_ts.set_token = MagicMock()
        mock_ts.pro_api.return_value = mock_api

        mock_api.daily.return_value = _make_daily_df([{
            "trade_date": "20260422",
            "open": None,
            "high": 1810.0,
            "low": None,
            "close": 1800.0,
            "change": None,
            "pct_chg": None,
            "pre_close": None,
            "vol": None,
            "amount": None,
        }])
        mock_api.namechange.return_value = _make_empty_df()

        provider = TushareProvider(token="test_token")
        quote = provider.get_realtime_quote("600519.SH")

        assert quote.price == 1800.0
        assert quote.open == 0.0  # None -> 0.0
        assert quote.low == 0.0
        assert quote.volume == 0.0
        assert quote.amount == 0.0

    @patch("dayu.market.providers.tushare_provider._ensure_tushare_available")
    def test_get_history_bars_happy(
        self,
        mock_ensure: MagicMock,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """正常获取历史 K 线。"""
        mock_ts = MagicMock()
        mock_ensure.return_value = mock_ts
        mock_api = MagicMock()
        mock_ts.set_token = MagicMock()
        mock_ts.pro_api.return_value = mock_api

        # tushare 默认降序返回
        mock_api.daily.return_value = _make_daily_df([
            {
                "trade_date": "20260422",
                "open": 1790.0, "high": 1810.0, "low": 1785.0,
                "close": 1800.0, "vol": 50000.0, "amount": 9000000.0,
            },
            {
                "trade_date": "20260421",
                "open": 1780.0, "high": 1800.0, "low": 1775.0,
                "close": 1790.0, "vol": 48000.0, "amount": 8500000.0,
            },
        ])

        provider = TushareProvider(token="test_token")
        bars = provider.get_history_bars(
            symbol="600519.SH",
            start_date="2026-04-21",
            end_date="2026-04-22",
            frequency=BarFrequency.DAILY,
        )

        assert len(bars) == 2
        # 应该升序
        assert bars[0].date == "2026-04-21"
        assert bars[1].date == "2026-04-22"

    @patch("dayu.market.providers.tushare_provider._ensure_tushare_available")
    def test_get_history_bars_weekly(
        self,
        mock_ensure: MagicMock,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """周线频率调用 weekly 接口。"""
        mock_ts = MagicMock()
        mock_ensure.return_value = mock_ts
        mock_api = MagicMock()
        mock_ts.set_token = MagicMock()
        mock_ts.pro_api.return_value = mock_api
        mock_api.weekly.return_value = _make_empty_df()

        provider = TushareProvider(token="test_token")
        bars = provider.get_history_bars(
            symbol="600519.SH",
            start_date="2026-01-01",
            end_date="2026-04-22",
            frequency=BarFrequency.WEEKLY,
        )

        assert bars == []
        mock_api.weekly.assert_called_once()

    @patch("dayu.market.providers.tushare_provider._ensure_tushare_available")
    def test_get_history_bars_monthly(
        self,
        mock_ensure: MagicMock,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """月线频率调用 monthly 接口。"""
        mock_ts = MagicMock()
        mock_ensure.return_value = mock_ts
        mock_api = MagicMock()
        mock_ts.set_token = MagicMock()
        mock_ts.pro_api.return_value = mock_api
        mock_api.monthly.return_value = _make_empty_df()

        provider = TushareProvider(token="test_token")
        bars = provider.get_history_bars(
            symbol="600519.SH",
            start_date="2026-01-01",
            end_date="2026-04-22",
            frequency=BarFrequency.MONTHLY,
        )

        assert bars == []
        mock_api.monthly.assert_called_once()
