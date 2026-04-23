"""AkshareProvider 单元测试。

使用 mock 替代真实 akshare API 调用。
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch
import pytest

from dayu.market.models import (
    BarFrequency,
    MarketDataError,
    MARKET_ERROR_DATA,
    MARKET_ERROR_DEPENDENCY,
)
from dayu.market.providers.akshare_provider import (
    AkshareProvider,
    _build_market_timestamp,
    _strip_exchange_suffix,
)


# ---------------------------------------------------------------------------
# 辅助工具
# ---------------------------------------------------------------------------

def _make_spot_df(rows: list[dict[str, object]]) -> MagicMock:
    """构造模拟实时行情 DataFrame。"""
    import pandas as pd
    return pd.DataFrame(rows)


def _make_hist_df(rows: list[dict[str, object]]) -> MagicMock:
    """构造模拟历史 K 线 DataFrame。"""
    import pandas as pd
    return pd.DataFrame(rows)


def _make_empty_df() -> MagicMock:
    """构造空 DataFrame。"""
    import pandas as pd
    return pd.DataFrame()


# ---------------------------------------------------------------------------
# 辅助函数测试
# ---------------------------------------------------------------------------

class TestHelpers:
    """辅助函数测试。"""

    def test_strip_exchange_suffix(self) -> None:
        """去除交易所后缀。"""
        assert _strip_exchange_suffix("600519.SH") == "600519"
        assert _strip_exchange_suffix("000001.SZ") == "000001"

    def test_build_market_timestamp(self) -> None:
        """生成 ISO 8601 时间戳。"""
        ts = _build_market_timestamp()
        assert "T" in ts
        assert "+08:00" in ts


# ---------------------------------------------------------------------------
# AkshareProvider 测试
# ---------------------------------------------------------------------------

class TestAkshareProvider:
    """AkshareProvider 单元测试（全部 mock）。"""

    @patch("dayu.market.providers.akshare_provider._ensure_akshare_available")
    def test_get_realtime_quote_happy(
        self,
        mock_ensure: MagicMock,
    ) -> None:
        """正常获取实时行情。"""
        mock_ak = MagicMock()
        mock_ensure.return_value = mock_ak

        mock_ak.stock_zh_a_spot_em.return_value = _make_spot_df([
            {
                "代码": "600519",
                "名称": "贵州茅台",
                "最新价": 1800.0,
                "涨跌额": 20.0,
                "涨跌幅": 1.12,
                "今开": 1780.0,
                "最高": 1810.0,
                "最低": 1775.0,
                "昨收": 1780.0,
                "成交量": 5000000.0,
                "成交额": 9000000000.0,
            },
            {
                "代码": "000001",
                "名称": "平安银行",
                "最新价": 12.0,
                "涨跌额": 0.1,
                "涨跌幅": 0.84,
                "今开": 11.9,
                "最高": 12.1,
                "最低": 11.85,
                "昨收": 11.9,
                "成交量": 100000000.0,
                "成交额": 1200000000.0,
            },
        ])

        provider = AkshareProvider()
        quote = provider.get_realtime_quote("600519.SH")

        assert quote.symbol == "600519.SH"
        assert quote.name == "贵州茅台"
        assert quote.price == 1800.0
        assert quote.change == 20.0
        assert quote.volume == 5000000.0

    @patch("dayu.market.providers.akshare_provider._ensure_akshare_available")
    def test_get_realtime_quote_not_found(
        self,
        mock_ensure: MagicMock,
    ) -> None:
        """股票代码不在结果中时抛出 MarketDataError。"""
        mock_ak = MagicMock()
        mock_ensure.return_value = mock_ak

        mock_ak.stock_zh_a_spot_em.return_value = _make_spot_df([
            {"代码": "000001", "名称": "平安银行", "最新价": 12.0},
        ])

        provider = AkshareProvider()
        with pytest.raises(MarketDataError) as exc_info:
            provider.get_realtime_quote("600519.SH")
        assert exc_info.value.code == MARKET_ERROR_DATA

    @patch("dayu.market.providers.akshare_provider._ensure_akshare_available")
    def test_get_realtime_quote_empty(
        self,
        mock_ensure: MagicMock,
    ) -> None:
        """空结果时抛出 MarketDataError。"""
        mock_ak = MagicMock()
        mock_ensure.return_value = mock_ak
        mock_ak.stock_zh_a_spot_em.return_value = _make_empty_df()

        provider = AkshareProvider()
        with pytest.raises(MarketDataError) as exc_info:
            provider.get_realtime_quote("600519.SH")
        assert exc_info.value.code == MARKET_ERROR_DATA

    @patch("dayu.market.providers.akshare_provider._ensure_akshare_available")
    def test_get_realtime_quote_with_none_values(
        self,
        mock_ensure: MagicMock,
    ) -> None:
        """DataFrame 含 None 值时安全降级为 0。"""
        mock_ak = MagicMock()
        mock_ensure.return_value = mock_ak

        mock_ak.stock_zh_a_spot_em.return_value = _make_spot_df([{
            "代码": "600519",
            "名称": "贵州茅台",
            "最新价": 1800.0,
            "涨跌额": None,
            "涨跌幅": None,
            "今开": None,
            "最高": None,
            "最低": None,
            "昨收": None,
            "成交量": None,
            "成交额": None,
        }])

        provider = AkshareProvider()
        quote = provider.get_realtime_quote("600519.SH")

        assert quote.price == 1800.0
        assert quote.change == 0.0
        assert quote.open == 0.0

    @patch("dayu.market.providers.akshare_provider._ensure_akshare_available")
    def test_get_history_bars_happy(
        self,
        mock_ensure: MagicMock,
    ) -> None:
        """正常获取历史 K 线。"""
        mock_ak = MagicMock()
        mock_ensure.return_value = mock_ak

        mock_ak.stock_zh_a_hist.return_value = _make_hist_df([
            {
                "日期": "2026-04-21",
                "开盘": 1780.0, "最高": 1800.0, "最低": 1775.0,
                "收盘": 1790.0, "成交量": 4800000.0, "成交额": 8500000000.0,
            },
            {
                "日期": "2026-04-22",
                "开盘": 1790.0, "最高": 1810.0, "最低": 1785.0,
                "收盘": 1800.0, "成交量": 5000000.0, "成交额": 9000000000.0,
            },
        ])

        provider = AkshareProvider()
        bars = provider.get_history_bars(
            symbol="600519.SH",
            start_date="2026-04-21",
            end_date="2026-04-22",
            frequency=BarFrequency.DAILY,
        )

        assert len(bars) == 2
        assert bars[0].date == "2026-04-21"
        assert bars[1].close == 1800.0

        # 验证调用参数
        mock_ak.stock_zh_a_hist.assert_called_once_with(
            symbol="600519",
            period="daily",
            start_date="20260421",
            end_date="20260422",
            adjust="",
        )

    @patch("dayu.market.providers.akshare_provider._ensure_akshare_available")
    def test_get_history_bars_empty(
        self,
        mock_ensure: MagicMock,
    ) -> None:
        """空结果时返回空列表。"""
        mock_ak = MagicMock()
        mock_ensure.return_value = mock_ak
        mock_ak.stock_zh_a_hist.return_value = _make_empty_df()

        provider = AkshareProvider()
        bars = provider.get_history_bars(
            symbol="600519.SH",
            start_date="2026-04-21",
            end_date="2026-04-22",
            frequency=BarFrequency.DAILY,
        )
        assert bars == []

    @patch("dayu.market.providers.akshare_provider._ensure_akshare_available")
    def test_get_history_bars_weekly(
        self,
        mock_ensure: MagicMock,
    ) -> None:
        """周线频率传递正确参数。"""
        mock_ak = MagicMock()
        mock_ensure.return_value = mock_ak
        mock_ak.stock_zh_a_hist.return_value = _make_empty_df()

        provider = AkshareProvider()
        provider.get_history_bars(
            symbol="600519.SH",
            start_date="2026-01-01",
            end_date="2026-04-22",
            frequency=BarFrequency.WEEKLY,
        )

        mock_ak.stock_zh_a_hist.assert_called_once_with(
            symbol="600519",
            period="weekly",
            start_date="20260101",
            end_date="20260422",
            adjust="",
        )

    @patch("dayu.market.providers.akshare_provider._ensure_akshare_available")
    def test_get_history_bars_with_datetime_dates(
        self,
        mock_ensure: MagicMock,
    ) -> None:
        """akshare 返回 datetime 对象时正确格式化。"""
        from datetime import date as date_cls
        mock_ak = MagicMock()
        mock_ensure.return_value = mock_ak

        mock_ak.stock_zh_a_hist.return_value = _make_hist_df([{
            "日期": date_cls(2026, 4, 22),
            "开盘": 1790.0, "最高": 1810.0, "最低": 1785.0,
            "收盘": 1800.0, "成交量": 5000000.0, "成交额": 9000000000.0,
        }])

        provider = AkshareProvider()
        bars = provider.get_history_bars(
            symbol="600519.SH",
            start_date="2026-04-22",
            end_date="2026-04-22",
            frequency=BarFrequency.DAILY,
        )

        assert len(bars) == 1
        assert bars[0].date == "2026-04-22"

    def test_akshare_not_installed(self) -> None:
        """akshare 未安装时抛出 MarketDataError。"""
        provider = AkshareProvider()
        # 强制清除缓存
        provider._ak = None

        with patch("dayu.market.providers.akshare_provider._ensure_akshare_available") as mock_ensure:
            mock_ensure.side_effect = MarketDataError(
                "akshare 未安装", code=MARKET_ERROR_DEPENDENCY,
            )
            with pytest.raises(MarketDataError) as exc_info:
                provider.get_realtime_quote("600519.SH")
            assert exc_info.value.code == MARKET_ERROR_DEPENDENCY
