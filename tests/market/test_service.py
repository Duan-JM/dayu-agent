"""MarketToolService 单元测试。"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import pytest

from dayu.contracts.tool_configs import MarketToolLimits
from dayu.engine.tool_errors import ToolBusinessError
from dayu.market.models import (
    BarData,
    BarFrequency,
    MarketDataError,
    RealtimeQuoteData,
    MARKET_ERROR_DATA,
    MARKET_ERROR_INVALID_TICKER,
)
from dayu.market.providers.protocol import MarketDataProviderProtocol
from dayu.market.tools.service import MarketToolService, _normalize_ticker


# ---------------------------------------------------------------------------
# Fixtures & Helpers
# ---------------------------------------------------------------------------

_SAMPLE_QUOTE = RealtimeQuoteData(
    symbol="600519.SH",
    name="贵州茅台",
    price=1800.0,
    change=20.0,
    change_pct=1.12,
    open=1780.0,
    high=1810.0,
    low=1775.0,
    prev_close=1780.0,
    volume=5_000_000.0,
    amount=9_000_000_000.0,
    timestamp="2026-04-22T15:00:00+08:00",
)

_SAMPLE_BARS = [
    BarData(
        date="2026-04-20",
        open=1770.0,
        high=1790.0,
        low=1765.0,
        close=1780.0,
        volume=4_500_000.0,
        amount=8_000_000_000.0,
    ),
    BarData(
        date="2026-04-21",
        open=1780.0,
        high=1800.0,
        low=1775.0,
        close=1790.0,
        volume=4_800_000.0,
        amount=8_500_000_000.0,
    ),
    BarData(
        date="2026-04-22",
        open=1790.0,
        high=1810.0,
        low=1785.0,
        close=1800.0,
        volume=5_000_000.0,
        amount=9_000_000_000.0,
    ),
]


class FakeProvider:
    """测试用 provider 桩。"""

    def __init__(
        self,
        *,
        quote: RealtimeQuoteData | None = None,
        bars: list[BarData] | None = None,
        error: MarketDataError | None = None,
    ) -> None:
        self._quote = quote or _SAMPLE_QUOTE
        self._bars = bars if bars is not None else _SAMPLE_BARS
        self._error = error
        self.call_log: list[dict[str, object]] = []

    def get_realtime_quote(self, symbol: str) -> RealtimeQuoteData:
        """桩：返回预设 quote 或抛出异常。"""
        self.call_log.append({"method": "get_realtime_quote", "symbol": symbol})
        if self._error:
            raise self._error
        return self._quote

    def get_history_bars(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        frequency: BarFrequency,
    ) -> list[BarData]:
        """桩：返回预设 bars 或抛出异常。"""
        self.call_log.append({
            "method": "get_history_bars",
            "symbol": symbol,
            "start_date": start_date,
            "end_date": end_date,
            "frequency": frequency,
        })
        if self._error:
            raise self._error
        return self._bars


def _build_service(
    *,
    provider: FakeProvider | None = None,
    limits: MarketToolLimits | None = None,
) -> tuple[MarketToolService, FakeProvider]:
    """构建测试用 service。"""
    p = provider or FakeProvider()
    lim = limits or MarketToolLimits()
    return MarketToolService(provider=p, limits=lim), p


# ---------------------------------------------------------------------------
# _normalize_ticker 测试
# ---------------------------------------------------------------------------

class TestNormalizeTicker:
    """ticker 标准化测试。"""

    def test_pure_digits_sh(self) -> None:
        """6 开头纯数字 -> .SH。"""
        assert _normalize_ticker("600519") == "600519.SH"

    def test_pure_digits_sz_0(self) -> None:
        """0 开头纯数字 -> .SZ。"""
        assert _normalize_ticker("000001") == "000001.SZ"

    def test_pure_digits_sz_3(self) -> None:
        """3 开头纯数字 -> .SZ。"""
        assert _normalize_ticker("300750") == "300750.SZ"

    def test_pure_digits_bj_4(self) -> None:
        """4 开头纯数字 -> .BJ。"""
        assert _normalize_ticker("430047") == "430047.BJ"

    def test_pure_digits_bj_8(self) -> None:
        """8 开头纯数字 -> .BJ。"""
        assert _normalize_ticker("830799") == "830799.BJ"

    def test_with_suffix_upper(self) -> None:
        """带大写后缀 -> 直接使用。"""
        assert _normalize_ticker("600519.SH") == "600519.SH"

    def test_with_suffix_lower(self) -> None:
        """带小写后缀 -> 转大写。"""
        assert _normalize_ticker("600519.sh") == "600519.SH"

    def test_with_whitespace(self) -> None:
        """前后有空格 -> 去空格后标准化。"""
        assert _normalize_ticker("  600519  ") == "600519.SH"

    def test_invalid_length(self) -> None:
        """非 6 位数字 -> 报错。"""
        with pytest.raises(ToolBusinessError) as exc_info:
            _normalize_ticker("12345")
        assert exc_info.value.code == MARKET_ERROR_INVALID_TICKER

    def test_invalid_chinese_name(self) -> None:
        """中文名称 -> 报错。"""
        with pytest.raises(ToolBusinessError) as exc_info:
            _normalize_ticker("贵州茅台")
        assert exc_info.value.code == MARKET_ERROR_INVALID_TICKER

    def test_invalid_prefix(self) -> None:
        """首位不对应已知交易所 -> 报错。"""
        with pytest.raises(ToolBusinessError) as exc_info:
            _normalize_ticker("999999")
        assert exc_info.value.code == MARKET_ERROR_INVALID_TICKER


# ---------------------------------------------------------------------------
# get_stock_quote 测试
# ---------------------------------------------------------------------------

class TestGetStockQuote:
    """get_stock_quote 服务层测试。"""

    def test_happy_path(self) -> None:
        """正常获取行情报价。"""
        service, provider = _build_service()
        result = service.get_stock_quote(ticker="600519")

        assert result["symbol"] == "600519.SH"
        assert result["name"] == "贵州茅台"
        assert result["price"] == 1800.0
        assert result["currency"] == "CNY"
        assert result["market"] == "SH"
        assert len(provider.call_log) == 1
        assert provider.call_log[0]["symbol"] == "600519.SH"

    def test_with_suffix(self) -> None:
        """带后缀的 ticker 也能正常工作。"""
        service, provider = _build_service()
        result = service.get_stock_quote(ticker="600519.SH")
        assert result["symbol"] == "600519.SH"

    def test_provider_error_converted(self) -> None:
        """provider 异常被转换为 ToolBusinessError。"""
        error = MarketDataError("网络超时", code=MARKET_ERROR_DATA)
        provider = FakeProvider(error=error)
        service, _ = _build_service(provider=provider)

        with pytest.raises(ToolBusinessError) as exc_info:
            service.get_stock_quote(ticker="600519")
        assert exc_info.value.code == MARKET_ERROR_DATA

    def test_invalid_ticker(self) -> None:
        """非法 ticker 直接报错，不调用 provider。"""
        service, provider = _build_service()

        with pytest.raises(ToolBusinessError) as exc_info:
            service.get_stock_quote(ticker="INVALID")
        assert exc_info.value.code == MARKET_ERROR_INVALID_TICKER
        assert len(provider.call_log) == 0


# ---------------------------------------------------------------------------
# get_stock_history 测试
# ---------------------------------------------------------------------------

class TestGetStockHistory:
    """get_stock_history 服务层测试。"""

    def test_happy_path(self) -> None:
        """正常获取历史 K 线。"""
        service, provider = _build_service()
        result = service.get_stock_history(
            ticker="600519",
            start_date="2026-04-20",
            end_date="2026-04-22",
        )

        assert result["symbol"] == "600519.SH"
        assert result["frequency"] == "daily"
        assert result["total_bars"] == 3
        assert result["truncated"] is False
        assert result["currency"] == "CNY"
        assert result["market"] == "SH"
        assert len(result["bars"]) == 3

    def test_default_dates(self) -> None:
        """不传日期时使用默认值。"""
        service, provider = _build_service()
        result = service.get_stock_history(ticker="600519")

        assert len(provider.call_log) == 1
        call = provider.call_log[0]
        assert call["start_date"] is not None
        assert call["end_date"] is not None

    def test_frequency_weekly(self) -> None:
        """传 weekly 频率。"""
        service, _ = _build_service()
        result = service.get_stock_history(
            ticker="600519",
            frequency="weekly",
        )
        assert result["frequency"] == "weekly"

    def test_frequency_invalid(self) -> None:
        """非法频率 -> 报错。"""
        service, _ = _build_service()
        with pytest.raises(ToolBusinessError) as exc_info:
            service.get_stock_history(ticker="600519", frequency="hourly")
        assert "不支持的频率" in exc_info.value.message

    def test_frequency_empty_string(self) -> None:
        """空白频率 -> 报错。"""
        service, _ = _build_service()
        with pytest.raises(ToolBusinessError) as exc_info:
            service.get_stock_history(ticker="600519", frequency="  ")
        assert "不能为空" in exc_info.value.message

    def test_truncation(self) -> None:
        """超出限制时截断。"""
        # 生成 10 条 bars，限制为 5
        bars = [
            BarData(
                date=f"2026-04-{i:02d}",
                open=100.0, high=101.0, low=99.0, close=100.5,
                volume=1000.0, amount=100000.0,
            )
            for i in range(1, 11)
        ]
        provider = FakeProvider(bars=bars)
        limits = MarketToolLimits(history_max_bars=5)
        service, _ = _build_service(provider=provider, limits=limits)

        result = service.get_stock_history(ticker="600519")
        assert result["truncated"] is True
        assert result["total_bars"] == 5
        # 截断保留最近的 5 条
        assert result["bars"][0]["date"] == "2026-04-06"

    def test_empty_result(self) -> None:
        """空结果正常返回。"""
        provider = FakeProvider(bars=[])
        service, _ = _build_service(provider=provider)

        result = service.get_stock_history(ticker="600519")
        assert result["total_bars"] == 0
        assert result["truncated"] is False
        assert result["bars"] == []

    def test_provider_error_converted(self) -> None:
        """provider 异常被转换为 ToolBusinessError。"""
        error = MarketDataError("连接超时", code=MARKET_ERROR_DATA)
        provider = FakeProvider(error=error)
        service, _ = _build_service(provider=provider)

        with pytest.raises(ToolBusinessError) as exc_info:
            service.get_stock_history(ticker="600519")
        assert exc_info.value.code == MARKET_ERROR_DATA
