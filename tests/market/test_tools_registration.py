"""行情工具注册测试。"""

from __future__ import annotations

import pytest

from dayu.contracts.tool_configs import MarketToolLimits
from dayu.engine.tool_registry import ToolRegistry
from dayu.market.models import (
    BarData,
    BarFrequency,
    RealtimeQuoteData,
)
from dayu.market.tools.market_tools import register_market_tools
from dayu.market.tools.service import MarketToolService


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

class _StubProvider:
    """工具注册测试用极简 provider。"""

    def get_realtime_quote(self, symbol: str) -> RealtimeQuoteData:
        """返回固定数据。"""
        return RealtimeQuoteData(
            symbol=symbol, name="测试", price=100.0,
            change=1.0, change_pct=1.0, open=99.0,
            high=101.0, low=98.0, prev_close=99.0,
            volume=1000.0, amount=100000.0,
            timestamp="2026-04-22T15:00:00+08:00",
        )

    def get_history_bars(
        self, symbol: str, start_date: str, end_date: str,
        frequency: BarFrequency,
    ) -> list[BarData]:
        """返回空列表。"""
        return []


class TestRegisterMarketTools:
    """工具注册测试。"""

    def test_registers_two_tools(self) -> None:
        """注册后应包含 get_stock_quote 和 get_stock_history。"""
        registry = ToolRegistry()
        provider = _StubProvider()
        limits = MarketToolLimits()
        service = MarketToolService(provider=provider, limits=limits)

        register_market_tools(registry, service=service, limits=limits)

        assert "get_stock_quote" in registry.tools
        assert "get_stock_history" in registry.tools

    def test_tool_schemas_valid(self) -> None:
        """工具 schema 包含必要字段。"""
        registry = ToolRegistry()
        provider = _StubProvider()
        limits = MarketToolLimits()
        service = MarketToolService(provider=provider, limits=limits)

        register_market_tools(registry, service=service, limits=limits)

        # 验证 schema 存在
        for tool_name in ("get_stock_quote", "get_stock_history"):
            assert tool_name in registry.tool_schemas
            schema = registry.tool_schemas[tool_name]
            assert schema.function.name == tool_name
            assert schema.function.description
            assert "properties" in schema.function.parameters
