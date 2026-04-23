"""行情工具注册模块。"""

from __future__ import annotations

from typing import Any, Optional

from dayu.contracts.tool_configs import MarketToolLimits
from dayu.engine.tool_contracts import ToolTruncateSpec
from dayu.engine.tool_registry import ToolRegistry
from dayu.engine.tools.base import tool
from dayu.log import Log

from .result_types import StockHistoryResult, StockQuoteResult
from .service import MarketToolService

MODULE = "MARKET.MARKET_TOOLS"
MARKET_TOOL_TAGS = frozenset({"market"})


def register_market_tools(
    registry: ToolRegistry,
    *,
    service: MarketToolService,
    limits: MarketToolLimits,
) -> None:
    """注册行情工具集合。

    Args:
        registry: ToolRegistry 实例。
        service: 预构建的 MarketToolService 实例。
        limits: 行情工具限制配置。

    Returns:
        无。

    Raises:
        ValueError: 配置非法时抛出。
    """

    tool_factories = [
        _create_get_stock_quote_tool,
        _create_get_stock_history_tool,
    ]

    for factory in tool_factories:
        name, func, schema = factory(registry, service, limits)
        registry.register(name, func, schema)

    Log.verbose(f"已注册 {len(tool_factories)} 个行情工具", module=MODULE)


def _create_get_stock_quote_tool(
    registry: ToolRegistry,
    service: MarketToolService,
    limits: MarketToolLimits,
) -> tuple[str, Any, Any]:
    """创建 ``get_stock_quote`` 工具。

    Args:
        registry: 工具注册表实例。
        service: 行情工具服务实例。
        limits: 行情工具限制配置。

    Returns:
        ``(tool_name, tool_callable, tool_schema)`` 三元组。

    Raises:
        ValueError: 工具 schema 非法时抛出。
    """

    del limits  # 当前 quote 工具无裁剪需求

    parameters = {
        "type": "object",
        "properties": {
            "ticker": {
                "type": "string",
                "description": "股票代码，直接传最自然的写法即可（如 600519、000001.SZ）。",
            },
        },
        "required": ["ticker"],
    }

    @tool(
        registry,
        name="get_stock_quote",
        description="获取股票最新行情报价，包括价格、涨跌幅、成交量等。",
        parameters=parameters,
        tags=MARKET_TOOL_TAGS,
    )
    def get_stock_quote(ticker: str) -> StockQuoteResult:
        """获取股票最新行情报价。

        Args:
            ticker: 股票代码。

        Returns:
            行情报价结果。

        Raises:
            ToolBusinessError: ticker 非法或数据源异常时抛出。
        """

        return service.get_stock_quote(ticker=ticker)

    return get_stock_quote.__tool_name__, get_stock_quote, get_stock_quote.__tool_schema__


def _create_get_stock_history_tool(
    registry: ToolRegistry,
    service: MarketToolService,
    limits: MarketToolLimits,
) -> tuple[str, Any, Any]:
    """创建 ``get_stock_history`` 工具。

    Args:
        registry: 工具注册表实例。
        service: 行情工具服务实例。
        limits: 行情工具限制配置。

    Returns:
        ``(tool_name, tool_callable, tool_schema)`` 三元组。

    Raises:
        ValueError: 工具 schema 非法时抛出。
    """

    del limits  # 裁剪逻辑在 service 层完成

    parameters = {
        "type": "object",
        "properties": {
            "ticker": {
                "type": "string",
                "description": "股票代码，直接传最自然的写法即可（如 600519、000001.SZ）。",
            },
            "start_date": {
                "type": "string",
                "description": "起始日期 YYYY-MM-DD。不传时默认最近 30 个自然日。",
            },
            "end_date": {
                "type": "string",
                "description": "结束日期 YYYY-MM-DD。不传时默认今天。",
            },
            "frequency": {
                "type": "string",
                "enum": ["daily", "weekly", "monthly"],
                "description": "K 线频率。不传时默认 daily。",
            },
        },
        "required": ["ticker"],
    }

    @tool(
        registry,
        name="get_stock_history",
        description=(
            "获取股票历史 K 线数据（开盘、最高、最低、收盘、成交量）。"
            "先用 get_stock_quote 确认股票代码正确，再查历史数据。"
        ),
        parameters=parameters,
        tags=MARKET_TOOL_TAGS,
        truncate=ToolTruncateSpec(
            enabled=True,
            strategy="list_items",
            limits={"max_items": 500},
            target_field="bars",
        ),
    )
    def get_stock_history(
        ticker: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        frequency: Optional[str] = None,
    ) -> StockHistoryResult:
        """获取股票历史 K 线数据。

        Args:
            ticker: 股票代码。
            start_date: 起始日期。
            end_date: 结束日期。
            frequency: K 线频率。

        Returns:
            历史 K 线结果。

        Raises:
            ToolBusinessError: 参数非法或数据源异常时抛出。
        """

        return service.get_stock_history(
            ticker=ticker,
            start_date=start_date,
            end_date=end_date,
            frequency=frequency,
        )

    return get_stock_history.__tool_name__, get_stock_history, get_stock_history.__tool_schema__


__all__ = [
    "register_market_tools",
]
