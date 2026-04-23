"""Market toolset adapter。"""

from __future__ import annotations

from typing import cast

from dayu.contracts.tool_configs import MarketToolLimits, build_market_tool_limits
from dayu.contracts.toolset_registrar import ToolsetRegistrationContext
from dayu.engine.tool_registry import ToolRegistry
from dayu.log import Log
from dayu.market.providers.tushare_provider import TushareProvider
from dayu.market.tools.market_tools import register_market_tools
from dayu.market.tools.service import MarketToolService

MODULE = "MARKET.TOOLSET_REGISTRARS"


def register_market_toolset(context: ToolsetRegistrationContext) -> int:
    """注册 market 行情 toolset。

    当前默认使用 tushare provider。后续 akshare 合入后可通过配置切换。

    Args:
        context: toolset 注册上下文。

    Returns:
        实际注册的工具数量。

    Raises:
        无。
    """

    limits = build_market_tool_limits(context.toolset_config)
    provider = TushareProvider()
    service = MarketToolService(provider=provider, limits=limits)

    before_count = len(context.registry.tools)
    register_market_tools(
        cast(ToolRegistry, context.registry),
        service=service,
        limits=limits,
    )
    registered = len(context.registry.tools) - before_count
    Log.verbose(f"market toolset 注册完成，共 {registered} 个工具", module=MODULE)
    return registered


__all__ = [
    "register_market_toolset",
]
