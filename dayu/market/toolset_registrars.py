"""Market toolset adapter。"""

from __future__ import annotations

import os
from typing import cast

from dayu.contracts.tool_configs import build_market_tool_limits
from dayu.contracts.toolset_registrar import ToolsetRegistrationContext
from dayu.engine.tool_registry import ToolRegistry
from dayu.log import Log
from dayu.market.providers.protocol import MarketDataProviderProtocol
from dayu.market.tools.market_tools import register_market_tools
from dayu.market.tools.service import MarketToolService

MODULE = "MARKET.TOOLSET_REGISTRARS"

# 支持的 provider 名称
PROVIDER_TUSHARE = "tushare"
PROVIDER_AKSHARE = "akshare"
_DEFAULT_PROVIDER = PROVIDER_TUSHARE


def _build_provider(provider_name: str) -> MarketDataProviderProtocol:
    """按名称构建 provider 实例。

    通过 ``DAYU_MARKET_PROVIDER`` 环境变量或显式参数选择数据源。

    Args:
        provider_name: provider 名称（``"tushare"`` 或 ``"akshare"``）。

    Returns:
        对应的 provider 实例。

    Raises:
        ValueError: 不支持的 provider 名称时抛出。
    """

    normalized = provider_name.strip().lower()
    if normalized == PROVIDER_TUSHARE:
        from dayu.market.providers.tushare_provider import TushareProvider
        return TushareProvider()
    if normalized == PROVIDER_AKSHARE:
        from dayu.market.providers.akshare_provider import AkshareProvider
        return AkshareProvider()
    raise ValueError(
        f"不支持的 market provider: {provider_name!r}，"
        f"可选值: {PROVIDER_TUSHARE}, {PROVIDER_AKSHARE}"
    )


def register_market_toolset(context: ToolsetRegistrationContext) -> int:
    """注册 market 行情 toolset。

    通过 ``DAYU_MARKET_PROVIDER`` 环境变量选择数据源，默认 tushare。

    Args:
        context: toolset 注册上下文。

    Returns:
        实际注册的工具数量。

    Raises:
        无。
    """

    limits = build_market_tool_limits(context.toolset_config)
    provider_name = os.environ.get("DAYU_MARKET_PROVIDER", _DEFAULT_PROVIDER)
    provider = _build_provider(provider_name)
    service = MarketToolService(provider=provider, limits=limits)

    before_count = len(context.registry.tools)
    register_market_tools(
        cast(ToolRegistry, context.registry),
        service=service,
        limits=limits,
    )
    registered = len(context.registry.tools) - before_count
    Log.verbose(
        f"market toolset 注册完成（provider={provider_name}），共 {registered} 个工具",
        module=MODULE,
    )
    return registered


__all__ = [
    "register_market_toolset",
]
