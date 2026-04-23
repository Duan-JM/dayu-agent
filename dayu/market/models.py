"""行情数据领域模型。

该模块定义 market 包内部流通的标准化数据结构与异常类型。
所有 provider 必须将外部数据源返回值转换为此处定义的模型。
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class BarFrequency(str, Enum):
    """K 线频率枚举。"""

    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"


@dataclass(frozen=True)
class BarData:
    """单根 K 线数据。

    Args:
        date: 交易日期，格式 YYYY-MM-DD。
        open: 开盘价。
        high: 最高价。
        low: 最低价。
        close: 收盘价。
        volume: 成交量（股）。
        amount: 成交额（元）。

    Returns:
        无。

    Raises:
        无。
    """

    date: str
    open: float
    high: float
    low: float
    close: float
    volume: float
    amount: float


@dataclass(frozen=True)
class RealtimeQuoteData:
    """实时行情快照。

    Args:
        symbol: 标准化股票代码（如 "600519.SH"）。
        name: 股票名称。
        price: 最新价。
        change: 涨跌额。
        change_pct: 涨跌幅（%）。
        open: 开盘价。
        high: 最高价。
        low: 最低价。
        prev_close: 昨收价。
        volume: 成交量（股）。
        amount: 成交额（元）。
        timestamp: 行情时间戳，ISO 8601 格式。

    Returns:
        无。

    Raises:
        无。
    """

    symbol: str
    name: str
    price: float
    change: float
    change_pct: float
    open: float
    high: float
    low: float
    prev_close: float
    volume: float
    amount: float
    timestamp: str


class MarketDataError(Exception):
    """行情数据源异常基类。

    所有 provider 层的可恢复异常统一使用此类型；service 层捕获后
    转换为 ``ToolBusinessError``。

    Args:
        message: 错误描述。
        code: 错误码，用于 service 层区分错误类型。

    Returns:
        无。

    Raises:
        无。
    """

    def __init__(self, message: str, *, code: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


# 标准错误码常量
MARKET_ERROR_CONFIG = "MARKET_CONFIG_ERROR"
MARKET_ERROR_INVALID_TICKER = "MARKET_INVALID_TICKER"
MARKET_ERROR_DATA = "MARKET_DATA_ERROR"
MARKET_ERROR_DEPENDENCY = "MARKET_DEPENDENCY_ERROR"


def safe_float(value: float | int | str | None, default: float = 0.0) -> float:
    """将 pandas Series 取出的值安全转为 float。

    pandas DataFrame 中缺失值为 NaN 或 None，``row.get(col, 0)`` 在列存在但值为
    None 时不会返回默认值，而是返回 None，导致 ``float(None)`` 抛 TypeError。

    Args:
        value: 从 DataFrame row 取出的原始值。
        default: 转换失败时的兜底值。

    Returns:
        转换后的浮点数。

    Raises:
        无。
    """

    if value is None:
        return default
    try:
        result = float(value)
        if result != result:  # NaN check
            return default
        return result
    except (TypeError, ValueError):
        return default


__all__ = [
    "BarFrequency",
    "BarData",
    "RealtimeQuoteData",
    "MarketDataError",
    "MARKET_ERROR_CONFIG",
    "MARKET_ERROR_INVALID_TICKER",
    "MARKET_ERROR_DATA",
    "MARKET_ERROR_DEPENDENCY",
    "safe_float",
]
