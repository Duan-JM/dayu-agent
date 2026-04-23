"""行情工具返回值类型。

所有对 LLM 暴露的工具返回值使用 TypedDict 定义，确保 schema 稳定。
"""

from __future__ import annotations

from typing import TypedDict


class BarEntry(TypedDict):
    """单根 K 线条目（用于 tool 返回）。

    Args:
        date: 交易日期 YYYY-MM-DD。
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


class StockQuoteResult(TypedDict):
    """``get_stock_quote`` 工具返回类型。

    Args:
        symbol: 标准化股票代码。
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
        timestamp: 行情时间戳。
        currency: 货币单位。
        market: 交易所标识（SH/SZ/BJ）。

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
    currency: str
    market: str


class StockHistoryResult(TypedDict):
    """``get_stock_history`` 工具返回类型。

    Args:
        symbol: 标准化股票代码。
        name: 股票名称。
        frequency: K 线频率。
        bars: K 线数据列表。
        currency: 货币单位。
        market: 交易所标识。
        total_bars: 实际返回的 K 线总数。
        truncated: 是否因超出限制而被截断。

    Returns:
        无。

    Raises:
        无。
    """

    symbol: str
    name: str
    frequency: str
    bars: list[BarEntry]
    currency: str
    market: str
    total_bars: int
    truncated: bool


__all__ = [
    "BarEntry",
    "StockQuoteResult",
    "StockHistoryResult",
]
