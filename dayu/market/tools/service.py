"""行情工具服务层。

该模块是行情工具与底层 provider 之间的中间调用层，职责包括：
- ticker 标准化（用户输入 -> 标准 symbol）
- 调用 provider 获取数据
- 异常转换（MarketDataError -> ToolBusinessError）
- 结果裁剪与格式化
"""

from __future__ import annotations

import re
from datetime import date, timedelta
from typing import Optional

from dayu.engine.tool_errors import ToolBusinessError
from dayu.contracts.tool_configs import MarketToolLimits
from dayu.log import Log
from dayu.market.models import (
    BarFrequency,
    MarketDataError,
    MARKET_ERROR_INVALID_TICKER,
)
from dayu.market.providers.protocol import MarketDataProviderProtocol
from .result_types import BarEntry, StockHistoryResult, StockQuoteResult

MODULE = "MARKET.SERVICE"

# ticker 标准化正则：纯 6 位数字 或 6 位数字.交易所后缀
_TICKER_PURE_DIGITS = re.compile(r"^(\d{6})$")
_TICKER_WITH_SUFFIX = re.compile(r"^(\d{6})\.(SH|SZ|BJ)$", re.IGNORECASE)

# 交易所前缀映射
_EXCHANGE_PREFIX_MAP: dict[str, str] = {
    "6": "SH",
    "0": "SZ",
    "3": "SZ",
    "4": "BJ",
    "8": "BJ",
}


def _normalize_ticker(raw: str) -> str:
    """将用户输入的 ticker 标准化为 ``XXXXXX.XX`` 格式。

    Args:
        raw: 用户输入的 ticker 字符串。

    Returns:
        标准化后的 symbol，如 ``"600519.SH"``。

    Raises:
        ToolBusinessError: 无法识别 ticker 格式时抛出。
    """

    stripped = raw.strip()

    # 已带后缀：600519.SH
    match_suffix = _TICKER_WITH_SUFFIX.match(stripped)
    if match_suffix:
        code = match_suffix.group(1)
        exchange = match_suffix.group(2).upper()
        return f"{code}.{exchange}"

    # 纯 6 位数字：600519
    match_pure = _TICKER_PURE_DIGITS.match(stripped)
    if match_pure:
        code = match_pure.group(1)
        prefix = code[0]
        exchange = _EXCHANGE_PREFIX_MAP.get(prefix)
        if exchange is None:
            raise ToolBusinessError(
                code=MARKET_ERROR_INVALID_TICKER,
                message=f"无法识别 ticker '{raw}'：首位 '{prefix}' 不对应已知交易所",
                hint="请提供正确的 A 股代码，如 600519 或 000001.SZ",
            )
        return f"{code}.{exchange}"

    raise ToolBusinessError(
        code=MARKET_ERROR_INVALID_TICKER,
        message=f"无法识别 ticker '{raw}'：需要 6 位数字代码（如 600519 或 600519.SH）",
        hint="请提供正确的 A 股代码，如 600519 或 000001.SZ",
    )


def _extract_market(symbol: str) -> str:
    """从标准化 symbol 中提取交易所标识。

    Args:
        symbol: 标准化后的 symbol（如 "600519.SH"）。

    Returns:
        交易所标识（"SH"/"SZ"/"BJ"）。

    Raises:
        无。
    """

    return symbol.split(".")[-1]


class MarketToolService:
    """行情工具服务层。

    负责 ticker 标准化、provider 调用、异常转换和结果格式化。
    不持有复杂状态，可安全跨请求复用。

    Args:
        provider: 行情数据 provider 实例。
        limits: 行情工具限制配置。

    Returns:
        无。

    Raises:
        无。
    """

    def __init__(
        self,
        provider: MarketDataProviderProtocol,
        limits: MarketToolLimits,
    ) -> None:
        self._provider = provider
        self._limits = limits

    def get_stock_quote(self, *, ticker: str) -> StockQuoteResult:
        """获取股票最新行情报价。

        Args:
            ticker: 用户输入的股票代码。

        Returns:
            标准化后的行情报价结果。

        Raises:
            ToolBusinessError: ticker 非法或数据源异常时抛出。
        """

        symbol = _normalize_ticker(ticker)
        market = _extract_market(symbol)

        try:
            quote = self._provider.get_realtime_quote(symbol)
        except MarketDataError as exc:
            raise ToolBusinessError(
                code=exc.code,
                message=exc.message,
            ) from exc

        return StockQuoteResult(
            symbol=quote.symbol,
            name=quote.name,
            price=quote.price,
            change=quote.change,
            change_pct=quote.change_pct,
            open=quote.open,
            high=quote.high,
            low=quote.low,
            prev_close=quote.prev_close,
            volume=quote.volume,
            amount=quote.amount,
            timestamp=quote.timestamp,
            currency="CNY",
            market=market,
        )

    def get_stock_history(
        self,
        *,
        ticker: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        frequency: Optional[str] = None,
    ) -> StockHistoryResult:
        """获取股票历史 K 线数据。

        Args:
            ticker: 用户输入的股票代码。
            start_date: 起始日期 YYYY-MM-DD，默认最近 30 个自然日。
            end_date: 结束日期 YYYY-MM-DD，默认今天。
            frequency: K 线频率，默认 daily。

        Returns:
            标准化后的历史 K 线结果。

        Raises:
            ToolBusinessError: ticker 非法、频率非法或数据源异常时抛出。
        """

        symbol = _normalize_ticker(ticker)
        market = _extract_market(symbol)

        # 解析频率
        freq = _parse_frequency(frequency)

        # 解析日期范围
        resolved_end = end_date or date.today().isoformat()
        resolved_start = start_date or (date.today() - timedelta(days=30)).isoformat()

        try:
            bars = self._provider.get_history_bars(
                symbol=symbol,
                start_date=resolved_start,
                end_date=resolved_end,
                frequency=freq,
            )
        except MarketDataError as exc:
            raise ToolBusinessError(
                code=exc.code,
                message=exc.message,
            ) from exc

        # 裁剪
        max_bars = self._limits.history_max_bars
        truncated = len(bars) > max_bars
        if truncated:
            Log.verbose(
                f"历史 K 线结果超出限制 {len(bars)} > {max_bars}，截断至最近 {max_bars} 条",
                module=MODULE,
            )
            bars = bars[-max_bars:]

        bar_entries: list[BarEntry] = [
            BarEntry(
                date=b.date,
                open=b.open,
                high=b.high,
                low=b.low,
                close=b.close,
                volume=b.volume,
                amount=b.amount,
            )
            for b in bars
        ]

        # 用第一条的 name 信息（provider 不一定在 bars 里返回 name，
        # 这里先用 symbol 占位，后续可从 quote 补充）
        name = symbol

        return StockHistoryResult(
            symbol=symbol,
            name=name,
            frequency=freq.value,
            bars=bar_entries,
            currency="CNY",
            market=market,
            total_bars=len(bar_entries),
            truncated=truncated,
        )


def _parse_frequency(raw: str | None) -> BarFrequency:
    """解析并校验 K 线频率参数。

    Args:
        raw: 用户传入的频率字符串，为 None 时默认 daily。

    Returns:
        对应的 BarFrequency 枚举值。

    Raises:
        ToolBusinessError: 频率值非法时抛出。
    """

    if raw is None:
        return BarFrequency.DAILY

    cleaned = raw.strip().lower()
    if not cleaned:
        raise ToolBusinessError(
            code="MARKET_INVALID_PARAM",
            message="频率参数不能为空",
        )
    try:
        return BarFrequency(cleaned)
    except ValueError:
        valid = ", ".join(f.value for f in BarFrequency)
        raise ToolBusinessError(
            code="MARKET_INVALID_PARAM",
            message=f"不支持的频率 '{raw}'，可选值：{valid}",
        )


__all__ = [
    "MarketToolService",
]
