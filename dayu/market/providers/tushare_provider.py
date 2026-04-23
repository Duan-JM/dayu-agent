"""Tushare 行情数据 provider。

使用 tushare pro API 获取 A 股行情数据。需要配置 ``TUSHARE_TOKEN`` 环境变量。
"""

from __future__ import annotations

import os

from dayu.market.models import (
    BarData,
    BarFrequency,
    MarketDataError,
    RealtimeQuoteData,
    MARKET_ERROR_CONFIG,
    MARKET_ERROR_DATA,
    MARKET_ERROR_DEPENDENCY,
)

# tushare 频率映射
_FREQ_MAP: dict[BarFrequency, str] = {
    BarFrequency.DAILY: "D",
    BarFrequency.WEEKLY: "W",
    BarFrequency.MONTHLY: "M",
}


def _safe_float(value: float | int | str | None, default: float = 0.0) -> float:
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
        # pandas NaN 处理
        if result != result:  # NaN check
            return default
        return result
    except (TypeError, ValueError):
        return default


def _ensure_tushare_available() -> object:
    """检测 tushare 是否可用并返回模块引用。

    Returns:
        tushare 模块对象。

    Raises:
        MarketDataError: tushare 未安装时抛出。
    """

    try:
        import tushare  # type: ignore[import-untyped]
        return tushare
    except ImportError:
        raise MarketDataError(
            "tushare 未安装，请执行 pip install tushare",
            code=MARKET_ERROR_DEPENDENCY,
        )


def _get_tushare_api(token: str | None = None) -> object:
    """获取 tushare pro API 实例。

    Args:
        token: tushare API token，为 None 时从环境变量读取。

    Returns:
        tushare pro_api 实例。

    Raises:
        MarketDataError: token 缺失或 tushare 不可用时抛出。
    """

    ts = _ensure_tushare_available()
    resolved_token = token or os.environ.get("TUSHARE_TOKEN", "")
    if not resolved_token:
        raise MarketDataError(
            "tushare API token 未配置，请设置 TUSHARE_TOKEN 环境变量",
            code=MARKET_ERROR_CONFIG,
        )
    ts.set_token(resolved_token)  # type: ignore[union-attr]
    return ts.pro_api()  # type: ignore[union-attr]


def _date_to_tushare_format(date_str: str) -> str:
    """将 YYYY-MM-DD 转为 tushare 的 YYYYMMDD 格式。

    Args:
        date_str: ISO 日期字符串。

    Returns:
        tushare 格式日期字符串。

    Raises:
        无。
    """

    return date_str.replace("-", "")


def _tushare_ts_code(symbol: str) -> str:
    """将标准 symbol 转为 tushare ts_code。

    tushare 使用 ``XXXXXX.SH`` / ``XXXXXX.SZ`` 格式，与我们的标准格式一致。

    Args:
        symbol: 标准化后的 symbol（如 "600519.SH"）。

    Returns:
        tushare ts_code。

    Raises:
        无。
    """

    return symbol


class TushareProvider:
    """Tushare 行情数据 provider。

    Args:
        token: tushare API token，为 None 时从 ``TUSHARE_TOKEN`` 环境变量读取。

    Returns:
        无。

    Raises:
        无。
    """

    def __init__(self, token: str | None = None) -> None:
        self._token = token or os.environ.get("TUSHARE_TOKEN", "")
        self._api: object | None = None

    def _get_api(self) -> object:
        """获取或缓存 tushare pro API 实例。

        Returns:
            tushare pro_api 实例。

        Raises:
            MarketDataError: token 缺失或 tushare 不可用时抛出。
        """

        if self._api is None:
            self._api = _get_tushare_api(self._token)
        return self._api

    def get_realtime_quote(self, symbol: str) -> RealtimeQuoteData:
        """获取最新行情快照。

        通过 tushare ``daily`` 接口获取最近一个交易日数据，
        结合 ``daily_basic`` 获取涨跌信息。

        Args:
            symbol: 标准化后的股票代码（如 "600519.SH"）。

        Returns:
            实时行情数据。

        Raises:
            MarketDataError: 数据源访问失败时抛出。
        """

        api = self._get_api()
        ts_code = _tushare_ts_code(symbol)

        try:
            # 获取最近交易日行情
            df = api.daily(ts_code=ts_code, limit=1)  # type: ignore[union-attr]
            if df is None or df.empty:
                raise MarketDataError(
                    f"未找到 {symbol} 的行情数据",
                    code=MARKET_ERROR_DATA,
                )

            row = df.iloc[0]
            trade_date = str(row.get("trade_date", ""))

            # 获取股票名称
            name = self._get_stock_name(api, ts_code)

            return RealtimeQuoteData(
                symbol=symbol,
                name=name,
                price=_safe_float(row.get("close")),
                change=_safe_float(row.get("change")),
                change_pct=_safe_float(row.get("pct_chg")),
                open=_safe_float(row.get("open")),
                high=_safe_float(row.get("high")),
                low=_safe_float(row.get("low")),
                prev_close=_safe_float(row.get("pre_close")),
                volume=_safe_float(row.get("vol")) * 100,  # tushare vol 单位是手，转为股
                amount=_safe_float(row.get("amount")) * 1000,  # tushare amount 单位是千元，转为元
                timestamp=_format_trade_date(trade_date),
            )
        except MarketDataError:
            raise
        except Exception as exc:
            raise MarketDataError(
                f"tushare 获取 {symbol} 行情失败: {exc}",
                code=MARKET_ERROR_DATA,
            ) from exc

    def get_history_bars(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        frequency: BarFrequency,
    ) -> list[BarData]:
        """获取历史 K 线数据。

        Args:
            symbol: 标准化后的股票代码。
            start_date: 起始日期（YYYY-MM-DD）。
            end_date: 结束日期（YYYY-MM-DD）。
            frequency: K 线频率。

        Returns:
            按日期升序排列的 K 线列表。

        Raises:
            MarketDataError: 数据源访问失败时抛出。
        """

        api = self._get_api()
        ts_code = _tushare_ts_code(symbol)
        ts_start = _date_to_tushare_format(start_date)
        ts_end = _date_to_tushare_format(end_date)
        freq = _FREQ_MAP[frequency]

        try:
            if freq == "D":
                df = api.daily(  # type: ignore[union-attr]
                    ts_code=ts_code,
                    start_date=ts_start,
                    end_date=ts_end,
                )
            elif freq == "W":
                df = api.weekly(  # type: ignore[union-attr]
                    ts_code=ts_code,
                    start_date=ts_start,
                    end_date=ts_end,
                )
            else:
                df = api.monthly(  # type: ignore[union-attr]
                    ts_code=ts_code,
                    start_date=ts_start,
                    end_date=ts_end,
                )

            if df is None or df.empty:
                return []

            bars: list[BarData] = []
            for _, row in df.iterrows():
                trade_date = str(row.get("trade_date", ""))
                bars.append(
                    BarData(
                        date=_format_trade_date_short(trade_date),
                        open=_safe_float(row.get("open")),
                        high=_safe_float(row.get("high")),
                        low=_safe_float(row.get("low")),
                        close=_safe_float(row.get("close")),
                        volume=_safe_float(row.get("vol")) * 100,
                        amount=_safe_float(row.get("amount")) * 1000,
                    )
                )

            # tushare 默认降序，需要反转为升序
            bars.reverse()
            return bars

        except MarketDataError:
            raise
        except Exception as exc:
            raise MarketDataError(
                f"tushare 获取 {symbol} 历史 K 线失败: {exc}",
                code=MARKET_ERROR_DATA,
            ) from exc

    def _get_stock_name(self, api: object, ts_code: str) -> str:
        """查询股票名称。

        Args:
            api: tushare pro_api 实例。
            ts_code: tushare 格式股票代码。

        Returns:
            股票名称；查询失败时返回 ts_code。

        Raises:
            无。
        """

        try:
            df = api.namechange(ts_code=ts_code, limit=1)  # type: ignore[union-attr]
            if df is not None and not df.empty:
                return str(df.iloc[0].get("name", ts_code))
        except Exception:
            pass
        return ts_code


def _format_trade_date(trade_date: str) -> str:
    """将 tushare 的 YYYYMMDD 格式转为 ISO 8601 时间戳。

    Args:
        trade_date: tushare 日期字符串。

    Returns:
        ISO 8601 格式时间戳。

    Raises:
        无。
    """

    if len(trade_date) == 8:
        return f"{trade_date[:4]}-{trade_date[4:6]}-{trade_date[6:8]}T15:00:00+08:00"
    return trade_date


def _format_trade_date_short(trade_date: str) -> str:
    """将 tushare 的 YYYYMMDD 格式转为 YYYY-MM-DD。

    Args:
        trade_date: tushare 日期字符串。

    Returns:
        YYYY-MM-DD 格式日期。

    Raises:
        无。
    """

    if len(trade_date) == 8:
        return f"{trade_date[:4]}-{trade_date[4:6]}-{trade_date[6:8]}"
    return trade_date


__all__ = [
    "TushareProvider",
]
