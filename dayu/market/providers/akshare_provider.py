"""Akshare 行情数据 provider。

使用 akshare 获取 A 股行情数据。无需 API token，直接调用东方财富数据源。
"""

from __future__ import annotations

from dayu.market.models import (
    BarData,
    BarFrequency,
    MarketDataError,
    RealtimeQuoteData,
    MARKET_ERROR_DATA,
    MARKET_ERROR_DEPENDENCY,
    safe_float,
)

# akshare 频率映射
_FREQ_MAP: dict[BarFrequency, str] = {
    BarFrequency.DAILY: "daily",
    BarFrequency.WEEKLY: "weekly",
    BarFrequency.MONTHLY: "monthly",
}


def _ensure_akshare_available() -> object:
    """检测 akshare 是否可用并返回模块引用。

    Returns:
        akshare 模块对象。

    Raises:
        MarketDataError: akshare 未安装时抛出。
    """

    try:
        import akshare  # type: ignore[import-untyped]
        return akshare
    except ImportError:
        raise MarketDataError(
            "akshare 未安装，请执行 pip install akshare",
            code=MARKET_ERROR_DEPENDENCY,
        )


def _strip_exchange_suffix(symbol: str) -> str:
    """从标准化 symbol 中提取纯数字代码。

    akshare 使用纯 6 位数字代码，不带交易所后缀。

    Args:
        symbol: 标准化后的 symbol（如 "600519.SH"）。

    Returns:
        纯数字代码（如 "600519"）。

    Raises:
        无。
    """

    return symbol.split(".")[0]


def _date_to_akshare_format(date_str: str) -> str:
    """将 YYYY-MM-DD 转为 akshare 的 YYYYMMDD 格式。

    Args:
        date_str: ISO 日期字符串。

    Returns:
        akshare 格式日期字符串。

    Raises:
        无。
    """

    return date_str.replace("-", "")


class AkshareProvider:
    """Akshare 行情数据 provider。

    无需 API token，通过东方财富数据源获取 A 股行情。

    Returns:
        无。

    Raises:
        无。
    """

    def __init__(self) -> None:
        self._ak: object | None = None

    def _get_ak(self) -> object:
        """获取或缓存 akshare 模块引用。

        Returns:
            akshare 模块对象。

        Raises:
            MarketDataError: akshare 不可用时抛出。
        """

        if self._ak is None:
            self._ak = _ensure_akshare_available()
        return self._ak

    def get_realtime_quote(self, symbol: str) -> RealtimeQuoteData:
        """获取最新行情快照。

        通过 ``stock_zh_a_spot_em`` 接口获取全市场实时行情，
        然后过滤出目标股票。

        Args:
            symbol: 标准化后的股票代码（如 "600519.SH"）。

        Returns:
            实时行情数据。

        Raises:
            MarketDataError: 数据源访问失败时抛出。
        """

        ak = self._get_ak()
        code = _strip_exchange_suffix(symbol)

        try:
            df = ak.stock_zh_a_spot_em()  # type: ignore[union-attr]
            if df is None or df.empty:
                raise MarketDataError(
                    f"akshare 返回空的实时行情数据",
                    code=MARKET_ERROR_DATA,
                )

            # 按代码过滤
            matched = df[df["代码"] == code]
            if matched.empty:
                raise MarketDataError(
                    f"未找到 {symbol} 的实时行情数据",
                    code=MARKET_ERROR_DATA,
                )

            row = matched.iloc[0]
            return RealtimeQuoteData(
                symbol=symbol,
                name=str(row.get("名称", code)),
                price=safe_float(row.get("最新价")),
                change=safe_float(row.get("涨跌额")),
                change_pct=safe_float(row.get("涨跌幅")),
                open=safe_float(row.get("今开")),
                high=safe_float(row.get("最高")),
                low=safe_float(row.get("最低")),
                prev_close=safe_float(row.get("昨收")),
                volume=safe_float(row.get("成交量")),
                amount=safe_float(row.get("成交额")),
                timestamp=_build_market_timestamp(),
            )
        except MarketDataError:
            raise
        except Exception as exc:
            raise MarketDataError(
                f"akshare 获取 {symbol} 行情失败: {exc}",
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

        ak = self._get_ak()
        code = _strip_exchange_suffix(symbol)
        ak_start = _date_to_akshare_format(start_date)
        ak_end = _date_to_akshare_format(end_date)
        period = _FREQ_MAP[frequency]

        try:
            df = ak.stock_zh_a_hist(  # type: ignore[union-attr]
                symbol=code,
                period=period,
                start_date=ak_start,
                end_date=ak_end,
                adjust="",
            )

            if df is None or df.empty:
                return []

            bars: list[BarData] = []
            for _, row in df.iterrows():
                date_val = row.get("日期")
                date_str = str(date_val) if date_val is not None else ""
                # akshare 返回的日期可能是 datetime 对象或字符串
                if hasattr(date_val, "strftime"):
                    date_str = date_val.strftime("%Y-%m-%d")  # type: ignore[union-attr]

                bars.append(
                    BarData(
                        date=date_str,
                        open=safe_float(row.get("开盘")),
                        high=safe_float(row.get("最高")),
                        low=safe_float(row.get("最低")),
                        close=safe_float(row.get("收盘")),
                        volume=safe_float(row.get("成交量")),
                        amount=safe_float(row.get("成交额")),
                    )
                )

            return bars

        except MarketDataError:
            raise
        except Exception as exc:
            raise MarketDataError(
                f"akshare 获取 {symbol} 历史 K 线失败: {exc}",
                code=MARKET_ERROR_DATA,
            ) from exc


def _build_market_timestamp() -> str:
    """生成当前时间的 ISO 8601 时间戳。

    Returns:
        当前时间的 ISO 8601 字符串。

    Raises:
        无。
    """

    from datetime import datetime, timezone, timedelta

    tz_cst = timezone(timedelta(hours=8))
    return datetime.now(tz_cst).isoformat(timespec="seconds")


__all__ = [
    "AkshareProvider",
]
