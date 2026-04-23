"""行情数据源最小协议。

provider 只需实现此协议中的两个方法。service 层通过协议解耦具体数据源。
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from dayu.market.models import BarData, BarFrequency, RealtimeQuoteData


@runtime_checkable
class MarketDataProviderProtocol(Protocol):
    """行情数据源最小协议。

    每个 provider（tushare / akshare）实现此协议，负责：
    - 调用外部 API 获取原始数据
    - 将原始数据转换为标准化模型
    - 将外部异常转换为 ``MarketDataError``
    """

    def get_realtime_quote(self, symbol: str) -> RealtimeQuoteData:
        """获取最新行情快照。

        Args:
            symbol: 标准化后的股票代码（如 "600519.SH"）。

        Returns:
            实时行情数据。

        Raises:
            MarketDataError: 数据源访问失败时抛出。
        """

        ...

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

        ...


__all__ = [
    "MarketDataProviderProtocol",
]
