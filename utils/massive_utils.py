import os
from massive import RESTClient
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.logger import setup_logger
from data_models.stock_data_min import StockDataMin
from utils.time_utils import to_eastern_time
from utils.trading_time_utils import get_trading_time
from data_models.trading_time import TradingTime

logger = setup_logger('MassiveUtils')
class MassiveUtils:
    @staticmethod
    def get_options_chain(underlying_ticker: str, expiration_date: str, strike_price_range: list[float], limit: int = 250, sort: str = "ticker"):
        client = RESTClient(os.getenv("MASSIVE_API_KEY"))
        options_chain = []
        for o in client.list_snapshot_options_chain(
            underlying_ticker,
            params={
                "strike_price.gte": strike_price_range[0],
                "strike_price.lte": strike_price_range[1],
                "expiration_date": expiration_date,
                "order": "asc",
                "limit": limit,
                "sort": sort,
            },
        ):
            options_chain.append(o)
        return options_chain

    @staticmethod
    def get_stock_data_by_min(stock_ticker: str, adjusted: bool, start_date: str, end_date: str, interval: int = 1) -> list[StockDataMin]:
        client = RESTClient(os.getenv("MASSIVE_API_KEY"))

        list_aggs = client.list_aggs(
            stock_ticker,
            interval,
            "minute",
            start_date,
            end_date,
            adjusted="true" if adjusted else "false",
            sort="asc",
            limit=50000,
        )

        data_list = []
        list_trading_time = TradingTime.get_trading_time(start_date=start_date, end_date=end_date)
        if not list_trading_time:
            return []
        list_trading_time_dict = {trading_time.trading_day: [trading_time.market_open_time, trading_time.market_close_time] for trading_time in list_trading_time}
        for agg in list_aggs:
            time_eastern = to_eastern_time(agg.timestamp)
            trading_day = time_eastern.strftime('%Y-%m-%d')
            timestamp = time_eastern.strftime('%Y-%m-%d %H:%M:%S')

            # 过滤掉非交易时间的数据    
            if trading_day not in list_trading_time_dict:
                continue
            # 过滤掉非交易时间的数据
            market_open_time = list_trading_time_dict[trading_day][0]
            market_close_time = list_trading_time_dict[trading_day][1]
            if timestamp < market_open_time or timestamp > market_close_time:
                continue

            # 保存数据
            data_list.append(StockDataMin(
                stock_code=stock_ticker,
                timestamp=timestamp,
                open=agg.open,
                high=agg.high,
                low=agg.low,
                close=agg.close,
                volume=agg.volume,
                turnover=0,
                vw=agg.vwap,
                interval=interval,
            ))

        return data_list

# if __name__ == "__main__":
#     data_list = MassiveUtils.get_stock_data_by_min(stock_ticker="SPY", adjusted=True, start_date="2025-12-01", end_date="2025-12-31", interval=1)
#     for data in data_list:
#         print(data.timestamp)