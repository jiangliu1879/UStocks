import pandas as pd
import pandas_market_calendars as mcal
import sys
import os
# 添加项目根目录到 Python 路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from data_models.trading_time import TradingTime

    
def get_trading_time(start_date: str, end_date: str) -> list[dict]:
    # 1. 获取纽交所 (NYSE) 日历
    nyse = mcal.get_calendar('NYSE')

    # 2. 获取指定日期范围的交易日程 (此时返回的是 UTC 时间)
    schedule = nyse.schedule(start_date=start_date, end_date=end_date)

    # 3. 将 UTC 时间转换为美东时间 ('America/New_York')

    schedule['market_open_et'] = schedule['market_open'].dt.tz_convert('America/New_York')
    schedule['market_close_et'] = schedule['market_close'].dt.tz_convert('America/New_York')

    cols = ['market_open_et', 'market_close_et']
    list_trading_time = []
    for _, row in schedule[cols].iterrows():
        trading_day = str(row['market_open_et'])[:10]
        market_open_time = str(row['market_open_et'])[:19]
        market_close_time = str(row['market_close_et'])[:19]
        list_trading_time.append(TradingTime(trading_day, market_open_time, market_close_time))

    return list_trading_time

if __name__ == "__main__":
    list_trading_time = get_trading_time(start_date='2026-01-01', end_date='2026-05-04')
    TradingTime.batch_save(list_trading_time)
