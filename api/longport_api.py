from datetime import datetime, date
from longport.openapi import QuoteContext, Config, Period, AdjustType
import pandas as pd
import os
import sys

 
# config = Config.from_env()
# ctx = QuoteContext(config)

# 获取历史K线数据
# from longport.openapi import QuoteContext, Config, CalcIndex

# config = Config.from_env()
# ctx = QuoteContext(config)

# resp = ctx.calc_indexes(["NVDA260515C180000.US"], [CalcIndex.Delta, CalcIndex.Gamma, CalcIndex.Theta, CalcIndex.Vega, CalcIndex.Rho, CalcIndex.ImpliedVolatility])
# print(resp)


# 获取标的盘口
#
# 运行前请访问“开发者中心”确保账户有正确的行情权限。
# 如没有开通行情权限，可以通过“LongPort”手机客户端，并进入“我的 - 我的行情 - 行情商城”购买开通行情权限。
# from longport.openapi import QuoteContext, Config

# config = Config.from_env()
# ctx = QuoteContext(config)

# resp = ctx.depth("NVDA.US")
# print(resp)


from datetime import datetime, date
from longport.openapi import QuoteContext, Config, Period, AdjustType

config = Config.from_env()
ctx = QuoteContext(config)

# Query after 2023-01-01
resp = ctx.history_candlesticks_by_date("NVDA.US", Period.Min_1, AdjustType.NoAdjust, date(2026, 3, 17), date(2026, 3, 17))
for candle in resp:
    # 用于调试：打印 K 线时间戳
    print(candle.timestamp)
# resp = ctx.history_candlesticks_by_offset("NVDA.US", Period.Min_1, AdjustType.NoAdjust, True, 1000, datetime(2026, 3, 17))
# print(resp)