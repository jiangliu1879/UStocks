from datetime import date
from longport.openapi import QuoteContext, Config
import pytz
from datetime import datetime
import sys
import os

# 添加项目根目录到 Python 路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from data_models.option_quote import OptionQuote
from utils.logger import logger

config = Config.from_env()
ctx = QuoteContext(config)

def get_eastern_time():
    """获取美东当前时间"""
    # 美东时区
    eastern = pytz.timezone('US/Eastern')
    # 获取当前UTC时间并转换为美东时间
    utc_now = datetime.now(pytz.UTC)
    eastern_time = utc_now.astimezone(eastern)
    return eastern_time


def get_option_chain_info_by_data(underlying_symbol: str, expiry_date: date,  strike_price_range: tuple[float, float]):
    try:
        resp = ctx.option_chain_info_by_date(underlying_symbol, expiry_date)
        list_option_symbol = []
        for item in resp:
            if float(item.price) >= strike_price_range[0] and float(item.price) <= strike_price_range[1]:
                list_option_symbol.append(item.call_symbol)
                list_option_symbol.append(item.put_symbol)
    except Exception as e:
        logger.error(f"获取期权链信息失败: {e}", exc_info=True)
        return []
    return list_option_symbol


def get_option_quote(list_option_symbol: list[str], expiry_date: date, update_time: str):
    try:
        resp = ctx.option_quote(list_option_symbol)

        list_option_quote = []
        expiry_date_str = expiry_date.strftime('%Y-%m-%d')
        for item in resp:
            list_option_quote.append(
                OptionQuote(
                    underlying_symbol=item.underlying_symbol,
                    expiry_date=expiry_date_str,
                    update_time=update_time,
                    strike_price=float(item.strike_price),
                    option_symbol=item.symbol,
                    direction=str(item.direction).split(".")[1],
                    last_done=float(item.last_done),
                    prev_close=float(item.prev_close),
                    high=float(item.high),
                    low=float(item.low),
                    volume=int(item.volume),
                    turnover=float(item.turnover),
                    open_interest=int(item.open_interest),
                    implied_volatility=float(item.implied_volatility),
                    historical_volatility=float(item.historical_volatility),
                    contract_multiplier=int(item.contract_multiplier),
                    contract_type=str(item.contract_type).split(".")[1],
                    contract_size=int(item.contract_size),
                )
            )
    except Exception as e:
        logger.error(f"获取期权行情失败: {e}", exc_info=True)
        return []
    return list_option_quote

if __name__ == "__main__":
      # 获取标的的期权链到期日列表
    import json
    import time
    with open('stock_option_info.json', 'r') as f:
        stock_option_info = json.load(f)

    for item in stock_option_info:
        underlying_symbol = item.get("stock_code")
        expiry_dates = item.get("expiry_dates")
        strike_price_range = item.get("strike_price_range")
        for expiry_date in expiry_dates:
            expiry_date = date.fromisoformat(expiry_date)
            eastern_time = get_eastern_time()
            update_time = eastern_time.strftime('%Y-%m-%d %H:%M:%S')
            list_option_symbol = get_option_chain_info_by_data(underlying_symbol, expiry_date, strike_price_range)
            list_option_quote = get_option_quote(list_option_symbol, expiry_date, update_time)
            # 保存到数据库
            saved_count = OptionQuote.batch_save(list_option_quote)
            logger.info(f"保存到数据库成功，共保存 {saved_count} 条期权报价记录: {underlying_symbol} - {expiry_date} - {strike_price_range}")
            time.sleep(10)

        time.sleep(30)