from longport.openapi import QuoteContext, Config, Market, TradeSession, Period, AdjustType
from datetime import date
from datetime import datetime
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.logger import setup_logger
from datetime import time
from utils.time_utils import get_eastern_now
logger = setup_logger('LongportUtils')

config = Config.from_env()
ctx = QuoteContext(config)

class LongportUtils:
    @staticmethod
    def get_us_market_open_close_time():
        """
        获取美国市场常规交易时段（Intraday）的开盘、收盘时间。
        Returns:
            tuple(begin_time, end_time)；未获取到时返回 (None, None)
        """
        resp = ctx.trading_session()
        for item in resp:
            if item.market == Market.US:
                for trade_session_info in item.trade_sessions:
                    if trade_session_info.trade_session == TradeSession.Intraday:
                        return trade_session_info.begin_time, trade_session_info.end_time
        return None, None

    @staticmethod
    def get_us_market_open_time():
        """获取美国市场开盘时间。"""
        begin_time, _ = LongportUtils.get_us_market_open_close_time()
        return begin_time

    @staticmethod
    def get_us_market_close_time():
        """获取美国市场收盘时间。"""
        _, end_time = LongportUtils.get_us_market_open_close_time()
        return end_time

    @staticmethod
    def get_history_candlesticks_by_date(stock_code: str, period: Period, adjust_type: AdjustType, start_date: date, end_date: date):
        # 获取历史K线数据
        resp = ctx.history_candlesticks_by_date(stock_code, period, adjust_type, start_date, end_date)
        return resp


    @staticmethod
    def get_trading_time():
        resp = ctx.trading_session()
        for item in resp:
            if item.market == Market.US:
                for trade_session_info in item.trade_sessions:
                    if trade_session_info.trade_session == TradeSession.Intraday:
                        return trade_session_info.begin_time, trade_session_info.end_time

    @staticmethod
    def is_trading_time():
        begin_time, end_time = LongportUtils.get_trading_time()
        if not begin_time or not end_time:
            return False

        now = get_eastern_now()

        # LongPort 可能返回 datetime.time 或 datetime.datetime，这里做兼容处理
        if isinstance(begin_time, datetime) and isinstance(end_time, datetime):
            return begin_time <= now <= end_time

        # 假设为 datetime.time
        if isinstance(begin_time, time) and isinstance(end_time, time):
            now_t = now.time()
            return begin_time <= now_t <= end_time

        # 兜底：尝试直接比较
        try:
            return now >= begin_time and now <= end_time
        except TypeError:
            return False 
            

    @staticmethod
    def get_option_chain_expiry_date_list(underlying_symbol: str) -> list[date]:
        """从长桥 API 获取期权链到期日列表，返回 datetime.date 列表。"""
        resp = ctx.option_chain_expiry_date_list(underlying_symbol)
        if not resp:
            return []
        out = []
        for item in resp:
            if isinstance(item, date):
                out.append(item)
            else:
                d = getattr(item, "date", None) or getattr(item, "expiry_date", None)
                if isinstance(d, date):
                    out.append(d)
        return sorted(set(out))

    @staticmethod
    def get_ticker_price(stock_code: str) -> float:
        resp = ctx.quote([stock_code])
        if not resp:
            return 0.0
        for item in resp:
            if item.symbol == stock_code:
                return float(item.last_done)
        return 0.0