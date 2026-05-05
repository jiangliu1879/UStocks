from longport.openapi import QuoteContext, Config, Market, TradeSession, Period, AdjustType
from datetime import date
from datetime import datetime
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.logger import setup_logger
from datetime import time
from utils.time_utils import get_eastern_now
from data_models.stock_data_min import StockDataMin
from utils.time_utils import to_eastern_time
from data_models.option_snapshot_day import OptionSnapshotDay
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
    def get_stock_data_by_min(stock_code: str, interval: int, adjusted: bool, start_date: date, end_date: date) -> list[StockDataMin]:
        period = Period.Min_1
        if interval == 5:
            period = Period.Min_5
        elif interval == 10:
            period = Period.Min_10
        elif interval == 30:
            period = Period.Min_30
        elif interval == 60:
            period = Period.Min_60

        # 获取历史K线数据
        resp = ctx.history_candlesticks_by_date(stock_code, period, AdjustType.ForwardAdjust if adjusted else AdjustType.NoAdjust, start_date, end_date)
        data_list = []
        for candle in resp:
            candle_eastern = to_eastern_time(candle.timestamp)
            # 直接构建批量数据，依赖表唯一键 + ON DUPLICATE KEY UPDATE 去重/更新
            data_list.append(StockDataMin(
                stock_code=stock_code,
                timestamp=candle_eastern.strftime('%Y-%m-%d %H:%M:%S'),
                open=candle.open,
                high=candle.high,
                low=candle.low,
                close=candle.close,
                volume=candle.volume,
                turnover=candle.turnover,
                vw=0,
                interval=interval
            ))

        return data_list


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


    @staticmethod
    def get_stock_data_realtime(stock_code: str) -> dict:
        resp = ctx.quote([stock_code])
        if not resp:
            return {}
        for item in resp:
            if item.symbol == stock_code:
                return item.to_dict()
        return {}

    @staticmethod
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

    @staticmethod
    def get_option_snapshots_by_day(list_option_symbol: list[str], expiry_date: date, update_time: str):
        try:
            resp = ctx.option_quote(list_option_symbol)

            list_option_snapshots = []
            expiry_date_str = expiry_date.strftime('%Y-%m-%d')
            for item in resp:
                list_option_snapshots.append(
                    OptionSnapshotDay(
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
        return list_option_snapshots            