from longport.openapi import QuoteContext, Config, Market, TradeSession

config = Config.from_env()
ctx = QuoteContext(config)

class LongportUtils:
    @staticmethod
    def get_trading_time():
        resp = ctx.trading_session()
        print(resp)

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


if __name__ == "__main__":
    open_time, close_time = LongportUtils.get_us_market_open_close_time()
    print(f"US open: {open_time}, close: {close_time}")