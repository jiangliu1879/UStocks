# 获取各市场当日交易时段
#
# 运行前请访问"开发者中心"确保账户有正确的行情权限。
# 如没有开通行情权限，可以通过"LongPort"手机客户端，并进入"我的 - 我的行情 - 行情商城"购买开通行情权限。
from longport.openapi import QuoteContext, Config, Market, TradeSession
from datetime import datetime
import sys
import os

# 添加项目根目录到 Python 路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.logger import setup_logger

# 创建模块级别的日志记录器
logger = setup_logger('GetTradingTime')

config = Config.from_env()
ctx = QuoteContext(config)


def get_trading_time():
    resp = ctx.trading_session()
    for item in resp:
        if item.market == Market.US:
            for trade_session_info in item.trade_sessions:
                if trade_session_info.trade_session == TradeSession.Intraday:
                    return trade_session_info.begin_time, trade_session_info.end_time

def is_trading_time():
    begin_time, end_time = get_trading_time()
    now = datetime.now()
    if now >= begin_time and now <= end_time:
        return True
    else:
        return False

if __name__ == "__main__":
    begin_time, end_time = get_trading_time()
    logger.info(f"[__main__] 交易开始时间: {begin_time}")
    logger.info(f"[__main__] 交易结束时间: {end_time}")
