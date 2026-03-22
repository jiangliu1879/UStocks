# 获取标的历史 K 线
#
# 运行前请访问"开发者中心"确保账户有正确的行情权限。
# 如没有开通行情权限，可以通过"LongPort"手机客户端，并进入"我的 - 我的行情 - 行情商城"购买开通行情权限。
from datetime import datetime, date, timedelta
from longport.openapi import QuoteContext, Config, Period, AdjustType
import pytz
import pandas as pd
import os
import sys

# 添加项目根目录到路径，以便导入模型
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from data_models.stock_data_min import StockDataMin
from utils.logger import setup_logger

# 创建模块级别的日志记录器
logger = setup_logger('GetStockDataMinutes')


def _to_eastern(dt: datetime) -> datetime:
    """
    将 LongPort 返回的时间戳转换为美东时间（US/Eastern）。
    如果 dt 是 naive（无时区信息），默认认为是中国时区（Asia/Shanghai）。
    """
    eastern = pytz.timezone("US/Eastern")
    if dt.tzinfo is None:
        shanghai = pytz.timezone("Asia/Shanghai")
        dt = shanghai.localize(dt)
    return dt.astimezone(eastern)

def get_all_stocks_data_to_db(start_date, end_date, interval=1):
    """
    获取数据库中所有股票代码的指定日期范围内的数据，并写入数据库
    
    参数:
    start_date (date): 开始日期
    end_date (date): 结束日期
    interval (int): 间隔时间
    返回:
    dict: 包含成功和失败统计的字典
    """
    logger.info(f"[get_all_stocks_data_to_db] 开始获取所有股票在 {start_date} 到 {end_date} 期间的数据...")
    
    # 获取数据库中所有股票代码
    stock_codes = StockDataMin.get_stock_codes()
    
    success_count = 0
    failed_count = 0
    
    for stock_code in stock_codes:
        try:
            logger.info(f"[get_all_stocks_data_to_db] 正在获取 {stock_code} 的数据...")
            result = get_single_stock_data_to_db(stock_code, start_date, end_date, interval)
            if result:
                success_count += 1
                logger.info(f"[get_all_stocks_data_to_db] ✅ {stock_code} 数据获取成功")
            else:
                failed_count += 1
                logger.warning(f"[get_all_stocks_data_to_db] ❌ {stock_code} 数据获取失败")
        except Exception as e:
            failed_count += 1
            logger.error(f"[get_all_stocks_data_to_db] ❌ {stock_code} 数据获取出错: {str(e)}", exc_info=True)
    
    result_summary = {
        "success": success_count,
        "failed": failed_count,
        "total": len(stock_codes)
    }
    
    logger.info(f"[get_all_stocks_data_to_db] 数据获取完成！成功: {success_count}, 失败: {failed_count}, 总计: {len(stock_codes)}")
    return result_summary

def get_single_stock_data_to_db(stock_code, start_date, end_date, interval=1):
    """
    获取指定股票代码的指定日期范围内的数据，并写入数据库
    
    参数:
    stock_code (str): 股票代码，如 "SPY.US"
    start_date (date): 开始日期
    end_date (date): 结束日期
    interval (int): 间隔时间
    返回:
    bool: 是否成功获取并保存数据
    """
    try:
        config = Config.from_env()
        ctx = QuoteContext(config)

        # 获取历史K线数据
        resp = ctx.history_candlesticks_by_date(stock_code, Period.Min_1, AdjustType.NoAdjust, start_date, end_date)
        
        if not resp or len(resp) == 0:
            logger.warning(f"[get_single_stock_data_to_db] 未获取到 {stock_code} 的数据")
            return False

        # 准备数据写入数据库
        try:
            # 提取数据并转换为数据库记录
            data_list = []
            for candle in resp:
                candle_eastern = _to_eastern(candle.timestamp)
                # 检查是否已存在相同记录（避免重复）
                existing_data = StockDataMin.query(conditions={
                    'stock_code': stock_code,
                    'timestamp': candle_eastern.strftime('%Y-%m-%d %H:%M:%S'),
                    'interval': interval,
                })
                if len(existing_data) == 0:
                    data_list.append(StockDataMin(
                        stock_code=stock_code,
                        timestamp=candle_eastern.strftime('%Y-%m-%d %H:%M:%S'),
                        open=candle.open,
                        high=candle.high,
                        low=candle.low,
                        close=candle.close,
                        volume=candle.volume,
                        turnover=candle.turnover,
                        interval=interval
                    ))
            StockDataMin.batch_save(data_list)
            logger.info(f"[get_single_stock_data_to_db] 成功保存 {len(data_list)} 条 {stock_code} 的数据到数据库")
            return True
        except Exception as e:
            logger.error(f"[get_single_stock_data_to_db] 保存 {stock_code} 数据到数据库时出错: {str(e)}", exc_info=True)
            return False
            
    except Exception as e:
        logger.error(f"[get_single_stock_data_to_db] 获取 {stock_code} 数据时出错: {str(e)}", exc_info=True)
        return False

if __name__ == "__main__":
    # 演示新的数据库方法
    logger.info("[__main__] 🚀 演示股票数据获取方法")
    logger.info("[__main__] " + "=" * 50)


    stock_code = "NVDA.US"
    start_day = date(2026, 3, 19)
    end_day = date(2026, 3, 19)
    interval = 1

    current_day = start_day
    success = 0
    fail = 0
    while current_day <= end_day:
        result = get_single_stock_data_to_db(stock_code, current_day, current_day, interval)
        if result:
            success += 1
            logger.info(f"[__main__] ✅ {stock_code} {current_day} 获取成功")
        else:
            fail += 1
            logger.warning(f"[__main__] ❌ {stock_code} {current_day} 获取失败")
        current_day += timedelta(days=1)
        import time
        time.sleep(10)

    logger.info(f"[__main__] 完成：成功 {success}，失败 {fail}，区间 {start_day} ~ {end_day}")