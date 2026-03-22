# 获取标的历史 K 线
#
# 运行前请访问"开发者中心"确保账户有正确的行情权限。
# 如没有开通行情权限，可以通过"LongPort"手机客户端，并进入"我的 - 我的行情 - 行情商城"购买开通行情权限。
from datetime import datetime, date
from longport.openapi import QuoteContext, Config, Period, AdjustType
import pandas as pd
import os
import sys

# 添加项目根目录到路径，以便导入模型
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from data_models.stock_data import StockData
from utils.logger import setup_logger

# 创建模块级别的日志记录器
logger = setup_logger('GetStockData')

def get_stock_history_data(stock_code, file_path, start_date, end_date):
    """
    获取股票历史K线数据并保存到CSV文件
    
    参数:
    stock_code (str): 股票代码，如 "SPY.US"
    file_path (str): 数据文件存储路径
    start_date (date, optional): 开始日期
    end_date (date, optional): 结束日期
    
    返回:
    bool: 是否成功获取并保存数据
    """
    
    config = Config.from_env()
    ctx = QuoteContext(config)

    # 获取历史K线数据
    resp = ctx.history_candlesticks_by_date(stock_code, Period.Day, AdjustType.NoAdjust, start_date, end_date)
    logger.debug(f"[get_stock_history_data] 获取到数据: {resp}")

    # 将数据转换为DataFrame并保存到CSV文件
    if resp and len(resp) > 0:
        # 提取数据并转换为DataFrame
        data_list = []
        for candle in resp:
            data_list.append({
                'timestamp': candle.timestamp.date(),
                'open': candle.open,
                'high': candle.high,
                'low': candle.low,
                'close': candle.close,
                'volume': candle.volume,
                'turnover': candle.turnover
            })
        
        df = pd.DataFrame(data_list)
        
        # 检查文件是否存在，决定是创建新文件还是追加数据
        if os.path.exists(file_path):
            # 文件存在，追加数据
            df.to_csv(file_path, mode='a', header=False, index=False)
            logger.info(f"[get_stock_history_data] 数据已追加到 {file_path}，共 {len(df)} 条记录")
        else:
            # 文件不存在，创建新文件
            df.to_csv(file_path, index=False)
            logger.info(f"[get_stock_history_data] 数据已保存到 {file_path}，共 {len(df)} 条记录")
        return True
    else:
        logger.warning(f"[get_stock_history_data] 未获取到数据")
        return False

def get_all_stocks_data_to_db(start_date, end_date):
    """
    获取数据库中所有股票代码的指定日期范围内的数据，并写入数据库
    
    参数:
    start_date (date): 开始日期
    end_date (date): 结束日期
    
    返回:
    dict: 包含成功和失败统计的字典
    """
    logger.info(f"[get_all_stocks_data_to_db] 开始获取所有股票在 {start_date} 到 {end_date} 期间的数据...")
    
    # 获取数据库中所有股票代码
    stock_codes = StockData.get_stock_codes()
    
    success_count = 0
    failed_count = 0
    
    for stock_code in stock_codes:
        try:
            logger.info(f"[get_all_stocks_data_to_db] 正在获取 {stock_code} 的数据...")
            result = get_single_stock_data_to_db(stock_code, start_date, end_date)
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

def get_single_stock_data_to_db(stock_code, start_date, end_date):
    """
    获取指定股票代码的指定日期范围内的数据，并写入数据库
    
    参数:
    stock_code (str): 股票代码，如 "SPY.US"
    start_date (date): 开始日期
    end_date (date): 结束日期
    
    返回:
    bool: 是否成功获取并保存数据
    """
    try:
        config = Config.from_env()
        ctx = QuoteContext(config)

        # 获取历史K线数据
        resp = ctx.history_candlesticks_by_date(stock_code, Period.Day, AdjustType.NoAdjust, start_date, end_date)

        logger.debug(f"[get_single_stock_data_to_db] 获取到 {stock_code} 的数据: {resp}")
        
        if not resp or len(resp) == 0:
            logger.warning(f"[get_single_stock_data_to_db] 未获取到 {stock_code} 的数据")
            return False

        # 准备数据写入数据库
        try:
            # 提取数据并转换为数据库记录
            data_list = []
            for candle in resp:
                # 检查是否已存在相同记录（避免重复）
                existing_data = StockData.query(conditions={
                    'stock_code': stock_code,
                    'timestamp': candle.timestamp.date()
                })
                if len(existing_data) == 0:
                    data_list.append(StockData(
                        stock_code=stock_code,
                        timestamp=candle.timestamp.date(),
                        open=candle.open,
                        high=candle.high,
                        low=candle.low,
                        close=candle.close,
                        volume=candle.volume,
                        turnover=candle.turnover
                    ))
            StockData.batch_save(data_list)
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

    
    # 示例1: 获取单个股票数据到数据库
    logger.info("[__main__] 📊 示例1: 获取单个股票数据到数据库")
    list_stock_codes = StockData.get_stock_codes()
    logger.info(f"[__main__] 股票代码列表: {list_stock_codes}")
    for stock_code in list_stock_codes:
        result = get_single_stock_data_to_db(stock_code, date(2026, 3, 17), date(2026, 3, 20))
        logger.info(f"[__main__] {stock_code},结果: {'成功' if result else '失败'}")

    # pair_years = [(2000, 2002), (2003, 2005), (2006, 2007), (2008, 2010), (2011, 2013), (2014, 2016), (2017, 2019), (2020, 2022), (2023, 2025)]
    # for start_year, end_year in pair_years:
    #     result = get_single_stock_data_to_db("PLTR.US", date(start_year, 1, 1), date(end_year, 12, 31))
    #     print(f"结果: {'成功' if result else '失败'}")
    #     import time
    #     time.sleep(10)

    # pair_years = [(2020, 2022), (2023, 2025), (2026, 2026)]
    # for start_year, end_year in pair_years:
    #     result = get_single_stock_data_to_db("HOOD.US", date(start_year, 1, 1), date(end_year, 12, 31))
    #     print(f"结果: {'成功' if result else '失败'}")
    #     import time
    #     time.sleep(5)