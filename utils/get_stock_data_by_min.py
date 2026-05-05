from calendar import monthrange
from datetime import date
from typing import Iterator, Tuple
from longport.openapi import Period, AdjustType
import os
import sys  
# 添加项目根目录到路径，以便导入模型
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.logger import setup_logger
from data_models.stock_data_min import StockDataMin
from utils.longport_utils import LongportUtils
from utils.time_utils import to_eastern_time
from utils.api_type import ApiType
from utils.massive_utils import MassiveUtils

# 创建模块级别的日志记录器
logger = setup_logger('GetStockDataByMin')


def iter_month_ranges(start_date: date, end_date: date) -> Iterator[Tuple[date, date]]:
    """将 [start_date, end_date] 拆成若干自然月区间（每月一段，首尾月按起止日截断）。"""
    y, m = start_date.year, start_date.month
    while True:
        first = date(y, m, 1)
        last = date(y, m, monthrange(y, m)[1])
        seg_start = max(start_date, first)
        seg_end = min(end_date, last)
        if seg_start <= seg_end:
            yield seg_start, seg_end
        if last >= end_date:
            break
        if m == 12:
            y += 1
            m = 1
        else:
            m += 1


def get_single_stock_data_to_db_by_minutes(stock_code: str, start_date: date, end_date: date, interval: int, api_type: ApiType, adjusted: bool = True):
    """
    获取指定股票代码的指定日期范围内的数据，并写入数据库
    
    参数:
    stock_code (str): 股票代码，如 "SPY.US"
    start_date (date): 开始日期
    end_date (date): 结束日期
    interval (int): 间隔时间, 单位: 分钟
    api_type (ApiType): 数据源类型, 可选值: ApiType.LONGPORT、ApiType.MASSIVE
    adjusted (bool): 是否调整数据, 可选值: True、False
    返回:
    bool: 是否成功获取并保存数据
    """

    if api_type == ApiType.LONGPORT:
        stock_code = stock_code + ".US"
        list_data = LongportUtils.get_stock_data_by_min(stock_code, interval, adjusted, start_date, end_date)
    elif api_type == ApiType.MASSIVE:
        list_data = MassiveUtils.get_stock_data_by_min(stock_code, adjusted, str(start_date), str(end_date), interval)
    else:
        logger.error(f"[get_single_stock_data_to_db_by_minutes] 不支持的 API 类型: {api_type}")
        return False

    if not list_data or len(list_data) == 0:
        logger.warning(f"[get_single_stock_data_to_db_by_minutes] 未获取到 {stock_code} 的数据")
        return False

    StockDataMin.batch_save(list_data)
    logger.info(f"[get_single_stock_data_to_db_by_minutes] 成功保存 {len(list_data)} 条 {stock_code} 的数据到数据库")
    return True


if __name__ == "__main__":
    # 演示新的数据库方法
    logger.info("[__main__] 🚀 演示股票数据获取方法")
    logger.info("[__main__] " + "=" * 50)

    start_date = date(2025, 1, 1)
    end_date = date(2025, 12, 31)

    ok = True
    for m_start, m_end in iter_month_ranges(start_date, end_date):
        result = get_single_stock_data_to_db_by_minutes(
            stock_code="SPY",
            start_date=m_start,
            end_date=m_end,
            interval=1,
            api_type=ApiType.MASSIVE,
        )
        ok = ok and result
        logger.info(f"[__main__] {m_start} ~ {m_end}: {'成功' if result else '失败'}")
    print(f"结果: {'成功' if ok else '失败'}")