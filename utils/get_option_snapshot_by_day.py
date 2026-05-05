from datetime import date
from longport.openapi import QuoteContext, Config
import sys
import os

# 添加项目根目录到 Python 路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from data_models.option_snapshot_day import OptionSnapshotDay
from utils.logger import setup_logger
from utils.time_utils import get_eastern_now
from utils.longport_utils import LongportUtils

logger = setup_logger('GetOptionSnapshotByDay')

if __name__ == "__main__":
      # 获取标的的期权链到期日列表
    import json
    import time
    with open('stock_option_info.json', 'r') as f:
        stock_option_info = json.load(f)

    for item in stock_option_info:
        underlying_symbol = item.get("stock_code")
        if underlying_symbol == "SPY.US":
            continue
        expiry_dates = item.get("expiry_dates")
        strike_price_range = item.get("strike_price_range")
        update_time = get_eastern_now().strftime('%Y-%m-%d %H:%M:%S')
        for expiry_date in expiry_dates:
            expiry_date = date.fromisoformat(expiry_date)
            list_option_chain = LongportUtils.get_option_chain_info_by_data(underlying_symbol, expiry_date, strike_price_range)
            list_option_snapshots = LongportUtils.get_option_snapshots_by_day(list_option_chain, expiry_date, update_time)
            # 保存到数据库
            saved_count = OptionSnapshotDay.batch_save(list_option_snapshots)
            logger.info(f"保存到数据库成功，共保存 {saved_count} 条期权报价记录: {underlying_symbol} - {expiry_date} - {strike_price_range}")
            time.sleep(10)
        time.sleep(30)