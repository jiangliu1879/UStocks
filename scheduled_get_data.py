"""
每日股票数据调度器

每天美东时间 18:00 自动更新所有股票数据，
调用 utils.get_stock_data.get_all_stocks_data_to_db(start_date, end_date)。
"""

import time
from datetime import datetime
import pytz

from utils.get_stock_data import get_all_stocks_data_to_db
from utils.longport_utils import LongportUtils
from utils.logger import setup_logger

logger = setup_logger("ScheduledStockData")


def get_eastern_now() -> datetime:
    """获取当前美东时间（US/Eastern）。"""
    eastern = pytz.timezone("US/Eastern")
    return datetime.now(pytz.UTC).astimezone(eastern)


class DailyStockDataScheduler:
    """盘后执行一次全量股票日线更新。"""

    def __init__(self):
        self.last_run_date = None
        self.is_running = False

    def should_run_now(self, now_et: datetime) -> bool:
        """
        当天首次达到收盘时间（含）后触发一次。
        防止同一天重复执行。
        """
        if self.last_run_date == now_et.date():
            return False

        # 按用户要求：使用 LongportUtils.get_us_market_open_time 获取美股当天收盘时间
        market_close_dt = LongportUtils.get_us_market_close_time()
        if market_close_dt is None:
            # 兜底：若接口异常，按 18:00 执行
            return (now_et.hour > 18) or (now_et.hour == 18 and now_et.minute >= 0)

        # 统一转换到美东时区后，仅比较“时:分:秒”，保证任务每天都会执行一次
        if market_close_dt.tzinfo is None:
            eastern = pytz.timezone("US/Eastern")
            market_close_dt = eastern.localize(market_close_dt)
        else:
            market_close_dt = market_close_dt.astimezone(pytz.timezone("US/Eastern"))

        return now_et.time() >= market_close_dt.time()

    def run_once(self):
        """执行一次更新任务：更新当日数据。"""
        now_et = get_eastern_now()
        target_date = now_et.date()
        logger.info(f"[run_once] 开始更新全部股票数据（美东日期 {target_date}）")
        try:
            result = get_all_stocks_data_to_db(target_date, target_date)
            logger.info(f"[run_once] 更新完成: {result}")
            self.last_run_date = target_date
        except Exception as e:
            # 任务失败不应导致调度器退出，保留到下次循环继续尝试
            logger.error(f"[run_once] 更新失败: {e}", exc_info=True)

    def start(self):
        """启动调度循环（每分钟检查一次）。"""
        self.is_running = True
        logger.info("调度器已启动：盘后更新全部股票数据")
        logger.info("按 Ctrl+C 停止")
        try:
            while self.is_running:
                now_et = get_eastern_now()
                if self.should_run_now(now_et):
                    self.run_once()
                time.sleep(60)
        except KeyboardInterrupt:
            logger.info("接收到中断信号，停止调度器")
        finally:
            self.stop()

    def stop(self):
        self.is_running = False
        logger.info("调度器已停止")


def main():
    scheduler = DailyStockDataScheduler()
    scheduler.start()


if __name__ == "__main__":
    main()
