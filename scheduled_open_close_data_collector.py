"""
开盘和收盘时段期权数据收集器

这个脚本在开盘后10分钟内和收盘后10分钟内收集期权数据，支持多个到期日期。
"""

import os
import sys
import time
import schedule
from datetime import datetime, date, timedelta
import signal
from typing import List
import json

# 添加项目根目录到路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.get_option_data import get_option_chain_info_by_data, get_option_quote, get_eastern_time
from utils.get_stock_data import get_stock_realtime_price
from data_models.option_quote import OptionQuote
from data_models.max_pain import MaxPain
from utils.logger import setup_logger
from utils.calculate_max_pain import calculate_max_pain


class MarketOpenCloseCollector:
    """开盘和收盘时段数据收集器类"""
    
    def __init__(self, stock_code: str, expiry_dates: List[date], strike_price_range: tuple[float, float]):
        """
        初始化数据收集器
        
        Args:
            stock_code: 股票代码
            expiry_dates: 到期日期列表
            strike_price_range: 行权价范围
        """
        self.stock_code = stock_code
        self.expiry_dates = expiry_dates
        self.strike_price_range = strike_price_range
        self.is_running = False
        self.collection_count = 0
        self.error_count = 0
        
        # 跟踪是否已在开盘窗口和收盘窗口收集过（每天重置）
        self.collected_in_open_window_today = False
        self.collected_in_close_window_today = False
        self.last_collection_date = None
        
        # 初始化日志记录器
        self.logger = setup_logger(f'MarketOpenCloseCollector-{stock_code}')
        
        # 设置信号处理
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
    
    def signal_handler(self, signum, frame):
        """信号处理器，用于优雅退出"""
        self.logger.info(f"接收到信号 {signum}，正在停止数据收集...")
        self.stop()
    
    def _collect_data_for_expiry_once(self, expiry_date: date):
        """执行一次数据收集操作（内部方法）"""
        # 当前时间
        eastern_time = get_eastern_time()
        update_time = eastern_time.strftime('%Y-%m-%d %H:%M:%S')

        self.logger.info(f"开始收集数据: {self.stock_code} - {expiry_date} - {update_time}")
        
        list_option_symbol = get_option_chain_info_by_data(self.stock_code, expiry_date, self.strike_price_range)
        if not list_option_symbol:
            self.logger.error(f"获取期权链信息失败: {self.stock_code} {expiry_date} {self.strike_price_range}")
            return False
        
        list_option_quote = get_option_quote(list_option_symbol, expiry_date, update_time)
        if not list_option_quote:
            self.logger.error(f"获取期权行情失败: {self.stock_code} {expiry_date} {self.strike_price_range}")
            return False
        
        # 保存到数据库
        saved_count = OptionQuote.batch_save(list_option_quote)
        if saved_count != len(list_option_quote):
            self.logger.error(f"保存期权报价数据失败: {self.stock_code} {expiry_date} {self.strike_price_range}")
            return False
        
        self.logger.info(f"数据收集成功，保存了 {saved_count} 条期权报价记录: {self.stock_code} - {expiry_date}")

        # 查询指定标的、到期日期和更新时间的所有期权报价
        expiry_date_str = expiry_date.strftime('%Y-%m-%d')
        option_quotes = OptionQuote.query({
            'underlying_symbol': self.stock_code,
            'expiry_date': expiry_date_str,
            'update_time': update_time
        })

        if option_quotes:
            max_pain = calculate_max_pain(option_quotes)
            max_pain.stock_price = get_stock_realtime_price(self.stock_code)
            max_pain.save()
            self.logger.info(f"保存最大痛点数据成功: {self.stock_code} {expiry_date} {update_time}")
        else:
            self.logger.error(f"未找到符合条件的期权报价数据: {self.stock_code} {expiry_date} {update_time}")
            return False
        
        return True
    
    def collect_data_for_expiry(self, expiry_date: date, max_retries: int = 3, retry_interval: int = 60):
        """
        收集指定到期日期的期权数据，带重试机制
        
        Args:
            expiry_date: 到期日期
            max_retries: 最大重试次数，默认3次
            retry_interval: 重试间隔（秒），默认60秒（1分钟）
        
        Returns:
            bool: 收集成功返回True，失败返回False
        """
        for attempt in range(1, max_retries + 1):
            try:
                if self._collect_data_for_expiry_once(expiry_date):
                    if attempt > 1:
                        self.logger.info(f"第 {attempt} 次尝试成功收集数据: {self.stock_code} - {expiry_date}")
                    return True
                else:
                    if attempt < max_retries:
                        self.logger.warning(f"第 {attempt} 次收集数据失败，将在 {retry_interval} 秒后重试: {self.stock_code} - {expiry_date}")
                        time.sleep(retry_interval)
                    else:
                        self.logger.error(f"收集数据失败，已达到最大重试次数 {max_retries}: {self.stock_code} - {expiry_date}")
                        self.error_count += 1
                        return False
                        
            except Exception as e:
                if attempt < max_retries:
                    self.logger.warning(f"第 {attempt} 次收集数据时发生异常，将在 {retry_interval} 秒后重试: {e}")
                    time.sleep(retry_interval)
                else:
                    self.logger.error(f"收集数据时发生异常，已达到最大重试次数 {max_retries}: {e}", exc_info=True)
                    self.error_count += 1
                    return False
        
        return False
    
    def collect_data(self):
        """收集所有到期日期的期权数据"""
        success_count = 0
        for expiry_date in self.expiry_dates:
            if self.collect_data_for_expiry(expiry_date):
                success_count += 1
        
        if success_count == len(self.expiry_dates):
            self.collection_count += 1
            self.logger.info(f"所有到期日期的数据收集完成: {success_count}/{len(self.expiry_dates)}")
        else:
            self.logger.warning(f"部分到期日期的数据收集失败: {success_count}/{len(self.expiry_dates)}")
    
    def _reset_daily_flags_if_needed(self):
        """如果是新的一天，重置收集标志"""
        eastern_time = get_eastern_time()
        current_date = eastern_time.date()
        
        if self.last_collection_date != current_date:
            self.collected_in_open_window_today = False
            self.collected_in_close_window_today = False
            self.last_collection_date = current_date
            self.logger.info(f"新的一天开始，重置收集标志: {current_date}")
    
    def is_in_open_window(self) -> bool:
        """检查是否在开盘后10分钟窗口内（9:30-9:40）"""
        try:
            eastern_time = get_eastern_time()
            current_time = eastern_time.time()
            
            # 开盘时间: 9:30 AM (美东时间)
            market_open = eastern_time.replace(hour=9, minute=30, second=0).time()
            # 开盘后10分钟: 9:40 AM
            open_window_end = eastern_time.replace(hour=9, minute=40, second=0).time()
            
            return market_open <= current_time <= open_window_end
            
        except Exception as e:
            self.logger.error(f"检查开盘窗口时间失败: {e}", exc_info=True)
            return False
    
    def is_in_close_window(self) -> bool:
        """检查是否在收盘后10分钟窗口内（16:00-16:10）"""
        try:
            eastern_time = get_eastern_time()
            current_time = eastern_time.time()
            
            # 收盘时间: 4:00 PM (美东时间)
            market_close = eastern_time.replace(hour=16, minute=0, second=0).time()
            # 收盘后10分钟: 4:10 PM
            close_window_end = eastern_time.replace(hour=16, minute=10, second=0).time()
            
            return market_close <= current_time <= close_window_end
            
        except Exception as e:
            self.logger.error(f"检查收盘窗口时间失败: {e}", exc_info=True)
            return False
    
    def collect_data_if_in_window(self):
        """在收集窗口内收集数据（每个窗口只收集一次）"""
        self._reset_daily_flags_if_needed()
        
        # 检查开盘窗口
        if self.is_in_open_window() and not self.collected_in_open_window_today:
            self.logger.info("在开盘后10分钟窗口内，开始收集数据...")
            self.collect_data()
            self.collected_in_open_window_today = True
            return
        
        # 检查收盘窗口
        if self.is_in_close_window() and not self.collected_in_close_window_today:
            self.logger.info("在收盘后10分钟窗口内，开始收集数据...")
            self.collect_data()
            self.collected_in_close_window_today = True
            return
    
    def start(self):
        """启动收集器"""
        self.logger.info("启动开盘和收盘时段数据收集器")
        self.logger.info(f"目标股票: {self.stock_code}")
        self.logger.info(f"到期日期列表: {[d.strftime('%Y-%m-%d') for d in self.expiry_dates]}")
        self.logger.info(f"行权价范围: {self.strike_price_range}")
        self.logger.info("收集窗口: 开盘后10分钟 (9:30-9:40) 和收盘后10分钟 (16:00-16:10)")
        
        # 每分钟检查一次是否在收集窗口内
        schedule.every(1).minutes.do(self.collect_data_if_in_window)
        
        self.run_scheduler()
    
    def run_scheduler(self):
        """运行调度器"""
        self.is_running = True
        
        # 启动时立即执行一次数据收集
        self.logger.info("立即执行启动时的数据收集...")
        self.collect_data()
        
        self.logger.info("调度器已启动，将在开盘后10分钟（9:40）和收盘后10分钟（16:10）各收集一次数据")
        self.logger.info("按 Ctrl+C 停止...")
        
        try:
            while self.is_running:
                schedule.run_pending()
                time.sleep(1)
        except KeyboardInterrupt:
            self.logger.info("接收到键盘中断信号")
        finally:
            self.stop()
    
    def stop(self):
        """停止数据收集器"""
        self.is_running = False
        schedule.clear()
        
        self.logger.info("数据收集器已停止")
        self.logger.info(f"总计收集次数: {self.collection_count}")
        self.logger.info(f"总错误次数: {self.error_count}")
        
        total_attempts = self.collection_count + self.error_count
        if total_attempts > 0:
            success_rate = (self.collection_count / total_attempts) * 100
            self.logger.info(f"成功率: {success_rate:.1f}%")


def main(stock_code: str, expiry_dates: List[date], strike_price_range: tuple[float, float]):
    """主函数"""
    logger = setup_logger('MarketOpenCloseCollector-Main')
    logger.info("=" * 60)
    logger.info("开盘和收盘时段期权数据收集器启动")
    logger.info("=" * 60)
    
    # 创建收集器实例
    collector = MarketOpenCloseCollector(stock_code, expiry_dates, strike_price_range)
    
    try:
        collector.start()
    except Exception as e:
        logger.error(f"程序运行出错: {e}", exc_info=True)
    finally:
        collector.stop()


if __name__ == "__main__":
    with open('stock_option_info.json', 'r') as f:
        stock_option_info = json.load(f)
    expiry_dates = [date.fromisoformat(expiry_date) for expiry_date in stock_option_info["expiry_dates"]]
    strike_price_range = tuple(stock_option_info["strike_price_range"])
    main(
        stock_code=stock_option_info["stock_code"],
        expiry_dates=expiry_dates,
        strike_price_range=strike_price_range
    )
