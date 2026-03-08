"""
定时期权数据收集器

这个脚本可以按设定的时间间隔自动收集期权数据，支持多种调度模式。
"""

import os
import sys
import time
import schedule
import threading
from datetime import datetime, date
import signal
from typing import Optional

# 添加项目根目录到路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.get_option_data import get_option_chain_info_by_data, get_option_quote, get_eastern_time
from utils.get_stock_data import get_stock_realtime_price
from data_models.option_quote import OptionQuote
from data_models.max_pain import MaxPain
from utils.logger import setup_logger
from utils.calculate_max_pain import calculate_max_pain


class ScheduledDataCollector:
    """定时数据收集器类"""
    
    def __init__(self, stock_code: str, expiry_date: date, strike_price_range: tuple[float, float]):
        """
        初始化数据收集器
        
        Args:
            stock_code: 股票代码
            expiry_date: 到期日期，如果为None则使用默认日期
            strike_price_range: 行权价范围
        """
        self.stock_code = stock_code
        self.expiry_date = expiry_date
        self.strike_price_range = strike_price_range
        self.is_running = False
        self.collection_count = 0
        self.error_count = 0
        
        # 初始化日志记录器
        self.logger = setup_logger(f'ScheduledDataCollector-{stock_code}')
        
        # 设置信号处理
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
    
    def signal_handler(self, signum, frame):
        """信号处理器，用于优雅退出"""
        self.logger.info(f"接收到信号 {signum}，正在停止数据收集...")
        self.stop()
    
    def collect_data(self):
        """收集期权数据"""
        try:
            # 当前时间
            eastern_time = get_eastern_time()
            update_time = eastern_time.strftime('%Y-%m-%d %H:%M:%S')

            self.logger.info(f"开始收集数据: {self.stock_code} - {self.expiry_date} - {update_time}")
            
            list_option_symbol = get_option_chain_info_by_data(self.stock_code, self.expiry_date, self.strike_price_range)
            if not list_option_symbol:
                self.logger.error(f"获取期权链信息失败: {self.stock_code} {self.expiry_date} {self.strike_price_range}")
                self.error_count += 1
                return
            
            list_option_quote = get_option_quote(list_option_symbol, self.expiry_date, update_time)
            if not list_option_quote:
                self.logger.error(f"获取期权行情失败: {self.stock_code} {self.expiry_date} {self.strike_price_range}")
                self.error_count += 1
                return
            
            # 保存到数据库
            saved_count = OptionQuote.batch_save(list_option_quote)
            if saved_count != len(list_option_quote):
                self.logger.error(f"保存期权报价数据失败: {self.stock_code} {expiry_date} {self.strike_price_range}")
                self.error_count += 1
                return
            
            self.collection_count += 1
            self.logger.info(f"数据收集成功，保存了 {saved_count} 条期权报价记录")

            # 查询指定标的、到期日期和更新时间的所有期权报价
            expiry_date_str = self.expiry_date.strftime('%Y-%m-%d')
            option_quotes = OptionQuote.query({
                'underlying_symbol': self.stock_code,
                'expiry_date': expiry_date_str,
                'update_time': update_time
            })

            if option_quotes:
                max_pain = calculate_max_pain(option_quotes)
                max_pain.stock_price = get_stock_realtime_price(self.stock_code)
                max_pain.save()
                self.logger.info(f"保存最大痛点数据成功: {self.stock_code} {self.expiry_date} {update_time}")
            else:
                self.logger.error(f"未找到符合条件的期权报价数据: {self.stock_code} {self.expiry_date} {update_time}")
                self.error_count += 1
        except Exception as e:
            self.logger.error(f"收集数据时发生异常: {e}", exc_info=True)
            self.error_count += 1

    def start_market_hours(self, interval_minutes: int = 10):
        """在交易时间内按间隔收集数据"""
        self.logger.info(f"启动定时收集器 - 交易时间内每 {interval_minutes} 分钟收集一次")
        self.logger.info(f"目标股票: {self.stock_code}")
        self.logger.info(f"到期日期: {self.expiry_date}")
        self.logger.info(f"行权价范围: {self.strike_price_range}")
        
        # 交易时间: 美东时间 9:30 - 16:00
        schedule.every(interval_minutes).minutes.do(self.collect_data_if_market_open)
        self.run_scheduler()
    
    def is_market_open(self) -> bool:
        """检查是否在交易时间内"""
        try:
            eastern_time = get_eastern_time()
            current_time = eastern_time.time()
            
            # 交易时间: 9:30 AM - 4:00 PM (美东时间)
            market_open = eastern_time.replace(hour=9, minute=30, second=0).time()
            # 交易结束时间: 16:15 (美东时间), 多出 15 分钟是为了获取收盘时的数据
            market_close = eastern_time.replace(hour=16, minute=15, second=0).time()
            
            # 检查是否为工作日 (周一到周五)
            weekday = eastern_time.weekday()  # 0=Monday, 6=Sunday
            
            return (weekday < 5 and market_open <= current_time <= market_close)
            
        except Exception as e:
            self.logger.error(f"检查交易时间失败: {e}", exc_info=True)
            return False
    
    def collect_data_if_market_open(self):
        """仅在交易时间内收集数据"""
        if self.is_market_open():
            self.collect_data()
        else:
            eastern_time = get_eastern_time()
            self.logger.info(f"当前时间 {eastern_time.strftime('%H:%M:%S')} 不在交易时间内，跳过数据收集")
    
    def run_scheduler(self):
        """运行调度器"""
        self.is_running = True
        
        # 立即执行一次
        self.logger.info("立即执行第一次数据收集...")
        self.collect_data()
        
        self.logger.info("调度器已启动，按 Ctrl+C 停止...")
        
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


def main(stock_code: str, expiry_date: date, strike_price_range: tuple[float, float]):
    """主函数"""
    logger = setup_logger('ScheduledDataCollector-Main')
    logger.info("=" * 60)
    logger.info("期权数据定时收集器启动")
    logger.info("=" * 60)
    
    # 创建收集器实例
    collector = ScheduledDataCollector(stock_code, expiry_date, strike_price_range)
    
    try:
        collector.start_market_hours(10)
    except Exception as e:
        logger.error(f"程序运行出错: {e}", exc_info=True)
    finally:
        collector.stop()

if __name__ == "__main__":
    eastern_time = get_eastern_time()
    main(stock_code="SPY.US", expiry_date=eastern_time.date(), strike_price_range=(650, 750))
