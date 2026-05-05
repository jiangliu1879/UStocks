from utils.logger import setup_logger
from init_database import db_manager
from mysql.connector import Error

logger = setup_logger('TradingTime')

class TradingTime:
    def __init__(self, trading_day: str, market_open_time: str, market_close_time: str):
        self.trading_day = trading_day
        self.market_open_time = market_open_time
        self.market_close_time = market_close_time

    def save(self) -> bool:
        connection = db_manager.get_connection()
        if not connection:
            return False
        try:
            cursor = connection.cursor()
            cursor.execute("INSERT INTO trading_time (trading_day, market_open_time, market_close_time) VALUES (%s, %s, %s)", (self.trading_day, self.market_open_time, self.market_close_time))
            connection.commit()
            return True
        except Error as e:
            logger.error(f"[TradingTime.save] 保存交易时间时出错: {e}", exc_info=True)
            return False

    @staticmethod
    def batch_save(list_trading_time: list['TradingTime']) -> bool:
        connection = db_manager.get_connection()
        if not connection:
            return False
        try:
            cursor = connection.cursor()
            cursor.executemany("INSERT INTO trading_time (trading_day, market_open_time, market_close_time) VALUES (%s, %s, %s)", [(trading_time.trading_day, trading_time.market_open_time, trading_time.market_close_time) for trading_time in list_trading_time])
            connection.commit()
            return True
        except Error as e:
            logger.error(f"[TradingTime.batch_save] 保存交易时间时出错: {e}", exc_info=True)
            return False

    @staticmethod
    def get_trading_time(start_date: str, end_date: str) -> list['TradingTime']:
        connection = db_manager.get_connection()
        if not connection:
            return None
        try:
            cursor = connection.cursor()
            cursor.execute("SELECT * FROM trading_time WHERE trading_day >= %s AND trading_day <= %s", (start_date, end_date))
            return [TradingTime(row[1], row[2], row[3]) for row in cursor.fetchall()]
        except Error as e:
            logger.error(f"[TradingTime.get_trading_time] 获取交易时间时出错: {e}", exc_info=True)
            return None


    @staticmethod
    def delete_by_trading_day(trading_day: str) -> bool:
        connection = db_manager.get_connection()
        if not connection:
            return False
        try:
            cursor = connection.cursor()
            cursor.execute("DELETE FROM trading_time WHERE trading_day = %s", (trading_day,))
            connection.commit()
            return True
        except Error as e:
            logger.error(f"[TradingTime.delete_by_trading_day] 删除交易时间时出错: {e}", exc_info=True)
            return False