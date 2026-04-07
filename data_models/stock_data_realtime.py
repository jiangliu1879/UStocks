import sys
import os
from typing import Optional, Dict

# 添加项目根目录到路径，以便导入 init_database
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from init_database import db_manager
from mysql.connector import Error
from utils.logger import setup_logger

# 创建模块级别的日志记录器
logger = setup_logger('StockDataRealTime')


class StockDataRealTime:
    """
    股票实时行情数据模型。
    """
    def __init__(
        self,
        symbol: str,
        last_done: float,
        prev_close: float,
        open: float,
        high: float,
        low: float,
        timestamp: str,
        volume: float,
        turnover: float,
        id: Optional[int] = None
    ):
        self.id = id
        self.symbol = symbol
        self.last_done = last_done
        self.prev_close = prev_close
        self.open = open
        self.high = high
        self.low = low
        self.timestamp = timestamp
        self.volume = volume
        self.turnover = turnover

    def save(self) -> bool:
        """
        保存实时行情到数据库（如果已存在则更新）。
        """
        connection = db_manager.get_connection()
        if not connection:
            return False

        try:
            cursor = connection.cursor()
            insert_sql = """
            INSERT INTO stock_data_realtime (
                symbol, last_done, prev_close, open, high, low, timestamp, volume, turnover
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON DUPLICATE KEY UPDATE
                last_done = VALUES(last_done),
                prev_close = VALUES(prev_close),
                open = VALUES(open),
                high = VALUES(high),
                low = VALUES(low),
                volume = VALUES(volume),
                turnover = VALUES(turnover)
            """

            values = (
                str(self.symbol) if self.symbol is not None else None,
                float(self.last_done) if self.last_done is not None else None,
                float(self.prev_close) if self.prev_close is not None else None,
                float(self.open) if self.open is not None else None,
                float(self.high) if self.high is not None else None,
                float(self.low) if self.low is not None else None,
                str(self.timestamp) if self.timestamp is not None else None,
                float(self.volume) if self.volume is not None else None,
                float(self.turnover) if self.turnover is not None else None,
            )

            cursor.execute(insert_sql, values)
            connection.commit()

            if cursor.lastrowid:
                self.id = cursor.lastrowid

            cursor.close()
            db_manager.close_connection(connection)
            return True
        except Error as e:
            logger.error(f"[StockDataRealTime::save] 保存实时行情时出错: {e}", exc_info=True)
            db_manager.close_connection(connection)
            return False

    @staticmethod
    def from_dict(data: Dict) -> 'StockDataRealTime':
        """
        从字典创建 StockDataRealTime 对象。
        """
        return StockDataRealTime(
            id=data.get('id'),
            symbol=data['symbol'],
            last_done=float(data['last_done']) if data.get('last_done') is not None else None,
            prev_close=float(data['prev_close']) if data.get('prev_close') is not None else None,
            open=float(data['open']) if data.get('open') is not None else None,
            high=float(data['high']) if data.get('high') is not None else None,
            low=float(data['low']) if data.get('low') is not None else None,
            timestamp=str(data['timestamp']),
            volume=float(data['volume']) if data.get('volume') is not None else None,
            turnover=float(data['turnover']) if data.get('turnover') is not None else None
        )
