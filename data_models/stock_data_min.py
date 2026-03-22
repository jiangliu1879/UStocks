import sys
import os
from datetime import datetime
from typing import Optional, List, Dict

# 添加项目根目录到路径，以便导入 init_database
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from init_database import db_manager
from mysql.connector import Error
from utils.logger import setup_logger

# 创建模块级别的日志记录器
logger = setup_logger('StockDataMin')


class StockDataMin:
    """
    Represents historical stock price data including OHLCV (Open, High, Low, Close, Volume)
    and additional metrics like turnover.
    """
    def __init__(self, stock_code: str, timestamp: str, open: float, high: float, low: float, close: float, volume: int, turnover: float, interval: int, id: Optional[int] = None):
        self.id = id
        self.stock_code = stock_code
        self.timestamp = timestamp
        self.open = open
        self.high = high
        self.low = low
        self.close = close
        self.volume = volume
        self.turnover = turnover
        self.interval = interval
        
    def save(self) -> bool:
        """
        保存数据到数据库（如果已存在则更新）
        
        Returns:
            bool: 保存成功返回True，失败返回False
        """
        connection = db_manager.get_connection()
        if not connection:
            return False
        
        try:
            cursor = connection.cursor()
            
            # 使用 INSERT ... ON DUPLICATE KEY UPDATE 处理唯一约束
            insert_sql = """
            INSERT INTO stock_data_min (
                stock_code, timestamp, open, high, low, close, volume, turnover, `interval`
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON DUPLICATE KEY UPDATE
                stock_code = VALUES(stock_code),
                timestamp = VALUES(timestamp),
                open = VALUES(open),
                high = VALUES(high),
                low = VALUES(low),
                close = VALUES(close),
                volume = VALUES(volume),
                turnover = VALUES(turnover),
                `interval` = VALUES(`interval`)
            """
            
            # 确保所有值都转换为基本类型，处理 None 值
            values = (
                str(self.stock_code) if self.stock_code is not None else None,
                str(self.timestamp) if self.timestamp is not None else None,
                float(self.open) if self.open is not None else None,
                float(self.high) if self.high is not None else None,
                float(self.low) if self.low is not None else None,
                float(self.close) if self.close is not None else None,
                int(self.volume) if self.volume is not None else None,
                float(self.turnover) if self.turnover is not None else None,
                int(self.interval) if self.interval is not None else None,
            )
            
            cursor.execute(insert_sql, values)
            connection.commit()
            
            # 如果是新插入的记录，获取生成的ID
            if cursor.lastrowid:
                self.id = cursor.lastrowid
            
            cursor.close()
            db_manager.close_connection(connection)
            return True
            
        except Error as e:
            logger.error(f"[StockDataMin::save] 保存股票数据时出错: {e}", exc_info=True)
            db_manager.close_connection(connection)
            return False

    @classmethod
    def batch_save(cls, data_list: List['StockDataMin'], batch_size: int = 1000) -> int:
        """
        批量保存多条数据到数据库
        
        Args:
            data_list: StockDataMin对象列表
            batch_size: 每批处理的记录数量，默认1000条
            
        Returns:
            int: 成功保存的记录数量
        """
        if not data_list:
            return 0
        
        connection = db_manager.get_connection()
        if not connection:
            return 0
        
        try:
            cursor = connection.cursor()
            
            insert_sql = """
            INSERT INTO stock_data_min (
                stock_code, timestamp, open, high, low, close, volume, turnover, `interval`
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON DUPLICATE KEY UPDATE
                stock_code = VALUES(stock_code),
                timestamp = VALUES(timestamp),
                open = VALUES(open),
                high = VALUES(high),
                low = VALUES(low),
                close = VALUES(close),
                volume = VALUES(volume),
                turnover = VALUES(turnover),
                `interval` = VALUES(`interval`)
            """
            
            values_list = []
            for item in data_list:
                values = (
                    str(item.stock_code) if item.stock_code is not None else None,
                    str(item.timestamp) if item.timestamp is not None else None,
                    float(item.open) if item.open is not None else None,
                    float(item.high) if item.high is not None else None,
                    float(item.low) if item.low is not None else None,
                    float(item.close) if item.close is not None else None,
                    int(item.volume) if item.volume is not None else None,
                    float(item.turnover) if item.turnover is not None else None,
                    int(item.interval) if item.interval is not None else None,
                )
                values_list.append(values)
            
            saved_count = 0
            for i in range(0, len(values_list), batch_size):
                batch = values_list[i:i + batch_size]
                cursor.executemany(insert_sql, batch)
                saved_count += len(batch)
            
            connection.commit()
            cursor.close()
            db_manager.close_connection(connection)
            
            logger.info(f"[StockDataMin::batch_save] 批量保存股票数据完成，共保存 {saved_count} 条记录")
            return saved_count
            
        except Error as e:
            logger.error(f"[StockDataMin::batch_save] 批量保存股票数据时出错: {e}", exc_info=True)
            connection.rollback()
            db_manager.close_connection(connection)
            return 0

    @staticmethod
    def from_dict(data: Dict) -> 'StockDataMin':
        """
        从字典创建StockDataMin对象
        
        Args:
            data: 包含字段数据的字典
            
        Returns:
            StockDataMin对象
        """
        return StockDataMin(
            id=data.get('id'),
            stock_code=data['stock_code'],
            timestamp=str(data['timestamp']),
            open=float(data['open']) if data.get('open') is not None else None,
            high=float(data['high']) if data.get('high') is not None else None,
            low=float(data['low']) if data.get('low') is not None else None,
            close=float(data['close']) if data.get('close') is not None else None,
            volume=int(data['volume']) if data.get('volume') is not None else None,
            turnover=float(data['turnover']) if data.get('turnover') is not None else None,
            interval=int(data['interval']) if data.get('interval') is not None else None,
        )

    @classmethod
    def get_stock_codes(cls) -> List[str]:
        """
        获取所有股票代码（去重）
        
        Returns:
            去重后的股票代码列表
        """
        connection = db_manager.get_connection()
        if not connection:
            return []
        
        try:
            cursor = connection.cursor()
            # 直接在数据库层面使用 DISTINCT 查询唯一股票代码，避免加载所有数据
            sql = "SELECT DISTINCT stock_code FROM stock_data_min ORDER BY stock_code ASC"
            cursor.execute(sql)
            rows = cursor.fetchall()
            cursor.close()
            db_manager.close_connection(connection)
            
            # 提取股票代码（rows 是元组列表，每个元组只有一个元素）
            stock_codes = [row[0] for row in rows]
            return stock_codes
            
        except Error as e:
            logger.error(f"[StockDataMin::get_stock_codes] 获取股票代码列表时出错: {e}", exc_info=True)
            db_manager.close_connection(connection)
            return []

    @classmethod
    def query(cls, conditions: Dict, limit: Optional[int] = None, order_by: Optional[str] = None) -> List['StockDataMin']:
        """
        根据条件查询多条记录
        
        Args:
            conditions: 查询条件字典，例如 {'stock_code': 'NVDA.US'}
            limit: 返回记录数量限制
            order_by: 排序字段，例如 'timestamp DESC'
            
        Returns:
            StockDataMin对象列表
        """
        connection = db_manager.get_connection()
        if not connection:
            return []
        
        try:
            cursor = connection.cursor(dictionary=True)
            
            where_clauses = []
            values = []
            for key, value in conditions.items():
                # `interval` 在 MySQL 中是保留关键字，统一对字段名加反引号更安全
                where_clauses.append(f"`{key}` = %s")
                values.append(value)
            
            sql = "SELECT * FROM stock_data_min"
            if where_clauses:
                sql += " WHERE " + " AND ".join(where_clauses)
            
            if order_by:
                sql += f" ORDER BY {order_by}"
            else:
                sql += " ORDER BY timestamp DESC"
            
            if limit:
                sql += f" LIMIT {limit}"
            
            cursor.execute(sql, tuple(values))
            rows = cursor.fetchall()
            cursor.close()
            db_manager.close_connection(connection)
            
            return [cls.from_dict(row) for row in rows]
            
        except Error as e:
            logger.error(f"[StockDataMin::query] 查询股票数据时出错: {e}", exc_info=True)
            db_manager.close_connection(connection)
            return []

    @classmethod
    def delete_by_stock_code(cls, stock_code: str) -> int:
        """
        删除指定股票代码在 stock_data_min 表中的全部记录。

        Args:
            stock_code: 股票代码，如 "NVDA.US"

        Returns:
            int: 被删除的行数，失败返回 0
        """
        connection = db_manager.get_connection()
        if not connection:
            return 0
        try:
            cursor = connection.cursor()
            sql = "DELETE FROM stock_data_min WHERE stock_code = %s"
            cursor.execute(sql, (stock_code,))
            deleted = cursor.rowcount
            connection.commit()
            cursor.close()
            db_manager.close_connection(connection)
            logger.info(f"[StockDataMin::delete_by_stock_code] 已删除 {stock_code} 共 {deleted} 条记录")
            return deleted
        except Error as e:
            logger.error(f"[StockDataMin::delete_by_stock_code] 删除时出错: {e}", exc_info=True)
            connection.rollback()
            db_manager.close_connection(connection)
            return 0