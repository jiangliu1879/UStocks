"""
拆股信息模型：stock_code、拆股日期(timestamp)、拆股倍数(times)
"""
import sys
import os
from typing import Optional, List, Dict

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from init_database import db_manager
from mysql.connector import Error
from utils.logger import setup_logger

logger = setup_logger("StockSplit")


class StockSplit:
    """
    拆股信息：股票代码、拆股日期、拆股倍数。
    """

    def __init__(
        self,
        stock_code: str,
        timestamp: str,
        times: float,
        id: Optional[int] = None,
    ):
        self.id = id
        self.stock_code = stock_code
        self.timestamp = timestamp  # 拆股日期，字符串
        self.times = times  # 拆股倍数，浮点

    def save(self) -> bool:
        """保存到数据库，已存在则更新。"""
        connection = db_manager.get_connection()
        if not connection:
            return False
        try:
            cursor = connection.cursor()
            sql = """
            INSERT INTO stock_split (stock_code, timestamp, times)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE
                stock_code = VALUES(stock_code),
                timestamp = VALUES(timestamp),
                times = VALUES(times)
            """
            values = (
                str(self.stock_code) if self.stock_code is not None else None,
                str(self.timestamp) if self.timestamp is not None else None,
                float(self.times) if self.times is not None else None,
            )
            cursor.execute(sql, values)
            connection.commit()
            if cursor.lastrowid:
                self.id = cursor.lastrowid
            cursor.close()
            db_manager.close_connection(connection)
            return True
        except Error as e:
            logger.error(f"[StockSplit::save] 保存拆股数据时出错: {e}", exc_info=True)
            db_manager.close_connection(connection)
            return False

    @staticmethod
    def from_dict(data: Dict) -> "StockSplit":
        return StockSplit(
            stock_code=data["stock_code"],
            timestamp=str(data["timestamp"]),
            times=float(data["times"]) if data.get("times") is not None else 0.0,
            id=data.get("id"),
        )

    @classmethod
    def query(
        cls,
        conditions: Dict,
        limit: Optional[int] = None,
        order_by: Optional[str] = None,
    ) -> List["StockSplit"]:
        """按条件查询拆股记录。"""
        connection = db_manager.get_connection()
        if not connection:
            return []
        try:
            cursor = connection.cursor(dictionary=True)
            where_clauses = []
            values = []
            for key, value in conditions.items():
                where_clauses.append(f"{key} = %s")
                values.append(value)
            sql = "SELECT * FROM stock_split"
            if where_clauses:
                sql += " WHERE " + " AND ".join(where_clauses)
            sql += f" ORDER BY {order_by}" if order_by else " ORDER BY timestamp DESC"
            if limit:
                sql += f" LIMIT {limit}"
            cursor.execute(sql, tuple(values))
            rows = cursor.fetchall()
            cursor.close()
            db_manager.close_connection(connection)
            return [cls.from_dict(row) for row in rows]
        except Error as e:
            logger.error(f"[StockSplit::query] 查询拆股数据时出错: {e}", exc_info=True)
            db_manager.close_connection(connection)
            return []
