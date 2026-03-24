"""
股票估值模型：stock_code、估值区间(valuation_range)、估值日期(valuation_date)
"""
import sys
import os
from typing import Optional, List, Dict

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from init_database import db_manager
from mysql.connector import Error
from utils.logger import setup_logger

logger = setup_logger("StockValuation")


class StockValuation:
    """
    股票估值：股票代码、估值区间、估值日期。
    """

    def __init__(
        self,
        stock_code: str,
        valuation_range: str,
        valuation_date: str,
        id: Optional[int] = None,
    ):
        self.id = id
        self.stock_code = stock_code
        self.valuation_range = valuation_range
        self.valuation_date = valuation_date

    def save(self) -> bool:
        """保存到数据库，已存在 (stock_code, valuation_range, valuation_date) 则更新。"""
        connection = db_manager.get_connection()
        if not connection:
            return False
        try:
            cursor = connection.cursor()
            sql = """
            INSERT INTO stock_valuation (stock_code, valuation_range, valuation_date)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE
                valuation_range = VALUES(valuation_range),
                valuation_date = VALUES(valuation_date)
            """
            values = (
                str(self.stock_code) if self.stock_code is not None else None,
                str(self.valuation_range) if self.valuation_range is not None else None,
                str(self.valuation_date) if self.valuation_date is not None else None,
            )
            cursor.execute(sql, values)
            connection.commit()
            if cursor.lastrowid:
                self.id = cursor.lastrowid
            cursor.close()
            db_manager.close_connection(connection)
            return True
        except Error as e:
            logger.error(f"[StockValuation::save] 保存估值数据时出错: {e}", exc_info=True)
            db_manager.close_connection(connection)
            return False

    @staticmethod
    def from_dict(data: Dict) -> "StockValuation":
        """从字典创建 StockValuation 对象。"""
        return StockValuation(
            id=data.get("id"),
            stock_code=data["stock_code"],
            valuation_range=data["valuation_range"],
            valuation_date=data.get("valuation_date") or ""
        )

    @classmethod
    def query(
        cls,
        conditions: Dict,
        limit: Optional[int] = None,
        order_by: Optional[str] = None,
    ) -> List["StockValuation"]:
        """根据条件查询多条记录。"""
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
            sql = "SELECT * FROM stock_valuation"
            if where_clauses:
                sql += " WHERE " + " AND ".join(where_clauses)
            if order_by:
                sql += f" ORDER BY {order_by}"
            if limit:
                sql += f" LIMIT {limit}"
            cursor.execute(sql, tuple(values))
            rows = cursor.fetchall()
            cursor.close()
            db_manager.close_connection(connection)
            return [cls.from_dict(row) for row in rows]
        except Error as e:
            logger.error(f"[StockValuation::query] 查询估值数据时出错: {e}", exc_info=True)
            db_manager.close_connection(connection)
            return []

    @classmethod
    def query_latest_by_stock_codes(cls, stock_codes: List[str]) -> Dict[str, "StockValuation"]:
        """
        批量查询多个股票代码的最新估值记录（按 valuation_date 最大）。

        Returns:
            Dict[stock_code, StockValuation]
        """
        if not stock_codes:
            return {}
        connection = db_manager.get_connection()
        if not connection:
            return {}
        try:
            cursor = connection.cursor(dictionary=True)
            placeholders = ", ".join(["%s"] * len(stock_codes))
            sql = f"""
            SELECT sv.*
            FROM stock_valuation sv
            INNER JOIN (
                SELECT stock_code, MAX(valuation_date) AS max_valuation_date
                FROM stock_valuation
                WHERE stock_code IN ({placeholders})
                GROUP BY stock_code
            ) t ON sv.stock_code = t.stock_code AND sv.valuation_date = t.max_valuation_date
            ORDER BY sv.stock_code ASC
            """
            cursor.execute(sql, tuple(stock_codes))
            rows = cursor.fetchall()
            cursor.close()
            db_manager.close_connection(connection)
            result: Dict[str, StockValuation] = {}
            for row in rows:
                item = cls.from_dict(row)
                result[item.stock_code] = item
            return result
        except Error as e:
            logger.error(f"[StockValuation::query_latest_by_stock_codes] 批量查询估值数据时出错: {e}", exc_info=True)
            db_manager.close_connection(connection)
            return {}
