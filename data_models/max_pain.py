import sys
import os
from typing import Optional, List, Dict

# 添加项目根目录到 Python 路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from init_database import db_manager
from mysql.connector import Error
from utils.logger import setup_logger

# 创建模块级别的日志记录器
logger = setup_logger('MaxPain')


class MaxPain:
    """
    Max Pain 数据模型

    主字段（按新需求）:
    - underlying_ticker
    - expiry_date
    - max_pain_vol
    - max_pain_oi
    - sum_vol
    - sum_oi
    - ticker_price
    - update_time
    """

    def __init__(
        self,
        underlying_ticker: Optional[str] = None,
        expiry_date: str = "",
        max_pain_vol: float = 0.0,
        max_pain_oi: float = 0.0,
        sum_vol: float = 0.0,
        sum_oi: float = 0.0,
        ticker_price: float = 0.0,
        update_time: str = "",
        id: Optional[int] = None,
        # 兼容旧参数名
        underlying_symbol: Optional[str] = None,
        stock_price: Optional[float] = None,
    ):
        self.id = id
        self.underlying_ticker = underlying_ticker or underlying_symbol or ""
        self.expiry_date = expiry_date
        self.update_time = update_time
        self.max_pain_oi = max_pain_oi
        self.max_pain_vol = max_pain_vol
        self.ticker_price = ticker_price if ticker_price is not None else (stock_price if stock_price is not None else 0.0)
        self.sum_vol = sum_vol
        self.sum_oi = sum_oi

        # 向后兼容属性
        self.underlying_symbol = self.underlying_ticker
        self.stock_price = self.ticker_price

    def save(self) -> bool:
        """
        保存数据到数据库（如果已存在则更新）
        """
        connection = db_manager.get_connection()
        if not connection:
            return False

        try:
            cursor = connection.cursor()
            
            # 使用 INSERT ... ON DUPLICATE KEY UPDATE 处理唯一约束冲突
            insert_sql = """
            INSERT INTO max_pain (
                underlying_ticker, expiry_date, update_time, max_pain_oi, max_pain_vol, ticker_price, sum_vol, sum_oi
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON DUPLICATE KEY UPDATE
                max_pain_oi = VALUES(max_pain_oi),
                max_pain_vol = VALUES(max_pain_vol),
                ticker_price = VALUES(ticker_price),
                sum_vol = VALUES(sum_vol),
                sum_oi = VALUES(sum_oi)
                    """
            
            # 确保所有值都转换为基本类型，处理 None 值
            values = (
                str(self.underlying_ticker) if self.underlying_ticker is not None else None,
                str(self.expiry_date) if self.expiry_date is not None else None,
                str(self.update_time) if self.update_time is not None else None,
                float(self.max_pain_oi) if self.max_pain_oi is not None else None,
                float(self.max_pain_vol) if self.max_pain_vol is not None else None,
                float(self.ticker_price) if self.ticker_price is not None else None,
                float(self.sum_vol) if self.sum_vol is not None else None,
                float(self.sum_oi) if self.sum_oi is not None else None
            )
            
            cursor.execute(insert_sql, values)
            connection.commit()
            
            # 如果是新插入的记录，获取生成的ID
            if cursor.lastrowid:
                self.id = cursor.lastrowid
            
            cursor.close()
            db_manager.close_connection(connection)
            logger.info(f"[MaxPain::save] 保存最大痛点数据成功: {self.underlying_ticker} - {self.expiry_date} - {self.update_time}")
            return True
        except Error as e:
            logger.error(f"[MaxPain::save] 保存最大痛点数据时出错: {e}", exc_info=True)
            connection.rollback()
            db_manager.close_connection(connection)
            return False

    @staticmethod
    def from_dict(data: Dict) -> 'MaxPain':
        """
        从字典创建MaxPain对象
        
        Args:
            data: 包含字段数据的字典
            
        Returns:
            MaxPain对象
        """
        return MaxPain(
            id=data.get('id'),
            underlying_ticker=data.get('underlying_ticker') or data.get('underlying_symbol'),
            expiry_date=str(data['expiry_date']),
            update_time=str(data['update_time']),
            max_pain_oi=float(data['max_pain_oi']) if data.get('max_pain_oi') is not None else 0.0,
            max_pain_vol=float(data['max_pain_vol']) if data.get('max_pain_vol') is not None else 0.0,
            ticker_price=float(data.get('ticker_price', data.get('stock_price', 0.0))) if (data.get('ticker_price') is not None or data.get('stock_price') is not None) else 0.0,
            sum_vol=float(data['sum_vol']) if data.get('sum_vol') is not None else 0.0,
            sum_oi=float(data['sum_oi']) if data.get('sum_oi') is not None else 0.0
        )

    @classmethod
    def get_by_underlying_ticker(cls, underlying_ticker: str, limit: Optional[int] = None, order_by: Optional[str] = None) -> List['MaxPain']:
        """
        根据标的股票代码查询所有最大痛点数据
        
        Args:
            underlying_ticker: 标的股票代码
            limit: 返回记录数量限制
            order_by: 排序字段，例如 'update_time DESC, expiry_date DESC'
            
        Returns:
            MaxPain对象列表
        """
        connection = db_manager.get_connection()
        if not connection:
            return []
        
        try:
            cursor = connection.cursor(dictionary=True)
            
            sql = "SELECT * FROM max_pain WHERE underlying_ticker = %s"
            
            if order_by:
                sql += f" ORDER BY {order_by}"
            else:
                sql += " ORDER BY update_time DESC, expiry_date DESC"
            
            if limit:
                sql += f" LIMIT {limit}"
            
            cursor.execute(sql, (underlying_ticker,))
            rows = cursor.fetchall()
            cursor.close()
            db_manager.close_connection(connection)
            
            return [cls.from_dict(row) for row in rows]
            
        except Error as e:
            logger.error(f"[MaxPain::get_by_underlying_ticker] 根据标的股票代码查询最大痛点数据时出错: {e}", exc_info=True)
            db_manager.close_connection(connection)
            return []

    @classmethod
    def get_by_underlying_symbol(cls, underlying_symbol: str, limit: Optional[int] = None, order_by: Optional[str] = None) -> List['MaxPain']:
        """兼容旧接口名。"""
        return cls.get_by_underlying_ticker(underlying_symbol, limit=limit, order_by=order_by)
