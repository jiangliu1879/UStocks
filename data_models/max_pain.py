import sys
import os

# 添加项目根目录到 Python 路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from init_database import db_manager
from mysql.connector import Error
from typing import Optional, List, Dict
from utils.logger import setup_logger

# 创建模块级别的日志记录器
logger = setup_logger('MaxPain')

class MaxPain:
    def __init__(self, underlying_symbol: str, expiry_date: str, update_time: str, max_pain_oi: float, max_pain_vol: float, stock_price: float = 0, sum_vol: int = 0, sum_oi: int = 0, id: Optional[int] = None):
        self.id = id
        self.underlying_symbol = underlying_symbol
        self.expiry_date = expiry_date
        self.update_time = update_time
        self.max_pain_oi = max_pain_oi
        self.max_pain_vol = max_pain_vol
        self.stock_price = stock_price
        self.sum_vol = sum_vol
        self.sum_oi = sum_oi

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
                underlying_symbol, expiry_date, update_time, max_pain_oi, max_pain_vol, stock_price, sum_vol, sum_oi
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON DUPLICATE KEY UPDATE
                max_pain_oi = VALUES(max_pain_oi),
                max_pain_vol = VALUES(max_pain_vol),
                stock_price = VALUES(stock_price),
                sum_vol = VALUES(sum_vol),
                sum_oi = VALUES(sum_oi)
                    """
            
            # 确保所有值都转换为基本类型，处理 None 值
            values = (
                str(self.underlying_symbol) if self.underlying_symbol is not None else None,
                str(self.expiry_date) if self.expiry_date is not None else None,
                str(self.update_time) if self.update_time is not None else None,
                float(self.max_pain_oi) if self.max_pain_oi is not None else None,
                int(self.max_pain_vol) if self.max_pain_vol is not None else None,
                float(self.stock_price) if self.stock_price is not None else None,
                int(self.sum_vol) if self.sum_vol is not None else None,
                int(self.sum_oi) if self.sum_oi is not None else None
            )
            
            cursor.execute(insert_sql, values)
            connection.commit()
            
            # 如果是新插入的记录，获取生成的ID
            if cursor.lastrowid:
                self.id = cursor.lastrowid
            
            cursor.close()
            db_manager.close_connection(connection)
            logger.info(f"[MaxPain::save] 保存最大痛点数据成功: {self.underlying_symbol} - {self.expiry_date} - {self.update_time}")
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
            underlying_symbol=data['underlying_symbol'],
            expiry_date=str(data['expiry_date']),
            update_time=str(data['update_time']),
            max_pain_oi=float(data['max_pain_oi']) if data.get('max_pain_oi') is not None else 0.0,
            max_pain_vol=int(data['max_pain_vol']) if data.get('max_pain_vol') is not None else 0,
            stock_price=float(data['stock_price']) if data.get('stock_price') is not None else 0.0,
            sum_vol=int(data['sum_vol']) if data.get('sum_vol') is not None else 0,
            sum_oi=int(data['sum_oi']) if data.get('sum_oi') is not None else 0
        )

    @classmethod
    def get_by_underlying_symbol(cls, underlying_symbol: str, limit: Optional[int] = None, order_by: Optional[str] = None) -> List['MaxPain']:
        """
        根据标的股票代码查询所有最大痛点数据
        
        Args:
            underlying_symbol: 标的股票代码
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
            
            sql = "SELECT * FROM max_pain WHERE underlying_symbol = %s"
            
            if order_by:
                sql += f" ORDER BY {order_by}"
            else:
                sql += " ORDER BY update_time DESC, expiry_date DESC"
            
            if limit:
                sql += f" LIMIT {limit}"
            
            cursor.execute(sql, (underlying_symbol,))
            rows = cursor.fetchall()
            cursor.close()
            db_manager.close_connection(connection)
            
            return [cls.from_dict(row) for row in rows]
            
        except Error as e:
            logger.error(f"[MaxPain::get_by_underlying_symbol] 根据标的股票代码查询最大痛点数据时出错: {e}", exc_info=True)
            db_manager.close_connection(connection)
            return []
