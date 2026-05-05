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
logger = setup_logger('OptionSnapshotDay')


class OptionSnapshotDay:
    """
    Represents option snapshot day data for a specific stock.
    """
    def __init__(self, underlying_symbol: str, expiry_date: str, update_time: str, strike_price: float, option_symbol: str, direction: str, last_done: float, prev_close: float, high: float, low: float, volume: int, turnover: float, open_interest: int, implied_volatility: float, historical_volatility: float, contract_multiplier: int, contract_type: str, contract_size: int, id: Optional[int] = None):
        self.id = id
        self.underlying_symbol = underlying_symbol
        self.expiry_date = expiry_date
        self.update_time = update_time
        self.strike_price = strike_price
        self.option_symbol = option_symbol
        self.direction = direction
        self.last_done = last_done
        self.prev_close = prev_close
        self.high = high
        self.low = low
        self.volume = volume
        self.turnover = turnover
        self.open_interest = open_interest
        self.implied_volatility = implied_volatility
        self.historical_volatility = historical_volatility
        self.contract_multiplier = contract_multiplier
        self.contract_type = contract_type
        self.contract_size = contract_size

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
            INSERT INTO option_snapshot_day (
                underlying_symbol, expiry_date, update_time, strike_price, option_symbol,
                direction, last_done, prev_close, high, low, volume, turnover,
                open_interest, implied_volatility, historical_volatility, contract_multiplier,
                contract_type, contract_size
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON DUPLICATE KEY UPDATE
                underlying_symbol = VALUES(underlying_symbol),
                expiry_date = VALUES(expiry_date),
                update_time = VALUES(update_time),
                strike_price = VALUES(strike_price),
                direction = VALUES(direction),
                last_done = VALUES(last_done),
                prev_close = VALUES(prev_close),
                high = VALUES(high),
                low = VALUES(low),
                volume = VALUES(volume),
                turnover = VALUES(turnover),
                open_interest = VALUES(open_interest),
                implied_volatility = VALUES(implied_volatility),
                historical_volatility = VALUES(historical_volatility),
                contract_multiplier = VALUES(contract_multiplier),
                contract_type = VALUES(contract_type),
                contract_size = VALUES(contract_size)
            """
            
            # 确保所有值都转换为基本类型，处理 None 值
            values = (
                str(self.underlying_symbol) if self.underlying_symbol is not None else None,
                str(self.expiry_date) if self.expiry_date is not None else None,
                str(self.update_time) if self.update_time is not None else None,
                float(self.strike_price) if self.strike_price is not None else None,
                str(self.option_symbol) if self.option_symbol is not None else None,
                str(self.direction) if self.direction is not None else None,
                float(self.last_done) if self.last_done is not None else None,
                float(self.prev_close) if self.prev_close is not None else None,
                float(self.high) if self.high is not None else None,
                float(self.low) if self.low is not None else None,
                int(self.volume) if self.volume is not None else None,
                float(self.turnover) if self.turnover is not None else None,
                int(self.open_interest) if self.open_interest is not None else None,
                float(self.implied_volatility) if self.implied_volatility is not None else None,
                float(self.historical_volatility) if self.historical_volatility is not None else None,
                int(self.contract_multiplier) if self.contract_multiplier is not None else None,
                str(self.contract_type) if self.contract_type is not None else None,
                int(self.contract_size) if self.contract_size is not None else None
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
            logger.error(f"[OptionSnapshotDay::save] 保存期权快照日数据时出错: {e}", exc_info=True)
            db_manager.close_connection(connection)
            return False

    @classmethod
    def batch_save(cls, snapshots: List['OptionSnapshotDay'], batch_size: int = 1000) -> int:
        """
        批量保存多条数据到数据库
        
        Args:
            snapshots: OptionSnapshotDay对象列表
            batch_size: 每批处理的记录数量，默认1000条
            
        Returns:
            int: 成功保存的记录数量
        """
        if not snapshots:
            return 0
        
        connection = db_manager.get_connection()
        if not connection:
            return 0
        
        try:
            cursor = connection.cursor()
            
            # 使用 INSERT ... ON DUPLICATE KEY UPDATE 处理唯一约束
            insert_sql = """
            INSERT INTO option_snapshot_day (
                underlying_symbol, expiry_date, update_time, strike_price, option_symbol,
                direction, last_done, prev_close, high, low, volume, turnover,
                open_interest, implied_volatility, historical_volatility, contract_multiplier,
                contract_type, contract_size
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON DUPLICATE KEY UPDATE
                underlying_symbol = VALUES(underlying_symbol),
                expiry_date = VALUES(expiry_date),
                update_time = VALUES(update_time),
                strike_price = VALUES(strike_price),    
                option_symbol = VALUES(option_symbol),  
                direction = VALUES(direction),
                last_done = VALUES(last_done),
                prev_close = VALUES(prev_close),
                high = VALUES(high),   
                low = VALUES(low),
                volume = VALUES(volume),
                turnover = VALUES(turnover),   
                open_interest = VALUES(open_interest),
                implied_volatility = VALUES(implied_volatility),
                historical_volatility = VALUES(historical_volatility),
                contract_multiplier = VALUES(contract_multiplier),
                contract_type = VALUES(contract_type),
                contract_size = VALUES(contract_size)
            """
            
            # 准备批量数据，确保所有值都是 MySQL 可以接受的类型
            values_list = []
            for snapshot in snapshots:
                # 确保所有值都转换为基本类型，处理 None 值
                values = (
                    str(snapshot.underlying_symbol) if snapshot.underlying_symbol is not None else None,
                    str(snapshot.expiry_date) if snapshot.expiry_date is not None else None,
                    str(snapshot.update_time) if snapshot.update_time is not None else None,
                    float(snapshot.strike_price) if snapshot.strike_price is not None else None,
                    str(snapshot.option_symbol) if snapshot.option_symbol is not None else None,
                    str(snapshot.direction) if snapshot.direction is not None else None,
                    float(snapshot.last_done) if snapshot.last_done is not None else None,
                    float(snapshot.prev_close) if snapshot.prev_close is not None else None,
                    float(snapshot.high) if snapshot.high is not None else None,
                    float(snapshot.low) if snapshot.low is not None else None,
                    int(snapshot.volume) if snapshot.volume is not None else None,
                    float(snapshot.turnover) if snapshot.turnover is not None else None,
                    int(snapshot.open_interest) if snapshot.open_interest is not None else None,
                    float(snapshot.implied_volatility) if snapshot.implied_volatility is not None else None,
                    float(snapshot.historical_volatility) if snapshot.historical_volatility is not None else None,
                    int(snapshot.contract_multiplier) if snapshot.contract_multiplier is not None else None,
                    str(snapshot.contract_type) if snapshot.contract_type is not None else None,
                    int(snapshot.contract_size) if snapshot.contract_size is not None else None
                )
                values_list.append(values)
            
            # 分批执行，避免一次性插入过多数据
            saved_count = 0
            for i in range(0, len(values_list), batch_size):
                batch = values_list[i:i + batch_size]
                cursor.executemany(insert_sql, batch)
                saved_count += len(batch)
            
            connection.commit()
            cursor.close()
            db_manager.close_connection(connection)
            
            logger.info(f"[OptionSnapshotDay::batch_save] 批量保存期权快照日完成，共保存 {saved_count} 条记录")
            return saved_count
            
        except Error as e:
            logger.error(f"[OptionSnapshotDay::batch_save] 批量保存期权快照日数据时出错: {e}", exc_info=True)
            connection.rollback()
            db_manager.close_connection(connection)
            return 0

    @staticmethod
    def from_dict(data: Dict) -> 'OptionSnapshotDay':
        """
        从字典创建OptionSnapshotDay对象
        
        Args:
            data: 包含字段数据的字典
            
        Returns:
            OptionSnapshotDay对象
        """
        return OptionSnapshotDay(
            id=data.get('id'),
            underlying_symbol=data['underlying_symbol'],
            expiry_date=str(data['expiry_date']),
            update_time=str(data['update_time']),
            strike_price=float(data['strike_price']),
            option_symbol=data['option_symbol'],
            direction=data['direction'],
            last_done=float(data['last_done']) if data.get('last_done') is not None else None,
            prev_close=float(data['prev_close']) if data.get('prev_close') is not None else None,
            high=float(data['high']) if data.get('high') is not None else None,
            low=float(data['low']) if data.get('low') is not None else None,
            volume=int(data['volume']) if data.get('volume') is not None else None,
            turnover=float(data['turnover']) if data.get('turnover') is not None else None,
            open_interest=int(data['open_interest']) if data.get('open_interest') is not None else None,
            implied_volatility=float(data['implied_volatility']) if data.get('implied_volatility') is not None else None,
            historical_volatility=float(data['historical_volatility']) if data.get('historical_volatility') is not None else None,
            contract_multiplier=int(data['contract_multiplier']) if data.get('contract_multiplier') is not None else None,
            contract_type=data.get('contract_type'),
            contract_size=int(data['contract_size']) if data.get('contract_size') is not None else None
        )

    @classmethod
    def query(cls, conditions: Dict, limit: Optional[int] = None, order_by: Optional[str] = None) -> List['OptionSnapshotDay']:
        """
        根据条件查询多条记录
        
        Args:
            conditions: 查询条件字典，例如 {'underlying_symbol': 'AAPL', 'expiry_date': '2026-01-23', 'update_time': '2026-01-19 19:38:11'}
            limit: 返回记录数量限制
            order_by: 排序字段，例如 'update_time DESC'
            
        Returns:
            OptionQuote对象列表
        """
        connection = db_manager.get_connection()
        if not connection:
            return []
        
        try:
            cursor = connection.cursor(dictionary=True)
            
            # 构建WHERE子句
            where_clauses = []
            values = []
            for key, value in conditions.items():
                where_clauses.append(f"{key} = %s")
                values.append(value)
            
            sql = "SELECT * FROM option_quote"
            if where_clauses:
                sql += " WHERE " + " AND ".join(where_clauses)
            
            if order_by:
                sql += f" ORDER BY {order_by}"
            else:
                sql += " ORDER BY update_time DESC"
            
            if limit:
                sql += f" LIMIT {limit}"
            
            cursor.execute(sql, tuple(values))
            rows = cursor.fetchall()
            cursor.close()
            db_manager.close_connection(connection)
            
            return [cls.from_dict(row) for row in rows]
            
        except Error as e:
            logger.error(f"[OptionSnapshotDay::query] 查询期权快照日数据时出错: {e}", exc_info=True)
            db_manager.close_connection(connection)
            return []

    @classmethod
    def query_by_latest_update_date(cls, limit: int = 50000) -> tuple:
        """
        读取 update_time 最新日期的全部期权数据。
        Returns:
            (latest_date_str, list of OptionSnapshotDay)，latest_date_str 为 'YYYY-MM-DD'
        """
        connection = db_manager.get_connection()
        if not connection:
            return None, []
        try:
            cursor = connection.cursor(dictionary=True)
            cursor.execute("SELECT MAX(DATE(update_time)) AS d FROM option_snapshot_day")
            row = cursor.fetchone()
            if not row or not row.get("d"):
                cursor.close()
                db_manager.close_connection(connection)
                return None, []
            latest_date = row["d"]
            if hasattr(latest_date, "strftime"):
                latest_date_str = latest_date.strftime("%Y-%m-%d")
            else:
                latest_date_str = str(latest_date)[:10]
            cursor.execute(
                "SELECT * FROM option_snapshot_day WHERE DATE(update_time) = %s ORDER BY underlying_symbol, expiry_date, strike_price LIMIT %s",
                (latest_date_str, limit),
            )
            rows = cursor.fetchall()
            cursor.close()
            db_manager.close_connection(connection)
            return latest_date_str, [cls.from_dict(r) for r in rows]
        except Error as e:
            logger.error(f"[OptionSnapshotDay::query_by_latest_update_date] 查询出错: {e}", exc_info=True)
            db_manager.close_connection(connection)
            return None, []

