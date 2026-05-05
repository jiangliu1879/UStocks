import os
import mysql.connector
from mysql.connector import Error
from utils.logger import setup_logger
import threading
import time
import atexit
from queue import Queue, Empty

# 创建模块级别的日志记录器
logger = setup_logger('InitDatabase')

# 数据库配置 - 从环境变量读取
DB_CONFIG = {
    'host': os.getenv('MYSQL_HOST'),
    'user': os.getenv('MYSQL_USER'),
    'password': os.getenv('MYSQL_PASS'),
    'database': 'ustocks',
    'port': 3306,
    'charset': 'utf8mb4',
    'collation': 'utf8mb4_unicode_ci'
}

# 连接池配置
POOL_CONFIG = {
    'max_size': 10,  # 连接池最大连接数
    'min_size': 2,   # 连接池最小连接数
    'timeout': 5,    # 获取连接的超时时间（秒）
    'idle_timeout': 300,  # 连接空闲超时时间（秒），超过此时间未使用的连接将被关闭
    'reconnect_interval': 60,  # 连接检查间隔（秒）
}


class DatabaseConnectionManager:
    """
    数据库连接管理器，维护连接池并管理数据库连接的生命周期
    """
    
    def __init__(self, db_config=None, pool_config=None):
        """
        初始化数据库连接管理器
        
        Args:
            db_config: 数据库配置字典，如果为None则使用默认配置
            pool_config: 连接池配置字典，如果为None则使用默认配置
        """
        self.db_config = db_config or DB_CONFIG.copy()
        self.pool_config = pool_config or POOL_CONFIG.copy()
        
        # 连接池
        self._connection_pool = Queue(maxsize=self.pool_config['max_size'])
        self._pool_lock = threading.Lock()
        self._pool_initialized = False
        self._active_connections = {}  # 跟踪活跃连接及其最后使用时间
        self._cleanup_thread = None  # 后台清理线程
        self._stop_cleanup = threading.Event()  # 停止清理线程的事件
        
        # 注册程序退出时的清理函数
        atexit.register(self.close_all_connections)
    
    def _create_connection(self, use_database=True):
        """
        创建新的数据库连接
        
        Args:
            use_database: 是否连接到指定数据库，如果为False则只连接服务器
        
        Returns:
            connection: MySQL连接对象，如果连接失败则返回None
        """
        try:
            config = self.db_config.copy()
            if not use_database:
                config.pop('database', None)
            connection = mysql.connector.connect(**config)
            if connection.is_connected():
                return connection
        except Error as e:
            logger.error(f"[DatabaseConnectionManager::_create_connection] 创建数据库连接时出错: {e}", exc_info=True)
            return None
        return None
    
    def _is_connection_valid(self, connection):
        """
        检查连接是否有效
        
        Args:
            connection: MySQL连接对象
        
        Returns:
            bool: 连接是否有效
        """
        if connection is None:
            return False
        try:
            if connection.is_connected():
                # 使用 ping 方法检查连接是否真的可用
                connection.ping(reconnect=False, attempts=1, delay=0)
                return True
            return False
        except Exception:
            return False
    
    def _cleanup_worker(self):
        """后台清理线程的工作函数"""
        while not self._stop_cleanup.is_set():
            try:
                self.cleanup_idle_connections()
                # 等待指定的间隔时间或直到收到停止信号
                self._stop_cleanup.wait(self.pool_config['reconnect_interval'])
            except Exception as e:
                logger.error(f"[DatabaseConnectionManager::_cleanup_worker] 清理线程出错: {e}", exc_info=True)
    
    def _start_cleanup_thread(self):
        """启动后台清理线程"""
        if self._cleanup_thread is None or not self._cleanup_thread.is_alive():
            self._stop_cleanup.clear()
            self._cleanup_thread = threading.Thread(target=self._cleanup_worker, daemon=True, name="ConnectionPoolCleanup")
            self._cleanup_thread.start()
            logger.debug(f"[DatabaseConnectionManager::_start_cleanup_thread] 后台清理线程已启动")
    
    def _initialize_pool(self):
        """初始化连接池，创建最小数量的连接"""
        with self._pool_lock:
            if self._pool_initialized:
                return
            
            logger.info(f"[DatabaseConnectionManager::_initialize_pool] 初始化连接池，创建 {self.pool_config['min_size']} 个连接...")
            for _ in range(self.pool_config['min_size']):
                conn = self._create_connection()
                if conn:
                    self._connection_pool.put(conn)
                    self._active_connections[id(conn)] = time.time()
            
            self._pool_initialized = True
            logger.info(f"[DatabaseConnectionManager::_initialize_pool] 连接池初始化完成，当前连接数: {self._connection_pool.qsize()}")
            
            # 启动后台清理线程
            self._start_cleanup_thread()
    
    def get_connection(self, use_database=True):
        """
        从连接池获取数据库连接，如果池中没有可用连接则创建新连接
        
        Args:
            use_database: 是否连接到指定数据库，如果为False则只连接服务器
        
        Returns:
            connection: MySQL连接对象，如果连接失败则返回None
        """
        # 初始化连接池（如果尚未初始化）
        if not self._pool_initialized:
            self._initialize_pool()
        
        # 如果 use_database=False，直接创建新连接（不放入池中）
        if not use_database:
            conn = self._create_connection(use_database=False)
            if conn:
                db_info = conn.server_info
                logger.debug(f"[DatabaseConnectionManager::get_connection] 创建临时连接（不使用数据库）: {db_info}")
            return conn
        
        # 尝试从池中获取连接
        try:
            connection = self._connection_pool.get(timeout=self.pool_config['timeout'])
            
            # 检查连接是否有效
            if self._is_connection_valid(connection):
                self._active_connections[id(connection)] = time.time()
                logger.debug(f"[DatabaseConnectionManager::get_connection] 从连接池获取连接，当前池中连接数: {self._connection_pool.qsize()}")
                return connection
            else:
                # 连接无效，关闭并创建新连接
                logger.warning(f"[DatabaseConnectionManager::get_connection] 池中连接已失效，创建新连接")
                try:
                    connection.close()
                except Exception:
                    pass
                if id(connection) in self._active_connections:
                    del self._active_connections[id(connection)]
        except Empty:
            # 池中没有可用连接，创建新连接
            logger.debug(f"[DatabaseConnectionManager::get_connection] 连接池为空，创建新连接")
        
        # 创建新连接
        connection = self._create_connection(use_database=True)
        if connection:
            db_info = connection.server_info
            logger.debug(f"[DatabaseConnectionManager::get_connection] 成功创建新连接: {db_info}")
            self._active_connections[id(connection)] = time.time()
        else:
            logger.error(f"[DatabaseConnectionManager::get_connection] 无法创建数据库连接")
        
        return connection
    
    def close_connection(self, connection):
        """
        将数据库连接返回到连接池（而不是真正关闭）
        
        Args:
            connection: MySQL连接对象
        """
        if connection is None:
            return
        
        # 检查连接是否有效
        if not self._is_connection_valid(connection):
            logger.warning(f"[DatabaseConnectionManager::close_connection] 连接已失效，直接关闭")
            try:
                connection.close()
            except Exception:
                pass
            if id(connection) in self._active_connections:
                del self._active_connections[id(connection)]
            return
        
        # 将连接返回到池中
        try:
            # 检查池是否已满
            if self._connection_pool.full():
                # 池已满，关闭连接
                logger.debug(f"[DatabaseConnectionManager::close_connection] 连接池已满，关闭连接")
                connection.close()
                if id(connection) in self._active_connections:
                    del self._active_connections[id(connection)]
            else:
                # 将连接返回到池中
                self._connection_pool.put_nowait(connection)
                self._active_connections[id(connection)] = time.time()
                logger.debug(f"[DatabaseConnectionManager::close_connection] 连接已返回到连接池，当前池中连接数: {self._connection_pool.qsize()}")
        except Exception as e:
            # 如果无法放回池中，关闭连接
            logger.warning(f"[DatabaseConnectionManager::close_connection] 无法将连接返回到池中: {e}，关闭连接")
            try:
                connection.close()
            except Exception:
                pass
            if id(connection) in self._active_connections:
                del self._active_connections[id(connection)]
    
    def cleanup_idle_connections(self):
        """
        清理空闲超时的连接
        应该在后台线程中定期调用
        """
        current_time = time.time()
        connections_to_close = []
        
        with self._pool_lock:
            # 检查池中的连接
            temp_connections = []
            while not self._connection_pool.empty():
                try:
                    conn = self._connection_pool.get_nowait()
                    conn_id = id(conn)
                    last_used = self._active_connections.get(conn_id, current_time)
                    
                    if current_time - last_used > self.pool_config['idle_timeout']:
                        # 连接空闲超时，关闭它
                        connections_to_close.append(conn)
                        if conn_id in self._active_connections:
                            del self._active_connections[conn_id]
                    else:
                        temp_connections.append(conn)
                except Empty:
                    break
            
            # 将未超时的连接放回池中
            for conn in temp_connections:
                try:
                    self._connection_pool.put_nowait(conn)
                except Exception:
                    try:
                        conn.close()
                    except Exception:
                        pass
        
        # 关闭超时的连接
        for conn in connections_to_close:
            try:
                conn.close()
                logger.debug(f"[DatabaseConnectionManager::cleanup_idle_connections] 关闭空闲超时连接")
            except Exception:
                pass
    
    def close_all_connections(self):
        """
        关闭连接池中的所有连接
        应该在程序退出时调用
        """
        # 停止清理线程
        self._stop_cleanup.set()
        if self._cleanup_thread and self._cleanup_thread.is_alive():
            self._cleanup_thread.join(timeout=2)
        
        logger.info(f"[DatabaseConnectionManager::close_all_connections] 关闭连接池中的所有连接...")
        closed_count = 0
        
        with self._pool_lock:
            while not self._connection_pool.empty():
                try:
                    conn = self._connection_pool.get_nowait()
                    try:
                        conn.close()
                        closed_count += 1
                    except Exception:
                        pass
                    if id(conn) in self._active_connections:
                        del self._active_connections[id(conn)]
                except Empty:
                    break
        
        logger.info(f"[DatabaseConnectionManager::close_all_connections] 已关闭 {closed_count} 个连接")


# 创建全局数据库连接管理器实例
db_manager = DatabaseConnectionManager()

def create_database():
    """
    创建数据库ustocks（如果不存在）
    """
    connection = db_manager.get_connection(use_database=False)
    if not connection:
        return False
    
    try:
        cursor = connection.cursor()
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_CONFIG['database']} CHARACTER SET {DB_CONFIG['charset']} COLLATE {DB_CONFIG['collation']}")
        logger.info(f"[create_database] 数据库 {DB_CONFIG['database']} 创建成功（或已存在）")
        cursor.close()
        # use_database=False 的连接不放入池中，直接关闭
        try:
            connection.close()
            logger.debug(f"[create_database] 临时连接已关闭")
        except Exception:
            pass
        return True
    except Error as e:
        logger.error(f"[create_database] 创建数据库时出错: {e}", exc_info=True)
        # use_database=False 的连接不放入池中，直接关闭
        try:
            connection.close()
        except Exception:
            pass
        return False

def create_option_snapshot_day_table():
    """
    根据OptionSnapshotDay类创建option_snapshot_day数据表
    """
    connection = db_manager.get_connection()
    if not connection:
        return False
    
    try:
        cursor = connection.cursor()
        
        # 根据OptionQuote类的字段定义创建表结构
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS option_snapshot_day(
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            underlying_symbol VARCHAR(32) NOT NULL COMMENT '标的股票代码',
            expiry_date DATE NOT NULL COMMENT '到期日期',
            update_time DATETIME NOT NULL COMMENT '更新时间',
            strike_price DECIMAL(10, 2) NOT NULL COMMENT '行权价',
            option_symbol VARCHAR(50) NOT NULL COMMENT '期权代码',
            direction VARCHAR(32) NOT NULL COMMENT '方向（CALL/PUT）',
            last_done DECIMAL(10, 2) COMMENT '最新成交价',
            prev_close DECIMAL(10, 2) COMMENT '前收盘价',
            high DECIMAL(10, 2) COMMENT '最高价',
            low DECIMAL(10, 2) COMMENT '最低价',
            volume INT COMMENT '成交量',
            turnover DECIMAL(15, 2) COMMENT '成交额',
            open_interest INT COMMENT '持仓量',
            implied_volatility DECIMAL(8, 4) COMMENT '隐含波动率',
            historical_volatility DECIMAL(8, 4) COMMENT '历史波动率',
            contract_multiplier INT COMMENT '合约乘数',
            contract_type VARCHAR(32) COMMENT '合约类型',
            contract_size INT COMMENT '合约规模',
            INDEX idx_underlying_symbol (underlying_symbol),
            INDEX idx_expiry_date (expiry_date),
            INDEX idx_option_symbol (option_symbol),
            UNIQUE KEY uk_option_quote (option_symbol, expiry_date, update_time)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='期权行情表';
        """
        
        cursor.execute(create_table_sql)
        connection.commit()
        logger.info(f"[create_option_snapshot_day_table] 数据表 option_snapshot_day 创建成功（或已存在）")
        cursor.close()
        db_manager.close_connection(connection)
        return True
    except Error as e:
        logger.error(f"[create_option_snapshot_day_table] 创建数据表时出错: {e}", exc_info=True)
        db_manager.close_connection(connection)
        return False

def create_option_snapshot_min_table():
    """
    根据 OptionSnapshotMin 模型创建 option_snapshot_min 数据表
    """
    connection = db_manager.get_connection()
    if not connection:
        return False

    try:
        cursor = connection.cursor()
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS option_snapshot_min(
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            underlying_ticker VARCHAR(32) NOT NULL COMMENT '标的代码',
            ticker VARCHAR(64) NOT NULL COMMENT '期权代码',
            expiration_date DATE NOT NULL COMMENT '到期日期',
            strike_price DECIMAL(10, 2) NOT NULL COMMENT '行权价',
            volume BIGINT COMMENT '成交量',
            open_interest BIGINT COMMENT '持仓量',
            implied_volatility DECIMAL(10, 6) COMMENT '隐含波动率',
            contract_type VARCHAR(32) COMMENT '合约类型',
            delta DECIMAL(12, 8) COMMENT 'Delta',
            gamma DECIMAL(12, 8) COMMENT 'Gamma',
            theta DECIMAL(12, 8) COMMENT 'Theta',
            vega DECIMAL(12, 8) COMMENT 'Vega',
            update_time DATETIME NOT NULL COMMENT '更新时间',
            INDEX idx_underlying_ticker (underlying_ticker),
            INDEX idx_expiration_date (expiration_date),
            INDEX idx_update_time (update_time),
            UNIQUE KEY uk_option_snapshot_min (ticker, expiration_date, update_time)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='期权快照表';
        """
        cursor.execute(create_table_sql)
        connection.commit()
        logger.info("[create_option_snapshot_min_table] 数据表 option_snapshot_min 创建成功（或已存在）")
        cursor.close()
        db_manager.close_connection(connection)
        return True
    except Error as e:
        logger.error(f"[create_option_snapshot_min_table] 创建数据表时出错: {e}", exc_info=True)
        db_manager.close_connection(connection)
        return False

def create_max_pain_table():
    """
    根据MaxPain类创建max_pain数据表
    """
    connection = db_manager.get_connection()
    if not connection:
        return False
    
    try:
        cursor = connection.cursor()
        
        # 根据MaxPain类的字段定义创建表结构
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS max_pain (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            underlying_ticker VARCHAR(32) NOT NULL COMMENT '标的股票代码',
            expiry_date DATE NOT NULL COMMENT '到期日期',
            update_time DATETIME NOT NULL COMMENT '更新时间',
            max_pain_oi DECIMAL(15, 2) COMMENT '最大痛点持仓量',
            max_pain_vol DECIMAL(15, 2) COMMENT '最大痛点成交量',
            ticker_price DECIMAL(10, 2) COMMENT '标的价格',
            sum_vol DECIMAL(18, 2) COMMENT '总成交量',
            sum_oi DECIMAL(18, 2) COMMENT '总持仓量',
            INDEX idx_underlying_ticker (underlying_ticker),
            INDEX idx_expiry_date (expiry_date),
            INDEX idx_update_time (update_time),
            UNIQUE KEY uk_max_pain (underlying_ticker, expiry_date, update_time, ticker_price, sum_vol, sum_oi)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='最大痛点表';
        """
        
        cursor.execute(create_table_sql)
        connection.commit()
        logger.info(f"[create_max_pain_table] 数据表 max_pain 创建成功（或已存在）")
        cursor.close()
        db_manager.close_connection(connection)
        return True
    except Error as e:
        logger.error(f"[create_max_pain_table] 创建数据表时出错: {e}", exc_info=True)
        db_manager.close_connection(connection)
        return False

def create_stock_split_table():
    """
    根据 StockSplit 类创建 stock_split 拆股信息表
    """
    connection = db_manager.get_connection()
    if not connection:
        return False
    try:
        cursor = connection.cursor()
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS stock_split (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            stock_code VARCHAR(32) NOT NULL COMMENT '股票代码',
            timestamp VARCHAR(50) NOT NULL COMMENT '拆股日期',
            times DECIMAL(10, 4) NOT NULL COMMENT '拆股倍数',
            INDEX idx_stock_code (stock_code),
            UNIQUE KEY uk_stock_split (stock_code, timestamp)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='拆股信息表';
        """
        cursor.execute(create_table_sql)
        connection.commit()
        logger.info("[create_stock_split_table] 数据表 stock_split 创建成功（或已存在）")
        cursor.close()
        db_manager.close_connection(connection)
        return True
    except Error as e:
        logger.error(f"[create_stock_split_table] 创建数据表时出错: {e}", exc_info=True)
        db_manager.close_connection(connection)
        return False


def create_stock_valuation_table():
    """
    根据 StockValuation 类创建 stock_valuation 估值表
    """
    connection = db_manager.get_connection()
    if not connection:
        return False
    try:
        cursor = connection.cursor()
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS stock_valuation (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            stock_code VARCHAR(32) NOT NULL COMMENT '股票代码',
            valuation_range VARCHAR(64) NOT NULL COMMENT '估值区间',
            valuation_date VARCHAR(32)  NOT NULL COMMENT '估值日期',
            INDEX idx_stock_code (stock_code),
            INDEX idx_valuation_range (valuation_range),
            INDEX idx_valuation_date (valuation_date),
            UNIQUE KEY uk_stock_valuation (stock_code, valuation_range, valuation_date)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='股票估值表';
        """
        cursor.execute(create_table_sql)
        connection.commit()
        logger.info("[create_stock_valuation_table] 数据表 stock_valuation 创建成功（或已存在）")
        cursor.close()
        db_manager.close_connection(connection)
        return True
    except Error as e:
        logger.error(f"[create_stock_valuation_table] 创建数据表时出错: {e}", exc_info=True)
        db_manager.close_connection(connection)
        return False


def create_stock_data_table():
    """
    根据StockData类创建stock_data数据表
    """
    connection = db_manager.get_connection()
    if not connection:
        return False
    
    try:
        cursor = connection.cursor()
        
        # 根据StockData类的字段定义创建表结构
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS stock_data (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            stock_code VARCHAR(32) NOT NULL COMMENT '股票代码',
            timestamp VARCHAR(50) NOT NULL COMMENT '时间戳',
            open DECIMAL(10, 2) COMMENT '开盘价',
            high DECIMAL(10, 2) COMMENT '最高价',
            low DECIMAL(10, 2) COMMENT '最低价',
            close DECIMAL(10, 2) COMMENT '收盘价',
            volume BIGINT COMMENT '成交量',
            turnover DECIMAL(15, 2) COMMENT '成交额',
            description VARCHAR(8192) COMMENT '市场描述',
            INDEX idx_stock_code (stock_code),
            INDEX idx_timestamp (timestamp),
            UNIQUE KEY uk_stock_data (stock_code, timestamp)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='股票行情数据表';
        """
        
        cursor.execute(create_table_sql)
        connection.commit()
        logger.info(f"[create_stock_data_table] 数据表 stock_data 创建成功（或已存在）")
        cursor.close()
        db_manager.close_connection(connection)
        return True
    except Error as e:
        logger.error(f"[create_stock_data_table] 创建数据表时出错: {e}", exc_info=True)
        db_manager.close_connection(connection)
        return False

def create_stock_data_min_table():
    """
    根据StockDataMin类创建stock_data_min数据表
    """
    connection = db_manager.get_connection()
    if not connection:
        return False
    try:
        cursor = connection.cursor()
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS stock_data_min (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            stock_code VARCHAR(32) NOT NULL COMMENT '股票代码',
            timestamp VARCHAR(50) NOT NULL COMMENT '时间戳',
            open DECIMAL(10, 2) COMMENT '开盘价',
            high DECIMAL(10, 2) COMMENT '最高价',
            low DECIMAL(10, 2) COMMENT '最低价',
            close DECIMAL(10, 2) COMMENT '收盘价',
            volume BIGINT COMMENT '成交量',
            turnover DECIMAL(15, 2) COMMENT '成交额',
            vw DECIMAL(15, 2) COMMENT '成交量加权平均价',
            `interval` INT COMMENT '间隔时间',
            INDEX idx_stock_code (stock_code),
            INDEX idx_timestamp (timestamp),
            UNIQUE KEY uk_stock_data_min (stock_code, timestamp, `interval`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='股票行情数据分钟表';
        """
        cursor.execute(create_table_sql)
        connection.commit()
        logger.info(f"[create_stock_data_min_table] 数据表 stock_data_min 创建成功（或已存在）")
        cursor.close()
        db_manager.close_connection(connection)
        return True
    except Error as e:
        logger.error(f"[create_stock_data_min_table] 创建数据表时出错: {e}", exc_info=True)
        db_manager.close_connection(connection)
        return False

def create_trading_time_table():
    """
    根据TradingTime类创建trading_time数据表
    """
    connection = db_manager.get_connection()
    if not connection:
        return False
    try:
        cursor = connection.cursor()
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS trading_time (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            trading_day VARCHAR(32) NOT NULL COMMENT '交易日期',
            market_open_time VARCHAR(32) NOT NULL COMMENT '市场开盘时间',
            market_close_time VARCHAR(32) NOT NULL COMMENT '市场收盘时间',
            INDEX idx_trading_day (trading_day),
            INDEX idx_market_open_time (market_open_time),
            INDEX idx_market_close_time (market_close_time),
            UNIQUE KEY uk_trading_time (trading_day, market_open_time, market_close_time)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='交易时间表';
        """
        cursor.execute(create_table_sql)
        connection.commit()
        logger.info("[create_trading_time_table] 数据表 trading_time 创建成功（或已存在）")
        cursor.close()
        db_manager.close_connection(connection)
        return True
    except Error as e:
        logger.error(f"[create_trading_time_table] 创建数据表时出错: {e}", exc_info=True)
        db_manager.close_connection(connection)
        return False

def test_connection():
    """
    测试数据库连接
    """
    connection = db_manager.get_connection()
    if connection:
        cursor = connection.cursor()
        cursor.execute("SELECT DATABASE();")
        database_name = cursor.fetchone()
        logger.info(f"[test_connection] 当前使用的数据库: {database_name[0]}")
        cursor.close()
        db_manager.close_connection(connection)
        return True
    return False

if __name__ == "__main__":
    """
    主函数：创建数据库和数据表
    """
    logger.info("[__main__] 开始初始化数据库...")
    
    # 创建数据库
    if create_database():
        if create_option_snapshot_day_table():
            logger.info("[__main__] 数据表 option_quote 创建成功（或已存在）")
        else:
            logger.error("[__main__] 数据表 option_quote 创建失败！")

        if create_option_snapshot_min_table():
            logger.info("[__main__] 数据表 option_chain_snapshot 创建成功（或已存在）")
        else:
            logger.error("[__main__] 数据表 option_chain_snapshot 创建失败！")

        if create_max_pain_table():
            logger.info("[__main__] 数据表 max_pain 创建成功（或已存在）")
        else:
            logger.error("[__main__] 数据表 max_pain 创建失败！")

        if create_stock_data_table():
            logger.info("[__main__] 数据表 stock_data 创建成功（或已存在）")
        else:
            logger.error("[__main__] 数据表 stock_data 创建失败！")

        if create_stock_split_table():
            logger.info("[__main__] 数据表 stock_split 创建成功（或已存在）")
        else:
            logger.error("[__main__] 数据表 stock_split 创建失败！")

        if create_stock_valuation_table():
            logger.info("[__main__] 数据表 stock_valuation 创建成功（或已存在）")
        else:
            logger.error("[__main__] 数据表 stock_valuation 创建失败！")

        if create_stock_data_min_table():
            logger.info("[__main__] 数据表 stock_data_min 创建成功（或已存在）")
        else:
            logger.error("[__main__] 数据表 stock_data_min 创建失败！")

        if create_trading_time_table():
            logger.info("[__main__] 数据表 trading_time 创建成功（或已存在）")
        else:
            logger.error("[__main__] 数据表 trading_time 创建失败！")
    else:
        logger.error("[__main__] 数据库创建失败！")

    logger.info("[__main__] 数据库初始化完成！")
