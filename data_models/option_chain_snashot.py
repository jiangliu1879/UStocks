from typing import Dict
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from init_database import db_manager
from mysql.connector import Error
from utils.logger import setup_logger

logger = setup_logger('OptionChainSnapshot')

class OptionChainSnapshot:
    """
    期权链快照数据模型（OptionChainSnapshot）。
    """

    def __init__(
        self,
        underlying_ticker: str,
        ticker: str,
        expiration_date: str,
        strike_price: float,
        volume: int,
        open_interest: int,
        implied_volatility: float,
        contract_type: str,
        delta: float,
        gamma: float,
        theta: float,
        vega: float,
        update_time: str,
    ):
        self.underlying_ticker = underlying_ticker
        self.ticker = ticker
        self.expiration_date = expiration_date
        self.strike_price = strike_price
        self.volume = volume
        self.open_interest = open_interest
        self.implied_volatility = implied_volatility
        self.contract_type = contract_type
        self.delta = delta
        self.gamma = gamma
        self.theta = theta
        self.vega = vega
        self.update_time = update_time

    @staticmethod
    def from_dict(data: Dict) -> "OptionChainSnapshot":
        return OptionChainSnapshot(
            underlying_ticker=str(data.get("underlying_ticker", "")),
            ticker=str(data.get("ticker", "")),
            expiration_date=str(data.get("expiration_date", "")),
            strike_price=float(data.get("strike_price", 0.0)) if data.get("strike_price") is not None else 0.0,
            volume=int(data.get("volume", 0)) if data.get("volume") is not None else 0,
            open_interest=int(data.get("open_interest", 0)) if data.get("open_interest") is not None else 0,
            implied_volatility=float(data.get("implied_volatility", 0.0)) if data.get("implied_volatility") is not None else 0.0,
            contract_type=str(data.get("contract_type", "")),
            delta=float(data.get("delta", 0.0)) if data.get("delta") is not None else 0.0,
            gamma=float(data.get("gamma", 0.0)) if data.get("gamma") is not None else 0.0,
            theta=float(data.get("theta", 0.0)) if data.get("theta") is not None else 0.0,
            vega=float(data.get("vega", 0.0)) if data.get("vega") is not None else 0.0,
            update_time=str(data.get("update_time", "")),
        )

    def save(self) -> bool:
        """
        保存数据到数据库（如果已存在则更新）
        """
        connection = db_manager.get_connection()
        if not connection:
            return False
        try:
            cursor = connection.cursor()
            sql = """
            INSERT INTO option_chain_snapshot (underlying_ticker, ticker, expiration_date, strike_price, volume, open_interest, implied_volatility, contract_type, delta, gamma, theta, vega, update_time)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                underlying_ticker = VALUES(underlying_ticker),
                ticker = VALUES(ticker),
                expiration_date = VALUES(expiration_date),
                strike_price = VALUES(strike_price),
                volume = VALUES(volume),
                open_interest = VALUES(open_interest),
                implied_volatility = VALUES(implied_volatility),
                contract_type = VALUES(contract_type),
                delta = VALUES(delta),
                gamma = VALUES(gamma),
                theta = VALUES(theta),
                vega = VALUES(vega),
                update_time = VALUES(update_time)
            """
            values = (
                str(self.underlying_ticker) if self.underlying_ticker is not None else None,
                str(self.ticker) if self.ticker is not None else None,
                str(self.expiration_date) if self.expiration_date is not None else None,
                float(self.strike_price) if self.strike_price is not None else None,
                int(self.volume) if self.volume is not None else None,
                int(self.open_interest) if self.open_interest is not None else None,
                float(self.implied_volatility) if self.implied_volatility is not None else None,
                str(self.contract_type) if self.contract_type is not None else None,
                float(self.delta) if self.delta is not None else None,
                float(self.gamma) if self.gamma is not None else None,
                float(self.theta) if self.theta is not None else None,
                float(self.vega) if self.vega is not None else None,
                str(self.update_time) if self.update_time is not None else None,
            )
            cursor.execute(sql, values)
            connection.commit()
            cursor.close()
            db_manager.close_connection(connection)
        except Error as e:
            logger.error(f"[OptionChainSnapshot::save] 保存期权链快照数据时出错: {e}", exc_info=True)
            db_manager.close_connection(connection)
            return False
        return True