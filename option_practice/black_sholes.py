from __future__ import annotations

import numpy as np
from scipy.stats import norm
from scipy.optimize import brentq
import sys
import os
from datetime import timedelta

# 假设你的项目结构不变，保留原有的相对路径导入
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.longport_utils import LongportUtils
from utils.time_utils import get_eastern_now
from longport.openapi import Period, AdjustType


def black_scholes(S: float, K: float, T: float, r: float, sigma: float, option_type: str, q: float = 0.0) -> float:
    """
    Black-Scholes-Merton公式计算欧式期权价格 (增加了连续股息率 q 的支持)
    """
    # 增加 T <= 0 的安全防护，避免除以零，直接返回内在价值
    if T <= 0:
        if option_type == "call":
            return max(0.0, S - K)
        elif option_type == "put":
            return max(0.0, K - S)
        else:
            raise ValueError(f"Invalid option type: {option_type}")

    # 引入股息率 q 后的 d1 计算
    d1 = (np.log(S/K) + (r - q + 0.5 * sigma**2) * T) / (sigma * np.sqrt(T))
    d2 = d1 - sigma * np.sqrt(T)
    
    if option_type == "call":
        return S * np.exp(-q * T) * norm.cdf(d1) - K * np.exp(-r * T) * norm.cdf(d2)
    elif option_type == "put":
        return K * np.exp(-r * T) * norm.cdf(-d2) - S * np.exp(-q * T) * norm.cdf(-d1)
    else:
        raise ValueError(f"Invalid option type: {option_type}")


def theoretical_price_from_iv(
    S: float,
    K: float,
    T: float,
    r: float,
    iv: float,
    option_type: str,
    q: float = 0.0,
) -> float:
    """
    用年化隐含波动率 iv 代入 Black-Scholes-Merton 公式计算欧式期权理论价格。
    与 black_scholes(..., sigma=iv) 等价；若 iv 来自该合约市价反解，结果应贴近该市价。
    """
    return black_scholes(S, K, T, r, iv, option_type, q)


def implied_volatility(
    S: float,
    K: float,
    T: float,
    r: float,
    market_price: float,
    option_type: str,
    q: float = 0.0
) -> float | None:
    """
    由市价反推隐含波动率（加入股息率贴现）
    """
    if T <= 0 or market_price <= 0:
        return None

    # 引入股息率后的内在价值严谨计算
    intrinsic = max(S * np.exp(-q * T) - K, 0.0) if option_type == "call" else max(K * np.exp(-r * T) - S * np.exp(-q * T), 0.0)
    
    if market_price < intrinsic - 1e-6:
        return None

    def objective(sigma: float) -> float:
        return black_scholes(S, K, T, r, sigma, option_type, q) - market_price

    try:
        return float(brentq(objective, 1e-6, 5.0, maxiter=200))
    except ValueError:
        return None


def get_historical_volatility(stock_code: str, trading_days: int = 30) -> float:
    """
    获取历史波动率：取最近 trading_days 个交易日的收盘价，
    计算日对数收益率的样本标准差，再按 252 个交易日年化。
    """
    end_d = get_eastern_now().date()
    # 日历区间放宽，保证能覆盖足够多交易日
    start_d = end_d - timedelta(days=max(90, trading_days * 3))
    try:
        resp = LongportUtils.get_history_candlesticks_by_date(
            stock_code, Period.Day, AdjustType.NoAdjust, start_d, end_d
        )
    except Exception:
        return 0.0

    if not resp:
        return 0.0

    candles = sorted(resp, key=lambda c: c.timestamp)
    closes = [float(c.close) for c in candles if c.close is not None and float(c.close) > 0]
    if len(closes) < 2:
        return 0.0

    closes = closes[-trading_days:]
    if len(closes) < 2:
        return 0.0

    arr = np.asarray(closes, dtype=float)
    log_returns = np.log(arr[1:] / arr[:-1])
    sigma_daily = float(np.std(log_returns, ddof=1))
    sigma_annual = sigma_daily * np.sqrt(252.0)
    return sigma_annual


if __name__ == "__main__":
    print("请运行测试: python option_practice/test_black_scholes.py")
    print("市场对照演示: python option_practice/test_black_scholes.py demo")