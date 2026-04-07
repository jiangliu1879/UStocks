"""
Black-Scholes 测试：断言「由隐含波动率算出的理论价」与「实际市价」一致。
get_option_price 用于拉取实际市价作为测试数据。
运行: python option_practice/test_black_scholes.py
演示: python option_practice/test_black_scholes.py demo
"""

from __future__ import annotations

import os
import sys
import unittest
from datetime import datetime

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)
_PRACTICE = os.path.dirname(os.path.abspath(__file__))
if _PRACTICE not in sys.path:
    sys.path.insert(0, _PRACTICE)

import black_sholes as bs
from utils.longport_utils import LongportUtils
from utils.time_utils import get_eastern_now

try:
    import massive  # noqa: F401
except ImportError:
    massive = None  # type: ignore


def _snapshot_option_mark_price(o) -> float | None:
    """快照：优先最近成交价，其次当日收盘，再昨收。"""
    lt = getattr(o, "last_trade", None)
    if lt is not None:
        p = getattr(lt, "price", None)
        if p is not None and float(p) > 0:
            return float(p)
    day = getattr(o, "day", None)
    if day is not None:
        for attr in ("close", "previous_close"):
            v = getattr(day, attr, None)
            if v is not None and float(v) > 0:
                return float(v)
    return None


def get_option_price(stock_code: str, strike_price: float, expiry_date: str) -> tuple[float, float]:
    """
    从 Massive 期权快照取与行权价、到期日匹配的 call/put 市价（供测试/演示生成对照数据）。
    """
    from massive import RESTClient

    key = os.getenv("MASSIVE_API_KEY")
    if not key:
        raise RuntimeError("请设置环境变量 MASSIVE_API_KEY")

    client = RESTClient(key)
    call_price = 0.0
    put_price = 0.0
    for o in client.list_snapshot_options_chain(
        stock_code,
        params={
            "strike_price": strike_price,
            "expiration_date": expiry_date,
            "order": "asc",
            "limit": 50,
            "sort": "ticker",
        },
    ):
        d = o.details
        if float(d.strike_price) != float(strike_price) or d.expiration_date != expiry_date:
            continue
        px = _snapshot_option_mark_price(o)
        if px is None:
            continue
        if d.contract_type == "call":
            call_price = px
        elif d.contract_type == "put":
            put_price = px

    return call_price, put_price


def _assert_theory_equals_actual(
    tc: unittest.TestCase,
    S: float,
    K: float,
    T: float,
    r: float,
    q: float,
    actual_price: float,
    option_type: str,
    *,
    places: int = 5,
) -> None:
    """实际价格 -> 反推 IV -> 理论价；断言理论价 == 实际价。"""
    iv = bs.implied_volatility(S, K, T, r, actual_price, option_type, q=q)
    tc.assertIsNotNone(iv, f"{option_type} 无法反推 IV（实际价={actual_price}）")
    assert iv is not None
    theory = bs.theoretical_price_from_iv(S, K, T, r, iv, option_type, q=q)
    tc.assertAlmostEqual(
        theory,
        actual_price,
        places=places,
        msg=f"{option_type}: 理论价 {theory} 应与实际价 {actual_price} 一致",
    )


class TestTheoryEqualsActual(unittest.TestCase):
    """仅校验：对给定实际价，IV 代回 BSM 后的理论价等于该实际价。"""

    def test_synthetic_call_and_put(self) -> None:
        S, K, T, r, q, sigma = 100.0, 100.0, 0.5, 0.04, 0.01, 0.25
        for opt in ("call", "put"):
            with self.subTest(option=opt):
                actual = bs.black_scholes(S, K, T, r, sigma, opt, q)
                _assert_theory_equals_actual(self, S, K, T, r, q, actual, opt)


@unittest.skipUnless(
    os.getenv("MASSIVE_API_KEY") and massive is not None,
    "需要 MASSIVE_API_KEY 且已安装 massive 客户端",
)
class TestTheoryEqualsMassiveMarket(unittest.TestCase):
    def test_live_call_put_theory_equals_actual(self) -> None:
        stock_code = "SPY"
        strike_price = 640.0
        expiry_date = "2026-03-30"
        actual_call, actual_put = get_option_price(stock_code, strike_price, expiry_date)
        self.assertGreater(actual_call, 0.0)
        self.assertGreater(actual_put, 0.0)

        S = LongportUtils.get_ticker_price(f"{stock_code}.US")
        self.assertGreater(S, 0.0)

        today = get_eastern_now().date()
        expiry_d = datetime.strptime(expiry_date, "%Y-%m-%d").date()
        T = max(0, (expiry_d - today).days) / 365.0
        self.assertGreater(T, 0.0)

        r, q = 0.0365, 0.013
        _assert_theory_equals_actual(self, S, strike_price, T, r, q, actual_call, "call", places=4)
        _assert_theory_equals_actual(self, S, strike_price, T, r, q, actual_put, "put", places=4)


def run_market_demo() -> None:
    """用 Massive 行情 + Longport 现货打印 IV 与理论价（需网络与密钥）。"""
    if massive is None:
        raise RuntimeError("未安装 massive，无法拉取期权快照")
    stock_code = "SPY"
    strike_price = 640.0
    expiry_date = "2026-03-30"

    real_call_price, real_put_price = get_option_price(stock_code, strike_price, expiry_date)
    print(f"看涨期权 (Call) 实际价格: {real_call_price:.4f}")
    print(f"看跌期权 (Put) 实际价格 : {real_put_price:.4f}")

    current_price = LongportUtils.get_ticker_price(f"{stock_code}.US")
    today = get_eastern_now().date()
    expiry_d = datetime.strptime(expiry_date, "%Y-%m-%d").date()
    time_to_maturity = max(0, (expiry_d - today).days) / 365.0

    risk_free_rate = 0.0365
    dividend_yield = 0.013

    print("-" * 30)
    print(f"股票当前价格 (S) : {current_price}")
    print(f"执行价格 (K)     : {strike_price}")
    print(f"到期时间 (T)     : {time_to_maturity:.4f} 年")
    print(f"无风险利率 (r)   : {risk_free_rate * 100:.2f}%")
    print(f"股息率 (q)       : {dividend_yield * 100:.2f}%")
    print("-" * 30)

    iv_call = bs.implied_volatility(
        current_price, strike_price, time_to_maturity, risk_free_rate, real_call_price, "call", q=dividend_yield
    )
    iv_put = bs.implied_volatility(
        current_price, strike_price, time_to_maturity, risk_free_rate, real_put_price, "put", q=dividend_yield
    )

    if iv_call is not None:
        print(f"由 Call 市价反推隐含波动率: {iv_call * 100:.2f}%")
    if iv_put is not None:
        print(f"由 Put 市价反推隐含波动率 : {iv_put * 100:.2f}%")

    print("-" * 30)
    if iv_call is not None:
        call_iv_theory = bs.theoretical_price_from_iv(
            current_price, strike_price, time_to_maturity, risk_free_rate, iv_call, "call", q=dividend_yield
        )
        print(f"  Call 理论价(IV): {call_iv_theory:.4f}  (市价 {real_call_price:.4f})")
    if iv_put is not None:
        put_iv_theory = bs.theoretical_price_from_iv(
            current_price, strike_price, time_to_maturity, risk_free_rate, iv_put, "put", q=dividend_yield
        )
        print(f"  Put 理论价(IV) : {put_iv_theory:.4f}  (市价 {real_put_price:.4f})")


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "demo":
        sys.argv.pop(1)
        run_market_demo()
    else:
        unittest.main(verbosity=2)
