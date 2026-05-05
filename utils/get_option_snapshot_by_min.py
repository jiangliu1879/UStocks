import sys
import os
import time
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.massive_utils import MassiveUtils
from utils.logger import setup_logger
from data_models.option_snapshot_min import OptionSnapshotMin
from data_models.max_pain import MaxPain
from utils.longport_utils import LongportUtils
from utils.time_utils import get_eastern_now

logger = setup_logger('GetOptionSnapshotByMin')

def get_option_snapshot_min(underlying_ticker: str, expiration_date: str, strike_price_range: list[float], limit: int = 250, sort: str = "ticker", update_time: str = None):
    list_option_snapshot_min = []

    def _to_int(v, default=0):
        return int(v) if v is not None else default

    def _to_float(v, default=0.0):
        return float(v) if v is not None else default

    def _to_str(v, default=""):
        return str(v) if v is not None else default

    try:
        options_chain = MassiveUtils.get_options_chain(underlying_ticker, expiration_date, strike_price_range, limit, sort)
        for option_chain in options_chain:
            day = getattr(option_chain, "day", None)
            greeks = getattr(option_chain, "greeks", None)
            details = getattr(option_chain, "details", None)
            underlying_asset = getattr(option_chain, "underlying_asset", None)
            option_snapshot_min = OptionSnapshotMin(
                underlying_ticker=_to_str(getattr(underlying_asset, "ticker", None)),
                ticker=_to_str(getattr(details, "ticker", None)),
                expiration_date=_to_str(getattr(details, "expiration_date", None)),
                strike_price=_to_float(getattr(details, "strike_price", None)),
                volume=_to_int(getattr(day, "volume", None)),
                open_interest=_to_int(getattr(option_chain, "open_interest", None)),
                implied_volatility=_to_float(getattr(option_chain, "implied_volatility", None)),
                contract_type=_to_str(getattr(details, "contract_type", None)),
                delta=_to_float(getattr(greeks, "delta", None)),
                gamma=_to_float(getattr(greeks, "gamma", None)),
                theta=_to_float(getattr(greeks, "theta", None)),
                vega=_to_float(getattr(greeks, "vega", None)),
                update_time=update_time,
            )
            option_snapshot_min.save()
            list_option_snapshot_min.append(option_snapshot_min)
    except Exception as e:
        logger.error(f"[get_option_snapshot_min] 获取期权快照分钟数据失败: {e}", exc_info=True)
        return []
    return list_option_snapshot_min


def _get_direction(option_snapshot_min: OptionSnapshotMin) -> str:
    """从 contract_type/ticker 推断方向：CALL 或 PUT。"""
    ct = (option_snapshot_min.contract_type or "").lower()
    if "call" in ct:
        return "CALL"
    if "put" in ct:
        return "PUT"
    ticker = (option_snapshot_min.ticker or "").upper()
    if "C" in ticker and "P" not in ticker:
        return "CALL"
    if "P" in ticker and "C" not in ticker:
        return "PUT"
    return ""


def calculate_max_pain_from_snapshots(list_option_snapshot_min: list[OptionSnapshotMin], ticker_price: float) -> MaxPain | None:
    """
    使用 list_option_snapshot_min 计算 max_pain（基于 open_interest 和 volume）。
    """
    if not list_option_snapshot_min:
        return None

    strikes = sorted({float(s.strike_price) for s in list_option_snapshot_min if s.strike_price is not None})
    if not strikes:
        return None

    max_pain_oi_price = None
    max_pain_vol_price = None
    min_loss_oi = float("inf")
    min_loss_vol = float("inf")

    for settlement in strikes:
        total_loss_oi = 0.0
        total_loss_vol = 0.0
        for s in list_option_snapshot_min:
            strike = float(s.strike_price) if s.strike_price is not None else 0.0
            oi = float(s.open_interest) if s.open_interest is not None else 0.0
            vol = float(s.volume) if s.volume is not None else 0.0
            direction = _get_direction(s)

            if direction == "CALL" and strike < settlement:
                total_loss_oi += oi * (settlement - strike)
                total_loss_vol += vol * (settlement - strike)
            elif direction == "PUT" and strike > settlement:
                total_loss_oi += oi * (strike - settlement)
                total_loss_vol += vol * (strike - settlement)

        if total_loss_oi < min_loss_oi:
            min_loss_oi = total_loss_oi
            max_pain_oi_price = settlement
        if total_loss_vol < min_loss_vol:
            min_loss_vol = total_loss_vol
            max_pain_vol_price = settlement

    first = list_option_snapshot_min[0]
    sum_vol = sum(float(s.volume) if s.volume is not None else 0.0 for s in list_option_snapshot_min)
    sum_oi = sum(float(s.open_interest) if s.open_interest is not None else 0.0 for s in list_option_snapshot_min)
    return MaxPain(
        underlying_ticker=first.underlying_ticker,
        expiry_date=first.expiration_date,
        max_pain_vol=float(max_pain_vol_price) if max_pain_vol_price is not None else 0.0,
        max_pain_oi=float(max_pain_oi_price) if max_pain_oi_price is not None else 0.0,
        sum_vol=sum_vol,
        sum_oi=sum_oi,
        ticker_price=ticker_price,
        update_time=first.update_time,
    )


def run_snapshot_and_max_pain_once(
    underlying_ticker: str,
    expiration_date: str,
    strike_price_range: list[float],
    limit: int = 250,
    sort: str = "ticker",
) -> MaxPain | None:
    """执行一次快照抓取 + max_pain 计算与保存。"""
    update_time = get_eastern_now().strftime('%Y-%m-%d %H:%M:%S')
    list_option_snapshot_min = get_option_snapshot_min(
        underlying_ticker=underlying_ticker,
        expiration_date=expiration_date,
        strike_price_range=strike_price_range,
        limit=limit,
        sort=sort,
        update_time=update_time,
    )
    ticker_price = LongportUtils.get_ticker_price(f"{underlying_ticker}.US")
    max_pain = calculate_max_pain_from_snapshots(list_option_snapshot_min, ticker_price)
    if max_pain:
        max_pain.save()
        logger.info(
            f"[run_snapshot_and_max_pain_once] MaxPain 已保存: {max_pain.underlying_ticker} {max_pain.expiry_date} "
            f"OI={max_pain.max_pain_oi} VOL={max_pain.max_pain_vol} update_time={max_pain.update_time}"
        )
    else:
        logger.warning("[run_snapshot_and_max_pain_once] 无法计算 MaxPain（快照数据为空或无有效行权价）")
    return max_pain


def run_snapshot_task_every_15min_until_close(
    underlying_ticker: str,
    expiration_date: str,
    strike_price_range: list[float],
    limit: int = 250,
    sort: str = "ticker",
):
    """
    每隔 15 分钟执行一次任务；美股收盘后执行最后一次并结束。
    """
    close_info = LongportUtils.get_us_market_close_time()
    now_et = get_eastern_now()
    if close_info is None:
        logger.warning("[run_snapshot_task_every_15min_until_close] 未获取到美股收盘时间，执行一次后结束。")
        run_snapshot_and_max_pain_once(underlying_ticker, expiration_date, strike_price_range, limit, sort)
        return

    close_time = close_info if hasattr(close_info, "hour") else now_et.replace(hour=16, minute=0, second=0, microsecond=0).time()
    logger.info(f"[run_snapshot_task_every_15min_until_close] 任务启动，收盘时间(ET): {close_time}")

    while True:
        now_et = get_eastern_now()
        # 收盘后执行最后一次并退出
        if now_et.time() >= close_time:
            logger.info("[run_snapshot_task_every_15min_until_close] 已到收盘后，执行最后一次任务。")
            run_snapshot_and_max_pain_once(underlying_ticker, expiration_date, strike_price_range, limit, sort)
            logger.info("[run_snapshot_task_every_15min_until_close] 任务结束。")
            break

        run_snapshot_and_max_pain_once(underlying_ticker, expiration_date, strike_price_range, limit, sort)
        logger.info("[run_snapshot_task_every_15min_until_close] 休眠 15 分钟后继续。")
        time.sleep(15 * 60)


if __name__ == "__main__":
    underlying_ticker = "SPY"
    expiration_date = "2026-05-05"
    strike_price_range = [650, 750]
    limit = 250
    sort = "ticker"
    run_snapshot_task_every_15min_until_close(
        underlying_ticker=underlying_ticker,
        expiration_date=expiration_date,
        strike_price_range=strike_price_range,
        limit=limit,
        sort=sort,
    )