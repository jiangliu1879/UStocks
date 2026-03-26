"""
获取期权链到期日列表，保留未来 60 天内的到期日，按 stock_option_info.json 格式写入。
"""
import json
import os
from datetime import date, timedelta
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.logger import setup_logger
logger = setup_logger('GetOptionChainExpiryDateList')
from utils.longport_utils import LongportUtils
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
STOCK_OPTION_INFO_PATH = os.path.join(PROJECT_ROOT, "stock_option_info.json")


def filter_expiry_dates_within_days(expiry_dates: list[date], days: int = 50) -> list[date]:
    """保留从今天起未来 days 天内的到期日（含今天）。"""
    today = date.today()
    end = today + timedelta(days=days)
    return sorted(d for d in expiry_dates if today <= d <= end)


def write_stock_option_info(
    stock_code: str,
    expiry_dates: list[date],
    strike_price_range: list[float] | None = None,
    json_path: str | None = None,
) -> None:
    """按 stock_option_info.json 格式写入：stock_code、expiry_dates（YYYY-MM-DD）、strike_price_range。支持 JSON 为数组或单对象。"""
    path = json_path or STOCK_OPTION_INFO_PATH
    expiry_strs = [d.strftime("%Y-%m-%d") for d in expiry_dates]
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    else:
        data = []

    if isinstance(data, list):
        found = False
        for item in data:
            if isinstance(item, dict) and item.get("stock_code") == stock_code:
                item["expiry_dates"] = expiry_strs
                if strike_price_range is not None:
                    item["strike_price_range"] = strike_price_range
                found = True
                break
        if not found:
            data.append({
                "stock_code": stock_code,
                "expiry_dates": expiry_strs,
                "strike_price_range": strike_price_range if strike_price_range is not None else [100, 250],
            })
    else:
        data = {
            "stock_code": stock_code,
            "expiry_dates": expiry_strs,
            "strike_price_range": strike_price_range if strike_price_range is not None else [100, 250],
        }

    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)

if __name__ == "__main__":
    # 读取 stock_option_info.json 中所有 stock_code、strike_price_range
    stocks_config = []
    if os.path.exists(STOCK_OPTION_INFO_PATH):
        with open(STOCK_OPTION_INFO_PATH, "r", encoding="utf-8") as f:
            raw = json.load(f)
        if isinstance(raw, list):
            stocks_config = [
                {
                    "stock_code": item.get("stock_code"),
                    "strike_price_range": item.get("strike_price_range", [100, 250]),
                    "expiry_dates": item.get("expiry_dates", []),
                }
                for item in raw
            ]
    for item in stocks_config:
        symbol = item.get("stock_code")
        strike_range = item.get("strike_price_range")
        expiry_dates = item.get("expiry_dates")
        all_dates = LongportUtils.get_option_chain_expiry_date_list(symbol)
        filtered = filter_expiry_dates_within_days(all_dates, days=50)
        write_stock_option_info(symbol, filtered, strike_price_range=strike_range)

        print(f"stock_code: {symbol}")
        print(f"未来 50 天内到期日 ({len(filtered)} 个):")
        for d in filtered:
            print(f"  {d.strftime('%Y-%m-%d')}")
        print(f"已写入 {STOCK_OPTION_INFO_PATH}")
