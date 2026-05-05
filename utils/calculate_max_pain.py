from datetime import date
from typing import List
import sys
import os

# 添加项目根目录到 Python 路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from data_models.option_snapshot_day import OptionSnapshotDay
from data_models.max_pain import MaxPain
# 添加 utils 目录到路径以便导入同目录下的模块
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.logger import setup_logger
from utils.longport_utils import LongportUtils

# 创建模块级别的日志记录器
logger = setup_logger('CalculateMaxPain')

def calculate_max_pain(option_snapshots: List[OptionSnapshotDay]) -> MaxPain:
    """
    计算最大痛点（Max Pain）

    常见定义：在到期时，使「所有未平仓合约若按内在价值行权，卖方需支付的总金额」最小的标的价。
    等价于：最小化 sum(内在价值 × 权重)，权重此处为 open_interest 或 volume。

    对候选价：仅遍历链上出现的行权价（分段线性，极小值必出现在行权价节点）。

    对每条合约：
    - CALL：若结算价 S > K，内在价值 = S - K
    - PUT：若 S < K，内在价值 = K - S

    变量名中的 profit 实为「期权买方总内在价值」（卖方支出）；取使该值最小的 S 作为 max pain。

    Args:
        option_snapshots: 期权报价列表

    Returns:
        MaxPain：max_pain_oi / max_pain_vol 分别为按持仓量、按成交量加权得到的最小总内在价值对应的行权价
    """
    if not option_snapshots:
        raise ValueError("期权报价列表不能为空")
    
    # 从第一个报价中获取公共信息
    first_quote = option_snapshots[0]
    underlying_symbol = first_quote.underlying_symbol
    expiry_date = first_quote.expiry_date
    update_time = first_quote.update_time
    
    # 按行权价和方向分组期权
    strikes_dict = {}
    for quote in option_snapshots:
        if quote.strike_price not in strikes_dict.keys():
            strikes_dict[quote.strike_price] = []
        strikes_dict[quote.strike_price].append(quote)
    
    # 按照 key（行权价）从小到大排序
    strikes_dict = dict(sorted(strikes_dict.items(), key=lambda x: x[0]))
 
    # 对每个候选结算价 S，计算买方总内在价值（OI / Volume 加权）；取最小者对应的行权价
    max_pain_oi_price = None
    max_pain_vol_price = None
    max_pain_oi_total = sys.float_info.max
    max_pain_vol_total = sys.float_info.max

    for potential_settlement in strikes_dict.keys():
        total_intrinsic_oi = 0.0
        total_intrinsic_vol = 0.0

        for strike_price in strikes_dict.keys():
            for quote in strikes_dict[strike_price]:
                d = (quote.direction or "").upper()
                if d == "CALL":
                    if strike_price < potential_settlement:
                        iv = potential_settlement - strike_price
                        total_intrinsic_oi += quote.open_interest * iv
                        total_intrinsic_vol += quote.volume * iv
                elif d == "PUT":
                    if strike_price > potential_settlement:
                        iv = strike_price - potential_settlement
                        total_intrinsic_oi += quote.open_interest * iv
                        total_intrinsic_vol += quote.volume * iv

        if total_intrinsic_oi < max_pain_oi_total:
            max_pain_oi_total = total_intrinsic_oi
            max_pain_oi_price = potential_settlement
        if total_intrinsic_vol < max_pain_vol_total:
            max_pain_vol_total = total_intrinsic_vol
            max_pain_vol_price = potential_settlement
    
    sum_vol = 0
    sum_oi = 0
    for quote in option_quotes:
        sum_vol += quote.volume
        sum_oi += quote.open_interest
    # 创建 MaxPain 对象
    return MaxPain(
        underlying_symbol=underlying_symbol,
        expiry_date=str(expiry_date) if expiry_date is not None else "",
        update_time=update_time,
        max_pain_oi=float(max_pain_oi_price) if max_pain_oi_price is not None else 0.0,
        max_pain_vol=float(max_pain_vol_price) if max_pain_vol_price is not None else 0.0,
        sum_vol=sum_vol,
        sum_oi=sum_oi,
    )
if __name__ == "__main__":
    underlying_symbol = "NVDA.US"
    expiry_date = date(2026, 1, 23)
    update_time = "2026-01-19 21:24:52"

    stock_price = LongportUtils.get_ticker_price(underlying_symbol)

    # 查询指定标的、到期日期和更新时间的所有期权报价
    expiry_date_str = expiry_date.strftime('%Y-%m-%d')
    option_quotes = OptionQuote.query({
        'underlying_symbol': underlying_symbol,
        'expiry_date': expiry_date_str,
        'update_time': update_time
    })
    
    if option_quotes:
        max_pain = calculate_max_pain(option_quotes)
        max_pain.stock_price = stock_price
        logger.info(f"[__main__] {max_pain}")
        max_pain.save()
        logger.info(f"[__main__] 保存最大痛点数据成功")
    else:
        logger.warning(f"[__main__] 未找到符合条件的期权报价数据")