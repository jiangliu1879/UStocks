from datetime import date
from typing import List
import sys
import os

# 添加项目根目录到 Python 路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from data_models.option_quote import OptionQuote
from data_models.max_pain import MaxPain
# 添加 utils 目录到路径以便导入同目录下的模块
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from get_stock_data import get_stock_realtime_price
from utils.logger import setup_logger

# 创建模块级别的日志记录器
logger = setup_logger('CalculateMaxPain')

def calculate_max_pain(option_quotes: List[OptionQuote]) -> MaxPain:
    """
    计算最大痛点（Max Pain）
    
    Max Pain 是指期权到期时，使得所有期权持有者总损失最大的股票价格。
    计算方法：
    1. 对于每个可能的行权价，计算如果股票在该价格到期时的总损失
    2. 看涨期权（CALL）：如果到期价格 > 行权价，损失 = (到期价格 - 行权价) * 持仓量/成交量
    3. 看跌期权（PUT）：如果到期价格 < 行权价，损失 = (行权价 - 到期价格) * 持仓量/成交量
    4. 找到总损失最大的行权价
    
    Args:
        option_quotes: 期权报价列表
        
    Returns:
        MaxPain对象，包含基于持仓量和成交量的最大痛点价格
    """
    if not option_quotes:
        raise ValueError("期权报价列表不能为空")
    
    # 从第一个报价中获取公共信息
    first_quote = option_quotes[0]
    underlying_symbol = first_quote.underlying_symbol
    expiry_date = first_quote.expiry_date
    update_time = first_quote.update_time
    
    # 按行权价和方向分组期权
    strikes_dict = {}
    for quote in option_quotes:
        if quote.strike_price not in strikes_dict.keys():
            strikes_dict[quote.strike_price] = []
        strikes_dict[quote.strike_price].append(quote)
    
    # 按照 key（行权价）从小到大排序
    strikes_dict = dict(sorted(strikes_dict.items(), key=lambda x: x[0]))
 
    # 计算每个行权价作为到期价格时的总损失
    max_pain_oi_price = None
    max_pain_vol_price = None
    max_pain_oi_profit = sys.float_info.max
    max_pain_vol_profit = sys.float_info.max

    for potential_settlement in strikes_dict.keys():
        total_profit_oi = 0.0  # 基于持仓量的总收益
        total_profit_vol = 0.0  # 基于成交量的总收益

        for strike_price in strikes_dict.keys():
            for quote in strikes_dict[strike_price]:
                if quote.direction.upper() == 'CALL':
                    if strike_price < potential_settlement:
                        total_profit_oi += quote.open_interest * (potential_settlement - strike_price)
                        total_profit_vol += quote.volume * (potential_settlement - strike_price)
                elif quote.direction.upper() == 'PUT':
                    if strike_price > potential_settlement:
                        total_profit_oi += quote.open_interest * (strike_price - potential_settlement)
                        total_profit_vol += quote.volume * (strike_price - potential_settlement)

        if total_profit_oi < max_pain_oi_profit:
            max_pain_oi_profit = total_profit_oi
            max_pain_oi_price = potential_settlement
        if total_profit_vol < max_pain_vol_profit:
            max_pain_vol_profit = total_profit_vol
            max_pain_vol_price = potential_settlement
    
    sum_vol = 0
    sum_oi = 0
    for quote in option_quotes:
        sum_vol += quote.volume
        sum_oi += quote.open_interest
    # 创建 MaxPain 对象
    return MaxPain(
        underlying_symbol=underlying_symbol,
        expiry_date=expiry_date,
        update_time=update_time,
        max_pain_oi=max_pain_oi_price if max_pain_oi_price is not None else 0.0,
        max_pain_vol=int(max_pain_vol_price) if max_pain_vol_price is not None else 0,
        sum_vol=sum_vol,
        sum_oi=sum_oi
    )
if __name__ == "__main__":
    underlying_symbol = "NVDA.US"
    expiry_date = date(2026, 1, 23)
    update_time = "2026-01-19 21:24:52"

    stock_price = get_stock_realtime_price(underlying_symbol)

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