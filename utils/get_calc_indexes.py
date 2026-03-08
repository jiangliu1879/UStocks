from datetime import date
from longport.openapi import QuoteContext, Config
import pytz
from datetime import datetime
import sys
import os

# 添加项目根目录到 Python 路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.logger import setup_logger

# 创建模块级别的日志记录器
logger = setup_logger('GetCalcIndexes')

from longport.openapi import QuoteContext, Config, CalcIndex

config = Config.from_env()
ctx = QuoteContext(config)

resp = ctx.calc_indexes(["NVDA260123C190000.US"], [CalcIndex.Gamma, CalcIndex.Theta, CalcIndex.ImpliedVolatility, CalcIndex.Gamma, CalcIndex.ExpiryDate])
logger.info(f"[__main__] 计算结果: {resp}")