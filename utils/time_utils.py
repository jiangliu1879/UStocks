import time
from datetime import datetime
import pytz


def get_eastern_now() -> datetime:
    """获取当前美东时间（US/Eastern）。"""
    eastern = pytz.timezone("US/Eastern")
    return datetime.now(pytz.UTC).astimezone(eastern)

def to_eastern_time(dt: datetime) -> datetime:
    """
    将 LongPort 返回的时间戳转换为美东时间（US/Eastern）。
    如果 dt 是 naive（无时区信息），默认认为是中国时区（Asia/Shanghai）。
    """
    eastern = pytz.timezone("US/Eastern")
    if dt.tzinfo is None:
        shanghai = pytz.timezone("Asia/Shanghai")
        dt = shanghai.localize(dt)
    return dt.astimezone(eastern)