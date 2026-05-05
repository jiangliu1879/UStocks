from datetime import datetime, timezone
from typing import Union

import pytz


def get_eastern_now() -> datetime:
    """获取当前美东时间（America/New_York）"""
    eastern = pytz.timezone("America/New_York")
    return datetime.now(pytz.UTC).astimezone(eastern)


def to_eastern_time(dt: Union[datetime, int, float]) -> datetime:
    """
    将时间转为美东时间（America/New_York）

    - LongPort：naive datetime，按 Asia/Shanghai 解释再转换。
    - Massive / Polygon 等：Unix 时间戳（秒、毫秒或纳秒整数/浮点），按 UTC 再转美东。
    """
    eastern = pytz.timezone("America/New_York")
    if isinstance(dt, (int, float)):
        ts = float(dt)
        if ts > 1e15:
            ts /= 1e9
        elif ts > 1e11:
            ts /= 1000.0
        dt = datetime.fromtimestamp(ts, tz=timezone.utc)
    elif dt.tzinfo is None:
        shanghai = pytz.timezone("Asia/Shanghai")
        dt = shanghai.localize(dt)
    return dt.astimezone(eastern)

