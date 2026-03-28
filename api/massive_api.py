import os
from massive import RESTClient
import json
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.logger import setup_logger
logger = setup_logger('MassiveAPI')

client = RESTClient(os.getenv("MASSIVE_API_KEY"))

options_chain = []
for o in client.list_snapshot_options_chain(
    "SPY",
    params={
        "strike_price.gte": 500,
        "strike_price.lte": 750,
        "expiration_date": "2026-03-30",
        "order": "asc",
        "limit": 250,
        "sort": "ticker",
    },
):
    options_chain.append(o)

def _to_jsonable(x):
    if isinstance(x, (str, int, float, bool)) or x is None:
        return x
    if isinstance(x, dict):
        return {str(k): _to_jsonable(v) for k, v in x.items()}
    if isinstance(x, (list, tuple)):
        return [_to_jsonable(v) for v in x]
    if hasattr(x, "to_dict") and callable(getattr(x, "to_dict")):
        try:
            return _to_jsonable(x.to_dict())
        except Exception:
            pass
    if hasattr(x, "__dict__"):
        try:
            return _to_jsonable(vars(x))
        except Exception:
            pass
    return str(x)


out_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "options_chain.json")
with open(out_path, "w", encoding="utf-8") as f:
    json.dump(_to_jsonable(options_chain), f, ensure_ascii=False, indent=2)

print(f"options_chain 已写入: {out_path}")

