import os
from massive import RESTClient
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.logger import setup_logger
logger = setup_logger('MassiveUtils')

class MassiveUtils:
    @staticmethod
    def get_options_chain(underlying_ticker: str, expiration_date: str, strike_price_range: list[float], limit: int = 250, sort: str = "ticker"):
        client = RESTClient(os.getenv("MASSIVE_API_KEY"))
        options_chain = []
        for o in client.list_snapshot_options_chain(
            underlying_ticker,
            params={
                "strike_price.gte": strike_price_range[0],
                "strike_price.lte": strike_price_range[1],
                "expiration_date": expiration_date,
                "order": "asc",
                "limit": limit,
                "sort": sort,
            },
        ):
            options_chain.append(o)
        return options_chain