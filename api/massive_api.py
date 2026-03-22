class MassiveAPI:
    def __init__(self):
        self.api_key = "e_Qc9Jd61GbZkWKyCKzKDHU_jHCHyeYZ"

    def get_option_data(self, stock_code: str):
        from massive import RESTClient

        client = RESTClient("e_Qc9Jd61GbZkWKyCKzKDHU_jHCHyeYZ")

        options_chain = []
        for o in client.list_snapshot_options_chain("NVDA", params={"order": "asc", "limit": 10, "sort": "ticker"}):
            options_chain.append(o)

        print(options_chain)

    def get_stock_data(self, stock_code: str):
        pass


if __name__ == "__main__":
    api = MassiveAPI()
    api.get_option_data("NVDA")