"""
删除指定股票在 stock_data 表中的全部数据。
用法: python delete_stock_data.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from data_models.stock_data import StockData

if __name__ == "__main__":
    stock_code = "NVDA.US"
    n = StockData.delete_by_stock_code(stock_code)
    print(f"已删除 {stock_code} 共 {n} 条记录。")
