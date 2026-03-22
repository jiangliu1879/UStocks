import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import sys
import os

# 添加项目根目录到路径，便于导入数据模型
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from data_models.stock_data_min import StockDataMin

def calculate_volume_profile(df, tick_size=1.0, value_area_pct=0.70):
    """
    计算筹码分布 (Volume Profile) 及核心支撑/阻力位
    
    :param df: pandas DataFrame, 必须包含 ['open', 'high', 'low', 'close', 'volume'] 列
    :param tick_size: 价格区间的划分步长 (例如 NVDA 可以设置为 1.0 或 0.5 美元)
    :param value_area_pct: 价值区域包含的成交量比例，默认 70% (0.70)
    :return: profile_df (价格区间及其对应的成交量), poc, vah, val
    """
    # 1. 确定全局价格的上下边界，并生成价格箱体 (Bins)
    min_price = np.floor(df['low'].min() / tick_size) * tick_size
    max_price = np.ceil(df['high'].max() / tick_size) * tick_size
    
    # 创建以 tick_size 为步长的价格索引
    bins = np.arange(min_price, max_price + tick_size, tick_size)
    profile = pd.Series(0.0, index=bins)
    
    # 2. 分配成交量 (精准均摊法)
    # 遍历每根1分钟K线，将其成交量均匀分布到该K线穿过的所有价格区间中
    for _, row in df.iterrows():
        # 找出该1分钟K线覆盖的价格区间起点和终点
        start_bin = np.floor(row['low'] / tick_size) * tick_size
        end_bin = np.floor(row['high'] / tick_size) * tick_size
        
        # 该K线碰触到的所有价格箱体
        touched_bins = np.arange(start_bin, end_bin + tick_size, tick_size)
        
        if len(touched_bins) > 0:
            # 将成交量均摊到这些箱体中
            vol_per_bin = row['volume'] / len(touched_bins)
            
            # 因为浮点数精度问题，使用 np.isclose 匹配 index
            for b in touched_bins:
                # 找到最接近的价格标签并累加成交量
                closest_idx = profile.index[np.abs(profile.index - b).argmin()]
                profile.loc[closest_idx] += vol_per_bin

    # 3. 计算关键点位 POC (Point of Control)
    poc_price = profile.idxmax()
    poc_volume = profile.max()
    
    # 4. 计算价值区域 VA (Value Area)
    total_volume = profile.sum()
    target_volume = total_volume * value_area_pct
    
    # 从 POC 开始向上下双向扩展，寻找 70% 的成交量
    current_vol = poc_volume
    val = poc_price  # Value Area Low
    vah = poc_price  # Value Area High
    
    while current_vol < target_volume:
        lower_price = val - tick_size
        upper_price = vah + tick_size
        
        # 获取上下相邻区间的成交量，若超出边界则视为0
        vol_lower = profile.get(lower_price, 0)
        vol_upper = profile.get(upper_price, 0)
        
        if vol_lower == 0 and vol_upper == 0:
            break  # 所有成交量已遍历完
            
        # 比较上下哪边成交量更大，就优先把哪边纳入价值区
        if vol_lower > vol_upper:
            current_vol += vol_lower
            val = lower_price
        elif vol_upper > vol_lower:
            current_vol += vol_upper
            vah = upper_price
        else:
            # 如果一样大，同时纳入
            current_vol += (vol_lower + vol_upper)
            val = lower_price
            vah = upper_price
            
    # 转换为 DataFrame 方便后续对接量化系统
    profile_df = profile.reset_index()
    profile_df.columns = ['price', 'volume']
    
    return profile_df, poc_price, vah, val


def plot_volume_profile(profile_df, poc, vah, val, symbol="NVDA"):
    """
    可视化筹码分布图
    """
    plt.figure(figsize=(10, 8))
    
    # 绘制基础横向柱状图
    plt.barh(profile_df['price'], profile_df['volume'], height=0.8, color='lightgray', edgecolor='none')
    
    # 标出 70% 价值区 (Value Area)
    va_mask = (profile_df['price'] >= val) & (profile_df['price'] <= vah)
    plt.barh(profile_df.loc[va_mask, 'price'], profile_df.loc[va_mask, 'volume'], height=0.8, color='steelblue', alpha=0.7)
    
    # 标出 POC (控制点)
    poc_volume = profile_df.loc[profile_df['price'] == poc, 'volume'].values[0]
    plt.barh(poc, poc_volume, height=0.8, color='red')
    
    # 绘制辅助线
    plt.axhline(vah, color='green', linestyle='--', alpha=0.7, label=f'VAH: {vah:.2f}')
    plt.axhline(poc, color='red', linestyle='-', alpha=0.7, label=f'POC: {poc:.2f}')
    plt.axhline(val, color='orange', linestyle='--', alpha=0.7, label=f'VAL: {val:.2f}')
    
    plt.title(f'{symbol} Volume Profile (筹码分布)')
    plt.xlabel('Volume (成交量)')
    plt.ylabel('Price (价格)')
    plt.legend()
    plt.grid(axis='x', linestyle='--', alpha=0.5)
    plt.show()

# ==========================================
# 模拟执行入口 (用你的真实数据替换掉这里的 df 即可)
# ==========================================
if __name__ == "__main__":
    # 1) 读取 stock_data_min 表中 NVDA.US 的全部交易数据
    records = StockDataMin.query(
        conditions={"stock_code": "NVDA.US"},
        order_by="timestamp ASC",
    )
    if not records:
        raise RuntimeError("stock_data_min 表中未找到 NVDA.US 的交易数据。")

    # 2) 转换为 pandas DataFrame，并确保 timestamp 为 datetime
    df = pd.DataFrame(
        [
            {
                "timestamp": r.timestamp,
                "open": r.open,
                "high": r.high,
                "low": r.low,
                "close": r.close,
                "volume": r.volume,
            }
            for r in records
        ]
    )
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    # 3) 运行计算（NVDA 建议步长 1.0 或 0.5；这里默认 1.0）
    profile_data, poc, vah, val = calculate_volume_profile(df, tick_size=1.0)

    # 4) 打印结果
    print(f"核心共识价位 (POC): {poc}")
    print(f"强势阻力位 (VAH): {vah}")
    print(f"强力支撑位 (VAL): {val}")

    # 5) 绘图（可选）
    plot_volume_profile(profile_data, poc, vah, val, symbol="NVDA")