"""
股票年化收益分析页面

展示股票的年化收益率，支持通过侧边栏多选股票代码进行筛选。
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import numpy as np
import os
import sys
from datetime import datetime, timedelta, date

# set_page_config 必须是第一个 Streamlit 命令
st.set_page_config(
    page_title="股票年化收益分析",
    page_icon="📈",
    layout="wide"
)

# 添加项目根目录到路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from data_models.stock_data import StockData
from data_models.stock_split import StockSplit


# 默认加载的股票代码（全部数据）
DEFAULT_STOCK_CODES = ["NVDA.US", "SPY.US"]

CACHE_TTL_SECONDS = 300


@st.cache_data(ttl=CACHE_TTL_SECONDS)
def get_stock_codes_cached():
    """缓存全部股票代码列表"""
    return sorted(StockData.get_stock_codes())


@st.cache_data(ttl=CACHE_TTL_SECONDS)
def load_stock_data(stock_codes: tuple):
    """从数据库加载指定股票的全部数据，结果缓存 5 分钟。stock_codes 为空时按默认 NVDA.US、SPY.US 加载。"""
    codes = list(stock_codes) if stock_codes else list(DEFAULT_STOCK_CODES)
    if not codes:
        return pd.DataFrame()
    try:
        data_list = []
        for stock_code in codes:
            results = StockData.query(
                conditions={"stock_code": stock_code},
                order_by="timestamp ASC",
            )
            for result in results:
                data_list.append({
                    "stock_code": result.stock_code,
                    "timestamp": result.timestamp,
                    "close": result.close,
                })
        if not data_list:
            return pd.DataFrame()
        df = pd.DataFrame(data_list)
        df["timestamp"] = pd.to_datetime(df["timestamp"], format="mixed")
        df = df.sort_values(["stock_code", "timestamp"])
        return df
    except Exception as e:
        import traceback
        traceback.print_exc()
        return pd.DataFrame()


def calculate_yearly_returns(df_stock, start_year=None, end_year=None):
    """
    计算股票每年的收益率
    公式：(每年12月31日的收盘价 - 每年1月1日的收盘价) / 每年1月1日的收盘价
    
    参数:
        df_stock: 单个股票的数据DataFrame，包含timestamp和close列
        start_year: 开始年份（可选）
        end_year: 结束年份（可选）
        
    返回:
        pd.DataFrame: 包含年份、年初价格、年末价格、年收益率的数据
    """
    if df_stock.empty or len(df_stock) < 2:
        return pd.DataFrame()
    
    # 确保按时间排序
    df_stock = df_stock.sort_values('timestamp').reset_index(drop=True)
    
    # 添加年份列
    df_stock['year'] = df_stock['timestamp'].dt.year
    df_stock['month'] = df_stock['timestamp'].dt.month
    df_stock['day'] = df_stock['timestamp'].dt.day
    
    # 获取所有年份
    years = sorted(df_stock['year'].unique())
    
    # 应用年份筛选
    if start_year is not None:
        years = [y for y in years if y >= start_year]
    if end_year is not None:
        years = [y for y in years if y <= end_year]
    
    yearly_returns = []
    
    for year in years:
        year_data = df_stock[df_stock['year'] == year].copy()
        
        if year_data.empty:
            continue
        
        # 找1月1日的数据（如果没有，找1月最早的数据）
        jan_data = year_data[year_data['month'] == 1]
        if jan_data.empty:
            # 如果没有1月数据，跳过该年
            continue
        
        jan_first_data = jan_data[jan_data['day'] == 1]
        if jan_first_data.empty:
            # 如果没有1月1日，找1月最早的一天
            jan_first_data = jan_data.nsmallest(1, 'day')
        
        jan_price = jan_first_data['close'].iloc[0]
        jan_date = jan_first_data['timestamp'].iloc[0]
        
        # 找12月31日的数据（如果没有，找12月最晚的数据）
        dec_data = year_data[year_data['month'] == 12]
        if dec_data.empty:
            # 如果没有12月数据，跳过该年
            continue
        
        dec_last_data = dec_data[dec_data['day'] == 31]
        if dec_last_data.empty:
            # 如果没有12月31日，找12月最晚的一天
            dec_last_data = dec_data.nlargest(1, 'day')
        
        dec_price = dec_last_data['close'].iloc[0]
        dec_date = dec_last_data['timestamp'].iloc[0]
        
        # 计算年收益率
        if jan_price > 0:
            yearly_return = ((dec_price - jan_price) / jan_price) * 100
            yearly_returns.append({
                'year': year,
                'jan_date': jan_date,
                'dec_date': dec_date,
                'jan_price': jan_price,
                'dec_price': dec_price,
                'yearly_return': yearly_return
            })
    
    if not yearly_returns:
        return pd.DataFrame()
    
    return pd.DataFrame(yearly_returns)


def create_yearly_return_chart(stock_code, yearly_returns_df):
    """
    创建单个股票的年收益率柱状图
    
    参数:
        stock_code: 股票代码
        yearly_returns_df: 包含每年收益率数据的DataFrame
    """
    if yearly_returns_df.empty:
        st.warning(f"⚠️ {stock_code}: 无法计算年化收益（数据不足）")
        return
    
    # 根据收益正负设置颜色
    colors = ['#2ca02c' if ret >= 0 else '#d62728' for ret in yearly_returns_df['yearly_return']]
    
    # 准备hover数据
    hover_data = []
    for _, row in yearly_returns_df.iterrows():
        hover_data.append(
            f"1月1日价格: ${row['jan_price']:.2f}<br>" +
            f"12月31日价格: ${row['dec_price']:.2f}"
        )
    
    # 创建柱状图
    fig = go.Figure()
    
    fig.add_trace(go.Bar(
        x=yearly_returns_df['year'].astype(str),
        y=yearly_returns_df['yearly_return'],
        marker_color=colors,
        text=[f'{ret:.2f}%' for ret in yearly_returns_df['yearly_return']],
        textposition='outside',
        customdata=hover_data,
        hovertemplate=f'<b>{stock_code}</b><br>' +
                     '年份: %{x}<br>' +
                     '年收益率: %{y:.2f}%<br>' +
                     '%{customdata}<extra></extra>',
        name=stock_code
    ))
    
    # 添加零线
    fig.add_hline(
        y=0,
        line_dash="dash",
        line_color="gray",
        opacity=0.5,
        annotation_text="0%",
        annotation_position="right"
    )
    
    # 更新布局
    fig.update_layout(
        title={
            'text': f'{stock_code} 年化收益率',
            'x': 0.5,
            'xanchor': 'center',
            'font': {'size': 18}
        },
        xaxis_title="年份",
        yaxis_title="年收益率 (%)",
        height=400,
        showlegend=False,
        margin=dict(l=50, r=50, t=80, b=50)
    )
    
    # 更新y轴，确保零线可见
    max_abs_return = yearly_returns_df['yearly_return'].abs().max()
    if max_abs_return > 0:
        y_range = max_abs_return * 1.2
        fig.update_yaxes(
            range=[-y_range, y_range]
        )
    
    st.plotly_chart(fig, use_container_width=True)


def main():
    """主函数"""
    st.title("📈 股票年化收益分析")
    st.markdown("---")
    
    # 侧边栏：选择股票代码（展示全部股票，默认只加载 NVDA.US、SPY.US 数据）
    st.sidebar.header("🔍 股票筛选")
    available_stocks = get_stock_codes_cached()
    if not available_stocks:
        st.sidebar.warning("暂无股票代码")
        st.stop()

    st.sidebar.markdown("**选择股票代码（可多选）:**")
    if "selected_stocks" not in st.session_state:
        st.session_state.selected_stocks = [
            s for s in DEFAULT_STOCK_CODES if s in available_stocks
        ]
    col1, col2 = st.sidebar.columns(2)
    with col1:
        if st.button("全选", use_container_width=True, key="gain_sel_all"):
            st.session_state.selected_stocks = available_stocks.copy()
    with col2:
        if st.button("取消全选", use_container_width=True, key="gain_sel_none"):
            st.session_state.selected_stocks = []
    st.sidebar.markdown("---")
    selected_stocks = []
    for stock_code in available_stocks:
        is_selected = st.sidebar.checkbox(
            stock_code,
            value=stock_code in st.session_state.selected_stocks,
            key=f"gain_cb_{stock_code}",
        )
        if is_selected:
            selected_stocks.append(stock_code)
    st.session_state.selected_stocks = selected_stocks
    st.sidebar.info(f"📊 已选择 {len(selected_stocks)} 个股票")
    st.sidebar.markdown("---")

    if not selected_stocks:
        st.warning("⚠️ 请在侧边栏选择至少一个股票代码")
        st.info("💡 默认仅加载 NVDA.US、SPY.US 数据，勾选其他股票会按需加载")
        st.stop()

    # 只加载所选股票的数据（默认仅 NVDA.US、SPY.US 有数据时即只读二者）
    with st.spinner("🔄 正在加载所选股票数据..."):
        df = load_stock_data(tuple(sorted(selected_stocks)))
    if df.empty:
        st.warning("⚠️ 所选股票在数据库中暂无数据")
        st.stop()

    # 自定义年份筛选
    st.sidebar.header("📅 自定义年份")
    df_temp = df.copy()
    df_temp['year'] = pd.to_datetime(df_temp['timestamp'], format='mixed').dt.year
    available_years = sorted(df_temp['year'].unique())
    
    if available_years:
        min_year = min(available_years)
        max_year = max(available_years)
        
        # 年份筛选开关
        use_year_filter = st.sidebar.checkbox(
            "启用年份筛选",
            value=False,
            help="启用后，只计算选定年份范围内的年化收益"
        )
        
        if use_year_filter:
            # 开始年份选择
            start_year = st.sidebar.selectbox(
                "开始年份",
                options=available_years,
                index=0,
                key="start_year_select",
                help="选择要分析的开始年份"
            )
            
            # 结束年份选择（只显示大于等于开始年份的选项）
            end_year_options = [y for y in available_years if y >= start_year]
            end_year_index = len(end_year_options) - 1 if end_year_options else 0
            
            end_year = st.sidebar.selectbox(
                "结束年份",
                options=end_year_options,
                index=end_year_index,
                key="end_year_select",
                help="选择要分析的结束年份（必须大于等于开始年份）"
            )
        else:
            start_year = None
            end_year = None
    else:
        use_year_filter = False
        start_year = None
        end_year = None
        st.sidebar.warning("⚠️ 无法获取年份信息")
    
    st.sidebar.markdown("---")

    # 计算每个股票的年化收益
    st.subheader("📊 年化收益率分析")
    
    # 显示年份筛选信息
    if use_year_filter and start_year is not None and end_year is not None:
        st.info(f"📅 年份筛选已启用：{start_year} 年 - {end_year} 年")
    
    # 存储结果用于汇总
    results_summary = []
    
    # 为每个选中的股票创建图表
    for i, stock_code in enumerate(selected_stocks):
        # 筛选该股票的数据
        df_stock = df[df['stock_code'] == stock_code].copy()
        
        if df_stock.empty:
            st.warning(f"⚠️ {stock_code}: 没有数据")
            continue

        # 用当前股票的 stock_code 从 stock_split 表读取拆股数据，并复权收盘价
        splits = StockSplit.query(
            conditions={"stock_code": stock_code},
            order_by="timestamp ASC",
        )
        # 拆股日之后（含拆股日）的收盘价乘以拆股倍数
        df_stock = df_stock.copy()
        for split in splits:
            split_date = pd.to_datetime(split.timestamp)
            mask = df_stock["timestamp"] >= split_date
            if mask.any():
                df_stock.loc[mask, "close"] = df_stock.loc[mask, "close"] * split.times

        # 计算每年的收益率（应用年份筛选）
        yearly_returns_df = calculate_yearly_returns(df_stock, start_year, end_year)
        
        if yearly_returns_df.empty:
            st.warning(f"⚠️ {stock_code}: 无法计算年化收益（数据不足）")
            continue
        
        # 计算平均年化收益率
        avg_annualized_return = yearly_returns_df['yearly_return'].mean()
        
        # 保存结果
        results_summary.append({
            'stock_code': stock_code,
            'annualized_return': avg_annualized_return,
            'years_count': len(yearly_returns_df),
            'yearly_returns': yearly_returns_df
        })
        
        # 创建图表
        st.markdown(f"### {stock_code}")
        create_yearly_return_chart(stock_code, yearly_returns_df)
        
        # 显示每年的详细数据
        with st.expander(f"📋 {stock_code} 详细数据"):
            display_yearly_df = yearly_returns_df[[
                'year', 'jan_date', 'dec_date', 'jan_price', 'dec_price', 'yearly_return'
            ]].copy()
            display_yearly_df['jan_date'] = pd.to_datetime(display_yearly_df['jan_date']).dt.strftime('%Y-%m-%d')
            display_yearly_df['dec_date'] = pd.to_datetime(display_yearly_df['dec_date']).dt.strftime('%Y-%m-%d')
            display_yearly_df.columns = [
                '年份', '1月1日', '12月31日', '年初价格', '年末价格', '年收益率 (%)'
            ]
            display_yearly_df['年初价格'] = display_yearly_df['年初价格'].apply(lambda x: f'${x:.2f}')
            display_yearly_df['年末价格'] = display_yearly_df['年末价格'].apply(lambda x: f'${x:.2f}')
            display_yearly_df['年收益率 (%)'] = display_yearly_df['年收益率 (%)'].apply(lambda x: f'{x:.2f}%')
            st.dataframe(display_yearly_df, use_container_width=True, hide_index=True)
        
        # 如果不是最后一个，添加分隔线
        if i < len(selected_stocks) - 1:
            st.markdown("---")
    
    # 显示汇总表格
    if results_summary:
        st.markdown("---")
        st.subheader("📋 年化收益汇总")
        
        # 创建汇总DataFrame
        summary_data = []
        for result in results_summary:
            # 计算累计收益率（复利方式）：(1 + r1) * (1 + r2) * ... * (1 + rn) - 1
            # 确保按年份顺序排序
            yearly_returns_df = result['yearly_returns'].sort_values('year').reset_index(drop=True)
            yearly_returns = yearly_returns_df['yearly_return'].values / 100  # 转换为小数
            
            # 复利计算：逐年累积
            cumulative_return = 1.0
            for yearly_return in yearly_returns:
                cumulative_return *= (1 + yearly_return)
            cumulative_return = cumulative_return - 1  # 减去初始本金
            cumulative_return_pct = cumulative_return * 100  # 转换为百分比
            
            summary_data.append({
                'stock_code': result['stock_code'],
                'annualized_return': result['annualized_return'],
                'cumulative_return': cumulative_return_pct,
                'years_count': result['years_count']
            })
        
        summary_df = pd.DataFrame(summary_data)
        summary_df = summary_df.sort_values('annualized_return', ascending=False)
        
        # 格式化显示
        display_df = summary_df[[
            'stock_code',
            'annualized_return',
            'cumulative_return',
            'years_count'
        ]].copy()
        
        display_df.columns = [
            '股票代码',
            '平均年化收益率 (%)',
            '累计收益率 (%)',
            '数据年份数'
        ]
        
        # 格式化数值
        display_df['平均年化收益率 (%)'] = display_df['平均年化收益率 (%)'].apply(lambda x: f'{x:.2f}%')
        display_df['累计收益率 (%)'] = display_df['累计收益率 (%)'].apply(lambda x: f'{x:.2f}%')
        
        st.dataframe(
            display_df,
            use_container_width=True,
            hide_index=True
        )


if __name__ == "__main__":
    main()

