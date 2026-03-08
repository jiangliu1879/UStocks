"""
股票数据分析页面

展示股票的历史价格、成交量和成交额数据，支持按股票代码筛选数据。
"""

import streamlit as st

# set_page_config 必须是第一个 Streamlit 命令
st.set_page_config(
    page_title="股票数据分析",
    page_icon="📊",
    layout="wide"
)

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import os
import sys
from datetime import datetime, timedelta, date

# 添加项目根目录到路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from data_models.stock_data import StockData

CACHE_TTL_SECONDS = 300


@st.cache_data(ttl=CACHE_TTL_SECONDS)
def get_stock_codes_cached():
    """缓存股票代码列表，减少重复查询"""
    return sorted(StockData.get_stock_codes())


@st.cache_data(ttl=CACHE_TTL_SECONDS)
def load_stock_data(stock_code: str):
    """从数据库加载指定股票代码的全部数据，带缓存以加快重复访问"""
    try:
        results = StockData.query(
            conditions={"stock_code": stock_code},
            order_by="timestamp DESC",
        )
        if not results:
            return pd.DataFrame()
        data_list = [
            {
                "stock_code": r.stock_code,
                "timestamp": r.timestamp,
                "open": r.open,
                "high": r.high,
                "low": r.low,
                "close": r.close,
                "volume": r.volume,
                "turnover": r.turnover,
            }
            for r in results
        ]
        df = pd.DataFrame(data_list)
        if df.empty:
            return df
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df = df.sort_values("timestamp")
        return df
    except Exception as e:
        st.error(f"❌ 从数据库加载数据失败: {e}")
        import traceback
        st.error(traceback.format_exc())
        return pd.DataFrame()


def create_stock_charts(df_filtered):
    """创建股票数据图表（close、volume、turnover）"""
    if df_filtered.empty:
        st.warning("⚠️ 没有数据可以显示")
        return
    
    # 确保数据按时间排序
    df_filtered = df_filtered.sort_values('timestamp')
    
    # 创建子图
    fig = make_subplots(
        rows=3, cols=1,
        subplot_titles=('收盘价 (Close)', '成交量 (Volume)', '成交额 (Turnover)'),
        vertical_spacing=0.08,
        shared_xaxes=True
    )
    
    # 定义颜色
    close_color = '#1f77b4'      # 蓝色 - 收盘价
    volume_color = '#ff7f0e'     # 橙色 - 成交量
    turnover_color = '#2ca02c'   # 绿色 - 成交额
    
    # 收盘价曲线
    fig.add_trace(
        go.Scatter(
            x=df_filtered['timestamp'],
            y=df_filtered['close'],
            mode='lines+markers',
            name='收盘价',
            line=dict(color=close_color, width=2),
            marker=dict(size=4, color=close_color),
            hovertemplate='<b>收盘价</b><br>' +
                        '时间: %{x}<br>' +
                        '价格: $%{y:.2f}<br>' +
                        '<extra></extra>'
        ),
        row=1, col=1
    )
    
    # 成交量曲线
    fig.add_trace(
        go.Scatter(
            x=df_filtered['timestamp'],
            y=df_filtered['volume'],
            mode='lines+markers',
            name='成交量',
            line=dict(color=volume_color, width=2),
            marker=dict(size=4, color=volume_color),
            hovertemplate='<b>成交量</b><br>' +
                        '时间: %{x}<br>' +
                        '成交量: %{y:,.0f}<br>' +
                        '<extra></extra>'
        ),
        row=2, col=1
    )
    
    # 成交额曲线（处理可能为None的情况）
    turnover_data = df_filtered['turnover'].fillna(0)  # 将None替换为0以便绘制
    fig.add_trace(
        go.Scatter(
            x=df_filtered['timestamp'],
            y=turnover_data,
            mode='lines+markers',
            name='成交额',
            line=dict(color=turnover_color, width=2),
            marker=dict(size=4, color=turnover_color),
            hovertemplate='<b>成交额</b><br>' +
                        '时间: %{x}<br>' +
                        '成交额: $%{y:,.2f}<br>' +
                        '<extra></extra>'
        ),
        row=3, col=1
    )
    
    # 更新布局
    fig.update_layout(
        height=900,
        title={
            'text': f'股票数据时间序列 - {df_filtered.iloc[0]["stock_code"] if not df_filtered.empty else ""}',
            'x': 0.5,
            'xanchor': 'center',
            'font': {'size': 20}
        },
        showlegend=True,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        )
    )
    
    # 更新x轴
    fig.update_xaxes(
        title_text="时间",
        row=3, col=1
    )
    
    # 更新y轴
    fig.update_yaxes(
        title_text="价格 ($)",
        row=1, col=1
    )
    fig.update_yaxes(
        title_text="成交量",
        row=2, col=1,
        tickformat=",.0f"
    )
    fig.update_yaxes(
        title_text="成交额 ($)",
        row=3, col=1,
        tickformat=",.2f"
    )
    
    # 格式化x轴时间显示
    fig.update_xaxes(
        tickformat="%Y-%m-%d",
        tickangle=45
    )
    
    st.plotly_chart(fig, use_container_width=True)


def main():
    """主函数"""
    st.title("📊 股票数据分析")
    st.markdown("---")
    
    # 侧边栏筛选器（先选股票，再按所选股票加载数据）
    st.sidebar.header("🔍 数据筛选")
    available_stocks = get_stock_codes_cached()
    if not available_stocks:
        st.sidebar.warning("暂无股票代码")
        st.stop()
    default_index = available_stocks.index("NVDA.US") if "NVDA.US" in available_stocks else 0
    selected_stock = st.sidebar.selectbox(
        "选择股票代码",
        options=available_stocks,
        index=default_index,
        help="选择一个股票代码进行查看",
    )

    # 只读取所选股票的全部数据
    with st.spinner(f"🔄 正在加载 {selected_stock} 数据..."):
        df = load_stock_data(selected_stock)
    if df.empty:
        st.warning(f"⚠️ 数据库中没有 {selected_stock} 的数据，请先运行数据收集脚本")
        st.stop()

    # 当前仅含所选股票，直接使用
    df_by_stock = df
    if not df_by_stock.empty:
        
        # 时间筛选
        st.sidebar.markdown("---")
        st.sidebar.header("⏰ 时间筛选")
        
        # 获取数据的时间范围
        df_by_stock['date'] = df_by_stock['timestamp'].dt.date
        available_dates = df_by_stock['date'].tolist()
        min_date = min(available_dates) if available_dates else date.today()
        max_date = max(available_dates) if available_dates else date.today()
        
        # 时间筛选选项
        time_filter_option = st.sidebar.radio(
            "选择时间筛选方式:",
            ["📅 最近N周", "📆 最近N月", "🎯 自定义日期范围"],
            help="选择不同的时间筛选方式来查看数据"
        )
        
        st.sidebar.markdown("---")
        
        # 根据选择显示不同的界面并计算时间范围
        if "最近N周" in time_filter_option:
            weeks = st.sidebar.selectbox(
                "周数选择:", 
                range(1, 13), 
                index=0,
                help="选择要查看的周数"
            )
            
            # 计算开始日期
            end_date = max_date
            start_date = end_date - timedelta(weeks=weeks)
            
        elif "最近N月" in time_filter_option:
            months = st.sidebar.selectbox(
                "月数选择:", 
                range(1, 25), 
                index=0,
                help="选择要查看的月数"
            )
            
            # 计算开始日期（使用更精确的月份计算）
            end_date = max_date
            # 使用relativedelta会更准确，但为了简化，使用近似值
            start_date = end_date - timedelta(days=months*30)  # 近似计算，每月30天
            
        else:  # 自定义日期范围
            start_date = st.sidebar.date_input(
                "开始日期",
                value=min_date,
                min_value=min_date,
                max_value=max_date,
                help="选择数据查询的开始日期"
            )
            
            end_date = st.sidebar.date_input(
                "结束日期",
                value=max_date,
                min_value=min_date,
                max_value=max_date,
                help="选择数据查询的结束日期"
            )
        
        # 应用时间筛选
        df_filtered = df_by_stock[
            (df_by_stock['date'] >= start_date) & 
            (df_by_stock['date'] <= end_date)
        ].copy()
        
        # 确保按时间排序
        df_filtered = df_filtered.sort_values('timestamp')
        
        # 移除临时添加的date列
        if 'date' in df_filtered.columns:
            df_filtered = df_filtered.drop(columns=['date'])
    else:
        df_filtered = pd.DataFrame()
    
    # 检查是否选择了股票代码
    if not selected_stock:
        st.warning("⚠️ 请选择股票代码来查看数据")
        st.stop()
    
    # 检查是否有数据
    if df_filtered.empty:
        st.warning(f"⚠️ 没有找到 {selected_stock} 的数据")
        st.stop()
    
    # 显示当前选择的股票代码和统计信息
    st.info(f"📊 当前查看: **{selected_stock}** | 数据点数: {len(df_filtered)} | "
            f"时间范围: {df_filtered['timestamp'].min().strftime('%Y-%m-%d')} 至 "
            f"{df_filtered['timestamp'].max().strftime('%Y-%m-%d')}")
    
    # 交易统计：股价变化、涨跌幅度 = 所选日期范围最后1个交易日收盘价 - 第1个交易日收盘价
    st.markdown("### 📊 交易统计")
    if len(df_filtered) >= 2:
        first_close = df_filtered["close"].iloc[0]
        last_close = df_filtered["close"].iloc[-1]
        close_diff = last_close - first_close
        change_pct = (close_diff / first_close) * 100 if first_close != 0 else 0
        delta_color = "inverse" if close_diff < 0 else "normal"
        delta_prefix = "+" if close_diff >= 0 else ""

        col1, col2 = st.columns(2)
        with col1:
            st.metric(
                "股价变化",
                f"${last_close:.2f}",
                delta=f"{delta_prefix}${close_diff:.2f}",
                delta_color=delta_color,
            )
        with col2:
            st.metric(
                "股价涨跌幅度",
                f"{change_pct:+.2f}%",
                delta=f"{change_pct:+.2f}%",
                delta_color=delta_color,
            )
    else:
        col1, col2 = st.columns(2)
        with col1:
            st.metric("股价变化", f"${df_filtered['close'].iloc[-1]:.2f}")
        with col2:
            st.metric("股价涨跌幅度", "N/A", help="仅有一条数据，无法计算变化")
    st.markdown("---")
    
    # 显示图表
    st.subheader("📈 股票数据图表")
    create_stock_charts(df_filtered)
    
    # 显示数据摘要
    with st.expander("📋 数据摘要"):
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("最新收盘价", f"${df_filtered['close'].iloc[-1]:.2f}")
        
        with col2:
            st.metric("平均成交量", f"{df_filtered['volume'].mean():,.0f}")
        
        with col3:
            turnover_mean = df_filtered['turnover'].mean()
            if pd.notna(turnover_mean):
                st.metric("平均成交额", f"${turnover_mean:,.2f}")
            else:
                st.metric("平均成交额", "N/A")
        
        with col4:
            st.metric("最高价", f"${df_filtered['high'].max():.2f}")
        
        # 显示数据表格
        st.dataframe(
            df_filtered[['timestamp', 'open', 'high', 'low', 'close', 'volume', 'turnover']],
            use_container_width=True,
            height=300
        )


if __name__ == "__main__":
    main()

