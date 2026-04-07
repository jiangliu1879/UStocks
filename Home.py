from __future__ import annotations

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, timedelta, date
import os
import sys
import numpy as np
import asyncio

# Add the parent directory to the path to import models
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from data_models.stock_data import StockData


# 设置页面标题，不显示侧边栏
st.set_page_config(page_title="市场概览", page_icon="📊", layout="wide", initial_sidebar_state="collapsed")


# 缓存 5 分钟，避免每次交互都查库
CACHE_TTL_SECONDS = 300


@st.cache_data(ttl=CACHE_TTL_SECONDS)
def get_latest_trade_date() -> date:
    """从数据库取最新交易日的日期，无数据时返回今天"""
    recs = StockData.query(
        conditions={"stock_code": "SPY.US"},
        limit=1,
        order_by="timestamp DESC",
    )
    if not recs:
        return date.today()
    ts = recs[0].timestamp
    if hasattr(ts, "date"):
        return ts.date()
    return datetime.strptime(str(ts)[:10], "%Y-%m-%d").date()


# 默认展示最新交易日，无侧边栏
selected_date = get_latest_trade_date()

st.title("📊 市场概览")
st.markdown(f"### 📅 {selected_date.strftime('%Y年%m月%d日')}")


def _timestamp_to_date_str(t) -> str:
    """将 timestamp 转为 YYYY-MM-DD 字符串便于比较"""
    if hasattr(t, "strftime"):
        return t.strftime("%Y-%m-%d")
    return str(t)[:10]


@st.cache_data(ttl=CACHE_TTL_SECONDS)
def get_latest_stock_info(stock_code: str, target_date: date | None = None):
    """
    获取股票信息，包括涨跌幅和成交量水位。
    若指定 target_date 则返回该日数据，否则返回最新一日数据。
    结果缓存 CACHE_TTL_SECONDS 秒，减少数据库查询。
    """
    recent_data = StockData.query(
        conditions={"stock_code": stock_code},
        limit=60,
        order_by="timestamp DESC",
    )
    if not recent_data:
        return None

    if target_date is not None:
        ts_str = target_date.strftime("%Y-%m-%d")
        idx = None
        for i, rec in enumerate(recent_data):
            if _timestamp_to_date_str(rec.timestamp) == ts_str:
                idx = i
                break
        if idx is None:
            return None
        latest = recent_data[idx]
        prev_day = recent_data[idx + 1] if idx + 1 < len(recent_data) else None
        # 成交量水位用该日及之前约 30 条记录
        volume_slice = recent_data[idx : idx + 31]
    else:
        latest = recent_data[0]
        prev_day = recent_data[1] if len(recent_data) > 1 else None
        volume_slice = recent_data[1:31]

    if prev_day and prev_day.close:
        change_pct = ((latest.close - prev_day.close) / prev_day.close) * 100
        prev_close = prev_day.close
    else:
        change_pct = 0.0
        prev_close = latest.close

    if len(volume_slice) > 1:
        avg_volume = np.mean([d.volume for d in volume_slice[1:] if d.volume])
        volume_ratio = (latest.volume / avg_volume) * 100 if (avg_volume and latest.volume) else 0
    else:
        volume_ratio = 100.0

    return {
        "stock_code": stock_code,
        "timestamp": latest.timestamp,
        "close": latest.close,
        "prev_close": prev_close,
        "change_pct": change_pct,
        "volume": latest.volume,
        "volume_ratio": volume_ratio,
        "high": latest.high,
        "low": latest.low,
        "open": latest.open,
        "description": getattr(latest, "description", None) or "",
    }


def format_change_pct(change_pct: float) -> str:
    """格式化涨跌幅显示"""
    color = "🟢" if change_pct >= 0 else "🔴"
    sign = "+" if change_pct >= 0 else ""
    return f"{color} {sign}{change_pct:.2f}%"


def format_volume_ratio(volume_ratio: float) -> str:
    """格式化成交量水位显示"""
    if volume_ratio >= 150:
        emoji = "🔥"
        level = "极高"
    elif volume_ratio >= 120:
        emoji = "📈"
        level = "较高"
    elif volume_ratio >= 80:
        emoji = "📊"
        level = "正常"
    elif volume_ratio >= 50:
        emoji = "📉"
        level = "较低"
    else:
        emoji = "❄️"
        level = "极低"
    
    return f"{emoji} {level} ({volume_ratio:.1f}%)"


@st.cache_data(ttl=CACHE_TTL_SECONDS)
def load_recent_stock_data(stock_code: str, end_date: date):
    """
    读取指定股票最近一个月数据（截至 end_date），返回 DataFrame。
    """
    results = StockData.query(
        conditions={"stock_code": stock_code},
        limit=120,
        order_by="timestamp DESC",
    )
    if not results:
        return pd.DataFrame()

    rows = []
    for r in results:
        ts = pd.to_datetime(str(r.timestamp))
        rows.append(
            {
                "stock_code": r.stock_code,
                "timestamp": ts,
                "open": r.open,
                "high": r.high,
                "low": r.low,
                "close": r.close,
                "volume": r.volume,
                "turnover": r.turnover,
            }
        )
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    start_date = pd.to_datetime(end_date) - pd.Timedelta(days=30)
    end_dt = pd.to_datetime(end_date) + pd.Timedelta(days=1)
    df = df[(df["timestamp"] >= start_date) & (df["timestamp"] < end_dt)].copy()
    return df.sort_values("timestamp")


def create_stock_charts(df_filtered: pd.DataFrame, title_suffix: str):
    """创建股票数据图表（close、volume、turnover），样式对齐 2_Stock_Analyzer。"""
    if df_filtered.empty:
        st.warning("⚠️ 没有数据可以显示")
        return

    df_filtered = df_filtered.sort_values("timestamp")
    fig = make_subplots(
        rows=3,
        cols=1,
        subplot_titles=("收盘价 (Close)", "成交量 (Volume)", "成交额 (Turnover)"),
        vertical_spacing=0.08,
        shared_xaxes=True,
    )

    close_color = "#1f77b4"
    volume_color = "#ff7f0e"
    turnover_color = "#2ca02c"

    fig.add_trace(
        go.Scatter(
            x=df_filtered["timestamp"],
            y=df_filtered["close"],
            mode="lines+markers",
            name="收盘价",
            line=dict(color=close_color, width=2),
            marker=dict(size=4, color=close_color),
        ),
        row=1,
        col=1,
    )
    fig.add_trace(
        go.Scatter(
            x=df_filtered["timestamp"],
            y=df_filtered["volume"],
            mode="lines+markers",
            name="成交量",
            line=dict(color=volume_color, width=2),
            marker=dict(size=4, color=volume_color),
        ),
        row=2,
        col=1,
    )
    fig.add_trace(
        go.Scatter(
            x=df_filtered["timestamp"],
            y=df_filtered["turnover"].fillna(0),
            mode="lines+markers",
            name="成交额",
            line=dict(color=turnover_color, width=2),
            marker=dict(size=4, color=turnover_color),
        ),
        row=3,
        col=1,
    )

    fig.update_layout(
        height=700,
        title={"text": f"股票数据图表 - {title_suffix}", "x": 0.5, "xanchor": "center"},
        showlegend=True,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )
    fig.update_xaxes(title_text="时间", row=3, col=1, tickformat="%Y-%m-%d", tickangle=45)
    fig.update_yaxes(title_text="价格 ($)", row=1, col=1)
    fig.update_yaxes(title_text="成交量", row=2, col=1, tickformat=",.0f")
    fig.update_yaxes(title_text="成交额 ($)", row=3, col=1, tickformat=",.2f")

    st.plotly_chart(fig, use_container_width=True)

col1, col2 = st.columns(2)

# SPY
with col1:
    st.subheader("SPY (标普500 ETF)")
    spy_info = get_latest_stock_info("SPY.US", target_date=selected_date)

    if spy_info:
        st.metric(
            label="最新价格",
            value=f"${spy_info['close']:.2f}",
            delta=f"{spy_info['change_pct']:.2f}%"
        )
        
        st.write(f"**涨跌幅:** {format_change_pct(spy_info['change_pct'])}")
        st.write(f"**成交量水位:** {format_volume_ratio(spy_info['volume_ratio'])}")
        st.write(f"**成交量:** {spy_info['volume']:,.0f}")
        st.write(f"**日期:** {spy_info['timestamp']}")
        
        # 显示价格区间
        st.write(f"**今日区间:** ${spy_info['low']:.2f} - ${spy_info['high']:.2f}")
    else:
        st.warning("暂无 SPY 数据")

# QQQ
with col2:
    st.subheader("QQQ (纳斯达克100 ETF)")
    qqq_info = get_latest_stock_info("QQQ.US", target_date=selected_date)

    if qqq_info:
        st.metric(
            label="最新价格",
            value=f"${qqq_info['close']:.2f}",
            delta=f"{qqq_info['change_pct']:.2f}%"
        )
        
        st.write(f"**涨跌幅:** {format_change_pct(qqq_info['change_pct'])}")
        st.write(f"**成交量水位:** {format_volume_ratio(qqq_info['volume_ratio'])}")
        st.write(f"**成交量:** {qqq_info['volume']:,.0f}")
        st.write(f"**日期:** {qqq_info['timestamp']}")
        
        # 显示价格区间
        st.write(f"**今日区间:** ${qqq_info['low']:.2f} - ${qqq_info['high']:.2f}")
    else:
        st.warning("暂无 QQQ 数据")

if spy_info is None and qqq_info is None:
    st.info("没有交易数据")

st.markdown("---")
st.subheader("📈 SPY / QQQ 最近一个月图表")
chart_col1, chart_col2 = st.columns(2)

with chart_col1:
    st.markdown("#### SPY")
    df_spy_1m = load_recent_stock_data("SPY.US", selected_date)
    if df_spy_1m.empty:
        st.warning("暂无 SPY 最近一个月数据")
    else:
        create_stock_charts(df_spy_1m, "SPY 最近一个月")

with chart_col2:
    st.markdown("#### QQQ")
    df_qqq_1m = load_recent_stock_data("QQQ.US", selected_date)
    if df_qqq_1m.empty:
        st.warning("暂无 QQQ 最近一个月数据")
    else:
        create_stock_charts(df_qqq_1m, "QQQ 最近一个月")

st.subheader("市场简述：")

# 用 session_state 保存编辑内容；切换日期时用该日 SPY 的 description 刷新
if "market_desc" not in st.session_state or st.session_state.get("market_desc_date") != selected_date:
    st.session_state["market_desc"] = (spy_info.get("description") or "") if spy_info else ""
    st.session_state["market_desc_date"] = selected_date
    st.session_state["market_desc_edit"] = False

if st.session_state.get("market_desc_edit", False):
    # 编辑状态：文本框显示原内容（带 Markdown 标记），按钮「更新」将当前内容写入数据库
    current_text = st.text_area(
        "市场简述",
        height=500,
        key="market_desc",
        label_visibility="collapsed",
        placeholder="输入或编辑市场简述…（支持 Markdown）",
    )
    if st.button("更新", key="market_btn_save"):
        new_desc = (current_text or "").replace("\r\n", "\n").replace("\r", "\n")
        if not spy_info:
            st.warning("暂无 SPY 数据，无法更新。")
        else:
            target_date_str = _timestamp_to_date_str(spy_info["timestamp"])
            recent = StockData.query(
                conditions={"stock_code": "SPY.US"},
                limit=60,
                order_by="timestamp DESC",
            )
            rec = None
            for r in recent:
                if _timestamp_to_date_str(r.timestamp) == target_date_str:
                    rec = r
                    break
            if rec:
                rec.description = new_desc
                if rec.save():
                    st.cache_data.clear()
                    st.session_state["market_desc_edit"] = False
                    st.success("市场简述已更新到数据库。")
                    st.rerun()
                else:
                    st.error("更新失败，请重试。")
            else:
                st.warning(f"未找到 SPY 对应日期的记录（{target_date_str}），无法更新。")
else:
    # 预览状态：渲染 Markdown，按钮为「编辑」
    content = st.session_state.get("market_desc") or ""
    if not content.strip():
        st.markdown("*暂无内容，点击「编辑」填写*")
    else:
        st.markdown(content)
    if st.button("编辑", key="market_btn_edit"):
        st.session_state["market_desc_edit"] = True
        # 进入编辑时用当前记录的 description 原始数据填充文本框
        raw = ((spy_info or {}).get("description") or "").replace("\r\n", "\n").replace("\r", "\n")
        st.session_state["market_desc"] = raw
        st.rerun()



