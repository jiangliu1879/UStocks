"""
Max Pain：从 max_pain 表读取数据，按标的与到期日筛选，按 update_time 升序展示曲线与表格。
"""
from __future__ import annotations

import os
import sys

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from data_models.max_pain import MaxPain

st.set_page_config(page_title="Max Pain", page_icon="📉", layout="wide")

CACHE_TTL_SECONDS = 120


def _fmt_date(v) -> str:
    if v is None:
        return ""
    if hasattr(v, "strftime"):
        return v.strftime("%Y-%m-%d")
    return str(v)[:10]


def _records_to_dataframe(records: list) -> pd.DataFrame:
    rows = []
    for r in records:
        rows.append(
            {
                "underlying_ticker": r.underlying_ticker or "",
                "expiry_date": _fmt_date(r.expiry_date),
                "update_time": r.update_time,
                "max_pain_oi": float(r.max_pain_oi) if r.max_pain_oi is not None else 0.0,
                "max_pain_vol": float(r.max_pain_vol) if r.max_pain_vol is not None else 0.0,
                "ticker_price": float(r.ticker_price) if r.ticker_price is not None else 0.0,
                "sum_vol": float(r.sum_vol) if r.sum_vol is not None else 0.0,
                "sum_oi": float(r.sum_oi) if r.sum_oi is not None else 0.0,
            }
        )
    return pd.DataFrame(rows)


@st.cache_data(ttl=CACHE_TTL_SECONDS)
def load_all_max_pain() -> pd.DataFrame:
    records = MaxPain.fetch_all()
    if not records:
        return pd.DataFrame()
    return _records_to_dataframe(records)


def main() -> None:
    st.title("Max Pain 走势")
    st.caption("数据来自表 max_pain；筛选后按更新时间从早到晚排序。")

    df = load_all_max_pain()
    if df.empty:
        st.warning("表 max_pain 中暂无数据。")
        return

    tickers = sorted(df["underlying_ticker"].dropna().unique().tolist())
    if not tickers:
        st.warning("无有效标的代码。")
        return

    with st.sidebar:
        st.header("筛选")
        sel_ticker = st.selectbox("标的 underlying_ticker", tickers, index=0)

        expiries = sorted(
            df[df["underlying_ticker"] == sel_ticker]["expiry_date"].dropna().unique().tolist()
        )
        if not expiries:
            st.warning("该标的无到期日数据。")
            return
        sel_expiry = st.selectbox("到期日 expiry_date", expiries, index=len(expiries) - 1)

    sub = df[(df["underlying_ticker"] == sel_ticker) & (df["expiry_date"] == sel_expiry)].copy()
    if sub.empty:
        st.warning("当前筛选无记录。")
        return

    sub["update_time"] = pd.to_datetime(sub["update_time"])
    sub = sub.sort_values("update_time", ascending=True)

    st.subheader(f"{sel_ticker} · {sel_expiry}")

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=sub["update_time"],
            y=sub["max_pain_oi"],
            mode="lines+markers",
            name="max_pain_oi",
            line=dict(color="#1f77b4"),
        )
    )
    fig.add_trace(
        go.Scatter(
            x=sub["update_time"],
            y=sub["max_pain_vol"],
            mode="lines+markers",
            name="max_pain_vol",
            line=dict(color="#ff7f0e"),
        )
    )
    fig.add_trace(
        go.Scatter(
            x=sub["update_time"],
            y=sub["ticker_price"],
            mode="lines+markers",
            name="ticker_price",
            line=dict(color="#2ca02c"),
        )
    )
    fig.update_layout(
        height=420,
        margin=dict(l=8, r=8, t=40, b=8),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        xaxis_title="update_time",
        yaxis_title="价格 / 痛点",
        hovermode="x unified",
    )
    st.plotly_chart(fig, use_container_width=True)

    st.subheader("总成交量 sum_vol")
    fig_vol = go.Figure()
    fig_vol.add_trace(
        go.Scatter(
            x=sub["update_time"],
            y=sub["sum_vol"],
            mode="lines+markers",
            name="sum_vol",
            line=dict(color="#9467bd"),
            fill="tozeroy",
        )
    )
    fig_vol.update_layout(
        height=320,
        margin=dict(l=8, r=8, t=24, b=8),
        xaxis_title="update_time",
        yaxis_title="sum_vol",
        showlegend=False,
        hovermode="x unified",
    )
    st.plotly_chart(fig_vol, use_container_width=True)

main()
