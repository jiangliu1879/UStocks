"""
期权分布页：按 update_time 最新的全部期权数据，按股票与到期日筛选，展示成交量分布与持仓量分布。
"""
from __future__ import annotations

import os
import sys
from datetime import datetime, date

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from data_models.option_quote import OptionQuote

st.set_page_config(page_title="期权分布", page_icon="📊", layout="wide")

CACHE_TTL_SECONDS = 300


@st.cache_data(ttl=CACHE_TTL_SECONDS)
def get_latest_option_data():
    """读取 update_time 最新日期的全部期权数据，返回 (latest_date_str, list of OptionQuote)。"""
    latest_date_str, all_records = OptionQuote.query_by_latest_update_date(limit=50000)
    return latest_date_str, all_records


def build_option_df(records):
    """将 OptionQuote 列表转为 DataFrame，含 strike_price, direction, volume, open_interest, expiry_date, underlying_symbol。"""
    if not records:
        return pd.DataFrame()
    rows = []
    for r in records:
        ed = r.expiry_date
        rows.append({
            "underlying_symbol": r.underlying_symbol,
            "expiry_date": str(ed)[:10] if ed else None,
            "strike_price": float(r.strike_price) if r.strike_price is not None else None,
            "direction": (r.direction or "").upper(),
            "volume": int(r.volume) if r.volume is not None else 0,
            "open_interest": int(r.open_interest) if r.open_interest is not None else 0,
        })
    return pd.DataFrame(rows)


def chart_volume_by_strike(df_filtered: pd.DataFrame, title: str, update_time: str):
    """成交量分布：横轴行权价，纵轴上方 Call、下方 Put；悬停显示行权价、Call/Put 成交量。"""
    if df_filtered.empty:
        st.warning("暂无数据，无法绘制成交量分布")
        return
    df_call = df_filtered[df_filtered["direction"] == "CALL"].groupby("strike_price", as_index=False).agg({"volume": "sum"})
    df_put = df_filtered[df_filtered["direction"] == "PUT"].groupby("strike_price", as_index=False).agg({"volume": "sum"})
    strikes = sorted(set(df_call["strike_price"].tolist()) | set(df_put["strike_price"].tolist()))
    call_by_strike = df_call.set_index("strike_price")["volume"]
    put_by_strike = df_put.set_index("strike_price")["volume"]
    call_vols = [int(call_by_strike.get(s, 0)) for s in strikes]
    put_vols = [int(put_by_strike.get(s, 0)) for s in strikes]
    customdata = [[s, c, p] for s, c, p in zip(strikes, call_vols, put_vols)]
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=strikes,
        y=call_vols,
        name="Call 成交量",
        marker_color="#2ca02c",
        customdata=customdata,
        hovertemplate="行权价: %{customdata[0]}<br>Call 成交量: %{customdata[1]:,.0f}<br>Put 成交量: %{customdata[2]:,.0f}<extra></extra>",
    ))
    fig.add_trace(go.Bar(
        x=strikes,
        y=[-p for p in put_vols],
        name="Put 成交量",
        marker_color="#d62728",
        customdata=customdata,
        hovertemplate="行权价: %{customdata[0]}<br>Call 成交量: %{customdata[1]:,.0f}<br>Put 成交量: %{customdata[2]:,.0f}<extra></extra>",
    ))
    fig.add_hline(y=0, line_dash="dash", line_color="gray", opacity=0.5)
    fig.update_layout(
        title=title,
        xaxis_title="行权价",
        yaxis_title="成交量",
        barmode="overlay",
        height=400,
        showlegend=True,
        hovermode="closest",
    )
    st.plotly_chart(fig, use_container_width=True)
    st.caption(f"数据更新时间: {update_time}")


def chart_oi_by_strike(df_filtered: pd.DataFrame, title: str, update_time: str):
    """持仓量分布：横轴行权价，纵轴上方 Call、下方 Put；悬停显示行权价、Call/Put 持仓量。"""
    if df_filtered.empty:
        st.warning("暂无数据，无法绘制持仓量分布")
        return
    df_call = df_filtered[df_filtered["direction"] == "CALL"].groupby("strike_price", as_index=False).agg({"open_interest": "sum"})
    df_put = df_filtered[df_filtered["direction"] == "PUT"].groupby("strike_price", as_index=False).agg({"open_interest": "sum"})
    strikes = sorted(set(df_call["strike_price"].tolist()) | set(df_put["strike_price"].tolist()))
    call_by_strike = df_call.set_index("strike_price")["open_interest"]
    put_by_strike = df_put.set_index("strike_price")["open_interest"]
    call_ois = [int(call_by_strike.get(s, 0)) for s in strikes]
    put_ois = [int(put_by_strike.get(s, 0)) for s in strikes]
    customdata = [[s, c, p] for s, c, p in zip(strikes, call_ois, put_ois)]
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=strikes,
        y=call_ois,
        name="Call 持仓量",
        marker_color="#2ca02c",
        customdata=customdata,
        hovertemplate="行权价: %{customdata[0]}<br>Call 持仓量: %{customdata[1]:,.0f}<br>Put 持仓量: %{customdata[2]:,.0f}<extra></extra>",
    ))
    fig.add_trace(go.Bar(
        x=strikes,
        y=[-p for p in put_ois],
        name="Put 持仓量",
        marker_color="#d62728",
        customdata=customdata,
        hovertemplate="行权价: %{customdata[0]}<br>Call 持仓量: %{customdata[1]:,.0f}<br>Put 持仓量: %{customdata[2]:,.0f}<extra></extra>",
    ))
    fig.add_hline(y=0, line_dash="dash", line_color="gray", opacity=0.5)
    fig.update_layout(
        title=title,
        xaxis_title="行权价",
        yaxis_title="持仓量",
        barmode="overlay",
        height=400,
        showlegend=True,
        hovermode="closest",
    )
    st.plotly_chart(fig, use_container_width=True)
    st.caption(f"数据更新时间: {update_time}")


def main():
    st.title("📊 期权分布")

    latest_update_time, all_records = get_latest_option_data()
    if not all_records:
        st.warning("暂无期权数据（或未找到最新 update_time 的记录）")
        st.stop()

    df = build_option_df(all_records)
    stock_codes = sorted(df["underlying_symbol"].dropna().unique().tolist())
    if not stock_codes:
        st.warning("最新一批数据中无股票代码")
        st.stop()

    # 侧边栏：股票下拉 + 该股票下的到期日 checkbox，默认选最近到期日
    with st.sidebar:
        st.subheader("筛选")
        selected_stock = st.selectbox("股票代码 (stock_code)", options=stock_codes, key="opt_stock")
        df_stock = df[df["underlying_symbol"] == selected_stock]
        expiry_dates = sorted([str(e)[:10] for e in df_stock["expiry_date"].dropna().unique()])
        if not expiry_dates:
            st.warning(f"{selected_stock} 无到期日数据")
            st.stop()

        # 默认选最近的到期日：按日期升序，第一个为最近
        default_expiry = expiry_dates[0]
        if st.session_state.get("opt_last_stock") != selected_stock:
            st.session_state["opt_last_stock"] = selected_stock
            for ed in expiry_dates:
                st.session_state[f"opt_exp_{selected_stock}_{ed}"] = ed == default_expiry

        st.write("**期权到期日**")
        selected_expiries = []
        for ed in expiry_dates:
            key = f"opt_exp_{selected_stock}_{ed}"
            if st.checkbox(ed, key=key):
                selected_expiries.append(ed)

    if not selected_expiries:
        st.info("请至少勾选一个到期日")
        st.stop()

    df_filtered = df[
        (df["underlying_symbol"] == selected_stock) & (df["expiry_date"].isin(selected_expiries))
    ].copy()
    if df_filtered.empty:
        st.warning("当前筛选无数据")
        st.stop()

    expiry_label = "、".join(selected_expiries) if len(selected_expiries) <= 3 else f"{len(selected_expiries)} 个到期日"
    st.subheader(f"成交量分布 — {selected_stock}（{expiry_label}）")
    chart_volume_by_strike(
        df_filtered,
        title=f"成交量分布 - {selected_stock}",
        update_time=latest_update_time,
    )
    st.subheader(f"持仓量分布 — {selected_stock}（{expiry_label}）")
    chart_oi_by_strike(
        df_filtered,
        title=f"持仓量分布 - {selected_stock}",
        update_time=latest_update_time,
    )


if __name__ == "__main__":
    main()
