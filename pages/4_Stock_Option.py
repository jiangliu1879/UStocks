"""
期权分布页：按 update_time 最新的全部期权数据，按股票与到期日筛选，展示成交量分布与持仓量分布。
"""
from __future__ import annotations

import os
import sys
import json
from datetime import datetime, date

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from data_models.option_snapshot_day import OptionSnapshotDay
from utils.calculate_max_pain import calculate_max_pain
from utils.longport_utils import LongportUtils
st.set_page_config(page_title="期权分布", page_icon="📊", layout="wide")

CACHE_TTL_SECONDS = 300


@st.cache_data(ttl=CACHE_TTL_SECONDS)
def load_stock_option_config():
    """读取 stock_option_info.json，返回 [{stock_code, expiry_dates, ...}, ...]。"""
    cfg_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "stock_option_info.json")
    with open(cfg_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data if isinstance(data, list) else [data]


@st.cache_data(ttl=CACHE_TTL_SECONDS)
def get_latest_records_for_stock_and_expiries(stock_code: str, expiry_dates: tuple[str, ...]):
    """
    从数据库读取指定 stock_code + expiry_dates 的数据，并仅保留 latest update_time 的那一批。
    Returns: (latest_update_time, list[OptionSnapshotDay])
    """
    records = OptionSnapshotDay.query(
        conditions={"underlying_symbol": stock_code},
        limit=100000,
        order_by="update_time DESC",
    )
    if not records:
        return None, []

    expiry_set = set(expiry_dates)
    matched = [r for r in records if str(r.expiry_date)[:10] in expiry_set]
    if not matched:
        return None, []

    # 取该筛选子集中的最新 update_time
    latest_update_time = max(str(r.update_time) for r in matched)
    latest_records = [r for r in matched if str(r.update_time) == latest_update_time]
    return latest_update_time, latest_records


@st.cache_data(ttl=CACHE_TTL_SECONDS)
def get_all_records_for_stock_and_expiries(stock_code: str, expiry_dates: tuple[str, ...]):
    """
    从数据库读取指定 stock_code + expiry_dates 的全部期权数据（不限 update_time）。
    """
    records = OptionSnapshotDay.query(
        conditions={"underlying_symbol": stock_code},
        limit=100000,
        order_by="update_time DESC",
    )
    if not records:
        return []
    expiry_set = set(expiry_dates)
    return [r for r in records if str(r.expiry_date)[:10] in expiry_set]


def build_option_df(records):
    """将 OptionSnapshotDay 列表转为 DataFrame，含 strike_price, direction, volume, open_interest, expiry_date, underlying_symbol。"""
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


def build_max_pain_timeseries(filtered_snapshots: list) -> pd.DataFrame:
    """
    将筛选出的全部期权数据按 update_time 分组，计算每组 max_pain_oi/max_pain_vol。
    """
    if not filtered_snapshots:
        return pd.DataFrame()
    grouped = {}
    for s in filtered_snapshots:
        k = str(s.update_time)
        grouped.setdefault(k, []).append(q)

    rows = []
    for update_time, snapshots in grouped.items():
        try:
            mp = calculate_max_pain(snapshots)
            rows.append(
                {
                    "update_time": pd.to_datetime(update_time),
                    "max_pain_oi": float(mp.max_pain_oi) if mp.max_pain_oi is not None else None,
                    "max_pain_vol": float(mp.max_pain_vol) if mp.max_pain_vol is not None else None,
                }
            )
        except Exception:
            continue
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows).sort_values("update_time")


def chart_max_pain_timeseries(df_mp: pd.DataFrame):
    """绘制 max pain 时间序列（OI / Volume 两条曲线）。"""
    if df_mp.empty:
        st.warning("暂无可用于计算 max pain 曲线的数据")
        return
    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=df_mp["update_time"],
            y=df_mp["max_pain_oi"],
            mode="lines+markers",
            name="Max Pain（持仓量）",
            line=dict(color="#1f77b4", width=2),
            marker=dict(size=5),
            hovertemplate="时间: %{x}<br>Max Pain(OI): %{y:.2f}<extra></extra>",
        )
    )
    fig.add_trace(
        go.Scatter(
            x=df_mp["update_time"],
            y=df_mp["max_pain_vol"],
            mode="lines+markers",
            name="Max Pain（成交量）",
            line=dict(color="#ff7f0e", width=2),
            marker=dict(size=5),
            hovertemplate="时间: %{x}<br>Max Pain(Vol): %{y:.2f}<extra></extra>",
        )
    )
    fig.update_layout(
        title="Max Pain 曲线（按 update_time）",
        xaxis_title="update_time",
        yaxis_title="Max Pain",
        height=420,
        hovermode="x unified",
    )
    st.plotly_chart(fig, use_container_width=True)


def main():
    st.title("📊 期权分布")

    config_items = load_stock_option_config()
    if not config_items:
        st.warning("stock_option_info.json 为空，无法加载股票和到期日配置")
        st.stop()

    stock_codes = [item.get("stock_code") for item in config_items if item.get("stock_code")]
    if not stock_codes:
        st.warning("stock_option_info.json 中没有有效 stock_code")
        st.stop()

    stock_to_expiry = {
        item.get("stock_code"): [str(d)[:10] for d in item.get("expiry_dates", [])]
        for item in config_items
        if item.get("stock_code")
    }

    # 侧边栏：股票下拉 + 该股票下的到期日 checkbox，默认选第一个到期日
    with st.sidebar:
        st.subheader("筛选")
        selected_stock = st.selectbox("股票代码 (stock_code)", options=stock_codes, index=0, key="opt_stock")
        expiry_dates = stock_to_expiry.get(selected_stock, [])
        if not expiry_dates:
            st.warning(f"{selected_stock} 在 stock_option_info.json 中无到期日配置")
            st.stop()

        # 默认选第一个到期日
        default_expiry = expiry_dates[0]
        if st.session_state.get("opt_last_stock") != selected_stock:
            st.session_state["opt_last_stock"] = selected_stock
            for ed in expiry_dates:
                st.session_state[f"opt_exp_{selected_stock}_{ed}"] = ed == default_expiry

        # 全选开关：勾选=全部到期日选中；取消=全部不选
        all_key = f"opt_exp_all_{selected_stock}"
        last_all_key = f"opt_exp_all_last_{selected_stock}"
        if all_key not in st.session_state:
            st.session_state[all_key] = False
        if last_all_key not in st.session_state:
            st.session_state[last_all_key] = st.session_state[all_key]
        all_col1, all_col2 = st.columns([3, 1])
        with all_col1:
            st.write("**期权到期日**")
        with all_col2:
            curr_all = st.checkbox("全选", key=all_key)
            prev_all = st.session_state.get(last_all_key, False)
            if curr_all != prev_all:
                for ed in expiry_dates:
                    st.session_state[f"opt_exp_{selected_stock}_{ed}"] = curr_all
                st.session_state[last_all_key] = curr_all

        selected_expiries = []
        for ed in expiry_dates:
            key = f"opt_exp_{selected_stock}_{ed}"
            if st.checkbox(ed, key=key):
                selected_expiries.append(ed)

    if not selected_expiries:
        st.info("请至少勾选一个到期日")
        st.stop()

    all_selected_records = get_all_records_for_stock_and_expiries(
        selected_stock, tuple(selected_expiries)
    )
    if not all_selected_records:
        st.warning("当前股票/到期日筛选下没有可用数据")
        st.stop()

    # 分布图仅展示筛选数据中的最新 update_time 批次
    latest_update_time = max(str(r.update_time) for r in all_selected_records)
    latest_records = [r for r in all_selected_records if str(r.update_time) == latest_update_time]

    df = build_option_df(latest_records)
    df_filtered = df[
        (df["underlying_symbol"] == selected_stock) & (df["expiry_date"].isin(selected_expiries))
    ].copy()
    if df_filtered.empty:
        st.warning("当前筛选无数据")
        st.stop()

    df_mp = build_max_pain_timeseries(all_selected_records)

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

    # 通过筛选出的全部期权数据，按 update_time 分组计算并绘制 max pain 曲线
    st.subheader("📐 Max Pain 曲线（预测股价波动范围）")
    chart_max_pain_timeseries(df_mp)

if __name__ == "__main__":
    main()
