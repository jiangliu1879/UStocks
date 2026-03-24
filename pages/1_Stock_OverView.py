"""
股票概览页：展示所有股票最新交易日的涨跌幅、成交量、成交量水位、市场概述（可编辑并更新到 DB）。
侧边栏通过 checkbox 勾选要展示的股票，默认全选。
"""
from __future__ import annotations

import os
import sys
from datetime import datetime, date

import numpy as np
import streamlit as st

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from data_models.stock_data import StockData
from data_models.stock_valuation import StockValuation

st.set_page_config(page_title="股票概览", page_icon="📋", layout="wide")

CACHE_TTL_SECONDS = 300
OVERVIEW_DATA_VERSION_KEY = "ov_data_version"


def _timestamp_to_date_str(t) -> str:
    if hasattr(t, "strftime"):
        return t.strftime("%Y-%m-%d")
    return str(t)[:10]


@st.cache_data(ttl=CACHE_TTL_SECONDS)
def get_latest_trade_date(_refresh_key: int) -> date:
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


@st.cache_data(ttl=CACHE_TTL_SECONDS)
def get_stock_codes(_refresh_key: int) -> list[str]:
    return StockData.get_stock_codes()


@st.cache_data(ttl=CACHE_TTL_SECONDS)
def get_latest_stock_info(stock_code: str, target_date: date | None = None, _refresh_key: int = 0):
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


@st.cache_data(ttl=CACHE_TTL_SECONDS)
def get_latest_valuations_map(stock_codes: tuple[str, ...], _refresh_key: int):
    """批量获取股票最新估值，减少页面 N+1 查询。"""
    if not stock_codes:
        return {}
    return StockValuation.query_latest_by_stock_codes(list(stock_codes))


def format_change_pct(change_pct: float) -> str:
    sign = "+" if change_pct >= 0 else ""
    return f"{sign}{change_pct:.2f}%"


def format_volume_ratio(volume_ratio: float) -> str:
    if volume_ratio >= 150:
        level = "极高"
    elif volume_ratio >= 120:
        level = "较高"
    elif volume_ratio >= 80:
        level = "正常"
    elif volume_ratio >= 50:
        level = "较低"
    else:
        level = "极低"
    return f"{level} ({volume_ratio:.1f}%)"


# ---------- 侧边栏：股票勾选（不展示 QQQ、SPY）----------
if OVERVIEW_DATA_VERSION_KEY not in st.session_state:
    st.session_state[OVERVIEW_DATA_VERSION_KEY] = 0
data_version = st.session_state[OVERVIEW_DATA_VERSION_KEY]

all_codes = get_stock_codes(data_version)
stock_codes = [c for c in all_codes if c not in ("QQQ.US", "SPY.US", "AAPL.US")]
if not stock_codes:
    st.warning("暂无股票数据")
    st.stop()

# 默认全部勾选：首次或新增股票时未在 session 里的都视为勾选
for code in stock_codes:
    if f"ov_cb_{code}" not in st.session_state:
        st.session_state[f"ov_cb_{code}"] = True

with st.sidebar:
    st.subheader("📋 选择股票")
    # 全选 / 取消全选
    c1, c2 = st.columns(2)
    with c1:
        if st.button("全选", key="ov_select_all"):
            for code in stock_codes:
                st.session_state[f"ov_cb_{code}"] = True
            st.rerun()
    with c2:
        if st.button("取消全选", key="ov_select_none"):
            for code in stock_codes:
                st.session_state[f"ov_cb_{code}"] = False
            st.rerun()

    for code in stock_codes:
        st.checkbox(code, key=f"ov_cb_{code}", label_visibility="visible")

selected_codes = [c for c in stock_codes if st.session_state.get(f"ov_cb_{c}", True)]

# ---------- 主区域 ----------
trade_date = get_latest_trade_date(data_version)
st.title("📋 股票概览")

if not selected_codes:
    st.info("请在侧边栏勾选要展示的股票。")
    st.stop()

# 拉取选中股票的最新交易日数据
info_by_code = {}
for code in selected_codes:
    info = get_latest_stock_info(code, target_date=trade_date, _refresh_key=data_version)
    if info is None:
        continue
    info_by_code[code] = info

if not info_by_code:
    st.info("所选股票在最新交易日没有数据。")
    st.stop()

st.subheader(f"最新交易日统计（{trade_date.strftime('%Y-%m-%d')}）")
# 一行一只股票，左侧为指标、右侧为市场概述
valid_codes = [c for c in selected_codes if c in info_by_code]
valuation_map = get_latest_valuations_map(tuple(valid_codes), data_version)
for code in valid_codes:
    info = info_by_code[code]
    st.markdown("---")
    col_left, col_right = st.columns([1, 2])  # 右侧市场概述更宽
    with col_left:
        st.subheader(code)
        st.metric(
            label="收盘价",
            value=f"${info['close']:.2f}",
            delta=f"{format_change_pct(info['change_pct'])}",
        )
        st.write(f"**涨跌幅:** {format_change_pct(info['change_pct'])}")
        st.write(f"**成交量水位:** {format_volume_ratio(info['volume_ratio'])}")
        st.write(f"**成交量:** {info['volume']:,.0f}")
        st.write(f"**今日区间:** ${info['low']:.2f} - ${info['high']:.2f}")
        latest_val = valuation_map.get(code)
        st.write("**估值区间:**", latest_val.valuation_range if latest_val else "*暂无*")
    with col_right:
        st.write("**市场概述：**")
        desc_key = f"ov_desc_{code}"
        date_key = f"ov_desc_date_{code}"
        edit_key = f"ov_edit_{code}"
        if desc_key not in st.session_state or st.session_state.get(date_key) != trade_date:
            raw = (info.get("description") or "").replace("\r\n", "\n").replace("\r", "\n")
            st.session_state[desc_key] = raw
            st.session_state[date_key] = trade_date
            st.session_state[edit_key] = False
        if st.session_state.get(edit_key, False):
            # 编辑状态：文本框显示原内容；点击「更新」将当前内容写入数据库
            current_text = st.text_area(
                "市场概述",
                height=400,
                key=desc_key,
                label_visibility="collapsed",
                placeholder="输入或编辑市场概述…（支持 Markdown）",
            )
            if st.button("更新", key=f"ov_btn_{code}"):
                # 以 session_state 为准，避免同轮渲染中 text_area 返回值未及时更新
                new_desc = (st.session_state.get(desc_key) or "").replace("\r\n", "\n").replace("\r", "\n")
                target_date_str = _timestamp_to_date_str(info["timestamp"])
                # 按 stock_code 取最近记录，再按日期匹配，避免 DB 中 timestamp 格式不一致导致查不到
                recent = StockData.query(
                    conditions={"stock_code": code},
                    limit=60,
                    order_by="timestamp DESC",
                )
                rec = None
                for r in recent:
                    if _timestamp_to_date_str(r.timestamp) == target_date_str:
                        rec = r
                        break
                if rec:
                    # 写回时保持与 DB 一致的 timestamp 字符串格式，确保 ON DUPLICATE KEY UPDATE 命中
                    rec.description = new_desc
                    rec.timestamp = _timestamp_to_date_str(rec.timestamp)
                    if rec.save():
                        st.session_state[OVERVIEW_DATA_VERSION_KEY] = st.session_state.get(OVERVIEW_DATA_VERSION_KEY, 0) + 1
                        st.session_state[edit_key] = False
                        st.success(f"已更新 {code} 的市场概述。")
                        st.rerun()
                    else:
                        st.error("更新失败，请重试。")
                else:
                    st.warning(f"未找到 {code} 对应日期的记录（{target_date_str}），无法更新。")
        else:
            # 预览状态：渲染 Markdown，按钮为「编辑」
            content = st.session_state.get(desc_key) or ""
            if not content.strip():
                st.markdown("*暂无内容，点击「编辑」填写*")
            else:
                st.markdown(content)
            if st.button("编辑", key=f"ov_edit_btn_{code}"):
                st.session_state[edit_key] = True
                # 进入编辑时用当前记录的 description 原始数据填充文本框
                raw = (info.get("description") or "").replace("\r\n", "\n").replace("\r", "\n")
                st.session_state[desc_key] = raw
                st.rerun()
