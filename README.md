# UStocks - 美股数据采集与分析系统

## 📖 项目简介

UStocks 是一个基于 Streamlit 的美股数据采集与分析系统，集成长桥 API 实现股票行情、期权链数据的获取与存储，提供市场概览、股票分析、年化收益计算、MaxPain 等可视化分析功能。

## ✨ 主要功能

### Web 界面（Streamlit）

| 页面 | 说明 |
|------|------|
| **Home** | 市场概览：SPY、QQQ 最新行情，市场简述（支持 Markdown 编辑） |
| **1_Stock_OverView** | 股票概览：多股票最新交易日涨跌幅、成交量、成交量水位、市场概述（可编辑） |
| **2_Stock_Analyzer** | 股票分析：历史价格、成交量、成交额图表，交易统计 |
| **3_SPY_MaxPain** | SPY 期权 MaxPain 分析 |
| **5_Stock_Gain** | 股票年化收益分析：多股票年化收益率对比，支持拆股复权 |

### 数据采集

- **股票数据**：`utils/get_stock_data.py` 通过长桥 API 获取 K 线并写入 `stock_data` 表
- **实时期权收集器**：`scheduled_realtime_data_collector.py` 交易时间内定时采集期权数据
- **开盘收盘收集器**：`scheduled_open_close_data_collector.py` 开盘/收盘时段采集，支持多到期日

### 数据模型

- **StockData**：股票行情（OHLCV、description）
- **StockSplit**：拆股信息（stock_code、timestamp、times）
- **option_quote**：期权报价
- **max_pain**：最大痛点指标

## 🛠️ 技术栈

- **Web 框架**：Streamlit
- **数据处理**：Pandas、NumPy
- **数据可视化**：Plotly
- **API**：长桥 Longport
- **数据库**：MySQL
- **任务调度**：Schedule
- **时区**：Pytz

## 📋 环境要求

- Python >= 3.10
- MySQL >= 8.0

## 🚀 快速开始

### 1. 安装依赖

```bash
pip install -r requirements.txt
```

### 2. 配置环境变量

```bash
export MYSQL_HOST=your_mysql_host
export MYSQL_USER=your_mysql_user
export MYSQL_PASS=your_mysql_password
```

### 3. 初始化数据库

```bash
python init_database.py
```

创建数据库 `ustocks` 及表：`stock_split`、`stock_data`、`option_quote`、`max_pain` 等。

### 4. 启动 Web 应用

```bash
streamlit run Home.py
```

访问 `http://localhost:8501`。

## 📂 项目结构

```
UStocks/
├── Home.py                              # Streamlit 主入口（市场概览）
├── init_database.py                     # 数据库初始化
├── delete_stock_data.py                 # 删除指定股票数据（示例脚本）
├── scheduled_realtime_data_collector.py # 实时期权数据收集
├── scheduled_open_close_data_collector.py # 开盘/收盘期权数据收集
├── stock_option_info.json               # 期权采集配置
├── requirements.txt
├── data_models/
│   ├── stock_data.py                    # 股票行情模型
│   ├── stock_split.py                   # 拆股信息模型
│   ├── option_quote.py                  # 期权报价模型
│   └── max_pain.py                      # MaxPain 模型
├── utils/
│   ├── get_stock_data.py                # 股票 K 线获取
│   ├── get_option_data.py               # 期权数据获取
│   ├── calculate_max_pain.py            # MaxPain 计算
│   ├── get_trading_time.py              # 交易时间判断
│   ├── get_calc_indexes.py              # 计算索引
│   └── logger.py                        # 日志配置
├── pages/                               # Streamlit 子页面
│   ├── 1_Stock_OverView.py              # 股票概览
│   ├── 2_Stock_Analyzer.py              # 股票分析
│   ├── 3_SPY_MaxPain.py                 # SPY MaxPain
│   └── 5_Stock_Gain.py                  # 年化收益分析
└── logs/                                # 日志目录
```

## 📊 数据库表

| 表名 | 说明 |
|------|------|
| `stock_data` | 股票行情：stock_code、timestamp、OHLCV、turnover、description |
| `stock_split` | 拆股信息：stock_code、timestamp（拆股日）、times（拆股倍数） |
| `option_quote` | 期权报价 |
| `max_pain` | 最大痛点分析 |

## 🔧 配置说明

### 期权采集配置 `stock_option_info.json`

```json
{
    "stock_code": "NVDA.US",
    "expiry_dates": ["2026-01-23", "2026-01-30", ...],
    "strike_price_range": [100, 250]
}
```

### 拆股数据

拆股信息存储在 `stock_split` 表，可通过 `data_models.stock_split.StockSplit` 写入。年化收益页会按拆股日对收盘价进行复权。

## 💻 常用命令

```bash
# 启动 Web
streamlit run Home.py

# 实时期权采集（交易时间内）
python scheduled_realtime_data_collector.py

# 开盘/收盘采集
python scheduled_open_close_data_collector.py

# 初始化数据库
python init_database.py
```

## ⚠️ 注意事项

1. 需有效长桥 API 权限（行情、期权等）
2. 确保 MySQL 服务可用
3. 数据采集建议用 `nohup` 或 systemd 常驻运行
4. 注意 API 调用频率限制

## 📄 许可证

MIT License
