# 📈 V3 统计套利系统 (交易所搬砖工)

本项目是一个基于 V3（微服务 + 容器化）架构的高可用统计套利交易系统。它严格遵循“规划-测试-实现-重构”的开发流程，旨在利用多个交易所（Binance, Bitget 等）之间的瞬时价格差（Spread）实现低风险套利。

系统使用 Docker Compose 编排 4 个独立的 Python 微服务（数据、策略、执行、风控）以及 3 个支持服务（Redis, InfluxDB, Grafana），以实现专业级的健壮性、可观测性和故障容错能力。

> **策略哲学 (V1)**: “少即是多。过滤 99% 假机会，只吃 1% 的肥肉。”

---

## 🏛️ V3 架构概览

本系统采用**事件驱动的微服务架构**，服务之间完全解耦，通过 `Redis` (消息队列) 进行异步通信。

### 服务组件 (Docker Compose)

| 服务名称 | 引擎 | 职责 (V3 理念) |
| :--- | :--- | :--- |
| `data_feed` | 🐍 Python | **"眼睛"**: 连接所有交易所 WS，采集行情，发布到 Redis (`CH_MARKET_DATA`)。 |
| `strategy_logic` | 🐍 Python | **"大脑"**: 订阅行情，计算价差 (vs 0.28%)，发布交易信号 (`CH_TRADE_SIGNALS`)，写入价差到 InfluxDB。 |
| `execution_engine` | 🐍 Python | **"双手"**: 订阅交易信号，执行（模拟/实盘）交易，处理单边成交风险 (风控前置)，发布成交回报 (`CH_EXEC_REPORTS`)，持久化仓位到 Redis。 |
| `risk_manager` | 🐍 Python | **"鹰眼"**: 订阅所有事件，监控 0.3% 止损、服务心跳、账户余额；记录所有日志到 InfluxDB；发布强制平仓信号。 |
| `redis` | 💾 Redis | **"神经中枢"**: (1) 消息队列 (Pub/Sub) (2) 状态持久化 (Key/Value)，用于崩溃恢复。 |
| `influxdb` | 💾 InfluxDB | **"时序数据库"**: 存储所有时间序列数据 (价差、成交、警报、余额)。 |
| `grafana` | 📊 Grafana | **"可视化仪表盘"**: 从 InfluxDB 读取数据并实时展示（`http://localhost:3000`）。 |

### 数据流 (Event-Driven Flow)

```mermaid
graph TD
    subgraph 交易所 (Binance, Bitget)
        direction LR
        API_B(WS API)
        API_G(WS API)
    end

    subgraph V3 系统 (Docker Network: arbitrage_net)
        direction TB

        API_B -- Ticker --> S1(data_feed);
        API_G -- Ticker --> S1;

        S1 -- Ticker (JSON) --> DB1(Redis Pub/Sub<br>[CH_MARKET_DATA]);

        DB1 -- Ticker (JSON) --> S2(strategy_logic);
        DB1 -- Ticker (JSON) --> S4(risk_manager<br>PnL 止损);

        S2 -- 价差 (Float) --> DB2(InfluxDB);
        S2 -- 信号 (JSON) --> DB1_SIG(Redis Pub/Sub<br>[CH_TRADE_SIGNALS]);

        DB1_SIG -- 信号 (JSON) --> S3(execution_engine);
        
        S3 -- API Call --> REST(交易所 REST API);
        REST -- 成交回报 --> S3;

        S3 -- 成交 (JSON) --> DB1_REP(Redis Pub/Sub<br>[CH_EXEC_REPORTS]);
        S3 -- 警报 (JSON) --> DB1_ALERT(Redis Pub/Sub<br>[CH_RISK_ALERTS]);
        S3 -- 写入仓位状态 --> DB1_STATE(Redis State<br>[KEY_POSITIONS]);

        DB1_REP -- 成交 (JSON) --> S2(解除 PENDING 锁);
        DB1_REP -- 成交 (JSON) --> S4(写入 InfluxDB);
        DB1_ALERT -- 警报 (JSON) --> S4(写入 InfluxDB);
        DB1_STATE -- 读取仓位 --> S4(PnL 计算);
        
        S4 -- 余额/日志 --> DB2;
    end
    
    DB2 -- Flux Query --> V(Grafana<br>http://localhost:3000);
🖥️ 环境要求 (Prerequisites)
此系统被设计为跨平台运行。在您的 Windows 11 (i5-12500H, 16GB) 机器上，您仅需要以下软件：

[必需] Docker Desktop for Windows:

这是运行所有 7 个服务（容器）的核心。

请确保在 Docker Desktop Settings -> Resources 中为 Docker 分配了足够的资源（推荐：4-6 GB 内存）。

[必需] VS Code (或任何代码编辑器):

用于编辑代码和 .env 配置文件。

[推荐] Git:

用于版本控制 (您已安装)。

重要提示: 您本地安装的 XAMPP / PHP / Python 环境在此项目中不会被使用。所有 Python 代码都在 Docker 容器内部隔离运行，确保了环境的 100% 纯净和一致性。

🚀 快速启动 (Quick Start)
这是一个 5 分钟指南，用于在模拟盘 (PAPER) 模式下启动完整系统。

准备 .env:

将 .env.example 复制并重命名为 .env。

打开 .env 文件。

找到 APP_DATASTORE__INFLUXDB_TOKEN 和 DOCKER_INFLUXDB_INIT_ADMIN_TOKEN。

为它们设置一个相同的强密码 (例如: my_secret_token_123!)。

构建并启动 (Build & Up):

在 VS Code 终端中 (确保位于 docker-compose.yml 所在的根目录)，运行：
docker-compose up --build

等待启动:

终端将显示所有 7 个服务的日志。

等待约 30-60 秒，直到日志滚动稳定，并看到 data_feed, strategy_logic, execution_engine, risk_manager 均显示 "服务已启动"。

访问仪表盘:

打开浏览器并访问: http://localhost:3000

登录 (默认: admin / admin)。

在左侧菜单点击 "Dashboards" -> "General" -> "V3 Arbitrage Dashboard"。

恭喜！ 系统现在正在 PAPER (模拟) 模式下运行。execution_engine 会模拟交易（含滑点），所有价差、模拟成交和风控警报都会实时显示在您的 Grafana 仪表盘上。

🛠️ 详细部署与配置 (Detailed Setup & Configuration)
步骤 1: 准备项目文件
获取代码: 克隆 (Clone) 或下载本项目到您的本地机器。

创建 .gitignore (关键): 在项目根目录创建 .gitignore 文件（我们已在 V3.1 中提供），确保 .env 文件被忽略，防止密钥泄露。

步骤 2: 关键安全配置 (.env)
docker-compose.yml 被配置为从 .env 文件中加载所有敏感信息。

复制模板: cp .env.example .env (或手动复制)。

编辑 .env:
# --- (1) Pydantic Settings (APP_ 前缀) ---
# 这些变量会被 shared/config.py 自动加载

# 🧩 模式切换: "PAPER" (模拟盘) 或 "LIVE" (实盘)
APP_SYSTEM__EXECUTION_MODE="PAPER"

# 💾 Redis URL (必须使用服务名 "redis" 而不是 "localhost")
APP_DATASTORE__REDIS_URL="redis://redis:6379/0"

# 💾 InfluxDB (必须与下面的 DOCKER_INFLUXDB_INIT 变量匹配!)
APP_DATASTORE__INFLUXDB_URL="http://influxdb:8086"
APP_DATASTORE__INFLUXDB_TOKEN="YOUR_SECURE_INFLUXDB_TOKEN" # ❗️ 填入强令牌
APP_DATASTORE__INFLUXDB_ORG="my-org"
APP_DATASTORE__INFLUXDB_BUCKET="arbitrage_bucket"

# 🔌 交易所 API 密钥
APP_EXCHANGES__BINANCE__API_KEY="YOUR_BINANCE_API_KEY"
APP_EXCHANGES__BINANCE__SECRET="YOUR_BINANCE_API_SECRET"

APP_EXCHANGES__BITGET__API_KEY="YOUR_BITGET_API_KEY"
APP_EXCHANGES__BITGET__SECRET="YOUR_BITGET_API_SECRET"
APP_EXCHANGES__BITGET__PASSWORD="YOUR_BITGET_PASSPHRASE"

# --- (2) Docker Compose (DOCKER_ 前缀) ---
# 这些变量用于初始化 InfluxDB 容器 (见 docker-compose.yml)
# ❗️ 这些值必须与上面的 APP_DATASTORE__ 匹配

DOCKER_INFLUXDB_INIT_MODE=setup
DOCKER_INFLUXDB_INIT_USERNAME=admin
DOCKER_INFLUXDB_INIT_PASSWORD=your_secure_influxdb_password # InfluxDB 内部密码 (可单独设置)
DOCKER_INFLUXDB_INIT_ORG=my-org
DOCKER_INFLUXDB_INIT_BUCKET=arbitrage_bucket
DOCKER_INFLUXDB_INIT_RETENTION=7d
DOCKER_INFLUXDB_INIT_ADMIN_TOKEN="YOUR_SECURE_INFLUXDB_TOKEN" # ❗️ 必须与 APP_DATASTORE__INFLUXDB_TOKEN 相同
🔒 安全警告

永远不要 将您的 .env 文件提交到 Git 或任何公共仓库。.gitignore 文件应始终包含 .env。

步骤 3: 启动系统 (Docker Compose)
首次启动 (或更新依赖后):

使用 --build 来强制 Docker 重新构建 Dockerfile 并安装 requirements.txt 中的库。
docker-compose up --build
常规启动 (代码修改后):

由于我们使用 volumes: [.:/app] 挂载了本地代码，您不需要在修改 Python 代码（例如 strategy_logic/main.py）后重新构建。

Docker 容器内的 Python 进程会自动检测到文件更改并（通常）重启，或在下次启动时加载新代码。
# 停止 (如果正在运行)
docker-compose down

# 重新启动 (不重建)
docker-compose up

步骤 4: 访问与理解可视化仪表盘
访问: http://localhost:3000

登录: admin / admin (首次登录会提示修改密码)。

导航: Dashboards -> General -> V3 Arbitrage Dashboard。

仪表盘详解:

Panel 1 & 2 (Time Series): 价差 vs 阈值 (ETH & SOL)

显示两个交易所之间的买/卖价差百分比 (例如 spread_binance_ask_bitget_bid)。

一条红色虚线显示您的 0.28% (threshold_open) 触发红线。

数据来源: services/strategy_logic -> InfluxDB (spreads 表)

Panel 3 (Table): Trade Execution Log

显示 execution_engine 发出的每一笔成交回报 (Order)。

显示 exchange, symbol, side (BUY/SELL), status (CLOSED/FAILED), price (成交均价), qty (数量)。

数据来源: services/risk_manager -> InfluxDB (trades 表)

Panel 4 (Table): Risk Alerts

最重要的风控面板。显示所有非正常事件。

STOP_LOSS_HIT: V1 止损 (0.3%) 被触发。

LEG_RISK_DETECTED: V3 风控前置 (清道夫) 被触发 (发生单边成交)。

HEARTBEAT_FAILURE: V1 断连保护被触发 (某个微服务已崩溃)。

BALANCE_ASYMMETRY: V1 余额保护被触发 (交易所余额不平衡)。

数据来源: services/risk_manager -> InfluxDB (risk_alerts 表)

Panel 5-8 (Stat): Account Balances

实时显示 (每 5 分钟刷新) 各交易所的主要资产余额。

数据来源: services/risk_manager -> InfluxDB (account_balances 表)

⚙️ 核心使用指南 (User Guide)
A. ❗️ 如何从 PAPER 切换到 LIVE (模拟盘 -> 实盘)
🔥🔥🔥 严重风险警告 🔥🔥🔥

切换到 LIVE 模式将使用您的真实资金通过 API 执行市价单 (Market Orders)。

确保 API 密钥安全: 密钥是否仅限于您的服务器 IP？

确保 API 权限正确: 密钥是否已在交易所后台启用了“现货交易” (Spot Trading) 权限？

从小资金开始: 始终使用小额资金 (trade_qty 变量) 测试实盘，直到您 100% 确认系统按预期运行。

您对所有交易负全责。

切换步骤:

停止系统: 在终端按 Ctrl+C，然后运行 docker-compose down。

检查 API 密钥: 确保 .env 文件中的 APP_EXCHANGES__... 密钥均已正确填入，并且在交易所后台已启用交易权限。

修改模式: 编辑 .env 文件:
- APP_SYSTEM__EXECUTION_MODE="PAPER"
+ APP_SYSTEM__EXECUTION_MODE="LIVE"

修改交易量 (可选但推荐):

打开 services/execution_engine/main.py。

找到 _handle_open_signal 函数。

修改 trade_qty 为一个您能接受的小额实盘测试量 (例如 0.005 ETH)。
# services/execution_engine/main.py -> _handle_open_signal()
async def _handle_open_signal(self, signal: TradeSignal):

    # ❗️ (简化) 假设固定交易量
    # ❗️ 专业的做法是基于余额、风险敞口和价格计算交易量
    trade_qty = 0.005 # ❗️ [实盘] 修改为您的小额测试量

重新启动:
docker-compose up

监控: 密切观察 http://localhost:3000 (Grafana) 仪表盘中的 "Trade Execution Log" 和 "Risk Alerts" 面板，同时在交易所 App/网站上核对真实成交。

B. 如何修改策略参数 (例如: 0.28% -> 0.30%)
得益于 volumes 挂载，修改策略参数无需重建 Docker 镜像。

停止系统: docker-compose down

修改代码: 打开 shared/config.py。

编辑参数: 找到 StrategyConfig 类并修改 default 值。
# shared/config.py
class StrategyConfig(BaseModel):
    trigger_threshold_pct: float = Field(
        default=0.0030, # ❗️ 从 0.0028 修改为 0.0030
        description="触发开仓的价差百分比 (例如 0.0030 对应 0.30%)"
    )

    # (您也可以修改止损线或平仓线)
    stop_loss_pct: float = Field(
        default=0.0035, # ❗️ (示例) 止损从 0.3% 修改为 0.35%
        description="单笔套利持仓的硬止损百分比 (例如 0.0035 对应 0.35%)"
    )

重新启动: docker-compose up

验证: strategy_logic 服务将使用新参数。您将在 Grafana 价差图表上看到红色虚线 (阈值) 移动到新位置 (0.30%)。

C. 如何添加新币对 (例如: BTC/USDT)
停止系统: docker-compose down

修改配置: 打开 shared/config.py。

编辑列表: 找到 SystemConfig 类并修改 ENABLED_SYMBOLS。

# shared/config.py
class SystemConfig(BaseModel):
    ENABLED_SYMBOLS: List[str] = Field(
        default_factory=lambda: ["ETH/USDT", "SOL/USDT", "BTC/USDT"], # ❗️ 新增 "BTC/USDT"
        description="要监控和交易的币对列表"
    )

重新启动: docker-compose up

验证: data_feed 服务现在将订阅 BTC/USDT。strategy_logic 将开始计算 BTC 的价差并将其写入 InfluxDB。您需要在 Grafana 仪表盘上复制 (Duplicate) 现有的 ETH 图表，并将查询中的 symbol 过滤条件从 "ETH/USDT" 修改为 "BTC/USDT" 才能看到新图表。

D. (高级) 如何添加新交易所 (例如: OKX)
停止系统: docker-compose down

[编码] 创建新连接器:

在 shared/connectors/ 目录下创建 okx_connector.py。

此类必须继承 BaseConnector (from .base_connector import BaseConnector)。

您必须实现所有 abstractmethod (例如 connect_ws, place_order, get_balance)，并处理 OKX 特有的 API 签名和数据格式。

命名约定: 类名必须是 OkxConnector (首字母大写 + "Connector")。

[配置] 启用连接器:

打开 shared/config.py -> SystemConfig。

将 "okx" 添加到 ENABLED_EXCHANGES 列表。
# shared/config.py
ENABLED_EXCHANGES: List[str] = Field(
    default_factory=lambda: ["binance", "bitget", "okx"], # ❗️ 新增 "okx"
)

[配置] 添加密钥:

打开 .env 文件。

添加 OKX 所需的 API 密钥 (OKX 可能需要 password，即 passphrase)。
# .env (文件末尾)
APP_EXCHANGES__OKX__API_KEY="YOUR_OKX_API_KEY"
APP_EXCHANGES__OKX__SECRET="YOUR_OKX_SECRET"
APP_EXCHANGES__OKX__PASSWORD="YOUR_OKX_PASSPHRASE"

[依赖] (可能):

如果 okx_connector.py 需要新的 Python 库，请将其添加到 requirements.txt。

重新启动:

如果添加了新依赖 (步骤 5)，必须使用 docker-compose up --build。

如果未添加新依赖，只需 docker-compose up。

验证: data_feed 和 risk_manager 现在将连接到 OKX。strategy_logic 将开始计算 binance vs okx 和 bitget vs okx 的价差 (V3.1 中 _check_arbitrage 仅限 2 个交易所，需要重构以支持 N-N 比较)。

🛑 系统维护 (Maintenance)
查看实时日志
# 查看所有服务的合并日志 (实时)
docker-compose logs -f

# 仅查看 "大脑" (strategy_logic) 的日志
docker-compose logs -f strategy_logic

# 仅查看 "双手" (execution_engine) 的日志
docker-compose logs -f execution_engine

停止系统

# (在前台运行 `docker-compose up` 时)
# 按 Ctrl+C

# (如果使用 -d 在后台运行)
docker-compose down

此命令会停止并移除所有 7 个容器。

数据安全: redis-data, influxdb-data 和 grafana-data 卷中存储的数据不会被删除。

🚨 完全重置 (清除所有数据)
如果您想彻底清除所有持久化数据（Redis 仓位、InfluxDB 日志、Grafana 设置）并从零开始：

# 1. 停止并移除容器、网络
docker-compose down

# 2. (危险!) 移除所有持久化数据卷
# (注意: 卷的名称可能包含项目文件夹前缀, 例如 v3_arbitrage_system_redis-data)
docker volume rm $(docker volume ls -q | grep "redis-data")
docker volume rm $(docker volume ls -q | grep "influxdb-data")
docker volume rm $(docker volume ls -q | grep "grafana-data")

下次运行 docker-compose up 时，系统将像全新安装一样启动。

最终目录结构 (V3.1)
V3_ARBITRAGE_SYSTEM/
│
├── 🚀 docker-compose.yml       (V3.1 最终版 - 7 合 1 启动器)
├── 📦 Dockerfile               (用于构建 Python 微服务的基础镜像)
├── 📋 requirements.txt         (所有 Python 依赖)
├── 🔒 .env.example            (❗️ 环境变量模板，必须复制为 .env)
├── 🙈 .gitignore              (Git 忽略文件，保护 .env)
├── 📖 README.md               (本文件)
│
├── 💾 datastore/
│   ├── __init__.py
│   ├── influx_client.py      (V3 可视化数据库客户端)
│   └── redis_client.py       (V3 消息队列 & 状态持久化客户端)
│
├── 🧩 shared/
│   ├── __init__.py
│   ├── config.py             (V3 配置中心, 含 0.28% 红线)
│   └── connectors/
│       ├── __init__.py         (V3 交易所工厂)
│       ├── base_connector.py (V3 交易所“合同”/抽象基类)
│       ├── binance_connector.py (Binance 实现)
│       └── bitget_connector.py  (Bitget 实现)
│
├── ⚙️ services/
│   ├── data_feed/
│   │   └── main.py             (V3 微服务 1: 眼睛 - 采集行情)
│   ├── execution_engine/
│   │   └── main.py             (V3 微服务 3: 双手 - 执行交易)
│   ├── risk_manager/
│   │   └── main.py             (V3 微服务 4: 鹰眼 - 风控/日志/止损)
│   └── strategy_logic/
│       └── main.py             (V3.1 微服务 2: 大脑 - 计算价差/写入 Influx)
│
└── 📊 monitoring/
    ├── grafana_dashboards/
    │   └── main_dashboard.json (V3.1 Grafana 仪表盘 JSON 定义)
    └── grafana_provisioning/
        ├── dashboards/
        │   └── dashboards.yml    (V3.1 告诉 Grafana 从哪里加载仪表盘)
        └── datasources/
            └── datasources.yml   (V3.1 告诉 Grafana 如何自动连接 InfluxDB)
