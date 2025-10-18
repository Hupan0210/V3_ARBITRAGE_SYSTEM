# -------------------------------------------------------------------------
# 📁 shared/config.py
# (依赖: pip install pydantic pydantic-settings)
# -------------------------------------------------------------------------
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import List, Dict, Literal

# =========================================================================
# 🎯 策略参数配置 (StrategyConfig)
# =========================================================================
class StrategyConfig(BaseModel):
    """
    定义统计套利策略的核心参数
    """
    
    # ⚠️ 关键触发红线：这是您策略的核心阈值 (0.28%)
    trigger_threshold_pct: float = Field(
        default=0.0028, 
        description="触发开仓的价差百分比 (例如 0.0028 对应 0.28%)"
    )
    
    # 平仓阈值：当价差回归到什么程度时平仓
    close_threshold_pct: float = Field(
        default=0.0001,  # 例如 0.01%
        description="触发平仓的价差回归百分比"
    )

    # 🛑 风控止损线：这是您的硬止损 (0.3%)
    stop_loss_pct: float = Field(
        default=0.003, 
        description="单笔套利持仓的硬止损百分比 (例如 0.003 对应 0.3%)"
    )

    # 交易所统一费率 (0.1%)
    exchange_fee_pct: float = Field(
        default=0.001, 
        description="交易所的统一手续费 (例如 0.001 对应 0.1%)"
    )
    
    # 滑点缓冲 (0.03%)
    slippage_buffer_pct: float = Field(
        default=0.0003,
        description="用于模拟盘和风控计算的滑点缓冲 (例如 0.0003 对应 0.03%)"
    )


# =========================================================================
# ⚙️ 系统运行配置 (SystemConfig)
# =========================================================================
class SystemConfig(BaseModel):
    """
    定义系统的运行模式和启用的组件
    """
    
    # 🧩 模拟/实盘切换：这是模拟盘和实盘的一键切换开关
    EXECUTION_MODE: Literal["PAPER", "LIVE"] = Field(
        default="PAPER",
        description="运行模式: 'PAPER' (模拟盘) 或 'LIVE' (实盘)"
    )

    # 要启用的交易所列表 (必须与 connectors/ 目录下的文件名对应)
    ENABLED_EXCHANGES: List[str] = Field(
        default_factory=lambda: ["binance", "bitget"],
        description="要加载的交易所连接器列表"
    )
    
    # 要启用的交易对列表 (必须使用 'BASE/QUOTE' 格式)
    ENABLED_SYMBOLS: List[str] = Field(
        default_factory=lambda: ["ETH/USDT", "SOL/USDT"],
        description="要监控和交易的币对列表"
    )

# =========================================================================
# 🔌 交易所 API 配置 (ExchangeApiConfig)
# =========================================================================
class ExchangeApiConfig(BaseModel):
    """
    存储单个交易所的 API 密钥信息 (通常从环境变量加载)
    """
    api_key: str = "YOUR_API_KEY"
    secret: str = "YOUR_API_SECRET"
    password: str | None = None  # 某些交易所 (如 OKX) 需要 passphrase


# =========================================================================
# 💾 数据库与消息队列配置 (DatastoreConfig)
# =========================================================================
class DatastoreConfig(BaseModel):
    """
    定义微服务所需的数据存储 (Redis, InfluxDB)
    """
    # Redis: 用于消息队列 (Pub/Sub) 和状态持久化
    REDIS_URL: str = "redis://localhost:6379/0"
    
    # InfluxDB (V2): 用于可视化和时序数据存储
    INFLUXDB_URL: str = "http://localhost:8086"
    INFLUXDB_TOKEN: str = "YOUR_INFLUXDB_TOKEN"
    INFLUXDB_ORG: str = "your_org"
    INFLUXDB_BUCKET: str = "arbitrage_bucket"


# =========================================================================
# 🚀 主配置类 (Settings)
# =========================================================================
class Settings(BaseSettings):
    """
    主配置类，聚合所有配置，并支持从环境变量加载
    
    Pydantic-settings 会自动尝试从 .env 文件或系统环境变量中
    加载匹配的变量 (例如 STRATEGY_TRIGGER_THRESHOLD_PCT)
    """
    
    # 嵌套的配置模型
    strategy: StrategyConfig = StrategyConfig()
    system: SystemConfig = SystemConfig()
    datastore: DatastoreConfig = DatastoreConfig()

    # 动态加载所有启用的交易所的 API 配置
    # 例如: {"binance": ExchangeApiConfig(...), "bitget": ExchangeApiConfig(...)}
    exchanges: Dict[str, ExchangeApiConfig] = Field(
        default_factory=lambda: {
            "binance": ExchangeApiConfig(api_key="BINANCE_KEY", secret="BINANCE_SECRET"),
            "bitget": ExchangeApiConfig(api_key="BITGET_KEY", secret="BITGET_SECRET"),
            "okx": ExchangeApiConfig(api_key="OKX_KEY", secret="OKX_SECRET", password="OKX_PASSWORD")
        },
        description="所有交易所的 API 密钥配置"
    )

    class Config:
        # Pydantic-settings 配置
        # 允许从 .env 文件加载配置
        env_file = ".env"  
        # 环境变量前缀，例如 'APP_STRATEGY_TRIGGER_THRESHOLD_PCT'
        env_prefix = "APP_"  
        # 允许嵌套的环境变量，例如 'APP_STRATEGY__TRIGGER_THRESHOLD_PCT'
        env_nested_delimiter = "__"  


# =========================================================================
#  全局配置实例 (供所有服务导入)
# =========================================================================

# 📦 在项目的任何地方 `from shared.config import settings` 即可使用
settings = Settings()


# --- (可选) 启动时打印配置以供调试 ---
if __name__ == "__main__":
    import json
    # 使用 .model_dump_json() 而不是 print(settings) 来获取清晰的 JSON 输出
    print("--- 启动配置 (Loaded Settings) ---")
    print(settings.model_dump_json(indent=2))
    print("---------------------------------")
    print(f"🎯 触发红线: {settings.strategy.trigger_threshold_pct * 100:.2f}%")
    print(f"🧩 运行模式: {settings.system.EXECUTION_MODE}")
    print(f"💾 Redis URL: {settings.datastore.REDIS_URL}")