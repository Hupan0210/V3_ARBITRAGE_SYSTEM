# -------------------------------------------------------------------------
# 📁 services/data_feed/main.py
# (V3 架构的第一个微服务：数据采集与分发中心)
#
# 运行依赖:
# 1. 确保已安装: pip install redis aiohttp websockets pydantic pydantic-settings
# 2. 确保 Redis 服务器正在运行 (在 config.py 中配置的地址)
# 3. 确保 'shared' 和 'datastore' 目录在 Python 路径中
#
# 启动命令 (在项目根目录): python services/data_feed/main.py
# -------------------------------------------------------------------------
import asyncio
import logging
import signal
from typing import Dict, List

# 导入共享基础
from shared.config import settings
from shared.connectors import BaseConnector, Ticker, TickerCallback, create_connectors

# 导入数据存储 (Redis)
from datastore.redis_client import redis_client, CH_MARKET_DATA

# =========================================================================
#  日志配置
# =========================================================================
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] [%(name)s] - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger("DataFeedService")

# =========================================================================
#  全局状态与停机事件
# =========================================================================
# 🔌 用于优雅停机的 asyncio 事件
shutdown_event = asyncio.Event()

# =========================================================================
#  核心回调函数
# =========================================================================

async def on_ticker_received(ticker: Ticker):
    """
    ❗️ 关键: Ticker 回调函数
    
    当任何交易所 (Binance, Bitget...) 的连接器 (Connector) 
    产生一个新的标准化 Ticker 时，此函数将被异步调用。
    
    职责: 将 Ticker 立即发布到 Redis 的 `CH_MARKET_DATA` 频道。
    """
    try:
        # log.debug(f"Ticker received: {ticker.model_dump_json()}") # INFO 级别日志量太大
        
        # 🚀 核心: 将 Ticker (Pydantic 模型) 发布到 Redis 消息队列
        await redis_client.publish(CH_MARKET_DATA, ticker)
        
    except Exception as e:
        log.error(
            f"[{ticker.exchange}:{ticker.symbol}] 处理 Ticker 或发布到 Redis 失败: {e}", 
            exc_info=True
        )

# =========================================================================
#  (V3) 心跳任务
# =========================================================================

async def run_heartbeat():
    """
    ❤️ (风控) 定期向 Redis 报告 "data_feed" 服务存活
    
    这允许 V3 的 `risk_manager` 服务监控此服务是否崩溃。
    """
    log.info("❤️ 心跳服务已启动 (10s 间隔)")
    while not shutdown_event.is_set():
        try:
            # 向 Redis Hash 键 `state:service_heartbeat` 写入 "data_feed": "timestamp"
            await redis_client.set_heartbeat("data_feed")
            
            # 等待 10 秒，或等待关闭信号
            await asyncio.wait_for(shutdown_event.wait(), timeout=10)
            
        except asyncio.TimeoutError:
            continue # 10 秒到了，继续下一次心跳
        except Exception as e:
            log.error(f"❤️ 心跳服务出错: {e}", exc_info=True)
            await asyncio.sleep(10) # 出错了，等 10s 重试

# =========================================================================
#  服务关闭处理
# =========================================================================

def handle_shutdown_signal():
    """
    捕获 SIGINT (Ctrl+C) 或 SIGTERM 信号并设置关闭事件
    """
    if not shutdown_event.is_set():
        log.warning("🔌 收到关闭信号 (SIGINT/SIGTERM)。正在准备优雅停机...")
        shutdown_event.set()
    else:
        log.warning("🔌 再次收到关闭信号。请耐心等待...")

# =========================================================================
#  主程序 (Main)
# =========================================================================

async def main():
    log.info("🚀 [V3 DataFeed] 微服务启动中...")
    log.info(f"🧩 运行模式: {settings.system.EXECUTION_MODE}")
    
    # 1. 注册信号处理器 (用于 Docker/Ctrl+C 优雅停机)
    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGINT, handle_shutdown_signal)
    loop.add_signal_handler(signal.SIGTERM, handle_shutdown_signal)

    connectors: Dict[str, BaseConnector] = {}
    tasks: List[asyncio.Task] = []

    try:
        # 2. 连接 Redis (V3 神经中枢)
        await redis_client.connect()
        
        # 3. 📦 (V3 理念) 调用工厂，创建所有启用的连接器
        connectors = create_connectors()
        if not connectors:
            log.critical("❗️ 没有任何连接器被加载 (检查 config.py)。服务即将退出。")
            return

        log.info(f"已加载 {len(connectors)} 个连接器: {list(connectors.keys())}")
        
        # 4. 获取要订阅的币对
        symbols_to_subscribe = settings.system.ENABLED_SYMBOLS
        if not symbols_to_subscribe:
             log.critical("❗️ 没有任何币对被启用 (ENABLED_SYMBOLS 为空)。服务即将退出。")
             return
        
        log.info(f"即将订阅 {len(symbols_to_subscribe)} 个币对: {symbols_to_subscribe}")

        # 5. 启动所有 WebSocket 连接任务
        for name, connector in connectors.items():
            log.info(f"正在启动 {name} 的 WebSocket 连接...")
            
            # ❗️ 关键: 将 `on_ticker_received` 回调函数传入
            task = asyncio.create_task(
                connector.connect_ws(symbols_to_subscribe, on_ticker_received)
            )
            tasks.append(task)
            
        # 6. 启动心跳任务
        heartbeat_task = asyncio.create_task(run_heartbeat())
        tasks.append(heartbeat_task)
        
        # 7. 运行所有任务直到收到关闭信号
        log.info("✅ [V3 DataFeed] 服务已启动。正在监听所有行情并推送到 Redis...")
        
        # 等待关闭信号
        await shutdown_event.wait()
        
        log.info("⏳ 正在关闭所有任务 (WebSocket / Heartbeat)...")

    except Exception as e:
        log.critical(f"💥 [V3 DataFeed] 发生未捕获的严重错误: {e}", exc_info=True)
    
    finally:
        log.info("🧹 正在清理资源...")
        
        # 1. (安全措施) 确保关闭事件被设置
        shutdown_event.set()
        
        # 2. 取消所有正在运行的任务 (WS, Heartbeat)
        for task in tasks:
            if not task.done():
                task.cancel()
        
        # 等待任务取消完成
        await asyncio.gather(*tasks, return_exceptions=True) 
        
        # 3. 关闭所有交易所连接
        for name, connector in connectors.items():
            await connector.close_connection()
            log.info(f"[{name}] 连接已关闭。")
            
        # 4. 关闭 Redis
        await redis_client.close()
        
        log.info("👋 [V3 DataFeed] 服务已安全关闭。")


# =========================================================================
#  程序入口
# =========================================================================
if __name__ == "__main__":
    # 
    # 
    # (在 V3 架构中, 这个文件将作为独立进程运行)
    # (例如: docker-compose.yml 中的 command: ["python", "services/data_feed/main.py"])
    # 
    asyncio.run(main())