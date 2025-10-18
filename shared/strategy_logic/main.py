# -------------------------------------------------------------------------
# 📁 services/strategy_logic/main.py
# (V3.1 - 增加了 InfluxDB 写入功能)
# -------------------------------------------------------------------------
import asyncio
import logging
import signal
import time
from typing import Dict, Literal, List
from pydantic import BaseModel, Field

# 导入共享基础
from shared.config import settings
from shared.connectors import Ticker, Order # 导入标准模型

# 导入数据存储 (Redis & InfluxDB)
from datastore.redis_client import (
    redis_client, 
    CH_MARKET_DATA,     # (Sub) 订阅行情
    CH_TRADE_SIGNALS,   # (Pub) 发布信号
    CH_EXEC_REPORTS,    # (Sub) 订阅成交回报
    KEY_CURRENT_POSITIONS # (State) 读/写持久化仓位
)
# ❗️ [新] 导入 InfluxDB 客户端
from datastore.influx_client import influx_client


# =========================================================================
#  日志配置
# =========================================================================
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] [%(name)s] - %(message)s",
    datefmt="%Y-m-%d %H:M:S"
)
log = logging.getLogger("StrategyLogicService")

# =========================================================================
#  🔌 (V3) 本地 Pydantic 模型
# =========================================================================

class TradeSignal(BaseModel):
    """
    (V3) 交易信号模型 (发布到 CH_TRADE_SIGNALS)
    """
    symbol: str
    action: Literal["OPEN", "CLOSE"]
    
    # (仅 OPEN 时需要)
    high_price_exchange: str | None = None
    high_price: float | None = None
    low_price_exchange: str | None = None
    low_price: float | None = None
    
    # (仅 CLOSE 时需要)
    position_details: Dict | None = None # (可选) 传递要平仓的仓位信息

# =========================================================================
#  🧠 策略状态机 (核心)
# =========================================================================

class StrategyEngine:
    """
    封装所有策略逻辑、状态和计算
    """
    
    def __init__(self):
        # 1. 交易所配置 (例如 ["binance", "bitget"])
        self.exchanges = settings.system.ENABLED_EXCHANGES
        
        # 2. 策略参数
        self.threshold_open = settings.strategy.trigger_threshold_pct
        self.threshold_close = settings.strategy.close_threshold_pct
        
        # 3. 🧠 状态 1: 最新市场快照
        # {"ETH/USDT": {"binance": Ticker, "bitget": Ticker}, ...}
        self.market_snapshot: Dict[str, Dict[str, Ticker]] = {}

        # 4. 🧠 状态 2: 当前仓位状态 (带内存锁)
        # {"ETH/USDT": "FLAT" | "PENDING_OPEN" | "OPEN" | "PENDING_CLOSE"}
        self.positions: Dict[str, str] = {}
        
        # 5. Ticker 新鲜度阈值 (例如 5 秒)
        self.max_ticker_age_sec = 5.0 

    async def load_initial_positions(self):
        """
        (V3 理念五) 启动时从 Redis 加载持久化仓位 (用于崩溃恢复)
        """
        log.info("💾 正在从 Redis 加载持久化仓位状态...")
        state = await redis_client.get_state(KEY_CURRENT_POSITIONS)
        
        if state:
            self.positions = state
            log.info(f"✅ 成功加载 {len(self.positions)} 个仓位: {self.positions}")
        else:
            # 如果 Redis 为空，为所有启用的币对初始化 "FLAT" 状态
            for symbol in settings.system.ENABLED_SYMBOLS:
                self.positions[symbol] = "FLAT"
            log.info("✅ 未找到持久化状态，已初始化所有币对为 'FLAT'。")

    # ---------------------------------------------------------------------
    #  (Sub) 订阅者 1: 处理行情
    # ---------------------------------------------------------------------
    async def on_market_data(self, message: Dict):
        """
        (Redis 回调) 收到来自 CH_MARKET_DATA 的新 Ticker
        """
        try:
            ticker = Ticker.model_validate(message)
            
            symbol = ticker.symbol
            exchange = ticker.exchange
            
            # 1. 初始化快照
            if symbol not in self.market_snapshot:
                self.market_snapshot[symbol] = {}
            
            # 2. 更新最新 Ticker
            self.market_snapshot[symbol][exchange] = ticker
            # log.debug(f"Snapshot updated: {symbol} on {exchange}")
            
            # 3. ❗️ 触发套利检查
            await self._check_arbitrage(symbol)
            
        except Exception as e:
            log.error(f"处理 Ticker 失败: {e} | 数据: {message}", exc_info=True)

    # ---------------------------------------------------------------------
    #  (Sub) 订阅者 2: 处理成交回报 (更新仓位锁)
    # ---------------------------------------------------------------------
    async def on_execution_report(self, message: Dict):
        """
        (Redis 回调) 收到来自 CH_EXEC_REPORTS 的成交回报
        
        用于解除 "PENDING_OPEN" / "PENDING_CLOSE" 内存锁
        """
        try:
            order = Order.model_validate(message)
            symbol = order.symbol
            status = order.status
            
            log.info(f"📋 收到成交回报: {symbol} 状态: {status} (来自 {order.exchange})")
            
            # ❗️ 关键: 更新本地仓位状态机 (内存锁)
            
            # (注意: 这是一个简化的锁，假设两个订单 (BUY/SELL) 状态一致)
            # (一个更复杂的锁会等待 *两个* 订单都确认)
            
            if status == "OPEN":
                if self.positions.get(symbol) == "PENDING_OPEN":
                    log.info(f"✅ [仓位解锁] {symbol} 确认为 OPEN。")
                    self.positions[symbol] = "OPEN"
            
            elif status in ["CLOSED", "FAILED", "CANCELED"]:
                if self.positions.get(symbol) == "PENDING_CLOSE" or self.positions.get(symbol) == "OPEN":
                     log.info(f"✅ [仓位解锁] {symbol} 确认为 FLAT。")
                     self.positions[symbol] = "FLAT"
                
                # (风控) 如果开仓失败
                elif self.positions.get(symbol) == "PENDING_OPEN":
                    log.warning(f"⚠️ [仓位解锁] {symbol} PENDING_OPEN 失败，回滚至 FLAT。")
                    self.positions[symbol] = "FLAT"

        except Exception as e:
            log.error(f"处理成交回报失败: {e} | 数据: {message}", exc_info=True)

    # ---------------------------------------------------------------------
    #  🎯 (V1) 核心决策逻辑
    # ---------------------------------------------------------------------
    async def _check_arbitrage(self, symbol: str):
        """
        (V1 决策触发器 & 平仓逻辑) 检查指定币对的套利机会
        """
        
        # 1. 检查仓位状态 (内存锁)
        position_status = self.positions.get(symbol, "FLAT")
        
        # 2. 获取快照
        snapshot = self.market_snapshot.get(symbol, {})
        if len(snapshot) < len(self.exchanges):
            # log.debug(f"[{symbol}] 快照不完整，等待所有交易所数据...")
            return # 数据不完整，无法比较

        # 3. 检查数据新鲜度
        current_time_ms = int(time.time() * 1000)
        fresh_tickers = []
        for ex, ticker in snapshot.items():
            age_sec = (current_time_ms - ticker.timestamp) / 1000.0
            if age_sec > self.max_ticker_age_sec:
                # log.warning(f"[{symbol}] {ex} Ticker 已过期 ({age_sec:.1f}s)，跳过检查。")
                return # 存在过期数据，跳过
            fresh_tickers.append(ticker)

        # (简化: 假设只有2个交易所)
        if len(fresh_tickers) != 2:
            log.warning("策略引擎目前只支持2个交易所的比较")
            return
            
        t_a = fresh_tickers[0]
        t_b = fresh_tickers[1]

        # 4. 计算价差
        # 机会 1: A 卖 > B 买 (在 A 卖, 在 B 买)
        spread_ab = (t_a.bid_price - t_b.ask_price) / t_b.ask_price
        # 机会 2: B 卖 > A 买 (在 B 卖, 在 A 买)
        spread_ba = (t_b.bid_price - t_a.ask_price) / t_a.ask_price
        
        # -------------------------------------------------------------
        # ❗️ [新] 写入 InfluxDB (V3 可视化)
        # -------------------------------------------------------------
        # 使用 create_task 异步执行，不阻塞核心逻辑
        asyncio.create_task(influx_client.write(
            "spreads",  # (Measurement: "spreads" 表)
            tags={"symbol": symbol},
            fields={
                f"spread_{t_a.exchange}_ask_{t_b.exchange}_bid": spread_ba,
                f"spread_{t_b.exchange}_ask_{t_a.exchange}_bid": spread_ab,
                "threshold_open": self.threshold_open # (0.0028)
            }
        ))
        # -------------------------------------------------------------

        # 5. 🧠 (V1) 决策触发器
        
        # --- 开仓逻辑 ---
        if position_status == "FLAT":
            best_spread = max(spread_ab, spread_ba)
            
            if best_spread > self.threshold_open:
                # ❗️ 触发开仓
                
                # 1. 确定方向
                if spread_ab > spread_ba: # A 卖, B 买
                    high_ex, high_p = t_a.exchange, t_a.bid_price
                    low_ex, low_p = t_b.exchange, t_b.ask_price
                else: # B 卖, A 买
                    high_ex, high_p = t_b.exchange, t_b.bid_price
                    low_ex, low_p = t_a.exchange, t_a.ask_price
                
                # 2. ❗️ 设置内存锁 (防止重复开仓)
                self.positions[symbol] = "PENDING_OPEN"
                
                # 3. 构造信号
                signal = TradeSignal(
                    symbol=symbol,
                    action="OPEN",
                    high_price_exchange=high_ex,
                    high_price=high_p,
                    low_price_exchange=low_ex,
                    low_price=low_p
                )
                
                # 4. 🚀 (V3) 发布开仓信号
                await redis_client.publish(CH_TRADE_SIGNALS, signal)
                
                log.warning(
                    f"🎯 [开仓信号] {symbol} 价差 {best_spread*100:.4f}% > {self.threshold_open*100:.4f}%."
                    f" 卖 @ {high_ex} ({high_p}) | 买 @ {low_ex} ({low_p})"
                )
                
        # --- 平仓逻辑 ---
        elif position_status == "OPEN":
            if abs(spread_ab) < self.threshold_close or abs(spread_ba) < self.threshold_close:
                # ❗️ 触发平仓
                
                # 1. ❗️ 设置内存锁
                self.positions[symbol] = "PENDING_CLOSE"
                
                # 2. 构造信号
                signal = TradeSignal(
                    symbol=symbol,
                    action="CLOSE",
                    position_details={"info": "close arbitrage position"}
                )
                
                # 3. 🚀 (V3) 发布平仓信号
                await redis_client.publish(CH_TRADE_SIGNALS, signal)
                
                log.info(
                    f"✅ [平仓信号] {symbol} 价差回归 < {self.threshold_close*100:.4f}%. 信号已发送。"
                )

# =========================================================================
#  (V3) 服务封装
# =========================================================================
class Service:
    def __init__(self):
        self.engine = StrategyEngine()
        self.tasks: List[asyncio.Task] = []
        self.shutdown_event = asyncio.Event()

    def handle_shutdown(self):
        log.warning("🔌 收到关闭信号... 正在设置关闭事件。")
        self.shutdown_event.set()

    async def run_heartbeat(self):
        log.info("❤️ 心跳服务已启动 (10s 间隔)")
        while not self.shutdown_event.is_set():
            try:
                await redis_client.set_heartbeat("strategy_logic")
                await asyncio.wait_for(self.shutdown_event.wait(), timeout=10)
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                log.error(f"❤️ 心跳服务出错: {e}", exc_info=True)
                await asyncio.sleep(10)

    async def start(self):
        log.info("🚀 [V3 StrategyLogic] 微服务启动中...")
        
        loop = asyncio.get_running_loop()
        loop.add_signal_handler(signal.SIGINT, self.handle_shutdown)
        loop.add_signal_handler(signal.SIGTERM, self.handle_shutdown)
        
        # ❗️ [新] 连接所有数据库
        await redis_client.connect()
        await influx_client.connect()
        
        await self.engine.load_initial_positions() # ❗️ 崩溃恢复

        # 1. 启动心跳
        self.tasks.append(asyncio.create_task(self.run_heartbeat()))
        
        # 2. 启动 Redis 订阅者 1 (行情)
        self.tasks.append(asyncio.create_task(
            redis_client.subscribe(CH_MARKET_DATA, self.engine.on_market_data)
        ))
        
        # 3. 启动 Redis 订阅者 2 (成交回报)
        self.tasks.append(asyncio.create_task(
            redis_client.subscribe(CH_EXEC_REPORTS, self.engine.on_execution_report)
        ))
        
        log.info("✅ [V3 StrategyLogic] 服务已启动。正在监听行情、成交回报，并写入价差...")
        await self.shutdown_event.wait()
        
    async def stop(self):
        log.info("🧹 正在清理资源...")
        self.shutdown_event.set() # 确保所有循环退出
        
        for task in self.tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        
        # (重要) 持久化 V3 状态
        log.info(f"💾 正在持久化最后仓位状态到 Redis: {self.engine.positions}")
        await redis_client.set_state(KEY_CURRENT_POSITIONS, self.engine.positions)
        
        # ❗️ [新] 关闭所有数据库
        await redis_client.close()
        await influx_client.close()
        
        log.info("👋 [V3 StrategyLogic] 服务已安全关闭。")

# =========================================================================
#  程序入口
# =========================================================================
if __name__ == "__main__":
    service = Service()
    try:
        asyncio.run(service.start())
    except KeyboardInterrupt:
        log.info("收到 KeyboardInterrupt")
    finally:
        # 确保在 asyncio.run() 之外调用 stop() (如果 start() 异常退出)
        # (在实际运行中，asyncio.run() 会等待 service.start() 完成)
        asyncio.run(service.stop())