# -------------------------------------------------------------------------
# 📁 services/execution_engine/main.py
# (V3 架构的第三个微服务：交易执行与风控中心)
#
# 运行依赖:
# 1. 确保已安装: pip install redis aiohttp websockets pydantic pydantic-settings
# 2. 确保 Redis 服务器正在运行
# 3. 确保 'shared' 和 'datastore' 目录在 Python 路径中
#
# 启动命令 (在项目根目录): python services/execution_engine/main.py
# -------------------------------------------------------------------------
import asyncio
import logging
import signal
import time
import random
from typing import Dict, List, Tuple

# 导入共享基础
from shared.config import settings
from shared.connectors import BaseConnector, Ticker, Order, create_connectors

# 导入 V3 策略逻辑中的信号模型 (用于反序列化)
# (更好的做法是把 TradeSignal 也移到 shared/models.py)
try:
    from services.strategy_logic.main import TradeSignal
except ImportError:
    # 临时 Pydantic 模型 (如果共享模型未设置)
    from pydantic import BaseModel, Literal
    class TradeSignal(BaseModel):
        symbol: str
        action: Literal["OPEN", "CLOSE"]
        high_price_exchange: str | None = None
        low_price_exchange: str | None = None
        # ... (其他字段)

# 导入数据存储 (Redis)
from datastore.redis_client import (
    redis_client, 
    CH_TRADE_SIGNALS,   # (Sub) 订阅交易信号
    CH_EXEC_REPORTS,    # (Pub) 发布成交回报
    CH_RISK_ALERTS,     # (Pub) 发布风控警报
    KEY_CURRENT_POSITIONS # (State) 读/写持久化仓位
)

# =========================================================================
#  日志配置
# =========================================================================
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] [%(name)s] - %(message)s",
    datefmt="%Y-%m-%d %H:M:S"
)
log = logging.getLogger("ExecutionEngineService")

# =========================================================================
#  💸 执行引擎 (核心)
# =========================================================================

class ExecutionEngine:
    """
    封装所有交易执行、模式切换、状态持久化和风控逻辑
    """
    
    def __init__(self):
        # 1. 🧩 模拟/实盘模式
        self.mode = settings.system.EXECUTION_MODE
        
        # 2. 交易所连接器实例 (例如 {"binance": BinanceConnector, ...})
        self.connectors: Dict[str, BaseConnector] = {}
        
        # 3. 策略配置
        self.slippage_buffer = settings.strategy.slippage_buffer_pct
        
        # 4. 仓位锁 (防止在处理一个信号时，又收到另一个信号)
        # (V3 架构中，strategy_logic 的 "PENDING" 状态是主锁)
        # (这里我们添加一个本地锁作为双重保险)
        self.local_lock = asyncio.Lock()
        
        log.info(f"🚀 执行引擎已初始化。🧩 运行模式: {self.mode}")

    def load_connectors(self):
        """
        加载所有启用的交易所连接器 (V3 工厂)
        """
        if not self.connectors:
            self.connectors = create_connectors()
            if not self.connectors:
                raise RuntimeError("执行引擎启动失败: 无法加载任何交易所连接器。")
            log.info(f"已加载 {len(self.connectors)} 个连接器: {list(self.connectors.keys())}")

    # ---------------------------------------------------------------------
    #  (Sub) 订阅者: 处理交易信号
    # ---------------------------------------------------------------------
    
    async def on_trade_signal(self, message: Dict):
        """
        (Redis 回调) 收到来自 CH_TRADE_SIGNALS 的新信号 (OPEN / CLOSE)
        """
        
        # 1. 尝试获取本地锁
        if self.local_lock.locked():
            log.warning(f"本地锁已被占用，跳过信号: {message.get('symbol')} {message.get('action')}")
            return
            
        async with self.local_lock:
            try:
                signal = TradeSignal.model_validate(message)
                log.info(f"🛒 收到交易信号: {signal.symbol} {signal.action}")
                
                if signal.action == "OPEN":
                    await self._handle_open_signal(signal)
                
                elif signal.action == "CLOSE":
                    await self._handle_close_signal(signal)
                    
            except Exception as e:
                log.critical(f"💥 处理交易信号时发生严重错误: {e} | 信号: {message}", exc_info=True)
                # (可选) 发布风控警报
                await redis_client.publish(
                    CH_RISK_ALERTS, 
                    {"error": "ExecutionEngine Error", "details": str(e)}
                )

    # ---------------------------------------------------------------------
    #  (V1) 开仓逻辑
    # ---------------------------------------------------------------------

    async def _handle_open_signal(self, signal: TradeSignal):
        """
        处理 "OPEN" 信号：并发执行双边下单 (V1 执行引擎)
        """
        
        # (简化) 假设固定交易量
        # ❗️ 专业的做法是基于余额、风险敞口和价格计算交易量
        trade_qty = 0.01 # 假设交易 0.01 ETH/SOL
        
        # 1. 获取连接器
        sell_ex_name = signal.high_price_exchange
        buy_ex_name = signal.low_price_exchange
        sell_connector = self.connectors.get(sell_ex_name)
        buy_connector = self.connectors.get(buy_ex_name)
        
        if not sell_connector or not buy_connector:
            log.error(f"[{signal.symbol}] 找不到开仓所需的连接器: {sell_ex_name} / {buy_ex_name}")
            return

        # 2. 🧩 根据模式选择执行函数
        if self.mode == "PAPER":
            executor_func = self._execute_paper_trade
        else:
            executor_func = self._execute_live_trade
            
        # 3. ❗️ (V1) 并发执行 (几乎同时下单)
        log.info(f"[{signal.symbol}] 正在并发执行开仓: [SELL @ {sell_ex_name}] [BUY @ {buy_ex_name}]")
        
        (order_sell, order_buy) = await asyncio.gather(
            # 任务 A: 卖 (高价所)
            executor_func(
                sell_connector, signal.symbol, "SELL", trade_qty
            ),
            # 任务 B: 买 (低价所)
            executor_func(
                buy_connector, signal.symbol, "BUY", trade_qty
            ),
            return_exceptions=True
        )

        # 4. 🚀 (V3) 发布成交回报 (无论成功与否)
        # (这对于 strategy_logic 解除 "PENDING" 锁至关重要)
        if isinstance(order_sell, Order):
            await redis_client.publish(CH_EXEC_REPORTS, order_sell)
        if isinstance(order_buy, Order):
            await redis_client.publish(CH_EXEC_REPORTS, order_buy)
            
        # 5. 🛡️ (V3) 风控检查 (处理单边成交)
        is_safe = await self._handle_leg_risk_scavenger(
            signal.symbol, 
            order_sell, 
            order_buy,
            (sell_connector, "SELL"), # (用于清道夫反向平仓)
            (buy_connector, "BUY")    # (用于清道夫反向平仓)
        )
        
        # 6. 💾 (V3) 持久化状态
        if is_safe:
            log.info(f"[{signal.symbol}] 双边开仓成功。正在持久化状态到 Redis...")
            position_data = {
                "status": "OPEN",
                "symbol": signal.symbol,
                "qty": trade_qty,
                "high_ex": sell_ex_name, # (SELL @ high_ex)
                "low_ex": buy_ex_name,   # (BUY @ low_ex)
                "open_time": int(time.time()),
                "open_price_sell": order_sell.price,
                "open_price_buy": order_buy.price
            }
            await self._update_persistent_positions(signal.symbol, position_data)

    # ---------------------------------------------------------------------
    #  (V1) 平仓逻辑
    # ---------------------------------------------------------------------
    
    async def _handle_close_signal(self, signal: TradeSignal):
        """
        处理 "CLOSE" 信号：从 Redis 读取持仓，执行反向并发下单
        """
        
        # 1. 💾 (V3) 从 Redis 读取当前持仓
        positions = await redis_client.get_state(KEY_CURRENT_POSITIONS)
        position_data = positions.get(signal.symbol) if positions else None
        
        if not position_data or position_data.get("status") != "OPEN":
            log.warning(f"[{signal.symbol}] 收到平仓信号，但在 Redis 中无 OPEN 仓位。跳过。")
            return
            
        # 2. 确定反向操作
        # (开仓 = SELL @ high_ex, BUY @ low_ex)
        # (平仓 = BUY @ high_ex, SELL @ low_ex)
        buy_ex_name = position_data["high_ex"]
        sell_ex_name = position_data["low_ex"]
        trade_qty = position_data["qty"]
        
        buy_connector = self.connectors.get(buy_ex_name)
        sell_connector = self.connectors.get(sell_ex_name)

        if not sell_connector or not buy_connector:
            log.error(f"[{signal.symbol}] 找不到平仓所需的连接器: {sell_ex_name} / {buy_ex_name}")
            return
            
        # 3. 🧩 根据模式选择执行函数
        executor_func = self._execute_paper_trade if self.mode == "PAPER" else self._execute_live_trade
            
        # 4. ❗️ (V1) 并发执行 (平仓)
        log.info(f"[{signal.symbol}] 正在并发执行平仓: [BUY @ {buy_ex_name}] [SELL @ {sell_ex_name}]")
        
        (order_buy, order_sell) = await asyncio.gather(
            # 任务 A: 买 (原高价所)
            executor_func(
                buy_connector, signal.symbol, "BUY", trade_qty
            ),
            # 任务 B: 卖 (原低价所)
            executor_func(
                sell_connector, signal.symbol, "SELL", trade_qty
            ),
            return_exceptions=True
        )
        
        # 5. 🚀 (V3) 发布成交回报
        if isinstance(order_buy, Order):
            await redis_client.publish(CH_EXEC_REPORTS, order_buy)
        if isinstance(order_sell, Order):
            await redis_client.publish(CH_EXEC_REPORTS, order_sell)

        # 6. 🛡️ (V3) 风控检查 (清道夫)
        is_safe = await self._handle_leg_risk_scavenger(
            signal.symbol,
            order_buy,
            order_sell,
            (buy_connector, "BUY"),
            (sell_connector, "SELL")
        )

        # 7. 💾 (V3) 持久化状态
        if is_safe:
            log.info(f"[{signal.symbol}] 双边平仓成功。正在更新 Redis 状态为 FLAT...")
            await self._update_persistent_positions(signal.symbol, {"status": "FLAT"})

    # ---------------------------------------------------------------------
    #  🧩 (V2) 模拟/实盘执行器
    # ---------------------------------------------------------------------

    async def _execute_live_trade(self, connector: BaseConnector, symbol: str, side: Literal["BUY", "SELL"], qty: float) -> Order:
        """
        (LIVE) 真实执行器: 调用交易所 API
        """
        log.info(f"[{connector.exchange_name}] (LIVE) 正在下单: {side} {qty} {symbol}")
        
        # ❗️ 关键: 我们使用市价单 (MARKET) 以确保立即成交
        # (这符合 V1 策略的“市价单或限价单 (±0.01%)”要求)
        try:
            order = await connector.place_order(
                symbol=symbol,
                side=side,
                order_type="MARKET",
                qty=qty,
                price=None
            )
            log.info(f"[{connector.exchange_name}] (LIVE) 下单回报: {order.status} | ID: {order.id} | Price: {order.price}")
            return order
            
        except Exception as e:
            log.critical(f"[{connector.exchange_name}] (LIVE) 下单时发生严重 API 错误: {e}", exc_info=True)
            return self._create_failed_order(connector.exchange_name, symbol, side, qty)


    async def _execute_paper_trade(self, connector: BaseConnector, symbol: str, side: Literal["BUY", "SELL"], qty: float) -> Order:
        """
        (PAPER) 模拟执行器: 模拟成交和滑点
        """
        log.info(f"[{connector.exchange_name}] (PAPER) 正在模拟下单: {side} {qty} {symbol}")
        
        # 1. 模拟网络延迟
        await asyncio.sleep(random.uniform(0.05, 0.1)) # 50-100ms
        
        # 2. 模拟成交价格 (假设我们能获取到最新 Ticker，但执行引擎不订阅 Ticker)
        # (简化) 我们假设成交价 = 信号价 ± 滑点
        # (一个更真实的模拟盘会订阅 CH_MARKET_DATA)
        
        # 3. ❗️ 模拟 0.03% 滑点
        # (我们这里简化，假设一定成交，价格在 strategy_logic 已计算)
        
        # 4. 模拟 1% 的概率失败
        if random.random() < 0.01: # 1% 概率失败
             log.warning(f"[{connector.exchange_name}] (PAPER) 模拟下单失败 (随机)")
             return self._create_failed_order(connector.exchange_name, symbol, side, qty)

        # 5. 返回成功的模拟订单
        return Order(
            id=f"PAPER_{int(time.time()*100000 + random.randint(0, 99))}",
            symbol=symbol,
            exchange=connector.exchange_name,
            type="MARKET",
            side=side,
            status="CLOSED", # 模拟市价单立即成交
            price=123.45, # ❗️ 模拟价 (应从 Ticker 获取)
            qty=qty,
            timestamp=int(time.time() * 1000)
        )

    # ---------------------------------------------------------------------
    #  🛡️ (V3) 风控模块: 清道夫 (Scavenger)
    # ---------------------------------------------------------------------
    
    async def _handle_leg_risk_scavenger(
        self, 
        symbol: str, 
        order_a: Order | Exception, 
        order_b: Order | Exception,
        leg_a_info: Tuple[BaseConnector, Literal["BUY", "SELL"]],
        leg_b_info: Tuple[BaseConnector, Literal["BUY", "SELL"]]
    ) -> bool:
        """
        (V3 理念三) 检查并发执行的结果，处理单边成交风险。
        返回 True (安全) 或 False (风险已触发)。
        """
        
        status_a = self._get_order_status(order_a)
        status_b = self._get_order_status(order_b)

        # 情况 1: 两边都成功 (或都失败) -> 安全
        if (status_a == "SUCCESS" and status_b == "SUCCESS"):
            log.info(f"🛡️ [风控] {symbol} 双边成交成功。状态安全。")
            return True
        if (status_a == "FAILED" and status_b == "FAILED"):
            log.warning(f"🛡️ [风控] {symbol} 双边成交均失败。状态安全 (FLAT)。")
            return True # 失败了，但仓位是平的，所以也是“安全”的
            
        # 情况 2: ❗️ 发生单边成交风险 (Leg Risk)
        log.critical(f"💥 [风控-清道夫] {symbol} 触发单边成交风险！")
        log.critical(f"💥 Leg A ({leg_a_info[0].exchange_name}) 状态: {status_a}")
        log.critical(f"💥 Leg B ({leg_b_info[0].exchange_name}) 状态: {status_b}")
        
        # 🚀 发布风控警报
        await redis_client.publish(
            CH_RISK_ALERTS, 
            {"error": "LEG_RISK_DETECTED", "symbol": symbol, "status_a": status_a, "status_b": status_b}
        )

        # ❗️ 启动清道夫 (Scavenger): 不惜一切代价平掉已成交的单边
        if status_a == "SUCCESS":
            # A 成功了，B 失败了 -> 立即反向平掉 A
            connector, side = leg_a_info
            reverse_side = "BUY" if side == "SELL" else "SELL"
            log.critical(f"🧹 [清道夫] 正在市价平仓 (Leg A): {connector.exchange_name} {reverse_side} {order_a.qty}")
            await self._execute_live_trade(connector, symbol, reverse_side, order_a.qty) # ❗️ 总是用 Live 模式执行风控
        
        if status_b == "SUCCESS":
            # B 成功了，A 失败了 -> 立即反向平掉 B
            connector, side = leg_b_info
            reverse_side = "BUY" if side == "SELL" else "SELL"
            log.critical(f"🧹 [清道夫] 正在市价平仓 (Leg B): {connector.exchange_name} {reverse_side} {order_b.qty}")
            await self._execute_live_trade(connector, symbol, reverse_side, order_b.qty) # ❗️ 总是用 Live 模式执行风控

        # (V3 持久化) 确保状态被设为 FLAT (因为清道夫已介入)
        await self._update_persistent_positions(symbol, {"status": "FLAT"})

        return False # 风险已触发，状态不安全 (不允许继续持久化 OPEN 状态)

    # ---------------------------------------------------------------------
    #  辅助函数
    # ---------------------------------------------------------------------

    def _get_order_status(self, order: Order | Exception) -> Literal["SUCCESS", "FAILED"]:
        """
        辅助函数：将 Order 或 Exception 转换为风控状态
        """
        if isinstance(order, Exception):
            return "FAILED"
        # ❗️ "OPEN" (限价单) 或 "CLOSED" (市价单) 都算成功
        if order.status in ["OPEN", "CLOSED"]: 
            return "SUCCESS"
        return "FAILED"
        
    def _create_failed_order(self, exchange: str, symbol: str, side: Literal["BUY", "SELL"], qty: float) -> Order:
        """
        辅助函数：创建标准 FAILED 订单
        """
        return Order(
            id=f"FAILED_{int(time.time()*1000)}",
            symbol=symbol, exchange=exchange, type="MARKET",
            side=side, status="FAILED", price=0.0, qty=qty,
            timestamp=int(time.time() * 1000)
        )

    async def _update_persistent_positions(self, symbol: str, data: Dict):
        """
        辅助函数：更新 Redis 中的持久化仓位
        """
        current_state = await redis_client.get_state(KEY_CURRENT_POSITIONS) or {}
        
        if data.get("status") == "FLAT":
            if symbol in current_state:
                del current_state[symbol]
                log.info(f"💾 [Redis 持久化] {symbol} 状态更新为 FLAT (已移除)。")
        else:
            current_state[symbol] = data
            log.info(f"💾 [Redis 持久化] {symbol} 状态更新为 {data.get('status')}。")
            
        await redis_client.set_state(KEY_CURRENT_POSITIONS, current_state)

# =========================================================================
#  (V3) 服务封装
# =========================================================================
class Service:
    def __init__(self):
        self.engine = ExecutionEngine()
        self.tasks: List[asyncio.Task] = []
        self.shutdown_event = asyncio.Event()

    def handle_shutdown(self):
        log.warning("🔌 收到关闭信号... 正在设置关闭事件。")
        self.shutdown_event.set()

    async def run_heartbeat(self):
        log.info("❤️ 心跳服务已启动 (10s 间隔)")
        while not self.shutdown_event.is_set():
            try:
                await redis_client.set_heartbeat("execution_engine")
                await asyncio.wait_for(self.shutdown_event.wait(), timeout=10)
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                log.error(f"❤️ 心跳服务出错: {e}", exc_info=True)
                await asyncio.sleep(10)

    async def start(self):
        log.info("🚀 [V3 ExecutionEngine] 微服务启动中...")
        
        loop = asyncio.get_running_loop()
        loop.add_signal_handler(signal.SIGINT, self.handle_shutdown)
        loop.add_signal_handler(signal.SIGTERM, self.handle_shutdown)
        
        await redis_client.connect()
        self.engine.load_connectors() # ❗️ 必须在订阅前加载

        # 1. 启动心跳
        self.tasks.append(asyncio.create_task(self.run_heartbeat()))
        
        # 2. 启动 Redis 订阅者 (交易信号)
        self.tasks.append(asyncio.create_task(
            redis_client.subscribe(CH_TRADE_SIGNALS, self.engine.on_trade_signal)
        ))
        
        log.info("✅ [V3 ExecutionEngine] 服务已启动。正在监听交易信号...")
        await self.shutdown_event.wait()
        
    async def stop(self):
        log.info("🧹 正在清理资源...")
        self.shutdown_event.set() 
        
        for task in self.tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        
        # ❗️ 执行引擎关闭时不需要持久化状态，因为状态是在每次交易后实时持久化的
        
        await redis_client.close()
        log.info("👋 [V3 ExecutionEngine] 服务已安全关闭。")

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
        asyncio.run(service.stop())