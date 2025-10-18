# -------------------------------------------------------------------------
# 📁 services/risk_manager/main.py
# (V3 架构的第四个微服务：全局风控、日志与监控中心)
#
# 运行依赖:
# 1. (所有 V3 依赖)
# 2. 确保 InfluxDB 服务器正在运行 (config.py 中配置)
#
# 启动命令 (在项目根目录): python services/risk_manager/main.py
# -------------------------------------------------------------------------
import asyncio
import logging
import signal
import time
from typing import Dict, List, Any

# 导入共享基础
from shared.config import settings
from shared.connectors import Ticker, Order, create_connectors, BaseConnector

# 导入 V3 策略逻辑中的信号模型 (用于发布平仓信号)
try:
    from services.strategy_logic.main import TradeSignal
except ImportError:
    from pydantic import BaseModel, Literal
    class TradeSignal(BaseModel):
        symbol: str
        action: Literal["OPEN", "CLOSE"]
        
# 导入数据存储 (Redis & InfluxDB)
from datastore.redis_client import (
    redis_client, 
    CH_MARKET_DATA,     # (Sub) 订阅行情 (用于 PnL)
    CH_EXEC_REPORTS,    # (Sub) 订阅成交 (用于日志)
    CH_RISK_ALERTS,     # (Sub/Pub) 订阅/发布警报
    CH_TRADE_SIGNALS,   # (Pub) 发布平仓信号 (止损)
    KEY_CURRENT_POSITIONS, # (State) 读持仓
    KEY_SERVICE_HEARTBEAT  # (State) 读心跳
)
from datastore.influx_client import influx_client

# =========================================================================
#  日志配置
# =========================================================================
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] [%(name)s] - %(message)s",
    datefmt="%Y-m-%d %H:M:S"
)
log = logging.getLogger("RiskManagerService")

# =========================================================================
#  🛡️ 风控引擎 (核心)
# =========================================================================

class RiskManagerEngine:
    """
    封装所有全局风控逻辑 (V1 策略要求)
    """
    
    def __init__(self):
        # 1. V1 策略参数
        self.stop_loss_pct = settings.strategy.stop_loss_pct # (例如 0.003)
        self.service_heartbeat_timeout = 30.0 # (例如 30s)
        
        # 2. 交易所连接器 (用于 V1 余额保护)
        self.connectors: Dict[str, BaseConnector] = {}

        # 3. 🧠 内部状态: 持仓
        # (由 _sync_positions_from_redis 任务 5s 刷新一次)
        self.positions: Dict[str, Dict[str, Any]] = {}
        
        # 4. 🧠 内部状态: 最新报价
        # {"binance:ETH/USDT": Ticker, ...}
        self.last_tickers: Dict[str, Ticker] = {}

    def load_connectors(self):
        """
        加载交易所连接器 (用于 get_balance)
        """
        if not self.connectors:
            self.connectors = create_connectors()
            log.info(f"已加载 {len(self.connectors)} 个连接器 (用于余额检查)")

    # ---------------------------------------------------------------------
    #  (Sub) 订阅者 1: 处理行情 -> (V1 止损线)
    # ---------------------------------------------------------------------
    async def on_market_data(self, message: Dict):
        """
        (Redis 回调) 收到来自 CH_MARKET_DATA 的新 Ticker
        """
        try:
            ticker = Ticker.model_validate(message)
            
            # 1. (V3 可视化) 写入 InfluxDB (可选，可能会产生海量数据)
            # await influx_client.write(
            #     "market_data",
            #     tags={"symbol": ticker.symbol, "exchange": ticker.exchange},
            #     fields={"bid": ticker.bid_price, "ask": ticker.ask_price}
            # )
            
            # 2. 更新最新报价板
            ticker_key = f"{ticker.exchange}:{ticker.symbol}"
            self.last_tickers[ticker_key] = ticker
            
            # 3. 检查是否有持仓
            symbol = ticker.symbol
            position = self.positions.get(symbol)
            
            # 4. ❗️ (V1 止损)
            if position and position.get("status") == "OPEN":
                # (为防止重复触发，我们检查是否有 "PENDING_CLOSE" 锁)
                if position.get("status_lock") != "PENDING_CLOSE":
                    await self._check_stop_loss(position)

        except Exception as e:
            log.error(f"处理 Ticker 失败: {e}", exc_info=True)

    # ---------------------------------------------------------------------
    #  (Sub) 订阅者 2: 处理成交回报 -> (V1/V3 日志)
    # ---------------------------------------------------------------------
    async def on_execution_report(self, message: Dict):
        """
        (Redis 回调) 收到来自 CH_EXEC_REPORTS 的成交回报
        """
        try:
            order = Order.model_validate(message)
            
            log.info(f"📋 [日志] 收到成交回报: {order.symbol} {order.side} @ {order.exchange} | 状态: {order.status}")

            # 1. 📊 (V3 可视化) 写入 InfluxDB
            await influx_client.write(
                "trades",
                tags={
                    "symbol": order.symbol,
                    "exchange": order.exchange,
                    "side": order.side,
                    "status": order.status,
                    "type": order.type
                },
                fields={
                    "price": order.price,
                    "qty": order.qty
                }
            )
        except Exception as e:
            log.error(f"处理成交回报失败: {e}", exc_info=True)
            
    # ---------------------------------------------------------------------
    #  (Sub) 订阅者 3: 处理风控警报 -> (V1/V3 日志)
    # ---------------------------------------------------------------------
    async def on_risk_alert(self, message: Dict):
        """
        (Redis 回调) 收到来自 CH_RISK_ALERTS 的警报 (例如 Leg Risk)
        """
        try:
            log.warning(f"🚨 [风控警报] 收到警报: {message}")
            
            # 1. 📊 (V3 可视化) 写入 InfluxDB
            alert_type = message.pop("error", "UNKNOWN")
            await influx_client.write(
                "risk_alerts",
                tags={"alert_type": alert_type, "service": message.get("service", "NA")},
                fields=message
            )
        except Exception as e:
            log.error(f"处理风控警报失败: {e}", exc_info=True)

    # ---------------------------------------------------------------------
    #  (Task) 任务 1: V1 止损线 (0.3%) 核心逻辑
    # ---------------------------------------------------------------------
    async def _check_stop_loss(self, position: Dict):
        """
        (V1 止损) 检查持仓的未实现盈亏 (PnL)
        """
        try:
            # 1. 解析持仓
            symbol = position["symbol"]
            high_ex = position["high_ex"] # (SELL @ high_ex)
            low_ex = position["low_ex"]   # (BUY @ low_ex)
            qty = position["qty"]
            open_price_sell = position["open_price_sell"] # 开仓时在 A 所的卖价
            open_price_buy = position["open_price_buy"]   # 开仓时在 B 所的买价

            # 2. 获取当前实时价格 (用于平仓)
            # (平仓 = BUY @ high_ex, SELL @ low_ex)
            current_buyback_price_ticker = self.last_tickers.get(f"{high_ex}:{symbol}")
            current_sell_price_ticker = self.last_tickers.get(f"{low_ex}:{symbol}")

            if not current_buyback_price_ticker or not current_sell_price_ticker:
                # log.debug(f"[{symbol}] (StopLoss) 价格快照不完整，跳过。")
                return

            # (平仓) 我们在 A 所 (high_ex) 买回 (ask)
            current_buyback_price = current_buyback_price_ticker.ask_price 
            # (平仓) 我们在 B 所 (low_ex) 卖出 (bid)
            current_sell_price = current_sell_price_ticker.bid_price
            
            # 3. 计算 PnL
            # A 腿 (空头): 利润 = (开仓卖价 - 当前买回价) * 数量
            pnl_leg_a = (open_price_sell - current_buyback_price) * qty
            # B 腿 (多头): 利润 = (当前卖出价 - 开仓买价) * 数量
            pnl_leg_b = (current_sell_price - open_price_buy) * qty
            
            total_pnl_usd = pnl_leg_a + pnl_leg_b
            
            # 4. 计算 PnL 百分比 (基于开仓名义价值)
            open_nominal_value = (open_price_sell + open_price_buy) / 2.0 * qty
            if open_nominal_value == 0:
                return # 避免除零

            pnl_pct = total_pnl_usd / open_nominal_value
            
            log.debug(f"[{symbol}] PnL Check: {total_pnl_usd:.2f} USD ({pnl_pct*100:.4f}%)")

            # 5. 🛑 触发止损
            if pnl_pct < -self.stop_loss_pct:
                log.critical(
                    f"🛑 [STOP LOSS HIT] {symbol} PnL {pnl_pct*100:.4f}% < "
                    f"{-self.stop_loss_pct*100:.4f}%. 强制平仓！"
                )
                
                # ❗️ 设置本地锁，防止重复发送
                self.positions[symbol]["status_lock"] = "PENDING_CLOSE"
                
                # 🚀 (V3) 发布风控警报
                await redis_client.publish(
                    CH_RISK_ALERTS, 
                    {"error": "STOP_LOSS_HIT", "symbol": symbol, "pnl_pct": pnl_pct}
                )
                
                # 🚀 (V3) 发布强制平仓信号
                await redis_client.publish(
                    CH_TRADE_SIGNALS, 
                    TradeSignal(symbol=symbol, action="CLOSE")
                )

        except Exception as e:
            log.error(f"StopLoss 检查失败: {e}", exc_info=True)

    # ---------------------------------------------------------------------
    #  (Task) 任务 2: 同步 Redis 持仓 (V3)
    # ---------------------------------------------------------------------
    async def _sync_positions_from_redis(self, shutdown_event: asyncio.Event):
        """
        (V3) 定期 (5s) 从 Redis (KEY_CURRENT_POSITIONS) 同步仓位到本地内存
        """
        log.info("💾 启动 Redis 持仓同步任务 (5s 间隔)...")
        while not shutdown_event.is_set():
            try:
                state = await redis_client.get_state(KEY_CURRENT_POSITIONS)
                self.positions = state if state else {}
                log.debug(f"💾 持仓已同步: {self.positions}")
                
                await asyncio.wait_for(shutdown_event.wait(), timeout=5.0)
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                log.error(f"💾 同步 Redis 持仓失败: {e}", exc_info=True)
                await asyncio.sleep(5.0)

    # ---------------------------------------------------------------------
    #  (Task) 任务 3: V1 断连保护 (心跳检查)
    # ---------------------------------------------------------------------
    async def run_heartbeat_check(self, shutdown_event: asyncio.Event):
        """
        (V1 断连保护) 定期 (10s) 检查其他微服务的心跳
        """
        log.info("❤️ 启动服务心跳检查任务 (10s 间隔)...")
        services_to_check = ["data_feed", "strategy_logic", "execution_engine"]
        
        while not shutdown_event.is_set():
            try:
                if not redis_client.redis_conn: # 等待 Redis 连接
                    await asyncio.sleep(1)
                    continue

                heartbeats = await redis_client.redis_conn.hgetall(KEY_SERVICE_HEARTBEAT)
                current_time = int(time.time())

                for service in services_to_check:
                    last_beat_str = heartbeats.get(service)
                    
                    if last_beat_str is None:
                        log.warning(f"❤️ [HEARTBEAT] 服务 {service} 从未启动或失联。")
                        continue # 第一次启动时可能还没有
                        
                    age = current_time - int(last_beat_str)
                    
                    if age > self.service_heartbeat_timeout:
                        log.critical(
                            f"❤️ [HEARTBEAT FAILURE] 服务 {service} 已失联 {age}s！ (阈值: {self.service_heartbeat_timeout}s)"
                        )
                        await redis_client.publish(
                            CH_RISK_ALERTS, 
                            {"error": "HEARTBEAT_FAILURE", "service": service, "age": age}
                        )
                        # (V1 策略: "暂停交易" - V3 中通过发布警报来实现)

                await asyncio.wait_for(shutdown_event.wait(), timeout=10.0)
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                log.error(f"❤️ 心跳检查失败: {e}", exc_info=True)
                await asyncio.sleep(10.0)

    # ---------------------------------------------------------------------
    #  (Task) 任务 4: V1 余额保护 (定时检查)
    # ---------------------------------------------------------------------
    async def run_balance_check(self, shutdown_event: asyncio.Event):
        """
        (V1 余额保护) 定期 (5分钟) 检查所有交易所的余额
        """
        log.info("💰 启动交易所余额检查任务 (5分钟 间隔)...")
        assets_to_check = ["USDT", "ETH", "SOL"] # (从 config 加载更好)
        
        while not shutdown_event.is_set():
            try:
                log.info("💰 正在执行余额检查...")
                all_balances_fields = {}
                
                for ex_name, connector in self.connectors.items():
                    for asset in assets_to_check:
                        balance = await connector.get_balance(asset)
                        log.info(f"💰 [{ex_name}] {asset} 余额: {balance}")
                        all_balances_fields[f"{ex_name}_{asset}_balance"] = balance
                
                # 1. 📊 (V3 可视化) 写入 InfluxDB
                await influx_client.write(
                    "account_balances",
                    tags={"account": "arbitrage_main"},
                    fields=all_balances_fields
                )
                
                # 2. ❗️ (V1) 检查不对称性 (简化)
                eth_bal = [v for k, v in all_balances_fields.items() if "ETH_balance" in k]
                if eth_bal and (max(eth_bal) - min(eth_bal) > 0.05): # 差异 > 0.05 ETH
                    log.warning(f"💰 [BALANCE ASYMMETRY] ETH 余额不对称: {eth_bal}")
                    await redis_client.publish(
                        CH_RISK_ALERTS, 
                        {"error": "BALANCE_ASYMMETRY", "asset": "ETH", "balances": str(eth_bal)}
                    )

                await asyncio.wait_for(shutdown_event.wait(), timeout=300.0) # 5 分钟
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                log.error(f"💰 余额检查失败: {e}", exc_info=True)
                await asyncio.sleep(60.0) # 出错，1分钟后重试


# =========================================================================
#  (V3) 服务封装
# =========================================================================
class Service:
    def __init__(self):
        self.engine = RiskManagerEngine()
        self.tasks: List[asyncio.Task] = []
        self.shutdown_event = asyncio.Event()

    def handle_shutdown(self):
        log.warning("🔌 收到关闭信号... 正在设置关闭事件。")
        self.shutdown_event.set()

    async def run_heartbeat(self):
        log.info("❤️ (RiskManager) 心跳服务已启动 (10s 间隔)")
        while not self.shutdown_event.is_set():
            try:
                await redis_client.set_heartbeat("risk_manager")
                await asyncio.wait_for(self.shutdown_event.wait(), timeout=10)
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                log.error(f"❤️ (RiskManager) 心跳服务出错: {e}", exc_info=True)
                await asyncio.sleep(10)

    async def start(self):
        log.info("🚀 [V3 RiskManager] 微服务启动中...")
        
        loop = asyncio.get_running_loop()
        loop.add_signal_handler(signal.SIGINT, self.handle_shutdown)
        loop.add_signal_handler(signal.SIGTERM, self.handle_shutdown)
        
        # 1. 连接数据库
        await redis_client.connect()
        await influx_client.connect() # ❗️ 连接 InfluxDB
        
        # 2. 加载 API 连接器
        self.engine.load_connectors()

        # 3. 启动所有监控任务
        self.tasks = [
            # 自身心跳
            asyncio.create_task(self.run_heartbeat()),
            
            # (Task) V3 仓位同步
            asyncio.create_task(self.engine._sync_positions_from_redis(self.shutdown_event)),
            # (Task) V1 断连保护
            asyncio.create_task(self.engine.run_heartbeat_check(self.shutdown_event)),
            # (Task) V1 余额保护
            asyncio.create_task(self.engine.run_balance_check(self.shutdown_event)),
            
            # (Sub) V1 止损线
            asyncio.create_task(
                redis_client.subscribe(CH_MARKET_DATA, self.engine.on_market_data)
            ),
            # (Sub) V1/V3 日志
            asyncio.create_task(
                redis_client.subscribe(CH_EXEC_REPORTS, self.engine.on_execution_report)
            ),
            # (Sub) V3 日志
            asyncio.create_task(
                redis_client.subscribe(CH_RISK_ALERTS, self.engine.on_risk_alert)
            )
        ]
        
        log.info("✅ [V3 RiskManager] 服务已启动。正在监控所有系统活动...")
        await self.shutdown_event.wait()
        
    async def stop(self):
        log.info("🧹 正在清理资源...")
        self.shutdown_event.set() 
        
        for task in self.tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        
        await redis_client.close()
        await influx_client.close() # ❗️ 关闭 InfluxDB
        
        log.info("👋 [V3 RiskManager] 服务已安全关闭。")

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