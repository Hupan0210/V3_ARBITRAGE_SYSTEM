# -------------------------------------------------------------------------
# 📁 datastore/redis_client.py
# (依赖: pip install redis)
# -------------------------------------------------------------------------
import asyncio
import redis.asyncio as aioredis
import json
import logging
from typing import Callable, Awaitable, Dict, Any
from pydantic import BaseModel

from shared.config import settings

log = logging.getLogger(__name__)

# =========================================================================
#  🏛️ V3 架构 - 全局频道/键名常量
# =========================================================================
# ❗️ 关键: 所有微服务都将导入这些常量，以确保通信频道和键名统一

# --- Pub/Sub 频道 (Channels) ---
CH_MARKET_DATA = "channel:market_data"     # 频道: 市场行情 (Ticker)
CH_TRADE_SIGNALS = "channel:trade_signals"   # 频道: 交易信号 (开仓/平仓)
CH_EXEC_REPORTS = "channel:exec_reports"  # 频道: 成交回报 (Order)
CH_RISK_ALERTS = "channel:risk_alerts"     # 频道: 风控警报

# --- Redis 键名 (Keys) ---
KEY_CURRENT_POSITIONS = "state:current_positions" # 键: 当前持仓状态
KEY_SERVICE_HEARTBEAT = "state:service_heartbeat"   # 键 (Hash): 服务心跳


# =========================================================================
#  💾 Redis 客户端封装
# =========================================================================

class RedisClient:
    """
    V3 架构的异步 Redis 客户端封装。
    
    封装了连接池、Pub/Sub (发布/订阅) 和 State (状态读写)。
    """
    
    def __init__(self, redis_url: str):
        self.redis_url = redis_url
        self.pool = aioredis.ConnectionPool.from_url(redis_url, decode_responses=True)
        self.redis_conn: aioredis.Redis | None = None
        log.info(f"Redis 客户端正在初始化... URL: {redis_url}")

    async def connect(self):
        """
        连接到 Redis 并 PING 测试
        """
        try:
            self.redis_conn = aioredis.Redis(connection_pool=self.pool)
            await self.redis_conn.ping()
            log.info(f"✅ [Redis] 连接成功 (Ping OK)")
        except Exception as e:
            log.critical(f"❗️ [Redis] 严重错误: 无法连接到 Redis: {e}", exc_info=True)
            raise

    async def close(self):
        """
        关闭 Redis 连接
        """
        if self.redis_conn:
            await self.redis_conn.close()
            await self.pool.disconnect()
            log.info("[Redis] 连接已关闭。")

    # ---------------------------------------------------------------------
    #  Pub/Sub (消息队列)
    # ---------------------------------------------------------------------

    async def publish(self, channel: str, message: BaseModel | Dict[str, Any]):
        """
        🚀 发布消息到指定的 Redis 频道
        
        :param channel: 频道名称 (例如 CH_MARKET_DATA)
        :param message: Pydantic 模型或字典，将自动序列化为 JSON
        """
        if not self.redis_conn:
            await self.connect()
            
        try:
            # 序列化
            if isinstance(message, BaseModel):
                payload = message.model_dump_json()
            else:
                payload = json.dumps(message)
                
            await self.redis_conn.publish(channel, payload)
            # log.debug(f"[Redis PUB {channel}] > {payload}")
            
        except Exception as e:
            log.error(f"[Redis] 发布消息到 {channel} 失败: {e}", exc_info=True)

    async def subscribe(self, channel: str, callback: Callable[[Dict[str, Any]], Awaitable[None]]):
        """
        🎧 订阅指定的 Redis 频道，并在收到消息时调用回调函数
        
        ❗️ 这是一个阻塞方法 (会永久循环)，通常应在 asyncio.create_task() 中运行。
        
        :param channel: 频道名称 (例如 CH_MARKET_DATA)
        :param callback: 异步回调函数 (接收一个 dict 消息)
        """
        if not self.redis_conn:
            await self.connect()
            
        pubsub = self.redis_conn.pubsub()
        await pubsub.subscribe(channel)
        log.info(f"[Redis SUB] 正在监听频道: {channel}...")
        
        while True:
            try:
                # 监听消息
                message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                
                if message is None:
                    # log.debug(f"[Redis SUB {channel}] (timeout)")
                    await asyncio.sleep(0.01) # 释放 CPU
                    continue
                    
                if message.get("type") == "message":
                    payload_str = message.get("data")
                    # log.debug(f"[Redis SUB {channel}] < {payload_str}")
                    
                    # 反序列化
                    try:
                        payload_dict = json.loads(payload_str)
                        # 异步执行回调
                        await callback(payload_dict)
                    except json.JSONDecodeError:
                        log.warning(f"[Redis SUB {channel}] 收到无法解析的 JSON: {payload_str}")
                    except Exception as e:
                         log.error(f"[Redis SUB {channel}] 回调函数执行出错: {e}", exc_info=True)
                
            except Exception as e:
                log.error(f"[Redis SUB {channel}] 订阅循环发生错误: {e}。5秒后重试...", exc_info=True)
                await asyncio.sleep(5)
                # 尝试重新订阅 (如果连接断开)
                try:
                    await pubsub.subscribe(channel)
                except Exception as re_e:
                    log.error(f"[Redis SUB {channel}] 重新订阅失败: {re_e}")

    # ---------------------------------------------------------------------
    #  State (状态持久化 - 理念五)
    # ---------------------------------------------------------------------

    async def set_state(self, key: str, data: Dict[str, Any]):
        """
        💾 将 Python 字典作为 JSON 字符串存储到 Redis 键中 (用于状态持久化)
        (例如: 存储 KEY_CURRENT_POSITIONS)
        
        :param key: 键名 (例如 KEY_CURRENT_POSITIONS)
        :param data: 要存储的字典
        """
        if not self.redis_conn:
            await self.connect()
        try:
            payload = json.dumps(data)
            await self.redis_conn.set(key, payload)
            log.info(f"[Redis SET] 状态已保存到键: {key}")
        except Exception as e:
            log.error(f"[Redis] 保存状态到 {key} 失败: {e}", exc_info=True)
            
    async def get_state(self, key: str) -> Dict[str, Any] | None:
        """
        💿 从 Redis 键中读取 JSON 字符串并解析为 Python 字典 (用于崩溃恢复)
        
        :param key: 键名 (例如 KEY_CURRENT_POSITIONS)
        :return: Python 字典，如果键不存在或解析失败则返回 None
        """
        if not self.redis_conn:
            await self.connect()
        try:
            payload = await self.redis_conn.get(key)
            if payload:
                log.info(f"[Redis GET] 从键 {key} 加载状态成功。")
                return json.loads(payload)
            else:
                log.info(f"[Redis GET] 键 {key} 不存在 (正常启动)。")
                return None
        except Exception as e:
            log.error(f"[Redis] 读取或解析 {key} 状态失败: {e}", exc_info=True)
            return None

    # (可选) 其他如 Heartbeat (心跳) 等高级功能
    async def set_heartbeat(self, service_name: str):
        """
        (风控) 设置服务心跳，表示服务存活
        """
        if not self.redis_conn:
            await self.connect()
        # 使用 Hash 结构: KEY_SERVICE_HEARTBEAT -> {"data_feed": "timestamp", ...}
        await self.redis_conn.hset(KEY_SERVICE_HEARTBEAT, service_name, int(time.time()))
        log.debug(f"[Redis Heartbeat] 服务 {service_name} 心跳已更新。")


# =========================================================================
#  全局 Redis 客户端实例
# =========================================================================

# 📦 在项目的任何地方 `from datastore.redis_client import redis_client` 即可使用
redis_client = RedisClient(settings.datastore.REDIS_URL)


# --- (可选) 启动时打印配置以供调试 ---
async def main_test():
    print("--- Redis 客户端连接测试 ---")
    await redis_client.connect()
    
    # 测试 Set/Get
    test_key = "state:test"
    await redis_client.set_state(test_key, {"status": "ok", "value": 123})
    state = await redis_client.get_state(test_key)
    print(f"Set/Get 测试: {state}")
    
    # 测试 Pub/Sub
    async def test_callback(message):
        print(f"Callback 收到消息: {message}")

    # 启动订阅者
    asyncio.create_task(redis_client.subscribe(CH_MARKET_DATA, test_callback))
    
    await asyncio.sleep(1) # 等待订阅成功
    
    # 发布者
    print("正在发布消息...")
    await redis_client.publish(CH_MARKET_DATA, {"symbol": "ETH/USDT", "price": 3000})
    
    await asyncio.sleep(1) # 等待消息处理
    await redis_client.close()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main_test())