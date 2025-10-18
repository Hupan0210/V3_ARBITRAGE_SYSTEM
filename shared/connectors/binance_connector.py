# -------------------------------------------------------------------------
# 📁 shared/connectors/binance_connector.py
# (依赖: pip install aiohttp websockets)
# -------------------------------------------------------------------------
import asyncio
import aiohttp
import websockets
import json
import time
import hmac
import hashlib
import logging
from typing import List, Literal, Callable, Awaitable, Dict
from websockets.exceptions import ConnectionClosed

from shared.connectors.base_connector import BaseConnector, Ticker, Order, TickerCallback
from shared.config import settings # 仅用于类型提示或日志，实际配置在 __init__ 中传入

# 日志记录
log = logging.getLogger(__name__)

# =========================================================================
#  Binance API 常量
# =========================================================================
# ❗️ 注意：V3 架构建议部署在靠近交易所的服务器 (如东京/新加坡)
# 生产环境应使用 api.binance.com，如果使用 V3 架构部署在 AWS 东京，
# 可以考虑 api.binance.jp 或 api.binance.com
BINANCE_SPOT_REST_URL = "https://api.binance.com"
BINANCE_SPOT_WS_URL = "wss://stream.binance.com:9443/stream"


# =========================================================================
#  Binance (币安) 连接器实现
# =========================================================================

class BinanceConnector(BaseConnector):
    """
    Binance (币安) 现货连接器实现
    """

    def __init__(self, exchange_name: str, api_key: str, api_secret: str, api_password: str | None = None):
        super().__init__(exchange_name, api_key, api_secret, api_password)
        
        self.rest_base_url = BINANCE_SPOT_REST_URL
        self.ws_base_url = BINANCE_SPOT_WS_URL
        
        # 异步 HTTP 客户端 Session
        self.session: aiohttp.ClientSession | None = None
        # WebSocket 连接对象
        self.ws_connection: websockets.WebSocketClientProtocol | None = None
        # WebSocket 处理任务
        self.ws_task: asyncio.Task | None = None
        # 内部维护一个从 "ETHUSDT" 映射回 "ETH/USDT" 的字典
        self._internal_symbol_map: Dict[str, str] = {}

    # ---------------------------------------------------------------------
    #  辅助函数 (Helpers)
    # ---------------------------------------------------------------------

    def _get_session(self) -> aiohttp.ClientSession:
        """
        懒加载并返回 aiohttp 客户端 Session
        """
        if self.session is None or self.session.closed:
            # ❗️ 关键: aiohttp.ClientSession 必须在 async 函数内部创建
            # 但这里我们用懒加载，确保它在需要时被创建
            # 更好的做法是在一个 async 'start' 方法中初始化
            self.session = aiohttp.ClientSession(headers=self._create_headers())
        return self.session

    def _format_symbol(self, standard_symbol: str) -> str:
        """
        将标准格式 'ETH/USDT' 转换为交易所格式 'ETHUSDT'
        """
        return standard_symbol.replace("/", "").upper()

    def _create_headers(self) -> dict:
        """
        创建 REST API 所需的请求头 (包含 API Key)
        """
        return {
            'X-MBX-APIKEY': self.api_key,
            'Content-Type': 'application/x-www-form-urlencoded'
        }

    def _generate_signature(self, params: dict) -> str:
        """
        ❗️ 关键: 生成 Binance API 所需的 HMAC SHA256 签名
        """
        query_string = "&".join(f"{k}={v}" for k, v in params.items())
        return hmac.new(
            self.api_secret.encode('utf-8'), 
            query_string.encode('utf-8'), 
            hashlib.sha256
        ).hexdigest()

    # ---------------------------------------------------------------------
    #  (实现) WebSocket 接口
    # ---------------------------------------------------------------------

    async def connect_ws(self, symbols: List[str], ticker_callback: TickerCallback):
        """
        (实现) 连接到币安的组合行情 WebSocket
        """
        if self.ws_task and not self.ws_task.done():
            log.warning(f"[{self.exchange_name}] WS 已经连接，无需重复操作。")
            return

        # 1. 更新内部币对映射表
        # {"ETHUSDT": "ETH/USDT", "SOLUSDT": "SOL/USDT"}
        self._internal_symbol_map = {self._format_symbol(s): s for s in symbols}
        
        # 2. 构造币安格式的流名称
        # ["ethusdt@bookTicker", "solusdt@bookTicker"]
        streams = [f"{self._format_symbol(s).lower()}@bookTicker" for s in symbols]
        
        # 3. 构造组合流 URL
        ws_url = f"{self.ws_base_url}?streams={'/'.join(streams)}"
        log.info(f"[{self.exchange_name}] 正在连接到 WebSocket: {ws_url}")
        
        # 4. 创建并启动后台任务
        self.ws_task = asyncio.create_task(
            self._ws_handler(ws_url, ticker_callback)
        )

    async def _ws_handler(self, url: str, ticker_callback: TickerCallback):
        """
        WebSocket 消息处理循环 (带自动重连)
        """
        while True:
            try:
                async with websockets.connect(url) as ws:
                    self.ws_connection = ws
                    self._connected = True
                    log.info(f"[{self.exchange_name}] WebSocket 连接成功。")
                    
                    # 持续接收消息
                    while True:
                        message = await ws.recv()
                        data = json.loads(message)
                        
                        # ❗️ 关键: 解析组合流数据
                        # 币安组合流格式: {"stream": "...", "data": {...}}
                        if "stream" in data and "data" in data:
                            ticker = self._parse_ws_ticker(data['data'])
                            if ticker:
                                # 异步调用回调函数，推送标准 Ticker
                                await ticker_callback(ticker)
                        else:
                            log.debug(f"[{self.exchange_name}] 收到非 Ticker 消息: {data}")

            except (ConnectionClosed, asyncio.CancelledError) as e:
                self._connected = False
                log.warning(f"[{self.exchange_name}] WebSocket 连接断开: {type(e).__name__}")
                if isinstance(e, asyncio.CancelledError):
                    log.info(f"[{self.exchange_name}] WebSocket 任务被取消，停止重连。")
                    break # 任务被取消，退出循环
                # 发生其他断线 (ConnectionClosed)
                log.info(f"[{self.exchange_name}] 5秒后尝试重连...")
                await asyncio.sleep(5)
                
            except Exception as e:
                log.error(f"[{self.exchange_name}] WebSocket 发生未知错误: {e}", exc_info=True)
                self._connected = False
                await asyncio.sleep(5) # 发生未知错误，等待后重连


    def _parse_ws_ticker(self, data: dict) -> Ticker | None:
        """
        (标准化) 将币安 @bookTicker 的原始数据解析为标准 Ticker 模型
        """
        try:
            # data 示例: {'u': 43453, 's': 'ETHUSDT', 'b': '2315.00', 'B': '10.0', 'a': '2315.10', 'A': '5.5'}
            binance_symbol = data['s']
            
            # 映射回标准格式 (例如 "ETH/USDT")
            standard_symbol = self._internal_symbol_map.get(binance_symbol)
            if not standard_symbol:
                log.warning(f"[{self.exchange_name}] 收到未知币对 {binance_symbol} 的 Ticker")
                return None
            
            return Ticker(
                symbol=standard_symbol,
                exchange=self.exchange_name,
                timestamp=int(time.time() * 1000), # 币安 bookTicker 不带时间戳，使用本地时间
                bid_price=float(data['b']),
                bid_qty=float(data['B']),
                ask_price=float(data['a']),
                ask_qty=float(data['A'])
            )
        except Exception as e:
            log.error(f"[{self.exchange_name}] 解析 Ticker 失败: {e} | 数据: {data}", exc_info=True)
            return None

    # ---------------------------------------------------------------------
    #  (实现) REST API 接口
    # ---------------------------------------------------------------------

    async def place_order(
        self, 
        symbol: str, 
        side: Literal["BUY", "SELL"], 
        order_type: Literal["LIMIT", "MARKET"], 
        qty: float, 
        price: float | None = None
    ) -> Order:
        """
        (实现) 现货下单
        """
        session = self._get_session()
        endpoint = "/api/v3/order"
        
        # 1. 准备参数
        params = {
            "symbol": self._format_symbol(symbol),
            "side": side.upper(),
            "type": order_type.upper(),
        }
        
        if order_type == "MARKET":
            params["quantity"] = f"{qty:.8f}" # ❗️ 市场单使用 quantity (数量)
        elif order_type == "LIMIT":
            if price is None:
                raise ValueError("限价单 (LIMIT) 必须提供价格 (price)")
            params["quantity"] = f"{qty:.8f}"
            params["price"] = f"{price:.8f}"
            params["timeInForce"] = "GTC" # Good 'Til Canceled
        else:
            raise ValueError(f"不支持的订单类型: {order_type}")
            
        params["timestamp"] = int(time.time() * 1000)
        
        # 2. 生成签名
        params["signature"] = self._generate_signature(params)
        
        # 3. 发送请求
        try:
            async with session.post(
                self.rest_base_url + endpoint, 
                params=params # 币安 POST 请求的参数放在 params (query string) 中
            ) as response:
                
                resp_data = await response.json()

                if response.status >= 400:
                    log.error(f"[{self.exchange_name}] 下单失败: {resp_data}")
                    # ❗️ 关键: 即使失败也要返回标准 Order 对象
                    return Order(
                        id=f"FAILED_{int(time.time()*1000)}",
                        symbol=symbol,
                        exchange=self.exchange_name,
                        type=order_type,
                        side=side,
                        status="FAILED",
                        price=price or 0.0,
                        qty=qty,
                        timestamp=params["timestamp"]
                    )
                
                # 4. (标准化) 解析成功的订单响应
                # ❗️ 市场单 (MARKET) 会立即返回 FILLED 状态和成交均价
                # ❗️ 限价单 (LIMIT) 会立即返回 NEW 状态
                
                order_status = resp_data.get("status", "FAILED").upper()
                avg_price = 0.0

                if order_status == "FILLED":
                    # 市场单或已立即成交的限价单
                    # 尝试从 'fills' 字段计算精确的成交均价
                    fills = resp_data.get("fills", [])
                    total_qty = 0.0
                    total_value = 0.0
                    if fills:
                        for fill in fills:
                            total_qty += float(fill["qty"])
                            total_value += float(fill["price"]) * float(fill["qty"])
                        if total_qty > 0:
                            avg_price = total_value / total_qty
                    else:
                        # 如果没有 fills 字段 (不应该发生)，使用 orderPrice
                        avg_price = float(resp_data.get("price", 0.0))
                
                elif order_status == "NEW" or order_status == "PARTIALLY_FILLED":
                    order_status = "OPEN" # 统一为我们的 "OPEN" 状态
                    avg_price = float(resp_data.get("price", 0.0))
                
                else:
                     order_status = "FAILED" # 其他状态 (CANCELED, REJECTED) 均视为 FAILED

                return Order(
                    id=str(resp_data["orderId"]),
                    symbol=symbol,
                    exchange=self.exchange_name,
                    type=order_type,
                    side=side,
                    # ❗️ 状态标准化
                    status="OPEN" if order_status == "NEW" else order_status, 
                    price=avg_price, # 成交均价
                    qty=float(resp_data["executedQty"]), # 已成交数量
                    timestamp=resp_data["transactTime"]
                )

        except Exception as e:
            log.error(f"[{self.exchange_name}] 下单时发生严重错误: {e}", exc_info=True)
            return Order(
                id=f"ERROR_{int(time.time()*1000)}",
                symbol=symbol,
                exchange=self.exchange_name,
                type=order_type,
                side=side,
                status="FAILED",
                price=price or 0.0,
                qty=qty,
                timestamp=int(time.time() * 1000)
            )

    async def get_balance(self, asset: str) -> float:
        """
        (实现) 获取特定资产的可用余额
        """
        session = self._get_session()
        endpoint = "/api/v3/account"
        
        params = {
            "timestamp": int(time.time() * 1000)
        }
        params["signature"] = self._generate_signature(params)
        
        try:
            async with session.get(
                self.rest_base_url + endpoint, 
                params=params
            ) as response:
                
                resp_data = await response.json()
                
                if response.status >= 400:
                    log.error(f"[{self.exchange_name}] 获取余额失败: {resp_data}")
                    return 0.0
                    
                balances = resp_data.get("balances", [])
                for balance in balances:
                    if balance["asset"].upper() == asset.upper():
                        # ❗️ 返回 "free" (可用) 余额，而不是 "locked" (冻结) 余额
                        return float(balance["free"])
                
                log.warning(f"[{self.exchange_name}] 未在账户中找到资产: {asset}")
                return 0.0

        except Exception as e:
            log.error(f"[{self.exchange_name}] 获取余额时发生严重错误: {e}", exc_info=True)
            return 0.0

    async def close_connection(self):
        """
        (实现) 关闭所有连接 (WebSocket 和 aiohttp 客户端)
        """
        log.info(f"[{self.exchange_name}] 正在关闭连接...")
        
        # 1. 关闭 WebSocket 任务
        if self.ws_task and not self.ws_task.done():
            self.ws_task.cancel()
            try:
                await self.ws_task
            except asyncio.CancelledError:
                log.debug(f"[{self.exchange_name}] WebSocket 任务已成功取消。")
        
        # 2. 关闭 aiohttp Session
        if self.session and not self.session.closed:
            await self.session.close()
            log.debug(f"[{self.exchange_name}] aiohttp session 已关闭。")
            
        self._connected = False
        self.session = None
        self.ws_connection = None
        log.info(f"[{self.exchange_name}] 连接已关闭。")