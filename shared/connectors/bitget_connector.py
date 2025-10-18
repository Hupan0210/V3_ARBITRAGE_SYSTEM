# -------------------------------------------------------------------------
# 📁 shared/connectors/bitget_connector.py
# (依赖: pip install aiohttp websockets)
# -------------------------------------------------------------------------
import asyncio
import aiohttp
import websockets
import json
import time
import hmac
import hashlib
import base64
import logging
from typing import List, Literal, Callable, Awaitable, Dict
from websockets.exceptions import ConnectionClosed

from shared.connectors.base_connector import BaseConnector, Ticker, Order, TickerCallback
from shared.config import settings

# 日志记录
log = logging.getLogger(__name__)

# =========================================================================
#  Bitget API 常量
# =========================================================================
BITGET_SPOT_REST_URL = "https://api.bitget.com"
# ❗️ Bitget 使用 V2 WebSocket API
BITGET_SPOT_WS_URL = "wss://ws.bitget.com/v2/spot/public" 
BITGET_SPOT_WS_PRIVATE_URL = "wss://ws.bitget.com/v2/spot/private" # 私有 WS (用于订单更新等)


# =========================================================================
#  Bitget (BG) 连接器实现
# =========================================================================

class BitgetConnector(BaseConnector):
    """
    Bitget (BG) 现货连接器实现
    """

    def __init__(self, exchange_name: str, api_key: str, api_secret: str, api_password: str | None = None):
        super().__init__(exchange_name, api_key, api_secret, api_password)
        
        # ❗️ Bitget 必须提供 api_password (passphrase)
        if not api_password:
            raise ValueError("Bitget 连接器必须提供 api_password (passphrase)")
            
        self.rest_base_url = BITGET_SPOT_REST_URL
        self.ws_base_url = BITGET_SPOT_WS_URL
        
        self.session: aiohttp.ClientSession | None = None
        self.ws_connection: websockets.WebSocketClientProtocol | None = None
        self.ws_task: asyncio.Task | None = None
        self._internal_symbol_map: Dict[str, str] = {} # "ETHUSDT_SPBL" -> "ETH/USDT"

    # ---------------------------------------------------------------------
    #  辅助函数 (Helpers)
    # ---------------------------------------------------------------------

    def _get_session(self) -> aiohttp.ClientSession:
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession()
        return self.session

    def _format_symbol(self, standard_symbol: str) -> str:
        """
        将标准格式 'ETH/USDT' 转换为 Bitget 现货格式 'ETHUSDT_SPBL'
        """
        return standard_symbol.replace("/", "").upper() + "_SPBL"

    def _generate_signature(self, timestamp: str, method: str, request_path: str, body: str = "") -> str:
        """
        ❗️ 关键: 生成 Bitget API 所需的 HMAC SHA256 签名
        
        签名字符串: timestamp + method + requestPath + body
        """
        message = timestamp + method.upper() + request_path + body
        
        mac = hmac.new(
            self.api_secret.encode('utf-8'), 
            message.encode('utf-8'), 
            hashlib.sha256
        )
        
        # ❗️ Bitget 需要 Base64 编码的签名
        return base64.b64encode(mac.digest()).decode('utf-8')

    def _create_headers(self, timestamp: str, method: str, request_path: str, body: str = "") -> dict:
        """
        创建 REST API 所需的请求头 (包含签名和 passphrase)
        """
        signature = self._generate_signature(timestamp, method, request_path, body)
        
        return {
            'ACCESS-KEY': self.api_key,
            'ACCESS-SIGN': signature,
            'ACCESS-TIMESTAMP': timestamp,
            'ACCESS-PASSPHRASE': self.api_password, # ❗️ 必须
            'Content-Type': 'application/json',
            'Connection': 'keep-alive' # BG 推荐
        }

    # ---------------------------------------------------------------------
    #  (实现) WebSocket 接口
    # ---------------------------------------------------------------------

    async def connect_ws(self, symbols: List[str], ticker_callback: TickerCallback):
        """
        (实现) 连接到 Bitget 的公共行情 WebSocket
        """
        if self.ws_task and not self.ws_task.done():
            log.warning(f"[{self.exchange_name}] WS 已经连接，无需重复操作。")
            return

        # 1. 更新内部币对映射表
        self._internal_symbol_map = {self._format_symbol(s): s for s in symbols}
        
        # 2. 构造 Bitget 的订阅参数 ("args")
        # 格式: [{"instType": "SPBL", "channel": "books_snapshot", "instId": "ETHUSDT"}, ...]
        # ❗️ Bitget 使用 "books_snapshot" (订单簿快照) 来获取买一卖一价，而不是 "ticker"
        args = [
            {
                "instType": "SPBL",
                "channel": "books_snapshot", # 我们用这个模拟 Ticker
                "instId": self._format_symbol(s)
            } 
            for s in symbols
        ]
        
        # 3. 创建并启动后台任务
        self.ws_task = asyncio.create_task(
            self._ws_handler(self.ws_base_url, args, ticker_callback)
        )

    async def _ws_handler(self, url: str, sub_args: List[Dict], ticker_callback: TickerCallback):
        """
        WebSocket 消息处理循环 (带自动重连和订阅)
        """
        while True:
            try:
                async with websockets.connect(url) as ws:
                    self.ws_connection = ws
                    self._connected = True
                    log.info(f"[{self.exchange_name}] WebSocket 连接成功。")

                    # ❗️ 关键: Bitget WS 需要发送 "subscribe" 消息
                    subscribe_message = {
                        "op": "subscribe",
                        "args": sub_args
                    }
                    await ws.send(json.dumps(subscribe_message))
                    log.debug(f"[{self.exchange_name}] 已发送订阅消息: {subscribe_message}")

                    # (可选) 如果需要私有数据 (如订单更新)，需要登录
                    # await self._ws_login(ws)

                    # 持续接收消息
                    while True:
                        message = await ws.recv()
                        
                        # Bitget 会发送 'pong' 作为心跳
                        if message == 'pong':
                            await ws.send('ping') # 保持连接
                            continue
                            
                        data = json.loads(message)

                        # 订阅成功的响应
                        if data.get("event") == "subscribe":
                            log.info(f"[{self.exchange_name}] WS 订阅成功: {data}")
                            continue
                        
                        # ❗️ 关键: 解析 "snapshot" (快照) 数据
                        if data.get("action") == "snapshot" and data.get("arg", {}).get("channel") == "books_snapshot":
                            ticker = self._parse_ws_ticker(data)
                            if ticker:
                                await ticker_callback(ticker)
                        
                        # (可选) Bitget WS 也可能推送 "update" (增量更新)
                        # 为简单起见，我们的策略只依赖 snapshot (全量买一卖一)

            except (ConnectionClosed, asyncio.CancelledError) as e:
                self._connected = False
                log.warning(f"[{self.exchange_name}] WebSocket 连接断开: {type(e).__name__}")
                if isinstance(e, asyncio.CancelledError):
                    log.info(f"[{self.exchange_name}] WebSocket 任务被取消，停止重连。")
                    break
                log.info(f"[{self.exchange_name}] 5秒后尝试重连...")
                await asyncio.sleep(5)
                
            except Exception as e:
                log.error(f"[{self.exchange_name}] WebSocket 发生未知错误: {e}", exc_info=True)
                self._connected = False
                await asyncio.sleep(5) # 发生未知错误，等待后重连

    def _parse_ws_ticker(self, data: dict) -> Ticker | None:
        """
        (标准化) 将 Bitget @books_snapshot 的原始数据解析为标准 Ticker 模型
        """
        try:
            # data 示例: 
            # { "action": "snapshot", 
            #   "arg": {"instType": "SPBL", "channel": "books_snapshot", "instId": "ETHUSDT_SPBL"}, 
            #   "data": [{
            #       "asks": [["2315.10", "5.5"], ...], 
            #       "bids": [["2315.00", "10.0"], ...], 
            #       "ts": "1697049300123" 
            #   }]
            # }
            
            arg = data.get("arg", {})
            snapshot_data = data.get("data", [{}])[0]
            
            bitget_symbol = arg.get("instId")
            standard_symbol = self._internal_symbol_map.get(bitget_symbol)
            
            if not standard_symbol or not snapshot_data:
                log.warning(f"[{self.exchange_name}] 收到未知或空快照: {bitget_symbol}")
                return None

            # ❗️ Bitget 的 asks/bids 是 [价格, 数量] 字符串数组
            # asks[0] 是卖一 (最低卖价)
            # bids[0] 是买一 (最高买价)
            
            if not snapshot_data.get("bids") or not snapshot_data.get("asks"):
                # log.debug(f"[{self.exchange_name}] 订单簿快照为空: {bitget_symbol}")
                return None # 订单簿暂时为空

            bid = snapshot_data["bids"][0]
            ask = snapshot_data["asks"][0]
            
            return Ticker(
                symbol=standard_symbol,
                exchange=self.exchange_name,
                timestamp=int(snapshot_data["ts"]), # ❗️ 使用 Bitget 提供的毫秒时间戳
                bid_price=float(bid[0]),
                bid_qty=float(bid[1]),
                ask_price=float(ask[0]),
                ask_qty=float(ask[1])
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
        # ❗️ Bitget API V2
        endpoint = "/api/v2/spot/trade/place-order" 
        
        # 1. 准备 Body (❗️ Bitget 使用 JSON Body)
        body_dict = {
            "symbol": self._format_symbol(symbol),
            "side": side.lower(), # ❗️ Bitget 用小写 (buy/sell)
            "orderType": order_type.lower(), # ❗️ Bitget 用小写 (limit/market)
            "quantity": f"{qty:.8f}",
            "force": "normal" # GTC
        }
        
        if order_type == "LIMIT":
            if price is None:
                raise ValueError("限价单 (LIMIT) 必须提供价格 (price)")
            body_dict["price"] = f"{price:.8f}"
        
        body_json = json.dumps(body_dict)
        
        # 2. 准备签名和请求头
        timestamp = str(int(time.time() * 1000))
        headers = self._create_headers(timestamp, "POST", endpoint, body_json)
        
        # 3. 发送请求
        try:
            async with session.post(
                self.rest_base_url + endpoint, 
                headers=headers,
                data=body_json # ❗️ POST 请求使用 data=body
            ) as response:
                
                resp_data = await response.json()
                
                # ❗️ Bitget 的响应格式: {"code": "00000", "msg": "success", "data": {...}}
                if resp_data.get("code") != "00000":
                    log.error(f"[{self.exchange_name}] 下单失败: {resp_data}")
                    return Order(
                        id=f"FAILED_{timestamp}",
                        symbol=symbol, exchange=self.exchange_name, type=order_type,
                        side=side, status="FAILED", price=price or 0.0, qty=qty,
                        timestamp=int(timestamp)
                    )
                
                # 4. (标准化) 解析成功的订单响应
                order_data = resp_data.get("data", {})
                order_id = order_data.get("orderId")
                
                if not order_id:
                     log.error(f"[{self.exchange_name}] 下单成功但未返回 orderId: {resp_data}")
                     return Order(
                        id=f"UNKNOWN_{timestamp}",
                        symbol=symbol, exchange=self.exchange_name, type=order_type,
                        side=side, status="FAILED", price=price or 0.0, qty=qty,
                        timestamp=int(timestamp)
                    )

                # ❗️ Bitget V2 API 在下单时 *不会* 立即返回成交状态或价格
                # 它只返回 "orderId"，我们必须假设它是 "OPEN"
                # (这就是为什么私有 WebSocket 用于接收订单更新很重要)
                
                return Order(
                    id=order_id,
                    symbol=symbol,
                    exchange=self.exchange_name,
                    type=order_type,
                    side=side,
                    status="OPEN", # ❗️ Bitget 总是先返回 OPEN
                    price=price or 0.0, # 市场单暂时无法知道价格
                    qty=qty, # 委托数量
                    timestamp=int(timestamp)
                )

        except Exception as e:
            log.error(f"[{self.exchange_name}] 下单时发生严重错误: {e}", exc_info=True)
            return Order(
                id=f"ERROR_{int(time.time()*1000)}",
                symbol=symbol, exchange=self.exchange_name, type=order_type,
                side=side, status="FAILED", price=price or 0.0, qty=qty,
                timestamp=int(time.time() * 1000)
            )

    async def get_balance(self, asset: str) -> float:
        """
        (实现) 获取特定资产的可用余额
        """
        session = self._get_session()
        # ❗️ Bitget API V2
        endpoint = "/api/v2/spot/account/assets"
        
        # ❗️ Bitget 余额查询使用 GET，但参数放在 query string 中
        params = {"coin": asset.upper()}
        query_string = f"coin={asset.upper()}"
        
        timestamp = str(int(time.time() * 1000))
        
        # ❗️ GET 请求的签名，body 为空，但 requestPath 包含 query string
        headers = self._create_headers(timestamp, "GET", f"{endpoint}?{query_string}", "")
        
        try:
            async with session.get(
                self.rest_base_url + endpoint, 
                headers=headers,
                params=params
            ) as response:
                
                resp_data = await response.json()

                if resp_data.get("code") != "00000":
                    log.error(f"[{self.exchange_name}] 获取余额失败: {resp_data}")
                    return 0.0
                    
                balances = resp_data.get("data", [])
                if not balances:
                    log.warning(f"[{self.exchange_name}] 未在账户中找到资产: {asset}")
                    return 0.0
                
                # ❗️ 返回 "available" (可用) 余额
                return float(balances[0].get("available", 0.0))

        except Exception as e:
            log.error(f"[{self.exchange_name}] 获取余额时发生严重错误: {e}", exc_info=True)
            return 0.0

    async def close_connection(self):
        """
        (实现) 关闭所有连接 (WebSocket 和 aiohttp 客户端)
        """
        log.info(f"[{self.exchange_name}] 正在关闭连接...")
        
        if self.ws_task and not self.ws_task.done():
            self.ws_task.cancel()
            try:
                await self.ws_task
            except asyncio.CancelledError:
                log.debug(f"[{self.exchange_name}] WebSocket 任务已成功取消。")
        
        if self.session and not self.session.closed:
            await self.session.close()
            log.debug(f"[{self.exchange_name}] aiohttp session 已关闭。")
            
        self._connected = False
        self.session = None
        self.ws_connection = None
        log.info(f"[{self.exchange_name}] 连接已关闭。")