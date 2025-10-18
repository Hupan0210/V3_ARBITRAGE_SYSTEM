# -------------------------------------------------------------------------
# 📁 shared/connectors/base_connector.py
# (依赖: pip install pydantic)
# -------------------------------------------------------------------------
import asyncio
from abc import ABC, abstractmethod
from pydantic import BaseModel
from typing import Literal, Callable, Awaitable

# =========================================================================
# 🔩 标准化数据模型 (Data Models)
# =========================================================================
# 我们使用 Pydantic 模型来确保所有交易所返回给系统的数据格式是统一的

class Ticker(BaseModel):
    """
    标准化的 Ticker 数据模型 (买一/卖一报价)
    """
    symbol: str               # 交易对 (例如 "ETH/USDT")
    exchange: str             # 交易所 (例如 "binance")
    timestamp: int            # 时间戳 (毫秒)
    bid_price: float          # 买一价
    bid_qty: float            # 买一量
    ask_price: float          # 卖一价
    ask_qty: float            # 卖一量

class Order(BaseModel):
    """
    标准化的订单数据模型
    """
    id: str                   # 订单 ID
    symbol: str               # 交易对
    exchange: str             # 交易所
    type: Literal["LIMIT", "MARKET"]
    side: Literal["BUY", "SELL"]
    status: Literal["OPEN", "CLOSED", "FAILED", "CANCELED"]
    price: float              # 成交均价 (如果是市价单，则为 0 或成交价)
    qty: float                # 数量
    timestamp: int            # 时间戳 (毫秒)

# 定义一个回调函数类型，用于 WebSocket 异步推送数据
# (例如：当 WebSocket 收到 Ticker 时，调用这个函数)
TickerCallback = Callable[[Ticker], Awaitable[None]]


# =========================================================================
# 🏛️ 交易所连接器抽象基类 (ABC)
# =========================================================================

class BaseConnector(ABC):
    """
    ❗️ 这是所有交易所连接器的抽象基类 (ABC) "标准合同"。

    所有具体的交易所连接器 (如 BinanceConnector, OkxConnector)
    都必须继承此类并实现所有标记为 @abstractmethod 的方法。
    """
    
    def __init__(self, exchange_name: str, api_key: str, api_secret: str, api_password: str | None = None):
        """
        初始化连接器
        :param exchange_name: 交易所名称 (例如 "binance")
        :param api_key: API Key
        :param api_secret: API Secret
        :param api_password: API Passphrase (某些交易所需要)
        """
        self.exchange_name = exchange_name
        self.api_key = api_key
        self.api_secret = api_secret
        self.api_password = api_password
        self._connected = False
        
    @abstractmethod
    async def connect_ws(self, symbols: List[str], ticker_callback: TickerCallback):
        """
        🚀 [WebSocket] 连接到交易所的行情 WebSocket
        
        这个方法应该是一个持久运行的循环 (loop)，
        当收到 Ticker 数据时，必须将其转换为标准的 Ticker 模型，
        并调用 await ticker_callback(ticker_data)。
        
        :param symbols: 要订阅的交易对列表 (例如 ["ETH/USDT", "SOL/USDT"])
        :param ticker_callback: 收到 Ticker 数据时要调用的异步回调函数
        """
        pass

    @abstractmethod
    async def place_order(
        self, 
        symbol: str, 
        side: Literal["BUY", "SELL"], 
        order_type: Literal["LIMIT", "MARKET"], 
        qty: float, 
        price: float | None = None
    ) -> Order:
        """
        🛒 [REST API] 下单
        
        :param symbol: 交易对 (例如 "ETH/USDT")
        :param side: "BUY" 或 "SELL"
        :param order_type: "LIMIT" 或 "MARKET"
        :param qty: 数量
        :param price: 价格 (市价单时可为 None)
        :return: 标准化的 Order 对象
        """
        pass

    @abstractmethod
    async def get_balance(self, asset: str) -> float:
        """
        💰 [REST API] 获取特定资产的可用余额
        
        :param asset: 资产名称 (例如 "USDT" 或 "ETH")
        :return: 可用余额 (浮点数)
        """
        pass
        
    @abstractmethod
    async def close_connection(self):
        """
        🔌 关闭所有连接 (WebSocket 和 aiohttp 客户端)
        """
        pass

    # --- (可选) 其他有用的辅助方法 (非强制实现) ---
    
    async def get_ticker(self, symbol: str) -> Ticker:
        """
        [REST API] 获取单个 Ticker (通常用于启动时检查或备用)
        """
        raise NotImplementedError(f"{self.exchange_name} 未实现 get_ticker REST 方法")

    async def get_open_positions(self) -> List[dict]:
        """
        [REST API] 获取当前持仓 (主要用于合约，现货套利可能不需要)
        """
        raise NotImplementedError(f"{self.exchange_name} 未实现 get_open_positions 方法")

    @property
    def is_connected(self) -> bool:
        """返回 WebSocket 连接状态"""
        return self._connected