# -------------------------------------------------------------------------
# 📁 shared/connectors/__init__.py
# (这是交易所工厂的核心)
# -------------------------------------------------------------------------
import importlib
import logging
from typing import Dict, Type

from shared.config import settings
from shared.connectors.base_connector import BaseConnector, Ticker, Order, TickerCallback

log = logging.getLogger(__name__)

# =========================================================================
#  🔌 交易所连接器工厂 (Connector Factory)
# =========================================================================

def create_connectors() -> Dict[str, BaseConnector]:
    """
    ❗️ 关键: 交易所工厂函数。
    
    读取 `settings.system.ENABLED_EXCHANGES` 列表 (例如 ["binance", "bitget"])，
    然后动态导入并实例化每个连接器。
    
    :return: 一个字典，键为交易所名称，值为已初始化的连接器实例。
             例如: {"binance": <BinanceConnector object>, "bitget": <BitgetConnector object>}
    """
    
    initialized_connectors: Dict[str, BaseConnector] = {}
    
    # 1. 从全局配置中获取启用的交易所列表
    enabled_names = settings.system.ENABLED_EXCHANGES
    
    log.info(f"正在初始化连接器工厂... 启用的交易所: {enabled_names}")
    
    for name in enabled_names:
        try:
            # 2. 检查该交易所的 API 密钥是否已在配置中定义
            api_config = settings.exchanges.get(name)
            
            if not api_config:
                log.error(
                    f"[{name}] 交易所已在 ENABLED_EXCHANGES 中启用, "
                    f"但在 settings.exchanges 中缺少对应的 API 密钥配置。"
                )
                continue

            # 3. ❗️ 动态导入模块 (例如: "shared.connectors.binance_connector")
            module_name = f"shared.connectors.{name}_connector"
            connector_module = importlib.import_module(module_name)

            # 4. ❗️ 根据命名约定获取类 (例如: "BinanceConnector")
            class_name = f"{name.capitalize()}Connector"
            ConnectorClass: Type[BaseConnector] = getattr(connector_module, class_name)

            # 5. 实例化连接器
            connector_instance = ConnectorClass(
                exchange_name=name,
                api_key=api_config.api_key,
                api_secret=api_config.secret,
                api_password=api_config.password  # (例如 Bitget 需要, Binance 为 None)
            )
            
            initialized_connectors[name] = connector_instance
            log.info(f"✅ 成功加载并初始化连接器: {class_name}")

        except ImportError:
            log.error(
                f"[{name}] 动态导入失败: 找不到模块 {module_name}。"
                f"请确保文件 'shared/connectors/{name}_connector.py' 存在。"
            )
        except AttributeError:
            log.error(
                f"[{name}] 动态加载失败: 在 {module_name} 中找不到类 {class_name}。"
                f"请确保类名符合命名约定。"
            )
        except Exception as e:
            log.critical(
                f"[{name}] 初始化连接器时发生未知严重错误: {e}", 
                exc_info=True
            )
            
    if not initialized_connectors:
        log.critical("❗️ 严重警告: 没有成功加载任何交易所连接器。系统无法运行。")
            
    return initialized_connectors


# =========================================================================
#  📦 重新导出 (Re-export)
# =========================================================================
#
# 将核心模型和工厂函数暴露在包的顶层, 
# 这样其他服务可以通过 from shared.connectors import ... 轻松访问
#
__all__ = [
    "BaseConnector",        # 抽象基类
    "Ticker",               # 标准 Ticker 模型
    "Order",                # 标准 Order 模型
    "TickerCallback",       # Ticker 回调类型
    "create_connectors"     # 核心工厂函数
]