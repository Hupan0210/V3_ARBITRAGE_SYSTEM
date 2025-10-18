# -------------------------------------------------------------------------
# 📁 datastore/influx_client.py
# (依赖: pip install "influxdb-client-python[async]")
# -------------------------------------------------------------------------
import asyncio
import logging
from typing import Dict, Any

# 导入 InfluxDB 异步客户端库
import influxdb_client.client.write_api_async
from influxdb_client.client.influxdb_client_async import InfluxDBClientAsync
from influxdb_client import Point
from influxdb_client.client.exceptions import InfluxDBError

from shared.config import settings

log = logging.getLogger(__name__)

# =========================================================================
#  📊 InfluxDB v2 客户端封装
# =========================================================================

class InfluxClient:
    """
    V3 架构的异步 InfluxDB 客户端封装。
    
    用于将时间序列数据 (TICKERS, SPREADS, PNL, ALERTS) 写入
    数据库，供 Grafana 进行可视化。
    """
    
    def __init__(self, url: str, token: str, org: str, bucket: str):
        self.url = url
        self.token = token
        self.org = org
        self.bucket = bucket
        
        self.client: InfluxDBClientAsync | None = None
        self.write_api: influxdb_client.client.write_api_async.WriteApiAsync | None = None
        log.info(f"InfluxDB 客户端正在初始化... URL: {self.url} Org: {self.org} Bucket: {self.bucket}")

    async def connect(self):
        """
        连接到 InfluxDB 并检查健康状况
        """
        try:
            self.client = InfluxDBClientAsync(url=self.url, token=self.token, org=self.org)
            
            # ❗️ 关键: 检查 InfluxDB 是否可达
            health = await self.client.health()
            if health.status != "pass":
                raise InfluxDBError(response=None, message=f"InfluxDB health check failed: {health.message}")
                
            # 初始化异步写入 API
            self.write_api = self.client.write_api()
            
            log.info(f"✅ [InfluxDB] 连接成功 (Health: {health.status})")

        except Exception as e:
            log.critical(f"❗️ [InfluxDB] 严重错误: 无法连接到 InfluxDB: {e}")
            log.critical(f"❗️ ... 确保 InfluxDB 正在运行于 {self.url} 并且 TOKEN/ORG 配置正确。")
            log.critical(f"❗️ ... 可视化功能 (Grafana) 将无法工作。")
            # (注意: 我们不在此处引发异常，允许服务在没有 InfluxDB 的情况下降级运行)
            self.client = None
            self.write_api = None

    async def close(self):
        """
        关闭 InfluxDB 客户端和写入 API
        """
        if self.write_api:
            await self.write_api.close()
            log.info("[InfluxDB] WriteAPI 已关闭。")
        if self.client:
            await self.client.close()
            log.info("[InfluxDB] 客户端已关闭。")

    async def write(self, measurement: str, tags: Dict[str, str], fields: Dict[str, Any]):
        """
        🚀 异步写入一个数据点 (Point) 到 InfluxDB
        
        :param measurement: 表名 (例如 "market_data" 或 "trades")
        :param tags: 索引列 (例如 {"symbol": "ETH/USDT", "exchange": "binance"})
        :param fields: 数据列 (例如 {"price": 3000.50, "qty": 0.1})
        """
        
        # 如果连接失败 (在 connect() 中被设为 None)，则跳过写入
        if not self.write_api:
            # log.warning("[InfluxDB] 未连接，跳过写入。")
            return
            
        try:
            point = Point(measurement)
            
            # 1. 添加 Tags (索引)
            for tag_key, tag_val in tags.items():
                if tag_val is not None:
                    point.tag(tag_key, str(tag_val))
                    
            # 2. 添加 Fields (数据)
            # ❗️ 关键: 必须正确处理数据类型
            has_fields = False
            for field_key, field_val in fields.items():
                if field_val is None:
                    continue
                    
                if isinstance(field_val, (int, float)):
                    point.field(field_key, float(field_val))
                    has_fields = True
                elif isinstance(field_val, bool):
                    point.field(field_key, field_val)
                    has_fields = True
                elif isinstance(field_val, str):
                    point.field(field_key, field_val)
                    has_fields = True
            
            # 3. 异步写入
            if has_fields:
                await self.write_api.write(bucket=self.bucket, org=self.org, record=point)
                # log.debug(f"[InfluxDB] 写入成功: {point.to_line_protocol()}")
            else:
                log.warning(f"[InfluxDB] 尝试写入 {measurement}，但没有有效的 fields。")

        except Exception as e:
            # (通常是 InfluxDBError)
            log.error(f"[InfluxDB] 写入点时失败 ({measurement}): {e}", exc_info=False)


# =========================================================================
#  全局 InfluxDB 客户端实例
# =========================================================================

# 📦 在项目的任何地方 `from datastore.influx_client import influx_client` 即可使用
influx_client = InfluxClient(
    url=settings.datastore.INFLUXDB_URL,
    token=settings.datastore.INFLUXDB_TOKEN,
    org=settings.datastore.INFLUXDB_ORG,
    bucket=settings.datastore.INFLUXDB_BUCKET
)