
import asyncio
import asyncpg
import aiomysql
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
import os


@dataclass
class DatabaseConfig:
    """数据库配置"""
    host: str
    port: str
    user: str
    password: str
    database: str
    database: str #'postgresql' or 'mysql'


class DatabaseConnectionManager:
    """数据库连接管理器"""

    def __init__(self):
        self.connections = {}
        self.db_configs = {
            "maintenance_db": DatabaseConfig(
                host=os.getenv("MAINTENANCE_DB_HOST", "localhost"),
                port=int(os.getenv("MAINTENANCE_DB_PORT", "5432")),
                user=os.getenv("MAINTENANCE_DB_USER", "postgres"),
                password=os.getenv("MAINTENANCE_DB_PASSWORD", ""),
                database=os.getenv("MAINTENANCE_DB_NAME", "maintenance"),
                db_type="postgresql"
            ),
            "hazard_db": DatabaseConfig(
                host=os.getenv("HAZARD_DB_HOST", "localhost"),
                port=int(os.getenv("HAZARD_DB_PORT", "3306")),
                user=os.getenv("HAZARD_DB_USER", "root"),
                password=os.getenv("HAZARD_DB_PASSWORD", ""),
                database=os.getenv("HAZARD_DB_NAME", "hazard"),
                db_type="mysql"
            ),
            "inspection_db": DatabaseConfig(
                host=os.getenv("INSPECTION_DB_HOST", "localhost"),
                port=int(os.getenv("INSPECTION_DB_PORT", "5432")),
                user=os.getenv("INSPECTION_DB_USER", "postgres"),
                password=os.getenv("INSPECTION_DB_PASSWORD", ""),
                database=os.getenv("INSPECTION_DB_NAME", "inspection"),
                db_type="postgresql"
            )
        }

    async def get_connection(self, db_name: str):
        """获取数据库连接"""
        if db_name not in self.connections:
            config = self.db_configs[db_name]

            if config.db_type == "postgresql":
                self.connections[db_name] = await asyncpg.connect(
                    host=config.host,
                    port=config.port,
                    user=config.user,
                    password=config.password,
                    database=config.database
                )
            elif config.db_type == "mysql":
                self.connections[db_name] = await aiomysql.connect(
                    host=config.host,
                    port=config.port,
                    user=config.user,
                    password=config.password,
                    db=config.database
                )

        return self.connections[db_name]

    async def close_all_connections(self):
        """关闭所有连接"""
        for conn in self.connections.values():
            await conn.close()
        self.connections.clear()