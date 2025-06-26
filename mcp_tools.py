from datetime import datetime
from typing import Dict, List, Any, Optional
import json

from database_connector import DatabaseConnectionManager


class MCPDatabaseTool:
    """MCP数据库查询工具"""

    def __init__(self, db_manager: DatabaseConnectionManager):
        self.db_manager = db_manager
        self.tool_definitions = {
            "maintenance_query": {
                "name": "query_maintenance_records",
                "description": "查询设备维护记录",
                "parameters": {
                    "device_code": {"type": "string", "description": "设备编码"},
                    "start_date": {"type": "string", "description": "开始日期"},
                    "end_date": {"type": "string", "description": "结束日期"},
                    "maintenance_type": {"type": "string", "description": "维护类型"}
                }
            },
            "hazard_query": {
                "name": "query_hazard_records",
                "description": "查询安全隐患记录",
                "parameters": {
                    "device_code": {"type": "string", "description": "设备编码"},
                    "area_code": {"type": "string", "description": "区域编码"},
                    "risk_level": {"type": "string", "description": "风险等级"},
                    "status": {"type": "string", "description": "处理状态"}
                }
            },
            "inspection_query": {
                "name": "query_inspection_records",
                "description": "查询点检记录",
                "parameters": {
                    "device_code": {"type": "string", "description": "设备编码"},
                    "inspection_type": {"type": "string", "description": "点检类型"},
                    "start_date": {"type": "string", "description": "开始日期"},
                    "end_date": {"type": "string", "description": "结束日期"}
                }
            }
        }

    async def execute_tool(self, tool_name: str, parameters: Dict[str, Any]) -> Dict[str, Any]:
        """执行MCP工具"""
        try:
            if tool_name == "query_maintenance_records":
                return await self._query_maintenance_records(parameters)
            elif tool_name == "query_hazard_records":
                return await self._query_hazard_records(parameters)
            elif tool_name == "query_inspection_records":
                return await self._query_inspection_records(parameters)
            else:
                return {"status": "error", "message": f"未知工具: {tool_name}"}
        except Exception as e:
            return {"status": "error", "message": str(e)}

    async def _query_maintenance_records(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """查询维护记录"""
        conn = await self.db_manager.get_connection("maintenance_db")

        sql = """  
        SELECT   
            m.maintenance_id,  
            m.device_code,  
            m.maintenance_type,  
            m.maintenance_date,  
            m.maintenance_content,  
            m.maintenance_person,  
            m.maintenance_cost,  
            m.next_maintenance_date,  
            d.device_name,  
            d.device_model  
        FROM maintenance_records m  
        LEFT JOIN device_info d ON m.device_code = d.device_code  
        WHERE 1=1  
        """

        query_params = []

        if params.get("device_code"):
            sql += " AND m.device_code = $1"
            query_params.append(params["device_code"])

        if params.get("start_date"):
            sql += f" AND m.maintenance_date >= ${len(query_params) + 1}"
            query_params.append(params["start_date"])

        if params.get("end_date"):
            sql += f" AND m.maintenance_date <= ${len(query_params) + 1}"
            query_params.append(params["end_date"])

        if params.get("maintenance_type"):
            sql += f" AND m.maintenance_type = ${len(query_params) + 1}"
            query_params.append(params["maintenance_type"])

        sql += " ORDER BY m.maintenance_date DESC LIMIT 100"

        rows = await conn.fetch(sql, *query_params)

        records = []
        for row in rows:
            records.append({
                "maintenance_id": row["maintenance_id"],
                "device_code": row["device_code"],
                "device_name": row["device_name"],
                "device_model": row["device_model"],
                "maintenance_type": row["maintenance_type"],
                "maintenance_date": row["maintenance_date"].isoformat() if row["maintenance_date"] else None,
                "maintenance_content": row["maintenance_content"],
                "maintenance_person": row["maintenance_person"],
                "maintenance_cost": float(row["maintenance_cost"]) if row["maintenance_cost"] else 0,
                "next_maintenance_date": row["next_maintenance_date"].isoformat() if row[
                    "next_maintenance_date"] else None
            })

        return {
            "status": "success",
            "data": records,
            "count": len(records),
            "query_time": datetime.now().isoformat()
        }

    async def _query_hazard_records(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """查询隐患记录"""
        conn = await self.db_manager.get_connection("hazard_db")

        # MySQL查询语法
        sql = """  
        SELECT   
            h.hazard_id,  
            h.device_code,  
            h.area_code,  
            h.hazard_description,  
            h.risk_level,  
            h.discovery_date,  
            h.discovery_person,  
            h.status,  
            h.rectification_measures,  
            h.rectification_deadline,  
            h.rectification_person,  
            a.area_name  
        FROM hazard_records h  
        LEFT JOIN area_info a ON h.area_code = a.area_code  
        WHERE 1=1  
        """

        query_params = []

        if params.get("device_code"):
            sql += " AND h.device_code = %s"
            query_params.append(params["device_code"])

        if params.get("area_code"):
            sql += " AND h.area_code = %s"
            query_params.append(params["area_code"])

        if params.get("risk_level"):
            sql += " AND h.risk_level = %s"
            query_params.append(params["risk_level"])

        if params.get("status"):
            sql += " AND h.status = %s"
            query_params.append(params["status"])

        sql += " ORDER BY h.discovery_date DESC LIMIT 100"

        cursor = await conn.cursor()
        await cursor.execute(sql, query_params)
        rows = await cursor.fetchall()

        # 获取列名
        columns = [desc[0] for desc in cursor.description]

        records = []
        for row in rows:
            record = dict(zip(columns, row))
            # 处理日期字段
            if record.get("discovery_date"):
                record["discovery_date"] = record["discovery_date"].isoformat()
            if record.get("rectification_deadline"):
                record["rectification_deadline"] = record["rectification_deadline"].isoformat()
            records.append(record)

        await cursor.close()

        return {
            "status": "success",
            "data": records,
            "count": len(records),
            "query_time": datetime.now().isoformat()
        }

    async def _query_inspection_records(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """查询点检记录"""
        conn = await self.db_manager.get_connection("inspection_db")

        sql = """  
        SELECT   
            i.inspection_id,  
            i.device_code,  
            i.inspection_type,  
            i.inspection_date,  
            i.inspection_person,  
            i.inspection_items,  
            i.inspection_results,  
            i.abnormal_items,  
            i.handling_measures,  
            i.next_inspection_date,  
            d.device_name,  
            d.device_model  
        FROM inspection_records i  
        LEFT JOIN device_info d ON i.device_code = d.device_code  
        WHERE 1=1  
        """

        query_params = []

        if params.get("device_code"):
            sql += " AND i.device_code = $1"
            query_params.append(params["device_code"])

        if params.get("inspection_type"):
            sql += f" AND i.inspection_type = ${len(query_params) + 1}"
            query_params.append(params["inspection_type"])

        if params.get("start_date"):
            sql += f" AND i.inspection_date >= ${len(query_params) + 1}"
            query_params.append(params["start_date"])

        if params.get("end_date"):
            sql += f" AND i.inspection_date <= ${len(query_params) + 1}"
            query_params.append(params["end_date"])

        sql += " ORDER BY i.inspection_date DESC LIMIT 100"

        rows = await conn.fetch(sql, *query_params)

        records = []
        for row in rows:
            records.append({
                "inspection_id": row["inspection_id"],
                "device_code": row["device_code"],
                "device_name": row["device_name"],
                "device_model": row["device_model"],
                "inspection_type": row["inspection_type"],
                "inspection_date": row["inspection_date"].isoformat() if row["inspection_date"] else None,
                "inspection_person": row["inspection_person"],
                "inspection_items": row["inspection_items"],
                "inspection_results": row["inspection_results"],
                "abnormal_items": row["abnormal_items"],
                "handling_measures": row["handling_measures"],
                "next_inspection_date": row["next_inspection_date"].isoformat() if row["next_inspection_date"] else None
            })

        return {
            "status": "success",
            "data": records,
            "count": len(records),
            "query_time": datetime.now().isoformat()
        }