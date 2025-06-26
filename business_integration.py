import asyncio
from datetime import datetime
from typing import Dict, Any, Optional, List

from database_connector import DatabaseConnectionManager
from fault_analysis_system import FaultAnalysisSystem
from lightrag.llm.ollama import ollama_model_complete

from mcp_tools import MCPDatabaseTool


class BusinessResourceIntegrator:
    """业务领域资源整合器"""

    def __init__(self, fault_system: FaultAnalysisSystem):
        self.fault_system = fault_system
        self.business_knowledge = self._load_business_knowledge()
        self.db_manager = DatabaseConnectionManager()
        self.mcp_tools = MCPDatabaseTool(self.db_manager)

        self.external_systems = {
            "maintenance_db": {
                "url": "http://maintenance-api:8080/api/v1",
                "auth_token": "your_Maintenance_token",
                "timeout": 30,
                "retry_count": 3,
                "description": "设备维护管理系统"
            },
            #台账库 查找相关的维护记录
            "hazard_db": {
                "url": "http://hazard-api"
            }

            #维保库

            #巡检库
        }

    def _load_business_knowledge(self) -> Dict[str, str]:
        """加载业务领域知识"""
        return {
            "maintenance_standards": """  
            设备维护标准规范：  
            1. 轧机设备每日检查项目  
            2. 润滑系统维护周期  
            3. 关键部件更换标准  
            4. 预防性维护计划  
            """,

            "safety_regulations": """  
            安全操作规程：  
            1. 设备操作安全要求  
            2. 应急处理程序  
            3. 人员安全防护措施  
            4. 事故报告流程  
            """,

            "technical_specifications": """  
            技术规格要求：  
            1. 设备技术参数  
            2. 工艺操作标准  
            3. 质量控制要求  
            4. 性能指标标准  
            """
        }


    def add_external_system(self, system_name:str, config:Dict[str, Any]) -> bool:
        """添加新的外部系统配置"""
        try:
            required_fields = ["url", "auth_token"]
            if not all(field in config for field in required_fields):
                raise ValueError(f"配置缺少字段: {required_fields}")

            default_config = {
                "timeout": 30,
                "retry_count": 3,
                "description": f"{system_name} 系统"
            }


            #合并配置
            final_config = {**default_config, **config}
            self.external_systems[system_name] = final_config

            return True
        except Exception as e:
            print(f"添加外部系统配置失败: {e}")

    def remove_external_system(self, system_name: str) -> bool:
        """移除外部系统配置"""
        try:
            if system_name in self.external_systems:
                del self.external_systems[system_name]
                return True
            return False
        except Exception as e:
            print(f"移除外部系统配置失败: {e}")
            return False

    def update_external_system(self, system_name: str, config: Dict[str, Any]) -> bool:
        """更新外部系统配置"""
        try:
            if system_name not in self.external_systems:
                raise ValueError(f"系统 {system_name} 不存在")

                # 更新配置
            self.external_systems[system_name].update(config)
            return True
        except Exception as e:
            print(f"更新外部系统配置失败: {e}")
            return False

    def get_external_system_config(self, system_name: str) -> Optional[Dict[str, Any]]:
        """获取外部系统配置"""
        return self.external_systems.get(system_name)

    def list_external_systems(self) -> List[str]:
        """列出所有外部系统名称"""
        return list(self.external_systems.keys())

    def validate_system_config(self, config: Dict[str, Any]) -> bool:
        """验证系统配置的有效性"""
        required_fields = ["url", "auth_token"]
        return all(field in config for field in required_fields)

    async def integrate_business_knowledge(self):
        """将业务知识集成到RAG系统"""
        print("正在集成业务领域知识...")

        for domain, knowledge in self.business_knowledge.items():
            await self.fault_system.rag.ainsert(
                knowledge,
                file_paths=f"business_knowledge_{domain}.txt,"
            )

            print("业务领域知识集成完成！")

    async def call_external_resources(self, resource_type: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """模拟调用外部业务资源"""
        """调用外部业务资源 - 支持HTTP API和直接数据库查询"""
        try:
            # 优先使用MCP工具进行直接数据库查询
            if resource_type == "maintenance_system":
                return await self._call_maintenance_with_mcp(params)
            elif resource_type == "hazard_system":
                return await self._call_hazard_with_mcp(params)
            elif resource_type == "inspection_system":
                return await self._call_inspection_with_mcp(params)
            elif resource_type == "combined_analysis":
                return await self._call_combined_resources_with_mcp(params)
            else:
                return {"status": "error", "message": f"未知资源类型: {resource_type}"}
        except Exception as e:
            # 如果MCP工具失败，回退到HTTP API调用
            print(f"MCP工具调用失败，回退到HTTP API: {e}")
            return await self._fallback_to_http_api(resource_type, params)

    async def _call_maintenance_with_mcp(self, params: Dict[str, Any]):
        """使用MCP工具查询维护数据库"""
        try:
            mcp_params = {
                "device_code": params.get("device_code"),
                "start_date": params.get("start_date"),
                "end_date": params.get("end_date"),
                "maintenance_type": params.get("maintenance_type")
            }

            #调用MCP工具
            result = await self.mcp_tools.execute_tool("query_maintenance_records", mcp_params)

            if result.get("status") == "success":
                #增强结果数据
                enhanced_result = self._enhance_maintenance_data(result["data"])
                return {
                    "status": "success",
                    "source": "maintenance_db_mcp",
                    "data": enhanced_result,
                    "maintenance_records": result["data"],
                    "record_count": result["count"],
                    "query_method": "direct_database",
                    "query_time": result["query_time"]
                }
            else:
                return result
        except Exception as e:
            raise e

    async def _call_hazard_with_mcp(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """使用MCP工具查询隐患数据库"""
        try:
            mcp_params = {
                "device_code": params.get("device_code"),
                "area_code": params.get("area_name"),  # 映射area_name到area_code
                "risk_level": params.get("risk_level"),
                "status": params.get("status", "active")
            }

            result = await self.mcp_tools.execute_tool("query_hazard_records", mcp_params)

            if result.get("status") == "success":
                # 分析风险等级分布
                risk_analysis = await self._analyze_risk_distribution(result["data"])

                return {
                    "status": "success",
                    "source": "hazard_db_mcp",
                    "data": result["data"],
                    "active_hazards": [h for h in result["data"] if h.get("status") == "active"],
                    "risk_analysis": risk_analysis,
                    "record_count": result["count"],
                    "query_method": "direct_database",
                    "query_time": result["query_time"]
                }
            else:
                return result

        except Exception as e:
            return {"status": "error", "message": f"隐患数据库MCP查询失败: {str(e)}"}

    async def _call_inspection_with_mcp(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """使用MCP工具查询点检数据库"""
        try:
            mcp_params = {
                "device_code": params.get("device_code"),
                "inspection_type": params.get("inspection_type"),
                "start_date": params.get("start_date"),
                "end_date": params.get("end_date")
            }

            result = await self.mcp_tools.execute_tool("query_inspection_records", mcp_params)

            if result.get("status") == "success":
                # 分析异常趋势
                trend_analysis = await self._analyze_inspection_trends(result["data"])

                return {
                    "status": "success",
                    "source": "inspection_db_mcp",
                    "data": result["data"],
                    "inspection_records": result["data"],
                    "abnormal_items": [item for record in result["data"]
                                       for item in (record.get("abnormal_items", "") or "").split(",")
                                       if item.strip()],
                    "trend_analysis": trend_analysis,
                    "record_count": result["count"],
                    "query_method": "direct_database",
                    "query_time": result["query_time"]
                }
            else:
                return result

        except Exception as e:
            return {"status": "error", "message": f"点检数据库MCP查询失败: {str(e)}"}

    async def _call_combined_resources_with_mcp(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """使用MCP工具组合查询多个数据库"""
        try:
            # 并发调用多个MCP工具
            tasks = [
                self._call_maintenance_with_mcp(params),
                self._call_hazard_with_mcp(params),
                self._call_inspection_with_mcp(params)
            ]

            results = await asyncio.gather(*tasks, return_exceptions=True)

            # 处理结果
            maintenance_data = results[0] if not isinstance(results[0], Exception) else None
            hazard_data = results[1] if not isinstance(results[1], Exception) else None
            inspection_data = results[2] if not isinstance(results[2], Exception) else None

            # 生成综合分析
            combined_analysis = await self._generate_combined_mcp_analysis(
                maintenance_data, hazard_data, inspection_data, params
            )

            return {
                "status": "success",
                "source": "combined_mcp_resources",
                "maintenance_data": maintenance_data,
                "hazard_data": hazard_data,
                "inspection_data": inspection_data,
                "combined_analysis": combined_analysis,
                "query_method": "direct_database_combined",
                "query_time": datetime.now().isoformat()
            }

        except Exception as e:
            return {"status": "error", "message": f"组合MCP查询失败: {str(e)}"}

    def _enhance_maintenance_data(self, maintenance_records: List[Dict]) -> List[Dict]:
        """增强维护数据"""
        enhanced_records = []

        for record in maintenance_records:
            enhanced_record = record.copy()

            # 计算维护间隔
            if record.get("maintenance_date") and record.get("next_maintenance_date"):
                try:
                    current_date = datetime.fromisoformat(record["maintenance_date"].replace('Z', '+00:00'))
                    next_date = datetime.fromisoformat(record["next_maintenance_date"].replace('Z', '+00:00'))
                    interval_days = (next_date - current_date).days
                    enhanced_record["maintenance_interval_days"] = interval_days
                except:
                    enhanced_record["maintenance_interval_days"] = None

                    # 维护成本分析
            cost = record.get("maintenance_cost", 0)
            if cost > 0:
                if cost > 10000:
                    enhanced_record["cost_level"] = "高"
                elif cost > 5000:
                    enhanced_record["cost_level"] = "中"
                else:
                    enhanced_record["cost_level"] = "低"

            enhanced_records.append(enhanced_record)

        return enhanced_records

    async def _analyze_risk_distribution(self, hazard_records: List[Dict]) -> Dict[str, Any]:
        """分析风险等级分布"""
        risk_distribution = {"高": 0, "中": 0, "低": 0, "未知": 0}
        status_distribution = {}

        for record in hazard_records:
            risk_level = record.get("risk_level", "未知")
            if risk_level in risk_distribution:
                risk_distribution[risk_level] += 1
            else:
                risk_distribution["未知"] += 1

            status = record.get("status", "未知")
            status_distribution[status] = status_distribution.get(status, 0) + 1

        return {
            "risk_distribution": risk_distribution,
            "status_distribution": status_distribution,
            "total_hazards": len(hazard_records),
            "high_risk_count": risk_distribution["高"],
            "active_hazards": status_distribution.get("active", 0)
        }

    async def _analyze_inspection_trends(self, inspection_records: List[Dict]) -> Dict[str, Any]:
        """分析点检趋势"""
        trend_data = {
            "total_inspections": len(inspection_records),
            "abnormal_count": 0,
            "inspection_frequency": {},
            "abnormal_trend": []
        }

        for record in inspection_records:
            # 统计异常项
            abnormal_items = record.get("abnormal_items", "")
            if abnormal_items and abnormal_items.strip():
                trend_data["abnormal_count"] += 1

                # 统计点检频率
            inspection_type = record.get("inspection_type", "未知")
            trend_data["inspection_frequency"][inspection_type] = \
                trend_data["inspection_frequency"].get(inspection_type, 0) + 1

            # 计算异常率
        if trend_data["total_inspections"] > 0:
            trend_data["abnormal_rate"] = trend_data["abnormal_count"] / trend_data["total_inspections"]
        else:
            trend_data["abnormal_rate"] = 0

        return trend_data

    async def _generate_combined_mcp_analysis(self, maintenance_data, hazard_data,
                                              inspection_data, params) -> Dict[str, Any]:
        """生成MCP组合分析"""
        analysis = {
            "device_code": params.get("device_code"),
            "analysis_time": datetime.now().isoformat(),
            "data_sources": [],
            "key_insights": [],
            "risk_indicators": [],
            "recommendations": []
        }

        # 分析维护数据
        if maintenance_data and maintenance_data.get("status") == "success":
            analysis["data_sources"].append("maintenance_db")
            records = maintenance_data.get("maintenance_records", [])
            if records:
                latest_maintenance = max(records, key=lambda x: x.get("maintenance_date", ""))
                analysis["key_insights"].append(f"最近维护: {latest_maintenance.get('maintenance_date')}")

                # 检查维护频率
                if len(records) < 3:
                    analysis["risk_indicators"].append("维护频率偏低")
                    analysis["recommendations"].append("建议增加维护频率")

                    # 分析隐患数据
        if hazard_data and hazard_data.get("status") == "success":
            analysis["data_sources"].append("hazard_db")
            risk_analysis = hazard_data.get("risk_analysis", {})
            high_risk_count = risk_analysis.get("high_risk_count", 0)

            if high_risk_count > 0:
                analysis["risk_indicators"].append(f"存在 {high_risk_count} 个高风险隐患")
                analysis["recommendations"].append("优先处理高风险隐患")

                # 分析点检数据
        if inspection_data and inspection_data.get("status") == "success":
            analysis["data_sources"].append("inspection_db")
            trend_analysis = inspection_data.get("trend_analysis", {})
            abnormal_rate = trend_analysis.get("abnormal_rate", 0)

            if abnormal_rate > 0.3:  # 异常率超过30%
                analysis["risk_indicators"].append(f"点检异常率高达 {abnormal_rate:.1%}")
                analysis["recommendations"].append("加强设备监控和预防性维护")

        return analysis
