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
        # 这里可以集成实际的外部API调用
        mock_responses = {
            "maintenance_system": {
                "status": "success",
                "data": f"维护系统响应: 设备 {params.get('device')} 的维护记录已查询",
                "recommendations": ["定期检查润滑系统", "更换磨损部件", "加强操作培训"]
            },
            "inventory_system": {
                "status": "success",
                "data": f"库存系统响应: {params.get('part')} 库存充足",
                "availability": True
            },
            "expert_system": {
                "status": "success",
                "data": "专家系统分析完成",
                "expert_advice": "建议立即停机检修，避免进一步损坏"
            }
        }

        return mock_responses.get(resource_type, {"status": "error", "message": "未知资源类型"})

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
