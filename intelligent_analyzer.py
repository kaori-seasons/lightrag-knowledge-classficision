from typing import Dict, Any

from lightrag import QueryParam

from business_integration import BusinessResourceIntegrator
from fault_analysis_system import FaultAnalysisSystem


class IntelligentFaultAnalyzer:
    """智能故障分析器"""

    def __init__(self, fault_system: FaultAnalysisSystem):
        self.fault_system = fault_system
        self.analysis_templates = self._load_analysis_templates()
        self.business_integrator = BusinessResourceIntegrator(fault_system)

    def _load_analysis_templates(self) -> Dict[str, str]:
        """加载分析模板"""
        return {
            "root_cause_analysis": """  
            基于故障库中的历史数据，请对以下故障进行根本原因分析：  

            故障信息：{fault_info}  

            请从以下角度进行分析：  
            1. 直接原因分析  
            2. 根本原因追溯  
            3. 系统性原因识别  
            4. 管理原因分析  
            5. 类似故障案例对比  

            请提供详细的分析报告。  
            """,

            "preventive_measures": """  
            基于故障：{fault_info}  

            请结合历史类似故障案例，提供以下预防措施建议：  
            1. 技术预防措施  
            2. 管理预防措施  
            3. 培训预防措施  
            4. 检查预防措施  
            5. 应急预案优化建议  

            请提供具体可执行的措施建议。  
            """,

            "comprehensive_report": """  
            请基于故障库数据，生成关于以下故障的综合分析报告：  

            故障信息：{fault_info}  

            报告应包含：  
            1. 故障概述  
            2. 现象分析  
            3. 原因分析（直接原因、根本原因、系统原因）  
            4. 影响评估（安全、生产、经济影响）  
            5. 处理措施评价  
            6. 类似故障统计分析  
            7. 预防改进建议  
            8. 经验教训总结  

            请生成详细的长文本分析报告。  
            """
        }

    async def analyze_fault_by_code(self, accident_code: str, analysis_type: str = "comprehensive_report") -> str:
        """根据事故编码分析故障"""
        # 首先查询该故障的详细信息
        fault_query = f"事故编码 {accident_code} 的详细信息"
        fault_info = await self.fault_system.rag.aquery(
            fault_query,
            param=QueryParam(mode="local")
        )

        # 使用模板生成分析查询
        template = self.analysis_templates.get(analysis_type, self.analysis_templates["comprehensive_report"])
        analysis_query = template.format(fault_info=fault_info)

        # 执行综合分析
        analysis_result = await self.fault_system.rag.aquery(
            analysis_query,
            param=QueryParam(mode="hybrid")
        )

        return analysis_result

    async def analyze_similar_faults(self, device_name: str, fault_type: str) -> str:
        """分析类似故障"""
        query = f"分析设备 {device_name} 发生的 {fault_type} 类型故障的共性问题和规律"

        result = await self.fault_system.rag.aquery(
            query,
            param=QueryParam(mode="global")
        )

        return result

    async def generate_trend_analysis(self, time_period: str, area: str = None) -> str:
        """生成趋势分析"""
        if area:
            query = f"分析 {area} 区域在 {time_period} 期间的故障趋势和特点"
        else:
            query = f"分析 {time_period} 期间的整体故障趋势和特点"

        result = await self.fault_system.rag.aquery(
            query,
            param=QueryParam(mode="global")
        )

        return result

    async def get_maintenance_recommendations(self, device_type: str) -> str:
        """获取维护建议"""
        query = f"基于历史故障数据，为 {device_type} 设备提供维护保养建议和预防措施"

        result = await self.fault_system.rag.aquery(
            query,
            param=QueryParam(mode="hybrid")
        )

        return result

    async def _get_fault_basic_info(self, accident_code: str) -> str:
        """获取故障基础信息"""
        # 构建查询语句，获取指定事故编码的详细信息
        query = f"""  
        请查询事故编码为 {accident_code} 的故障记录，包括以下信息：  
        1. 设备名称和编码  
        2. 故障发生时间  
        3. 故障区域  
        4. 故障现象描述  
        5. 事故等级  
        6. 停机时长  
        7. 根本原因  
        8. 处理措施  
        9. 处理人员  
        10. 损失情况  

        请提供完整的故障基础信息。  
        """

        try:
            # 使用本地模式查询，因为我们要查找特定的故障记录
            basic_info = await self.fault_system.rag.aquery(
                query,
                param=QueryParam(mode="local")
            )

            if not basic_info or basic_info.strip() == "":
                # 如果本地查询没有结果，尝试使用混合模式
                basic_info = await self.fault_system.rag.aquery(
                    f"事故编码 {accident_code} 的详细信息",
                    param=QueryParam(mode="hybrid")
                )

            return basic_info if basic_info else f"未找到事故编码 {accident_code} 的相关信息"

        except Exception as e:
            print(f"获取故障基础信息失败: {e}")
            return f"查询事故编码 {accident_code} 时发生错误: {str(e)}"

    def _extract_device_code(self, fault_info: str) -> str:
        """从故障信息中提取设备编码"""
        # 实现设备编码提取逻辑
        import re
        pattern = r'设备编码[：:]\s*([A-Z0-9\-]+)'
        match = re.search(pattern, fault_info)
        return match.group(1) if match else ""

    def _extract_area_name(self, fault_info: str) -> str:
        """从故障信息中提取区域名称"""
        import re
        pattern = r'区域[：:]\s*([^，。\n]+)'
        match = re.search(pattern, fault_info)
        return match.group(1).strip() if match else ""

    async def _generate_analysis_with_external_data(self, fault_info: str, external_data: Dict) -> str:
        """结合外部数据生成分析"""

        analysis_prompt = f"""  
        基于以下故障信息和外部系统数据，生成详细的综合分析报告：  

        【故障基础信息】  
        {fault_info}  

        【维护系统数据】  
        {external_data.get('maintenance_data', {}).get('data', '无数据')}  

        【隐患系统数据】  
        {external_data.get('hazard_data', {}).get('data', '无数据')}  

        【点检系统数据】  
        {external_data.get('inspection_data', {}).get('data', '无数据')}  

        请结合以上多源数据进行分析，重点关注：  
        1. 故障与历史维护记录的关联性  
        2. 是否存在相关的未处理隐患  
        3. 点检数据中的异常趋势  
        4. 跨系统数据的一致性分析  
        5. 基于多源数据的根本原因判断  
        6. 综合预防措施建议  

        请生成详细的分析报告。  
        """

        result = await self.fault_system.rag.aquery(
            analysis_prompt,
            param=QueryParam(mode="hybrid")
        )

        return result

    async def analyze_fault_with_external_resources(self, accident_code:str) -> Dict[str, Any]:
        """结合外部资源进行故障分析"""

        #1. 获取基础故障信息
        fault_info = await self._get_fault_basic_info(accident_code)


        #2. 提取设备和区域信息
        device_code = self._extract_device_code(fault_info)
        area_name = self._extract_area_name(fault_info)

        #3. 调用外部资源
        external_params = {
            "device_code": device_code,
            "area_name": area_name,
            "accident_code": accident_code
        }

        # 并发调用多个外部资源
        external_results = await self.business_integrator.call_external_resources(
            "combined_analysis", external_params
        )

        #4.生成综合分析
        comprehensive_analysis = await self._generate_analysis_with_external_data(
            fault_info, external_results
        )

        return {
            "accident_code": accident_code,
            "fault_info": fault_info,
            "external_resources": external_results,
            "comprehensive_analysis": comprehensive_analysis
        }
