from datetime import datetime
from pathlib import Path
from typing import Dict, Any

from lightrag import QueryParam

from fault_analysis_system import FaultAnalysisSystem
from intelligent_analyzer import IntelligentFaultAnalyzer


class FaultReportGenerator:
    """故障分析报告生成器"""

    def __init__(self, fault_system: FaultAnalysisSystem, analyzer: IntelligentFaultAnalyzer):
        self.fault_system = fault_system
        self.analyzer = analyzer

    async def generate_comprehensive_report(self, accident_code: str) -> str:
        """生成综合故障分析报告"""

        # 1. 获取基础故障信息
        basic_info = await self._get_fault_basic_info(accident_code)

        # 2. 进行多维度分析
        analyses = await self._perform_multi_dimensional_analysis(accident_code)

        # 3. 生成长窗口推理文本
        comprehensive_report = await self._generate_long_context_reasoning(
            basic_info, analyses
        )

        # 4. 保存报告
        report_path = await self._save_report(accident_code, comprehensive_report)

        return comprehensive_report

    async def _get_fault_basic_info(self, accident_code: str) -> Dict[str, Any]:
        """获取故障基础信息"""
        query = f"查询事故编码 {accident_code} 的基本信息，包括设备、时间、现象、原因等"

        basic_info = await self.fault_system.rag.aquery(
            query,
            param=QueryParam(mode="local")
        )

        return {"accident_code": accident_code, "basic_info": basic_info}

    async def _perform_multi_dimensional_analysis(self, accident_code: str) -> Dict[str, str]:
        """执行多维度分析"""
        analyses = {}

        # 根本原因分析
        analyses["root_cause"] = await self.analyzer.analyze_fault_by_code(
            accident_code, "root_cause_analysis"
        )

        # 预防措施分析
        analyses["preventive_measures"] = await self.analyzer.analyze_fault_by_code(
            accident_code, "preventive_measures"
        )

        # 类似故障分析
        device_query = f"获取事故编码 {accident_code} 的设备信息"
        device_info = await self.fault_system.rag.aquery(
            device_query,
            param=QueryParam(mode="local")
        )

        analyses["similar_faults"] = await self.analyzer.analyze_similar_faults(
            "轧机", "设备故障"  # 基于示例数据
        )

        return analyses

    async def _generate_long_context_reasoning(self, basic_info: Dict, analyses: Dict) -> str:
        """生成长窗口推理文本"""

        reasoning_prompt = f"""  
        基于以下故障信息和多维度分析结果，生成一份详细的故障分析推理报告：  

        【基础信息】  
        {basic_info['basic_info']}  

        【根本原因分析】  
        {analyses['root_cause']}  

        【预防措施建议】  
        {analyses['preventive_measures']}  

        【类似故障分析】  
        {analyses['similar_faults']}  

        请生成一份包含以下内容的长文本推理报告：  

        1. 执行摘要（200字）  
        2. 故障详细描述与时间线分析（500字）  
        3. 多层次原因分析（800字）  
           - 直接原因  
           - 根本原因  
           - 系统性原因  
           - 管理原因  
        4. 影响评估与损失分析（400字）  
        5. 处理措施有效性评价（300字）  
        6. 类似故障对比与规律总结（600字）  
        7. 预防改进措施与建议（500字）  
        8. 经验教训与知识沉淀（300字）  
        9. 后续行动计划（200字）  

        总字数要求：3800字以上的详细分析报告  

        请确保报告逻辑清晰、分析深入、建议具体可行。  
        """

        comprehensive_report = await self.fault_system.rag.aquery(
            reasoning_prompt,
            param=QueryParam(mode="hybrid")
        )

        return comprehensive_report

    async def _save_report(self, accident_code: str, report: str) -> str:
        """保存报告到文件"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_filename = f"fault_analysis_report_{accident_code}_{timestamp}.md"
        report_path = Path(self.fault_system.working_dir) / "reports" / report_filename

        with open(report_path, 'w', encoding='utf-8') as f:
            f.write(f"# 故障分析报告 - {accident_code}\n\n")
            f.write(f"生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            f.write(report)

        print(f"报告已保存至: {report_path}")
        return str(report_path)