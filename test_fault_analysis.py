import pytest
import asyncio
import pandas as pd
from pathlib import Path
import tempfile
import os

from lightrag import QueryParam

from fault_analysis_system import FaultAnalysisSystem
from data_processor import FaultDataProcessor
from intelligent_analyzer import IntelligentFaultAnalyzer
from report_generator import FaultReportGenerator
from business_integration import BusinessResourceIntegrator


class TestFaultAnalysisSystem:
    """故障分析系统测试"""

    @pytest.fixture
    async def fault_system(self):
        """创建测试用的故障分析系统"""
        with tempfile.TemporaryDirectory() as temp_dir:
            system = FaultAnalysisSystem(working_dir=temp_dir)
            await system.initialize_rag()
            yield system

    @pytest.fixture
    def sample_excel_data(self):
        """创建示例Excel数据"""
        data = {
            'accident_code': ['SGJL202505150001'],
            'device_short_name': ['某高棒厂/2#高棒生产线/2#棒预精轧区/16H轧机机列-430'],
            'occurrence_time': ['2025/5/5 9:30'],
            'accident_description': ['轧机因轧制力不稳定自动停机'],
            'area_name': ['轧制区域'],
            'accident_level': ['D 一般事故'],
            'total_duration': [420],
            'root_cause': ['未按照规定周期对轧辊进行检查和更换'],
            'treatment_measures': ['重新编与减速机有关的'],
            'five_whys': ['问题一：为什么430轧机停机？答案一：轧制力不稳定自动停机。']
        }
        return pd.DataFrame(data)

    async def test_system_initialization(self, fault_system):
        """测试系统初始化"""
        assert fault_system.rag is not None
        assert Path(fault_system.working_dir).exists()
        assert Path(f"{fault_system.working_dir}/data").exists()
        assert Path(f"{fault_system.working_dir}/reports").exists()

    async def test_excel_data_processing(self, fault_system, sample_excel_data):
        """测试Excel数据处理"""
        processor = FaultDataProcessor(fault_system)

        # 测试数据预处理
        processed_data = processor.preprocess_fault_data(sample_excel_data)
        assert len(processed_data) == 1
        assert processed_data[0]['id'] == 'SGJL202505150001'
        assert '故障基本信息' in processed_data[0]['content']

        # 测试数据导入
        await processor.import_to_rag(processed_data)

        # 验证数据已成功导入
        query_result = await fault_system.rag.aquery(
            "SGJL202505150001",
            param=QueryParam(mode="local")
        )
        assert query_result is not None

    async def test_intelligent_analysis(self, fault_system, sample_excel_data):
        """测试智能分析功能"""
        processor = FaultDataProcessor(fault_system)
        analyzer = IntelligentFaultAnalyzer(fault_system)

        # 导入测试数据
        processed_data = processor.preprocess_fault_data(sample_excel_data)
        await processor.import_to_rag(processed_data)

        # 测试根本原因分析
        root_cause_analysis = await analyzer.analyze_fault_by_code(
            "SGJL202505150001", "root_cause_analysis"
        )
        assert root_cause_analysis is not None
        assert len(root_cause_analysis) > 0

        # 测试类似故障分析
        similar_analysis = await analyzer.analyze_similar_faults("轧机", "设备故障")
        assert similar_analysis is not None

    async def test_report_generation(self, fault_system, sample_excel_data):
        """测试报告生成功能"""
        processor = FaultDataProcessor(fault_system)
        analyzer = IntelligentFaultAnalyzer(fault_system)
        report_generator = FaultReportGenerator(fault_system, analyzer)

        # 导入测试数据
        processed_data = processor.preprocess_fault_data(sample_excel_data)
        await processor.import_to_rag(processed_data)

        # 生成综合报告
        report = await report_generator.generate_comprehensive_report("SGJL202505150001")
        assert report is not None
        assert len(report) > 1000  # 确保报告足够详细

if __name__ == "__main__":
    pytest.main([__file__])