import asyncio
from pathlib import Path

from business_integration import BusinessResourceIntegrator
from data_processor import FaultDataProcessor
from fault_analysis_system import FaultAnalysisSystem
from intelligent_analyzer import IntelligentFaultAnalyzer
from report_generator import FaultReportGenerator


async def main():
    """主程序入口"""
    print("=== 故障库智能分析系统 ===")

    # 1. 初始化系统
    fault_system = FaultAnalysisSystem()
    await fault_system.initialize_rag()

    # 2. 初始化各个模块
    data_processor = FaultDataProcessor(fault_system)
    analyzer = IntelligentFaultAnalyzer(fault_system)
    report_generator = FaultReportGenerator(fault_system, analyzer)
    business_integrator = BusinessResourceIntegrator(fault_system)

    # 3. 集成业务知识
    await business_integrator.integrate_business_knowledge()

    # 4. 示例：处理Excel文件
    excel_file = "/Users/windwheel/Documents/fault_record_database.xlsx"  # 替换为实际文件路径
    if Path(excel_file).exists():
        print(f"正在处理Excel文件: {excel_file}")

        # 加载和预处理数据
        df = data_processor.load_excel_data(excel_file)
        processed_data = data_processor.preprocess_fault_data(df)

        # 导入到RAG系统
        await data_processor.import_to_rag(processed_data)

        # 5. 示例：分析特定故障
        accident_code = "SGJL202505150001"  # 使用您提供的示例编码
        print(f"\n正在分析故障: {accident_code}")

        # 生成综合分析报告
        comprehensive_report = await report_generator.generate_comprehensive_report(accident_code)

        print(f"\n=== 综合分析报告 ===")
        print(comprehensive_report[:500] + "..." if len(comprehensive_report) > 500 else comprehensive_report)

        # 6. 示例：其他类型分析
        similar_analysis = await analyzer.analyze_similar_faults("轧机", "设备故障")
        print(f"\n=== 类似故障分析 ===")
        print(similar_analysis[:300] + "..." if len(similar_analysis) > 300 else similar_analysis)

    else:
        print(f"Excel文件不存在: {excel_file}")
        print("请将故障库Excel文件放置在当前目录下")

    print("\n系统演示完成！")


if __name__ == "__main__":
    asyncio.run(main())