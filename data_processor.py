from typing import List, Dict

import pandas as pd

from fault_analysis_system import FaultAnalysisSystem


class FaultDataProcessor:
    """故障数据处理器"""

    def __init__(self, fault_system: FaultAnalysisSystem):
        self.fault_system = fault_system

    def load_excel_data(self, excel_path: str) -> pd.DataFrame:
        """加载Excel故障数据"""
        try:
            df = pd.read_excel(excel_path)
            print(f"成功加载 {len(df)} 条故障记录")
            return df
        except Exception as e:
            print(f"加载Excel文件失败: {e}")
            return None

    def preprocess_fault_data(self, df: pd.DataFrame) -> List[Dict]:
        """预处理故障数据，转换为结构化文本"""
        processed_data = []

        for _, row in df.iterrows():
            # 构建结构化的故障描述文本
            fault_text = self._build_fault_description(row)

            processed_data.append({
                'id': row.get('accident_code', f"FAULT_{len(processed_data)}"),
                'content': fault_text,
                'metadata': {
                    'device': row.get('device_short_name', ''),
                    'area': row.get('area_name', ''),
                    'level': row.get('accident_level', ''),
                    'date': row.get('occurrence_time', ''),
                    'duration': row.get('total_duration', 0)
                }
            })

        return processed_data

    def _build_fault_description(self, row: pd.Series) -> str:
        """构建详细的故障描述文本"""
        sections = []

        # 基本信息
        sections.append(f"【故障基本信息】")
        sections.append(f"事故编码: {row.get('accident_code', 'N/A')}")
        sections.append(f"设备名称: {row.get('device_short_name', 'N/A')}")
        sections.append(f"发生时间: {row.get('occurrence_time', 'N/A')}")
        sections.append(f"故障区域: {row.get('area_name', 'N/A')}")
        sections.append(f"事故等级: {row.get('accident_level', 'N/A')}")
        sections.append(f"停机时长: {row.get('total_duration', 0)}分钟")

        # 故障描述
        sections.append(f"\n【故障现象描述】")
        sections.append(f"{row.get('accident_description', 'N/A')}")
        sections.append(f"表面现象: {row.get('surface_phenomenon', 'N/A')}")

        # 原因分析
        sections.append(f"\n【原因分析】")
        sections.append(f"故障部位: {row.get('fault_location', 'N/A')}")
        sections.append(f"原因属性: {row.get('cause_type', 'N/A')}")
        sections.append(f"根本原因: {row.get('root_cause', 'N/A')}")
        sections.append(f"根本原因简述: {row.get('root_summary', 'N/A')}")

        # 处理措施
        sections.append(f"\n【处理措施】")
        sections.append(f"处理措施: {row.get('treatment_measures', 'N/A')}")
        sections.append(f"处理人员: {row.get('handler', 'N/A')}")

        # 五问分析
        if pd.notna(row.get('five_whys')):
            sections.append(f"\n【五问分析】")
            sections.append(f"{row.get('five_whys')}")

            # 事故调查
        if pd.notna(row.get('investigation')):
            sections.append(f"\n【事故调查结果】")
            sections.append(f"{row.get('investigation')}")

            # 损失评估
        sections.append(f"\n【损失评估】")
        sections.append(f"直接损失: {row.get('direct_loss', 0)}元")
        sections.append(f"间接损失: {row.get('indirect_loss', 0)}元")
        sections.append(f"产量损失: {row.get('production_loss', 0)}")
        sections.append(f"考核金额: {row.get('assessment_amount', 0)}元")

        return "\n".join(sections)

    async def import_to_rag(self, processed_data: List[Dict]):
        """将处理后的数据导入RAG系统"""
        if not self.fault_system.rag:
            await self.fault_system.initialize_rag()

        print(f"开始导入 {len(processed_data)} 条故障记录到RAG系统...")

        for i, fault_data in enumerate(processed_data):
            try:
                await self.fault_system.rag.ainsert(
                    fault_data['content'],
                    file_paths=f"fault_{fault_data['id']}.txt"
                )

                if (i + 1) % 10 == 0:
                    print(f"已导入 {i + 1}/{len(processed_data)} 条记录")

            except Exception as e:
                print(f"导入记录 {fault_data['id']} 失败: {e}")

        print("数据导入完成！")