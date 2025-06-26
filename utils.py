import re
import json
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime
import pandas as pd


def setup_logging(log_level: str = "INFO"):
    """设置日志"""
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('fault_analysis.log', encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)


def clean_text(text: str) -> str:
    """清理文本"""
    if not text or pd.isna(text):
        return ""

        # 移除多余的空白字符
    text = re.sub(r'\s+', ' ', str(text))
    # 移除特殊字符
    text = re.sub(r'[^\w\s\u4e00-\u9fff，。！？；：""''（）【】]', '', text)
    return text.strip()


def format_datetime(dt: Any) -> Optional[str]:
    """格式化日期时间"""
    if dt is None or pd.isna(dt):
        return None

    if isinstance(dt, str):
        return dt

    if isinstance(dt, datetime):
        return dt.strftime('%Y-%m-%d %H:%M:%S')

    return str(dt)


def validate_excel_structure(df: pd.DataFrame) -> bool:
    """验证Excel文件结构"""
    required_columns = [
        'accident_code', 'device_short_name', 'accident_description',
        'occurrence_time', 'area_name', 'accident_level'
    ]

    missing_columns = [col for col in required_columns if col not in df.columns]

    if missing_columns:
        logging.error(f"Excel文件缺少必要列: {missing_columns}")
        return False

    return True


def extract_keywords(text: str) -> List[str]:
    """提取关键词"""
    if not text:
        return []

        # 简单的关键词提取逻辑
    keywords = []

    # 设备相关关键词
    device_patterns = [r'轧机', r'润滑系统', r'轧辊', r'减速机', r'电机']
    for pattern in device_patterns:
        if re.search(pattern, text):
            keywords.append(pattern)

            # 故障类型关键词
    fault_patterns = [r'停机', r'异常', r'故障', r'磨损', r'泄漏', r'振动']
    for pattern in fault_patterns:
        if re.search(pattern, text):
            keywords.append(pattern)

    return list(set(keywords))


def calculate_severity_score(fault_record: Dict[str, Any]) -> float:
    """计算故障严重程度评分"""
    score = 0.0

    # 基于停机时长
    duration = fault_record.get('total_duration', 0)
    if duration > 0:
        score += min(duration / 60, 10)  # 最高10分

    # 基于经济损失
    direct_loss = fault_record.get('direct_loss', 0) or 0
    indirect_loss = fault_record.get('indirect_loss', 0) or 0
    total_loss = direct_loss + indirect_loss
    if total_loss > 0:
        score += min(total_loss / 10000, 10)  # 最高10分

    # 基于事故等级
    level_scores = {
        'A 重大事故': 20,
        'B 较大事故': 15,
        'C 一般事故': 10,
        'D 轻微事故': 5
    }
    level = fault_record.get('accident_level', '')
    score += level_scores.get(level, 0)

    return min(score, 40)  # 最高40分


def generate_summary_statistics(fault_records: List[Dict[str, Any]]) -> Dict[str, Any]:
    """生成汇总统计"""
    if not fault_records:
        return {}

    stats = {
        'total_count': len(fault_records),
        'device_distribution': {},
        'area_distribution': {},
        'level_distribution': {},
        'monthly_trend': {},
        'avg_duration': 0,
        'total_loss': 0
    }

    total_duration = 0
    total_loss = 0

    for record in fault_records:
        # 设备分布
        device = record.get('device_short_name', '未知')
        stats['device_distribution'][device] = stats['device_distribution'].get(device, 0) + 1

        # 区域分布
        area = record.get('area_name', '未知')
        stats['area_distribution'][area] = stats['area_distribution'].get(area, 0) + 1

        # 等级分布
        level = record.get('accident_level', '未知')
        stats['level_distribution'][level] = stats['level_distribution'].get(level, 0) + 1

        # 时间趋势
        occurrence_time = record.get('occurrence_time', '')
        if occurrence_time:
            try:
                month = occurrence_time[:7]  # YYYY-MM
                stats['monthly_trend'][month] = stats['monthly_trend'].get(month, 0) + 1
            except:
                pass

                # 累计统计
        duration = record.get('total_duration', 0) or 0
        total_duration += duration

        direct_loss = record.get('direct_loss', 0) or 0
        indirect_loss = record.get('indirect_loss', 0) or 0
        total_loss += (direct_loss + indirect_loss)

    stats['avg_duration'] = total_duration / len(fault_records) if fault_records else 0
    stats['total_loss'] = total_loss

    return stats