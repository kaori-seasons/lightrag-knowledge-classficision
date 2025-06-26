from typing import Dict, List, Tuple, Any
from collections import defaultdict
import re


class PriorityEntityManager:
    """基于优先级的实体管理器"""

    def __init__(self):
        self.priority_config = {
            1: ["设备", "故障", "时间"],
            2: ["原因", "措施"],
            3: ["人员"],
            4: ["区域"]
        }

        # 实体权重配置
        self.entity_weights = {
            "设备": 10.0,
            "故障": 9.0,
            "时间": 8.0,
            "原因": 7.0,
            "措施": 6.0,
            "人员": 5.0,
            "区域": 4.0
        }

    def get_priority_entities(self) -> List[str]:
        """获取按优先级排序的实体类型"""
        entities = []
        for priority in sorted(self.priority_config.keys()):
            entities.extend(self.priority_config[priority])
        return entities

    def get_entity_priority(self, entity_type: str) -> int:
        """获取实体类型的优先级"""
        for priority, types in self.priority_config.items():
            if entity_type in types:
                return priority
        return 999  # 未知实体类型的最低优先级

    def get_entity_weight(self, entity_type: str) -> float:
        """获取实体类型的权重"""
        return self.entity_weights.get(entity_type, 1.0)

    def should_create_relationship(self, source_type: str, target_type: str) -> bool:
        """判断是否应该在两个实体类型之间创建关系"""
        source_priority = self.get_entity_priority(source_type)
        target_priority = self.get_entity_priority(target_type)

        # 优先级相同或相邻的实体之间可以建立关系
        return abs(source_priority - target_priority) <= 1

    def calculate_relationship_weight(self, source_type: str, target_type: str, base_weight: float = 1.0) -> float:
        """计算关系权重"""
        source_weight = self.get_entity_weight(source_type)
        target_weight = self.get_entity_weight(target_type)

        # 高优先级实体之间的关系权重更高
        priority_bonus = (source_weight + target_weight) / 2
        return base_weight * priority_bonus / 10.0