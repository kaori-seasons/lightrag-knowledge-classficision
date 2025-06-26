import time
from collections import defaultdict
from typing import List, Dict, Any

from lightrag.base import TextChunkSchema, BaseKVStorage, BaseGraphStorage, BaseVectorStorage
from lightrag.operate import extract_entities
from lightrag.utils import compute_mdhash_id

from priority_entity_manager import PriorityEntityManager


async def extract_entities_with_priority(
        chunks: dict[str, TextChunkSchema],
        global_config: dict[str, str],
        priority_manager: PriorityEntityManager,
        pipeline_status: dict = None,
        pipeline_status_lock=None,
        llm_response_cache: BaseKVStorage | None = None,
) -> list:
    """带优先级的实体提取"""

    # 获取优先级排序的实体类型
    priority_entities = priority_manager.get_priority_entities()

    # 修改提示模板以包含优先级信息
    priority_prompt_addition = f"""  

    实体提取优先级规则：  
    第一优先级（最重要）: {', '.join(priority_manager.priority_config[1])}  
    第二优先级: {', '.join(priority_manager.priority_config[2])}  
    第三优先级: {', '.join(priority_manager.priority_config[3])}  
    第四优先级: {', '.join(priority_manager.priority_config[4])}  

    请优先识别高优先级的实体，并在建立关系时重点关注高优先级实体之间的连接。  
    """

    # 使用优先级实体类型
    global_config["addon_params"]["entity_types"] = priority_entities

    # 调用原始提取函数
    results = await extract_entities(chunks, global_config, pipeline_status, pipeline_status_lock, llm_response_cache)

    # 后处理：根据优先级调整实体和关系
    processed_results = []
    for maybe_nodes, maybe_edges in results:
        # 为实体添加优先级权重
        weighted_nodes = {}
        for entity_name, entities in maybe_nodes.items():
            for entity in entities:
                entity_type = entity.get('entity_type', '')
                entity['priority'] = priority_manager.get_entity_priority(entity_type)
                entity['weight'] = priority_manager.get_entity_weight(entity_type)
            weighted_nodes[entity_name] = entities

            # 过滤和调整关系
        filtered_edges = {}
        for edge_key, edges in maybe_edges.items():
            source_entity, target_entity = edge_key

            # 获取实体类型（需要从节点中查找）
            source_type = _get_entity_type(source_entity, weighted_nodes)
            target_type = _get_entity_type(target_entity, weighted_nodes)

            if priority_manager.should_create_relationship(source_type, target_type):
                # 调整关系权重
                for edge in edges:
                    original_weight = edge.get('weight', 1.0)
                    edge['weight'] = priority_manager.calculate_relationship_weight(
                        source_type, target_type, original_weight
                    )
                    edge['priority_score'] = (
                                                     priority_manager.get_entity_weight(source_type) +
                                                     priority_manager.get_entity_weight(target_type)
                                             ) / 2

                filtered_edges[edge_key] = edges

        processed_results.append((weighted_nodes, filtered_edges))

    return processed_results


def _get_entity_type(entity_name: str, nodes_dict: dict) -> str:
    """从节点字典中获取实体类型"""
    if entity_name in nodes_dict and nodes_dict[entity_name]:
        return nodes_dict[entity_name][0].get('entity_type', '')
    return ''


async def merge_nodes_and_edges_with_priority(
    chunk_results: list,
    knowledge_graph_inst: BaseGraphStorage,
    entity_vdb: BaseVectorStorage,
    relationships_vdb: BaseVectorStorage,
    global_config: dict[str, str],
    priority_manager: PriorityEntityManager,
    pipeline_status: dict = None,
    pipeline_status_lock=None,
    llm_response_cache: BaseKVStorage | None = None,
    current_file_number: int = 0,
    total_files: int = 0,
    file_path: str = "unknown_source",
) -> None:

    """带优先级的节点和边合并"""

    all_nodes = defaultdict(list)
    all_edges = defaultdict(list)

    for maybe_nodes, maybe_edges in chunk_results:
        # 按优先级排序节点
        for entity_name, entities in maybe_nodes.items():
            # 按优先级和权重排序
            sorted_entities = sorted(entities,
                                     key=lambda x: (x.get('priority', 999), -x.get('weight', 0)))
            all_nodes[entity_name].extend(sorted_entities)

            # 按优先级排序边
        for edge_key, edges in maybe_edges.items():
            sorted_edges = sorted(edges,
                                  key=lambda x: -x.get('priority_score', 0))
            all_edges[edge_key].extend(sorted_edges)

            # 优先处理高优先级实体
    priority_sorted_nodes = sorted(all_nodes.items(),
                                   key=lambda x: min(e.get('priority', 999) for e in x[1]))

    # 处理实体
    entities_data = []
    for entity_name, entities in priority_sorted_nodes:
        # 合并同名实体，保留最高优先级的属性
        merged_entity = _merge_entities_by_priority(entities, priority_manager)
        entities_data.append(merged_entity)

        # 处理关系
        relationships_data = []
        priority_sorted_edges = sorted(all_edges.items(),
                                       key=lambda x: -max(e.get('priority_score', 0) for e in x[1]))

        for edge_key, edges in priority_sorted_edges:
            merged_edge = _merge_relationships_by_priority(edges, priority_manager)
            relationships_data.append(merged_edge)

            # 调用原始合并逻辑
        await _process_merged_data(entities_data, relationships_data,
                                   knowledge_graph_inst, entity_vdb, relationships_vdb)


async def _process_merged_data(
    entities_data: List[Dict[str, Any]],
    relationships_data: List[Dict[str, Any]],
    knowledge_graph_inst: BaseGraphStorage,
    entity_vdb: BaseVectorStorage,
    relationships_vdb: BaseVectorStorage
) -> None:
    """处理合并后的实体和关系数据"""

    #处理实体数据
    if entities_data:
        #更新知识图谱中的节点
        for entity_data in entities_data:
            entity_name = entity_data.get("entity_name")
            if entity_name:
                await knowledge_graph_inst.upsert_node(entity_name, entity_data)

        # 更新实体向量数据库
        entity_vdb_data = {}
        for entity_data in entities_data:
            entity_name = entity_data.get("entity_name")
            if entity_name:
                entity_id = compute_mdhash_id(entity_name, prefix="ent-")
                entity_vdb_data[entity_id] = {
                    "entity_name": entity_name,
                    "entity_type": entity_data.get("entity_type", ""),
                    "content": f"{entity_name}\n{entity_data.get('description', '')}",
                    "source_id": entity_data.get("source_id", ""),
                    "file_path": entity_data.get("file_path", "unknown_source"),
                }
        if entity_vdb_data:
            await entity_vdb.upsert(entity_vdb_data)

    #处理关系数据
    if relationships_data:
        #更新知识图谱中的边
        for relationship_data in relationships_data:
            src_id = relationship_data.get('src_id')
            tgt_id = relationship_data.get('tgt_id')
            if src_id and tgt_id:
                edge_data = {
                    "weight": relationship_data.get("weight", 1.0),
                    "description": relationship_data.get("description", ""),
                    "keywords": relationship_data.get("keywords", ""),
                    "source_id": relationship_data.get("source_id", ""),
                    "file_path": relationship_data.get("file_path", "unknown"),
                    "created_at": int(time.time()),
                }
                await knowledge_graph_inst.upsert_edge(src_id, tgt_id, edge_data)

        #更新关系向量数据库
        relationship_vdb_data = {}
        for relationship_data in relationships_data:
            src_id = relationship_data.get("src_id")
            tgt_id = relationship_data.get("tgt_id")
            if src_id and tgt_id:
                relation_id = compute_mdhash_id(src_id + tgt_id, prefix="rel-")
                relationship_vdb_data[relation_id] = {
                    "src_id": src_id,
                    "tgt_id": tgt_id,
                    "keywords": relationship_data.get("keywords", ""),
                    "content": f"{relationship_data.get('keywords', '')}\t{src_id}\n{tgt_id}\n{relationship_data.get('description', '')}",
                    "source_id": relationship_data.get("source_id", ""),
                    "file_path": relationship_data.get("file_path", "unknown_source"),
                }
        if relationship_vdb_data:
            await relationships_vdb.upsert(relationship_vdb_data)

def _merge_entities_by_priority(entities: List[Dict], priority_manager: PriorityEntityManager) -> Dict:
    """按优先级合并实体"""
    if not entities:
        return {}

        # 选择最高优先级的实体作为基础
    base_entity = min(entities, key=lambda x: x.get('priority', 999))

    # 合并描述，优先级高的在前
    descriptions = []
    for entity in sorted(entities, key=lambda x: x.get('priority', 999)):
        if entity.get('description'):
            descriptions.append(entity['description'])

    base_entity['description'] = ' | '.join(descriptions)
    return base_entity


def _merge_relationships_by_priority(edges: List[Dict], priority_manager: PriorityEntityManager) -> Dict:
    """按优先级合并关系"""
    if not edges:
        return {}

        # 选择优先级分数最高的关系作为基础
    base_edge = max(edges, key=lambda x: x.get('priority_score', 0))

    # 合并权重（取最大值）
    base_edge['weight'] = max(e.get('weight', 1.0) for e in edges)

    return base_edge