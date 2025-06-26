import os
import asyncio
import pickle

import pandas as pd
import json
from typing import Dict, List, Any, Optional
from datetime import datetime
from pathlib import Path
from functools import partial


from lightrag import LightRAG, QueryParam
from lightrag.base import BaseKVStorage
from lightrag.kg.json_kv_impl import JsonKVStorage
from lightrag.kg.shared_storage import initialize_pipeline_status
from lightrag.llm.openai import openai_complete_if_cache
from lightrag.llm.zhipu import zhipu_complete, zhipu_complete_if_cache, zhipu_embedding
from lightrag.utils import setup_logger, EmbeddingFunc
from lightrag.llm.ollama import ollama_model_complete, ollama_embed, _ollama_model_if_cache


class FaultAnalysisSystem:
    """基于LightRAG的故障分析系统"""

    def __init__(self, working_dir: str = "./fault_analysis_rag"):
        self.working_dir = working_dir
        self.rag = None
        self.setup_directories()
        setup_logger("fault_analysis", level="INFO")

    def setup_directories(self):
        """创建必要的目录结构"""
        Path(self.working_dir).mkdir(exist_ok=True)
        Path(f"{self.working_dir}/data").mkdir(exist_ok=True)
        Path(f"{self.working_dir}/reports").mkdir(exist_ok=True)

    @staticmethod
    async def llm_model_func(prompt, system_prompt=None, history_messages=[], **kwargs) -> str:
        """静态LLM模型函数"""
        result = await zhipu_complete_if_cache(
            model="glm-4-plus",
            prompt=prompt,
            system_prompt=system_prompt,
            history_messages=history_messages,
            api_key="9079e8f00e204501bab232ac9597e451.0MAnH8uN6d3cbG9Z",
            **kwargs
        )
        return result

    @staticmethod
    async def embedding_func(texts: list[str]):
        """静态嵌入函数"""
        return await zhipu_embedding(
            texts,
            model="embedding-2",
            api_key="9079e8f00e204501bab232ac9597e451.0MAnH8uN6d3cbG9Z"
        )

    async def initialize_rag(self):
        """初始化LightRAG实例"""

        key_string_value_json_storage_cls: type[BaseKVStorage] = (
            JsonKVStorage
        )

        global_config = {
            "llm_model_name": "qwen-plus",
            "working_dir": "./fault_analysis_rag",
        }


        self.rag = LightRAG(
            working_dir=self.working_dir,
            llm_model_func=self.llm_model_func,
            llm_model_name="glm-4-plus",
            embedding_func=EmbeddingFunc(
                embedding_dim=1024,  # 智谱AI embedding维度
                max_token_size=8192,
                func=lambda texts: zhipu_embedding(
                     texts,
                     model="embedding-2",
                     api_key="9079e8f00e204501bab232ac9597e451.0MAnH8uN6d3cbG9Z"
                )
            ),
            chunk_token_size=1500,  # 增大chunk大小以处理复杂故障数据
            chunk_overlap_token_size=200,
            # 配置中文处理
            addon_params={
                "language": "Simplified Chinese",
                "entity_types": ["设备", "故障", "原因", "措施", "时间"]
            }
        )

        await self.rag.initialize_storages()
        await initialize_pipeline_status()
        return self.rag

    def to_dict(self) -> Dict[str, Any]:
        """转换为可序列化的字典"""
        return {
            "working_dir": self.working_dir,
            "rag_initialized": self.rag is not None,
            "system_status": "ready" if self.rag else "not_initialized"
        }

    def __getstate__(self):
        """自定义序列化状态"""
        state = self.__dict__.copy()
        # 移除不可序列化的对象
        state['rag'] = None
        return state

    def __setstate__(self, state):
        """自定义反序列化状态"""
        self.__dict__.update(state)