import os
from pathlib import Path
from typing import Dict, Any


class FaultAnalysisConfig:
    """故障分析系统配置"""

    def __init__(self):
        self.load_config()

    def load_config(self):
        """加载配置"""
        # 基础配置
        self.WORKING_DIR = os.getenv("FAULT_WORKING_DIR", "./fault_analysis_rag")
        self.LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

        # API配置
        self.API_HOST = os.getenv("API_HOST", "")
        self.API_PORT = os.getenv("API_PORT", "")

        self.MAX_ASYNC = os.getenv("MAX_ASYNC", "")
        self.MAX_TOKENS = os.getenv("MAX_TOKENS", "")

        # LLM配置
        self.OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
        self.OPENAI_API_BASE = os.getenv("OPENAI_API_BASE", "")

        self.LLM_MODEL_NAME = os.getenv("LLM_MODEL_NAME", "")
        self.LLM_BINDING = os.getenv("LLM_BINDING", "")
        self.LLM_BINDING_HOST = os.getenv("LLM_BINDING_HOST", "")
        self.LLM_BINDING_API_KEY = os.getenv("LLM_BINDING_API_KEY", "")

        self.EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "")
        self.EMBEDDING_BINDING = os.getenv("EMBEDDING_BINDING", "")
        self.EMBEDDING_BINDING_HOST = os.getenv("EMBEDDING_BINDING_HOST", "")
        self.EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "")
        self.EMBEDDING_BINDING_API_KEY = os.getenv("EMBEDDING_BINDING_API_KEY", "")
        self.EMBEDDING_DIM = os.getenv("EMBEDDING_DIM", "")
        self.EMBEDDING_BATCH_NUM = os.getenv("EMBEDDING_BATCH_NUM", "")


        # RAG配置
        self.CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", "1500"))
        self.CHUNK_OVERLAP = int(os.getenv("CHUNK_OVERLAP", "200"))
        self.MAX_ASYNC = int(os.getenv("MAX_ASYNC", "4"))

        # 业务配置
        self.LANGUAGE = os.getenv("SUMMARY_LANGUAGE", "Simplified Chinese")
        self.ENTITY_TYPES = ["设备", "故障", "原因", "措施", "人员", "区域", "时间"]

        # API配置
        self.API_HOST = os.getenv("API_HOST", "0.0.0.0")
        self.API_PORT = int(os.getenv("API_PORT", "8000"))

    # 全局配置实例


config = FaultAnalysisConfig()