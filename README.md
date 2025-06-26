# 故障库智能分析系统使用手册  
  
## 系统概述  
  
基于LightRAG的故障库智能分析系统，能够处理Excel格式的故障数据，构建知识图谱，并生成长窗口推理文本分析报告。  
  
## 功能特性  
  
- **Excel数据导入**: 支持标准故障库Excel格式数据导入  
- **知识图谱构建**: 自动提取设备、故障、原因等实体和关系  
- **智能分析**: 多模式查询（本地、全局、混合）  
- **长文本生成**: 生成详细的故障分析报告  
- **业务资源整合**: 集成维护标准、安全规程等业务知识  
- **批量处理**: 支持多文件批量处理  
- **Web API**: 提供RESTful API接口  
  
## 安装部署  
  
### 1. 环境准备  
  
```bash  
# 克隆项目  [header-3](#header-3)
git clone <repository-url>  
cd fault-analysis-system  
  
# 创建虚拟环境  [header-4](#header-4)
python -m venv venv  
source venv/bin/activate  # Linux/Mac  
# 或 venv\Scripts\activate  # Windows  [header-5](#header-5)
  
# 安装依赖  [header-6](#header-6)
pip install -r requirements.txt