#!/bin/bash
# start.sh

echo "=== 故障库智能分析系统启动 ==="

# 检查Python环境
if ! command -v python3 &> /dev/null; then
    echo "错误: 未找到Python3"
    exit 1
fi

# 检查.env文件
if [ ! -f .env ]; then
    echo "警告: 未找到.env文件，使用默认配置"
    cp .env.example .env 2>/dev/null || true
fi

# 激活虚拟环境（如果存在）
if [ -d "venv" ]; then
    echo "激活虚拟环境..."
    source venv/bin/activate
fi

# 安装依赖
echo "检查依赖..."
pip install -r requirements.txt

# 运行部署脚本
echo "运行部署检查..."
python deploy.py

# 启动API服务器
echo "启动API服务器..."
python api_server.py &
API_PID=$!

echo "API服务器已启动 (PID: $API_PID)"
echo "访问 http://localhost:8000 查看API文档"
echo "按 Ctrl+C 停止服务"

# 等待用户中断
trap "kill $API_PID; exit" INT
wait $API_PID