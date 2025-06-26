import os
import subprocess
import sys
from pathlib import Path


def check_dependencies():
    """检查依赖"""
    print("检查Python依赖...")

    required_packages = [
        'lightrag-hku', 'pandas', 'openpyxl', 'fastapi',
        'uvicorn', 'python-multipart', 'python-dotenv'
    ]

    missing_packages = []

    for package in required_packages:
        try:
            __import__(package.replace('-', '_'))
        except ImportError:
            missing_packages.append(package)

    if missing_packages:
        print(f"缺少依赖包: {missing_packages}")
        print("正在安装...")
        subprocess.check_call([
                                  sys.executable, "-m", "pip", "install"
                              ] + missing_packages)

    print("依赖检查完成！")


def setup_environment():
    """设置环境"""
    print("设置环境...")

    # 创建必要目录
    directories = [
        "./fault_analysis_rag",
        "./fault_analysis_rag/data",
        "./fault_analysis_rag/reports",
        "./input_excel_files",
        "./logs"
    ]

    for directory in directories:
        Path(directory).mkdir(exist_ok=True)
        print(f"创建目录: {directory}")

        # 检查环境变量
    if not os.getenv("OPENAI_API_KEY"):
        print("警告: 未设置OPENAI_API_KEY环境变量")
        print("请在.env文件中设置您的OpenAI API密钥")

    print("环境设置完成！")


def create_sample_data():
    """创建示例数据"""
    print("创建示例数据...")

    import pandas as pd

    sample_data = {
        'accident_code': ['SGJL202505150001', 'SGJL202505150002'],
        'device_short_name': [
            '某高棒厂/2#高棒生产线/2#棒预精轧区/16H轧机机列-430',
            '某高棒厂/1#高棒生产线/1#棒粗轧区/12H轧机机列-320'
        ],
        'occurrence_time': ['2025/5/5 9:30', '2025/5/6 14:20'],
        'accident_description': [
            '轧机因轧制力不稳定自动停机，无法正常启动',
            '轧机润滑系统压力异常，导致设备保护停机'
        ],
        'area_name': ['轧制区域', '轧制区域'],
        'accident_level': ['D 一般事故', 'C 一般事故'],
        'total_duration': [420, 180],
        'root_cause': [
            '未按照规定周期对轧辊进行检查和更换，且润滑系统维护不到位',
            '润滑系统滤芯堵塞，未及时更换'
        ],
        'treatment_measures': [
            '重新编与减速机有关的',
            '更换润滑系统滤芯，清洗管路'
        ],
        'five_whys': [
            '问题一：为什么430轧机停机？答案一：轧制力不稳定自动停机。',
            '问题一：为什么320轧机停机？答案一：润滑系统压力异常。'
        ]
    }

    df = pd.DataFrame(sample_data)
    df.to_excel("./input_excel_files/sample_fault_data.xlsx", index=False)

    print("示例数据已创建: ./input_excel_files/sample_fault_data.xlsx")


def main():
    """部署主程序"""
    print("=== 故障库智能分析系统部署 ===")

    try:
        check_dependencies()
        setup_environment()
        create_sample_data()

        print("\n部署完成！")
        print("\n使用说明:")
        print("1. 设置OpenAI API密钥在.env文件中")
        print("2. 将Excel故障库文件放入 ./input_excel_files/ 目录")
        print("3. 运行 python main.py 进行单文件处理")
        print("4. 运行 python batch_processor.py 进行批量处理")
        print("5. 运行 python api_server.py 启动Web API服务")

    except Exception as e:
        print(f"部署失败: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()