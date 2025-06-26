import asyncio
import os
import traceback
from datetime import datetime
from typing import Dict, Any

import aiohttp
from fastapi import FastAPI, HTTPException, UploadFile, File
from fastapi.responses import JSONResponse
import uvicorn
from lightrag import QueryParam

from business_integration import BusinessResourceIntegrator
from data_processor import FaultDataProcessor
from fault_analysis_system import FaultAnalysisSystem
from intelligent_analyzer import IntelligentFaultAnalyzer
from report_generator import FaultReportGenerator

app = FastAPI(title="故障库智能分析系统", version="1.0.0")



# 系统依赖注入
class SystemDependencies:
    def __init__(self):
        self.fault_system = None
        self.data_processor = None
        self.analyzer = None
        self.report_generator = None
        self.business_integrator = None


system_deps = SystemDependencies()


@app.post("/analyze_fault_with_external_resources")
async def analyze_fault_with_external_resources(request: Dict[str, Any]):
    """结合外部资源分析故障"""
    try:
        accident_code = request.get("accident_code")
        if not accident_code:
            raise HTTPException(status_code=400, detail="缺少事故编码")

            # 使用扩展的分析器
        result = await analyzer.analyze_fault_with_external_resources(accident_code)

        return JSONResponse({
            "status": "success",
            "accident_code": accident_code,
            "analysis_result": result,
            "external_data_sources": result.get("external_resources", {}).get("analysis_summary", {}).get(
                "data_sources", [])
        })

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"分析失败: {str(e)}")


@app.post("/call_external_resource")
async def call_external_resource(request: Dict[str, Any]):
    """直接调用外部资源"""
    try:
        resource_type = request.get("resource_type")
        params = request.get("params", {})

        if not resource_type:
            raise HTTPException(status_code=400, detail="缺少资源类型")

        result = await business_integrator.call_external_resources(resource_type, params)

        return JSONResponse({
            "status": "success",
            "resource_type": resource_type,
            "result": result
        })

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"外部资源调用失败: {str(e)}")


@app.post("/maintenance_query")
async def query_maintenance_system(request: Dict[str, Any]):
    """查询维护系统"""
    try:
        device_code = request.get("device_code")
        if not device_code:
            raise HTTPException(status_code=400, detail="缺少设备编码")

        params = {
            "device_code": device_code,
            "time_range": request.get("time_range", "30d")
        }

        result = await business_integrator.call_external_resources("maintenance_system", params)

        return JSONResponse({
            "status": "success",
            "device_code": device_code,
            "maintenance_data": result
        })

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"维护系统查询失败: {str(e)}")


@app.post("/hazard_query")
async def query_hazard_system(request: Dict[str, Any]):
    """查询隐患系统"""
    try:
        device_code = request.get("device_code")
        area_name = request.get("area_name")

        if not device_code and not area_name:
            raise HTTPException(status_code=400, detail="至少需要提供设备编码或区域名称")

        params = {
            "device_code": device_code,
            "area_name": area_name,
            "risk_level": request.get("risk_level", "all")
        }

        result = await business_integrator.call_external_resources("hazard_system", params)

        return JSONResponse({
            "status": "success",
            "query_params": params,
            "hazard_data": result
        })

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"隐患系统查询失败: {str(e)}")


@app.post("/inspection_query")
async def query_inspection_system(request: Dict[str, Any]):
    """查询点检系统"""
    try:
        device_code = request.get("device_code")
        if not device_code:
            raise HTTPException(status_code=400, detail="缺少设备编码")

        params = {
            "device_code": device_code,
            "time_range": request.get("time_range", "30d"),
            "include_abnormal_only": request.get("include_abnormal_only", False)
        }

        result = await business_integrator.call_external_resources("inspection_system", params)

        return JSONResponse({
            "status": "success",
            "device_code": device_code,
            "inspection_data": result
        })

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"点检系统查询失败: {str(e)}")


@app.post("/combined_external_analysis")
async def combined_external_analysis(request: Dict[str, Any]):
    """组合外部资源分析"""
    try:
        accident_code = request.get("accident_code")
        device_code = request.get("device_code")
        area_name = request.get("area_name")

        if not any([accident_code, device_code, area_name]):
            raise HTTPException(
                status_code=400,
                detail="至少需要提供事故编码、设备编码或区域名称之一"
            )

            # 如果提供了事故编码，先从故障信息中提取设备和区域信息
        if accident_code and not device_code:
            fault_info = await analyzer.analyze_fault_by_code(accident_code, "basic_info")
            device_code = analyzer._extract_device_code(fault_info)
            area_name = analyzer._extract_area_name(fault_info)

        params = {
            "accident_code": accident_code,
            "device_code": device_code,
            "area_name": area_name,
            "time_range": request.get("time_range", "30d")
        }

        # 调用组合分析
        combined_result = await business_integrator.call_external_resources(
            "combined_analysis", params
        )

        # 生成智能分析报告
        if accident_code:
            intelligent_analysis = await analyzer.analyze_fault_with_external_resources(accident_code)
        else:
            intelligent_analysis = await analyzer._generate_analysis_with_external_data(
                f"设备: {device_code}, 区域: {area_name}", combined_result
            )

        return JSONResponse({
            "status": "success",
            "query_params": params,
            "external_data": combined_result,
            "intelligent_analysis": intelligent_analysis,
            "data_sources": combined_result.get("analysis_summary", {}).get("data_sources", [])
        })

    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"组合分析失败: {str(e)}")


@app.get("/external_systems_status")
async def get_external_systems_status():
    """获取外部系统状态"""
    try:
        systems_status = {}

        # 检查各个外部系统的连接状态
        for system_name, config in business_integrator.external_systems.items():
            try:
                # 简单的健康检查
                import aiohttp
                async with aiohttp.ClientSession() as session:
                    health_url = f"{config['url']}/health"
                    async with session.get(health_url, timeout=5) as response:
                        if response.status == 200:
                            systems_status[system_name] = {
                                "status": "online",
                                "url": config['url'],
                                "response_time": "< 5s"
                            }
                        else:
                            systems_status[system_name] = {
                                "status": "error",
                                "url": config['url'],
                                "error": f"HTTP {response.status}"
                            }
            except Exception as e:
                systems_status[system_name] = {
                    "status": "offline",
                    "url": config['url'],
                    "error": str(e)
                }

        return JSONResponse({
            "status": "success",
            "systems": systems_status,
            "total_systems": len(systems_status),
            "online_systems": len([s for s in systems_status.values() if s["status"] == "online"])
        })

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"状态检查失败: {str(e)}")


@app.post("/batch_external_query")
async def batch_external_query(request: Dict[str, Any]):
    """批量外部资源查询"""
    try:
        device_codes = request.get("device_codes", [])
        resource_types = request.get("resource_types", ["maintenance_system", "hazard_system", "inspection_system"])

        if not device_codes:
            raise HTTPException(status_code=400, detail="缺少设备编码列表")

        batch_results = {}

        for device_code in device_codes:
            device_results = {}
            params = {"device_code": device_code}

            # 并发查询多个资源类型
            tasks = []
            for resource_type in resource_types:
                task = business_integrator.call_external_resources(resource_type, params)
                tasks.append((resource_type, task))

                # 等待所有查询完成
            for resource_type, task in tasks:
                try:
                    result = await task
                    device_results[resource_type] = result
                except Exception as e:
                    device_results[resource_type] = {
                        "status": "error",
                        "message": str(e)
                    }

            batch_results[device_code] = device_results

        return JSONResponse({
            "status": "success",
            "batch_results": batch_results,
            "processed_devices": len(device_codes),
            "queried_resources": resource_types
        })

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"批量查询失败: {str(e)}")


@app.on_event("startup")
async def startup_event():
    """系统启动初始化"""
    global fault_system, data_processor, analyzer, report_generator, business_integrator

    fault_system = FaultAnalysisSystem()
    await fault_system.initialize_rag()

    system_deps.data_processor = FaultDataProcessor(fault_system)
    system_deps.analyzer = IntelligentFaultAnalyzer(fault_system)
    system_deps.report_generator = FaultReportGenerator(fault_system, analyzer)
    system_deps.business_integrator = BusinessResourceIntegrator(fault_system)

    # 集成业务知识
    await system_deps.business_integrator.integrate_business_knowledge()

    print("故障库智能分析系统启动完成！")


@app.post("/upload_excel")
async def upload_excel(file: UploadFile = File(...)):
    """上传Excel故障库文件"""
    try:
        # 保存上传的文件
        file_paths = f"./temp_{file.filename}"
        with open(file_paths, "wb") as buffer:
            content = await file.read()
            buffer.write(content)

            # 处理Excel数据
        df = data_processor.load_excel_data(file_paths)
        if df is None:
            raise HTTPException(status_code=400, detail="Excel文件格式错误")

        processed_data = data_processor.preprocess_fault_data(df)
        await data_processor.import_to_rag(processed_data)

        # 清理临时文件
        os.remove(file_paths)

        return JSONResponse({
            "status": "success",
            "message": f"成功导入 {len(processed_data)} 条故障记录",
            "data_count": len(processed_data)
        })

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"处理失败: {str(e)}")


@app.post("/analyze_fault")
async def analyze_fault(request: Dict[str, Any]):
    """分析特定故障"""
    try:
        accident_code = request.get("accident_code")
        analysis_type = request.get("analysis_type", "comprehensive_report")

        if not accident_code:
            raise HTTPException(status_code=400, detail="缺少事故编码")

        result = await analyzer.analyze_fault_by_code(accident_code, analysis_type)

        return JSONResponse({
            "status": "success",
            "accident_code": accident_code,
            "analysis_type": analysis_type,
            "result": result
        })

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"分析失败: {str(e)}")


@app.post("/generate_comprehensive_report")
async def generate_comprehensive_report(request: Dict[str, Any]):
    """生成综合分析报告"""
    try:
        accident_code = request.get("accident_code")

        if not accident_code:
            raise HTTPException(status_code=400, detail="缺少事故编码")

        report = await report_generator.generate_comprehensive_report(accident_code)

        return JSONResponse({
            "status": "success",
            "accident_code": accident_code,
            "report": report,
            "report_length": len(report)
        })

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"报告生成失败: {str(e)}")


@app.post("/query_knowledge")
async def query_knowledge(request: Dict[str, Any]):
    """知识库查询"""
    try:
        query = request.get("query")
        mode = request.get("mode", "hybrid")

        if not query:
            raise HTTPException(status_code=400, detail="缺少查询内容")

        result = await fault_system.rag.aquery(
            query,
            param=QueryParam(mode=mode)
        )

        return JSONResponse({
            "status": "success",
            "query": query,
            "mode": mode,
            "result": result
        })

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"查询失败: {str(e)}")


@app.get("/external_systems_status")
async def get_external_systems_status():
    """获取外部系统状态"""
    try:
        systems_status = {}

        # 检查各个外部系统的连接状态
        for system_name, config in system_deps.business_integrator.external_systems.items():
            try:
                # 简单的健康检查
                async with aiohttp.ClientSession() as session:
                    health_url = f"{config['url']}/health"
                    start_time = datetime.now()

                    async with session.get(health_url, timeout=aiohttp.ClientTimeout(total=5)) as response:
                        response_time = (datetime.now() - start_time).total_seconds()

                        if response.status == 200:
                            systems_status[system_name] = {
                                "status": "online",
                                "url": config['url'],
                                "response_time": f"{response_time:.2f}s",
                                "last_check": datetime.now().isoformat()
                            }
                        else:
                            systems_status[system_name] = {
                                "status": "error",
                                "url": config['url'],
                                "error": f"HTTP {response.status}",
                                "last_check": datetime.now().isoformat()
                            }
            except asyncio.TimeoutError:
                systems_status[system_name] = {
                    "status": "timeout",
                    "url": config['url'],
                    "error": "连接超时",
                    "last_check": datetime.now().isoformat()
                }
            except Exception as e:
                systems_status[system_name] = {
                    "status": "offline",
                    "url": config['url'],
                    "error": str(e),
                    "last_check": datetime.now().isoformat()
                }

        online_count = len([s for s in systems_status.values() if s["status"] == "online"])

        return JSONResponse({
            "status": "success",
            "systems": systems_status,
            "total_systems": len(systems_status),
            "online_systems": online_count,
            "system_health": "healthy" if online_count == len(systems_status) else "degraded",
            "check_time": datetime.now().isoformat()
        })
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"状态检查失败: {str(e)}")


async def batch_external_query(request: Dict[str, Any]):
    """批量外部资源查询"""
    try:
        device_codes = request.get("device_codes", [])
        resource_types = request.get("resource_types", ["maintenance_system", "hazard_system", "inspection_system"])

        if not device_codes:
            raise HTTPException(status_code=400, detail="缺少设备编码列表")

        if len(device_codes) > 50: #限制批量查询数量
            raise HTTPException(status_code=400, detail="批量查询设备数量不能超过50个")
    except Exception as e:
        print(f"当前查询出现异常{e}")


@app.get("/system_status")
async def get_system_status():
    """获取系统状态"""
    try:
        # 导出系统数据统计
        await fault_system.rag.aexport_data(
            "./temp_export.csv",
            file_format="csv",
            include_vector_data=False
        )

        return JSONResponse({
            "status": "success",
            "system_ready": fault_system.rag is not None,
            "working_directory": fault_system.working_dir
        })

    except Exception as e:
        return JSONResponse({
            "status": "error",
            "message": str(e)
        })


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)