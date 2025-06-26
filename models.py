from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from datetime import datetime


class FaultRecord(BaseModel):
    """故障记录数据模型"""
    uuid: Optional[str] = None
    accident_code: str = Field(..., description="事故记录编码")
    report_status: Optional[str] = None
    rectify_status: Optional[str] = None
    discovery_date: Optional[str] = None
    occurrence_time: Optional[str] = None
    device_short_name: Optional[str] = None
    accident_description: Optional[str] = None
    treatment_measures: Optional[str] = None
    handler: Optional[str] = None
    mainline_time: Optional[int] = None
    sideline_time: Optional[int] = None
    total_duration: Optional[int] = None
    accident_level: Optional[str] = None
    assessment_amount: Optional[float] = None
    assessment_ratio: Optional[float] = None
    composite_type: Optional[str] = None
    specialty_type: Optional[str] = None
    area_name: Optional[str] = None
    fault_location: Optional[str] = None
    cause_type: Optional[str] = None
    tech_tag: Optional[str] = None
    direct_loss: Optional[float] = None
    indirect_loss: Optional[float] = None
    investigation: Optional[str] = None
    five_whys: Optional[str] = None
    common_code: Optional[str] = None
    surface_phenomenon: Optional[str] = None
    root_cause: Optional[str] = None
    root_summary: Optional[str] = None
    accident_source: Optional[str] = None
    device_code: Optional[str] = None
    found_time: Optional[str] = None
    end_time: Optional[str] = None
    closing_days: Optional[int] = None
    assessment_result: Optional[str] = None
    remark: Optional[str] = None
    actual_assessment_amount: Optional[float] = None
    actual_return_amount: Optional[float] = None
    return_confirmed: Optional[str] = None
    assessment_confirmed: Optional[str] = None
    production_loss: Optional[float] = None


class AnalysisRequest(BaseModel):
    """分析请求模型"""
    accident_code: str = Field(..., description="事故编码")
    analysis_type: str = Field(default="comprehensive_report", description="分析类型")


class QueryRequest(BaseModel):
    """查询请求模型"""
    query: str = Field(..., description="查询内容")
    mode: str = Field(default="hybrid", description="查询模式")


class ReportResponse(BaseModel):
    """报告响应模型"""
    status: str
    accident_code: str
    report: str
    report_length: int
    generated_at: datetime = Field(default_factory=datetime.now)