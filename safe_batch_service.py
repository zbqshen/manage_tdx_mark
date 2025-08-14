#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
安全批量操作服务
================

提供分组小批量 + 逐步验证的安全批量操作接口，
支持所有数据区块（TIP、MARK、TIPWORD、TIPCOLOR、TIME）的批量更新。

特性：
- 分组处理：大批量数据分成小组，降低风险
- 逐步验证：每组操作后验证结果，失败时自动回退
- 统一接口：支持所有数据区块的批量操作
- 详细报告：提供完整的操作统计和错误信息
- 安全机制：自动备份和回退功能
"""

import shutil
from typing import Dict, List, Optional, Union, Any
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

from .tdx_mark_manager import TdxMarkManager
from .data_service import DataOperationService
from .models import BatchOperationResult, OperationResult
from .constants import DataSection
from .validators import validate_section, validate_stock_code
from .exceptions import ValidationError, TdxMarkException


@dataclass
class SafeBatchConfig:
    """安全批量操作配置"""
    chunk_size: int = 5                    # 每组处理的数据量
    success_threshold: float = 100.0       # 成功率阈值（百分比）
    auto_rollback: bool = True             # 自动回退失败的组
    continue_on_chunk_failure: bool = True # 某组失败时是否继续处理后续组
    create_summary_report: bool = True     # 是否创建详细报告
    validate_before_save: bool = True      # 保存前是否验证数据完整性


@dataclass
class ChunkResult:
    """单组操作结果"""
    chunk_index: int
    chunk_data: Dict[str, str]
    success: bool
    success_rate: float
    successful_items: List[str] = field(default_factory=list)
    failed_items: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    backup_path: Optional[str] = None
    rolled_back: bool = False


@dataclass
class SafeBatchResult:
    """安全批量操作完整结果"""
    total_items: int
    total_chunks: int
    successful_chunks: int
    failed_chunks: int
    rolled_back_chunks: int
    overall_success_rate: float
    successful_items: List[str] = field(default_factory=list)
    failed_items: List[str] = field(default_factory=list)
    chunk_results: List[ChunkResult] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    start_time: datetime = field(default_factory=datetime.now)
    end_time: Optional[datetime] = None
    
    @property
    def total_successful_items(self) -> int:
        """成功处理的总项目数"""
        return len(self.successful_items)
    
    @property
    def total_failed_items(self) -> int:
        """失败的总项目数"""
        return len(self.failed_items)
    
    @property
    def duration(self) -> Optional[float]:
        """操作耗时（秒）"""
        if self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return None


class SafeBatchService:
    """
    安全批量操作服务
    
    提供分组小批量操作，支持自动回退和详细报告
    """
    
    def __init__(self, manager: Optional[TdxMarkManager] = None):
        """
        初始化安全批量服务
        
        Args:
            manager: TdxMarkManager实例，如果为None则自动创建
        """
        self.manager = manager or TdxMarkManager()
        self.service = DataOperationService()
        
    def safe_batch_update(self, 
                         updates: Dict[str, str], 
                         section: str,
                         config: Optional[SafeBatchConfig] = None) -> SafeBatchResult:
        """
        安全的批量更新操作
        
        Args:
            updates: 更新数据字典 {stock_code: value}
            section: 数据区块名称（MARK/TIP/TIPWORD/TIPCOLOR/TIME）
            config: 批量操作配置
            
        Returns:
            SafeBatchResult: 完整的操作结果
            
        Raises:
            ValidationError: 输入验证失败
            TdxMarkException: 操作异常
        """
        if config is None:
            config = SafeBatchConfig()
            
        # 输入验证
        self._validate_inputs(updates, section)
        
        # 初始化结果对象
        result = SafeBatchResult(
            total_items=len(updates),
            total_chunks=0,
            successful_chunks=0,
            failed_chunks=0,
            rolled_back_chunks=0,
            overall_success_rate=0.0
        )
        
        try:
            # 分组数据
            chunks = self._split_into_chunks(updates, config.chunk_size)
            result.total_chunks = len(chunks)
            
            print(f"🚀 开始安全批量操作：{len(updates)} 项数据，分为 {len(chunks)} 组")
            print(f"📊 配置：每组 {config.chunk_size} 项，成功率阈值 {config.success_threshold}%")
            
            # 逐组处理
            for i, chunk_data in enumerate(chunks):
                chunk_result = self._process_chunk(
                    chunk_data, section, i + 1, config
                )
                result.chunk_results.append(chunk_result)
                
                if chunk_result.success:
                    result.successful_chunks += 1
                    result.successful_items.extend(chunk_result.successful_items)
                else:
                    result.failed_chunks += 1
                    result.failed_items.extend(chunk_result.failed_items)
                    result.errors.extend(chunk_result.errors)
                    
                    if chunk_result.rolled_back:
                        result.rolled_back_chunks += 1
                    
                    # 检查是否继续处理
                    if not config.continue_on_chunk_failure:
                        print(f"⛔ 组 {i + 1} 失败，停止后续处理")
                        break
            
            # 计算总体成功率
            result.overall_success_rate = (
                len(result.successful_items) / result.total_items * 100
                if result.total_items > 0 else 0
            )
            
            result.end_time = datetime.now()
            
            # 生成报告
            if config.create_summary_report:
                self._print_summary_report(result, section)
                
            return result
            
        except Exception as e:
            result.end_time = datetime.now()
            result.errors.append(f"批量操作异常: {str(e)}")
            raise TdxMarkException(f"安全批量操作失败: {str(e)}")
    
    def _validate_inputs(self, updates: Dict[str, str], section: str) -> None:
        """验证输入参数"""
        if not updates:
            raise ValidationError("更新数据不能为空")
            
        # 验证区块名称
        validate_section(section)
        
        # 验证股票代码
        for stock_code in updates.keys():
            try:
                validate_stock_code(stock_code, allow_short=True)
            except Exception as e:
                raise ValidationError(f"无效的股票代码 {stock_code}: {str(e)}")
    
    def _split_into_chunks(self, updates: Dict[str, str], chunk_size: int) -> List[Dict[str, str]]:
        """将更新数据分组"""
        items = list(updates.items())
        chunks = []
        
        for i in range(0, len(items), chunk_size):
            chunk = dict(items[i:i + chunk_size])
            chunks.append(chunk)
            
        return chunks
    
    def _process_chunk(self, 
                      chunk_data: Dict[str, str], 
                      section: str, 
                      chunk_index: int,
                      config: SafeBatchConfig) -> ChunkResult:
        """处理单个数据组"""
        chunk_result = ChunkResult(
            chunk_index=chunk_index,
            chunk_data=chunk_data,
            success=False,
            success_rate=0.0
        )
        
        print(f"📦 处理第 {chunk_index} 组：{len(chunk_data)} 项数据")
        
        # 创建备份
        try:
            backup_path = self.manager.create_backup()
            chunk_result.backup_path = backup_path
            print(f"💾 备份已创建：{Path(backup_path).name}")
        except Exception as e:
            chunk_result.errors.append(f"创建备份失败: {str(e)}")
            return chunk_result
        
        try:
            # 加载数据
            data = self.manager.load_data(create_backup=False)
            
            # 执行批量更新
            batch_result = self.service.batch_update(chunk_data, section, data)
            
            # 分析结果
            chunk_result.success_rate = batch_result.success_rate
            
            for stock_code, success in batch_result.individual_results.items():
                if success:
                    chunk_result.successful_items.append(stock_code)
                else:
                    chunk_result.failed_items.append(stock_code)
            
            chunk_result.errors.extend(batch_result.errors)
            
            # 判断是否达到成功率阈值
            if chunk_result.success_rate >= config.success_threshold:
                # 验证数据完整性
                if config.validate_before_save:
                    validation_result = self.manager.validate_data_integrity(data)
                    if validation_result.get('invalid_codes'):
                        raise Exception(f"数据完整性验证失败: {validation_result['invalid_codes']}")
                
                # 保存数据
                self.manager.save_data(data)
                chunk_result.success = True
                print(f"✅ 第 {chunk_index} 组完成，成功率：{chunk_result.success_rate:.1f}%")
                
            else:
                # 成功率不达标，回退
                if config.auto_rollback:
                    shutil.copy2(backup_path, self.manager.mark_dat_path)
                    chunk_result.rolled_back = True
                    print(f"⚠️  第 {chunk_index} 组成功率过低（{chunk_result.success_rate:.1f}%），已自动回退")
                else:
                    print(f"⚠️  第 {chunk_index} 组成功率过低（{chunk_result.success_rate:.1f}%），未自动回退")
                
        except Exception as e:
            # 异常时回退
            error_msg = f"第 {chunk_index} 组处理异常: {str(e)}"
            chunk_result.errors.append(error_msg)
            
            if config.auto_rollback and chunk_result.backup_path:
                try:
                    shutil.copy2(chunk_result.backup_path, self.manager.mark_dat_path)
                    chunk_result.rolled_back = True
                    print(f"❌ {error_msg}，已自动回退")
                except Exception as rollback_error:
                    chunk_result.errors.append(f"回退失败: {str(rollback_error)}")
                    print(f"❌ {error_msg}，回退也失败了")
            else:
                print(f"❌ {error_msg}")
        
        return chunk_result
    
    def _print_summary_report(self, result: SafeBatchResult, section: str) -> None:
        """打印操作摘要报告"""
        print("\n" + "="*60)
        print("📊 安全批量操作摘要报告")
        print("="*60)
        print(f"数据区块: {section}")
        print(f"开始时间: {result.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"结束时间: {result.end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"耗时: {result.duration:.2f} 秒")
        print()
        
        print("📈 处理统计:")
        print(f"  总数据项: {result.total_items}")
        print(f"  总分组数: {result.total_chunks}")
        print(f"  成功组数: {result.successful_chunks}")
        print(f"  失败组数: {result.failed_chunks}")
        print(f"  回退组数: {result.rolled_back_chunks}")
        print()
        
        print("🎯 结果统计:")
        print(f"  成功项目: {result.total_successful_items}")
        print(f"  失败项目: {result.total_failed_items}")
        print(f"  总体成功率: {result.overall_success_rate:.1f}%")
        print()
        
        if result.failed_items:
            print("❌ 失败项目:")
            for item in result.failed_items[:10]:  # 只显示前10个
                print(f"  - {item}")
            if len(result.failed_items) > 10:
                print(f"  ... 还有 {len(result.failed_items) - 10} 个失败项目")
            print()
        
        if result.errors:
            print("⚠️  错误信息:")
            for error in result.errors[:5]:  # 只显示前5个错误
                print(f"  - {error}")
            if len(result.errors) > 5:
                print(f"  ... 还有 {len(result.errors) - 5} 个错误")
            print()
        
        print("="*60)
    
    def batch_update_tip(self, updates: Dict[str, str], 
                        config: Optional[SafeBatchConfig] = None) -> SafeBatchResult:
        """批量更新TIP区块"""
        return self.safe_batch_update(updates, DataSection.TIP.value, config)
    
    def batch_update_mark(self, updates: Dict[str, str], 
                         config: Optional[SafeBatchConfig] = None) -> SafeBatchResult:
        """批量更新MARK区块"""
        return self.safe_batch_update(updates, DataSection.MARK.value, config)
    
    def batch_update_tipword(self, updates: Dict[str, str], 
                           config: Optional[SafeBatchConfig] = None) -> SafeBatchResult:
        """批量更新TIPWORD区块"""
        return self.safe_batch_update(updates, DataSection.TIPWORD.value, config)
    
    def batch_update_tipcolor(self, updates: Dict[str, str], 
                             config: Optional[SafeBatchConfig] = None) -> SafeBatchResult:
        """批量更新TIPCOLOR区块"""
        return self.safe_batch_update(updates, DataSection.TIPCOLOR.value, config)
    
    def batch_update_time(self, updates: Dict[str, str], 
                         config: Optional[SafeBatchConfig] = None) -> SafeBatchResult:
        """批量更新TIME区块"""
        return self.safe_batch_update(updates, DataSection.TIME.value, config)


# 便捷函数
def create_safe_batch_config(chunk_size: int = 5,
                           success_threshold: float = 100.0,
                           auto_rollback: bool = True,
                           continue_on_failure: bool = True) -> SafeBatchConfig:
    """创建安全批量操作配置的便捷函数"""
    return SafeBatchConfig(
        chunk_size=chunk_size,
        success_threshold=success_threshold,
        auto_rollback=auto_rollback,
        continue_on_chunk_failure=continue_on_failure
    )


# 使用示例
if __name__ == "__main__":
    # 创建服务实例
    safe_service = SafeBatchService()
    
    # 配置操作参数
    config = create_safe_batch_config(
        chunk_size=3,                # 每组3个项目
        success_threshold=100.0,     # 100%成功率阈值
        auto_rollback=True,          # 自动回退
        continue_on_failure=True     # 继续处理后续组
    )
    
    # 准备测试数据
    test_updates = {
        "600613": "东阿阿胶-优质消费股",
        "000001": "平安银行-金融龙头", 
        "002415": "海康威视-安防龙头",
        "600036": "招商银行-优质银行股",
        "000858": "五粮液-白酒龙头"
    }
    
    # 执行安全批量更新
    try:
        result = safe_service.batch_update_tip(test_updates, config)
        
        if result.overall_success_rate >= 80:
            print(f"🎉 批量操作整体成功！成功率：{result.overall_success_rate:.1f}%")
        else:
            print(f"⚠️  批量操作成功率较低：{result.overall_success_rate:.1f}%")
            
    except Exception as e:
        print(f"❌ 批量操作失败：{e}")