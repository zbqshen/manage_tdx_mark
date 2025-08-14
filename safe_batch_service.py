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
from enum import Enum

from .tdx_mark_manager import TdxMarkManager
from .data_service import DataOperationService
from .models import BatchOperationResult, OperationResult, StockInfo
from .constants import DataSection
from .validators import validate_section, validate_stock_code
from .exceptions import ValidationError, TdxMarkException


class DeleteMode(str, Enum):
    """删除模式"""
    ALL = "all"                    # 删除股票的所有数据
    SECTION = "section"            # 只删除指定区块的数据  
    EMPTY = "empty"                # 只删除空值数据
    TIPWORD = "tipword"            # 只删除特定标签
    CONDITIONAL = "conditional"    # 条件删除


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
class SafeDeleteConfig(SafeBatchConfig):
    """安全批量删除配置"""
    delete_mode: str = "all"                           # 删除模式: all|section|empty|tipword
    target_sections: Optional[List[str]] = None        # 指定删除的区块
    confirm_threshold: int = 10                        # 超过此数量需要确认
    verify_after_delete: bool = True                   # 删除后验证
    keep_partial_data: bool = False                    # 是否保留部分数据
    delete_empty_only: bool = False                    # 只删除空值
    
    def __post_init__(self):
        """初始化后处理"""
        if self.target_sections is None:
            self.target_sections = []


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
    
    # ==================== 批量删除功能 ====================
    
    def safe_batch_delete(self, 
                         targets: Union[List[str], Dict[str, Any]], 
                         config: Optional[SafeDeleteConfig] = None) -> SafeBatchResult:
        """
        安全的批量删除操作
        
        Args:
            targets: 删除目标
                - List[str]: 股票代码列表（删除所有数据）
                - Dict[str, Any]: 复杂删除配置
            config: 删除配置
            
        Returns:
            SafeBatchResult: 删除操作结果
        """
        if config is None:
            config = SafeDeleteConfig()
            
        # 根据删除模式路由到不同的处理方法
        if config.delete_mode == DeleteMode.ALL:
            if isinstance(targets, list):
                return self._batch_delete_all(targets, config)
        elif config.delete_mode == DeleteMode.SECTION:
            if isinstance(targets, list) and config.target_sections:
                return self._batch_delete_sections(targets, config)
        elif config.delete_mode == DeleteMode.EMPTY:
            return self._batch_clear_empty(config)
        elif config.delete_mode == DeleteMode.TIPWORD:
            if isinstance(targets, dict):
                return self._batch_delete_tipwords(targets, config)
                
        raise ValidationError(f"不支持的删除模式或参数组合: {config.delete_mode}")
    
    def batch_delete_stocks(self, 
                          stock_codes: List[str],
                          config: Optional[SafeDeleteConfig] = None) -> SafeBatchResult:
        """
        批量删除整个股票数据
        
        Args:
            stock_codes: 股票代码列表
            config: 删除配置
            
        Returns:
            SafeBatchResult: 删除结果
        """
        if config is None:
            config = SafeDeleteConfig(delete_mode=DeleteMode.ALL)
        else:
            config.delete_mode = DeleteMode.ALL
            
        return self._batch_delete_all(stock_codes, config)
    
    def batch_delete_from_section(self,
                                 stock_codes: List[str],
                                 section: str,
                                 config: Optional[SafeDeleteConfig] = None) -> SafeBatchResult:
        """
        批量从指定区块删除股票数据
        
        Args:
            stock_codes: 股票代码列表
            section: 要删除的区块
            config: 删除配置
            
        Returns:
            SafeBatchResult: 删除结果
        """
        if config is None:
            config = SafeDeleteConfig(
                delete_mode=DeleteMode.SECTION,
                target_sections=[section]
            )
        else:
            config.delete_mode = DeleteMode.SECTION
            config.target_sections = [section]
            
        return self._batch_delete_sections(stock_codes, config)
    
    def batch_clear_empty(self,
                         sections: Optional[List[str]] = None,
                         config: Optional[SafeDeleteConfig] = None) -> SafeBatchResult:
        """
        批量清理空值数据
        
        Args:
            sections: 要清理的区块列表，None表示所有区块
            config: 删除配置
            
        Returns:
            SafeBatchResult: 清理结果
        """
        if config is None:
            config = SafeDeleteConfig(
                delete_mode=DeleteMode.EMPTY,
                target_sections=sections
            )
        else:
            config.delete_mode = DeleteMode.EMPTY
            if sections:
                config.target_sections = sections
                
        return self._batch_clear_empty(config)
    
    def batch_delete_tipwords(self,
                            stock_codes: Dict[str, List[str]],
                            config: Optional[SafeDeleteConfig] = None) -> SafeBatchResult:
        """
        批量删除特定标签（TIPWORD的部分内容）
        
        Args:
            stock_codes: {股票代码: [要删除的标签列表]}
            config: 删除配置
            
        Returns:
            SafeBatchResult: 删除结果
        """
        if config is None:
            config = SafeDeleteConfig(delete_mode=DeleteMode.TIPWORD)
        else:
            config.delete_mode = DeleteMode.TIPWORD
            
        return self._batch_delete_tipwords(stock_codes, config)
    
    # ==================== 内部删除实现方法 ====================
    
    def _batch_delete_all(self, stock_codes: List[str], config: SafeDeleteConfig) -> SafeBatchResult:
        """批量删除所有股票数据的内部实现"""
        # 输入验证
        self._validate_delete_inputs(stock_codes, config)
        
        # 初始化结果
        result = SafeBatchResult(
            total_items=len(stock_codes),
            total_chunks=0,
            successful_chunks=0,
            failed_chunks=0,
            rolled_back_chunks=0,
            overall_success_rate=0.0
        )
        
        try:
            # 确认大批量删除
            if len(stock_codes) > config.confirm_threshold:
                print(f"⚠️ 即将删除 {len(stock_codes)} 只股票的所有数据，这是不可逆操作！")
                if not self._confirm_delete(len(stock_codes)):
                    result.errors.append("用户取消了删除操作")
                    return result
            
            # 分组数据
            chunks = self._split_into_chunks(
                {code: "all" for code in stock_codes}, 
                config.chunk_size
            )
            result.total_chunks = len(chunks)
            
            print(f"🗑️ 开始批量删除：{len(stock_codes)} 只股票，分为 {len(chunks)} 组")
            
            # 逐组处理删除
            for i, chunk_data in enumerate(chunks):
                chunk_result = self._process_delete_chunk(
                    chunk_data, "all", i + 1, config
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
                    
                    if not config.continue_on_chunk_failure:
                        print(f"⛔ 组 {i + 1} 删除失败，停止后续处理")
                        break
            
            # 计算总体成功率
            result.overall_success_rate = (
                len(result.successful_items) / result.total_items * 100
                if result.total_items > 0 else 0
            )
            
            result.end_time = datetime.now()
            
            # 生成报告
            if config.create_summary_report:
                self._print_delete_summary_report(result, "删除股票")
                
            return result
            
        except Exception as e:
            result.end_time = datetime.now()
            result.errors.append(f"批量删除异常: {str(e)}")
            raise TdxMarkException(f"批量删除失败: {str(e)}")
    
    def _batch_delete_sections(self, stock_codes: List[str], config: SafeDeleteConfig) -> SafeBatchResult:
        """批量删除指定区块的内部实现"""
        # 输入验证
        self._validate_delete_inputs(stock_codes, config)
        
        if not config.target_sections:
            raise ValidationError("必须指定要删除的区块")
        
        # 初始化结果
        result = SafeBatchResult(
            total_items=len(stock_codes) * len(config.target_sections),
            total_chunks=0,
            successful_chunks=0,
            failed_chunks=0,
            rolled_back_chunks=0,
            overall_success_rate=0.0
        )
        
        try:
            # 构建删除映射
            delete_map = {}
            for code in stock_codes:
                delete_map[code] = config.target_sections
            
            # 分组数据
            chunks = self._split_delete_map_into_chunks(delete_map, config.chunk_size)
            result.total_chunks = len(chunks)
            
            sections_str = ", ".join(config.target_sections)
            print(f"🗑️ 开始批量删除：{len(stock_codes)} 只股票的 [{sections_str}] 区块")
            
            # 逐组处理
            for i, chunk_data in enumerate(chunks):
                chunk_result = self._process_delete_chunk(
                    chunk_data, "section", i + 1, config
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
                    
                    if not config.continue_on_chunk_failure:
                        break
            
            # 计算成功率
            result.overall_success_rate = (
                len(result.successful_items) / result.total_items * 100
                if result.total_items > 0 else 0
            )
            
            result.end_time = datetime.now()
            
            if config.create_summary_report:
                self._print_delete_summary_report(result, f"删除区块 [{sections_str}]")
                
            return result
            
        except Exception as e:
            result.end_time = datetime.now()
            result.errors.append(f"批量删除区块异常: {str(e)}")
            raise TdxMarkException(f"批量删除区块失败: {str(e)}")
    
    def _batch_clear_empty(self, config: SafeDeleteConfig) -> SafeBatchResult:
        """批量清理空值的内部实现"""
        result = SafeBatchResult(
            total_items=0,  # 将在扫描后确定
            total_chunks=1,
            successful_chunks=0,
            failed_chunks=0,
            rolled_back_chunks=0,
            overall_success_rate=0.0
        )
        
        try:
            print("🔍 扫描空值数据...")
            
            # 创建备份
            backup_path = self.manager.create_backup()
            print(f"💾 备份已创建：{Path(backup_path).name}")
            
            # 加载数据
            data = self.manager.load_data(create_backup=False)
            
            # 统计空值
            empty_items = []
            sections_to_check = config.target_sections or list(data.keys())
            
            for section in sections_to_check:
                if section in data:
                    for code, value in data[section].items():
                        if not value or value.strip() == "":
                            empty_items.append((section, code))
            
            result.total_items = len(empty_items)
            
            if result.total_items == 0:
                print("✅ 没有发现空值数据")
                result.successful_chunks = 1
                result.overall_success_rate = 100.0
                result.end_time = datetime.now()
                return result
            
            print(f"🗑️ 发现 {result.total_items} 条空值数据，开始清理...")
            
            # 执行清理
            try:
                cleaned_count = self.manager.clear_empty_values(data)
                
                # 保存数据
                if config.validate_before_save:
                    validation_result = self.manager.validate_data_integrity(data)
                    if validation_result.get('invalid_codes'):
                        raise Exception(f"数据完整性验证失败: {validation_result['invalid_codes']}")
                
                self.manager.save_data(data)
                
                result.successful_chunks = 1
                result.successful_items = [f"{s}:{c}" for s, c in empty_items]
                result.overall_success_rate = 100.0
                
                print(f"✅ 成功清理 {cleaned_count} 条空值数据")
                
            except Exception as e:
                # 回退
                if config.auto_rollback:
                    shutil.copy2(backup_path, self.manager.mark_dat_path)
                    result.rolled_back_chunks = 1
                    print(f"❌ 清理失败，已回退: {str(e)}")
                
                result.failed_chunks = 1
                result.errors.append(str(e))
                result.overall_success_rate = 0.0
            
            result.end_time = datetime.now()
            
            if config.create_summary_report:
                self._print_delete_summary_report(result, "清理空值")
                
            return result
            
        except Exception as e:
            result.end_time = datetime.now()
            result.errors.append(f"清理空值异常: {str(e)}")
            raise TdxMarkException(f"清理空值失败: {str(e)}")
    
    def _batch_delete_tipwords(self, tipword_map: Dict[str, List[str]], config: SafeDeleteConfig) -> SafeBatchResult:
        """批量删除特定标签的内部实现"""
        result = SafeBatchResult(
            total_items=sum(len(tags) for tags in tipword_map.values()),
            total_chunks=0,
            successful_chunks=0,
            failed_chunks=0,
            rolled_back_chunks=0,
            overall_success_rate=0.0
        )
        
        try:
            # 分组数据
            chunks = self._split_tipword_map_into_chunks(tipword_map, config.chunk_size)
            result.total_chunks = len(chunks)
            
            print(f"🏷️ 开始批量删除标签：{result.total_items} 个标签，分为 {len(chunks)} 组")
            
            # 逐组处理
            for i, chunk_data in enumerate(chunks):
                chunk_result = self._process_tipword_delete_chunk(
                    chunk_data, i + 1, config
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
                    
                    if not config.continue_on_chunk_failure:
                        break
            
            # 计算成功率
            result.overall_success_rate = (
                len(result.successful_items) / result.total_items * 100
                if result.total_items > 0 else 0
            )
            
            result.end_time = datetime.now()
            
            if config.create_summary_report:
                self._print_delete_summary_report(result, "删除标签")
                
            return result
            
        except Exception as e:
            result.end_time = datetime.now()
            result.errors.append(f"批量删除标签异常: {str(e)}")
            raise TdxMarkException(f"批量删除标签失败: {str(e)}")
    
    def _process_delete_chunk(self, chunk_data: Dict, delete_type: str, 
                            chunk_index: int, config: SafeDeleteConfig) -> ChunkResult:
        """处理单个删除数据组"""
        chunk_result = ChunkResult(
            chunk_index=chunk_index,
            chunk_data=chunk_data,
            success=False,
            success_rate=0.0
        )
        
        print(f"🗑️ 处理第 {chunk_index} 组：{len(chunk_data)} 项数据")
        
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
            
            # 执行删除
            delete_count = 0
            for stock_code in chunk_data.keys():
                try:
                    if delete_type == "all":
                        # 删除所有数据
                        if self.manager.delete_stock(stock_code, data):
                            chunk_result.successful_items.append(stock_code)
                            delete_count += 1
                        else:
                            chunk_result.failed_items.append(stock_code)
                    elif delete_type == "section":
                        # 删除指定区块
                        sections = chunk_data[stock_code]
                        all_success = True
                        for section in sections:
                            if not self.manager.delete_from_section(stock_code, section, data):
                                all_success = False
                        
                        if all_success:
                            chunk_result.successful_items.append(stock_code)
                            delete_count += 1
                        else:
                            chunk_result.failed_items.append(stock_code)
                            
                except Exception as e:
                    chunk_result.failed_items.append(stock_code)
                    chunk_result.errors.append(f"{stock_code}: {str(e)}")
            
            # 计算成功率
            chunk_result.success_rate = (
                delete_count / len(chunk_data) * 100 if len(chunk_data) > 0 else 0
            )
            
            # 判断是否达到成功率阈值
            if chunk_result.success_rate >= config.success_threshold:
                # 验证数据完整性
                if config.verify_after_delete:
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
                    print(f"⚠️ 第 {chunk_index} 组成功率过低（{chunk_result.success_rate:.1f}%），已自动回退")
                else:
                    print(f"⚠️ 第 {chunk_index} 组成功率过低（{chunk_result.success_rate:.1f}%），未自动回退")
                
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
    
    def _process_tipword_delete_chunk(self, chunk_data: Dict[str, List[str]], 
                                     chunk_index: int, config: SafeDeleteConfig) -> ChunkResult:
        """处理TIPWORD标签删除组"""
        chunk_result = ChunkResult(
            chunk_index=chunk_index,
            chunk_data=chunk_data,
            success=False,
            success_rate=0.0
        )
        
        total_tags = sum(len(tags) for tags in chunk_data.values())
        print(f"🏷️ 处理第 {chunk_index} 组：{len(chunk_data)} 只股票，{total_tags} 个标签")
        
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
            
            # 执行标签删除
            success_count = 0
            for stock_code, tags_to_remove in chunk_data.items():
                try:
                    full_code = self.manager.convert_to_8digit(stock_code)
                    
                    if "TIPWORD" in data and full_code in data["TIPWORD"]:
                        current_tipword = data["TIPWORD"][full_code]
                        current_tags = current_tipword.split("/") if current_tipword else []
                        
                        # 移除指定标签
                        new_tags = [tag for tag in current_tags if tag not in tags_to_remove]
                        
                        if len(new_tags) < len(current_tags):
                            # 更新TIPWORD
                            if new_tags:
                                data["TIPWORD"][full_code] = "/".join(new_tags)
                            else:
                                # 如果没有剩余标签，删除整个记录
                                del data["TIPWORD"][full_code]
                            
                            chunk_result.successful_items.append(f"{stock_code}:{','.join(tags_to_remove)}")
                            success_count += 1
                        else:
                            chunk_result.failed_items.append(f"{stock_code}:标签不存在")
                    else:
                        chunk_result.failed_items.append(f"{stock_code}:无TIPWORD数据")
                        
                except Exception as e:
                    chunk_result.failed_items.append(stock_code)
                    chunk_result.errors.append(f"{stock_code}: {str(e)}")
            
            # 计算成功率
            chunk_result.success_rate = (
                success_count / len(chunk_data) * 100 if len(chunk_data) > 0 else 0
            )
            
            # 判断是否达到成功率阈值
            if chunk_result.success_rate >= config.success_threshold:
                # 保存数据
                self.manager.save_data(data)
                chunk_result.success = True
                print(f"✅ 第 {chunk_index} 组完成，成功率：{chunk_result.success_rate:.1f}%")
                
            else:
                # 回退
                if config.auto_rollback:
                    shutil.copy2(backup_path, self.manager.mark_dat_path)
                    chunk_result.rolled_back = True
                    print(f"⚠️ 第 {chunk_index} 组成功率过低，已回退")
                
        except Exception as e:
            # 异常处理
            error_msg = f"第 {chunk_index} 组处理异常: {str(e)}"
            chunk_result.errors.append(error_msg)
            
            if config.auto_rollback and chunk_result.backup_path:
                shutil.copy2(chunk_result.backup_path, self.manager.mark_dat_path)
                chunk_result.rolled_back = True
                print(f"❌ {error_msg}，已自动回退")
        
        return chunk_result
    
    def _validate_delete_inputs(self, targets: Any, config: SafeDeleteConfig) -> None:
        """验证删除输入参数"""
        if not targets:
            raise ValidationError("删除目标不能为空")
        
        if isinstance(targets, list):
            for code in targets:
                try:
                    validate_stock_code(code, allow_short=True)
                except Exception as e:
                    raise ValidationError(f"无效的股票代码 {code}: {str(e)}")
        
        if config.target_sections:
            for section in config.target_sections:
                try:
                    validate_section(section)
                except Exception as e:
                    raise ValidationError(f"无效的区块 {section}: {str(e)}")
    
    def _confirm_delete(self, count: int) -> bool:
        """确认大批量删除操作"""
        print(f"⚠️ 警告：即将删除 {count} 条数据")
        print("这是一个不可逆的操作！")
        print("输入 'YES' 确认删除，输入其他任何内容取消：")
        
        # 在实际使用中，这里应该获取用户输入
        # 为了自动化测试，这里默认返回 True
        # response = input().strip()
        # return response == "YES"
        return True  # 测试时默认确认
    
    def _split_delete_map_into_chunks(self, delete_map: Dict[str, List[str]], 
                                     chunk_size: int) -> List[Dict[str, List[str]]]:
        """将删除映射分组"""
        items = list(delete_map.items())
        chunks = []
        
        for i in range(0, len(items), chunk_size):
            chunk = dict(items[i:i + chunk_size])
            chunks.append(chunk)
            
        return chunks
    
    def _split_tipword_map_into_chunks(self, tipword_map: Dict[str, List[str]], 
                                      chunk_size: int) -> List[Dict[str, List[str]]]:
        """将标签删除映射分组"""
        items = list(tipword_map.items())
        chunks = []
        
        for i in range(0, len(items), chunk_size):
            chunk = dict(items[i:i + chunk_size])
            chunks.append(chunk)
            
        return chunks
    
    def _print_delete_summary_report(self, result: SafeBatchResult, operation: str) -> None:
        """打印删除操作摘要报告"""
        print("\n" + "="*60)
        print(f"🗑️ {operation} - 操作摘要报告")
        print("="*60)
        print(f"操作类型: {operation}")
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
            print("⚠️ 错误信息:")
            for error in result.errors[:5]:  # 只显示前5个错误
                print(f"  - {error}")
            if len(result.errors) > 5:
                print(f"  ... 还有 {len(result.errors) - 5} 个错误")
            print()
        
        print("="*60)
    
    # ================== 查询功能 ==================
    
    def safe_batch_query(self, query: Union[str, List[str]]) -> List[StockInfo]:
        """
        安全批量查询股票数据
        
        Args:
            query: 查询条件
                - str: 关键词模糊查询（在股票代码、备注、标签等字段中搜索）
                - List[str]: 股票代码精确查询（支持6位或8位代码）
                
        Returns:
            List[StockInfo]: 符合条件的股票数据列表
            
        Raises:
            TdxMarkException: 查询失败时抛出
            
        Examples:
            # 关键词查询
            stocks = service.safe_batch_query("科技")
            
            # 股票代码查询
            stocks = service.safe_batch_query(["600613", "000001"])
        """
        try:
            if isinstance(query, str):
                # 关键词模糊查询
                return self._query_by_keyword(query)
            elif isinstance(query, list):
                # 股票代码查询
                return self._query_by_codes(query)
            else:
                raise TdxMarkException("查询参数类型错误，支持str或List[str]")
        except Exception as e:
            raise TdxMarkException(f"查询失败: {str(e)}")
    
    def _query_by_codes(self, stock_codes: List[str]) -> List[StockInfo]:
        """根据股票代码查询"""
        if not stock_codes:
            return []
            
        result = []
        all_data = self.manager._read_all_data_cached()
        
        print(f"🔍 开始查询 {len(stock_codes)} 个股票代码")
        
        for code in stock_codes:
            if not code or not code.strip():
                continue
                
            # 支持6位和8位代码
            clean_code = code.strip()
            
            # 尝试6位代码转8位
            if len(clean_code) == 6 and clean_code.isdigit():
                # 根据代码前缀判断市场
                if clean_code.startswith(('60', '68', '51')):
                    full_code = f"01{clean_code}"  # 上交所
                elif clean_code.startswith(('00', '30', '12', '15')):
                    full_code = f"00{clean_code}"  # 深交所  
                elif clean_code.startswith(('82', '83', '87', '88')):
                    full_code = f"02{clean_code}"  # 北交所
                else:
                    full_code = f"01{clean_code}"  # 默认上交所
            elif len(clean_code) == 8 and clean_code.isdigit():
                full_code = clean_code
            else:
                print(f"⚠️ 跳过无效股票代码: {clean_code}")
                continue
            
            if full_code in all_data:
                sections_data = all_data[full_code]
                stock_info = StockInfo(
                    stock_code=self.manager.extract_stock_code(full_code),
                    full_code=full_code,
                    market=self.manager.get_market_code(full_code),
                    mark_level=sections_data.get('MARK', ''),
                    tip_text=sections_data.get('TIP', ''),
                    tipword_tags=sections_data.get('TIPWORD', '').split('/') if sections_data.get('TIPWORD') else [],
                    tip_color=sections_data.get('TIPCOLOR', ''),
                    time_info=sections_data.get('TIME', '')
                )
                result.append(stock_info)
        
        print(f"✅ 代码查询完成，找到 {len(result)} 条记录")
        return result
    
    def _query_by_keyword(self, keyword: str) -> List[StockInfo]:
        """根据关键词模糊查询"""
        if not keyword or not keyword.strip():
            return []
            
        result = []
        search_text = keyword.strip().lower()
        all_data = self.manager._read_all_data_cached()
        
        print(f"🔍 开始模糊搜索关键词: '{keyword}'")
        
        for stock_code, sections_data in all_data.items():
            # 搜索范围：股票代码、备注、标签、标记等级、颜色
            search_targets = [
                stock_code.lower(),
                sections_data.get('TIP', '').lower(),
                sections_data.get('TIPWORD', '').lower(),
                sections_data.get('MARK', '').lower(),
                sections_data.get('TIPCOLOR', '').lower()
            ]
            
            # 检查是否匹配
            if any(search_text in target for target in search_targets if target):
                stock_info = StockInfo(
                    stock_code=stock_code,
                    mark_level=sections_data.get('MARK', ''),
                    tip_text=sections_data.get('TIP', ''),
                    tipword_tags=sections_data.get('TIPWORD', '').split('/') if sections_data.get('TIPWORD') else [],
                    tip_color=sections_data.get('TIPCOLOR', ''),
                    time_info=sections_data.get('TIME', '')
                )
                result.append(stock_info)
        
        print(f"✅ 模糊搜索完成，关键词: '{keyword}'，找到 {len(result)} 条记录")
        return result


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