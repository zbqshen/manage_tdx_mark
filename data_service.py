#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
TDX 标记管理器数据服务层
===========================

此模块为数据操作提供一个清洁的服务层，
消除代码重复并提供更好的关注点分离。
"""

from typing import Dict, List, Optional, Any, Union, Callable
from abc import ABC, abstractmethod

from .constants import DataSection, TIPWORD_SEPARATOR
from .models import StockInfo, OperationResult, BatchOperationResult
from .validators import validate_stock_code, validate_section, InputValidator
from .exceptions import ValidationError, StockCodeError, DataFormatError


class DataOperationStrategy(ABC):
    """数据操作策略的抽象基类"""
    
    @abstractmethod
    def execute(self, full_code: str, value: str, data: Dict[str, Dict[str, str]]) -> bool:
        """执行操作策略"""
        pass


class DirectUpdateStrategy(DataOperationStrategy):
    """直接值替换策略（MARK、TIP、TIPCOLOR、TIME）"""
    
    def __init__(self, section: str):
        self.section = validate_section(section)
    
    def execute(self, full_code: str, value: str, data: Dict[str, Dict[str, str]]) -> bool:
        """执行直接更新策略"""
        try:
            # 确保区块存在
            if self.section not in data:
                data[self.section] = {}
            
            data[self.section][full_code] = value
            return True
        except Exception:
            return False


class TipwordMergeStrategy(DataOperationStrategy):
    """使用分隔符的TIPWORD合并策略"""
    
    def __init__(self):
        self.section = DataSection.TIPWORD.value
    
    def execute(self, full_code: str, value: str, data: Dict[str, Dict[str, str]]) -> bool:
        """执行TIPWORD合并策略"""
        try:
            # 确保TIPWORD区块存在
            if self.section not in data:
                data[self.section] = {}
            
            current_value = data[self.section].get(full_code, '')
            
            if current_value:
                # 使用分隔符与现有值合并
                new_value = f"{current_value}{TIPWORD_SEPARATOR}{value}"
            else:
                # 设置为新值
                new_value = value
            
            data[self.section][full_code] = new_value
            return True
        except Exception:
            return False


class DataOperationService:
    """
    使用策略模式进行数据操作的服务类
    """
    
    def __init__(self):
        self._strategies = {
            DataSection.MARK.value: DirectUpdateStrategy(DataSection.MARK.value),
            DataSection.TIP.value: DirectUpdateStrategy(DataSection.TIP.value),
            DataSection.TIPCOLOR.value: DirectUpdateStrategy(DataSection.TIPCOLOR.value),
            DataSection.TIME.value: DirectUpdateStrategy(DataSection.TIME.value),
            DataSection.TIPWORD.value: TipwordMergeStrategy(),
        }
    
    def update_section_value(self, stock_code: str, section: str, value: str,
                           data: Dict[str, Dict[str, str]]) -> OperationResult:
        """
        使用适当的策略更新区块值
        
        Args:
            stock_code: 6位或8位股票代码
            section: 区块名称
            value: 新值
            data: 数据字典
            
        Returns:
            操作结果
        """
        try:
            # Validate inputs
            full_code = self._convert_to_8digit(stock_code)
            section = validate_section(section)
            
            # Get strategy for section
            strategy = self._strategies.get(section)
            if not strategy:
                raise DataFormatError(f"No strategy available for section: {section}")
            
            # Execute strategy
            success = strategy.execute(full_code, value, data)
            
            if success:
                return OperationResult(
                    success=True,
                    message=f"Successfully updated {section} for {stock_code}",
                    operation_type="UPDATE",
                    affected_records=1,
                    details={
                        'stock_code': stock_code,
                        'full_code': full_code,
                        'section': section,
                        'value': value
                    }
                )
            else:
                return OperationResult(
                    success=False,
                    message=f"Failed to update {section} for {stock_code}",
                    operation_type="UPDATE",
                    affected_records=0
                )
                
        except Exception as e:
            return OperationResult(
                success=False,
                message=f"Error updating {section} for {stock_code}: {str(e)}",
                operation_type="UPDATE",
                affected_records=0,
                details={'error': str(e)}
            )
    
    def add_stock_data(self, stock_info: StockInfo, 
                      data: Dict[str, Dict[str, str]]) -> OperationResult:
        """
        Add comprehensive stock data
        
        Args:
            stock_info: Stock information to add
            data: Data dictionary
            
        Returns:
            Operation result
        """
        try:
            full_code = self._convert_to_8digit(stock_info.stock_code)
            updated_sections = []
            
            # Update each non-None field
            field_mappings = {
                'mark': DataSection.MARK.value,
                'tip': DataSection.TIP.value,
                'tipword': DataSection.TIPWORD.value,
                'tipcolor': DataSection.TIPCOLOR.value,
                'time': DataSection.TIME.value,
            }
            
            for field, section in field_mappings.items():
                value = getattr(stock_info, field)
                if value is not None:
                    result = self.update_section_value(
                        stock_info.stock_code, section, value, data
                    )
                    if result.success:
                        updated_sections.append(section)
            
            return OperationResult(
                success=len(updated_sections) > 0,
                message=f"Added stock data for {stock_info.stock_code}",
                operation_type="CREATE",
                affected_records=len(updated_sections),
                details={
                    'stock_code': stock_info.stock_code,
                    'updated_sections': updated_sections
                }
            )
            
        except Exception as e:
            return OperationResult(
                success=False,
                message=f"Error adding stock data: {str(e)}",
                operation_type="CREATE",
                affected_records=0,
                details={'error': str(e)}
            )
    
    def delete_stock(self, stock_code: str, 
                    data: Dict[str, Dict[str, str]],
                    sections: Optional[List[str]] = None) -> OperationResult:
        """
        Delete stock from specified sections or all sections
        
        Args:
            stock_code: Stock code to delete
            data: Data dictionary
            sections: Specific sections to delete from, None for all
            
        Returns:
            Operation result
        """
        try:
            full_code = self._convert_to_8digit(stock_code)
            deleted_count = 0
            target_sections = sections or list(data.keys())
            
            for section in target_sections:
                if section in data and full_code in data[section]:
                    del data[section][full_code]
                    deleted_count += 1
            
            return OperationResult(
                success=deleted_count > 0,
                message=f"Deleted stock {stock_code} from {deleted_count} sections",
                operation_type="DELETE",
                affected_records=deleted_count,
                details={
                    'stock_code': stock_code,
                    'deleted_from_sections': deleted_count
                }
            )
            
        except Exception as e:
            return OperationResult(
                success=False,
                message=f"Error deleting stock: {str(e)}",
                operation_type="DELETE",
                affected_records=0,
                details={'error': str(e)}
            )
    
    def batch_update(self, updates: Dict[str, str], section: str,
                    data: Dict[str, Dict[str, str]]) -> BatchOperationResult:
        """
        Perform batch updates for a specific section
        
        Args:
            updates: Dictionary of stock_code -> value mappings
            section: Section to update
            data: Data dictionary
            
        Returns:
            Batch operation result
        """
        total_items = len(updates)
        successful_items = 0
        failed_items = 0
        individual_results = {}
        errors = []
        
        for stock_code, value in updates.items():
            try:
                result = self.update_section_value(stock_code, section, value, data)
                individual_results[stock_code] = result.success
                
                if result.success:
                    successful_items += 1
                else:
                    failed_items += 1
                    errors.append(f"{stock_code}: {result.message}")
                    
            except Exception as e:
                individual_results[stock_code] = False
                failed_items += 1
                errors.append(f"{stock_code}: {str(e)}")
        
        return BatchOperationResult(
            total_items=total_items,
            successful_items=successful_items,
            failed_items=failed_items,
            individual_results=individual_results,
            errors=errors
        )
    
    def get_stock_data(self, stock_code: str, 
                      data: Dict[str, Dict[str, str]]) -> Optional[StockInfo]:
        """
        Retrieve comprehensive stock data
        
        Args:
            stock_code: Stock code to retrieve
            data: Data dictionary
            
        Returns:
            Stock information or None if not found
        """
        try:
            full_code = self._convert_to_8digit(stock_code)
            market = self._get_market_from_code(full_code)
            
            # Extract data from all sections
            sections_data = {}
            for section in DataSection:
                section_data = data.get(section.value, {})
                sections_data[section.value] = section_data.get(full_code, '')
            
            return StockInfo(
                stock_code=stock_code,
                full_code=full_code,
                market=market,
                mark=sections_data.get(DataSection.MARK.value) or None,
                tip=sections_data.get(DataSection.TIP.value) or None,
                tipword=sections_data.get(DataSection.TIPWORD.value) or None,
                tipcolor=sections_data.get(DataSection.TIPCOLOR.value) or None,
                time=sections_data.get(DataSection.TIME.value) or None,
            )
            
        except Exception:
            return None
    
    def search_stocks(self, criteria: Dict[str, Any], 
                     data: Dict[str, Dict[str, str]]) -> List[StockInfo]:
        """
        Search stocks based on criteria
        
        Args:
            criteria: Search criteria
            data: Data dictionary
            
        Returns:
            List of matching stocks
        """
        results = []
        all_codes = set()
        
        # Collect all unique stock codes from all sections
        for section_data in data.values():
            all_codes.update(section_data.keys())
        
        for full_code in all_codes:
            try:
                stock_code = self._extract_stock_code(full_code)
                stock_info = self.get_stock_data(stock_code, data)
                
                if stock_info and self._matches_criteria(stock_info, criteria):
                    results.append(stock_info)
                    
            except Exception:
                continue
        
        return results
    
    def _convert_to_8digit(self, stock_code: str) -> str:
        """Convert stock code to 8-digit format"""
        from .tdx_mark_manager import TdxMarkManager
        return TdxMarkManager.convert_to_8digit(stock_code)
    
    def _extract_stock_code(self, full_code: str) -> str:
        """Extract 6-digit code from 8-digit format"""
        from .tdx_mark_manager import TdxMarkManager
        return TdxMarkManager.extract_stock_code(full_code)
    
    def _get_market_from_code(self, full_code: str) -> str:
        """Get market name from full code"""
        from .tdx_mark_manager import TdxMarkManager
        return TdxMarkManager.get_market_code(full_code)
    
    def _matches_criteria(self, stock_info: StockInfo, criteria: Dict[str, Any]) -> bool:
        """Check if stock matches search criteria"""
        # Implementation would check various criteria fields
        # This is a simplified version
        
        if 'tipword' in criteria and criteria['tipword']:
            if not stock_info.tipword or criteria['tipword'] not in stock_info.tipword:
                return False
        
        if 'market_code' in criteria and criteria['market_code']:
            if not stock_info.full_code.startswith(criteria['market_code']):
                return False
        
        return True