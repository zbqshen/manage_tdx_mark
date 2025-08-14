#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
TDX 标记管理器输入验证工具
========================

此模块提供全面的输入验证和净化功能，
以防止安全漏洞并确保数据完整性。
"""

import re
import os
from pathlib import Path
from typing import Optional, Any, Callable, TypeVar, Union
from functools import wraps

from .constants import (
    STOCK_CODE_LENGTH,
    STOCK_CODE_SHORT_LENGTH,
    MARKET_CODE_LENGTH,
    MarketCode,
    SHANGHAI_PREFIXES,
    SHENZHEN_PREFIXES,
    BEIJING_PREFIXES,
    MAX_REMARK_LENGTH,
    DataSection,
)
from .exceptions import ValidationError, StockCodeError

T = TypeVar('T')


def validate_stock_code(code: str, allow_short: bool = True) -> str:
    """
    验证和净化股票代码
    
    Args:
        code: 要验证的股票代码
        allow_short: 是否允许6位代码
        
    Returns:
        验证后的股票代码
        
    Raises:
        StockCodeError: 如果代码无效
    """
    if not code:
        raise StockCodeError("", "代码不能为空")
    
    # 移除任何空格
    code = code.strip()
    
    # 检查是否为数字
    if not code.isdigit():
        raise StockCodeError(code, "代码只能包含数字")
    
    # 检查长度
    if len(code) == STOCK_CODE_LENGTH:
        # 验证市场代码
        market_code = code[:MARKET_CODE_LENGTH]
        if market_code not in [mc.value for mc in MarketCode]:
            raise StockCodeError(code, f"无效的市场代码: {market_code}")
        return code
    elif len(code) == STOCK_CODE_SHORT_LENGTH and allow_short:
        return code
    else:
        expected = f"{STOCK_CODE_LENGTH} 或 {STOCK_CODE_SHORT_LENGTH}" if allow_short else str(STOCK_CODE_LENGTH)
        raise StockCodeError(code, f"代码必须为 {expected} 位数字")


def validate_path(path: Union[str, Path], must_exist: bool = False, 
                 allow_relative: bool = False) -> Path:
    """
    验证和净化文件路径以防止路径遍历攻击
    
    Args:
        path: 要验证的路径
        must_exist: 路径是否必须存在
        allow_relative: 是否允许相对路径
        
    Returns:
        验证后的Path对象
        
    Raises:
        ValidationError: 如果路径无效或危险
    """
    if not path:
        raise ValidationError("路径不能为空", field="path")
    
    # 转换为Path对象
    path_obj = Path(path) if not isinstance(path, Path) else path
    
    # 检查路径遍历尝试
    try:
        # 解析为绝对路径以检测遍历
        resolved = path_obj.resolve()
        
        # 检查可疑模式
        path_str = str(path)
        dangerous_patterns = ['..', '~', '$', '%', '\\\\']
        for pattern in dangerous_patterns:
            if pattern in path_str and not allow_relative:
                raise ValidationError(
                    f"检测到潜在危险的路径模式: {pattern}",
                    field="path",
                    value=str(path)
                )
        
        # 检查路径是否存在（当需要时）
        if must_exist and not resolved.exists():
            raise ValidationError(
                f"路径不存在: {resolved}",
                field="path",
                value=str(path)
            )
        
        return resolved
        
    except (OSError, RuntimeError) as e:
        raise ValidationError(
            f"无效的路径: {e}",
            field="path",
            value=str(path)
        )


def validate_section(section: str) -> str:
    """
    验证数据区块名称
    
    Args:
        section: 要验证的区块名称
        
    Returns:
        验证后的区块名称
        
    Raises:
        ValidationError: 如果区块无效
    """
    if not section:
        raise ValidationError("区块不能为空", field="section")
    
    section = section.strip().upper()
    
    if section not in [s.value for s in DataSection]:
        raise ValidationError(
            f"无效的区块: {section}",
            field="section",
            value=section,
            valid_sections=[s.value for s in DataSection]
        )
    
    return section


def validate_value_length(value: str, max_length: int = MAX_REMARK_LENGTH,
                         field_name: str = "value") -> str:
    """
    验证值的长度
    
    Args:
        value: Value to validate
        max_length: Maximum allowed length
        field_name: Name of the field for error messages
        
    Returns:
        Validated value
        
    Raises:
        ValidationError: If value exceeds max length
    """
    if len(value) > max_length:
        raise ValidationError(
            f"{field_name} exceeds maximum length",
            field=field_name,
            value=value[:50] + "...",
            length=len(value),
            max_length=max_length
        )
    
    return value


def sanitize_string(value: str, allow_newlines: bool = True,
                   allow_special: bool = True) -> str:
    """
    Sanitize string value to remove potentially dangerous characters
    
    Args:
        value: String to sanitize
        allow_newlines: Whether to allow newline characters
        allow_special: Whether to allow special characters
        
    Returns:
        Sanitized string
    """
    if not value:
        return ""
    
    # Remove null bytes
    value = value.replace('\x00', '')
    
    # Handle newlines
    if not allow_newlines:
        value = value.replace('\n', ' ').replace('\r', ' ')
    
    # Remove control characters except allowed ones
    if not allow_special:
        # Keep only printable ASCII and common Chinese characters
        value = re.sub(r'[^\x20-\x7E\u4e00-\u9fff\n\r\t]', '', value)
    
    return value.strip()


def validate_config_key(key: str, section: Optional[str] = None) -> str:
    """
    Validate configuration key
    
    Args:
        key: Configuration key to validate
        section: Optional section name
        
    Returns:
        Validated key
        
    Raises:
        ValidationError: If key is invalid
    """
    if not key:
        raise ValidationError("Configuration key cannot be empty", field="config_key")
    
    # Only allow alphanumeric and underscore
    if not re.match(r'^[A-Za-z0-9_]+$', key):
        raise ValidationError(
            f"Invalid configuration key: {key}",
            field="config_key",
            value=key,
            section=section
        )
    
    return key


def validation_decorator(validate_func: Callable) -> Callable:
    """
    Decorator to add validation to methods
    
    Args:
        validate_func: Validation function to apply
        
    Returns:
        Decorated function
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Apply validation
            validate_func(*args, **kwargs)
            # Call original function
            return func(*args, **kwargs)
        return wrapper
    return decorator


def validate_batch_size(size: int, max_size: int = 1000) -> int:
    """
    Validate batch operation size
    
    Args:
        size: Batch size to validate
        max_size: Maximum allowed batch size
        
    Returns:
        Validated batch size
        
    Raises:
        ValidationError: If size is invalid
    """
    if not isinstance(size, int):
        raise ValidationError(
            "Batch size must be an integer",
            field="batch_size",
            value=size
        )
    
    if size <= 0:
        raise ValidationError(
            "Batch size must be positive",
            field="batch_size",
            value=size
        )
    
    if size > max_size:
        raise ValidationError(
            f"Batch size exceeds maximum of {max_size}",
            field="batch_size",
            value=size,
            max_size=max_size
        )
    
    return size


def validate_market_code(code: str) -> str:
    """
    Validate market code
    
    Args:
        code: Market code to validate
        
    Returns:
        Validated market code
        
    Raises:
        ValidationError: If market code is invalid
    """
    if not code:
        raise ValidationError("Market code cannot be empty", field="market_code")
    
    if len(code) != MARKET_CODE_LENGTH:
        raise ValidationError(
            f"Market code must be {MARKET_CODE_LENGTH} digits",
            field="market_code",
            value=code
        )
    
    if code not in [mc.value for mc in MarketCode]:
        raise ValidationError(
            f"Invalid market code: {code}",
            field="market_code",
            value=code,
            valid_codes=[mc.value for mc in MarketCode]
        )
    
    return code


class InputValidator:
    """
    Comprehensive input validator class
    """
    
    @staticmethod
    def validate_data_dict(data: dict) -> dict:
        """
        Validate entire data dictionary structure
        
        Args:
            data: Data dictionary to validate
            
        Returns:
            Validated data dictionary
            
        Raises:
            ValidationError: If data structure is invalid
        """
        if not isinstance(data, dict):
            raise ValidationError(
                "Data must be a dictionary",
                field="data",
                value=type(data).__name__
            )
        
        for section, section_data in data.items():
            # Validate section name
            validate_section(section)
            
            if not isinstance(section_data, dict):
                raise ValidationError(
                    f"Section data must be a dictionary",
                    field=f"data[{section}]",
                    value=type(section_data).__name__
                )
            
            # Validate each entry
            for code, value in section_data.items():
                validate_stock_code(code, allow_short=False)
                
                if not isinstance(value, str):
                    raise ValidationError(
                        f"Value must be a string",
                        field=f"data[{section}][{code}]",
                        value=type(value).__name__
                    )
        
        return data