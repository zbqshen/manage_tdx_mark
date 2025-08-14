#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
TDX 标记管理器自定义异常
=============================

此模块定义自定义异常层次结构，以便更好地
处理错误和调试能力。
"""

from typing import Optional, Any, Dict
from .constants import ErrorCode


class TdxMarkException(Exception):
    """所有TDX标记管理器错误的基础异常类"""
    
    def __init__(
        self,
        message: str,
        error_code: ErrorCode = ErrorCode.SUCCESS,
        details: Optional[Dict[str, Any]] = None
    ):
        """
        初始化TDX标记异常
        
        Args:
            message: 错误消息
            error_code: 具体错误代码
            details: 附加错误详情
        """
        super().__init__(message)
        self.message = message
        self.error_code = error_code
        self.details = details or {}
    
    def __str__(self) -> str:
        """异常的字符串表示"""
        base_msg = f"[{self.error_code.name}] {self.message}"
        if self.details:
            details_str = ", ".join(f"{k}={v}" for k, v in self.details.items())
            return f"{base_msg} ({details_str})"
        return base_msg


class FileOperationError(TdxMarkException):
    """当文件操作失败时抛出"""
    
    def __init__(self, message: str, path: Optional[str] = None, **kwargs):
        details = {"path": path} if path else {}
        details.update(kwargs)
        super().__init__(message, ErrorCode.FILE_NOT_FOUND, details)


class ValidationError(TdxMarkException):
    """当数据验证失败时抛出"""
    
    def __init__(self, message: str, field: Optional[str] = None, value: Any = None, **kwargs):
        details = {}
        if field:
            details["field"] = field
        if value is not None:
            details["value"] = value
        details.update(kwargs)
        super().__init__(message, ErrorCode.VALIDATION_ERROR, details)


class DataFormatError(TdxMarkException):
    """当数据格式无效时抛出"""
    
    def __init__(self, message: str, section: Optional[str] = None, **kwargs):
        details = {"section": section} if section else {}
        details.update(kwargs)
        super().__init__(message, ErrorCode.INVALID_FORMAT, details)


class EncodingError(TdxMarkException):
    """当编码/解码失败时抛出"""
    
    def __init__(self, message: str, encoding: Optional[str] = None, **kwargs):
        details = {"encoding": encoding} if encoding else {}
        details.update(kwargs)
        super().__init__(message, ErrorCode.ENCODING_ERROR, details)


class BackupError(TdxMarkException):
    """当备份操作失败时抛出"""
    
    def __init__(self, message: str, backup_path: Optional[str] = None, **kwargs):
        details = {"backup_path": backup_path} if backup_path else {}
        details.update(kwargs)
        super().__init__(message, ErrorCode.BACKUP_FAILED, details)


class SaveError(TdxMarkException):
    """当保存操作失败时抛出"""
    
    def __init__(self, message: str, target_path: Optional[str] = None, **kwargs):
        details = {"target_path": target_path} if target_path else {}
        details.update(kwargs)
        super().__init__(message, ErrorCode.SAVE_FAILED, details)


class DataCorruptionError(TdxMarkException):
    """当检测到数据损坏时抛出"""
    
    def __init__(self, message: str, corruption_type: Optional[str] = None, **kwargs):
        details = {"corruption_type": corruption_type} if corruption_type else {}
        details.update(kwargs)
        super().__init__(message, ErrorCode.DATA_CORRUPTION, details)


class LockTimeoutError(TdxMarkException):
    """当锁获取超时时抛出"""
    
    def __init__(self, message: str, timeout: Optional[float] = None, **kwargs):
        details = {"timeout": timeout} if timeout else {}
        details.update(kwargs)
        super().__init__(message, ErrorCode.LOCK_TIMEOUT, details)


class StockCodeError(ValidationError):
    """当股票代码验证失败时抛出"""
    
    def __init__(self, code: str, reason: Optional[str] = None):
        message = f"无效的股票代码: {code}"
        if reason:
            message += f" - {reason}"
        super().__init__(message, field="stock_code", value=code)


class SectionNotFoundError(DataFormatError):
    """当找不到所需的区块时抛出"""
    
    def __init__(self, section: str):
        message = f"找不到区块: {section}"
        super().__init__(message, section=section)


class ConfigurationError(TdxMarkException):
    """当配置无效或缺失时抛出"""
    
    def __init__(self, message: str, config_key: Optional[str] = None, **kwargs):
        details = {"config_key": config_key} if config_key else {}
        details.update(kwargs)
        super().__init__(message, ErrorCode.VALIDATION_ERROR, details)