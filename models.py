#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
TDX 标记管理器数据模型
========================

此模块定义数据类和模型，以提供更好的类型安全
和代码组织。
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Any, Union
from datetime import datetime
from enum import Enum

from .constants import DataSection, MarketCode, OperationType


@dataclass
class StockInfo:
    """
    表示全面的股票信息
    """
    stock_code: str
    full_code: str
    market: str
    mark: Optional[str] = None
    tip: Optional[str] = None
    tipword: Optional[str] = None
    tipcolor: Optional[str] = None
    time: Optional[str] = None
    
    def __post_init__(self):
        """初始化后验证数据"""
        from .validators import validate_stock_code
        validate_stock_code(self.full_code, allow_short=False)


@dataclass
class ValidationResult:
    """
    数据验证操作的结果
    """
    is_valid: bool
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    stats: Dict[str, Any] = field(default_factory=dict)
    suggestions: List[str] = field(default_factory=list)
    
    @property
    def has_errors(self) -> bool:
        """检查验证是否有错误"""
        return len(self.errors) > 0
    
    @property
    def has_warnings(self) -> bool:
        """检查验证是否有警告"""
        return len(self.warnings) > 0


@dataclass
class OperationResult:
    """
    Result of an operation with detailed information
    """
    success: bool
    message: str
    operation_type: OperationType
    affected_records: int = 0
    backup_path: Optional[str] = None
    timestamp: datetime = field(default_factory=datetime.now)
    details: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        return {
            'success': self.success,
            'message': self.message,
            'operation_type': self.operation_type.value,
            'affected_records': self.affected_records,
            'backup_path': self.backup_path,
            'timestamp': self.timestamp.isoformat(),
            'details': self.details
        }


@dataclass
class BatchOperationResult:
    """
    Result of batch operations
    """
    total_items: int
    successful_items: int
    failed_items: int
    individual_results: Dict[str, bool] = field(default_factory=dict)
    errors: List[str] = field(default_factory=list)
    
    @property
    def success_rate(self) -> float:
        """Calculate success rate percentage"""
        if self.total_items == 0:
            return 0.0
        return (self.successful_items / self.total_items) * 100
    
    @property
    def is_complete_success(self) -> bool:
        """Check if all items succeeded"""
        return self.failed_items == 0 and self.total_items > 0


@dataclass
class DataStats:
    """
    Statistical information about the data
    """
    total_records: int
    sections: Dict[str, int] = field(default_factory=dict)
    markets: Dict[str, int] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=datetime.now)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        return {
            'total_records': self.total_records,
            'sections': self.sections,
            'markets': self.markets,
            'timestamp': self.timestamp.isoformat()
        }


@dataclass
class BackupInfo:
    """
    Information about a backup file
    """
    path: str
    timestamp: datetime
    size: int
    original_file: str
    checksum: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        return {
            'path': self.path,
            'timestamp': self.timestamp.isoformat(),
            'size': self.size,
            'original_file': self.original_file,
            'checksum': self.checksum
        }


@dataclass
class AuditEntry:
    """
    Single audit log entry
    """
    timestamp: datetime
    operation: OperationType
    user: Optional[str]
    details: Dict[str, Any] = field(default_factory=dict)
    result: Optional[OperationResult] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        return {
            'timestamp': self.timestamp.isoformat(),
            'operation': self.operation.value,
            'user': self.user,
            'details': self.details,
            'result': self.result.to_dict() if self.result else None
        }


@dataclass
class ProcessingOptions:
    """
    Options for data processing operations
    """
    auto_clean: bool = True
    auto_merge: bool = True
    auto_normalize: bool = True
    create_backup: bool = True
    validate_before_save: bool = True
    safe_mode: bool = True
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        return {
            'auto_clean': self.auto_clean,
            'auto_merge': self.auto_merge,
            'auto_normalize': self.auto_normalize,
            'create_backup': self.create_backup,
            'validate_before_save': self.validate_before_save,
            'safe_mode': self.safe_mode
        }


@dataclass
class SearchCriteria:
    """
    Criteria for searching stock data
    """
    tipword: Optional[str] = None
    market_code: Optional[str] = None
    section: Optional[DataSection] = None
    stock_code_pattern: Optional[str] = None
    value_pattern: Optional[str] = None
    limit: Optional[int] = None
    
    def is_empty(self) -> bool:
        """Check if no search criteria are specified"""
        return all(value is None for value in [
            self.tipword, self.market_code, self.section,
            self.stock_code_pattern, self.value_pattern
        ])


@dataclass
class ComparisonResult:
    """
    Result of comparing two data files
    """
    file1_path: str
    file2_path: str
    timestamp: datetime
    differences: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    stats: Dict[str, DataStats] = field(default_factory=dict)
    summary: Dict[str, int] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        return {
            'file1_path': self.file1_path,
            'file2_path': self.file2_path,
            'timestamp': self.timestamp.isoformat(),
            'differences': self.differences,
            'stats': {k: v.to_dict() for k, v in self.stats.items()},
            'summary': self.summary
        }


class CacheStatus(Enum):
    """Status of cached data"""
    VALID = "valid"
    EXPIRED = "expired"
    INVALID = "invalid"
    MISSING = "missing"


@dataclass
class CacheEntry:
    """
    Cached data entry
    """
    data: Any
    timestamp: datetime
    ttl: int  # Time to live in seconds
    access_count: int = 0
    
    @property
    def is_expired(self) -> bool:
        """Check if cache entry is expired"""
        age = (datetime.now() - self.timestamp).total_seconds()
        return age > self.ttl
    
    @property
    def status(self) -> CacheStatus:
        """Get cache entry status"""
        if self.is_expired:
            return CacheStatus.EXPIRED
        return CacheStatus.VALID
    
    def access(self) -> Any:
        """Access cached data and increment counter"""
        self.access_count += 1
        return self.data


@dataclass
class ConfigurationSchema:
    """
    Schema for configuration validation
    """
    required_sections: Set[str] = field(default_factory=lambda: {
        'PATHS', 'BACKUP', 'LOGGING', 'VALIDATION', 'PROCESSING'
    })
    required_keys: Dict[str, Set[str]] = field(default_factory=lambda: {
        'PATHS': {'primary_mark_dat', 'backup_directory', 'log_file'},
        'BACKUP': {'auto_backup', 'max_backup_files'},
        'LOGGING': {'level', 'max_file_size'},
        'VALIDATION': {'strict_code_validation'},
        'PROCESSING': {'enable_safe_mode'}
    })
    
    def validate_config(self, config: Dict[str, Any]) -> ValidationResult:
        """
        Validate configuration against schema
        
        Args:
            config: Configuration to validate
            
        Returns:
            Validation result
        """
        result = ValidationResult(is_valid=True)
        
        # Check required sections
        for section in self.required_sections:
            if section not in config:
                result.errors.append(f"Missing required section: {section}")
                result.is_valid = False
            elif section in self.required_keys:
                # Check required keys in section
                section_config = config[section]
                for key in self.required_keys[section]:
                    if key not in section_config:
                        result.errors.append(f"Missing required key: {section}.{key}")
                        result.is_valid = False
        
        return result