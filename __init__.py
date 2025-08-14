#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
通达信 mark.dat 文件管理包 - 增强版
=====================================

这个包提供了完整的通达信 mark.dat 文件管理功能，包括：

核心功能：
- 安全读取和解析 mark.dat 文件
- 数据结构管理和验证  
- 文件备份和恢复功能
- 数据操作（增删改查）
- 批量操作和数据清理
- 数据合并和标准化
- 统计分析和报告生成

新增特性（v3.1.0）：
- 🔒 增强的安全验证和输入净化
- ⚡ 智能缓存系统提升性能
- 🏗️ 模块化架构和组件分离
- 📊 详细的数据模型和类型提示
- 🚀 策略模式重构减少代码重复

主要组件：
    TdxMarkManager: 核心管理类（兼容原API）
    DataOperationService: 数据操作服务层
    DataCache: 智能缓存系统
    StockInfo: 类型安全的数据模型
    InputValidator: 输入验证工具

使用示例：
    # 基础使用（向后兼容）
    from manage_tdx_mark import TdxMarkManager
    manager = TdxMarkManager()
    data = manager.load_data()
    
    # 新增API使用
    from manage_tdx_mark import DataOperationService, StockInfo
    service = DataOperationService()
    stock = StockInfo(stock_code="600613", full_code="01600613", market="上交所")
    result = service.add_stock_data(stock, data)

作者: Kilo Code
版本: 3.1.0
"""

# 核心组件导入
from .tdx_mark_manager import TdxMarkManager
from .data_service import DataOperationService
from .safe_batch_service import (
    SafeBatchService, SafeBatchConfig, SafeDeleteConfig,
    DeleteMode, create_safe_batch_config
)
from .models import (
    StockInfo, ValidationResult, OperationResult, 
    BatchOperationResult, DataStats, BackupInfo
)
from .validators import (
    validate_stock_code, validate_path, validate_section,
    InputValidator
)
from .cache import CacheManager, LRUCache, cache_result
from .constants import DataSection, MarketCode, VERSION
from .exceptions import (
    TdxMarkException, ValidationError, FileOperationError,
    DataFormatError, EncodingError, BackupError
)

# 包版本信息
__version__ = VERSION
__author__ = "Kilo Code"
__email__ = "kilocode@example.com"
__description__ = "通达信 mark.dat 文件管理工具 - 增强版"

# 导出的公共接口
__all__ = [
    # 核心类
    'TdxMarkManager',
    'DataOperationService',
    'SafeBatchService',
    
    # 配置和工具
    'SafeBatchConfig',
    'SafeDeleteConfig',
    'DeleteMode',
    'create_safe_batch_config',
    
    # 数据模型
    'StockInfo', 
    'ValidationResult',
    'OperationResult',
    'BatchOperationResult',
    'DataStats',
    'BackupInfo',
    
    # 验证工具
    'validate_stock_code',
    'validate_path', 
    'validate_section',
    'InputValidator',
    
    # 缓存系统
    'CacheManager',
    'LRUCache',
    'cache_result',
    
    # 常量和枚举
    'DataSection',
    'MarketCode',
    
    # 异常类
    'TdxMarkException',
    'ValidationError',
    'FileOperationError',
    'DataFormatError',
    'EncodingError',
    'BackupError',
    
    # 包信息函数
    'get_version',
    'get_info',
    'get_supported_sections',
    'get_improvements'
]

# 包级别的常量（保持向后兼容）
SUPPORTED_SECTIONS = [section.value for section in DataSection]
DEFAULT_CONFIG_FILE = './manage_tdx_mark/tdx_mark_config.ini'
DEFAULT_MARK_DAT_PATH = r'D:\Tdx MPV V1.24++\T0002\mark.dat'


def get_version() -> str:
    """获取包版本号"""
    return __version__


def get_info() -> dict:
    """获取包信息"""
    return {
        'name': 'manage_tdx_mark',
        'version': __version__,
        'author': __author__,
        'description': __description__,
        'supported_sections': SUPPORTED_SECTIONS,
        'new_features': [
            '安全验证增强',
            '智能缓存系统', 
            '模块化架构',
            '类型安全保证'
        ]
    }


def get_supported_sections() -> list:
    """获取支持的数据区块列表"""
    return SUPPORTED_SECTIONS


def get_improvements() -> dict:
    """获取v3.1.0版本的改进内容"""
    return {
        'security': [
            '路径遍历攻击防护',
            '输入验证和净化',
            '更强的哈希算法',
            '敏感数据屏蔽'
        ],
        'performance': [
            'LRU缓存机制',
            '智能数据缓存',
            '批量操作优化',
            '内存使用优化'
        ],
        'architecture': [
            '单一职责原则重构',
            '策略模式消除重复代码',
            '依赖注入模式',
            '清晰的抽象层'
        ],
        'quality': [
            '全面的类型提示',
            '自定义异常层次',
            '代码质量检查'
        ],
        'maintainability': [
            '常量提取',
            '配置文件验证',
            '详细的操作日志',
            '模块化组件设计'
        ]
    } 