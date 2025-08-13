#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
通达信 mark.dat 文件管理包
===========================

这个包提供了完整的通达信 mark.dat 文件管理功能，包括：
- 安全读取和解析 mark.dat 文件
- 数据结构管理和验证
- 文件备份和恢复功能
- 数据操作（增删改查）
- 批量操作和数据清理
- 数据合并和标准化
- 统计分析和报告生成

主要类：
    TdxMarkManager: 核心管理类，提供所有功能

使用示例：
    from manage_tdx_mark import TdxMarkManager
    
    # 创建管理器实例
    manager = TdxMarkManager()
    
    # 执行功能测试
    test_results = manager.test_functionality()
    
    # 加载和处理数据
    data = manager.load_data()
    summary = manager.get_data_summary(data)

作者: Kilo Code
版本: 3.0.0
"""

from .tdx_mark_manager import TdxMarkManager

# 包版本信息
__version__ = "3.0.0"
__author__ = "Kilo Code"
__email__ = "kilocode@example.com"
__description__ = "通达信 mark.dat 文件管理工具"

# 导出的公共接口
__all__ = [
    'TdxMarkManager',
]

# 包级别的常量
SUPPORTED_SECTIONS = ['MARK', 'TIP', 'TIPWORD', 'TIPCOLOR', 'TIME']
DEFAULT_CONFIG_FILE = './manage_tdx_mark/tdx_mark_config.ini'
DEFAULT_MARK_DAT_PATH = r'D:\Tdx MPV V1.24++\T0002\mark.dat'

def get_version():
    """获取包版本号"""
    return __version__

def get_info():
    """获取包信息"""
    return {
        'name': 'manage_tdx_mark',
        'version': __version__,
        'author': __author__,
        'description': __description__,
        'supported_sections': SUPPORTED_SECTIONS
    } 