#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
通达信标记管理器常量和配置值
============================

此模块集中管理所有魔法值、常量和配置默认值，
提高可维护性并减少代码重复。
"""

from enum import Enum
from typing import Final

# 版本信息
VERSION: Final[str] = "3.1.0"
AUTHOR: Final[str] = "Kilo Code"
PACKAGE_NAME: Final[str] = "manage_tdx_mark"

# 文件编码
FILE_ENCODING: Final[str] = "gbk"
LOG_ENCODING: Final[str] = "utf-8"

# 股票代码格式
STOCK_CODE_LENGTH: Final[int] = 8
STOCK_CODE_SHORT_LENGTH: Final[int] = 6
MARKET_CODE_LENGTH: Final[int] = 2

# 文件路径
DEFAULT_MARK_DAT_PATH: Final[str] = r"D:\Tdx MPV V1.24++\T0002\mark.dat"
DEFAULT_CONFIG_FILE: Final[str] = "tdx_mark_config.ini"
DEFAULT_BACKUP_DIR: Final[str] = "./backups"
DEFAULT_LOG_DIR: Final[str] = "./log"

# 备份设置
MAX_BACKUP_FILES: Final[int] = 30
BACKUP_FILE_PREFIX: Final[str] = "mark_backup_"
BACKUP_TIMESTAMP_FORMAT: Final[str] = "%Y%m%d_%H%M%S"

# 日志设置
LOG_FORMAT: Final[str] = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
LOG_DATE_FORMAT: Final[str] = "%Y-%m-%d %H:%M:%S"
LOG_MAX_FILE_SIZE: Final[int] = 10 * 1024 * 1024  # 10MB文件大小
LOG_BACKUP_COUNT: Final[int] = 5
LOGGER_NAME: Final[str] = "tdx_mark_manager"

# 数据区块
class DataSection(str, Enum):
    """mark.dat文件中有效数据区块的枚举"""
    MARK = "MARK"
    TIP = "TIP"
    TIPWORD = "TIPWORD"
    TIPCOLOR = "TIPCOLOR"
    TIME = "TIME"

SUPPORTED_SECTIONS: Final[list] = [section.value for section in DataSection]

# 市场代码
class MarketCode(str, Enum):
    """股票市场代码的枚举"""
    SHENZHEN = "00"  # 深交所
    SHANGHAI = "01"  # 上交所
    BEIJING = "02"   # 北交所

MARKET_NAMES: Final[dict] = {
    MarketCode.SHENZHEN: "深交所",
    MarketCode.SHANGHAI: "上交所",
    MarketCode.BEIJING: "北交所",
}

# 各市场股票代码前缀
SHANGHAI_PREFIXES: Final[tuple] = ("6",)
SHENZHEN_PREFIXES: Final[tuple] = ("0", "3")
BEIJING_PREFIXES: Final[tuple] = ("4", "8")

# 数据验证
MAX_REMARK_LENGTH: Final[int] = 500
MAX_HISTORY_ENTRIES: Final[int] = 1000
MAX_OPERATION_HISTORY: Final[int] = 1000
HISTORY_CLEANUP_THRESHOLD: Final[int] = 500

# TIPWORD设置
TIPWORD_SEPARATOR: Final[str] = "/"
TIPWORD_DEFAULT_VALUE: Final[str] = ""

# 性能设置
DEFAULT_CACHE_TTL: Final[int] = 300  # 5分钟缓存时间
MAX_CACHE_SIZE: Final[int] = 100
BATCH_OPERATION_CHUNK_SIZE: Final[int] = 100

# 验证消息
class ValidationMessage(str, Enum):
    """标准验证错误消息"""
    INVALID_STOCK_CODE = "无效的股票代码格式: {code}"
    FILE_NOT_FOUND = "文件不存在: {path}"
    ENCODING_ERROR = "文件编码错误 (应使用GBK编码): {error}"
    SECTION_NOT_FOUND = "未找到数据区块: {section}"
    VALUE_TOO_LONG = "值超过最大长度限制: {length} > {max_length}"

# 错误代码
class ErrorCode(int, Enum):
    """操作的标准错误代码"""
    SUCCESS = 0
    FILE_NOT_FOUND = 1001
    INVALID_FORMAT = 1002
    ENCODING_ERROR = 1003
    VALIDATION_ERROR = 1004
    PERMISSION_ERROR = 1005
    BACKUP_FAILED = 2001
    SAVE_FAILED = 2002
    DATA_CORRUPTION = 3001
    LOCK_TIMEOUT = 4001

# 审计日志的操作类型
class OperationType(str, Enum):
    """审计日志的操作类型"""
    CREATE = "CREATE"
    READ = "READ"
    UPDATE = "UPDATE"
    DELETE = "DELETE"
    BACKUP = "BACKUP"
    RESTORE = "RESTORE"
    VALIDATE = "VALIDATE"
    REPAIR = "REPAIR"
    MERGE = "MERGE"
    CLEAN = "CLEAN"

# 配置默认值
DEFAULT_CONFIG = {
    "PATHS": {
        "primary_mark_dat": DEFAULT_MARK_DAT_PATH,
        "backup_directory": DEFAULT_BACKUP_DIR,
        "log_file": f"{DEFAULT_LOG_DIR}/tdx_mark_manager.log",
    },
    "BACKUP": {
        "auto_backup": True,
        "backup_interval_hours": 24,
        "max_backup_files": MAX_BACKUP_FILES,
        "auto_backup_before_operations": True,
    },
    "LOGGING": {
        "level": "INFO",
        "max_file_size": LOG_MAX_FILE_SIZE,
        "backup_count": LOG_BACKUP_COUNT,
        "enable_operation_history": True,
        "max_history_entries": MAX_HISTORY_ENTRIES,
    },
    "VALIDATION": {
        "strict_code_validation": True,
        "max_remark_length": MAX_REMARK_LENGTH,
        "enable_auto_repair": False,
        "validate_before_save": True,
    },
    "PROCESSING": {
        "auto_clean_empty_values": True,
        "auto_merge_duplicates": True,
        "auto_normalize_data": True,
        "enable_safe_mode": True,
        "max_processing_threads": 1,
    },
}

# 报告模板
REPORT_HEADER: Final[str] = "=" * 60
REPORT_FOOTER: Final[str] = "=" * 60
REPORT_SECTION_SEPARATOR: Final[str] = "-" * 40