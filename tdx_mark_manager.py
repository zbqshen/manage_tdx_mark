#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
通达信 mark.dat 文件管理工具 - 完整版本
================================

主要功能：
第一阶段：
1. 安全读取和解析 mark.dat 文件
2. 数据结构管理和验证
3. 文件备份功能
4. 基础测试功能

第二阶段（数据操作）：
5. 数据修改功能（TIPWORD特殊处理、其他区块直接替换）
6. 数据增加功能（单个和批量添加）
7. 数据删除功能（全部删除、区块删除、空值清理）
8. 数据查询功能（股票查询、关键词搜索、市场筛选）
9. 文件写入功能（保持原格式、自动备份）
10. 批量操作功能（批量更新、批量添加）

第三阶段（数据合并清理和集成）：
11. 数据合并功能（重复数据处理、TIPWORD合并）
12. 数据清理增强（标准化、验证）
13. 完整工具集成（一键处理、安全更新）
14. 统计分析（报告生成、文件比较）
15. 配置文件支持和命令行界面

作者: Kilo Code
版本: 3.0.0
"""

import os
import shutil
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Union, Tuple, Set
from logging.handlers import RotatingFileHandler
import re
import configparser
import json
import threading
import hashlib
from collections import defaultdict, Counter


class TdxMarkManager:
    """通达信 mark.dat 文件管理器 - 完整版本
    
    提供全面的mark.dat文件管理功能，包括：
    
    第一阶段功能：
    - 安全读取和解析 mark.dat 文件
    - 数据结构管理和验证
    - 文件备份功能
    - 基础测试功能
    
    第二阶段功能（数据操作）：
    - 数据修改：update_tipword, update_mark, update_tip, update_tipcolor, update_time
    - 数据增加：add_stock_data
    - 数据删除：delete_stock, delete_from_section, clear_empty_values
    - 数据查询：get_stock_data, search_by_tipword, get_stocks_by_market
    - 文件写入：save_data
    - 批量操作：batch_update_tipword, batch_add_stocks
    
    第三阶段功能（数据合并清理和集成）：
    - 数据合并：merge_duplicate_tipwords, merge_duplicate_sections
    - 数据清理：clean_empty_values, clean_all_duplicates, normalize_data
    - 数据验证：validate_data_integrity (增强版)
    - 完整集成：process_file, safe_update, repair_file
    - 统计分析：generate_report, compare_files, audit_trail
    - 配置支持：配置文件加载和管理
    """

    def __init__(self, mark_dat_path: str = None, config_file: str = None):
        """初始化管理器
        
        Args:
            mark_dat_path: mark.dat文件路径，如果为None则从配置文件读取
            config_file: 配置文件路径，如果为None则使用默认路径
        """
        
        # 如果未指定配置文件，使用模块相对路径
        if config_file is None:
            # 获取当前模块文件的目录路径
            current_dir = Path(__file__).parent
            config_file = current_dir / 'tdx_mark_config.ini'
        
        # 加载配置文件
        self.config = self._load_config(config_file)
        
        # 设置文件路径（优先使用参数，其次使用配置文件）
        self.mark_dat_path = mark_dat_path or self.config.get('PATHS', 'primary_mark_dat',
                                                            fallback=r'D:\Tdx MPV V1.24++\T0002\mark.dat')
        
        # 设置目录路径 - 使用绝对路径
        current_dir = Path(__file__).parent
        backup_dir_config = self.config.get('PATHS', 'backup_directory', fallback='./backups')
        
        # 如果配置路径是相对路径，则相对于模块目录
        if backup_dir_config.startswith('./'):
            self.backup_dir = current_dir / backup_dir_config[2:]  # 移除 './'
        else:
            self.backup_dir = Path(backup_dir_config)
        
        # 确保备份目录存在
        self.backup_dir.mkdir(parents=True, exist_ok=True)
        
        # 设置日志文件路径和目录 - 使用绝对路径
        log_file_config = self.config.get('PATHS', 'log_file', fallback='./log/tdx_mark_manager.log')
        
        # 如果配置路径是相对路径，则相对于模块目录
        if log_file_config.startswith('./'):
            self.log_file = current_dir / log_file_config[2:]  # 移除 './'
        else:
            self.log_file = Path(log_file_config)
        
        self.log_dir = self.log_file.parent
        self.log_dir.mkdir(parents=True, exist_ok=True)

        # 设置日志
        self.logger = self._setup_logger()
        
        # 支持的区块类型
        self.supported_sections = ['MARK', 'TIP', 'TIPWORD', 'TIPCOLOR', 'TIME']
        
        # 第三阶段新增属性
        self._lock = threading.Lock()  # 线程安全锁
        self._operation_history = []   # 操作历史
        self._current_data_hash = None # 当前数据哈希值
        self._cached_all_data = None
        
        self.logger.info(f"TdxMarkManager v3.0 初始化完成，目标文件: {self.mark_dat_path}")
        self.logger.info(f"备份目录: {self.backup_dir}")
        self.logger.info(f"日志目录: {self.log_dir}")
    
    def _load_config(self, config_file: str) -> configparser.ConfigParser:
        """加载配置文件
        
        Args:
            config_file: 配置文件路径
            
        Returns:
            配置对象
        """
        config = configparser.ConfigParser()
        config_path = Path(config_file)
        
        if config_path.exists():
            try:
                config.read(config_path, encoding='utf-8')
                print(f"配置文件加载成功: {config_file}")
            except Exception as e:
                print(f"配置文件加载失败: {e}，使用默认配置")
        else:
            print(f"配置文件不存在: {config_file}，使用默认配置")
            
        return config
    
    def _setup_logger(self) -> logging.Logger:
        """设置日志记录器
        
        Returns:
            配置好的Logger对象
        """
        logger = logging.getLogger('tdx_mark_manager')
        logger.setLevel(logging.INFO)
        
        # 防止重复添加处理器
        if not logger.handlers:
            # 文件处理器 - 使用轮转日志
            file_handler = RotatingFileHandler(
                self.log_file,
                maxBytes=10*1024*1024,  # 10MB
                backupCount=5,
                encoding='utf-8'
            )
            
            # 控制台处理器
            console_handler = logging.StreamHandler()
            
            # 日志格式
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
            
            file_handler.setFormatter(formatter)
            console_handler.setFormatter(formatter)
            
            logger.addHandler(file_handler)
            logger.addHandler(console_handler)
        
        return logger
    
    def create_backup(self) -> str:
        """创建mark.dat文件备份
        
        Returns:
            备份文件路径
            
        Raises:
            FileNotFoundError: 原文件不存在
            IOError: 备份创建失败
        """
        if not os.path.exists(self.mark_dat_path):
            raise FileNotFoundError(f"原文件不存在: {self.mark_dat_path}")
        
        # 生成备份文件名
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_filename = f"mark_backup_{timestamp}.dat"
        backup_path = self.backup_dir / backup_filename
        
        try:
            shutil.copy2(self.mark_dat_path, backup_path)
            self.logger.info(f"备份创建成功: {backup_path}")
            
            # 清理旧备份（保留最新30个）
            self._cleanup_old_backups()
            
            return str(backup_path)
            
        except Exception as e:
            self.logger.error(f"备份创建失败: {e}")
            raise IOError(f"备份创建失败: {e}")
    
    def _cleanup_old_backups(self, max_backups: int = 30):
        """清理旧备份文件
        
        Args:
            max_backups: 保留的最大备份数量
        """
        try:
            backup_files = list(self.backup_dir.glob('mark_backup_*.dat'))
            backup_files.sort(key=lambda x: x.stat().st_mtime, reverse=True)
            
            if len(backup_files) > max_backups:
                for old_backup in backup_files[max_backups:]:
                    old_backup.unlink()
                    self.logger.info(f"已删除旧备份: {old_backup.name}")
                    
        except Exception as e:
            self.logger.warning(f"清理旧备份时出现错误: {e}")
    
    def read_file(self, create_backup: bool = True) -> str:
        """安全读取mark.dat文件内容
        
        Args:
            create_backup: 是否在读取前创建备份
            
        Returns:
            文件内容字符串
            
        Raises:
            FileNotFoundError: 文件不存在
            UnicodeDecodeError: 编码错误
            IOError: 读取失败
        """
        if not os.path.exists(self.mark_dat_path):
            raise FileNotFoundError(f"mark.dat文件不存在: {self.mark_dat_path}")
        
        # 创建备份
        if create_backup:
            try:
                self.create_backup()
            except Exception as e:
                self.logger.warning(f"备份创建失败，继续读取: {e}")
        
        try:
            with open(self.mark_dat_path, 'r', encoding='gbk') as f:
                content = f.read()
            
            self.logger.info(f"成功读取文件，大小: {len(content)} 字符")
            return content
            
        except UnicodeDecodeError as e:
            self.logger.error(f"文件编码错误 (应使用GBK编码): {e}")
            raise
        except Exception as e:
            self.logger.error(f"文件读取失败: {e}")
            raise IOError(f"文件读取失败: {e}")
    
    def parse_content(self, content: str) -> Dict[str, Dict[str, str]]:
        """解析mark.dat文件内容到数据结构
        
        Args:
            content: 文件内容字符串
            
        Returns:
            解析后的数据字典，格式为：
            {
                'MARK': {'01600613': '8', '00002728': '7', ...},
                'TIP': {'01600613': '1、消费；\\n 2、旅游；', ...},
                ...
            }
        """
        data = {}
        current_section = None
        
        try:
            lines = content.split('\n')
            
            for line_num, line in enumerate(lines, 1):
                line = line.strip()
                
                # 跳过空行
                if not line:
                    continue
                
                # 识别区块标题 [SECTION]
                if line.startswith('[') and line.endswith(']'):
                    section_name = line[1:-1]  # 去掉方括号
                    
                    if section_name in self.supported_sections:
                        current_section = section_name
                        data[current_section] = {}
                        self.logger.debug(f"发现区块: {section_name}")
                    else:
                        self.logger.warning(f"未知区块类型: {section_name} (行 {line_num})")
                        current_section = None
                    continue
                
                # 解析键值对
                if '=' in line and current_section:
                    try:
                        key, value = line.split('=', 1)  # 只分割第一个等号
                        key = key.strip()
                        value = value.strip()
                        
                        # 验证股票代码格式 (应为8位：2位市场代码 + 6位股票代码)
                        if len(key) == 8 and key.isdigit():
                            data[current_section][key] = value
                        else:
                            self.logger.warning(f"无效的股票代码格式: {key} (行 {line_num})")
                            
                    except ValueError as e:
                        self.logger.warning(f"解析键值对失败: {line} (行 {line_num}): {e}")
            
            # 统计解析结果
            total_records = sum(len(section_data) for section_data in data.values())
            self.logger.info(f"解析完成，共 {len(data)} 个区块，{total_records} 条记录")
            
            for section, records in data.items():
                self.logger.debug(f"区块 {section}: {len(records)} 条记录")
            
            return data
            
        except Exception as e:
            self.logger.error(f"内容解析失败: {e}")
            raise ValueError(f"内容解析失败: {e}")
    
    def load_data(self, create_backup: bool = True) -> Dict[str, Dict[str, str]]:
        """加载并解析mark.dat文件
        
        Args:
            create_backup: 是否在读取前创建备份
            
        Returns:
            解析后的数据字典
        """
        content = self.read_file(create_backup)
        return self.parse_content(content)
    
    @staticmethod
    def get_market_code(full_code: str) -> str:
        """从完整股票代码中提取市场代码
        
        Args:
            full_code: 8位完整代码 (2位市场代码 + 6位股票代码)
            
        Returns:
            市场名称 ('深交所', '上交所', '北交所')
            
        Raises:
            ValueError: 代码格式无效
        """
        if len(full_code) != 8 or not full_code.isdigit():
            raise ValueError(f"无效的股票代码格式: {full_code}")
        
        market_prefix = full_code[:2]
        
        market_map = {
            '00': '深交所',
            '01': '上交所', 
            '02': '北交所'
        }
        
        return market_map.get(market_prefix, '未知市场')
    
    @staticmethod
    def extract_stock_code(full_code: str) -> str:
        """从完整股票代码中提取6位股票代码
        
        Args:
            full_code: 8位完整代码
            
        Returns:
            6位股票代码
        """
        if len(full_code) != 8:
            raise ValueError(f"无效的股票代码格式: {full_code}")
        
        return full_code[2:]
    
    @staticmethod
    def convert_to_8digit(stock_code: str) -> str:
        """将6位股票代码转换为8位格式
        
        Args:
            stock_code: 6位股票代码
            
        Returns:
            8位完整代码 (2位市场代码 + 6位股票代码)
            
        Raises:
            ValueError: 代码格式无效
        """
        if len(stock_code) == 8 and stock_code.isdigit():
            # 已经是8位格式，直接返回
            return stock_code
        
        if len(stock_code) != 6 or not stock_code.isdigit():
            raise ValueError(f"无效的股票代码格式: {stock_code}")
        
        first_digit = stock_code[0]
        
        # 根据首位数字判断市场
        if first_digit == '6':
            # 上交所
            return '01' + stock_code
        elif first_digit in ['0', '3']:
            # 深交所
            return '00' + stock_code
        elif first_digit in ['4', '8']:
            # 北交所
            return '02' + stock_code
        else:
            raise ValueError(f"无法识别的股票代码: {stock_code}")
    
    def validate_data(self, data: Dict[str, Dict[str, str]]) -> Dict[str, List[str]]:
        """验证数据结构的完整性
        
        Args:
            data: 解析后的数据字典
            
        Returns:
            验证结果字典，包含错误和警告信息
        """
        validation_result = {
            'errors': [],
            'warnings': [],
            'stats': {}
        }
        
        try:
            # 检查必需的区块
            missing_sections = []
            for section in self.supported_sections:
                if section not in data:
                    missing_sections.append(section)
            
            if missing_sections:
                validation_result['warnings'].append(f"缺少区块: {missing_sections}")
            
            # 统计各区块数据
            total_records = 0
            market_stats = {'深交所': 0, '上交所': 0, '北交所': 0, '未知市场': 0}
            
            for section, records in data.items():
                section_count = len(records)
                total_records += section_count
                validation_result['stats'][section] = section_count
                
                # 验证每条记录
                for full_code, value in records.items():
                    try:
                        # 验证代码格式
                        if len(full_code) != 8 or not full_code.isdigit():
                            validation_result['errors'].append(
                                f"区块 {section} 中发现无效股票代码: {full_code}"
                            )
                            continue
                        
                        # 统计市场分布
                        market = self.get_market_code(full_code)
                        market_stats[market] += 1
                        
                        # 检查备注长度 (建议不超过500字符)
                        if len(value) > 500:
                            validation_result['warnings'].append(
                                f"区块 {section} 中股票 {full_code} 的备注过长: {len(value)} 字符"
                            )
                            
                    except Exception as e:
                        validation_result['errors'].append(
                            f"验证股票代码 {full_code} 时出错: {e}"
                        )
            
            validation_result['stats']['总记录数'] = total_records
            validation_result['stats']['市场分布'] = market_stats
            
            # 记录验证结果
            if validation_result['errors']:
                self.logger.warning(f"数据验证发现 {len(validation_result['errors'])} 个错误")
            if validation_result['warnings']:
                self.logger.info(f"数据验证发现 {len(validation_result['warnings'])} 个警告")
            
            self.logger.info(f"数据验证完成，总记录数: {total_records}")
            
        except Exception as e:
            validation_result['errors'].append(f"验证过程中发生错误: {e}")
            self.logger.error(f"数据验证失败: {e}")
        
        return validation_result
    
    def get_data_summary(self, data: Dict[str, Dict[str, str]]) -> Dict:
        """获取数据摘要统计
        
        Args:
            data: 解析后的数据字典
            
        Returns:
            数据摘要字典
        """
        try:
            summary = {
                '文件路径': self.mark_dat_path,
                '读取时间': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                '区块数量': len(data),
                '区块详情': {},
                '市场分布': {'深交所': 0, '上交所': 0, '北交所': 0, '未知市场': 0},
                '总记录数': 0
            }
            
            # 统计各区块
            for section, records in data.items():
                section_stats = {
                    '记录数': len(records),
                    '样本数据': {}
                }
                
                # 取前3个作为样本
                sample_count = 0
                for full_code, value in records.items():
                    if sample_count >= 3:
                        break
                    
                    try:
                        stock_code = self.extract_stock_code(full_code)
                        market = self.get_market_code(full_code)
                        summary['市场分布'][market] += 1
                        
                        section_stats['样本数据'][stock_code] = {
                            '市场': market,
                            '备注': value[:50] + '...' if len(value) > 50 else value
                        }
                        
                    except Exception as e:
                        self.logger.warning(f"处理样本数据时出错: {e}")
                    
                    sample_count += 1
                
                summary['区块详情'][section] = section_stats
                summary['总记录数'] += len(records)
            
            return summary
            
        except Exception as e:
            self.logger.error(f"生成数据摘要失败: {e}")
            return {'错误': str(e)}
    
    # 更新TIPWORD值，如果TIPWORD值为空，则直接设置为新值，如果TIPWORD值不为空，则将新值添加到原值后面，用/分割
    def update_tipword(self, stock_code: str, new_tipword: str, data: Dict[str, Dict[str, str]] = None) -> bool:
        """更新股票的TIPWORD值
        
        Args:
            stock_code: 6位或8位股票代码
            new_tipword: 新的TIPWORD值
            data: 数据字典，如果为None则自动加载
            
        Returns:
            是否成功更新
        """
        try:
            # 转换为8位格式
            full_code = self.convert_to_8digit(stock_code)
            
            if data is None:
                data = self.load_data()
            
            # 确保TIPWORD区块存在
            if 'TIPWORD' not in data:
                data['TIPWORD'] = {}
            
            current_value = data['TIPWORD'].get(full_code, '')
            
            if current_value:
                # 如果已有值，按"原值/新值"格式更新
                new_value = f"{current_value}/{new_tipword}"
            else:
                # 如果原值为空，直接设为新值
                new_value = new_tipword
            
            data['TIPWORD'][full_code] = new_value
            
            self.logger.info(f"成功更新TIPWORD: {stock_code} -> {new_value}")
            return True
            
        except Exception as e:
            self.logger.error(f"更新TIPWORD失败: {e}")
            return False
    
    # 更新MARK值，直接设置为新值
    def update_mark(self, stock_code: str, new_value: str, data: Dict[str, Dict[str, str]] = None) -> bool:
        """更新股票的MARK值
        
        Args:
            stock_code: 6位或8位股票代码
            new_value: 新的MARK值
            data: 数据字典，如果为None则自动加载
            
        Returns:
            是否成功更新
        """
        return self._update_section_value(stock_code, 'MARK', new_value, data)
    
    # 更新TIP值，直接设置为新值
    def update_tip(self, stock_code: str, new_value: str, data: Dict[str, Dict[str, str]] = None) -> bool:
        """更新股票的TIP值
        
        Args:
            stock_code: 6位或8位股票代码
            new_value: 新的TIP值
            data: 数据字典，如果为None则自动加载
            
        Returns:
            是否成功更新
        """
        return self._update_section_value(stock_code, 'TIP', new_value, data)
    
    # 更新TIPCOLOR值，直接设置为新值
    def update_tipcolor(self, stock_code: str, new_value: str, data: Dict[str, Dict[str, str]] = None) -> bool:
        """更新股票的TIPCOLOR值
        
        Args:
            stock_code: 6位或8位股票代码
            new_value: 新的TIPCOLOR值
            data: 数据字典，如果为None则自动加载
            
        Returns:
            是否成功更新
        """
        return self._update_section_value(stock_code, 'TIPCOLOR', new_value, data)
    
    # 更新TIME值，直接设置为新值
    def update_time(self, stock_code: str, new_value: str, data: Dict[str, Dict[str, str]] = None) -> bool:
        """更新股票的TIME值
        
        Args:
            stock_code: 6位或8位股票代码
            new_value: 新的TIME值
            data: 数据字典，如果为None则自动加载
            
        Returns:
            是否成功更新
        """
        return self._update_section_value(stock_code, 'TIME', new_value, data)
    
    # 通用的区块值更新，直接设置为新值
    def _update_section_value(self, stock_code: str, section: str, new_value: str, data: Dict[str, Dict[str, str]] = None) -> bool:
        """通用的区块值更新方法
        
        Args:
            stock_code: 6位或8位股票代码
            section: 区块名称
            new_value: 新值
            data: 数据字典，如果为None则自动加载
            
        Returns:
            是否成功更新
        """
        try:
            # 转换为8位格式
            full_code = self.convert_to_8digit(stock_code)
            
            if data is None:
                data = self.load_data()
            
            # 确保区块存在
            if section not in data:
                data[section] = {}
            
            data[section][full_code] = new_value
            
            self.logger.info(f"成功更新{section}: {stock_code} -> {new_value}")
            return True
            
        except Exception as e:
            self.logger.error(f"更新{section}失败: {e}")
            return False
    
    # 为一个股票添加多个区块的数据
    def add_stock_data(self, stock_code: str, mark: str = None, tip: str = None,
                      tipword: str = None, tipcolor: str = None, time: str = None,
                      data: Dict[str, Dict[str, str]] = None) -> bool:
        """一次性为股票添加多个区块的数据
        
        Args:
            stock_code: 6位或8位股票代码
            mark: MARK值
            tip: TIP值
            tipword: TIPWORD值
            tipcolor: TIPCOLOR值
            time: TIME值
            data: 数据字典，如果为None则自动加载
            
        Returns:
            是否成功添加
        """
        try:
            if data is None:
                data = self.load_data()
            
            success_count = 0
            total_count = 0
            
            # 更新各个区块的值
            updates = [
                ('MARK', mark, self.update_mark),
                ('TIP', tip, self.update_tip),
                ('TIPWORD', tipword, self.update_tipword),
                ('TIPCOLOR', tipcolor, self.update_tipcolor),
                ('TIME', time, self.update_time)
            ]
            
            for section_name, value, update_func in updates:
                if value is not None:
                    total_count += 1
                    if update_func(stock_code, value, data):
                        success_count += 1
            
            self.logger.info(f"股票数据添加完成: {stock_code}, 成功 {success_count}/{total_count} 项")
            return success_count == total_count and total_count > 0
            
        except Exception as e:
            self.logger.error(f"添加股票数据失败: {e}")
            return False
    
    # 从所有区块中删除指定股票的数据
    def delete_stock(self, stock_code: str, data: Dict[str, Dict[str, str]] = None) -> bool:
        """从所有区块中删除指定股票的数据
        
        Args:
            stock_code: 6位或8位股票代码
            data: 数据字典，如果为None则自动加载
            
        Returns:
            是否成功删除
        """
        try:
            # 转换为8位格式
            full_code = self.convert_to_8digit(stock_code)
            
            if data is None:
                data = self.load_data()
            
            deleted_count = 0
            
            # 从所有区块中删除
            for section in self.supported_sections:
                if section in data and full_code in data[section]:
                    del data[section][full_code]
                    deleted_count += 1
            
            self.logger.info(f"成功删除股票: {stock_code}, 从 {deleted_count} 个区块中删除")
            return deleted_count > 0
            
        except Exception as e:
            self.logger.error(f"删除股票失败: {e}")
            return False
    
    # 从指定区块删除股票数据
    def delete_from_section(self, stock_code: str, section: str, data: Dict[str, Dict[str, str]] = None) -> bool:
        """从指定区块删除股票数据
        
        Args:
            stock_code: 6位或8位股票代码
            section: 区块名称
            data: 数据字典，如果为None则自动加载
            
        Returns:
            是否成功删除
        """
        try:
            # 转换为8位格式
            full_code = self.convert_to_8digit(stock_code)
            
            if data is None:
                data = self.load_data()
            
            if section not in self.supported_sections:
                raise ValueError(f"不支持的区块类型: {section}")
            
            if section in data and full_code in data[section]:
                del data[section][full_code]
                self.logger.info(f"成功从{section}区块删除股票: {stock_code}")
                return True
            else:
                self.logger.warning(f"股票{stock_code}在{section}区块中不存在")
                return False
            
        except Exception as e:
            self.logger.error(f"从{section}区块删除股票失败: {e}")
            return False
    
    # 删除所有值为空的数据行
    def clear_empty_values(self, data: Dict[str, Dict[str, str]] = None) -> int:
        """删除所有值为空的数据行
        
        Args:
            data: 数据字典，如果为None则自动加载
            
        Returns:
            删除的记录数量
        """
        try:
            if data is None:
                data = self.load_data()
            
            deleted_count = 0
            
            for section in list(data.keys()):
                if section in data:
                    empty_keys = [key for key, value in data[section].items() if not value.strip()]
                    
                    for key in empty_keys:
                        del data[section][key]
                        deleted_count += 1
            
            self.logger.info(f"清理空值完成，删除了 {deleted_count} 条记录")
            return deleted_count
            
        except Exception as e:
            self.logger.error(f"清理空值失败: {e}")
            return 0
    
    # 获取指定股票在所有区块的数据
    def get_stock_data(self, stock_code: str, data: Dict[str, Dict[str, str]] = None) -> Dict[str, str]:
        """获取指定股票在所有区块的数据
        
        Args:
            stock_code: 6位或8位股票代码
            data: 数据字典，如果为None则自动加载
            
        Returns:
            股票在各区块的数据字典
        """
        try:
            # 转换为8位格式
            full_code = self.convert_to_8digit(stock_code)
            
            if data is None:
                data = self.load_data()
            
            result = {
                'stock_code': stock_code,
                'full_code': full_code,
                'market': self.get_market_code(full_code)
            }
            
            # 获取各区块的数据
            for section in self.supported_sections:
                if section in data and full_code in data[section]:
                    result[section] = data[section][full_code]
                else:
                    result[section] = ''
            
            self.logger.debug(f"获取股票数据: {stock_code}")
            return result
            
        except Exception as e:
            self.logger.error(f"获取股票数据失败: {e}")
            return {}
    
    # 根据TIPWORD搜索股票
    def search_by_tipword(self, tipword: str, data: Dict[str, Dict[str, str]] = None) -> List[Dict[str, str]]:
        """根据TIPWORD搜索股票
        
        Args:
            tipword: 要搜索的TIPWORD关键词
            data: 数据字典，如果为None则自动加载
            
        Returns:
            匹配的股票列表
        """
        try:
            if data is None:
                data = self.load_data()
            
            results = []
            
            if 'TIPWORD' not in data:
                return results
            
            for full_code, value in data['TIPWORD'].items():
                if tipword in value:
                    stock_data = self.get_stock_data(self.extract_stock_code(full_code), data)
                    results.append(stock_data)
            
            self.logger.info(f"TIPWORD搜索完成: '{tipword}', 找到 {len(results)} 条记录")
            return results
            
        except Exception as e:
            self.logger.error(f"TIPWORD搜索失败: {e}")
            return []
    
    # 获取指定市场的所有股票
    def get_stocks_by_market(self, market_code: str, data: Dict[str, Dict[str, str]] = None) -> List[Dict[str, str]]:
        """获取指定市场的所有股票
        
        Args:
            market_code: 市场代码 ('00'-深交所, '01'-上交所, '02'-北交所)
            data: 数据字典，如果为None则自动加载
            
        Returns:
            指定市场的股票列表
        """
        try:
            if data is None:
                data = self.load_data()
            
            if market_code not in ['00', '01', '02']:
                raise ValueError(f"无效的市场代码: {market_code}")
            
            results = []
            all_codes = set()
            
            # 收集所有股票代码
            for section_data in data.values():
                all_codes.update(section_data.keys())
            
            # 筛选指定市场的股票
            for full_code in all_codes:
                if full_code.startswith(market_code):
                    stock_data = self.get_stock_data(self.extract_stock_code(full_code), data)
                    results.append(stock_data)
            
            market_name = self.get_market_code(market_code + '000000')
            self.logger.info(f"获取{market_name}股票完成: 找到 {len(results)} 条记录")
            return results
            
        except Exception as e:
            self.logger.error(f"获取市场股票失败: {e}")
            return []
    
    # 将数据写回文件
    def save_data(self, data: Dict[str, Dict[str, str]], file_path: str = None) -> bool:
        """将数据写回文件
        
        Args:
            data: 要保存的数据字典
            file_path: 保存路径，如果为None则写回原文件
            
        Returns:
            是否成功保存
        """
        try:
            target_path = file_path or self.mark_dat_path
            
            # 写入前创建备份
            if target_path == self.mark_dat_path:
                try:
                    self.create_backup()
                except Exception as e:
                    self.logger.warning(f"创建备份失败，继续保存: {e}")
            
            # 生成文件内容
            content_lines = []
            
            # 按指定顺序写入区块
            for section in self.supported_sections:
                if section in data and data[section]:
                    content_lines.append(f'[{section}]')
                    
                    # 按股票代码排序
                    sorted_items = sorted(data[section].items())
                    for stock_code, value in sorted_items:
                        if value.strip():  # 只写入非空值
                            content_lines.append(f'{stock_code}={value}')
                    
                    content_lines.append('')  # 区块间加空行
            
            # 写入文件
            content = '\n'.join(content_lines)
            
            with open(target_path, 'w', encoding='gbk') as f:
                f.write(content)
            
            self.logger.info(f"数据保存成功: {target_path}, 共 {len(content_lines)} 行")
            return True 
            
        except Exception as e:
            self.logger.error(f"数据保存失败: {e}")
            return False
    
    # 批量更新TIPWORD
    def batch_update_tipword(self, updates_dict: Dict[str, str], data: Dict[str, Dict[str, str]] = None) -> Dict[str, bool]:
        """批量更新TIPWORD
        
        Args:
            updates_dict: 更新字典，格式：{"600613": "AI", "002728": "金融"}
            data: 数据字典，如果为None则自动加载
            
        Returns:
            更新结果字典
        """
        try:
            if data is None:
                data = self.load_data()
            
            results = {}
            
            for stock_code, tipword in updates_dict.items():
                try:
                    success = self.update_tipword(stock_code, tipword, data)
                    results[stock_code] = success
                except Exception as e:
                    self.logger.error(f"批量更新TIPWORD失败 {stock_code}: {e}")
                    results[stock_code] = False
            
            success_count = sum(1 for success in results.values() if success)
            self.logger.info(f"批量更新TIPWORD完成: 成功 {success_count}/{len(updates_dict)} 项")
            
            return results
            
        except Exception as e:
            self.logger.error(f"批量更新TIPWORD失败: {e}")
            return {}
    
    # 批量添加股票数据
    def batch_add_stocks(self, stocks_data: List[Dict[str, str]], data: Dict[str, Dict[str, str]] = None) -> Dict[str, bool]:
        """批量添加股票数据
        
        Args:
            stocks_data: 股票数据列表，每个元素包含stock_code和各区块的值
            data: 数据字典，如果为None则自动加载
            
        Returns:
            添加结果字典
        """
        try:
            if data is None:
                data = self.load_data()
            
            results = {}
            
            for stock_info in stocks_data:
                stock_code = stock_info.get('stock_code')
                if not stock_code:
                    continue
                
                try:
                    success = self.add_stock_data(
                        stock_code=stock_code,
                        mark=stock_info.get('mark'),
                        tip=stock_info.get('tip'),
                        tipword=stock_info.get('tipword'),
                        tipcolor=stock_info.get('tipcolor'),
                        time=stock_info.get('time'),
                        data=data
                    )
                    results[stock_code] = success
                except Exception as e:
                    self.logger.error(f"批量添加股票失败 {stock_code}: {e}")
                    results[stock_code] = False
            
            success_count = sum(1 for success in results.values() if success)
            self.logger.info(f"批量添加股票完成: 成功 {success_count}/{len(stocks_data)} 项")
            
            return results
            
        except Exception as e:
            self.logger.error(f"批量添加股票失败: {e}")
            return {}
    
    def test_functionality(self) -> Dict[str, Union[bool, str]]:
        """测试核心功能
        
        Returns:
            测试结果字典
        """
        test_results = {
            '文件存在检查': False,
            '文件读取测试': False,
            '内容解析测试': False,
            '数据验证测试': False,
            '备份创建测试': False,
            '错误信息': []
        }
        
        try:
            # 1. 检查文件是否存在
            if os.path.exists(self.mark_dat_path):
                test_results['文件存在检查'] = True
                self.logger.info("✓ 文件存在检查通过")
            else:
                test_results['错误信息'].append(f"文件不存在: {self.mark_dat_path}")
                return test_results
            
            # 2. 测试文件读取
            try:
                content = self.read_file(create_backup=False)
                if content:
                    test_results['文件读取测试'] = True
                    self.logger.info("✓ 文件读取测试通过")
                else:
                    test_results['错误信息'].append("文件内容为空")
            except Exception as e:
                test_results['错误信息'].append(f"文件读取失败: {e}")
                return test_results
            
            # 3. 测试内容解析
            try:
                data = self.parse_content(content)
                if data and isinstance(data, dict):
                    test_results['内容解析测试'] = True
                    self.logger.info(f"✓ 内容解析测试通过，解析到 {len(data)} 个区块")
                else:
                    test_results['错误信息'].append("解析结果为空或格式错误")
            except Exception as e:
                test_results['错误信息'].append(f"内容解析失败: {e}")
                return test_results
            
            # 4. 测试数据验证
            try:
                validation = self.validate_data(data)
                test_results['数据验证测试'] = True
                self.logger.info(f"✓ 数据验证测试通过，发现 {len(validation['errors'])} 个错误")
            except Exception as e:
                test_results['错误信息'].append(f"数据验证失败: {e}")
            
            # 5. 测试备份创建
            try:
                backup_path = self.create_backup()
                if os.path.exists(backup_path):
                    test_results['备份创建测试'] = True
                    self.logger.info(f"✓ 备份创建测试通过: {backup_path}")
                else:
                    test_results['错误信息'].append("备份文件未成功创建")
            except Exception as e:
                test_results['错误信息'].append(f"备份创建失败: {e}")
            
        except Exception as e:
            test_results['错误信息'].append(f"测试过程中发生未知错误: {e}")
            self.logger.error(f"功能测试失败: {e}")
        
        # 统计测试结果
        passed_tests = sum(1 for key, value in test_results.items() 
                          if key != '错误信息' and value is True)
        total_tests = len(test_results) - 1  # 减去错误信息字段
        
        self.logger.info(f"功能测试完成: {passed_tests}/{total_tests} 项通过")
        
        return test_results
    
    # ==================== 第三阶段功能：数据合并清理和集成 ====================
    
    # 合并TIPWORD区块中的重复股票代码
    def merge_duplicate_tipwords(self, data: Dict[str, Dict[str, str]] = None) -> int:
        """合并TIPWORD区块中的重复股票代码
        
        如果存在重复代码，用"/"连接相应数据，去除重复的tipword值
        
        Args:
            data: 数据字典，如果为None则自动加载
            
        Returns:
            合并的记录数量
        """
        try:
            if data is None:
                data = self.load_data()
            
            if 'TIPWORD' not in data:
                return 0
            
            merged_count = 0
            tipword_data = data['TIPWORD']
            
            # 按股票代码分组
            code_groups = defaultdict(list)
            for full_code, tipword in list(tipword_data.items()):
                code_groups[full_code].append(tipword)
            
            # 处理重复代码
            for full_code, tipwords in code_groups.items():
                if len(tipwords) > 1:
                    # 去重并合并
                    unique_tipwords = []
                    seen = set()
                    
                    for tipword in tipwords:
                        # 分割现有的tipword
                        parts = [part.strip() for part in tipword.split('/') if part.strip()]
                        for part in parts:
                            if part not in seen:
                                seen.add(part)
                                unique_tipwords.append(part)
                    
                    # 合并为新值
                    merged_value = '/'.join(unique_tipwords)
                    tipword_data[full_code] = merged_value
                    merged_count += 1
                    
                    self.logger.info(f"合并TIPWORD重复数据: {full_code} -> {merged_value}")
            
            self.logger.info(f"TIPWORD重复数据合并完成，处理了 {merged_count} 条记录")
            return merged_count
            
        except Exception as e:
            self.logger.error(f"合并TIPWORD重复数据失败: {e}")
            return 0
    
    # 处理其他区块的重复数据，保留最后一行数据
    def merge_duplicate_sections(self, data: Dict[str, Dict[str, str]] = None) -> Dict[str, int]:
        """处理其他区块的重复数据，保留最后一行数据
        
        Args:
            data: 数据字典，如果为None则自动加载
            
        Returns:
            各区块合并的记录数量
        """
        try:
            if data is None:
                data = self.load_data()
            
            merge_results = {}
            other_sections = [s for s in self.supported_sections if s != 'TIPWORD']
            
            for section in other_sections:
                if section not in data:
                    merge_results[section] = 0
                    continue
                
                section_data = data[section]
                original_count = len(section_data)
                
                # 由于字典的特性，相同键的值会被自动覆盖（保留最后一个）
                # 这里主要是记录和日志
                code_counts = Counter(section_data.keys())
                duplicates = {code: count for code, count in code_counts.items() if count > 1}
                
                merge_results[section] = len(duplicates)
                
                if duplicates:
                    self.logger.info(f"区块 {section} 发现重复代码: {duplicates}")
                
            total_merged = sum(merge_results.values())
            self.logger.info(f"其他区块重复数据处理完成，总计处理 {total_merged} 条记录")
            
            return merge_results
            
        except Exception as e:
            self.logger.error(f"处理其他区块重复数据失败: {e}")
            return {}
    
    # 清理所有区块的重复数据
    def clean_all_duplicates(self, data: Dict[str, Dict[str, str]] = None) -> Dict[str, int]:
        """清理所有区块的重复数据
        
        Args:
            data: 数据字典，如果为None则自动加载
            
        Returns:
            清理结果统计
        """
        try:
            if data is None:
                data = self.load_data()
            
            results = {}
            
            # 合并TIPWORD重复数据
            tipword_merged = self.merge_duplicate_tipwords(data)
            results['TIPWORD_merged'] = tipword_merged
            
            # 处理其他区块重复数据
            other_results = self.merge_duplicate_sections(data)
            results.update(other_results)
            
            # 清理空值
            empty_cleaned = self.clear_empty_values(data)
            results['empty_values_cleaned'] = empty_cleaned
            
            total_processed = sum(results.values())
            self.logger.info(f"重复数据清理完成，总计处理 {total_processed} 条记录")
            
            return results
            
        except Exception as e:
            self.logger.error(f"清理重复数据失败: {e}")
            return {}
    
    # 数据标准化处理（排序、格式化等）
    def normalize_data(self, data: Dict[str, Dict[str, str]] = None) -> bool:
        """数据标准化处理（排序、格式化等）
        
        Args:
            data: 数据字典，如果为None则自动加载
            
        Returns:
            是否成功标准化
        """
        try:
            if data is None:
                data = self.load_data()
            
            # 对每个区块的数据进行排序
            for section in self.supported_sections:
                if section in data:
                    # 按股票代码排序
                    sorted_items = dict(sorted(data[section].items()))
                    data[section] = sorted_items
            
            # 清理和标准化字符串值
            cleaned_count = 0
            for section, section_data in data.items():
                for code, value in list(section_data.items()):
                    if isinstance(value, str):
                        # 清理前后空白字符
                        cleaned_value = value.strip()
                        if cleaned_value != value:
                            section_data[code] = cleaned_value
                            cleaned_count += 1
            
            self.logger.info(f"数据标准化完成，清理了 {cleaned_count} 个值")
            return True
            
        except Exception as e:
            self.logger.error(f"数据标准化失败: {e}")
            return False
    
    def validate_data_integrity(self, data: Dict[str, Dict[str, str]] = None) -> Dict:
        """增强的数据完整性检查
        
        Args:
            data: 数据字典，如果为None则自动加载
            
        Returns:
            详细的验证报告
        """
        try:
            if data is None:
                data = self.load_data()
            
            report = {
                'total_records': 0,
                'duplicates': {},
                'empty_values': {},
                'invalid_codes': [],
                'market_distribution': defaultdict(int),
                'section_stats': {},
                'issues': [],
                'suggestions': []
            }
            
            all_codes = set()
            
            # 检查每个区块
            for section in self.supported_sections:
                if section not in data:
                    report['issues'].append(f"缺少区块: {section}")
                    continue
                
                section_data = data[section]
                section_stats = {
                    'total': len(section_data),
                    'duplicates': 0,
                    'empty_values': 0,
                    'invalid_codes': 0
                }
                
                code_counts = Counter()
                
                for full_code, value in section_data.items():
                    all_codes.add(full_code)
                    code_counts[full_code] += 1
                    
                    # 检查空值
                    if not value or not value.strip():
                        section_stats['empty_values'] += 1
                        if section not in report['empty_values']:
                            report['empty_values'][section] = []
                        report['empty_values'][section].append(full_code)
                    
                    # 检查股票代码格式
                    if len(full_code) != 8 or not full_code.isdigit():
                        section_stats['invalid_codes'] += 1
                        if full_code not in report['invalid_codes']:
                            report['invalid_codes'].append(full_code)
                    else:
                        # 统计市场分布
                        market = self.get_market_code(full_code)
                        report['market_distribution'][market] += 1
                
                # 统计重复
                duplicates = {code: count for code, count in code_counts.items() if count > 1}
                section_stats['duplicates'] = len(duplicates)
                if duplicates:
                    report['duplicates'][section] = duplicates
                
                report['section_stats'][section] = section_stats
                report['total_records'] += len(section_data)
            
            # 生成建议
            if report['duplicates']:
                report['suggestions'].append("发现重复数据，建议运行 clean_all_duplicates() 清理")
            if report['empty_values']:
                report['suggestions'].append("发现空值数据，建议运行 clear_empty_values() 清理")
            if report['invalid_codes']:
                report['suggestions'].append("发现无效股票代码，建议检查数据来源")
            
            self.logger.info(f"数据完整性检查完成，总记录数: {report['total_records']}")
            return report
            
        except Exception as e:
            self.logger.error(f"数据完整性检查失败: {e}")
            return {'error': str(e)}
    
    def process_file(self, output_path: str = None, auto_clean: bool = True,
                    auto_merge: bool = True, auto_normalize: bool = True) -> Dict:
        """一键处理整个文件（读取→清理→合并→验证→保存）
        
        Args:
            output_path: 输出文件路径，如果为None则覆盖原文件
            auto_clean: 是否自动清理空值
            auto_merge: 是否自动合并重复数据
            auto_normalize: 是否自动标准化数据
            
        Returns:
            处理结果报告
        """
        with self._lock:
            try:
                self.logger.info("开始一键处理文件...")
                
                # 1. 读取数据
                data = self.load_data()
                original_stats = self._get_data_stats(data)
                
                # 2. 数据处理
                processing_results = {}
                
                if auto_clean:
                    clean_results = self.clear_empty_values(data)
                    processing_results['empty_cleaned'] = clean_results
                
                if auto_merge:
                    merge_results = self.clean_all_duplicates(data)
                    processing_results['merge_results'] = merge_results
                
                if auto_normalize:
                    normalize_success = self.normalize_data(data)
                    processing_results['normalized'] = normalize_success
                
                # 3. 验证数据
                validation_report = self.validate_data_integrity(data)
                
                # 4. 保存数据
                save_path = output_path or self.mark_dat_path
                save_success = self.save_data(data, save_path)
                
                # 5. 生成报告
                final_stats = self._get_data_stats(data)
                
                report = {
                    'success': save_success,
                    'processing_results': processing_results,
                    'validation_report': validation_report,
                    'stats': {
                        'original': original_stats,
                        'final': final_stats
                    },
                    'file_path': save_path,
                    'timestamp': datetime.now().isoformat()
                }
                
                # 记录操作历史
                self._add_to_history('process_file', report)
                
                self.logger.info("一键处理文件完成")
                return report
                
            except Exception as e:
                self.logger.error(f"一键处理文件失败: {e}")
                return {'success': False, 'error': str(e)}
    
    def safe_update(self, update_func, *args, **kwargs) -> Dict:
        """安全更新操作（带回滚机制）
        
        Args:
            update_func: 更新函数
            *args: 位置参数
            **kwargs: 关键字参数
            
        Returns:
            操作结果
        """
        with self._lock:
            try:
                # 创建备份
                backup_path = self.create_backup()
                
                # 记录当前数据状态
                original_data = self.load_data(create_backup=False)
                original_hash = self._calculate_data_hash(original_data)
                
                # 执行更新操作
                result = update_func(*args, **kwargs)
                
                # 验证更新后的数据
                if isinstance(result, bool) and result:
                    updated_data = self.load_data(create_backup=False)
                    validation = self.validate_data_integrity(updated_data)
                    
                    if validation.get('invalid_codes'):
                        # 发现严重错误，回滚
                        shutil.copy2(backup_path, self.mark_dat_path)
                        self.logger.warning("发现数据错误，已自动回滚")
                        return {
                            'success': False,
                            'rolled_back': True,
                            'reason': '数据验证失败',
                            'backup_path': backup_path
                        }

                return {
                    'success': True,
                    'result': result,
                    'backup_path': backup_path,
                    'original_hash': original_hash
                }
               
            except Exception as e:
                self.logger.error(f"安全更新操作失败: {e}")
                return {'success': False, 'error': str(e)}

    def safe_update_tip(self, code: str, new_value: str) -> bool:
        """安全更新股票的TIP值
        
        Args:
            code: 股票代码
            new_value: 新的TIP值
            
        Returns:
            是否成功更新
        """

        data = self.load_data(create_backup=False)
        # 执行安全更新
        result = self.safe_update(self.update_tip, code, new_value,data)
        if result['success']:
            self.save_data(data)
            print(f"[成功] 更新成功，备份: {result['backup_path']}")
        else:
            if result.get('rolled_back'):
                print("[警告] 发现问题，已自动回滚")
            else:
                print(f"[错误] 更新失败: {result.get('error')}")


    def repair_file(self, repair_options: Dict = None) -> Dict:
        """文件修复功能
        
        Args:
            repair_options: 修复选项配置
            
        Returns:
            修复结果报告
        """
        try:
            default_options = {
                'fix_duplicates': True,
                'fix_empty_values': True,
                'fix_invalid_codes': False,  # 谨慎操作
                'normalize_data': True,
                'create_backup': True
            }
            
            options = {**default_options, **(repair_options or {})}
            
            self.logger.info("开始文件修复...")
            
            # 加载数据
            data = self.load_data(create_backup=options['create_backup'])
            
            # 记录修复前状态
            before_stats = self._get_data_stats(data)
            validation_before = self.validate_data_integrity(data)
            
            repair_results = {}
            
            # 修复重复数据
            if options['fix_duplicates']:
                duplicate_results = self.clean_all_duplicates(data)
                repair_results['duplicates_fixed'] = duplicate_results
            
            # 修复空值
            if options['fix_empty_values']:
                empty_fixed = self.clear_empty_values(data)
                repair_results['empty_values_fixed'] = empty_fixed
            
            # 标准化数据
            if options['normalize_data']:
                normalize_success = self.normalize_data(data)
                repair_results['data_normalized'] = normalize_success
            
            # 保存修复后的数据
            save_success = self.save_data(data)
            
            # 验证修复结果
            after_stats = self._get_data_stats(data)
            validation_after = self.validate_data_integrity(data)
            
            report = {
                'success': save_success,
                'repair_results': repair_results,
                'stats': {
                    'before': before_stats,
                    'after': after_stats
                },
                'validation': {
                    'before': validation_before,
                    'after': validation_after
                },
                'timestamp': datetime.now().isoformat()
            }
            
            self.logger.info("文件修复完成")
            return report
            
        except Exception as e:
            self.logger.error(f"文件修复失败: {e}")
            return {'success': False, 'error': str(e)}
    
    def generate_report(self, data: Dict[str, Dict[str, str]] = None,
                       include_samples: bool = True) -> Dict:
        """生成详细的数据分析报告
        
        Args:
            data: 数据字典，如果为None则自动加载
            include_samples: 是否包含样本数据
            
        Returns:
            详细报告
        """
        try:
            if data is None:
                data = self.load_data(create_backup=False)
            
            # 基础统计
            stats = self._get_data_stats(data)
            
            # 数据验证
            validation = self.validate_data_integrity(data)
            
            # 市场分析
            market_analysis = self._analyze_markets(data)
            
            # TIPWORD分析
            tipword_analysis = self._analyze_tipwords(data)
            
            report = {
                'report_info': {
                    'generated_at': datetime.now().isoformat(),
                    'file_path': self.mark_dat_path,
                    'data_hash': self._calculate_data_hash(data)
                },
                'basic_stats': stats,
                'validation': validation,
                'market_analysis': market_analysis,
                'tipword_analysis': tipword_analysis
            }
            
            if include_samples:
                report['samples'] = self._get_sample_data(data)
            
            self.logger.info("数据分析报告生成完成")
            return report
            
        except Exception as e:
            self.logger.error(f"生成报告失败: {e}")
            return {'error': str(e)}
    
    def compare_files(self, other_file_path: str) -> Dict:
        """比较两个mark.dat文件的差异
        
        Args:
            other_file_path: 另一个文件的路径
            
        Returns:
            比较结果
        """
        try:
            # 加载两个文件的数据
            data1 = self.load_data(create_backup=False)
            
            other_manager = TdxMarkManager(other_file_path)
            data2 = other_manager.load_data(create_backup=False)
            
            comparison = {
                'file1': self.mark_dat_path,
                'file2': other_file_path,
                'timestamp': datetime.now().isoformat(),
                'differences': {},
                'stats': {
                    'file1': self._get_data_stats(data1),
                    'file2': self._get_data_stats(data2)
                }
            }
            
            # 比较各个区块
            all_sections = set(data1.keys()) | set(data2.keys())
            
            for section in all_sections:
                section_diff = {
                    'only_in_file1': [],
                    'only_in_file2': [],
                    'different_values': [],
                    'common_count': 0
                }
                
                data1_section = data1.get(section, {})
                data2_section = data2.get(section, {})
                
                all_codes = set(data1_section.keys()) | set(data2_section.keys())
                
                for code in all_codes:
                    value1 = data1_section.get(code)
                    value2 = data2_section.get(code)
                    
                    if value1 is None:
                        section_diff['only_in_file2'].append((code, value2))
                    elif value2 is None:
                        section_diff['only_in_file1'].append((code, value1))
                    elif value1 != value2:
                        section_diff['different_values'].append((code, value1, value2))
                    else:
                        section_diff['common_count'] += 1
                
                comparison['differences'][section] = section_diff
            
            self.logger.info(f"文件比较完成: {self.mark_dat_path} vs {other_file_path}")
            return comparison
            
        except Exception as e:
            self.logger.error(f"文件比较失败: {e}")
            return {'error': str(e)}
    
    def audit_trail(self, limit: int = 50) -> List[Dict]:
        """操作审计跟踪
        
        Args:
            limit: 返回的历史记录数量限制
            
        Returns:
            操作历史列表
        """
        return self._operation_history[-limit:] if self._operation_history else []
    
    # ==================== 辅助方法 ====================
    
    def _get_data_stats(self, data: Dict[str, Dict[str, str]]) -> Dict:
        """获取数据统计信息"""
        stats = {
            'total_records': 0,
            'sections': {},
            'markets': defaultdict(int)
        }
        
        for section, section_data in data.items():
            count = len(section_data)
            stats['sections'][section] = count
            stats['total_records'] += count
            
            # 统计市场分布
            for full_code in section_data.keys():
                if len(full_code) == 8 and full_code.isdigit():
                    market = self.get_market_code(full_code)
                    stats['markets'][market] += 1
        
        return stats
    
    def _calculate_data_hash(self, data: Dict[str, Dict[str, str]]) -> str:
        """计算数据的哈希值"""
        data_str = json.dumps(data, sort_keys=True, ensure_ascii=False)
        return hashlib.md5(data_str.encode('utf-8')).hexdigest()
    
    def _add_to_history(self, operation: str, details: Dict):
        """添加到操作历史"""
        entry = {
            'timestamp': datetime.now().isoformat(),
            'operation': operation,
            'details': details
        }
        self._operation_history.append(entry)
        
        # 限制历史记录数量
        if len(self._operation_history) > 1000:
            self._operation_history = self._operation_history[-500:]
    
    def _analyze_markets(self, data: Dict[str, Dict[str, str]]) -> Dict:
        """分析市场分布"""
        market_stats = defaultdict(lambda: {'count': 0, 'sections': defaultdict(int)})
        
        for section, section_data in data.items():
            for full_code in section_data.keys():
                if len(full_code) == 8 and full_code.isdigit():
                    market = self.get_market_code(full_code)
                    market_stats[market]['count'] += 1
                    market_stats[market]['sections'][section] += 1
        
        return dict(market_stats)
    
    def _analyze_tipwords(self, data: Dict[str, Dict[str, str]]) -> Dict:
        """分析TIPWORD数据"""
        if 'TIPWORD' not in data:
            return {}
        
        tipword_stats = {
            'total_entries': len(data['TIPWORD']),
            'unique_tipwords': set(),
            'tipword_frequency': defaultdict(int),
            'multi_tipword_stocks': []
        }
        
        for full_code, tipword in data['TIPWORD'].items():
            if '/' in tipword:
                tipword_parts = [part.strip() for part in tipword.split('/')]
                tipword_stats['multi_tipword_stocks'].append((full_code, tipword_parts))
                for part in tipword_parts:
                    tipword_stats['unique_tipwords'].add(part)
                    tipword_stats['tipword_frequency'][part] += 1
            else:
                tipword_stats['unique_tipwords'].add(tipword)
                tipword_stats['tipword_frequency'][tipword] += 1
        
        # 转换set为list以便JSON序列化
        tipword_stats['unique_tipwords'] = list(tipword_stats['unique_tipwords'])
        tipword_stats['tipword_frequency'] = dict(tipword_stats['tipword_frequency'])
        
        return tipword_stats
    
    def _get_sample_data(self, data: Dict[str, Dict[str, str]],
                        sample_size: int = 3) -> Dict:
        """获取样本数据"""
        samples = {}
        
        for section, section_data in data.items():
            section_samples = []
            count = 0
            for full_code, value in section_data.items():
                if count >= sample_size:
                    break
                try:
                    stock_code = self.extract_stock_code(full_code)
                    market = self.get_market_code(full_code)
                    section_samples.append({
                        'stock_code': stock_code,
                        'full_code': full_code,
                        'market': market,
                        'value': value[:100] + '...' if len(value) > 100 else value
                    })
                    count += 1
                except:
                    continue
            
            samples[section] = section_samples
        
        return samples

    def _read_all_data_cached(self) -> Dict[str, Dict[str, str]]:
        if self._cached_all_data is not None:
            return self._cached_all_data
        
        data = self.load_data(create_backup=False)
        all_codes = set()
        for section_data in data.values():
            all_codes.update(section_data.keys())
        
        self._cached_all_data = {}
        for code in all_codes:
            self._cached_all_data[code] = {
                section: data.get(section, {}).get(code, '')
                for section in self.supported_sections
            }
        
        return self._cached_all_data


def main():
    """主函数 - 演示基本功能"""
    print("=" * 60)
    print("          通达信 mark.dat 文件管理工具")
    print("                 完整版本 v2.0")
    print("=" * 60)
    
    #try:
    # 创建管理器实例
    manager = TdxMarkManager()
    
    print("\n1. 执行功能测试...")
    test_results = manager.test_functionality()
    
    print("\n测试结果:")
    for test_name, result in test_results.items():
        if test_name == '错误信息':
            continue
        status = "✓ 通过" if result else "✗ 失败"
        print(f"  {test_name}: {status}")
    
    if test_results['错误信息']:
        print("\n错误信息:")
        for error in test_results['错误信息']:
            print(f"  - {error}")
        return
    
    print("\n2. 加载和解析数据...")
    data = manager.load_data()
    
    print("\n3. 生成数据摘要...")
    summary = manager.get_data_summary(data)
    
    print(f"\n数据摘要:")
    print(f"  文件路径: {summary['文件路径']}")
    print(f"  总记录数: {summary['总记录数']}")
    print(f"  区块数量: {summary['区块数量']}")
    
    print(f"\n市场分布:")
    for market, count in summary['市场分布'].items():
        if count > 0:
            print(f"  {market}: {count} 条")
    
    print(f"\n区块详情:")
    for section, details in summary['区块详情'].items():
        print(f"  {section}: {details['记录数']} 条记录")
    
    print("\n4. 数据验证...")
    validation = manager.validate_data(data)
    
    if validation['errors']:
        print(f"  发现 {len(validation['errors'])} 个错误")
    if validation['warnings']:
        print(f"  发现 {len(validation['warnings'])} 个警告")
    
    print(f"\n✓ 所有核心功能测试完成！")
    print(f"✓ 备份文件已创建在 ./manage_tdx_mark/backups目录下")
    print(f"✓ 日志记录在 ./manage_tdx_mark/log/tdx_mark_manager.log 文件中")
        
    #except Exception as e:
    #    print(f"\n✗ 程序执行失败: {e}")
    #    return 1
    
    return 0


if __name__ == '__main__':
    #exit(main())
    manager = TdxMarkManager()
    manager.safe_update_tip('000001','123456')