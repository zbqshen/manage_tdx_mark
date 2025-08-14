# CLAUDE.md

本文件为 Claude Code (claude.ai/code) 在此仓库中工作时提供指导。

## 项目概述

TDX Mark Manager（通达信标记文件管理工具）是一个专业的通达信 mark.dat 文件管理工具，提供完整的股票标记数据管理功能，具有企业级架构设计。

## 核心命令

### 代码质量
```bash
# 格式化代码
black .

# 整理导入
isort .

# 代码检查
flake8 .

# 类型检查
mypy .
```

## 架构设计

### 核心组件

1. **TdxMarkManager** (`tdx_mark_manager.py`): 主管理类，提供完整的 mark.dat 文件操作功能，保持与原始 API 的向后兼容

2. **DataOperationService** (`data_service.py`): 服务层，使用策略模式处理不同的数据操作（大部分区块使用 DirectUpdateStrategy，TIPWORD 使用 TipwordMergeStrategy）

3. **Models** (`models.py`): 类型安全的数据模型，包括 StockInfo、ValidationResult、OperationResult

4. **Cache System** (`cache.py`): LRU 缓存实现，性能提升 70% 以上

5. **Validators** (`validators.py`): 输入验证，包含路径遍历保护和数据净化

6. **Constants** (`constants.py`): 使用枚举集中配置（DataSection、MarketCode、OperationType）

### 数据结构

mark.dat 文件包含 5 个区块：
- **MARK**: 股票标记等级（1-9）
- **TIP**: 股票备注说明
- **TIPWORD**: 股票标签（使用"/"分隔符连接多个标签）
- **TIPCOLOR**: 显示颜色代码
- **TIME**: 时间戳信息

股票代码为 8 位格式：`{市场代码}{股票代码}`
- `01`: 上海证券交易所
- `00`: 深圳证券交易所
- `02`: 北京证券交易所

### 关键设计模式

1. **策略模式**: TIPWORD 使用合并策略（带分隔符合并），其他区块使用直接替换策略

2. **服务层**: DataOperationService 提供清晰的 API，抽象数据操作复杂性

3. **防御性编程**: 所有文件操作前需先进行读取验证，全面的输入验证，路径遍历保护

## 配置管理

使用 `tdx_mark_config.ini` 进行配置：
- 主 mark.dat 文件路径
- 备份设置（自动备份、最大文件数）
- 验证规则
- 处理选项

## 开发指南

1. **操作前必须验证**: 对所有用户输入使用验证器
2. **保持向后兼容**: 在 TdxMarkManager 中保留原始 API
3. **使用类型提示**: 所有函数应有完整的类型注解
4. **遵循现有模式**: 数据操作使用策略模式，业务逻辑使用服务层
5. **安全优先**: 路径验证、输入净化、未经验证不直接写入文件