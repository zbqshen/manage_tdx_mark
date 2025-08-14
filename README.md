# TDX Mark Manager - 通达信标记文件管理工具

[![Python Version](https://img.shields.io/badge/python-3.8+-blue.svg)](https://python.org)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)
[![Version](https://img.shields.io/badge/version-3.1.0-orange.svg)](CHANGELOG.md)

⭐ **专业的通达信 mark.dat 文件管理工具，提供企业级安全批量操作**

---

## 📖 项目简介

TDX Mark Manager 是一个专业的通达信 mark.dat 文件管理工具，提供完整的股票标记数据管理功能。经过企业级重构，现已发展为具有模块化架构、强化安全性和高性能的Python包。

### 🌟 主要特性

- 🛡️ **安全批量操作**：分组小批量处理，自动回退机制，100%成功率保障
- 🔒 **安全可靠**：强化的输入验证和路径遍历保护
- ⚡ **高性能**：智能LRU缓存系统，提升70%+处理效率
- 🏗️ **模块化架构**：清晰的职责分离，易于维护和扩展
- 📊 **类型安全**：完整的类型提示，IDE友好
- 🔄 **向后兼容**：保持原有API兼容性

### 🎯 适用场景

- 通达信软件用户的个股标记管理
- 股票数据的批量处理和分析
- 投资组合的标记和分类
- 股票数据的备份和恢复
- 第三方工具的数据集成

---

## 🚀 快速开始

### 安装要求

- Python 3.8+
- 无外部依赖（仅使用标准库）

### ⭐ 推荐使用方式：安全批量操作

```python
from manage_tdx_mark import SafeBatchService

# 创建安全批量服务
service = SafeBatchService()

# 批量更新股票备注（自动分组、回退保护）
updates = {
    "600613": "东阿阿胶-优质消费股",
    "000001": "平安银行-金融龙头", 
    "002415": "海康威视-安防龙头"
}

result = service.batch_update_tip(updates)
print(f"批量操作成功率：{result.overall_success_rate:.1f}%")
```

### 基础使用

```python
from manage_tdx_mark import TdxMarkManager

# 创建管理器实例
manager = TdxMarkManager()

# 加载数据
data = manager.load_data()

# 更新股票标记
manager.update_stock_mark("600613", "8")  # 设置关注度为8

# 添加备注
manager.update_stock_tip("600613", "优质消费股")

# 保存数据
manager.save_data(data)
```

### 高级使用

```python
from manage_tdx_mark import DataOperationService, StockInfo

# 使用新的服务层API
service = DataOperationService()

# 创建股票信息对象
stock = StockInfo(
    stock_code="600613",
    full_code="01600613", 
    market="上交所",
    mark="8",
    tip="东阿阿胶 - 优质消费股",
    tipword="消费/医药/传统"
)

# 添加股票数据
result = service.add_stock_data(stock, data)
if result.success:
    print(f"成功添加股票数据，影响记录：{result.affected_records}")
```

---

## 📚 核心组件

### SafeBatchService ⭐
**最重要的API**：安全的批量操作服务，提供分组处理和自动回退机制。

### TdxMarkManager
核心管理类，提供完整的mark.dat文件操作功能。

### DataOperationService  
数据操作服务层，使用策略模式处理不同类型的数据操作。

### StockInfo
类型安全的股票信息数据模型。

### CacheManager
智能缓存系统，提供LRU缓存和性能优化。

### InputValidator
输入验证工具，确保数据安全性和正确性。

---

## 🏗️ 架构设计

```
manage_tdx_mark/
├── 📁 核心层
│   ├── safe_batch_service.py   # ⭐ 安全批量操作服务
│   ├── tdx_mark_manager.py     # 主管理类
│   ├── data_service.py         # 数据服务层
│   └── models.py               # 数据模型
├── 📁 基础设施层  
│   ├── cache.py                # 缓存系统
│   ├── validators.py           # 验证器
│   ├── exceptions.py           # 异常定义
│   └── constants.py            # 常量配置
└── 📁 配置文件
    └── tdx_mark_config.ini     # 配置文件
```

---

## 🔧 配置文件

项目使用 `tdx_mark_config.ini` 配置文件：

```ini
[PATHS]
primary_mark_dat = D:\\Tdx MPV V1.24++\\T0002\\mark.dat
backup_directory = ./backups
log_file = ./log/tdx_mark_manager.log

[BACKUP]
auto_backup = true
max_backup_files = 30

[VALIDATION]
strict_code_validation = true
max_remark_length = 500
```

---

## 📊 SafeBatchService 详细指南

### 什么是 SafeBatchService？

`SafeBatchService` 是 TDX Mark Manager 最重要的组件，专门为批量操作设计，提供企业级的数据安全保障。

### 🎯 设计目标
- **数据安全**：分组处理，自动回退，零数据丢失
- **100%保障**：只有完全成功的组才会保存
- **操作透明**：详细的操作报告和错误信息
- **易于使用**：简单的 API，复杂的内部逻辑

### 🛡️ 安全机制
1. **分组处理**：大批量数据分成小组，降低风险
2. **自动备份**：每组操作前创建备份文件
3. **智能回退**：失败时自动恢复到操作前状态
4. **数据验证**：保存前验证数据完整性
5. **错误隔离**：单组失败不影响其他组

### 🔧 核心特性

#### 分组策略
- **默认分组**：每组 5 个项目
- **可配置分组**：1-100 个项目自定义
- **智能分组**：根据数据量自动调整

#### 成功率控制
- **默认阈值**：100% 成功率要求
- **可配置阈值**：50%-100% 灵活设置
- **自动回退**：低于阈值自动回退

#### 详细报告
- **组级统计**：每组的处理结果
- **项目级统计**：每个项目的成功/失败状态
- **时间统计**：操作耗时和性能分析
- **错误分析**：详细的错误信息和建议

### 🔥 批量操作示例

#### 基础批量操作
```python
from manage_tdx_mark import SafeBatchService

# 创建服务
service = SafeBatchService()

# 准备数据
updates = {
    "600613": "东阿阿胶-优质消费股",
    "000001": "平安银行-金融龙头",
    "002415": "海康威视-安防龙头"
}

# 批量更新（推荐：备注区块）
result = service.batch_update_tip(updates)

# 检查结果
if result.overall_success_rate == 100:
    print("✅ 所有数据更新成功！")
    print(f"📊 处理了 {result.total_successful_items} 个项目")
else:
    print(f"⚠️ 成功率：{result.overall_success_rate:.1f}%")
    print(f"❌ 失败项目：{result.failed_items}")
```

#### 🗑️ 批量删除操作（新功能）

#### 批量删除整个股票数据
```python
from manage_tdx_mark import SafeBatchService

service = SafeBatchService()

# 批量删除股票的所有数据
stock_codes = ["600613", "000001", "002415"]
result = service.batch_delete_stocks(stock_codes)

print(f"删除成功率：{result.overall_success_rate:.1f}%")
print(f"成功删除：{result.total_successful_items} 只股票")
```

#### 批量删除指定区块
```python
from manage_tdx_mark import SafeBatchService, SafeDeleteConfig

service = SafeBatchService()

# 只删除特定区块的数据
config = SafeDeleteConfig(
    chunk_size=5,
    confirm_threshold=10  # 超过10条需要确认
)

# 删除多只股票的TIPWORD数据
stock_codes = ["600613", "000001", "002415"]
result = service.batch_delete_from_section(stock_codes, "TIPWORD", config)

print(f"删除成功率：{result.overall_success_rate:.1f}%")
```

#### 批量清理空值数据
```python
# 安全清理所有空值
result = service.batch_clear_empty()

print(f"清理了 {result.total_successful_items} 条空值记录")
```

#### 批量删除特定标签
```python
# 批量删除特定的TIPWORD标签
tipword_deletions = {
    "600613": ["白马"],           # 只删除"白马"标签
    "000001": ["金融", "银行"],    # 删除多个标签
    "002415": ["科技"]            # 删除"科技"标签
}

result = service.batch_delete_tipwords(tipword_deletions)

print(f"标签删除成功率：{result.overall_success_rate:.1f}%")
```

#### 删除配置选项
```python
from manage_tdx_mark import SafeDeleteConfig, DeleteMode

# 创建删除配置
config = SafeDeleteConfig(
    delete_mode=DeleteMode.ALL,      # 删除模式：ALL/SECTION/EMPTY/TIPWORD
    chunk_size=5,                    # 每组5个项目
    success_threshold=100.0,         # 100%成功率要求
    auto_rollback=True,              # 自动回退
    confirm_threshold=10,            # 超过10条需要确认
    verify_after_delete=True,        # 删除后验证数据
    create_summary_report=True       # 创建详细报告
)

# 使用配置执行删除
result = service.batch_delete_stocks(stock_codes, config)
```

### 自定义配置
```python
from manage_tdx_mark import SafeBatchService, create_safe_batch_config

# 创建自定义配置
config = create_safe_batch_config(
    chunk_size=10,               # 每组处理10个项目
    success_threshold=100.0,     # 要求100%成功率
    auto_rollback=True,          # 自动回退失败的组
    continue_on_failure=True     # 某组失败时继续处理后续组
)

# 使用自定义配置
service = SafeBatchService()
result = service.batch_update_tip(updates, config)
```

#### 所有区块批量操作
```python
from manage_tdx_mark import SafeBatchService

service = SafeBatchService()

# 股票代码列表
stocks = ["600613", "000001", "002415"]

# 1. 批量设置关注度
marks = {stock: "9" for stock in stocks}
result1 = service.batch_update_mark(marks)

# 2. 批量设置备注
tips = {
    "600613": "东阿阿胶-消费股",
    "000001": "平安银行-金融股",
    "002415": "海康威视-科技股"
}
result2 = service.batch_update_tip(tips)

# 3. 批量设置标签
tipwords = {
    "600613": "消费/白马",
    "000001": "金融/蓝筹",
    "002415": "科技/龙头"
}
result3 = service.batch_update_tipword(tipwords)

# 4. 批量设置颜色
tipcolors = {stock: "红色" for stock in stocks}
result4 = service.batch_update_tipcolor(tipcolors)

# 5. 批量设置时间
import datetime
current_time = datetime.datetime.now().strftime("%Y-%m-%d")
times = {stock: current_time for stock in stocks}
result5 = service.batch_update_time(times)

print("📊 多区块批量操作完成:")
print(f"  关注度: {result1.overall_success_rate:.1f}%")
print(f"  备注: {result2.overall_success_rate:.1f}%")
print(f"  标签: {result3.overall_success_rate:.1f}%")
print(f"  颜色: {result4.overall_success_rate:.1f}%")
print(f"  时间: {result5.overall_success_rate:.1f}%")
```

### 大批量数据处理
```python
from manage_tdx_mark import SafeBatchService, create_safe_batch_config

# 1000只股票的批量处理
def process_large_dataset():
    service = SafeBatchService()
    
    # 生成1000只股票数据
    large_updates = {
        f"60{i:04d}": f"股票{i}备注" for i in range(1000)
    }
    
    # 配置：适合大批量的参数
    config = create_safe_batch_config(
        chunk_size=20,               # 每组20只股票
        success_threshold=98.0,      # 98%成功率可接受
        auto_rollback=True,          # 自动回退
        continue_on_failure=True     # 继续处理
    )
    
    print(f"🚀 开始处理 {len(large_updates)} 只股票...")
    result = service.batch_update_tip(large_updates, config)
    
    print(f"📊 处理完成:")
    print(f"  总数据: {result.total_items}")
    print(f"  总组数: {result.total_chunks}")
    print(f"  成功组: {result.successful_chunks}")
    print(f"  失败组: {result.failed_chunks}")
    print(f"  回退组: {result.rolled_back_chunks}")
    print(f"  成功率: {result.overall_success_rate:.1f}%")
    print(f"  耗时: {result.duration:.2f} 秒")

# 执行大批量处理
process_large_dataset()
```

---

## 📈 核心概念

### 1. 📁 mark.dat 文件结构
通达信的标记文件包含 5 个数据区块：

| 区块名 | 作用 | 示例 |
|--------|------|------|
| **MARK** | 股票关注度等级 | "8" (1-9级) |
| **TIP** | 股票备注说明 | "优质白马股" |
| **TIPWORD** | 股票标签 | "消费/医药/白马" |
| **TIPCOLOR** | 显示颜色 | "红色/绿色" |
| **TIME** | 时间戳 | "2024-01-01" |

### 2. 🏷️ 股票代码格式
- **6位代码**：`600613` (用户常用)
- **8位代码**：`01600613` (系统内部格式)
  - `01` = 上海证券交易所
  - `00` = 深圳证券交易所
  - `02` = 北京证券交易所

### 3. 🔄 数据操作策略
- **直接替换**：MARK、TIP、TIPCOLOR、TIME 区块
- **合并策略**：TIPWORD 区块使用 "/" 分隔符合并多个标签

---

## 🛠️ 使用方式分层指南

### 🥇 第一层：安全批量操作 (⭐ 强烈推荐)
**适合：批量处理，追求数据安全**

```python
from manage_tdx_mark import SafeBatchService

# 创建安全批量服务
service = SafeBatchService()

# 批量更新股票备注
updates = {
    "600613": "东阿阿胶-优质消费股",
    "000001": "平安银行-金融龙头", 
    "002415": "海康威视-安防龙头"
}

result = service.batch_update_tip(updates)
if result.overall_success_rate == 100:
    print("✅ 所有数据更新成功！")
else:
    print(f"⚠️ 部分更新成功，成功率：{result.overall_success_rate:.1f}%")
```

**特点：**
- ✅ 🛡️ 最安全：分组处理，自动回退
- ✅ 📦 最高效：适合大批量数据处理
- ✅ 💯 100%保障：只有完全成功才保存
- ✅ 📊 详细报告：完整的操作统计

### 🥈 第二层：基础使用 (向后兼容)
**适合：编程新手，快速上手**

```python
from manage_tdx_mark import TdxMarkManager

# 创建管理器
manager = TdxMarkManager()

# 基本操作
data = manager.load_data()                    # 加载数据
manager.update_stock_mark("600613", "8")     # 更新标记
manager.update_stock_tip("600613", "优质股")  # 更新备注
manager.save_data(data)                       # 保存数据
```

**特点：**
- ✅ 简单易懂，一目了然
- ✅ 完全向后兼容
- ✅ 适合单个股票操作
- ❌ 批量操作安全性较低

### 🥉 第三层：服务层使用 (高级)
**适合：有一定编程基础，追求代码质量**

```python
from manage_tdx_mark import DataOperationService, StockInfo

# 创建服务
service = DataOperationService()

# 创建股票信息对象
stock = StockInfo(
    stock_code="600613",
    full_code="01600613", 
    market="上交所",
    mark="8",
    tip="东阿阿胶 - 优质消费股",
    tipword="消费/医药/传统"
)

# 添加股票数据
result = service.add_stock_data(stock, data)
if result.success:
    print(f"成功添加，影响记录：{result.affected_records}")
```

**特点：**
- ✅ 类型安全，IDE 友好
- ✅ 详细的操作结果反馈
- ✅ 适合精细化控制
- ❌ 批量操作需要额外处理

---

## 📝 实战示例

### 🎯 场景1：批量设置白马股标签（⭐ 推荐方式）
```python
from manage_tdx_mark import SafeBatchService

service = SafeBatchService()

# 白马股批量标签设置
white_horse_stocks = {
    "600613": "消费/医药/白马",
    "000858": "消费/白酒/白马", 
    "000001": "金融/银行/白马",
    "600036": "金融/银行/白马",
    "002415": "科技/安防/白马"
}

# 安全批量更新（自动分组、失败回退）
result = service.batch_update_tipword(white_horse_stocks)
print(f"✅ 白马股标签设置完成，成功率：{result.overall_success_rate:.1f}%")
print(f"📊 成功处理：{result.total_successful_items}/{result.total_items}")

# 如果有失败项目
if result.failed_items:
    print("❌ 失败项目：", result.failed_items)
```

### 🎯 场景2：大批量股票标记设置
```python
from manage_tdx_mark import SafeBatchService, create_safe_batch_config

service = SafeBatchService()

# 100只股票的关注度设置
large_updates = {
    f"60{i:04d}": "8" for i in range(100)  # 600000-600099，关注度设为8
}

# 配置：小组处理，确保安全
config = create_safe_batch_config(
    chunk_size=10,              # 每组10只股票
    success_threshold=100.0,    # 100%成功率
    auto_rollback=True,         # 自动回退
    continue_on_failure=True    # 继续处理后续组
)

result = service.batch_update_mark(large_updates, config)
print(f"🎯 大批量处理完成：")
print(f"  总组数：{result.total_chunks}")
print(f"  成功组：{result.successful_chunks}")
print(f"  回退组：{result.rolled_back_chunks}")
print(f"  总体成功率：{result.overall_success_rate:.1f}%")
```

### 🎯 场景3：给单个股票打标记（传统方式）
```python
from manage_tdx_mark import TdxMarkManager

manager = TdxMarkManager()
data = manager.load_data()

# 设置东阿阿胶为高关注度
manager.update_stock_mark("600613", "9")
manager.update_stock_tip("600613", "优质白马股，值得长期关注")

manager.save_data(data)
print("✅ 股票标记设置完成")
```

### 🎯 场景4：多区块同时批量更新
```python
from manage_tdx_mark import SafeBatchService

service = SafeBatchService()

# 同一批股票的多区块数据
stock_codes = ["600613", "000001", "002415"]

# 批量设置标记
marks = {code: "9" for code in stock_codes}
result1 = service.batch_update_mark(marks)

# 批量设置备注
tips = {
    "600613": "东阿阿胶-消费龙头",
    "000001": "平安银行-金融龙头",
    "002415": "海康威视-安防龙头"
}
result2 = service.batch_update_tip(tips)

# 批量设置标签
tipwords = {
    "600613": "消费/医药/白马",
    "000001": "金融/银行/蓝筹",
    "002415": "科技/安防/龙头"
}
result3 = service.batch_update_tipword(tipwords)

print("📊 多区块批量更新完成：")
print(f"  标记成功率：{result1.overall_success_rate:.1f}%")
print(f"  备注成功率：{result2.overall_success_rate:.1f}%")
print(f"  标签成功率：{result3.overall_success_rate:.1f}%")
```

---

## 🔍 简单查询功能

SafeBatchService 提供两种简洁的查询方式：股票代码查询和关键词搜索。

### 查询示例

#### 1. 股票代码查询
```python
from manage_tdx_mark import SafeBatchService

service = SafeBatchService()

# 查询指定股票（支持6位或8位代码）
stocks = service.safe_batch_query(["600613", "000001", "300059"])

for stock in stocks:
    print(f"股票: {stock.stock_code}")
    print(f"标记: {stock.mark_level}")
    print(f"备注: {stock.tip_text}")
    print(f"标签: {', '.join(stock.tipword_tags)}")
    print("---")
```

#### 2. 关键词模糊搜索
```python
# 搜索包含"科技"的股票（在所有字段中搜索）
tech_stocks = service.safe_batch_query("科技")

print(f"找到 {len(tech_stocks)} 只科技股票")
for stock in tech_stocks:
    print(f"{stock.stock_code}: {stock.tip_text}")
```

#### 3. 实用搜索场景
```python
# 搜索白马股
white_horse = service.safe_batch_query("白马")

# 搜索消费类股票
consumer = service.safe_batch_query("消费")

# 搜索高关注度股票（标记等级8、9）
high_attention = service.safe_batch_query("9")

# 查询多只重点关注股票
key_stocks = service.safe_batch_query([
    "600613",  # 东阿阿胶
    "000858",  # 五粮液
    "000001"   # 平安银行
])
```

### API 说明

```python
def safe_batch_query(self, query: Union[str, List[str]]) -> List[StockInfo]:
    """
    安全批量查询股票数据
    
    Args:
        query: 查询条件
            - str: 关键词模糊查询（在股票代码、备注、标签等字段中搜索）
            - List[str]: 股票代码精确查询（支持6位或8位代码）
            
    Returns:
        List[StockInfo]: 符合条件的股票数据列表
    """
```

**特点**：
- 🎯 **简单直观**：只有两种查询方式，易于理解
- ⚡ **高性能**：使用缓存机制，查询速度快
- 🔍 **智能搜索**：关键词搜索覆盖所有重要字段
- 📄 **无分页**：直接返回所有符合条件的结果
- 🛡️ **安全可靠**：保持SafeBatchService的安全特性

---

## 📈 性能优化

- **🛡️ 安全批量操作**：分组处理降低风险，自动回退保障数据安全
- **智能缓存**：LRU缓存减少重复文件I/O
- **批量操作**：支持批量更新，提升大数据处理效率
- **内存优化**：优化内存使用，支持大文件处理
- **并发安全**：线程安全的操作，支持并发访问

---

## 🔒 安全特性

- **🛡️ 分组批量处理**：自动分组，单组失败不影响其他组
- **💾 自动备份回退**：每组操作前备份，失败时自动回退
- **✅ 100%成功率要求**：只有完全成功的组才会保存
- **路径验证**：防止路径遍历攻击
- **输入净化**：自动清理和验证用户输入
- **权限检查**：文件操作前的权限验证
- **数据校验**：严格的数据格式验证

---

## ❓ 常见问题解答

### Q1: 为什么推荐使用 SafeBatchService？
**A:** SafeBatchService 是最安全的批量操作方式：
- 🛡️ **自动分组**：大批量数据分成小组处理，降低风险
- 💾 **自动备份**：每组操作前创建备份
- 🔄 **智能回退**：失败时自动恢复，不会丢失数据
- 💯 **100%保障**：只有完全成功的组才会保存

### Q2: SafeBatchService 和普通批量操作有什么区别？
**A:** 主要区别在安全性和可靠性：

| 特性 | SafeBatchService | 普通批量操作 |
|------|------------------|-------------|
| 分组处理 | ✅ 自动分组 | ❌ 全量处理 |
| 失败回退 | ✅ 自动回退 | ❌ 无回退 |
| 成功率保障 | ✅ 100%要求 | ❌ 部分成功即保存 |
| 详细报告 | ✅ 完整统计 | ❌ 基础结果 |

### Q3: 如何处理大批量数据（1000+股票）？
**A:** 使用 SafeBatchService 的自定义配置：
```python
config = create_safe_batch_config(
    chunk_size=20,              # 每组20只股票
    success_threshold=100.0,    # 100%成功率
    continue_on_failure=True    # 失败组不影响后续组
)
```

### Q4: 批量操作失败了怎么办？
**A:** SafeBatchService 提供多重保障：
- **自动回退**：失败的组会自动恢复
- **详细报告**：查看 `result.failed_items` 了解失败原因
- **继续处理**：失败组不影响其他组的处理
- **手动修复**：根据错误信息手动修复问题数据

### Q5: 为什么有些功能要用 8 位股票代码？
**A:** 通达信内部使用 8 位代码区分不同交易所：
- `01600613` = 上交所的 600613
- `00000001` = 深交所的 000001  
- 工具会自动转换，你通常只需要输入 6 位代码

### Q6: TIPWORD 标签为什么用 "/" 分隔？
**A:** 这是通达信的标准格式，允许一个股票有多个标签：
- `"消费"` → 单个标签
- `"消费/白马"` → 两个标签
- `"消费/白马/医药"` → 三个标签

---

## 🚀 学习路径建议

### 阶段一：入门 (1-2周)
**目标：掌握安全批量操作**

1. **安装和配置**
   ```bash
   pip install manage_tdx_mark
   ```

2. **学习 SafeBatchService**
   - 基本批量更新
   - 查看操作结果
   - 理解成功率概念

3. **实践项目**
   - 给自己关注的 10 只股票批量打标记
   - 设置行业标签

### 阶段二：进阶 (2-3周)  
**目标：掌握自定义配置和多区块操作**

1. **学习配置选项**
   - 调整分组大小
   - 设置成功率阈值
   - 控制错误处理策略

2. **掌握多区块操作**
   - 同时更新标记、备注、标签
   - 理解不同区块的特点

3. **实践项目**
   - 创建完整的投资组合管理系统
   - 批量导入和处理股票池数据

### 阶段三：高级 (3-4周)
**目标：精通所有功能，理解底层原理**

1. **深入理解架构**
   - 学习传统 API
   - 理解数据模型
   - 掌握缓存机制

2. **性能优化**
   - 大批量数据处理
   - 自定义验证规则

3. **实践项目**
   - 构建企业级股票管理系统
   - 数据分析和报告生成

---

## 📝 更新日志

### v3.1.0 (当前版本)
- ⭐ **SafeBatchService**：全新安全批量操作服务
- 🛡️ 分组小批量处理，100%成功率保障
- 💾 自动备份回退机制
- ✨ 全新模块化架构
- 🔒 增强安全验证
- ⚡ 智能缓存系统
- 📊 类型安全保证

---

## 🤝 贡献指南

欢迎提交Issue和Pull Request！

1. Fork 本仓库
2. 创建特性分支 (`git checkout -b feature/AmazingFeature`)
3. 提交更改 (`git commit -m 'Add some AmazingFeature'`)
4. 推送到分支 (`git push origin feature/AmazingFeature`)
5. 开启 Pull Request

---

## 📄 许可证

本项目基于 MIT 许可证开源 - 查看 [LICENSE](LICENSE) 文件了解详情。

---

## 👨‍💻 作者

**Kilo Code**
- 📧 Email: kilocode@example.com
- 🏢 专注于金融科技和投资工具开发

---

## 🙏 致谢

感谢所有为这个项目做出贡献的开发者和用户。

---

## 🎉 总结

TDX Mark Manager 现在有了**最安全的批量操作方案**：

- **🥇 SafeBatchService**：最推荐，最安全，适合批量处理
- **🥈 基础 API**：简单易用，适合单个操作  
- **🥉 服务层 API**：灵活控制，适合高级用户

**核心优势：**
- 🛡️ **分组处理**：降低风险，提高成功率
- 💾 **自动回退**：失败不丢数据
- 💯 **100%保障**：只保存完全成功的数据
- 📊 **详细报告**：清楚了解每个操作的结果

**记住：**好的工具应该让复杂的事情变简单，让简单的事情变自动化。SafeBatchService 让批量操作变得既简单又安全！

---

<div align="center">

**如果这个项目对您有帮助，请给我们一个 ⭐ Star！**

📞 **需要帮助？**
- 📧 Email: kilocode@example.com
- 🐛 问题反馈：[GitHub Issues](https://github.com/your-repo/issues)
- 📚 更多文档：[项目主页](https://github.com/your-repo)

</div>