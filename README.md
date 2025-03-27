# 数据库迁移工具

这是一个用于SQL Server数据库迁移的Python工具，支持一对一、一对多和多对一的表迁移，并提供灵活的数据转换功能。

## 功能特点

- 支持多种迁移模式：
  - 一对一：从旧库的表迁移到新库的单个表
  - 一对多：从旧库的单个表迁移到新库的多个表
  - 多对一：从多个旧库的表合并到新库的单个表
- 通过Excel配置文件定义迁移关系
- 支持数据类型转换
- 支持默认值设置
- 支持自定义转换规则
- 批量处理以提高性能
- 事务支持，确保数据一致性

## 安装要求

1. Python 3.7+
2. 安装依赖包：
```bash
pip install -r requirements.txt
```

## 配置文件说明

迁移配置通过Excel文件进行管理，包含两个工作表：

### TableMappings工作表

定义表之间的映射关系：

- MappingName: 映射配置名称
- MappingType: 映射类型（一对一、一对多、多对一）
- SourceTables: 源表名称（多个表用逗号分隔）
- TargetTables: 目标表名称（多个表用逗号分隔）

### FieldMappings工作表

定义字段之间的映射关系：

- MappingName: 映射配置名称（与TableMappings中的对应）
- TargetTable: 目标表名称
- TargetField: 目标字段名称
- SourceField: 源字段名称
- TargetType: 目标字段类型
- DefaultValue: 默认值（可选）
- TransformRule: 转换规则（可选）

## 环境变量配置

创建 `.env` 文件并配置数据库连接信息：

```
SOURCE_DB_SERVER=source_server_name
SOURCE_DB_NAME=source_db_name
SOURCE_DB_USER=source_user
SOURCE_DB_PASSWORD=source_password

TARGET_DB_SERVER=target_server_name
TARGET_DB_NAME=target_db_name
TARGET_DB_USER=target_user
TARGET_DB_PASSWORD=target_password
```

## 使用示例

```python
from migration_executor import MigrationExecutor

# 初始化迁移执行器
executor = MigrationExecutor('migration_config.xlsx')

# 执行指定的迁移配置
executor.execute_migration('mapping_name', batch_size=1000)
```

## 注意事项

1. 确保已安装SQL Server驱动程序
2. 配置文件中的表名和字段名要与数据库中的完全匹配
3. 建议在执行迁移前备份目标数据库
4. 对于大量数据迁移，可以调整batch_size参数优化性能

## 错误处理

- 数据类型转换失败时，如果配置了默认值，将使用默认值
- 如果没有配置默认值，将抛出异常
- 所有的数据库操作都在事务中执行，确保数据一致性 