# 数据迁移工具

这是一个用于数据迁移的工具，支持多种迁移模式，包括一对一、一对多和多对一的表数据迁移。

## 功能特点

- 支持多种迁移模式：
  - 一对一迁移（One-to-One）
  - 一对多迁移（One-to-Many）
  - 多对一迁移（Many-to-One）
- 支持Excel配置文件
- 支持数据类型转换
- 支持批量数据插入
- 支持事务管理
- 支持环境变量配置

## 项目结构

```
.
├── main.py                    # 主程序入口
├── main2.py                   # 测试数据生成程序入口
├── excel_parser.py            # Excel配置文件解析器
├── db_connector.py            # 数据库连接器（用于数据迁移）
├── db_connector2.py           # 数据库连接器（用于测试数据生成）
├── data_migration_onetoone.py # 一对一迁移实现
├── data_migration_onetomany.py# 一对多迁移实现
├── data_migration_manytoone.py# 多对一迁移实现
├── util.py                    # 通用工具函数
├── requirements.txt           # 项目依赖
└── .env                      # 环境变量配置文件
```

## 环境要求

- Python 3.8+
- SQL Server数据库
- 必要的Python包（见requirements.txt）

## 安装

1. 克隆项目到本地
2. 安装依赖包：
   ```bash
   pip install -r requirements.txt
   ```
3. 配置环境变量（.env文件）：
   ```
   # 源数据库配置
   SOURCE_DB_SERVER=your_source_server
   SOURCE_DB_NAME=your_source_database
   SOURCE_DB_USER=your_source_username
   SOURCE_DB_PASSWORD=your_source_password

   # 目标数据库配置
   TARGET_DB_SERVER=your_target_server
   TARGET_DB_NAME=your_target_database
   TARGET_DB_USER=your_target_username
   TARGET_DB_PASSWORD=your_target_password
   ```

## 使用方法

### 1. 数据迁移

使用main.py执行数据迁移：

```bash
python main.py
```

### 2. 生成测试数据

使用main2.py生成测试数据：

```bash
python main2.py
```

## Excel配置文件格式

配置文件需要包含以下sheet：

1. マッピング一覧（Mapping List）
   - 包含所有需要迁移的表配置
   - 指定迁移类型（一对一、一对多、多对一）
   - 指定源表和目标表

2. 每个表的配置sheet
   - 字段映射关系
   - 数据类型转换规则
   - 表联合条件（多对一迁移）

## 注意事项

1. 确保数据库连接信息正确
2. 确保Excel配置文件格式正确
3. 确保目标表已经创建
4. 建议在执行迁移前备份数据

## 错误处理

- 程序会记录详细的错误信息
- 支持事务回滚
- 支持批量插入失败时的错误处理

## 依赖包

- pyodbc==4.0.39：数据库连接
- pandas==2.1.4：数据处理
- python-dotenv==1.0.0：环境变量管理
- openpyxl==3.1.2：Excel文件处理
- numpy==1.26.2：数值计算
- tqdm==4.66.1：进度条显示

## 许可证

MIT License 