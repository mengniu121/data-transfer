from dotenv import load_dotenv
import os

load_dotenv()

# 数据库连接配置
SOURCE_DB_CONFIG = {
    'driver': 'SQL Server',
    'server': os.getenv('SOURCE_DB_SERVER'),
    'database': os.getenv('SOURCE_DB_NAME'),
    'uid': os.getenv('DB_USER'),  # SQL Server身份验证用户名
    'pwd': os.getenv('DB_PASSWORD')  # SQL Server身份验证密码
}

TARGET_DB_CONFIG = {
    'driver': 'SQL Server',
    'server': os.getenv('TARGET_DB_SERVER'),
    'database': os.getenv('TARGET_DB_NAME'),
    'uid': os.getenv('DB_USER'),  # SQL Server身份验证用户名
    'pwd': os.getenv('DB_PASSWORD')  # SQL Server身份验证密码
}

# 数据类型映射
DATA_TYPE_MAPPINGS = {
    'int': str,
    'datetime': str,
    'float': str,
    'decimal': str,
    'varchar': str,
    'nvarchar': str,
    'char': str,
    'nchar': str,
    'text': str,
    'ntext': str
} 