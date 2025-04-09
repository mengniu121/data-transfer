from dotenv import load_dotenv
import os

load_dotenv()

# データベース接続設定
SOURCE_DB_CONFIG = {
    'driver': 'SQL Server',
    'server': os.getenv('SOURCE_DB_SERVER'),
    'database': os.getenv('SOURCE_DB_NAME'),
    'uid': os.getenv('SOURCE_DB_USER'),  # SQL Server認証ユーザー名
    'pwd': os.getenv('SOURCE_DB_PASSWORD')  # SQL Server認証パスワード
}

TARGET_DB_CONFIG = {
    'driver': 'SQL Server',
    'server': os.getenv('TARGET_DB_SERVER'),
    'database': os.getenv('TARGET_DB_NAME'),
    'uid': os.getenv('TARGET_DB_USER'),  # SQL Server認証ユーザー名
    'pwd': os.getenv('TARGET_DB_PASSWORD')  # SQL Server認証パスワード
}

# データ型マッピング
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