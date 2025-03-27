import pyodbc
from config import SOURCE_DB_CONFIG, TARGET_DB_CONFIG

class DatabaseConnector:
    def __init__(self, is_source=True):
        """
        初始化数据库连接器
        :param is_source: True表示源数据库，False表示目标数据库
        """
        self.config = SOURCE_DB_CONFIG if is_source else TARGET_DB_CONFIG
        self.conn = None
        self.cursor = None

    def connect(self):
        """建立数据库连接"""
        if not self.conn:
            try:
                # 使用标准的ODBC连接字符串格式
                conn_str = (
                    "Driver={SQL Server};"
                    f"Server={self.config['server']};"
                    f"Database={self.config['database']};"
                    f"Uid={self.config['uid']};"
                    f"Pwd={self.config['pwd']};"
                    "TrustServerCertificate=yes"
                )
                print(f"尝试连接到数据库，连接字符串: {conn_str}")
                self.conn = pyodbc.connect(conn_str)
                self.cursor = self.conn.cursor()
                print(f"成功连接到数据库: {self.config['server']}/{self.config['database']}")
            except pyodbc.Error as e:
                print(f"连接失败: {str(e)}")
                raise
        return self.cursor

    def execute_query(self, query, params=None):
        """
        执行SQL查询
        :param query: SQL查询语句
        :param params: 查询参数
        :return: 查询结果
        """
        cursor = self.connect()
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        return cursor

    def fetch_all(self, query, params=None):
        """
        获取所有查询结果
        :param query: SQL查询语句
        :param params: 查询参数
        :return: 查询结果列表
        """
        cursor = self.execute_query(query, params)
        return cursor.fetchall()

    def commit(self):
        """提交事务"""
        if self.conn:
            self.conn.commit()

    def rollback(self):
        """回滚事务"""
        if self.conn:
            self.conn.rollback()

    def close(self):
        """关闭数据库连接"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
            self.conn = None
            self.cursor = None

    def __enter__(self):
        """上下文管理器入口"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器出口"""
        self.close() 