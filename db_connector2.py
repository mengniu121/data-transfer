import pyodbc
import os
from dotenv import load_dotenv

class DatabaseConnector2:
    def __init__(self):
        """
        初始化数据库连接器
        """
        try:
            # 加载环境变量
            load_dotenv()
            
            # 获取数据库连接信息
            server = os.getenv("SOURCE_DB_SERVER")
            database = os.getenv("SOURCE_DB_NAME")
            username = os.getenv("DB_USER")
            password = os.getenv("DB_PASSWORD")
            
            if not all([server, database, username, password]):
                raise ValueError("缺少必要的数据库连接信息: TARGET_DB_*")
            
            # 构建连接字符串
            conn_str = (
                f"DRIVER={{SQL Server}};"
                f"SERVER={server};"
                f"DATABASE={database};"
                f"UID={username};"
                f"PWD={password};"
                "TrustServerCertificate=yes"
            )
            
            # 建立连接
            self.conn = pyodbc.connect(conn_str)
            self.cursor = self.conn.cursor()
            print(f"成功连接到数据库: {server}/{database}")
            
        except Exception as e:
            print(f"数据库连接失败: {str(e)}")
            self.conn = None
            self.cursor = None
    
    def test_connection(self) -> bool:
        """
        测试数据库连接
        :return: 是否连接成功
        """
        try:
            if self.conn is None:
                return False
            self.cursor.execute("SELECT 1")
            return True
        except Exception as e:
            print(f"数据库连接测试失败: {str(e)}")
            return False
    
    def execute_query(self, query: str, params: tuple = None):
        """
        执行SQL查询
        :param query: SQL查询语句
        :param params: 查询参数
        """
        try:
            if params:
                self.cursor.execute(query, params)
            else:
                self.cursor.execute(query)
        except Exception as e:
            print(f"执行查询失败: {str(e)}")
            raise
    
    def executemany(self, query: str, params_list: list):
        """
        执行批量SQL查询
        :param query: SQL查询语句
        :param params_list: 查询参数列表
        """
        try:
            self.cursor.fast_executemany = True
            self.cursor.executemany(query, params_list)
        except Exception as e:
            print(f"执行批量查询失败: {str(e)}")
            raise
    
    def commit(self):
        """
        提交事务
        """
        try:
            self.conn.commit()
        except Exception as e:
            print(f"提交事务失败: {str(e)}")
            raise
    
    def rollback(self):
        """
        回滚事务
        """
        try:
            self.conn.rollback()
        except Exception as e:
            print(f"回滚事务失败: {str(e)}")
            raise
    
    def close(self):
        """
        关闭数据库连接
        """
        try:
            if self.cursor:
                self.cursor.close()
            if self.conn:
                self.conn.close()
        except Exception as e:
            print(f"关闭数据库连接失败: {str(e)}")
    
    def __enter__(self):
        """上下文管理器入口"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器出口"""
        self.close() 