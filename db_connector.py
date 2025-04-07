import pyodbc
from config import SOURCE_DB_CONFIG, TARGET_DB_CONFIG

class DatabaseConnector:
    def __init__(self, is_source=True):
        """
        データベース接続の初期化
        :param is_source: Trueはソースデータベース、Falseはターゲットデータベース
        """
        self.config = SOURCE_DB_CONFIG if is_source else TARGET_DB_CONFIG
        self.conn = None
        self.cursor = None

    def connect(self):
        """データベース接続の確立"""
        if not self.conn:
            try:
                # 標準的なODBC接続文字列フォーマットを使用
                conn_str = (
                    "Driver={ODBC Driver 17 for SQL Server};"
                    f"Server={self.config['server']};"
                    f"Database={self.config['database']};"
                    f"Uid={self.config['uid']};"
                    f"Pwd={self.config['pwd']};"
                    "TrustServerCertificate=yes"
                )
                print(f"データベースへの接続を試みます。接続文字列: {conn_str}")
                self.conn = pyodbc.connect(conn_str)
                self.cursor = self.conn.cursor()
                print(f"データベースへの接続が成功しました: {self.config['server']}/{self.config['database']}")
            except pyodbc.Error as e:
                print(f"接続に失敗しました: {str(e)}")
                raise
        return self.cursor

    def execute_query(self, query, params=None):
        """
        SQLクエリの実行
        :param query: SQLクエリ文
        :param params: クエリパラメータ
        :return: クエリ結果
        """
        cursor = self.connect()
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        return cursor

    def fetch_all(self, query, params=None):
        """
        クエリ結果の全件取得
        :param query: SQLクエリ文
        :param params: クエリパラメータ
        :return: クエリ結果リスト
        """
        cursor = self.execute_query(query, params)
        return cursor.fetchall()

    def commit(self):
        """トランザクションのコミット"""
        if self.conn:
            self.conn.commit()

    def rollback(self):
        """トランザクションのロールバック"""
        if self.conn:
            self.conn.rollback()

    def close(self):
        """データベース接続のクローズ"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
            self.conn = None
            self.cursor = None

    def __enter__(self):
        """コンテキストマネージャーのエントリー"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """コンテキストマネージャーのエグジット"""
        self.close() 