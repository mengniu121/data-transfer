import pandas as pd
from excel_parser import ExcelParser, MigrationType
from db_connector import DatabaseConnector
import os
from dotenv import load_dotenv
from data_migration_onetoone3 import execute_one_to_one_migration
from data_migration_onetomany import execute_one_to_many_migration
from data_migration_manytoone import execute_many_to_one_migration

class DataMigrationExecutor:
    def __init__(self, excel_path: str):
        """
        データ移行実行の初期化
        :param excel_path: Excel設定ファイルパス
        """
        self.excel_path = excel_path
        self.parser = None
        self.source_db = None
        self.target_db = None

    def initialize(self):
        """
        データベース接続とExcelパーサーの初期化
        """
        try:
            # 環境変数の読み込み
            load_dotenv()
            
            # データベース接続の作成
            print("\nデータベースに接続中...")
            self.source_db = DatabaseConnector(is_source=True)
            self.target_db = DatabaseConnector(is_source=False)
            
            # 验证数据库连接
            if self.source_db is None:
                raise Exception("ソースデータベース接続に失敗しました")
            if self.target_db is None:
                raise Exception("ターゲットデータベース接続に失敗しました")
                
            print("データベース接続が成功しました")
            
            # 创建Excel解析器
            self.parser = ExcelParser(self.excel_path)
            
            # 解析マッピング一覧sheet
            self.parser.parse_mapping_sheet()
            
            return True
            
        except Exception as e:
            print(f"初期化に失敗しました: {str(e)}")
            return False

    def cleanup(self):
        """
        リソースのクリーンアップ
        """
        if self.source_db:
            self.source_db.close()
        if self.target_db:
            self.target_db.close()

    def execute_migration(self):
        """
        データ移行の実行
        """
        try:
            if not self.initialize():
                return
            
            print("\nデータ移行を開始します...")
            
            # 移行対象のテーブルを取得
            migration_sheets = self.parser.get_migration_sheets()
            
            # 移行タイプでグループ化
            one_to_one_sheets = []
            one_to_many_sheets = []
            many_to_one_sheets = []
            
            for sheet in migration_sheets:
                if sheet.migration_type == MigrationType.ONE_TO_ONE:
                    one_to_one_sheets.append(sheet)
                elif sheet.migration_type == MigrationType.ONE_TO_MANY:
                    one_to_many_sheets.append(sheet)
                elif sheet.migration_type == MigrationType.MANY_TO_ONE:
                    many_to_one_sheets.append(sheet)
            
            # 1対1移行を開始します
            if one_to_one_sheets:
                print("\n=== 1対1移行を開始します ===")
                execute_one_to_one_migration(
                    self.excel_path,
                    self.parser,
                    self.source_db,
                    self.target_db,
                    one_to_one_sheets
                )
            
            # 1対多移行を開始します
            if one_to_many_sheets:
                print("\n=== 1対多移行を開始します ===")
                execute_one_to_many_migration(
                    self.excel_path,
                    self.parser,
                    self.source_db,
                    self.target_db,
                    one_to_many_sheets
                )
            
            # 多対1移行を開始します
            if many_to_one_sheets:
                print("\n=== 多対1移行を開始します ===")
                execute_many_to_one_migration(
                    self.excel_path,
                    self.parser,
                    self.source_db,
                    self.target_db,
                    many_to_one_sheets
                )
            
            print("\n全移行タスクが完了しました")
            
        except Exception as e:
            print(f"移行中にエラーが発生しました: {str(e)}")
            import traceback
            traceback.print_exc()
        finally:
            self.cleanup()

def main():
    """
    メインプログラム
    """
    excel_path = "データ移行.xlsx"
    executor = DataMigrationExecutor(excel_path)
    executor.execute_migration()

if __name__ == "__main__":
    main() 