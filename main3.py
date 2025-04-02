import sys
from excel_parser import ExcelParser, MigrationSheet, MigrationType
from db_connector import DatabaseConnector
from data_migration_onetoone import execute_one_to_one_migration
from data_migration_onetomany import execute_one_to_many_migration
from data_migration_manytoone import execute_many_to_one_migration

class DataMigrationExecutor:
    def __init__(self, excel_path: str):
        """
        データ移行実行クラスの初期化
        :param excel_path: Excelファイルパス
        """
        self.excel_path = excel_path
        self.parser = None
        self.source_db = None
        self.target_db = None
    
    def initialize(self) -> bool:
        """
        必要なリソースの初期化
        :return: 初期化が成功したかどうか
        """
        try:
            # Excelパーサーの初期化
            self.parser = ExcelParser(self.excel_path)
            
            # データベース接続の初期化
            self.source_db = DatabaseConnector(is_source=True)
            self.target_db = DatabaseConnector(is_source=False)
            
            # データベース接続の確認
            if self.source_db is None:
                raise Exception("ソースデータベース接続に失敗しました")
            if self.target_db is None:
                raise Exception("ターゲットデータベース接続に失敗しました")
            print("データベース接続が成功しました")
            return True
            
        except Exception as e:
            print(f"初期化に失敗しました: {str(e)}")
            import traceback
            traceback.print_exc()
            return False
    
    def cleanup(self):
        """
        リソースのクリーンアップ
        """
        try:
            if self.source_db:
                self.source_db.close()
            if self.target_db:
                self.target_db.close()
        except Exception as e:
            print(f"リソースのクリーンアップ中にエラーが発生しました: {str(e)}")
    
    def execute_migration(self, mapping_name: str):
        """
        データ移行の実行
        :param mapping_name: マッピング一覧のマッピング名
        """
        try:
            if not self.initialize():
                return
            
            print("\nデータ移行を開始します...")
            
            # 指定された移行設定の取得
            migration_sheet = self.parser.parse_mapping_data_to_run(mapping_name)
            
            # 移行タイプに応じた移行の実行
            if migration_sheet.migration_type == MigrationType.ONE_TO_ONE:
                print("\n=== 1対1移行を開始します ===")
                execute_one_to_one_migration(
                    self.excel_path,
                    self.parser,
                    self.source_db,
                    self.target_db,
                    [migration_sheet]
                )
            elif migration_sheet.migration_type == MigrationType.ONE_TO_MANY:
                print("\n=== 1対多移行を開始します ===")
                execute_one_to_many_migration(
                    self.excel_path,
                    self.parser,
                    self.source_db,
                    self.target_db,
                    [migration_sheet]
                )
            elif migration_sheet.migration_type == MigrationType.MANY_TO_ONE:
                print("\n=== 多対1移行を開始します ===")
                execute_many_to_one_migration(
                    self.excel_path,
                    self.parser,
                    self.source_db,
                    self.target_db,
                    [migration_sheet]
                )
            
            print("\nデータ移行が完了しました")
            
        except Exception as e:
            print(f"データ移行中にエラーが発生しました: {str(e)}")
            import traceback
            traceback.print_exc()
        finally:
            self.cleanup()

def main():
    """
    メイン関数
    """
    # コマンドライン引数のチェック
    # if len(sys.argv) != 2:
    #     print("使用方法: python main3.py <マッピング一覧名称>")
    #     sys.exit(1)
    
    # マッピング名パラメータの取得
    # mapping_name = sys.argv[1]
    mapping_name="dbo.AccountingDetailTbl"
    excel_path = "数据移行2.xlsx"
    # 移行の実行
    executor = DataMigrationExecutor(excel_path)
    executor.execute_migration(mapping_name)

if __name__ == "__main__":
    main() 