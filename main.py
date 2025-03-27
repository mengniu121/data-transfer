import pandas as pd
from excel_parser import ExcelParser, MigrationType
from db_connector import DatabaseConnector
import os
from dotenv import load_dotenv
from data_migration_onetoone import execute_one_to_one_migration
from data_migration_onetomany import execute_one_to_many_migration
from data_migration_manytoone import execute_many_to_one_migration

class DataMigrationExecutor:
    def __init__(self, excel_path: str):
        """
        初始化数据迁移执行器
        :param excel_path: Excel配置文件路径
        """
        self.excel_path = excel_path
        self.parser = None
        self.source_db = None
        self.target_db = None

    def initialize(self):
        """
        初始化数据库连接和Excel解析器
        """
        try:
            # 加载环境变量
            load_dotenv()
            
            # 创建数据库连接器
            print("\n正在连接数据库...")
            self.source_db = DatabaseConnector(is_source=True)
            self.target_db = DatabaseConnector(is_source=False)
            
            # 验证数据库连接
            if self.source_db is None:
                raise Exception("源数据库连接失败")
            if self.target_db is None:
                raise Exception("目标数据库连接失败")
                
            print("数据库连接成功")
            
            # 创建Excel解析器
            self.parser = ExcelParser(self.excel_path)
            
            # 解析マッピング一覧sheet
            self.parser.parse_mapping_sheet()
            
            return True
            
        except Exception as e:
            print(f"初始化失败: {str(e)}")
            return False

    def cleanup(self):
        """
        清理资源
        """
        if self.source_db:
            self.source_db.close()
        if self.target_db:
            self.target_db.close()

    def execute_migration(self):
        """
        执行数据迁移
        """
        try:
            if not self.initialize():
                return
            
            print("\n开始数据迁移...")
            
            # 获取需要迁移的表
            migration_sheets = self.parser.get_migration_sheets()
            
            # 按迁移类型分组
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
            
            # 执行一对一迁移
            if one_to_one_sheets:
                print("\n=== 开始执行一对一迁移 ===")
                execute_one_to_one_migration(
                    self.excel_path,
                    self.parser,
                    self.source_db,
                    self.target_db,
                    one_to_one_sheets
                )
            
            # 执行一对多迁移
            if one_to_many_sheets:
                print("\n=== 开始执行一对多迁移 ===")
                execute_one_to_many_migration(
                    self.excel_path,
                    self.parser,
                    self.source_db,
                    self.target_db,
                    one_to_many_sheets
                )
            
            # 执行多对一迁移
            if many_to_one_sheets:
                print("\n=== 开始执行多对一迁移 ===")
                execute_many_to_one_migration(
                    self.excel_path,
                    self.parser,
                    self.source_db,
                    self.target_db,
                    many_to_one_sheets
                )
            
            print("\n所有迁移任务完成")
            
        except Exception as e:
            print(f"迁移过程中发生错误: {str(e)}")
            import traceback
            traceback.print_exc()
        finally:
            self.cleanup()

def main():
    """
    主函数
    """
    excel_path = "数据移行2.xlsx"
    executor = DataMigrationExecutor(excel_path)
    executor.execute_migration()

if __name__ == "__main__":
    main() 