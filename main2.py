from db_connector2 import DatabaseConnector2
from data_migration_manytoone2 import generate_test_data

def main():
    """
    主函数
    """
    try:
        print("\n开始生成测试数据...")
        
        # 创建数据库连接
        print("\n正在连接数据库...")
        target_db = DatabaseConnector2()
        
        if not target_db.test_connection():
            raise Exception("目标数据库连接失败")
            
        print("数据库连接成功")
        
        # 生成测试数据
        generate_test_data(target_db, "K_KEIJO_MEISAI_TBL", 10000)
        
    except Exception as e:
        print(f"发生错误: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        # 关闭数据库连接
        if 'target_db' in locals():
            target_db.close()

if __name__ == "__main__":
    main() 