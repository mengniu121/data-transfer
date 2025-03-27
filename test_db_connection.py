from db_connector import DatabaseConnector

def test_connection():
    """测试数据库连接和查询"""
    try:
        # 测试源数据库连接
        print("测试源数据库连接...")
        source_db = DatabaseConnector(is_source=True)
        print("源数据库连接成功！")

        # 测试查询
        print("\n测试查询...")
        query = """
        SELECT TOP 5 *
        FROM dbo.K_KEIJO_MEISAI_TBL
        """
        
        results = source_db.fetch_all(query)
        if results:
            print("\n查询结果（前5行）:")
            for row in results:
                print(row)
        else:
            print("表中没有数据")

    except Exception as e:
        print(f"\n发生错误: {str(e)}")
    finally:
        # 关闭连接
        source_db.close()

if __name__ == "__main__":
    test_connection() 