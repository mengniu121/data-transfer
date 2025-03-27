import pandas as pd
from typing import List
from excel_parser import MigrationSheet
from util import convert_type

def execute_one_to_one_migration(excel_path: str, parser, source_db, target_db, sheets: List[MigrationSheet]):
    """
    执行一对一迁移
    :param excel_path: Excel文件路径
    :param parser: Excel解析器实例
    :param source_db: 源数据库连接
    :param target_db: 目标数据库连接
    :param sheets: 需要迁移的表配置列表
    """
    try:
        for sheet in sheets:
            print(f"\n处理表 {sheet.logical_name}:")
            print(f"  从 {sheet.source_name} 迁移到 {sheet.physical_name}")
            
            # 获取字段映射和转换规则
            field_mapping = {}
            type_conversion_mapping = {}
            
            # 读取字段映射sheet
            df = pd.read_excel(excel_path, sheet_name=sheet.logical_name)
            
            for _, row in df.iterrows():
                if str(row.get('Transform', '')).upper() == 'Y':
                    source_field = str(row.get('現行Type物理名'))
                    target_field = str(row.get('次期Type物理名'))
                    
                    field_mapping[source_field] = target_field
                    
                    # 构建转换规则
                    type_conversion_mapping[source_field] = {
                        'data_type': str(row.get('データ型', '')),
                        'not_null': str(row.get('Not Null', '')).upper() == 'Y',
                        'default_value': row.get('デフォルト')
                    }
            
            if not field_mapping:
                print(f"  警告: 没有找到需要迁移的字段")
                continue
            
            print(f"  找到 {len(field_mapping)} 个需要迁移的字段")
            
            try:
                # 从源表读取数据
                select_query = f"SELECT {', '.join(field_mapping.keys())} FROM {sheet.source_name}"
                print(f"  执行查询: {select_query}")
                rows = source_db.fetch_all(select_query)
                
                if not rows:
                    print(f"  警告: 源表 {sheet.source_name} 没有数据")
                    continue
                
                print(f"  从源表读取到 {len(rows)} 条记录")
                
                # 准备插入语句
                insert_query = f"INSERT INTO {sheet.physical_name} ({', '.join(field_mapping.values())}) VALUES ({', '.join(['?' for _ in field_mapping])})"
                print(f"  执行插入: {insert_query}")
                
                # 逐行处理数据
                for i, row in enumerate(rows, 1):
                    # 转换数据
                    converted_values = []
                    for idx, (old_field, new_field) in enumerate(field_mapping.items()):
                        value = row[idx]
                        conversion_rule = type_conversion_mapping.get(old_field)
                        converted_value = convert_type(value, conversion_rule)
                        converted_values.append(converted_value)
                    
                    # 执行插入
                    target_db.execute_query(insert_query, converted_values)
                    
                    # 每100条记录提交一次
                    if i % 100 == 0:
                        target_db.commit()
                        print(f"  已处理 {i}/{len(rows)} 条记录")
                
                # 提交剩余的事务
                target_db.commit()
                print(f"  迁移成功完成，共处理 {len(rows)} 条记录")
                
            except Exception as e:
                target_db.rollback()
                print(f"  迁移失败: {str(e)}")
                continue
                
    except Exception as e:
        print(f"一对一迁移过程中发生错误: {str(e)}")
        import traceback
        traceback.print_exc() 