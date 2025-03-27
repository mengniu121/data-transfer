import pandas as pd
from typing import List
from excel_parser import MigrationSheet
from util import convert_type

def execute_one_to_many_migration(excel_path: str, parser, source_db, target_db, sheets: List[MigrationSheet]):
    """
    执行一对多迁移
    :param excel_path: Excel文件路径
    :param parser: Excel解析器实例
    :param source_db: 源数据库连接
    :param target_db: 目标数据库连接
    :param sheets: 需要迁移的表配置列表
    """
    try:
        for sheet in sheets:
            print(f"\n处理表组 {sheet.logical_name}:")
            print(f"源表: {sheet.source_name}")
            
            # 读取字段映射sheet
            df = pd.read_excel(excel_path, sheet_name=sheet.logical_name)
            
            # 用于存储每个目标表的字段映射
            target_mappings = {}
            
            # 按目标表分组处理字段映射
            for _, row in df.iterrows():
                if str(row.get('Transform', '')).upper() == 'Y':
                    target_table = str(row.get('次期DB物理名'))
                    source_field = str(row.get('現行Type物理名'))
                    target_field = str(row.get('次期Type物理名'))
                    
                    # 如果目标表不存在，创建映射结构
                    if target_table not in target_mappings:
                        target_mappings[target_table] = {
                            'field_mapping': {},
                            'type_conversion_mapping': {}
                        }
                    
                    target_mappings[target_table]['field_mapping'][source_field] = target_field
                    target_mappings[target_table]['type_conversion_mapping'][source_field] = {
                        'data_type': str(row.get('データ型', '')),
                        'not_null': str(row.get('Not Null', '')).upper() == 'Y',
                        'default_value': row.get('デフォルト')
                    }
            
            # 从源表读取数据
            all_source_fields = set()
            for mappings in target_mappings.values():
                all_source_fields.update(mappings['field_mapping'].keys())
            
            if not all_source_fields:
                print("  警告: 没有找到需要迁移的字段")
                continue
            
            select_query = f"SELECT {', '.join(all_source_fields)} FROM {sheet.source_name}"
            print(f"  执行查询: {select_query}")
            rows = source_db.fetch_all(select_query)
            
            if not rows:
                print(f"  警告: 源表 {sheet.source_name} 没有数据")
                continue
            
            print(f"  从源表读取到 {len(rows)} 条记录")
            
            # 处理每个目标表
            for target_table, mappings in target_mappings.items():
                if not mappings['field_mapping']:
                    continue
                    
                print(f"\n处理目标表: {target_table}")
                field_mapping = mappings['field_mapping']
                type_conversion_mapping = mappings['type_conversion_mapping']
                
                # 准备插入语句
                insert_query = f"INSERT INTO {target_table} ({', '.join(field_mapping.values())}) VALUES ({', '.join(['?' for _ in field_mapping])})"
                print(f"  执行插入: {insert_query}")
                
                # 逐行处理数据
                for i, row_data in enumerate(rows, 1):
                    # 转换数据
                    converted_values = []
                    for source_field in field_mapping.keys():
                        # 找到源字段在select结果中的索引
                        source_index = list(all_source_fields).index(source_field)
                        value = row_data[source_index]
                        conversion_rule = type_conversion_mapping.get(source_field)
                        converted_value = convert_type(value, conversion_rule)
                        converted_values.append(converted_value)
                    
                    try:
                        # 执行插入
                        target_db.execute_query(insert_query, converted_values)
                        
                        # 每100条记录提交一次
                        if i % 100 == 0:
                            target_db.commit()
                            print(f"  已处理 {i}/{len(rows)} 条记录")
                    except Exception as e:
                        target_db.rollback()
                        print(f"  插入失败: {str(e)}")
                        continue
                
                # 提交剩余的事务
                target_db.commit()
                print(f"  迁移成功完成，共处理 {len(rows)} 条记录")
                
    except Exception as e:
        print(f"一对多迁移过程中发生错误: {str(e)}")
        import traceback
        traceback.print_exc() 