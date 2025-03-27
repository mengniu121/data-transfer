import pandas as pd
from typing import List
from excel_parser import MigrationSheet
from util import convert_type

def execute_many_to_one_migration(excel_path: str, parser, source_db, target_db, sheets: List[MigrationSheet]):
    """
    执行多对一迁移
    :param excel_path: Excel文件路径
    :param parser: Excel解析器实例
    :param source_db: 源数据库连接
    :param target_db: 目标数据库连接
    :param sheets: 需要迁移的表配置列表
    """
    try:
        for sheet in sheets:
            print(f"\n处理表组 {sheet.logical_name}:")
            print(f"目标表: {sheet.physical_name}")
            
            # 读取字段映射sheet
            df = pd.read_excel(excel_path, sheet_name=sheet.logical_name)
            
            # 用于存储源表的字段映射
            source_mappings = {}
            target_fields = []
            join_conditions = None
            
            # 处理字段映射
            for _, row in df.iterrows():
                # 获取联合条件
                if pd.notna(row.get('Union')):
                    join_conditions = str(row.get('Union')).strip()
                    
                if str(row.get('Transform', '')).upper() == 'Y':
                    source_table = str(row.get('現行DB物理名'))
                    source_field = str(row.get('現行Type物理名'))
                    target_field = str(row.get('次期Type物理名'))
                    
                    # 如果源表不存在，创建映射结构
                    if source_table not in source_mappings:
                        source_mappings[source_table] = {
                            'fields': [],
                            'type_conversion': {}
                        }
                    
                    # 添加字段映射
                    source_mappings[source_table]['fields'].append({
                        'source_field': source_field,
                        'target_field': target_field
                    })
                    
                    # 添加类型转换规则
                    source_mappings[source_table]['type_conversion'][source_field] = {
                        'data_type': str(row.get('データ型', '')),
                        'not_null': str(row.get('Not Null', '')).upper() == 'Y',
                        'default_value': row.get('デフォルト')
                    }
                    
                    # 收集目标字段
                    if target_field not in target_fields:
                        target_fields.append(target_field)
            
            if not source_mappings:
                print("  警告: 没有找到需要迁移的字段")
                continue
            
            if not join_conditions:
                print("  警告: 未找到表联合条件")
                continue
            
            print(f"  使用联合条件: {join_conditions}")
            
            # 构建SELECT部分
            select_parts = []
            for source_table, mapping in source_mappings.items():
                for field in mapping['fields']:
                    table_alias = 'a' if source_table == 'dbo.Test1' else 'b'
                    select_parts.append(f"{table_alias}.{field['source_field']}")
            
            # 使用Excel中指定的联合查询条件
            select_query = f"SELECT {', '.join(select_parts)} FROM {join_conditions}"
            print(f"  执行查询: {select_query}")
            
            # 执行联合查询
            rows = source_db.fetch_all(select_query)
            
            if not rows:
                print(f"  警告: 源表联合查询没有返回数据")
                continue
            
            print(f"  从源表读取到 {len(rows)} 条记录")
            
            # 准备插入语句
            insert_query = f"INSERT INTO {sheet.physical_name} ({', '.join(target_fields)}) VALUES ({', '.join(['?' for _ in target_fields])})"
            print(f"  执行插入: {insert_query}")
            
            # 逐行处理数据
            for i, row_data in enumerate(rows, 1):
                # 转换数据
                converted_values = []
                field_index = 0
                
                for source_table, mapping in source_mappings.items():
                    for field in mapping['fields']:
                        value = row_data[field_index]
                        conversion_rule = mapping['type_conversion'][field['source_field']]
                        converted_value = convert_type(value, conversion_rule)
                        converted_values.append(converted_value)
                        field_index += 1
                
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
        print(f"多对一迁移过程中发生错误: {str(e)}")
        import traceback
        traceback.print_exc() 