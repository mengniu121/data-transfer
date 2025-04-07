import pandas as pd
import json
from typing import List, Dict, Any
from excel_parser import MigrationSheet
import datetime
from util import convert_type

def process_default_value(default_config: str) -> Any:
    """
    处理默认值配置
    :param default_config: 默认值配置的JSON字符串
    :return: 处理后的值
    """
    if not default_config or pd.isna(default_config):
        return None
        
    try:
        config = json.loads(default_config)
        value_type = config.get('type', '').lower()
        value = config.get('value')
        
        if value_type == 'nvarchar':
            return str(value)
        elif value_type == 'decimal':
            return float(value)
        elif value_type == 'function':
            if value == 'now()':
                # 暂时返回当前时间，后续可以扩展更多函数
                return datetime.datetime.now()
            else:
                raise ValueError(f"Unsupported function: {value}")
        else:
            return value
    except json.JSONDecodeError:
        return default_config
    except Exception as e:
        print(f"Error processing default value: {str(e)}")
        return None

def execute_one_to_one_migration(excel_path: str, parser, source_db, target_db, sheets: List[MigrationSheet]):
    """
    执行一对一数据迁移
    :param excel_path: Excel文件路径
    :param parser: Excel解析器实例
    :param source_db: 源数据库连接
    :param target_db: 目标数据库连接
    :param sheets: 需要迁移的表配置列表
    """
    try:
        for sheet in sheets:
            print(f"\n处理表 {sheet.logical_name}:")
            print(f"从 {sheet.source_name} 迁移到 {sheet.physical_name}")
            
            # 读取字段映射sheet
            df = pd.read_excel(excel_path, sheet_name=sheet.logical_name)
            
            # 构建字段映射
            select_fields = {}  # 用于SELECT语句的字段
            insert_fields = {}  # 用于INSERT语句的字段
            merge_fields = {}  # 需要处理默认值的字段
            type_conversion_mapping = {}  # 用于类型转换的映射
            
            for _, row in df.iterrows():
                target_field = str(row.get('次期Type物理名'))
                source_field = str(row.get('現行Type物理名'))
                is_select = str(row.get('Select', '')).upper() == 'Y'
                is_transform = str(row.get('Transform', '')).upper() == 'Y'
                is_merge = str(row.get('Merge', '')).upper() == 'Y'
                default_value = row.get('デフォルト')
                
                if is_select:
                    select_fields[source_field] = target_field
                
                if is_transform:
                    insert_fields[target_field] = None  # 值稍后填充
                    # 添加类型转换规则
                    type_conversion_mapping[source_field] = {
                        'data_type': str(row.get('データ型', '')),
                        'not_null': str(row.get('Not Null', '')).upper() == 'Y',
                        'default_value': row.get('デフォルト')
                    }
                    
                if is_merge and not pd.isna(default_value):
                    merge_fields[target_field] = default_value
            
            if not select_fields:
                print("  警告: 没有找到需要查询的字段")
                continue
                
            if not insert_fields:
                print("  警告: 没有找到需要插入的字段")
                continue
            
            # 构建并执行SELECT语句
            select_query = f"SELECT {', '.join(select_fields.keys())} FROM {sheet.source_name}"
            print(f"  执行查询: {select_query}")
            rows = source_db.fetch_all(select_query)
            
            if not rows:
                print(f"  警告: 源表 {sheet.source_name} 没有数据")
                continue
            
            print(f"  从源表读取到 {len(rows)} 条记录")
            
            # 准备INSERT语句
            insert_fields_list = list(insert_fields.keys())
            insert_query = f"INSERT INTO {sheet.physical_name} ({', '.join(insert_fields_list)}) VALUES ({', '.join(['?' for _ in insert_fields_list])})"
            print(f"  执行插入: {insert_query}")
            
            # 处理每一行数据
            for row_data in rows:
                insert_values = []
                
                for target_field in insert_fields_list:
                    # 如果字段需要合并处理
                    if target_field in merge_fields:
                        value = process_default_value(merge_fields[target_field])
                    else:
                        # 从查询结果中获取对应的值
                        source_field = next((k for k, v in select_fields.items() if v == target_field), None)
                        if source_field:
                            value = row_data[list(select_fields.keys()).index(source_field)]
                            # 应用类型转换
                            conversion_rule = type_conversion_mapping.get(source_field)
                            if conversion_rule:
                                value = convert_type(value, conversion_rule)
                        else:
                            value = None
                    
                    insert_values.append(value)
                
                try:
                    target_db.execute_query(insert_query, insert_values)
                except Exception as e:
                    print(f"  插入失败: {str(e)}")
                    target_db.rollback()
                    continue
            
            target_db.commit()
            print(f"  迁移成功完成，共处理 {len(rows)} 条记录")
            
    except Exception as e:
        print(f"一对一迁移过程中发生错误: {str(e)}")
        import traceback
        traceback.print_exc()
        raise 