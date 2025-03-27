import pandas as pd
import numpy as np
from typing import Dict, List, Any, Tuple
from dataclasses import dataclass
from enum import Enum

class MigrationType(Enum):
    ONE_TO_ONE = "one_to_one"
    ONE_TO_MANY = "one_to_many"
    MANY_TO_ONE = "many_to_one"

@dataclass
class MigrationSheet:
    """表迁移配置"""
    logical_name: str          # 次期DB論理名
    physical_name: str         # 次期DB物理名
    source_name: str          # 現行DB物理名
    migration_type: MigrationType
    should_migrate: bool      # 是否需要迁移

@dataclass
class TableMapping:
    source_tables: List[str]  # 源表列表
    target_tables: List[str]  # 目标表列表
    migration_type: MigrationType  # 迁移类型

@dataclass
class FieldMapping:
    source_field: str  # 源字段
    target_field: str  # 目标字段
    data_type: str    # 数据类型
    not_null: bool    # 是否非空
    default_value: Any  # 默认值
    transform: str    # 转换规则
    source_table: str  # 源表名
    target_table: str  # 目标表名

@dataclass
class MigrationConfig:
    table_mapping: TableMapping
    field_mappings: List[FieldMapping]

class ExcelParser:
    def __init__(self, excel_path: str):
        """
        初始化Excel解析器
        :param excel_path: Excel文件路径
        """
        self.excel_path = excel_path
        self.migration_sheets: Dict[str, MigrationSheet] = {}  # 使用逻辑名称作为key

    def parse_mapping_sheet(self) -> Dict[str, MigrationSheet]:
        """
        解析マッピング一覧sheet，获取需要迁移的表信息
        """
        try:
            # 读取マッピング一覧sheet
            df = pd.read_excel(self.excel_path, sheet_name="マッピング一覧")
            
            # 遍历每一行，解析迁移配置
            for _, row in df.iterrows():
                logical_name = row.get('次期DB論理名')
                if pd.notna(logical_name):
                    migration_type_str = row.get('MigrationType', 'one_to_one')  # 默认为一对一
                    try:
                        migration_type = MigrationType(migration_type_str.lower())
                    except ValueError:
                        print(f"警告：未知的迁移类型 {migration_type_str}，使用默认值 one_to_one")
                        migration_type = MigrationType.ONE_TO_ONE

                    sheet = MigrationSheet(
                        logical_name=str(logical_name),
                        physical_name=str(row.get('次期DB物理名', '')),
                        source_name=str(row.get('現行DB物理名', '')),
                        migration_type=migration_type,
                        should_migrate=str(row.get('移行', '')).upper() == 'Y'
                    )
                    
                    self.migration_sheets[logical_name] = sheet
            
            return self.migration_sheets
            
        except Exception as e:
            print(f"解析マッピング一覧sheet时发生错误: {str(e)}")
            import traceback
            traceback.print_exc()
            return {}

    def get_sheet_info(self, logical_name: str) -> MigrationSheet:
        """
        获取指定逻辑名称的表配置信息
        """
        return self.migration_sheets.get(logical_name)

    def get_migration_sheets(self) -> List[MigrationSheet]:
        """
        获取所有需要迁移的表配置
        """
        return [sheet for sheet in self.migration_sheets.values() if sheet.should_migrate]

    def get_table_mapping(self, sheet_name: str) -> pd.DataFrame:
        """
        获取表的字段映射关系
        :param sheet_name: sheet名称
        :return: 字段映射DataFrame
        """
        try:
            df = pd.read_excel(self.excel_path, sheet_name)
            # 只保留必要的列
            required_columns = [
                '次期DB物理名', '次期Type物理名', '次期Typeデータ型',
                '次期TypeNot Null', '次期Typeデフォルト', 'Transform',
                '現行DB物理名', '現行Type物理名'
            ]
            return df[required_columns]
        except Exception as e:
            raise ValueError(f"读取sheet {sheet_name} 失败: {str(e)}")

    def validate_data_type(self, value: Any, target_type: str) -> tuple[bool, Any]:
        """
        验证并转换数据类型
        :param value: 原始值
        :param target_type: 目标数据类型
        :return: (是否有效, 转换后的值)
        """
        if pd.isna(value):
            return True, None

        try:
            if 'nvarchar' in target_type or 'varchar' in target_type:
                return True, str(value)
            elif target_type == 'int':
                return True, int(float(value))
            elif target_type == 'decimal':
                return True, float(value)
            elif target_type == 'date':
                if isinstance(value, str):
                    # 尝试解析日期字符串
                    return True, pd.to_datetime(value)
                return True, value
            else:
                return True, value
        except Exception:
            return False, None

    def process_table_data(self, sheet_name: str, source_data: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
        """
        处理表数据
        :param sheet_name: sheet名称
        :param source_data: 源数据DataFrame
        :return: (有效数据DataFrame, 无效数据DataFrame)
        """
        mapping = self.get_table_mapping(sheet_name)
        valid_data = []
        invalid_data = []

        for _, row in mapping.iterrows():
            source_column = row['現行Type物理名']
            target_column = row['次期Type物理名']
            data_type = row['次期Typeデータ型']
            not_null = row['次期TypeNot Null'] == 'Y'
            default_value = row['次期Typeデフォルト']

            if source_column not in source_data.columns:
                continue

            for idx, value in source_data[source_column].items():
                is_valid, converted_value = self.validate_data_type(value, data_type)

                if not is_valid or (not_null and converted_value is None and pd.isna(default_value)):
                    invalid_data.append({
                        '表名': sheet_name,
                        '源字段': source_column,
                        '目标字段': target_column,
                        '原始值': value,
                        '目标类型': data_type,
                        '行号': idx + 1
                    })
                    continue

                if pd.isna(converted_value) and not pd.isna(default_value):
                    converted_value = default_value

                valid_data.append({
                    '目标字段': target_column,
                    '值': converted_value,
                    '行号': idx + 1
                })

        valid_df = pd.DataFrame(valid_data)
        invalid_df = pd.DataFrame(invalid_data)
        
        return valid_df, invalid_df

    def save_invalid_data(self, invalid_df: pd.DataFrame, output_path: str):
        """
        保存无效数据到CSV文件
        :param invalid_df: 无效数据DataFrame
        :param output_path: 输出文件路径
        """
        if not invalid_df.empty:
            invalid_df.to_csv(output_path, index=False, encoding='utf-8-sig')

    def get_migration_table_list(self) -> List[str]:
        """
        从Excel文件中读取需要迁移的表名列表
        
        Returns:
            List[str]: 需要迁移的表名列表
        """
        try:
            # 读取マッピング一覧sheet
            df = pd.read_excel(self.excel_path, sheet_name="マッピング一覧")
            
            # 初始化结果列表
            migration_tables = []
            
            # 遍历每一行数据
            for index, row in df.iterrows():
                migration_flag = row.get('移行')
                next_db_name = row.get('次期DB論理名')
                
                # 如果移行标志为Y，则添加到列表中
                if pd.notna(migration_flag) and str(migration_flag).upper() == 'Y':
                    migration_tables.append(str(next_db_name))
            
            return migration_tables
            
        except Exception as e:
            print(f"读取Excel文件时发生错误: {str(e)}")
            import traceback
            traceback.print_exc()
            return []

    def determine_migration_type(self, source_tables: List[str], target_tables: List[str]) -> MigrationType:
        """
        根据源表和目标表的数量确定迁移类型
        """
        if len(source_tables) == 1 and len(target_tables) == 1:
            return MigrationType.ONE_TO_ONE
        elif len(source_tables) == 1 and len(target_tables) > 1:
            return MigrationType.ONE_TO_MANY
        elif len(source_tables) > 1 and len(target_tables) == 1:
            return MigrationType.MANY_TO_ONE
        else:
            raise ValueError(f"不支持的迁移类型: 源表数量={len(source_tables)}, 目标表数量={len(target_tables)}")

    def get_migration_configs(self) -> List[MigrationConfig]:
        """
        获取所有迁移配置
        返回包含表映射和字段映射的配置列表
        """
        try:
            # 读取マッピング一覧sheet
            df = pd.read_excel(self.excel_path, sheet_name="マッピング一覧")
            
            # 获取需要迁移的配置
            migration_configs = []
            current_source_tables = []
            current_target_tables = []
            
            for _, row in df.iterrows():
                migration_flag = row.get('移行')
                if pd.notna(migration_flag) and str(migration_flag).upper() == 'Y':
                    source_table = row.get('現行DB物理名')
                    target_table = row.get('次期DB物理名')
                    
                    if pd.notna(source_table) and pd.notna(target_table):
                        # 检查是否是新的迁移组
                        if not current_source_tables or source_table not in current_source_tables:
                            # 如果有之前的配置，先保存
                            if current_source_tables:
                                migration_type = self.determine_migration_type(current_source_tables, current_target_tables)
                                table_mapping = TableMapping(
                                    source_tables=current_source_tables.copy(),
                                    target_tables=current_target_tables.copy(),
                                    migration_type=migration_type
                                )
                                field_mappings = self.get_field_mappings_for_tables(current_source_tables, current_target_tables)
                                migration_configs.append(MigrationConfig(table_mapping, field_mappings))
                            
                            # 开始新的配置
                            current_source_tables = [source_table]
                            current_target_tables = [target_table]
                        else:
                            # 继续当前配置
                            if target_table not in current_target_tables:
                                current_target_tables.append(target_table)
            
            # 保存最后一个配置
            if current_source_tables:
                migration_type = self.determine_migration_type(current_source_tables, current_target_tables)
                table_mapping = TableMapping(
                    source_tables=current_source_tables.copy(),
                    target_tables=current_target_tables.copy(),
                    migration_type=migration_type
                )
                field_mappings = self.get_field_mappings_for_tables(current_source_tables, current_target_tables)
                migration_configs.append(MigrationConfig(table_mapping, field_mappings))
            
            return migration_configs
            
        except Exception as e:
            print(f"获取迁移配置时发生错误: {str(e)}")
            import traceback
            traceback.print_exc()
            return []

    def get_field_mappings_for_tables(self, source_tables: List[str], target_tables: List[str]) -> List[FieldMapping]:
        """
        获取指定表之间的字段映射关系
        """
        field_mappings = []
        
        for target_table in target_tables:
            # 读取目标表的sheet
            df = pd.read_excel(self.excel_path, sheet_name=target_table)
            
            for _, row in df.iterrows():
                source_field = row.get('現行Type物理名')
                target_field = row.get('次期Type物理名')
                source_table = row.get('現行DB物理名')
                
                if pd.notna(source_field) and pd.notna(target_field) and source_table in source_tables:
                    field_mapping = FieldMapping(
                        source_field=str(source_field),
                        target_field=str(target_field),
                        data_type=str(row.get('データ型', '')),
                        not_null=row.get('Not Null', '') == 'Y',
                        default_value=row.get('デフォルト', None),
                        transform=row.get('Transform', ''),
                        source_table=str(source_table),
                        target_table=str(target_table)
                    )
                    field_mappings.append(field_mapping)
        
        return field_mappings

    def get_field_mappings(self, sheet_name: str) -> Dict[str, Dict]:
        """
        获取字段映射关系，包括数据类型和其他属性
        
        Args:
            sheet_name (str): sheet名称
            
        Returns:
            Dict[str, Dict]: 字段映射信息，格式为：
            {
                '目标字段名': {
                    'source_field': '源字段名',
                    'data_type': '数据类型',
                    'not_null': 是否非空,
                    'default_value': '默认值',
                    'transform': '转换规则'
                }
            }
        """
        try:
            # 读取Excel数据
            df = pd.read_excel(self.excel_path, sheet_name=sheet_name)
            
            # 打印列名，用于调试
            print("可用的列名：", df.columns.tolist())
            
            mappings = {}
            
            # 遍历每一行数据
            for _, row in df.iterrows():
                # 获取字段名
                target_field = row.get('次期Type物理名', None)  # 使用次期Type物理名作为目标字段
                source_field = row.get('現行Type物理名', None)  # 使用現行Type物理名作为源字段
                
                if pd.notna(target_field) and pd.notna(source_field):
                    mappings[str(target_field)] = {
                        'source_field': str(source_field),
                        'data_type': str(row.get('データ型', '')),  # 使用データ型列
                        'not_null': row.get('Not Null', '') == 'Y',  # 使用Not Null列
                        'default_value': row.get('デフォルト', None),  # 使用デフォルト列
                        'transform': row.get('Transform', '')
                    }
            
            return mappings
            
        except Exception as e:
            print(f"获取字段映射关系时发生错误: {str(e)}")
            import traceback
            traceback.print_exc()
            return {}

    def process_one_to_one_migration(self, source_table: str, target_table: str, sheet_name: str) -> Tuple[List[str], List[str], List[str], List[Dict]]:
        """
        处理一对一迁移的数据转换
        
        Args:
            source_table (str): 源表名
            target_table (str): 目标表名
            sheet_name (str): Excel中的sheet名称
            
        Returns:
            Tuple[List[str], List[str], List[str], List[Dict]]: 
            - target_fields: 目标字段列表
            - source_fields: 源字段列表
            - data_types: 数据类型列表
            - validation_rules: 验证规则列表
        """
        try:
            # 读取字段映射sheet
            df = pd.read_excel(self.excel_path, sheet_name=sheet_name)
            
            target_fields = []
            source_fields = []
            data_types = []
            validation_rules = []
            
            # 遍历每一行
            for _, row in df.iterrows():
                # 检查Transform标志
                transform = str(row.get('Transform', '')).upper()
                if transform != 'Y':
                    continue
                    
                source_field = row.get('現行Type物理名')
                target_field = row.get('次期Type物理名')
                data_type = row.get('データ型')
                not_null = str(row.get('Not Null', '')).upper() == 'Y'
                default_value = row.get('デフォルト')
                
                if pd.notna(source_field) and pd.notna(target_field):
                    target_fields.append(str(target_field))
                    source_fields.append(str(source_field))
                    data_types.append(str(data_type))
                    
                    # 构建验证规则
                    rule = {
                        'field': str(target_field),
                        'not_null': not_null,
                        'data_type': str(data_type),
                        'default_value': default_value if pd.notna(default_value) else None
                    }
                    validation_rules.append(rule)
            
            return target_fields, source_fields, data_types, validation_rules
            
        except Exception as e:
            print(f"处理一对一迁移时发生错误: {str(e)}")
            import traceback
            traceback.print_exc()
            return [], [], [], []

    def generate_one_to_one_sql(self, source_table: str, target_table: str, sheet_name: str) -> str:
        """
        生成一对一迁移的SQL语句，使用db_connector.py中的数据库连接方式
        
        Args:
            source_table (str): 源表名
            target_table (str): 目标表名
            sheet_name (str): Excel中的sheet名称
            
        Returns:
            str: 生成的SQL语句
        """
        target_fields, source_fields, data_types, validation_rules = self.process_one_to_one_migration(
            source_table, target_table, sheet_name
        )
        
        if not target_fields or not source_fields:
            return ""
            
        # 构建字段转换表达式
        field_expressions = []
        for i, (source_field, target_field, data_type) in enumerate(zip(source_fields, target_fields, data_types)):
            expression = self.generate_field_conversion(source_field, data_type, validation_rules[i])
            field_expressions.append(f"{expression} AS {target_field}")
        
        # 生成SQL语句
        sql = f"-- 数据迁移SQL\n"
        sql += f"-- 请在目标数据库上执行此SQL\n"
        sql += f"BEGIN TRY\n"
        sql += f"    BEGIN TRANSACTION;\n\n"
        
        # 使用db_connector.py中的连接方式
        sql += f"    -- 从源数据库读取数据并插入到目标数据库\n"
        sql += f"    INSERT INTO {target_table} ({', '.join(target_fields)})\n"
        sql += f"    SELECT {', '.join(field_expressions)}\n"
        sql += f"    FROM [{source_table}] AS source_data;\n\n"
        
        # 添加事务提交和错误处理
        sql += f"    COMMIT TRANSACTION;\n"
        sql += f"    PRINT '数据迁移成功完成';\n"
        sql += f"END TRY\n"
        sql += f"BEGIN CATCH\n"
        sql += f"    IF @@TRANCOUNT > 0\n"
        sql += f"        ROLLBACK TRANSACTION;\n"
        sql += f"    DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();\n"
        sql += f"    DECLARE @ErrorSeverity INT = ERROR_SEVERITY();\n"
        sql += f"    DECLARE @ErrorState INT = ERROR_STATE();\n\n"
        sql += f"    RAISERROR (@ErrorMessage, @ErrorSeverity, @ErrorState);\n"
        sql += f"END CATCH\n"
        
        return sql

    def generate_field_conversion(self, field: str, data_type: str, rule: Dict) -> str:
        """
        生成字段转换表达式
        
        Args:
            field (str): 源字段名
            data_type (str): 目标数据类型
            rule (Dict): 验证规则
            
        Returns:
            str: 字段转换表达式
        """
        expression = field
        
        # 处理NULL值
        if rule['not_null']:
            if rule['default_value'] is not None:
                expression = f"COALESCE({field}, {rule['default_value']})"
            else:
                expression = f"COALESCE({field}, '')"  # 或其他适当的默认值
        
        # 数据类型转换
        if 'varchar' in data_type.lower() or 'nvarchar' in data_type.lower():
            # 字符串类型，检查长度
            length = int(''.join(filter(str.isdigit, data_type)))
            expression = f"CAST(LEFT({expression}, {length}) AS {data_type})"
        elif 'decimal' in data_type.lower():
            # 处理decimal类型
            precision = data_type.split('(')[1].split(')')[0]
            expression = f"CAST({expression} AS decimal({precision}))"
        elif data_type.lower() == 'date':
            expression = f"CAST({expression} AS date)"
        elif data_type.lower() == 'int':
            expression = f"CAST({expression} AS int)"
            
        return expression 