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
    """表の移行設定"""
    logical_name: str          # 次期DB論理名
    physical_name: str         # 次期DB物理名
    source_name: str          # 現行DB物理名
    migration_type: MigrationType

@dataclass
class TableMapping:
    source_tables: List[str]  # ソーステーブルリスト
    target_tables: List[str]  # ターゲットテーブルリスト
    migration_type: MigrationType  # 移行タイプ

@dataclass
class FieldMapping:
    source_field: str  # ソースフィールド
    target_field: str  # ターゲットフィールド
    data_type: str    # データ型
    not_null: bool    # NULL許可
    default_value: Any  # デフォルト値
    transform: str    # 変換ルール
    source_table: str  # ソーステーブル名
    target_table: str  # ターゲットテーブル名

@dataclass
class MigrationConfig:
    table_mapping: TableMapping
    field_mappings: List[FieldMapping]

class ExcelParser:
    def __init__(self, excel_path: str):
        """
        Excelパーサーの初期化
        :param excel_path: Excelファイルパス
        """
        self.excel_path = excel_path
        self.migration_sheets: Dict[str, MigrationSheet] = {}  # 論理名をキーとして使用

    def parse_mapping_data_to_run(self, mapping_name: str) -> MigrationSheet:
        """
        指定されたマッピング名に基づいて移行設定を解析
        :param mapping_name: マッピング一覧の値
        :return: 移行設定オブジェクト
        """
        try:
            # マッピング一覧シートの読み込み
            df = pd.read_excel(self.excel_path, sheet_name='マッピング一覧')
            
            # 指定されたマッピング名の検索
            row = df[df['次期DB物理名'] == mapping_name]
            if row.empty:
                raise ValueError(f"マッピング名が見つかりません: {mapping_name}")
            
            # 最初の一致行の取得
            row = row.iloc[0]
            
            # 必須フィールドのチェック
            if pd.isna(row['次期DB物理名']) or pd.isna(row['現行DB物理名']):
                raise ValueError(f"マッピング設定が不完全です: {mapping_name}")
            
            # 移行タイプの確定
            migration_type_str = str(row.get('MigrationType', '')).lower()
            if migration_type_str == 'one_to_one':
                migration_type = MigrationType.ONE_TO_ONE
            elif migration_type_str == 'one_to_many':
                migration_type = MigrationType.ONE_TO_MANY
            elif migration_type_str == 'many_to_one':
                migration_type = MigrationType.MANY_TO_ONE
            else:
                raise ValueError(f"サポートされていない移行タイプです: {migration_type_str}")
            
            # MigrationSheetオブジェクトの作成と返却
            return MigrationSheet(
                logical_name=str(row['次期DB論理名']),
                physical_name=str(row['次期DB物理名']),
                source_name=str(row['現行DB物理名']),
                migration_type=migration_type
            )  
        except Exception as e:
            print(f"マッピングデータの解析中にエラーが発生しました: {str(e)}")
            raise

    def parse_mapping_sheet(self) -> Dict[str, MigrationSheet]:
        """
        マッピング一覧シートの解析
        :return: 移行設定オブジェクトのリスト
        """
        try:
            # マッピング一覧シートの読み込み
            df = pd.read_excel(self.excel_path, sheet_name='マッピング一覧')
            # 各行の処理
            for _, row in df.iterrows():
                logical_name = row.get('次期DB論理名')
                # 移行対象のチェック
                if str(row.get('移行', '')).upper() != 'Y':
                    continue
                
                # 必須フィールドのチェック
                if pd.isna(row['次期DB物理名']) or pd.isna(row['現行DB物理名']):
                    continue
                
                # 移行タイプの確定
                migration_type_str = str(row.get('MigrationType', '')).lower()
                if migration_type_str == 'one_to_one':
                    migration_type = MigrationType.ONE_TO_ONE
                elif migration_type_str == 'one_to_many':
                    migration_type = MigrationType.ONE_TO_MANY
                elif migration_type_str == 'many_to_one':
                    migration_type = MigrationType.MANY_TO_ONE
                else:
                    print(f"サポートされていない移行タイプです: {migration_type_str}")
                    continue
                
                # MigrationSheetオブジェクトの作成
                sheet = MigrationSheet(
                    logical_name=logical_name,
                    physical_name=str(row['次期DB物理名']),
                    source_name=str(row['現行DB物理名']),
                    migration_type=migration_type
                )
                
                self.migration_sheets[logical_name] = sheet
            
            return self.migration_sheets
            
        except Exception as e:
            print(f"マッピング一覧シートの解析中にエラーが発生しました: {str(e)}")
            return {}

    def get_sheet_info(self, logical_name: str) -> MigrationSheet:
        """
        指定された論理名のテーブル設定情報を取得
        """
        return self.migration_sheets.get(logical_name)

    def get_migration_sheets(self) -> List[MigrationSheet]:
        """
        移行対象の全テーブル設定を取得
        """
        return [sheet for sheet in self.migration_sheets.values()]

    def get_table_mapping(self, sheet_name: str) -> pd.DataFrame:
        """
        テーブルのフィールドマッピング関係を取得
        :param sheet_name: シート名
        :return: フィールドマッピングのDataFrame
        """
        try:
            df = pd.read_excel(self.excel_path, sheet_name)
            # 必要な列のみを保持
            required_columns = [
                '次期DB物理名', '次期Type物理名', '次期Typeデータ型',
                '次期TypeNot Null', '次期Typeデフォルト', 'Transform',
                '現行DB物理名', '現行Type物理名'
            ]
            return df[required_columns]
        except Exception as e:
            raise ValueError(f"シート {sheet_name} の読み込みに失敗しました: {str(e)}")

    def validate_data_type(self, value: Any, target_type: str) -> tuple[bool, Any]:
        """
        データ型の検証と変換
        :param value: 元の値
        :param target_type: ターゲットデータ型
        :return: (有効かどうか, 変換後の値)
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
                    # 日付文字列の解析を試みる
                    return True, pd.to_datetime(value)
                return True, value
            else:
                return True, value
        except Exception:
            return False, None

    def process_table_data(self, sheet_name: str, source_data: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
        """
        テーブルデータの処理
        :param sheet_name: シート名
        :param source_data: ソースデータのDataFrame
        :return: (有効データのDataFrame, 無効データのDataFrame)
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
        無効データをCSVファイルに保存
        :param invalid_df: 無効データのDataFrame
        :param output_path: 出力ファイルパス
        """
        if not invalid_df.empty:
            invalid_df.to_csv(output_path, index=False, encoding='utf-8-sig')



    