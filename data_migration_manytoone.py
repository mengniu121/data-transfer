import pandas as pd
from typing import List
from excel_parser import MigrationSheet
from util import convert_type

def execute_many_to_one_migration(excel_path: str, parser, source_db, target_db, sheets: List[MigrationSheet]):
    """
    多対1データ移行の実行
    :param excel_path: Excelファイルパス
    :param parser: Excelパーサーインスタンス
    :param source_db: ソースデータベース接続
    :param target_db: ターゲットデータベース接続
    :param sheets: 移行対象のテーブル設定リスト
    """
    try:
        for sheet in sheets:
            print(f"\nテーブルグループ {sheet.logical_name} の処理:")
            print(f"ターゲットテーブル: {sheet.physical_name}")
            
            # フィールドマッピングシートの読み込み
            df = pd.read_excel(excel_path, sheet_name=sheet.logical_name)
            
            # ソーステーブルのフィールドマッピングを保存するための辞書
            source_mappings = {}
            target_fields = []
            join_conditions = None
            
            # フィールドマッピングの処理
            for _, row in df.iterrows():
                # 結合条件の取得
                if pd.notna(row.get('Union')):
                    join_conditions = str(row.get('Union')).strip()
                    
                if str(row.get('Transform', '')).upper() == 'Y':
                    source_table = str(row.get('現行DB物理名'))
                    source_field = str(row.get('現行Type物理名'))
                    target_field = str(row.get('次期Type物理名'))
                    
                    # ソーステーブルが存在しない場合、マッピング構造を作成
                    if source_table not in source_mappings:
                        source_mappings[source_table] = {
                            'fields': [],
                            'type_conversion': {}
                        }
                    
                    # フィールドマッピングの追加
                    source_mappings[source_table]['fields'].append({
                        'source_field': source_field,
                        'target_field': target_field
                    })
                    
                    # 型変換ルールの追加
                    source_mappings[source_table]['type_conversion'][source_field] = {
                        'data_type': str(row.get('データ型', '')),
                        'not_null': str(row.get('Not Null', '')).upper() == 'Y',
                        'default_value': row.get('デフォルト')
                    }
                    
                    # ターゲットフィールドの収集
                    if target_field not in target_fields:
                        target_fields.append(target_field)
            
            if not source_mappings:
                print("  警告: 移行対象のフィールドが見つかりません")
                continue
            
            if not join_conditions:
                print("  警告: テーブル結合条件が見つかりません")
                continue
            
            print(f"  結合条件: {join_conditions}")
            
            # SELECT部分の構築
            select_parts = []
            for source_table, mapping in source_mappings.items():
                for field in mapping['fields']:
                    table_alias = 'a' if source_table == 'dbo.Test1' else 'b'
                    select_parts.append(f"{table_alias}.{field['source_field']}")
            
            # Excelで指定された結合クエリ条件の使用
            select_query = f"SELECT {', '.join(select_parts)} FROM {join_conditions}"
            print(f"  クエリ実行: {select_query}")
            
            # 結合クエリの実行
            rows = source_db.fetch_all(select_query)
            
            if not rows:
                print(f"  警告: ソーステーブルの結合クエリでデータが返されませんでした")
                continue
            
            print(f"  ソーステーブルから {len(rows)} 件のレコードを読み取りました")
            
            # 挿入文の準備
            insert_query = f"INSERT INTO {sheet.physical_name} ({', '.join(target_fields)}) VALUES ({', '.join(['?' for _ in target_fields])})"
            print(f"  挿入実行: {insert_query}")
            
            # データの行ごとの処理
            for i, row_data in enumerate(rows, 1):
                # データの変換
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
                    # 挿入の実行
                    target_db.execute_query(insert_query, converted_values)
                    
                    # 100件ごとにトランザクションをコミット
                    if i % 100 == 0:
                        target_db.commit()
                        print(f"  {i}/{len(rows)} 件のレコードを処理しました")
                except Exception as e:
                    target_db.rollback()
                    print(f"  挿入エラー: {str(e)}")
                    continue
            
            # 残りのトランザクションのコミット
            target_db.commit()
            print(f"  移行が完了しました。合計 {len(rows)} 件のレコードを処理しました")
            
    except Exception as e:
        print(f"多対1移行中にエラーが発生しました: {str(e)}")
        import traceback
        traceback.print_exc() 