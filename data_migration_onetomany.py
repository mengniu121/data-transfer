import pandas as pd
from typing import List
from excel_parser import MigrationSheet
from util import convert_type

def execute_one_to_many_migration(excel_path: str, parser, source_db, target_db, sheets: List[MigrationSheet]):
    """
    1対多データ移行の実行
    :param excel_path: Excelファイルパス
    :param parser: Excelパーサーインスタンス
    :param source_db: ソースデータベース接続
    :param target_db: ターゲットデータベース接続
    :param sheets: 移行対象のテーブル設定リスト
    """
    try:
        for sheet in sheets:
            print(f"\nテーブルグループ {sheet.logical_name} の処理:")
            print(f"ソーステーブル: {sheet.source_name}")
            
            # フィールドマッピングシートの読み込み
            df = pd.read_excel(excel_path, sheet_name=sheet.logical_name)
            
            # ターゲットテーブルのフィールドマッピングを保存するための辞書
            target_mappings = {}
            
            # ターゲットテーブルごとにフィールドマッピングを処理
            for _, row in df.iterrows():
                if str(row.get('Transform', '')).upper() == 'Y':
                    target_table = str(row.get('次期DB物理名'))
                    source_field = str(row.get('現行Type物理名'))
                    target_field = str(row.get('次期Type物理名'))
                    
                    # ターゲットテーブルが存在しない場合、マッピング構造を作成
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
            
            # ソーステーブルからデータを取得
            all_source_fields = set()
            for mappings in target_mappings.values():
                all_source_fields.update(mappings['field_mapping'].keys())
            
            if not all_source_fields:
                print("  警告: 移行対象のフィールドが見つかりません")
                continue
            
            select_query = f"SELECT {', '.join(all_source_fields)} FROM {sheet.source_name}"
            print(f"  クエリ実行: {select_query}")
            rows = source_db.fetch_all(select_query)
            
            if not rows:
                print(f"  警告: ソーステーブル {sheet.source_name} にデータがありません")
                continue
            
            print(f"  ソーステーブルから {len(rows)} 件のレコードを読み取りました")
            
            # 各ターゲットテーブルの処理
            for target_table, mappings in target_mappings.items():
                if not mappings['field_mapping']:
                    continue
                    
                print(f"\nターゲットテーブルの処理: {target_table}")
                field_mapping = mappings['field_mapping']
                type_conversion_mapping = mappings['type_conversion_mapping']
                
                # 挿入文の準備
                insert_query = f"INSERT INTO {target_table} ({', '.join(field_mapping.values())}) VALUES ({', '.join(['?' for _ in field_mapping])})"
                print(f"  挿入実行: {insert_query}")
                
                # データの行ごとの処理
                for i, row_data in enumerate(rows, 1):
                    # データの変換
                    converted_values = []
                    for source_field in field_mapping.keys():
                        # ソースフィールドのインデックスを取得
                        source_index = list(all_source_fields).index(source_field)
                        value = row_data[source_index]
                        conversion_rule = type_conversion_mapping.get(source_field)
                        converted_value = convert_type(value, conversion_rule)
                        converted_values.append(converted_value)
                    
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
        print(f"1対多移行中にエラーが発生しました: {str(e)}")
        import traceback
        traceback.print_exc() 