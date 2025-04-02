import pandas as pd
from typing import List
from excel_parser import MigrationSheet
from util import convert_type

def execute_one_to_one_migration(excel_path: str, parser, source_db, target_db, sheets: List[MigrationSheet]):
    """
    1対1データ移行の実行
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
            print(f"ターゲットテーブル: {sheet.physical_name}")
            
            # フィールドマッピングシートの読み込み
            df = pd.read_excel(excel_path, sheet_name=sheet.logical_name)
            
            # フィールドマッピングの処理
            field_mapping = {}
            type_conversion_mapping = {}
            
            for _, row in df.iterrows():
                if str(row.get('Transform', '')).upper() == 'Y':
                    source_field = str(row.get('現行Type物理名'))
                    target_field = str(row.get('次期Type物理名'))
                    
                    field_mapping[source_field] = target_field
                    type_conversion_mapping[source_field] = {
                        'data_type': str(row.get('データ型', '')),
                        'not_null': str(row.get('Not Null', '')).upper() == 'Y',
                        'default_value': row.get('デフォルト')
                    }
            
            if not field_mapping:
                print("  警告: 移行対象のフィールドが見つかりません")
                continue
            
            # ソーステーブルからデータを取得
            select_query = f"SELECT {', '.join(field_mapping.keys())} FROM {sheet.source_name}"
            print(f"  クエリ実行: {select_query}")
            rows = source_db.fetch_all(select_query)
            
            if not rows:
                print(f"  警告: ソーステーブル {sheet.source_name} にデータがありません")
                continue
            
            print(f"  ソーステーブルから {len(rows)} 件のレコードを読み取りました")
            
            # 挿入文の準備
            insert_query = f"INSERT INTO {sheet.physical_name} ({', '.join(field_mapping.values())}) VALUES ({', '.join(['?' for _ in field_mapping])})"
            print(f"  挿入実行: {insert_query}")
            
            # データの行ごとの処理
            for i, row_data in enumerate(rows, 1):
                # データの変換
                converted_values = []
                for source_field in field_mapping.keys():
                    value = row_data[list(field_mapping.keys()).index(source_field)]
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
        print(f"1対1移行中にエラーが発生しました: {str(e)}")
        import traceback
        traceback.print_exc() 