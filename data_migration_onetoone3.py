import pandas as pd
import json
from typing import List, Dict, Any
from excel_parser import MigrationSheet
import datetime
from util import convert_type
import os
import csv
from pathlib import Path

def process_default_value(default_config: str) -> Any:
    """
    デフォルト値の設定を処理する
    :param default_config: デフォルト値設定のJSON文字列
    :return: 処理後の値
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
                # 一時的に現在の日時を返す、後で関数を追加可能
                return datetime.datetime.now()
            else:
                raise ValueError(f"サポートされていない関数: {value}")
        else:
            return value
    except json.JSONDecodeError:
        return default_config
    except Exception as e:
        print(f"デフォルト値の処理中にエラーが発生しました: {str(e)}")
        return None

def execute_one_to_one_migration(excel_path: str, parser, source_db, target_db, sheets: List[MigrationSheet]):
    """
    一対一のデータ移行を実行する
    :param excel_path: Excelファイルのパス
    :param parser: Excelパーサーインスタンス
    :param source_db: ソースデータベース接続
    :param target_db: ターゲットデータベース接続
    :param sheets: 移行するテーブルの設定リスト
    """
    try:
        # 1回の読み取りデータ数、デフォルトは1000
        batch_size = int(os.getenv('READ_NUM', '1000'))
        print(f"バッチごとの処理データ数: {batch_size}")
        
        # 创建错误日志目录
        error_log_dir = Path("error_logs")
        error_log_dir.mkdir(exist_ok=True)
        
        for sheet in sheets:
            print(f"\nテーブル {sheet.logical_name} を処理中:")
            print(f"ソーステーブル {sheet.source_name} からターゲットテーブル {sheet.physical_name} へ")
            
            # 为每个表创建错误日志文件
            error_log_file = error_log_dir / f"error_log_{sheet.source_name}_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            error_records = []
            
            # フィールドマッピングのシートを読み込む
            df = pd.read_excel(excel_path, sheet_name=sheet.logical_name)
            
            # フィールドマッピングの作成
            select_fields = {}  # SELECT文用のフィールド
            insert_fields = {}  # INSERT文用のフィールド
            merge_fields = {}  # デフォルト値を処理するフィールド
            type_conversion_mapping = {}  # 型変換のマッピング
            
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
                    insert_fields[target_field] = None  # 後で値を埋める
                    # 型変換ルールを追加
                    type_conversion_mapping[source_field] = {
                        'data_type': str(row.get('データ型', '')),
                        'not_null': str(row.get('Not Null', '')).upper() == 'Y',
                        'default_value': row.get('デフォルト')
                    }
                    
                if is_merge and not pd.isna(default_value):
                    merge_fields[target_field] = default_value
            
            if not select_fields:
                print("  警告: クエリするフィールドが見つかりませんでした")
                continue
                
            if not insert_fields:
                print("  警告: 挿入するフィールドが見つかりませんでした")
                continue
            
            # INSERT文の準備
            insert_fields_list = list(insert_fields.keys())
            insert_query = f"INSERT INTO {sheet.physical_name} ({', '.join(insert_fields_list)}) VALUES ({', '.join(['?' for _ in insert_fields_list])})"
            
            # 総レコード数の取得
            count_query = f"SELECT COUNT(*) as total FROM {sheet.source_name}"
            total_count = source_db.fetch_all(count_query)[0][0]
            print(f"  ソーステーブルの総レコード数: {total_count}")
            
            if total_count == 0:
                print(f"  警告: ソーステーブル {sheet.source_name} にデータがありません")
                continue
            
            # バッチ処理
            offset = 0
            processed_count = 0
            batch_count = 0
            error_count = 0
            
            while offset < batch_size: #total_count:
                # ページングクエリを作成
                select_query = f"SELECT [{'], ['.join(select_fields.keys())}] FROM {sheet.source_name} ORDER BY (SELECT NULL) OFFSET {offset} ROWS FETCH NEXT {batch_size} ROWS ONLY"
                print(f"  バッチ {batch_count + 1} 実行中: {select_query}")
                rows = source_db.fetch_all(select_query)
                
                if not rows:
                    break
                
                print(f"  今回のバッチで {len(rows)} 件のレコードを取得しました")
                
                # 各行データを処理
                insert_count = 0
                batch_error_records = []  # 現在のバッチのエラーレコード
                
                for row_data in rows:
                    insert_values = []
                    row_dict = {}  # エラーログの行データ
                    
                    try:
                        for target_field in insert_fields_list:
                            # フィールドにマージ処理が必要な場合
                            if target_field in merge_fields:
                                value = process_default_value(merge_fields[target_field])
                            else:
                                # クエリ結果から対応する値を取得
                                source_field = next((k for k, v in select_fields.items() if v == target_field), None)
                                if source_field:
                                    value = row_data[list(select_fields.keys()).index(source_field)]
                                    # 元の値を記録します
                                    row_dict[source_field] = value
                                    # 型変換を適用
                                    conversion_rule = type_conversion_mapping.get(source_field)
                                    if conversion_rule:
                                        value = convert_type(value, conversion_rule)
                                else:
                                    value = None
                            
                            # 値を文字列形式に変換する
                            if value is None:
                                row_dict[target_field] = ''
                            elif isinstance(value, datetime.datetime):
                                row_dict[target_field] = value.strftime('%Y-%m-%d %H:%M:%S')
                            elif isinstance(value, datetime.date):
                                row_dict[target_field] = value.strftime('%Y-%m-%d')
                            else:
                                row_dict[target_field] = str(value)
                            
                            insert_values.append(value)
                        
                        target_db.execute_query(insert_query, insert_values)
                        insert_count += 1
                        
                    except Exception as e:
                        error_count += 1
                        row_dict['error_message'] = str(e)
                        row_dict['error_time'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        batch_error_records.append(row_dict)
                        print(f"    データの挿入に失敗しました: {str(e)}")
                        # target_db.rollback()
                        continue
                    
                    # 100件のレコードごとに1回送信
                    if insert_count % 100 == 0:
                        target_db.commit()
                        print(f"    {insert_count}/{len(rows)} 件のレコードが挿入されました")
                
                # 残りのトランザクションをコミットする
                target_db.commit()
                
                # エラーデータをログに記録する
                if batch_error_records:
                    error_records.extend(batch_error_records)
                    # 最初のデータバッチの場合は、ヘッダーを書き込む必要があります
                    write_header = not error_log_file.exists()
                    with open(error_log_file, 'a', newline='', encoding='utf-8') as f:
                        writer = csv.DictWriter(f, fieldnames=batch_error_records[0].keys())
                        if write_header:
                            writer.writeheader()
                        writer.writerows(batch_error_records)
                
                processed_count += insert_count
                offset += batch_size
                batch_count += 1
                
                print(f"  バッチ {batch_count} 完了、{insert_count} 件のレコードが正常に挿入されました")
                print(f"  総進捗: {processed_count}/{total_count} ({(processed_count/total_count*100):.2f}%)")
            
            print(f"  移行が完了しました:")
            print(f"    処理済みレコード数: {processed_count}")
            print(f"    エラーレコード数: {error_count}")
            print(f"    バッチ数: {batch_count}")
            if error_count > 0:
                print(f"    エラーログファイル: {error_log_file}")
            
    except Exception as e:
        print(f"一対一移行中にエラーが発生しました: {str(e)}")
        import traceback
        traceback.print_exc()
        raise
