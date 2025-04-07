import pandas as pd
import json
from typing import List, Dict, Any
import datetime
from pathlib import Path
from util import convert_type
import os
import glob
from dotenv import load_dotenv
from db_connector import DatabaseConnector
import sys
from excel_parser import ExcelParser, MigrationSheet

# エラーログディレクトリを定義
ERROR_LOG_DIR = Path("error_logs")

def process_default_value(default_config: str) -> Any:
    """
    デフォルト値設定を処理する
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
                return datetime.datetime.now()
            else:
                raise ValueError(f"サポートされていない関数: {value}")
        else:
            return value
    except json.JSONDecodeError:
        return default_config
    except Exception as e:
        print(f"デフォルト値処理中にエラーが発生しました: {str(e)}")
        return None

def get_error_files(source_table: str = None) -> List[Path]:
    """
    エラーログファイルリストを取得する
    :param source_table: ソーステーブル名（オプション）
    :return: エラーログファイルのパスリスト
    """
    if not ERROR_LOG_DIR.exists():
        return []
    
    if source_table:
        pattern = f"error_log_{source_table}_*.csv"
    else:
        pattern = "error_log_*.csv"
    
    return sorted(ERROR_LOG_DIR.glob(pattern), key=lambda x: x.stat().st_mtime, reverse=True)

def recover_data(excel_path: str, target_db, error_file: Path, sheet_name: str, target_sheet: MigrationSheet):
    """
    エラーログファイルからデータを復旧する
    :param excel_path: Excelマッピングファイルのパス
    :param target_db: ターゲットデータベース接続
    :param error_file: エラーログファイルのパス
    :param sheet_name: 対応するシート名
    """
    try:
        print(f"\nエラーデータファイルの処理を開始: {error_file}")
        
        # エラーデータの読み込み
        error_df = pd.read_csv(error_file, encoding='utf-8')  # shift-jisエンコードでファイルを読み込み
        total_errors = len(error_df)
        print(f"エラー記録は {total_errors} 件です")
        
        if total_errors == 0:
            return
        
        # フィールドマッピング設定を読み込む
        mapping_df = pd.read_excel(excel_path, sheet_name=sheet_name)
        
        # フィールドマッピングの作成
        select_fields = {}  # SELECT文用のフィールド
        insert_fields = {}  # INSERT文用のフィールド
        merge_fields = {}  # デフォルト値を処理するフィールド
        type_conversion_mapping = {}  # 型変換のマッピング
        
        for _, row in mapping_df.iterrows():
            target_field = str(row.get('次期Type物理名'))
            source_field = str(row.get('現行Type物理名'))
            is_select = str(row.get('Select', '')).upper() == 'Y'
            is_transform = str(row.get('Transform', '')).upper() == 'Y'
            is_merge = str(row.get('Merge', '')).upper() == 'Y'
            default_value = row.get('デフォルト')

            if is_transform:
                select_fields[target_field] = source_field

            if is_transform:
                insert_fields[target_field] = None
                type_conversion_mapping[source_field] = {
                    'data_type': str(row.get('データ型', '')),
                    'not_null': str(row.get('Not Null', '')).upper() == 'Y',
                    'default_value': row.get('デフォルト')
                }
                
            if is_merge and not pd.isna(default_value):
                merge_fields[target_field] = default_value
        
        # INSERT文の準備
        target_table = target_sheet.physical_name  # ファイル名からターゲットテーブル名を取得
        insert_fields_list = list(insert_fields.keys())
        insert_query = f"INSERT INTO {target_table} ({', '.join(insert_fields_list)}) VALUES ({', '.join(['?' for _ in insert_fields_list])})"
        
        # 各エラー記録を処理
        success_count = 0
        new_error_records = []
        
        for index, row in error_df.iterrows():
            try:
                insert_values = []
                row_dict = {}
                
                for target_field in insert_fields_list:
                    if target_field in merge_fields:
                        value = process_default_value(merge_fields[target_field])
                    else:
                        # エラーレコードから元の値を取得
                        source_field = select_fields[target_field]
                        if source_field:
                            value = row[source_field]
                            row_dict[source_field] = value
                            # 型変換を適用
                            conversion_rule = type_conversion_mapping.get(source_field)
                            if conversion_rule:
                                value = convert_type(value, conversion_rule)
                        else:
                            value = None
                    
                    insert_values.append(value)
                    row_dict[target_field] = value
                
                target_db.execute_query(insert_query, insert_values)
                success_count += 1
                
                # 100件ごとにコミット
                if success_count % 100 == 0:
                    target_db.commit()
                    print(f"成功した処理: {success_count}/{total_errors} 件")
                
            except Exception as e:
                row_dict['error_message'] = str(e)
                row_dict['error_time'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                new_error_records.append(row_dict)
                print(f"レコード {index + 1} の処理に失敗しました: {str(e)}")
                continue
        
        # 残りのトランザクションをコミット
        target_db.commit()
        
        # 新しいエラーレコードがあれば、新しいエラーログファイルに保存
        if new_error_records:
            new_error_file = ERROR_LOG_DIR / f"error_log_{target_table}_recover_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            pd.DataFrame(new_error_records).to_csv(new_error_file, index=False)
            print(f"新しいエラーレコードは以下に保存されました: {new_error_file}")
        
        print(f"\n復旧処理が完了しました: ")
        print(f"  総レコード数: {total_errors}")
        print(f"  成功した処理: {success_count}")
        print(f"  新しいエラー数: {len(new_error_records)}")
        
    except Exception as e:
        print(f"エラーデータ処理中に例外が発生しました: {str(e)}")
        import traceback
        traceback.print_exc()
        raise

def main():
    """
    メイン関数
    """
    try:
        # 環境変数を読み込む
        load_dotenv()
        
        # データベース接続の作成
        print("\nデータベースに接続中...")
        target_db = DatabaseConnector(is_source=False)
        
        # データベース接続の確認
        if target_db is None:
            raise Exception("ターゲットデータベース接続に失敗しました")
            
        print("データベース接続に成功しました")

        # コマンドライン引数のチェック
        # if len(sys.argv) != 2:
        #     print("使用方法: python main3.py <マッピング一覧名称>")
        #     sys.exit(1)
        
        # マッピング名パラメータの取得
        # mapping_name = sys.argv[1]
        mapping_name="dbo.AccountingDetailTbl"
        # Excelパーサーの作成
        excel_path = "数据移行2.xlsx"
        parser = ExcelParser(excel_path)
        
        # 対応するシートの取得
        target_sheet = parser.parse_mapping_data_to_run(mapping_name)
        
        if not target_sheet:
            print(f"エラー: マッピング名 '{mapping_name}' に対応する設定が見つかりません")
            return
            
        # 対応するエラーログファイルの取得
        error_files = get_error_files(target_sheet.source_name)
        if not error_files:
            print(f"テーブル '{target_sheet.source_name}' のエラーログファイルが見つかりません")
            return
            
        print("以下のエラーログファイルが見つかりました:")
        for i, file in enumerate(error_files, 1):
            print(f"{i}. {file.name} ({datetime.datetime.fromtimestamp(file.stat().st_mtime).strftime('%Y-%m-%d %H:%M:%S')})")
        
        # ユーザーにファイル選択を促す
        while True:
            try:
                choice = int(input("\n処理するファイル番号を選択してください（終了するには0を入力）: "))
                if choice == 0:
                    return
                if 1 <= choice <= len(error_files):
                    break
                print("無効な選択です。もう一度やり直してください")
            except ValueError:
                print("有効な数字を入力してください")
        
        error_file = error_files[choice - 1]
        
        # エラーデータを処理する
        recover_data(excel_path, target_db, error_file, target_sheet.logical_name, target_sheet)
        
    except Exception as e:
        print(f"プログラム実行中にエラーが発生しました: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        if 'target_db' in locals():
            target_db.close()  

if __name__ == "__main__":
    main()
