import pandas as pd
import re

def convert_type(value, conversion_rule):
    """
    データ型変換ルールに基づいて、値の型を変換します。
    Parameters:
        value : 任意の型  
            入力された元の値
        conversion_rule : dict  
            以下のキーを含む辞書:
                - 'data_type': 変換したいデータ型（例: 'int', 'decimal(5,2)', 'date', 'datetime'など）
                - 'default_value': 変換できなかったときのデフォルト値（オプション）

    Returns:
        変換後の値、またはエラー時は default_value または None
    """
    # if value is None:
    #     if conversion_rule.get('not_null', False):
    #         return conversion_rule.get('default_value', '')
    #     return None
        
    try:
        data_type = conversion_rule.get('data_type', '').lower()

        # None や NaN の場合はそのまま None を返す（DBのNULLとして扱う）
        if value is None or pd.isna(value):
            return None
        
        # 文字列型（varchar, nvarchar）の処理
        if 'varchar' in data_type or 'nvarchar' in data_type:
            if value == True:
                value = '1'
            elif value == False:
                value = '0'
            return str(value)
        
        # 整数型の処理 
        elif data_type == 'int' or data_type == 'tinyint':
            return int(float(value)) # 文字列やfloatをintに変換（例: "5.0" → 5）
        
        # 小数型（decimal）の処理
        elif 'decimal' in data_type:
            if re.match(r'decimal\(\d+,\s*0\)', data_type.lower()):
                return int(float(value))  # ← 小数なし → int に変換
            else:
                return float(value)  # 小数あり → float でOK
        
        # 日付型の処理（date）
        elif data_type == 'date':
            date_str = str(value).strip()
            if date_str in ['', '00000000', '000', '//', 'nan']:
                return None
            
            # 例: 20240105 → 2024-01-05
            if len(date_str) == 8 and date_str.isdigit():
                year = date_str[:4]
                month = date_str[4:6]
                day = date_str[6:8]
                date_str = f"{year}-{month}-{day}"
            # 例: 202401 → 2024-01-01
            elif len(date_str) == 6 and date_str.isdigit():
                year = date_str[:4]
                month = date_str[4:6]
                date_str = f"{year}-{month}-01"
            # 和暦（R050101など）対応
            elif len(date_str) == 7:
                era_map = {
                    'R': 2018,  # 令和元年 = 2019年 → 2018 + 1 = 2019
                    'H': 1988,  # 平成元年 = 1989年
                    'S': 1925   # 昭和元年 = 1926年
                }
                era = date_str[0]
                if era in era_map:
                    year = int(date_str[1:3])
                    month = int(date_str[3:5])
                    day = int(date_str[5:7])
                    western_year = era_map[era] + year
                    date_str = f"{western_year}-{month}-{day}"
            
            date_obj = pd.to_datetime(date_str, errors='coerce')
            if pd.isna(date_obj):
                return None
            else:
                return date_obj.strftime('%Y-%m-%d')
        
        # 日時型（datetime）の処理
        elif data_type == 'datetime' or data_type == 'datetime2':
            if value=='':
                return None
            date_str = str(value).strip()
            # try:
            #     # フォーマット1: "08  3 2018  6:49PM"
            #     return pd.to_datetime(date_str, format="%d %m %Y %I:%M%p").strftime('%Y-%m-%d %H:%M:%S')
            # except ValueError:
            #     pass  # フォーマット1で失敗した場合は、次に進む
            
            try:
                # フォーマット1: "10 27 2015  5:36PM"
                return pd.to_datetime(date_str, format="%m %d %Y %I:%M%p").strftime('%Y-%m-%d %H:%M:%S')
            except ValueError:
                pass  # フォーマット1で失敗した場合は、次に進む
                           
            try:
                # フォーマット2: "2009082722312"
                if len(date_str) == 13 and date_str.isdigit():
                    year = date_str[:4]
                    month = date_str[4:6]
                    day = date_str[6:8]
                    return f"{year}-{month}-{day}"               
            except ValueError:
                pass  # フォーマット2で失敗した場合は、次に進む
            
            try:
                # フォーマット3: '2022/03/23 07:33:05'
                return pd.to_datetime(date_str, format='%Y/%m/%d %H:%M:%S').strftime('%Y-%m-%d %H:%M:%S')
            except ValueError:
                pass  # フォーマット3で失敗した場合は、次に進む

            try:
                # フォーマット4: '20180403'
                return pd.to_datetime(date_str, format='%Y%m%d').strftime('%Y-%m-%d')
            except ValueError:
                pass  # フォーマット4で失敗した場合は、次に進む

            date_obj = pd.to_datetime(date_str, errors='coerce')
            if pd.isna(date_obj):
                return None
            else:
                return date_obj.strftime('%Y-%m-%d %H:%M:%S')
        
        else:
            return value
    except Exception as e:
        print(f"データ型変換エラー: {str(e)}")
        return conversion_rule.get('default_value', None) 