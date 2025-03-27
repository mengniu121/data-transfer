import pandas as pd

def convert_type(value, conversion_rule):
    """
    根据转换规则转换数据类型
    """
    if value is None:
        if conversion_rule.get('not_null', False):
            return conversion_rule.get('default_value', '')
        return None
        
    try:
        data_type = conversion_rule.get('data_type', '').lower()
        if 'varchar' in data_type or 'nvarchar' in data_type:
            return str(value)
        elif data_type == 'int':
            return int(float(value))
        elif 'decimal' in data_type:
            return float(value)
        elif data_type == 'date':
            if value is None:
                return None
            date_str = str(value).strip()
            if len(date_str) == 8 and date_str.isdigit():
                year = date_str[:4]
                month = date_str[4:6]
                day = date_str[6:8]
                return f"{year}-{month}-{day}"
            elif len(date_str) == 6 and date_str.isdigit():
                year = date_str[:4]
                month = date_str[4:6]
                return f"{year}-{month}-01"
            else:
                try:
                    return pd.to_datetime(date_str).strftime('%Y-%m-%d')
                except:
                    print(f"警告: 无法解析日期格式: {date_str}")
                    return conversion_rule.get('default_value', None)
        else:
            return value
    except Exception as e:
        print(f"数据类型转换错误: {str(e)}")
        return conversion_rule.get('default_value', None) 