import pandas as pd
from typing import List
from excel_parser import MigrationSheet
from util import convert_type

def generate_test_data(target_db, table_name: str, count: int = 10000):
    """
    生成测试数据
    :param target_db: 目标数据库连接
    :param table_name: 目标表名
    :param count: 需要生成的数据条数
    """
    try:
        print(f"\n开始生成测试数据，目标表: {table_name}")
        print(f"计划生成 {count} 条数据")
        
        # 准备插入语句
        insert_query = f"""
        INSERT INTO {table_name} (
            [INST_TYPE_ID], [INST_TYPE_SEQ], [INST_ID], [EDA_NO], [DATA_KBN], 
            [PROCESS_KBN], [PROCESS_YMD], [PROCESS_TTL_AMOUNT], [PROCESS_P_AMOUNT], 
            [PROCESS_I_AMOUNT], [MISHU_JIYU], [BIKO], [SEIKYU_MEISAI_NUM], 
            [KEIJO_YM], [KEIRI_YM], [OHTOH_YM], [OHTOH_S_NUM], [OHTOH_E_NUM], 
            [NYUKIN_MEISAI_NUM], [CREATE_D], [CREATE_OP], [CREATE_PG], 
            [UPDATE_D], [UPDATE_OP], [UPDATE_PG]
        ) VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )
        """
        
        # 批量插入数据
        batch_size = 1000
        for i in range(0, count, batch_size):
            current_batch_size = min(batch_size, count - i)
            values_list = []
            
            for j in range(current_batch_size):
                inst_type_id = str(i + j + 1)  # 从1开始递增
                values = [
                    inst_type_id,  # INST_TYPE_ID
                    '1',           # INST_TYPE_SEQ
                    '1',           # INST_ID
                    '1',           # EDA_NO
                    '1',           # DATA_KBN
                    '1',           # PROCESS_KBN
                    '20250321',    # PROCESS_YMD
                    1,             # PROCESS_TTL_AMOUNT
                    1,             # PROCESS_P_AMOUNT
                    1,             # PROCESS_I_AMOUNT
                    '1',           # MISHU_JIYU
                    '1',           # BIKO
                    '1',           # SEIKYU_MEISAI_NUM
                    '1',           # KEIJO_YM
                    '1',           # KEIRI_YM
                    '1',           # OHTOH_YM
                    1,             # OHTOH_S_NUM
                    1,             # OHTOH_E_NUM
                    '1',           # NYUKIN_MEISAI_NUM
                    '202503',      # CREATE_D
                    '1',           # CREATE_OP
                    '1',           # CREATE_PG
                    '202503',      # UPDATE_D
                    '1',           # UPDATE_OP
                    '1'            # UPDATE_PG
                ]
                values_list.append(values)
            
            try:
                # 执行批量插入
                target_db.executemany(insert_query, values_list)
                target_db.commit()
                print(f"  已插入 {i + current_batch_size}/{count} 条数据")
            except Exception as e:
                target_db.rollback()
                print(f"  插入失败: {str(e)}")
                continue
        
        print(f"测试数据生成完成，共插入 {count} 条数据")
        
    except Exception as e:
        print(f"生成测试数据时发生错误: {str(e)}")
        import traceback
        traceback.print_exc()
            
    except Exception as e:
        print(f"多对一迁移过程中发生错误: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    # 这里可以添加测试代码
    pass 