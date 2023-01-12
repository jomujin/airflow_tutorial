import gc
import pandas as pd
import numpy as np
from cond import (
    DB_PATH,
    KB_RAW_COMPLEX_TABLE,
    KB_RAW_PYTYPE_TABLE,
    KB_RAW_PRICE_TABLE,
    KB_COMPLEX_PNU_MAP_TABLE,
    KB_PY_INFO_TABLE
)
from ...py.helper.kb_py_info_helper import (
    get_type_code,
    get_is_not_provide,
    get_pyeong_type,
    get_10000_price
)


def create_kb_py_info_table(kb_base_wk):

    table_name = KB_PY_INFO_TABLE

    kb_raw_complex = pd.read_csv(
        f'{DB_PATH}/{KB_RAW_COMPLEX_TABLE}_{kb_base_wk}.csv',
        encoding='utf-8'
    )

    kb_raw_peongtype = pd.read_csv(
        f'{DB_PATH}/{KB_RAW_PYTYPE_TABLE}_{kb_base_wk}.csv',
        encoding='utf-8'
    )

    kb_raw_price = pd.read_csv(
        f'{DB_PATH}/{KB_RAW_PRICE_TABLE}_{kb_base_wk}.csv',
        encoding='utf-8'
    )

    kb_raw_complex_pnu_map = pd.read_csv(
        f'{DB_PATH}/{KB_COMPLEX_PNU_MAP_TABLE}_{kb_base_wk}.csv',
        encoding='utf-8'
    )

    print(f"""Succeed loading sub table, table name : 
        {KB_RAW_COMPLEX_TABLE}_{kb_base_wk},
        {KB_RAW_PYTYPE_TABLE}_{kb_base_wk},
        {KB_RAW_PRICE_TABLE}_{kb_base_wk},
        {KB_COMPLEX_PNU_MAP_TABLE}_{kb_base_wk},
        """)
        
    kb_raw_complex = kb_raw_complex.rename(columns={
        'complex_id':'kb_complex_code',
        'is_price_unexposed':'is_price_unexposed_complex'
    })
    kb_raw_peongtype = kb_raw_peongtype.rename(columns={
        'complex_id':'kb_complex_code',
        'is_price_unexposed':'is_price_unexposed_py'
    })
    kb_raw_price = kb_raw_price.rename(columns={
        'complex_id':'kb_complex_code'
    })
    kb_raw_peongtype['kb_type_code'] = kb_raw_peongtype[[
        'kb_complex_code', 
        'py_serial_no'
        ]].apply(lambda x: get_type_code(*x), axis=1)
    kb_raw_price['kb_type_code'] = kb_raw_price[[
        'kb_complex_code', 
        'py_serial_no'
        ]].apply(lambda x: get_type_code(*x), axis=1)

    merge_py_typ = pd.merge(
        left=kb_raw_peongtype[[
            'kb_complex_code', # 연결
            'kb_type_code', # 연결 complex_id + 평형순번
            'jy_ar',
            'supply_ar',
            'etc_jy_ar',
            'py',
            'py_typ',
            'rm_cnt',
            'brm_cnt',
            'direction',
            'ent_strc',
            'is_price_unexposed_py'
        ]],
        right=kb_raw_price[[
            'kb_type_code', # 연결 complex_id + 주택형순번
            'base_ymd',
            'deal_price',
            'lower_deal_price',
            'upper_deal_price',
            'jeonse_price',
            'lower_jeonse_price',
            'upper_jeonse_price',
            'mth_rent_dep',
            'lower_mth_rent_dep',
            'upper_mth_rent_dep',
            'agent_nm_1',
            'agent_tel_1',
            'agent_nm_2',
            'agent_tel_2',
            'deadline_ymd'
        ]],
        how='outer',
        on='kb_type_code'
    )

    merge_all = pd.merge(
        left=kb_raw_complex[[
            'kb_complex_code', # 연결
            'complex_nm',
            'const_ym',
            'move_in_ym',
            'is_price_unexposed_complex' # 단지 시세미노출여부
        ]],
        right=merge_py_typ,
        how='outer',
        on='kb_complex_code'
    )

    merge_all['is_price_unexposed'] = merge_all[[
        'is_price_unexposed_complex', 
        'is_price_unexposed_py'
        ]].apply(lambda x: get_is_not_provide(*x), axis=1)

    merge_all = merge_all.rename(columns={
        'complex_id':'kb_complex_code',
        'complex_nm':'kb_complex_name',
        'const_ym':'kb_approval_ym',
        'move_in_ym':'kb_occupancy_ym',
        'jy_ar':'kb_area_jy',
        'supply_ar':'kb_area_contract',
        'etc_jy_ar':'kb_etc_area_jy',
        'py':'kb_pyeong_typ_main',
        'py_typ':'kb_pyeong_typ_sub',
        'rm_cnt':'kb_rm_cnt',
        'brm_cnt':'kb_brm_cnt',
        'direction':'kb_dir',
        'ent_strc':'kb_ent_strc',
        'deal_price':'kb_price',
        'lower_deal_price':'kb_lower_price',
        'upper_deal_price':'kb_upper_price',
        'jeonse_price':'kb_jeonse_price',
        'lower_jeonse_price':'kb_lower_jeonse_price',
        'upper_jeonse_price':'kb_upper_jeonse_price',
        'mth_rent_dep':'kb_mth_rent_deposit',
        'lower_mth_rent_dep':'kb_lower_mth_rent_price',
        'upper_mth_rent_dep':'kb_upper_mth_rent_price',
        'base_ymd':'kb_base_dt',
        'deadline_ymd':'kb_limit_dt',
        'is_price_unexposed':'kb_is_not_prov'
    })

    merge_all['kb_pyeong_typ'] = merge_all[[
        'kb_pyeong_typ_main', 
        'kb_pyeong_typ_sub'
        ]].apply(lambda x: get_pyeong_type(*x), axis=1)

    merge_all['kb_price'] = merge_all['kb_price'].apply(lambda x: get_10000_price(x))
    merge_all['kb_lower_price'] = merge_all['kb_lower_price'].apply(lambda x: get_10000_price(x))
    merge_all['kb_upper_price'] = merge_all['kb_upper_price'].apply(lambda x: get_10000_price(x))
    merge_all['kb_jeonse_price'] = merge_all['kb_jeonse_price'].apply(lambda x: get_10000_price(x))
    merge_all['kb_lower_jeonse_price'] = merge_all['kb_lower_jeonse_price'].apply(lambda x: get_10000_price(x))
    merge_all['kb_upper_jeonse_price'] = merge_all['kb_upper_jeonse_price'].apply(lambda x: get_10000_price(x))
    merge_all['kb_mth_rent_deposit'] = merge_all['kb_mth_rent_deposit'].apply(lambda x: get_10000_price(x))
    merge_all['kb_lower_mth_rent_price'] = merge_all['kb_lower_mth_rent_price'].apply(lambda x: get_10000_price(x))
    merge_all['kb_upper_mth_rent_price'] = merge_all['kb_upper_mth_rent_price'].apply(lambda x: get_10000_price(x))

    save_cols = [
        # 'asset_pnu', 
        # 'pnu',
        'kb_complex_code', 
        'kb_complex_name',
        'kb_approval_ym', 
        'kb_occupancy_ym', 
        'kb_type_code', 
        'kb_area_jy',
        'kb_area_contract', 
        'kb_etc_area_jy', 
        'kb_pyeong_typ', 
        'kb_pyeong_typ_main',
        'kb_pyeong_typ_sub',
        'kb_rm_cnt',
        'kb_brm_cnt', 
        'kb_dir', 
        'kb_ent_strc', 
        'kb_price', 
        'kb_lower_price', 
        'kb_upper_price', 
        'kb_jeonse_price',
        'kb_lower_jeonse_price', 
        'kb_upper_jeonse_price', 
        'kb_mth_rent_deposit',
        'kb_lower_mth_rent_price', 
        'kb_upper_mth_rent_price', 
        'kb_base_dt',
        'kb_limit_dt', 
        'kb_is_not_prov'
    ]
    
    merge_all = merge_all[save_cols]
    merge_all['kb_base_wk'] = kb_base_wk
    merge_all = merge_all.replace('', np.nan)

    # 저장
    merge_all.to_csv(f'{DB_PATH}/{table_name}_{kb_base_wk}.csv',
        encoding='utf-8',
        index=False)
    print(f'Succeed creating table, table name : {table_name}')

    del_df_list = [
        kb_raw_complex_pnu_map,
        kb_raw_complex,
        kb_raw_peongtype,
        kb_raw_price,
        merge_py_typ,
        merge_all
    ]

    del del_df_list
    gc.collect()


