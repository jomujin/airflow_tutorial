import numpy as np
from .common_helper import (
    isnan
)


def get_type_code(물건식별자, 평형순번):
    return f'{물건식별자}_{평형순번}'

def get_is_not_provide(단지시세미노출여부, 평형시세미노출여부):
    # 노출 : 0
    # 미노출 : 1
    if (단지시세미노출여부 == '1') or (평형시세미노출여부 == '1'):
        return '1'
    else:
        return '0'

def get_pyeong_type(kb_pyeong_typ_main, kb_pyeong_typ_sub):
    if isnan(kb_pyeong_typ_main)==False:
        kb_pyeong_typ_main = str(kb_pyeong_typ_main)
        kb_pyeong_typ_sub = str(kb_pyeong_typ_sub)
        kb_pyeong_typ = kb_pyeong_typ_main + kb_pyeong_typ_sub
        return kb_pyeong_typ
    else:
        return np.nan

def get_10000_price(price):
    try:
        return float(price) * 10000
    except:
        return np.nan

