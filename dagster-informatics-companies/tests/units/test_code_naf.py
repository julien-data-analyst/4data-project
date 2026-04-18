import pytest
import pandas as pd
from src.dagster_informatics_companies.defs.assets.code_naf import get_naf_codes, clean_naf_codes, load_naf_codes
import dagster as dg

def test_lecture_code_naf():
    """
    Voir si on peut lire le fichier Excel de la requête et qu'il y a du contenu
    """
    try:

        # Voir contenu
        excel_data = pd.read_excel(get_naf_codes(context = dg.build_asset_context()).contents)
        if len(excel_data) > 0:
            lu_et_contenu = True
    except Exception as e:
        lu_et_contenu = False
    
    assert lu_et_contenu

