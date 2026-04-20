import pytest
import pandas as pd
from src.dagster_informatics_companies.defs.assets.code_naf import get_naf_codes, clean_naf_codes, load_naf_codes
import dagster as dg
import os
from src.dagster_informatics_companies.defs.resources import PostgresResource

##################################################-
# FIXTURES POUR NOS TESTS
##################################################-
@pytest.fixture
def collecte_naf_codes():
    return get_naf_codes(context = dg.build_asset_context())

@pytest.fixture
def transform_naf_codes():
    return clean_naf_codes(context= dg.build_asset_context())

@pytest.fixture
def context_postgresql():
    return dg.build_op_context(
        resources={
            "postgres": PostgresResource(
            host=os.getenv("HOST_DB"),
            port=os.getenv("PORT_DB"),
            user=os.getenv("USER_DB"),
            password=os.getenv("PASSWORD_DB"),
            database=os.getenv("NAME_DB"),
            )
        }
    )

##################################################-
# TESTS UNITAIRES POUR LA COLLECTE DE DONNEES 
##################################################-

def test_extension_fichier(collecte_naf_codes):
    """
    Voir si on enregistre bien un fichier xls et pas une autre extension
    """
    try:

        # Chemin du fichier après exécution
        chemin = collecte_naf_codes

        # Vérifier extension du fichier Excel
        assert chemin.endswith(".xls")
        
    except Exception as e:
        raise AssertionError("Erreur 'get_naf_codes' pour le test de lecture : ", e)

def test_contenu_fichier(collecte_naf_codes):
    """
    Voir si on a bien du contenu dans ce fichier xls
    """
    try:

        # Chemin du fichier après exécution
        chemin = collecte_naf_codes

        # Lecture du fichier de données
        excel_data = pd.read_excel(chemin)
        
        assert len(excel_data) > 0

    except Exception as e:
        raise AssertionError("Erreur 'get_naf_codes' pour le test de contenu : ", e)
    
def test_entete_fichier(collecte_naf_codes):
    """
    Vérifie que l'en-tête du fichier 'xls' correspond bien à ce qu'on doit s'attendre
    """

    try:

        # Chemin du fichier après exécution
        chemin = collecte_naf_codes

        # Lecture du fichier de données
        en_tete_excel = list(pd.read_excel(chemin).columns)
        
        print("l'en-tête du fichier Excel : ", en_tete_excel)
        assert len(en_tete_excel) >= 3 # Les trois colonnes minimums pour nos transformations
        assert en_tete_excel[0:3] == ["ligne", "Code", " Intitulés de la  NAF rév. 2, version finale "]

    except Exception as e:
        raise AssertionError("Erreur 'get_naf_codes' pour le test d'en-tête : ", e)

##################################################-
# TESTS UNITAIRES POUR LA TRANSFORMATION DE DONNEES 
##################################################-

def test_pas_absence_data(transform_naf_codes):
    """
    Vérifie qu'il n'y a pas de données nulles
    """

    try:
        chemin = transform_naf_codes

        data_test = pd.read_csv(chemin, sep=";", header=0)

        assert any(data_test.isna())
    
    except Exception as e:
        raise AssertionError("Erreur 'clean_naf_codes' pour le test de non absence de données : ", e)

def test_pas_doublons_code_naf(transform_naf_codes):
    """
    Vérifie qu'il n'y a pas de doublons dans les codes naf
    """

    try:
        chemin = transform_naf_codes

        data_test = pd.read_csv(chemin, sep=";", header=0)

        assert data_test["subclasses_codes"].is_unique # Retourne vrai si chaque valeur est unique
    
    except Exception as e:
        raise AssertionError("Erreur 'clean_naf_codes' pour le test de pas de doublons code naf : ", e)

def test_entete_code_naf_cleaned(transform_naf_codes):

    """
    Vérifie que l'en-tête est bien celle qu'on veut
    """

    try:
        chemin = transform_naf_codes

        data_test = list(pd.read_csv(chemin, sep=";", header=0).columns)

        assert data_test == ["excel_line", "subclasses_codes", 
                             "subclasses_title", "classes_codes",
                             "classe_title", "section_title"]
    
    except Exception as e:
        raise AssertionError("Erreur 'clean_naf_codes' pour le test de l'entête : ", e)
    
############################################################-
# TESTS UNITAIRES POUR LE CHARGEMENT DES DONNEES 
############################################################-

def test_entete_code_naf_codes(context_postgresql):
    """
    Vérifie que la table PostgreSQL a la bonne structure
    """

    try:
        # Exécuter l'asset
        load_naf_codes(context=context_postgresql)

        # Accéder à la ressource via le contexte
        with context_postgresql.resources.postgres.get_connection() as conn:
            cursor = conn.cursor()

            cursor.execute("""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = 'public'
                AND table_name = 'code_naf'
                ORDER BY ordinal_position;
            """)

            noms_types_colonnes = [row for row in cursor]

        expected = [
            ("excel_line", 'integer'),
            ("subclasses_codes", 'text'),
            ("subclasses_title", 'text'),
            ("classes_codes", 'text'),
            ("classe_title", 'text'),
            ("section_title", 'text')
        ]

        #print("Les noms et types des colonnes : ", noms_types_colonnes)

        assert noms_types_colonnes == expected
    
    except Exception as e:
        raise AssertionError("Erreur 'load_naf_codes' pour le test de l'en-tête : ", e)

def test_contenu_db_code_naf_codes(context_postgresql):
    """
    Vérifie que la table PostgreSQL a la bonne structure
    """

    try:
        # Exécuter l'asset
        load_naf_codes(context=context_postgresql)

        # Accéder à la ressource via le contexte
        with context_postgresql.resources.postgres.get_connection() as conn:
            cursor = conn.cursor()

            cursor.execute("""
                SELECT COUNT(excel_line)
                FROM code_naf
            """)

            count = [row for row in cursor]
            #print("Nombre de lignes : ", count)
            assert count[0][0] > 0

    except Exception as e:
        raise AssertionError("Erreur 'load_naf_codes' pour le test du contenu dans la bdd : ", e)

