import requests
from dagster import asset
from .constants import API_ENTREPRISES_URL
import os
import pandas as pd
import time
from ..resources import PostgresResource
from io import StringIO

@asset(
    deps=["load_naf_codes"],
    group_name="companies"
)
def extract_load_companies(context, postgres: PostgresResource):
    # Récupérer la liste des codes NAF informatiques depuis la base
    with postgres.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            Select distinct subclasses_codes 
            From code_naf 
            Where classes_codes in ('62', '63')
        """)
        naf_codes = [row[0] for row in cursor.fetchall()]

    # Préparer la table
    with postgres.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            Drop table if exists companies;
            Create table companies (
                siren Text,
                nom_raison_sociale Text,
                activite_principale Text,
                categorie_entreprise Text,
                date_creation Text,
                etat_administratif Text,
                nature_juridique Text,
                tranche_effectif_salarie Text,
                adresse Text,
                code_postal Text,
                commune Text,
                latitude Float,
                longitude Float
            );
        """)
        conn.commit()
    all_companies = []
    request_count = 0

    for naf_code in naf_codes:        
        # Première requête pour obtenir le nombre total de pages
        params = {
            'activite_principale': naf_code,
            'est_entrepreneur_individuel': 'false',
            'per_page': 25,
            'page': 1
        }
        
        response = requests.get(API_ENTREPRISES_URL, params=params)
        request_count += 1
        
        if response.status_code != 200:
            context.log.error(f"Erreur API pour {naf_code}: {response.status_code}")
            continue
        
        data = response.json()
        total_pages = data.get('total_pages', 1)
        process_page(data, all_companies)
        
        # Traiter les pages suivantes si existantes
        for page in range(2, total_pages + 1):
            if request_count % 7 == 0:
                time.sleep(1) # sleep pour eviter spam 7 req
            
            params['page'] = page
            response = requests.get(API_ENTREPRISES_URL, params=params)
            request_count += 1
            
            if response.status_code == 200:
                data = response.json()
                process_page(data, all_companies)
            else:
                context.log.error(f"Erreur page {page} pour {naf_code} : {response.status_code}")
        
        # Pause entre codes NAF
        time.sleep(1)
    
    # Charger en base
    if all_companies:
        df = pd.DataFrame(all_companies)
        
        with postgres.get_connection() as conn:
            cursor = conn.cursor()
            
            buffer = StringIO()
            df.to_csv(buffer, index=False, header=False)
            buffer.seek(0)
            
            cursor.copy_expert(
                sql="""
                    Copy companies (
                        siren,
                        nom_raison_sociale,
                        activite_principale,
                        categorie_entreprise,
                        date_creation,
                        etat_administratif,
                        nature_juridique,
                        tranche_effectif_salarie,
                        adresse,
                        code_postal,
                        commune,
                        latitude,
                        longitude
                    )
                    from stdin with csv
                """,
                file=buffer
            )
            conn.commit()
    else:
        context.log.warning("Aucune entreprise trouvé")


def process_page(data, companies_list):
    for result in data.get('results', []):
        unite_legale = result.get('unite_legale', {})
        siege = result.get('siege', {})
        
        company = {
            'siren': unite_legale.get('siren'),
            'nom_raison_sociale': unite_legale.get('denomination') or unite_legale.get('nom') or unite_legale.get('prenom'),
            'activite_principale': unite_legale.get('activite_principale'),
            'categorie_entreprise': unite_legale.get('categorie_entreprise'),
            'date_creation': unite_legale.get('date_creation'),
            'etat_administratif': unite_legale.get('etat_administratif'),
            'nature_juridique': unite_legale.get('nature_juridique'),
            'tranche_effectif_salarie': unite_legale.get('tranche_effectif_salarie'),
            'adresse': siege.get('adresse'),
            'code_postal': siege.get('code_postal'),
            'commune': siege.get('commune'),
            'latitude': siege.get('latitude'),
            'longitude': siege.get('longitude')
        }
        
        companies_list.append(company)