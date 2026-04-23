{{
    config(
        materialized='table',
    )
}}

WITH cleaned_companies AS (
    SELECT
        siren,
        TRIM(nom_raison_sociale) AS nom_raison_sociale,
        activite_principale,
        categorie_entreprise,
        date_creation,
        etat_administratif,
        nature_juridique,
        tranche_effectif_salarie,
        TRIM(adresse) AS adresse,
        code_postal,
        TRIM(commune) AS commune,
        latitude,
        longitude
    FROM {{ source('codes_nafs', 'companies') }}
    WHERE 
        siren IS NOT NULL
        AND nom_raison_sociale IS NOT NULL
        AND etat_administratif = 'A'  -- Garder que les Actives
)

SELECT * FROM cleaned_companies