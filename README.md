# 4data-project
## Auteur : Julien RENOULT - Tom JOUSSET - Béatrice BEAVOGUI
## Promo : SUPINFO Programme Grande École 4ème année
### Spécialité : Ingénierie Data
### Date : 10/04/2026

Vous trouverez ci-joint le projet dans la création d'une pipeline de données pour capter les informations des entreprises informatiques françaises.
On utilisera différentes technologies notamment :
- [dagster](https://dagster.io/)
- [dbt](https://www.getdbt.com/)
- [PostgreSQL](https://www.postgresql.org/)
- [Power BI](https://www.microsoft.com/fr-fr/power-platform/products/power-bi)

Pour lancer la phase de dev de ce projet sous Docker, veuillez exécuter la commande ci-dessous :
```sh

docker compose -f docker-compose-dev.yaml up --build

```

ou si vous l'avez déjà build et que vous voulez l'arrêter ou la relancer :

```sh

docker compose -f docker-compose-dev.yaml down

# Avec suppression des volumes
docker compose -f docker-compose-dev.yaml down -v

# Relancer
docker compose -f docker-compose-dev.yaml up

```

À compléter plus tards avec explication de comment ça va se passer