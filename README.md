# 4DATA : création d'un flux de données pour les entreprises informatiques françaises
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

Pour les branches, veuillez respecter la syntaxe suivante :
- feat/fix/docs:fct (exemple : git branch "feat:create-assets-metiers")

Pour les commits, veuillez respecter la syntaxe suivante :
- INFO/FIX/DEL/MODIFY: explication fct (exemple : git commit -m "INFO: ajouter collecte des données de l'API pour les coordonnées géographiques")

Avec cette syntaxe, cela nous permettra de bien comprendre ce qui a été développée.

# Sources de données pour les pipelines

Pour nos différentes pipelines, nous utiliserons deux principales sources de données :
- API sur l'annuaire des entreprises françaises mise en place par le gouvernement [data.gouv.fr](https://recherche-entreprises.api.gouv.fr/docs/)
- Un fichier [Excel](https://www.insee.fr/fr/information/2120875) sur les intitulés des codes NAF (Nomenclature d'Activités Françaises) afin d'identifier les entreprises dans le domaine informatique

# Partie ETL pour les codes NAF 
Pour ce qui est des codes NAF, il a été décider pour simplifier le travail et avec l'exécution qu'une fois par an de ce processus de créer un ETL.
Cette ETL permet d'aller chercher le contenu d'un fichier Excel pour capturer tous les intitulés des codes NAFs, leurs intitulés, les intitulés des classes et des sections.
Ca permettra notamment de se simplifier la tâche dans le filtrage des activités de type informatique.


À compléter plus tards avec explication de comment ça va se passer