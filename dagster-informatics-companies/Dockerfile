# Image Python
FROM python:3.11-slim

# Variables d'environnement
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Dépendances système (dbt + psycopg2)
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    libpq-dev \
    git \
    && rm -rf /var/lib/apt/lists/*

# Dossier de travail
WORKDIR /app

# Copier les fichiers de dépendances
COPY dagster-informatics-companies/pyproject.toml dagster-informatics-companies/uv.lock ./

# Installer uv (rapide)
RUN pip install uv

# Installer les dépendances Python
RUN uv sync

# Copier le code Dagster
COPY dagster-informatics-companies/src/ ./src/

# Copier la config dagster.yaml 
COPY dagster-informatics-companies/dagster.yaml ./dagster.yaml

# Copier le code pour les tests
COPY dagster-informatics-companies/tests/ ./tests/

# Copier dbt (important)
COPY dbt_informatics_companies/ /dbt

# Variable dbt
ENV DBT_PROFILES_DIR=/dbt

# Exposer port Dagster
EXPOSE 3000

# Commande de dev
CMD ["uv", "run", "dg", "dev", "-h", "0.0.0.0", "-p", "3000"]