import psycopg2
from dagster import ConfigurableResource
from contextlib import contextmanager

# Définir la ressource pour postgresql (classe recommandée)
# https://docs.dagster.io/guides/build/external-resources/defining-resources
class PostgresResource(ConfigurableResource):
    host: str
    port: int
    user: str
    password: str
    database: str

    # Méthode pour se connecter
    def get_connection(self):
        return psycopg2.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            dbname=self.database,
        )
    
    # Permets de fermer la connexion automatiquement
    @contextmanager
    def get_connection(self):
        conn = psycopg2.connect(  host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            dbname=self.database,
            )
        try:
            yield conn
        finally:
            conn.close()