import psycopg2
import psycopg2.extras
from .config import Config
from .logger import logger

class Database:
    def __init__(self):
        self.config = Config()
        
    def get_connection(self):
        """Establish database connection"""
        try:
            conn = psycopg2.connect(
                host=self.config.DB_HOST,
                port=self.config.DB_PORT,
                database=self.config.DB_NAME,
                user=self.config.DB_USER,
                password=self.config.DB_PASSWORD
            )
            logger.info("Database connection established")
            return conn
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            raise

    def check_schema_exists(self, conn):
        """Check if target schema exists"""
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT EXISTS(SELECT 1 FROM information_schema.schemata WHERE schema_name = %s);",
                    (self.config.TARGET_SCHEMA,)
                )
                return cursor.fetchone()[0]
        except Exception as e:
            logger.error(f"Error checking schema: {e}")
            raise

    def create_schema(self, conn):
        """Create target schema if not exists"""
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    f"CREATE SCHEMA IF NOT EXISTS {self.config.TARGET_SCHEMA};"
                )
                conn.commit()
                logger.info(f"Schema {self.config.TARGET_SCHEMA} created/verified")
        except Exception as e:
            logger.error(f"Error creating schema: {e}")
            raise

    def create_target_table(self, conn):
        """Create target table if not exists"""
        try:
            create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {self.config.target_table_full} (
                date DATE,
                funil TEXT,
                total_cost NUMERIC,
                total_conversions NUMERIC,
                total_clicks NUMERIC,
                total_impressions NUMERIC,
                plataforma TEXT,
                acao TEXT,
                PRIMARY KEY (date, acao)
            );
            """
            
            with conn.cursor() as cursor:
                cursor.execute(create_table_query)
                conn.commit()
            logger.info(f"Table {self.config.target_table_full} created/verified")
        except Exception as e:
            logger.error(f"Error creating table: {e}")
            raise

    def execute_query(self, conn, query):
        """Execute query and return DataFrame"""
        try:
            with conn.cursor() as cursor:
                cursor.execute(query)
                
                if cursor.description:
                    columns = [desc[0] for desc in cursor.description]
                    data = cursor.fetchall()
                    return columns, data
            return None, None
        except Exception as e:
            logger.error(f"Error executing query: {e}")
            raise

    def upsert_data(self, conn, df):
        """Upsert data into target table using (date, acao) as conflict key"""
        if df.empty:
            logger.warning("No data to upsert - empty DataFrame")
            return
        
        try:
            # Certifique-se que a ordem das colunas no INSERT corresponde à ordem no DataFrame
            insert_query = f"""
            INSERT INTO {self.config.target_table_full} (
                date, acao, plataforma, funil, 
                total_cost, total_conversions, 
                total_clicks, total_impressions
            ) VALUES %s
            ON CONFLICT (date, acao) DO UPDATE SET
                total_cost = EXCLUDED.total_cost,
                total_conversions = EXCLUDED.total_conversions,
                total_clicks = EXCLUDED.total_clicks,
                total_impressions = EXCLUDED.total_impressions,
                plataforma = EXCLUDED.plataforma,
                funil = EXCLUDED.funil
            """
            
            # Converter DataFrame para tuplas na ordem correta
            data_tuples = [tuple(x) for x in df[['date', 'acao', 'plataforma', 'funil', 
                                            'total_cost', 'total_conversions',
                                            'total_clicks', 'total_impressions']].to_numpy()]
            
            with conn.cursor() as cursor:
                psycopg2.extras.execute_values(
                    cursor, insert_query, data_tuples
                )
                conn.commit()
            logger.info(f"Upserted {len(df)} records successfully")
        except Exception as e:
            logger.error(f"Error during upsert: {e}")
            raise

    def compare_aggregates(self, conn):
        """Compare source and target aggregates to validate data integrity"""
        try:
            query = f"""
            WITH source_agg AS (
                SELECT 
                    SUM(CAST(cost AS numeric)) AS total_cost,
                    SUM(CAST(NULLIF(conversions, '') AS numeric)) AS total_conversions,
                    SUM(CAST(NULLIF(clicks, '') AS numeric)) AS total_clicks,
                    SUM(CAST(NULLIF(impressions, '') AS numeric)) AS total_impressions
                FROM {self.config.SOURCE_TABLE}
                WHERE date >= '{self.config.START_DATE}'
            ),
            target_agg AS (
                SELECT 
                    SUM(total_cost) AS total_cost,
                    SUM(total_conversions) AS total_conversions,
                    SUM(total_clicks) AS total_clicks,
                    SUM(total_impressions) AS total_impressions
                FROM {self.config.target_table_full}
                WHERE date >= '{self.config.START_DATE}'
            )
            SELECT 
                'source' AS type, total_cost, total_conversions, total_clicks, total_impressions
            FROM source_agg
            UNION ALL
            SELECT 
                'target' AS type, total_cost, total_conversions, total_clicks, total_impressions
            FROM target_agg
            """
            
            with conn.cursor() as cursor:
                cursor.execute(query)
                columns = [desc[0] for desc in cursor.description]
                data = cursor.fetchall()
                return pd.DataFrame(data, columns=columns)
                
        except Exception as e:
            logger.error(f"Error comparing aggregates: {e}")
            raise