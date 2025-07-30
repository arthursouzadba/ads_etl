import pandas as pd
from datetime import datetime
from src.database import Database  # Import absoluto do pacote src
from src.logger import logger
from src.config import Config

def build_query(config):

    """Build the aggregation query"""
    return f"""
    SELECT 
        date,
        acao,
        MAX(funil) AS funil,  
        MAX(plataforma) AS plataforma,
        SUM(CAST(cost AS numeric)) AS total_cost,
        SUM(CAST(NULLIF(conversions, '') AS numeric)) AS total_conversions,
        SUM(CAST(NULLIF(clicks, '') AS numeric)) AS total_clicks,
        SUM(CAST(NULLIF(impressions, '') AS numeric)) AS total_impressions
    FROM {config.SOURCE_TABLE}
    WHERE date >= '{config.START_DATE}'
    GROUP BY date, acao
    ORDER BY date DESC;
    """

# etl.py - modificar validate_data
def validate_data(df):
    """Validate and clean data with strict type checking"""
    type_checks = {
        'date': 'datetime64[ns]',
        'acao': 'object',
        'funil': 'object',
        'plataforma': 'object',
        'total_cost': 'float64',
        'total_conversions': 'float64',
        'total_clicks': 'float64',
        'total_impressions': 'float64'
    }
    
    for col, dtype in type_checks.items():
        if col in df.columns:
            try:
                df[col] = df[col].astype(dtype)
                if dtype == 'datetime64[ns]':
                    df[col] = df[col].dt.date
            except Exception as e:
                logger.error(f"Error converting column {col} to {dtype}: {e}")
                raise
                
    return df

def check_missing_data(df):
    """Check for missing data in critical columns"""
    critical_cols = ['date', 'acao', 'plataforma']
    missing = df[critical_cols].isnull().sum()
    
    if missing.any():
        logger.warning(f"Missing data found:\n{missing}")
        # Optionally: raise ValueError if missing data is not acceptable

def main():
    logger.info("Iniciando processo ETL")
    start_time = datetime.now()
    
    try:
        db = Database()
        conn = db.get_connection()
        logger.info("Conexão com o banco estabelecida")

        if not db.check_schema_exists(conn):
            logger.info("Criando schema...")
            db.create_schema(conn)
        
        logger.info("Verificando tabela de destino...")
        db.create_target_table(conn)

        logger.info("Executando query...")
        query = build_query(db.config)
        columns, data = db.execute_query(conn, query)
        
        if data:
            logger.info(f"Processando {len(data)} registros")
            df = pd.DataFrame(data, columns=columns)
            df_clean = validate_data(df)
            
            logger.info("Realizando upsert...")
            db.upsert_data(conn, df_clean)
            logger.info(f"Dados inseridos/atualizados: {len(df_clean)}")
        
        execution_time = datetime.now() - start_time
        logger.info(f"ETL concluído em {execution_time}")
        
    except Exception as e:
        logger.error(f"Falha no ETL: {str(e)}", exc_info=True)
        raise
    finally:
        if 'conn' in locals() and conn:
            conn.close()
            logger.info("Conexão com o banco encerrada")

if __name__ == "__main__":
    main()