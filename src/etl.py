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

def validate_data(df):
    """Validate and clean data"""
    try:
        # Convert date to datetime if needed
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date']).dt.date
            
        # Fill numeric NA values with 0
        numeric_cols = ['total_cost', 'total_conversions', 'total_clicks', 'total_impressions']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
                
        return df
    except Exception as e:
        logger.error(f"Data validation error: {e}")
        raise

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