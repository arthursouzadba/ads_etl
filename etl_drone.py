import sys
import time
import subprocess
from pathlib import Path
from datetime import datetime, timedelta
import schedule
from src.logger import logger
import logging
from pathlib import Path

# Configura caminhos absolutos
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

# Agora importa o logger do módulo src
from src.logger import logger

def run_etl_process():
    """Executa o ETL principal como subprocesso"""
    try:
        process = subprocess.Popen(
            [sys.executable, "-m", "src.etl"],  # Executa como módulo
            cwd=project_root,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
            universal_newlines=True
        )

        while process.poll() is None:
            output = process.stdout.readline()
            error = process.stderr.readline()
            
            if output:
                logger.info(f"[ETL] {output.strip()}")
            if error:
                logger.error(f"[ETL] {error.strip()}")

        return process.returncode == 0
        
    except Exception as e:
        logger.error(f"Erro no drone: {str(e)}")
        return False

def run_etl_job():
    logger.info("Executing scheduled ETL job")
    success = run_etl_process()
    if success:
        logger.info("ETL job completed successfully")
    else:
        logger.error("ETL job failed")

def main():
    logger.info("ETL Drone initialized - Running daily at 3:00 AM")
    
    # Schedule the job to run every day at 3:00 AM
    schedule.every().day.at("03:00").do(run_etl_job)
    
    # Initial run if needed
    run_etl_job()
    
    try:
        while True:
            schedule.run_pending()
            time.sleep(60)
    except KeyboardInterrupt:
        logger.info("ETL Drone stopped by user")

if __name__ == "__main__":
    main()