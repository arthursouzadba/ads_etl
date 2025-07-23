import sys
import time
import subprocess
from pathlib import Path
from datetime import datetime, timedelta

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

def main():
    logger.info("🛸 ETL Drone initialized - Ctrl+C to stop")
    while True:
        success = run_etl_process()
        wait_time = 120 if success else 60
        next_run = datetime.now() + timedelta(seconds=wait_time)
        logger.info(f"⏳ Next run in {wait_time}s at {next_run.strftime('%H:%M:%S')}")
        time.sleep(wait_time)

if __name__ == "__main__":
    main()