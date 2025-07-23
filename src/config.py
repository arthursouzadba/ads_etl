import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    DB_HOST = os.getenv('DB_HOST')
    DB_PORT = os.getenv('DB_PORT')
    DB_NAME = os.getenv('DB_NAME')
    DB_USER = os.getenv('DB_USER')
    DB_PASSWORD = os.getenv('DB_PASSWORD')
    
    TARGET_SCHEMA = os.getenv('TARGET_SCHEMA')
    TARGET_TABLE = os.getenv('TARGET_TABLE')
    SOURCE_TABLE = os.getenv('SOURCE_TABLE')
    START_DATE = os.getenv('START_DATE')
    
    @property
    def target_table_full(self):
        return f"{self.TARGET_SCHEMA}.{self.TARGET_TABLE}"