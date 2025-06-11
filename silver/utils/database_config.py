from sqlalchemy import create_engine
import pandas as pd

# Configuración de conexión MySQL
def get_mysql_engine():
    """Crea y retorna el engine de conexión a MySQL"""
    user = 'root'
    password = 'test123'
    host = '127.0.0.1'
    port = 3306
    database = 'silver_db'

    connection_string = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
    engine = create_engine(connection_string)
    return engine

def save_to_mysql(df, table_name, if_exists='replace'):
    """
    Guarda un DataFrame en MySQL
    
    Args:
        df: DataFrame a guardar
        table_name: Nombre de la tabla
        if_exists: 'replace', 'append', 'fail'
    """
    engine = get_mysql_engine()
    df.to_sql(table_name, engine, if_exists=if_exists, index=False)
    print(f"✅ Tabla {table_name} guardada en MySQL con {len(df)} registros")