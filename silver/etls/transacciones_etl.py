import pandas as pd
from prefect import task, flow
import os

@task(name="Leer ventas limpias")
def read_ventas_limpias():
    """Lee las ventas ya procesadas"""
    df = pd.read_csv('result/ventas_limpias.csv')
    return df

@task(name="Leer compras limpias")
def read_compras_limpias():
    """Lee las compras ya procesadas"""
    df = pd.read_csv('result/compras_limpias.csv')
    return df

@task(name="Unificar transacciones")
def unify_transactions(df_ventas, df_compras):
    """
    Unifica ventas y compras en un solo dataset de transacciones.
    Mantiene la trazabilidad del origen de cada transacción.
    """
    # Seleccionar columnas comunes y renombrar para consistencia
    columnas_comunes = ['codigo_producto', 'descripcion', 'precio', 'cantidad', 'total', 'tipo_transaccion', 'fecha']
    
    # Preparar ventas para unificación
    ventas_unificadas = df_ventas[columnas_comunes].copy()
    ventas_unificadas['folio_origen'] = df_ventas['folio']
    
    # Preparar compras para unificación
    compras_unificadas = df_compras[columnas_comunes].copy()
    compras_unificadas['folio_origen'] = df_compras.get('lote', 'N/A')  # Usar lote como referencia
    
    # Unificar ambos datasets
    transacciones_unificadas = pd.concat([ventas_unificadas, compras_unificadas], ignore_index=True)
    
    # Ordenar por fecha y código de producto
    transacciones_unificadas = transacciones_unificadas.sort_values(['fecha', 'codigo_producto'])
    
    return transacciones_unificadas

@task(name="Guardar transacciones unificadas")
def save_transacciones_unificadas(df_transacciones):
    """Guarda el archivo de transacciones unificadas en CSV y MySQL"""
    from utils.database_config import save_to_mysql
    
    os.makedirs('result', exist_ok=True)
    
    # Guardar en CSV
    df_transacciones.to_csv('result/transacciones_unificadas.csv', index=False)
    
    # Guardar en MySQL
    save_to_mysql(df_transacciones, 'transacciones_unificadas')
    
    return df_transacciones

@flow(name="ETL Transacciones Unificadas")
def transacciones_etl():
    """
    Flow para unificar ventas y compras en transacciones.
    Depende de ventas_etl y compras_etl.
    Proporciona trazabilidad completa del flujo de dinero e inventario.
    """
    # Leer datos procesados
    df_ventas = read_ventas_limpias()
    df_compras = read_compras_limpias()
    
    # Unificar transacciones
    transacciones_unificadas = unify_transactions(df_ventas, df_compras)
    
    # Guardar resultado
    transacciones_final = save_transacciones_unificadas(transacciones_unificadas)
    
    return transacciones_final

if __name__ == "__main__":
    transacciones_etl()