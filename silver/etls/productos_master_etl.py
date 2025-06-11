import pandas as pd
from prefect import task, flow
import os

@task(name="Leer productos bronce")
def read_productos_bronze():
    """Lee el archivo de productos desde la capa bronce"""
    df = pd.read_csv('../bronze/productos.csv')
    return df

@task(name="Limpiar y consolidar productos")
def clean_and_consolidate_productos(df):
    """
    Consolida productos duplicados por descripción:
    - Mantiene el primer código encontrado
    - Suma stocks y cantidades
    - Promedia precios
    """
    # Limpiar datos básicos
    df = df.dropna(subset=['descripcion'])
    df['descripcion'] = df['descripcion'].str.strip().str.upper()
    
    # Agrupar por descripción para consolidar duplicados
    productos_consolidados = []
    
    for descripcion, group in df.groupby('descripcion'):
        # Mantener el primer código encontrado
        primer_producto = group.iloc[0].copy()
        
        # Consolidar valores numéricos
        primer_producto['valor_compra'] = int(round(group['valor_compra'].mean()))
        primer_producto['lista_1'] = int(round(group['lista_1'].mean()))
        primer_producto['stock_original'] = int(round(group['stock_original'].sum()))
        primer_producto['bod_1'] = int(round(group['bod_1'].sum()))
        primer_producto['minimo'] = int(round(group['minimo'].mean()))
        
        productos_consolidados.append(primer_producto)
    
    resultado = pd.DataFrame(productos_consolidados)
    
    # Crear mapeo de códigos antiguos a código maestro
    mapeo_codigos = {}
    for descripcion, group in df.groupby('descripcion'):
        codigo_maestro = group.iloc[0]['codigo']
        for _, row in group.iterrows():
            mapeo_codigos[row['codigo']] = codigo_maestro
    
    return resultado, mapeo_codigos

@task(name="Guardar productos master")
def save_productos_master(df_productos, mapeo_codigos):
    """Guarda el archivo maestro de productos y el mapeo de códigos"""
    from utils.database_config import save_to_mysql
    os.makedirs('result', exist_ok=True)
    
    # Guardar productos master
    df_productos.to_csv('result/productos_master.csv', index=False)

    # Guardar en MySQL
    save_to_mysql(df_productos, 'productos_master')
    
    # Guardar mapeo para uso posterior
    mapeo_df = pd.DataFrame(list(mapeo_codigos.items()), 
                           columns=['codigo_original', 'codigo_maestro'])
    mapeo_df.to_csv('result/mapeo_codigos.csv', index=False)
    
    return df_productos, mapeo_codigos

@flow(name="ETL Productos Master")
def productos_master_etl():
    """
    Flow principal para crear el archivo maestro de productos.
    Consolida productos duplicados y crea mapeo de códigos.
    """
    # Leer datos bronce
    df_productos = read_productos_bronze()
    
    # Limpiar y consolidar
    productos_limpios, mapeo = clean_and_consolidate_productos(df_productos)
    
    # Guardar resultados
    productos_final, mapeo_final = save_productos_master(productos_limpios, mapeo)
    
    return productos_final, mapeo_final

if __name__ == "__main__":
    productos_master_etl()