import pandas as pd
from prefect import task, flow
import os
import numpy as np

@task(name="Leer compras bronce")
def read_compras_bronze():
    """Lee el archivo de compras desde la capa bronce"""
    df = pd.read_csv('../../bronze/compras.csv')
    return df

@task(name="Leer mapeo de códigos")
def read_mapeo_codigos():
    """Lee el mapeo de códigos desde productos master"""
    mapeo_df = pd.read_csv('result/mapeo_codigos.csv')
    mapeo = dict(zip(mapeo_df['codigo_original'], mapeo_df['codigo_maestro']))
    return mapeo

@task(name="Leer productos master")
def read_productos_master():
    """Lee el archivo maestro de productos"""
    df = pd.read_csv('result/productos_master.csv')
    return df

@task(name="Limpiar compras con integridad referencial")
def clean_compras_with_integrity(df_compras, mapeo_codigos, df_productos):
    """
    Limpia compras aplicando integridad referencial:
    - Mapea códigos a código maestro
    - Actualiza precios y descripciones desde productos master
    - Elimina compras de productos inexistentes
    """
    # Mapear códigos a códigos maestros
    df_compras['codigo_producto'] = df_compras['codigo_producto'].map(mapeo_codigos).fillna(df_compras['codigo_producto'])
    
    # REMOVE "elimina" COLUMN:
    df_compras = df_compras.drop(columns=['elimina'], errors='ignore')

    # Crear diccionario de productos master para lookup
    productos_dict = df_productos.set_index('codigo').to_dict('index')
    
    # Aplicar integridad referencial
    compras_limpias = []
    
    for _, compra in df_compras.iterrows():
        codigo = compra['codigo_producto']
        
        if codigo in productos_dict:
            # Actualizar con datos del producto master
            compra_limpia = compra.copy()
            compra_limpia['descripcion'] = productos_dict[codigo]['descripcion']
            compra_limpia['precio'] = productos_dict[codigo]['valor_compra']  # Precio de compra
            
            compras_limpias.append(compra_limpia)
    
    resultado = pd.DataFrame(compras_limpias)
    
    # Agregar columna de tipo de transacción
    resultado['tipo_transaccion'] = 'COMPRA'
    resultado['fecha'] = pd.to_datetime(np.random.randint(
    pd.Timestamp('2023-01-01').value,
    pd.Timestamp('2023-12-31').value,
    len(resultado)
    ))
    
    return resultado

@task(name="Guardar compras limpias")
def save_compras_limpias(df_compras):
    """Guarda el archivo de compras limpias en CSV y MySQL"""
    from database_config import save_to_mysql
    
    os.makedirs('result', exist_ok=True)
    
    # Guardar en CSV
    df_compras.to_csv('result/compras_limpias.csv', index=False)
    
    # Guardar en MySQL
    save_to_mysql(df_compras, 'compras_limpias')
    
    return df_compras

@flow(name="ETL Compras")
def compras_etl():
    """
    Flow para procesar compras con integridad referencial.
    Depende del productos master ETL.
    """
    # Leer datos necesarios
    df_compras = read_compras_bronze()
    mapeo_codigos = read_mapeo_codigos()
    df_productos = read_productos_master()
    
    # Limpiar con integridad referencial
    compras_limpias = clean_compras_with_integrity(df_compras, mapeo_codigos, df_productos)
    
    # Guardar resultado
    compras_final = save_compras_limpias(compras_limpias)
    
    return compras_final

if __name__ == "__main__":
    compras_etl()