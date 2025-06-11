import pandas as pd
from prefect import task, flow
import os
import numpy as np

@task(name="Leer ventas bronce")
def read_ventas_bronze():
    """Lee el archivo de ventas desde la capa bronce"""
    df = pd.read_csv('../../bronze/ventas.csv')
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

@task(name="Limpiar ventas con integridad referencial")
def clean_ventas_with_integrity(df_ventas, mapeo_codigos, df_productos):
    """
    Limpia ventas aplicando integridad referencial:
    - Mapea códigos a código maestro
    - Actualiza precios y descripciones desde productos master
    - Elimina ventas de productos inexistentes
    """
    # Mapear códigos a códigos maestros
    df_ventas['codigo_producto'] = df_ventas['codigo_producto'].map(mapeo_codigos).fillna(df_ventas['codigo_producto'])
    
    # Crear diccionario de productos master para lookup
    productos_dict = df_productos.set_index('codigo').to_dict('index')
    
    # Aplicar integridad referencial
    ventas_limpias = []
    
    for _, venta in df_ventas.iterrows():
        codigo = venta['codigo_producto']
        
        if codigo in productos_dict:
            # Actualizar con datos del producto master
            venta_limpia = venta.copy()
            venta_limpia['descripcion'] = productos_dict[codigo]['descripcion']
            venta_limpia['precio'] = productos_dict[codigo]['lista_1']  # Precio de venta
            
            ventas_limpias.append(venta_limpia)
    
    resultado = pd.DataFrame(ventas_limpias)
    
    # Agregar columna de tipo de transacción
    resultado['tipo_transaccion'] = 'VENTA'
    resultado['fecha'] = pd.to_datetime(np.random.randint(
    pd.Timestamp('2023-01-01').value,
    pd.Timestamp('2023-12-31').value,
    len(resultado)
    ))
    
    return resultado

@task(name="Guardar ventas limpias")
def save_ventas_limpias(df_ventas):
    """Guarda el archivo de ventas limpias en CSV y MySQL"""
    from database_config import save_to_mysql
    
    os.makedirs('result', exist_ok=True)
    
    # Guardar en CSV
    df_ventas.to_csv('result/ventas_limpias.csv', index=False)
    
    # Guardar en MySQL
    save_to_mysql(df_ventas, 'ventas_limpias')
    
    return df_ventas

@flow(name="ETL Ventas")
def ventas_etl():
    """
    Flow para procesar ventas con integridad referencial.
    Depende del productos master ETL.
    """
    # Leer datos necesarios
    df_ventas = read_ventas_bronze()
    mapeo_codigos = read_mapeo_codigos()
    df_productos = read_productos_master()
    
    # Limpiar con integridad referencial
    ventas_limpias = clean_ventas_with_integrity(df_ventas, mapeo_codigos, df_productos)
    
    # Guardar resultado
    ventas_final = save_ventas_limpias(ventas_limpias)
    
    return ventas_final

if __name__ == "__main__":
    ventas_etl()