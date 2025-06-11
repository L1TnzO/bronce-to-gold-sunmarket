import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from prefect import flow, task
from prefect.logging import get_run_logger
import os
from pathlib import Path

# ====================================================================
# TASKS PARA CARGA DE DATOS SILVER
# ====================================================================

@task(name="load_silver_data")
def load_silver_data(silver_path: str = "../../silver/etls/result") -> dict:
    """
    Carga todas las tablas de la capa Silver
    """
    logger = get_run_logger()
    
    try:
        # Rutas de archivos Silver
        files = {
            'ventas': f"{silver_path}/ventas_limpias.csv",
            'compras': f"{silver_path}/compras_limpias.csv", 
            'productos': f"{silver_path}/productos_master.csv",
            'transacciones': f"{silver_path}/transacciones_unificadas.csv"
        }
        
        data = {}
        for key, file_path in files.items():
            if os.path.exists(file_path):
                data[key] = pd.read_csv(file_path)
                logger.info(f"Cargado {key}: {len(data[key])} registros")
            else:
                logger.warning(f"Archivo no encontrado: {file_path}")
                data[key] = pd.DataFrame()
        
        return data
    
    except Exception as e:
        logger.error(f"Error cargando datos Silver: {str(e)}")
        raise

# ====================================================================
# TASKS PARA KPI 1: MARGEN DE UTILIDAD BRUTA POR PRODUCTO
# ====================================================================

@task(name="calculate_profit_margin")
def calculate_profit_margin(productos_df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula el margen de utilidad bruta por producto
    Fórmula: ((lista_1 - valor_compra) / lista_1) * 100
    """
    logger = get_run_logger()
    
    try:
        # Crear copia para evitar modificar original
        df = productos_df.copy()
        
        # Validar columnas necesarias
        required_cols = ['codigo', 'descripcion', 'valor_compra', 'lista_1']
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            raise ValueError(f"Columnas faltantes: {missing_cols}")
        
        # Filtrar productos con precios válidos
        df = df[(df['valor_compra'] > 0) & (df['lista_1'] > 0)].copy()
        
        # Calcular margen de utilidad bruta
        df['margen_utilidad_bruta'] = ((df['lista_1'] - df['valor_compra']) / df['lista_1']) * 100
        df['utilidad_unitaria'] = df['lista_1'] - df['valor_compra']
        
        # Clasificar productos por rentabilidad
        df['clasificacion_rentabilidad'] = pd.cut(
            df['margen_utilidad_bruta'], 
            bins=[-np.inf, 10, 25, 50, np.inf],
            labels=['Baja', 'Media', 'Alta', 'Muy Alta']
        )
        
        # Seleccionar columnas finales
        result = df[['codigo', 'descripcion', 'valor_compra', 'lista_1', 
                    'margen_utilidad_bruta', 'utilidad_unitaria', 'clasificacion_rentabilidad']].copy()
        
        # Ordenar por margen descendente
        result = result.sort_values('margen_utilidad_bruta', ascending=False)
        
        logger.info(f"Calculado margen de utilidad para {len(result)} productos")
        logger.info(f"Margen promedio: {result['margen_utilidad_bruta'].mean():.2f}%")
        
        return result
    
    except Exception as e:
        logger.error(f"Error calculando margen de utilidad: {str(e)}")
        raise

# ====================================================================
# TASKS PARA KPI 2: ROTACIÓN DE INVENTARIO
# ====================================================================

@task(name="calculate_inventory_turnover")
def calculate_inventory_turnover(
    transacciones_df: pd.DataFrame, 
    productos_df: pd.DataFrame,
    period_days: int = 30
) -> pd.DataFrame:
    """
    Calcula la rotación de inventario por producto
    Fórmula: Cantidad_vendida_período / Stock_promedio
    """
    logger = get_run_logger()
    
    try:
        # Filtrar solo ventas del período especificado
        transacciones_df['fecha'] = pd.to_datetime(transacciones_df['fecha'])
        fecha_limite = transacciones_df['fecha'].max() - timedelta(days=period_days)
        
        ventas_periodo = transacciones_df[
            (transacciones_df['tipo_transaccion'] == 'VENTA') & 
            (transacciones_df['fecha'] >= fecha_limite)
        ].copy()
        
        # Agregar ventas por producto
        ventas_agregadas = ventas_periodo.groupby('codigo_producto').agg({
            'cantidad': 'sum',
            'total': 'sum',
            'fecha': ['min', 'max', 'count']
        }).reset_index()
        
        # Aplanar columnas multinivel
        ventas_agregadas.columns = [
            'codigo_producto', 'cantidad_vendida', 'total_vendido',
            'primera_venta', 'ultima_venta', 'num_transacciones'
        ]
        
        # Merge con datos de productos para obtener stock
        productos_stock = productos_df[['codigo', 'descripcion', 'bod_1', 'stock_original']].copy()
        productos_stock = productos_stock.rename(columns={'codigo': 'codigo_producto'})
        
        # Combinar ventas con inventario
        rotacion = ventas_agregadas.merge(productos_stock, on='codigo_producto', how='left')
        
        # Calcular stock promedio (aproximación)
        rotacion['stock_promedio'] = (rotacion['bod_1'] + rotacion['stock_original']) / 2
        rotacion['stock_promedio'] = rotacion['stock_promedio'].fillna(rotacion['bod_1'])
        
        # Calcular rotación de inventario
        rotacion['rotacion_inventario'] = np.where(
            rotacion['stock_promedio'] > 0,
            rotacion['cantidad_vendida'] / rotacion['stock_promedio'],
            0
        )
        
        # Calcular días de inventario
        rotacion['dias_inventario'] = np.where(
            rotacion['rotacion_inventario'] > 0,
            period_days / rotacion['rotacion_inventario'],
            999  # Valor alto para productos sin rotación
        )
        
        # Clasificar por rotación
        rotacion['clasificacion_rotacion'] = pd.cut(
            rotacion['rotacion_inventario'],
            bins=[0, 0.5, 1, 2, np.inf],
            labels=['Muy Baja', 'Baja', 'Media', 'Alta']
        )
        
        # Calcular velocidad de venta diaria
        rotacion['venta_diaria_promedio'] = rotacion['cantidad_vendida'] / period_days
        
        # Seleccionar columnas finales
        result = rotacion[['codigo_producto', 'descripcion', 'cantidad_vendida', 
                          'stock_promedio', 'rotacion_inventario', 'dias_inventario',
                          'clasificacion_rotacion', 'venta_diaria_promedio', 
                          'num_transacciones', 'total_vendido']].copy()
        
        # Ordenar por rotación descendente
        result = result.sort_values('rotacion_inventario', ascending=False)
        
        logger.info(f"Calculada rotación para {len(result)} productos")
        logger.info(f"Rotación promedio: {result['rotacion_inventario'].mean():.2f}")
        
        return result
    
    except Exception as e:
        logger.error(f"Error calculando rotación de inventario: {str(e)}")
        raise

# ====================================================================
# TASKS PARA KPI 3: CONTRIBUCIÓN A VENTAS POR PRODUCTO
# ====================================================================

@task(name="calculate_sales_contribution")
def calculate_sales_contribution(
    transacciones_df: pd.DataFrame,
    period_days: int = 30
) -> pd.DataFrame:
    """
    Calcula la contribución a ventas por producto
    Fórmula: (Total_ventas_producto / Total_ventas_general) * 100
    """
    logger = get_run_logger()
    
    try:
        # Filtrar ventas del período
        transacciones_df['fecha'] = pd.to_datetime(transacciones_df['fecha'])
        fecha_limite = transacciones_df['fecha'].max() - timedelta(days=period_days)
        
        ventas_periodo = transacciones_df[
            (transacciones_df['tipo_transaccion'] == 'VENTA') & 
            (transacciones_df['fecha'] >= fecha_limite)
        ].copy()
        
        # Agregar ventas por producto
        ventas_por_producto = ventas_periodo.groupby(['codigo_producto', 'descripcion']).agg({
            'cantidad': 'sum',
            'total': 'sum',
            'fecha': 'count'
        }).reset_index()
        
        ventas_por_producto.columns = ['codigo_producto', 'descripcion', 
                                      'cantidad_total', 'total_ventas', 'num_transacciones']
        
        # Calcular totales generales
        total_ventas_general = ventas_por_producto['total_ventas'].sum()
        total_cantidad_general = ventas_por_producto['cantidad_total'].sum()
        
        # Calcular contribuciones
        ventas_por_producto['contribucion_ventas_pct'] = (
            ventas_por_producto['total_ventas'] / total_ventas_general * 100
        )
        
        ventas_por_producto['contribucion_cantidad_pct'] = (
            ventas_por_producto['cantidad_total'] / total_cantidad_general * 100
        )
        
        # Calcular precio promedio
        ventas_por_producto['precio_promedio'] = (
            ventas_por_producto['total_ventas'] / ventas_por_producto['cantidad_total']
        )
        
        # Calcular contribución acumulada
        ventas_por_producto = ventas_por_producto.sort_values('total_ventas', ascending=False)
        ventas_por_producto['contribucion_acumulada'] = ventas_por_producto['contribucion_ventas_pct'].cumsum()
        
        # Clasificar productos ABC
        ventas_por_producto['clasificacion_abc'] = np.where(
            ventas_por_producto['contribucion_acumulada'] <= 80, 'A',
            np.where(ventas_por_producto['contribucion_acumulada'] <= 95, 'B', 'C')
        )
        
        # Calcular ranking
        ventas_por_producto['ranking_ventas'] = range(1, len(ventas_por_producto) + 1)
        
        # Seleccionar columnas finales
        result = ventas_por_producto[['codigo_producto', 'descripcion', 'total_ventas',
                                    'cantidad_total', 'contribucion_ventas_pct', 
                                    'contribucion_cantidad_pct', 'contribucion_acumulada',
                                    'clasificacion_abc', 'ranking_ventas', 'precio_promedio',
                                    'num_transacciones']].copy()
        
        logger.info(f"Calculada contribución para {len(result)} productos")
        logger.info(f"Top 10 productos representan {result.head(10)['contribucion_ventas_pct'].sum():.1f}% de ventas")
        
        return result
    
    except Exception as e:
        logger.error(f"Error calculando contribución a ventas: {str(e)}")
        raise

# ====================================================================
# TASK PARA CREAR RESUMEN EJECUTIVO
# ====================================================================

@task(name="create_executive_summary")
def create_executive_summary(
    margen_df: pd.DataFrame,
    rotacion_df: pd.DataFrame, 
    contribucion_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Crea un resumen ejecutivo combinando los 3 KPIs principales
    """
    logger = get_run_logger()
    
    try:
        # Base: productos con margen de utilidad
        base = margen_df[['codigo', 'descripcion', 'margen_utilidad_bruta', 
                         'utilidad_unitaria', 'clasificacion_rentabilidad']].copy()
        
        # Merge con rotación
        rotacion_slim = rotacion_df[['codigo_producto', 'rotacion_inventario', 
                                   'dias_inventario', 'clasificacion_rotacion']].copy()
        rotacion_slim = rotacion_slim.rename(columns={'codigo_producto': 'codigo'})
        
        base = base.merge(rotacion_slim, on='codigo', how='left')
        
        # Merge con contribución
        contribucion_slim = contribucion_df[['codigo_producto', 'contribucion_ventas_pct',
                                           'clasificacion_abc', 'ranking_ventas']].copy()
        contribucion_slim = contribucion_slim.rename(columns={'codigo_producto': 'codigo'})
        
        base = base.merge(contribucion_slim, on='codigo', how='left')
        
        # Calcular score combinado (0-100)
        # Normalizar métricas
        base['score_margen'] = np.clip(base['margen_utilidad_bruta'] / 50 * 30, 0, 30)
        base['score_rotacion'] = np.clip(base['rotacion_inventario'] / 2 * 35, 0, 35)
        base['score_contribucion'] = np.clip(base['contribucion_ventas_pct'] / 10 * 35, 0, 35)
        
        # Score total
        base['score_total'] = (base['score_margen'].fillna(0) + 
                              base['score_rotacion'].fillna(0) + 
                              base['score_contribucion'].fillna(0))
        
        # Clasificación estratégica
        base['clasificacion_estrategica'] = pd.cut(
            base['score_total'],
            bins=[0, 25, 50, 75, 100],
            labels=['Revisar', 'Mantener', 'Impulsar', 'Estrella']
        )
        
        # Ordenar por score total
        base = base.sort_values('score_total', ascending=False)
        
        logger.info(f"Resumen ejecutivo creado para {len(base)} productos")
        
        return base
    
    except Exception as e:
        logger.error(f"Error creando resumen ejecutivo: {str(e)}")
        raise

# ====================================================================
# TASK PARA GUARDAR RESULTADOS
# ====================================================================

@task(name="save_gold_tables")
def save_gold_tables(
    margen_df: pd.DataFrame,
    rotacion_df: pd.DataFrame,
    contribucion_df: pd.DataFrame,
    resumen_df: pd.DataFrame,
    gold_path: str = "result"
) -> dict:
    """
    Guarda todas las tablas Gold en CSV
    """
    logger = get_run_logger()
    
    try:
        # Crear directorio si no existe
        Path(gold_path).mkdir(parents=True, exist_ok=True)
        
        # Guardar tablas
        tables = {
            'margen_utilidad_productos': margen_df,
            'rotacion_inventario_productos': rotacion_df,
            'contribucion_ventas_productos': contribucion_df,
            'resumen_ejecutivo_productos': resumen_df
        }
        
        saved_files = {}
        for table_name, df in tables.items():
            file_path = f"{gold_path}/{table_name}.csv"
            df.to_csv(file_path, index=False)
            saved_files[table_name] = file_path
            logger.info(f"Guardado {table_name}: {len(df)} registros en {file_path}")
        
        return saved_files
    
    except Exception as e:
        logger.error(f"Error guardando tablas Gold: {str(e)}")
        raise

# ====================================================================
# FLOW PRINCIPAL
# ====================================================================

@flow(name="silver_to_gold")
def silver_to_gold(
    silver_path: str = "../../silver/etls/result",
    gold_path: str = "result",
    period_days: int = 30
):
    """
    Flow principal que transforma datos Silver a Gold
    Genera KPIs Tier 1: Margen, Rotación y Contribución
    """
    logger = get_run_logger()
    logger.info("=== INICIANDO TRANSFORMACIÓN SILVER TO GOLD - TIER 1 KPIs ===")
    
    # 1. Cargar datos Silver
    silver_data = load_silver_data(silver_path)
    
    # 2. Calcular KPI 1: Margen de Utilidad Bruta
    logger.info("Calculando KPI 1: Margen de Utilidad Bruta")
    margen_df = calculate_profit_margin(silver_data['productos'])
    
    # 3. Calcular KPI 2: Rotación de Inventario
    logger.info("Calculando KPI 2: Rotación de Inventario")
    rotacion_df = calculate_inventory_turnover(
        silver_data['transacciones'], 
        silver_data['productos'],
        period_days
    )
    
    # 4. Calcular KPI 3: Contribución a Ventas
    logger.info("Calculando KPI 3: Contribución a Ventas")
    contribucion_df = calculate_sales_contribution(
        silver_data['transacciones'],
        period_days
    )
    
    # 5. Crear Resumen Ejecutivo
    logger.info("Creando Resumen Ejecutivo")
    resumen_df = create_executive_summary(margen_df, rotacion_df, contribucion_df)
    
    # 6. Guardar todas las tablas Gold
    logger.info("Guardando tablas en capa Gold")
    saved_files = save_gold_tables(
        margen_df, rotacion_df, contribucion_df, resumen_df, gold_path
    )
    
    logger.info("=== TRANSFORMACIÓN COMPLETADA ===")
    logger.info(f"Archivos generados: {list(saved_files.keys())}")
    
    return {
        'status': 'success',
        'files_created': saved_files,
        'summary': {
            'productos_analizados': len(resumen_df),
            'periodo_dias': period_days,
            'fecha_ejecucion': datetime.now().isoformat()
        }
    }

# ====================================================================
# EJECUCIÓN DEL FLOW
# ====================================================================

if __name__ == "__main__":
    # Ejecutar el flow
    result = silver_to_gold()
    print("Transformación completada:", result)