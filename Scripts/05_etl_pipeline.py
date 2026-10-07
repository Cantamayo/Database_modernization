"""
FleetLogix - Pipeline ETL Automático
Extrae de PostgreSQL, Transforma y Carga en Snowflake
Ejecución diaria automatizada
"""

import psycopg2
import snowflake.connector
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
import schedule
import time
import json
from typing import Dict, List, Tuple

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('etl_pipeline.log'),
        logging.StreamHandler()
    ]
)

# Configuración de conexiones
POSTGRES_CONFIG = {
    'host': 'localhost',
    'database': 'Fleetlogix',
    'user': 'postgres',
    'password': 'TU_PASSWORD_AQUI',
    'port': 5432
}

SNOWFLAKE_CONFIG = {
    'user': 'Andmerch3',
    'password': 'TU_PASSWORD_AQUI',
    'account': 'XTKPHRF-AL07773',
    'warehouse': 'FLEETLOGIX_WH',
    'database': 'FLEETLOGIX_DW',
    'schema': 'ANALYTICS'
}

class FleetLogixETL:
    def __init__(self):
        self.pg_conn = None
        self.sf_conn = None
        self.batch_id = int(datetime.now().timestamp())
        self.metrics = {
            'records_extracted': 0,
            'records_transformed': 0,
            'records_loaded': 0,
            'errors': 0
        }
    
    def connect_databases(self):
        """Establecer conexiones con PostgreSQL y Snowflake"""
        try:
            # PostgreSQL
            self.pg_conn = psycopg2.connect(**POSTGRES_CONFIG)
            logging.info(" Conectado a PostgreSQL")
            
            # Snowflake
            self.sf_conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
            logging.info(" Conectado a Snowflake")
            
            return True
        except Exception as e:
            logging.error(f" Error en conexión: {e}")
            return False
    
    def extract_daily_data(self) -> pd.DataFrame:
        """Extraer datos del día anterior de PostgreSQL"""
        logging.info(" Iniciando extracción de datos...")
        
        query = """
        WITH LastMaintenance AS (
            SELECT 
                vehicle_id, 
                MAX(maintenance_date) AS last_maintenance_date
            FROM maintenance
            GROUP BY vehicle_id
        )
        SELECT 
            -- IDs Y CLAVES
            d.delivery_id,
            t.trip_id,
            v.vehicle_id,
            dr.driver_id,
            r.route_id,
            d.tracking_number,

            -- FECHAS
            t.departure_datetime,
            t.arrival_datetime,
            d.scheduled_datetime,
            d.delivered_datetime,

            -- RUTAS
            r.route_code,
            r.origin_city,
            r.destination_city,
            r.distance_km,
            r.estimated_duration_hours,
            r.toll_cost,
            
            -- ESTADOS, VEHICULO Y MANTENIMIENTO
            t.status AS trip_status,
            v.license_plate,
            v.vehicle_type,
            v.capacity_kg AS vehicle_capacity,
            v.fuel_type,
            v.acquisition_date,          -- Para calcular age_months en Python
            v.status AS vehicle_status,  
            lm.last_maintenance_date,    -- Traído de la CTE
            t.fuel_consumed_liters,
            t.total_weight_kg AS trip_total_weight,
            
            -- CONDUCTOR Y CLIENTE
            dr.employee_code,
            CONCAT(dr.first_name, ' ', dr.last_name) AS driver_name,
            dr.license_number,           -- Para dim_driver en Snowflake
            dr.license_expiry,           -- Para dim_driver en Snowflake
            dr.phone,                    -- Para dim_driver en Snowflake
            dr.hire_date,                -- Para calcular experience_months en Python
            dr.status AS driver_status,  
            d.customer_name,
            
            -- MÉTRICAS DE ENTREGA
            d.package_weight_kg,
            d.delivery_status,
            d.recipient_signature

        FROM deliveries d
        JOIN trips t ON d.trip_id = t.trip_id
        JOIN vehicles v ON t.vehicle_id = v.vehicle_id
        JOIN drivers dr ON t.driver_id = dr.driver_id
        JOIN routes r ON t.route_id = r.route_id
        LEFT JOIN LastMaintenance lm ON v.vehicle_id = lm.vehicle_id
        WHERE t.departure_datetime >= CURRENT_DATE - INTERVAL '120 days';
        """
        
        try:
            df = pd.read_sql(query, self.pg_conn)
            self.metrics['records_extracted'] = len(df)
            logging.info(f" Extraídos {len(df)} registros")
            return df
        except Exception as e:
            logging.error(f" Error en extracción: {e}")
            self.metrics['errors'] += 1
            return pd.DataFrame()
    
    def transform_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transformar datos para el modelo dimensional"""
        logging.info(" Iniciando transformación de datos...")
        
        try:
            # Calcular métricas
            df['delivery_time_minutes'] = (
                (pd.to_datetime(df['delivered_datetime']) - 
                 pd.to_datetime(df['scheduled_datetime'])).dt.total_seconds() / 60
            ).round(2)
            
            df['delay_minutes'] = df['delivery_time_minutes'].apply(
                lambda x: max(0, x) if x > 0 else 0
            )
            
            df['is_on_time'] = df['delay_minutes'] <= 30
            
            # Calcular entregas por hora
            df['trip_duration_hours'] = (
                (pd.to_datetime(df['arrival_datetime']) - 
                 pd.to_datetime(df['departure_datetime'])).dt.total_seconds() / 3600
            ).round(2)
            
            # Agrupar entregas por trip para calcular entregas/hora
            deliveries_per_trip = df.groupby('trip_id').size()
            df['deliveries_in_trip'] = df['trip_id'].map(deliveries_per_trip)
            df['deliveries_per_hour'] = (
                df['deliveries_in_trip'] / df['trip_duration_hours']
            ).round(2)
            
            # Eficiencia de combustible
            df['fuel_efficiency_km_per_liter'] = (
                df['distance_km'] / df['fuel_consumed_liters']
            ).round(2)
            
            # Costo estimado por entrega
            df['cost_per_delivery'] = (
                (df['fuel_consumed_liters'] * 5000 + df['toll_cost']) / 
                df['deliveries_in_trip']
            ).round(2)
            
            # Revenue estimado (ejemplo: $20,000 base + $500 por kg)
            df['revenue_per_delivery'] = (20000 + df['package_weight_kg'] * 500).round(2)

            # ========================================================
            # --- INICIO DE NUEVAS REGLAS DE NEGOCIO AÑADIDAS ---
            # ========================================================
            
            # 1. Nivel de dificultad de la ruta
            def calc_difficulty(row):
                if row['distance_km'] < 50 and row['estimated_duration_hours'] < 1.5:
                    return 'Fácil'
                elif row['distance_km'] > 200 or row['estimated_duration_hours'] > 4:
                    return 'Difícil'
                else:
                    return 'Medio'
            
            df['difficulty_level'] = df.apply(calc_difficulty, axis=1)
            
            # 2. Categoría de rendimiento del conductor
            def calc_performance(row):
                if row['delay_minutes'] > 60 or row['delivery_status'] in ['failed', 'damaged']:
                    return 'Bajo'
                elif row['is_on_time'] and row['fuel_efficiency_km_per_liter'] > 3.5:
                    return 'Alto'
                else:
                    return 'Medio'
            
            df['performance_category'] = df.apply(calc_performance, axis=1)
            
            # 3. Edad del vehículo y experiencia del conductor en meses
            # Se convierte a datetime por seguridad y se divide por 30 días
            df['acquisition_date'] = pd.to_datetime(df['acquisition_date'])
            df['age_months'] = ((pd.Timestamp.now() - df['acquisition_date']).dt.days / 30).fillna(0).astype(int)
            
            df['hire_date'] = pd.to_datetime(df['hire_date'])
            df['experience_months'] = ((pd.Timestamp.now() - df['hire_date']).dt.days / 30).fillna(0).astype(int)
            
            # ========================================================
            # --- FIN DE NUEVAS REGLAS DE NEGOCIO ---
            # ========================================================
            
            # Validaciones de calidad
            # 1. Los tiempos negativos significan entregas anticipadas, son válidos.
            # Solo eliminamos si el peso es ilógico (0 o más de 10 toneladas).
            # Nota: Los paquetes en status 'pending' tendrán NaN en tiempo, Pandas los maneja bien después.
            
            # No permitir pesos fuera de rango
            df = df[(df['package_weight_kg'] > 0) & (df['package_weight_kg'] < 10000)]
            
            # Manejar cambios históricos (SCD Type 2 para conductor/vehículo)
            df['valid_from'] = pd.to_datetime(df['scheduled_datetime']).dt.date
            from datetime import date           # -- importamos para solucionar el limite de pandas
            df['valid_to'] = date(9999, 12, 31) #----limitacion de Pandas
            df['is_current'] = True
            
            self.metrics['records_transformed'] = len(df)
            logging.info(f" Transformados {len(df)} registros")
            
            return df
            
        except Exception as e:
            logging.error(f" Error en transformación: {e}")
            self.metrics['errors'] += 1
            return pd.DataFrame()
    
    def load_dimensions(self, df: pd.DataFrame):
        """Cargar o actualizar dimensiones en Snowflake"""
        logging.info(" Cargando dimensiones...")
        
        cursor = self.sf_conn.cursor()
        
        try:
            # ========================================================
            # --- NUEVO: Convertir fechas de Pandas a strings      ---
            # --- para evitar el error de "timestamp not supported"---
            # ========================================================
            date_cols = ['acquisition_date', 'last_maintenance_date', 'license_expiry', 'hire_date']
            for col in date_cols:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col]).dt.strftime('%Y-%m-%d')
                    df[col] = df[col].replace({'NaT': None, 'nan': None})
            # ========================================================

            # Cargar dim_customer (nuevos clientes)
            customers = df[['customer_name']].drop_duplicates()
            for _, row in customers.iterrows():
                cursor.execute("""
                    MERGE INTO dim_customer c
                    USING (SELECT %s as customer_name) s
                    ON c.customer_name = s.customer_name
                    WHEN NOT MATCHED THEN
                        INSERT (customer_name, customer_type, city, first_delivery_date, 
                               total_deliveries, customer_category)
                        VALUES (%s, 'Individual', %s, CURRENT_DATE(), 0, 'Regular')
                """, (row['customer_name'], row['customer_name'], 
                     df[df['customer_name'] == row['customer_name']]['destination_city'].iloc[0]))
            
            # ==========================================
            # --- INICIO DE CAMBIOS / AÑADIDOS ---
            # ==========================================

            # 2. Cargar dim_route (Actualiza dificultad si cambia)
            routes = df[['route_id', 'route_code', 'origin_city', 'destination_city', 
                         'distance_km', 'estimated_duration_hours', 'toll_cost', 'difficulty_level']].drop_duplicates('route_id')
            
            for _, row in routes.iterrows():
                cursor.execute("""
                    MERGE INTO dim_route r
                    USING (SELECT %s as route_id) s
                    ON r.route_id = s.route_id
                    WHEN NOT MATCHED THEN
                        INSERT (route_id, route_code, origin_city, destination_city, distance_km, estimated_duration_hours, toll_cost, difficulty_level, route_type)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'Urbana')
                    WHEN MATCHED THEN
                        UPDATE SET difficulty_level = %s, toll_cost = %s
                """, (row['route_id'], row['route_id'], row['route_code'], row['origin_city'], 
                      row['destination_city'], row['distance_km'], row['estimated_duration_hours'], 
                      row['toll_cost'], row['difficulty_level'], row['difficulty_level'], row['toll_cost']))

            # 3. Cargar dim_vehicle (Actualiza edad y mantenimiento)
            vehicles = df[['vehicle_id', 'license_plate', 'vehicle_type', 'vehicle_capacity', 
                           'fuel_type', 'acquisition_date', 'age_months', 'vehicle_status', 
                           'last_maintenance_date']].drop_duplicates('vehicle_id')
                           
            for _, row in vehicles.iterrows():
                cursor.execute("""
                    MERGE INTO dim_vehicle v
                    USING (SELECT %s as vehicle_id) s
                    ON v.vehicle_id = s.vehicle_id
                    WHEN NOT MATCHED THEN
                        INSERT (vehicle_id, license_plate, vehicle_type, capacity_kg, fuel_type, acquisition_date, age_months, status, last_maintenance_date, valid_from, valid_to, is_current)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_DATE(), '9999-12-31', TRUE)
                    WHEN MATCHED THEN
                        UPDATE SET age_months = %s, last_maintenance_date = %s, status = %s
                """, (row['vehicle_id'], row['vehicle_id'], row['license_plate'], row['vehicle_type'], 
                      row['vehicle_capacity'], row['fuel_type'], row['acquisition_date'], row['age_months'], 
                      row['vehicle_status'], row['last_maintenance_date'], 
                      row['age_months'], row['last_maintenance_date'], row['vehicle_status']))

            # 4. Cargar dim_driver (SCD Tipo 2 ajustado para no usar la staging table)
            drivers = df[['driver_id', 'employee_code', 'driver_name', 'license_number', 
                          'license_expiry', 'phone', 'hire_date', 'experience_months', 
                          'driver_status', 'performance_category']].drop_duplicates('driver_id')
                          
            for _, row in drivers.iterrows():
                # A. Cerrar registro histórico si cambió el rendimiento o estado
                cursor.execute("""
                    UPDATE dim_driver 
                    SET valid_to = CURRENT_DATE() - 1, is_current = FALSE
                    WHERE driver_id = %s AND is_current = TRUE 
                      AND (performance_category != %s OR status != %s)
                """, (row['driver_id'], row['performance_category'], row['driver_status']))
                
                # B. Insertar nuevo registro (o el inicial si no existe)
                cursor.execute("""
                    MERGE INTO dim_driver d
                    USING (SELECT %s as driver_id, TRUE as is_current) s
                    ON d.driver_id = s.driver_id AND d.is_current = s.is_current
                    WHEN NOT MATCHED THEN
                        INSERT (driver_id, employee_code, full_name, license_number, license_expiry, phone, hire_date, experience_months, status, performance_category, valid_from, valid_to, is_current)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_DATE(), '9999-12-31', TRUE)
                """, (row['driver_id'], row['driver_id'], row['employee_code'], row['driver_name'], 
                      row['license_number'], row['license_expiry'], row['phone'], row['hire_date'], 
                      row['experience_months'], row['driver_status'], row['performance_category']))

            # ==========================================
            # --- FIN DE CAMBIOS ---
            # ==========================================
            
            self.sf_conn.commit()
            logging.info(" Dimensiones actualizadas")
            
        except Exception as e:
            logging.error(f" Error cargando dimensiones: {e}")
            self.sf_conn.rollback()
            self.metrics['errors'] += 1
    
    def load_facts(self, df: pd.DataFrame):
        """Cargar hechos en Snowflake resolviendo Surrogate Keys"""
        logging.info(" Cargando tabla de hechos...")
        
        # Necesitamos write_pandas de la librería oficial de Snowflake
        from snowflake.connector.pandas_tools import write_pandas
        
        cursor = self.sf_conn.cursor()
        
        try:
            # 1. Crear claves de fecha y tiempo directamente en el DataFrame para facilitar la subida
            df['date_key'] = pd.to_datetime(df['scheduled_datetime']).dt.strftime('%Y%m%d').astype(int)
            df['scheduled_time_key'] = pd.to_datetime(df['scheduled_datetime']).dt.hour * 100
            df['delivered_time_key'] = pd.to_datetime(df['delivered_datetime']).dt.hour * 100
            
            # 2. Subir el DataFrame a una tabla temporal en Snowflake
            cursor.execute("""
                CREATE TEMPORARY TABLE temp_fact_staging (
                    date_key INT, scheduled_time_key INT, delivered_time_key INT,
                    customer_name VARCHAR, vehicle_id INT, driver_id INT, route_id INT,
                    delivery_id INT, trip_id INT, tracking_number VARCHAR,
                    package_weight_kg FLOAT, distance_km FLOAT, fuel_consumed_liters FLOAT,
                    delivery_time_minutes FLOAT, delay_minutes FLOAT, deliveries_per_hour FLOAT,
                    fuel_efficiency_km_per_liter FLOAT, cost_per_delivery FLOAT, revenue_per_delivery FLOAT,
                    is_on_time BOOLEAN, is_damaged BOOLEAN, has_signature BOOLEAN, delivery_status VARCHAR
                )
            """)
            
            # Seleccionamos solo las columnas necesarias para el staging
            cols_to_upload = ['date_key', 'scheduled_time_key', 'delivered_time_key', 'customer_name',
                              'vehicle_id', 'driver_id', 'route_id', 'delivery_id', 'trip_id', 'tracking_number',
                              'package_weight_kg', 'distance_km', 'fuel_consumed_liters', 'delivery_time_minutes',
                              'delay_minutes', 'deliveries_per_hour', 'fuel_efficiency_km_per_liter',
                              'cost_per_delivery', 'revenue_per_delivery', 'is_on_time', 
                              'recipient_signature', 'delivery_status']
            
            # is_damaged no existe en el origen, la pasamos como False por defecto
            df_staging = df[cols_to_upload].copy()
            df_staging['is_damaged'] = df_staging['delivery_status'].isin(['damaged', 'failed'])
            df_staging.rename(columns={'recipient_signature': 'has_signature'}, inplace=True)
            
            # --- NUEVO: Convertir columnas a mayúsculas y limpiar índice ---
            df_staging.columns = df_staging.columns.str.upper()
            df_staging = df_staging.reset_index(drop=True)
            # ---------------------------------------------------------------
            
            # Subida ultra rápida de pandas a Snowflake
            success, num_chunks, num_rows, output = write_pandas(self.sf_conn, df_staging, 'TEMP_FACT_STAGING')
            
            if not success:
                raise Exception("Fallo la subida a temp_fact_staging con write_pandas")

            # 3. Insertar a fact_deliveries resolviendo las Keys mediante JOINs
            cursor.execute(f"""
                INSERT INTO fact_deliveries (
                    date_key, scheduled_time_key, delivered_time_key,
                    vehicle_key, driver_key, route_key, customer_key,
                    delivery_id, trip_id, tracking_number,
                    package_weight_kg, distance_km, fuel_consumed_liters,
                    delivery_time_minutes, delay_minutes, deliveries_per_hour,
                    fuel_efficiency_km_per_liter, cost_per_delivery, revenue_per_delivery,
                    is_on_time, is_damaged, has_signature, delivery_status,
                    etl_batch_id
                )
                SELECT 
                    s.date_key, s.scheduled_time_key, s.delivered_time_key,
                    v.vehicle_key, d.driver_key, r.route_key, c.customer_key,
                    s.delivery_id, s.trip_id, s.tracking_number,
                    s.package_weight_kg, s.distance_km, s.fuel_consumed_liters,
                    s.delivery_time_minutes, s.delay_minutes, s.deliveries_per_hour,
                    s.fuel_efficiency_km_per_liter, s.cost_per_delivery, s.revenue_per_delivery,
                    s.is_on_time, s.is_damaged, s.has_signature, s.delivery_status,
                    {self.batch_id}
                FROM temp_fact_staging s
                -- Búsqueda (Lookup) de las Surrogate Keys en las Dimensiones
                LEFT JOIN dim_vehicle v ON s.vehicle_id = v.vehicle_id AND v.is_current = TRUE
                LEFT JOIN dim_driver d ON s.driver_id = d.driver_id AND d.is_current = TRUE
                LEFT JOIN dim_route r ON s.route_id = r.route_id
                LEFT JOIN dim_customer c ON s.customer_name = c.customer_name;
            """)
            
            self.sf_conn.commit()
            
            # Obtener registros insertados desde el cursor de Snowflake
            records_inserted = cursor.rowcount
            self.metrics['records_loaded'] = records_inserted
            logging.info(f" Cargados {records_inserted} registros en fact_deliveries resolviendo Keys")
            
        except Exception as e:
            logging.error(f" Error cargando hechos: {e}")
            self.sf_conn.rollback()
            self.metrics['errors'] += 1
    
    def run_etl(self):
        """Ejecutar pipeline ETL completo"""
        start_time = datetime.now()
        logging.info(f" Iniciando ETL - Batch ID: {self.batch_id}")
        
        try:
            # Conectar
            if not self.connect_databases():
                return
            
            # ETL
            df = self.extract_daily_data()
            if not df.empty:
                df_transformed = self.transform_data(df)
                if not df_transformed.empty:
                    self.load_dimensions(df_transformed)
                    self.load_facts(df_transformed)
            
            # Calcular totales para reportes
            self._calculate_daily_totals()
            
            # Cerrar conexiones
            self.close_connections()
            
            # Log final
            duration = (datetime.now() - start_time).total_seconds()
            logging.info(f" ETL completado en {duration:.2f} segundos")
            logging.info(f" Métricas: {json.dumps(self.metrics, indent=2)}")
            
        except Exception as e:
            logging.error(f" Error fatal en ETL: {e}")
            self.metrics['errors'] += 1
            self.close_connections()
    
    def _calculate_daily_totals(self):
        """Pre-calcular totales para reportes rápidos"""
        cursor = self.sf_conn.cursor()
        
        try:
            # Crear tabla de totales si no existe
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS agg_daily_summary (
                    summary_date DATE,
                    total_deliveries INT,
                    total_revenue DECIMAL(15,2),
                    total_cost DECIMAL(15,2),
                    total_weight_kg DECIMAL(15,2),
                    total_distance_km DECIMAL(15,2),
                    on_time_deliveries INT,
                    etl_batch_id INT,
                    calculated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
                );
            """)
            
            # Insertar totales del día
            cursor.execute("""
                INSERT INTO agg_daily_summary (
                    summary_date, total_deliveries, total_revenue, total_cost, 
                    total_weight_kg, total_distance_km, on_time_deliveries, etl_batch_id
                )
                SELECT 
                    d.full_date,
                    COUNT(f.delivery_id),
                    COALESCE(SUM(f.revenue_per_delivery), 0),
                    COALESCE(SUM(f.cost_per_delivery), 0),
                    COALESCE(SUM(f.package_weight_kg), 0),
                    COALESCE(SUM(f.distance_km), 0),
                    COUNT_IF(f.is_on_time),
                    f.etl_batch_id
                FROM fact_deliveries f
                JOIN dim_date d ON f.date_key = d.date_key
                WHERE f.etl_batch_id = %s
                GROUP BY 
                    d.full_date, 
                    f.etl_batch_id;
            """, (self.batch_id,))
            
            self.sf_conn.commit()
            logging.info(" Totales diarios calculados")
            
        except Exception as e:
            logging.error(f" Error calculando totales: {e}")
            self.sf_conn.rollback() # Práctica de seguridad: revertir en caso de error
    
    def close_connections(self):
        """Cerrar conexiones a bases de datos"""
        if self.pg_conn:
            self.pg_conn.close()
        if self.sf_conn:
            self.sf_conn.close()
        logging.info(" Conexiones cerradas")

def job():
    """Función para programar con schedule"""
    etl = FleetLogixETL()
    etl.run_etl()

def main():
    """Función principal - Automatización diaria"""
    logging.info(" Pipeline ETL FleetLogix iniciado")
    
    # Programar ejecución diaria a las 2:00 AM
    schedule.every().day.at("02:00").do(job)
    
    logging.info(" ETL programado para ejecutarse diariamente a las 2:00 AM")
    logging.info("Presiona Ctrl+C para detener")
    
    # Ejecutar una vez al inicio (para pruebas)
    job()
    
    # Loop infinito esperando la hora programada
    while True:
        schedule.run_pending()
        time.sleep(60)  # Verificar cada minuto

if __name__ == "__main__":
    main()