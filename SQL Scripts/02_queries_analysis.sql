-------Conteo de vehiculos por tipo

SELECT 
    vehicle_type AS "Tipo de Vehículo",
    COUNT(*) AS "Cantidad",
    -- Opcional: Calculamos el porcentaje del total
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM vehicles), 2) || '%' AS "Porcentaje"
FROM vehicles
GROUP BY vehicle_type
ORDER BY "Cantidad" DESC;

--------------Conductores cuya licencia esta proxima a vencer
  
SELECT 
    driver_id,
    first_name || ' ' || last_name AS "Conductor",
    license_number AS "Licencia",
    license_expiry AS "Fecha Vencimiento",
    status,
    -- Columna extra: Cálculo automático de días faltantes
    (license_expiry - '2026-02-02'::date) AS "Dias_para_Vencer"
FROM drivers
WHERE license_expiry BETWEEN '2026-02-02' AND ('2026-02-02'::date + INTERVAL '60 days')
ORDER BY license_expiry ASC;

------------Total de viajes por Estado

SELECT 
    status AS "Estado del Viaje",
    COUNT(*) AS "Total Viajes"
FROM trips
GROUP BY status
ORDER BY "Total Viajes" DESC;

----------------------Querys 4 y 5

-------------4. Query 4: Total de entregas por ciudad (Últimos 60 días)

SELECT 
    r.destination_city AS "Ciudad Destino",
    COUNT(d.delivery_id) AS "Total Entregas",
    ROUND(SUM(d.package_weight_kg), 2) AS "Peso Total (kg)",
    -- Extra: Peso promedio por paquete para ver si es carga pesada o liviana
    ROUND(AVG(d.package_weight_kg), 2) AS "Peso Promedio (kg)"
FROM deliveries d
JOIN trips t ON d.trip_id = t.trip_id
JOIN routes r ON t.route_id = r.route_id
-- Filtramos los últimos 60 días
WHERE t.arrival_datetime >= ('2026-02-02'::date - INTERVAL '60 days')
GROUP BY r.destination_city
ORDER BY "Total Entregas" DESC;


------------Query #5 Conductores activos y carga de trabajo

SELECT 
    d.employee_code,
    d.first_name || ' ' || d.last_name AS "Conductor",
    COUNT(t.trip_id) AS "Viajes Completados",
    MAX(t.departure_datetime) AS "Última Salida"
FROM drivers d
LEFT JOIN trips t ON d.driver_id = t.driver_id 
    AND t.status = 'completed' -- << OBLIGATORIO para activar Índice 1
WHERE d.status = 'active'      -- << OBLIGATORIO para activar Índice 4
GROUP BY d.driver_id, d.employee_code, d.first_name, d.last_name
ORDER BY "Viajes Completados" DESC;

-----------------QUERYS COMPLEJAS
--------------------Query #7 Rutas con mayor consumo

SELECT 
    r.route_code,
    r.origin_city || ' -> ' || r.destination_city AS "Ruta",
    -- Cálculo de eficiencia L/100km
    ROUND(CAST(SUM(t.fuel_consumed_liters) / SUM(r.distance_km) * 100 AS numeric), 2) AS "Litros/100km"
FROM routes r
JOIN trips t ON r.route_id = t.route_id
WHERE t.status = 'completed' -- << OBLIGATORIO para activar Índice 1
  -- El optimizador usará el Índice 5 para sacar la distance_km sin leer la tabla routes completa
GROUP BY r.route_id, r.route_code, r.origin_city, r.destination_city
HAVING SUM(r.distance_km) > 0
ORDER BY "Litros/100km" DESC
LIMIT 10;

------------------Query #9 Costo de mantenimiento por km

WITH MaintenanceStats AS (
    -- Usa Índice 3: Escaneo directo por vehicle_id y suma de cost
    SELECT vehicle_id, SUM(cost) as total_maint_cost
    FROM maintenance
    GROUP BY vehicle_id
),
TripStats AS (
    -- Usa Índice 1: Agrupa por vehicle_id filtrando solo 'completed'
    SELECT 
        t.vehicle_id, 
        SUM(r.distance_km) as total_km
    FROM trips t
    JOIN routes r ON t.route_id = r.route_id
    WHERE t.status = 'completed' -- << LLAVE MAESTRA
    GROUP BY t.vehicle_id
)
SELECT 
    v.vehicle_type,
    ROUND(CAST(SUM(ms.total_maint_cost) / NULLIF(SUM(ts.total_km), 0) AS numeric), 4) AS "Costo $/km"
FROM vehicles v
JOIN MaintenanceStats ms ON v.vehicle_id = ms.vehicle_id
JOIN TripStats ts ON v.vehicle_id = ts.vehicle_id
GROUP BY v.vehicle_type;

-----------------Query #10 Ranking de conductores

SELECT 
    RANK() OVER (ORDER BY (SUM(t.fuel_consumed_liters) / SUM(r.distance_km)) ASC) as "Ranking",
    d.first_name || ' ' || d.last_name AS "Conductor",
    ROUND(CAST(SUM(t.fuel_consumed_liters) / SUM(r.distance_km) * 100 AS numeric), 2) AS "Eficiencia L/100km"
FROM drivers d
JOIN trips t ON d.driver_id = t.driver_id
JOIN routes r ON t.route_id = r.route_id
WHERE d.status = 'active'     -- << Activa Índice 4
  AND t.status = 'completed'  -- << Activa Índice 1
GROUP BY d.driver_id, d.first_name, d.last_name
HAVING COUNT(t.trip_id) > 10
LIMIT 20;
