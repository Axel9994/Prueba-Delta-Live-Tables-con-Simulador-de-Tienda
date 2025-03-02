-- Tabla de streaming desde Event Hubs (nivel bronze)
CREATE OR REFRESH STREAMING LIVE TABLE sales_raw
COMMENT "Raw sales data from Event Hubs"
TBLPROPERTIES ("quality" = "bronze")
AS SELECT 
    CAST(body AS STRING) AS json_data
FROM STREAMING EVENTHUBS (
    "eventhubs.connectionString" = "{{secrets/eventhub-secrets/connectionString}}",
    "eventhubs.eventhubName" = "eh-demo"
);

-- Tabla de productos crudos (extraídos de las ventas)
CREATE OR REFRESH STREAMING LIVE TABLE productos_raw
COMMENT "Raw product data extracted from sales"
TBLPROPERTIES ("quality" = "bronze")
AS SELECT DISTINCT
    p.id AS producto_id,
    p.nombre AS nombre,
    p.precio AS precio
FROM STREAMING LIVE sales_raw
CROSS JOIN UNNEST(from_json(json_data, 'timestamp STRING, productos ARRAY<STRUCT<id INT, nombre STRING, precio DOUBLE, cantidad INT>>, metodo_pago STRING, tienda_id INT>').productos) AS p
WHERE p.id IS NOT NULL;

-- Dimensión de Productos (usando MERGE para SCD Tipo 1)
CREATE LIVE TABLE dimProducto
COMMENT "Product dimension with price updates"
TBLPROPERTIES ("quality" = "silver")
AS
MERGE INTO dimProducto AS target
USING (SELECT producto_id, nombre, precio FROM LIVE productos_raw) AS source
ON target.producto_id = source.producto_id
WHEN MATCHED AND target.precio != source.precio THEN
    UPDATE SET target.precio = source.precio
WHEN NOT MATCHED THEN
    INSERT (producto_id, nombre, precio)
    VALUES (source.producto_id, source.nombre, source.precio);

-- Dimensión de Métodos de Pago
CREATE OR REFRESH LIVE TABLE dimMetodoPago
COMMENT "Payment method dimension"
TBLPROPERTIES ("quality" = "silver")
AS SELECT DISTINCT
    ROW_NUMBER() OVER (ORDER BY metodo_pago) AS metodo_pago_id,
    metodo_pago AS nombre
FROM LIVE sales_raw
CROSS JOIN UNNEST(from_json(json_data, 'timestamp STRING, productos ARRAY<STRUCT<id INT, nombre STRING, precio DOUBLE, cantidad INT>>, metodo_pago STRING, tienda_id INT>').*) AS data
WHERE metodo_pago IS NOT NULL;

-- Tabla Silver de Ventas (integrando dimensiones con venta_id)
CREATE OR REFRESH STREAMING LIVE TABLE sales_silver
COMMENT "Sales data with dimension references and unique venta_id"
TBLPROPERTIES ("quality" = "silver")
AS SELECT
    ROW_NUMBER() OVER (ORDER BY data.timestamp) AS venta_id,
    data.timestamp AS sale_timestamp,
    data.tienda_id,
    mp.metodo_pago_id,
    p.producto_id,
    p.cantidad,
    (p.cantidad * dp.precio) AS total_linea
FROM STREAMING LIVE sales_raw
CROSS JOIN UNNEST(from_json(json_data, 'timestamp STRING, productos ARRAY<STRUCT<id INT, nombre STRING, precio DOUBLE, cantidad INT>>, metodo_pago STRING, tienda_id INT>').*) AS data
CROSS JOIN UNNEST(data.productos) AS p
JOIN LIVE dimMetodoPago mp ON mp.nombre = data.metodo_pago
JOIN LIVE dimProducto dp ON dp.producto_id = p.id
WHERE data.timestamp IS NOT NULL;

-- Tabla Dorada: Detalle diario por método de pago y producto
CREATE OR REFRESH LIVE TABLE sales_daily_gold
COMMENT "Daily sales summary by payment method and product"
TBLPROPERTIES ("quality" = "gold")
AS SELECT
    DATE_TRUNC('day', sale_timestamp) AS sale_date,
    metodo_pago_id,
    producto_id,
    SUM(cantidad) AS total_quantity,
    SUM(total_linea) AS total_amount
FROM LIVE sales_silver
GROUP BY 
    DATE_TRUNC('day', sale_timestamp),
    metodo_pago_id,
    producto_id;