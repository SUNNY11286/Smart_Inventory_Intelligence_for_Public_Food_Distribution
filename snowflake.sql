
CREATE DATABASE IF NOT EXISTS AI_FOR_GOOD_DB;
USE DATABASE AI_FOR_GOOD_DB;

CREATE SCHEMA IF NOT EXISTS INVENTORY;
USE SCHEMA INVENTORY;

USE WAREHOUSE COMPUTE_WH;



CREATE OR REPLACE TABLE inventory_daily (
    date DATE,

    location_id STRING,
    location_name STRING,
    location_type STRING,
    region STRING,

    item_id STRING,
    item_name STRING,
    category STRING,
    is_essential BOOLEAN,

    opening_stock NUMBER,
    received_qty NUMBER,
    issued_qty NUMBER,
    closing_stock NUMBER,

    lead_time_days NUMBER,

    expiry_date DATE,
    unit_price NUMBER,
    total_stock_value NUMBER,

    data_source STRING
);




INSERT INTO inventory_daily VALUES
(
  '2026-01-01','LOC01','Ahmedabad Ration Center','Ration Center','Gujarat',
  'ITEM01','Rice','Grain',TRUE,
  1200,300,450,1050,
  5,'2026-02-10',42,44100,
  'Kaggle + FAO'
),
(
  '2026-01-01','LOC02','Surat NGO Warehouse','NGO','Gujarat',
  'ITEM02','Wheat','Grain',TRUE,
  800,100,300,600,
  5,'2026-02-05',38,22800,
  'Kaggle + FAO'
),
(
  '2026-01-01','LOC03','Vadodara Relief Camp','Relief Camp','Gujarat',
  'ITEM03','Pulses','Pulse',TRUE,
  500,50,200,350,
  7,'2026-01-25',90,31500,
  'Kaggle + FAO'
);

SELECT * FROM inventory_daily;


























CREATE OR REPLACE TABLE dt_consumption_metrics (
    location_id STRING,
    location_name STRING,
    item_id STRING,
    item_name STRING,
    category STRING,
    avg_daily_consumption NUMBER
);



CREATE OR REPLACE TABLE dt_inventory_health (
    date DATE,
    location_id STRING,
    location_name STRING,
    region STRING,
    item_id STRING,
    item_name STRING,
    category STRING,
    is_essential BOOLEAN,
    closing_stock NUMBER,
    avg_daily_consumption NUMBER,
    days_of_stock_remaining NUMBER,
    lead_time_days NUMBER,
    stock_status STRING,
    recommended_reorder_qty NUMBER,
    days_to_expiry NUMBER,
    unit_price NUMBER,
    total_stock_value NUMBER
);











CREATE OR REPLACE TASK task_refresh_consumption_metrics
WAREHOUSE = COMPUTE_WH
SCHEDULE = 'USING CRON 0 2 * * * UTC'
AS
INSERT OVERWRITE INTO dt_consumption_metrics
SELECT
    location_id,
    location_name,
    item_id,
    item_name,
    category,
    AVG(issued_qty) AS avg_daily_consumption
FROM inventory_daily
GROUP BY
    location_id,
    location_name,
    item_id,
    item_name,
    category;




CREATE OR REPLACE TASK task_refresh_inventory_health
WAREHOUSE = COMPUTE_WH
SCHEDULE = 'USING CRON 5 2 * * * UTC'
AS
INSERT OVERWRITE INTO dt_inventory_health
SELECT
    i.date,
    i.location_id,
    i.location_name,
    i.region,
    i.item_id,
    i.item_name,
    i.category,
    i.is_essential,
    i.closing_stock,
    c.avg_daily_consumption,

    CASE
        WHEN c.avg_daily_consumption = 0 THEN NULL
        ELSE ROUND(i.closing_stock / c.avg_daily_consumption, 1)
    END AS days_of_stock_remaining,

    i.lead_time_days,

    CASE
        WHEN c.avg_daily_consumption = 0 THEN 'Healthy'
        WHEN (i.closing_stock / c.avg_daily_consumption) <= i.lead_time_days THEN 'Critical'
        WHEN (i.closing_stock / c.avg_daily_consumption) <= (i.lead_time_days + 2) THEN 'At Risk'
        ELSE 'Healthy'
    END AS stock_status,

    CASE
        WHEN c.avg_daily_consumption = 0 THEN 0
        ELSE GREATEST(
            ROUND((c.avg_daily_consumption * (i.lead_time_days + 2)) - i.closing_stock),
            0
        )
    END AS recommended_reorder_qty,

    DATEDIFF('day', CURRENT_DATE, i.expiry_date) AS days_to_expiry,
    i.unit_price,
    i.total_stock_value
FROM inventory_daily i
LEFT JOIN dt_consumption_metrics c
  ON i.location_id = c.location_id
 AND i.item_id = c.item_id;







ALTER TASK task_refresh_consumption_metrics RESUME;
ALTER TASK task_refresh_inventory_health RESUME;

EXECUTE TASK task_refresh_consumption_metrics;
EXECUTE TASK task_refresh_inventory_health;




CREATE OR REPLACE TABLE inventory_alerts (
    alert_id STRING,
    alert_date TIMESTAMP,
    location_id STRING,
    location_name STRING,
    item_id STRING,
    item_name STRING,
    stock_status STRING,
    days_of_stock_remaining NUMBER,
    recommended_reorder_qty NUMBER,
    days_to_expiry NUMBER,
    alert_type STRING,        -- STOCK_OUT / EXPIRY
    severity STRING,          -- HIGH / MEDIUM
    alert_message STRING
);



CREATE OR REPLACE STREAM stream_inventory_health
ON TABLE dt_inventory_health
APPEND_ONLY = FALSE;




CREATE OR REPLACE TASK task_generate_inventory_alerts
WAREHOUSE = COMPUTE_WH
SCHEDULE = 'USING CRON 10 2 * * * UTC'
AS
INSERT INTO inventory_alerts
SELECT
    UUID_STRING() AS alert_id,
    CURRENT_TIMESTAMP AS alert_date,
    location_id,
    location_name,
    item_id,
    item_name,
    stock_status,
    days_of_stock_remaining,
    recommended_reorder_qty,
    days_to_expiry,

    'STOCK_OUT' AS alert_type,
    'HIGH' AS severity,

    CONCAT(
        'Critical stock alert: ',
        item_name,
        ' at ',
        location_name,
        ' may run out in ',
        days_of_stock_remaining,
        ' days. Recommended reorder quantity: ',
        recommended_reorder_qty
    ) AS alert_message

FROM stream_inventory_health
WHERE stock_status = 'Critical'
  AND METADATA$ACTION IN ('INSERT','UPDATE');





ALTER TASK task_generate_inventory_alerts RESUME;

EXECUTE TASK task_generate_inventory_alerts;


SHOW TASKS IN SCHEMA AI_FOR_GOOD_DB.INVENTORY;




SELECT *
FROM inventory_alerts
ORDER BY alert_date DESC;



SELECT
  location_name,
  item_name,
  closing_stock,
  avg_daily_consumption,
  days_of_stock_remaining,
  lead_time_days,
  stock_status
FROM dt_inventory_health;


SELECT *
FROM stream_inventory_health;





UPDATE inventory_daily
SET closing_stock = 50
WHERE item_name = 'Rice'
  AND location_name = 'Ahmedabad Ration Center';



EXECUTE TASK task_refresh_consumption_metrics;
EXECUTE TASK task_refresh_inventory_health;



SELECT
  location_name,
  item_name,
  days_of_stock_remaining,
  lead_time_days,
  stock_status
FROM dt_inventory_health
WHERE item_name = 'Rice';



EXECUTE TASK task_generate_inventory_alerts;



SELECT *
FROM inventory_alerts
ORDER BY alert_date DESC;






CREATE OR REPLACE DYNAMIC TABLE dt_consumption_metrics
TARGET_LAG = '1 day'
WAREHOUSE = COMPUTE_WH
AS
SELECT
    location_id,
    location_name,
    item_id,
    item_name,
    category,
    AVG(issued_qty) AS avg_daily_consumption
FROM inventory_daily
GROUP BY
    location_id,
    location_name,
    item_id,
    item_name,
    category;







CREATE OR REPLACE VIEW inventory_risk_summary AS
SELECT
    COUNT(*) AS critical_item_count,
    COUNT(DISTINCT location_name) AS impacted_locations,
    LISTAGG(
        item_name || ' at ' || location_name,
        '; '
    ) WITHIN GROUP (ORDER BY location_name) AS issue_details,
    SUM(recommended_reorder_qty * unit_price) AS estimated_reorder_cost
FROM AI_FOR_GOOD_DB.INVENTORY.DT_INVENTORY_HEALTH
WHERE stock_status = 'Critical';












CREATE OR REPLACE TABLE storage_units (
    storage_id STRING,
    storage_name STRING,
    region STRING,
    latitude FLOAT,
    longitude FLOAT
);


INSERT INTO storage_units VALUES
('ST01','Surat Central Warehouse','Gujarat',21.1702,72.8311),
('ST02','Vadodara Central Warehouse','Gujarat',22.3072,73.1812),
('ST03','Ahmedabad Central Warehouse','Gujarat',23.0225,72.5714);



CREATE OR REPLACE TABLE storage_inventory (
    storage_id STRING,
    item_name STRING,
    available_qty NUMBER
);

INSERT INTO storage_inventory VALUES
('ST01','Rice',3000),
('ST01','Wheat',2500),
('ST02','Pulses',1800),
('ST03','Rice',1200);



select * from storage_units;
select * from storage_inventory;




CREATE OR REPLACE VIEW all_locations AS
SELECT
    storage_id AS location_id,
    storage_name AS location_name,
    'STORAGE' AS location_type,
    region,
    latitude,
    longitude
FROM storage_units

UNION ALL

SELECT DISTINCT
    location_id,
    location_name,
    'DISTRIBUTION_CENTER' AS location_type,
    region,

    -- approximate city-level coordinates (demo-safe)
    CASE location_name
        WHEN 'Ahmedabad Ration Center' THEN 23.0225
        WHEN 'Surat NGO Warehouse' THEN 21.1702
        WHEN 'Vadodara Relief Camp' THEN 22.3072
    END AS latitude,

    CASE location_name
        WHEN 'Ahmedabad Ration Center' THEN 72.5714
        WHEN 'Surat NGO Warehouse' THEN 72.8311
        WHEN 'Vadodara Relief Camp' THEN 73.1812
    END AS longitude

FROM inventory_daily;



CREATE OR REPLACE VIEW closest_storage_for_item AS
SELECT
    h.location_name AS demand_location,
    h.item_name,

    s.location_name AS source_storage,
    si.available_qty,

    ROUND(
        SQRT(
            POWER(d.latitude - s.latitude, 2) +
            POWER(d.longitude - s.longitude, 2)
        ) * 111,
        2
    ) AS approx_distance_km

FROM dt_inventory_health h

JOIN all_locations d
    ON h.location_name = d.location_name
   AND d.location_type = 'DISTRIBUTION_CENTER'

JOIN storage_inventory si
    ON h.item_name = si.item_name

JOIN all_locations s
    ON si.storage_id = s.location_id
   AND s.location_type = 'STORAGE'

WHERE h.stock_status = 'Critical'
  AND si.available_qty > 0;







CREATE OR REPLACE VIEW recommended_source AS
SELECT *
FROM (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY demand_location, item_name
            ORDER BY approx_distance_km ASC
        ) AS rn
    FROM closest_storage_for_item
)
WHERE rn = 1;










CREATE OR REPLACE VIEW inventory_health_with_source AS
SELECT
    h.location_name,
    h.item_name,
    h.stock_status,

    r.source_storage,
    r.approx_distance_km
FROM dt_inventory_health h
LEFT JOIN recommended_source r
  ON h.location_name = r.demand_location
 AND h.item_name = r.item_name;








CREATE OR REPLACE VIEW enhanced_inventory_alerts AS
SELECT
    CURRENT_TIMESTAMP AS alert_time,
    h.location_name,
    h.item_name,
    h.days_of_stock_remaining,
    h.recommended_reorder_qty,

    r.source_storage,
    r.approx_distance_km,

    CONCAT(
        'Critical: ',
        h.item_name,
        ' at ',
        h.location_name,
        '. Order ',
        h.recommended_reorder_qty,
        ' units from ',
        r.source_storage,
        ' (',
        ROUND(r.approx_distance_km,1),
        ' km away).'
    ) AS alert_message

FROM dt_inventory_health h
LEFT JOIN recommended_source r
  ON h.location_name = r.demand_location
 AND h.item_name = r.item_name
WHERE h.stock_status = 'Critical';



















CREATE OR REPLACE VIEW closest_storage_for_item AS
SELECT
    h.location_name AS demand_location,
    h.item_name,

    s.location_name AS source_storage,
    si.available_qty,

    ROUND(
        SQRT(
            POWER(d.latitude - s.latitude, 2) +
            POWER(d.longitude - s.longitude, 2)
        ) * 111,
        2
    ) AS approx_distance_km

FROM dt_inventory_health h

-- Demand location coordinates
JOIN all_locations d
    ON h.location_name = d.location_name
   AND d.location_type = 'DISTRIBUTION_CENTER'

-- Storage inventory (must have stock)
JOIN storage_inventory si
    ON h.item_name = si.item_name
   AND si.available_qty > 0

-- Storage unit coordinates
JOIN all_locations s
    ON si.storage_id = s.location_id
   AND s.location_type = 'STORAGE'

WHERE h.stock_status = 'Critical'

-- 🔴 KEY FIX: exclude same location / same city
AND s.location_name <> h.location_name;







CREATE OR REPLACE VIEW recommended_source AS
SELECT *
FROM (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY demand_location, item_name
            ORDER BY approx_distance_km ASC
        ) AS rn
    FROM closest_storage_for_item
)
WHERE rn = 1;















-- SELECT *
-- FROM recommended_source;


CREATE OR REPLACE VIEW sourcing_transparency AS
SELECT
    h.location_name        AS demand_location,
    h.item_name,

    s.location_name        AS storage_location,
    si.available_qty,

    ROUND(
        SQRT(
            POWER(d.latitude - s.latitude, 2) +
            POWER(d.longitude - s.longitude, 2)
        ) * 111,
        2
    ) AS approx_distance_km

FROM dt_inventory_health h

JOIN all_locations d
    ON h.location_name = d.location_name
   AND d.location_type = 'DISTRIBUTION_CENTER'

JOIN storage_inventory si
    ON h.item_name = si.item_name

JOIN all_locations s
    ON si.storage_id = s.location_id
   AND s.location_type = 'STORAGE'

WHERE h.stock_status = 'Critical';





CREATE OR REPLACE VIEW recommended_source AS
SELECT *
FROM (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY demand_location, item_name
            ORDER BY approx_distance_km ASC
        ) AS rn
    FROM closest_storage_for_item
)
WHERE rn = 1;









UPDATE storage_units
SET
    latitude  = latitude  + 0.03,
    longitude = longitude + 0.02
WHERE storage_name = 'Ahmedabad Central Warehouse';

UPDATE storage_units
SET
    latitude  = latitude  - 0.01,
    longitude = longitude + 0.05
WHERE storage_name = 'Surat Central Warehouse';

UPDATE storage_units
SET
    latitude  = latitude  + 0.025,
    longitude = longitude - 0.02
WHERE storage_name = 'Vadodara Central Warehouse';




SELECT *
FROM recommended_source;








CREATE OR REPLACE VIEW inventory_action_summary AS
SELECT
    h.location_name,
    h.item_name,
    h.days_of_stock_remaining,
    h.recommended_reorder_qty,

    r.source_storage,
    r.approx_distance_km,

    CASE
        WHEN r.source_storage IS NOT NULL THEN
            CONCAT(
                h.item_name,
                ' is critical at ',
                h.location_name,
                '. Recommended redistribution from ',
                r.source_storage,
                ' (approx ',
                ROUND(r.approx_distance_km, 1),
                ' km away).'
            )
        ELSE
            CONCAT(
                h.item_name,
                ' is critical at ',
                h.location_name,
                '. No external stock available; procurement required.'
            )
    END AS summary_text

FROM dt_inventory_health h
LEFT JOIN recommended_source r
  ON h.location_name = r.demand_location
 AND h.item_name = r.item_name
WHERE h.stock_status = 'Critical';








CREATE OR REPLACE TABLE data_update_log (
    last_updated TIMESTAMP
);

INSERT INTO data_update_log VALUES (CURRENT_TIMESTAMP);

select * from data_update_log;






















INSERT INTO storage_units (storage_id, storage_name, region, latitude, longitude) VALUES
('ST04','Gandhinagar Warehouse','Gujarat',23.2156,72.6369),
('ST05','Rajkot Warehouse','Gujarat',22.3039,70.8022),
('ST06','Bhavnagar Warehouse','Gujarat',21.7645,72.1519),
('ST07','Jamnagar Warehouse','Gujarat',22.4707,70.0577),
('ST08','Junagadh Warehouse','Gujarat',21.5222,70.4579),
('ST09','Anand Warehouse','Gujarat',22.5645,72.9289),
('ST10','Nadiad Warehouse','Gujarat',22.6916,72.8634),
('ST11','Mehsana Warehouse','Gujarat',23.5880,72.3693),
('ST12','Palanpur Warehouse','Gujarat',24.1722,72.4342),
('ST13','Himmatnagar Warehouse','Gujarat',23.5986,72.9666),
('ST14','Morbi Warehouse','Gujarat',22.8173,70.8377),
('ST15','Surendranagar Warehouse','Gujarat',22.7201,71.6495),
('ST16','Bharuch Warehouse','Gujarat',21.7051,72.9959),
('ST17','Valsad Warehouse','Gujarat',20.5992,72.9342),
('ST18','Navsari Warehouse','Gujarat',20.9467,72.9520),
('ST19','Godhra Warehouse','Gujarat',22.7788,73.6143),
('ST20','Dahod Warehouse','Gujarat',22.8379,74.2531),
('ST21','Patan Warehouse','Gujarat',23.8507,72.1266),
('ST22','Porbandar Warehouse','Gujarat',21.6417,69.6293),
('ST23','Amreli Warehouse','Gujarat',21.5995,71.2114),
('ST24','Botad Warehouse','Gujarat',22.1704,71.6663),
('ST25','Kutch Warehouse','Gujarat',23.7337,69.8597),
('ST26','Dwarka Warehouse','Gujarat',22.2442,68.9685),
('ST27','Chhota Udaipur Warehouse','Gujarat',22.3046,74.0119),
('ST28','Modasa Warehouse','Gujarat',23.4643,73.2986),
('ST29','Kalol Warehouse','Gujarat',23.2464,72.5087),
('ST30','Sanand Warehouse','Gujarat',22.9924,72.3816);




INSERT INTO storage_inventory (storage_id, item_name, available_qty) VALUES
('ST04','Rice',1800), ('ST04','Wheat',1200), ('ST04','Pulses',900),
('ST05','Rice',2500), ('ST05','Wheat',2000),
('ST06','Rice',1400), ('ST06','Pulses',1100),
('ST07','Wheat',1600), ('ST07','Pulses',1300),
('ST08','Rice',900),  ('ST08','Wheat',700),
('ST09','Rice',1000), ('ST09','Pulses',800),
('ST10','Rice',1200), ('ST10','Wheat',900),
('ST11','Rice',1600), ('ST11','Pulses',1400),
('ST12','Wheat',2000),
('ST13','Rice',1100), ('ST13','Pulses',1000),
('ST14','Rice',1300), ('ST14','Wheat',1100),
('ST15','Pulses',900),
('ST16','Rice',1700), ('ST16','Wheat',1500),
('ST17','Rice',800),
('ST18','Wheat',1200),
('ST19','Rice',1000), ('ST19','Pulses',600),
('ST20','Wheat',900),
('ST21','Rice',1400), ('ST21','Pulses',1200),
('ST22','Rice',700),
('ST23','Wheat',1000),
('ST24','Rice',900), ('ST24','Pulses',700),
('ST25','Rice',2000), ('ST25','Wheat',1800),
('ST26','Rice',600),
('ST27','Pulses',500),
('ST28','Rice',1100), ('ST28','Wheat',1000),
('ST29','Rice',1300),
('ST30','Rice',900), ('ST30','Pulses',800);
