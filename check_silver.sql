SELECT
    ml_data_quality_status,
    COUNT(*) AS total
FROM lakehouse.silver.machine_sensors_clean
GROUP BY ml_data_quality_status;
