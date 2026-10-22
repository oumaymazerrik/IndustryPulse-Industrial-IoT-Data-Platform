
  create view "industrypulse"."analytics"."stg_machine_kpi__dbt_tmp"
    
    
  as (
    -- Préparation des KPI industriels pour les analyses dbt

WITH source_data AS (

    SELECT
        machine_id,
        window_start,
        window_end,
        total_measurements,
        avg_temperature,
        min_temperature,
        max_temperature,
        avg_vibration,
        max_vibration,
        avg_pressure,
        avg_energy,
        total_alerts,
        alert_rate,
        gold_processing_timestamp

    FROM public.machine_kpi

)

SELECT
    *,
    CAST(window_start AS DATE) AS measurement_date

FROM source_data
  );