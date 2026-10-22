SELECT
    machine_id,
    COUNT(*) AS total_windows,
    SUM(total_measurements) AS total_measurements,
    ROUND(AVG(avg_temperature)::numeric, 2) AS avg_temperature,
    ROUND(AVG(avg_vibration)::numeric, 2) AS avg_vibration,
    ROUND(AVG(avg_energy)::numeric, 2) AS avg_energy,
    SUM(total_alerts) AS total_alerts
FROM "industrypulse"."analytics"."stg_machine_kpi"
GROUP BY machine_id