SELECT
    machine_id,
    total_measurements,
    total_alerts
FROM "industrypulse"."analytics"."mart_machine_performance"
WHERE
    total_measurements < 0
    OR total_alerts < 0
    OR total_alerts > total_measurements