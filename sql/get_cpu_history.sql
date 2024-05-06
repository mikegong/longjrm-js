SELECT server_name,
       vmstat_r,
       vmstat_q,
       vmstat_wa,
       vmstat_pc,
       vmstat_ec,
       cpu_load_medium,
       TIMESTAMP(create_ts, 0) CREATE_TIME
FROM   pcm.db_server_vmstat_usage
WHERE  server_name = ?
       AND create_ts >= ?
       AND create_ts <= DATE(CAST(? as varchar)) + 1 DAY
ORDER  BY create_ts