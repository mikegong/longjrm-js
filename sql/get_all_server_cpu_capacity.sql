WITH
  v0 AS (
SELECT
    a.server_name,
    a.server_id,
    c.database_id,
    d.app_platform,
    c.database_name
  FROM
    long.server a
  LEFT JOIN
    long.instance b
  ON
    (a.server_id=b.server_id)
  LEFT JOIN
    long.database c
  ON
    (b.instance_id=c.instance_id)
  left join long.database_base d on (c.database_base_id = d.database_base_id)
  WHERE
    a.env_server='PROD'
    AND b.env_instance='PROD'
    AND UPPER(database_name) NOT IN ('SAMPLE',
      'TEST01')),
  v1 AS (
  SELECT
    server_id,
    listagg(distinct DATABASE_NAME,
      ', ') AS DATABASE_LIST,
    listagg(distinct app_platform,
      ', ') AS APP_PLATFORM
  FROM
    v0
  GROUP BY
    server_id),
  v3_cpu_usage AS (
  SELECT
    a.server_id,
    server_name,
    VMSTAT_PC,
    VMSTAT_EC,
    (VMSTAT_US+VMSTAT_SY) AS CPU_USAGE,
    CASE
      WHEN p_cpu_number > 0 THEN CAST((VMSTAT_US+VMSTAT_SY)*(VMSTAT_PC/P_CPU_NUMBER) AS decimal(6, 0))
    ELSE
    0
  END
    AS real_cpu_Usage,
    b.P_CPU_NUMBER,
    CAST(double(b.MEMORY_SIZE)/1024 AS decimal(13,
        2)) AS MEMORY_SIZE,
    SUBSTR(create_ts,1,16) AS date_timestamp
  FROM
    PCM.DB_SERVER_VMSTAT_USAGE a
  LEFT JOIN (
    SELECT
      SERVER_ID,
      P_CPU_NUMBER,
      MEMORY_SIZE
    FROM (
      SELECT
        SERVER_ID,
        LCPU_NUMBER AS P_CPU_NUMBER,
        MEMORY_SIZE,
        ROW_NUMBER() OVER (PARTITION BY server_ID ORDER BY CREATE_TS DESC) AS row_num
      FROM
        PCM.SERVER_CPU_MEM_DISK_MAPPING) AS T
    WHERE
      row_num=1) b
  ON
    (a.server_id=b.server_id)
  WHERE
    a.CREATE_TS BETWEEN '$time_from$'
    AND '$time_end$'
  ) , 
v4 as (SELECT
  --ROW_NUMBER() OVER () row_Num,
  a.SERVER_ID,
  SERVER_NAME,
  b.APP_PLATFORM,
  b.DATABASE_LIST,
  real_cpu_Usage,
  CASE
    WHEN real_cpu_Usage >=80 THEN 1
  ELSE
  0
END
  AS high_cpu,
  DATE_TIMESTAMP
FROM
  v3_cpu_usage a
LEFT JOIN
  v1 AS b
ON
  (a.server_id=b.server_id))
  
  select 
         ROW_NUMBER() OVER () row_Num,
         SERVER_ID,
         APP_PLATFORM,
         SERVER_NAME,
         DATABASE_LIST as DATABASE_NAME,
         case when count(*) >0 then cast((double(sum(high_cpu))/double(count(*)))*100 as decimal(13,2)) else 0 end as HIGH_CPU_FREQ
         from v4 
         group by 
         SERVER_ID,
         APP_PLATFORM,
         SERVER_NAME,
         DATABASE_LIST
         order by HIGH_CPU_FREQ desc;