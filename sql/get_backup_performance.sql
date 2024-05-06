WITH
  temp_date AS (
  SELECT
    CURRENT timestamp-30 days AS pre_time,
    CURRENT timestamp-1 days AS mid_time
  FROM
    sysibm.sysdummy1 ),
  
  temp_date_str AS (
  SELECT
    SUBSTR(pre_time, 1, 4) || SUBSTR(pre_time, 6, 2) || SUBSTR(pre_time, 9, 2) AS pre_time,
    SUBSTR(mid_time, 1, 4) || SUBSTR(mid_time, 6, 2) || SUBSTR(mid_time, 9, 2) AS mid_time
  FROM
    temp_date )

SELECT 
  SUBSTR(start_time,1,4)||'-'||substr(start_time,5,2)||'-'||substr(start_time,7,2) AS backup_date,
  decimal(((SUM(database_size)/SUM(backup_time))/1024),
    12,
    2) as backup_speed
FROM
  pcm.db_backup_history
WHERE
  SUBSTR(start_time,1,8)>=(select pre_time from temp_date_str)
  AND SUBSTR(start_time,1,8) <= (select MID_TIME from temp_date_str)
  AND backup_time <>0
  AND sqlcode='0'
GROUP BY
  SUBSTR(start_time,1,4)||'-'||substr(start_time,5,2)||'-'||substr(start_time,7,2);
