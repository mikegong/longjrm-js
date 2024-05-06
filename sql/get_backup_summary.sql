WITH temp_date AS (
    SELECT
        CURRENT timestamp-(cast('$back_days$' AS INTEGER)+cast('$back_days$' AS INTEGER)) days AS pre_time,
        CURRENT timestamp-cast('$back_days$' AS INTEGER) days AS mid_time,
        CURRENT timestamp AS cur_time
    FROM
        sysibm.sysdummy1
),
temp_date_str AS (
    SELECT
        SUBSTR(pre_time, 1, 4) || SUBSTR(pre_time, 6, 2) || SUBSTR(pre_time, 9, 2) || SUBSTR(pre_time, 12, 2) || SUBSTR(pre_time, 15, 2) || SUBSTR(pre_time, 18, 2) AS pre_time,
        SUBSTR(mid_time, 1, 4) || SUBSTR(mid_time, 6, 2) || SUBSTR(mid_time, 9, 2) || SUBSTR(mid_time, 12, 2) || SUBSTR(mid_time, 15, 2) || SUBSTR(mid_time, 18, 2) AS mid_time,
        SUBSTR(cur_time, 1, 4) || SUBSTR(cur_time, 6, 2) || SUBSTR(cur_time, 9, 2) || SUBSTR(cur_time, 12, 2) || SUBSTR(cur_time, 15, 2) || SUBSTR(cur_time, 18, 2) AS cur_time
    FROM
        temp_date
),
backup_his_cur AS (
    SELECT
        DISTINCT database_name,
        start_time,
        CASE
            WHEN sqlcode = 0 THEN 'cur_success'
            ELSE 'cur_failed'
        END AS status
    FROM
        pcm.db_backup_history
    WHERE
        start_time >= (
            SELECT
                mid_time
            FROM
                temp_date_str
        )
        AND start_time < (
            SELECT
                cur_time
            FROM
                temp_date_str
        )
),
backup_his_cur_summary AS (
    SELECT
        status,
        COUNT(*) AS counts
    FROM
        backup_his_cur
    GROUP BY
        status
),
backup_his_pre AS (
    SELECT
        DISTINCT database_name,
        start_time,
        CASE
            WHEN sqlcode = 0 THEN 'pre_success'
            ELSE 'pre_failed'
        END AS status
    FROM
        pcm.db_backup_history
    WHERE
        start_time >= (
            SELECT
                pre_time
            FROM
                temp_date_str
        )
        AND start_time < (
            SELECT
                mid_time
            FROM
                temp_date_str
        )
),
backup_his_pre_summary AS (
    SELECT
        status,
        COUNT(*) AS counts
    FROM
        backup_his_pre
    GROUP BY
        status
),
running_backup_summary AS (
    SELECT
        'running_backup' AS status,
        COUNT(DISTINCT a.database_name) AS counts
    FROM
        pcm.db_running_backup_snapshot a
    WHERE
        snapshot_timestamp =(
            SELECT
                MAX(snapshot_timestamp)
            FROM
                pcm.db_running_backup_snapshot
        )
        AND timestamp_FORMAT(
            snapshot_timestamp,
            'YYYY-MM-DD HH24:MI:SS'
        ) >=(CURRENT timestamp - 11 minutes)
)
SELECT
    STATUS, COUNTS
FROM
    backup_his_pre_summary
UNION
SELECT
   STATUS, COUNTS
FROM
    backup_his_cur_summary
UNION
SELECT
    STATUS, COUNTS
FROM
    running_backup_summary;