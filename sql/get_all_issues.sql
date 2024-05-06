select ISSUE_TRACKER_ID,
varchar_format(issue_ts, 'YYYY-MM-DD HH24:MI:SS') ISSUE_TS,
AREA_ID,STATUS,ISSUE_SOURCE,ISSUE_SOURCE_PK_ID,REPORTED_BY,
SERVER_ID,SERVER_NAME,
INSTANCE_ID,INSTANCE_NAME,
DATABASE_ID,DATABASE_NAME,
ISSUE, ROOT_CAUSE, SOLUTION, CREATE_USER, 
varchar_format(create_ts, 'YYYY-MM-DD HH24:MI:SS') CREATE_TS, 
cu.first_name ||' '||cu.last_name as create_user_fullname
from PCM.ISSUE_TRACKER it
   join long.user cu on it. create_user = cu.bns_id
   order by issue_ts desc