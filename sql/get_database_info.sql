SELECT database_name,
       DECIMAL(database_size/1024,6,1) AS DATABASE_SIZE,
       gss_name,
       i.instance_name,
       i.instance_port,
       i.ssl_port,
       i.VERSION,
       i.env_instance,
       s.server_name,
       s.ip_address,
       s.os,
       s.os_version,
       s.ssl_expiry_date
FROM   LONG.DATABASE d
       INNER JOIN LONG.instance i
               ON d.instance_id = i.instance_id
       INNER JOIN LONG.server s
               ON i.server_id = s.server_id
WHERE d.database_id = $DATABASE_ID$;               