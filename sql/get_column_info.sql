select distinct db.database_base_id, db.database_base_name,
database_alias, tabschema, type, tabname, 
upper(colname) colname, typename, length, scale,
default,nulls, codepage, keyseq, inline_length,
identity, rowchangetimestamp, generated, text,
implicitvalue, remarks, colno 
from pcm.syscat_columns col
join LONG.database_base db
on col.database_base_id = db.database_base_id
where upper(colname) like upper('$colname$')
			AND tabschema not in ('SYSIBM','SYSTOOLS','SYSPROC', 'ZG020','ZG017','S2756067') 
		  AND tabname not like 'EXPLAIN_%' AND TABNAME NOT LIKE 'ADVISE_%'
			AND database_base_name <> 'LONGDB'						
order by database_base_name,database_alias,
	tabschema, type, tabname,  colname