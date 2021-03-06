SELECT
T1.NAME
, CONVERT(varchar,DATEADD(ms,T2.TOTAL_ELAPSED_TIME,0),108) [Running Time]
, CONVERT(varchar,DATEADD(ms,T2.ESTIMATED_COMPLETION_TIME,0),108) [Time To Finish]
, CONVERT(varchar,DATEADD(ms,T2.ESTIMATED_COMPLETION_TIME,GETDATE()),108) [Should Be Done@]
,T2.PERCENT_COMPLETE as [% Complete]
,(SELECT TEXT FROM sys.dm_exec_sql_text(T2.SQL_HANDLE))AS COMMAND FROM
MASTER..SYSDATABASES T1, sys.dm_exec_requests T2
WHERE T1.DBID = T2.DATABASE_ID AND T2.COMMAND LIKE '%BACKUP%'
ORDER BY percent_complete DESC,[Running Time] DESC