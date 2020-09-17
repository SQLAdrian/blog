/*-*-*-*-*-*-*-*-* ﷽‎ *-*-*-*-*-*-*-*-*-*-*-*/
SELECT objtype AS [CacheType]
, COUNT_BIG(*) AS [Total Plans]
, SUM(size_in_bytes*0.000000953613) [TotalMB]
, AVG(usecounts) AS [Avg Use Count], SUM(CAST((CASE 
WHEN usecounts = 1 THEN size_in_bytes 
ELSE 0 END) as decimal(18,2)))/1024/1024 AS [Total MBs - USE Count 1]
, SUM(CASE WHEN usecounts = 1 THEN 1 ELSE 0 END) AS [Total Plans - USE Count 1] 
FROM sys.dm_exec_cached_plans 
GROUP BY objtype
ORDER BY [Total MBs - USE Count 1] DESC

SELECT TOP 10 cp.objtype, ISNULL(sp.name,CASE
WHEN PATINDEX('%CREATE F%',LTRIM(st.text)) > 0 THEN 'Other'
WHEN PATINDEX('%CREATE P%',LTRIM(st.text)) > 0 THEN 'Other'
ELSE LTRIM(st.text)
END) [Culprit], SUM(cp.usecounts) [UseCounts]
, SUM(cp.size_in_bytes*0.000000953613) [TotalMB]
FROM sys.dm_exec_cached_plans AS cp
CROSS APPLY sys.dm_exec_sql_text(cp.plan_handle) AS st
CROSS APPLY sys.dm_exec_query_plan(plan_handle) AS qp
LEFT OUTER JOIN sys.procedures sp ON Sp.object_id = st.objectid
GROUP BY cp.objtype
, ISNULL(sp.name,CASE
WHEN PATINDEX('%CREATE F%',LTRIM(st.text)) > 0 THEN 'Other'
WHEN PATINDEX('%CREATE P%',LTRIM(st.text)) > 0 THEN 'Other'
ELSE LTRIM(st.text)
END)
ORDER BY [TotalMB] DESC