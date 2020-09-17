/* ﷽‎-*/
DECLARE @ErrorLog AS TABLE(LogDate DATETIME, ProcessInfo VARCHAR(64), Txt VARCHAR(MAX))
INSERT INTO @ErrorLog
EXEC sys.xp_readerrorlog 0, 1, N'Recovery of database'
SELECT TOP 5
CONVERT(VARCHAR,T1.LogDate,120) LogDate

, LEFT(RIGHT(T1.Txt,LEN(T1.Txt) - PATINDEX('%database%',T1.Txt) - 9)
,PATINDEX('%''%',RIGHT(T1.Txt,LEN(T1.Txt) - PATINDEX('%database%',T1.Txt) - 9))-1) DBName

, LTRIM(RIGHT(LEFT(T1.Txt,PATINDEX('% complete%',T1.Txt)),4)) [%Complete]

, CONVERT(varchar,DATEADD(s,CONVERT(INT,LEFT(RIGHT(T1.Txt,LEN(T1.Txt) 
- PATINDEX('%approximately%',T1.Txt) - 13),PATINDEX('% %',RIGHT(T1.Txt,LEN(T1.Txt) 
- PATINDEX('%approximately%',T1.Txt) - 13))-1)),0),108) [Time To Finish]

, CONVERT(varchar,DATEADD(s,CONVERT(INT,LEFT(RIGHT(T1.Txt,LEN(T1.Txt) 
- PATINDEX('%approximately%',T1.Txt) - 13),PATINDEX('% %',RIGHT(T1.Txt,LEN(T1.Txt) 
- PATINDEX('%approximately%',T1.Txt) - 13))-1)),GETDATE()),108) [Should Be Done@]
,T1.Txt
FROM @ErrorLog T1
ORDER BY [LogDate] DESC