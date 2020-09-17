/*-*-*-*-*-*-*-*-* ﷽‎  *-*-*-*-*-*-*-*-*-*-*-*/
SET FMTONLY OFF;/*This is for temporary tables in SSRS, you never know if you want to use this in a report*/
SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SET STATISTICS IO ON
DECLARE @Database VARCHAR(255), @SpeedFactor INT, @isEnterprise INT, @rebuildonline VARCHAR(5),@MinForRebuild INT, @Table VARCHAR(255), @cmd NVARCHAR(3500),@cmd2 NVARCHAR(3500), @fillfactor INT, @pagemin INT, @fragmin INT, @WaitFor VARCHAR(12),@ActivateWait VARCHAR(1), @JustShowMe VARCHAR(1);
 
SET @WaitFor = '00:00:15';      /*hh:mm:ss Average time for a big index is around 2 minutes, and assume around 25% the trans log filled by each rebuild*/
SET @fillfactor = 100;           /*Depending or your index growth anywhere between 70-90 should work*/
SET @pagemin = 500;             /*not concerned with small tables, ignore less than 1500 pages index*/
SET @fragmin = 5;               /*Ignore fragmentation less than 5%*/
SET @ActivateWait = 0;          /*1 will active it*/
SET @JustShowMe = 1;            /*0 is off*/
SET @SpeedFactor = 17.5;        /* The estimated time factor.. change this to fine tune actual time rebuild will take*/
SET @MinForRebuild = 25;        /*Min fragmentation % for rebuild*/
SET @rebuildonline = 'OFF';     /*Assume this is not Enterprise, we will test in the next line and if it is , woohoo.*/
SELECT @isEnterprise = PATINDEX('%enterprise%',@@Version);
IF (@isEnterprise > 0)
BEGIN
    PRINT 'This is a Enterprise Server, we will use online rebuild' ;
    /*Can also use CAST(SERVERPROPERTY('EngineEdition') AS INT), thanks http://www.brentozar.com/ */
    SET @MinForRebuild = 15;    /*Lets lower our expectations for rebuilds, as we are splurging half our budget on the Enterprise License, so who cares*/
    SET @rebuildonline = 'ON';
END;
 
DECLARE @Databases TABLE
    (
    id INT IDENTITY(1,1)
    , databasename VARCHAR(250)
    , [compatibility_level] INT
    , user_access INT
    , user_access_desc VARCHAR(50)
    , [state] INT
    , state_desc  VARCHAR(50)
    , recovery_model INT
    , recovery_model_desc  VARCHAR(50)
    );
 
INSERT INTO @Databases
    SELECT
    db.name
    , db.compatibility_level
    , db.user_access
    , db.user_access_desc
    , db.state
    , db.state_desc
    , db.recovery_model
    , db.recovery_model_desc
    FROM
    sys.databases db
    WHERE name NOT IN ('master','msdb','tempdb','model','distribution') /*List of excluded Databases*/
    AND db.state <> 6 AND db.user_access <> 1
 
IF @JustShowMe = 0
BEGIN
    DECLARE @Outputme TABLE
        (
        id INT
        , type_desc VARCHAR(50)
        , database_id INT
        , object_id VARCHAR(250)
        , tablename VARCHAR(250)
        , index_id INT
        , name VARCHAR(250)
        , page_count INT
        , avg_fragmentation_in_percent DECIMAL(10,3)
        , PageSize_MB DECIMAL(10,3)
        , [Rows] BIGINT
        );
END;
DECLARE @i_Count INT, @i_Max INT
SET @i_Max = (SELECT MAX(id) FROM @Databases )
SET @i_Count = 1
WHILE @i_Count <= @i_Max
BEGIN
SET @Database = (SELECT databasename FROM @Databases WHERE id = @i_Count)
/*@cmd2 is the read only part of the code*/
SET @cmd2 = 'USE ['+@Database+']
    DECLARE @IndexStats TABLE(TableName VARCHAR(100), IndexName VARCHAR(100), IndexId TINYINT, [object_id] INT, [Rows] BIGINT, IndexSize_MB DECIMAL(10,3), TotalSize_MB DECIMAL(10,3))
    INSERT INTO @IndexStats
    SELECT
    OBJECT_NAME(i.OBJECT_ID) AS TableName
    , i.name AS IndexName
    , i.index_id AS IndexID
    , i.object_id
    , SUM(p.rows) [Rows]
    ,8 * SUM(CONVERT(DECIMAL(10,3),a.used_pages))/1024 AS ''Indexsize(MB)''
    ,8 * SUM(CONVERT(DECIMAL(10,3),a.total_pages))/1024 AS ''TotalSize(MB)''
    FROM sys.indexes AS i
    JOIN sys.partitions AS p ON p.OBJECT_ID = i.OBJECT_ID AND p.index_id = i.index_id
    JOIN sys.allocation_units AS a ON a.container_id = p.partition_id
    WHERE OBJECT_NAME(i.OBJECT_ID) NOT LIKE ''ifts_comp_fragment%''
    AND OBJECT_NAME(i.OBJECT_ID) NOT LIKE ''fulltext_%''
    AND OBJECT_NAME(i.OBJECT_ID) NOT LIKE ''filestream_%''
    AND OBJECT_NAME(i.OBJECT_ID) NOT LIKE ''queue_messages_%''
    GROUP BY i.OBJECT_ID,i.index_id,i.name,i.object_id
    HAVING SUM(a.total_pages) > 40
    ORDER BY OBJECT_NAME(i.OBJECT_ID),i.index_id,i.object_id
 
    DECLARE @IndexBase TABLE(id INT IDENTITY(1,1), type_desc VARCHAR(50), database_id INT, [object_id] INT, [tablename] VARCHAR(250), index_id INT, name VARCHAR(250), page_count INT,  avg_fragmentation_in_percent DECIMAL(10,3))
    INSERT INTO @IndexBase
    SELECT b.type_desc, a.database_id, b.[object_id],''[''+ isc.TABLE_SCHEMA +''].[''+ t.name+'']'', a.index_id, b.name, a.page_count, a.avg_fragmentation_in_percent
    FROM sys.dm_db_index_physical_stats (DB_ID(), OBJECT_ID(N''Production.Product''),NULL, NULL, NULL) AS a
        JOIN sys.indexes AS b ON a.object_id = b.object_id AND a.index_id = b.index_id
        LEFT OUTER JOIN sys.tables AS t ON b.[object_id] = t.[object_id]
        LEFT OUTER JOIN INFORMATION_SCHEMA.TABLES isc ON isc.TABLE_NAME = t.name
        WHERE page_count > @minPageCount
        AND a.avg_fragmentation_in_percent > @frag_min
        ORDER BY b.type_desc ASC, page_count desc
 
        IF EXISTS(SELECT * FROM @IndexBase ib WHERE ib.type_desc = ''CLUSTERED'')
        BEGIN
        IF @JustShow_Me = 1
            PRINT 1
        ELSE
            DELETE FROM @IndexBase
            WHERE type_desc <> ''CLUSTERED''
            AND tablename IN (SELECT DISTINCT tablename FROM @IndexBase ib WHERE ib.type_desc = ''CLUSTERED'')
        END
        '
/*@cmd is the actual code that does the indexes*/
SET @cmd = @cmd2 + '
    DECLARE @Numbers TABLE(number INT);DECLARE @Index AS INT, @Until INT, @FinalCMD NVARCHAR(3800)
    SET @Index = 1
    DECLARE @ibname VARCHAR(250), @ibtablename VARCHAR(250), @waittime INT, @frags INT
    SET @Until = (SELECT MAX(id) FROM @IndexBase)
    WHILE @Index  < @Until + 1
    BEGIN
        SET @frags = (SELECT ib.avg_fragmentation_in_percent FROM @IndexBase ib WHERE ib.name IS NOT NULL AND ib.id = @Index)
        SET @waittime = (SELECT ib.seconds FROM @IndexBase ib WHERE ib.name IS NOT NULL AND ib.id = @Index)
        SET @ibname = (SELECT ib.name FROM @IndexBase ib WHERE ib.name IS NOT NULL AND ib.id = @Index)
        SET @ibtablename = (SELECT ib.tablename FROM @IndexBase ib WHERE ib.name IS NOT NULL AND ib.id = @Index)
        IF @frags > 25
            BEGIN
                SET @FinalCMD = ''USE ['+@Database+'];
                ALTER INDEX [''+@ibname+''] ON ''+@ibtablename+''
                REBUILD PARTITION = ALL
                WITH ( FILLFACTOR = @fillme, PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS = ON, ONLINE = '+@rebuildonline+', SORT_IN_TEMPDB = ON)''
            END
        ELSE
            BEGIN
                SET @FinalCMD = ''USE ['+@Database+'];
                ALTER INDEX [''+@ibname+''] ON ''+@ibtablename+''
                REORGANIZE WITH ( LOB_COMPACTION = ON )''
            END
        IF @FinalCMD IS NOT NULL
        BEGIN
            BEGIN TRY
                DECLARE @Waitforme VARCHAR(15)
                SET @Waitforme = CONVERT(VARCHAR,DATEADD(SECOND,@waittime,''00:00:00''),108)
                EXEC sp_executesql @FinalCMD
                IF @Activate_Wait > 0 BEGIN WAITFOR DELAY @Waitforme END
            END TRY
            BEGIN CATCH
                BEGIN TRY
                    WAITFOR DELAY ''00:00:05''
                    EXEC sp_executesql @FinalCMD
                END TRY
                BEGIN CATCH
                    PRINT ''Error: '' +@FinalCMD
                END CATCH;
            END CATCH;
        END
        SET @Index = @Index + 1
    END     '
    BEGIN TRY
    IF @JustShowMe = 1
    BEGIN
        SET @cmd = @cmd2 + '
        SELECT T1.* , T2.IndexSize_MB, T2.Rows
        FROM @IndexBase T1
        INNER JOIN @IndexStats T2 ON T1.object_id = T2.object_id AND T1.name = T2.IndexName
        --SELECT *      FROM @IndexStats
        --SELECT SUM(T2.IndexSize_MB)
        --FROM @IndexStats T2
        '
        INSERT INTO  @Outputme
        EXEC sp_executesql @cmd, N'@JustShow_Me TINYINT, @fillme TINYINT,@minPageCount INT,@frag_min TINYINT,@Activate_Wait TINYINT',@JustShow_Me = @JustShowMe, @fillme = @fillfactor,@minPageCount = @MinForRebuild,@frag_min = @fragmin, @Activate_Wait = @ActivateWait
        PRINT @cmd
    END
    IF @JustShowMe = 0
    BEGIN
        EXEC sp_executesql @cmd, N'@JustShow_Me TINYINT, @fillme TINYINT,@minPageCount INT,@frag_min TINYINT,@Activate_Wait TINYINT',@JustShow_Me = @JustShowMe, @fillme = @fillfactor,@minPageCount = @MinForRebuild,@frag_min = @fragmin, @Activate_Wait = @ActivateWait
        PRINT @cmd
        IF @ActivateWait > 0 BEGIN WAITFOR DELAY @WaitFor END
    END
    END TRY
    BEGIN CATCH
        INSERT INTO @Outputme
        SELECT 0,'Error', (SELECT sdb.dbid  FROM  MASTER.dbo.sysdatabases sdb WHERE sdb.name = @Database ) ,0,'', 0, '', 0, 0, 0 ,0
    END CATCH
   SET @i_Count = @i_Count + 1
END
IF @JustShowMe = 1
BEGIN
    SELECT  om.id, om.type_desc, sdb.name, om.database_id, om.object_id, om.tablename, om.index_id, om.name, om.page_count, om.avg_fragmentation_in_percent, om.PageSize_MB, om.Rows
    FROM @Outputme  om
    LEFT OUTER JOIN MASTER.dbo.sysdatabases sdb ON sdb.dbid = om.database_id
END