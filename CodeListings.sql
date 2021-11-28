-- Listing 2-1
CREATE TABLE dbo.Example
(
    Col1 INT
);
INSERT INTO dbo.Example
(
    Col1
)
VALUES
(1  );
SELECT e.Col1
FROM dbo.Example AS e;


--Listing 2-2
SELECT soh.AccountNumber,
       soh.OrderDate,
       soh.PurchaseOrderNumber,
       soh.SalesOrderNumber
FROM Sales.SalesOrderHeader AS soh
WHERE soh.SalesOrderID
BETWEEN 62500 AND 62550;

--Listing 2-3
SELECT soh.SalesOrderNumber,
       sod.OrderQty,
       sod.LineTotal,
       sod.UnitPrice,
       sod.UnitPriceDiscount,
       p.Name AS ProductName,
       p.ProductNumber,
       ps.Name AS ProductSubCategoryName,
       pc.Name AS ProductCategoryName
FROM Sales.SalesOrderHeader AS soh
    JOIN Sales.SalesOrderDetail AS sod
        ON soh.SalesOrderID = sod.SalesOrderID
    JOIN Production.Product AS p
        ON sod.ProductID = p.ProductID
    JOIN Production.ProductModel AS pm
        ON p.ProductModelID = pm.ProductModelID
    JOIN Production.ProductSubcategory AS ps
        ON p.ProductSubcategoryID = ps.ProductSubcategoryID
    JOIN Production.ProductCategory AS pc
        ON ps.ProductCategoryID = pc.ProductCategoryID
WHERE soh.CustomerID = 29658;


--Listing 2-4
SELECT deqoi.counter,
       deqoi.occurrence,
       deqoi.value
FROM sys.dm_exec_query_optimizer_info AS deqoi;


--Listing 2-5
USE master;
EXEC sp_configure 'show advanced option', '1';
RECONFIGURE;
EXEC sp_configure 'max degree of parallelism', 2;
RECONFIGURE;


--Listing 2-6
SELECT e.ID,
       e.SomeValue
FROM dbo.Example AS e
WHERE e.ID = 42
OPTION (MAXDOP 2);


--Listing 2-7
USE master;
EXEC sp_configure 'show advanced option', '1';
RECONFIGURE;
EXEC sp_configure 'cost threshold for parallelism', 35;
RECONFIGURE;


--Listing 3-1
SELECT dest.text,
       deqp.query_plan,
       der.cpu_time,
       der.logical_reads,
       der.writes
FROM sys.dm_exec_requests AS der
    CROSS APPLY sys.dm_exec_query_plan(der.plan_handle) AS deqp
    CROSS APPLY sys.dm_exec_sql_text(der.plan_handle) AS dest;




--Listing 3-2
SELECT dest.text,
       deqp.query_plan,
       deqs.execution_count,
       deqs.min_logical_writes,
       deqs.max_logical_reads,
       deqs.total_logical_reads,
       deqs.total_elapsed_time,
       deqs.last_elapsed_time
FROM sys.dm_exec_query_stats AS deqs
    CROSS APPLY sys.dm_exec_query_plan(deqs.plan_handle) AS deqp
    CROSS APPLY sys.dm_exec_sql_text(deqs.sql_handle) AS dest;


--Listing 3-3
CREATE EVENT SESSION [QueryPerformanceMetrics]
ON SERVER
    ADD EVENT sqlserver.rpc_completed
    (SET collect_statement = (1)
     WHERE ([sqlserver].[database_name] = N'Adventureworks')
    ),
    ADD EVENT sqlserver.sql_batch_completed
    (WHERE ([sqlserver].[database_name] = N'Adventureworks'))
    ADD TARGET package0.event_file
    (SET filename = N'QueryPerformanceMetrics', max_file_size = (2048));
GO


--Listing 3-4
ALTER EVENT SESSION QueryPerformanceMetrics ON SERVER STATE = START;

ALTER EVENT SESSION QueryPerformanceMetrics ON SERVER STATE = STOP;

--Listing 3-5
SELECT fx.object_name,
       fx.file_name,
       fx.event_data
FROM sys.fn_xe_file_target_read_file('.\QueryPerformanceMetrics_*.xel', NULL, NULL, NULL) AS fx;


USE AdventureWorks;
DBCC FREEPROCCACHE();


--Listing 4-1
SELECT soh.SalesOrderNumber,
       p.Name,
       sod.OrderQty
FROM Sales.SalesOrderHeader AS soh
    JOIN Sales.SalesOrderDetail AS sod
        ON sod.SalesOrderID = soh.SalesOrderID
    JOIN Production.Product AS p
        ON p.ProductID = sod.ProductID
WHERE soh.CustomerID = 30052;

--Listing 4-2
SELECT dest.text,
       deqp.query_plan,
       deqs.execution_count,
       deqs.total_elapsed_time,
       deqs.last_elapsed_time
FROM sys.dm_exec_query_stats AS deqs
    CROSS APPLY sys.dm_exec_query_plan(deqs.plan_handle) AS deqp
    CROSS APPLY sys.dm_exec_sql_text(deqs.sql_handle) AS dest
WHERE dest.text LIKE 'SELECT soh.SalesOrderNumber,
       p.Name,%';



--Listing 4-3
SELECT qsq.query_id,
       qsq.query_hash,
       CAST(qsp.query_plan AS XML) AS QueryPlan
FROM sys.query_store_query AS qsq
    JOIN sys.query_store_plan AS qsp
        ON qsp.query_id = qsq.query_id
    JOIN sys.query_store_query_text AS qsqt
        ON qsqt.query_text_id = qsq.query_text_id
WHERE qsqt.query_sql_text LIKE 'SELECT soh.SalesOrderNumber,
       p.Name,%';


--Listing 4-4
SELECT pv.OnOrderQty,
       a.City
FROM Purchasing.ProductVendor AS pv,
     Person.Address AS a
WHERE a.City = 'Tulsa';

--Listing 4-5
SELECT *
FROM Production.UnitMeasure AS um;


--Listing 5-1
IF
(
    SELECT OBJECT_ID('Test1')
) IS NOT NULL
    DROP TABLE dbo.Test1;
GO
CREATE TABLE dbo.Test1
(
    C1 INT,
    C2 INT IDENTITY
);

SELECT TOP 1500
       IDENTITY(INT, 1, 1) AS n
INTO #Nums
FROM master.dbo.syscolumns AS sC1,
     master.dbo.syscolumns AS sC2;

INSERT INTO dbo.Test1
(
    C1
)
SELECT n
FROM #Nums;

DROP TABLE #Nums;

CREATE NONCLUSTERED INDEX i1 ON dbo.Test1 (C1);


--Listing 5-2
SELECT t.C1,
       t.C2
FROM dbo.Test1 AS t
WHERE t.C1 = 2;
GO 50


--Listing 5-3
CREATE EVENT SESSION [Statistics]
ON SERVER
    ADD EVENT sqlserver.auto_stats
    (ACTION
     (
         sqlserver.sql_text
     )
     WHERE (sqlserver.database_name = N'AdventureWorks')
    ),
    ADD EVENT sqlserver.sql_batch_completed
    (WHERE (sqlserver.database_name = N'AdventureWorks'));
GO
ALTER EVENT SESSION [Statistics] ON SERVER STATE = START;


--Listing 5-4
INSERT INTO dbo.Test1
(
    C1
)
VALUES
(2  );

--Listing 5-5
SELECT TOP 1500
       IDENTITY(INT, 1, 1) AS n
INTO #Nums
FROM master.dbo.syscolumns AS scl,
     master.dbo.syscolumns AS sC2;
INSERT INTO dbo.Test1
(
    C1
)
SELECT 2
FROM #Nums;
DROP TABLE #Nums;


--Listing 5-6
ALTER DATABASE AdventureWorks SET AUTO_UPDATE_STATISTICS OFF;

--Listing 5-7
ALTER DATABASE AdventureWorks SET AUTO_UPDATE_STATISTICS ON;


--Listing 5-8
IF
(
    SELECT OBJECT_ID('dbo.Test1')
) IS NOT NULL
    DROP TABLE dbo.Test1;
GO

CREATE TABLE dbo.Test1
(
    Test1_C1 INT IDENTITY,
    Test1_C2 INT
);

INSERT INTO dbo.Test1
(
    Test1_C2
)
VALUES
(1  );

SELECT TOP 10000
       IDENTITY(INT, 1, 1) AS n
INTO #Nums
FROM master.dbo.syscolumns AS scl,
     master.dbo.syscolumns AS sC2;

INSERT INTO dbo.Test1
(
    Test1_C2
)
SELECT 2
FROM #Nums;
GO

CREATE CLUSTERED INDEX i1 ON dbo.Test1 (Test1_C1);

--Create second table with 10001 rows, -- but opposite data distribution 
IF
(
    SELECT OBJECT_ID('dbo.Test2')
) IS NOT NULL
    DROP TABLE dbo.Test2;
GO

CREATE TABLE dbo.Test2
(
    Test2_C1 INT IDENTITY,
    Test2_C2 INT
);

INSERT INTO dbo.Test2
(
    Test2_C2
)
VALUES
(2  );

INSERT INTO dbo.Test2
(
    Test2_C2
)
SELECT 1
FROM #Nums;
DROP TABLE #Nums;
GO

CREATE CLUSTERED INDEX il ON dbo.Test2 (Test2_C1);


--Listing 5-9
SELECT DATABASEPROPERTYEX('AdventureWorks', 'IsAutoCreateStatistics');


--Listing 5-10
ALTER DATABASE AdventureWorks SET AUTO_CREATE_STATISTICS ON;




ALTER DATABASE SCOPED CONFIGURATION CLEAR PROCEDURE_CACHE;


--Listing 5-11
SELECT t1.Test1_C2,
       t2.Test2_C2
FROM dbo.Test1 AS t1
    JOIN dbo.Test2 AS t2
        ON t1.Test1_C2 = t2.Test2_C2
WHERE t1.Test1_C2 = 2;
GO 50

--Listing 5-12
SELECT s.name,
       s.auto_created,
       s.user_created
FROM sys.stats AS s
WHERE object_id = OBJECT_ID('Test1');


--Listing 5-13
SELECT t1.Test1_C2,
       t2.Test2_C2
FROM dbo.Test1 AS t1
    JOIN dbo.Test2 AS t2
        ON t1.Test1_C2 = t2.Test2_C2
WHERE t1.Test1_C2 = 1;


--Listing 5-14
ALTER DATABASE AdventureWorks SET AUTO_CREATE_STATISTICS OFF;

ALTER DATABASE AdventureWorks SET AUTO_CREATE_STATISTICS ON;


--Listing 5-15
IF
(
    SELECT OBJECT_ID('dbo.Test1')
) IS NOT NULL
    DROP TABLE dbo.Test1;
GO

CREATE TABLE dbo.Test1
(
    C1 INT,
    C2 INT IDENTITY
);

INSERT INTO dbo.Test1
(
    C1
)
VALUES
(1  );

SELECT TOP 10000
       IDENTITY(INT, 1, 1) AS n
INTO #Nums
FROM master.dbo.syscolumns sc1,
     master.dbo.syscolumns sc2;

INSERT INTO dbo.Test1
(
    C1
)
SELECT 2
FROM #Nums;

DROP TABLE #Nums;

CREATE NONCLUSTERED INDEX FirstIndex ON dbo.Test1 (C1);


--Listing 5-16
DBCC SHOW_STATISTICS(Test1,FirstIndex);


--Listing 5-17
SELECT 1.0 / COUNT(DISTINCT C1)
FROM dbo.Test1;


--Listing 5-18
DBCC SHOW_STATISTICS('Sales.SalesOrderDetail','IX_SalesOrderDetail_ProductID');


--Listing 5-19
CREATE EVENT SESSION [CardinalityEstimation]
ON SERVER
    ADD EVENT sqlserver.auto_stats
    (WHERE ([sqlserver].[database_name] = N'AdventureWorks')),
    ADD EVENT sqlserver.query_optimizer_estimate_cardinality
    (WHERE ([sqlserver].[database_name] = N'AdventureWorks')),
    ADD EVENT sqlserver.sql_batch_completed
    (WHERE ([sqlserver].[database_name] = N'AdventureWorks')),
    ADD EVENT sqlserver.sql_batch_starting
    (WHERE ([sqlserver].[database_name] = N'AdventureWorks'))
    ADD TARGET package0.event_file
    (SET filename = N'cardinalityestimation')
WITH
(
    TRACK_CAUSALITY = ON
);
GO


--Listing 5-20
SELECT so.Description,
       p.Name AS ProductName,
       p.ListPrice,
       p.Size,
       pv.AverageLeadTime,
       pv.MaxOrderQty,
       v.Name AS VendorName
FROM Sales.SpecialOffer AS so
    JOIN Sales.SpecialOfferProduct AS sop
        ON sop.SpecialOfferID = so.SpecialOfferID
    JOIN Production.Product AS p
        ON p.ProductID = sop.ProductID
    JOIN Purchasing.ProductVendor AS pv
        ON pv.ProductID = p.ProductID
    JOIN Purchasing.Vendor AS v
        ON v.BusinessEntityID = pv.BusinessEntityID
WHERE so.DiscountPct > .15;

--Listing 5-21
SELECT p.Name,
       p.Class
FROM Production.Product AS p
WHERE p.Color = 'Red'
      AND p.DaysToManufacture > 2;


--Listing 5-22
SELECT s.name,
       s.auto_created,
       s.user_created,
       s.filter_definition,
       sc.column_id,
       c.name AS ColumnName
FROM sys.stats AS s
    JOIN sys.stats_columns AS sc
        ON sc.stats_id = s.stats_id
           AND sc.object_id = s.object_id
    JOIN sys.columns AS c
        ON c.column_id = sc.column_id
           AND c.object_id = s.object_id
WHERE s.object_id = OBJECT_ID('Production.Product');

--Listing 5-23
CREATE NONCLUSTERED INDEX FirstIndex
ON dbo.Test1 (
                 C1,
                 C2
             )
WITH (DROP_EXISTING = ON);


DBCC SHOW_STATISTICS(Test1,FirstIndex);


--Listing 5-24
CREATE INDEX IX_Test ON Sales.SalesOrderHeader (PurchaseOrderNumber);

DBCC SHOW_STATISTICS('sales.SalesOrderHeader','IX_Test');

--Listing 5-25
CREATE INDEX IX_Test
ON Sales.SalesOrderHeader (PurchaseOrderNumber)
WHERE PurchaseOrderNumber IS NOT NULL
WITH (DROP_EXISTING = ON);


--Listing 5-26
DROP INDEX Sales.SalesOrderHeader.IX_Test;


--Listing 5-27
ALTER DATABASE AdventureWorks SET COMPATIBILITY_LEVEL = 110;

--Listing 5-28
ALTER DATABASE SCOPED CONFIGURATION SET LEGACY_CARDINALITY_ESTIMATION = ON;

--Listing 5-29
SELECT p.Name,
       p.Class
FROM Production.Product AS p
WHERE p.Color = 'Red'
      AND p.DaysToManufacture > 15
OPTION (USE HINT ('FORCE_LEGACY_CARDINALITY_ESTIMATION'));


--Listing 5-30
ALTER DATABASE AdventureWorks SET AUTO_CREATE_STATISTICS OFF;

--Listing 5-31
ALTER DATABASE AdventureWorks SET AUTO_UPDATE_STATISTICS OFF;

--Listing 5-32
ALTER DATABASE AdventureWorks SET AUTO_UPDATE_STATISTICS_ASYNC ON;

--Listing 5-33
USE AdventureWorks;
EXEC sp_autostats 
    'HumanResources.Department',
    'OFF';


--Listing 5-34
EXEC sp_autostats 
    'HumanResources.Department',
    'OFF',
    AK_Department_Name;


--Listing 5-35
EXEC sp_autostats 'HumanResources.Department';

--Listing 5-36
EXEC sp_autostats 
    'HumanResources.Department',
    'ON';
EXEC sp_autostats 
    'HumanResources.Department',
    'ON',
    AK_Department_Name;


--Listing 5-37
UPDATE STATISTICS dbo.bigProduct
WITH RESAMPLE,
     INCREMENTAL = ON;

--Listing 5-38
ALTER DATABASE AdventureWorks SET AUTO_CREATE_STATISTICS OFF;
ALTER DATABASE AdventureWorks SET AUTO_UPDATE_STATISTICS OFF;
GO

IF EXISTS
(
    SELECT *
    FROM sys.objects
    WHERE object_id = OBJECT_ID(N'dbo.Test1')
)
    DROP TABLE dbo.Test1;
GO

CREATE TABLE dbo.Test1
(
    C1 INT,
    C2 INT,
    C3 CHAR(50)
);
INSERT INTO dbo.Test1
(
    C1,
    C2,
    C3
)
VALUES
(51, 1, 'C3'),
(52, 1, 'C3');

CREATE NONCLUSTERED INDEX iFirstIndex ON dbo.Test1 (C1, C2);

SELECT TOP 10000
       IDENTITY(INT, 1, 1) AS n
INTO #Nums
FROM master.dbo.syscolumns AS scl,
     master.dbo.syscolumns AS sC2;

INSERT INTO dbo.Test1
(
    C1,
    C2,
    C3
)
SELECT n % 50,
       n,
       'C3'
FROM #Nums;
DROP TABLE #Nums;


--Listing 5-39
SELECT t.C1,
       t.C2,
	   t.C3
FROM dbo.Test1 AS t
WHERE t.C2 = 1;
GO 50
ALTER DATABASE SCOPED CONFIGURATION CLEAR PROCEDURE_CACHE


--Listing 5-40
CREATE STATISTICS Stats1 ON Test1(C2);


--Listing 5-41
DECLARE @Planhandle VARBINARY(64);

SELECT @Planhandle = deqs.plan_handle
FROM sys.dm_exec_query_stats AS deqs
    CROSS APPLY sys.dm_exec_sql_text(deqs.sql_handle) AS dest
WHERE dest.text = 'SELECT  *
FROM    dbo.Test1
WHERE   C2 = 1;';

IF @Planhandle IS NOT NULL
BEGIN
    DBCC FREEPROCCACHE(@Planhandle);
END;
GO

--Listing 5-42
DBCC SHOW_STATISTICS (Test1, iFirstIndex);


--Listing 5-43
SELECT C1,
       C2,
       C3
FROM dbo.Test1
WHERE C1 = 51;
GO 50

--Listing 5-44
UPDATE STATISTICS Test1 iFirstIndex
WITH FULLSCAN;

--Listing 5-45
ALTER DATABASE AdventureWorks SET AUTO_CREATE_STATISTICS ON;
ALTER DATABASE AdventureWorks SET AUTO_UPDATE_STATISTICS ON;



--Listing 6-1
ALTER DATABASE AdventureWorks SET QUERY_STORE = ON;


--Listing 6-2
SELECT qsq.query_id,
       qsq.object_id,
       qsqt.query_sql_text,
	   qsp.plan_id,
       CAST(qsp.query_plan AS XML) AS QueryPlan
FROM sys.query_store_query AS qsq
    JOIN sys.query_store_query_text AS qsqt
        ON qsq.query_text_id = qsqt.query_text_id
    JOIN sys.query_store_plan AS qsp
        ON qsp.query_id = qsq.query_id
WHERE qsq.object_id = OBJECT_ID('dbo.ProductTransactionHistoryByReference');

GO

--Listing 6-3
CREATE OR ALTER PROC dbo.ProductTransactionHistoryByReference
(@ReferenceOrderID int)
AS
BEGIN
    SELECT p.Name,
           p.ProductNumber,
           th.ReferenceOrderID
    FROM Production.Product AS p
        JOIN Production.TransactionHistory AS th
            ON th.ProductID = p.ProductID
    WHERE th.ReferenceOrderID = @ReferenceOrderID;
END;


GO

--Listing 6-4
SELECT a.AddressID,
       a.AddressLine1
FROM Person.Address AS a
WHERE a.AddressID = 72;


--Listing 6-5
SELECT qsq.query_id,
       qsq.query_hash,
       qsqt.query_sql_text,
       qsq.query_parameterization_type
FROM sys.query_store_query_text AS qsqt
    JOIN sys.query_store_query AS qsq
        ON qsq.query_text_id = qsqt.query_text_id
    JOIN sys.fn_stmt_sql_handle_from_sql_stmt(
             'SELECT a.AddressID,
       a.AddressLine1
FROM Person.Address AS a
WHERE a.AddressID = 72;',
             2)  AS fsshfss
        ON fsshfss.statement_sql_handle = qsqt.statement_sql_handle;

--Listing 6-6
DECLARE @CompareTime DATETIME = '2021-11-28 15:55';

SELECT CAST(qsp.query_plan AS XML),
       qsrs.count_executions,
       qsrs.avg_duration,
       qsrs.stdev_duration,
       qsws.wait_category_desc,
       qsws.avg_query_wait_time_ms,
       qsws.stdev_query_wait_time_ms
FROM sys.query_store_plan AS qsp
    JOIN sys.query_store_runtime_stats AS qsrs
        ON qsrs.plan_id = qsp.plan_id
    JOIN sys.query_store_runtime_stats_interval AS qsrsi
        ON qsrsi.runtime_stats_interval_id = qsrs.runtime_stats_interval_id
    JOIN sys.query_store_wait_stats AS qsws
        ON qsws.plan_id = qsrs.plan_id
           AND qsws.plan_id = qsrs.plan_id
           AND qsws.execution_type = qsrs.execution_type
           AND qsws.runtime_stats_interval_id = qsrs.runtime_stats_interval_id
WHERE qsp.plan_id = 329
      AND @CompareTime
      BETWEEN qsrsi.start_time AND qsrsi.end_time;


SELECT GETDATE();


--Listing 6-7
WITH QSAggregate
AS (SELECT qsrs.plan_id,
           SUM(qsrs.count_executions) AS CountExecutions,
           AVG(qsrs.avg_duration) AS AvgDuration,
           AVG(qsrs.stdev_duration) AS StDevDuration,
           qsws.wait_category_desc,
           AVG(qsws.avg_query_wait_time_ms) AS AvgQueryWaitTime,
           AVG(qsws.stdev_query_wait_time_ms) AS StDevQueryWaitTime
    FROM sys.query_store_runtime_stats AS qsrs
        LEFT JOIN sys.query_store_wait_stats AS qsws
            ON qsws.plan_id = qsrs.plan_id
               AND qsws.runtime_stats_interval_id = qsrs.runtime_stats_interval_id
    GROUP BY qsrs.plan_id,
             qsws.wait_category_desc)
SELECT CAST(qsp.query_plan AS XML),
       qsa.*
FROM sys.query_store_plan AS qsp
    JOIN QSAggregate AS qsa
        ON qsa.plan_id = qsp.plan_id
WHERE qsp.plan_id = 329;

EXEC sys.sp_query_store_flush_db

--Listing 6-8
ALTER DATABASE AdventureWorks SET QUERY_STORE CLEAR;


--Listing 6-9
EXEC sys.sp_query_store_remove_query @query_id = @QueryId;
EXEC sys.sp_query_store_remove_plan @plan_id = @PlanID;


--Listing 6-10
EXEC sys.sp_query_store_flush_db;


--Listing 6-11
SELECT *
FROM sys.database_query_store_options AS dqso;


--Listing 6-12
ALTER DATABASE AdventureWorks SET QUERY_STORE (MAX_STORAGE_SIZE_MB = 200);


--Listing 6-13
EXEC sys.sp_query_store_force_plan 550, 339;
EXEC sys.sp_query_store_unforce_plan 550, 339;



--Listing 6-14
EXEC sp_query_store_set_hints 550, N'OPTION(OPTIMIZE FOR UNKOWN)';


--Listing 6-15
SELECT qsqh.query_hint_id,
       qsqh.query_id,
       qsqh.query_hint_text,
       qsqh.source_desc
FROM sys.query_store_query_hints AS qsqh;


--Listing 6-16
EXEC sp_query_store_clear_hints @query_id = 550;
