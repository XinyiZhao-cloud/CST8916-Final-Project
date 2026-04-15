WITH
    AggregatedReadings
    AS
    (
        SELECT
            [location],
            System.Timestamp AS windowEnd,
            AVG(CAST([iceThickness] AS float)) AS avgIceThickness,
            MIN(CAST([iceThickness] AS float)) AS minIceThickness,
            MAX(CAST([iceThickness] AS float)) AS maxIceThickness,
            AVG(CAST([surfaceTemperature] AS float)) AS avgSurfaceTemperature,
            MIN(CAST([surfaceTemperature] AS float)) AS minSurfaceTemperature,
            MAX(CAST([surfaceTemperature] AS float)) AS maxSurfaceTemperature,
            MAX(CAST([snowAccumulation] AS float)) AS maxSnowAccumulation,
            AVG(CAST([externalTemperature] AS float)) AS avgExternalTemperature,
            COUNT(*) AS readingCount
        FROM [iotinput] TIMESTAMP
     BY [timestamp]
    GROUP BY
        [location],
        TumblingWindow
(minute, 5)
)

SELECT
    CONCAT([location], '-', CAST(windowEnd AS nvarchar(max))) AS id,
    [location],
    windowEnd,
    avgIceThickness,
    minIceThickness,
    maxIceThickness,
    avgSurfaceTemperature,
    minSurfaceTemperature,
    maxSurfaceTemperature,
    maxSnowAccumulation,
    avgExternalTemperature,
    readingCount,
    CASE
        WHEN avgIceThickness >= 30 AND avgSurfaceTemperature <= -2 THEN 'Safe'
        WHEN avgIceThickness >= 25 AND avgSurfaceTemperature <= 0 THEN 'Caution'
        ELSE 'Unsafe'
    END AS safetyStatus
INTO [cosmosoutput]
FROM AggregatedReadings;

SELECT
    CONCAT([location], '-', CAST(windowEnd AS nvarchar(max))) AS id,
    [location],
    windowEnd,
    avgIceThickness,
    minIceThickness,
    maxIceThickness,
    avgSurfaceTemperature,
    minSurfaceTemperature,
    maxSurfaceTemperature,
    maxSnowAccumulation,
    avgExternalTemperature,
    readingCount,
    CASE
        WHEN avgIceThickness >= 30 AND avgSurfaceTemperature <= -2 THEN 'Safe'
        WHEN avgIceThickness >= 25 AND avgSurfaceTemperature <= 0 THEN 'Caution'
        ELSE 'Unsafe'
    END AS safetyStatus
INTO [bloboutput]
FROM AggregatedReadings;