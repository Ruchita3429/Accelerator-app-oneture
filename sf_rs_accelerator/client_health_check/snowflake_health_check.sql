-- DataBridge client-run Snowflake health checks
-- Select your approved role and warehouse in Snowsight first.
-- Run each named statement and download its result as CSV.
-- No tables, grants or account settings are modified.
-- Original 7/30-day windows are preserved. Some checks scan all available history.
-- SELECT results are capped at max_rows + 1; SHOW uses Snowflake's own limits.
-- An empty result does not certify health. Record any failed statements.
-- Operational names/IDs are included; review results before sharing.
-- Raw query/error text included: False

-- CHECK: account_information | account_overview | Account Information
-- NOTE: Account name/locator aliases corrected in this standalone export; dashboard aliases are reversed.
SELECT * FROM (
SELECT CURRENT_ACCOUNT() AS ACCOUNT_LOCATOR,
       CURRENT_REGION() AS REGION,
       CURRENT_VERSION() AS SNOWFLAKE_VERSION,
       CURRENT_ACCOUNT_NAME() AS ACCOUNT_NAME
) AS health_check_result LIMIT 1001;

-- CHECK: object_counts | account_overview | Object Counts
-- NOTE: Preserves dashboard metadata filters; deleted objects may be included when the view retains them.
SELECT * FROM (
SELECT
  (SELECT COUNT(*) FROM SNOWFLAKE.ACCOUNT_USAGE.DATABASES) AS DATABASES,
  (SELECT COUNT(*) FROM SNOWFLAKE.ACCOUNT_USAGE.TABLES)    AS TABLES,
  (SELECT COUNT(*) FROM SNOWFLAKE.ACCOUNT_USAGE.VIEWS)     AS VIEWS,
  (SELECT COUNT(*) FROM SNOWFLAKE.ACCOUNT_USAGE.USERS)     AS USERS,
  (SELECT COUNT(*) FROM SNOWFLAKE.ACCOUNT_USAGE.ROLES)     AS ROLES
) AS health_check_result LIMIT 1001;

-- CHECK: login_failures | account_overview | Login Failures
-- NOTE: Uses the available Account Usage history, without a reporting-window filter. The row cap limits output, not the scan.
SELECT * FROM (
SELECT USER_NAME, EVENT_TIMESTAMP, ERROR_CODE
FROM SNOWFLAKE.ACCOUNT_USAGE.LOGIN_HISTORY
WHERE IS_SUCCESS = 'NO'
ORDER BY EVENT_TIMESTAMP DESC
) AS health_check_result LIMIT 1001;

-- CHECK: cost_summary_daily | account_overview | Cost Summary (Daily)
-- NOTE: Uses the available Account Usage history, without a reporting-window filter. The row cap limits output, not the scan.
SELECT * FROM (
SELECT DATE_TRUNC('DAY', START_TIME) AS DAY,
       SUM(CREDITS_USED) AS TOTAL_CREDITS
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
GROUP BY 1 ORDER BY 1 DESC
) AS health_check_result LIMIT 1001;

-- CHECK: credits_by_feature | account_overview | Credits by Feature
-- NOTE: Uses the available Account Usage history, without a reporting-window filter. The row cap limits output, not the scan.
SELECT * FROM (
SELECT SERVICE_TYPE, SUM(CREDITS_USED) AS CREDITS
FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_HISTORY
GROUP BY SERVICE_TYPE ORDER BY CREDITS DESC
) AS health_check_result LIMIT 1001;

-- CHECK: query_cost_breakdown | account_overview | Query Cost Breakdown
-- NOTE: Legacy approximation: each query receives its warehouse entire hourly credits multiplied by USD 2. This is NOT query-level cost; do not sum this column or treat it as a bill.
SELECT * FROM (
WITH account_info AS (
    SELECT CURRENT_ACCOUNT() AS account_name, CURRENT_REGION() AS account_region
),
query_usage AS (
    SELECT QUERY_ID, USER_NAME, WAREHOUSE_NAME,
           TOTAL_ELAPSED_TIME AS elapsed_ms, START_TIME
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
    WHERE START_TIME >= DATEADD(DAY, -7, CURRENT_TIMESTAMP())
),
warehouse_cost AS (
    SELECT WAREHOUSE_NAME, START_TIME, CREDITS_USED
    FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
    WHERE START_TIME >= DATEADD(DAY, -7, CURRENT_TIMESTAMP())
)
SELECT q.QUERY_ID, q.USER_NAME, a.account_name,
    CASE
        WHEN a.account_region ILIKE '%AWS_US_EAST_1%' THEN 'US East (N. Virginia)'
        WHEN a.account_region ILIKE '%AWS_US_WEST_2%' THEN 'US West (Oregon)'
        WHEN a.account_region ILIKE '%AWS_EU_WEST_1%' THEN 'EU (Ireland)'
        WHEN a.account_region ILIKE '%AZURE%' THEN 'Azure Region'
        WHEN a.account_region ILIKE '%GCP%'   THEN 'GCP Region'
        ELSE 'Other Region'
    END AS region_name,
    COALESCE(w.CREDITS_USED, 0) * 2.0 AS estimated_cost_usd,
    q.elapsed_ms
FROM query_usage q
LEFT JOIN warehouse_cost w
    ON q.WAREHOUSE_NAME = w.WAREHOUSE_NAME
    AND DATE_TRUNC('HOUR', q.START_TIME) = DATE_TRUNC('HOUR', w.START_TIME)
JOIN account_info a ON TRUE
ORDER BY q.START_TIME DESC LIMIT 100
) AS health_check_result LIMIT 1001;

-- CHECK: warehouse_utilization | warehouse_health | Warehouse Utilization
-- NOTE: Uses the available Account Usage history, without a reporting-window filter. The row cap limits output, not the scan.
SELECT * FROM (
SELECT WAREHOUSE_NAME,
       AVG(AVG_RUNNING) AS AVG_LOAD,
       MAX(AVG_RUNNING) AS PEAK_LOAD
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_LOAD_HISTORY
GROUP BY 1
) AS health_check_result LIMIT 1001;

-- CHECK: queue_blocking_7_days | warehouse_health | Queue & Blocking (7 days)
SELECT * FROM (
SELECT WAREHOUSE_NAME,
       ROUND(AVG(AVG_RUNNING), 3)     AS AVG_RUNNING,
       ROUND(MAX(AVG_RUNNING), 3)     AS PEAK_RUNNING,
       ROUND(AVG(AVG_QUEUED_LOAD), 3) AS AVG_QUEUE_LOAD,
       ROUND(MAX(AVG_QUEUED_LOAD), 3) AS PEAK_QUEUE_LOAD
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_LOAD_HISTORY
WHERE START_TIME >= DATEADD(DAY, -7, CURRENT_TIMESTAMP())
GROUP BY WAREHOUSE_NAME
) AS health_check_result LIMIT 1001;

-- CHECK: utilization_status | warehouse_health | Utilization Status
-- NOTE: Dashboard thresholds are heuristics. AVG_RUNNING measures query load, not CPU utilization or a validated warehouse sizing recommendation.
SELECT * FROM (
SELECT WAREHOUSE_NAME,
       ROUND(AVG(AVG_RUNNING), 3) AS AVG_RUNNING,
       CASE WHEN AVG(AVG_RUNNING) < 0.2  THEN 'OVER-PROVISIONED'
            WHEN AVG(AVG_RUNNING) <= 0.7 THEN 'OPTIMAL'
            ELSE 'UNDER-PROVISIONED' END AS UTILIZATION_STATUS,
       ROUND(MAX(AVG_QUEUED_LOAD), 3) AS PEAK_QUEUE,
       CASE WHEN MAX(AVG_QUEUED_LOAD) > 0 THEN 'CONCURRENCY ISSUE'
            ELSE 'NO QUEUE' END AS QUEUE_STATUS
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_LOAD_HISTORY
WHERE START_TIME >= DATEADD(DAY, -7, CURRENT_TIMESTAMP())
GROUP BY WAREHOUSE_NAME
) AS health_check_result LIMIT 1001;

-- CHECK: idle_warehouse_credits | warehouse_health | Idle Warehouse Credits
-- NOTE: Uses the available Account Usage history, without a reporting-window filter. The row cap limits output, not the scan.
-- NOTE: This query measures compute credits, not idle credits. Idle attribution is not calculated.
SELECT * FROM (
SELECT WAREHOUSE_NAME,
       SUM(CREDITS_USED) - SUM(CREDITS_USED_CLOUD_SERVICES) AS COMPUTE_CREDITS
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
GROUP BY 1
) AS health_check_result LIMIT 1001;

-- CHECK: slow_queries_last_7_days | query_performance | Slow Queries (Last 7 Days)
SELECT * FROM (
SELECT QUERY_ID, USER_NAME, WAREHOUSE_NAME, EXECUTION_STATUS,
       TOTAL_ELAPSED_TIME/1000      AS EXEC_TIME_SEC,
       COMPILATION_TIME/1000        AS COMPILATION_SEC,
       BYTES_SCANNED/1024/1024/1024 AS GB_SCANNED
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE START_TIME >= DATEADD(DAY, -7, CURRENT_TIMESTAMP())
ORDER BY EXEC_TIME_SEC DESC LIMIT 50
) AS health_check_result LIMIT 1001;

-- CHECK: query_antipatterns | query_performance | Query Antipatterns
-- NOTE: Uses the available Account Usage history, without a reporting-window filter. The row cap limits output, not the scan.
SELECT * FROM (
SELECT QUERY_ID
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE QUERY_TEXT ILIKE '%SELECT *%'
   OR QUERY_TEXT ILIKE '%CROSS JOIN%'
LIMIT 100
) AS health_check_result LIMIT 1001;

-- CHECK: query_spills | query_performance | Query Spills
-- NOTE: Uses the available Account Usage history, without a reporting-window filter. The row cap limits output, not the scan.
SELECT * FROM (
SELECT QUERY_ID,
       BYTES_SPILLED_TO_LOCAL_STORAGE,
       BYTES_SPILLED_TO_REMOTE_STORAGE
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE BYTES_SPILLED_TO_REMOTE_STORAGE > 0
ORDER BY BYTES_SPILLED_TO_REMOTE_STORAGE DESC
) AS health_check_result LIMIT 1001;

-- CHECK: largest_tables | table_schema | Largest Tables
-- NOTE: Preserves dashboard metadata filters; deleted objects may be included when the view retains them.
SELECT * FROM (
SELECT TABLE_SCHEMA, TABLE_NAME,
       BYTES/1024/1024/1024 AS SIZE_GB
FROM SNOWFLAKE.ACCOUNT_USAGE.TABLES
ORDER BY SIZE_GB DESC LIMIT 50
) AS health_check_result LIMIT 1001;

-- CHECK: table_growth | table_schema | Table Growth
-- NOTE: Current metadata snapshot, not measured growth over time.
-- NOTE: Preserves dashboard metadata filters; deleted objects may be included when the view retains them.
SELECT * FROM (
SELECT TABLE_NAME,
       DATE_TRUNC('DAY', CREATED) AS CREATED_DATE,
       BYTES/1024/1024/1024 AS SIZE_GB
FROM SNOWFLAKE.ACCOUNT_USAGE.TABLES
ORDER BY CREATED_DATE DESC
) AS health_check_result LIMIT 1001;

-- CHECK: clustering_health | table_schema | Clustering Health
-- NOTE: Lists configured clustering keys; does not measure clustering quality.
-- NOTE: Preserves dashboard metadata filters; deleted objects may be included when the view retains them.
SELECT * FROM (
SELECT TABLE_NAME, CLUSTERING_KEY
FROM SNOWFLAKE.ACCOUNT_USAGE.TABLES
WHERE CLUSTERING_KEY IS NOT NULL
) AS health_check_result LIMIT 1001;

-- CHECK: poor_pruning_queries | table_schema | Poor Pruning Queries
-- NOTE: Uses the available Account Usage history, without a reporting-window filter. The row cap limits output, not the scan.
SELECT * FROM (
SELECT QUERY_ID, PARTITIONS_SCANNED, PARTITIONS_TOTAL,
       (PARTITIONS_SCANNED/NULLIF(PARTITIONS_TOTAL,0))*100 AS SCAN_PERCENT
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE PARTITIONS_SCANNED > 0
ORDER BY SCAN_PERCENT DESC
) AS health_check_result LIMIT 1001;

-- CHECK: schema_growth | table_schema | Schema Growth
-- NOTE: Current metadata snapshot, not measured growth over time.
-- NOTE: Preserves dashboard metadata filters; deleted objects may be included when the view retains them.
SELECT * FROM (
SELECT TABLE_SCHEMA,
       SUM(BYTES)/1024/1024/1024 AS SIZE_GB
FROM SNOWFLAKE.ACCOUNT_USAGE.TABLES
GROUP BY TABLE_SCHEMA ORDER BY SIZE_GB DESC
) AS health_check_result LIMIT 1001;

-- CHECK: pipe_cost_7_days | pipeline_health | Pipe Cost (7 days)
SELECT * FROM (
SELECT PIPE_NAME, SUM(CREDITS_USED) AS TOTAL_CREDITS,
       SUM(BYTES_INSERTED)/1024/1024/1024 AS GB_INSERTED
FROM SNOWFLAKE.ACCOUNT_USAGE.PIPE_USAGE_HISTORY
WHERE START_TIME >= DATEADD(DAY, -7, CURRENT_TIMESTAMP())
GROUP BY PIPE_NAME ORDER BY TOTAL_CREDITS DESC
) AS health_check_result LIMIT 1001;

-- CHECK: pipe_small_files | pipeline_health | Pipe Small Files
-- NOTE: Only pipes with visible usage history are represented; absence is not proof of health. Names can repeat across schemas.
SELECT * FROM (
SELECT PIPE_NAME,
       AVG(BYTES_INSERTED/NULLIF(FILES_INSERTED,0)) AS AVG_FILE_SIZE_BYTES,
       SUM(FILES_INSERTED) AS TOTAL_FILES
FROM SNOWFLAKE.ACCOUNT_USAGE.PIPE_USAGE_HISTORY
WHERE START_TIME >= DATEADD(DAY, -7, CURRENT_TIMESTAMP())
GROUP BY PIPE_NAME
HAVING AVG_FILE_SIZE_BYTES < 10*1024*1024
) AS health_check_result LIMIT 1001;

-- CHECK: pipe_trend | pipeline_health | Pipe Trend
SELECT * FROM (
SELECT PIPE_NAME,
       DATE_TRUNC('HOUR', START_TIME) AS HOUR,
       SUM(BYTES_INSERTED)/1024/1024/1024 AS GB_LOADED
FROM SNOWFLAKE.ACCOUNT_USAGE.PIPE_USAGE_HISTORY
WHERE START_TIME >= DATEADD(DAY, -7, CURRENT_TIMESTAMP())
GROUP BY PIPE_NAME, HOUR
) AS health_check_result LIMIT 1001;

-- CHECK: idle_pipes | pipeline_health | Idle Pipes
-- NOTE: Uses the available Account Usage history, without a reporting-window filter. The row cap limits output, not the scan.
-- NOTE: Only pipes with visible usage history are represented; absence is not proof of health. Names can repeat across schemas.
SELECT * FROM (
SELECT PIPE_NAME, MAX(END_TIME) AS LAST_LOAD_TIME
FROM SNOWFLAKE.ACCOUNT_USAGE.PIPE_USAGE_HISTORY
GROUP BY PIPE_NAME
HAVING LAST_LOAD_TIME < DATEADD(DAY, -1, CURRENT_TIMESTAMP())
) AS health_check_result LIMIT 1001;

-- CHECK: unused_users_30_days | security_rbac | Unused Users (30+ days)
-- NOTE: Preserves dashboard metadata filters; deleted objects may be included when the view retains them.
SELECT * FROM (
SELECT NAME, LAST_SUCCESS_LOGIN
FROM SNOWFLAKE.ACCOUNT_USAGE.USERS
WHERE LAST_SUCCESS_LOGIN < DATEADD(DAY, -30, CURRENT_TIMESTAMP())
) AS health_check_result LIMIT 1001;

-- CHECK: stored_procedure_failures | security_rbac | Stored Procedure Failures
-- NOTE: Uses the available Account Usage history, without a reporting-window filter. The row cap limits output, not the scan.
SELECT * FROM (
SELECT QUERY_ID, ERROR_CODE
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE ERROR_MESSAGE IS NOT NULL AND QUERY_TYPE = 'CALL'
) AS health_check_result LIMIT 1001;

-- CHECK: data_staleness_24_hours | security_rbac | Data Staleness (24+ hours)
-- NOTE: LAST_ALTERED includes metadata/background changes; this is not proof of data freshness. Legacy query groups by table name only, which can combine same-named tables across schemas.
-- NOTE: Preserves dashboard metadata filters; deleted objects may be included when the view retains them.
SELECT * FROM (
SELECT TABLE_NAME, MAX(LAST_ALTERED) AS LAST_UPDATED
FROM SNOWFLAKE.ACCOUNT_USAGE.TABLES
GROUP BY TABLE_NAME
HAVING LAST_UPDATED < DATEADD(HOUR, -24, CURRENT_TIMESTAMP())
) AS health_check_result LIMIT 1001;

-- CHECK: resource_monitors | security_rbac | Resource Monitors
SHOW RESOURCE MONITORS;

-- CHECK: total_credits_7d | executive_insights | Total Credits 7D
SELECT * FROM (
SELECT ROUND(SUM(CREDITS_USED), 2)
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE START_TIME >= DATEADD(DAY, -7, CURRENT_TIMESTAMP())
) AS health_check_result LIMIT 1001;

-- CHECK: total_credits_30d | executive_insights | Total Credits 30D
SELECT * FROM (
SELECT ROUND(SUM(CREDITS_USED), 2)
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE START_TIME >= DATEADD(DAY, -30, CURRENT_TIMESTAMP())
) AS health_check_result LIMIT 1001;

-- CHECK: over_prov_count | executive_insights | Over Prov Count
-- NOTE: Dashboard thresholds are heuristics. AVG_RUNNING measures query load, not CPU utilization or a validated warehouse sizing recommendation.
SELECT * FROM (
SELECT COUNT(*)
FROM (
    SELECT WAREHOUSE_NAME, AVG(AVG_RUNNING) AS AVG_RUN
    FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_LOAD_HISTORY
    WHERE START_TIME >= DATEADD(DAY, -7, CURRENT_TIMESTAMP())
    GROUP BY WAREHOUSE_NAME
    HAVING AVG_RUN < 0.2
)
) AS health_check_result LIMIT 1001;

-- CHECK: under_prov_count | executive_insights | Under Prov Count
-- NOTE: Dashboard thresholds are heuristics. AVG_RUNNING measures query load, not CPU utilization or a validated warehouse sizing recommendation.
SELECT * FROM (
SELECT COUNT(*)
FROM (
    SELECT WAREHOUSE_NAME, AVG(AVG_RUNNING) AS AVG_RUN
    FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_LOAD_HISTORY
    WHERE START_TIME >= DATEADD(DAY, -7, CURRENT_TIMESTAMP())
    GROUP BY WAREHOUSE_NAME
    HAVING AVG_RUN > 0.7
)
) AS health_check_result LIMIT 1001;

-- CHECK: wh_queue_issues | executive_insights | Wh Queue Issues
SELECT * FROM (
SELECT COUNT(*)
FROM (
    SELECT WAREHOUSE_NAME
    FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_LOAD_HISTORY
    WHERE START_TIME >= DATEADD(DAY, -7, CURRENT_TIMESTAMP())
    GROUP BY WAREHOUSE_NAME
    HAVING MAX(AVG_QUEUED_LOAD) > 0.5
)
) AS health_check_result LIMIT 1001;

-- CHECK: slow_query_count | executive_insights | Slow Query Count
SELECT * FROM (
SELECT COUNT(*)
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE START_TIME >= DATEADD(DAY, -7, CURRENT_TIMESTAMP())
  AND TOTAL_ELAPSED_TIME > 300000
) AS health_check_result LIMIT 1001;

-- CHECK: spill_count | executive_insights | Spill Count
SELECT * FROM (
SELECT COUNT(*)
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE BYTES_SPILLED_TO_REMOTE_STORAGE > 0
  AND START_TIME >= DATEADD(DAY, -7, CURRENT_TIMESTAMP())
) AS health_check_result LIMIT 1001;

-- CHECK: antipattern_count | executive_insights | Antipattern Count
SELECT * FROM (
SELECT COUNT(*)
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE START_TIME >= DATEADD(DAY, -7, CURRENT_TIMESTAMP())
  AND (QUERY_TEXT ILIKE '%SELECT *%' OR QUERY_TEXT ILIKE '%CROSS JOIN%')
) AS health_check_result LIMIT 1001;

-- CHECK: unused_users | executive_insights | Unused Users
-- NOTE: Preserves dashboard metadata filters; deleted objects may be included when the view retains them.
SELECT * FROM (
SELECT COUNT(*)
FROM SNOWFLAKE.ACCOUNT_USAGE.USERS
WHERE LAST_SUCCESS_LOGIN < DATEADD(DAY, -30, CURRENT_TIMESTAMP())
   OR LAST_SUCCESS_LOGIN IS NULL
) AS health_check_result LIMIT 1001;

-- CHECK: login_failures_7d | executive_insights | Login Failures 7D
SELECT * FROM (
SELECT COUNT(*)
FROM SNOWFLAKE.ACCOUNT_USAGE.LOGIN_HISTORY
WHERE IS_SUCCESS = 'NO'
  AND EVENT_TIMESTAMP >= DATEADD(DAY, -7, CURRENT_TIMESTAMP())
) AS health_check_result LIMIT 1001;

-- CHECK: sp_failures | executive_insights | Sp Failures
SELECT * FROM (
SELECT COUNT(*)
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE ERROR_MESSAGE IS NOT NULL AND QUERY_TYPE = 'CALL'
  AND START_TIME >= DATEADD(DAY, -7, CURRENT_TIMESTAMP())
) AS health_check_result LIMIT 1001;

-- CHECK: total_tables | executive_insights | Total Tables
-- NOTE: Preserves dashboard metadata filters; deleted objects may be included when the view retains them.
SELECT * FROM (
SELECT COUNT(*) FROM SNOWFLAKE.ACCOUNT_USAGE.TABLES
) AS health_check_result LIMIT 1001;

-- CHECK: stale_tables | executive_insights | Stale Tables
-- NOTE: LAST_ALTERED includes metadata/background changes; this is not proof of data freshness. Legacy query groups by table name only, which can combine same-named tables across schemas.
-- NOTE: Preserves dashboard metadata filters; deleted objects may be included when the view retains them.
SELECT * FROM (
SELECT COUNT(*)
FROM (
    SELECT TABLE_NAME, MAX(LAST_ALTERED) AS LAST_UPDATED
    FROM SNOWFLAKE.ACCOUNT_USAGE.TABLES
    GROUP BY TABLE_NAME
    HAVING LAST_UPDATED < DATEADD(HOUR, -72, CURRENT_TIMESTAMP())
)
) AS health_check_result LIMIT 1001;

-- CHECK: clustered_tables | executive_insights | Clustered Tables
-- NOTE: Preserves dashboard metadata filters; deleted objects may be included when the view retains them.
SELECT * FROM (
SELECT COUNT(*)
FROM SNOWFLAKE.ACCOUNT_USAGE.TABLES
WHERE CLUSTERING_KEY IS NOT NULL
) AS health_check_result LIMIT 1001;

-- CHECK: idle_pipes_count | executive_insights | Idle Pipes Count (7+ days)
-- NOTE: Uses the available Account Usage history, without a reporting-window filter. The row cap limits output, not the scan.
-- NOTE: Only pipes with visible usage history are represented; absence is not proof of health. Names can repeat across schemas.
SELECT * FROM (
SELECT COUNT(*)
FROM (
    SELECT PIPE_NAME, MAX(END_TIME) AS LAST_LOAD
    FROM SNOWFLAKE.ACCOUNT_USAGE.PIPE_USAGE_HISTORY
    GROUP BY PIPE_NAME
    HAVING LAST_LOAD < DATEADD(DAY, -7, CURRENT_TIMESTAMP())
)
) AS health_check_result LIMIT 1001;

-- CHECK: small_file_pipes | executive_insights | Small File Pipes
-- NOTE: Only pipes with visible usage history are represented; absence is not proof of health. Names can repeat across schemas.
SELECT * FROM (
SELECT COUNT(*)
FROM (
    SELECT PIPE_NAME,
           AVG(BYTES_INSERTED / NULLIF(FILES_INSERTED, 0)) AS AVG_FILE_SIZE
    FROM SNOWFLAKE.ACCOUNT_USAGE.PIPE_USAGE_HISTORY
    WHERE START_TIME >= DATEADD(DAY, -7, CURRENT_TIMESTAMP())
    GROUP BY PIPE_NAME
    HAVING AVG_FILE_SIZE < 1024 * 1024
)
) AS health_check_result LIMIT 1001;

-- CHECK: df_top_wh | executive_insights | Top Wh
SELECT * FROM (
SELECT WAREHOUSE_NAME, ROUND(SUM(CREDITS_USED), 2) AS CREDITS
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE START_TIME >= DATEADD(DAY, -7, CURRENT_TIMESTAMP())
GROUP BY WAREHOUSE_NAME ORDER BY CREDITS DESC LIMIT 5
) AS health_check_result LIMIT 1001;

-- CHECK: df_daily_spend | executive_insights | Daily Spend
SELECT * FROM (
SELECT DATE_TRUNC('DAY', START_TIME) AS DAY,
       ROUND(SUM(CREDITS_USED), 2) AS CREDITS
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE START_TIME >= DATEADD(DAY, -7, CURRENT_TIMESTAMP())
GROUP BY 1 ORDER BY 1 ASC
) AS health_check_result LIMIT 1001;

-- CHECK: df_top_users | executive_insights | Top Users
SELECT * FROM (
SELECT USER_NAME, COUNT(*) AS QUERY_COUNT,
       ROUND(SUM(TOTAL_ELAPSED_TIME)/1000/60, 1) AS TOTAL_EXEC_MIN
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE START_TIME >= DATEADD(DAY, -7, CURRENT_TIMESTAMP())
GROUP BY USER_NAME ORDER BY TOTAL_EXEC_MIN DESC LIMIT 8
) AS health_check_result LIMIT 1001;
