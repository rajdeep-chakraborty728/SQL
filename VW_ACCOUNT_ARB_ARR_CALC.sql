CREATE OR REPLACE VIEW PRD_DATA_VAULT.PRD_INFO_MART_OTC.VW_ACCOUNT_ARB_ARR_CALC
AS
-- Aggregate Account Wise Quarterly ARB Figures
WITH CTE_ACCOUNT_FY_QTR_ARB AS
(
    SELECT 
        X.OPP_ACCOUNT_ID                            AS ACCOUNT_ID,
        X.FISCAL_YEAR                               AS FISCAL_YEAR,
        X.FISCAL_QUARTER                            AS FISCAL_QUARTER,
        X.FISCAL_QUARTER_START_DATE                 AS FISCAL_QUARTER_START_DATE,
        X.FISCAL_QUARTER_END_DATE                   AS FISCAL_QUARTER_END_DATE,
        SUM(X.ARB_AMOUNT)                           AS ARB_AMOUNT
    FROM "PRD_INFO_MART_OTC"."VW_TRANSACTION_ARB_ANNIVERSARY_SPLIT" X
    WHERE 1=1
    GROUP BY 
        X.OPP_ACCOUNT_ID,
        X.FISCAL_YEAR,
        X.FISCAL_QUARTER,
        X.FISCAL_QUARTER_START_DATE,
        X.FISCAL_QUARTER_END_DATE
)
-- Get The Quarter End Date of Last Quarter Based On Current Calender Date
,CTE_GET_CURR_FY_QTR_STRT_END AS
(   
    SELECT 
        FISCAL_QUARTER_START_DATE,
        FISCAL_QUARTER_END_DATE
    FROM "PRD_INFO_MART_DIM"."DATE_DIM"
    WHERE 1=1
    AND FISCAL_DATE=DATEADD(day,-1,CURRENT_DATE)
)
-- Get The Minimum Quarter Start / End Date from ARB Aggregation Data and Merge With Current Quarter Start End Dates
,CTE_QUARTER_RANGE_ARR AS
(
    SELECT
        ARR.MIN_FISCAL_QUARTER_START_DATE                   AS MIN_FISCAL_QUARTER_START_DATE,
        ARR.MIN_FISCAL_QUARTER_END_DATE                     AS MIN_FISCAL_QUARTER_END_DATE,
        DATEADD(year,-1,ARR.MIN_FISCAL_QUARTER_START_DATE)  AS ARR_MIN_FISCAL_QUARTER_START_DATE,
        DATEADD(year,-1,ARR.MIN_FISCAL_QUARTER_END_DATE)    AS ARR_MIN_FISCAL_QUARTER_END_DATE,
        CURR_QTR.FISCAL_QUARTER_START_DATE                  AS MAX_FISCAL_QUARTER_START_DATE,
        CURR_QTR.FISCAL_QUARTER_END_DATE                    AS MAX_FISCAL_QUARTER_END_DATE
    FROM
    (
        SELECT 
            MIN(FISCAL_QUARTER_START_DATE)                  AS MIN_FISCAL_QUARTER_START_DATE,
            MIN(FISCAL_QUARTER_END_DATE)                    AS MIN_FISCAL_QUARTER_END_DATE
        FROM
        CTE_ACCOUNT_FY_QTR_ARB
        WHERE 1=1
    ) ARR
    CROSS JOIN CTE_GET_CURR_FY_QTR_STRT_END CURR_QTR
)
-- Get START and END Quarter Range For Each & Every Quarter For Rolling 4 Quarters
,CTE_QUARTER_RANGE_CALC_ARR AS
(
    SELECT
        FISCAL_YEAR,
        FISCAL_QUARTER,
        FISCAL_QUARTER_START_DATE,
        FISCAL_QUARTER_END_DATE,
        QTR_TYP,
        RN,
        LEAD(FISCAL_QUARTER_END_DATE,3) OVER(ORDER BY FISCAL_QUARTER_END_DATE DESC) AS END_RANGE_FISCAL_QUARTER_END_DATE
    FROM
    (
        SELECT 
            DT.FISCAL_YEAR,
            DT.FISCAL_QUARTER,
            DT.FISCAL_QUARTER_START_DATE,
            DT.FISCAL_QUARTER_END_DATE,
            CASE
                WHEN DT.FISCAL_QUARTER_START_DATE < MIN_FISCAL_QUARTER_START_DATE 
                THEN 'Art'
                ELSE 'Tr'
            END AS QTR_TYP,
            ROW_NUMBER() OVER(ORDER BY DT.FISCAL_QUARTER_END_DATE DESC) AS RN
        FROM 
        "PRD_INFO_MART_DIM"."DATE_DIM" DT
        CROSS JOIN CTE_QUARTER_RANGE_ARR RANGE_ARR
        WHERE 1=1
        AND DT.FISCAL_DATE = DT.FISCAL_QUARTER_END_DATE
        AND DT.FISCAL_DATE BETWEEN RANGE_ARR.ARR_MIN_FISCAL_QUARTER_START_DATE AND RANGE_ARR.MAX_FISCAL_QUARTER_END_DATE
    ) T
)
-- Get The 4 Rolling Quarter Date Ranges Per Quarter
,CTE_QUARTER_RANGE_CALC_ARR_4_ROLLING AS
(
    SELECT  
        ACT.FISCAL_YEAR                                             AS FISCAL_YEAR,
        ACT.FISCAL_QUARTER                                          AS FISCAL_QUARTER,
        ACT.FISCAL_QUARTER_START_DATE                               AS FISCAL_QUARTER_START_DATE,
        ACT.FISCAL_QUARTER_END_DATE                                 AS FISCAL_QUARTER_END_DATE,
        ACT.END_RANGE_FISCAL_QUARTER_END_DATE                       AS END_RANGE_FISCAL_QUARTER_END_DATE,
        CROSS_ACT.FISCAL_YEAR                                       AS RUNNING_FISCAL_YEAR,
        CROSS_ACT.FISCAL_QUARTER                                    AS RUNNING_FISCAL_QUARTER,
        CROSS_ACT.FISCAL_QUARTER_START_DATE                         AS RUNNING_FISCAL_QUARTER_START_DATE,
        CROSS_ACT.FISCAL_QUARTER_END_DATE                           AS RUNNING_FISCAL_QUARTER_END_DATE,
        ROW_NUMBER() OVER(PARTITION BY ACT.FISCAL_QUARTER_END_DATE ORDER BY CROSS_ACT.FISCAL_QUARTER_END_DATE DESC) AS RN
    FROM CTE_QUARTER_RANGE_CALC_ARR ACT
    CROSS JOIN CTE_QUARTER_RANGE_CALC_ARR CROSS_ACT
    WHERE 1=1
    AND ACT.QTR_TYP='Tr'
    AND CROSS_ACT.FISCAL_QUARTER_END_DATE <= ACT.FISCAL_QUARTER_END_DATE AND CROSS_ACT.FISCAL_QUARTER_END_DATE >= ACT.END_RANGE_FISCAL_QUARTER_END_DATE
)
-- Calculate ARR For Last Four Rolling Quarters by Adding ARB's
,CTE_CALC_ACCOUNT_QTR_ARR AS
(
    SELECT 
        ACCOUNT_ID,
        FISCAL_YEAR,
        FISCAL_QUARTER,
        FISCAL_QUARTER_START_DATE,
        FISCAL_QUARTER_END_DATE,
        ARR_AMOUNT
    FROM
    (
        SELECT
            INNR.ACCOUNT_ID                         AS ACCOUNT_ID,
            INNR.FISCAL_YEAR                        AS FISCAL_YEAR,
            INNR.FISCAL_QUARTER                     AS FISCAL_QUARTER,
            INNR.FISCAL_QUARTER_START_DATE          AS FISCAL_QUARTER_START_DATE,
            INNR.FISCAL_QUARTER_END_DATE            AS FISCAL_QUARTER_END_DATE,
            INNR.RUNNING_FISCAL_YEAR                AS ARB_FISCAL_YEAR,
            INNR.RUNNING_FISCAL_QUARTER             AS ARB_FISCAL_QUARTER,
            INNR.RUNNING_FISCAL_QUARTER_START_DATE  AS ARB_FISCAL_QUARTER_START_DATE,
            INNR.RUNNING_FISCAL_QUARTER_END_DATE    AS ARB_FISCAL_QUARTER_END_DATE,
            ARB_AGG_DATA.ARB_AMOUNT                 AS ARB_AMOUNT,
            ROW_NUMBER() OVER(PARTITION BY INNR.ACCOUNT_ID,INNR.FISCAL_QUARTER ORDER BY INNR.RUNNING_FISCAL_QUARTER DESC) 
                                                    AS RN,
            0-NVL(SUM(ARB_AGG_DATA.ARB_AMOUNT) OVER(PARTITION BY INNR.ACCOUNT_ID,INNR.FISCAL_QUARTER),0) 
                                                    AS ARR_AMOUNT
        FROM
        (
            SELECT
                ARB_AGG.ACCOUNT_ID,
                ARR_QRTR.FISCAL_YEAR,
                ARR_QRTR.FISCAL_QUARTER,
                ARR_QRTR.FISCAL_QUARTER_START_DATE,
                ARR_QRTR.FISCAL_QUARTER_END_DATE,
                ARR_QRTR.RUNNING_FISCAL_YEAR,
                ARR_QRTR.RUNNING_FISCAL_QUARTER,
                ARR_QRTR.RUNNING_FISCAL_QUARTER_START_DATE,
                ARR_QRTR.RUNNING_FISCAL_QUARTER_END_DATE
            FROM 
            CTE_QUARTER_RANGE_CALC_ARR_4_ROLLING ARR_QRTR
            CROSS JOIN (SELECT DISTINCT ACCOUNT_ID FROM CTE_ACCOUNT_FY_QTR_ARB) ARB_AGG
            WHERE 1=1           
        ) INNR
        LEFT JOIN CTE_ACCOUNT_FY_QTR_ARB ARB_AGG_DATA
        ON (INNR.ACCOUNT_ID = ARB_AGG_DATA.ACCOUNT_ID AND INNR.RUNNING_FISCAL_QUARTER_END_DATE = ARB_AGG_DATA.FISCAL_QUARTER_END_DATE)
    ) OTR
    WHERE 1=1
    AND OTR.RN = 1
)
-- Combine ARR and ARB Together at the Account and Quarter Level
,CTE_CALC_ACCOUNT_QTR_ARR_ARB_COMBINED AS
(
    SELECT 
        ARR.ACCOUNT_ID,
        ACCOUNT_DIM.NAME,
        ACCOUNT_DIM.INDUSTRY,
        ACCOUNT_DIM.ACCOUNT_GEO,
        ACCOUNT_DIM.ACCOUNT_SEGMENT,
        ACCOUNT_DIM.INDUSTRY_NEW,
        ARR.FISCAL_YEAR,
        ARR.FISCAL_QUARTER,
        ARR.FISCAL_QUARTER_START_DATE,
        ARR.FISCAL_QUARTER_END_DATE,
        ARR.ARR_AMOUNT,
        ARB.ARB_AMOUNT,
        ROW_NUMBER() OVER(PARTITION BY ARR.ACCOUNT_ID,ARR.FISCAL_QUARTER ORDER BY ARR.FISCAL_QUARTER DESC)  AS RN
    FROM CTE_CALC_ACCOUNT_QTR_ARR ARR
    LEFT JOIN CTE_ACCOUNT_FY_QTR_ARB ARB
    ON (ARR.ACCOUNT_ID = ARB.ACCOUNT_ID AND ARR.FISCAL_QUARTER_END_DATE = ARB.FISCAL_QUARTER_END_DATE)
    LEFT JOIN "PRD_INFO_MART_DIM"."ACCOUNT_DIM" ACCOUNT_DIM ON (ARR.ACCOUNT_ID = ACCOUNT_DIM.SF_ACCOUNT_ID)
    WHERE 1=1
)
SELECT
   ACCOUNT_ID,
   NAME,
   INDUSTRY,
   INDUSTRY_NEW,
   ACCOUNT_GEO,
   ACCOUNT_SEGMENT,
   FISCAL_YEAR,
   FISCAL_QUARTER,
   FISCAL_QUARTER_START_DATE,
   FISCAL_QUARTER_END_DATE,
   ARB_AMOUNT,
   ARR_AMOUNT
FROM CTE_CALC_ACCOUNT_QTR_ARR_ARB_COMBINED
WHERE 1=1
;


