CREATE OR REPLACE VIEW PRD_DATA_VAULT.PRD_INFO_MART_OTC.VW_TRANSACTION_ARB_NEW_EXISTING_CLASSIFICATION
AS
/* --------------------------------------------------------------------------------------------------------------*/
/* ----------    GET Previous Fiscal Year Same Quarter Dates Based ON the FISCAL quarter --------------*/
/* ----------    Starting from the Quarter which CURRENT Date Belongs upto backward  --------------*/
/* --------------------------------------------------------------------------------------------------------------*/
WITH CTE_LIST_PREV_FY_SAME_QTR_FY_QTR AS
(
    SELECT
        CURR.FISCAL_QUARTER                                                   AS FISCAL_QUARTER,
        CURR.FISCAL_QUARTER_START_DATE                                        AS FISCAL_QUARTER_START_DATE,
        CURR.FISCAL_QUARTER_END_DATE                                          AS FISCAL_QUARTER_END_DATE,
        PREV_YR.FISCAL_QUARTER                                                AS PREV_YR_FISCAL_QUARTER,
        CURR.PREV_YR_FISCAL_QUARTER_START_DATE                                AS PREV_YR_FISCAL_QUARTER_START_DATE,
        CURR.PREV_YR_FISCAL_QUARTER_END_DATE                                  AS PREV_YR_FISCAL_QUARTER_END_DATE
    FROM
    (
        SELECT
          DT.FISCAL_QUARTER                                                   AS FISCAL_QUARTER,
          DT.FISCAL_QUARTER_START_DATE                                        AS FISCAL_QUARTER_START_DATE,
          DT.FISCAL_QUARTER_END_DATE                                          AS FISCAL_QUARTER_END_DATE,
          DATEADD(year,-1,DT.FISCAL_QUARTER_START_DATE)                       AS PREV_YR_FISCAL_QUARTER_START_DATE,
          DATEADD(year,-1,DT.FISCAL_QUARTER_END_DATE)                         AS PREV_YR_FISCAL_QUARTER_END_DATE
        FROM
        "PRD_INFO_MART_DIM"."DATE_DIM" DT
        WHERE 1=1
        AND FISCAL_DATE = FISCAL_QUARTER_END_DATE
        AND DT.FISCAL_DATE <= (
                                SELECT FISCAL_QUARTER_END_DATE 
                                FROM "PRD_INFO_MART_DIM"."DATE_DIM" 
                                WHERE 1=1 
                                AND FISCAL_DATE=DATEADD(day,-1,CURRENT_DATE)
                              )
    ) CURR
    JOIN (SELECT FISCAL_DATE,FISCAL_QUARTER,FISCAL_QUARTER_START_DATE,FISCAL_QUARTER_END_DATE 
          FROM "PRD_INFO_MART_DIM"."DATE_DIM" 
          WHERE 1=1 AND FISCAL_DATE = FISCAL_QUARTER_END_DATE
         ) PREV_YR
    ON (
            CURR.PREV_YR_FISCAL_QUARTER_START_DATE = PREV_YR.FISCAL_QUARTER_START_DATE  
        AND CURR.PREV_YR_FISCAL_QUARTER_END_DATE = PREV_YR.FISCAL_QUARTER_END_DATE
       )
    WHERE 1=1
)
/* --------------------------------------------------------------------------------------------------------------*/
/* -------   GET Accounts With Non Zero ARR's    -----------*/
/* --------------------------------------------------------------------------------------------------------------*/
,CTE_ACCT_NON_ZERO_ARR AS
(
    SELECT 
      ARR.ACCOUNT_ID                                                AS ACCOUNT_ID,
      ARR.FISCAL_YEAR                                               AS FISCAL_YEAR,
      ARR.FISCAL_QUARTER                                            AS FISCAL_QUARTER,
      ARR.FISCAL_QUARTER_START_DATE                                 AS FISCAL_QUARTER_START_DATE,
      ARR.FISCAL_QUARTER_END_DATE                                   AS FISCAL_QUARTER_END_DATE,
      ARR.ARB_AMOUNT                                                AS ARB_AMOUNT,
      ARR.ARR_AMOUNT                                                AS ARR_AMOUNT
    FROM 
    "PRD_INFO_MART_OTC"."VW_ACCOUNT_ARB_ARR_CALC" ARR
    WHERE 1=1
    AND ARR.ARR_AMOUNT > 0
)
/* --------------------------------------------------------------------------------------------------------------*/
/* -------   Calculate NEW/Existing Flag Against Each Account Per Netsuite Transactional Record with No Proportion Data   -----------*/
/* --------------------------------------------------------------------------------------------------------------*/
,CTE_CALC_NEW_ACCOUNT_FLAG AS
(
    SELECT
          TRNS.FISCAL_YEAR,
          TRNS.FISCAL_QUARTER,
          TRNS.FISCAL_QUARTER_START_DATE,
          TRNS.FISCAL_QUARTER_END_DATE,
          TRNS.TRANS_SF_OPPORTUNITY,
          TRNS.END_USER_NAME, 
          TRNS.OPP_ACCOUNT_ID,
          ACCOUNT_DIM.NAME,
          ACCOUNT_DIM.INDUSTRY,
          ACCOUNT_DIM.INDUSTRY_NEW,
          ACCOUNT_DIM.ACCOUNT_GEO,
          ACCOUNT_DIM.ACCOUNT_SEGMENT,
          TRNS.OPPORTUNITY_ID,
          TRNS.OPP_TYPE,
          TRNS.OPPORTUNITY_NAME, 
          TRNS.OPP_OWNER_NAME,
          TRNS.TRANSACTION_TYPE, 
          TRNS.PO_NUMBER, 
          TRNS.TRANDATE, 
          TRNS.TRANID,
          TRNS.TRANSACTION_ID,
          TRNS.TRANSACTION_LINE_ID,
          TRNS.ITEM_FULL_NAME, 
          TRNS.ITEM_SALES_DESCRIPTION,
          TRNS.PRIMARY_USE_CASE,
          TRNS.SECONDARY_USE_CASES,
          TRNS.PRODUCT_FAMILY,
          TRNS.PRODUCT_FAMILY_SUBTYPE_NAME,
          TRNS.PRODUCT_CATEGORY,
          TRNS.SUB_RENEWAL,
          TRNS.SUPPOSED_TO_BE_SUB_RENEWAL,
          TRNS.CHART_OF_ACCOUNT_FULL_NAME,
          TRNS.CHART_OF_ACCOUNTS_TYPE_NAME,
          TRNS.LEGACY_TERM_START_DATE,
          TRNS.LEGACY_TERM_END_DATE,
          TRNS.AMOUNT_FOREIGN,
          TRNS.ARB_FLAG,
          TRNS.ROUNDED_LEGACY_TERMS_MONTHS,
          TRNS.ROUNDED_LEGACY_TERMS_QUARTERS,
          TRNS.AMOUNT_CHANGE,
          TRNS.SW_PERCENT,
          TRNS.NEW_SW_SUPPORT,
          TRNS.NEW_AMOUNT,
          TRNS.MULTI_YEAR,
          TRNS.ARB_AMOUNT,
          QTR.PREV_YR_FISCAL_QUARTER,
          QTR.PREV_YR_FISCAL_QUARTER_START_DATE,
          QTR.PREV_YR_FISCAL_QUARTER_END_DATE,
          CASE
            WHEN NZA.FISCAL_QUARTER IS NULL THEN 'New'
            ELSE 'Existing'
          END AS NEW_ACCOUNT_FLAG
    FROM "PRD_INFO_MART_OTC"."TRANSACTION_ARB_CALC" TRNS
    LEFT JOIN CTE_LIST_PREV_FY_SAME_QTR_FY_QTR QTR
    ON (TRNS.FISCAL_QUARTER_START_DATE = QTR.FISCAL_QUARTER_START_DATE AND TRNS.FISCAL_QUARTER_END_DATE = QTR.FISCAL_QUARTER_END_DATE)
    LEFT JOIN CTE_ACCT_NON_ZERO_ARR NZA
    ON (TRNS.OPP_ACCOUNT_ID = NZA.ACCOUNT_ID AND QTR.PREV_YR_FISCAL_QUARTER_END_DATE = NZA.FISCAL_QUARTER_END_DATE)
    LEFT JOIN "PRD_INFO_MART_DIM"."ACCOUNT_DIM" ACCOUNT_DIM ON (TRNS.OPP_ACCOUNT_ID = ACCOUNT_DIM.SF_ACCOUNT_ID)
    WHERE 1=1
    AND TRNS.ARB_FLAG='Y'
    AND TRNS.TRANSACTION_TYPE IN ('Invoice','Credit Memo')
    AND TRNS.CHART_OF_ACCOUNT_FULL_NAME IS NOT NULL
    AND TRNS.CHART_OF_ACCOUNT_FULL_NAME <> ''
    AND TRNS.TRANS_SF_OPPORTUNITY IS NOT NULL
    AND TRNS.TRANS_SF_OPPORTUNITY <> ''
)
SELECT
    TRNS.FISCAL_YEAR,
    TRNS.FISCAL_QUARTER,
    TRNS.FISCAL_QUARTER_START_DATE,
    TRNS.FISCAL_QUARTER_END_DATE,
    TRNS.TRANS_SF_OPPORTUNITY,
    TRNS.END_USER_NAME, 
    TRNS.OPP_ACCOUNT_ID, 
    TRNS.NAME,
    TRNS.INDUSTRY,
    TRNS.INDUSTRY_NEW,
    TRNS.ACCOUNT_GEO,
    TRNS.ACCOUNT_SEGMENT,
    TRNS.OPPORTUNITY_ID,
    TRNS.OPP_TYPE,
    TRNS.OPPORTUNITY_NAME, 
    TRNS.OPP_OWNER_NAME,
    TRNS.TRANSACTION_TYPE, 
    TRNS.PO_NUMBER, 
    TRNS.TRANDATE, 
    TRNS.TRANID,
    TRNS.TRANSACTION_ID,
    TRNS.TRANSACTION_LINE_ID,
    TRNS.ITEM_FULL_NAME, 
    TRNS.ITEM_SALES_DESCRIPTION,
    TRNS.PRIMARY_USE_CASE,
    TRNS.SECONDARY_USE_CASES,
    TRNS.PRODUCT_FAMILY,
    TRNS.PRODUCT_FAMILY_SUBTYPE_NAME,
    TRNS.PRODUCT_CATEGORY,
    TRNS.SUB_RENEWAL,
    TRNS.SUPPOSED_TO_BE_SUB_RENEWAL,
    TRNS.CHART_OF_ACCOUNT_FULL_NAME,
    TRNS.CHART_OF_ACCOUNTS_TYPE_NAME,
    TRNS.LEGACY_TERM_START_DATE,
    TRNS.LEGACY_TERM_END_DATE,
    TRNS.AMOUNT_FOREIGN,
    TRNS.ARB_FLAG,
    TRNS.ROUNDED_LEGACY_TERMS_MONTHS,
    TRNS.ROUNDED_LEGACY_TERMS_QUARTERS,
    TRNS.AMOUNT_CHANGE,
    TRNS.SW_PERCENT,
    TRNS.NEW_SW_SUPPORT,
    TRNS.NEW_AMOUNT,
    TRNS.MULTI_YEAR,
    TRNS.ARB_AMOUNT,
    TRNS.PREV_YR_FISCAL_QUARTER,
    TRNS.PREV_YR_FISCAL_QUARTER_START_DATE,
    TRNS.PREV_YR_FISCAL_QUARTER_END_DATE,
    TRNS.NEW_ACCOUNT_FLAG
FROM CTE_CALC_NEW_ACCOUNT_FLAG TRNS
WHERE 1=1
;

