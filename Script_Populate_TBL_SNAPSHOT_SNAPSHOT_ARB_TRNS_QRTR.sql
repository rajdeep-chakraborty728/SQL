/****************************************************************************************** */
/* MODIFICATION LOG */
/* Version       Developer         Date                 Description */
/*------------------------------------------------------------------------------------------- */
/* 1      	    Rajdeep            01-DEC-2021          Initial Version */
/* 2        	Rajdeep            09-DEC-2021          NUMERIC Field Precission Changed to NUMBER(30,15) */
/*                                                      PRODUCT_FAMILY = 'Adjustment' Excluded From ARB Calculation */
/*                                                      Bundle Map Table Changed To ARR_ARB_BNDL_CONFIG_MAP and Joining Criteria Added With TRANDATE Between Active Dates */
/* 3			Rajdeep			   22-DEC-2021			ROUNDED_LEGACY_TERMS_QUARTERS logic changed to Rounding from Rounding Up */
/*														HW/SW Table Changed To SKU_HW_SW_PERCENTAGE	*/
/* 4			Rajdeep			   05-JAN-2022			ITEM_DIM Fields Pulled */
/****************************************************************************************** */



USE ${EnvVarTargetDatabase};

SET CONFIG_EXECUTION_DATE       ='${JobVarExecutionDate}';
SET CONFIG_FISCAL_YEAR          ='${JobVarFiscalYear}';
SET CONFIG_FISCAL_QUARTER       ='${JobVarFiscalQuarter}';
SET CONFIG_FISCAL_QUARTER_START_DATE   ='${JobVarFiscalQuarterStartDate}';
SET CONFIG_FISCAL_QUARTER_END_DATE     ='${JobVarFiscalQuarterEndDate}';

SET CONFIG_CONTROL_DATE_KEY = (SELECT TO_NUMERIC(TO_VARCHAR(TO_DATE($CONFIG_EXECUTION_DATE,'YYYY-MM-DD'),'YYYYMMDD')));

SET SNAPSHOT_SCHEMA= '${JobVarTargetSchema}';
SET SNAPSHOT_TABLE= '${JobVarTargetTable}';

SET SNAPSHOT_TABLE_FULL_QUALIFIER=$SNAPSHOT_SCHEMA||'.'||$SNAPSHOT_TABLE;

SET RUNTIME_SNAPSHOT_SCHEMA= $SNAPSHOT_SCHEMA;
SET RUNTIME_SNAPSHOT_TABLE= 'RUNTIME_'||$SNAPSHOT_TABLE;

SET RUNTIME_SNAPSHOT_TABLE_FULL_QUALIFIER=$RUNTIME_SNAPSHOT_SCHEMA||'.'||$RUNTIME_SNAPSHOT_TABLE;


DROP TABLE IF EXISTS identifier($RUNTIME_SNAPSHOT_TABLE_FULL_QUALIFIER);
CREATE TABLE identifier($RUNTIME_SNAPSHOT_TABLE_FULL_QUALIFIER)
AS
WITH OPP_IDS AS 
(
	SELECT 
		OPPORTUNITY_ID AS LONG_OPP_ID, 
		LEFT(OPPORTUNITY_ID,15) AS SHORT_OPP_ID 
	FROM "PRD_DATA_VAULT"."PRD_INFO_MART_OTC"."OPPORTUNITY_FACT"
)
/* --------------------------------------------------------------------------------------------------------------*/
/* ----------    Custom Date Dimension INCLUDING Fiscal Week Calculation Logic AND END Date Flags  --------------*/
/* --------------------------------------------------------------------------------------------------------------*/
,CTE_DATE_DIM AS
(
	  SELECT 
	    T.DATE,
	    T.DATE_KEY,
	    T.FISCAL_DATE,
	    T.FISCAL_YEAR,
	    T.FISCAL_QUARTER,
	    T.FISCAL_QUARTER_NUM,
	    T.FISCAL_QUARTER_START_DATE,
	    T.FISCAL_QUARTER_END_DATE,
	    T.FISCAL_MONTH_NUM,
	    T.FISCAL_MONTH,
	    T.FISCAL_MONTH_START_DATE,
	    T.FISCAL_MONTH_END_DATE,
	    T.FISCAL_WEEK_PERIOD,
	    T.FISCAL_WEEK,
	    T.FISCAL_WEEK_START_DATE,
	    T.FISCAL_WEEK_END_DATE,
	    CASE WHEN FISCAL_DATE = FISCAL_QUARTER_START_DATE THEN 'Y' ELSE 'N' END AS FISCAL_QTR_STRT_IND,
	CASE WHEN FISCAL_DATE = FISCAL_QUARTER_END_DATE THEN 'Y' ELSE 'N' END AS FISCAL_QTR_END_IND,
	CASE WHEN FISCAL_DATE = FISCAL_MONTH_START_DATE THEN 'Y' ELSE 'N' END AS FISCAL_MO_STRT_IND,
	CASE WHEN FISCAL_DATE = FISCAL_MONTH_END_DATE THEN 'Y' ELSE 'N' END AS FISCAL_MO_END_IND,
	CASE WHEN FISCAL_DATE = FISCAL_WEEK_START_DATE THEN 'Y' ELSE 'N' END AS FISCAL_WK_STRT_IND,
	CASE WHEN FISCAL_DATE = FISCAL_WEEK_END_DATE THEN 'Y' ELSE 'N' END AS FISCAL_WK_END_IND
	  FROM
	  (
	    SELECT
	    S.DATE,
	    S.DATE_KEY,
	    S.FISCAL_DATE,
	    S.FISCAL_YEAR,
	    S.FISCAL_QUARTER,
	    S.FISCAL_QUARTER_NUM,
	    S.FISCAL_QUARTER_START_DATE,
	    S.FISCAL_QUARTER_END_DATE,
	    S.FISCAL_MONTH_NUM,
	    S.FISCAL_MONTH,
	    S.FISCAL_MONTH_START_DATE,
	    S.FISCAL_MONTH_END_DATE,
	    S.FISCAL_WEEK_PERIOD,
	    S.FISCAL_WEEK,
	    FIRST_VALUE(S.DATE) OVER(PARTITION BY S.FISCAL_WEEK ORDER BY S.DATE ASC) AS FISCAL_WEEK_START_DATE,
	    LAST_VALUE(S.DATE) OVER(PARTITION BY S.FISCAL_WEEK ORDER BY S.DATE ASC) AS FISCAL_WEEK_END_DATE
	    FROM
	    (
	      SELECT 
	        DT.DATE,
	        DT.DATE_KEY,
	        DT.FISCAL_DATE,
	        DT.FISCAL_YEAR,
	        DT.FISCAL_QUARTER,
	        DT.FISCAL_QUARTER_NUM,
	        DT.FISCAL_QUARTER_START_DATE,
	        DT.FISCAL_QUARTER_END_DATE,
	        DT.FISCAL_YEAR||'-'||LPAD(DT.FISCAL_MONTH_NUM,2,'0') AS FISCAL_MONTH,
			DT.FISCAL_MONTH_NUM,
			DT.FISCAL_MONTH_START_DATE,
			DT.FISCAL_MONTH_END_DATE,
			DT.FISCAL_WEEK_PERIOD,
			DT.FISCAL_YEAR||'-'||DT.FISCAL_QUARTER_NUM||'-'||DT.FISCAL_WEEK_PERIOD AS FISCAL_WEEK
		  FROM "PRD_INFO_MART_DIM"."DATE_DIM" DT
	      WHERE 1=1
	    ) S
	    WHERE 1=1 
	  ) T
	  WHERE 1=1
	)
	/* --------------------------------------------------------------------------------------------------------------*/
	/* ----------    GET Previous Fiscal Year Same Quarter END Date Based ON the immediate previous quarter of CURRENT Date   --------------*/
	/* --------------------------------------------------------------------------------------------------------------*/
	,CTE_GET_PREV_FY_QTR_END AS
	(
		 SELECT
		 	DATEADD(day,-1,FISCAL_QUARTER_START_DATE) AS CURR_QTR_END_DATE,
		    DATEADD(YEAR,-1,DATEADD(day,-1,FISCAL_QUARTER_START_DATE)) AS PREV_FY_QTR_END_DATE
		 FROM
		 CTE_DATE_DIM
		 WHERE 1=1
		 AND DATE_KEY=$CONFIG_CONTROL_DATE_KEY
	)
    /* --------------------------------------------------------------------------------------------------------------*/
	/* ----------    GET Previous Fiscal Year Same Quarter Start Date Based ON the Prev YR Quarter End Date  --------------*/
	/* --------------------------------------------------------------------------------------------------------------*/
    ,CTE_GET_PREV_FY_QTR_STRT AS
    (
        SELECT
		 	FISCAL_QUARTER_START_DATE AS PREV_FY_QTR_START_DATE
		 FROM
		 CTE_DATE_DIM
		 WHERE 1=1
		 AND FISCAL_DATE=(SELECT PREV_FY_QTR_END_DATE FROM CTE_GET_PREV_FY_QTR_END)
    )
    /* --------------------------------------------------------------------------------------------------------------*/
	/* ----------    GET Current Fiscal Year Same Quarter Start Date Based ON the Same Quarter End Date   --------------*/
	/* --------------------------------------------------------------------------------------------------------------*/
    ,CTE_GET_CURR_FY_QTR_STRT AS
    (
        SELECT
		 	FISCAL_QUARTER_START_DATE AS CURR_QTR_START_DATE
		 FROM
		 CTE_DATE_DIM
		 WHERE 1=1
		 AND FISCAL_DATE=(SELECT CURR_QTR_END_DATE FROM CTE_GET_PREV_FY_QTR_END)
    )  
    ,CTE_ARR_TRNS_DATA AS
    (
        SELECT 
            A.SF_OPPORTUNITY                                                        AS TRANS_SF_OPPORTUNITY,
            A.END_USER_NAME                                                         AS END_USER_NAME, 
            OPP.ACCOUNT_ID                                                          AS OPP_ACCOUNT_ID, 
            A.OPPORTUNITY_ID                                                        AS OPPORTUNITY_ID,
            OPP."TYPE"                                                              AS OPP_TYPE,
            OPP.OPPORTUNITY_NAME                                                    AS OPPORTUNITY_NAME, 
            OPP_OWNER."NAME"                                                        AS OPP_OWNER_NAME,
            A.TRANSACTION_TYPE                                                      AS TRANSACTION_TYPE, 
            OPP.PO_NUMBER                                                           AS PO_NUMBER, 
            A.TRANDATE                                                              AS TRANDATE, 
            A.TRANID                                                                AS TRANID,
            A.TRANSACTION_ID                                                        AS TRANSACTION_ID,
            A.TRANSACTION_LINE_ID                                                   AS TRANSACTION_LINE_ID,
      		A.ITEM_DIM_ITEM_KEY														AS ITEM_KEY,
      		A.ITEM_DIM_PRODUCT_KEY													AS PRODUCT_KEY,
      		A.ITEM_DIM_ITEM_ID														AS ITEM_ID,
      		A.ITEM_DIM_SFDC_PRODUCT_ID												AS SFDC_PRODUCT_ID,
      		A.ITEM_DIM_SUBTYPE														AS SUBTYPE,
      		A.ITEM_DIM_TYPE_NAME													AS TYPE_NAME,
      		A.ITEM_DIM_SAAS_PRODUCT_FAMILY											AS SAAS_PRODUCT_FAMILY,
            A.ITEM_FULL_NAME                                                        AS ITEM_FULL_NAME, 
            A.ITEM_SALES_DESCRIPTION                                                AS ITEM_SALES_DESCRIPTION,
            OPP.PRIMARY_USE_CASE                                                    AS PRIMARY_USE_CASE,
            OPP.SECONDARY_USE_CASES                                                 AS SECONDARY_USE_CASES,
            A.PRODUCT_FAMILY                                                        AS PRODUCT_FAMILY,
            A.PRODUCT_FAMILY_SUBTYPE_NAME                                           AS PRODUCT_FAMILY_SUBTYPE_NAME,
            A.PRODUCT_CATEGORY                                                      AS PRODUCT_CATEGORY,
            OPP.SUB_RENEWAL                                                         AS SUB_RENEWAL,
            OPP.SUPPOSED_TO_BE_SUB_RENEWAL                                          AS SUPPOSED_TO_BE_SUB_RENEWAL,
            A.CHART_OF_ACCOUNT_FULL_NAME                                            AS CHART_OF_ACCOUNT_FULL_NAME,
            A.CHART_OF_ACCOUNTS_TYPE_NAME                                           AS CHART_OF_ACCOUNTS_TYPE_NAME,
            A.LEGACY_TERM_START_DATE                                                AS LEGACY_TERM_START_DATE,
            A.LEGACY_TERM_END_DATE                                                  AS LEGACY_TERM_END_DATE,
            CAST(A.AMOUNT_FOREIGN  AS NUMBER(30,15))                                AS AMOUNT_FOREIGN,

        /* --------------------------------------------------------------------------*/
        /* ----------    New Logic For ARR ARB Implementation  --------------*/
        /* --------------------------------------------------------------------------*/
      
      		CASE 
              WHEN A.PRODUCT_FAMILY IS NULL OR A.PRODUCT_FAMILY = '' THEN 'N'
              WHEN A.PRODUCT_FAMILY_SUBTYPE_NAME IS NULL OR A.PRODUCT_FAMILY_SUBTYPE_NAME = '' THEN 'N'
              WHEN A.PRODUCT_FAMILY IN ('Hardware','Pay Per Use','Software') THEN 'N'
              WHEN A.PRODUCT_FAMILY = 'Professional Services' AND A.PRODUCT_FAMILY_SUBTYPE_NAME <> 'TAM' THEN 'N'
              WHEN A.PRODUCT_FAMILY = 'Support' AND A.PRODUCT_FAMILY_SUBTYPE_NAME = 'HW Support' THEN 'N'
              WHEN A.PRODUCT_FAMILY = 'Adjustment' THEN 'N'
              ELSE 'Y'
          	END 																	 AS ARB_FLAG,

            ROUND(MONTHS_BETWEEN(A.LEGACY_TERM_END_DATE,A.LEGACY_TERM_START_DATE))   AS ROUNDED_LEGACY_TERMS_MONTHS,
            ROUND(MONTHS_BETWEEN(A.LEGACY_TERM_END_DATE,A.LEGACY_TERM_START_DATE)/3) AS ROUNDED_LEGACY_TERMS_QUARTERS,
            CASE
                WHEN A.PRODUCT_FAMILY_SUBTYPE_NAME IN ('Support','HW Support - Bundle') THEN 'Yes'
                ELSE 'No'
              END                                                                   AS AMOUNT_CHANGE,
              CASE 
                WHEN MAP."Final_SW_Look_up" IS NULL THEN 1
                ELSE MAP."Final_SW_Look_up"
              END                                                                   AS SW_PERCENT,
              CASE 
                WHEN A.PRODUCT_FAMILY_SUBTYPE_NAME IN ('Support','HW Support - Bundle') THEN MAP."Final_SW_Look_up"
                ELSE 1
              END                                                                   AS NEW_SW_SUPPORT,
              CASE 
                WHEN A.PRODUCT_FAMILY_SUBTYPE_NAME IN ('Support','HW Support - Bundle') 
                THEN 
                    CAST
                    (
                        CAST(A.AMOUNT_FOREIGN  AS NUMBER(30,15)) 
                        * 
                        MAP."Final_SW_Look_up" AS NUMBER(30,15)
                    )
                ELSE CAST(A.AMOUNT_FOREIGN AS NUMBER(30,15))
              END                                                                   AS NEW_AMOUNT,
              CASE
                WHEN ROUND(MONTHS_BETWEEN(A.LEGACY_TERM_END_DATE,A.LEGACY_TERM_START_DATE)) > 12 THEN 'Yes'
                ELSE 'No'
              END                                                                   AS MULTI_YEAR,
              CASE
                WHEN ROUND(MONTHS_BETWEEN(A.LEGACY_TERM_END_DATE,A.LEGACY_TERM_START_DATE)) IS NULL AND A.PRODUCT_FAMILY_SUBTYPE_NAME IN ('Support','HW Support - Bundle') 
                THEN
                    CAST
                    (
                      (
                        CAST(A.AMOUNT_FOREIGN  AS NUMBER(30,15)) 
                        * 
                        MAP."Final_SW_Look_up"
                      ) 
                      AS NUMBER(30,15)
                    )
                WHEN ROUND(MONTHS_BETWEEN(A.LEGACY_TERM_END_DATE,A.LEGACY_TERM_START_DATE)) IS NULL AND NOT(A.PRODUCT_FAMILY_SUBTYPE_NAME IN ('Support','HW Support - Bundle'))
                THEN
                    CAST
                    (
                      A.AMOUNT_FOREIGN 
                      AS NUMBER(30,15)
                    )
                WHEN ROUND(MONTHS_BETWEEN(A.LEGACY_TERM_END_DATE,A.LEGACY_TERM_START_DATE)) > 12 AND A.PRODUCT_FAMILY_SUBTYPE_NAME IN ('Support','HW Support - Bundle') 
                THEN
                    CAST
                    (
                      (
                        (
                          (
                          CAST(A.AMOUNT_FOREIGN  AS NUMBER(30,15)) 
                          * 
                          MAP."Final_SW_Look_up"
                          )  
                        ) / ROUND(MONTHS_BETWEEN(A.LEGACY_TERM_END_DATE,A.LEGACY_TERM_START_DATE))
                      ) * 12
                      AS NUMBER(30,15)
                    )
                WHEN ROUND(MONTHS_BETWEEN(A.LEGACY_TERM_END_DATE,A.LEGACY_TERM_START_DATE)) > 12 AND NOT(A.PRODUCT_FAMILY_SUBTYPE_NAME IN ('Support','HW Support - Bundle'))
                THEN 
                    CAST
                    (
                      (
                        CAST(A.AMOUNT_FOREIGN  AS NUMBER(30,15)) / ROUND(MONTHS_BETWEEN(A.LEGACY_TERM_END_DATE,A.LEGACY_TERM_START_DATE))
                      ) * 12
                      AS NUMBER(30,15)
                    )
                WHEN ROUND(MONTHS_BETWEEN(A.LEGACY_TERM_END_DATE,A.LEGACY_TERM_START_DATE)) <= 12 AND A.PRODUCT_FAMILY_SUBTYPE_NAME IN ('Support','HW Support - Bundle') 
                THEN 
                    CAST
                    (
                      (
                          CAST(A.AMOUNT_FOREIGN  AS NUMBER(30,15)) 
                          * 
                          MAP."Final_SW_Look_up"
                      ) 
                      AS NUMBER(30,15)
                    )
                WHEN ROUND(MONTHS_BETWEEN(A.LEGACY_TERM_END_DATE,A.LEGACY_TERM_START_DATE)) <= 12 AND NOT(A.PRODUCT_FAMILY_SUBTYPE_NAME IN ('Support','HW Support - Bundle'))
                THEN 
                    CAST
                    (
                        A.AMOUNT_FOREIGN AS NUMBER(30,15)
                    )
              END                                                                           AS ARB_AMOUNT

        /* --------------------------------------------------------------------------*/
        /* ----------    New Logic For ARR ARB Implementation  --------------*/
        /* --------------------------------------------------------------------------*/

        FROM ( 
        SELECT 
            TRAN_LINE.*,
            CHART_OF_ACCOUNTS.CHART_OF_ACCOUNT_FULL_NAME,
            CHART_OF_ACCOUNTS.TYPE_NAME AS CHART_OF_ACCOUNTS_TYPE_NAME,
            PRD_CATGRY.PRODUCT_CATEGORY,
            PRD_FAMILY.PRODUCT_FAMILY_FULL_NAME,
            PRD_FAMILY.PRODUCT_FAMILY,
            PRD_FAMILY_SUBTYPE.PRODUCT_FAMILY_SUBTYPE_NAME,
            END_USER.COMPANY_NAME      AS END_USER_NAME,
            ITEM.FULL_NAME             AS ITEM_FULL_NAME,
            ITEM.SALES_DESCRIPTION     AS ITEM_SALES_DESCRIPTION,
          	ITEM.ITEM_KEY			   AS ITEM_DIM_ITEM_KEY,
      		ITEM.PRODUCT_KEY		   AS ITEM_DIM_PRODUCT_KEY,
      		ITEM.ITEM_ID			   AS ITEM_DIM_ITEM_ID,
      		ITEM.SFDC_PRODUCT_ID	   AS ITEM_DIM_SFDC_PRODUCT_ID,
      		ITEM.SUBTYPE			   AS ITEM_DIM_SUBTYPE,
      		ITEM.TYPE_NAME			   AS ITEM_DIM_TYPE_NAME,
      		ITEM.SAAS_PRODUCT_FAMILY   AS ITEM_DIM_SAAS_PRODUCT_FAMILY,
            IFF(LENGTH(TRIM(TRAN.SF_OPPORTUNITY))=15, OPP_IDS.LONG_OPP_ID, TRAN.SF_OPPORTUNITY) AS OPPORTUNITY_ID,
            TRAN_DATE_DIM.FISCAL_QUARTER AS TRAN_DATE_DIM_FISCAL_QUARTER,
            TRAN.TRANDATE,
            TRAN.TRANID,
            TRAN.TRANSACTION_TYPE,
            TRAN.SF_OPPORTUNITY
        FROM PRD_DATA_VAULT.PRD_INFO_MART_OTC.TRANSACTION_LINE_FACT TRAN_LINE 
        LEFT JOIN PRD_DATA_VAULT.PRD_INFO_MART_OTC.TRANSACTION_FACT TRAN
         ON TRAN_LINE.TRANSACTION_KEY = TRAN.TRANSACTION_KEY 
        LEFT JOIN OPP_IDS ON OPP_IDS.SHORT_OPP_ID = TRAN.SF_OPPORTUNITY
        LEFT JOIN PRD_DATA_VAULT.PRD_INFO_MART_DIM.CUSTOMER_DIM END_USER 
         ON END_USER.CUSTOMER_KEY = TRAN.END_USER_KEY
        LEFT JOIN PRD_DATA_VAULT.PRD_INFO_MART_DIM.DATE_DIM TRAN_DATE_DIM 
         ON TRAN_DATE_DIM.DATE_KEY = TRAN.TRANDATE_KEY
        LEFT JOIN PRD_DATA_VAULT.PRD_INFO_MART_DIM.CHART_OF_ACCOUNTS_DIM CHART_OF_ACCOUNTS 
         ON CHART_OF_ACCOUNTS.CHART_OF_ACCOUNTS_KEY = TRAN_LINE.CHART_OF_ACCOUNTS_KEY 
        LEFT JOIN PRD_DATA_VAULT.PRD_INFO_MART_DIM.ITEM_DIM ITEM 
         ON ITEM.ITEM_KEY = TRAN_LINE.ITEM_KEY 
        LEFT JOIN PRD_DATA_VAULT.PRD_INFO_MART_DIM.PRODUCT_CATEGORY_DIM PRD_CATGRY
         ON PRD_CATGRY.PRODUCT_CATEGORY_KEY = ITEM.PRODUCT_CATEGORY_KEY 
        LEFT JOIN PRD_DATA_VAULT.PRD_INFO_MART_DIM.PRODUCT_FAMILY_DIM PRD_FAMILY
         ON PRD_FAMILY.PRODUCT_FAMILY_KEY = ITEM.PRODUCT_FAMILY_KEY 
        LEFT JOIN PRD_DATA_VAULT.PRD_INFO_MART_DIM.PRODUCT_FAMILY_SUBTYPE_DIM PRD_FAMILY_SUBTYPE 
         ON PRD_FAMILY_SUBTYPE.PRODUCT_FAMILY_SUBTYPE_KEY = ITEM.PRODUCT_FAMILY_SUBTYPE_KEY
        /* --------------------------------------------------------------------------------------------------------------*/
        /* ----------    GET Transactionas Only For Immediate Previous Quarter & ARR ARB Filters  --------------*/
        /* --------------------------------------------------------------------------------------------------------------*/
        WHERE 1=1
          AND TRAN.TRANDATE :: date >= TO_DATE($CONFIG_FISCAL_QUARTER_START_DATE,'YYYY-MM-DD')
          AND TRAN.TRANDATE :: date <= TO_DATE($CONFIG_FISCAL_QUARTER_END_DATE,'YYYY-MM-DD')
        /* --------------------------------------------------------------------------------------------------------------*/
        /* ----------    GET Transactionas Only For Immediate Previous Quarter & ARR ARB Filters  --------------*/
        /* --------------------------------------------------------------------------------------------------------------*/
         ) A
        LEFT JOIN PRD_DATA_VAULT.PRD_INFO_MART_OTC.OPPORTUNITY_FACT OPP 
         ON OPP.OPPORTUNITY_ID = A.OPPORTUNITY_ID
        LEFT JOIN PRD_DATA_VAULT.PRD_INFO_MART_DIM.USER_DIM OPP_OWNER
         ON OPP_OWNER.USER_KEY = OPP.OWNER_KEY
        /*
        LEFT JOIN "MATILLION_TEMP"."ARR_ARB_BNDL_MAP" MAP 
         ON (A.ITEM_FULL_NAME = MAP."Full_Name")
        */
        LEFT JOIN "MATILLION_TEMP"."SKU_HW_SW_PERCENTAGE" MAP
         ON
         (
                A.ITEM_FULL_NAME = MAP."Full_Name"
          AND   A.TRANDATE :: date >= MAP."Start_Date"
          AND   A.TRANDATE :: date <= MAP."End_Date"
         )
    )
    SELECT
        $CONFIG_FISCAL_YEAR                                       AS FISCAL_YEAR,
        $CONFIG_FISCAL_QUARTER                                    AS FISCAL_QUARTER,
        TO_DATE($CONFIG_FISCAL_QUARTER_START_DATE,'YYYY-MM-DD')   AS FISCAL_QUARTER_START_DATE,
        TO_DATE($CONFIG_FISCAL_QUARTER_END_DATE,'YYYY-MM-DD')     AS FISCAL_QUARTER_END_DATE,
	    TRANS_SF_OPPORTUNITY,
	    END_USER_NAME, 
	   	OPP_ACCOUNT_ID, 
	    OPPORTUNITY_ID,
	   	OPP_TYPE,
	    OPPORTUNITY_NAME, 
	    OPP_OWNER_NAME,
	    TRANSACTION_TYPE, 
	    PO_NUMBER, 
	    TRANDATE, 
	    TRANID,
	    TRANSACTION_ID,
	    TRANSACTION_LINE_ID,
        ITEM_KEY,
      	PRODUCT_KEY,
      	ITEM_ID,
      	SFDC_PRODUCT_ID,
      	SUBTYPE,
      	TYPE_NAME,
      	SAAS_PRODUCT_FAMILY,
	    ITEM_FULL_NAME, 
	    ITEM_SALES_DESCRIPTION,
	    PRIMARY_USE_CASE,
	    SECONDARY_USE_CASES,
	    PRODUCT_FAMILY,
	    PRODUCT_FAMILY_SUBTYPE_NAME,
	    PRODUCT_CATEGORY,
	    SUB_RENEWAL,
	    SUPPOSED_TO_BE_SUB_RENEWAL,
	    CHART_OF_ACCOUNT_FULL_NAME,
	    CHART_OF_ACCOUNTS_TYPE_NAME,
	    LEGACY_TERM_START_DATE,
	    LEGACY_TERM_END_DATE,
	    AMOUNT_FOREIGN,
        ARB_FLAG,
        ROUNDED_LEGACY_TERMS_MONTHS,
  		ROUNDED_LEGACY_TERMS_QUARTERS,
  		AMOUNT_CHANGE,
  		SW_PERCENT,
  		NEW_SW_SUPPORT,
  		NEW_AMOUNT,
  		MULTI_YEAR,
  		ARB_AMOUNT
    FROM CTE_ARR_TRNS_DATA
    ;
    
    DELETE FROM identifier($SNAPSHOT_TABLE_FULL_QUALIFIER)
    WHERE 1=1
    AND FISCAL_QUARTER_END_DATE=TO_DATE($CONFIG_FISCAL_QUARTER_END_DATE,'YYYY-MM-DD');
    
    INSERT INTO identifier($SNAPSHOT_TABLE_FULL_QUALIFIER)
    (
        FISCAL_YEAR,
        FISCAL_QUARTER,
        FISCAL_QUARTER_START_DATE,
        FISCAL_QUARTER_END_DATE,
	    TRANS_SF_OPPORTUNITY,
	    END_USER_NAME, 
	   	OPP_ACCOUNT_ID, 
	    OPPORTUNITY_ID,
	   	OPP_TYPE,
	    OPPORTUNITY_NAME, 
	    OPP_OWNER_NAME,
	    TRANSACTION_TYPE, 
	    PO_NUMBER, 
	    TRANDATE, 
	    TRANID,
	    TRANSACTION_ID,
	    TRANSACTION_LINE_ID,
      	ITEM_KEY,
      	PRODUCT_KEY,
      	ITEM_ID,
      	SFDC_PRODUCT_ID,
      	SUBTYPE,
      	TYPE_NAME,
      	SAAS_PRODUCT_FAMILY,
	    ITEM_FULL_NAME, 
	    ITEM_SALES_DESCRIPTION,
	    PRIMARY_USE_CASE,
	    SECONDARY_USE_CASES,
	    PRODUCT_FAMILY,
	    PRODUCT_FAMILY_SUBTYPE_NAME,
	    PRODUCT_CATEGORY,
	    SUB_RENEWAL,
	    SUPPOSED_TO_BE_SUB_RENEWAL,
	    CHART_OF_ACCOUNT_FULL_NAME,
	    CHART_OF_ACCOUNTS_TYPE_NAME,
	    LEGACY_TERM_START_DATE,
	    LEGACY_TERM_END_DATE,
	    AMOUNT_FOREIGN,
      	ARB_FLAG,
        ROUNDED_LEGACY_TERMS_MONTHS,
  		ROUNDED_LEGACY_TERMS_QUARTERS,
  		AMOUNT_CHANGE,
  		SW_PERCENT,
  		NEW_SW_SUPPORT,
  		NEW_AMOUNT,
  		MULTI_YEAR,
  		ARB_AMOUNT
    )
    SELECT
        FISCAL_YEAR,
        FISCAL_QUARTER,
        FISCAL_QUARTER_START_DATE,
        FISCAL_QUARTER_END_DATE,
	    TRANS_SF_OPPORTUNITY,
	    END_USER_NAME, 
	   	OPP_ACCOUNT_ID, 
	    OPPORTUNITY_ID,
	   	OPP_TYPE,
	    OPPORTUNITY_NAME, 
	    OPP_OWNER_NAME,
	    TRANSACTION_TYPE, 
	    PO_NUMBER, 
	    TRANDATE, 
	    TRANID,
	    TRANSACTION_ID,
	    TRANSACTION_LINE_ID,
        ITEM_KEY,
      	PRODUCT_KEY,
      	ITEM_ID,
      	SFDC_PRODUCT_ID,
      	SUBTYPE,
      	TYPE_NAME,
      	SAAS_PRODUCT_FAMILY,
	    ITEM_FULL_NAME, 
	    ITEM_SALES_DESCRIPTION,
	    PRIMARY_USE_CASE,
	    SECONDARY_USE_CASES,
	    PRODUCT_FAMILY,
	    PRODUCT_FAMILY_SUBTYPE_NAME,
	    PRODUCT_CATEGORY,
	    SUB_RENEWAL,
	    SUPPOSED_TO_BE_SUB_RENEWAL,
	    CHART_OF_ACCOUNT_FULL_NAME,
	    CHART_OF_ACCOUNTS_TYPE_NAME,
	    LEGACY_TERM_START_DATE,
	    LEGACY_TERM_END_DATE,
	    AMOUNT_FOREIGN,
        ARB_FLAG,
        ROUNDED_LEGACY_TERMS_MONTHS,
  		ROUNDED_LEGACY_TERMS_QUARTERS,
  		AMOUNT_CHANGE,
  		SW_PERCENT,
  		NEW_SW_SUPPORT,
  		NEW_AMOUNT,
  		MULTI_YEAR,
  		ARB_AMOUNT
    FROM identifier($RUNTIME_SNAPSHOT_TABLE_FULL_QUALIFIER)
    WHERE 1=1
    ;
    
    COMMIT;
