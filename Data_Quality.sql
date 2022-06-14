DROP TABLE IF EXISTS ${EnvVarTargetDatabase}.MATILLION_DATA_QUALITY_CHECK.DMAAS_DQ; 
CREATE TABLE ${EnvVarTargetDatabase}.MATILLION_DATA_QUALITY_CHECK.DMAAS_DQ
(
  	RESULT VARIANT
);
  
INSERT INTO ${EnvVarTargetDatabase}.MATILLION_DATA_QUALITY_CHECK.DMAAS_DQ
(
	RESULT  
)
WITH CTE_ACCOUNT AS
(
    SELECT 
      ACC.SF_ACCOUNT_ID   AS ACCOUNT_ID,
      ACC.NAME            AS ACCOUNT_NAME,
      ACC.CREATED_DATE,
      ACC.LAST_MODIFIED_DATE,
      ACC.TYPE,
      ACC.COHESITY_CUSTOMER_REPORTING_DEDUPE,
      ACC.CHURNED,
      ACC.CUSTOMER_ACTIVE_PRODUCTS,
      ACC.DMAA_S_CUSTOMER_TYPE
    FROM ${EnvVarTargetDatabase}.${EnvVarInfoMartDIMSchema}.DELETE_ME_ACCOUNT_DIM ACC
    WHERE 1=1
    AND TYPE <> 'Internal'
    AND COHESITY_CUSTOMER_REPORTING_DEDUPE = FALSE
    AND CHURNED = FALSE
)
,CTE_CLUSTER_HELIOS
AS
(
  SELECT
      ACCOUNT,
      CLUSTER_ID,
      CLUSTER_SOFTWARE_VESION,
      CLUSTER_NAME,
      HELIOS_END_POINT,
      HELIOS_CLAIMED
  FROM
  (
  SELECT 
      CLUSTER_ID,
      CLUSTER_SOFTWARE_VESION,
      CLUSTER_NAME,
      HELIOS_END_POINT,
      ACCOUNT,
      HELIOS_CLAIMED,
      COUNT(*) OVER(PARTITION BY ACCOUNT) AS CNT_ACCNT,
      ROW_NUMBER() OVER(PARTITION BY ACCOUNT ORDER BY ACCOUNT) AS RN_ACCNT
  FROM ${EnvVarTargetDatabase}.${EnvVarInfoMartDIMSchema}.CLUSTER_DIM
  WHERE 1=1
  AND HELIOS_CLAIMED = TRUE
  ) T
  WHERE 1=1
  AND RN_ACCNT = 1
),CTE_ENTITLE AS
(
    SELECT
        ACCOUNT_ID,
        LISTAGG(ENTITLEMENT_NAME||CHAR(10),'') WITHIN GROUP (ORDER BY ENTITLEMENT_ID ASC)           AS ENTITLEMENT_NAME,
        MIN(CREATED_DATE)                                                                           AS CREATED_DATE,
        MAX(LAST_MODIFIED_DATE)                                                                     AS LAST_MODIFIED_DATE,
        LISTAGG(SAA_S_PRODUCT_FAMILY||CHAR(10),'') WITHIN GROUP (ORDER BY ENTITLEMENT_ID ASC)       AS SAA_S_PRODUCT_FAMILY,
        COUNT(1)                                                                                    AS CNT
    FROM ${EnvVarTargetDatabase}.${EnvVarInfoMartOTCSchema}.ENTITLEMENT_FACT
    WHERE 1=1
    AND STATUS = 'Active'
    AND SAA_S_PRODUCT_FAMILY IS NOT NULL
    AND SAA_S_PRODUCT_FAMILY <> ''
    AND UPPER(ENTITLEMENT_NAME) NOT LIKE '%FREETRIAL%'
    AND UPPER(ENTITLEMENT_NAME) NOT LIKE '%FREE TRIAL%'
    AND UPPER(ENTITLEMENT_NAME) NOT LIKE '%FREE-TRIAL%'
    GROUP BY 
        ACCOUNT_ID  
),CTE_OPP AS
(
    SELECT
        ACCOUNT_ID,
        LISTAGG(OPPORTUNITY_NAME||CHAR(10),'') WITHIN GROUP (ORDER BY OPPORTUNITY_ID ASC)           AS OPPORTUNITY_NAME,
        MIN(CREATED_DATE)                                                                           AS CREATED_DATE,
        MAX(LAST_MODIFIED_DATE)                                                                     AS LAST_MODIFIED_DATE,
        SUM(COMPUTED_OPP_LINE_DMAAS_SALES_ACV)                                                      AS COMPUTED_OPP_LINE_DMAAS_SALES_ACV,
        COUNT(1)                                                                                    AS CNT
    FROM ${EnvVarTargetDatabase}.${EnvVarInfoMartOTCSchema}.OPPORTUNITY_FACT
    WHERE 1=1
    AND STAGE_NAME = '6-Closed Won'
    AND DMaaS_Opportunity = TRUE
    AND OPPORTUNITY_TYPE_CLASSIFICATION IN ('DmaaS Only','Hybrid')
    AND COMPUTED_OPP_LINE_DMAAS_SALES_ACV > 0
    GROUP BY 
        ACCOUNT_ID  
),CTE_LATEST_SNAP_ALL_CUST_FLATTEN AS
(
    SELECT
       ACCOUNT_ID,
       ACCOUNT_NAME,
       CREATED_DATE,
       LAST_MODIFIED_DATE,
       CUSTOMER_ACTIVE_PRODUCTS,
       DMAA_S_CUSTOMER_TYPE,
       CUSTOMER_ACTIVE_PRODUCTS_SPLIT,
       CUSTOMER_CATEGORY,
       CNT AS NO_OF_ACTIVE_PRODUCTS,
       FIRST_VALUE(CUSTOMER_CATEGORY) OVER(PARTITION BY ACCOUNT_ID ORDER BY CUSTOMER_CATEGORY ASC) AS CUSTOMER_CATEGORY_FIRST,
       LAST_VALUE(CUSTOMER_CATEGORY) OVER(PARTITION BY ACCOUNT_ID ORDER BY CUSTOMER_CATEGORY ASC) AS CUSTOMER_CATEGORY_LAST
    FROM
    (
        SELECT
           ACCOUNT_ID,
           ACCOUNT_NAME,
           CREATED_DATE,
           LAST_MODIFIED_DATE,
           CUSTOMER_ACTIVE_PRODUCTS,
           DMAA_S_CUSTOMER_TYPE, 
           CUSTOMER_ACTIVE_PRODUCTS_SPLIT,
           CASE
                WHEN CUSTOMER_ACTIVE_PRODUCTS_SPLIT = 'On-Prem' THEN 'On-Prem' 
                ELSE 'DmaaS' 
           END AS CUSTOMER_CATEGORY,
           RN,
           CNT
        FROM 
        (
          SELECT 
            ACC.ACCOUNT_ID              AS ACCOUNT_ID,
            ACC.ACCOUNT_NAME            AS ACCOUNT_NAME,
            ACC.CREATED_DATE,
            ACC.LAST_MODIFIED_DATE,
            ACC.TYPE,
            ACC.COHESITY_CUSTOMER_REPORTING_DEDUPE,
            ACC.CHURNED,
            ACC.CUSTOMER_ACTIVE_PRODUCTS,
            ACC.DMAA_S_CUSTOMER_TYPE,
            SPLT.value :: VARCHAR(512) AS CUSTOMER_ACTIVE_PRODUCTS_SPLIT,
            ROW_NUMBER() OVER(PARTITION BY ACC.ACCOUNT_ID ORDER BY SPLT.value :: VARCHAR(512) ASC) AS RN,
            COUNT(*) OVER(PARTITION BY ACC.ACCOUNT_ID) AS CNT
          FROM CTE_ACCOUNT ACC,
          LATERAL FLATTEN (INPUT=>SPLIT(CUSTOMER_ACTIVE_PRODUCTS,';')) AS SPLT
          WHERE 1=1
        ) X
        WHERE 1=1
        AND CUSTOMER_ACTIVE_PRODUCTS_SPLIT NOT LIKE '%FreeTrial%'
        AND CUSTOMER_ACTIVE_PRODUCTS_SPLIT IS NOT NULL
    ) T
    WHERE 1=1
),CTE_LATEST_SNAP_ALL_CUST_TYPE AS 
(
    SELECT
         ACCOUNT_ID,
         ACCOUNT_NAME,
         CREATED_DATE,
         LAST_MODIFIED_DATE,
         CUSTOMER_ACTIVE_PRODUCTS,
         DMAA_S_CUSTOMER_TYPE,
         CUSTOMER_ACTIVE_PRODUCTS_SPLIT,
         CUSTOMER_CATEGORY,
         NO_OF_ACTIVE_PRODUCTS,
         CUSTOMER_CATEGORY_FIRST,
         CUSTOMER_CATEGORY_LAST,
         CASE 
              WHEN CUSTOMER_CATEGORY_FIRST = 'DmaaS' AND CUSTOMER_CATEGORY_LAST = 'DmaaS' THEN 'DmaaS_Only'
              WHEN CUSTOMER_CATEGORY_FIRST = 'On-Prem' AND CUSTOMER_CATEGORY_LAST = 'On-Prem' THEN 'OnPrem_Only'
              ELSE 'Both_DmaaS_OnPrem'
         END AS CUSTOMER_TYPE            
    FROM CTE_LATEST_SNAP_ALL_CUST_FLATTEN
    WHERE 1=1
),CTE_VALIDATION_RESULT AS
(
    SELECT 
        LHS.ACCOUNT_ID,
        LHS.ACCOUNT_NAME,
        LHS.CUSTOMER_ACTIVE_PRODUCTS,
        LHS.DMAA_S_CUSTOMER_TYPE,
        LHS.CUSTOMER_ACTIVE_PRODUCTS_SPLIT,
        LHS.CUSTOMER_CATEGORY,
        'Customer Active Product Validation' AS VALIDATION_TYPE,
        'DmaaS or Hybrid Customer Validation Based On Customer Active Product' AS VALIDATION_CATEGORY,
        /*
        CASE
            WHEN ENT.ACCOUNT_ID IS NULL AND OPP.ACCOUNT_ID IS NULL
            THEN 'Active Entitlement & Closed Opportunity with Positive Sales ACV both Missing'
            WHEN ENT.ACCOUNT_ID IS NULL AND OPP.ACCOUNT_ID IS NOT NULL
            THEN 'Active Entitlement Missing '||CHAR(10)||CHAR(10)||' Closed Opportunities with Positive Sales ACV - '||CHAR(10)||CHAR(10)||OPP.OPPORTUNITY_NAME||''
            WHEN ENT.ACCOUNT_ID IS NOT NULL AND OPP.ACCOUNT_ID IS NULL
            THEN 'Active Entitlements - '||CHAR(10)||CHAR(10)||ENT.ENTITLEMENT_NAME||''||CHAR(10)||CHAR(10)||' Closed Opportunity with Positive Sales ACV Missing'
         END AS VALIDATION_RESULT ,
         */
         'Active Entitlement Missing' AS VALIDATION_RESULT,
         'Shouldn''t be a DmaaS Customer' AS VALIDATION_COMMENT
    FROM
    (
        SELECT 
            ACCOUNT_ID,
            ACCOUNT_NAME,
            CUSTOMER_ACTIVE_PRODUCTS,
            COALESCE(DMAA_S_CUSTOMER_TYPE,'') AS DMAA_S_CUSTOMER_TYPE,
            CUSTOMER_ACTIVE_PRODUCTS_SPLIT,
            CUSTOMER_CATEGORY
            
        FROM CTE_LATEST_SNAP_ALL_CUST_TYPE C
        WHERE 1=1
        AND CUSTOMER_TYPE IN ('DmaaS_Only','Both_DmaaS_OnPrem')
    ) LHS 
    LEFT JOIN CTE_ENTITLE ENT ON (LHS.ACCOUNT_ID = ENT.ACCOUNT_ID)
    --LEFT JOIN CTE_OPP OPP ON (LHS.ACCOUNT_ID = OPP.ACCOUNT_ID)
    WHERE 1=1
    AND (
      ENT.ACCOUNT_ID IS NULL 
      --OR 
      --OPP.ACCOUNT_ID IS NULL
    )
    UNION ALL
    SELECT 
        LHS.ACCOUNT_ID,
        LHS.ACCOUNT_NAME,
        LHS.CUSTOMER_ACTIVE_PRODUCTS,
        LHS.DMAA_S_CUSTOMER_TYPE,
        LHS.CUSTOMER_ACTIVE_PRODUCTS_SPLIT,
        LHS.CUSTOMER_CATEGORY,
        'Customer Active Product Validation' AS VALIDATION_TYPE,
        'OnPrem Customer Validation Based On Customer Active Product' AS VALIDATION_CATEGORY,
        /*
        'Active Entitlements - '||CHAR(10)||CHAR(10)||ENT.ENTITLEMENT_NAME||''||CHAR(10)||CHAR(10)||'Closed Opportunities with Positive Sales ACV - '||CHAR(10)||CHAR(10)||OPP.OPPORTUNITY_NAME||'' 
                                     AS VALIDATION_RESULT,
        */
        'Active Entitlements Present. Should be a DmaaS Customer' AS VALIDATION_RESULT,
        'Should be a DmaaS Customer' AS VALIDATION_COMMENT
    FROM
    (
        SELECT 
            ACCOUNT_ID,
            ACCOUNT_NAME,
            CUSTOMER_ACTIVE_PRODUCTS,
            COALESCE(DMAA_S_CUSTOMER_TYPE,'') AS DMAA_S_CUSTOMER_TYPE,
            CUSTOMER_ACTIVE_PRODUCTS_SPLIT,
            CUSTOMER_CATEGORY
        FROM CTE_LATEST_SNAP_ALL_CUST_TYPE C
        WHERE 1=1
        AND CUSTOMER_TYPE IN ('OnPrem_Only')
        AND EXISTS (SELECT 1 FROM CTE_CLUSTER_HELIOS H WHERE C.ACCOUNT_ID = H.ACCOUNT )
    ) LHS 
    LEFT JOIN CTE_ENTITLE ENT ON (LHS.ACCOUNT_ID = ENT.ACCOUNT_ID)
    --LEFT JOIN CTE_OPP OPP ON (LHS.ACCOUNT_ID = OPP.ACCOUNT_ID)
    WHERE 1=1
    AND (
      ENT.ACCOUNT_ID IS NOT NULL 
      --AND 
      --OPP.ACCOUNT_ID IS NOT NULL
    )
    UNION ALL
    SELECT 
        LHS.ACCOUNT_ID,
        LHS.ACCOUNT_NAME,
        LHS.CUSTOMER_ACTIVE_PRODUCTS,
        LHS.DMAA_S_CUSTOMER_TYPE,
        LHS.CUSTOMER_ACTIVE_PRODUCTS_SPLIT,
        LHS.CUSTOMER_CATEGORY,
        'Customer Active Product Validation' AS VALIDATION_TYPE,
        'Empty Customer Validation Based On Customer Active Product' AS VALIDATION_CATEGORY,
        /*
        'Active Entitlements - '||CHAR(10)||CHAR(10)||ENT.ENTITLEMENT_NAME||''||CHAR(10)||CHAR(10)||'Closed Opportunities with Positive Sales ACV - '||CHAR(10)||CHAR(10)||OPP.OPPORTUNITY_NAME||'' 
                                     AS VALIDATION_RESULT,
        */
        'Active Entitlements Present. Should be a DmaaS Customer' AS VALIDATION_RESULT,
        'Should be a DmaaS Customer' AS VALIDATION_COMMENT
    FROM
    (
        SELECT 
            ACCOUNT_ID,
            ACCOUNT_NAME,
            '' AS CUSTOMER_ACTIVE_PRODUCTS,
            COALESCE(DMAA_S_CUSTOMER_TYPE,'') AS DMAA_S_CUSTOMER_TYPE,
            '' AS CUSTOMER_ACTIVE_PRODUCTS_SPLIT,
            'Blank' AS CUSTOMER_CATEGORY
        FROM CTE_ACCOUNT C
        WHERE 1=1
        AND (CUSTOMER_ACTIVE_PRODUCTS IS NULL OR TRIM(CUSTOMER_ACTIVE_PRODUCTS) = '')
    ) LHS 
    LEFT JOIN CTE_ENTITLE ENT ON (LHS.ACCOUNT_ID = ENT.ACCOUNT_ID)
    --LEFT JOIN CTE_OPP OPP ON (LHS.ACCOUNT_ID = OPP.ACCOUNT_ID)
    WHERE 1=1
    AND 
    (
      ENT.ACCOUNT_ID IS NOT NULL 
      --AND 
      --OPP.ACCOUNT_ID IS NOT NULL
    )
    UNION ALL
    SELECT 
        LHS.ACCOUNT_ID,
        LHS.ACCOUNT_NAME,
        LHS.CUSTOMER_ACTIVE_PRODUCTS,
        LHS.DMAA_S_CUSTOMER_TYPE,
        LHS.CUSTOMER_ACTIVE_PRODUCTS_SPLIT,
        LHS.CUSTOMER_CATEGORY,
        'DmaaS-Customer-Type Validation' AS VALIDATION_TYPE,
        'Paid DmaaS-Customer-Type Validation' AS VALIDATION_CATEGORY,
        'Closed Opportunity with Positive Sales DMaaS ACV Missing' AS VALIDATION_RESULT,
        'DmaaS Customer Type Field is incorrectly populated' AS VALIDATION_COMMENT
    FROM
    (
        SELECT 
            ACCOUNT_ID,
            ACCOUNT_NAME,
            COALESCE(CUSTOMER_ACTIVE_PRODUCTS,'') AS CUSTOMER_ACTIVE_PRODUCTS,
            COALESCE(DMAA_S_CUSTOMER_TYPE,'') AS DMAA_S_CUSTOMER_TYPE,
            '' AS CUSTOMER_ACTIVE_PRODUCTS_SPLIT,
            'Blank' AS CUSTOMER_CATEGORY
        FROM CTE_ACCOUNT C
        WHERE 1=1
        AND DMAA_S_CUSTOMER_TYPE = 'Paid'
    ) LHS 
    LEFT JOIN CTE_OPP OPP ON (LHS.ACCOUNT_ID = OPP.ACCOUNT_ID)
    WHERE 1=1
    AND 
    (
      OPP.ACCOUNT_ID IS NULL
    )
    UNION ALL
    SELECT 
        LHS.ACCOUNT_ID,
        LHS.ACCOUNT_NAME,
        LHS.CUSTOMER_ACTIVE_PRODUCTS,
        LHS.DMAA_S_CUSTOMER_TYPE,
        LHS.CUSTOMER_ACTIVE_PRODUCTS_SPLIT,
        LHS.CUSTOMER_CATEGORY,
        'DmaaS-Customer-Type Validation' AS VALIDATION_TYPE,
        'Blank/Empty DmaaS-Customer-Type Validation' AS VALIDATION_CATEGORY,
        'Closed Opportunity with Positive Sales DMaaS ACV Present' AS VALIDATION_RESULT,
        'DmaaS Customer Type Field is incorrectly populated' AS VALIDATION_COMMENT
    FROM
    (
        SELECT 
            ACCOUNT_ID,
            ACCOUNT_NAME,
            COALESCE(CUSTOMER_ACTIVE_PRODUCTS,'') AS CUSTOMER_ACTIVE_PRODUCTS,
            COALESCE(DMAA_S_CUSTOMER_TYPE,'') AS DMAA_S_CUSTOMER_TYPE,
            '' AS CUSTOMER_ACTIVE_PRODUCTS_SPLIT,
            'Blank' AS CUSTOMER_CATEGORY
        FROM CTE_ACCOUNT C
        WHERE 1=1
        AND (DMAA_S_CUSTOMER_TYPE <> 'Paid' OR DMAA_S_CUSTOMER_TYPE IS NULL OR DMAA_S_CUSTOMER_TYPE = '')
    ) LHS 
    LEFT JOIN CTE_OPP OPP ON (LHS.ACCOUNT_ID = OPP.ACCOUNT_ID)
    WHERE 1=1
    AND 
    (
      OPP.ACCOUNT_ID IS NOT NULL
    )
),CTE_VALIDATION_RESULT_FORMAT AS
(
    SELECT
        ACCOUNT_ID                      AS AccountId,
        ACCOUNT_NAME                    AS AccountName,
        CUSTOMER_ACTIVE_PRODUCTS        AS CustomerActiveProducts,
        CUSTOMER_CATEGORY               AS CustomerType,
        VALIDATION_TYPE                 AS ValidationType,
        DMAA_S_CUSTOMER_TYPE            AS DmaaS_Customer_Type,
        VALIDATION_CATEGORY             AS ValidationCategory,
        VALIDATION_RESULT               AS ValidationResult,
        VALIDATION_COMMENT              AS ValidationComment
    FROM CTE_VALIDATION_RESULT 
),CTE_DIS_ACCT AS
(
    SELECT DISTINCT AccountId FROM CTE_VALIDATION_RESULT_FORMAT
),CTE_ACCT_OPP_DETAILS AS
(
SELECT 
    F.AccountId,
    ARRAY_AGG(
              OBJECT_CONSTRUCT('OpportunityId',OPPORTUNITY_ID,'OpportunityName',COALESCE(OPPORTUNITY_NAME,''),'StageName',COALESCE(STAGE_NAME,''),
                     'DMaaS_Opportunity',COALESCE(DMaaS_Opportunity,FALSE),'OpportunityTypeClassification',COALESCE(OPPORTUNITY_TYPE_CLASSIFICATION,''),'ComputedOppLineDmaasSalesAcv',COALESCE(COMPUTED_OPP_LINE_DMAAS_SALES_ACV,0)
                    )
             ) WITHIN GROUP (ORDER BY F.AccountId) AS Opportunity_Details
FROM CTE_DIS_ACCT F 
LEFT JOIN PRD_INFO_MART_OTC.OPPORTUNITY_FACT OP ON (F.AccountId = OP.ACCOUNT_ID)
GROUP BY F.AccountId
),CTE_ACCT_ENT_DETAILS AS
(
SELECT 
    F.AccountId,
    ARRAY_AGG(
              OBJECT_CONSTRUCT('EntitlementId',ENTITLEMENT_ID,'EntitlementName',COALESCE(ENTITLEMENT_NAME,''),'Status',COALESCE(STATUS,''),
                     'SaaSProductFamily',COALESCE(SAA_S_PRODUCT_FAMILY,'')
                    )
             ) WITHIN GROUP (ORDER BY F.AccountId) AS Entitlement_Details
FROM CTE_DIS_ACCT F 
LEFT JOIN PRD_INFO_MART_OTC.ENTITLEMENT_FACT OP ON (F.AccountId = OP.ACCOUNT_ID) 
GROUP BY F.AccountId  
)
SELECT
   ARRAY_AGG(OBJECT_CONSTRUCT('ValidationCategory',ValidationCategory,'ValidationResult',"Value")) WITHIN GROUP (ORDER BY ValidationCategory) AS "Result"
FROM
(
    SELECT
        ValidationCategory,
        ARRAY_AGG(Result) WITHIN GROUP (ORDER BY ValidationCategory,AccountId) AS "Value"
    FROM
    (
            SELECT
                  C.ValidationCategory,C.AccountId,OBJECT_CONSTRUCT('AccountId',C.AccountId,'AccountName',C.AccountName,'CustomerActiveProducts',C.CustomerActiveProducts,
                                                                'CustomerType',C.CustomerType,
                                                                'DmaaS Customer Type',C.DmaaS_Customer_Type,
                                                                'ValidationResult',C.ValidationResult,
                                                                'ValidationComment',C.ValidationComment,
                                                                --'OpportunityDetails',D.Opportunity_Details,
                                                                'EntitlementDetails',E.Entitlement_Details    
                                                               ) AS Result
            FROM CTE_VALIDATION_RESULT_FORMAT C
            --LEFT JOIN CTE_ACCT_OPP_DETAILS D ON (C.AccountId = D.AccountId)
            LEFT JOIN CTE_ACCT_ENT_DETAILS E ON (C.AccountId = E.AccountId)
            WHERE 1=1
            AND ValidationType = 'Customer Active Product Validation'
        UNION ALL
            SELECT
                  C.ValidationCategory,C.AccountId,OBJECT_CONSTRUCT('AccountId',C.AccountId,'AccountName',C.AccountName,'CustomerActiveProducts',C.CustomerActiveProducts,
                                                                'CustomerType',C.CustomerType,
                                                                'DmaaS Customer Type',C.DmaaS_Customer_Type,
                                                                'ValidationResult',C.ValidationResult,
                                                                'ValidationComment',C.ValidationComment,
                                                                'OpportunityDetails',D.Opportunity_Details
                                                                --,'EntitlementDetails',E.Entitlement_Details    
                                                               ) AS Result
            FROM CTE_VALIDATION_RESULT_FORMAT C
            LEFT JOIN CTE_ACCT_OPP_DETAILS D ON (C.AccountId = D.AccountId)
            --LEFT JOIN CTE_ACCT_ENT_DETAILS E ON (C.AccountId = E.AccountId)
            WHERE 1=1
            AND ValidationType = 'DmaaS-Customer-Type Validation'
    ) T
    GROUP BY
        ValidationCategory
) X
;
