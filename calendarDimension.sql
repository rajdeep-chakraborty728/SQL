--USE DEV_DATA_VAULT;
--use DEV_DATA_VAULT.dev_info_mart_dim;

USE PRD_DATA_VAULT;
use PRD_DATA_VAULT.prd_info_mart_dim;

alter session set week_start = 6;

create or replace table DELETE_ME_date_dim as (
with date_generator as (
select 	cast(dateadd('day', RN, '2010-08-01') as date) as date
from 	(
						SELECT  seq4(),ROW_NUMBER() OVER(ORDER BY seq4() ASC) RN
			FROM TABLE(generator(rowcount => 100000))
				)
)
SELECT
X.*,
fiscal_quarter_num || '-' || fiscal_year AS fiscal_quarter_finance
FROM
(
select
date,
(year(date) * 10000) + (month(date) * 100) + day(date) as  date_key,

-- Calendar Year
extract('year', date) as calendar_year,
extract('quarter', date) as calendar_quarter,
case calendar_quarter
	when 1 then 'Q1'
	when 2 then 'Q2'
	when 3 then 'Q3'
	when 4 then 'Q4'
end as calendar_quarter_short,
to_char(calendar_year) || '-' || calendar_quarter_short as calendar_year_quarter,

-- Calendar Month
extract('month', date) as calendar_month,
to_char(calendar_year) || '-' ||
	iff(
		len(calendar_month) = 1,
		'0' || to_char(calendar_month),
		to_char(calendar_month)
) as calendar_year_month,
case calendar_month
	when 1 then 'January'
	when 2 then 'February'
	when 3 then 'March'
	when 4 then 'April'
	when 5 then 'May'
	when 6 then 'June'
	when 7 then 'July'
	when 8 then 'August'
	when 9 then 'September'
	when 10 then 'October'
	when 11 then 'November'
	when 12 then 'December'
end as month_name,
monthname(date) as calendar_month_short,
extract('day', date)  as calendar_day_of_month,

-- Calendar Week
replace(dayOfWeek(date), 0, 7) calendar_day_of_week_start_monday,
replace(dayOfWeek(date) + 1, 0, 7) calendar_day_of_week_start_sunday,
dayname(date) as day_name,
case dayname(date)
	when 'Mon' then 'Monday'
	when 'Tue' then 'Tuesday'
	when 'Wed' then 'Wednesday'
	when 'Thu' then 'Thursday'
	when 'Fri' then 'Friday'
	when 'Sat' then 'Saturday'
	when 'Sun' then 'Sunday'
end as calendar_day_of_week,
weekofyear(date) as calendar_week_of_year,
to_char(calendar_year) || '-' || to_char(weekofyear(date)) as calendar_year_week,

-- Calendar Day
dayofyear(date) as calendar_day_of_year,

-- Fiscal Year
iff(
	to_char(month(date)) <= 7,
	calendar_year,
	calendar_year + 1
) as fiscal_year_num,
'FY' || substr(to_char(fiscal_year_num), 3) as fiscal_year,
iff(
	to_char(month(date)) <= 7,
	to_char(calendar_year - 1) || '-08-01',
	to_char(calendar_year) || '-08-01'
) as fiscal_year_start_date,
iff(
	to_char(month(date)) <= 7,
	to_char(calendar_year) || '-07-31',
	to_char(calendar_year + 1) || '-07-31'
) as fiscal_year_end_date,

-- Fiscal Quarter
case
	when calendar_month between 2 and 4 then 'Q3'
	when calendar_month between 5 and 7 then 'Q4'
	when calendar_month between 8 and 10 then 'Q1'
	else 'Q2'
end as fiscal_quarter_num,
to_char(fiscal_year_num) || '-' || fiscal_quarter_num as fiscal_quarter,
min(date) over(
	partition by
		fiscal_year_num,
		fiscal_quarter
) as fiscal_quarter_start_date,
max(date) over(
	partition by
		fiscal_year_num,
		fiscal_quarter
) as fiscal_quarter_end_date,
dense_rank() over(
	order by fiscal_quarter asc
) as fiscal_quarter_sequence_num,

-- Fiscal Month
calendar_month_short as fiscal_month,
iff(
	calendar_month >= 8,
	calendar_month - 7,
	calendar_month + 5
) as fiscal_month_num,
dense_rank() over(
	partition by fiscal_year, fiscal_quarter
	order by fiscal_month_num asc
) as fiscal_month_sequence,
min(date) over(
	partition by
		fiscal_year,
		fiscal_quarter,
		calendar_month
) as fiscal_month_start_date,
max(date) over(
	partition by
		fiscal_year,
		fiscal_quarter,
		calendar_month
) as fiscal_month_end_date,

-- Fiscal Week
iff(
	floor((date - dateadd('day', -6, fiscal_quarter_end_date)) / 7) + 13  < 1,
	1,
	floor((date - dateadd('day', -6, fiscal_quarter_end_date)) / 7) + 13
) as fiscal_week,
'W-' || to_char(fiscal_week) as fiscal_week_period,

-- Fiscal Day
date as fiscal_date,
row_number() over(partition by fiscal_year_num order by date asc) as fiscal_day_num
from 	date_generator
where	date < '2099-08-01'
) X
WHERE 1=1
order by date
);

ALTER TABLE DELETE_ME_date_dim ADD QUARTER_WEEK_NO INT, QUARTER_WEEK_ID VARCHAR(30),WEEK_START_DATE DATE, WEEK_END_DATE DATE, WEEK_END_IND CHAR(1);

create or replace table DELETE_ME_date_dim_week_calc as
-- Logic - If Last Week Of Quarter is less than 3 days, club it with Penultimate week of the Quarter
WITH CTE_CUSTOM_QUARTER_END_WEEK_CALC AS
	(
	    SELECT
	        FISCAL_QUARTER,
	        DATE,
	        DAY_NAME,
	        DAY_INDICATOR,
	        UPD_QUARTER_END_IND     AS QUARTER_END_IND,
	        UPD_WEEK_END_IND        AS WEEK_END_IND,
	        WEEK_START_DATE,
	        UPD_WEEK_END_DATE       AS WEEK_END_DATE,
	        QUARTER_WEEK_ID,
	        UPD_NO_OF_DAYS_PER_WEEK AS NO_OF_DAYS_PER_WEEK,
            QUARTER_END_MERGE_IND
	    FROM
	    (
	    SELECT
	        FISCAL_QUARTER,
	        DATE,
	        DAY_NAME,
	        DAY_INDICATOR,
	        RN_QTR,
	        QUARTER_END_IND,
	        WEEK_END_IND,
	        WEEK_START_DATE,
	        WEEK_END_DATE,
	        QUARTER_WEEK_ID,
	        NO_OF_DAYS_PER_WEEK,
	        NEXT_WEEK_END_DATE,
	        NEXT_NO_OF_DAYS_PER_WEEK,
	        NEXT_QUARTER_END_IND,
	        CASE
	            WHEN NEXT_QUARTER_END_IND = 'Y' AND NEXT_NO_OF_DAYS_PER_WEEK <= 2
	            THEN DATEADD(day,NEXT_NO_OF_DAYS_PER_WEEK,WEEK_END_DATE)
	            ELSE WEEK_END_DATE
	        END AS UPD_WEEK_END_DATE,
	        CASE
	            WHEN NEXT_QUARTER_END_IND = 'Y' AND NEXT_NO_OF_DAYS_PER_WEEK <= 2
	            THEN NO_OF_DAYS_PER_WEEK+ NEXT_NO_OF_DAYS_PER_WEEK
	            ELSE NO_OF_DAYS_PER_WEEK
	        END AS UPD_NO_OF_DAYS_PER_WEEK,
	        CASE
	            WHEN NEXT_QUARTER_END_IND = 'Y' AND NEXT_NO_OF_DAYS_PER_WEEK <= 2
	            THEN 'Y'
	            ELSE WEEK_END_IND
	        END AS UPD_WEEK_END_IND,
	        CASE
	            WHEN NEXT_QUARTER_END_IND = 'Y' AND NEXT_NO_OF_DAYS_PER_WEEK <= 2
	            THEN 'Y'
	            ELSE QUARTER_END_IND
	        END AS UPD_QUARTER_END_IND,
	        CASE
	            WHEN QUARTER_END_IND = 'Y' AND NO_OF_DAYS_PER_WEEK <= 2
	            THEN 'Y'
	            ELSE 'N'
	        END AS QUARTER_END_MERGE_IND
	    FROM
	    (
	      SELECT
	          FISCAL_QUARTER,
	          DATE,
	          DAY_NAME,
	          DAY_INDICATOR,
	          RN_QTR,
	          QUARTER_END_IND,
	          WEEK_END_IND,
	          WEEK_START_DATE,
	          WEEK_END_DATE,
	          QUARTER_WEEK_ID,
	          DATEDIFF(day,WEEK_START_DATE,WEEK_END_DATE)+1                                           AS NO_OF_DAYS_PER_WEEK,
	          LEAD(WEEK_END_DATE,1) OVER (PARTITION BY FISCAL_QUARTER ORDER BY DATE ASC)              AS NEXT_WEEK_END_DATE,
	          LEAD(NO_OF_DAYS_PER_WEEK,1) OVER (PARTITION BY FISCAL_QUARTER ORDER BY DATE ASC)        AS NEXT_NO_OF_DAYS_PER_WEEK,
	          LEAD(QUARTER_END_IND,1) OVER (PARTITION BY FISCAL_QUARTER ORDER BY DATE ASC)            AS NEXT_QUARTER_END_IND
	      FROM
	      (
	        SELECT
	                FISCAL_QUARTER,
	                DATE,
	                DAY_NAME,
	                DAY_INDICATOR,
	                RN_QTR,
	                QUARTER_END_IND,
	                WEEK_END_IND,
	                CASE
	                        WHEN LAG(DATE,1) OVER(PARTITION BY FISCAL_QUARTER ORDER BY RN_QTR ASC) IS NULL THEN FISCAL_QUARTER_START_DATE
	                        ELSE DATEADD(day,1,LAG(DATE,1) OVER(PARTITION BY FISCAL_QUARTER ORDER BY RN_QTR ASC))
	                END AS WEEK_START_DATE,
	                DATE AS WEEK_END_DATE,
	                ROW_NUMBER() OVER(PARTITION BY FISCAL_QUARTER ORDER BY RN_QTR ASC) AS QUARTER_WEEK_ID
	        FROM
	        (
	                SELECT
	                        DATE,
	                        DATE_KEY,
	                        DAY_NAME,
	                        CASE
	                                WHEN DAY_NAME = 'Sun' THEN 1
	                                WHEN DAY_NAME = 'Mon' THEN 2
	                                WHEN DAY_NAME = 'Tue' THEN 3
	                                WHEN DAY_NAME = 'Wed' THEN 4
	                                WHEN DAY_NAME = 'Thu' THEN 5
	                                WHEN DAY_NAME = 'Fri' THEN 6
	                                WHEN DAY_NAME = 'Sat' THEN 7
	                        END AS DAY_INDICATOR,
	                        CASE
	                                WHEN DATE=FISCAL_QUARTER_END_DATE THEN 'Y'
	                                ELSE 'N'
	                        END AS QUARTER_END_IND,
	                        ROW_NUMBER() OVER(PARTITION BY FISCAL_QUARTER ORDER BY FISCAL_DATE ASC) AS RN_QTR,
	                        CASE
	                                WHEN DATE=FISCAL_QUARTER_END_DATE THEN 'Y'
	                                WHEN DAY_NAME = 'Sat' THEN 'Y'
	                                ELSE NULL
	                        END AS WEEK_END_IND,
	                        FISCAL_YEAR,
	                        FISCAL_YEAR_NUM,
	                        FISCAL_YEAR_START_DATE,
	                        FISCAL_YEAR_END_DATE,
	                        FISCAL_QUARTER_NUM,
	                        FISCAL_QUARTER,
	                        FISCAL_QUARTER_START_DATE,
	                        FISCAL_QUARTER_END_DATE,
	                        FISCAL_QUARTER_SEQUENCE_NUM,
	                        FISCAL_MONTH,
	                        FISCAL_MONTH_NUM,
	                        FISCAL_MONTH_SEQUENCE,
	                        FISCAL_MONTH_START_DATE,
	                        FISCAL_MONTH_END_DATE
	                FROM DELETE_ME_date_dim
	                WHERE 1=1
	        ) T
	        WHERE 1=1
	        AND WEEK_END_IND ='Y'
	        ) S
	      ) M
	    )N
	    WHERE 1=1

)
-- Logic - If 1st of Week Of Quarter is less than 3 days, club it with 2nd week of the Quarter
,CTE_CUSTOM_QUARTER_START_WEEK_CALC AS
(
    SELECT
        FISCAL_QUARTER,
        DATE,
        QUARTER_END_IND,
        UPD_WEEK_START_DATE AS WEEK_START_DATE,
        WEEK_END_DATE,
        WEEK_END_IND,
        UPD_NO_OF_DAYS_PER_WEEK AS NO_OF_DAYS_PER_WEEK,
        UPD_QUARTER_WEEK_ID AS QUARTER_WEEK_ID,
        AFFECTED_QUARTER_IND,
        QUARTER_START_MERGE_IND,
        QUARTER_END_MERGE_IND
    FROM
    (
        SELECT
            FISCAL_QUARTER,
            DATE,
            DAY_NAME,
            DAY_INDICATOR,
            QUARTER_END_IND,
            WEEK_END_IND,
            WEEK_START_DATE,
            WEEK_END_DATE,
            QUARTER_WEEK_ID,
            NO_OF_DAYS_PER_WEEK,
            QUARTER_END_MERGE_IND,
            QUARTER_START_MERGE_IND,
            AFFECTED_QUARTER_IND,
            PREV_NO_OF_DAYS_PER_WEEK,
            CASE
                WHEN AFFECTED_QUARTER_IND = 'Y' AND QUARTER_WEEK_ID = 2
                THEN DATEADD(day,0-PREV_NO_OF_DAYS_PER_WEEK,WEEK_START_DATE)
                ELSE WEEK_START_DATE
            END AS UPD_WEEK_START_DATE,
            CASE
                WHEN AFFECTED_QUARTER_IND = 'Y' AND QUARTER_WEEK_ID = 2
                THEN NO_OF_DAYS_PER_WEEK+PREV_NO_OF_DAYS_PER_WEEK
                ELSE NO_OF_DAYS_PER_WEEK
            END AS UPD_NO_OF_DAYS_PER_WEEK,
            CASE
                WHEN AFFECTED_QUARTER_IND = 'Y'
                THEN QUARTER_WEEK_ID-1
                ELSE QUARTER_WEEK_ID
            END AS UPD_QUARTER_WEEK_ID
        FROM
        (
            SELECT
                FISCAL_QUARTER,
                DATE,
                DAY_NAME,
                DAY_INDICATOR,
                QUARTER_END_IND,
                WEEK_END_IND,
                WEEK_START_DATE,
                WEEK_END_DATE,
                QUARTER_WEEK_ID,
                NO_OF_DAYS_PER_WEEK,
                QUARTER_END_MERGE_IND,
                CASE
                  WHEN QUARTER_WEEK_ID = 1 AND DATEDIFF(day,WEEK_START_DATE,WEEK_END_DATE)+1 <= 2
                  THEN 'Y'
                  ELSE 'N'
                END AS QUARTER_START_MERGE_IND,
                MAX(
                  CASE
                    WHEN QUARTER_WEEK_ID = 1 AND DATEDIFF(day,WEEK_START_DATE,WEEK_END_DATE)+1 <= 2
                    THEN 'Y'
                    ELSE 'N'
                  END
                    ) OVER (PARTITION BY FISCAL_QUARTER) AS AFFECTED_QUARTER_IND,
                LAG(NO_OF_DAYS_PER_WEEK,1) OVER (PARTITION BY FISCAL_QUARTER ORDER BY DATE ASC) AS PREV_NO_OF_DAYS_PER_WEEK
            FROM CTE_CUSTOM_QUARTER_END_WEEK_CALC
            WHERE 1=1
        ) T
        WHERE 1=1
    ) S
    WHERE 1=1
    AND QUARTER_START_MERGE_IND = 'N'
    AND QUARTER_END_MERGE_IND = 'N'
),CTE_CTE_CUSTOM_WEEK_NORMALISE AS
(
	SELECT
			W.WEEK_START_DATE,
			W.WEEK_END_DATE,
			W.QUARTER_WEEK_ID AS QUARTER_WEEK_NO,
			DT.FISCAL_YEAR||'-'||DT.FISCAL_QUARTER_NUM||'-WK-'||W.QUARTER_WEEK_ID AS QUARTER_WEEK_ID,
			CASE
					WHEN W.WEEK_END_DATE = DT.FISCAL_DATE THEN 'Y'
					ELSE 'N'
			END AS WEEK_END_IND,
			DT.DATE
	FROM (SELECT * FROM DELETE_ME_date_dim DT WHERE 1=1) DT
	JOIN CTE_CUSTOM_QUARTER_START_WEEK_CALC W ON (DT.DATE BETWEEN W.WEEK_START_DATE AND W.WEEK_END_DATE)
	WHERE 1=1
)
SELECT * FROM CTE_CTE_CUSTOM_WEEK_NORMALISE
ORDER BY DATE ASC
;

UPDATE DELETE_ME_date_dim TGT
SET
	TGT.WEEK_START_DATE = SRC.WEEK_START_DATE,
	TGT.WEEK_END_DATE = SRC.WEEK_END_DATE,
	TGT.QUARTER_WEEK_NO = SRC.QUARTER_WEEK_NO,
	TGT.QUARTER_WEEK_ID = SRC.QUARTER_WEEK_ID,
	TGT.WEEK_END_IND = SRC.WEEK_END_IND
FROM DELETE_ME_date_dim_week_calc SRC
WHERE (TGT.DATE = SRC.DATE)
;

drop table IF EXISTS DELETE_ME_date_dim_week_calc;
