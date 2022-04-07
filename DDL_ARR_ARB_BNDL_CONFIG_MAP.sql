USE PRD_DATA_VAULT;

DROP TABLE IF EXISTS "MATILLION_TEMP"."SKU_HW_SW_PERCENTAGE";

CREATE TABLE "MATILLION_TEMP"."SKU_HW_SW_PERCENTAGE"
(
  	"BUNDL_HashKey"			VARCHAR(255),
    "Full_Name"             VARCHAR(2000),
    "Final_SW_Look_up"      NUMBER(30,15),
    "HW_Support"            NUMBER(30,15),
    "Start_Date"            DATE,
    "End_Date"              DATE,
  	"Load_Date"				TIMESTAMP_NTZ(9)
)
;

