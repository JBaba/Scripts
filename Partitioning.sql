-- Partition By Date in Db2

-- Sql for partition data every 15 days
CREATE
  TABLE ED_ELIG_TEMP_PART
  (
    CASE_NUM DECFLOAT(16), CREATE_DT TIMESTAMP
  )
   PARTITION BY RANGE
  (
    CREATE_DT
  )
  (
    STARTING
  FROM
    ('2016-01-16') ENDING AT ('2016-03-25') EVERY 15 DAYS
  );
  
-- Sql for partition data every 7 days  
CREATE
  TABLE ED_ELIG_TEMP_PART_10PART
  (
    CASE_NUM DECFLOAT(16), CREATE_DT TIMESTAMP
  )
   PARTITION BY RANGE
  (
    CREATE_DT
  )
  (
    STARTING
  FROM
    ('2016-01-16') ENDING AT ('2016-03-25') EVERY 7 DAYS
  );
  
-- partition By renge on numbers  
CREATE TABLE departments
	(
		dept_no INT
		desc CHAR(3)
	)
PARTITION BY 
	(dept_no NULLS FIRST)
	(STARTING 0 ENDING 9 IN tbsp0,
		STARTING 10 ENDING 19 IN tbsp1,
		STARTING 20 ENDING 29 IN tbsp2,
		STARTING 30 ENDING 39 IN tbsp3
	);  
  
-- Total size for all partition on the table
SELECT SUM(DATA_OBJECT_L_SIZE) FROM TABLE (SYSPROC.ADMIN_GET_TAB_INFO_V97('IE_APP_ONLINE', 'ED_ELIG_TEMP_PART')) AS T;

-- Gives individual size for all partition on the table
SELECT * FROM TABLE (SYSPROC.ADMIN_GET_TAB_INFO_V97('IE_APP_ONLINE', 'ED_ELIG_TEMP_PART')) AS T;

-- limit first 100 row
select * from ED_ELIG_TEMP FETCH FIRST 100 ROWS ONLY;


-- If milions record then fetch based on row number
select cast(to_number(CASE_NUM) as INTEGER),CREATE_DT from (select rownumber() over() rowid,t.* from ED_ELIGIBILITY t) tab_1 where tab_1.ROWID >60000 and tab_1.ROWID <70000; 

-- Get row number for database
select rownumber() over() rowid,t.* from ED_ELIG_TEMP t where rowid=1;

-- Extra
SELECT
  SUBSTR(TABSCHEMA,1,15) AS SCHEMA,
  SUBSTR(TABNAME,1,20)   AS TABLENAME,
  DATA_OBJECT_P_SIZE ,
  INDEX_OBJECT_P_SIZE ,
  LONG_OBJECT_P_SIZE ,
  LOB_OBJECT_P_SIZE ,
  XML_OBJECT_P_SIZE,
  (DATA_OBJECT_P_SIZE + INDEX_OBJECT_P_SIZE + LONG_OBJECT_P_SIZE +
  LOB_OBJECT_P_SIZE   + XML_OBJECT_P_SIZE ) AS TOTAL_P_SIZE
FROM
  SYSIBMADM.ADMINTABINFO
WHERE
  TABNAME='ED_ELIG_TEMP';





