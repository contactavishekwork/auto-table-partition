/* 
    This is the grand plan:
    =============================
    
    *   Have the tables to be partitioned along with the column to be 
        partitioned on and the retention period in a separate feeder table. 
    *   Query the table above to get each row and perform the partition on a 
        new table.
    *   Feed the new table with data from the old table.
    *   Drop the old table.
*/

--Statement to rename table:
--RENAME /* OLD TABLE NAME */ TO /* NEW TABLE NAME */

--Create New Table with Partition:
CREATE TABLE sales (
    prod_id       NUMBER(6),
    cust_id       NUMBER,
    time_id       DATE,
    channel_id    CHAR(1),
    promo_id      NUMBER(6),
    quantity_sold NUMBER(3),
    amount_sold   NUMBER(10, 2));
--PARTITION BY RANGE (time_id)
-- ( PARTITION sales_q1_2006 VALUES LESS THAN (TO_DATE('01-APR-2006','dd-MON-yyyy'))
--    TABLESPACE tsa
-- ); 
 
 
CREATE TABLE activity_audit_temp (
    activity_id     NUMBER(38),
    activity_code   varchar(30),
    ba_id           varchar(100),
    activity_time   date,
    wac_id          varchar(100),
    flex_field1     varchar(255),
    flex_field2     varchar(255),
    create_time     date,
    csr_user_id     varchar(65));
--partition by range extract(day(create_time))
--(partition partition_quota values less than extract(day(sysdate)))
--    tablespace pq
--);   

CREATE INDEX ACTIVITY_AUDIT_AU
ON ACTIVITY_AUDIT_TEMP(SCG_MYACCT_CUSTOMPC, AA_BAID_XX);


-- Create partitions by month

--ALTER TABLE t1 
--PARTITION BY RANGE( MONTH(FROM_UNIXTIME(transaction_date) )
--SUBPARTITION BY HASH( DAY(FROM_UNIXTIME(transaction_date)) )
--SUBPARTITIONS 31 (
--    PARTITION p0 VALUES LESS THAN (2),
--    PARTITION p1 VALUES LESS THAN (3),
--    PARTITION p2 VALUES LESS THAN (4),
--    PARTITION p3 VALUES LESS THAN (5),
--    PARTITION p4 VALUES LESS THAN (6),
--    PARTITION p5 VALUES LESS THAN (7),
--    PARTITION p6 VALUES LESS THAN (8),
--    PARTITION p7 VALUES LESS THAN (9),
--    PARTITION p8 VALUES LESS THAN (10),
--    PARTITION p9 VALUES LESS THAN (11),
--    PARTITION p10 VALUES LESS THAN (12),
--    PARTITION p11 VALUES LESS THAN MAXVALUE
--);

ALTER TABLE ACTIVITY_AUDIT MODIFY
PARTITION BY RANGE (CREATE_TIME) SUBPARTITION BY HASH (ACTIVITY_ID) (
    PARTITION ACTIVITY_AUDIT_2016 VALUES LESS THAN (TO_DATE('01-JAN-2017', 'DD-MON-YYYY'))(
        SUBPARTITION AA_PART_2016_1,
        SUBPARTITION AA_PART_2016_2,
        SUBPARTITION AA_PART_2016_3,
        SUBPARTITION AA_PART_2016_4
    ),
    PARTITION ACTIVITY_AUDIT_2017 VALUES LESS THAN (TO_DATE('01-JAN-2018', 'DD-MON-YYYY'))(
        SUBPARTITION AA_PART_2017_1,
        SUBPARTITION AA_PART_2017_2,
        SUBPARTITION AA_PART_2017_3,
        SUBPARTITION AA_PART_2017_4
    ),
    PARTITION ACTIVITY_AUDIT_2018 VALUES LESS THAN (TO_DATE('01-JAN-2019', 'DD-MON-YYYY'))(
        SUBPARTITION AA_PART_2018_1,
        SUBPARTITION AA_PART_2018_2,
        SUBPARTITION AA_PART_2018_3,
        SUBPARTITION AA_PART_2018_4
    ),
    PARTITION ACTIVITY_AUDIT_2019 VALUES LESS THAN (TO_DATE('01-JAN-2020', 'DD-MON-YYYY'))(
        SUBPARTITION AA_PART_2019_1,
        SUBPARTITION AA_PART_2019_2,
        SUBPARTITION AA_PART_2019_3,
        SUBPARTITION AA_PART_2019_4
    ),
    PARTITION ACTIVITY_AUDIT_2020 VALUES LESS THAN (TO_DATE('01-JAN-2021', 'DD-MON-YYYY'))(
        SUBPARTITION AA_PART_2020_1,
        SUBPARTITION AA_PART_2020_2,
        SUBPARTITION AA_PART_2020_3,
        SUBPARTITION AA_PART_2020_4
    ),
    PARTITION ACTIVITY_AUDIT_2021 VALUES LESS THAN (TO_DATE('01-JAN-2022', 'DD-MON-YYYY'))(
        SUBPARTITION AA_PART_2021_1,
        SUBPARTITION AA_PART_2021_2,
        SUBPARTITION AA_PART_2021_3,
        SUBPARTITION AA_PART_2021_4
    )
)  
UPDATE INDEXES(
    AA_BAID_IDX LOCAL,
    AA_CRTM_IDX LOCAL,
    AA_ACTCD_IDX    LOCAL,
    AA_ACTTM_IDX    LOCAL,
    AA_WACID_IDX    LOCAL,
    PK_ACCOUNT_ACTIVITY GLOBAL
);

desc activity_audit;

CREATE TABLE partition_table (
    TABLE_NAME VARCHAR(50),
    PARTITION_COLUMN VARCHAR(100) );
    
COMMIT;    

INSERT INTO partition_table (
    table_name,
    partition_column
) VALUES (
    'ACTIVITY_AUDIT',
    'CREATE_TIME'
);

COMMIT;

select count(*)
from partition_table;

--DECLARE
--    count_current PLS_INTEGER := 0,
--    max_count pls_integer := 26,
--    tablename nvarchar(50) DEFAULT '',
--    columnname nvarchar(100) DEFAULT '';
--
--WHILE COUNT_CURRENT< MAX_COUNT
--BEGIN
--        SELECT TABLENAME AS TABLE_NAME, COLUMNNAME AS COLUMN_NAME
--        FROM PARTITION_TABLE WHERE ID = COUNT_CURRENT,
--        
--        ALTER TABLE TABLENAME MODIFY
--        PARTITION BY RANGE (COLUMN_NAME) (
--        
--            PARTITION p0 VALUES LESS THAN (TO_MONTH(2)),
--            PARTITION p1 VALUES LESS THAN (3),
--            PARTITION p2 VALUES LESS THAN (4),
--            PARTITION p3 VALUES LESS THAN (5),
--            PARTITION p4 VALUES LESS THAN (6),
--            PARTITION p5 VALUES LESS THAN (7),
--            PARTITION p6 VALUES LESS THAN (8),
--            PARTITION p7 VALUES LESS THAN (9),
--            PARTITION p8 VALUES LESS THAN (10),
--            PARTITION p9 VALUES LESS THAN (11),
--            PARTITION p10 VALUES LESS THAN (12),
--            PARTITION p11 VALUES LESS THAN maxvalue ) count_current = count_current + 1;
--        end;

CREATE TABLE PARTITION_LOG (
    PROCEDURE_RUN_ID    NUMBER(5),
    PROCEDURE_RUN   VARCHAR(50),
    LAST_EXECUTE_TIME   DATE,
    LAST_EXECUTE_USER   VARCHAR(50)
);

SELECT *
FROM partition_log;

SELECT *
FROM partition_table;

CREATE OR REPLACE PROCEDURE PARTITION_DB(USERNAME IN VARCHAR) AS
BEGIN
    FOR RESULT_CURSOR IN (SELECT TABLE_NAME, PARTITION_COLUMN FROM partition_table) LOOP        
        execute immediate
            -- CREATE THE TEMPORARY TABLE FIRST FOR EACH TABLE
            'CREATE TABLE RESULT_CURSOR.TABLE_NAME||''_TEMP'' AS (SELECT * FROM RESULT_CURSOR.TABLE_NAME WHERE ROWNUM=0);
            
            ALTER TABLE RESULT_CURSOR.TABLE_NAME||''_TEMP'' MODIFY 
            PARTITION BY RANGE (RESULT_CURSOR.PARTITION_COLUMN) (                
                 PARTITION observations_past    VALUES LESS THAN (TO_DATE(''20000101'',''YYYYMMDD'')),
                 PARTITION observations_CY_2000 VALUES LESS THAN (TO_DATE(''20010101'',''YYYYMMDD'')),
                 PARTITION observations_CY_2001 VALUES LESS THAN (TO_DATE(''20020101'',''YYYYMMDD'')),
                 PARTITION observations_CY_2002 VALUES LESS THAN (TO_DATE(''20030101'',''YYYYMMDD'')),
                 PARTITION observations_CY_2003 VALUES LESS THAN (TO_DATE(''20040101'',''YYYYMMDD'')),
                 PARTITION observations_CY_2004 VALUES LESS THAN (TO_DATE(''20050101'',''YYYYMMDD'')),
                 PARTITION observations_CY_2005 VALUES LESS THAN (TO_DATE(''20060101'',''YYYYMMDD'')),
                 PARTITION observations_CY_2006 VALUES LESS THAN (TO_DATE(''20070101'',''YYYYMMDD'')),
                 PARTITION observations_CY_2007 VALUES LESS THAN (TO_DATE(''20080101'',''YYYYMMDD'')),
                 PARTITION observations_CY_2008 VALUES LESS THAN (TO_DATE(''20090101'',''YYYYMMDD'')),
                 PARTITION observations_CY_2009 VALUES LESS THAN (TO_DATE(''20100101'',''YYYYMMDD'')),
                 PARTITION observations_CY_2010 VALUES LESS THAN (TO_DATE(''20110101'',''YYYYMMDD'')),
                 PARTITION observations_FUTURE  VALUES LESS THAN ( MAXVALUE ) )';
    END LOOP;
END;

CREATE TABLE xxjktst_part (
    owner       VARCHAR2(128),
    object_name VARCHAR2(128),
    object_type VARCHAR2(23)
)
    PARTITION BY LIST ( owner ) ( PARTITION apps VALUES ( 'APPS' ),
        PARTITION applsys VALUES ( 'APPLSYS' ),
        PARTITION pub VALUES ( 'PUBLIC' ),
        PARTITION the_rest VALUES ( DEFAULT )
    );
    
drop table XXJKTST_PART;

-- Works:
create or replace procedure test_proc(num_test in Number) as
begin
FOR i IN 1..3 LOOP
    DBMS_OUTPUT.PUT_LINE (i);
  END LOOP;    
end;  

begin
test_proc(60);
end;

CREATE OR REPLACE PROCEDURE test_proc_table_create IS
    stmt VARCHAR2(4000);
BEGIN
    stmt := 'create table test_table (test_id number not null primary key, test_name varchar2(50))';
    EXECUTE IMMEDIATE stmt;
END;

BEGIN
    test_proc_table_create;
END;

CREATE OR REPLACE PROCEDURE exec_multiple_table_create (
    table_name  IN VARCHAR2,
    column_name IN VARCHAR2
) IS
    stmt VARCHAR2(5000);    
BEGIN
    stmt := 'create table '
            || table_name
            || '_temp as (select * from '
            || table_name
            || ')
            (PARTITION observations_past    VALUES LESS THAN (TO_DATE(''20000101'',''YYYYMMDD'')),
                 PARTITION observations_CY_2000 VALUES LESS THAN (TO_DATE(''20010101'',''YYYYMMDD'')),
                 PARTITION observations_CY_2001 VALUES LESS THAN (TO_DATE(''20020101'',''YYYYMMDD'')),
                 PARTITION observations_CY_2002 VALUES LESS THAN (TO_DATE(''20030101'',''YYYYMMDD'')),
                 PARTITION observations_CY_2003 VALUES LESS THAN (TO_DATE(''20040101'',''YYYYMMDD'')),
                 PARTITION observations_CY_2004 VALUES LESS THAN (TO_DATE(''20050101'',''YYYYMMDD'')),
                 PARTITION observations_CY_2005 VALUES LESS THAN (TO_DATE(''20060101'',''YYYYMMDD'')),
                 PARTITION observations_CY_2006 VALUES LESS THAN (TO_DATE(''20070101'',''YYYYMMDD'')),
                 PARTITION observations_CY_2007 VALUES LESS THAN (TO_DATE(''20080101'',''YYYYMMDD'')),
                 PARTITION observations_CY_2008 VALUES LESS THAN (TO_DATE(''20090101'',''YYYYMMDD'')),
                 PARTITION observations_CY_2009 VALUES LESS THAN (TO_DATE(''20100101'',''YYYYMMDD'')),
                 PARTITION observations_CY_2010 VALUES LESS THAN (TO_DATE(''20110101'',''YYYYMMDD'')),
                 PARTITION observations_FUTURE  VALUES LESS THAN ( MAXVALUE ) )';
    EXECUTE IMMEDIATE stmt;
--    stmt := 'alter table '
--            || table_name
--            || '_temp modify partition by range('
--            || column_name
--            || ') (PARTITION observations_past    VALUES LESS THAN (TO_DATE(''20000101'',''YYYYMMDD'')),
--                 PARTITION observations_CY_2000 VALUES LESS THAN (TO_DATE(''20010101'',''YYYYMMDD'')),
--                 PARTITION observations_CY_2001 VALUES LESS THAN (TO_DATE(''20020101'',''YYYYMMDD'')),
--                 PARTITION observations_CY_2002 VALUES LESS THAN (TO_DATE(''20030101'',''YYYYMMDD'')),
--                 PARTITION observations_CY_2003 VALUES LESS THAN (TO_DATE(''20040101'',''YYYYMMDD'')),
--                 PARTITION observations_CY_2004 VALUES LESS THAN (TO_DATE(''20050101'',''YYYYMMDD'')),
--                 PARTITION observations_CY_2005 VALUES LESS THAN (TO_DATE(''20060101'',''YYYYMMDD'')),
--                 PARTITION observations_CY_2006 VALUES LESS THAN (TO_DATE(''20070101'',''YYYYMMDD'')),
--                 PARTITION observations_CY_2007 VALUES LESS THAN (TO_DATE(''20080101'',''YYYYMMDD'')),
--                 PARTITION observations_CY_2008 VALUES LESS THAN (TO_DATE(''20090101'',''YYYYMMDD'')),
--                 PARTITION observations_CY_2009 VALUES LESS THAN (TO_DATE(''20100101'',''YYYYMMDD'')),
--                 PARTITION observations_CY_2010 VALUES LESS THAN (TO_DATE(''20110101'',''YYYYMMDD'')),
--                 PARTITION observations_FUTURE  VALUES LESS THAN ( MAXVALUE ) )';
--    EXECUTE IMMEDIATE stmt;
END;


BEGIN
    FOR partition_item IN (
        SELECT
            table_name as table_name,
            partition_column as partition_column
        FROM
            partition_table
    ) LOOP
        exec_multiple_table_create(partition_item.table_name, partition_item.partition_column);
        feed_data(partition_item.table_name);
        drop_table(partition_table.table_name);
        rename_table(partition_table.table_name);
        COMMIT;
    END LOOP;
END;


BEGIN
    FOR partition_item IN (
        SELECT
            table_name,
            partition_column
        FROM
            partition_table
    ) LOOP
        exec_multiple_table_create_alt(partition_item.table_name, partition_item.partition_column);
        feed_data(partition_item.table_name);
        drop_table(partition_table.table_name);
        rename_table(partition_table.table_name);
        COMMIT;        
    END LOOP;
END;


create or replace procedure feed_data(source_table in varchar2) as
stmt varchar2(100);
begin
    stmt := 'insert into ' || source_table || '_temp select * from activity_audit';
    execute immediate stmt;
end;

create or replace procedure rename_table_test(source_table in varchar2) as
stmt varchar(100);
begin
    stmt := 'rename ' || source_table || ' to ' || source_table || '_new';
    commit;
end;    

select *
from temp_table_new;

commit;

create or replace procedure drop_table(source_table in varchar2) as
stmt varchar(50);
begin
    stmt := 'drop table ' || source_table;
    execute immediate stmt;
    commit;
end;    

desc activity_audit;
                  
ALTER TABLE activity_audit 
 MODIFY
   DROP RANGE BETWEEN DATE '1992-01-01' 
              AND     DATE '1992-12-31'
              EACH INTERVAL '1' MONTH
   ADD  RANGE BETWEEN  DATE '1999-01-01' 
              AND      DATE '2000-12-31' 
              EACH INTERVAL '1' MONTH;   

stmt := 'create index '
    || tablename
    || '_idx_jan on '
    || tablename
    || '('
    || columnname
    || ' GLOBAL PARTITION BY HASH ('
    || columnname
    || ') (PARTITION IDX_JAN TABLESPACE SCG_MYACCT_CUSTOM_IDX )'; 
    
CREATE OR REPLACE PROCEDURE create_indexes (
    tablename  IN VARCHAR2,
    columnname IN VARCHAR2
) AS
    stmt VARCHAR(1000);
BEGIN
    stmt := 'create index '
            || tablename
            || '_idx_jan on '
            || tablename
            || '('
            || columnname
            || ') GLOBAL PARTITION BY HASH ('
            || columnname
            || ') (PARTITION IDX_JAN TABLESPACE SCG_MYACCT_CUSTOM_IDX )';
    EXECUTE IMMEDIATE stmt;
        stmt := 'create index '
            || tablename
            || '_idx_feb on '
            || tablename
            || '('
            || columnname
            || ') GLOBAL PARTITION BY HASH ('
            || columnname
            || ') (PARTITION IDX_FEB TABLESPACE SCG_MYACCT_CUSTOM_IDX )';
    EXECUTE IMMEDIATE stmt;
        stmt := 'create index '
            || tablename
            || '_idx_mar on '
            || tablename
            || '('
            || columnname
            || ') GLOBAL PARTITION BY HASH ('
            || columnname
            || ') (PARTITION IDX_MAR TABLESPACE SCG_MYACCT_CUSTOM_IDX )';
    EXECUTE IMMEDIATE stmt;
        stmt := 'create index '
            || tablename
            || '_idx_apr on '
            || tablename
            || '('
            || columnname
            || ') GLOBAL PARTITION BY HASH ('
            || columnname
            || ') (PARTITION IDX_APR TABLESPACE SCG_MYACCT_CUSTOM_IDX )';
    EXECUTE IMMEDIATE stmt;
        stmt := 'create index '
            || tablename
            || '_idx_may on '
            || tablename
            || '('
            || columnname
            || ') GLOBAL PARTITION BY HASH ('
            || columnname
            || ') (PARTITION IDX_MAY TABLESPACE SCG_MYACCT_CUSTOM_IDX )';
    EXECUTE IMMEDIATE stmt;
        stmt := 'create index '
            || tablename
            || '_idx_jun on '
            || tablename
            || '('
            || columnname
            || ') GLOBAL PARTITION BY HASH ('
            || columnname
            || ') (PARTITION IDX_JUN TABLESPACE SCG_MYACCT_CUSTOM_IDX )';
    EXECUTE IMMEDIATE stmt;
        stmt := 'create index '
            || tablename
            || '_idx_jul on '
            || tablename
            || '('
            || columnname
            || ') GLOBAL PARTITION BY HASH ('
            || columnname
            || ') (PARTITION IDX_JUL TABLESPACE SCG_MYACCT_CUSTOM_IDX )';
    EXECUTE IMMEDIATE stmt;
        stmt := 'create index '
            || tablename
            || '_idx_aug on '
            || tablename
            || '('
            || columnname
            || ') GLOBAL PARTITION BY HASH ('
            || columnname
            || ') (PARTITION IDX_AUG TABLESPACE SCG_MYACCT_CUSTOM_IDX )';
    EXECUTE IMMEDIATE stmt;
        stmt := 'create index '
            || tablename
            || '_idx_sep on '
            || tablename
            || '('
            || columnname
            || ') GLOBAL PARTITION BY HASH ('
            || columnname
            || ') (PARTITION IDX_SEP TABLESPACE SCG_MYACCT_CUSTOM_IDX )';
    EXECUTE IMMEDIATE stmt;
        stmt := 'create index '
            || tablename
            || '_idx_oct on '
            || tablename
            || '('
            || columnname
            || ') GLOBAL PARTITION BY HASH ('
            || columnname
            || ') (PARTITION IDX_OCT TABLESPACE SCG_MYACCT_CUSTOM_IDX )';
    EXECUTE IMMEDIATE stmt;
        stmt := 'create index '
            || tablename
            || '_idx_nov on '
            || tablename
            || '('
            || columnname
            || ') GLOBAL PARTITION BY HASH ('
            || columnname
            || ') (PARTITION IDX_NOV TABLESPACE SCG_MYACCT_CUSTOM_IDX )';
    EXECUTE IMMEDIATE stmt;
        stmt := 'create index '
            || tablename
            || '_idx_dec on '
            || tablename
            || '('
            || columnname
            || ') GLOBAL PARTITION BY HASH ('
            || columnname
            || ') (PARTITION IDX_DEC TABLESPACE SCG_MYACCT_CUSTOM_IDX )';
    EXECUTE IMMEDIATE stmt;        
END;

create or replace procedure test_proc(ab in varchar2) as
    stmt varchar(5000);
begin
    dbms_output.put_line('Value of ab again: ' || ab);
    stmt := 'select * from activity_audit';
    execute immediate stmt;
end; 

declare
    ab varchar2(50);
begin
    ab := 'Testy Test';
    test_proc(ab);
end;    


create or replace procedure cleanup as
begin
  for rec in (select table_name 
              from   user_tables 
              where  table_name like '%_TEMP_TEMP'
             )
  loop
    execute immediate 'drop table '||rec.table_name;
  end loop;             
end;

begin
    cleanup;
end;    


SET SERVEROUTPUT ON;
     CREATE OR REPLACE DIRECTORY USER_DIR AS 'C:\Project\Database/ Scripts\Partition/ Research\Procedures'; 
     GRANT READ ON DIRECTORY USER_DIR TO PUBLIC;

     DECLARE 
        V1 VARCHAR2(200); --32767
        F1 UTL_FILE.FILE_TYPE; 
     BEGIN
f1 := utl_file.fopen('USER_DIR', 'Partition_file.csv', 'R');

LOOP
    BEGIN
        utl_file.get_line(f1, v1);
        dbms_output.put_line(v1);
    EXCEPTION
        WHEN no_data_found THEN
            EXIT;
    END;
END LOOP;

IF utl_file.is_open(f1) THEN
    dbms_output.put_line('File is Open');
END IF;

utl_file.fclose(f1);

end;
/

SET SERVEROUTPUT OFF;

BEGIN
    proc_subpartition_test('ACTIVITY_AUDIT', 'CREATE_TIME');
END;


BEGIN
    dbms_output.put_line('-- RUN SCRIPT --> BEGINNING EXECUTION <--');
    FOR partition_item IN (
        SELECT
            table_name,
            partition_column
        FROM
            partition_table
    ) LOOP
        dbms_output.put_line('  RUN SCRIPT.INITIATING PROCEDURE --> exec_multiple_table_create <--');
        dbms_output.put_line(' Executing table: '
                             || partition_item.table_name
                             || ' and column_name: '
                             || partition_item.partition_column);

        proc_subpartition_test(partition_item.table_name, partition_item.partition_column);        
        COMMIT;
    END LOOP;
END;    


select owner, table_name, partitioned from all_tables where owner in ('SCG_MYACCT_CUSTOMPC', 'SCG_MYACCT_SPRINGB');


create table test_part_table (
    test_id number(2),
    test_time timestamp not null
)
partition by range (test_time) interval (NUMTOYMINTERVAL(1, 'MONTH')) subpartition by hash (test_id) subpartitions 12(
    partition p0 values less than (TO_DATE('1-1-2021', 'DD-MM-YYYY')) ,
    partition p1 values less than (TO_DATE('1-1-2022', 'DD-MM-YYYY')) 
);

SELECT NUMTOYMINTERVAL(34, 'MONTH') FROM DUAL;

SELECT activity_id, create_time + TO_YMINTERVAL('01-02') "15 months"
FROM ACTIVITY_AUDIT;


CREATE TABLE salestable
  (s_productid  NUMBER,
   s_saledate   DATE,
   s_custid     NUMBER,
   s_totalprice NUMBER)
PARTITION BY RANGE(s_saledate)
INTERVAL(NUMTOYMINTERVAL(1,'MONTH'))
 (PARTITION sal05q1 VALUES LESS THAN (TO_DATE('01-APR-2005', 'DD-MON-YYYY')) ,
  PARTITION sal05q2 VALUES LESS THAN (TO_DATE('01-JUL-2005', 'DD-MON-YYYY')) ,
  PARTITION sal05q3 VALUES LESS THAN (TO_DATE('01-OCT-2005', 'DD-MON-YYYY')) ,
  PARTITION sal05q4 VALUES LESS THAN (TO_DATE('01-JAN-2006', 'DD-MON-YYYY')) ,
  PARTITION sal06q1 VALUES LESS THAN (TO_DATE('01-APR-2006', 'DD-MON-YYYY')) ,
  PARTITION sal06q2 VALUES LESS THAN (TO_DATE('01-JUL-2006', 'DD-MON-YYYY')) ,
  PARTITION sal06q3 VALUES LESS THAN (TO_DATE('01-OCT-2006', 'DD-MON-YYYY')) ,
  PARTITION sal06q4 VALUES LESS THAN (TO_DATE('01-JAN-2007', 'DD-MON-YYYY')) );
  
  
CREATE TABLE sales
  ( prod_id       NUMBER(6)
  , cust_id       NUMBER
  , time_id       DATE
  , channel_id    CHAR(1)
  , promo_id      NUMBER(6)
  , quantity_sold NUMBER(3)
  , amount_sold   NUMBER(10,2)
  )
 PARTITION BY RANGE (time_id) SUBPARTITION BY HASH (cust_id)
  SUBPARTITIONS 8
 ( PARTITION sales_q1_2006 VALUES LESS THAN (TO_DATE('01-APR-2006','dd-MON-yyyy'))
 , PARTITION sales_q2_2006 VALUES LESS THAN (TO_DATE('01-JUL-2006','dd-MON-yyyy'))
 , PARTITION sales_q3_2006 VALUES LESS THAN (TO_DATE('01-OCT-2006','dd-MON-yyyy'))
 , PARTITION sales_q4_2006 VALUES LESS THAN (TO_DATE('01-JAN-2007','dd-MON-yyyy'))
 );  
 
select count(*) from activity_audit_temp partition (OBSERVATIONS_CY_2006); 

select count(*)
from activity_audit aa
join activity_audit_temp aat partition (OBSERVATIONS_CY_2001)
on aa.create_time = aat.create_time
ORDER BY AA.CREATE_TIME ASC;

SELECT
    O.OBJECT_NAME,
    S.SID,
    S.SERIAL#,
    P.SPID,
    S.PROGRAM,
    SQ.SQL_FULLTEXT,
    S.LOGON_TIME
FROM
    V$LOCKED_OBJECT L,
    DBA_OBJECTS O,
    V$SESSION S,
    V$PROCESS P,
    V$SQL SQ
WHERE
    L.OBJECT_ID = O.OBJECT_ID
    AND L.SESSION_ID = S.SID
    AND S.PADDR = P.ADDR
    AND S.SQL_ADDRESS = SQ.ADDRESS;
    
flashback table ACTIVITY_AUDIT to before drop;

create table activity_audit as select * from activity_audit_temp;
insert into activity_audit select * from activity_audit_temp;


CREATE OR REPLACE PROCEDURE analyze_table (
    table_name IN VARCHAR2
) AS
    stmt VARCHAR2(50);
BEGIN
    stmt := 'ANALYZE TABLE '
            || table_name
            || ' COMPUTE STATISTICS';
    EXECUTE IMMEDIATE stmt;
END;
    