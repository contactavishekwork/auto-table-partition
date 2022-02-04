/**
The procedure is responsible for creating multiple tables. The procedure 
receives the table name of the existing table from Run Script and creates a 
mirror table with new partitions. 

@author: Avishek Datta
**/

CREATE OR REPLACE PROCEDURE exec_multiple_table_create (
    table_name  IN VARCHAR2,
    column_name IN VARCHAR2
) IS

    stmt       VARCHAR2(5000);
    tablename  VARCHAR2(50);
    columnname VARCHAR2(50);
    proc_id    NUMBER(10);
BEGIN
    tablename := table_name;
    columnname := column_name;
--    dbms_output.put_line('-- EXECUTING PROCEDURE -> CREATE TABLES WITH PARTITIONS <--');
    stmt := 'create table '
            || tablename
            || '_temp as (select * from '
            || tablename
            || ' where 1=2)';
    EXECUTE IMMEDIATE stmt;
--    dbms_output.put_line('-- Table Created --');
--    dbms_output.put_line('-- Adding Partitions --');
    stmt := 'alter table '
            || tablename
            || '_temp modify partition by range('
            || columnname
            || ') subpartition by hash('
            || columnname
            || ') subpartitions 12
            (PARTITION observations_past    VALUES LESS THAN (TO_DATE(''20100101'',''YYYYMMDD'')),
                 PARTITION observations_CY_2011 VALUES LESS THAN (TO_DATE(''20110101'',''YYYYMMDD'')),
                 PARTITION observations_CY_2012 VALUES LESS THAN (TO_DATE(''20120101'',''YYYYMMDD'')),
                 PARTITION observations_CY_2013 VALUES LESS THAN (TO_DATE(''20130101'',''YYYYMMDD'')),
                 PARTITION observations_CY_2014 VALUES LESS THAN (TO_DATE(''20140101'',''YYYYMMDD'')),
                 PARTITION observations_CY_2015 VALUES LESS THAN (TO_DATE(''20150101'',''YYYYMMDD'')),
                 PARTITION observations_CY_2016 VALUES LESS THAN (TO_DATE(''20160101'',''YYYYMMDD'')),
                 PARTITION observations_CY_2017 VALUES LESS THAN (TO_DATE(''20170101'',''YYYYMMDD'')),
                 PARTITION observations_CY_2018 VALUES LESS THAN (TO_DATE(''20180101'',''YYYYMMDD'')),
                 PARTITION observations_CY_2019 VALUES LESS THAN (TO_DATE(''20190101'',''YYYYMMDD'')),
                 PARTITION observations_CY_2020 VALUES LESS THAN (TO_DATE(''20200101'',''YYYYMMDD'')),
                 PARTITION observations_CY_2021 VALUES LESS THAN (TO_DATE(''20210101'',''YYYYMMDD'')),
                 PARTITION observations_FUTURE  VALUES LESS THAN ( MAXVALUE ) )';

    EXECUTE IMMEDIATE stmt;
--    dbms_output.put_line('-- Partitions Created --');
--    dbms_output.put_line('-- PROCEDURE COMPLETE -> CREATE TABLE <--');
END;