--------------------------------------------------------
--  File created - Thursday-January-27-2022   
--------------------------------------------------------
--------------------------------------------------------
--  DDL for Procedure PROC_SUBPARTITION_TEST
--------------------------------------------------------
set define off;

  CREATE OR REPLACE EDITIONABLE PROCEDURE "SCG_MYACCT_CUSTOMPC"."PROC_SUBPARTITION_TEST" (
    table_name  IN VARCHAR2,
    column_name IN VARCHAR2
) AS

    stmt       VARCHAR2(32767);
    tablename  VARCHAR2(500);
    columnname VARCHAR2(500);
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
    dbms_output.put_line('-- Table Created --');
    dbms_output.put_line('-- Adding Partitions --');
    stmt := 'alter table '
            || tablename
            || '_temp modify partition by range('
            || columnname
            || ') subpartition by hash('
            || 'ACTIVITY_ID'
            || ')
            (PARTITION observations_past    VALUES LESS THAN (TO_DATE(''20120101'',''YYYYMMDD'')) (
        SUBPARTITION JAN,
        SUBPARTITION FEB,
        SUBPARTITION MAR,
        SUBPARTITION APR,
        SUBPARTITION MAY,
        SUBPARTITION JUN,
        SUBPARTITION JUL,
        SUBPARTITION AUG,
        SUBPARTITION SEP,
        SUBPARTITION OCT,
        SUBPARTITION NOV,
        SUBPARTITION DEC
    ),
                 PARTITION observations_CY_2000 VALUES LESS THAN (TO_DATE(''20130101'',''YYYYMMDD'')) (
        SUBPARTITION JAN,
        SUBPARTITION FEB,
        SUBPARTITION MAR,
        SUBPARTITION APR,
        SUBPARTITION MAY,
        SUBPARTITION JUN,
        SUBPARTITION JUL,
        SUBPARTITION AUG,
        SUBPARTITION SEP,
        SUBPARTITION OCT,
        SUBPARTITION NOV,
        SUBPARTITION DEC
    ),
                 PARTITION observations_CY_2001 VALUES LESS THAN (TO_DATE(''20140101'',''YYYYMMDD'')) (
        SUBPARTITION JAN,
        SUBPARTITION FEB,
        SUBPARTITION MAR,
        SUBPARTITION APR,
        SUBPARTITION MAY,
        SUBPARTITION JUN,
        SUBPARTITION JUL,
        SUBPARTITION AUG,
        SUBPARTITION SEP,
        SUBPARTITION OCT,
        SUBPARTITION NOV,
        SUBPARTITION DEC
    ),
                 PARTITION observations_CY_2002 VALUES LESS THAN (TO_DATE(''20130101'',''YYYYMMDD'')) (
        SUBPARTITION JAN,
        SUBPARTITION FEB,
        SUBPARTITION MAR,
        SUBPARTITION APR,
        SUBPARTITION MAY,
        SUBPARTITION JUN,
        SUBPARTITION JUL,
        SUBPARTITION AUG,
        SUBPARTITION SEP,
        SUBPARTITION OCT,
        SUBPARTITION NOV,
        SUBPARTITION DEC
    ),
                 PARTITION observations_CY_2003 VALUES LESS THAN (TO_DATE(''20140101'',''YYYYMMDD'')) (
        SUBPARTITION JAN,
        SUBPARTITION FEB,
        SUBPARTITION MAR,
        SUBPARTITION APR,
        SUBPARTITION MAY,
        SUBPARTITION JUN,
        SUBPARTITION JUL,
        SUBPARTITION AUG,
        SUBPARTITION SEP,
        SUBPARTITION OCT,
        SUBPARTITION NOV,
        SUBPARTITION DEC
    ),
                 PARTITION observations_CY_2004 VALUES LESS THAN (TO_DATE(''20150101'',''YYYYMMDD'')) (
        SUBPARTITION JAN,
        SUBPARTITION FEB,
        SUBPARTITION MAR,
        SUBPARTITION APR,
        SUBPARTITION MAY,
        SUBPARTITION JUN,
        SUBPARTITION JUL,
        SUBPARTITION AUG,
        SUBPARTITION SEP,
        SUBPARTITION OCT,
        SUBPARTITION NOV,
        SUBPARTITION DEC
    ),
                 PARTITION observations_CY_2005 VALUES LESS THAN (TO_DATE(''20160101'',''YYYYMMDD'')) (
        SUBPARTITION JAN,
        SUBPARTITION FEB,
        SUBPARTITION MAR,
        SUBPARTITION APR,
        SUBPARTITION MAY,
        SUBPARTITION JUN,
        SUBPARTITION JUL,
        SUBPARTITION AUG,
        SUBPARTITION SEP,
        SUBPARTITION OCT,
        SUBPARTITION NOV,
        SUBPARTITION DEC
    ),
                 PARTITION observations_CY_2006 VALUES LESS THAN (TO_DATE(''20170101'',''YYYYMMDD'')) (
        SUBPARTITION JAN,
        SUBPARTITION FEB,
        SUBPARTITION MAR,
        SUBPARTITION APR,
        SUBPARTITION MAY,
        SUBPARTITION JUN,
        SUBPARTITION JUL,
        SUBPARTITION AUG,
        SUBPARTITION SEP,
        SUBPARTITION OCT,
        SUBPARTITION NOV,
        SUBPARTITION DEC
    ),
                 PARTITION observations_CY_2007 VALUES LESS THAN (TO_DATE(''20180101'',''YYYYMMDD'')) (
        SUBPARTITION JAN,
        SUBPARTITION FEB,
        SUBPARTITION MAR,
        SUBPARTITION APR,
        SUBPARTITION MAY,
        SUBPARTITION JUN,
        SUBPARTITION JUL,
        SUBPARTITION AUG,
        SUBPARTITION SEP,
        SUBPARTITION OCT,
        SUBPARTITION NOV,
        SUBPARTITION DEC
    ),
                 PARTITION observations_CY_2008 VALUES LESS THAN (TO_DATE(''20190101'',''YYYYMMDD'')) (
        SUBPARTITION JAN,
        SUBPARTITION FEB,
        SUBPARTITION MAR,
        SUBPARTITION APR,
        SUBPARTITION MAY,
        SUBPARTITION JUN,
        SUBPARTITION JUL,
        SUBPARTITION AUG,
        SUBPARTITION SEP,
        SUBPARTITION OCT,
        SUBPARTITION NOV,
        SUBPARTITION DEC
    ),
                 PARTITION observations_CY_2009 VALUES LESS THAN (TO_DATE(''20200101'',''YYYYMMDD'')) (
        SUBPARTITION JAN,
        SUBPARTITION FEB,
        SUBPARTITION MAR,
        SUBPARTITION APR,
        SUBPARTITION MAY,
        SUBPARTITION JUN,
        SUBPARTITION JUL,
        SUBPARTITION AUG,
        SUBPARTITION SEP,
        SUBPARTITION OCT,
        SUBPARTITION NOV,
        SUBPARTITION DEC
    ),
                 PARTITION observations_CY_2010 VALUES LESS THAN (TO_DATE(''20210101'',''YYYYMMDD'')) (
        SUBPARTITION JAN,
        SUBPARTITION FEB,
        SUBPARTITION MAR,
        SUBPARTITION APR,
        SUBPARTITION MAY,
        SUBPARTITION JUN,
        SUBPARTITION JUL,
        SUBPARTITION AUG,
        SUBPARTITION SEP,
        SUBPARTITION OCT,
        SUBPARTITION NOV,
        SUBPARTITION DEC
    ),
                 PARTITION observations_FUTURE  VALUES LESS THAN ( MAXVALUE ) ) (
        SUBPARTITION JAN,
        SUBPARTITION FEB,
        SUBPARTITION MAR,
        SUBPARTITION APR,
        SUBPARTITION MAY,
        SUBPARTITION JUN,
        SUBPARTITION JUL,
        SUBPARTITION AUG,
        SUBPARTITION SEP,
        SUBPARTITION OCT,
        SUBPARTITION NOV,
        SUBPARTITION DEC
    )';
    dbms_output.put_line('Query: ' || stmt);
    EXECUTE IMMEDIATE stmt;
    dbms_output.put_line('-- Partitions Created --');
    dbms_output.put_line('-- PROCEDURE COMPLETE -> CREATE TABLE <--');
END proc_subpartition_test;

/
