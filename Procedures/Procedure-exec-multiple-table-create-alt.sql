CREATE OR REPLACE PROCEDURE exec_multiple_table_create_alt (
    table_name  IN VARCHAR2,
    column_name IN VARCHAR2
) IS
    stmt       VARCHAR2(5000);
    tablename  VARCHAR2(50);
    columnname VARCHAR2(50);
BEGIN
    tablename := table_name;
    columnname := column_name;
    stmt := 'create table '
            || tablename
            || '_temp as (select * from '
            || tablename
            || ' where 1=2)
            partition by range('
            || columnname
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
    RETURN;
END exec_multiple_table_create_alt;