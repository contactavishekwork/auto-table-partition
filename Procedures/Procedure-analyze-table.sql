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