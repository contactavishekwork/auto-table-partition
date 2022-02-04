/** 
This procedure gets the data from the source table and feeds the data to the 
new table. 
@author : Avishek Datta (adatta)
**/
CREATE OR REPLACE PROCEDURE feed_data (
    source_table IN VARCHAR2
) AS
    stmt    VARCHAR2(100);
    proc_id NUMBER(1);
BEGIN
--    dbms_output.put_line('-- EXECUTING PROCEDURE -> FEED DATA <--');
    stmt := 'INSERT INTO '
            || source_table
            || '_temp (SELECT * FROM '
            || source_table
            || ')';
    EXECUTE IMMEDIATE stmt;
    COMMIT;
--    dbms_output.put_line('-- PROCEDURE COMPLETE -> FEED DATA <--');
END;