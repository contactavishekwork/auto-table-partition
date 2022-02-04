/**
This table is responsible for renaming the temporary tables to the original
tables (non-partitioned). The original name of the table will be provided by the 
Run Script.

@author: Avishek Datta
**/

CREATE OR REPLACE PROCEDURE RENAME_TABLE (
    SOURCE_TABLE IN VARCHAR2
) AS
    STMT VARCHAR2(100);
BEGIN
--    DBMS_OUTPUT.PUT_LINE('-- EXECUTING PROCEDURE -> RENAME TABLE <--');
    STMT := 'RENAME '
            || SOURCE_TABLE
            || ' _TEMP to '
            || SOURCE_TABLE;
    EXECUTE IMMEDIATE STMT;        
    COMMIT;        
--    DBMS_OUTPUT.PUT_LINE('-- PROCEDURE COMPLETE -> RENAME TABLE <--');
END;