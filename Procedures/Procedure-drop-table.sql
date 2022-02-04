/**
This procedure is responsible for deleting the table. The table to be deleted
is provided by the Run Script. In order to execute this script, the user will 
require DROP permission. 

@author: Avishek Datta
**/

CREATE OR REPLACE PROCEDURE drop_table (
    source_table IN VARCHAR2
) AS
    stmt VARCHAR(50);
BEGIN
--    DBMS_OUTPUT.PUT_LINE('-- EXECUTING PROCEDURE -> DROP TABLE <--');
    stmt := 'drop table ' || source_table;
    EXECUTE IMMEDIATE stmt;
    COMMIT;
--    DBMS_OUTPUT.PUT_LINE('-- PROCEDURE COMPLETE -> DROP TABLE <--');
END;