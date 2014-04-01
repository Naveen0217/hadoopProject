/*
    Author: Michaael Kepple
    #mysql --user="$uname" --password="$passw" --execute "SET @currDb='"$name"'; SOURCE sqlScript.sql;"
*/
USE mysql;
DELIMITER //
DROP PROCEDURE IF EXISTS upgradeFormat
//
CREATE PROCEDURE upgradeFormat(p_inparam VARCHAR(100))
BEGIN
    DECLARE oldDb VARCHAR(100);
    SET oldDb :=
    (
        SELECT SCHEMA_NAME
        FROM INFORMATION_SCHEMA.SCHEMATA
        WHERE SCHEMA_NAME = CONCAT('#mysql50#',@currDb)
        LIMIT 1
    );
    SELECT oldDb;
    SELECT p_inparam;
    IF oldDb IS NOT NULL THEN
        SELECT 'Equal!' AS '';
    ELSE
        SELECT 'Not Equal!' AS '';
    END IF;
END
//
CALL upgradeFormat(@currDb) //
DELIMITER ;
