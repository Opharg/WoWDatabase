procedures = [
    f"""DROP PROCEDURE IF EXISTS `PROC_SET_PRIMARY_ANDOR_INDEX`;
CREATE DEFINER=`root`@`localhost` PROCEDURE `PROC_SET_PRIMARY_ANDOR_INDEX`(IN p_tableName varchar(100), IN p_columnName varchar(100))
BEGIN
    DECLARE primaryKeyCount INT;

    -- Check if the primary key is already defined
    SET @dyn_sql = CONCAT('SELECT COUNT(*) INTO @primaryKeyCount FROM INFORMATION_SCHEMA.STATISTICS WHERE TABLE_NAME = ''', p_tableName, ''' AND INDEX_NAME = ''PRIMARY''');
    PREPARE stmt FROM @dyn_sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    IF @primaryKeyCount = 0 THEN
        -- If no primary key exists, make the input column the primary key
        SET @dyn_sql = CONCAT('ALTER TABLE ', p_tableName, ' ADD PRIMARY KEY (', p_columnName, ')');
        PREPARE stmt FROM @dyn_sql;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;
    ELSE
        -- If a primary key exists, add an index to the input column
        SET @dyn_sql = CONCAT('ALTER TABLE ', p_tableName, ' ADD INDEX (', p_columnName, ')');
        PREPARE stmt FROM @dyn_sql;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;
    END IF;
END;

"""
]
