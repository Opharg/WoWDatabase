procedures = [
    f"""DROP PROCEDURE IF EXISTS `PROC_SET_PRIMARY_ANDOR_INDEX`;
    CREATE DEFINER=`root`@`localhost` PROCEDURE `PROC_SET_PRIMARY_ANDOR_INDEX`(IN p_tableName varchar(100), IN p_columnName varchar(100))
BEGIN
    DECLARE colExists INT;
    DECLARE idExists INT;
    DECLARE primaryKeyCount INT;
    SET @tableName = p_tableName;
    SET @columnName = p_columnName;
    
    SET @dyn_sql = CONCAT('SELECT COUNT(*) INTO @colExists FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = ''', @tableName, ''' AND COLUMN_NAME = ''', @columnName, '''');
    PREPARE stmt FROM @dyn_sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
    
    SET @dyn_sql = CONCAT('SELECT COUNT(*) INTO @idExists FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = ''', @tableName, ''' AND COLUMN_NAME = ''ID''');
    PREPARE stmt FROM @dyn_sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
    
    IF @idExists > 0 THEN
        SET @dyn_sql = CONCAT('SELECT COUNT(*) INTO @primaryKeyCount FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE TABLE_NAME = ''', @tableName, ''' AND CONSTRAINT_NAME = ''PRIMARY''');
        PREPARE stmt FROM @dyn_sql;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;
        
        IF @primaryKeyCount > 0 THEN
            SET @dyn_sql = CONCAT('ALTER TABLE ', @tableName, ' DROP PRIMARY KEY');
            PREPARE stmt FROM @dyn_sql;
            EXECUTE stmt;
            DEALLOCATE PREPARE stmt;
        END IF;
         
        SET @dyn_sql = CONCAT('ALTER TABLE ', @tableName, ' ADD PRIMARY KEY (ID)');
        PREPARE stmt FROM @dyn_sql;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;
        
        IF @colExists > 0 THEN
            SET @dyn_sql = CONCAT('ALTER TABLE ', @tableName, ' ADD INDEX (', @columnName, ')');
            PREPARE stmt FROM @dyn_sql;
            EXECUTE stmt;
            DEALLOCATE PREPARE stmt;
        END IF;
    END IF;

    IF @colExists > 0 AND @idExists = 0 THEN
        SET @dyn_sql = CONCAT('ALTER TABLE ', @tableName, ' ADD INDEX (', @columnName, ')');
        PREPARE stmt FROM @dyn_sql;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;

    END IF;
END;""",
    f"""DROP PROCEDURE IF EXISTS `PROC_GET_TABLE_COUNT_IN_SCHEMA`;
    CREATE DEFINER=`root`@`localhost` PROCEDURE `PROC_GET_TABLE_COUNT_IN_SCHEMA`(IN schema_name VARCHAR(100), OUT table_count INT)
BEGIN
    SELECT COUNT(*) INTO table_count
    FROM information_schema.tables
    WHERE table_schema = schema_name;
END;""",
    f"""DROP PROCEDURE IF EXISTS `PROC_GET_FOREIGN_KEY_COUNT_IN_SCHEMA`;
    CREATE DEFINER = root@localhost PROCEDURE PROC_GET_FOREIGN_KEY_COUNT_IN_SCHEMA(IN schema_name VARCHAR(100), OUT foreign_key_count INT)
BEGIN
    SELECT COUNT(*)
    INTO foreign_key_count
    FROM information_schema.key_column_usage
    WHERE table_schema = schema_name
      AND referenced_table_name IS NOT NULL;
END;"""
]
