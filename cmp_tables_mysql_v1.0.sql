/*
* version：cmp_tables_mysql v1.0
* author： xijianhe@creditease.cn
* 
* 用途：
* 在mysql表在导入导出过程中数据表可能会出现记录丢失、出现未知记录或字段错误等情况，此脚本用于比较两个mysql表的差异
* 
* 输入：两张mysql表,原表（期望生成的表expected_table）和新表（实际生成的表actual_table）
* 1. 原表为期望期望生成的表（expected_table）
* 2. 新表为实际生成的表(actual_table)
*
* 输出：视图或临时表，包括
* 1. 视图exp_dup：原表（期望生成的表expected_table）中重复的记录，即原表中所有字段均相同的记录（全字段比较）;
* 2. 视图act_dup：新表（实际生成的表actual_table）中重复的记录，即新表中所有字段均相同的记录（全字段比较）;
* 3. 视图missing：原表中有但新表中没有的记录（仅比较key）
* 4. 视图unexpected：原表中没有但新表中有的记录（仅比较key）
* 5. 临时表mismatch：原表和新表都有相同的key，但对应的字段不同。表除key外，字段为: 原表v1 | v2 --> 新表v3 
*         即原表中某个key对应的两个或多个记录在该字段的值v1，v2在新表中对应字段为v3
* 
* 需要配置的参数：
* 1. 原表（期望生成的表expected_table）所在的库名称（变量expected_schema）
* 2. 新表（实际生成的表actual_table）所在的库名称（变量expected_schema）
* 3. 原表（期望生成的表expected_table）的表名称（变量expected_table）
* 4. 新表（实际生成的表actual_table）的表名称（变量actual_table）
* 5. 两个表比较的key（变量cmp_key）
*
* 使用限制：
* 1. 仅能用于mysql数据库
* 2. 两个表可以在mysql的不同库中，但必须同时是mysql表
* 3. 不支持联合主键，联合主键只能选其中一个主键比较
*
* 程序流程说明：https://github.com/xijianhe-edp/sql_script
*/


SET @expected_schema = 'exp_db';	#原表schema名称
SET @actual_schema = 'act_db';		#新表schema名称
SET @expected_table = 'exp';		#原表表名称
SET @actual_table = 'act';			#新表表名称
SET @cmp_key = 'uid';				#表key字段

##---------------以上参数需要设置-----------------------


#获取去重后的e表
DROP VIEW IF EXISTS exp_nodup;
SET @str = '';
SET @str = CONCAT('CREATE VIEW exp_nodup AS SELECT DISTINCT * FROM ',
	@expected_schema, '.' ,@expected_table);
PREPARE cmd FROM @str;
EXECUTE cmd;

#获取去重后的a表
DROP VIEW IF EXISTS act_nodup;
SET @str = '';
SET @str = CONCAT('CREATE VIEW act_nodup AS SELECT DISTINCT * FROM ',
	@actual_schema, '.', @actual_table);
PREPARE cmd FROM @str;
EXECUTE cmd;

#获取表的列数
SET @str = 'SET @col_num = (SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS
	WHERE TABLE_SCHEMA = \'';
SET @str = CONCAT(@str, @expected_schema, '\' AND TABLE_NAME = \'',@expected_table, '\');');
PREPARE cmd FROM @str;
EXECUTE cmd;

DROP PROCEDURE IF EXISTS gen_equal_exp;
DELIMITER //
CREATE PROCEDURE gen_equal_exp()
BEGIN
    DECLARE exp_row_num INT(11);
    DECLARE total_row_num INT(11);
    DECLARE cnt INT(11);
    
	SET @equal_str_ab = '';
    SET @equal_str_ac = '';
    SET @dup_str = '';
    SET @group_concat_str1 = '';
    SET @group_concat_str2 = '';
    
	SET @cnt = 1;
    SET @str = 'SET @col_name = (SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS 
		WHERE TABLE_SCHEMA = \'';
    SET @str = CONCAT(@str, @expected_schema, '\' AND TABLE_NAME = \'',
			@expected_table, '\' AND CAST(ordinal_position AS SIGNED) = @cnt);');
	PREPARE cmd  FROM @str;

    WHILE @cnt < @col_num + 1 DO
        EXECUTE cmd;            
		SET @equal_str_ab = CONCAT(@equal_str_ab, 'a.', @col_name,
			'=b.', @col_name, ' AND ');
        SET @equal_str_ac = CONCAT(@equal_str_ac, 'a.', @col_name,
			'=c.', @col_name, ' AND ');
        SET @dup_str = CONCAT(@dup_str, @col_name, ',');
		IF @col_name = @cmp_key THEN
			SET @group_concat_str1 = CONCAT(@group_concat_str1, @cmp_key, ',');
            SET @group_concat_str2 = CONCAT(@group_concat_str2, @cmp_key, ',');
		ELSE 
			SET @group_concat_str1 = CONCAT(@group_concat_str1, 'GROUP_CONCAT(', @col_name,
				' ORDER BY ', @cmp_key, ' SEPARATOR \' | \') AS ', @col_name, ',');
			SET @group_concat_str2 = CONCAT(@group_concat_str2, 'GROUP_CONCAT(', @col_name,
				' ORDER BY table_source DESC SEPARATOR \' --> \') AS ', @col_name, ',');                
		END IF;
		SET @cnt = @cnt + 1;
	END WHILE;
    
    SET @equal_str_ab = LEFT(@equal_str_ab, LENGTH(@equal_str_ab)-4);	#去掉最后的AND和空格
    SET @equal_str_ac = LEFT(@equal_str_ac, LENGTH(@equal_str_ac)-4);	#去掉最后的AND和空格
    SET @dup_str = LEFT(@dup_str, LENGTH(@dup_str)-1);					#去掉最后的','
    SET @group_concat_str1 = LEFT(@group_concat_str1, LENGTH(@group_concat_str1)-1);#去掉最后的','
    SET @group_concat_str2 = LEFT(@group_concat_str2, LENGTH(@group_concat_str2)-1);#去掉最后的','

	SET @expected_table = CONCAT(@expected_schema, '.', @expected_table);
	SET @actual_table = CONCAT(@actual_schema, '.', @actual_table);
	
    
    /*#获取表e的重复行
    SET @exp_dup_str = CONCAT('SELECT "duplicate",COUNT(*) AS dup_num,', @expected_table,
		'.* FROM ',	@expected_table, ' GROUP BY ', @dup_str, ' HAVING COUNT(*) > 1;');
	PREPARE cmd FROM @exp_dup_str;
	EXECUTE cmd;*/
    
    #获取原表的重复记录
    DROP VIEW IF EXISTS exp_dup_d;
    SET @str = '';
    SET @str = CONCAT('CREATE VIEW exp_dup_d AS SELECT * FROM ', @expected_table, 
		' GROUP BY ', @dup_str, ' HAVING COUNT(*) > 1;');        
	PREPARE cmd FROM @str;
	EXECUTE cmd;
    
    DROP VIEW IF EXISTS exp_dup;
    SET @str = '';
    SET @str = CONCAT('CREATE VIEW exp_dup AS SELECT * FROM ', @expected_table,
		' a WHERE EXISTS (SELECT * FROM exp_dup_d b WHERE ', @equal_str_ab, ')');
	PREPARE cmd FROM @str;
	EXECUTE cmd;
    
    #获取新表的重复记录
	DROP VIEW IF EXISTS act_dup_d;
    SET @str = '';
    SET @str = CONCAT('CREATE VIEW act_dup_d AS SELECT * FROM ', @actual_table, 
		' GROUP BY ', @dup_str, ' HAVING COUNT(*) > 1;');        
	PREPARE cmd FROM @str;
	EXECUTE cmd;
    
    DROP VIEW IF EXISTS act_dup;
    SET @str = '';
    SET @str = CONCAT('CREATE VIEW act_dup AS SELECT * FROM ', @actual_table,
		' a WHERE EXISTS (SELECT * FROM act_dup_d b WHERE ', @equal_str_ab, ')');
	PREPARE cmd FROM @str;
	EXECUTE cmd;
    
    
    #获取missing记录
    DROP VIEW IF EXISTS missing;    
    SET @str = 'CREATE VIEW missing AS SELECT * FROM exp_nodup WHERE exp_nodup.';
    SET @str = CONCAT(@str, @cmp_key, ' NOT IN (SELECT act_nodup.', 
		@cmp_key, ' FROM act_nodup);');
    PREPARE cmd FROM @str;
	EXECUTE cmd;
	
    
	#获取unexpected记录
    DROP VIEW IF EXISTS unexpected;
	SET @str = 'CREATE VIEW unexpected AS SELECT * FROM act_nodup WHERE act_nodup.';
    SET @str = CONCAT(@str, @cmp_key, ' NOT IN (SELECT exp_nodup.', 
		@cmp_key, ' FROM exp_nodup);');
    PREPARE cmd FROM @str;
	EXECUTE cmd;
	
    
    #获取原表中的不匹配记录exp_mismatch
	DROP VIEW IF EXISTS exp_mismatch;
    SET @str = 'CREATE VIEW exp_mismatch AS SELECT * FROM exp_nodup a
		WHERE NOT EXISTS ((SELECT * FROM act_nodup b WHERE ';
	SET @str = CONCAT(@str, @equal_str_ab, ') UNION ALL (SELECT * FROM missing c WHERE ',
		@equal_str_ac, '));');
    PREPARE cmd FROM @str;
	EXECUTE cmd;
    
    #获取原表中的不匹配记录act_mismatch
    DROP VIEW IF EXISTS act_mismatch;
    SET @str = 'CREATE VIEW act_mismatch AS SELECT * FROM act_nodup a
		WHERE NOT EXISTS ((SELECT * FROM exp_nodup b WHERE ';
	SET @str = CONCAT(@str, @equal_str_ab, ') UNION ALL (SELECT * FROM unexpected c WHERE ',
		@equal_str_ac, '));');
    PREPARE cmd FROM @str;
	EXECUTE cmd;
    
    #生成临时表，并插入列table_source，作为GROUP_CONCAT合并的条件
    DROP TABLE IF EXISTS tmp_exp_unmatched;
    CREATE TEMPORARY TABLE tmp_exp_unmatched SELECT * FROM exp_mismatch;
    ALTER TABLE tmp_exp_unmatched ADD COLUMN table_source VARCHAR(20) FIRST;
    SET SQL_SAFE_UPDATES=0;
    UPDATE tmp_exp_unmatched SET table_source = 'expected';
    
    #生成临时表，并插入列table_source，作为GROUP_CONCAT合并的条件
	DROP TABLE IF EXISTS tmp_act_unmatched;
    CREATE TEMPORARY TABLE tmp_act_unmatched SELECT * FROM act_mismatch;
    ALTER TABLE tmp_act_unmatched ADD COLUMN table_source VARCHAR(20) FIRST;
    UPDATE tmp_act_unmatched SET table_source = 'actual';
	SET SQL_SAFE_UPDATES=1;
    
    #将两个临时表合并
    DROP TABLE IF EXISTS mismatch;
    CREATE TEMPORARY TABLE mismatch SELECT * FROM tmp_exp_unmatched 
		UNION ALL SELECT * FROM tmp_act_unmatched;
	DROP TABLE tmp_exp_unmatched;
    DROP TABLE tmp_act_unmatched;
    
    #两次GROUP_CONCAT，并生成最终的mismatch表
    DROP TABLE IF EXISTS mismatch_tmp;
    SET @str = 'CREATE TEMPORARY TABLE  mismatch_tmp SELECT table_source,';
    SET @str = CONCAT(@str, @group_concat_str1, ' FROM mismatch GROUP BY ', 
		@cmp_key, ',table_source');
	PREPARE cmd FROM @str;
    EXECUTE cmd;
    DROP TABLE mismatch;
    SET @str = 'CREATE TEMPORARY TABLE  mismatch SELECT ';
    SET @str = CONCAT(@str, @group_concat_str2, ' FROM mismatch_tmp GROUP BY ', @cmp_key);
	PREPARE cmd FROM @str;
    EXECUTE cmd;
    DROP TABLE mismatch_tmp;

	SELECT 'exp_duplicate',exp_dup.* FROM exp_dup;
    SELECT 'act_duplicate',act_dup.* FROM act_dup;
    SELECT 'missing', missing.* FROM missing;
    SELECT 'unexpected', unexpected.* FROM unexpected;
    SELECT 'mismatch', mismatch.* FROM mismatch;


	DROP VIEW exp_mismatch;
	DROP VIEW act_mismatch;
	DROP VIEW missing;
	DROP VIEW unexpected;
	DROP VIEW exp_nodup;
	DROP VIEW act_nodup;
	DROP VIEW exp_dup;
	DROP VIEW act_dup;
	DROP VIEW exp_dup_d;
	DROP VIEW act_dup_d;
	
    
END;//

CALL gen_equal_exp();//
DROP PROCEDURE gen_equal_exp;





