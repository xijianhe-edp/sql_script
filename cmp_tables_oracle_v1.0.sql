/*
* version：cmp_tables_oracle v1.0
* author： xijianhe@creditease.cn
* 
* 用途：
* 在oracle表在导入导出过程中数据表可能会出现记录丢失、出现未知记录或字段错误等情况，此脚本用于比较两个oracle表的差异
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
* 1. database link类型（变量dblink_type）：0表示原表和新表在同一个库中， 
*     1表示原表所在库连接到新表所在库， 2表示新表所在库连接到原表所在库，
*     oracle的database link是单向的，请注意连接方向.
* 2. database link名称（变量dblink_name）
* 3. 当前用户的用户名
* 4. 原表（期望生成的表expected_table）的表名称（变量expected_table）
* 5. 新表（实际生成的表actual_table）的表名称（变量actual_table）
* 6. 两个表比较的key（变量cmp_key）
*
* 使用限制：
* 1. 仅能用于oracle数据库
* 2. 两个表可以在oracle的不同库中，但必须同时是oracle表
* 3. 不支持联合主键，联合主键只能选其中一个主键比较
*
* 程序流程说明：https://github.com/xijianhe-edp/sql_script
*/




DECLARE
 
  -- 0表示原表和新表在同一个库中， 1表示原表所在库连接到新表所在库， 2表示新表所在库连接到原表所在库
  dblink_type INT := 1; 

  -- database link名称
  dblink_name VARCHAR(30) := 'mylink';  
  
  -- 当前用户名
  username VARCHAR(30) := 'sys';
  
  -- 原表和新表的表名称
  expected_table VARCHAR(30) := 'u_exp';
  actual_table VARCHAR(30) := 'bu_act';
  
  -- 两表的key
  cmp_key VARCHAR(30) := 'tid';
  
  -- 以上参数需要设置
  -----------------------------------------------------------------------------
  
  dbl_exp VARCHAR(31) := '';
  dbl_act VARCHAR(31) := '';
  
  str VARCHAR(30000) := 'hello';
  col_num INT;
  cnt INT;
  col_name VARCHAR(30);
  
  dup_str VARCHAR(3500);
  equal_str VARCHAR(3500);
  group_concat_str1 VARCHAR(3500);
  group_concat_str2 VARCHAR(3500);
  
BEGIN

  IF dblink_type = 1 THEN
    dbl_act := '@' || dblink_name;
  ELSE IF dblink_type = 2 THEN
         dbl_exp := '@' || dblink_name;                    
       END IF;
  END IF;
  
  --获取去重后的e表
  EXECUTE IMMEDIATE 'CREATE OR REPLACE VIEW exp_nodup AS SELECT DISTINCT * FROM ' || expected_table || dbl_exp;
  --获取去重后的a表
  EXECUTE IMMEDIATE 'CREATE OR REPLACE VIEW act_nodup AS SELECT DISTINCT * FROM ' || actual_table || dbl_act;
  --获取表的列数
  EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM USER_TAB_COLUMNS' || dbl_exp || ' WHERE TABLE_NAME = '|| CHR(39)
      || UPPER(expected_table) || CHR(39) INTO col_num;
      
  cnt := 1;
  WHILE cnt < col_num + 1 LOOP
    --获取表的列名
    EXECUTE IMMEDIATE 'SELECT COLUMN_NAME FROM USER_TAB_COLUMNS' || dbl_exp || ' WHERE TABLE_NAME = ' 
        || CHR(39) || UPPER(expected_table) || CHR(39) || ' AND COLUMN_ID = ' || cnt INTO col_name;
    dup_str := dup_str || col_name || ',';
    equal_str := equal_str || 'a.' || col_name || '=b.' || col_name || ' AND ';
    IF col_name = UPPER(cmp_key) THEN
      group_concat_str1 := group_concat_str1 || cmp_key || ',';
      group_concat_str2 := group_concat_str2 || cmp_key || ',';
    ELSE
      group_concat_str1 := group_concat_str1 || 'LISTAGG(' || col_name || ',' || CHR(39) ||' | ' 
          || CHR(39) || ') within GROUP (ORDER BY ' || cmp_key || ') AS ' || col_name || ',';          
      group_concat_str2 := group_concat_str2 || 'LISTAGG(' || col_name || ',' || CHR(39) ||' --> ' 
          || CHR(39) || ') within GROUP (ORDER BY tab_source DESC) AS ' || col_name || ',';
    END IF;
    cnt := cnt + 1;
  END LOOP;     
      
  dup_str := SUBSTR(dup_str, 1, LENGTH(dup_str) - 1);       --去掉最后的','
  equal_str := SUBSTR(equal_str, 1, LENGTH(equal_str) - 4); --去掉最后的AND和空格
  group_concat_str1 := SUBSTR(group_concat_str1, 1, LENGTH(group_concat_str1) - 1);--去掉最后的','
  group_concat_str2 := SUBSTR(group_concat_str2, 1, LENGTH(group_concat_str2) - 1);--去掉最后的','    
      
  --获取原表表重复行  
  EXECUTE IMMEDIATE 'CREATE OR REPLACE VIEW exp_dup_d AS SELECT * FROM ' || expected_table
      || dbl_exp || ' GROUP BY ' || dup_str || ' HAVING COUNT(*) > 1';
  EXECUTE IMMEDIATE'CREATE OR REPLACE VIEW exp_dup AS SELECT * FROM ' || expected_table 
      || dbl_exp || ' a WHERE EXISTS (SELECT * FROM exp_dup_d b WHERE ' || equal_str || ')';
  --DROP VIEW exp_dup_d;
  
  --获取新表表重复行
  EXECUTE IMMEDIATE 'CREATE OR REPLACE VIEW act_dup_d AS SELECT * FROM ' || actual_table
      || dbl_act || ' GROUP BY ' || dup_str || ' HAVING COUNT(*) > 1';
  EXECUTE IMMEDIATE'CREATE OR REPLACE VIEW act_dup AS SELECT * FROM ' || actual_table 
      || dbl_act || ' a WHERE EXISTS (SELECT * FROM act_dup_d b WHERE ' || equal_str || ')';

  --获取两表相同的记录
  EXECUTE IMMEDIATE 'CREATE OR REPLACE VIEW matched_ AS SELECT a.* FROM exp_nodup a,
       act_nodup b WHERE ' || equal_str;
       
  --获取两表不匹配的记录，包括misming,unexpected,mismatch
  EXECUTE IMMEDIATE 'CREATE OR REPLACE VIEW exp_unmatched AS
       (SELECT * FROM exp_nodup MINUS SELECT * FROM matched_)';      
  EXECUTE IMMEDIATE 'CREATE OR REPLACE VIEW act_unmatched AS
       (SELECT * FROM act_nodup MINUS SELECT * FROM matched_)';      
     
  --获取新表丢失的记录missing表
  EXECUTE IMMEDIATE 'CREATE OR REPLACE VIEW missing AS SELECT exp_unmatched.* FROM 
       exp_unmatched WHERE exp_unmatched.' || cmp_key ||
       ' NOT IN (SELECT act_unmatched.' || cmp_key || ' FROM act_unmatched)';
  --获取新表未知的记录unexpected表
  EXECUTE IMMEDIATE 'CREATE OR REPLACE VIEW unexpected AS SELECT act_unmatched.* FROM 
       act_unmatched WHERE act_unmatched.' || cmp_key ||
       ' NOT IN (SELECT exp_unmatched.' || cmp_key || ' FROM exp_unmatched)';
  
  --获取e表中的mismatch部分
  EXECUTE IMMEDIATE 'CREATE OR REPLACE VIEW exp_mismatch AS SELECT * FROM 
       (SELECT * FROM exp_unmatched MINUS SELECT * FROM missing)';
  --获取a表中的mismatch部分
  EXECUTE IMMEDIATE 'CREATE OR REPLACE VIEW act_mismatch AS SELECT * FROM 
       (SELECT * FROM act_unmatched MINUS SELECT * FROM unexpected)';  
  
  --判断临时表tmp_mismatch是否存在，存在则drop     
  cnt := 0;
  EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM ALL_TABLES  WHERE TABLE_NAME = ' || CHR(39) || 'TMP_MISMATCH' 
      || CHR(39) || ' AND OWNER = '|| CHR(39) || UPPER(username) || CHR(39) INTO cnt;
  IF cnt = 1 THEN  --临时表已存在
    EXECUTE IMMEDIATE 'TRUNCATE TABLE tmp_mismatch';
    EXECUTE IMMEDIATE 'DROP TABLE tmp_mismatch';
  END IF;     
  
  --生成临时表tmp_mismatch，并添加tab_source列，表示记录是来自原表还是新表，用于字段按行合并
  EXECUTE IMMEDIATE 'CREATE GLOBAL TEMPORARY TABLE tmp_mismatch ON COMMIT PRESERVE ROWS AS (
     SELECT ' || CHR(39) || 'exp_table' || CHR(39) || ' AS tab_source, exp_mismatch.* FROM exp_mismatch 
     UNION      
     SELECT ' || CHR(39) || 'act_table' || CHR(39) || ' AS tab_source, act_mismatch.* FROM act_mismatch)';   
     
  --判断临时表mismatch是否存在，存在则drop
  cnt := 0;
  EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM ALL_TABLES  WHERE TABLE_NAME = ' || CHR(39) || 'MISMATCH' 
      || CHR(39) || ' AND OWNER = '|| CHR(39) || UPPER(username) || CHR(39) INTO cnt;
  IF cnt = 1 THEN  --临时表已存在
    EXECUTE IMMEDIATE 'TRUNCATE TABLE mismatch';
    EXECUTE IMMEDIATE 'DROP TABLE mismatch';
  END IF;   
     
  --生成mismatch结果表  
  EXECUTE IMMEDIATE 'CREATE GLOBAL TEMPORARY TABLE mismatch ON COMMIT PRESERVE ROWS AS ( SELECT ' 
      || group_concat_str2 || ' FROM (' || 'SELECT tab_source,' || group_concat_str1 
      || ' FROM tmp_mismatch GROUP BY ' || cmp_key || ',tab_source) GROUP BY ' || cmp_key || ')'; 
  
  
END; 
/


SELECT * FROM exp_dup;       --原表重复记录
SELECT * FROM act_dup;       --新表重复记录
SELECT * FROM missing;       --missing的记录
SELECT * FROM unexpected;    --unexpected的记录
SELECT * FROM mismatch;      --不匹配的记录




/*DROP VIEW act_mismatch;
DROP VIEW exp_mismatch;
DROP VIEW missing;
DROP VIEW unexpected;
DROP VIEW exp_nodup;
DROP VIEW act_nodup;
DROP VIEW exp_dup;
DROP VIEW act_dup;
DROP VIEW exp_dup_d;
DROP VIEW act_dup_d;*/
