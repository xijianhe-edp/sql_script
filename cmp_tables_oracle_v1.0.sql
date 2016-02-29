/*
* version��cmp_tables_oracle v1.0
* author�� xijianhe@creditease.cn
* 
* ��;��
* ��oracle���ڵ��뵼�����������ݱ���ܻ���ּ�¼��ʧ������δ֪��¼���ֶδ����������˽ű����ڱȽ�����oracle��Ĳ���
* 
* ���룺����mysql��,ԭ���������ɵı�expected_table�����±�ʵ�����ɵı�actual_table��
* 1. ԭ��Ϊ�����������ɵı�expected_table��
* 2. �±�Ϊʵ�����ɵı�(actual_table)
*
* �������ͼ����ʱ������
* 1. ��ͼexp_dup��ԭ���������ɵı�expected_table�����ظ��ļ�¼����ԭ���������ֶξ���ͬ�ļ�¼��ȫ�ֶαȽϣ�;
* 2. ��ͼact_dup���±�ʵ�����ɵı�actual_table�����ظ��ļ�¼�����±��������ֶξ���ͬ�ļ�¼��ȫ�ֶαȽϣ�;
* 3. ��ͼmissing��ԭ�����е��±���û�еļ�¼�����Ƚ�key��
* 4. ��ͼunexpected��ԭ����û�е��±����еļ�¼�����Ƚ�key��
* 5. ��ʱ��mismatch��ԭ����±�����ͬ��key������Ӧ���ֶβ�ͬ�����key�⣬�ֶ�Ϊ: ԭ��v1 | v2 --> �±�v3 
*         ��ԭ����ĳ��key��Ӧ������������¼�ڸ��ֶε�ֵv1��v2���±��ж�Ӧ�ֶ�Ϊv3
* 
* ��Ҫ���õĲ�����
* 1. database link���ͣ�����dblink_type����0��ʾԭ����±���ͬһ�����У� 
*     1��ʾԭ�����ڿ����ӵ��±����ڿ⣬ 2��ʾ�±����ڿ����ӵ�ԭ�����ڿ⣬
*     oracle��database link�ǵ���ģ���ע�����ӷ���.
* 2. database link���ƣ�����dblink_name��
* 3. ��ǰ�û����û���
* 4. ԭ���������ɵı�expected_table���ı����ƣ�����expected_table��
* 5. �±�ʵ�����ɵı�actual_table���ı����ƣ�����actual_table��
* 6. ������Ƚϵ�key������cmp_key��
*
* ʹ�����ƣ�
* 1. ��������oracle���ݿ�
* 2. �����������oracle�Ĳ�ͬ���У�������ͬʱ��oracle��
* 3. ��֧��������������������ֻ��ѡ����һ�������Ƚ�
*
* ��������˵����https://github.com/xijianhe-edp/sql_script
*/




DECLARE
 
  -- 0��ʾԭ����±���ͬһ�����У� 1��ʾԭ�����ڿ����ӵ��±����ڿ⣬ 2��ʾ�±����ڿ����ӵ�ԭ�����ڿ�
  dblink_type INT := 1; 

  -- database link����
  dblink_name VARCHAR(30) := 'mylink';  
  
  -- ��ǰ�û���
  username VARCHAR(30) := 'sys';
  
  -- ԭ����±�ı�����
  expected_table VARCHAR(30) := 'u_exp';
  actual_table VARCHAR(30) := 'bu_act';
  
  -- �����key
  cmp_key VARCHAR(30) := 'tid';
  
  -- ���ϲ�����Ҫ����
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
  
  --��ȡȥ�غ��e��
  EXECUTE IMMEDIATE 'CREATE OR REPLACE VIEW exp_nodup AS SELECT DISTINCT * FROM ' || expected_table || dbl_exp;
  --��ȡȥ�غ��a��
  EXECUTE IMMEDIATE 'CREATE OR REPLACE VIEW act_nodup AS SELECT DISTINCT * FROM ' || actual_table || dbl_act;
  --��ȡ�������
  EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM USER_TAB_COLUMNS' || dbl_exp || ' WHERE TABLE_NAME = '|| CHR(39)
      || UPPER(expected_table) || CHR(39) INTO col_num;
      
  cnt := 1;
  WHILE cnt < col_num + 1 LOOP
    --��ȡ�������
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
      
  dup_str := SUBSTR(dup_str, 1, LENGTH(dup_str) - 1);       --ȥ������','
  equal_str := SUBSTR(equal_str, 1, LENGTH(equal_str) - 4); --ȥ������AND�Ϳո�
  group_concat_str1 := SUBSTR(group_concat_str1, 1, LENGTH(group_concat_str1) - 1);--ȥ������','
  group_concat_str2 := SUBSTR(group_concat_str2, 1, LENGTH(group_concat_str2) - 1);--ȥ������','    
      
  --��ȡԭ����ظ���  
  EXECUTE IMMEDIATE 'CREATE OR REPLACE VIEW exp_dup_d AS SELECT * FROM ' || expected_table
      || dbl_exp || ' GROUP BY ' || dup_str || ' HAVING COUNT(*) > 1';
  EXECUTE IMMEDIATE'CREATE OR REPLACE VIEW exp_dup AS SELECT * FROM ' || expected_table 
      || dbl_exp || ' a WHERE EXISTS (SELECT * FROM exp_dup_d b WHERE ' || equal_str || ')';
  --DROP VIEW exp_dup_d;
  
  --��ȡ�±���ظ���
  EXECUTE IMMEDIATE 'CREATE OR REPLACE VIEW act_dup_d AS SELECT * FROM ' || actual_table
      || dbl_act || ' GROUP BY ' || dup_str || ' HAVING COUNT(*) > 1';
  EXECUTE IMMEDIATE'CREATE OR REPLACE VIEW act_dup AS SELECT * FROM ' || actual_table 
      || dbl_act || ' a WHERE EXISTS (SELECT * FROM act_dup_d b WHERE ' || equal_str || ')';

  --��ȡ������ͬ�ļ�¼
  EXECUTE IMMEDIATE 'CREATE OR REPLACE VIEW matched_ AS SELECT a.* FROM exp_nodup a,
       act_nodup b WHERE ' || equal_str;
       
  --��ȡ����ƥ��ļ�¼������misming,unexpected,mismatch
  EXECUTE IMMEDIATE 'CREATE OR REPLACE VIEW exp_unmatched AS
       (SELECT * FROM exp_nodup MINUS SELECT * FROM matched_)';      
  EXECUTE IMMEDIATE 'CREATE OR REPLACE VIEW act_unmatched AS
       (SELECT * FROM act_nodup MINUS SELECT * FROM matched_)';      
     
  --��ȡ�±�ʧ�ļ�¼missing��
  EXECUTE IMMEDIATE 'CREATE OR REPLACE VIEW missing AS SELECT exp_unmatched.* FROM 
       exp_unmatched WHERE exp_unmatched.' || cmp_key ||
       ' NOT IN (SELECT act_unmatched.' || cmp_key || ' FROM act_unmatched)';
  --��ȡ�±�δ֪�ļ�¼unexpected��
  EXECUTE IMMEDIATE 'CREATE OR REPLACE VIEW unexpected AS SELECT act_unmatched.* FROM 
       act_unmatched WHERE act_unmatched.' || cmp_key ||
       ' NOT IN (SELECT exp_unmatched.' || cmp_key || ' FROM exp_unmatched)';
  
  --��ȡe���е�mismatch����
  EXECUTE IMMEDIATE 'CREATE OR REPLACE VIEW exp_mismatch AS SELECT * FROM 
       (SELECT * FROM exp_unmatched MINUS SELECT * FROM missing)';
  --��ȡa���е�mismatch����
  EXECUTE IMMEDIATE 'CREATE OR REPLACE VIEW act_mismatch AS SELECT * FROM 
       (SELECT * FROM act_unmatched MINUS SELECT * FROM unexpected)';  
  
  --�ж���ʱ��tmp_mismatch�Ƿ���ڣ�������drop     
  cnt := 0;
  EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM ALL_TABLES  WHERE TABLE_NAME = ' || CHR(39) || 'TMP_MISMATCH' 
      || CHR(39) || ' AND OWNER = '|| CHR(39) || UPPER(username) || CHR(39) INTO cnt;
  IF cnt = 1 THEN  --��ʱ���Ѵ���
    EXECUTE IMMEDIATE 'TRUNCATE TABLE tmp_mismatch';
    EXECUTE IMMEDIATE 'DROP TABLE tmp_mismatch';
  END IF;     
  
  --������ʱ��tmp_mismatch�������tab_source�У���ʾ��¼������ԭ�����±������ֶΰ��кϲ�
  EXECUTE IMMEDIATE 'CREATE GLOBAL TEMPORARY TABLE tmp_mismatch ON COMMIT PRESERVE ROWS AS (
     SELECT ' || CHR(39) || 'exp_table' || CHR(39) || ' AS tab_source, exp_mismatch.* FROM exp_mismatch 
     UNION      
     SELECT ' || CHR(39) || 'act_table' || CHR(39) || ' AS tab_source, act_mismatch.* FROM act_mismatch)';   
     
  --�ж���ʱ��mismatch�Ƿ���ڣ�������drop
  cnt := 0;
  EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM ALL_TABLES  WHERE TABLE_NAME = ' || CHR(39) || 'MISMATCH' 
      || CHR(39) || ' AND OWNER = '|| CHR(39) || UPPER(username) || CHR(39) INTO cnt;
  IF cnt = 1 THEN  --��ʱ���Ѵ���
    EXECUTE IMMEDIATE 'TRUNCATE TABLE mismatch';
    EXECUTE IMMEDIATE 'DROP TABLE mismatch';
  END IF;   
     
  --����mismatch�����  
  EXECUTE IMMEDIATE 'CREATE GLOBAL TEMPORARY TABLE mismatch ON COMMIT PRESERVE ROWS AS ( SELECT ' 
      || group_concat_str2 || ' FROM (' || 'SELECT tab_source,' || group_concat_str1 
      || ' FROM tmp_mismatch GROUP BY ' || cmp_key || ',tab_source) GROUP BY ' || cmp_key || ')'; 
  
  
END; 
/


SELECT * FROM exp_dup;       --ԭ���ظ���¼
SELECT * FROM act_dup;       --�±��ظ���¼
SELECT * FROM missing;       --missing�ļ�¼
SELECT * FROM unexpected;    --unexpected�ļ�¼
SELECT * FROM mismatch;      --��ƥ��ļ�¼




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
