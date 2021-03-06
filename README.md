# sql_script
#说明文档
====================================
###脚本用途
数据库表在导入导出过程中数据表可能会出现记录丢失、出现未知记录或字段错误等情况，此脚本用于比较两个表的差异

###输入
两张mysql或oracle表,原表（期望生成的表expected_table）和新表（实际生成的表actual_table）

###输出
视图或临时表，包括：
* 1. 视图exp_dup：原表（期望生成的表expected_table）中重复的记录，即原表中所有字段均相同的记录（全字段比较）;
* 2. 视图act_dup：新表（实际生成的表actual_table）中重复的记录，即新表中所有字段均相同的记录（全字段比较）;
* 3. 视图missing：原表中有但新表中没有的记录（仅比较key）
* 4. 视图unexpected：原表中没有但新表中有的记录（仅比较key）
* 5. 临时表mismatch：原表和新表都有相同的key，但对应的字段不同。表除key外，其它字段为: 原表v1 | v2 -->    新表v3，即原表中某个key对应的两个或多个记录在该字段的值v1，v2在新表中对应字段为v3


###需要配置的参数
mysql和oracle版本有所不同，详见脚本内部说明


###使用限制
* 1. mysql版本仅能用于mysql数据库，oracle版本仅能用于oracle数据库
* 2. mysql版本两个表可以在mysql的不同库中，但必须同时是mysql表；oracle版本两个表可以在oracle的不同库中，但必须同时是oracle表
* 3. 不支持联合主键，联合主键只能选其中一个主键比较

###程序流程


![github](https://github.com/xijianhe-edp/sql_script/blob/master/processing_chart.jpg "github") 
