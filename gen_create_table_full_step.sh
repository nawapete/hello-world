#!/bin/ksh

########################################################################################
#  
#  Program Name     : gen_create_table_full_step.sh
#  Description      : 1.Gen Script Create Temp Table
#                     2.Gen Script Create Staging Table
#                     3.Gen Script Create DIM Table
#                     4.Create Temp Table
#                     5.Create Staging Table
#                     6.Gen Script DDL Sql Loader
#                     7.Insert into DSL_EXTRACT_SYSTEM_FILE
#                     8.Gen Script Rule Base
#  Created by       : Aekavut V.
#  Create Date      : 20/08/2019
#   
########################################################################################
#

. /data1/misapps/dsl_dev/script/MISBI.cfg

if [ "$1" != "" ] && [ "$2" != "" ]
then
	LOGTIME=`date +'%Y%m%d_%H%M%S'`
	echo '-----------------------------------------------------------------------------------'
	echo '-----------------------------------------------------------------------------------' > $p_main$p_log/gen_create_table_full_step_${LOGTIME}.log
	echo 'Start Time : '`date +'%Y-%m-%d %H:%M:%S'`
	echo 'Start Time : '`date +'%Y-%m-%d %H:%M:%S'` >> $p_main$p_log/gen_create_table_full_step_${LOGTIME}.log	
	echo '-----------------------------------------------------------------------------------'
	echo '-----------------------------------------------------------------------------------' >> $p_main$p_log/gen_create_table_full_step_${LOGTIME}.log
	echo 'Log File : '$p_main$p_log/gen_create_table_full_step_${LOGTIME}.log
	echo 'Log File : '$p_main$p_log/gen_create_table_full_step_${LOGTIME}.log >> $p_main$p_log/gen_create_table_full_step_${LOGTIME}.log
	echo '-----------------------------------------------------------------------------------'
	echo '-----------------------------------------------------------------------------------' >> $p_main$p_log/gen_create_table_full_step_${LOGTIME}.log


	typeset -u OWNER=${1}
	typeset -l RUNOWNER=${1}
	typeset -u TARGETOWNER=${2}
	typeset -l RUNTARGETOWNER=${2}
	TABLENAME_PATH=$p_main$p_cfg/
	TABLENAME_FILE=${RUNOWNER}_table_name_v2.txt
	CREATETABLE_PATH=$p_main$p_tmp/create_table/
	SQLLDR_PATH=$p_main$p_tmp/sqlldr_script/
	RULEBASE_PATH=$p_main$p_tmp/rulebase/

	while read line
	do
		TABLE_NAME=$(echo ${line} | awk -F'|' '{print $1}')
		TEMP_TABLE_NAME=$(echo ${line} | awk -F'|' '{print $2}')
		STG_TABLE_NAME=$(echo ${line} | awk -F'|' '{print $3}')
		DIM_TABLE_NAME=$(echo ${line} | awk -F'|' '{print $4}')
		
##### GEN CREATE TEMP TABLE #####
		cat << EOF > ${CREATETABLE_PATH}gen_create_table_${TEMP_TABLE_NAME}.sql
set feedback off
set trimspool on
set heading off
set echo off
set linesize 4000
set pages

spool ${CREATETABLE_PATH}create_table_${TEMP_TABLE_NAME}.sql

select case when rowno = (select count(*)+2 from all_tab_columns where owner = '${OWNER}' and table_name = '${TABLE_NAME}') then rtrim(text,',') else text end
from (select 'create table ${TEMP_TABLE_NAME}' text,1 rowno from dual
union
select '(' text,2 rowno from dual
union
select ');' text,999999 rowno from dual
union
select text,rownum+2 rowno
from (select case when column_name = 'UID' then '"'||column_name||'"' else column_name end ||
case when data_type = 'NUMBER' and data_length is not null and data_precision is null and data_scale is not null then ' INTEGER'
else ' '||data_type end||
case when data_type in ('CHAR','VARCHAR2') then '('||data_length||')'
when data_type = 'DATE' then ''
when data_type = 'CLOB' then ''
when data_type = 'BLOB' then ''
when data_type = 'LONG' then ''
when substr(data_type,1,9) = 'TIMESTAMP' then ''
when data_type = 'NVARCHAR2' then '('||data_length/2||')'
when data_type = 'NUMBER' and data_length is not null and data_precision is null and data_scale is not null then ''
when data_type = 'NUMBER' and data_length is not null and data_precision is null and data_scale is not null then ''
when data_type = 'NUMBER' and data_length is not null and data_precision is not null and data_scale is not null then '('||data_precision||','||data_scale||')' end||
case when data_default is not null then ' default '||replace(replace(trim(get_long_test('${OWNER}','${TABLE_NAME}',column_id)),chr(10),''),chr(13),'') else '' end||
case when nullable = 'N' then ' not null,' else ',' end text
from all_tab_columns 
where owner = '${OWNER}' 
and table_name = '${TABLE_NAME}' 
order by column_id))
order by rowno;

select distinct 'comment on table ${TEMP_TABLE_NAME} is '''||replace(b.comments,'''','''''')||''';' 
from all_tab_columns a
inner join ALL_TAB_COMMENTS b
on a.owner = b.owner
and a.table_name = b.table_name 
where b.owner = '${OWNER}' 
and b.table_name = '${TABLE_NAME}'
and b.comments is not null;

select comments
from (select distinct a.column_id,'comment on column ${TEMP_TABLE_NAME}.'||b.column_name||' is '''||replace(b.comments,'''','''''')||''';' comments
from all_tab_columns a
inner join ALL_COL_COMMENTS b
on a.owner = b.owner
and a.table_name = b.table_name
and a.column_name = b.column_name
where b.owner = '${OWNER}'
and b.table_name = '${TABLE_NAME}'
and b.comments is not null
order by a.column_id);

select case when logging = 'YES' then 'create index '||index_name||' on ${TEMP_TABLE_NAME} '||column_name||';'
else 'create index '||index_name||' on ${TEMP_TABLE_NAME} '||column_name||' nologging;' end
from (select case when length(replace(index_name,index_name,'IDX_${TEMP_TABLE_NAME}')||'_'||rownum) > 30 then
substr(replace(index_name,index_name,'IDX_${TEMP_TABLE_NAME}'),1,26)||'_'||rownum
else replace(index_name,index_name,'IDX_${TEMP_TABLE_NAME}')||'_'||rownum end index_name
,logging,column_name
from (select b.index_name index_name,a.logging logging
,'('||listagg(case when substr(b.column_name,1,6) = 'SYS_NC' then 
replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(
get_long_test2(b.index_name,b.table_owner,b.table_name,b.column_position)
,'"',''),'RTRIM(',''),')',''),'LTRIM(',''),')',''),'TRIM(',''),')',''),'TRUNC(',''),')',''),'SUBSTR(',''),',1,3',''),'TO_CHAR(',''),',''yyyymmdd''',''),',''dd/mm/yyyy''',''),'NVL(',''),',''null''','')
else b.column_name end,',') within group (order by b.index_name,b.column_position)||')' column_name
from ALL_INDEXES a
inner join ALL_IND_COLUMNS b
on a.table_owner = b.table_owner
and a.table_name = b.table_name
and a.index_name = b.index_name
left join all_ind_expressions c 
on c.index_owner = b.index_owner
and c.index_name = b.index_name
and c.column_position = b.column_position
where a.table_owner = '${OWNER}'
and a.table_name = '${TABLE_NAME}'
and a.uniqueness = 'NONUNIQUE'
and not exists
(select 1
from ALL_CONS_COLUMNS c
where b.table_owner = c.owner
and b.table_name = c.table_name
and b.index_name = c.constraint_name
and b.column_name = c.column_name)
group by b.index_name,a.logging
order by b.index_name,a.logging));

select case when logging = 'YES' then 'create unique index '||index_name||' on ${TEMP_TABLE_NAME} '||column_name||';'
else 'create unique index '||index_name||' on ${TEMP_TABLE_NAME} '||column_name||' nologging;' end
from (select case when length(replace(index_name,index_name,'UIDX_${TEMP_TABLE_NAME}')||'_'||rownum) > 30 then
substr(replace(index_name,index_name,'UIDX_${TEMP_TABLE_NAME}'),1,26)||'_'||rownum
else replace(index_name,index_name,'UIDX_${TEMP_TABLE_NAME}')||'_'||rownum end index_name
,logging,column_name
from (select b.index_name index_name,a.logging logging
,'('||listagg(case when substr(b.column_name,1,6) = 'SYS_NC' then 
replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(
get_long_test2(b.index_name,b.table_owner,b.table_name,b.column_position)
,'"',''),'RTRIM(',''),')',''),'LTRIM(',''),')',''),'TRIM(',''),')',''),'TRUNC(',''),')',''),'SUBSTR(',''),',1,3',''),'TO_CHAR(',''),',''yyyymmdd''',''),',''dd/mm/yyyy''',''),'NVL(',''),',''null''','')
else b.column_name end,',') within group (order by b.index_name,b.column_position)||')' column_name
from ALL_INDEXES a
inner join ALL_IND_COLUMNS b
on a.table_owner = b.table_owner
and a.table_name = b.table_name
and a.index_name = b.index_name
left join all_ind_expressions c 
on c.index_owner = b.index_owner
and c.index_name = b.index_name
and c.column_position = b.column_position
where a.table_owner = '${OWNER}'
and a.table_name = '${TABLE_NAME}'
and a.uniqueness = 'UNIQUE'
and not exists
(select 1
from ALL_CONS_COLUMNS c
where b.table_owner = c.owner
and b.table_name = c.table_name
and b.index_name = c.constraint_name
and b.column_name = c.column_name)
group by b.index_name,a.logging
order by b.index_name,a.logging));

select 'alter table ${TEMP_TABLE_NAME} add constraint '||constraint_name||' primary key '||column_name||';'
from (select case when length(replace(a.constraint_name,a.constraint_name,'PK_${TEMP_TABLE_NAME}')) > 30 then
substr(replace(a.constraint_name,a.constraint_name,'PK_${TEMP_TABLE_NAME}'),1,30)
else replace(a.constraint_name,a.constraint_name,'PK_${TEMP_TABLE_NAME}') end constraint_name
,'('||listagg(a.column_name,',') within group (order by a.constraint_name,a.position)||')' column_name
from ALL_CONS_COLUMNS a
where a.owner = '${OWNER}'
and a.table_name = '${TABLE_NAME}'
and exists
(select 1
from ALL_CONSTRAINTS b
where a.owner = b.owner
and a.constraint_name = b.constraint_name
and b.constraint_type = 'P')
group by a.constraint_name);

select 'alter index '||index_name||' nologging;'
from (select case when length(replace(a.index_name,a.index_name,'PK_${TEMP_TABLE_NAME}')) > 30 then
substr(replace(a.index_name,a.index_name,'PK_${TEMP_TABLE_NAME}'),1,30)
else replace(a.index_name,a.index_name,'PK_${TEMP_TABLE_NAME}') end index_name
from ALL_INDEXES a
where a.table_owner = '{OWNER}'
and a.table_name = '${TABLE_NAME}'
and a.uniqueness = 'UNIQUE'
and a.logging = 'NO'
and exists
(select 1
from ALL_CONSTRAINTS c
where a.table_owner = c.owner
and a.table_name = c.table_name
and a.index_name = c.constraint_name));

select 'exit;' from dual;

spool off

exit;
EOF
	
		rm -f ${CREATETABLE_PATH}create_table_${TEMP_TABLE_NAME}.sql
		
		cd /data1/DSL
		java DSL -plus ${RUNOWNER} ESLDEV @${CREATETABLE_PATH}gen_create_table_${TEMP_TABLE_NAME}.sql > /dev/null 2>&1
		
		echo 'Gen Create Temp Table Completed --> '${CREATETABLE_PATH}create_table_${TEMP_TABLE_NAME}.sql
		echo 'Gen Create Temp Table Completed --> '${CREATETABLE_PATH}create_table_${TEMP_TABLE_NAME}.sql >> $p_main$p_log/gen_create_table_full_step_${LOGTIME}.log
		
		rm -f ${CREATETABLE_PATH}gen_create_table_${TEMP_TABLE_NAME}.sql

##### GEN CREATE STAGING TABLE #####	
		cat << EOF > ${CREATETABLE_PATH}gen_create_table_${STG_TABLE_NAME}.sql
set feedback off
set trimspool on
set heading off
set echo off
set linesize 4000
set pages

spool ${CREATETABLE_PATH}create_table_${STG_TABLE_NAME}.sql

select text
from (select 'create table ${STG_TABLE_NAME}' text,1 rowno from dual
union
select '(' text,2 rowno from dual
union
select 'DATA_DATE VARCHAR2(20),' text,3 rowno from dual
union
select 'PPN_TM TIMESTAMP(9),' text,999997 rowno from dual
union
select 'SRC_FILE_NAME VARCHAR2(100)' text,999998 rowno from dual
union
select ');' text,999999 rowno from dual
union
select text,rownum+3 rowno
from (select case when column_name = 'UID' then '"'||column_name||'"' else column_name end ||
case when data_type = 'NUMBER' and data_length is not null and data_precision is null and data_scale is not null then ' INTEGER'
else ' '||data_type end||
case when data_type in ('CHAR','VARCHAR2') then '('||data_length||')'
when data_type = 'DATE' then ''
when data_type = 'CLOB' then ''
when data_type = 'BLOB' then ''
when data_type = 'LONG' then ''
when substr(data_type,1,9) = 'TIMESTAMP' then ''
when data_type = 'NVARCHAR2' then '('||data_length/2||')'
when data_type = 'NUMBER' and data_length is not null and data_precision is null and data_scale is not null then ''
when data_type = 'NUMBER' and data_length is not null and data_precision is null and data_scale is not null then ''
when data_type = 'NUMBER' and data_length is not null and data_precision is not null and data_scale is not null then '('||data_precision||','||data_scale||')' end||
case when data_default is not null then ' default '||replace(replace(trim(get_long_test('${OWNER}','${TABLE_NAME}',column_id)),chr(10),''),chr(13),'') else '' end||
case when nullable = 'N' then ' not null,' else ',' end text
from all_tab_columns 
where owner = '${OWNER}' 
and table_name = '${TABLE_NAME}' 
order by column_id))
order by rowno;

select distinct 'comment on table ${STG_TABLE_NAME} is '''||replace(b.comments,'''','''''')||''';' 
from all_tab_columns a
inner join ALL_TAB_COMMENTS b
on a.owner = b.owner
and a.table_name = b.table_name 
where b.owner = '${OWNER}' 
and b.table_name = '${TABLE_NAME}'
and b.comments is not null;

select comments
from (select distinct a.column_id,'comment on column ${STG_TABLE_NAME}.'||b.column_name||' is '''||replace(b.comments,'''','''''')||''';' comments
from all_tab_columns a
inner join ALL_COL_COMMENTS b
on a.owner = b.owner
and a.table_name = b.table_name
and a.column_name = b.column_name
where b.owner = '${OWNER}'
and b.table_name = '${TABLE_NAME}'
and b.comments is not null
order by a.column_id);

select case when logging = 'YES' then 'create index '||index_name||' on ${STG_TABLE_NAME} '||column_name||';'
else 'create index '||index_name||' on ${STG_TABLE_NAME} '||column_name||' nologging;' end
from (select case when length(replace(index_name,index_name,'IDX_${STG_TABLE_NAME}')||'_'||rownum) > 30 then
substr(replace(index_name,index_name,'IDX_${STG_TABLE_NAME}'),1,26)||'_'||rownum
else replace(index_name,index_name,'IDX_${STG_TABLE_NAME}')||'_'||rownum end index_name
,logging,column_name
from (select b.index_name index_name,a.logging logging
,'('||listagg(case when substr(b.column_name,1,6) = 'SYS_NC' then 
replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(
get_long_test2(b.index_name,b.table_owner,b.table_name,b.column_position)
,'"',''),'RTRIM(',''),')',''),'LTRIM(',''),')',''),'TRIM(',''),')',''),'TRUNC(',''),')',''),'SUBSTR(',''),',1,3',''),'TO_CHAR(',''),',''yyyymmdd''',''),',''dd/mm/yyyy''',''),'NVL(',''),',''null''','')
else b.column_name end,',') within group (order by b.index_name,b.column_position)||')' column_name
from ALL_INDEXES a
inner join ALL_IND_COLUMNS b
on a.table_owner = b.table_owner
and a.table_name = b.table_name
and a.index_name = b.index_name
left join all_ind_expressions c 
on c.index_owner = b.index_owner
and c.index_name = b.index_name
and c.column_position = b.column_position
where a.table_owner = '${OWNER}'
and a.table_name = '${TABLE_NAME}'
and a.uniqueness = 'NONUNIQUE'
and not exists
(select 1
from ALL_CONS_COLUMNS c
where b.table_owner = c.owner
and b.table_name = c.table_name
and b.index_name = c.constraint_name
and b.column_name = c.column_name)
group by b.index_name,a.logging
order by b.index_name,a.logging));

select case when logging = 'YES' then 'create unique index '||index_name||' on ${STG_TABLE_NAME} '||column_name||';'
else 'create unique index '||index_name||' on ${STG_TABLE_NAME} '||column_name||' nologging;' end
from (select case when length(replace(index_name,index_name,'UIDX_${STG_TABLE_NAME}')||'_'||rownum) > 30 then
substr(replace(index_name,index_name,'UIDX_${STG_TABLE_NAME}'),1,26)||'_'||rownum
else replace(index_name,index_name,'UIDX_${STG_TABLE_NAME}')||'_'||rownum end index_name
,logging,column_name
from (select b.index_name index_name,a.logging logging
,'('||listagg(case when substr(b.column_name,1,6) = 'SYS_NC' then 
replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(
get_long_test2(b.index_name,b.table_owner,b.table_name,b.column_position)
,'"',''),'RTRIM(',''),')',''),'LTRIM(',''),')',''),'TRIM(',''),')',''),'TRUNC(',''),')',''),'SUBSTR(',''),',1,3',''),'TO_CHAR(',''),',''yyyymmdd''',''),',''dd/mm/yyyy''',''),'NVL(',''),',''null''','')
else b.column_name end,',') within group (order by b.index_name,b.column_position)||')' column_name
from ALL_INDEXES a
inner join ALL_IND_COLUMNS b
on a.table_owner = b.table_owner
and a.table_name = b.table_name
and a.index_name = b.index_name
left join all_ind_expressions c 
on c.index_owner = b.index_owner
and c.index_name = b.index_name
and c.column_position = b.column_position
where a.table_owner = '${OWNER}'
and a.table_name = '${TABLE_NAME}'
and a.uniqueness = 'UNIQUE'
and not exists
(select 1
from ALL_CONS_COLUMNS c
where b.table_owner = c.owner
and b.table_name = c.table_name
and b.index_name = c.constraint_name
and b.column_name = c.column_name)
group by b.index_name,a.logging
order by b.index_name,a.logging));

select 'alter table ${STG_TABLE_NAME} add constraint '||constraint_name||' primary key '||column_name||';'
from (select case when length(replace(a.constraint_name,a.constraint_name,'PK_${STG_TABLE_NAME}')) > 30 then
substr(replace(a.constraint_name,a.constraint_name,'PK_${STG_TABLE_NAME}'),1,30)
else replace(a.constraint_name,a.constraint_name,'PK_${STG_TABLE_NAME}') end constraint_name
,'('||listagg(a.column_name,',') within group (order by a.constraint_name,a.position)||')' column_name
from ALL_CONS_COLUMNS a
where a.owner = '${OWNER}'
and a.table_name = '${TABLE_NAME}'
and exists
(select 1
from ALL_CONSTRAINTS b
where a.owner = b.owner
and a.constraint_name = b.constraint_name
and b.constraint_type = 'P')
group by a.constraint_name);

select 'alter index '||index_name||' nologging;'
from (select case when length(replace(a.index_name,a.index_name,'PK_${STG_TABLE_NAME}')) > 30 then
substr(replace(a.index_name,a.index_name,'PK_${STG_TABLE_NAME}'),1,30)
else replace(a.index_name,a.index_name,'PK_${STG_TABLE_NAME}') end index_name
from ALL_INDEXES a
where a.table_owner = '{OWNER}'
and a.table_name = '${TABLE_NAME}'
and a.uniqueness = 'UNIQUE'
and a.logging = 'NO'
and exists
(select 1
from ALL_CONSTRAINTS c
where a.table_owner = c.owner
and a.table_name = c.table_name
and a.index_name = c.constraint_name));

select 'exit;' from dual;

spool off

exit;
EOF

		rm -f ${CREATETABLE_PATH}create_table_${STG_TABLE_NAME}.sql
		
		cd /data1/DSL
		java DSL -plus ${RUNOWNER} ESLDEV @${CREATETABLE_PATH}gen_create_table_${STG_TABLE_NAME}.sql > /dev/null 2>&1
		
		echo 'Gen Create Staging Table Completed --> '${CREATETABLE_PATH}create_table_${STG_TABLE_NAME}.sql
		echo 'Gen Create Staging Table Completed --> '${CREATETABLE_PATH}create_table_${STG_TABLE_NAME}.sql >> $p_main$p_log/gen_create_table_full_step_${LOGTIME}.log
		
		rm -f ${CREATETABLE_PATH}gen_create_table_${STG_TABLE_NAME}.sql

##### GEN CREATE DIM TABLE #####		
		cat << EOF > ${CREATETABLE_PATH}gen_create_table_${DIM_TABLE_NAME}.sql
set feedback off
set trimspool on
set heading off
set echo off
set linesize 4000
set pages

spool ${CREATETABLE_PATH}create_table_${DIM_TABLE_NAME}.sql

select text
from (select 'create table ${DIM_TABLE_NAME}' text,1 rowno from dual
union
select '(' text,2 rowno from dual
union
select 'HASH_CHECK VARCHAR2(100),' text,999994 rowno from dual
union
select 'START_DATE DATE,' text,999995 rowno from dual
union
select 'END_DATE DATE,' text,999996 rowno from dual
union
select 'RECORD_DELETED_FLAG NUMBER,' text,999997 rowno from dual
union
select 'PPN_TM TIMESTAMP(6)' text,999998 rowno from dual
union
select ');' text,999999 rowno from dual
union
select text,rownum+2 rowno
from (select case when column_name = 'UID' then '"'||column_name||'"' else column_name end ||
case when data_type = 'NUMBER' and data_length is not null and data_precision is null and data_scale is not null then ' INTEGER'
else ' '||data_type end||
case when data_type in ('CHAR','VARCHAR2') then '('||data_length||')'
when data_type = 'DATE' then ''
when data_type = 'CLOB' then ''
when data_type = 'BLOB' then ''
when data_type = 'LONG' then ''
when substr(data_type,1,9) = 'TIMESTAMP' then ''
when data_type = 'NVARCHAR2' then '('||data_length/2||')'
when data_type = 'NUMBER' and data_length is not null and data_precision is null and data_scale is not null then ''
when data_type = 'NUMBER' and data_length is not null and data_precision is null and data_scale is not null then ''
when data_type = 'NUMBER' and data_length is not null and data_precision is not null and data_scale is not null then '('||data_precision||','||data_scale||')' end||
case when data_default is not null then ' default '||replace(replace(trim(get_long_test('${OWNER}','${TABLE_NAME}',column_id)),chr(10),''),chr(13),'') else '' end||
case when nullable = 'N' then ' not null,' else ',' end text
from all_tab_columns 
where owner = '${OWNER}' 
and table_name = '${TABLE_NAME}'
and column_name not in ('UPD_USER','UPD_DATE','CREATE_BY','CREATE_DATE','UPDATE_BY','UPDATE_DATE')
order by column_id))
order by rowno;

select distinct 'comment on table ${DIM_TABLE_NAME} is '''||replace(b.comments,'''','''''')||''';' 
from all_tab_columns a
inner join ALL_TAB_COMMENTS b
on a.owner = b.owner
and a.table_name = b.table_name 
where b.owner = '${OWNER}' 
and b.table_name = '${TABLE_NAME}'
and b.comments is not null;

select comments
from (select distinct a.column_id,'comment on column ${DIM_TABLE_NAME}.'||b.column_name||' is '''||replace(b.comments,'''','''''')||''';' comments
from all_tab_columns a
inner join ALL_COL_COMMENTS b
on a.owner = b.owner
and a.table_name = b.table_name
and a.column_name = b.column_name
where b.owner = '${OWNER}'
and b.table_name = '${TABLE_NAME}'
and a.column_name not in ('UPD_USER','UPD_DATE','CREATE_BY','CREATE_DATE','UPDATE_BY','UPDATE_DATE')
and b.comments is not null
order by a.column_id);

select case when logging = 'YES' then 'create index '||index_name||' on ${DIM_TABLE_NAME} '||column_name||';'
else 'create index '||index_name||' on ${DIM_TABLE_NAME} '||column_name||' nologging;' end
from (select case when length(replace(index_name,index_name,'IDX_${DIM_TABLE_NAME}')||'_'||rownum) > 30 then
substr(replace(index_name,index_name,'IDX_${DIM_TABLE_NAME}'),1,26)||'_'||rownum
else replace(index_name,index_name,'IDX_${DIM_TABLE_NAME}')||'_'||rownum end index_name
,logging,column_name
from (select b.index_name index_name,a.logging logging
,'('||listagg(case when substr(b.column_name,1,6) = 'SYS_NC' then 
replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(
get_long_test2(b.index_name,b.table_owner,b.table_name,b.column_position)
,'"',''),'RTRIM(',''),')',''),'LTRIM(',''),')',''),'TRIM(',''),')',''),'TRUNC(',''),')',''),'SUBSTR(',''),',1,3',''),'TO_CHAR(',''),',''yyyymmdd''',''),',''dd/mm/yyyy''',''),'NVL(',''),',''null''','')
else b.column_name end,',') within group (order by b.index_name,b.column_position)||')' column_name
from ALL_INDEXES a
inner join ALL_IND_COLUMNS b
on a.table_owner = b.table_owner
and a.table_name = b.table_name
and a.index_name = b.index_name
left join all_ind_expressions c 
on c.index_owner = b.index_owner
and c.index_name = b.index_name
and c.column_position = b.column_position
where a.table_owner = '${OWNER}'
and a.table_name = '${TABLE_NAME}'
and a.uniqueness = 'NONUNIQUE'
and not exists
(select 1
from ALL_CONS_COLUMNS c
where b.table_owner = c.owner
and b.table_name = c.table_name
and b.index_name = c.constraint_name
and b.column_name = c.column_name)
group by b.index_name,a.logging
order by b.index_name,a.logging));

select case when logging = 'YES' then 'create unique index '||index_name||' on ${DIM_TABLE_NAME} '||column_name||';'
else 'create unique index '||index_name||' on ${DIM_TABLE_NAME} '||column_name||' nologging;' end
from (select case when length(replace(index_name,index_name,'UIDX_${DIM_TABLE_NAME}')||'_'||rownum) > 30 then
substr(replace(index_name,index_name,'UIDX_${DIM_TABLE_NAME}'),1,26)||'_'||rownum
else replace(index_name,index_name,'UIDX_${DIM_TABLE_NAME}')||'_'||rownum end index_name
,logging,column_name
from (select b.index_name index_name,a.logging logging
,'('||listagg(case when substr(b.column_name,1,6) = 'SYS_NC' then 
replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(
get_long_test2(b.index_name,b.table_owner,b.table_name,b.column_position)
,'"',''),'RTRIM(',''),')',''),'LTRIM(',''),')',''),'TRIM(',''),')',''),'TRUNC(',''),')',''),'SUBSTR(',''),',1,3',''),'TO_CHAR(',''),',''yyyymmdd''',''),',''dd/mm/yyyy''',''),'NVL(',''),',''null''','')
else b.column_name end,',') within group (order by b.index_name,b.column_position)||')' column_name
from ALL_INDEXES a
inner join ALL_IND_COLUMNS b
on a.table_owner = b.table_owner
and a.table_name = b.table_name
and a.index_name = b.index_name
left join all_ind_expressions c 
on c.index_owner = b.index_owner
and c.index_name = b.index_name
and c.column_position = b.column_position
where a.table_owner = '${OWNER}'
and a.table_name = '${TABLE_NAME}'
and a.uniqueness = 'UNIQUE'
and not exists
(select 1
from ALL_CONS_COLUMNS c
where b.table_owner = c.owner
and b.table_name = c.table_name
and b.index_name = c.constraint_name
and b.column_name = c.column_name)
group by b.index_name,a.logging
order by b.index_name,a.logging));

select 'alter table ${DIM_TABLE_NAME} add constraint '||constraint_name||' primary key '||column_name||';'
from (select case when length(replace(a.constraint_name,a.constraint_name,'PK_${DIM_TABLE_NAME}')) > 30 then
substr(replace(a.constraint_name,a.constraint_name,'PK_${DIM_TABLE_NAME}'),1,30)
else replace(a.constraint_name,a.constraint_name,'PK_${DIM_TABLE_NAME}') end constraint_name
,'('||listagg(a.column_name,',') within group (order by a.constraint_name,a.position)||')' column_name
from ALL_CONS_COLUMNS a
where a.owner = '${OWNER}'
and a.table_name = '${TABLE_NAME}'
and exists
(select 1
from ALL_CONSTRAINTS b
where a.owner = b.owner
and a.constraint_name = b.constraint_name
and b.constraint_type = 'P')
group by a.constraint_name);

select 'alter index '||index_name||' nologging;'
from (select case when length(replace(a.index_name,a.index_name,'PK_${DIM_TABLE_NAME}')) > 30 then
substr(replace(a.index_name,a.index_name,'PK_${DIM_TABLE_NAME}'),1,30)
else replace(a.index_name,a.index_name,'PK_${DIM_TABLE_NAME}') end index_name
from ALL_INDEXES a
where a.table_owner = '{OWNER}'
and a.table_name = '${TABLE_NAME}'
and a.uniqueness = 'UNIQUE'
and a.logging = 'NO'
and exists
(select 1
from ALL_CONSTRAINTS c
where a.table_owner = c.owner
and a.table_name = c.table_name
and a.index_name = c.constraint_name));

select 'exit;' from dual;

spool off

exit;
EOF

		rm -f ${CREATETABLE_PATH}create_table_${DIM_TABLE_NAME}.sql
		
		cd /data1/DSL
		java DSL -plus ${RUNOWNER} ESLDEV @${CREATETABLE_PATH}gen_create_table_${DIM_TABLE_NAME}.sql > /dev/null 2>&1
		
		echo 'Gen Create Dim Table Completed --> '${CREATETABLE_PATH}create_table_${DIM_TABLE_NAME}.sql
		echo 'Gen Create Dim Table Completed --> '${CREATETABLE_PATH}create_table_${DIM_TABLE_NAME}.sql >> $p_main$p_log/gen_create_table_full_step_${LOGTIME}.log
		
		rm -f ${CREATETABLE_PATH}gen_create_table_${DIM_TABLE_NAME}.sql

##### CREATE TEMP TABLE #####		
		cat << EOF > ${CREATETABLE_PATH}gen_check_table_${TEMP_TABLE_NAME}.sql
set feedback off
set trimspool on
set heading off
set echo off
set linesize 4000
set pages

spool ${CREATETABLE_PATH}check_table_${TEMP_TABLE_NAME}.txt

select table_name
from user_tables
where table_name = '${TEMP_TABLE_NAME}';

spool off

exit;
EOF
			
		cd /data1/DSL
		java DSL -plus ${RUNTARGETOWNER} ESLDEV @${CREATETABLE_PATH}gen_check_table_${TEMP_TABLE_NAME}.sql > /dev/null 2>&1
		
		CHK_TEMP_TABLE_NAME=$(cat ${CREATETABLE_PATH}check_table_${TEMP_TABLE_NAME}.txt)
		
		if [ "${CHK_TEMP_TABLE_NAME}" == "${TEMP_TABLE_NAME}" ]
		then
			#echo "drop table ${TEMP_TABLE_NAME};" > ${CREATETABLE_PATH}drop_table_${TEMP_TABLE_NAME}.sql
			#echo "exit;" >> ${CREATETABLE_PATH}drop_table_${TEMP_TABLE_NAME}.sql
			#cd /data1/DSL
			#java DSL -plus ${RUNTARGETOWNER} ESLDEV @${CREATETABLE_PATH}drop_table_${TEMP_TABLE_NAME}.sql > /dev/null 2>&1
			#echo "Drop Table Completed --> Table ${TEMP_TABLE_NAME}"
			#echo "Drop Table Completed --> Table ${TEMP_TABLE_NAME}" >> $p_main$p_log/gen_create_table_full_step_${LOGTIME}.log
			#java DSL -plus ${RUNTARGETOWNER} ESLDEV @${CREATETABLE_PATH}create_table_${TEMP_TABLE_NAME}.sql > /dev/null 2>&1
			#echo "Create Table Completed --> Table ${TEMP_TABLE_NAME}"
			#echo "Create Table Completed --> Table ${TEMP_TABLE_NAME}" >> $p_main$p_log/gen_create_table_full_step_${LOGTIME}.log
			#rm -f ${CREATETABLE_PATH}drop_table_${TEMP_TABLE_NAME}.sql
			echo "Table is Exists --> Table ${TEMP_TABLE_NAME}"
			echo "Table is Exists --> Table ${TEMP_TABLE_NAME}" >> $p_main$p_log/gen_create_table_full_step_${LOGTIME}.log
		else
			cd /data1/DSL
			java DSL -plus ${RUNTARGETOWNER} ESLDEV @${CREATETABLE_PATH}create_table_${TEMP_TABLE_NAME}.sql > /dev/null 2>&1
			echo "Create Table Completed --> Table ${TEMP_TABLE_NAME}"
			echo "Create Table Completed --> Table ${TEMP_TABLE_NAME}" >> $p_main$p_log/gen_create_table_full_step_${LOGTIME}.log
		fi
		
		rm -f ${CREATETABLE_PATH}gen_check_table_${TEMP_TABLE_NAME}.sql
		rm -f ${CREATETABLE_PATH}check_table_${TEMP_TABLE_NAME}.txt

##### CREATE STAGING TABLE #####		
		cat << EOF > ${CREATETABLE_PATH}gen_check_table_${STG_TABLE_NAME}.sql
set feedback off
set trimspool on
set heading off
set echo off
set linesize 4000
set pages

spool ${CREATETABLE_PATH}check_table_${STG_TABLE_NAME}.txt

select table_name
from user_tables
where table_name = '${STG_TABLE_NAME}';

spool off

exit;
EOF
			
		cd /data1/DSL
		java DSL -plus ${RUNTARGETOWNER} ESLDEV @${CREATETABLE_PATH}gen_check_table_${STG_TABLE_NAME}.sql > /dev/null 2>&1
		
		CHK_STG_TABLE_NAME=$(cat ${CREATETABLE_PATH}check_table_${STG_TABLE_NAME}.txt)
		
		if [ "${CHK_STG_TABLE_NAME}" == "${STG_TABLE_NAME}" ]
		then
			#echo "drop table ${STG_TABLE_NAME};" > ${CREATETABLE_PATH}drop_table_${STG_TABLE_NAME}.sql
			#echo "exit;" >> ${CREATETABLE_PATH}drop_table_${STG_TABLE_NAME}.sql
			#cd /data1/DSL
			#java DSL -plus ${RUNTARGETOWNER} ESLDEV @${CREATETABLE_PATH}drop_table_${STG_TABLE_NAME}.sql > /dev/null 2>&1
			#echo "Drop Table Completed --> Table ${STG_TABLE_NAME}"
			#echo "Drop Table Completed --> Table ${STG_TABLE_NAME}" >> $p_main$p_log/gen_create_table_full_step_${LOGTIME}.log
			#java DSL -plus ${RUNTARGETOWNER} ESLDEV @${CREATETABLE_PATH}create_table_${STG_TABLE_NAME}.sql > /dev/null 2>&1
			#echo "Create Table Completed --> Table ${STG_TABLE_NAME}"
			#echo "Create Table Completed --> Table ${STG_TABLE_NAME}" >> $p_main$p_log/gen_create_table_full_step_${LOGTIME}.log
			#rm -f ${CREATETABLE_PATH}drop_table_${STG_TABLE_NAME}.sql
			echo "Table is Exists --> Table ${STG_TABLE_NAME}"
			echo "Table is Exists --> Table ${STG_TABLE_NAME}" >> $p_main$p_log/gen_create_table_full_step_${LOGTIME}.log
		else
			cd /data1/DSL
			java DSL -plus ${RUNTARGETOWNER} ESLDEV @${CREATETABLE_PATH}create_table_${STG_TABLE_NAME}.sql > /dev/null 2>&1
			echo "Create Table Completed --> Table ${STG_TABLE_NAME}"
			echo "Create Table Completed --> Table ${STG_TABLE_NAME}" >> $p_main$p_log/gen_create_table_full_step_${LOGTIME}.log
		fi
		
		rm -f ${CREATETABLE_PATH}gen_check_table_${STG_TABLE_NAME}.sql
		rm -f ${CREATETABLE_PATH}check_table_${STG_TABLE_NAME}.txt

##### GEN SQL LOADER TEMP TABLE #####		
		cat << EOF > ${SQLLDR_PATH}gen_sqlldr_${TEMP_TABLE_NAME}.sql
set feedback off
set trimspool on
set heading off
set echo off
set linesize 4000
set pages

spool ${SQLLDR_PATH}sqlldr_${TEMP_TABLE_NAME}.txt

select case when rowno = (select count(*)+7 from ALL_TAB_COLUMNS where owner = '${TARGETOWNER}' and table_name = '${TEMP_TABLE_NAME}') then rtrim(text,',') else text end
from (select 'OPTIONS(skip=1)' text,1 rowno from dual
union
select 'LOAD DATA' text,2 rowno from dual
union
select 'CHARACTERSET UTF8' text,3 rowno from dual
union
select 'INFILE '||'''dummy.txt''' text,4 rowno from dual
union
select 'INTO TABLE ${TARGETOWNER}.'||'${TEMP_TABLE_NAME}' text,5 rowno from dual
union
select 'FIELDS TERMINATED BY '||'''|''' text,6 rowno from dual
union
select 'TRAILING NULLCOLS (' text,7 rowno from dual
union
select ')' text,999999 rowno from dual
union
select text,rownum+7 rowno
from (select 
case when data_type = 'CHAR' then 
     case when nullable = 'N' then column_name||' '||data_type||' "CASE WHEN TRIM(:'||column_name||') IS NULL THEN '' '' #ELSE TRIM(:'||column_name||') END",'
     else column_name||' '||data_type||' "TRIM(:'||column_name||')",' end
when data_type = 'VARCHAR2' then 
     case when nullable = 'N' then column_name||' '||'"CASE WHEN TRIM(:'||column_name||') IS NULL THEN '' '' #ELSE TRIM(:'||column_name||') END",'
     else column_name||' "TRIM(:'||column_name||')",' end
when data_type = 'NVARCHAR2' then 
     case when nullable = 'N' then column_name||' '||'"CASE WHEN TRIM(:'||column_name||') IS NULL THEN '' '' #ELSE TRIM(:'||column_name||') END",'
     else column_name||' "TRIM(:'||column_name||')",' end
when data_type = 'NUMBER' then column_name||','
when data_type = 'DATE' then column_name||' TIMESTAMP "YYYYMMDD HH24MISSFF",'
when substr(data_type,1,9) = 'TIMESTAMP' then column_name||' TIMESTAMP "YYYYMMDD HH24MISSFF",'
when data_type = 'CLOB' then column_name||' CHAR(4000),'
end text
from ALL_TAB_COLUMNS 
where owner = '${TARGETOWNER}'
and table_name = '${TEMP_TABLE_NAME}'
order by column_id))
order by rowno;

spool off

exit;
EOF

		rm -f ${SQLLDR_PATH}sqlldr_${TEMP_TABLE_NAME}.txt

		cd /data1/DSL
		java DSL -plus ${RUNTARGETOWNER} ESLDEV @${SQLLDR_PATH}gen_sqlldr_${TEMP_TABLE_NAME}.sql > /dev/null 2>&1
		
		cat ${SQLLDR_PATH}sqlldr_${TEMP_TABLE_NAME}.txt | tr '#' '\n' > ${SQLLDR_PATH}sqlldr_${TEMP_TABLE_NAME}.tmp
		rm -f ${SQLLDR_PATH}sqlldr_${TEMP_TABLE_NAME}.txt
		mv ${SQLLDR_PATH}sqlldr_${TEMP_TABLE_NAME}.tmp ${SQLLDR_PATH}sqlldr_${TEMP_TABLE_NAME}.txt
		
		echo 'Gen Sql Loader Completed --> '${SQLLDR_PATH}sqlldr_${TEMP_TABLE_NAME}.txt
		echo 'Gen Sql Loader Completed --> '${SQLLDR_PATH}sqlldr_${TEMP_TABLE_NAME}.txt >> $p_main$p_log/gen_create_table_full_step_${LOGTIME}.log
		
		rm -f ${SQLLDR_PATH}gen_sqlldr_${TEMP_TABLE_NAME}.sql
		
##### INSERT INTO DSL_EXTRACT_SYSTEM_FILE #####		
		OWNER_3=$(echo ${OWNER} | cut -c1-3)
		cat << EOF > ${CREATETABLE_PATH}get_owner_path.sql
set feedback off
set trimspool on
set heading off
set echo off
set linesize 4000
set pages

spool ${CREATETABLE_PATH}get_owner_path.txt

select ctl_id||'|'||path_name 
from DSL_EXTRACT_SYSTEM
where system_name = '${OWNER_3}';

spool off

exit;
EOF
			
		cd /data1/DSL
		java DSL -plus ${RUNTARGETOWNER} ESLDEV @${CREATETABLE_PATH}get_owner_path.sql > /dev/null 2>&1
		
		rm -f ${CREATETABLE_PATH}get_owner_path.sql
		
		OWNER_PATH=$(cat ${CREATETABLE_PATH}get_owner_path.txt | awk -F'|' '{print $2}')
		CTL_ID=$(cat ${CREATETABLE_PATH}get_owner_path.txt | awk -F'|' '{print $1}')
		rm -f ${CREATETABLE_PATH}get_owner_path.txt
		
		cd ${OWNER_PATH}
		TOTAL_FILE=$(ls -1 *.TXT | sort -u | wc -l)
		ls -1 *.TXT | sort -u | head -$(echo ${TOTAL_FILE}/4 | bc) | nl > ${CREATETABLE_PATH}STG01.txt
		ls -1 *.TXT | sort -u | head -$(echo ${TOTAL_FILE}/4*2 | bc) | tail -$(echo ${TOTAL_FILE}/4 | bc) | nl > ${CREATETABLE_PATH}STG02.txt
		ls -1 *.TXT | sort -u | head -$(echo ${TOTAL_FILE}/4*3 | bc) | tail -$(echo ${TOTAL_FILE}/4 | bc) | nl > ${CREATETABLE_PATH}STG03.txt
		ls -1 *.TXT | sort -u | head -$(echo ${TOTAL_FILE}/4*4+${TOTAL_FILE}%4 | bc) | tail -$(echo ${TOTAL_FILE}/4+${TOTAL_FILE}%4 | bc) | nl > ${CREATETABLE_PATH}STG04.txt
		
		if [ "`grep ${OWNER_3}_${TABLE_NAME} ${CREATETABLE_PATH}STG01.txt`" != "" ]
		then
			CTL_JOB_NM=${OWNER_3}_D_STG01
			if [ "`grep ${OWNER_3}_${TABLE_NAME}_FULL ${CREATETABLE_PATH}STG01.txt`" != "" ]
			then
				FILE_NAME=`grep ${OWNER_3}_${TABLE_NAME}_FULL ${CREATETABLE_PATH}STG01.txt | awk '{print $2}'`
				FILE_ID=`grep ${OWNER_3}_${TABLE_NAME}_FULL ${CREATETABLE_PATH}STG01.txt | awk '{print $1}'`
			elif [ "`grep ${OWNER_3}_${TABLE_NAME}_DEL ${CREATETABLE_PATH}STG01.txt`" != "" ]
			then
				FILE_NAME=`grep ${OWNER_3}_${TABLE_NAME}_DEL ${CREATETABLE_PATH}STG01.txt | awk '{print $2}'`
				FILE_ID=`grep ${OWNER_3}_${TABLE_NAME}_DEL ${CREATETABLE_PATH}STG01.txt | awk '{print $1}'`
			fi
		elif [ "`grep ${OWNER_3}_${TABLE_NAME} ${CREATETABLE_PATH}STG02.txt`" != "" ]
		then
			CTL_JOB_NM=${OWNER_3}_D_STG02
			if [ "`grep ${OWNER_3}_${TABLE_NAME}_FULL ${CREATETABLE_PATH}STG02.txt`" != "" ]
			then
				FILE_NAME=`grep ${OWNER_3}_${TABLE_NAME}_FULL ${CREATETABLE_PATH}STG02.txt | awk '{print $2}'`
				FILE_ID=`grep ${OWNER_3}_${TABLE_NAME}_FULL ${CREATETABLE_PATH}STG02.txt | awk '{print $1}'`
			elif [ "`grep ${OWNER_3}_${TABLE_NAME}_DEL ${CREATETABLE_PATH}STG02.txt`" != "" ]
			then
				FILE_NAME=`grep ${OWNER_3}_${TABLE_NAME}_DEL ${CREATETABLE_PATH}STG02.txt | awk '{print $2}'`
				FILE_ID=`grep ${OWNER_3}_${TABLE_NAME}_DEL ${CREATETABLE_PATH}STG02.txt | awk '{print $1}'`
			fi
		elif [ "`grep ${OWNER_3}_${TABLE_NAME} ${CREATETABLE_PATH}STG03.txt`" != "" ]
		then
			CTL_JOB_NM=${OWNER_3}_D_STG03
			if [ "`grep ${OWNER_3}_${TABLE_NAME}_FULL ${CREATETABLE_PATH}STG03.txt`" != "" ]
			then
				FILE_NAME=`grep ${OWNER_3}_${TABLE_NAME}_FULL ${CREATETABLE_PATH}STG03.txt | awk '{print $2}'`
				FILE_ID=`grep ${OWNER_3}_${TABLE_NAME}_FULL ${CREATETABLE_PATH}STG03.txt | awk '{print $1}'`
			elif [ "`grep ${OWNER_3}_${TABLE_NAME}_DEL ${CREATETABLE_PATH}STG03.txt`" != "" ]
			then
				FILE_NAME=`grep ${OWNER_3}_${TABLE_NAME}_DEL ${CREATETABLE_PATH}STG03.txt | awk '{print $2}'`
				FILE_ID=`grep ${OWNER_3}_${TABLE_NAME}_DEL ${CREATETABLE_PATH}STG03.txt | awk '{print $1}'`
			fi
		elif [ "`grep ${OWNER_3}_${TABLE_NAME} ${CREATETABLE_PATH}STG04.txt`" != "" ]
		then
			CTL_JOB_NM=${OWNER_3}_D_STG04
			if [ "`grep ${OWNER_3}_${TABLE_NAME}_FULL ${CREATETABLE_PATH}STG04.txt`" != "" ]
			then
				FILE_NAME=`grep ${OWNER_3}_${TABLE_NAME}_FULL ${CREATETABLE_PATH}STG04.txt | awk '{print $2}'`
				FILE_ID=`grep ${OWNER_3}_${TABLE_NAME}_FULL ${CREATETABLE_PATH}STG04.txt | awk '{print $1}'`
			elif [ "`grep ${OWNER_3}_${TABLE_NAME}_DEL ${CREATETABLE_PATH}STG04.txt`" != "" ]
			then
				FILE_NAME=`grep ${OWNER_3}_${TABLE_NAME}_DEL ${CREATETABLE_PATH}STG04.txt | awk '{print $2}'`
				FILE_ID=`grep ${OWNER_3}_${TABLE_NAME}_DEL ${CREATETABLE_PATH}STG04.txt | awk '{print $1}'`
			fi
		fi
		
		FILE_EXPORT_TYPE=`echo ${FILE_NAME} | awk -F'.' '{print $1}' | sed s/${TABLE_NAME}_//g | sed s/${OWNER_3}_//g | awk -F '_' '{print $1}'`
		
		rm -f ${CREATETABLE_PATH}STG01.txt
		rm -f ${CREATETABLE_PATH}STG02.txt
		rm -f ${CREATETABLE_PATH}STG03.txt
		rm -f ${CREATETABLE_PATH}STG04.txt
		
		echo "delete ${TARGETOWNER}.DSL_EXTRACT_SYSTEM_FILE" > ${CREATETABLE_PATH}insert_into_DSL_EXTRACT_SYSTEM_FILE.sql
		echo "where data_file_name = '${TABLE_NAME}'" >> ${CREATETABLE_PATH}insert_into_DSL_EXTRACT_SYSTEM_FILE.sql
		echo "and temp_table_name = '${TEMP_TABLE_NAME}'" >> ${CREATETABLE_PATH}insert_into_DSL_EXTRACT_SYSTEM_FILE.sql
		echo "and stg_table_name = '${STG_TABLE_NAME}';" >> ${CREATETABLE_PATH}insert_into_DSL_EXTRACT_SYSTEM_FILE.sql
		echo "commit;" >> ${CREATETABLE_PATH}insert_into_DSL_EXTRACT_SYSTEM_FILE.sql
		echo "INSERT INTO ${TARGETOWNER}.DSL_EXTRACT_SYSTEM_FILE" >> ${CREATETABLE_PATH}insert_into_DSL_EXTRACT_SYSTEM_FILE.sql
		echo "(CTL_ID,CTL_JOB_NM,FILE_ID,SCHEMA_NAME,TEMP_TABLE_NAME" >> ${CREATETABLE_PATH}insert_into_DSL_EXTRACT_SYSTEM_FILE.sql
		echo ",STG_TABLE_NAME,TABLE_TYPE,DATA_FILE_NAME,CTL_FILE_NAME,STORE_PROC" >> ${CREATETABLE_PATH}insert_into_DSL_EXTRACT_SYSTEM_FILE.sql
		echo ",JOB_FREQ,DESCRIPTION,DATA_FILE_SUFFIX,CTL_FILE_SUFFIX,EXPORT_TYPE" >> ${CREATETABLE_PATH}insert_into_DSL_EXTRACT_SYSTEM_FILE.sql
		echo ",FILTER_CONDITION,DELIMETER,DDL,IS_LOAD,UPDATE_DATE" >> ${CREATETABLE_PATH}insert_into_DSL_EXTRACT_SYSTEM_FILE.sql
		echo ",UPDATE_USER,UPDATE_TS)" >> ${CREATETABLE_PATH}insert_into_DSL_EXTRACT_SYSTEM_FILE.sql
		echo "VALUES" >> ${CREATETABLE_PATH}insert_into_DSL_EXTRACT_SYSTEM_FILE.sql
		echo "(${CTL_ID},'${CTL_JOB_NM}',${FILE_ID},'${TARGETOWNER}','${TEMP_TABLE_NAME}'" >> ${CREATETABLE_PATH}insert_into_DSL_EXTRACT_SYSTEM_FILE.sql
		echo ",'${STG_TABLE_NAME}','TABLE','${TABLE_NAME}','${TABLE_NAME}','SP_STG_LOAD'" >> ${CREATETABLE_PATH}insert_into_DSL_EXTRACT_SYSTEM_FILE.sql
		echo ",'DAILY','${OWNER_3}_${TABLE_NAME}_${FILE_EXPORT_TYPE}','TXT','CTL','${FILE_EXPORT_TYPE}'" >> ${CREATETABLE_PATH}insert_into_DSL_EXTRACT_SYSTEM_FILE.sql
		
		SQLLDR_WORD=`cat ${SQLLDR_PATH}sqlldr_${TEMP_TABLE_NAME}.txt | tr '\n' '#'`
		TOTAL_WORD=`echo ${SQLLDR_WORD} | wc -c`
		CUT_WORD=2000
		
		if [ ${TOTAL_WORD} -gt ${CUT_WORD} ]
		then
			ROUND_CLOB=`echo ${TOTAL_WORD}/${CUT_WORD}+1 | bc`
			i=1
			while [ ${i} -le ${ROUND_CLOB} ]
			do
				if [ ${i} -eq 1 ]
				then
					CUT_FROM=1
					CUT_TO=${CUT_WORD}
					SDLLDR_DDL="TO_CLOB(q''"`echo ${SQLLDR_WORD} | cut -c${CUT_FROM}-${CUT_TO}`"'')"
				else
					let CUT_FROM=CUT_WORD*(i-1)+1
					let CUT_TO=CUT_WORD*i
					SDLLDR_DDL=${SDLLDR_DDL}"||TO_CLOB(q''"`echo ${SQLLDR_WORD} | cut -c${CUT_FROM}-${CUT_TO}`"'')"
				fi
				let i=i+1
			done
			echo ${SDLLDR_DDL} | tr '#' '\n' > ${SQLLDR_PATH}temp_SDLLDR_DDL.txt
			SDLLDR_DDL_TEMP=$(cat ${SQLLDR_PATH}temp_SDLLDR_DDL.txt)
			echo ",'NULL','|',${SDLLDR_DDL_TEMP},'Y',sysdate" >> ${CREATETABLE_PATH}insert_into_DSL_EXTRACT_SYSTEM_FILE.sql
			rm -f ${SQLLDR_PATH}temp_SDLLDR_DDL.txt
		else
			SDLLDR_DDL="q''"`cat ${SQLLDR_PATH}sqlldr_${TEMP_TABLE_NAME}.txt`"''"
			echo ",'NULL','|',${SDLLDR_DDL},'Y',sysdate" >> ${CREATETABLE_PATH}insert_into_DSL_EXTRACT_SYSTEM_FILE.sql
		fi		

		echo ",'SYSDBA',sysdate);" >> ${CREATETABLE_PATH}insert_into_DSL_EXTRACT_SYSTEM_FILE.sql
		echo "commit;" >> ${CREATETABLE_PATH}insert_into_DSL_EXTRACT_SYSTEM_FILE.sql
		echo "exit;" >> ${CREATETABLE_PATH}insert_into_DSL_EXTRACT_SYSTEM_FILE.sql
		
		cd /data1/DSL
		java DSL -plus ${RUNTARGETOWNER} ESLDEV @${CREATETABLE_PATH}insert_into_DSL_EXTRACT_SYSTEM_FILE.sql > /dev/null 2>&1
		
		echo "Insert into Table DSL_EXTRACT_SYSTEM_FILE Completed --> Table ${TABLE_NAME}, Temp Table ${TEMP_TABLE_NAME}, Staging Table ${STG_TABLE_NAME}"
		echo "Insert into Table DSL_EXTRACT_SYSTEM_FILE Completed --> Table ${TABLE_NAME}, Temp Table ${TEMP_TABLE_NAME}, Staging Table ${STG_TABLE_NAME}" >> $p_main$p_log/gen_create_table_full_step_${LOGTIME}.log
		
		rm -f ${CREATETABLE_PATH}insert_into_DSL_EXTRACT_SYSTEM_FILE.sql

##### GEN RULE BASE #####		
		cat << EOF > ${RULEBASE_PATH}gen_key_rulebase_${STG_TABLE_NAME}.sql
set feedback off
set trimspool on
set heading off
set echo off
set linesize 4000
set pages

spool ${RULEBASE_PATH}key_rulebase_${STG_TABLE_NAME}.txt

select listagg('SRC.'||a.column_name,',') within group (order by a.position) column_name
from ALL_CONS_COLUMNS a
where a.owner = '${TARGETOWNER}'
and a.table_name = '${STG_TABLE_NAME}'
and exists
(select 1
from ALL_CONSTRAINTS b
where a.owner = b.owner
and a.constraint_name = b.constraint_name
and b.constraint_type = 'P')
group by a.constraint_name;

spool off

exit;
EOF
		
		cd /data1/DSL
		java DSL -plus ${RUNTARGETOWNER} ESLDEV @${RULEBASE_PATH}gen_key_rulebase_${STG_TABLE_NAME}.sql > /dev/null 2>&1
		
		rm -f ${RULEBASE_PATH}gen_key_rulebase_${STG_TABLE_NAME}.sql
		
		cat << EOF > ${RULEBASE_PATH}gen_nonkey_rulebase_${STG_TABLE_NAME}.sql
set feedback off
set trimspool on
set heading off
set echo off
set linesize 4000
set pages

spool ${RULEBASE_PATH}nonkey_rulebase_${STG_TABLE_NAME}.txt

select listagg('SRC.'||a.column_name,'||') within group (order by a.column_id)
from all_tab_columns a
where a.owner = '${TARGETOWNER}'
and a.table_name = '${STG_TABLE_NAME}'
and a.column_name not in ('DATA_DATE','UPD_USER','UPD_DATE','CREATE_BY','CREATE_DATE','PPN_TM','SRC_FILE_NAME')
and not exists
(select 1
from (select a.column_name column_name
from ALL_CONS_COLUMNS a
where a.owner = '${TARGETOWNER}'
and a.table_name = '${STG_TABLE_NAME}'
and exists
(select 1
from ALL_CONSTRAINTS b
where a.owner = b.owner
and a.constraint_name = b.constraint_name
and b.constraint_type = 'P')) b
where a.column_name = b.column_name)
group by a.owner,a.table_name;

spool off

exit;
EOF
		
		cd /data1/DSL
		java DSL -plus ${RUNTARGETOWNER} ESLDEV @${RULEBASE_PATH}gen_nonkey_rulebase_${STG_TABLE_NAME}.sql > /dev/null 2>&1
		
		rm -f ${RULEBASE_PATH}gen_nonkey_rulebase_${STG_TABLE_NAME}.sql
		
		cat << EOF > ${RULEBASE_PATH}gen_key_rulebase_isnull_${STG_TABLE_NAME}.sql
set feedback off
set trimspool on
set heading off
set echo off
set linesize 4000
set pages

spool ${RULEBASE_PATH}key_rulebase_isnull_${STG_TABLE_NAME}.txt

select case when rownum = 1 then 'JSRC.'||a.column_name||' IS NULL'
else 'AND JSRC.'||a.column_name||' IS NULL' end
from ALL_CONS_COLUMNS a
where a.owner = '${TARGETOWNER}'
and a.table_name = '${STG_TABLE_NAME}'
and exists
(select 1
from ALL_CONSTRAINTS b
where a.owner = b.owner
and a.constraint_name = b.constraint_name
and b.constraint_type = 'P')
order by a.position;

spool off

exit;
EOF
		
		cd /data1/DSL
		java DSL -plus ${RUNTARGETOWNER} ESLDEV @${RULEBASE_PATH}gen_key_rulebase_isnull_${STG_TABLE_NAME}.sql > /dev/null 2>&1
		
		rm -f ${RULEBASE_PATH}gen_key_rulebase_isnull_${STG_TABLE_NAME}.sql
		
		cat << EOF > ${RULEBASE_PATH}gen_key_rulebase_join_${STG_TABLE_NAME}.sql
set feedback off
set trimspool on
set heading off
set echo off
set linesize 4000
set pages

spool ${RULEBASE_PATH}key_rulebase_join_${STG_TABLE_NAME}.txt

select case when rownum = 1 then 'JSRC.'||a.column_name||' = SRC.'||a.column_name
else 'AND JSRC.'||a.column_name||' = SRC.'||a.column_name end
from ALL_CONS_COLUMNS a
where a.owner = '${TARGETOWNER}'
and a.table_name = '${STG_TABLE_NAME}'
and exists
(select 1
from ALL_CONSTRAINTS b
where a.owner = b.owner
and a.constraint_name = b.constraint_name
and b.constraint_type = 'P')
order by a.position;

spool off

exit;
EOF
		
		cd /data1/DSL
		java DSL -plus ${RUNTARGETOWNER} ESLDEV @${RULEBASE_PATH}gen_key_rulebase_join_${STG_TABLE_NAME}.sql > /dev/null 2>&1
		
		rm -f ${RULEBASE_PATH}gen_key_rulebase_join_${STG_TABLE_NAME}.sql
		
		PK_SRC=$(cat ${RULEBASE_PATH}key_rulebase_${STG_TABLE_NAME}.txt)
		PK_JSRC=$(echo ${PK_SRC} | sed 's/SRC./JSRC./g')
		PK_TRGT=$(echo ${PK_SRC} | sed 's/SRC./TRGT./g')
		PK_NOSRC=$(echo ${PK_SRC} | sed 's/SRC.//g')
		NONPK_SRC=$(cat ${RULEBASE_PATH}nonkey_rulebase_${STG_TABLE_NAME}.txt)
		PK_ISNULL=$(cat ${RULEBASE_PATH}key_rulebase_isnull_${STG_TABLE_NAME}.txt)
		PK_SRCISNULL=$(echo ${PK_ISNULL} | sed 's/JSRC./SRC./g')
		PK_JOIN=$(cat ${RULEBASE_PATH}key_rulebase_join_${STG_TABLE_NAME}.txt)
		
		rm -f ${RULEBASE_PATH}key_rulebase_${STG_TABLE_NAME}.txt
		rm -f ${RULEBASE_PATH}nonkey_rulebase_${STG_TABLE_NAME}.txt
		rm -f ${RULEBASE_PATH}key_rulebase_join_${STG_TABLE_NAME}.txt
		rm -f ${RULEBASE_PATH}key_rulebase_isnull_${STG_TABLE_NAME}.txt

		cat << EOF > ${RULEBASE_PATH}gen_export_type_${STG_TABLE_NAME}.sql
set feedback off
set trimspool on
set heading off
set echo off
set linesize 4000
set pages

spool ${RULEBASE_PATH}export_type_${STG_TABLE_NAME}.txt

select export_type
from DSL_EXTRACT_SYSTEM_FILE
where stg_table_name = '${STG_TABLE_NAME}';

spool off

exit;
EOF
		
		cd /data1/DSL
		java DSL -plus ${RUNTARGETOWNER} ESLDEV @${RULEBASE_PATH}gen_export_type_${STG_TABLE_NAME}.sql > /dev/null 2>&1
		
		rm -f ${RULEBASE_PATH}gen_export_type_${STG_TABLE_NAME}.sql

		EXPORT_TYPE=$(cat ${RULEBASE_PATH}export_type_${STG_TABLE_NAME}.txt)
		
		rm -f ${RULEBASE_PATH}export_type_${STG_TABLE_NAME}.txt
		
		echo "DELETE FROM #SCHEMA#."${DIM_TABLE_NAME}" WHERE START_DATE >= to_date('1111-11-11','yyyy-mm-dd')" > ${RULEBASE_PATH}rulebase_${EXPORT_TYPE}_${DIM_TABLE_NAME}.sql
		echo "" >> ${RULEBASE_PATH}rulebase_${EXPORT_TYPE}_${DIM_TABLE_NAME}.sql
		echo "UPDATE #SCHEMA#."${DIM_TABLE_NAME}" TRGT" >> ${RULEBASE_PATH}rulebase_${EXPORT_TYPE}_${DIM_TABLE_NAME}.sql
		echo "SET" >> ${RULEBASE_PATH}rulebase_${EXPORT_TYPE}_${DIM_TABLE_NAME}.sql
		echo "TRGT.END_DATE = to_date('9999-12-31', 'yyyy-mm-dd')" >> ${RULEBASE_PATH}rulebase_${EXPORT_TYPE}_${DIM_TABLE_NAME}.sql
		echo ", TRGT.RECORD_DELETED_FLAG = 0" >> ${RULEBASE_PATH}rulebase_${EXPORT_TYPE}_${DIM_TABLE_NAME}.sql
		echo "WHERE (TRGT.END_DATE >= to_date('1111-11-11','yyyy-mm-dd') - 1)" >> ${RULEBASE_PATH}rulebase_${EXPORT_TYPE}_${DIM_TABLE_NAME}.sql
		echo "AND TRGT.END_DATE <> to_date('9999-12-31', 'yyyy-mm-dd')" >> ${RULEBASE_PATH}rulebase_${EXPORT_TYPE}_${DIM_TABLE_NAME}.sql
		echo "" >> ${RULEBASE_PATH}rulebase_${EXPORT_TYPE}_${DIM_TABLE_NAME}.sql
		echo "UPDATE #SCHEMA#."${DIM_TABLE_NAME}" TRGT" >> ${RULEBASE_PATH}rulebase_${EXPORT_TYPE}_${DIM_TABLE_NAME}.sql
		echo "SET TRGT.RECORD_DELETED_FLAG = 1" >> ${RULEBASE_PATH}rulebase_${EXPORT_TYPE}_${DIM_TABLE_NAME}.sql
		echo ",TRGT.END_DATE = TRUNC(TO_DATE('1111-11-11', 'YYYY-MM-DD')) - 1" >> ${RULEBASE_PATH}rulebase_${EXPORT_TYPE}_${DIM_TABLE_NAME}.sql
		echo ",TRGT.PPN_TM = sysdate" >> ${RULEBASE_PATH}rulebase_${EXPORT_TYPE}_${DIM_TABLE_NAME}.sql
		if [ "${EXPORT_TYPE}" == "DEL" ]
		then
			echo "WHERE ("${PK_TRGT}") IN (" >> ${RULEBASE_PATH}rulebase_${EXPORT_TYPE}_${DIM_TABLE_NAME}.sql
			echo "SELECT "${PK_SRC} >> ${RULEBASE_PATH}rulebase_${EXPORT_TYPE}_${DIM_TABLE_NAME}.sql
			echo "FROM" >> ${RULEBASE_PATH}rulebase_${EXPORT_TYPE}_${DIM_TABLE_NAME}.sql
			echo "(" >> ${RULEBASE_PATH}rulebase_${EXPORT_TYPE}_${DIM_TABLE_NAME}.sql
			echo "SELECT *" >> ${RULEBASE_PATH}rulebase_${EXPORT_TYPE}_${DIM_TABLE_NAME}.sql
			echo "FROM #SCHEMA#."${STG_TABLE_NAME} >> ${RULEBASE_PATH}rulebase_${EXPORT_TYPE}_${DIM_TABLE_NAME}.sql
			echo "WHERE to_date(DATA_DATE,'yyyy-mm-dd') >= to_date('1111-11-11','yyyy-mm-dd')" >> ${RULEBASE_PATH}rulebase_${EXPORT_TYPE}_${DIM_TABLE_NAME}.sql
			echo ") SRC" >> ${RULEBASE_PATH}rulebase_${EXPORT_TYPE}_${DIM_TABLE_NAME}.sql
			echo "LEFT OUTER JOIN #SCHEMA#."${DIM_TABLE_NAME}" JSRC" >> ${RULEBASE_PATH}rulebase_${EXPORT_TYPE}_${DIM_TABLE_NAME}.sql
			echo "ON (${PK_JOIN})" >> ${RULEBASE_PATH}rulebase_${EXPORT_TYPE}_${DIM_TABLE_NAME}.sql
			echo "WHERE (JSRC.HASH_CHECK <> STANDARD_HASH("${NONPK_SRC}",'MD5'))" >> ${RULEBASE_PATH}rulebase_${EXPORT_TYPE}_${DIM_TABLE_NAME}.sql
			echo "AND (JSRC.RECORD_DELETED_FLAG = 0)" >> ${RULEBASE_PATH}rulebase_${EXPORT_TYPE}_${DIM_TABLE_NAME}.sql
			echo ")" >> ${RULEBASE_PATH}rulebase_${EXPORT_TYPE}_${DIM_TABLE_NAME}.sql
		elif [ "${EXPORT_TYPE}" == "FULL" ]
		then
			echo "WHERE ("${PK_TRGT}") IN (" >> ${RULEBASE_PATH}rulebase_${EXPORT_TYPE}_${DIM_TABLE_NAME}.sql
			echo "SELECT ${PK_JSRC}" >> ${RULEBASE_PATH}rulebase_${EXPORT_TYPE}_${DIM_TABLE_NAME}.sql
			echo "FROM #SCHEMA#."${STG_TABLE_NAME}" SRC" >> ${RULEBASE_PATH}rulebase_${EXPORT_TYPE}_${DIM_TABLE_NAME}.sql
			echo "RIGHT OUTER JOIN #SCHEMA#."${DIM_TABLE_NAME}" JSRC" >> ${RULEBASE_PATH}rulebase_${EXPORT_TYPE}_${DIM_TABLE_NAME}.sql
			echo "ON (${PK_JOIN})" >> ${RULEBASE_PATH}rulebase_${EXPORT_TYPE}_${DIM_TABLE_NAME}.sql
			echo "AND JSRC.HASH_CHECK = STANDARD_HASH("${NONPK_SRC}",'MD5')" >> ${RULEBASE_PATH}rulebase_${EXPORT_TYPE}_${DIM_TABLE_NAME}.sql
			echo "WHERE "${PK_SRCISNULL} >> ${RULEBASE_PATH}rulebase_${EXPORT_TYPE}_${DIM_TABLE_NAME}.sql
			echo "AND JSRC.RECORD_DELETED_FLAG = 0)" >> ${RULEBASE_PATH}rulebase_${EXPORT_TYPE}_${DIM_TABLE_NAME}.sql
		fi
		echo "" >> ${RULEBASE_PATH}rulebase_${EXPORT_TYPE}_${DIM_TABLE_NAME}.sql
		
		cat << EOF > ${RULEBASE_PATH}gen_step4_1_${STG_TABLE_NAME}.sql
set feedback off
set trimspool on
set heading off
set echo off
set linesize 4000
set pages

spool ${RULEBASE_PATH}step4_1_${STG_TABLE_NAME}.txt

select text
from (select 'INSERT INTO #SCHEMA#.${DIM_TABLE_NAME}' text,1 rowno from dual
union
select '(' text,2 rowno from dual
union
select 'HASH_CHECK,' text,999994 rowno from dual
union
select 'START_DATE,' text,999995 rowno from dual
union
select 'END_DATE,' text,999996 rowno from dual
union
select 'RECORD_DELETED_FLAG,' text,999997 rowno from dual
union
select 'PPN_TM' text,999998 rowno from dual
union
select ')' text,999999 rowno from dual
union
select text,rownum+2 rowno
from (select column_name||',' text
from all_tab_columns 
where owner = '${TARGETOWNER}' 
and table_name = '${STG_TABLE_NAME}'
and column_name not in ('DATA_DATE','UPD_USER','UPD_DATE','CREATE_BY','CREATE_DATE','PPN_TM','SRC_FILE_NAME')
order by column_id))
order by rowno;

spool off

exit;
EOF
		
		cd /data1/DSL
		java DSL -plus ${RUNTARGETOWNER} ESLDEV @${RULEBASE_PATH}gen_step4_1_${STG_TABLE_NAME}.sql > /dev/null 2>&1
		
		rm -f ${RULEBASE_PATH}gen_step4_1_${STG_TABLE_NAME}.sql
		
		cat << EOF > ${RULEBASE_PATH}gen_step4_2_${STG_TABLE_NAME}.sql
set feedback off
set trimspool on
set heading off
set echo off
set linesize 4000
set pages

spool ${RULEBASE_PATH}step4_2_${STG_TABLE_NAME}.txt

select text
from (select 'SELECT ' text,1 rowno from dual
union
select 'STANDARD_HASH(${NONPK_SRC},''MD5'') AS HASH_CHECK,' text,999985 rowno from dual
union
select 'TO_DATE(''1111-11-11'',''YYYY-MM-DD'') AS START_DATE,' text,999986 rowno from dual
union
select 'TO_DATE(''9999-12-31'',''YYYY-MM-DD'') AS END_DATE,' text,999987 rowno from dual
union
select '0 AS RECORD_DELETED_FLAG,' text,999988 rowno from dual
union
select 'SYSDATE AS PPN_TM' text,999989 rowno from dual
union
select 'FROM (' text,999990 rowno from dual
union
select 'SELECT *' text,999991 rowno from dual
union
select 'FROM #SCHEMA#.${STG_TABLE_NAME}' text,999992 rowno from dual
union
select 'WHERE TO_DATE(DATA_DATE,''YYYY-MM-DD'') >= TO_DATE(''1111-11-11'',''YYYY-MM-DD'')' text,999993 rowno from dual
union
select ')SRC' text,999994 rowno from dual
union
select 'LEFT OUTER JOIN #SCHEMA#.${DIM_TABLE_NAME} JSRC' text,999995 rowno from dual
union
select 'ON (${PK_JOIN})' text,999996 rowno from dual
union
select 'AND JSRC.HASH_CHECK = STANDARD_HASH(${NONPK_SRC},''MD5'')' text,999997 rowno from dual
union
select 'AND JSRC.RECORD_DELETED_FLAG=0' text,999998 rowno from dual
union
select 'WHERE ${PK_ISNULL}' text,999999 rowno from dual
union
select text,rownum+1 rowno
from (select 'SRC.'||column_name||',' text
from all_tab_columns 
where owner = '${TARGETOWNER}' 
and table_name = '${STG_TABLE_NAME}'
and column_name not in ('DATA_DATE','UPD_USER','UPD_DATE','CREATE_BY','CREATE_DATE','PPN_TM','SRC_FILE_NAME')
order by column_id))
order by rowno;

spool off

exit;
EOF
		
		cd /data1/DSL
		java DSL -plus ${RUNTARGETOWNER} ESLDEV @${RULEBASE_PATH}gen_step4_2_${STG_TABLE_NAME}.sql > /dev/null 2>&1
		
		rm -f ${RULEBASE_PATH}gen_step4_2_${STG_TABLE_NAME}.sql
		
		cat ${RULEBASE_PATH}step4_1_${STG_TABLE_NAME}.txt >> ${RULEBASE_PATH}rulebase_${EXPORT_TYPE}_${DIM_TABLE_NAME}.sql
		cat ${RULEBASE_PATH}step4_2_${STG_TABLE_NAME}.txt >> ${RULEBASE_PATH}rulebase_${EXPORT_TYPE}_${DIM_TABLE_NAME}.sql
		
		echo 'Gen Rule Base Completed --> '${RULEBASE_PATH}rulebase_${EXPORT_TYPE}_${DIM_TABLE_NAME}.sql
		echo 'Gen Rule Base Completed --> '${RULEBASE_PATH}rulebase_${EXPORT_TYPE}_${DIM_TABLE_NAME}.sql >> $p_main$p_log/gen_create_table_full_step_${LOGTIME}.log

		rm -f ${RULEBASE_PATH}step4_1_${STG_TABLE_NAME}.txt
		rm -f ${RULEBASE_PATH}step4_2_${STG_TABLE_NAME}.txt

		echo '-----------------------------------------------------------------------------------'
		echo '-----------------------------------------------------------------------------------' >> $p_main$p_log/gen_create_table_full_step_${LOGTIME}.log
	done < ${TABLENAME_PATH}${TABLENAME_FILE}
	
	echo 'Finish Time : '`date +'%Y-%m-%d %H:%M:%S'`
	echo 'Finish Time : '`date +'%Y-%m-%d %H:%M:%S'` >> $p_main$p_log/gen_create_table_full_step_${LOGTIME}.log
	echo '-----------------------------------------------------------------------------------'
	echo '-----------------------------------------------------------------------------------' >> $p_main$p_log/gen_create_table_full_step_${LOGTIME}.log
else
	echo '-----------------------------------------------------------------------------------'
	echo "Please Pass Parameter : Source Owner & Target Owner!!"
	echo '-----------------------------------------------------------------------------------'
fi
