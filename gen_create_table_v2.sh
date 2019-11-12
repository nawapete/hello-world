#!/bin/ksh

########################################################################################
#  
#  Program Name     : gen_create_table_v2.sh
#  Description      : Gen Script Create Temp Table & Staging Table
#  Created by       : Aekavut V.
#  Create Date      : 20/08/2019
#   
########################################################################################
#

. /data1/misapps/dsl_dev/script/MISBI.cfg

#if [ "$1" != "" ] && [ "$2" != "" ]
if [ "$1" != "" ]
then
	LOGTIME=`date +'%Y%m%d_%H%M%S'`
	echo 'Start Time : '`date +'%Y-%m-%d %H:%M:%S'`
	echo 'Log File : '$p_main$p_log/gen_create_table_v2_${LOGTIME}.log
	echo 'Start Time : '`date +'%Y-%m-%d %H:%M:%S'` > $p_main$p_log/gen_create_table_v2_${LOGTIME}.log

	typeset -u OWNER=${1}
	typeset -l RUNOWNER=${1}
	#typeset -u TARGETOWNER=${2}
	#typeset -l RUNTARGETOWNER=${2}
	TABLENAME_PATH=$p_main$p_cfg/
	TABLENAME_FILE=${RUNOWNER}_table_name_v2.txt
	CREATETABLE_PATH=$p_main$p_tmp/create_table/
	#SQLLDR_PATH=$p_main$p_tmp/sqlldr_script/

	while read line
	do
		TABLE_NAME=$(echo ${line} | awk -F'|' '{print $1}')
		TEMP_TABLE_NAME=$(echo ${line} | awk -F'|' '{print $2}')
		STG_TABLE_NAME=$(echo ${line} | awk -F'|' '{print $3}')
		DIM_TABLE_NAME=$(echo ${line} | awk -F'|' '{print $4}')

		cat << EOF > ${CREATETABLE_PATH}gen_create_table_${TEMP_TABLE_NAME}.sql
set feedback off
set trimspool on
set heading off
set echo off
set linesize 2000
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
from (select column_name||
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

select case when logging = 'YES' then 'create index '||replace(index_name,index_name,'IDX_${TEMP_TABLE_NAME}_')||rownum||' on ${TEMP_TABLE_NAME} '||column_name||';'
else 'create index '||replace(index_name,index_name,'IDX_${TEMP_TABLE_NAME}_')||rownum||' on ${TEMP_TABLE_NAME} '||column_name||' nologging;' end
from (select b.index_name index_name,a.logging logging,'('||listagg(b.column_name,',') within group (order by b.index_name,b.column_position)||')' column_name
from ALL_INDEXES a
inner join ALL_IND_COLUMNS b
on a.table_owner = b.table_owner
and a.table_name = b.table_name
and a.index_name = b.index_name
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
order by b.index_name,a.logging);

select case when logging = 'YES' then 'create unique index '||replace(index_name,index_name,'UIDX_${TEMP_TABLE_NAME}_')||rownum||' on ${TEMP_TABLE_NAME} '||column_name||';'
else 'create unique index '||replace(index_name,index_name,'UIDX_${TEMP_TABLE_NAME}_')||rownum||' on ${TEMP_TABLE_NAME} '||column_name||' nologging;' end
from (select b.index_name index_name,a.logging logging,'('||listagg(b.column_name,',') within group (order by b.index_name,b.column_position)||')' column_name
from ALL_INDEXES a
inner join ALL_IND_COLUMNS b
on a.table_owner = b.table_owner
and a.table_name = b.table_name
and a.index_name = b.index_name
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
order by b.index_name,a.logging);

select 'alter table ${TEMP_TABLE_NAME} add constraint '||replace(constraint_name,constraint_name,'PK_${TEMP_TABLE_NAME}')||' primary key '||column_name||';'
from (select a.constraint_name constraint_name,'('||listagg(a.column_name,',') within group (order by a.constraint_name,a.position)||')' column_name
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

select 'alter index '||replace(index_name,index_name,'PK_${TEMP_TABLE_NAME}')||' nologging;'
from (select a.index_name index_name
from ALL_INDEXES a
where a.table_owner = '${OWNER}'
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
		
		echo 'Gen Completed --> '${CREATETABLE_PATH}create_table_${TEMP_TABLE_NAME}.sql
		echo 'Gen Completed --> '${CREATETABLE_PATH}create_table_${TEMP_TABLE_NAME}.sql >> $p_main$p_log/gen_create_table_v2_${LOGTIME}.log
		
		rm -f ${CREATETABLE_PATH}gen_create_table_${TEMP_TABLE_NAME}.sql
	
		cat << EOF > ${CREATETABLE_PATH}gen_create_table_${STG_TABLE_NAME}.sql
set feedback off
set trimspool on
set heading off
set echo off
set linesize 2000
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
from (select column_name||
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

select case when logging = 'YES' then 'create index '||replace(index_name,index_name,'IDX_${STG_TABLE_NAME}_')||rownum||' on ${STG_TABLE_NAME} '||column_name||';'
else 'create index '||replace(index_name,index_name,'IDX_${STG_TABLE_NAME}_')||rownum||' on ${STG_TABLE_NAME} '||column_name||' nologging;' end
from (select b.index_name index_name,a.logging logging,'('||listagg(b.column_name,',') within group (order by b.index_name,b.column_position)||')' column_name
from ALL_INDEXES a
inner join ALL_IND_COLUMNS b
on a.table_owner = b.table_owner
and a.table_name = b.table_name
and a.index_name = b.index_name
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
order by b.index_name,a.logging);

select case when logging = 'YES' then 'create unique index '||replace(index_name,index_name,'UIDX_${STG_TABLE_NAME}_')||rownum||' on ${STG_TABLE_NAME} '||column_name||';'
else 'create unique index '||replace(index_name,index_name,'UIDX_${STG_TABLE_NAME}_')||rownum||' on ${STG_TABLE_NAME} '||column_name||' nologging;' end
from (select b.index_name index_name,a.logging logging,'('||listagg(b.column_name,',') within group (order by b.index_name,b.column_position)||')' column_name
from ALL_INDEXES a
inner join ALL_IND_COLUMNS b
on a.table_owner = b.table_owner
and a.table_name = b.table_name
and a.index_name = b.index_name
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
order by b.index_name,a.logging);

select 'alter table ${STG_TABLE_NAME} add constraint '||replace(constraint_name,constraint_name,'PK_${STG_TABLE_NAME}')||' primary key '||column_name||';'
from (select a.constraint_name constraint_name,'('||listagg(a.column_name,',') within group (order by a.constraint_name,a.position)||')' column_name
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

select 'alter index '||replace(index_name,index_name,'PK_${STG_TABLE_NAME}')||' nologging;'
from (select a.index_name index_name
from ALL_INDEXES a
where a.table_owner = '${OWNER}'
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
		
		echo 'Gen Completed --> '${CREATETABLE_PATH}create_table_${STG_TABLE_NAME}.sql
		echo 'Gen Completed --> '${CREATETABLE_PATH}create_table_${STG_TABLE_NAME}.sql >> $p_main$p_log/gen_create_table_v2_${LOGTIME}.log
		
		rm -f ${CREATETABLE_PATH}gen_create_table_${STG_TABLE_NAME}.sql
		
		cat << EOF > ${CREATETABLE_PATH}gen_create_table_${DIM_TABLE_NAME}.sql
set feedback off
set trimspool on
set heading off
set echo off
set linesize 2000
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
from (select column_name||
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

select case when logging = 'YES' then 'create index '||replace(index_name,index_name,'IDX_${DIM_TABLE_NAME}_')||rownum||' on ${DIM_TABLE_NAME} '||column_name||';'
else 'create index '||replace(index_name,index_name,'IDX_${DIM_TABLE_NAME}_')||rownum||' on ${DIM_TABLE_NAME} '||column_name||' nologging;' end
from (select b.index_name index_name,a.logging logging,'('||listagg(b.column_name,',') within group (order by b.index_name,b.column_position)||')' column_name
from ALL_INDEXES a
inner join ALL_IND_COLUMNS b
on a.table_owner = b.table_owner
and a.table_name = b.table_name
and a.index_name = b.index_name
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
order by b.index_name,a.logging);

select case when logging = 'YES' then 'create unique index '||replace(index_name,index_name,'UIDX_${DIM_TABLE_NAME}_')||rownum||' on ${DIM_TABLE_NAME} '||column_name||';'
else 'create unique index '||replace(index_name,index_name,'UIDX_${DIM_TABLE_NAME}_')||rownum||' on ${DIM_TABLE_NAME} '||column_name||' nologging;' end
from (select b.index_name index_name,a.logging logging,'('||listagg(b.column_name,',') within group (order by b.index_name,b.column_position)||')' column_name
from ALL_INDEXES a
inner join ALL_IND_COLUMNS b
on a.table_owner = b.table_owner
and a.table_name = b.table_name
and a.index_name = b.index_name
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
order by b.index_name,a.logging);

select 'alter table ${DIM_TABLE_NAME} add constraint '||replace(constraint_name,constraint_name,'PK_${DIM_TABLE_NAME}')||' primary key '||column_name||';'
from (select a.constraint_name constraint_name,'('||listagg(a.column_name,',') within group (order by a.constraint_name,a.position)||')' column_name
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

select 'alter index '||replace(index_name,index_name,'PK_${DIM_TABLE_NAME}')||' nologging;'
from (select a.index_name index_name
from ALL_INDEXES a
where a.table_owner = '${OWNER}'
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
		
		echo 'Gen Completed --> '${CREATETABLE_PATH}create_table_${DIM_TABLE_NAME}.sql
		echo 'Gen Completed --> '${CREATETABLE_PATH}create_table_${DIM_TABLE_NAME}.sql >> $p_main$p_log/gen_create_table_v2_${LOGTIME}.log
		
		rm -f ${CREATETABLE_PATH}gen_create_table_${DIM_TABLE_NAME}.sql
		
#		cat << EOF > ${CREATETABLE_PATH}gen_check_table_${TEMP_TABLE_NAME}.sql
#set feedback off
#set trimspool on
#set heading off
#set echo off
#set linesize 2000
#set pages
#
#spool ${CREATETABLE_PATH}check_table_${TEMP_TABLE_NAME}.txt
#
#select table_name
#from user_tables
#where table_name = '${TEMP_TABLE_NAME}';
#
#spool off
#
#exit;
#EOF
#			
#		cd /data1/DSL
#		java DSL -plus ${RUNTARGETOWNER} ESLDEV @${CREATETABLE_PATH}gen_check_table_${TEMP_TABLE_NAME}.sql > /dev/null 2>&1
#		
#		CHK_TEMP_TABLE_NAME=$(cat ${CREATETABLE_PATH}check_table_${TEMP_TABLE_NAME}.txt)
#		
#		if [ "${CHK_TEMP_TABLE_NAME}" == "${TEMP_TABLE_NAME}" ]
#		then
#			echo "drop table ${TEMP_TABLE_NAME};" > ${CREATETABLE_PATH}drop_table_${TEMP_TABLE_NAME}.sql
#			echo "exit;" >> ${CREATETABLE_PATH}drop_table_${TEMP_TABLE_NAME}.sql
#			cd /data1/DSL
#			java DSL -plus ${RUNTARGETOWNER} ESLDEV @${CREATETABLE_PATH}drop_table_${TEMP_TABLE_NAME}.sql > /dev/null 2>&1
#			echo "Drop Table Completed --> Table ${TEMP_TABLE_NAME}"
#			echo "Drop Table Completed --> Table ${TEMP_TABLE_NAME}" >> $p_main$p_log/gen_create_table_v2_${LOGTIME}.log
#			java DSL -plus ${RUNTARGETOWNER} ESLDEV @${CREATETABLE_PATH}create_table_${TEMP_TABLE_NAME}.sql > /dev/null 2>&1
#			echo "Create Table Completed --> Table ${TEMP_TABLE_NAME}"
#			echo "Create Table Completed --> Table ${TEMP_TABLE_NAME}" >> $p_main$p_log/gen_create_table_v2_${LOGTIME}.log
#			rm -f ${CREATETABLE_PATH}drop_table_${TEMP_TABLE_NAME}.sql
#		else
#			cd /data1/DSL
#			java DSL -plus ${RUNTARGETOWNER} ESLDEV @${CREATETABLE_PATH}create_table_${TEMP_TABLE_NAME}.sql > /dev/null 2>&1
#			echo "Create Table Completed --> Table ${TEMP_TABLE_NAME}"
#			echo "Create Table Completed --> Table ${TEMP_TABLE_NAME}" >> $p_main$p_log/gen_create_table_v2_${LOGTIME}.log
#		fi
#		
#		rm -f ${CREATETABLE_PATH}gen_check_table_${TEMP_TABLE_NAME}.sql
#		rm -f ${CREATETABLE_PATH}check_table_${TEMP_TABLE_NAME}.txt
#		
#		cat << EOF > ${CREATETABLE_PATH}gen_check_table_${STG_TABLE_NAME}.sql
#set feedback off
#set trimspool on
#set heading off
#set echo off
#set linesize 2000
#set pages
#
#spool ${CREATETABLE_PATH}check_table_${STG_TABLE_NAME}.txt
#
#select table_name
#from user_tables
#where table_name = '${STG_TABLE_NAME}';
#
#spool off
#
#exit;
#EOF
#			
#		cd /data1/DSL
#		java DSL -plus ${RUNTARGETOWNER} ESLDEV @${CREATETABLE_PATH}gen_check_table_${STG_TABLE_NAME}.sql > /dev/null 2>&1
#		
#		CHK_STG_TABLE_NAME=$(cat ${CREATETABLE_PATH}check_table_${STG_TABLE_NAME}.txt)
#		
#		if [ "${CHK_STG_TABLE_NAME}" == "${STG_TABLE_NAME}" ]
#		then
#			echo "drop table ${STG_TABLE_NAME};" > ${CREATETABLE_PATH}drop_table_${STG_TABLE_NAME}.sql
#			echo "exit;" >> ${CREATETABLE_PATH}drop_table_${STG_TABLE_NAME}.sql
#			cd /data1/DSL
#			java DSL -plus ${RUNTARGETOWNER} ESLDEV @${CREATETABLE_PATH}drop_table_${STG_TABLE_NAME}.sql > /dev/null 2>&1
#			echo "Drop Table Completed --> Table ${STG_TABLE_NAME}"
#			echo "Drop Table Completed --> Table ${STG_TABLE_NAME}" >> $p_main$p_log/gen_create_table_v2_${LOGTIME}.log
#			java DSL -plus ${RUNTARGETOWNER} ESLDEV @${CREATETABLE_PATH}create_table_${STG_TABLE_NAME}.sql > /dev/null 2>&1
#			echo "Create Table Completed --> Table ${STG_TABLE_NAME}"
#			echo "Create Table Completed --> Table ${STG_TABLE_NAME}" >> $p_main$p_log/gen_create_table_v2_${LOGTIME}.log
#			rm -f ${CREATETABLE_PATH}drop_table_${STG_TABLE_NAME}.sql
#		else
#			cd /data1/DSL
#			java DSL -plus ${RUNTARGETOWNER} ESLDEV @${CREATETABLE_PATH}create_table_${STG_TABLE_NAME}.sql > /dev/null 2>&1
#			echo "Create Table Completed --> Table ${STG_TABLE_NAME}"
#			echo "Create Table Completed --> Table ${STG_TABLE_NAME}" >> $p_main$p_log/gen_create_table_v2_${LOGTIME}.log
#		fi
#		
#		rm -f ${CREATETABLE_PATH}gen_check_table_${STG_TABLE_NAME}.sql
#		rm -f ${CREATETABLE_PATH}check_table_${STG_TABLE_NAME}.txt
#		
#		cat << EOF > ${SQLLDR_PATH}gen_sqlldr_${TEMP_TABLE_NAME}.sql
#set feedback off
#set trimspool on
#set heading off
#set echo off
#set linesize 2000
#set pages
#
#spool ${SQLLDR_PATH}sqlldr_${TEMP_TABLE_NAME}.txt
#
#select case when rowno = (select count(*)+7 from ALL_TAB_COLUMNS where owner = '${TARGETOWNER}' and table_name = '${TEMP_TABLE_NAME}') then rtrim(text,',') else text end
#from (select 'OPTIONS(skip=1)' text,1 rowno from dual
#union
#select 'LOAD DATA' text,2 rowno from dual
#union
#select 'CHARACTERSET UTF8' text,3 rowno from dual
#union
#select 'INFILE '||'''dummy.txt''' text,4 rowno from dual
#union
#select 'INTO TABLE ${TARGETOWNER}.'||'${TEMP_TABLE_NAME}' text,5 rowno from dual
#union
#select 'FIELDS TERMINATED BY '||'''|''' text,6 rowno from dual
#union
#select 'TRAILING NULLCOLS (' text,7 rowno from dual
#union
#select ')' text,999999 rowno from dual
#union
#select text,rownum+7 rowno
#from (select case when data_type = 'CHAR' then column_name||' '||data_type||' "TRIM(:'||column_name||')",'
#when data_type = 'VARCHAR2' then column_name||' '||'"TRIM(:'||column_name||')",'
#when data_type = 'NUMBER' then column_name||','
#when data_type = 'DATE' then column_name||' TIMESTAMP "YYYYMMDD HH24MISSFF",'
#when substr(data_type,1,9) = 'TIMESTAMP' then column_name||' TIMESTAMP "YYYYMMDD HH24MISSFF",'
#when data_type = 'CLOB' then column_name||' CHAR(4000),'
#end text
#from ALL_TAB_COLUMNS 
#where owner = '${TARGETOWNER}'
#and table_name = '${TEMP_TABLE_NAME}'
#order by column_id))
#order by rowno;
#
#spool off
#
#exit;
#EOF
#
#		rm -f ${SQLLDR_PATH}sqlldr_${TEMP_TABLE_NAME}.txt
#
#		cd /data1/DSL
#		java DSL -plus ${RUNTARGETOWNER} ESLDEV @${SQLLDR_PATH}gen_sqlldr_${TEMP_TABLE_NAME}.sql > /dev/null 2>&1
#		
#		echo 'Gen Completed --> '${SQLLDR_PATH}sqlldr_${TEMP_TABLE_NAME}.txt
#		echo 'Gen Completed --> '${SQLLDR_PATH}sqlldr_${TEMP_TABLE_NAME}.txt >> $p_main$p_log/gen_create_table_v2_${LOGTIME}.log
#		
#		rm -f ${SQLLDR_PATH}gen_sqlldr_${TEMP_TABLE_NAME}.sql
	done < ${TABLENAME_PATH}${TABLENAME_FILE}
	
	echo 'Finish Time : '`date +'%Y-%m-%d %H:%M:%S'`
	echo 'Finish Time : '`date +'%Y-%m-%d %H:%M:%S'` >> $p_main$p_log/gen_create_table_v2_${LOGTIME}.log
else
	#echo "Please Pass Parameter : Source Owner & Target Owner!!"
	echo "Please Pass Parameter : Source Owner!!"
fi
