#!/bin/ksh

########################################################################################
#  
#  Program Name     : register_rulebase.sh
#  Description      : Register rulebase (FULL, DEL)
#  Created by       : Aekavut V.
#  Create Date      : 22/08/2019
#   
########################################################################################
#

. /data1/misapps/dsl_dev/script/MISBI.cfg

if [ "$1" != "" ]
then
	LOGTIME=`date +'%Y%m%d_%H%M%S'`
	echo 'Start Time : '`date +'%Y-%m-%d %H:%M:%S'`
	echo 'Log File : '$p_main$p_log/register_rulebase_${LOGTIME}.log
	echo 'Start Time : '`date +'%Y-%m-%d %H:%M:%S'` > $p_main$p_log/register_rulebase_${LOGTIME}.log

	typeset -u OWNER=$1
	typeset -l RUNOWNER=$1
	TABLENAME_PATH=$p_main$p_cfg/
	TABLENAME_FILE=table_name_rulebase.txt
	RULEBASE_PATH=$p_main$p_tmp/rulebase/

	while read line
	do
		STG_TABLE_NAME=$(echo ${line} | awk -F'|' '{print $1}')
		DIM_TABLE_NAME=$(echo ${line} | awk -F'|' '{print $2}')
	
		cat << EOF > ${RULEBASE_PATH}gen_key_rulebase_${STG_TABLE_NAME}.sql
set feedback off
set trimspool on
set heading off
set echo off
set linesize 2000
set pages

spool ${RULEBASE_PATH}key_rulebase_${STG_TABLE_NAME}.txt

select listagg('SRC.'||a.column_name,',') within group (order by a.position) column_name
from ALL_CONS_COLUMNS a
where a.owner = '${OWNER}'
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
		java DSL -plus ${RUNOWNER} ESLDEV @${RULEBASE_PATH}gen_key_rulebase_${STG_TABLE_NAME}.sql > /dev/null 2>&1
		
		rm -f ${RULEBASE_PATH}gen_key_rulebase_${STG_TABLE_NAME}.sql
		
		cat << EOF > ${RULEBASE_PATH}gen_nonkey_rulebase_${STG_TABLE_NAME}.sql
set feedback off
set trimspool on
set heading off
set echo off
set linesize 2000
set pages

spool ${RULEBASE_PATH}nonkey_rulebase_${STG_TABLE_NAME}.txt

select listagg('SRC.'||a.column_name,'||') within group (order by a.column_id)
from all_tab_columns a
where a.owner = '${OWNER}'
and a.table_name = '${STG_TABLE_NAME}'
and a.column_name not in ('DATA_DATE','UPD_USER','UPD_DATE','CREATE_BY','CREATE_DATE','PPN_TM','SRC_FILE_NAME')
and not exists
(select 1
from (select a.column_name column_name
from ALL_CONS_COLUMNS a
where a.owner = '${OWNER}'
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
		java DSL -plus ${RUNOWNER} ESLDEV @${RULEBASE_PATH}gen_nonkey_rulebase_${STG_TABLE_NAME}.sql > /dev/null 2>&1
		
		rm -f ${RULEBASE_PATH}gen_nonkey_rulebase_${STG_TABLE_NAME}.sql
		
		cat << EOF > ${RULEBASE_PATH}gen_key_rulebase_isnull_${STG_TABLE_NAME}.sql
set feedback off
set trimspool on
set heading off
set echo off
set linesize 2000
set pages

spool ${RULEBASE_PATH}key_rulebase_isnull_${STG_TABLE_NAME}.txt

select case when rownum = 1 then 'JSRC.'||a.column_name||' IS NULL'
else 'AND JSRC.'||a.column_name||' IS NULL' end
from ALL_CONS_COLUMNS a
where a.owner = '${OWNER}'
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
		java DSL -plus ${RUNOWNER} ESLDEV @${RULEBASE_PATH}gen_key_rulebase_isnull_${STG_TABLE_NAME}.sql > /dev/null 2>&1
		
		rm -f ${RULEBASE_PATH}gen_key_rulebase_isnull_${STG_TABLE_NAME}.sql
		
		cat << EOF > ${RULEBASE_PATH}gen_key_rulebase_join_${STG_TABLE_NAME}.sql
set feedback off
set trimspool on
set heading off
set echo off
set linesize 2000
set pages

spool ${RULEBASE_PATH}key_rulebase_join_${STG_TABLE_NAME}.txt

select case when rownum = 1 then 'JSRC.'||a.column_name||' = SRC.'||a.column_name
else 'AND JSRC.'||a.column_name||' = SRC.'||a.column_name end
from ALL_CONS_COLUMNS a
where a.owner = '${OWNER}'
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
		java DSL -plus ${RUNOWNER} ESLDEV @${RULEBASE_PATH}gen_key_rulebase_join_${STG_TABLE_NAME}.sql > /dev/null 2>&1
		
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
set linesize 2000
set pages

spool ${RULEBASE_PATH}export_type_${STG_TABLE_NAME}.txt

select export_type
from DSL_EXTRACT_SYSTEM_FILE
where stg_table_name = '${STG_TABLE_NAME}';

spool off

exit;
EOF
		
		cd /data1/DSL
		java DSL -plus ${RUNOWNER} ESLDEV @${RULEBASE_PATH}gen_export_type_${STG_TABLE_NAME}.sql > /dev/null 2>&1
		
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
set linesize 2000
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
where owner = '${OWNER}' 
and table_name = '${STG_TABLE_NAME}'
and column_name not in ('DATA_DATE','UPD_USER','UPD_DATE','CREATE_BY','CREATE_DATE','PPN_TM','SRC_FILE_NAME')
order by column_id))
order by rowno;

spool off

exit;
EOF
		
		cd /data1/DSL
		java DSL -plus ${RUNOWNER} ESLDEV @${RULEBASE_PATH}gen_step4_1_${STG_TABLE_NAME}.sql > /dev/null 2>&1
		
		rm -f ${RULEBASE_PATH}gen_step4_1_${STG_TABLE_NAME}.sql
		
		cat << EOF > ${RULEBASE_PATH}gen_step4_2_${STG_TABLE_NAME}.sql
set feedback off
set trimspool on
set heading off
set echo off
set linesize 2000
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
where owner = '${OWNER}' 
and table_name = '${STG_TABLE_NAME}'
and column_name not in ('DATA_DATE','UPD_USER','UPD_DATE','CREATE_BY','CREATE_DATE','PPN_TM','SRC_FILE_NAME')
order by column_id))
order by rowno;

spool off

exit;
EOF
		
		cd /data1/DSL
		java DSL -plus ${RUNOWNER} ESLDEV @${RULEBASE_PATH}gen_step4_2_${STG_TABLE_NAME}.sql > /dev/null 2>&1
		
		rm -f ${RULEBASE_PATH}gen_step4_2_${STG_TABLE_NAME}.sql
		
		cat ${RULEBASE_PATH}step4_1_${STG_TABLE_NAME}.txt >> ${RULEBASE_PATH}rulebase_${EXPORT_TYPE}_${DIM_TABLE_NAME}.sql
		cat ${RULEBASE_PATH}step4_2_${STG_TABLE_NAME}.txt >> ${RULEBASE_PATH}rulebase_${EXPORT_TYPE}_${DIM_TABLE_NAME}.sql
		
		echo 'Gen Completed --> '${RULEBASE_PATH}rulebase_${EXPORT_TYPE}_${DIM_TABLE_NAME}.sql
		echo 'Gen Completed --> '${RULEBASE_PATH}rulebase_${EXPORT_TYPE}_${DIM_TABLE_NAME}.sql >> $p_main$p_log/register_rulebase_${LOGTIME}.log
		
		rm -f ${RULEBASE_PATH}step4_1_${STG_TABLE_NAME}.txt
		rm -f ${RULEBASE_PATH}step4_2_${STG_TABLE_NAME}.txt
		
	done < ${TABLENAME_PATH}${TABLENAME_FILE}
	
	echo 'Finish Time : '`date +'%Y-%m-%d %H:%M:%S'`
	echo 'Finish Time : '`date +'%Y-%m-%d %H:%M:%S'` >> $p_main$p_log/register_rulebase_${LOGTIME}.log
else
	echo "Please Pass Parameter : OWNER!!"
fi
