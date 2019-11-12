#!/bin/ksh

########################################################################################
#  
#  Program Name     : register_table_structure_v2.sh
#  Description      : Gen DDL TEMP Table
#  Created by       : Aekavut V.
#  Create Date      : 21/08/2019
#   
########################################################################################
#

. /data1/misapps/dsl_dev/script/MISBI.cfg

if [ "$1" != "" ]
then
	LOGTIME=`date +'%Y%m%d_%H%M%S'`
	echo 'Start Time : '`date +'%Y-%m-%d %H:%M:%S'`
	echo 'Log File : '$p_main$p_log/register_table_structure_v2_${LOGTIME}.log
	echo 'Start Time : '`date +'%Y-%m-%d %H:%M:%S'` > $p_main$p_log/register_table_structure_v2_${LOGTIME}.log
	
	typeset -u OWNER=$1
	typeset -l RUNOWNER=$1
	SQLLDR_PATH=$p_main$p_tmp/sqlldr_script/
	
	cat << EOF > ${SQLLDR_PATH}list_temp_table.sql
set feedback off
set trimspool on
set heading off
set echo off
set linesize 2000
set pages

spool ${SQLLDR_PATH}list_temp_table.txt

select table_name
from all_tab_columns
where owner = '${OWNER}'
and substr(table_name,1,4) = 'TEMP'
group by table_name
order by table_name;

spool off

exit;
EOF
	
	cd /data1/DSL
	java DSL -plus ${RUNOWNER} ESLDEV @${SQLLDR_PATH}list_temp_table.sql > /dev/null 2>&1
	
	rm -f ${SQLLDR_PATH}list_temp_table.sql
	
	while read line
	do
		TEMP_TABLE_NAME=${line}
		
		cat << EOF > ${SQLLDR_PATH}gen_sqlldr_${TEMP_TABLE_NAME}.sql
set feedback off
set trimspool on
set heading off
set echo off
set linesize 2000
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
		java DSL -plus ${RUNOWNER} ESLDEV @${SQLLDR_PATH}gen_sqlldr_${TEMP_TABLE_NAME}.sql > /dev/null 2>&1
		
		cat ${SQLLDR_PATH}sqlldr_${TEMP_TABLE_NAME}.txt | tr '#' '\n' > ${SQLLDR_PATH}sqlldr_${TEMP_TABLE_NAME}.tmp
		rm -f ${SQLLDR_PATH}sqlldr_${TEMP_TABLE_NAME}.txt
		mv ${SQLLDR_PATH}sqlldr_${TEMP_TABLE_NAME}.tmp ${SQLLDR_PATH}sqlldr_${TEMP_TABLE_NAME}.txt
		
		echo 'Gen Completed --> '${SQLLDR_PATH}sqlldr_${TEMP_TABLE_NAME}.txt
		echo 'Gen Completed --> '${SQLLDR_PATH}sqlldr_${TEMP_TABLE_NAME}.txt >> $p_main$p_log/register_table_structure_v2_${LOGTIME}.log
		
		rm -f ${SQLLDR_PATH}gen_sqlldr_${TEMP_TABLE_NAME}.sql
		
	done < ${SQLLDR_PATH}list_temp_table.txt
	
	rm -f ${SQLLDR_PATH}list_temp_table.txt
	
	echo 'Finish Time : '`date +'%Y-%m-%d %H:%M:%S'`
	echo 'Finish Time : '`date +'%Y-%m-%d %H:%M:%S'` >> $p_main$p_log/register_table_structure_v2_${LOGTIME}.log
else
	echo "Please Pass Parameter : OWNER!!"
fi
