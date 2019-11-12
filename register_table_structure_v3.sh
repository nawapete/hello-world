#!/bin/ksh

########################################################################################
#  
#  Program Name     : register_table_structure_v3.sh
#  Description      : Gen DDL STG Table
#  Created by       : Nawapon L.
#  Create Date      : 16/10/2019
#   
########################################################################################
#

. /data1/misapps/dsl_dev/script/MISBI.cfg

FILEPATH=/data1/misapps/dsl_dev/tmp/
USERDB=misdba
PASSDB=misdba#19
SIDDB=ESLDEV

if [ "$1" != "" ]
then

echo 'Start Time : '`date +'%Y-%m-%d %H:%M:%S'`
typeset -u SYSTEM=$1
export STG_NAME=`echo "*STG_${SYSTEM}*"`

cd ${FILEPATH}
find . -type f -name "*STG_${SYSTEM}*" -exec rm -f {} \;

LISTTABLE=`sqlplus -s ${USERDB}/${PASSDB}@${SIDDB} << EOF
set feedback off
set trimspool on
set heading off
set echo off
set linesize 2000
set pages

select table_name
from all_tab_columns
where owner = 'MISDBA'
and substr(table_name,1,7) = 'STG_${SYSTEM}'
group by table_name
order by table_name;

exit;
EOF`

for TABLENAME in ${LISTTABLE}
do
	`sqlplus -s ${USERDB}/${PASSDB}@${SIDDB} << EOF > ${FILEPATH}${TABLENAME}.txt
	set feedback off
	set trimspool on
	set heading off
	set echo off
	set linesize 2000
	set pages
	
	select case when rowno = (select count(*)+8 from ALL_TAB_COLUMNS where owner = 'MISDBA' and table_name = '${TABLENAME}') then rtrim(text,',') else text end
	from (select 'OPTIONS(skip=1,direct=true)' text,1 rowno from dual
	union
	select 'UNRECOVERABLE LOAD DATA' text,2 rowno from dual
	union
	select 'CHARACTERSET UTF8' text,3 rowno from dual
	union
	select 'INFILE '||'''dummy.txt''' text,4 rowno from dual
	union
	select 'TRUNCATE' text,5 rowno from dual
	union
	select 'INTO TABLE #SCHEMA#.'||'${TABLENAME}' text,6 rowno from dual
	union
	select 'FIELDS TERMINATED BY '||'''#DELIMETER#''' text,7 rowno from dual
	union
	select 'TRAILING NULLCOLS (' text,8 rowno from dual
	union
	select 'DATA_DATE "''#DATE#''",' text,999997 rowno from dual
	union
	select 'PPN_TM EXPRESSION "CURRENT_TIMESTAMP(6)"' text,999998 rowno from dual
	union
	select ')' text,999999 rowno from dual
	union
	select text,rownum+8 rowno
	from (select 
case when data_type = 'CHAR' and data_length < 100 then 
     case when nullable = 'N' then column_name||' '||data_type||' "CASE WHEN TRIM(:'||column_name||') IS NULL THEN '' '' @ELSE TRIM(:'||column_name||') END",'
     else column_name||' '||data_type||' "TRIM(:'||column_name||')",' end
when data_type = 'VARCHAR2' and data_length < 100 then 
     case when nullable = 'N' then column_name||' '||'"CASE WHEN TRIM(:'||column_name||') IS NULL THEN '' '' @ELSE TRIM(:'||column_name||') END",'
     else column_name||' "TRIM(:'||column_name||')",' end
when data_type = 'NVARCHAR2' and data_length < 100 then 
     case when nullable = 'N' then column_name||' '||'"CASE WHEN TRIM(:'||column_name||') IS NULL THEN '' '' @ELSE TRIM(:'||column_name||') END",'
     else column_name||' "TRIM(:'||column_name||')",' end
when data_type = 'NUMBER' then column_name||','
when data_type = 'DATE' then column_name||' TIMESTAMP "YYYYMMDD HH24MISSFF",'
when substr(data_type,1,9) = 'TIMESTAMP' then column_name||' TIMESTAMP "YYYYMMDD HH24MISSFF",'
when data_type = 'CLOB' or data_type ='BLOB' or (data_type = 'CHAR' and data_length >= 100) or (data_type = 'VARCHAR2' and data_length >= 100) or (data_type = 'NVARCHAR2' and data_length >= 100) then column_name||' CHAR(4000),'
end text
from ALL_TAB_COLUMNS 
where table_name = '${TABLENAME}'
and column_name not in ('DATA_DATE','PPN_TM')
order by column_id))
order by rowno;
	
	exit;
	EOF`
		
		cat ${FILEPATH}${TABLENAME}.txt | tr '@' '\n' > ${FILEPATH}${TABLENAME}.tmp
		rm -f ${FILEPATH}${TABLENAME}.txt
		mv ${FILEPATH}${TABLENAME}.tmp ${FILEPATH}${TABLENAME}.txt
		
done

echo 'Finish Time : '`date +'%Y-%m-%d %H:%M:%S'`

else
	echo "Please Pass Parameter : SYSTEM!!"
fi

