#!/bin/ksh

FILEPATH=/data1/misapps/dsl_dev/tmp/
USERDB=misdba
PASSDB=misdba#19
SIDDB=ESLDEV

echo 'Start Time : '`date +'%Y-%m-%d %H:%M:%S'`

rm -f ${FILEPATH}TEMP_*.txt

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
and substr(table_name,1,4) = 'TEMP'
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
	
	select case when rowno = (select count(*)+7 from ALL_TAB_COLUMNS where owner = 'MISDBA' and table_name = '${TABLENAME}') then rtrim(text,',') else text end
	from (select 'OPTIONS(skip=1)' text,1 rowno from dual
	union
	select 'LOAD DATA' text,2 rowno from dual
	union
	select 'CHARACTERSET UTF8' text,3 rowno from dual
	union
	select 'INFILE '||'''dummy.txt''' text,4 rowno from dual
	union
	select 'INTO TABLE MISDBA.'||'${TABLENAME}' text,5 rowno from dual
	union
	select 'FIELDS TERMINATED BY '||'''|''' text,6 rowno from dual
	union
	select 'TRAILING NULLCOLS (' text,7 rowno from dual
	union
	select ')' text,999999 rowno from dual
	union
	select text,rownum+7 rowno
	from (select case when data_type = 'CHAR' then column_name||' '||data_type||' "TRIM(:'||column_name||')",'
	when data_type = 'VARCHAR2' then column_name||' '||'"TRIM(:'||column_name||')",'
	when data_type = 'NUMBER' then column_name||','
	when data_type = 'DATE' then column_name||' TIMESTAMP "YYYYMMDD HH24MISSFF",'
	when substr(data_type,1,9) = 'TIMESTAMP' then column_name||' TIMESTAMP "YYYYMMDD HH24MISSFF",'
	when data_type = 'CLOB' then column_name||' CHAR(4000),'
	end text
	from ALL_TAB_COLUMNS 
	where owner = 'MISDBA'
	and table_name = '${TABLENAME}'
	order by column_id))
	order by rowno;
	
	exit;
	EOF`
done

echo 'Finish Time : '`date +'%Y-%m-%d %H:%M:%S'`
