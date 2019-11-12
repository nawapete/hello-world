#!/bin/ksh

#################################################################################################
#------------------------------------------------------------------------------------------------
#  Program Name     : MISBI_LOAD.ksh
#  Description      : Load data from text file into staging table
#  Created by       : Nawapon.L
#  Create Date      : 16/10/2019
#
#  Usage: . MISBI_LOAD.ksh <Job name STG in rulebase>
#  Example: . MISBI_LOAD.ksh DMS_D_STG01
#------------------------------------------------------------------------------------------------
# EXIT CODES:
#   0 - Normal script execution
#   1 - Process error
#   2 - Usage error
#   5 - Reconcile fail
#------------------------------------------------------------------------------------------------
#################################################################################################

##********************************* IMPORT VARIABLE FORM CONFIG FILE************************************************##
. /data1/misapps/dsl_dev/script/MISBI.cfg

##********************************* DECLARED VARIABLE ************************************************##
export v_job_name=$1

##********************************* CHECK ARGUMENT PASSED 1 VALUES ************************************************##
if [ -z "${v_job_name}" ];
then
	iecho "ERR: MISBI_LOAD.ksh argument is missing"
	iecho "INF: Stop MISBI_LOAD.ksh"
	echo "******************************************************************************************************"
	return 2
else
	:
fi

##********************************* GET DATA DATE IN TABLE CTL_JOB_SCHD *********************************##
cat << EOF > ${p_main}${p_sct}/${v_job_name}_DATA_DATE.sql
set pages 0 feed off trimspool on line 2000
set time off timing off
set echo off
set verify off

spool ${p_main}${p_sct}/SPOOL_${v_job_name}_DATA_DATE.txt

SELECT TO_CHAR(DATA_DT,'YYYYMMDD') FROM $p_schema.CTL_JOB_SCHD WHERE JOB_NM='$v_job_name';

spool off

disc
exit
EOF

cd ${p_java_path}
${p_sqlplus} @${p_main}${p_sct}/${v_job_name}_DATA_DATE.sql >/dev/null 2>&1

export v_date=`awk -F'|' 'NR==1{print $1}' $p_main$p_sct/SPOOL_${v_job_name}_DATA_DATE.txt`

rm ${p_main}${p_sct}/SPOOL_${v_job_name}_DATA_DATE.txt
rm ${p_main}${p_sct}/${v_job_name}_DATA_DATE.sql

iecho "INF: Start ${v_job_name} on data date = ${v_date}"

##********************************* GET TABLE BY JOB NAME IN TABLE EXTRACT_SYSTEM_FILE *********************************##
cat << EOF > ${p_main}${p_sct}/${v_date}_${v_job_name}_EXTRACT_SYSTEM_FILE.sql
set pages 0 feed off trimspool on line 2000
set time off timing off
set echo off
set verify off

spool ${p_main}${p_sct}/${v_date}_${v_job_name}_EXTRACT_SYSTEM_FILE.txt

SELECT 
SYSTEM_NAME||'|'||PATH_NAME||'|'||TO_CHAR(DATA_DT,'YYYYMMDD')||'|'||DATA_FILE_NAME||'|'||STG_TABLE_NAME||'|'||EXPORT_TYPE||'|'||DATA_FILE_SUFFIX||'|'||CTL_FILE_SUFFIX||'|'||DELIMETER
FROM
(
SELECT A.SYSTEM_NAME, C.PATH_NAME, A.DATA_DT, B.DATA_FILE_NAME, B.STG_TABLE_NAME, B.EXPORT_TYPE, B.DATA_FILE_SUFFIX, B.CTL_FILE_SUFFIX, B.CTL_JOB_NM, B.FILE_ID, B.DELIMETER, B.IS_LOAD
FROM $p_schema.CTL_JOB_SCHD A
INNER JOIN  $p_schema.DSL_EXTRACT_SYSTEM_FILE B
ON A.JOB_NM = B.CTL_JOB_NM
INNER JOIN $p_schema.DSL_EXTRACT_SYSTEM C
ON B.CTL_ID = C.CTL_ID
WHERE A.JOB_NM = '${v_job_name}'
)ctl
LEFT JOIN (SELECT T1.DATE_KEY, T1.JOB_NAME, T1.FLAG_COMPLETED 
FROM $p_schema.DSL_AUDIT_JOB T1
INNER JOIN (SELECT DATE_KEY, JOB_NAME, MAX(SEQ) AS MAX_SEQ FROM $p_schema.DSL_AUDIT_JOB GROUP BY DATE_KEY, JOB_NAME, LOAD_TYPE) T2
ON T1.DATE_KEY = T2.DATE_KEY AND T1.JOB_NAME = T2.JOB_NAME AND T1.SEQ = T2.MAX_SEQ
WHERE SUBSTR(T1.JOB_NAME,1,3) = 'STG') aud
ON ctl.STG_TABLE_NAME = aud.JOB_NAME AND TO_CHAR(ctl.DATA_DT,'YYYYMMDD') = aud.DATE_KEY
WHERE (aud.FLAG_COMPLETED IS NULL OR aud.FLAG_COMPLETED NOT IN ('Y')) AND ctl.IS_LOAD ='Y'
ORDER BY FILE_ID;

spool off

disc
exit
EOF

cd ${p_java_path}
${p_sqlplus} @${p_main}${p_sct}/${v_date}_${v_job_name}_EXTRACT_SYSTEM_FILE.sql >/dev/null 2>&1

chmod 777 ${p_main}${p_sct}/${v_date}_${v_job_name}_EXTRACT_SYSTEM_FILE.txt


##********************************* RUN JOBS IN EXTRACT_SYSTEM_FILE TABLE LOOP *********************************##
integer v_count=1
while IFS= read -r line
do
	export v_app=`awk -F'|' 'NR=='$v_count'{print $1}' ${p_main}${p_sct}/${v_date}_${v_job_name}_EXTRACT_SYSTEM_FILE.txt`
	export v_src_path=`awk -F'|' 'NR=='$v_count'{print $2}' ${p_main}${p_sct}/${v_date}_${v_job_name}_EXTRACT_SYSTEM_FILE.txt`
	export v_date=`awk -F'|' 'NR=='$v_count'{print $3}' ${p_main}${p_sct}/${v_date}_${v_job_name}_EXTRACT_SYSTEM_FILE.txt`
	export v_file_nm=`awk -F'|' 'NR=='$v_count'{print $4}' ${p_main}${p_sct}/${v_date}_${v_job_name}_EXTRACT_SYSTEM_FILE.txt`
	export v_stg_table=`awk -F'|' 'NR=='$v_count'{print $5}' ${p_main}${p_sct}/${v_date}_${v_job_name}_EXTRACT_SYSTEM_FILE.txt`
	export v_type=`awk -F'|' 'NR=='$v_count'{print $6}' ${p_main}${p_sct}/${v_date}_${v_job_name}_EXTRACT_SYSTEM_FILE.txt`
	export v_data_sf=`awk -F'|' 'NR=='$v_count'{print $7}' ${p_main}${p_sct}/${v_date}_${v_job_name}_EXTRACT_SYSTEM_FILE.txt`
	export v_ctl_sf=`awk -F'|' 'NR=='$v_count'{print $8}' ${p_main}${p_sct}/${v_date}_${v_job_name}_EXTRACT_SYSTEM_FILE.txt`
	export v_delim=`awk '{print substr($0,length,1)}' ${p_main}${p_sct}/${v_date}_${v_job_name}_EXTRACT_SYSTEM_FILE.txt`
	
##********************************* CREATE PATH OF LOG FOLDER *********************************##
	if [ ! -d "${p_main}""${p_log}"/"${v_date}" ];
	then
		mkdir -p $p_main$p_log/$v_date
		chmod 777 $p_main$p_log/$v_date
	else
		:
	fi
		
	. /data1/misapps/dsl_dev/script/MISBI_Control_noncheck.ksh ${v_app} ${v_src_path} ${v_date} ${v_file_nm} ${v_stg_table} ${v_type} ${v_data_sf} ${v_ctl_sf} ${v_delim} | tee -a ${p_main}${p_log}/${v_date}/MISBI_LOAD_${v_job_name}_${p_currdate}.log
	v_count=$((v_count+1))
	
##********************************* END LOOP *********************************##
done < ${p_main}${p_sct}/${v_date}_${v_job_name}_EXTRACT_SYSTEM_FILE.txt

rm ${p_main}${p_sct}/${v_date}_${v_job_name}_EXTRACT_SYSTEM_FILE.txt
rm ${p_main}${p_sct}/${v_date}_${v_job_name}_EXTRACT_SYSTEM_FILE.sql

##********************************* CHECK JOBS RUN FAILED & NO DATA *********************************##
cat << EOF > ${p_main}${p_sct}/${v_date}_${v_job_name}_FAILED.sql
set pages 0 feed off trimspool on line 2000
set time off timing off
set echo off
set verify off

spool ${p_main}${p_log}/${v_date}/MISBI_LOAD_${v_job_name}_FAILED.log

SELECT dsl.DATE_KEY||'|'||dsl.JOB_NAME||'|'||seq.MAX_SEQ||'|'||dsl.LOAD_TYPE||'|'||dsl.FLAG_COMPLETED||'|'||dsl.REMARK
FROM
(
SELECT TRIM(DATE_KEY)AS DATE_KEY
,TRIM(JOB_NAME) AS JOB_NAME
,SEQ
,TRIM(LOAD_TYPE) AS LOAD_TYPE
,TRIM(FLAG_COMPLETED) AS FLAG_COMPLETED
,TRIM(REMARK) AS REMARK
FROM $p_schema.DSL_AUDIT_JOB
)dsl
INNER JOIN 
(SELECT JOB_NAME,DATE_KEY ,MAX(SEQ) AS MAX_SEQ FROM $p_schema.DSL_AUDIT_JOB WHERE DATE_KEY='${v_date}' GROUP BY JOB_NAME, DATE_KEY) seq
ON dsl.JOB_NAME = seq.JOB_NAME AND dsl.SEQ = seq.MAX_SEQ AND dsl.DATE_KEY = seq.DATE_KEY
INNER JOIN $p_schema.DSL_EXTRACT_SYSTEM_FILE esf
ON dsl.JOB_NAME = esf.STG_TABLE_NAME
WHERE dsl.FLAG_COMPLETED NOT IN ('Y','W') AND SUBSTR(dsl.JOB_NAME,1,3)='STG' AND CTL_JOB_NM = '${v_job_name}';
spool off

spool ${p_main}${p_log}/${v_date}/MISBI_LOAD_${v_job_name}_NODATA.log

SELECT dsl.DATE_KEY||'|'||dsl.JOB_NAME||'|'||seq.MAX_SEQ||'|'||dsl.LOAD_TYPE||'|'||dsl.FLAG_COMPLETED||'|'||dsl.REMARK
FROM
(
SELECT TRIM(DATE_KEY)AS DATE_KEY
,TRIM(JOB_NAME) AS JOB_NAME
,SEQ
,TRIM(LOAD_TYPE) AS LOAD_TYPE
,TRIM(FLAG_COMPLETED) AS FLAG_COMPLETED
,TRIM(REMARK) AS REMARK
FROM $p_schema.DSL_AUDIT_JOB
)dsl
INNER JOIN 
(SELECT JOB_NAME,DATE_KEY ,MAX(SEQ) AS MAX_SEQ FROM $p_schema.DSL_AUDIT_JOB WHERE DATE_KEY='${v_date}' GROUP BY JOB_NAME, DATE_KEY) seq
ON dsl.JOB_NAME = seq.JOB_NAME AND dsl.SEQ = seq.MAX_SEQ AND dsl.DATE_KEY = seq.DATE_KEY
INNER JOIN $p_schema.DSL_EXTRACT_SYSTEM_FILE esf
ON dsl.JOB_NAME = esf.STG_TABLE_NAME
WHERE dsl.FLAG_COMPLETED IN ('W') AND SUBSTR(dsl.JOB_NAME,1,3)='STG' AND CTL_JOB_NM = '${v_job_name}';
spool off

disc
exit;
EOF

cd ${p_java_path}
${p_sqlplus} @${p_main}${p_sct}/${v_date}_${v_job_name}_FAILED.sql >/dev/null 2>&1

	rm ${p_main}${p_sct}/${v_date}_${v_job_name}_FAILED.sql
	rm ${p_main}${p_log}/${v_date}/MISBI_LOAD_${v_job_name}_FAILED.log
	
return 0
