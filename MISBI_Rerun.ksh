#!/bin/ksh

#################################################################################################
# -----------------------------------------------------------------------------------------------
#  Program Name     : MISBI_Rerun.ksh
#  Description      : Rerun full load only daily type
#  Created by       : Nawapon L.
#  Create Date      : 05/09/2019
#
#  Usage: . MISBI_Rerun.ksh <System name> <Data date format YYYYMMDD> <Round amount>
#  Example: . MISBI_Rerun.ksh DMS 20190716 5 
#-----------------------------------------------------------------------------------------------
# EXIT CODES:
#   0 - Normal script execution
#   1 - Process error
#   2 - Usage error
#   5 - Reconcile fail
#------------------------------------------------------------------------------------------------
#################################################################################################

##********************************* IMPORT VARIABLE FORM CONFIG FILE##*********************************##
. /data1/misapps/dsl_dev/script/MISBI.cfg

##********************************* DECLARED VARIABLE *********************************##
export v_system_name=$1
export v_date=$2
integer v_round_no=$3

iecho "INF: Start MISBI_Rerun.ksh" | tee -a ${p_main}${p_log}/${v_date}/MISBI_RERUN_${v_system_name}_${p_currdate}.log

##********************************* CHECK PARAMETER PASSED 3 VALUES *********************************##
if [ -z "${v_system_name}" ] || [ -z "${v_date}" ] || [ -z "${v_round_no}" ];
then
	iecho "ERR: MISBI_Rerun.ksh parameter is missing" | tee -a ${p_main}${p_log}/${v_date}/MISBI_RERUN_${v_system_name}_${p_currdate}.log
	iecho "INF: Stop MISBI_Rerun.ksh" | tee -a ${p_main}${p_log}/${v_date}/MISBI_RERUN_${v_system_name}_${p_currdate}.log
	echo "******************************************************************************************************" | tee -a ${p_main}${p_log}/${v_date}/MISBI_RERUN_${v_system_name}_${p_currdate}.log
	return 2
else
	:
fi

##********************************* START LOOP FULL LOAD *********************************##
integer i=1
while [ $i -le "${v_round_no}" ]
do 
iecho "INF: Start ${v_system_name} system jobs on data date ${v_date}" | tee -a ${p_main}${p_log}/${v_date}/MISBI_RERUN_${v_system_name}_${p_currdate}.log

##********************************* GET ALL CONTROL STG JOBS IN SYSTEM *********************************##
cat << EOF > ${p_main}${p_sct}/${v_date}_${v_system_name}_STG.sql
set pages 0 feed off trimspool on line 2000
set time off timing off
set echo off
set verify off

spool ${p_main}${p_sct}/${v_date}_${v_system_name}_STG.txt

SELECT JOB_NM FROM $p_schema.CTL_JOB_SCHD
WHERE SYSTEM_NAME = '${v_system_name}' AND JOB_TYPE = 'STG_DWH' AND JOB_FREQ = 'D'
ORDER BY JOB_NM;
spool off

disc
exit;
EOF

cd ${p_java_path}
${p_sqlplus} @${p_main}${p_sct}/${v_date}_${v_system_name}_STG.sql >/dev/null 2>&1
chmod 777 ${p_main}${p_sct}/${v_date}_${v_system_name}_STG.txt

rm ${p_main}${p_sct}/${v_date}_${v_system_name}_STG.sql

##********************************* LOOP STAGING *********************************##
iecho "INF: Start loading STAGING tables" | tee -a ${p_main}${p_log}/${v_date}/MISBI_RERUN_${v_system_name}_${p_currdate}.log
integer v_count=1
while IFS= read -r line
do
	export v_job_name=`awk -F'|' 'NR=='$v_count'{print $1}' ${p_main}${p_sct}/${v_date}_${v_system_name}_STG.txt`

##**************** UPDATE DATA DATE IN CTL_JOB_SCHD BY JOB AND DELETE LOG IN DSL_AUDIT_JOB BY DATA DATE BEFORE LOAD DATA ****************##
cat << EOF > ${p_main}${p_sct}/PROC_${v_system_name}_${v_job_name}_${v_date}.sql
UPDATE $p_schema.CTL_JOB_SCHD
SET DATA_DT = TO_DATE('${v_date}','YYYY-MM-DD')
,DATA_TM = TO_TIMESTAMP(${v_date}||''||235959,'YYYY-MM-DDHH24MISS')
WHERE SYSTEM_NAME = '${v_system_name}' AND JOB_NM = '${v_job_name}';
COMMIT;

/
disc
exit;
EOF

#### DELETE LOG IN DSL_AUDIT_LOG ####

#DELETE FROM $p_schema.DSL_AUDIT_JOB
#WHERE DATE_KEY = '${v_date}' AND JOB_NAME IN (
#SELECT TEMP_TABLE_NAME FROM DSL_EXTRACT_SYSTEM_FILE
#WHERE CTL_JOB_NM = '${v_job_name}'
#UNION ALL
#SELECT STG_TABLE_NAME FROM DSL_EXTRACT_SYSTEM_FILE
#WHERE CTL_JOB_NM = '${v_job_name}'
#);
#COMMIT;

#####

	chmod 777 ${p_main}${p_sct}/PROC_${v_system_name}_${v_job_name}_${v_date}.sql
	
	cd ${p_java_path}
	${p_sqlplus} @${p_main}${p_sct}/PROC_${v_system_name}_${v_job_name}_${v_date}.sql > /dev/null 2>&1
	
	rm ${p_main}${p_sct}/PROC_${v_system_name}_${v_job_name}_${v_date}.sql
	
	. ${p_job_load} ${v_job_name} | tee -a ${p_main}${p_log}/${v_date}/MISBI_RERUN_${v_system_name}_${p_currdate}.log

##********************************* CHECK STG LOAD COMPLETED OR FAILED *********************************##
cat << EOF > ${p_main}${p_sct}/${v_date}_${v_system_name}_${v_job_name}_STATUS.sql
set pages 0 feed off trimspool on line 2000
set time off timing off
set echo off
set verify off

spool ${p_main}${p_sct}/${v_date}_${v_system_name}_${v_job_name}_STATUS.txt

SELECT CASE WHEN count(*) = 1 THEN 'Y' ELSE 'N' END FROM $p_schema.CTL_JOB_HIST_LOG A
INNER JOIN (SELECT JOB_NM,DATA_DT,MAX(JOB_STR_TM) AS MAX_JOB_STR_TM FROM $p_schema.CTL_JOB_HIST_LOG
GROUP BY JOB_NM, DATA_DT) B
ON A.JOB_NM = B.JOB_NM AND A.DATA_DT = B.DATA_DT AND A.JOB_STR_TM = B.MAX_JOB_STR_TM
WHERE A.JOB_NM = '${v_job_name}' AND TO_CHAR(A.DATA_DT,'YYYYMMDD') = '${v_date}' AND A.RUN_ST = 'S' AND A.REMARK = 'Finished';

spool off

disc
exit;
EOF

	cd ${p_java_path}
	${p_sqlplus} @${p_main}${p_sct}/${v_date}_${v_system_name}_${v_job_name}_STATUS.sql >/dev/null 2>&1
	
	chmod 777 ${p_main}${p_sct}/${v_date}_${v_system_name}_${v_job_name}_STATUS.txt
	
	rm ${p_main}${p_sct}/${v_date}_${v_system_name}_${v_job_name}_STATUS.sql
	
	export v_check_flag=`awk -F'|' 'NR==1{print $1}' ${p_main}${p_sct}/${v_date}_${v_system_name}_${v_job_name}_STATUS.txt`
	
	rm ${p_main}${p_sct}/${v_date}_${v_system_name}_${v_job_name}_STATUS.txt

##********************************* CHECK FAIL FOR BREAK STG LOOP *********************************##	
	if [ "${v_check_flag}" = 'N' ];
	then
		iecho "INF: Stop MISBI_Rerun.ksh" | tee -a ${p_main}${p_log}/${v_date}/MISBI_RERUN_${v_system_name}_${p_currdate}.log
		echo "******************************************************************************************************" | tee -a ${p_main}${p_log}/${v_date}/MISBI_RERUN_${v_system_name}_${p_currdate}.log
		break
		return 1
	else
		v_count=$((v_count+1))
	fi
done < ${p_main}${p_sct}/${v_date}_${v_system_name}_STG.txt

rm ${p_main}${p_sct}/${v_date}_${v_system_name}_STG.txt

##********************************* CHECK FAIL FOR BREAK FULL LOOP *********************************##
if [ "${v_check_flag}" = 'N' ];
then
	break
	return 1
else
	:
fi

##********************************* GET ALL CONTROL DIM & FACT JOBS IN SYSTEM *********************************##
cat << EOF > ${p_main}${p_sct}/${v_date}_${v_system_name}_DWH.sql
set pages 0 feed off trimspool on line 2000
set time off timing off
set echo off
set verify off

spool ${p_main}${p_sct}/${v_date}_${v_system_name}_DWH.txt

SELECT JOB_NM FROM $p_schema.CTL_JOB_SCHD
WHERE SYSTEM_NAME = '${v_system_name}' AND JOB_TYPE IN ('DIM_DWH','FCT_DWH') AND JOB_FREQ = 'D'
ORDER BY JOB_NM;
spool off

disc
exit;
EOF

cd ${p_java_path}
${p_sqlplus} @${p_main}${p_sct}/${v_date}_${v_system_name}_DWH.sql >/dev/null 2>&1

chmod 777 ${p_main}${p_sct}/${v_date}_${v_system_name}_DWH.txt

rm ${p_main}${p_sct}/${v_date}_${v_system_name}_DWH.sql

##*********************************LOOP DIMENSIONS & FACT *********************************##
iecho "INF: Start loading DIMENSIONS tables AND FACT tables" | tee -a ${p_main}${p_log}/${v_date}/MISBI_RERUN_${v_system_name}_${p_currdate}.log
integer v_count=1
while IFS= read -r line
do
	export v_job_name=`awk -F'|' 'NR=='$v_count'{print $1}' ${p_main}${p_sct}/${v_date}_${v_system_name}_DWH.txt`
	
cat << EOF > ${p_main}${p_sct}/PROC_${v_system_name}_${v_job_name}_${v_date}.sql
UPDATE $p_schema.CTL_JOB_SCHD
SET DATA_DT = TO_DATE('${v_date}','YYYY-MM-DD')
,DATA_TM = TO_TIMESTAMP(${v_date}||''||235959,'YYYY-MM-DDHH24MISS')
WHERE SYSTEM_NAME = '${v_system_name}' AND JOB_NM = '${v_job_name}';
COMMIT;
/
disc
exit;
EOF
	chmod 777 ${p_main}${p_sct}/PROC_${v_system_name}_${v_job_name}_${v_date}.sql
	
	cd ${p_java_path}
	${p_sqlplus} @${p_main}${p_sct}/PROC_${v_system_name}_${v_job_name}_${v_date}.sql > /dev/null 2>&1
	
	rm ${p_main}${p_sct}/PROC_${v_system_name}_${v_job_name}_${v_date}.sql
	
	. ${p_rulebase_load} ${p_schema} ${v_job_name} | tee -a ${p_main}${p_log}/${v_date}/MISBI_RERUN_${v_system_name}_${p_currdate}.log
	
##********************************* CHECK DIM & FACT LOAD COMPLETED OR FAILED *********************************##
cat << EOF > ${p_main}${p_sct}/${v_date}_${v_system_name}_${v_job_name}_STATUS.sql
set pages 0 feed off trimspool on line 2000
set time off timing off
set echo off
set verify off

spool ${p_main}${p_sct}/${v_date}_${v_system_name}_${v_job_name}_STATUS.txt

SELECT CASE WHEN count(*) = 1 THEN 'Y' ELSE 'N' END FROM $p_schema.CTL_JOB_HIST_LOG A
INNER JOIN (SELECT JOB_NM,DATA_DT,MAX(JOB_STR_TM) AS MAX_JOB_STR_TM FROM $p_schema.CTL_JOB_HIST_LOG
GROUP BY JOB_NM, DATA_DT) B
ON A.JOB_NM = B.JOB_NM AND A.DATA_DT = B.DATA_DT AND A.JOB_STR_TM = B.MAX_JOB_STR_TM
WHERE A.JOB_NM = '${v_job_name}' AND TO_CHAR(A.DATA_DT,'YYYYMMDD') = '${v_date}' AND A.RUN_ST = 'S' AND A.REMARK = 'Finished';

spool off

disc
exit;
EOF

	cd ${p_java_path}
	${p_sqlplus} @${p_main}${p_sct}/${v_date}_${v_system_name}_${v_job_name}_STATUS.sql >/dev/null 2>&1
	
	chmod 777 ${p_main}${p_sct}/${v_date}_${v_system_name}_${v_job_name}_STATUS.txt
	
	rm ${p_main}${p_sct}/${v_date}_${v_system_name}_${v_job_name}_STATUS.sql
	
	export v_check_flag=`awk -F'|' 'NR==1{print $1}' ${p_main}${p_sct}/${v_date}_${v_system_name}_${v_job_name}_STATUS.txt`
	
	rm ${p_main}${p_sct}/${v_date}_${v_system_name}_${v_job_name}_STATUS.txt
	
##********************************* CHECK FAIL FOR BREAK DIM & FACT LOOP *********************************##		
	if [ "${v_check_flag}" = 'N' ];
	then
		iecho "INF: Stop MISBI_Rerun.ksh" | tee -a ${p_main}${p_log}/${v_date}/MISBI_RERUN_${v_system_name}_${p_currdate}.log
		echo "******************************************************************************************************" | tee -a ${p_main}${p_log}/${v_date}/MISBI_RERUN_${v_system_name}_${p_currdate}.log
		break
		return 1
	else
		v_count=$((v_count+1))
	fi
done < ${p_main}${p_sct}/${v_date}_${v_system_name}_DWH.txt

rm ${p_main}${p_sct}/${v_date}_${v_system_name}_DWH.txt

##********************************* CHECK FAIL FOR BREAK FULL LOOP *********************************##
if [ "${v_check_flag}" = 'N' ];
then
	break
	return 1
else
	:
fi

##********************************* GET ALL CONTROL MART JOB IN SYSTEM *********************************##
cat << EOF > ${p_main}${p_sct}/${v_date}_${v_system_name}_MART.sql
set pages 0 feed off trimspool on line 2000
set time off timing off
set echo off
set verify off

spool ${p_main}${p_sct}/${v_date}_${v_system_name}_MART.txt

SELECT JOB_NM FROM $p_schema.CTL_JOB_SCHD
WHERE SYSTEM_NAME = '${v_system_name}' AND JOB_TYPE IN ('DIM_MRT','FCT_MRT') AND JOB_FREQ = 'D'
ORDER BY JOB_NM;
spool off

disc
exit;
EOF

cd ${p_java_path}
${p_sqlplus} @${p_main}${p_sct}/${v_date}_${v_system_name}_MART.sql >/dev/null 2>&1

chmod 777 ${p_main}${p_sct}/${v_date}_${v_system_name}_MART.txt

rm ${p_main}${p_sct}/${v_date}_${v_system_name}_MART.sql

##********************************* LOOP MART *********************************##
iecho "INF: Start loading DIMENSIONS tables AND FACT tables to MART" | tee -a ${p_main}${p_log}/${v_date}/MISBI_RERUN_${v_system_name}_${p_currdate}.log
integer v_count=1
while IFS= read -r line
do
	export v_job_name=`awk -F'|' 'NR=='$v_count'{print $1}' ${p_main}${p_sct}/${v_date}_${v_system_name}_MART.txt`
	
cat << EOF > ${p_main}${p_sct}/PROC_${v_system_name}_${v_job_name}_${v_date}.sql
UPDATE $p_schema.CTL_JOB_SCHD
SET DATA_DT = TO_DATE('${v_date}','YYYY-MM-DD')
,DATA_TM = TO_TIMESTAMP(${v_date}||''||235959,'YYYY-MM-DDHH24MISS')
WHERE SYSTEM_NAME = '${v_system_name}' AND JOB_NM = '${v_job_name}';
COMMIT;
/
disc
exit;
EOF
	chmod 777 ${p_main}${p_sct}/PROC_${v_system_name}_${v_job_name}_${v_date}.sql
	
	cd ${p_java_path}
	${p_sqlplus} @${p_main}${p_sct}/PROC_${v_system_name}_${v_job_name}_${v_date}.sql > /dev/null 2>&1
	
	rm ${p_main}${p_sct}/PROC_${v_system_name}_${v_job_name}_${v_date}.sql
	
	. ${p_rulebase_load} ${p_schema} ${v_job_name} | tee -a ${p_main}${p_log}/${v_date}/MISBI_RERUN_${v_system_name}_${p_currdate}.log

##********************************* CHECK MART LOAD COMPLETED OR FAILED *********************************##
cat << EOF > ${p_main}${p_sct}/${v_date}_${v_system_name}_${v_job_name}_STATUS.sql
set pages 0 feed off trimspool on line 2000
set time off timing off
set echo off
set verify off

spool ${p_main}${p_sct}/${v_date}_${v_system_name}_${v_job_name}_STATUS.txt

SELECT CASE WHEN count(*) = 1 THEN 'Y' ELSE 'N' END FROM $p_schema.CTL_JOB_HIST_LOG A
INNER JOIN (SELECT JOB_NM,DATA_DT,MAX(JOB_STR_TM) AS MAX_JOB_STR_TM FROM $p_schema.CTL_JOB_HIST_LOG
GROUP BY JOB_NM, DATA_DT) B
ON A.JOB_NM = B.JOB_NM AND A.DATA_DT = B.DATA_DT AND A.JOB_STR_TM = B.MAX_JOB_STR_TM
WHERE A.JOB_NM = '${v_job_name}' AND TO_CHAR(A.DATA_DT,'YYYYMMDD') = '${v_date}' AND A.RUN_ST = 'S' AND A.REMARK = 'Finished';

spool off

disc
exit;
EOF

	cd ${p_java_path}
	${p_sqlplus} @${p_main}${p_sct}/${v_date}_${v_system_name}_${v_job_name}_STATUS.sql >/dev/null 2>&1
	
	chmod 777 ${p_main}${p_sct}/${v_date}_${v_system_name}_${v_job_name}_STATUS.txt
	
	rm ${p_main}${p_sct}/${v_date}_${v_system_name}_${v_job_name}_STATUS.sql
	
	export v_check_flag=`awk -F'|' 'NR==1{print $1}' ${p_main}${p_sct}/${v_date}_${v_system_name}_${v_job_name}_STATUS.txt`
	
	rm ${p_main}${p_sct}/${v_date}_${v_system_name}_${v_job_name}_STATUS.txt

##********************************* CHECK FAIL FOR BREAK MART LOOP *********************************##	
	if [ "${v_check_flag}" = 'N' ];
	then
		iecho "INF: Stop MISBI_Rerun.ksh" | tee -a ${p_main}${p_log}/${v_date}/MISBI_RERUN_${v_system_name}_${p_currdate}.log
		echo "******************************************************************************************************" | tee -a ${p_main}${p_log}/${v_date}/MISBI_RERUN_${v_system_name}_${p_currdate}.log
		break
		return 1
	else
		v_count=$((v_count+1))
	fi
done < ${p_main}${p_sct}/${v_date}_${v_system_name}_MART.txt

rm ${p_main}${p_sct}/${v_date}_${v_system_name}_MART.txt

##********************************* CHECK FAIL FOR BREAK FULL LOOP *********************************##	
if [ "${v_check_flag}" = 'N' ];
then
	break
	return 1
else
	:
fi

##********************************* CHECK ROUND_NO *********************************##
if [ ${v_round_no} = 1 ];
then
	i=$((i+1))
else
	i=$((i+1))

##********************************* GET NEW DATE IF ROUND_NO > 1 *********************************##
cat << EOF > ${p_main}${p_sct}/${v_system_name}_DATA_DATE.sql
set pages 0 feed off trimspool on line 2000
set time off timing off
set echo off
set verify off

spool ${p_main}${p_sct}/${v_system_name}_DATA_DATE.txt

SELECT TO_CHAR(MAX(DATA_DT),'YYYYMMDD') FROM $p_schema.CTL_JOB_SCHD
WHERE SYSTEM_NAME = '${v_system_name}'AND JOB_FREQ = 'D'
GROUP BY SYSTEM_NAME;

spool off

disc
exit;
EOF

	cd ${p_java_path}
	${p_sqlplus} @${p_main}${p_sct}/${v_system_name}_DATA_DATE.sql >/dev/null 2>&1

	export v_date=`awk -F'|' 'NR==1{print $1}' ${p_main}${p_sct}/${v_system_name}_DATA_DATE.txt`
	
	rm ${p_main}${p_sct}/${v_system_name}_DATA_DATE.sql
	rm ${p_main}${p_sct}/${v_system_name}_DATA_DATE.txt
fi

##********************************* END LOOP FULL LOAD *********************************##
iecho "INF: All jobs in ${v_system_name} system on data date ${v_date} completed" | tee -a ${p_main}${p_log}/${v_date}/MISBI_RERUN_${v_system_name}_${p_currdate}.log
echo "******************************************************************************************************" | tee -a ${p_main}${p_log}/${v_date}/MISBI_RERUN_${v_system_name}_${p_currdate}.log
done
return 0
