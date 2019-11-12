#!/bin/ksh

#################################################################################################
#  ----------------------------------------------------------------------------------------------
#  Program Name     : MISBI_RunSpExec.ksh
#  Description      : Using script for run SP_EXEC on Oracle Database
#  Created by       : Nawapon L.
#  Create Date      : 03/09/2019
#
#  Usage: MISBI_RunSpExec.ksh <Database name (Schema)> <Job name (DIM FCT MRT) in rulebase>
#  Example: . MISBI_RunSpExec.ksh MISDBA SP_D_DMS_DIM01
#------------------------------------------------------------------------------------------------
# EXIT CODES:
#   0 - Normal script execution
#   1 - Process error
#   2 - Usage error
#   5 - Reconcile fail
#------------------------------------------------------------------------------------------------
#################################################################################################

##********************************* IMPORT VARIABLE FORM CONFIG FILE *********************************##
. /data1/misapps/dsl_dev/script/MISBI.cfg

##********************************* DECLARED VARIABLE *********************************##
export v_dbname=$1
export v_job_name=$2

##********************************* CHECK PARAMETER PASSED 2 VALUES ************************************************##
if [ -z "${v_dbname}" ] || [ -z "${v_job_name}" ]
then
	iecho "INF: Start MISBI_RunSpExec.ksh"
	iecho "ERR: MISBI_RunSpExec.ksh parameter is missing"
	iecho "INF: Stop MISBI_RunSpExec.ksh"
	echo "******************************************************************************************************"
	return 2
else
	:
fi

##********************************* GET DATA DATE IN RULEBASE BY JOB NAME ************************************************##
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

iecho "INF: Start MISBI_RunSpExec.ksh" | tee -a ${p_main}${p_log}/${v_date}/MISBI_RUNSPEXEC_${v_dbname}_${v_job_name}_${p_currdate}.log

##********************************* CHECK JOB NAME IN TABLE CTL_JOB_SCHD ************************************************##
if [ -z "${v_date}" ];
then
	iecho "ERR: Database name: ${v_dbname} AND Job name: ${v_job_name} are not exist in table CTL_JOB_SCHD" | tee -a ${p_main}${p_log}/${v_date}/MISBI_RUNSPEXEC_${v_dbname}_${v_job_name}_${p_currdate}.log
	iecho "INF: Stop MISBI_RunSpExec.ksh" | tee -a ${p_main}${p_log}/${v_date}/MISBI_RUNSPEXEC_${v_dbname}_${v_job_name}_${p_currdate}.log
	echo "******************************************************************************************************" | tee -a ${p_main}${p_log}/${v_date}/MISBI_RUNSPEXEC_${v_dbname}_${v_job_name}_${p_currdate}.log
	return 2
else
	if [ -f "${p_main}""${p_log}"/"${v_date}"/"${v_date}"_SP_EXEC_"${v_dbname}"_"${v_job_name}".log ];
	then
		rm ${p_main}${p_log}/${v_date}/${v_date}_SP_EXEC_${v_dbname}_${v_job_name}.log
	else
		:
	fi
fi

iecho "INF: Database name: ${v_dbname}" | tee -a ${p_main}${p_log}/${v_date}/MISBI_RUNSPEXEC_${v_dbname}_${v_job_name}_${p_currdate}.log
iecho "INF: Job name: ${v_job_name}" | tee -a ${p_main}${p_log}/${v_date}/MISBI_RUNSPEXEC_${v_dbname}_${v_job_name}_${p_currdate}.log
iecho "INF: Data date: ${v_date}" | tee -a ${p_main}${p_log}/${v_date}/MISBI_RUNSPEXEC_${v_dbname}_${v_job_name}_${p_currdate}.log

##************** GET COUNT IN TABLE DSL_EXTRACT_SYSTEM_FILE AND GET COUNT IN TABLE DSL_AUDIT_JOB BY SYSTEM **************##
cat << EOF > ${p_main}${p_sct}/${v_job_name}_RunSpExec.sql
set pages 0 feed off trimspool on line 2000
set time off timing off
set echo off
set verify off

spool ${p_main}${p_sct}/SPOOL_${v_job_name}_COUNT_EXTRACT_SYSTEM_FILE.txt

SELECT COUNT(*) FROM $p_schema.DSL_EXTRACT_SYSTEM_FILE A
INNER JOIN $p_schema.DSL_EXTRACT_SYSTEM B
ON A.CTL_ID = B.CTL_ID
WHERE B.SYSTEM_NAME = SUBSTR('${v_job_name}',6,3);

spool off

spool ${p_main}${p_sct}/SPOOL_${v_job_name}_COUNT_AUDIT_JOB.txt

SELECT COUNT(*),'|'||SUBSTR('${v_job_name}',6,3) FROM $p_schema.DSL_AUDIT_JOB A
INNER JOIN (SELECT DATE_KEY,JOB_NAME,MAX(SEQ) AS MAX_SEQ FROM $p_schema.DSL_AUDIT_JOB GROUP BY DATE_KEY,JOB_NAME) B
ON A.DATE_KEY = B.DATE_KEY AND A.JOB_NAME = B.JOB_NAME AND A.SEQ = B.MAX_SEQ
INNER JOIN $p_schema.DSL_EXTRACT_SYSTEM_FILE C
ON A.JOB_NAME = C.STG_TABLE_NAME
INNER JOIN $p_schema.DSL_EXTRACT_SYSTEM D
ON C.CTL_ID = D.CTL_ID
WHERE D.SYSTEM_NAME = SUBSTR('${v_job_name}',6,3) AND A.DATE_KEY = '${v_date}' AND A.FLAG_COMPLETED IN ('Y','W');

spool off

disc
exit
EOF

cd ${p_java_path}
${p_sqlplus} @${p_main}${p_sct}/${v_job_name}_RunSpExec.sql >/dev/null 2>&1

chmod 777 ${p_main}${p_sct}/SPOOL_${v_job_name}_COUNT_EXTRACT_SYSTEM_FILE.txt
chmod 777 ${p_main}${p_sct}/SPOOL_${v_job_name}_COUNT_AUDIT_JOB.txt

export v_cnt_sysfile=`awk -F'|' 'NR==1{print $1}' ${p_main}${p_sct}/SPOOL_${v_job_name}_COUNT_EXTRACT_SYSTEM_FILE.txt`
export v_cnt_auditjob=`awk -F'|' 'NR==1{print $1}' ${p_main}${p_sct}/SPOOL_${v_job_name}_COUNT_AUDIT_JOB.txt`
export v_system=`awk -F'|' 'NR==1{print $2}' ${p_main}${p_sct}/SPOOL_${v_job_name}_COUNT_AUDIT_JOB.txt`

rm ${p_main}${p_sct}/${v_job_name}_RunSpExec.sql
rm ${p_main}${p_sct}/SPOOL_${v_job_name}_COUNT_EXTRACT_SYSTEM_FILE.txt
rm ${p_main}${p_sct}/SPOOL_${v_job_name}_COUNT_AUDIT_JOB.txt

iecho "INF: Total ${v_cnt_sysfile} jobs in ${v_system} system" | xargs | tee -a ${p_main}${p_log}/${v_date}/MISBI_RUNSPEXEC_${v_dbname}_${v_job_name}_${p_currdate}.log
iecho "INF: Total ${v_cnt_auditjob} jobs finished in ${v_system} system on ${v_date}" | xargs | tee -a ${p_main}${p_log}/${v_date}/MISBI_RUNSPEXEC_${v_dbname}_${v_job_name}_${p_currdate}.log

##********************************* CHECK TOTAL JOBS IN SYSTEM EQUALS TOTAL JOBS FINISH IN SYSTEM ON DATE *********************************##
if [ "${v_cnt_sysfile}" -eq "${v_cnt_auditjob}" ];
then

##********************************* EXECUTE SP_EXEC IN ORACLE *********************************##
iecho "INF: Load data from ${v_dbname} ${v_job_name} on ${v_date}" | tee -a ${p_main}${p_log}/${v_date}/MISBI_RUNSPEXEC_${v_dbname}_${v_job_name}_${p_currdate}.log
cat << EOF > ${p_main}${p_sct}/SP_${v_dbname}_${v_job_name}.sql
set serveroutput on;
BEGIN
DECLARE
V_SCHEMA VARCHAR2(200);
V_JOBNAME VARCHAR2(200);
V_RETURN NUMBER;
BEGIN
SP_EXEC('${v_dbname}','${v_job_name}',V_RETURN);
DBMS_OUTPUT.PUT_LINE('return:'||V_RETURN);
end;
end;
/
disc
exit;
EOF

# DELETE OLD ERROR FILE
if [ -f ${p_main}${p_log}/${v_date}/MISBI_SP_EXEC_${v_dbname}_${v_job_name}_${v_date}.log ]
then
	rm ${p_main}${p_log}/${v_date}/MISBI_SP_EXEC_${v_dbname}_${v_job_name}_${v_date}.log
else
	:
fi

cd ${p_java_path}
	${p_sqlplus} @${p_main}${p_sct}/SP_${v_dbname}_${v_job_name}.sql | tee -a ${p_main}${p_log}/${v_date}/MISBI_SP_EXEC_${v_dbname}_${v_job_name}_${v_date}.log > /dev/null 2>&1	

export v_error_txt=`grep -i "return:-1" ${p_main}${p_log}/${v_date}/MISBI_SP_EXEC_${v_dbname}_${v_job_name}_${v_date}.log`
rm ${p_main}${p_sct}/SP_${v_dbname}_${v_job_name}.sql

##********************************* GET STATUS SEQ_NO IN JOB_NAME ************************************************##
cat << EOF > ${p_main}${p_sct}/${v_job_name}_SEQNO_STATUS.sql
set pages 0 feed off trimspool on line 2000
set time off timing off
set echo off
set verify off

spool ${p_main}${p_sct}/SPOOL_${v_job_name}_SEQNO_STATUS.txt

SELECT 
TRIM(CASE WHEN A.ERR_MSG = 'DONE' THEN 'INF: ' ELSE 'ERR: ' END||'STEP_NM:'||C.STEP_NM||' SEQ_NO:'||C.SEQ_NO||' '||COALESCE(C.DESCRIPTION,'')||' ---> '||CASE WHEN A.ERR_MSG = 'DONE' THEN 'Finished' ELSE 'Failed' END)
FROM $p_schema.CC_EXEC_AUDT_LOG A
INNER JOIN
(
SELECT JOB_NM, PCS_DT, STEP_NM, SEQ_NO, MAX(ID) AS MAX_ID FROM $p_schema.CC_EXEC_AUDT_LOG
WHERE JOB_NM = '${v_job_name}' AND REPLACE(PCS_DT,'-','') = '${v_date}' AND ERR_MSG <> 'START'
GROUP BY JOB_NM, PCS_DT, STEP_NM, SEQ_NO
ORDER BY SEQ_NO ASC
) B
ON A.ID = B.MAX_ID
INNER JOIN $p_schema.CC_SQL_STMT C
ON A.SEQ_NO = C.SEQ_NO
INNER JOIN
(
SELECT Y.JOB_NM, Y.SESSION_ID, Y.JOB_STR_TM, Y.JOB_END_TM FROM $p_schema.CTL_JOB_HIST_LOG Y INNER JOIN 
(SELECT MAX(SESSION_ID) AS MAX_SESSION_ID, JOB_NM FROM $p_schema.CTL_JOB_HIST_LOG WHERE JOB_NM = '${v_job_name}' GROUP BY JOB_NM) Z
ON Y.JOB_NM = Z.JOB_NM AND Y.SESSION_ID = Z.MAX_SESSION_ID
) D
ON D.JOB_NM = A.JOB_NM
WHERE C.EXEC_F = '0' 
AND TO_CHAR(A.UNQ_TMS,'YYYYMMDDHH24MISS') BETWEEN TO_CHAR(D.JOB_STR_TM,'YYYYMMDDHH24MISS') AND COALESCE(TO_CHAR(D.JOB_END_TM,'YYYYMMDDHH24MISS'),'99999999999999')
ORDER BY A.SEQ_NO;

spool off

disc
exit
EOF

cd ${p_java_path}
${p_sqlplus} @${p_main}${p_sct}/${v_job_name}_SEQNO_STATUS.sql >/dev/null 2>&1

integer v_count=1
while IFS= read -r line
do
	export v_msg_st=`awk 'NR=='$v_count'' ${p_main}${p_sct}/SPOOL_${v_job_name}_SEQNO_STATUS.txt`
	iecho "${v_msg_st}" | tee -a ${p_main}${p_log}/${v_date}/MISBI_RUNSPEXEC_${v_dbname}_${v_job_name}_${p_currdate}.log
	
	v_count=$((v_count+1))
done < ${p_main}${p_sct}/SPOOL_${v_job_name}_SEQNO_STATUS.txt

rm ${p_main}${p_sct}/SPOOL_${v_job_name}_SEQNO_STATUS.txt
rm ${p_main}${p_sct}/${v_job_name}_SEQNO_STATUS.sql

##********************************* CHECK ERROR WHEN LOAD SP_EXEC *********************************##
	if [ ! -z "${v_error_txt}" ];
	then
		iecho "ERR: Execute SP_EXEC by ${v_dbname} ${v_job_name} failed" | tee -a ${p_main}${p_log}/${v_date}/MISBI_RUNSPEXEC_${v_dbname}_${v_job_name}_${p_currdate}.log
		iecho "INF: Stop MISBI_RunSpExec.ksh" | tee -a ${p_main}${p_log}/${v_date}/MISBI_RUNSPEXEC_${v_dbname}_${v_job_name}_${p_currdate}.log
		echo "******************************************************************************************************" | tee -a ${p_main}${p_log}/${v_date}/MISBI_RUNSPEXEC_${v_dbname}_${v_job_name}_${p_currdate}.log
		return 1
	else
		iecho "INF: Execute SP_EXEC by ${v_dbname} ${v_job_name} completed" | tee -a ${p_main}${p_log}/${v_date}/MISBI_RUNSPEXEC_${v_dbname}_${v_job_name}_${p_currdate}.log
		rm ${p_main}${p_log}/${v_date}/MISBI_SP_EXEC_${v_dbname}_${v_job_name}_${v_date}.log
		echo "******************************************************************************************************" | tee -a ${p_main}${p_log}/${v_date}/MISBI_RUNSPEXEC_${v_dbname}_${v_job_name}_${p_currdate}.log
		return 0
	fi
else
		iecho "ERR: Total jobs in ${v_system} system NOT EQUAL total jobs finished on ${v_date}" | tee -a ${p_main}${p_log}/${v_date}/MISBI_RUNSPEXEC_${v_dbname}_${v_job_name}_${p_currdate}.log
		
cat << EOF > ${p_main}${p_sct}/PROC_${v_job_name}${v_date}.sql
		INSERT INTO $p_schema.CTL_JOB_HIST_LOG (SESSION_ID, JOB_NM, DATA_DT, JOB_STR_TM, RUN_ST, REMARK)
		VALUES (CAST(TO_CHAR(CURRENT_TIMESTAMP,'YYYYMMDDHH24MISS')AS INT),'${v_job_name}',TO_DATE('${v_date}','YYYY-MM-DD'),CURRENT_TIMESTAMP,'F','Failed');

		COMMIT;
		/
		disc
		exit;
EOF
		chmod 777 ${p_main}${p_sct}/PROC_${v_job_name}${v_date}.sql

		cd ${p_java_path}
		${p_sqlplus} @${p_main}${p_sct}/PROC_${v_job_name}${v_date}.sql > /dev/null 2>&1
	
		rm ${p_main}${p_sct}/PROC_${v_job_name}${v_date}.sql
		
		iecho "INF: Stop MISBI_RunSpExec.ksh" | tee -a ${p_main}${p_log}/${v_date}/MISBI_RUNSPEXEC_${v_dbname}_${v_job_name}_${p_currdate}.log
		echo "******************************************************************************************************" | tee -a ${p_main}${p_log}/${v_date}/MISBI_RUNSPEXEC_${v_dbname}_${v_job_name}_${p_currdate}.log
		return 5
fi
