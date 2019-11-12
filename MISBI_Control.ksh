#!/bin/ksh

#################################################################################################
#------------------------------------------------------------------------------------------------
#  Program Name     : MISBI_Control.ksh
#  Description      : Check before load data into staging table
#  Created by       : Nawapon.L
#  Create Date      : 16/10/2019
#
#  Usage: Call form MISBI_Load.ksh
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

export v_app=$1
export v_src_path=$2
export v_date=$3
export v_file_nm=$4
export v_stg_table=$5
export v_type=$6
export v_data_sf=$7
export v_ctl_sf=$8
export v_delim=$9

export v_date_ctl=`awk -F'|' 'NR==1{print $1}' ${v_src_path}/${v_app}_${v_file_nm}_${v_type}_${v_date}.${v_ctl_sf}`
export v_ctl=`awk -F'|' 'NR==1{print $2}' ${v_src_path}/${v_app}_${v_file_nm}_${v_type}_${v_date}.${v_ctl_sf}`
export v_file_txt=${v_src_path}/${v_app}_${v_file_nm}_${v_type}_${v_date}.${v_data_sf}
export v_file_ctl=${v_src_path}/${v_app}_${v_file_nm}_${v_type}_${v_date}.${v_ctl_sf}
export v_filename=${v_app}_${v_file_nm}_${v_type}_${v_date}.${v_data_sf}
export v_cnt_txt=`awk 'NR>1{c++} END {print c}' ${v_file_txt}`

if [ -z "${v_cnt_txt}" ];
then 
	export v_cnt_txt=0
else
	:
fi

iecho "INF: Start load file ${v_app}_${v_file_nm}_${v_type}_${v_date}"

##************************* CHECK SRC & CTL FILE *************************##
iecho "INF: Check ${v_data_sf} file and ${v_ctl_sf} file before load data"

if [ ! -f "${v_file_txt}" ] && [ ! -f "${v_file_ctl}" ];
then
	iecho "ERR: File name ${v_app}_${v_file_nm}_${v_type}_${v_date} -> ${v_data_sf} file AND ${v_ctl_sf} file do NOT exist"
cat << EOF > ${p_main}${p_sct}/PROC_${v_file_nm}${v_date}.sql
	INSERT INTO $p_schema.DSL_AUDIT_JOB(DATE_KEY,JOB_NAME,SEQ,LOAD_TYPE,START_DATE,CTL_ROWS,SRC_ROWS,LOAD_DATA_ROWS,END_DATE,FLAG_COMPLETED,REMARK)
				VALUES('${v_date}','${v_stg_table}',(SELECT CASE WHEN MAX(SEQ) IS NULL THEN 1 ELSE MAX(SEQ)+1 END FROM $p_schema.DSL_AUDIT_JOB WHERE (DATE_KEY='$v_date' 
				AND JOB_NAME='${v_stg_table}')),'${v_type}',SYSDATE,NULL,NULL,NULL,SYSDATE,'B','SOURCE FILE AND ${v_ctl_sf} FILE DO NOT EXIST');
	COMMIT;
/
disc
exit;
EOF
	chmod 777 ${p_main}${p_sct}/PROC_${v_file_nm}${v_date}.sql

	cd ${p_java_path}
	${p_sqlplus} @${p_main}${p_sct}/PROC_${v_file_nm}${v_date}.sql > /dev/null 2>&1
	
	rm ${p_main}${p_sct}/PROC_${v_file_nm}${v_date}.sql

	iecho "INF: Stop load file ${v_app}_${v_file_nm}_${v_type}_${v_date}"
	echo "******************************************************************************************************"
	return 5
	
elif [ -f "${v_file_txt}" ] && [ ! -f "${v_file_ctl}" ];
then
	iecho "ERR: File name ${v_app}_${v_file_nm}_${v_type}_${v_date} -> ${v_ctl_sf} file does NOT exists"
	
cat << EOF > ${p_main}${p_sct}/PROC_${v_file_nm}${v_date}.sql
	INSERT INTO $p_schema.DSL_AUDIT_JOB(DATE_KEY,JOB_NAME,SEQ,LOAD_TYPE,START_DATE,CTL_ROWS,SRC_ROWS,LOAD_DATA_ROWS,END_DATE,FLAG_COMPLETED,REMARK)
				VALUES('${v_date}','${v_stg_table}',(SELECT CASE WHEN MAX(SEQ) IS NULL THEN 1 ELSE MAX(SEQ)+1 END FROM $p_schema.DSL_AUDIT_JOB WHERE (DATE_KEY='${v_date}' 
				AND JOB_NAME= '${v_stg_table}'))
				,'${v_type}',SYSDATE,NULL,'${v_cnt_txt}',NULL,SYSDATE,'B','${v_ctl_sf} FILE DOES NOT EXISTS');
	COMMIT;
/
disc
exit;
EOF
	chmod 777 ${p_main}${p_sct}/PROC_${v_file_nm}${v_date}.sql

	cd ${p_java_path}
	${p_sqlplus} @${p_main}${p_sct}/PROC_${v_file_nm}${v_date}.sql > /dev/null 2>&1
	
	rm ${p_main}${p_sct}/PROC_${v_file_nm}${v_date}.sql
	
	iecho "INF: Stop load file ${v_app}_${v_file_nm}_${v_type}_${v_date}"
	echo "******************************************************************************************************"
	return 5
	
elif [ ! -f "${v_file_txt}" ] && [ -f "${v_file_ctl}" ];
then
	iecho "ERR: File name ${v_app}_${v_file_nm}_${v_type}_${v_date} -> ${v_data_sf} file does NOT exists"
cat << EOF > ${p_main}${p_sct}/PROC_${v_file_nm}${v_date}.sql
	INSERT INTO $p_schema.DSL_AUDIT_JOB(DATE_KEY,JOB_NAME,SEQ,LOAD_TYPE,START_DATE,CTL_ROWS,SRC_ROWS,LOAD_DATA_ROWS,END_DATE,FLAG_COMPLETED,REMARK)
				VALUES('${v_date}','${v_stg_table}',(SELECT CASE WHEN MAX(SEQ) IS NULL THEN 1 ELSE MAX(SEQ)+1 END FROM DSL_AUDIT_JOB WHERE (DATE_KEY='${v_date}' 
				AND JOB_NAME= '${v_stg_table}'))
				,'${v_type}',SYSDATE,'${v_ctl}',NULL,NULL,SYSDATE,'B','SOURCE FILE DOES NOT EXISTS');
	COMMIT;
/
disc
exit;
EOF
	chmod 777 ${p_main}${p_sct}/PROC_${v_file_nm}${v_date}.sql

	cd ${p_java_path}
	${p_sqlplus} @${p_main}${p_sct}/PROC_${v_file_nm}${v_date}.sql > /dev/null 2>&1
	
	rm ${p_main}${p_sct}/PROC_${v_file_nm}${v_date}.sql
	iecho "INF: Stop load file ${v_app}_${v_file_nm}_${v_type}_${v_date}"
	echo "******************************************************************************************************"
	return 5
	
elif [ "${v_date}" -ne "${v_date_ctl}" ];
then
	iecho "INF: File name ${v_app}_${v_file_nm}_${v_type}_${v_date} -> Data date = ${v_date}"
	iecho "INF: File name ${v_app}_${v_file_nm}_${v_type}_${v_date} -> Date in ${v_ctl_sf} file = ${v_date_ctl}"
	iecho "ERR: File name ${v_app}_${v_file_nm}_${v_type}_${v_date} -> Date in ${v_ctl_sf} file NOT equal data date"
	
cat << EOF > ${p_main}${p_sct}/PROC_${v_file_nm}${v_date}.sql
	INSERT INTO $p_schema.DSL_AUDIT_JOB(DATE_KEY,JOB_NAME,SEQ,LOAD_TYPE,START_DATE,CTL_ROWS,SRC_ROWS,LOAD_DATA_ROWS,END_DATE,FLAG_COMPLETED,REMARK)
				VALUES('${v_date}','${v_stg_table}',(SELECT CASE WHEN MAX(SEQ) IS NULL THEN 1 ELSE MAX(SEQ)+1 END FROM $p_schema.DSL_AUDIT_JOB WHERE (DATE_KEY='${v_date}' 
				AND JOB_NAME= '${v_stg_table}'))
				,'${v_type}',SYSDATE,NULL,NULL,NULL,SYSDATE,'B','DATE IN CTL FILE NOT EQUALS DATA DATE');
	COMMIT;
/
disc
exit;
EOF
	chmod 777 ${p_main}${p_sct}/PROC_${v_file_nm}${v_date}.sql

	cd ${p_java_path}
	${p_sqlplus} @${p_main}${p_sct}/PROC_${v_file_nm}${v_date}.sql > /dev/null 2>&1
	
	rm ${p_main}${p_sct}/PROC_${v_file_nm}${v_date}.sql
	iecho "INF: Stop load file ${v_app}_${v_file_nm}_${v_type}_${v_date}"
	echo "******************************************************************************************************"
	return 5

##********************CHECK CTL AND SRC NO DATA ********************##
elif [ "${v_ctl}" -eq 0 ] && [ "${v_cnt_txt}" -eq 0 ];
then
cat << EOF > ${p_main}${p_sct}/PROC_${v_file_nm}${v_date}.sql
	INSERT INTO $p_schema.DSL_AUDIT_JOB(DATE_KEY,JOB_NAME,SEQ,LOAD_TYPE,START_DATE,CTL_ROWS,SRC_ROWS,LOAD_DATA_ROWS,END_DATE,FLAG_COMPLETED,REMARK)
                VALUES('${v_date}','${v_stg_table}',(SELECT CASE WHEN MAX(SEQ) IS NULL THEN 1 ELSE MAX(SEQ)+1 END FROM $p_schema.DSL_AUDIT_JOB WHERE (DATE_KEY='${v_date}' 
				AND JOB_NAME= '${v_stg_table}'))
				,'${v_type}',SYSDATE,'0','0',NULL,SYSDATE,'W','NO DATA FROM SOURCE FILE');
COMMIT;
/
disc
exit;
EOF
	chmod 777 ${p_main}${p_sct}/PROC_${v_file_nm}${v_date}.sql

	cd ${p_java_path}
	${p_sqlplus} @${p_main}${p_sct}/PROC_${v_file_nm}${v_date}.sql > /dev/null 2>&1
	
	iecho "INF: No data from source file"
	iecho "INF: Stop load file ${v_app}_${v_file_nm}_${v_type}_${v_date}"
	echo "******************************************************************************************************"
	
	rm ${p_main}${p_sct}/PROC_${v_file_nm}${v_date}.sql
	return 0
	
elif [ "${v_ctl}" -ne "${v_cnt_txt}" ];
then
	iecho "INF: File name ${v_app}_${v_file_nm}_${v_type}_${v_date} -> ${v_data_sf} file ${v_cnt_txt} rows" | xargs
	iecho "INF: File name ${v_app}_${v_file_nm}_${v_type}_${v_date} -> ${v_ctl_sf} file ${v_ctl} rows"
	iecho "ERR: File name ${v_app}_${v_file_nm}_${v_type}_${v_date} -> Rows in ${v_data_sf} file NOT equal ${v_ctl_sf} file"
cat << EOF > ${p_main}${p_sct}/PROC_${v_file_nm}${v_date}.sql
	INSERT INTO $p_schema.DSL_AUDIT_JOB(DATE_KEY,JOB_NAME,SEQ,LOAD_TYPE,START_DATE,CTL_ROWS,SRC_ROWS,LOAD_DATA_ROWS,END_DATE,FLAG_COMPLETED,REMARK)
				VALUES('${v_date}','${v_stg_table}',(SELECT CASE WHEN MAX(SEQ) IS NULL THEN 1 ELSE MAX(SEQ)+1 END FROM $p_schema.DSL_AUDIT_JOB WHERE (DATE_KEY='${v_date}' 
				AND JOB_NAME= '${v_stg_table}'))
				,'${v_type}',SYSDATE,'${v_ctl}','${v_cnt_txt}',NULL,SYSDATE,'B','${v_ctl_sf} FILE AND SOURCE FILE ROWS NOT MATCHED');
	COMMIT;
/
disc
exit;
EOF
	chmod 777 ${p_main}${p_sct}/PROC_${v_file_nm}${v_date}.sql

	cd ${p_java_path}
	${p_sqlplus} @${p_main}${p_sct}/PROC_${v_file_nm}${v_date}.sql > /dev/null 2>&1
	
	rm ${p_main}${p_sct}/PROC_${v_file_nm}${v_date}.sql
	iecho "INF: Stop load file ${v_app}_${v_file_nm}_${v_type}_${v_date}"
	echo "******************************************************************************************************"
	return 5
else
	iecho "INF: File name ${v_app}_${v_file_nm}_${v_type}_${v_date} -> ${v_data_sf} file and ${v_ctl_sf} file do exist" 
	iecho "INF: File name ${v_app}_${v_file_nm}_${v_type}_${v_date} -> ${v_data_sf} file ${v_cnt_txt} rows" | xargs
	iecho "INF: File name ${v_app}_${v_file_nm}_${v_type}_${v_date} -> ${v_ctl_sf} file ${v_ctl} rows"
	iecho "INF: File name ${v_app}_${v_file_nm}_${v_type}_${v_date} -> Rows in ${v_data_sf} file equal ${v_ctl_sf} file"
>&2
fi

##********************************* CHECK ^M IN THIS FILE ************************************************##
iecho "INF: Check ^M in file ${v_app}_${v_file_nm}_${v_type}_${v_date}"
tr -d '\r' < ${v_file_txt} > temp.$$ && mv temp.$$ ${v_file_txt}
iecho "INF: Remove ^M in file ${v_app}_${v_file_nm}_${v_type}_${v_date} completed"

##********************************* CHECK STAGING TABLE IN DATABASE ************************************************##
iecho "INF: Check table ${v_stg_table} in database"

cat << EOF > ${p_main}${p_sct}/PROC_${v_file_nm}${v_date}.sql
set pages 0 feed off trimspool on line 2000
set time off timing off
set echo off
set verify off

spool ${p_main}${p_sct}/${v_date}_CHECK_TABLE.txt

SELECT CASE WHEN count(*) = 1 THEN 'Y' ELSE 'N' END FROM ALL_TABLES
WHERE TABLE_NAME ='${v_stg_table}';

spool off

disc
exit
EOF

cd ${p_java_path}
${p_sqlplus} @${p_main}${p_sct}/PROC_${v_file_nm}${v_date}.sql >/dev/null 2>&1

chmod 777 ${p_main}${p_sct}/${v_date}_CHECK_TABLE.txt

export v_check_tb=`awk 'NR==1{print $1}' ${p_main}${p_sct}/${v_date}_CHECK_TABLE.txt`
rm ${p_main}${p_sct}/PROC_${v_file_nm}${v_date}.sql

if [ ${v_check_tb} = 'Y' ];
then
	iecho "INF: Table ${v_stg_table} already existed in database"
	rm ${p_main}${p_sct}/${v_date}_CHECK_TABLE.txt
else
	iecho "ERR: Table ${v_stg_table} NOT in database."
	
cat << EOF > ${p_main}${p_sct}/PROC_${v_file_nm}${v_date}.sql
	INSERT INTO $p_schema.DSL_AUDIT_JOB(DATE_KEY,JOB_NAME,SEQ,LOAD_TYPE,START_DATE,CTL_ROWS,SRC_ROWS,LOAD_DATA_ROWS,END_DATE,FLAG_COMPLETED,REMARK)
				VALUES('${v_date}','${v_stg_table}',(SELECT CASE WHEN MAX(SEQ) IS NULL THEN 1 ELSE MAX(SEQ)+1 END FROM DSL_AUDIT_JOB WHERE (DATE_KEY='${v_date}' 
				AND JOB_NAME= '${v_stg_table}'))
				,'${v_type}',SYSDATE,'${v_ctl}','${v_cnt_txt}',NULL,SYSDATE,'N','TABLE ${v_stg_table} NOT IN DATABASE');
	COMMIT;
/
disc
exit;
EOF
	chmod 777 ${p_main}${p_sct}/PROC_${v_file_nm}${v_date}.sql

	cd ${p_java_path}
	${p_sqlplus} @${p_main}${p_sct}/PROC_${v_file_nm}${v_date}.sql > /dev/null 2>&1
	
	rm ${p_main}${p_sct}/PROC_${v_file_nm}${v_date}.sql
	rm ${p_main}${p_sct}/${v_date}_CHECK_TABLE.txt
	iecho "INF: Stop load file ${v_app}_${v_file_nm}_${v_type}_${v_date}"
	echo "******************************************************************************************************"
	return 1
fi

##********************CHECK LOG FILE & BAD FILE IN PATH******************************************##
if [ -f "${p_main}""${p_log}"/"${v_date}"/"${v_app}"_"${v_file_nm}"_"${v_type}"_"${v_date}".log ] && 
[ -f "${p_main}""${p_log}"/"${v_date}"/"${v_app}"_"${v_file_nm}"_"${v_type}"_"${v_date}".bad ]
then
	rm ${p_main}${p_log}/${v_date}/${v_app}_${v_file_nm}_${v_type}_${v_date}.log
	rm ${p_main}${p_log}/${v_date}/${v_app}_${v_file_nm}_${v_type}_${v_date}.bad
elif [ -f "${p_main}""${p_log}"/"${v_date}"/"${v_app}"_"${v_file_nm}"_"${v_type}"_"${v_date}".log ];
then
	rm ${p_main}${p_log}/${v_date}/${v_app}_${v_file_nm}_${v_type}_${v_date}.log
elif [ -f "${p_main}""${p_log}"/"${v_date}"/"${v_app}"_"${v_file_nm}"_"${v_type}"_"${v_date}".bad ];
then
	rm ${p_main}${p_log}/${v_date}/${v_app}_${v_file_nm}_${v_type}_${v_date}.bad
else
	:
fi

##********************CREATE DDL FILE FOR LOAD DATA******************************************##
cat << EOF > ${p_main}${p_sct}/DDL_${v_file_nm}.sql
set pages 0 feed off trimspool on line 2000
set time off timing off
set long 90000
set echo off
set verify off

spool ${p_main}${p_sct}/SPOOL_LOAD_${v_app}_${v_file_nm}.DDL

SELECT A.DDL FROM $p_schema.DSL_EXTRACT_SYSTEM_FILE A
INNER JOIN $p_schema.DSL_EXTRACT_SYSTEM B
ON A.CTL_ID = B.CTL_ID
WHERE SYSTEM_NAME = '${v_app}' AND DATA_FILE_NAME='${v_file_nm}' AND IS_LOAD ='Y';

spool off

disc
exit
EOF

cd ${p_java_path}
${p_sqlplus} @${p_main}${p_sct}/DDL_${v_file_nm}.sql >/dev/null 2>&1

##*************************************REPLACE PARAMETER IN DDL SPOOL FILE******************************************##

sed "s/#DELIMETER#/${v_delim}/g; s/#SCHEMA#/${p_schema}/g; s/#DATE#/${v_date}/g" ${p_main}${p_sct}/SPOOL_LOAD_${v_app}_${v_file_nm}.DDL > ${p_main}${p_sct}/LOAD_${v_app}_${v_file_nm}.DDL
rm ${p_main}${p_sct}/SPOOL_LOAD_${v_app}_${v_file_nm}.DDL

##********************LOAD DATA FROM SOURCE FILE TO STG TABLE******************************************##
iecho "INF: Load data from file ${v_app}_${v_file_nm}_${v_type}_${v_date}.${v_data_sf} into table ${v_stg_table}"

java DSL -ldr misdba ESLDEV control=${p_main}${p_sct}/LOAD_${v_app}_${v_file_nm}.DDL data=${v_src_path}/${v_app}_${v_file_nm}_${v_type}_${v_date}.${v_data_sf} bad=${p_main}${p_log}/${v_date}/${v_app}_${v_file_nm}_${v_type}_${v_date}.bad log=${p_main}${p_log}/${v_date}/${v_app}_${v_file_nm}_${v_type}_${v_date}.log discardmax=10000 errors=100000 > /dev/null 2>&1

rm ${p_main}${p_sct}/DDL_${v_file_nm}.sql
rm ${p_main}${p_sct}/LOAD_${v_app}_${v_file_nm}.DDL

##*************************************CHECK BAD RECORD******************************************##
if [ -f "${p_main}""${p_log}"/"${v_date}"/"${v_app}"_"${v_file_nm}"_"${v_type}"_"${v_date}".bad ]
then
export v_bad_row=`cat ${p_main}${p_log}/${v_date}/${v_app}_${v_file_nm}_${v_type}_${v_date}.bad | wc -l`

		iecho "ERR: Load data failed ${v_bad_row} rows" | xargs
		iecho "ERR: Load data into table ${v_stg_table} failed"
		
cat << EOF > ${p_main}${p_sct}/PROC_${v_file_nm}${v_date}.sql
	INSERT INTO $p_schema.DSL_AUDIT_JOB(DATE_KEY,JOB_NAME,SEQ,LOAD_TYPE,START_DATE,CTL_ROWS,SRC_ROWS,LOAD_DATA_ROWS,END_DATE,FLAG_COMPLETED,REMARK)
				VALUES('${v_date}','${v_stg_table}',(SELECT CASE WHEN MAX(SEQ) IS NULL THEN 1 ELSE MAX(SEQ)+1 END FROM $p_schema.DSL_AUDIT_JOB WHERE (DATE_KEY='${v_date}' 
				AND JOB_NAME= '${v_stg_table}'))
				,'${v_type}',SYSDATE,'${v_ctl}','${v_cnt_txt}',NULL,SYSDATE,'N','LOAD DATA INTO STAGING TABLE FAILED');
	COMMIT;
/
disc
exit;
EOF
	chmod 777 ${p_main}${p_sct}/PROC_${v_file_nm}${v_date}.sql

	cd ${p_java_path}
	${p_sqlplus} @${p_main}${p_sct}/PROC_${v_file_nm}${v_date}.sql > /dev/null 2>&1
	
	rm ${p_main}${p_sct}/PROC_${v_file_nm}${v_date}.sql
	iecho "INF: Stop load file ${v_app}_${v_file_nm}_${v_type}_${v_date}"
	echo "******************************************************************************************************"
	return 1	
else
	:
fi
	
##*************************************RECONCILE DATA BETWEEN STG AND SOURCE FILE******************************************##

#cat << EOF > ${p_main}${p_sct}/COUNT_${v_stg_table}.sql
#set pages 0 feed off trimspool on line 2000
#set time off timing off
#set long 90000
#set echo off
#set verify off

#spool ${p_main}${p_sct}/COUNT_${v_stg_table}.txt

#SELECT COUNT(*) FROM $p_schema.${v_stg_table}
#WHERE DATA_DATE='${v_date}';

#spool off

#disc
#exit
#EOF

#cd ${p_java_path}
#${p_sqlplus} @${p_main}${p_sct}/COUNT_${v_stg_table}.sql >/dev/null 2>&1

#export v_cnt_stg=`awk 'NR==1{print $1}' ${p_main}${p_sct}/COUNT_${v_stg_table}.txt`

#rm ${p_main}${p_sct}/COUNT_${v_stg_table}.sql
#rm ${p_main}${p_sct}/COUNT_${v_stg_table}.txt

#if [ -z "${v_cnt_stg}" ];
#then 
#	export v_cnt_stg=0
#else
#	:
#fi

export v_cnt_stg=`grep 'successfully loaded' ${p_main}${p_log}/${v_date}/${v_app}_${v_file_nm}_${v_type}_${v_date}.log | awk -F ' ' 'NR==1{print $1}'`

if [ "${v_cnt_stg}" -eq "${v_ctl}" ] && [ "${v_cnt_stg}" -eq "${v_cnt_txt}" ] && [ "${v_cnt_txt}" -eq "${v_ctl}" ]
then
	iecho "INF: Load data into table ${v_stg_table} completed"
cat << EOF > ${p_main}${p_sct}/PROC_${v_file_nm}${v_date}.sql
	INSERT INTO $p_schema.DSL_AUDIT_JOB(DATE_KEY,JOB_NAME,SEQ,LOAD_TYPE,START_DATE,CTL_ROWS,SRC_ROWS,LOAD_DATA_ROWS,END_DATE,FLAG_COMPLETED,REMARK)
				VALUES('${v_date}','${v_stg_table}',(SELECT CASE WHEN MAX(SEQ) IS NULL THEN 1 ELSE MAX(SEQ)+1 END FROM $p_schema.DSL_AUDIT_JOB WHERE (DATE_KEY='${v_date}' 
				AND JOB_NAME= '${v_stg_table}'))
				,'${v_type}',SYSDATE,'${v_ctl}','${v_cnt_txt}','${v_cnt_stg}',SYSDATE,'Y','LOAD DATA COMPLETED');
	COMMIT;
/
disc
exit;
EOF
	chmod 777 ${p_main}${p_sct}/PROC_${v_file_nm}${v_date}.sql

	cd ${p_java_path}
	${p_sqlplus} @${p_main}${p_sct}/PROC_${v_file_nm}${v_date}.sql > /dev/null 2>&1
	
	rm ${p_main}${p_sct}/PROC_${v_file_nm}${v_date}.sql
	
	iecho "INF: Load file ${v_app}_${v_file_nm}_${v_type}_${v_date} finished"
	echo "******************************************************************************************************"
	return 0
else
	iecho "ERR: Load data into table ${v_stg_table} NOT equal ${v_file_nm}"
cat << EOF > ${p_main}${p_sct}/PROC_${v_file_nm}${v_date}.sql
	INSERT INTO $p_schema.DSL_AUDIT_JOB(DATE_KEY,JOB_NAME,SEQ,LOAD_TYPE,START_DATE,CTL_ROWS,SRC_ROWS,LOAD_DATA_ROWS,END_DATE,FLAG_COMPLETED,REMARK)
				VALUES('${v_date}','${v_stg_table}',(SELECT CASE WHEN MAX(SEQ) IS NULL THEN 1 ELSE MAX(SEQ)+1 END FROM $p_schema.DSL_AUDIT_JOB WHERE (DATE_KEY='${v_date}' 
				AND JOB_NAME= '${v_stg_table}'))
				,'${v_type}',SYSDATE,'${v_ctl}','${v_cnt_txt}','${v_cnt_stg}',SYSDATE,'N','LOAD DATA INTO STAGING TABLE NOT EQUAL SOURCE FILE');
	COMMIT;
/
disc
exit;
EOF
	chmod 777 ${p_main}${p_sct}/PROC_${v_file_nm}${v_date}.sql

	cd ${p_java_path}
	${p_sqlplus} @${p_main}${p_sct}/PROC_${v_file_nm}${v_date}.sql > /dev/null 2>&1
	
	rm ${p_main}${p_sct}/PROC_${v_file_nm}${v_date}.sql
	
	iecho "INF: Stop load file ${v_app}_${v_file_nm}_${v_type}_${v_date}"
	echo "******************************************************************************************************"
	return 5
fi
