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

export v_file_txt=${v_src_path}/${v_app}_${v_file_nm}_${v_type}_${v_date}.${v_data_sf}
export v_filename=${v_app}_${v_file_nm}_${v_type}_${v_date}.${v_data_sf}


iecho "INF: Start load file ${v_app}_${v_file_nm}_${v_type}_${v_date}"


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

java DSL -ldr misdba ESLDEV control=${p_main}${p_sct}/LOAD_${v_app}_${v_file_nm}.DDL data=${v_src_path}/${v_app}_${v_file_nm}_${v_type}_${v_date}.${v_data_sf} bad=${p_main}${p_log}/${v_date}/${v_app}_${v_file_nm}_${v_type}_${v_date}.bad log=${p_main}${p_log}/${v_date}/${v_app}_${v_file_nm}_${v_type}_${v_date}.log discardmax=10000

rm ${p_main}${p_sct}/DDL_${v_file_nm}.sql
rm ${p_main}${p_sct}/LOAD_${v_app}_${v_file_nm}.DDL

iecho "INF: Load file ${v_app}_${v_file_nm}_${v_type}_${v_date} finished"
echo "******************************************************************************************************"
return 0
