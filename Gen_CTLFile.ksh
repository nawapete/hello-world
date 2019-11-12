#!/bin/ksh

#################################################################################################
# -----------------------------------------------------------------------------------------------
#  Program Name     : Gen_CTLFile.ksh
#  Description      : Generate Control file in DSL_EXTRACT_SYSTEM_FILE each system
#  Created by       : Nawapon L.
#  Create Date      : 02/10/2019
#
#  Usage: . Gen_CTLFile.ksh <System name> <Data date format YYYYMMDD>
#  Example: . Gen_CTLFile.ksh DMS 20190716
#  File Path: /data1/misapps/dsl_dev/tmp/gen_ctlfile
#-----------------------------------------------------------------------------------------------
#################################################################################################

##********************************* IMPORT VARIABLE FORM CONFIG FILE************************************************##
. /data1/misapps/dsl_dev/script/MISBI.cfg

##********************************* DECLARED VARIABLE ************************************************##
export v_system=$1
export v_date=$2

#Check parameter
if [ "${v_system}" != "" ] && [ "${v_date}" != "" ]
then

#Check folder keep ctl file
	if [ ! -d "${p_main}""${p_tmp}"/gen_ctlfile/ ];
	then
		mkdir -p $p_main$p_tmp/gen_ctlfile/
		chmod 777 $p_main$p_tmp/gen_ctlfile/
	else
		:
	fi
	
##Get file name in DSL_EXTRACT_SYSTEM
cat << EOF > ${p_main}${p_tmp}/gen_ctlfile/${v_system}_${v_date}.sql
set pages 0 feed off trimspool on line 2000
set time off timing off
set echo off
set verify off

spool ${p_main}${p_tmp}/gen_ctlfile/${v_system}_${v_date}.txt

SELECT C.SYSTEM_NAME||'|'||C.PATH_NAME||'|'||B.DATA_FILE_NAME||'|'||B.DATA_FILE_SUFFIX||'|'||B.CTL_FILE_NAME||'|'||B.CTL_FILE_SUFFiX||'|'||B.EXPORT_TYPE
FROM $p_schema.DSL_EXTRACT_SYSTEM_FILE B
INNER JOIN $p_schema.DSL_EXTRACT_SYSTEM C
ON B.CTL_ID = C.CTL_ID
WHERE C.SYSTEM_NAME ='${v_system}' AND B.IS_LOAD ='Y';

spool off

disc
exit
EOF

cd ${p_java_path}
${p_sqlplus} @${p_main}${p_tmp}/gen_ctlfile/${v_system}_${v_date}.sql >/dev/null 2>&1

chmod 777 ${p_main}${p_tmp}/gen_ctlfile/${v_system}_${v_date}.txt


##********************************* RUN EXTRACT_SYSTEM_FILE TABLE LOOP *********************************##
integer v_count=1
while IFS= read -r line
do
	export v_src_path=`awk -F'|' 'NR=='$v_count'{print $2}' ${p_main}${p_tmp}/gen_ctlfile/${v_system}_${v_date}.txt`
	export v_file_nm=`awk -F'|' 'NR=='$v_count'{print $3}' ${p_main}${p_tmp}/gen_ctlfile/${v_system}_${v_date}.txt`
	export v_data_sf=`awk -F'|' 'NR=='$v_count'{print $4}' ${p_main}${p_tmp}/gen_ctlfile/${v_system}_${v_date}.txt`
	export v_ctl_nm=`awk -F'|' 'NR=='$v_count'{print $5}' ${p_main}${p_tmp}/gen_ctlfile/${v_system}_${v_date}.txt`
	export v_ctl_sf=`awk -F'|' 'NR=='$v_count'{print $6}' ${p_main}${p_tmp}/gen_ctlfile/${v_system}_${v_date}.txt`
	export v_type=`awk -F'|' 'NR=='$v_count'{print $7}'  ${p_main}${p_tmp}/gen_ctlfile/${v_system}_${v_date}.txt`
			
	export v_cnt_src=`awk 'NR>1{c++} END {print c}' ${v_src_path}/${v_system}_${v_file_nm}_${v_type}_${v_date}.${v_data_sf}`
	
	if [ -z "${v_cnt_src}" ]
		then 
			export v_cnt_txt=0
		else
			export v_cnt_txt=${v_cnt_src}
	fi
	
	echo "${v_date}|${v_cnt_txt}" > ${p_main}${p_tmp}/gen_ctlfile/${v_system}_${v_file_nm}_${v_type}_${v_date}.${v_ctl_sf}
	
	echo "Create finish ---> ${v_system}_${v_file_nm}_${v_type}_${v_date}.${v_ctl_sf}"
	v_count=$((v_count+1))
	
done < ${p_main}${p_tmp}/gen_ctlfile/${v_system}_${v_date}.txt

rm ${p_main}${p_tmp}/gen_ctlfile/${v_system}_${v_date}.sql
rm ${p_main}${p_tmp}/gen_ctlfile/${v_system}_${v_date}.txt

else
	echo "Please Pass Parameter : System Name & Data Date!!"
fi
