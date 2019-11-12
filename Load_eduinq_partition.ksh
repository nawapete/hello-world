#!/bin/ksh

#################################################################################################

##********************************* IMPORT VARIABLE FORM CONFIG FILE *********************************##
. /data1/misapps/dsl_dev/script/MISBI.cfg

##********************************* DECLARED VARIABLE *********************************##
typeset -l OWNER=$1
typeset -u OWNERSRC=$1


export FM_DBUSER="eslinq"
export FM_DBPWD="eslinq#19"
export FM_DBHOST="10.9.225.147:1522"
export FM_DBSERVICE="ESLCONDB"

export TO_DBUSER="misdba"
export TO_DBPWD="misdba#19"
export TO_DBHOST="10.9.224.230:1522"
export TO_DBSERVICE="poc"

export TBNAME_PATH=$p_main$p_cfg/
export TBNAME_FILE=${OWNER}_config_table_partition.txt

echo 'Start Time : '`date +'%Y-%m-%d %H:%M:%S'`

while read line
	do
		export TABLE_NAME=$(echo ${line} | awk -F'|' '{print $1}')
		export TEMP_TABLE_NAME=$(echo ${line} | awk -F'|' '{print $2}')
		export COL_FILTER=$(echo ${line} | awk -F'|' '{print $3}')
		integer PARTITION_NO=$(echo ${line} | awk -F'|' '{print $4}')
		
cat << EOF > ${p_main}${p_cfg}/DDL_TRANCATE_${TABLE_NAME}.sql

TRUNCATE TABLE $p_schema.${TEMP_TABLE_NAME}
/
disc
exit;
EOF
	chmod 777 ${p_main}${p_cfg}/DDL_TRANCATE_${TABLE_NAME}.sql
	
	cd ${p_java_path}
	${p_sqlplus} @${p_main}${p_cfg}/DDL_TRANCATE_${TABLE_NAME}.sql
		
	rm ${p_main}${p_cfg}/DDL_TRANCATE_${TABLE_NAME}.sql
	
		integer MOD=0
		integer MAX_MOD=PARTITION_NO-1
		integer ROW_SUBSTR=`echo ${#MAX_MOD}+1`
		
		#MOD(CAST(SUBSTR(CIF,-3)AS INTEGER),30) = 0
		
		while [ $MOD -le "${MAX_MOD}" ]
			do 
				iecho "START ${TABLE_NAME} [ MOD = ${MOD} ] [ TOTAL MOD = ${MAX_MOD} ]"
cat << EOF > ${p_main}${p_cfg}/DDL_COPY_${TABLE_NAME}.sql

COPY FROM ${FM_DBUSER}/${FM_DBPWD}@${FM_DBHOST}/${FM_DBSERVICE} TO ${TO_DBUSER}/${TO_DBPWD}@${TO_DBHOST}/${TO_DBSERVICE} - 
APPEND ${TEMP_TABLE_NAME} USING -
SELECT * FROM ${OWNERSRC}.${TABLE_NAME} WHERE MOD(CAST(SUBSTR(${COL_FILTER},-${ROW_SUBSTR}) AS INTEGER),${PARTITION_NO}) = ${MOD}
/
disc
exit;
EOF
				chmod 777 ${p_main}${p_cfg}/DDL_COPY_${TABLE_NAME}.sql 
	
				cd ${p_java_path}
				${p_sqlplus} @${p_main}${p_cfg}/DDL_COPY_${TABLE_NAME}.sql > /dev/null 2>&1
				
				iecho "Load data from ${TABLE_NAME} [ MOD = ${MOD} ] ---> finished"
		
				rm ${p_main}${p_cfg}/DDL_COPY_${TABLE_NAME}.sql 
				
				MOD=$((MOD+1))
			done
	
	iecho "Load data from ${TABLE_NAME} ---> completed all partitions"
	
	done < ${TBNAME_PATH}${TBNAME_FILE}
	
echo 'Finish Time : '`date +'%Y-%m-%d %H:%M:%S'`
