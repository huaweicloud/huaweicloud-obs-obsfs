#!/bin/bash
if [ `ps x | grep -v grep| grep "/opt/dfv/obsfs/obsfs_log_rotate.sh" | wc -l` -gt 3 ]; then
    echo "already run obsfs_log_rotatei, not repeat"
    exit 0
fi

ONE_KB=1024
ONE_MB=`expr $ONE_KB \* 1024`
ONE_GB=`expr $ONE_MB \* 1024`
LOG_MAX_SIZE=`expr $ONE_MB \* 512`
OBSFS_LOG_PATH="/var/log/obsfs"
mkdir -p ${OBSFS_LOG_PATH}
MAX_LOG_DIR_SIZE=$(df ${OBSFS_LOG_PATH} | awk '{print $2}' | tail -n 1)
MAX_LOG_DIR_SIZE=`expr ${MAX_LOG_DIR_SIZE} \* 4 / 5`
TIME_BASE_STR=`date -d -360day +%Y-%m-%d`
TIME_BASE=`date -d $TIME_BASE_STR +%s`
SCRIPT_PATH="$(cd $(dirname $0);pwd)"
TEMP_LOG_SORT_PATH=/tmp/dfv/obsfs
TEMP_BEFORE_SORT_FILE=${TEMP_LOG_SORT_PATH}/temp_before_sort
TEMP_AFTER_SORT_FILE=${TEMP_LOG_SORT_PATH}/temp_after_sort
MAX_USED_SPACE=80
THRESHOLD_USED_SPACE=75

function delete8Space()
{ 
    cd $1

    local log_package_list=`find -name "*.tar.gz" |grep -v ".old."`
    if [ -n "${log_package_list}" ];then
        space_str=`df ${OBSFS_LOG_PATH} | tail -1 | awk '{print $5}'`
        space=${space_str%%%*}
        if [ ${MAX_USED_SPACE} -lt ${space} ];then
            mkdir -p ${TEMP_LOG_SORT_PATH}
            touch ${TEMP_BEFORE_SORT_FILE}
            touch ${TEMP_AFTER_SORT_FILE}

            for file_path in ${log_package_list}
            do
                time_str=${file_path#*_log*.}
                time8file="${time_str} ${file_path}"
                if [ ! -L ${TEMP_BEFORE_SORT_FILE} ];
                then 
                    # for safe
                    echo ${time8file} >> ${TEMP_BEFORE_SORT_FILE}
                fi
            done
            if [ ! -L ${TEMP_AFTER_SORT_FILE} ];
            then
                # for safe
                sort -n -k 1 -t' ' ${TEMP_BEFORE_SORT_FILE} >> ${TEMP_AFTER_SORT_FILE}
            fi
            log_dir_size_KB=`du -s ${1} | awk '{print $1}'`
            log_dir_size_B=`expr ${log_dir_size_KB} \* 1024`
            while [[ ${THRESHOLD_USED_SPACE} -lt ${space} && ${MAX_LOG_DIR_SIZE} -lt ${log_dir_size_B} ]]
            do
                temp_str=`cat ${TEMP_AFTER_SORT_FILE} | head -1`
                file_str=${temp_str#*./}
                echo ${file_str}
                if [[ "${file_str}" =~ "/" ]];
                then
                    echo "illegal path ${file_str}"
                else
                    # for safe
                    rm -rf ${file_str}
                fi
                if [ ! -L ${TEMP_AFTER_SORT_FILE} ];
                then
                    # for safe
                    sed -i '1d' ${TEMP_AFTER_SORT_FILE}
                fi
                space_str=`df ${OBSFS_LOG_PATH} | tail -1 | awk '{print $5}'`
                space=${space_str%%%*}
                log_dir_size_KB=`du -s ${1} | awk '{print $1}'`
                log_dir_size_B=`expr ${log_dir_size_KB} \* 1024` 
            done

            rm -rf ${TEMP_BEFORE_SORT_FILE}
            rm -rf ${TEMP_AFTER_SORT_FILE}
        fi
    fi
    
    cd -
}

function delete8Time()
{
    cd $1

    local log_package_list=`find -name "*.tar.gz" | grep -v ".old."`
    for file_path in ${log_package_list}
    do
        temp_str=${file_path#*_log*.}
        temp_str=${temp_str:0:10}
        file_create_time_str=`echo ${temp_str} | tr -cd "[0-9]"}`
        if [ -z "${file_create_time_str}" ];then
            continue
        fi
        file_create_time=`date -d ${file_create_time_str} +%s`
        if [ ${file_create_time} -lt ${TIME_BASE} ];
        then
            rm -rf ${file_path}
        fi
    done

    cd -
}

function delete()
{
    delete8Space ${1} 
    delete8Time ${1}
}

function compress()
{
    cd $1
    local old_log_list=`find -name "obsfs_log*" | grep -v ".tar.gz" | grep -v ".swp"`
    local num_suffix=0
    for file_path in ${old_log_list}
    do
        filename=${file_path##*/}
        filepath=${file_path%/*}
        component_log_name=${filename%.*}
        cd $filepath
        need_tar="false"
        # have T, need tar
        files=`find ./ -maxdepth 1 -name "${component_log_name}" | grep -v ".old." | grep -v ".tar.gz" | grep "T"`
        if [ -n "${files}" ];then
            # get current time as the file name of compression package
            if [ ! -L file_list.txt ];
            then
                # for safe
                echo "${files}" >> file_list.txt
                need_tar="true"
            fi
        fi

        # after tar, delete log file one by one
        if [[ "${need_tar}" == "true" ]];then
            time=`date +%Y-%m-%dT%H-%M-%S`
            tar_file="${component_log_name}.${time}_${num_suffix}"
            tar -cvz -T file_list.txt -f ${tar_file}.tar.gz
            ret=$?
            if [ ${ret} -eq 0 ];then
                num_suffix=`expr ${num_suffix} + 1`
                chmod 440 ${tar_file}.tar.gz
            fi
            while read line
            do
                rm -rf ${line}
            done < file_list.txt
            rm -rf file_list.txt
        fi

        cd -
    done
    cd -

    # no T,but process not exist ,need tar
    cd $1
    files_no_T=`find ./ -maxdepth 1 -name "obsfs_log*" | grep -v ".old." | grep -v ".tar.gz" | grep -v "T"`
    if [ -n "${files_no_T}" ];then
        for file_path in ${files_no_T}
        do
            cd $1
            filename=${file_path##*/}
            component_log_name=${filename%.*}
            curr_file_pid=$(echo ${filename} | awk -F 'obsfs_log' '{print $2}')
            check_pid_exist=$(ps -ef | grep obsfs | grep -v grep | awk '{print $2}' | grep ${curr_file_pid})
            if [ -z "${check_pid_exist}" ]; then
                time=`date +%Y-%m-%dT%H-%M-%S`
                tar_file="${component_log_name}.${time}_${num_suffix}"
                tar -cvzf ${tar_file}.tar.gz ${filename}
                ret=$?
                if [ ${ret} -eq 0 ];then
                    num_suffix=`expr ${num_suffix} + 1`
                    chmod 440 ${tar_file}.tar.gz
                    rm -rf ${file_path}
                fi
            fi
            cd -
        done
    fi
    cd -
}

delete ${OBSFS_LOG_PATH}   

compress ${OBSFS_LOG_PATH}