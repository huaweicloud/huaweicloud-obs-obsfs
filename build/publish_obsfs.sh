#!/bin/sh
SCRIPT_PATH="$(cd $(dirname $0);pwd)"
function log()
{
    echo "[`date "+%Y-%m-%d %H:%M:%S"`] $*"
}

currenttime=$(date +%F%T | sed 's/-//g' | sed 's/://g' | sed 's/ //g')


mkdir -p ${SCRIPT_PATH}/obsfs
if [ ! -f "${SCRIPT_PATH}/../src/obsfs" ]; then
    log "ERROR: there is no obsfs."
    exit 1
fi

# copy obsfs
cp ${SCRIPT_PATH}/../src/obsfs ${SCRIPT_PATH}/obsfs
sh ${SCRIPT_PATH}/get_version.sh ${SCRIPT_PATH}/obsfs
if [ $? -ne 0 ]; then
    log "ERROR: get version failed."
    exit 1
fi

# copy log_rotate
cp -f ${SCRIPT_PATH}/install_obsfs.sh ${SCRIPT_PATH}/obsfs
cp -f ${SCRIPT_PATH}/obsfs_log_rotate.sh ${SCRIPT_PATH}/obsfs

# make tar.gz
platform=`cat /etc/euleros-release`
if [ "$platform" == "EulerOS release 2.0 (SP2)" ] || [ "$platform" == "EulerOS release 2.0 (SP8)" ]; then
	if [ `uname -m` == "x86_64" ]; then
                mv obsfs obsfs_CentOS7.6_amd64.${currenttime}
                tar -cvzf obsfs_CentOS7.6_amd64.${currenttime}.tar.gz obsfs_CentOS7.6_amd64.${currenttime}
	else
                mv obsfs obsfs_EulerOS2.8_arm64.${currenttime}
                tar -cvzf obsfs_EulerOS2.8_arm64.${currenttime}.tar.gz obsfs_EulerOS2.8_arm64.${currenttime}
	fi
else
        mv obsfs obsfs_Ubuntu16.04_amd64.${currenttime}
        tar -cvzf obsfs_Ubuntu16.04_amd64.${currenttime}.tar.gz obsfs_Ubuntu16.04_amd64.${currenttime}
fi
if [ $? -ne 0 ]; then
    log "ERROR: tar obsfs failed."
    exit 1
fi
