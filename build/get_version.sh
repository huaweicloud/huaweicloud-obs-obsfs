#!/bin/sh

# include build_lib.sh
SCRIPT_PATH="$(cd $(dirname $0);pwd)"
function log()
{
    echo "[`date "+%Y-%m-%d %H:%M:%S"`] $*"
}

if [ $# -ne 1 ] || [ ! -d $1 ];then
    log "ERROR: add git version.txt file failed."
    exit 1
fi

VERSION_FILE_PATH=$1
GIT_VERSION_FILE=${VERSION_FILE_PATH}/version.txt
COMMIT_ID=`git log | head -n 4 | grep commit | awk  '{print $2}'`
COMMIT_DATE=`git log | head -n 4 | grep Date`
BRANCH=`git branch`
if [ -n "`echo ${BRANCH} | grep ' '`" ];then
    if [ -n "$(echo ${BRANCH} | grep detached)" ];then
        BRANCH=`git branch | grep -v detached | xargs echo`
    else
        BRANCH=`git branch | grep "*" | awk  '{print $2}'`
    fi
fi

if [ ! -n "${COMMIT_ID}" ] || [ ! -n "${COMMIT_DATE}" ] || [ ! -n "${BRANCH}" ];then
    log "ERROR: get git info failed."
    exit 1
fi

cat <<EOF >${GIT_VERSION_FILE}
Commit: ${COMMIT_ID}
Branch: ${BRANCH}
${COMMIT_DATE}
EOF

exit 0
