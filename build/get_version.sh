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
BRANCH_COUNT=`git branch | wc -l`
BRANCH=""
if [ ${BRANCH_COUNT} -gt 1 ];then
    BRANCH=`git branch | grep "*" | awk  '{print $2}'`
else
    BRANCH=`git branch`
fi

if [ ! -n "${COMMIT_ID}" ] || [ ! -n "${COMMIT_DATE}" ] || [ ! -n "${BRANCH_COUNT}" ];then
    log "ERROR: get git info failed."
    exit 1
fi

cat <<EOF >${GIT_VERSION_FILE}
Commit: ${COMMIT_ID}
Branch: ${BRANCH}
${COMMIT_DATE}
EOF

exit 0
