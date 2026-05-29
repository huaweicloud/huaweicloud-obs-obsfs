#!/bin/bash
current_path="$(cd $(dirname $0);pwd)"
obsfsconfig_path="/etc/obsfsconfig"

function log()
{
    echo "[`date "+%Y-%m-%d %H:%M:%S"`] $*"
}

# uninstall obsfs
function uninstall_obsfs()
{
    rm -f /usr/local/bin/obsfs && rm -f /etc/obsfsversion && rm -f ${obsfsconfig_path}
    if [ $? -ne 0 ];then
        log "Uninstall obsfs failed."
        exit 1
    fi
}

# uninstall log-rotate
function uninstall_log_rotate()
{
    # uninstall shell
    log_rotate_shell_path=/opt/dfv/obsfs
    rm -rf ${log_rotate_shell_path}
    if [ $? -ne 0 ]; then
        log "uninstall shell failed."
        exit 1
    fi

    # delete crontab job
    is_support_crontab=$(which crontab)
    if [ -n "${is_support_crontab}" ]; then
        (crontab -l 2>/dev/null | grep -v "obsfs_log_rotate.sh") | crontab -
        result=$(crontab -l | grep obsfs_log_rotate.sh)
        if [ ! -n "${result}" ]; then
            log "Delete crontab job for obsfs_log_rotate.sh success."
        else
            log "Delete crontab job for obsfs_log_rotate.sh failed."
            exit 1
        fi
    fi
}

function main() {
    uninstall_obsfs
    uninstall_log_rotate
    log "Uninstall obsfs success."
}

main