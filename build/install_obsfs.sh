#!/bin/sh
current_path="$(cd $(dirname $0);pwd)"
obsfsconfig_path="/etc/obsfsconfig"

function echo_tips()
{
    echo ""
    echo "Please select your scene using obsfs:"
    echo " --default    : The default configuration can meet your basic performance requirements."
    echo " --gene       : Optimized configuration for gene sequencing scenarios."
    echo " --hpc        : High performance computing scene, such as atmosphere,ocean."
    echo ""
    echo -e "For example, if you use obsfs for hpc scene, you can run \033[31msh $0 --hpc\033[0m to install it."
    exit 1
}

function log()
{
    echo "[`date "+%Y-%m-%d %H:%M:%S"`] $*"
}

# install obsfs
function install_obsfs()
{
    chmod 700 obsfs
    cp obsfs /usr/local/bin/  &&  cp version.txt /etc/obsfsversion
    if [ $? -ne 0 ];then
        log "Install obsfs failed."
        exit 1
    else
        log "Install obsfs success."
    fi
}

# install log-rotate
function install_log_rotate()
{
    # install shell
    log_rotate_shell_path=/opt/dfv/obsfs
    if [ ! -d ${log_rotate_shell_path} ]; then
        mkdir -p ${log_rotate_shell_path}
    fi
    cp -r ${current_path}/obsfs_log_rotate.sh ${log_rotate_shell_path}/
    if [ $? -ne 0 ];then
        log "Install obsfs_log_rotate.sh failed."
        exit 1
    else
        log "Install obsfs_log_rotate.sh success."
    fi

    # check crontab job
    result=$(cat /var/spool/cron/root | grep obsfs_log_rotate.sh)
    if [ -n "${result}" ]; then
        log "Add crontab job for obsfs_log_rotate.sh success."
        return
    fi

    # add crontab job
    is_support_crontab=$(which crontab)
    if [ -n "${is_support_crontab}" ]; then
        cmd_log_rotate="[ -f /opt/dfv/obsfs/obsfs_log_rotate.sh ] && sh /opt/dfv/obsfs/obsfs_log_rotate.sh"
        echo "*/10 * * * *  ${cmd_log_rotate}" >> /var/spool/cron/root
        result=$(cat /var/spool/cron/root | grep obsfs_log_rotate.sh)
        if [ -n "${result}" ]; then
            log "Add crontab job for obsfs_log_rotate.sh success."
        else
            log "Add crontab job for obsfs_log_rotate.sh failed."
            exit 1
        fi
    fi
}

# check config file 
function check_obsfsconfig()
{
    if [ ! -f ${obsfsconfig_path} ]; then
        log "Not found ${obsfsconfig_path}, install failed."
        exit 1
    fi

    file_size=$(ls -l ${obsfsconfig_path} | awk '{print $5}')
    if [ ${file_size} -eq 0 ]; then
        log "Check ${obsfsconfig_path} size is 0, install failed."
        exit 1
    fi
}

function install_hpc_obsfsconfig()
{
    if [ ! -f ${obsfsconfig_path} ]; then
cat <<EOF > ${obsfsconfig_path}
dbglogmode=1
intersect_write_merge=1
max_cache_mem_size_mb=10240
read_page_clean_ms=100
read_page_num=50
read_stat_sequential_size=4
read_stat_vec_size=6
write_page_num=240
EOF
    fi
check_obsfsconfig
}

function install_default_obsfsconfig()
{
    if [ ! -f ${obsfsconfig_path} ]; then
cat <<EOF > ${obsfsconfig_path}
dbglogmode=1
EOF
    fi
check_obsfsconfig
    
}

function install_gene_obsfsconfig()
{
    if [ ! -f ${obsfsconfig_path} ]; then
cat <<EOF > ${obsfsconfig_path}
dbglogmode=1
max_cache_mem_size_mb=10240
read_ahead_stat_diff_long_ms=7200000
read_page_clean_ms=10000
read_stat_sequential_size=4
read_stat_vec_size=6
cache_attr_switch_open=1
cache_attr_valid_ms=5000
whole_cache_switch=1
EOF
    fi
check_obsfsconfig
}


# install config file
function install_obsfsconfig()
{
    mode="--default"
    if [ -n "$1" ];then
        mode=$1
    fi

    case ${mode} in
        --default)
            install_default_obsfsconfig
            log "Creat obsfsconfig file for default scene success."
            ;;        

        --gene)
            install_gene_obsfsconfig
            log "Creat obsfsconfig file for gene scene success."
            ;;  

        --hpc)
            install_hpc_obsfsconfig
            log "Creat obsfsconfig file for hpc scene success."
            ;;

        --help)
            echo_tips
            exit 0
            ;;

        --*)
            log "Unknown scene of : ${mode}"
            echo_tips
            exit 1
            ;;

        *)
            log "Unknown options of : ${mode}"
            echo_tips
            exit 1
            ;;
    esac
}

function main()
{
    install_obsfsconfig $1
    install_obsfs
    install_log_rotate
    log "Install obsfs finished, now you can use obsfs to mount OBS-PFS."
}

main $1
