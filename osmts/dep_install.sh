#!/bin/bash
# 环境配置自动化脚本 (2025-07-02)

set -u
set -o pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_FILE="${SCRIPT_DIR}/pre_install.log"

echo "=== 开始环境配置 $(date) ===" | tee -a "$LOG_FILE"

run_and_log() {
    local description="$1"
    shift

    echo "$description" | tee -a "$LOG_FILE"
    "$@" 2>&1 | tee -a "$LOG_FILE"
    local command_status=${PIPESTATUS[0]}
    if [ "$command_status" -ne 0 ]; then
        echo "${description}失败，请检查日志：$LOG_FILE" | tee -a "$LOG_FILE"
        exit "$command_status"
    fi
}

# 检查root权限
if [ "$(id -u)" -ne 0 ]; then
    echo "请使用root权限运行此脚本" | tee -a "$LOG_FILE"
    exit 1
fi

cd "$SCRIPT_DIR" || exit 1

# 安装基础依赖
run_and_log "正在安装系统依赖..." \
    dnf install -y \
    gcc g++ clang make git cmake \
    python python3-devel python3-pip python3-Cython python3-xlrd python3-openpyxl \
    python3-psycopg2 python3-paramiko python3-numpy python3-pandas \
    systemd-devel libxml2 libxslt libxslt-devel libxml2-devel \
    tmux automake autoconf ntp

# 手动同步时间，失败仅告警，不阻断后续安装
echo "手动同步时间..." | tee -a "$LOG_FILE"
if ! ntpdate cn.pool.ntp.org 2>&1 | tee -a "$LOG_FILE"; then
    echo "时间同步失败，后续如果遇到SSL或证书问题，请优先检查系统时间" | tee -a "$LOG_FILE"
fi

# 升级pip和setuptools
run_and_log "升级Python工具链..." \
    python3 -m pip install --upgrade pip setuptools -i https://mirrors.aliyun.com/pypi/simple

# 安装requirements.txt依赖
if [ -f "requirements.txt" ]; then
    run_and_log "安装Python依赖包..." \
        python3 -m pip install -r requirements.txt -i https://mirrors.aliyun.com/pypi/simple
else
    echo "未找到requirements.txt文件" | tee -a "$LOG_FILE"
    exit 1
fi

echo "=== 环境配置完成 $(date) ===" | tee -a "$LOG_FILE"
