#!/bin/sh
CURRENT_PATH=$(cd "$(dirname "$0")"; pwd)
sh ${CURRENT_PATH}/autogen.sh
${CURRENT_PATH}/configure
cd ${CURRENT_PATH}
make
make install
