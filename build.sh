#!/bin/sh
sh autogen.sh
./configure
cd src
make obsfs
make install
