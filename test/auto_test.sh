#!/bin/bash

set -e
cd ..
make clean
make
cd ./test
make clean
make
rm -rf /tmp/test_*
echo 3 > /proc/sys/vm/drop_caches
./office_test
