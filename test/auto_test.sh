#!/bin/bash

set -e
cd ..
make clean
make
cd ./test
rm -rf ./office_test
make
rm -rf /tmp/test_*
echo 3 > /proc/sys/vm/drop_caches
./office_test
