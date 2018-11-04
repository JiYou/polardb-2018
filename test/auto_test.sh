#!/bin/bash

set -e
cd ..
make clean
make
cd ./test
make clean
make
rm -rf /tmp/test_*
./office_test
