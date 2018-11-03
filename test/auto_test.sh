#!/bin/bash

set -e
cd ..
make clean
make
cd ./test
make clean
make
./office_test
