#!/bin/bash

# driver.sh - The combined script compiling and running unit test.
#   Usage: bash ./autolab_scripts/driver.sh

# Compile the code
# NOTE: You may need to change the -j8 depending on your machine.
# We use -j1 to make sure each grading job only uses one core.
echo "Compiling Project"
(rm -rf build; mkdir build; cd build; cmake .. -DBUILD_JIT=OFF -DCMAKE_BUILD_TYPE="release"; cmake --build . -j1)
status=$?
if [ ${status} -ne 0 ]; then
    echo "Failure: Unable to compile wing (return status = ${status})"
    exit
fi

# Run the test
python3 autolab_scripts/test.py

exit
