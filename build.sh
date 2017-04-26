#!/bin/bash
rm -rf artifacts
dotnet restore
if [ "$(uname)" == "Darwin" ]; then
    dotnet test ./src/LightningQueues.Tests -f netcoreapp1.0
elif [ "$(expr substr $(uname -s) 1 5)" == "Linux" ]; then
    git clone https://github.com/LMDB/lmdb.git
    type make >/dev/null 2>&1 || { echo >&2 "Can't find dependency 'make' for lmdb native lib compile.  Aborting."; exit 1; }
    type gcc >/dev/null 2>&1 || { echo >&2 "Can't find dependency 'gcc' for lmdb native lib compile.  Aborting."; exit 1; }
    cd lmdb/libraries/liblmdb/
    make
    cd ../../../
    LD_LIBRARY_PATH=./lmdb/libraries/liblmdb/:$LD_LIBRARY_PATH dotnet test ./src/LightningQueues.Tests -f netcoreapp1.0
fi

rc=$?; if [[ $rc != 0 ]]; then exit $rc; fi
