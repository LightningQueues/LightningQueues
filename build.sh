#!/bin/bash
rm -rf artifacts

if ! type dnvm > /dev/null 2>&1; then
    curl -sSL https://raw.githubusercontent.com/aspnet/Home/dev/dnvminstall.sh | DNX_BRANCH=dev sh && source ~/.dnx/dnvm/dnvm.sh
fi

dnvm install 1.0.0-beta6
dnvm alias default 1.0.0-beta6
dnvm use default
dnu restore
rc=$?; if [[ $rc != 0 ]]; then exit $rc; fi

if [ "$(uname)" == "Darwin" ]; then
    dnx ./tests/LightningQueues.Tests test        
elif [ "$(expr substr $(uname -s) 1 5)" == "Linux" ]; then
    git clone https://github.com/LMDB/lmdb.git
    type make >/dev/null 2>&1 || { echo >&2 "Can't find dependency 'make' for lmdb native lib compile.  Aborting."; exit 1; }
    type gcc >/dev/null 2>&1 || { echo >&2 "Can't find dependency 'gcc' for lmdb native lib compile.  Aborting."; exit 1; }
    cd lmdb/libraries/liblmdb/
    make
    cd ../../../
    LD_LIBRARY_PATH=./lmdb/libraries/liblmdb/:$LD_LIBRARY_PATH dnx ./tests/LightningQueues.Tests test
fi

rc=$?; if [[ $rc != 0 ]]; then exit $rc; fi
