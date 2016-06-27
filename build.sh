#!/bin/bash
rm -rf artifacts
if ! type dotnet > /dev/null 2>&1; then
    curl -sSL https://raw.githubusercontent.com/dotnet/cli/rel/1.0.0-preview2/scripts/obtain/dotnet-install.sh | bash /dev/stdin --version 1.0.0-preview2-003121 --install-dir ~/dotnet
    sudo ln -s ~/dotnet/dotnet /usr/local/bin
fi

if [ "$(uname)" == "Darwin" ]; then
    dotnet test ./tests/LightningQueues.Tests -f netcoreapp1.0
elif [ "$(expr substr $(uname -s) 1 5)" == "Linux" ]; then
    git clone https://github.com/LMDB/lmdb.git
    type make >/dev/null 2>&1 || { echo >&2 "Can't find dependency 'make' for lmdb native lib compile.  Aborting."; exit 1; }
    type gcc >/dev/null 2>&1 || { echo >&2 "Can't find dependency 'gcc' for lmdb native lib compile.  Aborting."; exit 1; }
    cd lmdb/libraries/liblmdb/
    make
    cd ../../../
    LD_LIBRARY_PATH=./lmdb/libraries/liblmdb/:$LD_LIBRARY_PATH dotnet test ./tests/LightningQueues.Tests -f netcoreapp1.0
fi

rc=$?; if [[ $rc != 0 ]]; then exit $rc; fi
