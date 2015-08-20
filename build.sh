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
dnx ./tests/LightningQueues.Tests test
rc=$?; if [[ $rc != 0 ]]; then exit $rc; fi
