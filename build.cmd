@echo off

pushd %~dp0

setlocal EnableDelayedExpansion 
where dnvm
if %ERRORLEVEL% neq 0 (
    @powershell -NoProfile -ExecutionPolicy unrestricted -Command "&{$Branch='dev';iex ((new-object net.webclient).DownloadString('https://raw.githubusercontent.com/aspnet/Home/dev/dnvminstall.ps1'))}"
    set PATH=!PATH!;!userprofile!\.dnx\bin
    set DNX_HOME=!USERPROFILE!\.dnx
    goto install
)

:install
call dnvm install 1.0.0-beta7
call dnvm use 1.0.0-beta7
rem set the runtime path because the above commands set \.dnx<space>\runtimes
set PATH=!USERPROFILE!\.dnx\runtimes\dnx-clr-win-x86.1.0.0-beta7\bin;!PATH!

call dnu restore
if %errorlevel% neq 0 exit /b %errorlevel%
call dnx -p .\tests\LightningQueues.Tests test
if %errorlevel% neq 0 exit /b %errorlevel%

echo Packing LightningQueues Nuget Version %LQ_NUGET_VERSION%
call dnu pack src\LightningQueues --configuration Release --out artifacts
call dnu pack src\LightningQueues.Storage.LMDB --configuration Release --out artifacts

popd