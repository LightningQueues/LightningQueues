@echo off

pushd %~dp0

call dotnet restore
if %errorlevel% neq 0 exit /b %errorlevel%
call dotnet test .\src\LightningQueues.Tests
if %errorlevel% neq 0 exit /b %errorlevel%

echo Packing LightningQueues Nuget Version %LQ_NUGET_VERSION%
call dotnet pack src\LightningQueues --configuration Release --output artifacts
call dnu pack src\LightningQueues.Storage.LMDB --configuration Release --output artifacts

popd