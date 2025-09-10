@echo off
set input=%1
set output=%2
if "%input%" == "" goto :bailout
if "%output%" == "" goto :bailout
goto :expand

:bailout
echo %0 input_file output_file
goto :eof

:expand

powershell -command "get-content '%input%' | foreach { [System.Environment]::ExpandEnvironmentVariables($_) } | set-content -path '%output%'" 
