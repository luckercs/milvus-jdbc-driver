@echo off
setlocal enabledelayedexpansion

set "BINPATH=%~dp0"
set "BINPATH=!BINPATH:~0,-1!"
set "LIBPATH=!BINPATH!\..\lib"
for %%i in ("!LIBPATH!") do set "LIBPATH=%%~fi"

java -cp ".;!LIBPATH!\*" sqlline.SqlLine %*
endlocal
