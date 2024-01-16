@ECHO OFF
setlocal enabledelayedexpansion

:: User Variables
:: WoW version
set version=10.0.7.48999
:: dbfilesclient folder containing .dbc or .db2 files
set dbfilesclient=F:\WoWStuff\wow.tools.local\dbcs\%version%\dbfilesclient
:: path of DBC2CSV.exe
set DBX2CSV="F:\WoWStuff\DBC2CSV\DBC2CSV.exe"

:: Create the version folder
mkdir %~dp0\dbfilesclient\%version%

:: convert dbc to csv
call %DBX2CSV% "%dbfilesclient%"

:: move .csv from %dbfilesclient% to %~dp0\dbfilesclient\%version%
for /r "%dbfilesclient%" %%x in (*.csv) do (
  move "%%x" "%~dp0\dbfilesclient\%version%"
)
pause
