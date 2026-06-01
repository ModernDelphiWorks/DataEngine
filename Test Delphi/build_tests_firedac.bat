@echo off
call "C:\Program Files (x86)\Embarcadero\Studio\37.0\bin\rsvars.bat"

set DE=D:\Ecossistema-Delphi\Janus\Source\Dependencies\DataEngine
set BDS_LIB=C:\Program Files (x86)\Embarcadero\Studio\37.0\lib\win32\release

set PATHS=%BDS_LIB%
set PATHS=%PATHS%;%DE%\Source\Core
set PATHS=%PATHS%;%DE%\Source\Drivers

set NS=System;System.Win;Winapi;Data;Vcl;Xml

dcc32 TestsFireDAC.dpr -E. -N. -U"%PATHS%" -ns"%NS%"
