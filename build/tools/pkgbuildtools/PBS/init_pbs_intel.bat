@ECHO OFF
pushd tools
if not exist ironruby\nul 7zip\7z.exe x -y ironruby.7z
popd

set PATH=^
C:\WINDOWS\system32;^
C:\WINDOWS;^
C:\WINDOWS\System32\Wbem;^
C:\Windows\Microsoft.NET\Framework\v3.5;^
%CD%\tools\ironruby\bin;^
C:\Program Files (x86)\Windows Resource Kits\Tools\;^
C:\Programs\MiKTeX2.7\miktex\bin;^
C:\Program Files (x86)\CollabNet\Subversion Client



set RTools=%CD%/RTools
set RHOME=%CD%\R-2009-12-22_02-46_INTEL
call %RHOME%\win64\bin\setvars.bat x64 Release %RHOME%

set GEM_PATH=%CD%\tools\ironruby\lib\ruby\gems\1.8
set GEM_HOME=%CD%\tools\ironruby\lib\ruby\gems\1.8