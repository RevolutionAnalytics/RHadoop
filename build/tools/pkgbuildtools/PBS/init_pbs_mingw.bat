@ECHO OFF
pushd tools
if not exist ironruby\nul 7zip\7z.exe x -y ironruby.7z
popd

set RTOOLS=%CD%\Rtools
set RHOME=%CD%\R-2009-12-22_02-46_MINGW

set PATH=^
%RTOOLS%\bin;^
%RTOOLS%\perl\bin;^
%RTOOLS%\MinGW\bin;^
C:\Python25;^
C:\Program Files (x86)\HTML Help Workshop;^
C:\Programs\MiKTeX2.7\miktex\bin;^
%RHOME%\bin;^
C:\WINDOWS\system32;^
C:\WINDOWS;^
C:\WINDOWS\System32\Wbem;^
C:\Windows\Microsoft.NET\Framework\v3.5;^
%CD%\tools\ironruby\bin;^
C:\Program Files (x86)\Windows Resource Kits\Tools\;^
C:\Program Files (x86)\CollabNet\Subversion Client

set GEM_PATH=%CD%\tools\ironruby\lib\ruby\gems\1.8
set GEM_HOME=%CD%\tools\ironruby\lib\ruby\gems\1.8