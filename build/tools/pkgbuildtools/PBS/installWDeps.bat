
rem net use y: \\seafile\sw\Packages\PACKAGES-src-2.9
rem net use x: \\seafile\sw\Packages\PACKAGES-bin
set SRCREPOS="file:///y:/"
set BINREPOS="file:///x:/"
R --slave --no-save --no-restore --args rtiff %SRCREPOS% %BINREPOS% < .\installWDeps.R


