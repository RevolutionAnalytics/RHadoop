Package Build System Notes
--------------------------
DKB 2009/11/25

This is a simple package build system: Given an R installation, and a folder with source packages, 
build binary packages and place them in a binary repository structure. This script should 
be tool chain agnostic assuming that the R installation can handle tool chain. 

For each package (.tar.gz) in the folders specified system attempts to install dependencies, then 
builds the .zip. 

How to set it up: 
-----------------
Get a dedicated copy of R, 
 copy latest from latest 64 bit build naming it the with the build 
 identifer so we know what we are building with
Install RTools in sub folder. 

How to run it
-------------
Edit in paths into rakefile.rb to where you have a set of R source packages
make sure that init_pbs looks good, then run 
net use s: \\seafile\sw
net use y: \\seafile\sw\Packages\PACKAGES-src-2.9
net use x: \\seafile\sw\Packages\PACKAGES-bin
cd s:\PACKAGES\PBS
init_pbs.bat
irake

irake clean : Removes intermediate work
irake clobber : removes all work, (Don't do this when set to drop to production binary place)

Logging Scheme
-------------
Logging scheme has three components: 
Standard out shows single line for each package build with <OK> if it built fine, <not> if not. 
Then in workingdir there is a small <pkgname>.log and big <packagename>.[success|failed].log

Log ANalysers
-------------
Two rough log analyser scripts exist to help determine which packages are causing the failures
to load. 
- logAnalyser.R
- logAnalyserExt.R
See their headers for usage. 

Source Control
--------------
This system is currently source controlled at
http://seasvn.revolution-computing.com/parallelR/trunk/RevoR/pkgbuildtools

Potential Enhancements: 
-----------------------
- Use yaml configuration file, for source, binary repository, list of files to process, etc. 
- extract source package before doing the install of dependencies, so that if things fail we don't 
  have to extract the source file to see the dependencies list.
- Update the PACKAGES file in the binary folder after running. (don't do svn though)  
- get init.bat installing RTools if its not there. 
- get init.bat installing R if it's not there. 
