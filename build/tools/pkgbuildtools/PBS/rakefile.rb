# rakefile to build R packages. 
# usage: 
#  set envorinment: init_pbs.bat
#  Set inline environment, see configuration below. 
#   "irake " will attempt to load dependencies and build windows binary packages and drop them to binrepos_base. 
#   "irake clean" will remove the working dir under this directory. 
#   "irake clobber" will remove workingdir and contents of binrepos_base. 
# DKB 2009/11/23
#
# RAKE resources
# http://martinfowler.com/articles/rake.html
# http://railsenvy.com/2007/6/11/ruby-on-rails-rake-tutorial
# http://dylandoesdigits.blogspot.com/2009/11/rake-for-net-projects.html

# http://andypike.wordpress.com/category/teamcity/
# http://aardvark-cms.googlecode.com/svn/trunk/build/rakefile.rb

# RUBY resources
# http://ruby-doc.org/

require 'rake/clean'
require 'date'

# Configuration
REVO_VER="2.9"
REVO_ARCH="win64"
SRCREPOS_BASE="Y:/"
BINREPOS_BASE="X:/"
#BINREPOS_BASE="c:/DerekWorkspace/PackageBuildSystem/PACKAGES-BIN/"

#
REPOS_PREFIX="file:///"
SRCREPOS="#{REPOS_PREFIX}#{SRCREPOS_BASE}"
BINREPOS="#{REPOS_PREFIX}#{BINREPOS_BASE}"
SRCREPOSDIR="#{SRCREPOS_BASE}src/contrib"
BINREPOSDIR="#{BINREPOS_BASE}bin/#{REVO_ARCH}/contrib/#{REVO_VER}"

PROJECTROOT_PATH = Dir.getwd
WORKINGDIR_PATH = PROJECTROOT_PATH + "/workingdir"

####

print "SRCREPOS= #{SRCREPOS}\n"
print "BINREPOS= #{BINREPOS}\n"
print "WORKINGDIR_PATH= #{WORKINGDIR_PATH}\n"

####
$srcFileList = []

#verbose(true)
verbose(false)	# I am precisely formatting STDOUT for further processing, so don't want 
				# to see commands echoed to STDOUT

#initialize Rake clean and clobber targets
CLEAN.include(WORKINGDIR_PATH)
CLOBBER.include(BINREPOSDIR)

# Default task: For each .gz file in the SOURCE PAth build it. 
task :default  do
  mkdir_p BINREPOSDIR
  mkdir_p WORKINGDIR_PATH
  
  $srcFileList = FileList[SRCREPOSDIR + '/*tar.gz']
  #  $srcFileList = FileList[SRCREPOSDIR + '/iful*tar.gz']
  if 0 == $srcFileList.length
	abort "ERROR: no tar.gz files in #{SRCREPOSDIR}" 
  end

  $srcFileList.each do |f|

	packageNameFull = File.basename(f) # E.g. TWIX_0.10.12.tar.gz
	packageNameVer = packageNameFull.gsub(/.tar.gz/,'') # e.g. TWIX_0.10.12
	packageName = packageNameFull.gsub(/_.*/,'')  #e.g. TWIX
	
    # set up a log file to log work in this package. 
    pkgLog = open("#{WORKINGDIR_PATH + '/' + packageNameVer + '.log'}","a")
	startTime = Time.now
	timenow = startTime.strftime("%Y-%m-%d_%H:%M:%S")
	pkgLog << "\n==========  #{timenow} \n"
 	pkgLog << "Processing Package: #{packageNameVer}\n"
	print "#{timenow} Processing Package: #{packageNameVer} ... "

    # now do the real stuff. 
	buildSuccess = buildPkg(pkgLog, packageNameFull, packageNameVer, packageName)

	pkgLog.close

	# Write success/failure. 
	endTime = Time.now
	elapsedSecs = ( endTime - startTime )
	elapsedSecsStr = "%.2f" % [ elapsedSecs ]
	if buildSuccess
		print "(#{elapsedSecsStr} secs) <OK>\n"
	else
		print "(#{elapsedSecsStr} secs) <not>\n"
	end
  end
end
	
def buildPkg(pkgLog, packageNameFull,packageNameVer,packageName)

	if ! File.exists?("#{SRCREPOSDIR}/#{packageNameFull}" )
		pkgLog << " ERROR: File does not exist:  #{SRCREPOSDIR}/#{packageNameFull}\n"
		return false
	end
	pkgSuccessLog="#{WORKINGDIR_PATH}/#{packageNameVer}.success.log"
	pkgFailedLog="#{WORKINGDIR_PATH}/#{packageNameVer}.failed.log"
	pkgOut = packageNameFull.sub(".tar.gz",".zip")
	pkgLog << "pkgOut: " + pkgOut  + "\n"
	fullTargetPath = File.join BINREPOSDIR, pkgOut
	pkgLog << "fullTargetPath: " + fullTargetPath + "\n"
	
	fullPackagePath= File.join SRCREPOSDIR, packageNameFull
	pkgLog << "fullPackagePath: " + fullPackagePath  + "\n"
	processSuccess = true

	if uptodate?(fullTargetPath, fullPackagePath)
		pkgLog << "Up To Date: " + fullTargetPath + "\n"		
		return true
	else 
		if File.exists?("#{pkgFailedLog}")
			pkgLog << "Build Previously failed, skipping" 
			return false
		else
			# Install Dependencies
			installSuccess = installPkgDepends(pkgLog,pkgSuccessLog,packageNameFull,packageNameVer,packageName)
		
			if ! installSuccess
				pkgLog << "Failed Dependency Install"
				FileUtils.mv("#{pkgSuccessLog}","#{pkgFailedLog}")
				return false
			end
			# Clear the 00LOCK
			zzLOCK = ENV['RHOME'] + "/library/00LOCK"
			if File.exists?(zzLOCK)
				FileUtils.rm_rf(zzLOCK)
				pkgLog << "Removed #{zzLOCK}"
			end
			# If we are regenerating,s make sure the zip file does not exist before re-creating. 
			if File.exists?(pkgOut)
				FileUtils.rm pkgOut
			end
			
			pkgLog << "packageName: " + packageName + "\n"
		
			Dir.chdir WORKINGDIR_PATH
			pkgLog << "Extracting " + fullPackagePath + " ..."  + "\n"
			sh %{echo "****----****----****----" >> "#{pkgSuccessLog}"}
			sh %{echo "Extracting #{packageNameFull}" >> "#{pkgSuccessLog}"}
			
			# Extract the package tar.gz
			sh %{tar zxf "#{fullPackagePath}" >> "#{pkgSuccessLog}" 2>&1} do |ok,res|
				if ! ok
					pkgLog << "Bad tar return : #{res.exitstatus}"  + "\n"
					# ignore tar return value, don't return false 
				end
			end
			
			# Run R CMD BUILD --binary on the package (to ensure vignettes are built) 
			pkgLog << "R CMD Build --binary --no-vignette " + packageName + " ..." + "\n"
			sh %{echo "****----****----****----" >> "#{pkgSuccessLog}"}
			sh %{echo "R CMD Build --binary --no-vignette #{packageName}" >> "#{pkgSuccessLog}"}
			
			sh %{R CMD build --binary --no-vignette "#{packageName}" >> "#{pkgSuccessLog}" 2>&1 } do |ok,res|
				if ! ok
					pkgLog << "Bad R CMD Return : #{res.exitstatus}" + "\n"
					FileUtils.mv("#{pkgSuccessLog}","#{pkgFailedLog}")
					return false
				end
			end
		
			# Run R CMD INSTALL --build on the package, to buil dthe actual zip to be kept.
			pkgLog << "R CMD INSTALL --build " + packageName + " ..." + "\n"
			sh %{echo "****----****----****----" >> "#{pkgSuccessLog}"}
			sh %{echo "R CMD INSTALL --build #{packageNameFull}" >> "#{pkgSuccessLog}"}
			
			sh %{R CMD INSTALL --build "#{packageName}" >> "#{pkgSuccessLog}" 2>&1 } do |ok,res|
				if ! ok
					pkgLog << "Bad R CMD Return : #{res.exitstatus}" + "\n"
					FileUtils.mv("#{pkgSuccessLog}","#{pkgFailedLog}")
					return false
				end
			end
		
			if File.exists?(pkgOut)
				pkgLog << "Copying #{pkgOut} to targetdir\n"
				FileUtils.cp pkgOut, fullTargetPath
			else
				pkgLog "ERROR: No package.zip to copy\n"
				FileUtils.mv("#{pkgSuccessLog}","#{pkgFailedLog}")
				return false
			end
		end
	end
	return true
	
end

def installPkgDepends(pkgLog,pkgSuccessLog,packageNameFull, packageNameVer, packageName)
	if ! File.exists?("#{SRCREPOSDIR}/#{packageNameFull}" )
		pkgLog << " ERROR: File does not exist:  ${SRCREPOSDIR}/#{packageNameFull}\n"
		return false
	end
	pkgLog <<  "\nInstall Dependencys #{packageNameFull}\n"
	sh %{echo "****----****----****----" >> "#{pkgSuccessLog}"}
	sh %{echo "Install Dependencys #{packageNameFull}" >> "#{pkgSuccessLog}"}

	sh %{R --slave --no-save --no-restore --args "#{packageName}" "#{SRCREPOS}" "#{BINREPOS}" < "#{PROJECTROOT_PATH}/installWDeps.R" >> "#{pkgSuccessLog}" "2>&1" } do |ok,res|
		if ok
			pkgLog << "Depenencies installed successfully\n"
		else
			pkgLog << "Failed installing Dependencies: #{res.exitstatus}" + "\n"
			return false
		end
	end
	return true
end

