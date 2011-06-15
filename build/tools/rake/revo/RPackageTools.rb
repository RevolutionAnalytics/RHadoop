# Tools for building R Packages. 
# Derek Brown 2010/03/04

class RPackageTools
    def self.buildPackage (attributes)
        # standard package building function. 
        # Need to distinguish between package base 
        # and packageSrc dir because of revoIPC,Ripc names. . .
        # The rest of the packages have common packageBaseName and packageSRC names. 

        # Could Open the Description file and read packageBase. 

        rExePath        = attributes.fetch(:rExePath)
        bldArch         = attributes.fetch(:buildArchitecture)
        packageBaseName = attributes.fetch(:packageBaseName)
        packageSrcDir   = attributes.fetch(:packageSrcDir)    # Full Path to source.
        stagingDir      = attributes.fetch(:stagingDir)       # Full path to where result gets dropped
        pkgInstallDest  = attributes.fetch(:pkgInstallDest)
        packageSuffix   = attributes.fetch(:packageSuffix)    # tar.gz for unix, .zip for windows. 
        macAPPDIR       = attributes.fetch(:macAPPDIR,'')
        buildVignette   = attributes.fetch(:buildVignette,TRUE)

        puts("#{RevoUtils.getTimeStr()}: Building package #{packageBaseName}\n")

        # Write the value of the attributes
        attributes.each {|key,value| printf("\t%+20s : %s\n", key, value) }

        # Check attributes
        errmsg = ""
        errmsg = errmsg + "ERROR: stagingDir #{stagingDir} does not exist\n" unless File.exists? stagingDir
        abort errmsg unless errmsg.empty?

        # if a file of the proper form exists in the stagingDir, don't rebuild it. 
        theFiles = Dir.glob "#{stagingDir}/#{packageBaseName}_*.#{packageSuffix}"
        if 0 != theFiles.length
             puts("#{RevoUtils.getTimeStr()}: #{theFiles[0]} exists, Not building it\n")
        else
            # localVIGNETTE_SPEC is used by darwin build. 
            localVIGNETTE_SPEC = ""
            if (false == buildVignette)
                localVIGNETTE_SPEC = "--no-vignette"
                puts ("\tNot Building Vignettes")
            end

            Dir.chdir "#{packageSrcDir}/.."
            pkgDir = Dir.pwd            
            # Copy the source, and exclude .svn (e.g. svn export)

            packageSrcDirClean =  "#{pkgDir}/#{packageBaseName}_cleanSrc"
            FileUtils.rm_r packageSrcDirClean  if File.exists? packageSrcDirClean 
            FileUtils.cp_r packageSrcDir, packageSrcDirClean
            # spent too much time on trying to figure out FileList.exclude to get a readable one liner like Dale wants. . .Just doing it this way.  DKB. 
            Find.find( packageSrcDirClean) do |path|
                if FileTest.directory?(path)
                    if File.basename(path) == '.svn'
                        puts "Removing: #{path}\n"
                        FileUtils.rm_rf path
                        Find.prune
                    end
                end
            end
            
            if bldArch == "windows" 
                
                Dir.chdir pkgDir
                if (true == buildVignette)
                    sh "#{rExePath} CMD build #{packageSrcDirClean}"
                end
                sh "#{rExePath} CMD INSTALL --preclean --build --clean #{packageSrcDirClean} -l #{pkgInstallDest}"
                junk = Dir.glob "#{Dir.getwd}/#{packageBaseName}_*.#{packageSuffix}"
                pkgFilePath = junk[0]

            elsif bldArch == "darwin"
                # Dir.glob packageSrcDirClean
                # FileList.exclude ( ".svn" )
                # Build the package. 
                Dir.chdir pkgDir
                
                sh "#{rExePath} CMD build --binary #{localVIGNETTE_SPEC} #{packageSrcDirClean}"
                junk = Dir.glob "#{Dir.getwd}/#{packageBaseName}_*.#{packageSuffix}"
                pkgFilePath = junk[0]
                # If this package contains a library we need to make sure that libraries it depends on 
                # are found in appropriate place: open the package up, do surgery, package it back up
                Dir.chdir "#{pkgDir}/.."
                if  File.exists?( "#{packageSrcDirClean}/src" )
                    puts "Updating libraries in Mac shared objects" 
                    FileUtils.mkdir_p "#{pkgDir}/work", :verbose=>true
                    Dir.chdir "#{pkgDir}/work"
                    sh "tar xfz #{pkgFilePath}"
                    Dir.chdir "#{pkgDir}/work/#{packageBaseName}/libs/x86_64"
                    puts "#{packageBaseName}.so libraries before surgery:"
                    sh "otool -L #{packageBaseName}.so"
                    sh "install_name_tool -change /usr/local/lib/libgfortran.2.dylib #{macAPPDIR}/R.framework/Libraries/libgfortran.2.dylib #{packageBaseName}.so"
                    sh "install_name_tool -change /usr/local/lib/libgcc_s.1.dylib #{macAPPDIR}/R.framework/Libraries/libgcc_s.1.dylib #{packageBaseName}.so"
                    sh "install_name_tool -change libR.dylib #{macAPPDIR}/R.framework/Libraries/libR.dylib #{packageBaseName}.so"
                    puts "#{packageBaseName}.so libraries after surgery:"
                    sh "otool -L #{packageBaseName}.so"
                    Dir.chdir "#{pkgDir}/work" 
                    FileUtils.rm "#{pkgFilePath}"
                    sh "tar cfz #{pkgFilePath} #{packageBaseName}"
                end
                
                # Install the package: so that if another package build relies on it, it is there
                puts("======================================================\n")
                puts("#{RevoUtils.getTimeStr()}: Installing package #{packageBaseName}\n")
                Dir.chdir pkgDir
                sh "#{rExePath} CMD INSTALL #{pkgFilePath} -l #{pkgInstallDest}"
            elsif
                abort "ERROR: Unrecognized build architecture: #{bldArch}\n"
            end

            # on successful build move package to staging area. 
            Dir.chdir "#{packageSrcDir}/.."
            FileUtils.cp "#{pkgFilePath}", "#{stagingDir}", :verbose=>true
        end
    end # def self.buildPackage ()

    def self.installPackage (attributes)
        # standard package installing function. 

        rPath        = attributes.fetch(:rPath)
        packageBaseName = attributes.fetch(:packageBaseName)
        packageBinary   = attributes.fetch(:packageBinary)    # Full Path to source.
        packageInstallDest  = attributes.fetch(:packageInstallDest)
        dryRun = attributes.fetch( :dryRun, FALSE )

        # Write the value of the attributes
        attributes.each {|key,value| printf("\t%+20s : %s\n", key, value) }

        # if previously installed, don't re-install it. 
        if File.exists?( "#{packageInstallDest}/#{packageBaseName}" )
             puts("#{RevoUtils.getTimeStr()} : #{packageInstallDest}/#{packageBaseName} exists, Not building it\n")
        else
            puts("#{RevoUtils.getTimeStr()} : Installing package binary #{packageBinary}\n")
            puts( "      to package library : #{packageInstallDest}" )

            rCmd = "install.packages(\\\"#{packageBinary}\\\", \\\"#{packageInstallDest}\\\", repos=NULL)"
            # PLATFORMPATH is a hack to work around problem with Rpath/bin/Rscript not passing parameters correctly in 2.12. 
            # For usage against 2.12 put "SET PLATFORMPATH=\x64" in environment. 
            # Once we are not using this script against 2.11 any longer, the below line can become
            # shCmd = "#{rPath}/bin/x64/Rscript -e \"#{rCmd}\" "
            shCmd = "#{rPath}/bin#{ENV['PLATFORMPATH']}/Rscript -e \"#{rCmd}\" "
            puts( "      using command: #{shCmd}" )
            sh( shCmd ) if ! dryRun 

        end
    end # def self.installPackage ()
end
