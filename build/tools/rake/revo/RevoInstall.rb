require 'tmpdir'
class RevoInstall
    # Class to support installing RevoR as specified in revo-infrastructure.yml
    #########
    def self.expandInstDir( attributes )
        # Given a string pointing to an install dir with a place holder for base_revo_dir
        # replace place holder with base_revo_dir from canonical source. 
        instDirStr = attributes.fetch( :instDirStr )
        instDirTemplate = RevoTemplate.new( instDirStr )
        instDirTemplate.set( ":::", { 'revo_base_ver' => configatron.global.versions.base_revo } )
        temp = "#{instDirTemplate}"
        return( temp )
    end

    #########
    def self.RevoR(attributes)
        # for each installer specified in configatron.installRevo.installer
        # install it. 
        # Example:
        #    Ensure that hudson_home is mapped at J: 
        #    RevoInstall::RevoR( :buildArtifacts => BUILD_ARTIFACTS_DIR )
        #  

        buildArtifactsDir = attributes.fetch(:buildArtifactsDir )

        os = RevoUtils::setHostOS()

        hudsonArtifactDir   = File.join( configatron.global.hudson.mapPoint.retrieve(os), 
                                         configatron.global.hudson.jobBase, 
                                         configatron.installRevo.installJobName.retrieve(os),
                                         configatron.global.hudson.lastSuccessful )
        installers      = configatron.installRevo.installers.retrieve(os)

        installers.each do |installInfo|
            instRegex   = installInfo["regEx"]
            instCmd     = installInfo["cmd"]
            instDir     = self::expandInstDir( :instDirStr => installInfo["instDir"] )

            if File.exists?( instDir ) then
                puts "\tAssuming install is completed since directory exists - instDir: \'#{instDir}\'"
                next
            end

            srcInstaller = RevoUtils::globOneFile( :glob => File.join(hudsonArtifactDir, instRegex))

            # Copy installer to local disk
            localInstaller = File.join(Dir.tmpdir, File.basename(srcInstaller))

            if uptodate?(localInstaller, srcInstaller) then
                puts( "\tLocal installer up to date:\n#{localInstaller}" )
            else
                FileUtils.copy( srcInstaller, localInstaller, :verbose => true )
            end
            
            # Generate command to invoke installer
            localInstaller.gsub!(/\//, '\\')    if configatron.host.os =~ /^win/i   # convert localInstaller to DOS (with backslash)
            instCmd.gsub!(/%INSTALLFILE%/, localInstaller)      # Substitute %INSTALLFILE% with local install filename
            
            # Execute installer
            RevoSh::run( :cmd => instCmd )
            
            # Generate list of installed files and output to file
            fileList = Dir.glob(File.join(instDir, '**/*'))
            fileListOut = File.join(buildArtifactsDir, File.basename(srcInstaller).ext("filelist.txt"))
            File.open( fileListOut, 'w' ) do |output|
                fileList.each { |fn| output.print fn + "\n" }
            end
            
            # Copy desired files to local artifacts directory
            Dir.glob(File.join(Dir.tmpdir, 'Revo*{txt,log}')) do |fn| 
                FileUtils.copy( fn, buildArtifactsDir, :verbose => true )
            end
        end
    end

    #########
    # Installing RevoR2 Method
    # Differs from RevoR method class in that it does not know any thing about yml file, and
    # only does one install. 
    # is multiplatform
    #                RevoInstall::RevoR2( :downloadDir  => "#{WORKSPACE}/downloadDir", 
    #                                     :installRegex => installInfo["regEx"],
    #                                     :installCmd   => installInfo["cmd"] )
    #########
    def self.RevoR2(attributes)

        # buildArtifactsDir = attributes.fetch(:buildArtifactsDir )
        downloadDir = attributes.fetch(:downloadDir)
        instRegex   = attributes.fetch(:installRegex)
        instCmd     = attributes.fetch(:installCmd)

        localInstaller = RevoUtils::globOneFile( :glob => File.join( downloadDir, instRegex))

        # Generate command to invoke installer
        localInstaller.gsub!(/\//, '\\')    if configatron.host.os =~ /^win/i   # convert localInstaller to DOS (with backslash)
        instCmd.gsub!(/%INSTALLFILE%/, localInstaller)      # Substitute %INSTALLFILE% with local install filename
        instCmd.gsub!(/%REVOVER%/, "#{configatron.global.versions.base_revo}" )
                                  
        # instCmd.gsub!(/%REVOVER%/, "4.2")
        
        # Execute installer
        RevoSh::run( :cmd => instCmd )
    end


    ######### 
    # Installing RevoR3 Method
    # Differs from RevoR method class in that it 
    # is multiplatform, and the installers are already downloaded in downloadDir. 
    #    RevoInstall::RevoR2( :artifactDir => "#{WORKSPACE}/HudsonArtifacts", 
    #                         :workspace    => "#{WORKSPACE}",
    #                         :downloadDir  => "#{WORKSPACE}/downloadDir" )
    #       Assumes that revo-infrustructure.yml has been read into configatron object. 
    #########

    def self.RevoR3(attributes)
        # for each installer specified in configatron.installRevo.installer
        # install it. 
        # and installers are found at downloaddir. 
        # Example:

        artifactDir = attributes.fetch( :artifactDir )
        workspace    = attributes.fetch( :workspace )
        installerDir = attributes.fetch( :installerDir )

        puts( "\n\nartifactDir: #{artifactDir}\n\n")
        os = RevoUtils::setHostOS()
 
        installers      = configatron.installRevo.installers.retrieve(os)

        installers.each do |installInfo|
            instRegex   = installInfo["regEx"]
            instCmd     = installInfo["cmd"]
            instDir     = self::expandInstDir( :instDirStr => installInfo["instDir"] )

            localInstaller = RevoUtils::globOneFile( :glob => File.join( installerDir, instRegex))

            temp = RevoTemplate.new( "#{instDir}/#{installInfo['rscriptPath']}" )
            temp.set( ":::", { 'full-r' => configatron.global.versions.full_r } )
            revoScript = "#{temp}"
            case os
            when "win"
                localInstaller.gsub!(/\//, '\\')  # convert localInstaller to DOS (with backslash)
            end

            puts( "\n\n\nrevoScript=#{revoScript}\n\n\n")
            if File.exists?( revoScript ) then
                puts "\n\n\tAssuming install is un-needed since #{revoScript} exists\n\n"
                # collect install artifacts from the installInfoDir. 
                if ! File.exists?( configatron.installRevo.installInfoDir.retrieve(os) ) then
                    puts( "\n\n Directory #{configatron.installRevo.installInfoDir.retrieve(os)} does not exist, so not collecting it for build artifacts"  )
                end
            else
            
                # Generate command to invoke installer
                instCmd.gsub!(/%INSTALLFILE%/, localInstaller)      # Substitute %INSTALLFILE% with local install filename
                instCmd.gsub!(/%REVOVER%/, "#{configatron.global.versions.base_revo}" )
                
                # Execute installer
                RevoSh::run( :cmd => instCmd )
                
                puts( "done installing: os: #{os}." )
                case os 
                when "win"
                    instInfoDir = configatron.installRevo.installInfoDir.retrieve(os)
                    FileUtils.mkdir_p instInfoDir if ! File.exists? instInfoDir
                    # Generate list of installed files and output to file
                    fileList = Dir.glob(File.join(instDir, '**/*'))
                    fileListOut = File.join(instInfoDir, File.basename(localInstaller).ext("filelist.txt"))
                    File.open( fileListOut, 'w' ) do |output|
                        fileList.each { |fn| output.print fn + "\n" }
                    end
                    
                    # Copy installer log files to top level of install dir for users of this install
                    # to pick up later. 
                    Dir.glob(File.join(Dir.tmpdir, 'Revo*{txt,log}')) do |fn| 
                        FileUtils.copy( fn, instInfoDir, :verbose => true )
                    end
                    Dir.glob( File.join( installerDir, 'buildNotes.*')) do |fn| 
                        FileUtils.copy( fn, instInfoDir, :verbose => true )
                    end
                    
                end
            end


            # keep a copy of installArtifacts in theArtifactDir
            if File.exists? configatron.installRevo.installInfoDir.retrieve(os)
                theArtifactDir = "#{artifactDir}/installArtifacts"
                FileUtils.cp_r( configatron.installRevo.installInfoDir.retrieve(os), theArtifactDir, :verbose => TRUE )
            end

            # Keep a copy of the BuildID in theArtifactDir
            buildIDfile = "#{instDir}/BuildID"
            if File.exists? buildIDfile
                FileUtils.cp_r( buildIDfile, artifactDir, :verbose => TRUE )
            else
                puts( "BuildID File '#{buildIDfile}' does not exist." )
            end
        end
    end
end
