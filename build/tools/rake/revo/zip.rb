class RevoZip
    # Simple Minded Wrapper around system calls to Info Zip. 
    # Depends on zip and unzip being in the PATH, and being Info Zip. 
    
    def self.unzip (attributes)
        # 
        # Example Usage
        #   RevoZip::unzip( :zipFile     => File.join ( downloadDir, "STAGEDIR.zip" ),  
        #               :destination => stagingDir,
        #               :verbose     => TRUE )
        #
        zipFile     = attributes.fetch(:zipFile)
        destination = attributes.fetch(:destination)
        verbose     = attributes.fetch(:verbose, FALSE)		

        verbose && puts( "unzip :     zipFile = #{zipFile}" )
        verbose && puts( "unzip : destination = #{destination}" )
        verbose && puts( "unzip :     verbose = #{verbose}" )

        zipArgs = "-o"  # Overwrite existing files
        ! verbose && zipArgs += ' -q'
            
        backdir = Dir.getwd
        Dir.chdir destination
        syscmd =  "unzip #{zipArgs} #{zipFile}" 
        if ! system( syscmd )
            abort("RevoZip::unzip ERROR: #{$?}")
        end
        Dir.chdir backdir
        verbose && puts( "unzip : complete" )
    end
    
    def self.zip (attributes)
        # 
        # Example Usage
        #   RevoZip::zip( :zipFile        => File.join ( downloadDir, "STAGEDIR.zip" ),  
        #                 :rootDir        => stagingDir,
        #                 :fileFolderList => ["BLD_INSTALL_STAGE", "Revo*.tar.gz"]
        #                 :verbose        => TRUE )
        #
        zipFile        = attributes.fetch(:zipFile)
        rootDir        = attributes.fetch(:rootDir)
        fileFolderList = attributes.fetch(:fileFolderList)
        verbose        = attributes.fetch(:verbose, FALSE)		

        verbose && puts( "zip :        zipFile = #{zipFile}" )
        verbose && puts( "zip :        rootDir = #{rootDir}" )
        verbose && puts( "zip : fileFolderList = #{fileFolderList}" )
        verbose && puts( "zip :        verbose = #{verbose}" )

        zipArgs = "-r"
        ! verbose && zipArgs += ' -q'

        backdir = Dir.getwd
        Dir.chdir rootDir
        toZip = " " 
        Dir.glob(fileFolderList).each do |component|
            toZip = "#{toZip} #{component}"
        end
		
        syscmd = "zip #{zipArgs} #{zipFile} #{toZip}" 
        verbose && puts("zip : zip command : #{syscmd}")
        if ! system( syscmd )
            abort("RevoZip::zip failed : #{$?}")
        end
        Dir.chdir backdir
        verbose && puts( "zip : complete" )
    end
end
