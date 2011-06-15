require 'pathname'

class RevoDevenv
    # Simple Minded Wrapper around Visual Studio devenv command. 
    
    def self.compile (attributes)
        functionID = "RevoDevenv::compile"
        # 
        # Example Usage
		# RevoDevenv::compile( :slnFile          => File.join( REPOROOT, "build/installer/windows/resources/customaction/SerialValidationDLL/Serial Validation DLL.sln" ),
		#			     :project          => "Serial Validation DLL",
		#			     :clean            => TRUE,
		#				 :slnConfiguration => "Release",
		#                :slnPlatform      => "Win32",
		#				 :compilerPlatform => "Win32",
		#				 :verbose => TRUE)
        #

        slnFile          = attributes.fetch(:slnFile)
        project          = attributes.fetch(:project,nil)
        clean            = attributes.fetch(:clean,TRUE)
        slnConfig        = attributes.fetch(:slnConfiguration)	
		slnPlatform      = attributes.fetch(:slnPlatform)
		compilerPlatform = attributes.fetch(:compilerPlatform)
		verbose          = attributes.fetch(:verbose,FALSE)
		
        verbose && puts( "#{functionID} :          slnFile = #{slnFile}" )
        verbose && puts( "#{functionID} :          project = #{project}" )
        verbose && puts( "#{functionID} :            clean = #{clean}" )
        verbose && puts( "#{functionID} :        slnConfig = #{slnConfig}" )
        verbose && puts( "#{functionID} :      slnPlatform = #{slnPlatform}" )
        verbose && puts( "#{functionID} : compilerPlatform = #{compilerPlatform}" )
        
		CheckSystem::environment( :envVariDir => [ "VS90ComnTools" ] )
		visualStudioRoot=Pathname.new(ENV["VS90ComnTools"]).parent.parent 
		verbose && puts( "#{functionID} : visualStudioRoot = #{visualStudioRoot}" )
		
		case compilerPlatform
			when "x64"   : vcvars = "\"#{visualStudioRoot}/VC/vcvarsall.bat\" x86_amd64"
#			when "x64"   : vcvars = "\"#{visualStudioRoot}/VC/vcvarsall\""
			when "Win32" : vcvars = "\"#{visualStudioRoot}/VC/Bin/Vcvars32.bat\""
			when "Any CPU" : vcvars = "\"#{visualStudioRoot}/VC/Bin/Vcvars32.bat\""
			else abort( "#{functionID} : ERROR : Unrecognized compilerPlatform : #{compilerPlatform}")
		end
        backdir = Dir.getwd
		slnDir = File.dirname slnFile
        Dir.chdir slnDir
		
		slnFileBasename = File.basename( slnFile )
		verbose && puts( "#{functionID} : slnFileBasename = #{slnFileBasename}]") 
        
        projectSpec = nil
        if ( project ) then
            projectSpec = "/project #{project}"
        end

        if clean 
			syscmd =  "#{vcvars} && devenv \"#{slnFileBasename}\" /clean \"#{slnConfig}|#{slnPlatform}\" #{projectSpec}"
			verbose && puts( "#{functionID} : clean command [#{syscmd}]") 
			if ! system( syscmd )
				abort("RevoDevenv::clean : ERROR : #{$?}")
			end
		end	
		
		syscmd =  "#{vcvars} && devenv \"#{slnFileBasename}\" /build \"#{slnConfig}|#{slnPlatform}\" #{projectSpec}"
		verbose && puts( "#{functionID} : #{RevoUtils::getTimeStr} compile starting ") 
		verbose && puts( "#{functionID} : compile command [#{syscmd}]") 
		if ! system( syscmd )
			abort("#{functionID} ERROR: #{$?}")
		end	
		
        Dir.chdir backdir
        verbose && puts( "#{functionID} : #{RevoUtils::getTimeStr()} compile complete") 
    end
end
