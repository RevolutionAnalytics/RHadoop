require 'pathname'

class RevoMSTEST
    # Simple Minded Wrapper around Visual Studio devenv command. 
    
    def self.run (attributes)
        functionID = "RevoMSTEST::run"
        # 
        # Example Usage
		# RevoMSTEST::run( :testmetadata   => "REvolutionStudio.vsmdi",
		#			     :runconfig        => "UnitTests.testrunconfig",
		#				 :testlist         => "HistoryManager_Test",
		#				 :bits             => "32",
		#				 :verbose          => TRUE)
        #

        testmetadata     = attributes.fetch(:testmetadata)
        runconfig        = attributes.fetch(:runconfig)
        testlist         = attributes.fetch(:testlist,TRUE)
        bits             = attributes.fetch(:bits, "32")
		verbose          = attributes.fetch(:verbose,FALSE)
        abortOnFailure   = attributes.fetch(:abortOnFailure,FALSE)
		
        verbose && puts( "#{functionID} :     testmetadata = #{testmetadata}" )
        verbose && puts( "#{functionID} :        runconfig = #{runconfig}" )
        verbose && puts( "#{functionID} :         testlist = #{testlist}" )
        verbose && puts( "#{functionID} :             bits = #{bits}" )
        
		CheckSystem::environment( :envVariDir => [ "VS90ComnTools" ] )
		visualStudioRoot=Pathname.new(ENV["VS90ComnTools"]).parent.parent 
		verbose && puts( "#{functionID} : visualStudioRoot = #{visualStudioRoot}" )

        if bits =~ /64/ then
            vcvars = "\"#{visualStudioRoot}/VC/vcvarsall.bat\" x86_amd64"
        else
			vcvars = "\"#{visualStudioRoot}/VC/Bin/Vcvars32.bat\""
        end
		
        testmetaFile = File.basename( testmetadata )
        verbose && puts( "#{functionID} : testmetaFile = #{testmetaFile}]") 
        
        Dir.chdir(File.dirname(testmetadata)) do
            cmd =  "#{vcvars} && MSTEST /testmetadata:#{testmetaFile} /runconfig:#{runconfig} /testlist:#{testlist}"
            verbose && puts( "#{functionID} : #{RevoUtils::getTimeStr} test starting ") 
            RevoSh::run( :cmd => cmd, :abortOnFailure => abortOnFailure )
        end

    end
end
