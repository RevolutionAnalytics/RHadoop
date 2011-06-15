class RevoSystem
    # System commands
    
    def self.shutdown (attributes)
        # 
        # Example Usage:
		# RevoSystem::shutdown( :restart    => true)
        #
        functionID = "RevoSystem::shutdown"

        restart     = attributes.fetch(:restart,   false)
        
        buildParentDir    = Pathname.new(File.dirname(__FILE__)).parent.parent.parent.parent
		
        case RevoUtils::setHostOS
            when "win"
                shutdownExe = configatron.tools.shutdown.win
                shutdownCmd = "#{buildParentDir}/#{shutdownExe} -f"
                shutdownCmd = restart ? "#{shutdownCmd} -r" : "#{shutdownCmd} -k"
            when "linux"
                shutdownCmd = restart ? "shutdown -r now" : "shutdown -P now"
            when "mac"
                abort "TODO: Shutdown command for Mac."
            else
                abort "Unexpected platform: #{configatron.host.os}."
        end
        
        RevoSh::run( :cmd => shutdownCmd )
        
    end
    
end
