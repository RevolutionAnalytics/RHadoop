class RevoSh
    
    def self.run(attributes)
        # Example:
        #    RevoSh::run( :cmd => "The command which may include ThePassWord", :password => "ThePassWord", abortOnFailure => false)

        cmd             = attributes.fetch(:cmd)
        password        = attributes.fetch(:password, nil)
        abortOnFailure  = attributes.fetch(:abortOnFailure, true)

        if password then
            publicCmd = cmd.gsub(/#{password}/, 'PasswordNotLogged')
        else
            publicCmd = cmd
        end
        
        verbose(! password) do   
            sh cmd do |ok,res| 
                msg = "running: '#{publicCmd}'.\n\tResult: #{res}\n\tExit status: #{res.exitstatus}." 
                if ( ! ok ) 
                    msg = "\tERROR #{msg}" 
                    if (abortOnFailure)
                        abort msg
                    else
                        puts msg
                        return false
                    end
                else
                    puts "\tSuccess #{msg}"
                    return true
                end
            end
        end 

        
    end # run
end
