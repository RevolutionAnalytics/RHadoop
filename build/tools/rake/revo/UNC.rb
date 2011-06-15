class UNC
    
    def self.map(attributes)
        # Example:
        #    UNC::map( :unc => "\\\\server\\share",
        #              :mapPoint => "X",

        unc 		= attributes.fetch(:unc)
        mapPoint    = attributes.fetch(:mapPoint)
        domain      = attributes.fetch(:domain)
        user        = attributes.fetch(:user)
        pwd         = attributes.fetch(:pwd)

        case configatron.host.os

            when "win"
                drive = "#{mapPoint}"
                if File.exists?(drive)
                    puts "--- Current drive mapping. ---"
                    sh %{ NET USE #{drive} }
                    puts "--- Delete existing drive mapping. ---"
                    sh %{ NET USE #{drive} /DELETE /Y } do |ok,res| 
                        abort "Unable to delete mapping for drive '#{drive}'." unless ok
                    end
                end
                verbose(false) do   # don't want to show password in logs
                    sh %{ NET USE #{drive} #{unc} /USER:#{domain}\\#{user} #{pwd} /PERSISTENT:NO } do |ok,res| 
                        abort "Unable to map drive '#{drive}' to '#{unc}'." unless ok
                    end
                end 
                puts "--- New drive mapping. ---"
                sh %{ NET USE #{drive} }

            when "mac"

            when "linux"

            else
                abort "Unknown OS."

        end # case configatron.host.os

    end # map

end
