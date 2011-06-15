class RevoMail
    # http://github.com/benprew/pony

    # TODO - uncomment line below when all build systems have 'pony' gem installed
    #require 'Pony'

    def self.send(attributes)
        # 
        # Example Usage
        #    require "#{REPOROOT}/build/tools/rake/revo/RevoMail.rb" 
        # Using default sender and recipients
        #    RevoMail::send( :subject => "My Subject Line" , :text => "Email text\ngoes here.\nDone." )
        # Specify sender and recipients
        #    RevoMail::send( :subject => "My Subject Line" , :text => "Email text\ngoes here.\nDone.", :recipient => "dale@revolutionanalytics.com", :sender => "betty@revolutionanalytics.com" )
        #

        recipient   = attributes.fetch(:recipient,  'builders@revolutionanalytics.com' )
        sender		= attributes.fetch(:sender,     'betty@revolutionanalytics.com')
        subjectLine = attributes.fetch(:subject,    'No subject specified')
        text 	    = attributes.fetch(:text,       'No text specified')
        #attachment	= attributes.fetch(:attachment, nil)

        Pony.mail(  :to         => recipient, 
                    :from       => sender, 
                    :subject    => subjectLine, 
                    :body       => text, 
                    :via        => :smtp, 
                    :via_options => {   :address    => 'seasmtp.revolution-computing.com',
                                        :port       => '25',
                                        :domain     => "seasmtp.revolution-computing.com" }
            )
    end 
    
end 