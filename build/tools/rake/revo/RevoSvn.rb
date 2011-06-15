class RevoSvn

    def self.checkout(attributes)
        functionID = "RevoSvn::checkout"
        # Example:
        #    RevoSvn::checkout( :svnURL => "SVN URL to checkout from", :workspace => "The directory to which to checkout to", :verbose => FALSE )
        svnURL = attributes.fetch(:svnURL)
        workspace = attributes.fetch(:workspace)
        verbose = attributes.fetch( :verbose, FALSE )

        verbose && puts( "#{functionID} : #{RevoUtils::getTimeStr} starting") 

        cmd = "svn checkout --username #{configatron.global.builder.user} --password #{configatron.global.builder.pw} #{svnURL} #{workspace}"
        if ! system( cmd ) then
          abort "\t#{functionID} : Error running: '#{cmd}'.\n\tExit status:#{$?}."
        end

        verbose && puts( "#{functionID} : #{RevoUtils::getTimeStr} complete") 
    end

    def self.commit(attributes)
        functionID = "RevoSvn::commit"
        # Example:
        #    RevoSvn::commit( :workspace => "The directory to which to checkout to", 
        #                     :message => "Checkin message"
        #                     :verbose => FALSE)
        workspace = attributes.fetch(:workspace)
        message   = attributes.fetch( :message )
        verbose   = attributes.fetch( :verbose, FALSE )

        verbose && puts( "#{functionID} : #{RevoUtils::getTimeStr} starting") 

        cmd = "svn commit --username #{configatron.global.builder.user} --password #{configatron.global.builder.pw} --message \"#{message}\" #{workspace}"
        if ! system( cmd ) then
          abort "\t#{functionID} : Error running: svn commit command.\n\tExit status:#{$?}."
        end

        verbose && puts( "#{functionID} : #{RevoUtils::getTimeStr} complete") 
    end
    
    def self.revert(attributes)
        functionID = "RevoSvn::revert"
        # Example:
        #    RevoSvn::revert( :path => "The file or directory to revert", :recursive => FALSE )

        path = attributes.fetch(:path)
        recursive = attributes.fetch(:recursive,FALSE)
        verbose = attributes.fetch(:verbose,FALSE)
        verbose && puts( "#{functionID} : #{RevoUtils::getTimeStr} starting") 

        revertArgs = ""
        if ( recursive == TRUE ) then
            revertArgs = "#{revertArgs} -R"
        end
        cmd = "svn revert #{revertArgs} #{path}"
        if ! system( cmd ) then
            abort "\tError running: '#{cmd}'.\n\tExit status:#{$?}."
        end
        verbose && puts( "#{functionID} : #{RevoUtils::getTimeStr} complete") 
    end # revert

    def self.removeUnversionedFiles(attributes)
        functionID = "RevoSvn::removeUnversionedFiles"
        # Example:
        #    RevoSvn::removeUnversionedFiles( :path => "The directory from which to remove unversioned files", :recursive => FALSE )
        path = attributes.fetch(:path)
        verbose = attributes.fetch( :verbose, FALSE )

        verbose && puts( "#{functionID} : #{RevoUtils::getTimeStr} starting") 

        output = `svn status #{path}`

        output.each do |statusLine|

            if statusLine.match(/^\?.*/) then # match any line starting with ?
                strLen = statusLine.length
                fileName = statusLine[8,strLen].chomp
                fileName.sub!('\\','/')
                FileUtils.rm_r( fileName, :verbose=>verbose )
            end
        end
        verbose && puts( "#{functionID} : #{RevoUtils::getTimeStr} complete") 
    end

end
