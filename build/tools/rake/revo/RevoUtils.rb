# Utilities for use in REvolution Rake files. 

private
def this_method_name
  Kernel::caller[0] =~ /(\/[^']*)'/ and $1
  #Kernel::caller[0]
end

class RevoUtils
    def self.getTimeStr()
        theTimeStr = Time.now.strftime("%Y-%m-%d_%H:%M:%S")
        theTimeStr
    end # end getTimeStr()
    
    def self.setHostOS
        #puts "\tClass: " + object.class.name + "  Method: " + this_method_name
        
        
        os = RbConfig::CONFIG['host_os']
        if os =~ /mswin|windows|cygwin|mingw32/i
            configatron.host.os = "win"
        elsif os =~ /darwin|mac os/i
            configatron.host.os = "mac"
        elsif os =~ /linux/i
            configatron.host.os = "linux"
        else
            abort "Unable to determine host OS."
        end
        os = configatron.host.os
    end
    
    def self.globOneFile (attributes)
        # Return results from glob.  Vary error behavior based on failOnZero and failOnMulti attributes.
        # e.g. - RevoUtils::globOneFile(:glob => "#{WORKSPACE}/*", :failOnMulti => false )
        methodID    = "RevoUtils::globOneFile"
        fileGlob    = attributes.fetch(:glob, nil)
        failOnZero  = attributes.fetch(:failOnZero, true)
        failOnMulti = attributes.fetch(:failOnMulti, true)
        abort("#{methodID} - missing parameter: glob.") unless fileGlob
        
        puts "#{methodID} - Glob: #{fileGlob}."
        fileArray    = Dir.glob( fileGlob )
        case fileArray.length
            when 0 
                puts "#{methodID} - Unable to find file."
                abort "Aborting..." if failOnZero
                return nil
            when 1
                file = fileArray[0]
                puts "#{methodID} - File found: #{file}."
                return file
            else
                puts "#{methodID} - Multiple files found: #{fileArray.inspect}."
                abort "Aborting..." if failOnMulti
                return fileArray
        end
    end  # globOneFile 

    def self.grep ( attributes ) 
        functionID = "RevoUtils::grep"

        # Usage: 
        # retarray = RevoUtils::grep ( :file => "filepath", 
        #                             :regexp => "a regular expression",
        #                             :exceptions => [ "regexp1", "regexp2", ... ],
        #                             :verbose => TRUE ) 
        # Returns array of lines that match theRegExp, with exceptions removed. 


        theFile = attributes.fetch(:file )
        theRegExp = attributes.fetch(:regexp )
        theExceptions = attributes.fetch( :exceptions, [] )
        verbose = attributes.fetch( :verbose, FALSE )
        retArray = Array.new( )
        if verbose then
            puts( "\n\n" )
            puts( "#{functionID} : Looking for \'#{theRegExp.to_s}\'" )
            if 0 != theExceptions.length then
                theExceptions.each do |anException| 
                    puts( "#{functionID} : \t\twith Exception \'#{anException}\' " )
                end
            end
            puts( "#{functionID} : \t\tin #{theFile}\n\n" )
        end

        File.open( theFile ) do |theFileId|
            lineNum = 0
            while ( line = theFileId.gets )
                cLine = line.chomp
                lineNum += 1
                if cLine.match( theRegExp ) then
                    # make sure line does not match any of the exceptions
                    noExcptMatch = TRUE
                    theExceptions.each do | exception | 
                        if cLine.match( exception ) then
                            noExcptMatch = FALSE
                        end
                    end
                    if ( TRUE == noExcptMatch ) then
                        verbose && puts( "#{lineNum} : #{cLine} " )
                        retArray.concat( [ cLine ] )
                    end
                end
            end
        end

        verbose && retArray.length == 0 && puts( "#{functionID} :\tNo matching lines found" )

        return retArray
    end # grep 

    def self.timingSeparator( scriptname, idString)
        puts("#{scriptname} : ==============================================\n")
        puts("#{scriptname} : Timing : #{idString} : #{RevoUtils::getTimeStr}\n")
    end # timingSeparator
    
    ########################
    def self.recreateDir(dirName)
        if File.exists? dirName
            puts "Removing directory: #{dirName}."
            FileUtils.rm_rf dirName
        end
        puts "Creating directory: #{dirName}."
        FileUtils.mkdir_p( dirName)
    end 

end
