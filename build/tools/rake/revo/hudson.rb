class Hudson
    require 'open-uri'
    
    def self.get_buildArtifacts(attributes)
        # 
        # Example Usage
        #    require "#{REPOROOT}/build/tools/rake/revo/hudson.rb" 
        #    Hudson::get_buildArtifacts( :server => "http://seahudson.revolution-computing.com:8080",
        #                           :jobName => "Derek-InstallBuilderTemplate.Mac",
        #                           :fileList => [ "DummyInstaller" ] ,
        #                           :destination => "/tmp" ,
        #                           :verbose => TRUE )
        #
        # See test/hudsonClass.test.rakefile for regression tests and more examples of 
        # how this method is used. 
        #

        server 		= attributes.fetch(:server)
        jobName		= attributes.fetch(:jobName)
        buildNumber	= attributes.fetch(:buildNumber, 'lastSuccessfulBuild')
#        auth		= attributes.fetch(:auth, [])  # no authorization right now because its not needed for artifacts DKB
        fileList 	= attributes.fetch(:fileList, [])
        destination	= attributes.fetch(:destination)
        verbose         = attributes.fetch(:verbose, FALSE)
        
        url 		= "#{server}/job/#{jobName}/#{buildNumber}/artifact/buildArtifacts"

        if fileList.empty?
            puts( "get_buildArtifacts : Warning : no files specified" )
            return 
        end

        # puts "VERBOSE: #{verbose}"        
        fileList.each do |fn|
            inputFileName = "#{url}/#{fn}"
            outputFileName = File.join( destination, fn )
            verbose && puts( "get_artifacts : #{inputFileName} => #{outputFileName}" )

#           open( inputFileName, :http_basic_authentication => auth ) { |f|
            open( inputFileName ) { |f|
                # the b is needed for windows, it means do binary mode download. 
                File.open( outputFileName ,"wb" ) do |file|
                    file.puts f.read
                end
            }
        end
    end # self.get_buildArtifacts(attributes)

    ############################
    def self.get_lastSuccessfulBuild_buildNumber(attributes)
        # 
        # Example Usage
        # 
        #    require "#{REPOROOT}/build/tools/rake/revo/hudson.rb" 

        #
        # See test/hudsonClass.test.rakefile for regression tests and more examples of 
        # how this method is used. 
        #

        server 		= attributes.fetch(:server)
        jobName		= attributes.fetch(:jobName)
        verbose         = attributes.fetch(:verbose, FALSE)

        buildNumber = -1
        url =  "#{server}/job/#{jobName}/lastSuccessfulBuild/buildNumber"

        verbose && puts( "get_lastSuccessfulBuild_buildNumber : #{url}" )

        open( url ) do |f|
            buildNumber = f.read
            puts buildNumber
        end

        return buildNumber

    end # get_lastSuccessfulBuild_buildNumber(attributes)

end
