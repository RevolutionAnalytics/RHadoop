# 
# Regression tests for the RevoUtils class
#
#  Should be cross platform: can be run on mac or windows. 
#
#  Setup: 
#
#  To run: 
#    ruby RevoUtils.unittest.rb
#    # To run individual test
#    ruby RevoUtils.unittest.rb --name test_name
#
#  Potential enhancements
#    - none at this time
#
# DKB 2010/9/2
#

require 'fileutils'
require 'pathname'
require 'test/unit'
require File.join( File.dirname(__FILE__), "..", "RevoUtils.rb")

# Test harness setup
BASEDIR = Pathname.new( File.dirname(__FILE__) )
BASENAME = File.basename(__FILE__)
TestDataDir = "#{BASEDIR}/#{BASENAME}.data"
###########
class Test_grep < Test::Unit::TestCase

    ############
    def test_basic_grep
        testID = "test_basic_grep"
        puts( "Test: #{testID}" )
        Dir.chdir( BASEDIR )

        testFile = "#{TestDataDir}/testdata.txt"
        regexp = "pattern number one"
        retArray = RevoUtils::grep( :file => testFile , 
                                    :regexp => regexp,
                                    :verbose => true )

        assert( retArray.length == 3, "Did not find 3 instances of \'#{regexp}\' in \'#{testFile}\'." )


        testFile = "#{TestDataDir}/testdata.txt"
        regexp = "pattern number two"
        retArray = RevoUtils::grep( :file => testFile , 
                                    :regexp => regexp,
                                    :verbose => true )

        assert( retArray.length == 2, "Did not find 2 instances of \'#{regexp}\' in \'#{testFile}\'." )


        testFile = "#{TestDataDir}/testdata.txt"
        regexp = /pattern number two/i
        retArray = RevoUtils::grep( :file => testFile , 
                                    :regexp => regexp,
                                    :verbose => true )

        assert( retArray.length == 3, "Did not find 3 instances of \'#{regexp}\' in \'#{testFile}\'." )

    end 

    ############
    def test_single_exception
        testID = "test_single_exception"
        puts( "Test: #{testID}" )
        Dir.chdir( BASEDIR )

        testFile = "#{TestDataDir}/testdata.txt"
        regexp = /pattern number three/
        exception = /with exception/
        retArray = RevoUtils::grep( :file => testFile , 
                                    :regexp => regexp,
                                    :exceptions => [ exception ],
                                    :verbose => true )

        assert( retArray.length == 2, "Did not find 2 instances of \'#{regexp}\' with exception #{exception} in \'#{testFile}\'." )

    end 

    ############
    def test_several_exceptions
        testID = "test_several_exceptions"
        puts( "Test: #{testID}" )
        Dir.chdir( BASEDIR )

        testFile = "#{TestDataDir}/testdata.txt"
        regexp = /pattern number four/
        exceptions = [ /with exception/, /another exception/, /an exception/ ]
        retArray = RevoUtils::grep( :file => testFile , 
                                    :regexp => regexp,
                                    :exceptions => exceptions,
                                    :verbose => true )

        assert( retArray.length == 4, "Did not find 4 instances of \'#{regexp}\' with exceptions #{exceptions} in \'#{testFile}\'." )

    end 

    ############
    def test_empty_file
        testID = "test_empty_file"
        puts( "Test: #{testID}" )
        Dir.chdir( BASEDIR )

        testFile = "#{TestDataDir}/testdata.zerolength.txt"
        regexp = /pattern number four/
        retArray = RevoUtils::grep( :file => testFile , 
                                    :regexp => regexp,
                                    :verbose => true )

        assert( retArray.length == 0, "Did not find 0 instances of \'#{regexp}\' in zero length file, #{testFile}" )

    end 

    ############
    def test_verbose_false
        testID = "test_verbose_false"
        puts( "Test: #{testID}" )
        Dir.chdir( BASEDIR )

        testFile = "#{TestDataDir}/testdata.txt"
        regexp = /pattern number three/
        exception = /with exception/
        stdoutFile = "#{BASEDIR}/#{BASENAME}.#{testID}.stdoutfile"
        stdoutOrig = $stdout
        $stdout = File.new( stdoutFile, 'w' )
        retArray = RevoUtils::grep( :file => testFile , 
                                    :regexp => regexp,
                                    :exceptions => [ exception ],
                                    :verbose => false )
        $stdout.close
        $stdout = stdoutOrig

        assert( File.size( stdoutFile ) == 0, "Verbose did not produce empty stdout file" )

    end 


    ############
    def test_no_match
        testID = "test_no_match"
        puts( "Test: #{testID}" )
        Dir.chdir( BASEDIR )

        testFile = "#{TestDataDir}/testdata.txt"
        regexp = /no match in data file/

        retArray = RevoUtils::grep( :file => testFile , 
                                    :regexp => regexp )

        assert( retArray.size == 0 , "No match did not produce empty array" )

    end 

end
