# 
# Regression tests for the RevoDevenv class
#
#  Setup: 
#
#  To run: 
#    svn checkout http://seasvn/parallelR/trunk/build/tools/rake/revo revoTest
#    cd revoTest
#    ruby RevoDevenv.unittest.rb
#    # To run individual test
#    ruby RevoDevenv.unittest.rb --name test_bare_solution
#
#  Potential enhancements
#
# DKB 2010/8/10
#

require 'fileutils'
require 'pathname'
require 'test/unit'
require File.join( File.dirname(__FILE__), "..", "RevoDevenv.rb")
require File.join( File.dirname(__FILE__), "..", "checksystem.rb")
require File.join( File.dirname(__FILE__), "..", "RevoUtils.rb")

# Test harness setup
BASEDIR = Pathname.new( File.dirname(__FILE__) )

###########
class Test_compile < Test::Unit::TestCase

    ############
    def test_bare_solution
        # test devenv with no project specified. 

        Dir.chdir( BASEDIR )
        # Redirect std output
        stdoutFile = "#{BASEDIR}/RevoDevenv.unittest.test_bare_solution.stdoutfile"
        stdoutOrig = $stdout
        $stdout = File.new( stdoutFile, 'w' )
        RevoDevenv::compile( :slnFile => "#{BASEDIR}/RevoDevenv.unittest.data/RevoDevenvTest/RevoDevenvTest.sln" , 
                             :clean => TRUE,
                             :slnConfiguration => "Release",
                             :slnPlatform => "Win32",
                             :compilerPlatform => "Win32",
                             :verbose => TRUE )
        $stdout.close
        $stdout = stdoutOrig

        # if modified files exist then there is a problem. 
        assert( File.exists?("#{BASEDIR}/RevoDevenv.unittest.data/RevoDevenvTest/Release/RevoDevenvTest.exe" ) ,  
                "RevoDevenv::compile did not produce RevoDevenvTest.exe" ) 

        matchP = RevoUtils::grep( :file => stdoutFile, 
                                  :regexp => "^RevoDevenv::compile : [0-9]{4}-[0-9]{2}-[0-9]{2}_[0-9]{2}:[0-9]{2}:[0-9]{2} compile starting.*$" )
        assert( matchP.length > 0,  "RevoDevenv::compile did not output appropriately formatted compile start message" ) 

        matchP = RevoUtils::grep( :file => stdoutFile, 
                                  :regexp => "^RevoDevenv::compile : [0-9]{4}-[0-9]{2}-[0-9]{2}_[0-9]{2}:[0-9]{2}:[0-9]{2} compile complete.*$" )
        assert( matchP.length > 0 ,  "RevoDevenv::compile did not output appropriately formatted compile complete message" ) 

    end 
    ############
    def test_specify_project
        # test devenv with project specified. 

        Dir.chdir( BASEDIR )
        
        RevoDevenv::compile( :slnFile => "#{BASEDIR}/RevoDevenv.unittest.data/RevoDevenvTest/RevoDevenvTest.sln" , 
                             :project => "HelloWorld", 
                             :clean => TRUE,
                             :slnConfiguration => "Release",
                             :slnPlatform => "Win32",
                             :compilerPlatform => "Win32",
                             :verbose => TRUE )

        assert( File.exists?("#{BASEDIR}/RevoDevenv.unittest.data/RevoDevenvTest/Release/HelloWorld.exe" ) ,  
                "RevoDevenv::compile did not produce HelloWorld.exe" ) 

    end 

    ############
    def test_x64_project
        # test devenv with project specified. 

        Dir.chdir( BASEDIR )
        
        RevoDevenv::compile( :slnFile => "#{BASEDIR}/RevoDevenv.unittest.data/RevoDevenvTest/RevoDevenvTest.sln" , 
                             :project => "HelloWorld", 
                             :clean => TRUE,
                             :slnConfiguration => "Release",
                             :slnPlatform => "x64",
                             :compilerPlatform => "x64",
                             :verbose => TRUE )

        assert( File.exists?("#{BASEDIR}/RevoDevenv.unittest.data/RevoDevenvTest/Release/HelloWorld.exe" ) ,  
                "RevoDevenv::compile did not produce HelloWorld.exe" ) 

    end 

end

