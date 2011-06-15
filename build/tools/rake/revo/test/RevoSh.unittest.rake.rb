# 
# Regression tests for the RevoSh class
#
#  Setup: 
#
#  To run: 
#    svn checkout http://seasvn/parallelR/trunk/build/tools/rake/revo revoTest
#    cd revoTest
#    rake -f RevoSh.unittest.rb
#    Note: This one requires running rake, because RevoSh::run is encapsulating a rake methos, 'sh'. 
#
#  Potential enhancements
#
# DKB 2010/8/10
#

require 'rake/clean'
require 'fileutils'
require 'pathname'
require 'test/unit'

BASEDIR = Pathname.new( File.dirname(__FILE__) )
require File.join( File.dirname(__FILE__), "..", "RevoSh.rb")

task :default do
    puts( "complete" )
end

###########
class Test_run < Test::Unit::TestCase

    ############
    def test_simple_run

        Dir.chdir( BASEDIR )

        assert_nothing_raised do 
            RevoSh::run( :cmd => "dir" )
        end 

    end 

    ############
    def test_password_masking

        stdoutFile = "RevoSh.unittest.test_password_masking.stdoutput.txt"
        stdoutOrig = $stdout
        $stdout = File.new( stdoutFile, 'w' )

        pw = "b0bthebuilder"
        RevoSh::run( :cmd => "svn --username bob --password #{pw} ls http://seasvn/parallelR", 
                     :password => "#{pw}")

        $stdout.close
        $stdout = stdoutOrig

        firstLine = File.read( stdoutFile )

        assert( firstLine.match("PasswordNotLogged"), "Password not appropriately logged" ) 

    end 

    ############
    def test_abortOnFailure_false
        assert_nothing_raised do 
            RevoSh::run( :cmd => "dir c:\\nonexistantdir",
                         :abortOnFailure => FALSE )
        end 
    end 


    ############
    def test_abortOnFailure_true
        assert_raise SystemExit do 
            RevoSh::run( :cmd => "dir c:\\nonexistantdir" )
        end 
    end 


    ############
    def test_returnvalue_true
        retVal = RevoSh::run( :cmd => "dir", :abortOnFailure => FALSE )
        assert( retVal == TRUE, "RevoSH::run did not return TRUE when it should have" )
    end 

    ############
    def test_returnvalue_false
        retVal = RevoSh::run( :cmd => "dir c:/nonexistantdirectory", :abortOnFailure => FALSE )
        assert( retVal == FALSE, "RevoSH::run did not return FALSE when it should have" )
    end 

end
