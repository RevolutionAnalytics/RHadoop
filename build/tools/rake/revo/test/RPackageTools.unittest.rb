# 
# Regression tests for the RPackageTools class
#
#  Setup: 
#
#  To run: 
#    svn checkout http://seasvn/parallelR/trunk/build/tools/rake/revo revoTest
#    cd revoTest
#    ruby RPackageTools.unittest.rb
#    # To run individual test
#    ruby RPackageTools.unittest.rb --name test_bare_solution
#
#  Potential enhancements
#
# DKB 2010/10/07
#

require 'fileutils'
require 'pathname'
require 'test/unit'
require File.join( File.dirname(__FILE__), "..", "RPackageTools.rb")
require File.join( File.dirname(__FILE__), "..", "checksystem.rb")
require File.join( File.dirname(__FILE__), "..", "RevoUtils.rb")

# Test harness setup
BASEDIR = Pathname.new( File.dirname(__FILE__) )

###########
class Test_installPackage < Test::Unit::TestCase

    ############
    def test_installpackage_dryrun
        # test devenv with no project specified. 

        Dir.chdir( BASEDIR )
        FileUtils.rm_rf("DummyDest") if File.exists?( "DummyDest" ) #clean up after other test
        # Redirect std output
        stdoutFile = "#{BASEDIR}/RPackageTools.unittest.test_installpackage_dryrun.stdoutfile"
        stdoutOrig = $stdout
        $stdout = File.new( stdoutFile, 'w' )
        RPackageTools.installPackage(:rPath              => "C:/R4BLD/R-2.11.1-32", 
                                     :packageBaseName    => "DummyPkg",         
                                     :packageBinary      => "DummyPkg_0.0-0.zip", 
                                     :packageInstallDest => "DummyDest",
                                     :dryRun             => TRUE )
        $stdout.close
        $stdout = stdoutOrig

        # Not the most definitive test, but will do for now. 
        matchP = RevoUtils::grep( :file => stdoutFile, 
                                  :regexp => 'C:/R4BLD/R-2.11.1-32/bin/Rscript -e .*DummyPkg_0.0-0.zip.*DummyDest.*repos=NULL' )
        assert( matchP.length > 0,  "RPackageTools::installPackage did not output appropriate Rscript string" ) 


    end 

    ############
    def test_installpackage_dryrun_exists
        # test devenv with no project specified. 

        Dir.chdir( BASEDIR )
        # Redirect std output
        FileUtils.mkdir_p( "DummyDest" )
        File.open( "DummyDest/DummyPkg", "w" ) do |theFile|
            theFile.puts( "stuff" )
        end

        stdoutFile = "#{BASEDIR}/RPackageTools.unittest.test_installpackage_dryrun_exists.stdoutfile"

        stdoutOrig = $stdout
        $stdout = File.new( stdoutFile, 'w' )
        RPackageTools.installPackage(:rPath              => "C:/R4BLD/R-2.11.1-32", 
                                     :packageBaseName    => "DummyPkg",         
                                     :packageBinary      => "DummyPkg_0.0-0.zip", 
                                     :packageInstallDest => "DummyDest",
                                     :dryRun             => TRUE )
        $stdout.close
        $stdout = stdoutOrig

        # Not the most definitive test, but will do for now. 
        matchP = RevoUtils::grep( :file => stdoutFile, 
                                  :regexp => 'DummyDest/DummyPkg exists' )
        assert( matchP.length > 0,  "RPackageTools::packageInstall did not output appropriately formatted package does not exist message" ) 


    end 

end

