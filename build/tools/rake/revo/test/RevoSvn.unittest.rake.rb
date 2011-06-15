# 
# Regression tests for the RevoSvn class
#
#  Should be cross platform: can be run on mac or windows. 
#
#  Setup: 
#    Ensure that the entire build/tools/ folder is available, 
#    as this script uses revo utils. 
#  To run: 
#    rake -f RevoSvn.unittest.rb
#
#  Potential enhancements
#
# DKB 2010/12/8
#
require 'date'
require 'find'
require 'fileutils'
require 'pathname'
require 'yaml'
require 'test/unit'

# Test harness setup
BASEDIR = Pathname.new( File.dirname( File.expand_path(__FILE__)) )
WORKSPACE  = "#{Pathname.new(BASEDIR).parent.parent.parent.parent.parent}"

# Make the Revo build utilities available. 
puts("BASEDIR: #{BASEDIR}")
puts("WORKSPACE: #{WORKSPACE}")
Dir.glob(File.join(WORKSPACE, 'build/tools/rake/r*/*.rb')).each do |util|
    # puts "Requiring: #{util}"
    require util
end

####
# Load "configatron" with global, then rakefile specific values.  If overlaps in namespace, last load "wins"
env_key = ENV['DEPLOY'] || 'production'
RevoConfigatron::init_Configatron( :ymlFile => File.join(WORKSPACE, 'build/tools/revo-infrastructure.yml'), :env_key => env_key )
#RevoConfigatron::init_Configatron( :ymlFile => __FILE__.ext("yml"),                                         :env_key => env_key )

$unitTestBaseSvnURL = "http://seasvn/parallelR/trunk/build/tools/rake/revo/test"
$revoSvnUnitTestDataDir = "RevoSvn.unittest.data"


###########
# need a task to get tests to run 
task :default do
    puts( "complete" )
end

###########
class Test_checkout < Test::Unit::TestCase

    ############
    def test_simple_checkout
        puts( "Test: test_simple_checkout" )
        Dir.chdir( BASEDIR ) do 

            RevoSvn::checkout( :svnURL    => "#{$unitTestBaseSvnURL}/#{$revoSvnUnitTestDataDir}" , 
                               :workspace => "checkoutWorkspace" )
            expectedFile = "checkoutWorkspace/afile"
            assert( File.exists?( "#{expectedFile}" ), "File #{expectedFile} does not exist!!!" )
        end

    end

end # test checkout 

###########
class Test_commit < Test::Unit::TestCase

    ############
    def test_simple_commit
        puts( "Test: test_simple_commit" )
        Dir.chdir( BASEDIR ) do 
        
            testCommitWorkspace = "TestCommitWorkspace"

            RevoSvn::checkout( :svnURL    => "#{$unitTestBaseSvnURL}/#{$revoSvnUnitTestDataDir}" , 
                               :workspace => "testCommitWorkspace" )

            theFileName = "#{testCommitWorkspace}/afile"
            
            theTimeStr = RevoUtils::getTimeStr()
            open( theFileName, 'a' ) do |theFile|
                theFile.puts( RevoUtils::getTimeStr() )
            end

            RevoSvn::commit( :workspace => testCommitWorkspace,
                             :message   => "simple RevoSvn.commit test" )
            
            # now checkout again and look to see the change is there
            FileUtils.rm_rf( testCommitWorkspace )

            RevoSvn::checkout( :svnURL    => "#{$unitTestBaseSvnURL}/#{$revoSvnUnitTestDataDir}" , 
                               :workspace => "testCommitWorkspace" )

            retArray = RevoUtils::grep( :file => theFileName , 
                                        :regexp => theTimeStr,
                                        :verbose => true )
            
            assert( retArray.length == 1, "Did not find instances of \'#{theTimeStr}\' in \'#{theFileName}\'." )
        
        end













    end

end # test checkout 

###########
class Test_revert < Test::Unit::TestCase

    ############
    def test_recursive_revert
        puts( "Test: test_recursive_revert" )
        Dir.chdir( BASEDIR )

        # modify the test data and try to revert it. 
        File.open( "#{$revoSvnUnitTestDataDir}/afile" , "w" ) do |outfile|
            outfile.puts( "a modification" )
        end
        
        RevoSvn::revert( :path => "#{$revoSvnUnitTestDataDir}" , 
                         :recursive => TRUE )

        # svn status | grep "^M "         
        cmd = "svn status -q #{$revoSvnUnitTestDataDir} > junk.txt"
        if ! system( cmd ) then
            abort "\tError running: '#{cmd}'.\n\tResult: #{res}\n\tExit status:#{$?}."
        end

        fileSize = File.size( "junk.txt" )

        # if modified files exist then there is a problem. 
        assert( fileSize == 0, "Found modified svn files in #{$revoSvnUnitTestDataDir} after revert!!!" )

    end # test_recursive_revert

    ############
    def test_single_file_revert
        puts( "Test: test_single_file_revert" )
        Dir.chdir( BASEDIR )

        # modify the test data and try to revert it. 
        File.open( "#{$revoSvnUnitTestDataDir}/bfile" , "w" ) do |outfile|
            outfile.puts( "a modification" )
        end
        
        RevoSvn::revert( :path => "#{$revoSvnUnitTestDataDir}/bfile" , 
                         :recursive => FALSE )

        File.delete( "junk.txt" )
        cmd = "svn status -q #{$revoSvnUnitTestDataDir} > junk.txt"
        if ! system( cmd ) then
            abort "\tError running: '#{cmd}'.\n\tResult: #{res}\n\tExit status:#{$?}."
        end

        fileSize = File.size( "junk.txt" )
        # if modified files exist then there is a problem. 

        assert( fileSize == 0, "Found modified svn files in #{$revoSvnUnitTestDataDir} after single file revert!!!" )

    end # test_single_file_revert

    ############
    def test_verbose_single_file_revert
        puts( "Test: test_verbose_single_file_revert" )
        Dir.chdir( BASEDIR )
        
        # modify the test data and try to revert it. 
        File.open( "#{$revoSvnUnitTestDataDir}/bfile" , "w" ) do |outfile|
            outfile.puts( "a modification" )
        end
        # Redirect std output
        stdoutFile = "#{BASEDIR}/RevoDevenv.unittest.test_verbose_single_file_revert.stdoutfile"
        stdoutOrig = $stdout
        $stdout = File.new( stdoutFile, 'w' )
        RevoSvn::revert( :path => "#{$revoSvnUnitTestDataDir}/bfile" , 
                         :recursive => FALSE,
                         :verbose => TRUE )
        $stdout.close
        $stdout = stdoutOrig

        
        matchP = RevoUtils::grep( :file => stdoutFile, 
                                  :regexp => "^RevoSvn::revert : [0-9]{4}-[0-9]{2}-[0-9]{2}_[0-9]{2}:[0-9]{2}:[0-9]{2} complete.*$" )
        assert( matchP.length > 0, "RevoSvn::revert did not output appropriately formatted revert complete message" ) 
        
        matchP = RevoUtils::grep( :file => stdoutFile, 
                                  :regexp => "^RevoSvn::revert : [0-9]{4}-[0-9]{2}-[0-9]{2}_[0-9]{2}:[0-9]{2}:[0-9]{2} starting.*$" )
        assert( matchP.length > 0, "RevoSvn::revert did not output appropriately formatted revert start message" ) 
        
    end 

    ############
    def test_revert_abort
        puts( "Test: test_revert_abort" )
        Dir.chdir( BASEDIR )
        assert_raise SystemExit do 
            RevoSvn::revert( :path => "dir c:\\nonexistantdir" )
        end 

    end # test_revert_abort

end

class Test_removeUnversionedFiles < Test::Unit::TestCase

    ############
    def test_one_unversioned_file
        puts( "Test: test_one_unversioned_file" )
        Dir.chdir( BASEDIR )

        # add an unversioned file and check to see that it gets removed.
        File.open( "#{$revoSvnUnitTestDataDir}/unversionedfile" , "w" ) do |outfile|
            outfile.puts( "a modification" )
        end

        # Redirect std output
        stdoutFile = "#{BASEDIR}/RevoDevenv.unittest.test_one_unversioned_file.stdoutfile"
        stdoutOrig = $stdout
        $stdout = File.new( stdoutFile, 'w' )
        
        RevoSvn::removeUnversionedFiles( :path => "#{$revoSvnUnitTestDataDir}" , :verbose => TRUE )
        $stdout.close
        $stdout = stdoutOrig

        
        if File.exists?( "junk.txt" ) then File.delete( "junk.txt" ) end
        cmd = "svn status #{$revoSvnUnitTestDataDir} > junk.txt"
        if ! system( cmd ) then
            abort "\tError running: '#{cmd}'.\n\tResult: #{res}\n\tExit status:#{$?}."
        end

        fileSize = File.size( "junk.txt" )

        # if modified files exist then there is a problem. 
        assert( fileSize == 0, "Found unversioned files in #{$revoSvnUnitTestDataDir} after removeUnversionedFiles!!!" )
        
        matchP = RevoUtils::grep( :file => stdoutFile, 
                                  :regexp => "^RevoSvn::removeUnversionedFiles : [0-9]{4}-[0-9]{2}-[0-9]{2}_[0-9]{2}:[0-9]{2}:[0-9]{2} complete.*$" )
        assert( matchP.length > 0, "RevoSvn::removeUnversionedFiles did not output appropriately formatted removeUnversionedFiles complete message" ) 
        
        matchP = RevoUtils::grep( :file => stdoutFile, 
                                  :regexp => "^RevoSvn::removeUnversionedFiles : [0-9]{4}-[0-9]{2}-[0-9]{2}_[0-9]{2}:[0-9]{2}:[0-9]{2} starting.*$" )
        assert( matchP.length > 0, "RevoSvn::removeUnversionedFiles did not output appropriately formatted removeUnversionedFiles start message" ) 

    end

end # Test_removeUnversionedFiles

