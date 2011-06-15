# 
# Regression tests for the hudson class
#
#  Should be cross platform: can be run on mac or windows. 
#
#  Setup: 
#
#  To run: 
#    ruby hudsonClass.test.rb
#
#  Potential enhancements
#   - Use the test setup and takedown functionality. 
# DKB 2010/4/5
#

require 'fileutils'
require 'test/unit'
require File.join( File.dirname(__FILE__), "..", "hudson.rb")

# Test harness setup
BASEDIR = File.dirname(__FILE__)
DESTDIR = File.join( BASEDIR, "destinationDir" )

FileUtils.mkdir_p DESTDIR

###########
#
class Test_get_lastSuccessfulBuild_buildNumber < Test::Unit::TestCase

    ############
    def test_HappyPath
        # Test the get_buildArtifacts class. 
        # Expect: Warning that no files were specified

        stdoutFile = File.join( DESTDIR, "HappyPath.stdoutput.txt")
        stdoutOrig = $stdout
        $stdout = File.new( stdoutFile, 'w' )

        actualJobNumber = Hudson::get_lastSuccessfulBuild_buildNumber( :server => "http://seahudson.revolution-computing.com:8080", 
                                                                       :jobName => "RevoHudsonClassTest", 
                                                                       :verbose => TRUE )
        $stdout.close
        $stdout = stdoutOrig

        
        firstLine = File.read( stdoutFile )
        secondLine = File.read( stdoutFile )

        puts "secondLine: #{firstLine}\n"
        assert_match( "#{actualJobNumber}", secondLine )

    end # test_HappyPath

    ###################
    def test_VerboseOff
        # Test the get_buildArtifacts class. 
        # Expect to see no output
        artifactName1 = "Artifact.One.txt"
        destFileFullPath1 = File.join( DESTDIR, artifactName1 )

        if File.exists? destFileFullPath1 
            File.delete destFileFullPath1
        end

        verboseOffTestStdOut = File.join( DESTDIR, "GetBuildNumberVerboseOff.stdoutput.txt")
        if File.exists? verboseOffTestStdOut
            File.delete verboseOffTestStdOut
        end

        $stdout = File.new( verboseOffTestStdOut, 'w' )
        Hudson::get_lastSuccessfulBuild_buildNumber ( :server => "http://seahudson.revolution-computing.com:8080",
                                                      :jobName => "RevoHudsonClassTest",
                                                      :verbose => FALSE )

        assert_equal( 0, File.size(verboseOffTestStdOut), "Output not empty" )

    end # test_VerboseOff

end # class Test_get_lastSuccessfulBuild_buildNumber < Test::Unit::TestCase

#########
#
class TestGetArtifactsTests < Test::Unit::TestCase
    ############
    def test_NoFilesSpecified
        # Test the get_buildArtifacts class. 
        # Expect: Warning that no files were specified

        stdoutFile = File.join( DESTDIR, "NoFilesSpecified.stdoutput.txt")
        stdoutOrig = $stdout
        $stdout = File.new( stdoutFile, 'w' )
        Hudson::get_buildArtifacts( :server => "http://seahudson.revolution-computing.com:8080",
                                    :jobName => "RevoHudsonClassTest",
                                    :destination => DESTDIR,
                                    :verbose => TRUE )
        $stdout.close
        $stdout = stdoutOrig
        
        firstLine = File.read( stdoutFile )

        puts "FirstLine: #{firstLine}\n"
        assert_match( /.*Warning.*/, firstLine )

    end # test_NoFilesSpecified

    ###############
    def test_OneArtifact
        # Test the get_buildArtifacts class. 
        # Expect to see just one artifact pulled from RevoHudsonClassTest
        artifactName = "STAGEDIR.zip"
        destFileFullPath = File.join( DESTDIR, artifactName )

        if File.exists? destFileFullPath 
            File.delete destFileFullPath
        end

        $stdout = File.new(File.join( DESTDIR, "stdoutput.txt"), 'w' )
        Hudson::get_buildArtifacts( :server => "http://seahudson.revolution-computing.com:8080",
                                    :jobName => "RevoHudsonClassTest",
                                    :fileList => [ "STAGEDIR.zip" ] ,
                                    :destination => DESTDIR,
                                    :verbose => TRUE )

        existsP = File.exists? destFileFullPath
        assert( existsP, "Artifact did not get extracted"  )

    end # test_OneArtifact

    ############
    def test_SpecificJobNumber
        # Test the get_buildArtifacts class. 
        # Expect one artifact pulled from RevoHudsonClassTest, should indicate it is from build 7

        artifactName = "Artifact.One.txt"
        destFileFullPath = File.join( DESTDIR, artifactName )

        if File.exists? destFileFullPath 
            File.delete destFileFullPath
        end

        $stdout = File.new(File.join( DESTDIR, "SpecificJobNumber.stdoutput.txt"), 'w' )
        Hudson::get_buildArtifacts( :server => "http://seahudson.revolution-computing.com:8080",
                                    :jobName => "RevoHudsonClassTest",
                                    :buildNumber => 245,
                                    :fileList => [ artifactName ] ,
                                    :destination => DESTDIR,
                                    :verbose => TRUE )

        existsP = File.exists? destFileFullPath
        assert( existsP, "Artifact did not get extracted"  )
        firstLine = File.read( destFileFullPath )
        assert_equal( "http://seahudson:8080/job/RevoHudsonClassTest/245/\n", firstLine )

    end # test_SpecificJobNumber

    ###################
    def test_TwoArtifacts
        # Test the get_buildArtifacts class. 
        # Expect to see two artifacts pulled from RevoHudsonClassTest
        artifactName1 = "Artifact.One.txt"
        destFileFullPath1 = File.join( DESTDIR, artifactName1 )
        artifactName2 = "Artifact.Two.txt"
        destFileFullPath2 = File.join( DESTDIR, artifactName1 )

        if File.exists? destFileFullPath1 
            File.delete destFileFullPath1
        end

        if File.exists? destFileFullPath2
            File.delete destFileFullPath2
        end

        $stdout = File.new(File.join( DESTDIR, "stdoutput.txt"), 'w' )
        Hudson::get_buildArtifacts( :server => "http://seahudson.revolution-computing.com:8080",
                                    :jobName => "RevoHudsonClassTest",
                                    :fileList => [ artifactName1, artifactName2 ] ,
                                    :destination => DESTDIR,
                                    :verbose => TRUE )

        existsP = File.exists? destFileFullPath1
        assert( existsP, "Artifact.One.txt did not get extracted"  )
        existsP = File.exists? destFileFullPath2
        assert( existsP, "Artifact.Two.txt did not get extracted"  )

    end # test_TwoArtifacts


    ###################
    def test_VerboseOff
        # Test the get_buildArtifacts class. 
        # Expect to see no output, and one artifact pulled from RevoHudsonClassTest
        artifactName1 = "Artifact.One.txt"
        destFileFullPath1 = File.join( DESTDIR, artifactName1 )

        if File.exists? destFileFullPath1 
            File.delete destFileFullPath1
        end

        verboseOffTestStdOut = File.join( DESTDIR, "VerboseOff.stdoutput.txt")
        if File.exists? verboseOffTestStdOut
            File.delete verboseOffTestStdOut
        end

        $stdout = File.new( verboseOffTestStdOut, 'w' )
        Hudson::get_buildArtifacts( :server => "http://seahudson.revolution-computing.com:8080",
                                    :jobName => "RevoHudsonClassTest",
                                    :fileList => [ artifactName1 ] ,
                                    :destination => DESTDIR,
                                    :verbose => FALSE )

        existsP = File.exists? destFileFullPath1
        assert( existsP, "Artifact.One.txt did not get extracted"  )

        assert_equal( 0, File.size(verboseOffTestStdOut), "Output not empty" )

    end # test_VerboseOff

end
