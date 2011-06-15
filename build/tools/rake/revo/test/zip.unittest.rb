# 
# Regression tests for the zip class
#
#  Should be cross platform: can be run on mac or windows. 
#
#  Setup: 
#
#  To run: 
#    ruby zip.test.rb
#
#  Potential enhancements
#
# DKB 2010/4/7
#

require 'fileutils'
require 'pathname'
require 'test/unit'
require File.join( File.dirname(__FILE__), "..", "zip.rb")

# Test harness setup
BASEDIR = Pathname.new( File.dirname(__FILE__) )
DESTDIR = File.join( BASEDIR, "zipdestinationDir" )

FileUtils.mkdir_p DESTDIR

###########
#
class Test_zip < Test::Unit::TestCase

    ############
    def test_OneFolder

        zipFile = File.join( Dir.getwd, "zipfile.zip" )

        if File.exists? zipFile
            File.delete zipFile
        end

        RevoZip::zip( :zipFile => zipFile , 
                      :rootDir => Pathname.new(BASEDIR).parent,
                      :fileFolderList => ["test"], 
                      :verbose => TRUE )

        assert( File.exists?( zipFile ), "#{zipFile} does not exist" )

    end # test_OneFolder

    ############
    def test_globbing

        zipFile = File.join( Dir.getwd, "zipfile.zip" )

        if File.exists? zipFile
            File.delete zipFile
        end

        RevoZip::zip( :zipFile => zipFile , 
                      :rootDir => Pathname.new(BASEDIR).parent,
                      :fileFolderList => ["test", "*.rb"], 
                      :verbose => TRUE )

        assert( File.exists?( zipFile ), "#{zipFile} does not exist" )

    end # test_OneFolder

end
