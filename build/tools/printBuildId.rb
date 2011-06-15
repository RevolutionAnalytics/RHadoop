#!/usr/bin/ruby
#
# Create a buildID of the form <jobname>-<svnrevision>-<buildnumber>
# for a RevoR build job. Write the build id to standard out. 
#
# Usage: printBuildId [options]
#     -j, --jobname <jobname>          Mandatory argument, string
#     -b, --buildnum <build number>    Mandatory argument, integer number
#     -s, --svnpath <subversion path>  Mandatory argument, path from subversion root server
#     -v, --verbose                    Optional argument
#     -h, --help                       Display this screen
#
# Example Usage: Redefine hudson BUILD_TAG with our BuildID
# linux> export BUILD_TAG=`ruby printBuildId.rb -j $JOB_NAME -b $BUILD_NUMBER -s parallelR/trunk` 
#
# Windows> ruby printBuildId.rb -j %JOB_NAME% -b %BUILD_NUMBER% -s parallelR/trunk > BuildID
# Windows> set /p BUILD_TAG=<BuildID
#
# Notes: 
# 
# Derek Brown 2011/03/17
#
require 'rubygems'
require 'pathname'
require 'fileutils'
require 'optparse'
require 'pp'

$basedir    = File.dirname(__FILE__)
$workspace  = "#{Pathname.new($basedir).parent.parent}"

#########################
# Parse and verify the options
options = {} # This hash will hold all of the options parsed from the command-line by OptionParser.
verbose = FALSE
optparse = OptionParser.new do|opts|
    options[:jobname] = ""
    opts.on( '-j', '--jobname <jobname>', "Mandatory argument, string" ) do|f|
        options[:jobname] = f
    end
    options[:buildnumber] = ""
    opts.on( '-b', '--buildnum <build number> ', "Mandatory argument, integer number" ) do|f|
        options[:buildnumber] = f
    end
    options[:svnpath] = ""
    opts.on( '-s', '--svnpath <subversion path> ', "Mandatory argument, path from subversion root server" ) do|f|
        options[:svnpath] = f
    end
    options[:verbose] = ""
    opts.on( '-v', '--verbose', "Optional argument" ) do|f|
        verbose = TRUE
    end
    # This displays the help screen, all programs are
    # assumed to have this option.
    opts.on( '-h', '--help', 'Display this screen' ) do
        puts opts
        exit
    end
end
optparse.parse!
# pp "Options:", options

jobname=options[:jobname]
buildnumber=options[:buildnumber]
svnpath=options[:svnpath]

# looked for better way of specifying required parameters.
# Google turned up several examples similar to this. 
abortFlag = false
if 0 == jobname.length
  puts "ERROR: Parameter --jobname is required\n"    
  abortFlag = true
end
if 0 == buildnumber.length
  puts "ERROR: Parameter --buildnum is required\n"    
  abortFlag = true
end
if 0 == svnpath.length
  puts "ERROR: Parameter --svnpath is required\n"    
  abortFlag = true
end

if abortFlag
  abort "aborting, bad parameters\n"
end

######################
# load configatron
require(File.join($workspace, 'build/tools/rake/rake-me/configuration.rb'))
Dir.glob(File.join($workspace, 'build/tools/rake/revo/*.rb')).each do |f|
    puts( "Requiring #{f}" ) if verbose
    require f      
end

# Load "configatron" with global values
env_key = ENV['DEPLOY'] || 'production'
RevoConfigatron::init_Configatron( :ymlFile => File.join($workspace, 'build/tools/revo-infrastructure.yml'), :env_key => env_key, :verbose => verbose )

#######################
# get the svn revision number
fullsvnpath = "#{configatron.svn.seasvn.URL}/#{svnpath}"
puts( "full svn path: " + fullsvnpath ) if verbose
user = configatron.global.builder.user
pw = configatron.global.builder.pw
svncmd = "svn --username #{user} --password #{pw} info #{fullsvnpath}"
puts( "svncmd: #{svncmd}" ) if verbose
svninfo = `#{svncmd}`
svnrevision = "undef"
svninfo.each do |line|
    puts( line ) if verbose
    #if line.match(/Last Changed Rev: .*/) then
    if line.match(/Revision: .*/) then
        # svnrevision = line.match(/Last Changed Rev: (.*)/)[1]
        svnrevision = line.match(/Revision: (.*)/)[1]
    end
end

#######################
# finally build up the BuildID
puts "#{jobname}-#{svnrevision}-#{buildnumber}"
exit 0
