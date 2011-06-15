# rhadoop.linux.rake.rb (Justin, February 2011)
#  - Linux build file for RHadoop packages

require 'pathname'
require 'rake/clean'
require 'configatron'

BASE_DIR       = File.dirname( __FILE__ )
SCRIPT_NAME    = File.basename( __FILE__ )

# define some basic paths for this script and to load the custom
# Revo libraries
TOOLS_DIR      = File.join( Pathname.new(BASE_DIR).parent, 'tools' )
RAKE_REVO_DIR  = File.join( TOOLS_DIR, 'rake', 'revo' )

require File.join(RAKE_REVO_DIR, "RevoSh.rb")

# initialize the configatron object and load the generic revo
# yaml file
configatron = Configatron::Store.new()
configatron.configure_from_yaml( File.join( TOOLS_DIR, 
   'revo-infrastructure.yml' ), :hash => 'production' )

# if WORKSPACE is defined, that means there's probably a Hudson build happening
# so, we'll build our worspace accordingly, otherwise just look for the source
# relative to the current script
if (ENV['WORKSPACE']).nil? then
   WORKSPACE = File.join(BASE_DIR, "workspace")
   TRUNK_DIR = "#{Pathname.new(BASE_DIR).parent.parent}"
else
   WORKSPACE = ENV['WORKSPACE']
   TRUNK_DIR = File.join(WORKSPACE, "src")
end

DOWNLOADS_DIR = "#{WORKSPACE}/downloads"
CLEAN.include(DOWNLOADS_DIR)

SVN_BASE = "#{configatron.svn.seasvn.URL}/rhadoop"
                  
desc "Prepare the environment"
task :prep do

   # create the workspace directory
   FileUtils.mkdir_p WORKSPACE

end

desc "Grab the source"
task :source => [ :prep ] do

   RevoSh::run( :cmd => "svn co #{SVN_BASE}/trunk #{TRUNK_DIR}" )

end

desc "Build the packages"
task :build do

   packages = [ 'rhbase',
                'rhdfs',
                'rhstream'
              ]

   packages.each do | package | 

      package_path = File.join( TRUNK_DIR, package )
      RevoSh::run( :cmd => "pushd #{package_path}; Revo64 CMD build pkg; popd" )

   end

end

desc "Sync whirr downloads to S3"
task :sync_whirr_downloads do

   FileUtils.mkdir_p DOWNLOADS_DIR

   # download the latest Revo Community build
   RevoSh::run( :cmd => "wget -nv -P #{DOWNLOADS_DIR} #{configatron.global.hudson.URL}/job/R2.12-RevoR_Installer-Linux/lastSuccessfulBuild/artifact/installer/redhat/BUILD/Community/Revo-Co-4.3-RHEL5.tar.gz" )

   rhadoop_hudson_path = "#{configatron.global.hudson.URL}/job/RHadoop-Packages-Linux/lastSuccessfulBuild/artifact/src"
   rhadoop_pkgs = [ 'rhbase/RevoHBase_0.2.tar.gz',
                    'rhdfs/RevoHDFS_0.2.tar.gz',
                    'rhstream/RevoHStream_0.2.tar.gz'
                  ]

   # download the latest RHadoop packages
   rhadoop_pkgs.each do | pkg |
      RevoSh::run( :cmd => "wget -nv -P #{DOWNLOADS_DIR} #{rhadoop_hudson_path}/#{pkg}" )
   end

   # push everything up to our S3 bucket, and make it available for public download
   RevoSh::run( :cmd => "s3cmd sync #{DOWNLOADS_DIR}/*.tar.gz s3://revo-whirr-downloads" )
   RevoSh::run( :cmd => "s3cmd setacl -r --acl-public s3://revo-whirr-downloads" )

end

desc "Sync whirr config scripts to S3"
task :sync_whirr_config do

   # first, remove all the .svn files
   RevoSh::run( :cmd => "s3cmd sync --exclude=*.svn* -r #{TRUNK_DIR}/build/config/whirr/0.3.0/* s3://revo-whirr-config" )
   RevoSh::run( :cmd => "s3cmd setacl --acl-public -r s3://revo-whirr-config" ) 

end

desc "Sync all whirr pieces to S3"
task :sync do

   Rake::Task["sync_whirr_config"].invoke
   Rake::Task["sync_whirr_downloads"].invoke

end

desc "Install the Cloudera Hadoop pieces to a remote server"
task :install_remote, :connect_string do |t, args|

   # if no connect string is specified, set a default 
   args.with_defaults( :connect_string => "root@test10-centos-55-64-hadoop" )
   connect_string = args['connect_string']

   # specify the script file
   script_file = "cloudera_standalone_remote.sh"

   # push the script file over to our remote hadoop host, then execute it with logging
   RevoSh::run( :cmd => "scp #{BASE_DIR}/#{script_file} #{connect_string}:/tmp" )
   RevoSh::run( :cmd => "ssh -t #{connect_string} \"pushd /tmp && chmod u+x #{script_file} && ./#{script_file} 2>&1 | tee cloudera_standalone_remote.log\"" )

end
