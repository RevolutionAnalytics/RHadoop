class RevoConfigatron
    require 'yaml'
    require 'configatron'

    def self.init_Configatron(attributes)
        # 
        # Example Usage
        #    require "#{REPOROOT}/build/tools/rake/revo/revoConfigatron.rb" 
        #    revoConfigatron::init_Configatron( :ymlFile => ymlFile,
        #                                       :env_key => env_key )
        #
        # This uses code described here.
        # http://therightstuff.de/2010/01/30/Rake-YAML-And-Inherited-Build-Configuration.aspx
        # Configatron: http://github.com/markbates/configatron/

        ymlFile     = attributes.fetch(:ymlFile)
        env_key 	= attributes.fetch(:env_key, 'production')
        verbose 	= attributes.fetch(:verbose, TRUE)
        abort "init_Configatron : Yaml file does not exist - #{ymlFile}."  unless File.exists?(ymlFile)
        abort "init_Configatron : env_key value is empty."                 if env_key.nil?
     
        puts "Populating \"configatron\" from file:\n\t#{ymlFile}\n\tusing environment '#{env_key}'" if verbose

        yaml = Configuration.load_yaml ymlFile, :hash => env_key, :inherit => :default_to, :override_with => :local_properties
        configatron.configure_from_hash yaml

    end

end
