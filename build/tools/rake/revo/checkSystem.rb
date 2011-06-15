class CheckSystem

	def self.environment(attributes)
		envVariSet = attributes.fetch(:envVariSet, [] )
		envVariDir = attributes.fetch(:envVariDir, [] )
		fileExists = attributes.fetch(:fileExists, [] )

		errmsg = ""

		puts "--- Ensure environment variables are set..." unless envVariSet.empty?
		envVariSet.each do |env|
			envValue = "#{ENV["#{env}"]}"
			if envValue.empty?
				errmsg = errmsg + "Unset environment variable: #{env}.\n"
			else
				puts "#{env}: #{envValue}\n"
			end
		end

		puts "--- Ensure environment variables are directories ..."  unless envVariDir.empty?
		envVariDir.each do |env|
			envValue = "#{ENV["#{env}"]}"
			if envValue.empty?
				errmsg = errmsg + "Unset environment variable: #{env}.\n"
			elsif not File.directory?(envValue)
				errmsg = errmsg + "#{env} value is not a directory: #{envValue}\n"
			else
				puts "#{env}: #{envValue}\n"
			end
		end

		puts "--- Ensure files exist ..."  unless fileExists.empty?
		fileExists.each do |fn|
			if File.exists?(fn)
				puts "File exists: #{fn}\n"
			else
				errmsg = errmsg + "File does not exist: #{fn}\n"
			end
		end
		
		if errmsg == ""
			puts "--- All checks OK."
		else
			errmsg = "\n------\nERROR:\n" + errmsg + "------\n"
			abort errmsg
		end

	end

end