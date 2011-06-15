class MSDeploy
	def self.run(attributes)
		tool = attributes.fetch(:tool)
		tee_tool = attributes.fetch(:tee_tool, "tee.exe".in(tool.dirname))
		logFile = attributes.fetch(:log_file, "msdeploy.log")
		
		attributes.reject! do |key, value|
			key == :tool || key == :log_file
		end
		
		switches = generate_switches(attributes)
		
		msdeploy = tool.to_absolute
		tee = tee_tool.to_absolute
		
		sh "#{msdeploy.escape} #{switches} 2>&1 | #{tee.escape} -a #{logFile}" do
			doc = File.read(logFile)
			errors = doc.scan(/(\n|\s)+(error|exception|fehler)/i)
			
			if errors.any?
				message = "\nLog string indicating the deployment error: #{errors.first[1]} ...and #{errors.nitems - 1} more"
				puts message
				raise "\nDeployment errors occurred. Please review #{logFile}." + message
			else
				puts "\nDeployment successful."
			end
		end
	end
	
	def self.generate_switches(attributes)
		switches = ""
		
		attributes.each do |switch, value|
			if value.kind_of? Array
				switches += value.collect { |element|
					generate_switches({ switch => element })
				}.join
				
				next
			end
			
			switches += "-#{switch}#{":#{value}" unless value.kind_of? Enumerable or value.kind_of? TrueClass or value.kind_of? FalseClass}" if value		
			
			if value.kind_of? Enumerable
				switches += ":"
				switches += value.collect { |key, value|
					"#{key}#{"=#{value}" unless value.kind_of? TrueClass or value.kind_of? FalseClass}" if value
				}.join ","
			end
			
			switches += " "
		end
		
		switches
	end
end
