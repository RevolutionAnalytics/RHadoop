require 'net/ftp'

class Ftp
	def self.upload(attributes)
		server = attributes.fetch(:server)
		username = attributes.fetch(:username)
		password = attributes.fetch(:password)
		files = attributes.fetch(:files)
		target_dir = attributes.fetch(:target_dir, nil)
		remove_prefix = attributes.fetch(:remove_prefix, '')
		
		return if files.empty?
		
		files.map! do |source|    
			if remove_prefix
				target = source.pathmap("%{^#{remove_prefix},#{target_dir}}p") 
			else
				target = source.pathmap("#{target_dir}%s%p")
			end
			
			{:source => source, :target => target}
		end
				
		files.each do |file|    
			puts file[:source] + " => " + file[:target]
		end
		
		Net::FTP.open(server, username, password) do |ftp|
			if target_dir
					puts "Creating dir #{target_dir}" 
					ftp.mkdir target_dir
			end
			
			files.each do |file|    
				s = file[:source]
				t = file[:target]
				
				begin
					puts "Creating dir #{t}" if File.directory?(s)
					ftp.mkdir t if File.directory?(s)
				rescue 
					puts $!
				end
				
				begin
					puts "Copying #{s} -> #{t}" unless File.directory?(s)
					ftp.putbinaryfile(s, t) unless File.directory?(s)
				rescue 
					puts $!
				end
			end
		end
	end
end