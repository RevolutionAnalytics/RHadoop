module Yamler
	class Template
		def optional_yaml(path)
			path = File.extname(path) == '' ? "#{path}.yml" : path
			require_yaml(path) if File.exists?(path)
		end
	end
end