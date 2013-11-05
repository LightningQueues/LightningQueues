require 'fuburake'

@solution = FubuRake::Solution.new do |sln|
	sln.assembly_info = {
		:product_name => "LightningQueues",
		:copyright => 'Copyright 2013 Corey Kaylor.',
		:description => 'Fast persistent queues for .NET'
	}

	sln.ripple_enabled = true
end

require_relative 'ILRepack'

desc "Merge dotnetzip assembly into Bottles projects"
task :ilrepack => [:compile] do
  merge_esent("src/LightningQueues/bin/#{@solution.compilemode}", 'LightningQueues.dll')
end

Rake::Task[:unit_test].enhance [:ilrepack]

def merge_esent(dir, assembly)
	output = File.join(dir, assembly)
	packer = ILRepack.new :out => output, :lib => dir
	packer.merge :lib => dir, :refs => [assembly, 'Esent.Interop.dll'], :clrversion => @solution.options[:clrversion]
end
