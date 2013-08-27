require 'fuburake'

begin
  require 'bundler/setup'
rescue LoadError
#  puts 'Bundler and all the gems need to be installed prior to running this rake script. Installing...'
#  system("gem install bundler --source http://rubygems.org")
#  sh 'bundle install'
#  system("bundle exec rake", *ARGV)
#  exit 0
end


@solution = FubuRake::Solution.new do |sln|
	sln.assembly_info = {
		:product_name => "LightningQueues",
		:copyright => 'Copyright 2013 Corey Kaylor.',
		:description => 'Fast persistent queues for .NET'
	}

	sln.ripple_enabled = true
end
