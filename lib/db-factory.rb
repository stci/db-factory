ROOT = File.expand_path(File.dirname(__FILE__))

%w(db_factory version helpers).each do |file|
  require "db_factory/#{file}"
end
