Gem::Specification.new do |s|
  s.name        = 'db-factory'
  s.version    = '0.1.0'
  s.date        = '2013-04-16'
  s.summary    = "Prepare and compare test data for PL/SQL unit testing"
  s.description = "db-factory uses definition files with YAML/ERB syntax to define test data (preconditions) for unit PL/SQL testing and can compare actual data with defined expected data."
  s.authors    = ["Stefan Cibiri"]
  s.email      = 'stefancibiri@yahoo.com'
  s.files      = ["History.txt", "VERSION", "db-factory.gemspec", "examples/simple_example_1/Readme.txt", "examples/simple_example_1/create_objects.sql", "examples/simple_example_1/simple_example_1.yml", "examples/simple_example_1/simple_example_1_spec.rb", "examples/simple_example_1/spec_helper.rb", "lib/config.yml", "lib/db-factory.rb", "lib/db_factory/db_factory.rb", "lib/db_factory/helpers.rb", "lib/db_factory/version.rb"]
  s.homepage    = "https://github.com/stefancibiri/db-factory"
  s.add_dependency('ruby-plsql')
end
