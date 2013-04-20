require "rspec"
require "ruby-plsql"
require "db-factory"

$LOAD_PATH << File.dirname(__FILE__) + '/factories'

# Establish connection to database where tests will be performed.
# Change according to your needs.
DATABASE_USER    = "TEST_USER"
DATABASE_PASSWORD = "TEST_USER"
DATABASE_NAME    = "DEV_DB" # TNS

plsql.connect! DATABASE_USER, DATABASE_PASSWORD, DATABASE_NAME

# Set autocommit to false so that automatic commits after each statement are _not_ performed
plsql.connection.autocommit = false
# reduce network traffic in case of large resultsets
plsql.connection.prefetch_rows = 100
# uncomment to log DBMS_OUTPUT to standard output
# plsql.dbms_output_stream = STDOUT

# Do logoff when exiting to ensure that session temporary tables
# (used when calling procedures with table types defined in packages)
at_exit do
  plsql.logoff
end

RSpec.configure do |config|
  config.before(:each) do |test|
    plsql.savepoint "before_each"

    plsql.sys.dbms_application_info.set_module(:module_name => test_filename, :action_name => test.example.metadata[:example_group][:full_description]);
  end
  config.after(:each) do
    # Always perform rollback to savepoint after each test
    plsql.rollback_to "before_each"
  end
  config.after(:all) do
    # Always perform rollback after each describe block
    plsql.rollback
  end
end

# require all helper methods which are located in any helpers subdirectories
Dir[File.dirname(__FILE__) + '/**/helpers/*.rb'].each {|f| require f}

# require all factory modules which are located in any factories subdirectories
Dir[File.dirname(__FILE__) + '/**/factories/*.rb'].each {|f| require f}

# Add source directory to load path where PL/SQL example procedures are defined.
# It is not required if PL/SQL procedures are already loaded in test database in some other way.
$:.push File.dirname(__FILE__) + '/../../source'
