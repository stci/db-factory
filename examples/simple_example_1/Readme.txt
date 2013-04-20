Simple Example 1 Readme
-----------------------

1) run "create_objects.sql" under e.g. SCOTT schema (which normaly should be schema where your application and data is)

2) create user TEST_USER with there privileges
  grant connect to TEST_USER;
  grant delete_catalog_role to TEST_USER;
  grant execute_catalog_role to TEST_USER;
  grant select_catalog_role to TEST_USER;
  grant alter any table to TEST_USER;
  grant create role to TEST_USER;
  grant delete any table to TEST_USER;
  grant execute any procedure to TEST_USER;
  grant flashback any table to TEST_USER;
  grant insert any table to TEST_USER;
  grant select any dictionary to TEST_USER;
  grant select any sequence to TEST_USER;
  grant select any table to TEST_USER;
  grant update any table to TEST_USER;

3) execute in command line:
  rspec simple_example_1_spec.rb
