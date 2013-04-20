require File.expand_path(File.dirname(__FILE__) + '/spec_helper')

describe "Simple example 1" do

  DBFactory.load('simple_example_1.yml')

  before(:all) do
    DBFactory.setup('SALARY')
  end

  it "should update salary of employees according coeficient defined on department" do

    plsql.scott.salary_update(1, 2).should == 0
    DBFactory.evaluate('stage-1').should == true

    plsql.scott.salary_update(3, 3).should == 0
    DBFactory.evaluate().should == true

  end

end
