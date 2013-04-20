module PLSQL
  class Schema
    # Returns object ID of table object. Examples:
    #  plsql.find_table('scott.my_table')
    #  plsql.find_table('scott', 'my_table')
    def find_table(*args)
      if args.size == 1
        table_owner = args[0].split('.')[0].upcase
        table_name = args[0].split('.')[1].upcase
      else
        table_name = args[0]
        table_owner = args[1]
      end
      find_database_object(table_name, table_owner)
    end
  end
end

class Hash
  # Returns difference between 2 hashes (actual values and expecyted values)
  # as Hash with "expected" and "actual" keys
  def diff(other)
    l_keys = self.keys.concat(other.keys).uniq
    l_keys.inject({}) do |memo, key|
      l_expected = other[key] rescue {}
      unless self[key] == l_expected
        memo[key] = {"actual" => self[key], "expected" => l_expected}
      end
      memo
    end
  end
end
