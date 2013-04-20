require "logger"
require "bigdecimal/util"
require "yaml"
require "erb"

class DBFactoryClass

  @cfg
  @log
  @definition
  @case
  @scn

  def initialize()
    l_erb  = ERB.new(File.read("#{ROOT}/config.yml"))
    @cfg = YAML.load(l_erb.result(binding).to_s)

    @log = Logger.new("#{@cfg['LOG']['path']}/#{@cfg['LOG']['filename']}")
    @log.info  "================ Initializing DBFactory ================"
    @log.debug "[ DBFactory::initialize(...) ]"

    @log.datetime_format = @cfg['LOG']['date format']
    @log.level = @cfg['LOG']['level']

    return self
  end

  def load(definition_file)
    @log.debug "[ DBFactory::load(...) ]"
    @log.debug " > definition_file = <#{definition_file}>"

    begin
      @log.debug "Reading constants ..."
      constants  = YAML.load_file(definition_file)['CONSTANTS'].reduce(Hash.new, :merge)
    rescue Exception => e
      @log.error "[ DBFactory::load(...) ] ERROR: <#{e.to_s}>"
      @log.error "Error occurred during load of definition file <#{definition_file}>"
      raise "Check defintion file <#{definition_file}>: #{e.to_s}"
    end

    @log.debug "Injecting constants ..."
    l_erb      = ERB.new(File.read(definition_file).strip.gsub(/^\s*#.*/,'').gsub(/\n^\s*$/,''))

    @log.debug "Parsing defintion file ..."
    @definition = YAML.load(l_erb.result(binding).to_s)
    @case = 'COMMON'

#    puts @cfg['GENERAL']['supported_file_format_versions'].to_s
#    puts @definition['FORMAT VERSION']
#    puts @definition.inspect
    raise "Unsupported version of YAML file. Supported file versions: #{@cfg['GENERAL']['supported format'].to_s}" unless @cfg['GENERAL']['supported
format'].include?(@definition['%FORMAT'])
  end

  def setup(casename = 'COMMON')
    @log.debug "[ DBFactory::setup(...) ]"
    @log.debug " > casename = <#{casename}>"

    if casename != 'COMMON'
      setup()
    else
      @scn = plsql.select("select current_scn from v$database")[0][:current_scn]
      @log.info("CURRENT SCN from DB = <#{@scn}>")
    end

    @case = casename

    if @definition[@case].include?('cleanup')
      @definition[@case]['cleanup']['tables'].each { |t|
        cleanup_table(t)
      }
    end

#    puts @definition[@case] unless casename == 'COMMON'
    @definition[@case]['setup']['tables'].each { |t|
      prepare_table(t)
    }

  end

  def check_postconditions(stage = nil)
    @log.debug "[ DBFactory::check_postconditions(...) ]"
    l_result = true
    raise "Use stage name (must contain string 'stage-[stage name]'" unless stage.nil? || stage.include?('stage')
#    l_stage = "stage-#{stage}" unless stage == nil
    l_diff_file = @cfg['DIFF']['filename_tpl'].gsub('#CASE#',@case)
    File.delete(l_diff_file) unless not File.exist?(l_diff_file)
    @definition[@case]['postconditions']['tables'].each { |t|
      if stage.nil? || t.values[0].include?(stage)
        l_result_tmp = check_postconditions_for_table(t, stage)
        l_result = l_result_tmp unless l_result == false
      end
    }

    if l_result
      @log.info("> DBFactory::evaluate(...) ... PASS")
    else
      @log.warn("> DBFactory::evaluate(...) ... FAIL")
    end

    return l_result
  end

  def flashback()
    @log.debug "[ DBFactory::flashback(...) ]"
    l_result = true
    @definition[@case]['flashback']['tables'].each { |t|
        table_owner = t.split('.')[0].upcase
        table_name = t.split('.')[1].upcase
        @log.debug " > table_owner = <#{table_owner}>"
        @log.debug " > table_name  = <#{table_name}>"
        @log.debug " > scn        = <#{@scn}>"
        l_result = plsql.flashback_table(table_owner, table_name, @scn) unless l_result == false
        @log.debug "...DONE"
    }
    @scn = nil

    return l_result
  end

  private

  def execute_sql(sql_command)
    @log.debug "[ DBFactory::sql_command(...) ]"
    @log.debug " > sql_command = <#{sql_command}>"

    begin
      eval("plsql.execute('#{sql_command}')")
    rescue Exception => e
      @log.error "[ DBFactory::execute_sql(...) ] ERROR: <#{e.to_s}>"
      @log.error "Error occurred during execution of SQL command <#{sql_command}>"
      raise
    end
  end

  def cleanup_table(table)
    @log.debug "[ DBFactory::cleanup_table(...) ]"

    begin
#      l_delete = @definition[@case]['cleanup']['tables'][tablename]['delete']['condition']
      l_delete = table.values[0]['delete']['condition']
      @log.debug "Using delete condition: <#{l_delete}>"
      l_object_id  = plsql.find_table(table.keys[0])
      l_object_id.delete("WHERE #{l_delete}")
    rescue Exception => e
      @log.error "[ DBFactory::cleanup(...) ] ERROR: <#{e.to_s}>"
#      @log.warn "Error occured during cleanup procedure on table <#{tablename}> failed in <#{@case}> block"
#      raise "Error occured during cleanup procedutre on table <#{tablename}> failed in <#{@case}> block"
      raise "Error occured during cleanup procedutre"
    end
  end

  def delete(table)
    @log.debug "[ DBFactory::delete(...) ]"

    begin
      if table.values[0].include?('delete')
        l_delete = table.values[0]['delete']['condition']
      else
        l_delete = '1=0'
      end
      l_object_id  = plsql.find_table(table.keys[0])
      l_object_id.delete("WHERE #{l_delete}") unless l_delete.nil?
    rescue Exception => e
      @log.error " >> ERROR: <#{e.to_s}>"
      raise "Failed on delete statement."
    end
  end

  def insert(table)
    @log.debug "[ DBFactory::insert(...) ]"

    begin
      tablename = table.keys[0]
      l_data = data(table)
      l_object_id  = plsql.find_table(tablename)
      l_object_id.insert(l_data)
      @log.info("#{l_data.size} records were inserted into <#{tablename}> table")
    rescue Exception => e
      raise "[ DBFactory::insert ]: Failed insert data. #{e.to_s}"
    end
  end

  def sql_statement(table)
    @log.debug "[ DBFactory::sql_statement(...) ]"

    begin
      l_statement = table.values[0]['sql statement']
      execute_sql(l_statement) unless l_statement.nil?
    rescue Exception => e
      @log.error " >> ERROR: <#{e.to_s}>"
      raise "Failed on SQL statement"
    end
  end

  def other_column_data(table)
    @log.debug "[ DBFactory::other_column_data(...) ]"

    tablename = table.keys[0]
    begin
      l_object_id  = plsql.find_table(tablename)
      l_other_column_data = l_object_id.first || {}
    rescue Exception => e
      raise "[ DBFactory::other_column_data ]: #{e.to_s}"
    end

    return l_other_column_data
  end

  def defaults(table)
    @log.debug "[ DBFactory::defaults(...) ]"

    l_other_column_data = other_column_data(table)

    tablename = table.keys[0]
    l_defaults1 = {}
    begin
      @definition['DEFAULTS']['tables'].each { |t|
        if t.keys[0] == table.keys[0]
          l_defaults1 = t.values[0] || {}
          break
        end
      }
#      if l_defaults1 != nil
#        @log.debug "Loading of defaults for table <#{tablename}> was succesfull in <DEFAULTS> block"
#      else
#        l_defaults1 = {}
#      end
    rescue Exception => e
      @log.error "Error: <#{e.to_s}>"
      @log.warn "Loading of defaults for table <#{tablename}> failed in <DEFAULTS> block"
    end

    l_defaults2 = {}
    begin
      l_defaults2 = table.values[0]['defaults'] || {}
#      if l_defaults2 != nil
#        @log.debug "Loading of defaults for table <#{tablename}> was succesfull in <#{@case}> block"
#      else
#        l_defaults2 = {}
#      end
    rescue Exception => e
      @log.error "[ DBFactory::defaults(...) ] ERROR: <#{e.to_s}>"
      @log.warn "Loading of defaults for table <#{tablename}> failed in <#{@case}> block"
    end

    return(l_other_column_data.merge(l_defaults1).merge(l_defaults2))
  end

  def data(table)
    @log.debug "[ DBFactory::data(...) ]"

    table_def = table[table.keys[0]]
    begin
      l_data = []
      l_defaults = defaults(table)
      l_data_tmp = table_def['data']

      case
        when l_data_tmp.nil?
          l_data = []
        when l_data_tmp[0].instance_of?(Hash)
          l_data_tmp.collect! { |values|
            values = l_defaults.merge(values)
          }
          l_data = l_data_tmp
        when l_data_tmp[0].instance_of?(Array)
          l_columns = table_def['columns']
          l_data_tmp.each_index { |i|
            l_data[i] = {}
            l_data_tmp[i].each_index { |k|
              l_data[i] = l_data[i].merge({l_columns[k] => l_data_tmp[i][k]})
            }
          }
          l_data.collect! { |values|
            values = l_defaults.merge(values)
          }
      end
    rescue Exception => e
      raise "[ DBFactory::data ]: Failed loading of data. #{e.to_s}"
    end

    # convert FLOAT - otherwise OCI error will be rasied
    l_data.each_index {|i|
      l_data[i].each { |k,v|
        l_data[i][k] = v.instance_of?(Float) ? v.to_d : v
      }
    }
    return l_data
  end

  def prepare_table(table)
    @log.debug "[ DBFactory::prepare_table(...) ]"
    @log.debug ">> table: #{table.keys[0]}"

    begin
      tablename = table.keys[0]
      l_object_id  = plsql.find_table(tablename)
    rescue Exception => e
      raise "Failed to find table <#{tablename}> in DB"
    end

    # delete data according to delete condition in file
    delete(table)

    # execute SQL statement
    sql_statement(table)

    # insert defined data
    insert(table)

  end

  def expected_columns(table)
    l_data_tmp = table.values[0]['expected data']
    case
      when l_data_tmp[0].instance_of?(Hash)
        l_expected_columns = l_data_tmp[0].keys
      when l_data_tmp[0].instance_of?(Array)
        l_expected_columns = table.values[0]['columns']
    end
    l_expected_columns
  end

  def expected_data(table, stage = nil)
    @log.debug "[ DBFactory::expected_data(...) ]"
    @log.debug " > stage    = <#{stage}>"

    stage ||= 'expected data'

    l_data = []
    begin
      l_data_tmp = table.values[0][stage]
      case
        when l_data_tmp[0].instance_of?(Hash)
          l_data = l_data_tmp
        when l_data_tmp[0].instance_of?(Array)
          l_columns = table.values[0]['columns']
#          l_columns = expected_columns(tablename)
          l_data_tmp.each_index { |i|
            l_data[i] = {}
            l_data_tmp[i].each_index { |k|
              l_data[i] = l_data[i].merge({l_columns[k] => l_data_tmp[i][k]})
            }
          }
      end
    rescue Exception => e
      @log.error "[ DBFactory::expected_data(...) ] ERROR: <#{e.to_s}>"
      raise "Failed to get expected data."
    end

#    @log.info("There are currently #{l_data.size} records expected in <#{tablename}> table")
    return l_data
  end

  def actual_data(table)
    @log.debug "[ DBFactory::actual_data(...) ]"

    l_object_id  = plsql.find_table(table.keys[0])
    begin
      l_filter = table.values[0]['filter'] || '1=1'
      @log.debug("filter: <#{l_filter}>")
      l_data = l_object_id.all("WHERE #{l_filter}")
      l_columns = expected_columns(table)
      @log.debug "columns: <#{l_columns}>"
      l_data.each_index { |i|
        l_data[i].delete_if { |k,v| not l_columns.include?(k) }
      }
      l_data.each_index { |i|
        l_data[i].each { |k,v|
          if v.instance_of?(String)
            l_data[i][k] = v.rstrip
          end
          if v.instance_of?(BigDecimal)
            l_data[i][k] = v.to_f
          end
        }
      }
    rescue Exception => e
      @log.error "[ DBFactory::actual_data(...) ] ERROR: <#{e.to_s}>"
#      @log.warn "Loading actual data for table <#{tablename}> failed in <#{@case}> block"
      raise "Failed to load actual data from DB." #or table <#{tablename}> failed in <#{@case}> block"
    end

#    @log.info("There are currently #{l_data.size} records in <#{tablename}> table")
    return l_data
  end

  def pk_data_split(data, pk)
    l_split_data = []
    data.each { |values|
      l_pk = {}
      pk.each { |k|
        l_pk[k] = values[k]
        values.delete(k)
      }
      l_split_data.push({"PK" => l_pk, "DATA" => values})
    }
#    puts l_split_data.inspect
    return l_split_data
  end

  def get_all_pk(data1, data2)
    l_all_pks = []
    data1.each { |v|
      l_tmp = {"PK" => v["PK"]}
      l_all_pks.push(l_tmp) unless l_all_pks.include?(l_tmp)
    }
    data2.each { |v|
      l_tmp = {"PK" => v["PK"]}
      l_all_pks.push(l_tmp) unless l_all_pks.include?(l_tmp)
    }
    return l_all_pks #.sort_by { |k| k["PK"] }
  end

  def check_postconditions_for_table(table, stage = nil)
    @log.debug "[ DBFactory::check_postconditions_for_table(...) ]"
    @log.debug " > stage    = <#{stage}>"

    tablename = table.keys[0]
    l_data = nil
    begin
      l_expected_data = expected_data(table, stage)
      l_actual_data  = actual_data(table)

      if l_expected_data == l_actual_data
        @log.info("> DBFactory::compare_table(#{tablename}, #{stage}) ... PASS")
        return true
      end

      @log.warn "Seems that some differences found when comparing actual and expected data in table <#{tablename}>"

#      l_pk = @definition[@case]['postconditions']['tables'][tablename]['connect keys']
      l_pk = table.values[0]['connect keys']
      @log.debug "connect keys: <#{l_pk}>"
      if l_pk == nil
        raise "Missing <connect keys> definition"
      end

      te = pk_data_split(l_expected_data, l_pk) #.sort_by { |v| v.values_at(0) }
      ta = pk_data_split(l_actual_data, l_pk) #.sort_by { |v| v.values_at(0) }
#      puts te.inspect
#      puts ta.inspect
      d = []
      get_all_pk(te, ta).each { |k|
        vei = te.index { |v| v["PK"] == k["PK"] }
        if vei != nil
          ve = te[vei]
          te.delete_at(vei)
        else
          ve = {"PK" => {}, "DATA" => {}}
        end

#        ve = te.detect { |v| v["PK"] == k["PK"] }
#        if ve == nil
#          ve = {"PK" => {}, "DATA" => {}}
#        end
        vai = ta.index { |v| v["PK"] == k["PK"] }
        if vai != nil
          va = ta[vai]
          ta.delete_at(vai)
        else
          va = {"PK" => {}, "DATA" => {}}
        end
#        va = ta.detect { |v| v["PK"] == k["PK"] }
#        if va == nil
#          va = {"PK" => {}, "DATA" => {}}
#        end
        diff = va["DATA"].diff(ve["DATA"])
#        puts diff.inspect
#        d.push({"PK" => k["PK"], "DATA" => diff}) unless diff == {}
        d.push(k["PK"].merge(diff)) unless diff == {}
      }

      if d.empty?
#        puts "l_expected_datasize = <#{l_expected_data.size}>"
#        puts l_expected_data.inspect
#        puts "l_actual_data.size = <#{l_actual_data.size}>"
        @log.info("> DBFactory::compare_table(#{tablename}, #{stage}) ... PASS")
        return true
#      return false
      else
        if stage.nil?
          d = {"#{@case}" => {"#{tablename}" => d}}.to_yaml
        else
          d = {"#{@case}" => {"#{stage}" => {"#{tablename}" => d}}}.to_yaml
        end
        l_diff_file = @cfg['DIFF']['filename_tpl'].gsub('#CASE#',@case)
        @log.info "Writing differences for table <#{tablename}> for <#{@case}> block into <#{l_diff_file}>"
        l_fw = File.open(l_diff_file, "a")
        l_fw.write(d)
        l_fw.close
      end

    rescue Exception => e
      @log.error "[ DBFactory::compare_table(...) ] ERROR: <#{e.to_s}>"
      @log.warn "Comparison of actual and expected data for table <#{tablename}> failed in <#{@case}> block"
      raise "Error occurred during comparison if data for table <#{tablename}> in <#{@case}> block"
    end

    @log.warn("> DBFactory::compare_table(#{tablename}, #{stage}) ... FAIL")

    return false
  end

end

module DBFactory

  LOG      = "#{ROOT}/log"
  DEFAULTS = "#{ROOT}/defaults"

  @instance

  def self.load(*args)
    @instance = DBFactoryClass.new
    @instance.load(*args)
  end

  def self.setup(*args)
    @instance.setup(*args)
  end

  def self.evaluate(*args)
    @instance.check_postconditions(*args)
  end

  def self.flashback(*args)
    @instance.flashback(*args)
  end

end
