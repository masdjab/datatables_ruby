# Pluk, written by Heryudi Praja (mr_orche@yahoo.com)
# future improvements plan:
# - query can output an array instead of object

require 'mysql2'

module Pluk
  Version   = "1.0.0.5"
  BuildDate = "190407a"
  
  class SQLValue
    def initialize(value)
      @value = value
    end
    def sql_value_syntax
      if @value.nil?
        "NULL"
      elsif @value.is_a?(Time)
        "\"#{@value.strftime("%Y-%m-%d %H:%M:%S")}\""
      else
        "\"#{@value}\""
      end
    end
  end
  
  
  class SQLField
    attr_reader :table, :name, :type, :null, :key, :default, :extra
    
    def initialize(table, name, type, null, key, default, extra)
      @table    = table
      @name     = name
      @type     = type
      @null     = null
      @key      = key
      @default  = default
      @extra    = extra
    end
    def sql_value_syntax
      "`#{@table}`.`#{@name}`"
    end
  end
  
  
  class ConnectionPool
    attr_accessor :max_connections, :max_retries, :retry_interval
    
    def initialize(options)
      if options.is_a?(Hash)
        @options = options
        @max_connections = 10
        @max_timeout = 10
        @retry_interval = 0.5
        @free_conns = []
        @used_count = 0
        @last_id = nil
      else
        raise "Parameter 'options' for #{self.class}.new must be a Hash."
      end
    end
    def host
      @options[:host]
    end
    def user
      @options[:user]
    end
    def password
      @options[:password]
    end
    def database
      @options[:database]
    end
    def used_count
      @used_count
    end
    def free_count
      @free_conns.count
    end
    def total_conns
      used_count + free_count
    end
    def request_conn
      drop = nil
      conn = nil
      atts = 0
      
      if conn.nil?
        if !@free_conns.empty?
          tc = @free_conns.shift
          if tc.ping
            conn = tc if tc.ping
          else
            @free_conns.clear
          end
        end
      end
      
      if conn.nil?
        t1 = Time.new
        et = t1 + @max_timeout
        
        loop do
          begin
            atts += 1
            tc = Mysql2::Client.new(@options) if @free_conns.empty?
            if tc.ping
              conn = tc
              break
            end
          rescue
            if drop .nil?
              drop = true
            end
            sleep @retry_interval
          end
          
          break if Time.new > et
        end
        
        t2 = Time.new
        
        if conn.nil?
          raise "Get mysql connection pool failed after #{t2 - t1} secs."
        end
      end
      
      conn
    end
    def with_engine
      if used_count >= @max_connections
        raise "Maximum connection pools (#{@max_connections}) reached."
      else
        if conn = request_conn
          @used_count += 1
          yield conn
          @free_conns << conn
          @used_count -= 1
        end
      end
    end
    def last_id
      @last_id
    end
    def escape(t)
      rt = nil
      with_engine{|e|rt = e.escape(t)}
      rt
    end
    def query(c)
      rt = nil
      
      with_engine do |e|
        rt = e.query(c)
        
        begin
          @last_id = e.last_id if e.last_id
          @affected_rows = e.affected_rows if e.affected_rows
        rescue
        end
      end
      
      rt
    end
    def create_db(name)
      self.query("CREATE DATABASE `#{name}`")
    end
    def select_db(name)
      with_engine{|e|e.select_db(name)}
    end
    def affected_rows
      @affected_rows
    end
  end
  
  
  class Connection
    def initialize(options = {})
      @mysql = ConnectionPool.new(options)
    end
    def escape(text)
      @mysql.escape(text)
    end
    def query(cmd)
      begin
        @mysql.query(cmd)
      rescue Exception => ex
        puts "Error executing following SQL:\n#{cmd}\n#{ex.message}"
        raise ex.message
      end
    end
    def create_db(name)
      self.query("CREATE DATABASE `#{name}`")
    end
    def select_db(name)
      @mysql.select_db name
    end
    def db_exist?(name)
      !get_database_list(name).empty?
    end
    def affected_rows
      @mysql.affected_rows
    end
    def last_id
      @mysql.last_id
    end
    def info
      @mysql.info
    end
    def get_field_list(table, database = nil)
      tt = database ? "#{database}." : ""
      cc = self.query("SHOW COLUMNS FROM #{tt}#{table}").map{|x|x}
      cc.map do |x|
        SQLField.new(
          table, 
          x["Field"].to_sym, 
          x["Type"].to_sym, 
          x["Null"] == "YES", 
          !x["Key"].empty? ? x["Key"] : nil, 
          x["Default"], 
          !x["Extra"].empty? ? x["Extra"] : nil
        )
      end
    end
    def get_table_list(database = nil)
      tt = database ? " FROM #{database.to_s}" : ""
      self.query("SHOW TABLES#{tt}").map{|x|x[x.keys[0]]}
    end
    def get_database_list(name = "", match_pattern = false)
      cc = !name.empty? ? " LIKE " + (match_pattern ? "'%#{name}%'" : "'#{name}'") : ""
      self.query("SHOW DATABASES#{cc}").map{|x|x["Database"]}
    end
  end
  
  
  class ColumnOrder
    attr_accessor :column, :dir
    
    def initialize(column, dir)
      @column = column
      @dir = dir
    end
    def sql_syntax
      "`#{@column}`#{@dir != :asc ? " DESC" : ""}"
    end
    def self.asc(column)
      self.new(column, :asc)
    end
    def self.desc(column)
      self.new(column, :desc)
    end
  end
  
  
  class QueryParams
    private
    def initialize(conn)
      @connection = conn
    end
    def escape(text)
      Mysql2::Client.escape(text)
    end
    def extract_words(text, separator)
      kk = text.empty? ? [] : text.strip.lines(separator)
      kk.map{|x|x.chomp(separator).strip}.select{|x|!x.empty?}
    end
    def criteria_array(conditions)
      if conditions.is_a?(Hash)
        conditions.keys.map do |x|
          "(`#{x.to_s.gsub(".", "`.`")}` = #{SQLValue.new(conditions[x]).sql_value_syntax})"
        end
      elsif conditions.is_a?(String)
        [conditions]
      end
    end
    def criteria_string(conditions, clause = "")
      rc = clause.strip
      rc = !rc.empty? ? " #{rc} " : ""
      
      if conditions.is_a?(Hash)
        cc = criteria_array(conditions).join(" AND ")
      elsif conditions.is_a?(String)
        cc = conditions
      end
      
      !cc.empty? ? "#{rc}#{cc}" : ""
    end
  end
  
  
  class SelectParams < QueryParams
    attr_accessor \
      :search_fields, :search_keywords, :filter, 
      :having, :order_by, :offset, :max_rows
    
    private
    def initialize(conn, options = {})
      super(conn)
      
      oo                = options.clone
      @search_fields    = oo.delete(:search_fields){|k|""}
      @search_keywords  = oo.delete(:keywords){|k|""}
      @having           = oo.delete(:having){|k|{}}
      @order_by         = oo.delete(:order_by){|k|[]}
      @offset           = oo.delete(:offset){|k|0}
      @offset           = 0 if @offset < 0
      @max_rows         = oo.delete(:max_rows){|k|0}
      @filter           = oo.has_key?(:filter) ? oo[:filter] : oo
    end
    
    public
    def sql_search_fields
      sf = @search_fields.strip
      sk = @search_keywords.strip
      
      if !sf.empty? && !sk.empty?
        ", CONCAT_WS('|', #{sf}) AS keywords"
      else
        ""
      end
    end
    def sql_filter(clause = "WHERE")
      criteria_string(@filter, clause)
    end
    def sql_having(clause = "HAVING")
      cl = clause.strip
      cl = !cl.empty? ? " #{cl} " : ""
      ss = extract_words(@search_keywords, " ").map{|x|"(keywords LIKE '%#{escape(x)}%')"}
      kk = !@search_fields.strip.empty? && !@search_keywords.strip.empty? ? ss : []
      hh = criteria_array(@having) + kk
      !hh.empty? ? cl + hh.join(" AND ") : ""
    end
    def sql_order_by(clause = "ORDER BY")
      cl = clause.strip
      cc = cl == "," ? ", " : " #{cl} "
      ss = @order_by.is_a?(Array) ? @order_by.map{|x|x.sql_syntax}.join(", ") : @order_by.strip
      !ss.empty? ? "#{cc}#{ss}" : ""
    end
    def sql_limit
      lo = @offset
      lc = @max_rows
      (lc > 0) && (lo >= 0) ? " LIMIT #{lo}, #{lc}" : ""
    end
    def self.create(connection, args = nil)
      if args.is_a?(self)
        args
      elsif args.is_a?(Hash)
        self.new(connection, args)
      elsif args.is_a?(String)
        self.new(connection, filter: args)
      elsif args.nil?
        self.new(connection)
      else
        raise "#{self}.create expect argument-2 is a #{self}, Hash, String, or nil."
      end
    end
  end
  
  
  class WriteParams < QueryParams
    attr_accessor :filter, :order_by, :max_rows
    
    private
    def initialize(conn, options = {})
      super(conn)
      
      oo        = options.clone
      @order_by = oo.delete(:order_by){|k|[]}
      @max_rows = oo.delete(:max_rows){|k|0}
      @filter   = oo.has_key?(:filter) ? oo[:filter] : oo
    end
    
    public
    def sql_filter(clause = "WHERE")
      criteria_string(@filter, clause)
    end
    def sql_order_by(clause = "ORDER BY")
      cl = clause.strip
      cc = cl == "," ? ", " : " #{cl} "
      ss = @order_by.is_a?(Array) ? @order_by.map{|x|x.sql_syntax}.join(", ") : @order_by.strip
      !ss.empty? ? "#{cc}#{ob}" : ""
    end
    def sql_limit
      @max_rows > 0 ? " LIMIT #{@max_rows}" : ""
    end
    def self.create(connection, args)
      if args.is_a?(self)
        args
      elsif args.is_a?(Hash)
        self.new(connection, args)
      elsif args.is_a?(String)
        self.new(connection, filter: args)
      elsif args.nil?
        self.new(connection)
      else
        raise "#{self}.create expect argument-2 is a #{self}, Hash, String, or nil."
      end
    end
  end
  
  
  class BaseTableModel
    attr_reader :type, :table_name, :calc_found_rows
    
    def initialize(type, table_name, calc_found_rows = false)
      @type = type
      @table_name = table_name
      @calc_found_rows = calc_found_rows
    end
    def sql_query(query_params)
      ""
    end
  end
  
  
  class TableAdapter
    attr_reader :connection, :table_model, :column_hash, :found_rows
    
    def initialize(connection, table_model)
      @connection = connection
      @table_model = table_model
      @column_hash = @connection.get_field_list(self.table_name).inject({}){|a,b|a[b.name] = b; a}
      @found_rows = -1
    end
    def table_name
      @table_model.table_name
    end
    def create_select_params(args = nil)
      SelectParams.create(@connection, args)
    end
    def create_update_params(args = nil)
      WriteParams.create(@connection, args)
    end
    def count
      @connection.query("SELECT COUNT(*) AS items_count FROM `#{table_name}`").first["items_count"]
    end
    def empty?
      self.count == 0
    end
    def all(args = nil)
      if !(qp = create_select_params(args)).nil?
        list = 
          @connection.query(@table_model.select_query(qp)).map do |r|
            tmp = @table_model.type.new
            r.keys.each do |k|
              mm = :"#{k}="
              tmp.__send__(mm, r[k]) if tmp.respond_to?(mm)
            end
            tmp
          end
        
        if @table_model.calc_found_rows
          @found_rows = @connection.query("SELECT FOUND_ROWS() AS count").first["count"]
        end
        
        list
      else
        raise "Parameter for #{self.class}.all must be a SelectParams, String, or nil. #{args.class} given."
      end
    end
    def first(args = nil)
      if !(qp = create_select_params(args)).nil?
        qp.max_rows = 1
        rr = all(qp)
        !rr.empty? ? rr[0] : nil
      end
    end
    def load(filter)
      if !(qp = create_select_params(filter)).nil?
        first(qp)
      end
    end
    def load_to(target, args)
      if !(qp = create_select_params(args)).nil?
        oo = load(qp)
        @column_hash.keys.each do |k|
          rr = :"#{k}"
          ww = :"#{k}="
          target.__send__(ww, oo.__send__(rr))
        end
      end
    end
    def exist?(filter)
      if !(qp = create_select_params(filter: filter)).nil?
        qp.max_rows = 1
        !all(qp).empty?
      end
    end
    def insert(instance)
      fld = ""
      val = ""
      
      @column_hash.keys.each do |k|
        fld = fld + (!fld.empty? ? ", " : "") + "`#{k}`"
        val = val + (!val.empty? ? ", " : "") + SQLValue.new(instance.__send__(k.to_sym)).sql_value_syntax
      end
      
      @connection.query("INSERT INTO `#{table_name}`(#{fld}) VALUES(#{val})")
    end
    def update(instance, args = nil, limit = 0)
      sc = ""
      qp = args ? create_update_params(args) : WriteParams.new
      
      @column_hash.keys.each do |k|
        sc = sc + (!sc.empty? ? ", " : "") + "`#{k}` = #{SQLValue.new(instance.__send__(k.to_sym)).sql_value_syntax}"
      end
      
      qs = 
        "UPDATE `#{table_name}` SET #{sc}" \
        "#{qp.sql_filter}#{limit > 0 ? " LIMIT #{limit}" : ""}"
      
      @connection.query(qs)
    end
    def delete(args = nil, limit = 0)
      qp = create_update_params(args)
      qs = 
        "DELETE FROM `#{table_name}`" \
        + qp.sql_filter + (limit > 0 ? " LIMIT #{limit}" : "")
      @connection.query(qs)
    end
    def truncate
      @connection.query "TRUNCATE `#{table_name}`"
    end
  end
end
