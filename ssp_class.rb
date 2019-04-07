require 'mysql2'
require 'json'
require './pluk'

module DataTables
  class ServerSide
    def initialize(table_adapter, columns)
      @table_adapter = table_adapter
      @columns = columns
    end
    def result(request)
      list = 
        @table_adapter.all({
          :keywords => request["search"]["regex"] == "false" ? request["search"]["value"] : "", 
          :order_by => request["order"].values.map{|x|Pluk::ColumnOrder.new(@columns[x["column"].to_i].to_sym, x["dir"].to_sym)}, 
          :offset   => request["start"].to_i, 
          :max_rows => request["length"].to_i
        })
      
      match_rows = list.map{|x|@columns.map{|c|x.__send__(c.to_sym)}}
      found_rows = @table_adapter.found_rows
      rows_count = @table_adapter.count
      
      response = 
        {
          "draw" => request["draw"], 
          "recordsTotal" => rows_count, 
          "recordsFiltered" => found_rows, 
          "data" => match_rows, 
          "error" => ""
        }
      
      JSON.pretty_generate(response)
    end
    def self.handle(table_adapter, columns, request)
      self.new(table_adapter, columns).result(request)
    end
  end
end
