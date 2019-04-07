require 'sinatra/base'
require 'mysql2'
require 'json'
require './pluk'
require './ssp_class'

module DataTables
  class Agency
    attr_accessor :rowid, :nm_instansi, :center_y, :center_x, :header
  end
  
  
  class AgencyModel < Pluk::BaseTableModel
    def initialize
      super(Agency, "ms_instansi", true)
    end
    def select_query(qp)
      qp.search_fields = "nm_instansi, header"
      
<<EOS
SELECT SQL_CALC_FOUND_ROWS rowid, nm_instansi, center_y, center_x, header#{qp.sql_search_fields} 
FROM ms_instansi#{qp.sql_filter}#{qp.sql_having}#{qp.sql_order_by}#{qp.sql_limit}
EOS
    end
  end
end


module DataTables
  class App < Sinatra::Base
    Conn = Pluk::Connection.new(host: "localhost", username: "root", password: "", database: "moni_v20")
    Agencies = Pluk::TableAdapter.new(Conn, AgencyModel.new)
    
    before do
      response.headers['Access-Control-Allow-Origin'] = '*'
    end
    
    get '/coba' do
      # http://localhost:4567/coba?draw=1&order[0][column]=0&order[0][dir]=desc&start=0&length=10&search[value]=&search[regex]=false
      ServerSide.handle(Agencies, ["nm_instansi", "center_y", "center_x", "header"], params)
    end
  end
end
