class SogousController < ApplicationController
  # before_action :set_sogou, only: [:show, :edit, :update, :destroy]
  helper_method :account, :nextrecord
  before_action :tmp
  
  require 'savon'
  require "azure"
  
  require 'rubygems'
  require 'mongo'
  require 'zlib'
  
  
  def test
    
  end
  
  
  
  
  
  def checkreport
  
      @logger.info "checkreport sogou start"
      
      @current_not_done_report = @db[:miss_report].find({ "$and" => [{:status => 1},{:worker => @port.to_i},{:network_type => "sogou"}] })
      @db.close
      
      if @current_not_done_report.count.to_i > 0
          data = {:message => "check report sogou running", :status => "true"}
          return render :json => data, :status => :ok  
      end
    
      @not_done_report = @db[:miss_report].find({ "$and" => [{:status => 0},{:worker => @port.to_i},{:network_type => "sogou"}] }).limit(1)
      @db.close
      
      
      
      if @not_done_report.count.to_i > 0
          @not_done_report.no_cursor_timeout.each do |not_done_report_d|
            
              begin
                
              
                  @db[:miss_report].find('_id' => not_done_report_d["_id"], 'status' => 0 ).update_one('$set'=> { 'status' => 1, 'update_date' => @now })
                  @db.close
                  
                  id = not_done_report_d["network_id"]
                  report_day = not_done_report_d["report_date"]
                  
                  @logger.info "checkreport sogou running "+id.to_s+" - "+report_day.to_s
                  
                  days = @today.to_date - report_day.to_date
                  
                  url = "http://china.adeqo.com:"+@port.to_s+"/sogous/report?day="+days.to_i.to_s+"&id="+id.to_s
                  # res = Net::HTTP.get_response(URI(url))
                  
                  link = URI.parse(url)
                  http = Net::HTTP.new(link.host, link.port)
                  
                  http.read_timeout = 800
                  http.open_timeout = 800
                  res = http.start() {|http|
                    http.get(URI(url))
                  }
    
                  
                  @logger.info "checkreport running sogou report "+id.to_s+" - "+report_day.to_s
                  
                  if res.code.to_i == 200 
                          
                      @db[:miss_report].find('_id' => not_done_report_d["_id"], 'status' => 1 ).delete_one
                      @db.close
                      
                      @logger.info "checkreport done sogous report "+id.to_s+" - "+report_day.to_s
                      
                  else
                      @db[:miss_report].find('_id' => not_done_report_d["_id"], 'status' => 1 ).update_one('$set'=> { 'status' => 0, 'update_date' => @now })
                      @db.close
                      
                      data = {:message => @not_done_report, :status => "false"}
                      return render :json => data, :status => :ok
                  end
              
              rescue Exception
                  @db[:miss_report].find('_id' => not_done_report_d["_id"], 'status' => 1 ).update_one('$set'=> { 'status' => 0, 'update_date' => @now })
                  @db.close
                  
                  data = {:message => @not_done_report, :status => "false"}
                  return render :json => data, :status => :ok
              end
              # data = {:url => url, :upper_url => upper_url, :status => "true"}
              # return render :json => data, :status => :ok
          end
      end
      
      
      data = {:message => @not_done_report, :status => "true"}
      return render :json => data, :status => :ok
  
  end
  
  
  def tmp
    @tmp = "/datadrive"    
  end
    
  
  def drop
    return render :nothing => true
  end
  
  
  def sogou_api(username,password,token,api_string)            
    @sogou_api = Savon.client(
      wsdl: "http://api.agent.sogou.com:80/sem/sms/v1/"+api_string+"?wsdl",
      pretty_print_xml: true,
      log: true,
      env_namespace: :soap,
      namespaces: {"xmlns:common" => "http://api.sogou.com/sem/common/v1"},
      soap_header: { 
        "common:AuthHeader" => {
          'common:token' => token,
          'common:username' => username,
          'common:password' => password
        }
      }
    )    
  end
  
  
  
  def redownload(networkid)
    
      @redownload_network = @db[:network].find('type' => 'sogou', 'id' => networkid.to_i)
      @db.close
    
      if @redownload_network.count.to_i == 1
          
          @redownload_network.no_cursor_timeout.each do |doc|
              if doc["tmp_file"] != ""
                  unzip_folder = @tmp+"/"+doc["tmp_file"]
                  if File.directory?(unzip_folder)
                      FileUtils.remove_dir unzip_folder, true
                  end
              end
          end
          
          @db[:network].find(id: networkid.to_i).update_one('$set'=> { 'tmp_file' => "" , 'fileid' => "",'run_time' => 0, 'file_update_1' => 0, 'file_update_2' => 0, 'file_update_3' => 0, 'file_update_4' => 0, 'last_update' => @now, 'worker' => ""})  
          @db.close
      end
  end
  
  
  
  def getsogoufile(username, password, token, fileid ,network_id, tmp_file)
      
      @logger.info "getsogoufile start"
      @acc_file_path = ""
      @tmp_file = ""
      @run_csv = 0
      
      begin
          #if file_id in db, means last time the file is not ready, so reuse the id and check              
          if fileid.to_s == "" && tmp_file.to_s == "" 
            sogou_api(username,password,token,"AccountDownloadService")
            @acc_file = @sogou_api.call(:get_account_file)
            
            @header = @acc_file.header.to_hash
            @logger.info @header 
            
            @logger.info "sa1"
            
            if @header[:res_header][:desc].to_s != 'failure'
                # @return_num =  @header[:res_header][:oprs]
                # @quota =  @header[:res_header][:quota]
                
                @acc_file_body = @acc_file.body.to_hash
                @acc_file_id = @acc_file_body[:get_account_file_response][:account_file_id]
                
            else
                if @header[:res_header][:failures][:code].to_s == '30'
                    @run_csv = 2
                    @logger.info "run csv 2"
                end
                # if cant get file id, stop doing
                @db[:network].find(id: network_id.to_i).update_one('$set'=> { 'quota' => @quota.to_i })
                @db.close
            end    
                
          elsif tmp_file.to_s != ""
            @acc_file_id = tmp_file.to_s
            @run_csv = 1
          else
            @acc_file_id = fileid.to_s   
          end
          
          
            
          if @run_csv == 0 && @acc_file_id.to_s != "" 
              sogou_api(username,password,token,"AccountDownloadService")
              @acc_file_status = @sogou_api.call(:get_account_file_status, message: { accountFileId: @acc_file_id.to_s })
              @header = @acc_file_status.header.to_hash
              
              @logger.info @header
              
              if @header[:res_header][:desc].to_s != 'failure'
                
                  # @return_num =  @header[:res_header][:oprs]
                  @acc_file_status =  @acc_file_status.body.to_hash
                  @acc_file_status = @acc_file_status[:get_account_file_status_response][:is_generated]
                
                  if @acc_file_status.to_i == 1
                      # get csv path
                      @acc_file_path_r = @sogou_api.call(:get_account_file_path, message: { accountFileId: @acc_file_id.to_s })
                      
                      @header = @acc_file_path_r.header.to_hash
                      # @return_num =  @header[:res_header][:oprs]
                      
                      if @header[:res_header][:desc].to_s != 'failure'
                          @acc_file_path =  @acc_file_path_r.body.to_hash
                          @acc_file_path = @acc_file_path[:get_account_file_path_response][:account_file_path]
                          
                          @db[:network].find(id: network_id.to_i).update_one('$set'=> { 'tmp_file' => @acc_file_id.to_s, 'fileid' => ""  })
                          @db.close
                          @run_csv = 1
                      end
                  else
                      # save file id
                      @db[:network].find(id: network_id.to_i).update_one('$set'=> { 'fileid' => @acc_file_id.to_s })
                      @db.close                  
                  end
              
              end
          end
              
          @logger.info "getsogoufile done"
      rescue Exception
          @acc_file_path = ""
          @tmp_file = ""
          @run_csv = 0
          @logger.info "getsogoufile fail"
      end
  end
  
  
  def csvdetail(acc_file_id,acc_file_path, table)
            
      @logger.info "csvdetail start"      
      @unzip_name = @tmp+"/"+acc_file_id
      
      begin
          if acc_file_path.to_s != ""
              @logger.info "download file start"
              @zip_file = @tmp+"/"+acc_file_id + ".zip"
              open(@zip_file.to_s, 'wb') do |file|
                file << open(acc_file_path.to_s).read
              end
              
              @logger.info "download file done"
              
              unzip_file(@zip_file.to_s, @unzip_name.to_s)
              
              @logger.info "delete zip file start"
              File.delete(@zip_file)
              @logger.info "delete zip file done"
          end
          
          @unzip_folder = @unzip_name + "/*"
          @files = Dir.glob(@unzip_folder)
          
          @logger.info "read csv data start"
          @files.each_with_index do |file, index|
              if file.include?("account_")
                @account = CSV.read(file, :encoding => 'GB18030')
              end
              
              if table == "campaign" 
                if file.include?("cpcplan_")
                  # @cpcplan = CSV.read(file, :encoding => 'GB18030')
                  @cpcplan = file 
                end
              end
              
              if table == "adgroup"
                  if file.include?("cpcgrp_")
                    # @adgroup = CSV.read(file, :encoding => 'GB18030')
                    @adgroup = file
                  end
              end
              
              if table == "ad"
                  if file.include?("cpcidea_")
                    # @ad = CSV.read(file, :encoding => 'GB18030')
                    @ad = file
                  end
              end
              
              
              if file.include?("cpcexidea_")
              end
              
              
              if file.include?("cpcmobiexidea_")
              end
              
              if table == "keyword"
                  if file.include?("cpc_")
                    # @keyword = CSV.read(file, :encoding => 'GB18030')
                    @keyword = file
                    # @logger.info @keyword 
                  end
              end
              
          end
          @logger.info "read csv data done"
          @logger.info "csvdetail done"
      
      rescue Exception
          @account = nil
          @cpcplan = nil
          @adgroup = nil
          @ad = nil
          @keyword = nil
          @logger.info "csvdetail fail"
      end
  end
  
  
  
  def unzip_file (file, destination)
   @logger.info "unzip start"
    Zip::File.open(file) { |zip_file|
     zip_file.each { |f|
       f_path=File.join(destination, f.name)
       FileUtils.mkdir_p(File.dirname(f_path))
       zip_file.extract(f, f_path) unless File.exist?(f_path)
     }
    }
    @logger.info "unzip done"
    
#     
    # Zip::Archive.open(file) do |ar|
        # ar.each do |zf|
            # dirname = File.dirname(zf.name)
            # FileUtils.mkdir_p(dirname) unless File.exist?(dirname)
#       
            # # open(zf.name, 'wb') do |f|
              # # f << zf.read
            # # end
        # end
    # end
    
    
  end
  
  
  
  
  def insert_keyword(network_id,keyword,add_url,m_add_url)
    # @logger.info "insert : network "+network_id.to_s+ " keyword: "+keyword.to_s
                        
    db_name = "keyword_sogou_"+network_id.to_s
    
    @sogou_db[db_name].insert_one({ 
                                    network_id: network_id.to_i,
                                    cpc_plan_id: keyword[0].to_i, 
                                    cpc_grp_id: keyword[1].to_i,
                                    keyword_id: keyword[2].to_i,
                                    keyword: keyword[3].to_s,
                                    price: keyword[4].to_f, 
                                    visit_url: add_url.to_s,
                                    mobile_visit_url: m_add_url.to_s,
                                    match_type: keyword[7].to_i,
                                    pause: keyword[8].to_s,
                                    status: keyword[9].to_i,
                                    cpc_quality: keyword[10].to_f,
                                    active: keyword[11].to_i,
                                    display: keyword[12].to_i,
                                    use_grp_price: keyword[13].to_i,
                                    mobile_match_type: keyword[14].to_f,
                                    keyword_not_show_reason: keyword[15],
                                    keyword_not_approve_reason: keyword[16],
                                    response_code: "",
                                    m_response_code: "",
                                    update_date: @now,                                            
                                    create_date: @now })
    @sogou_db.close()                                    
  end
  
  
  
 
  def insert_ad(network_id,ad,add_url,m_add_url)
    # @logger.info "insert : network "+network_id.to_s+ " ad: "+ad.to_s
    
    db_name = "ad_sogou_"+network_id.to_s
    @sogou_db[db_name].insert_one({ 
                                    network_id: network_id.to_i,
                                    cpc_plan_id: ad[0].to_i, 
                                    cpc_grp_id: ad[1].to_i,
                                    cpc_idea_id: ad[2].to_i,
                                    cpc_idea_id_2: ad[3].to_s,
                                    title: ad[4].to_s, 
                                    description_1: ad[5].to_s, 
                                    description_2: ad[6].to_s, 
                                    visit_url: add_url.to_s,
                                    show_url: ad[8].to_s,
                                    mobile_visit_url: m_add_url.to_s,
                                    mobile_show_url: ad[10].to_s,
                                    pause: ad[11].to_s,
                                    status: ad[12].to_i,
                                    active: ad[13].to_s,
                                    idea_not_approve_reason: ad[14],
                                    mobile_visit_not_approve_reason: ad[15],
                                    response_code: "",
                                    m_response_code: "",
                                    update_date: @now,                                            
                                    create_date: @now })
    @sogou_db.close()                                    
  end 
 
  
  
  
  def insert_adgroup(network_id,adgroup)
    # @logger.info "insert : network "+network_id.to_s+ " adgroup: "+adgroup.to_s
    
    db_name = "adgroup_sogou_"+network_id.to_s
    @sogou_db[db_name].insert_one({ 
                                    network_id: network_id.to_i,
                                    cpc_plan_id: adgroup[0].to_i,
                                    cpc_grp_id: adgroup[1].to_i,
                                    name: adgroup[2].to_s,
                                    max_price: adgroup[3].to_f,
                                    negative_words: adgroup[4],
                                    exact_negative_words: adgroup[5],
                                    pause: adgroup[6].to_s,
                                    status: adgroup[7].to_i,
                                    opt: adgroup[8].to_s,
                                    update_date: @now,                                            
                                    create_date: @now })
    @sogou_db.close()                                    
  end 
 
 
 
  
  
  def insert_campaign(network_id,network_name,campaign)
    @logger.info "insert : network "+network_id.to_s+ " campaign: "+campaign.to_s
    
    @db["all_campaign"].insert_one({ 
                                    network_id: network_id.to_i,
                                    network_type: "sogou",
                                    account_name: network_name.to_s, 
                                    cpc_plan_id: campaign[0].to_i,
                                    campaign_name: campaign[1].to_s, 
                                    budget: campaign[2].to_f, 
                                    regions: campaign[3], 
                                    exclude_ips: campaign[5].to_s,
                                    negative_words: campaign[6],
                                    exact_negative_words: campaign[7],
                                    schedule: campaign[8],
                                    budget_offline_time: campaign[9],
                                    show_prob: campaign[10].to_i,
                                    pause: campaign[11].to_s,
                                    join_union: campaign[12].to_s,
                                    union_price: campaign[13].to_f,
                                    status: campaign[14].to_i,
                                    mobile_price_rate: campaign[15].to_f,
                                    opt: campaign[16].to_s,
                                    update_date: @now,                                            
                                    create_date: @now })
                                    
    @db.close                                
    # db_name = "campaign_sogou_"+network_id.to_s
    # @db[db_name].drop()
    # @db[db_name].insert_one({ 
                                    # network_id: network_id.to_i, 
                                    # cpc_plan_id: campaign[0].to_i,
                                    # name: campaign[1].to_s, 
                                    # budget: campaign[2].to_f, 
                                    # regions: campaign[3].to_i, 
                                    # exclude_ips: campaign[5].to_s,
                                    # negative_words: campaign[6].to_s,
                                    # exact_negative_words: campaign[7].to_s,
                                    # schedule: campaign[8].to_s,
                                    # budget_offline_time: campaign[9].to_s,
                                    # show_prob: campaign[10].to_i,
                                    # pause: campaign[11].to_s,
                                    # join_union: campaign[12].to_s,
                                    # union_price: campaign[13].to_f,
                                    # status: campaign[14].to_i,
                                    # mobile_price_rate: campaign[15].to_f,
                                    # opt: campaign[16].to_s,
                                    # update_date: @now,                                            
                                    # create_date: @now })
  end
  
  
  
  
  
  
  
  
  
  
  
    
    
    
  def index
    
      @logger.info "sogou index called"
      
      @current_network = @db[:network].find({ '$or' => [ {'file_update_1' => 3},{'file_update_2' => 3},{'file_update_3' => 3},{'file_update_4' => 3} ]})
      @db.close
      
      if @current_network.count.to_i >= 1
          @logger.info "working, no need update sogou index"
          return render :nothing => true
      end
      
      @id = params[:id]
      if @id.nil?
          @network = @db[:network].find('type' => 'sogou', 'file_update_1' => 2, 'file_update_2' => 2, 'file_update_3' => 2, 'file_update_4' => 2).sort({ last_update: -1 }).limit(1)
          @db.close
      else
          @network = @db[:network].find('type' => 'sogou', 'id' => @id.to_i).sort({ last_update: -1 }).limit(1)
          @db.close
      end
      
      @network.no_cursor_timeout.each do |doc|
            
            
            @do = 1
        
            if doc['tmp_file'].to_s != ""
                @tmp_file = "/datadrive/"+doc['tmp_file'].to_s
                if !File.directory?(@tmp_file)
                    @db[:network].find(id: doc["id"].to_i).update_one('$set'=> { 'fileid' => "", 'tmp_file' => "",'run_time' => 0,'file_update_1' => 0, 'file_update_2' => 0, 'file_update_3' => 0, 'file_update_4' => 0 })
                    @db.close
                    @do = 0
                    @logger.info "sogou " + doc['id'].to_s + " need to re download structure"
                end
            end
            
            if @do == 1
                @logger.info "sogou index " + doc['id'].to_s + " update start"
                
                getsogoufile(doc["username"],doc["password"],doc["api_token"], doc["fileid"].to_s, doc["id"].to_s, doc["tmp_file"].to_s)
            
                if @run_csv == 1 && doc["tmp_file"] != ""
                    @cpcplan = nil
                    csvdetail(@acc_file_id, @acc_file_path, "campaign")
                      
                    if @cpcplan.nil?
                        @db[:network].find(id: doc["id"].to_i).update_one('$set'=> { 'tmp_file' => "" , 'fileid' => "",'run_time' => 0, 'file_update_1' => 0, 'file_update_2' => 0, 'file_update_3' => 0, 'file_update_4' => 0})
                        @db.close
                        @logger.info "sogou campaign " + doc['id'].to_s + " csv not exist"
                    else
                        @logger.info "sogou campaign " + doc["id"].to_s + " updating "
                        
                        @db[:network].find(id: doc["id"].to_i).update_one('$set'=> {'file_update_1' => 3})
                        @db.close
                        
                        @db["all_campaign"].find(network_id: doc["id"].to_i, 'network_type' => "sogou").delete_many
                        @db.close
                        
                        
                        CSV.foreach(@cpcplan, :encoding => 'GB18030').each_with_index do |campaign, index|
                            if index != 0
                                begin
                                    insert_campaign(doc["id"],doc["name"],campaign)
                                rescue Exception
                                end
                            end
                        end
                        
                        @logger.info "sogou campaign " + doc['id'].to_s + " update done"
                        @db[:network].find(id: doc["id"].to_i).update_one('$set'=> {'file_update_1' => 4, 'last_update' => @now})
                        @db.close
                    end
                    
                    
                    @adgroup = nil
                    csvdetail(@acc_file_id, @acc_file_path, "adgroup")
                    
                    if @adgroup.nil?
                        @db[:network].find(id: doc["id"].to_i).update_one('$set'=> { 'tmp_file' => "" , 'fileid' => "",'run_time' => 0, 'file_update_1' => 0, 'file_update_2' => 0, 'file_update_3' => 0, 'file_update_4' => 0})
                        @db.close
                        @logger.info "sogou adroup " + doc['id'].to_s + " need to re download structure"
                    else
                      
                        @logger.info "sogou adgroup " + doc["id"].to_s + " updating "
                        @db[:network].find(id: doc["id"].to_i).update_one('$set'=> {'file_update_2' => 3})
                        @db.close
                        
                        db_name = "adgroup_sogou_"+doc['id'].to_s
                        @sogou_db[db_name].drop
                        @sogou_db.close()
                        
                        @sogou_db[db_name].indexes.create_one(network_id: Mongo::Index::ASCENDING)
                        @sogou_db[db_name].indexes.create_one(cpc_plan_id: Mongo::Index::ASCENDING)
                        @sogou_db[db_name].indexes.create_one(cpc_grp_id: Mongo::Index::ASCENDING)
                        @sogou_db[db_name].indexes.create_one(name: Mongo::Index::ASCENDING)
                        @sogou_db[db_name].indexes.create_one(max_price: Mongo::Index::ASCENDING)
                        @sogou_db[db_name].indexes.create_one(negative_words: Mongo::Index::ASCENDING)
                        @sogou_db[db_name].indexes.create_one(exact_negative_words: Mongo::Index::ASCENDING)
                        @sogou_db[db_name].indexes.create_one(pause: Mongo::Index::ASCENDING)
                        @sogou_db[db_name].indexes.create_one(status: Mongo::Index::ASCENDING)
                        @sogou_db[db_name].indexes.create_one(opt: Mongo::Index::ASCENDING)
                        @sogou_db[db_name].indexes.create_one(update_date: Mongo::Index::ASCENDING)
                        @sogou_db[db_name].indexes.create_one(create_date: Mongo::Index::ASCENDING)
                        
                        
                        CSV.foreach(@adgroup, :encoding => 'GB18030').each_with_index do |adgroup, index|  
                          if index != 0
                              begin
                                  insert_adgroup(doc["id"],adgroup)
                              rescue Exception
                              end
                          end
                        end  
                         
                        @logger.info "sogou adgroup " + doc['id'].to_s + " update done"
                        @db[:network].find(id: doc["id"].to_i).update_one('$set'=> {'file_update_2' => 4, 'last_update' => @now})
                        @db.close
                    end 
                    
                    
                    
                    
                    @ad = nil
                    csvdetail(@acc_file_id, @acc_file_path, "ad")
                      
                    if @ad.nil?
                        @db[:network].find(id: doc["id"].to_i).update_one('$set'=> { 'tmp_file' => "" , 'fileid' => "",'run_time' => 0, 'file_update_1' => 0, 'file_update_2' => 0, 'file_update_3' => 0, 'file_update_4' => 0})
                        @db.close
                        @logger.info "sogou ad " + doc['id'].to_s + " need to re download structure"
                    else
                      
                        @logger.info "sogou ad " + doc["id"].to_s + " updating "
                        @db[:network].find(id: doc["id"].to_i).update_one('$set'=> {'file_update_3' => 3})
                        @db.close
                        
                        db_name = "ad_sogou_"+doc['id'].to_s
                        @sogou_db[db_name].drop
                        @sogou_db.close()
                        
                        @sogou_db[db_name].indexes.create_one(network_id: Mongo::Index::ASCENDING)
                        @sogou_db[db_name].indexes.create_one(cpc_plan_id: Mongo::Index::ASCENDING)
                        @sogou_db[db_name].indexes.create_one(cpc_grp_id: Mongo::Index::ASCENDING)
                        @sogou_db[db_name].indexes.create_one(cpc_idea_id: Mongo::Index::ASCENDING)
                        @sogou_db[db_name].indexes.create_one(cpc_idea_id_2: Mongo::Index::ASCENDING)
                        @sogou_db[db_name].indexes.create_one(title: Mongo::Index::ASCENDING)
                        @sogou_db[db_name].indexes.create_one(description_1: Mongo::Index::ASCENDING)
                        @sogou_db[db_name].indexes.create_one(description_2: Mongo::Index::ASCENDING)
                        @sogou_db[db_name].indexes.create_one(visit_url: Mongo::Index::ASCENDING)
                        @sogou_db[db_name].indexes.create_one(show_url: Mongo::Index::ASCENDING)
                        @sogou_db[db_name].indexes.create_one(mobile_visit_url: Mongo::Index::ASCENDING)
                        @sogou_db[db_name].indexes.create_one(mobile_show_url: Mongo::Index::ASCENDING)
                        @sogou_db[db_name].indexes.create_one(pause: Mongo::Index::ASCENDING)
                        @sogou_db[db_name].indexes.create_one(status: Mongo::Index::ASCENDING)
                        @sogou_db[db_name].indexes.create_one(active: Mongo::Index::ASCENDING)
                        @sogou_db[db_name].indexes.create_one(idea_not_approve_reason: Mongo::Index::ASCENDING)
                        @sogou_db[db_name].indexes.create_one(mobile_visit_not_approve_reason: Mongo::Index::ASCENDING)
                        @sogou_db[db_name].indexes.create_one(update_date: Mongo::Index::ASCENDING)
                        @sogou_db[db_name].indexes.create_one(create_date: Mongo::Index::ASCENDING)
        
                        CSV.foreach(@ad, :encoding => 'GB18030').each_with_index do |ad, index|
                          if index != 0
                                insert_ad(doc["id"],ad)
                          end
                        end
                        
                        @logger.info "sogou ad " + doc['id'].to_s + " update done"
                        @db[:network].find(id: doc["id"].to_i).update_one('$set'=> {'file_update_3' => 4, 'last_update' => @now})
                        @db.close          
                    end
                    
                    
                    
                    
                    @keyword = nil
                    csvdetail(@acc_file_id, @acc_file_path, "keyword")
                    
                    if @keyword.nil? 
                        @db[:network].find(id: doc["id"].to_i).update_one('$set'=> { 'tmp_file' => "" , 'fileid' => "",'run_time' => 0, 'file_update_1' => 0, 'file_update_2' => 0, 'file_update_3' => 0, 'file_update_4' => 0})
                        @db.close
                        @logger.info "sogou keyword " + doc['id'].to_s + " need to re download structure"
                    else
                        @logger.info "sogou keyword " + doc["id"].to_s + " updating "
                        @db[:network].find(id: doc["id"].to_i).update_one('$set'=> {'file_update_4' => 3})
                        @db.close
                        
                        db_name = "keyword_sogou_"+doc['id'].to_s
                        @sogou_db[db_name].drop
                        @sogou_db.close()
                        
                        @sogou_db[db_name].indexes.create_one(network_id: Mongo::Index::ASCENDING)
                        @sogou_db[db_name].indexes.create_one(cpc_plan_id: Mongo::Index::ASCENDING)
                        @sogou_db[db_name].indexes.create_one(cpc_grp_id: Mongo::Index::ASCENDING)
                        @sogou_db[db_name].indexes.create_one(keyword_id: Mongo::Index::ASCENDING)
                        @sogou_db[db_name].indexes.create_one(keyword: Mongo::Index::ASCENDING)
                        @sogou_db[db_name].indexes.create_one(price: Mongo::Index::ASCENDING)
                        @sogou_db[db_name].indexes.create_one(visit_url: Mongo::Index::ASCENDING)
                        @sogou_db[db_name].indexes.create_one(mobile_visit_url: Mongo::Index::ASCENDING)
                        @sogou_db[db_name].indexes.create_one(match_type: Mongo::Index::ASCENDING)
                        @sogou_db[db_name].indexes.create_one(pause: Mongo::Index::ASCENDING)
                        @sogou_db[db_name].indexes.create_one(status: Mongo::Index::ASCENDING)
                        @sogou_db[db_name].indexes.create_one(cpc_quality: Mongo::Index::ASCENDING)
                        @sogou_db[db_name].indexes.create_one(active: Mongo::Index::ASCENDING)
                        @sogou_db[db_name].indexes.create_one(display: Mongo::Index::ASCENDING)
                        @sogou_db[db_name].indexes.create_one(use_grp_price: Mongo::Index::ASCENDING)
                        @sogou_db[db_name].indexes.create_one(mobile_match_type: Mongo::Index::ASCENDING)
                        @sogou_db[db_name].indexes.create_one(keyword_not_show_reason: Mongo::Index::ASCENDING)
                        @sogou_db[db_name].indexes.create_one(keyword_not_approve_reason: Mongo::Index::ASCENDING)
                        @sogou_db[db_name].indexes.create_one(update_date: Mongo::Index::ASCENDING)
                        @sogou_db[db_name].indexes.create_one(create_date: Mongo::Index::ASCENDING)
                        
                        CSV.foreach(@keyword, :encoding => 'GB18030').each_with_index do |keyword, index|
                          if index != 0
                              begin
                                  insert_keyword(doc["id"],keyword)
                              rescue Exception
                              end
                          end
                        end  
                          
                        @logger.info "sogou keyword " + doc['id'].to_s + " update done"
                        @db[:network].find(id: doc["id"].to_i).update_one('$set'=> {'file_update_4' => 4, 'last_update' => @now})
                        @db.close       
                    end
                    
                    update_account
                    @logger.info "sogou index " + doc['id'].to_s + " update done"
                end
            end
      end
    
      return render :nothing => true
  end  
    
    
    
    
  def updateaccount
    
      @id = params[:id]
      
      if @id.nil?
          @network = @db[:network].find('type' => 'sogou')
          @db.close
      else
          @network = @db[:network].find({ "$and" => [{:type => 'sogou'}, {:id => @id.to_i}] })
          @db.close
      end
    
      @network.no_cursor_timeout.each do |doc|
        
          sogou_api(doc["username"].to_s,doc["password"].to_s,doc["api_token"].to_s,"AccountService")
          @sogou_result = @sogou_api.call(:get_account_info)
          
          @header = @sogou_result.header.to_hash
          
          @logger.info @header
          
          # data = {:message => "sogou index", :datas => @sogou_result, :id => doc['id'], :status => "true"}
          # return render :json => data, :status => :ok 
          
          if @header[:res_header][:desc].to_s != 'failure'
                
                @sogou_body = @sogou_result.body.to_hash
                @sogou_body = @sogou_body[:get_account_info_response][:account_info_type]
                  
                # @return_num =  @header[:res_header][:oprs]
                @quota =  @header[:res_header][:rquota]
            
                @accountid = @sogou_body[:accountid]
                @balance = @sogou_body[:balance]
                # @budget = @sogou_body[:budget]
                @domains = @sogou_body[:domains]
                @regions = @sogou_body[:regions]
                @total_cost = @sogou_body[:total_cost]
                @total_pay = @sogou_body[:total_pay]
               
            
                @db[:network].find(id: doc["id"].to_i).update_one('$set'=> { 'accountid' => @accountid.to_i,
                                                                            'balance' => @balance.to_f,
                                                                            'domains' => @domains.to_s,
                                                                            'regions' => @regions,
                                                                            'total_cost' => @total_cost.to_f,
                                                                            'total_pay' => @total_pay.to_f,
                                                                            'quota' => @quota.to_i                                                                    
                                                                          })
                @db.close
          end
      end
      @db.close
      return render :nothing => true
  end
  
  
  
  def resetnetwork
    @logger.info "start reset sogou network api status"
    
    @id = params[:id]
    if @id.nil?
        @network = @db[:network].find({ "$and" => [{:type => 'sogou'}, {:file_update_1 => 4}, {:file_update_2 => 4}, {:file_update_3 => 4}, {:file_update_4 => 4}] })
        @db.close
    else
        @network = @db[:network].find({ "$and" => [{:type => 'sogou'}, {:id => @id.to_i}] })
        @db.close
    end
    
    @network.no_cursor_timeout.each do |doc|
          @db[:network].find(id: doc["id"].to_i).update_one('$set'=> { 'file_update_1' => 2, 'file_update_2' => 2, 'file_update_3' => 2, 'file_update_4' => 2 })
          @db.close
          @logger.info "done reset sogou network api status " +doc['id'].to_s
    end
    
    @logger.info "done reset sogou api network api status"
    return render :nothing => true
  end  
    
    
    
  def resetdlfile
    @logger.info "start reset sogou api download file"
    
    @id = params[:id]
    if @id.nil?
        @network = @db[:network].find({ "$and" => [{:type => 'sogou'}, {:file_update_1 => { '$gte' => 4 }}, {:file_update_2 => { '$gte' => 4 }}, {:file_update_3 => { '$gte' => 4 }}, {:file_update_4 => { '$gte' => 4 }}] })
        # @network = @db[:network].find('type' => 'sogou')
        @db.close
    else
        @network = @db[:network].find({ "$and" => [{:type => 'sogou'}, {:id => @id.to_i}] })
        @db.close
    end
    
    @network.no_cursor_timeout.each do |doc|
          if doc["tmp_file"] != ""
              unzip_folder = @tmp+"/"+doc["tmp_file"]
              if File.directory?(unzip_folder)
                  FileUtils.remove_dir unzip_folder, true
              end
          end
          @logger.info "done reset sogou api download file " +doc['id'].to_s
          
          @db[:network].find(id: doc["id"].to_i).update_one('$set'=> { 'fileid' => "", 'tmp_file' => "", 'run_time' => 0, 'file_update_1' => 0, 'file_update_2' => 0, 'file_update_3' => 0, 'file_update_4' => 0, 'worker' => "", 'last_update' => @now })
          @db.close
    end
    
    @logger.info "done reset sogou api download file"
    return render :nothing => true
  end   
  
   
   
  def update_account
      @logger.info "start update account"
      @account.each_with_index do |account, index|
          if index != 0
              
              @db[:network].find(id: account[0].to_i).update_one('$set'=> { 
                                                                              'balance' => account[1].to_f,
                                                                              'total_cost' => account[2].to_f,
                                                                              'total_pay' => account[3].to_f,
                                                                              'regions' => account[5],
                                                                              'domains' => account[8].to_s
                                                                              
                                                                            })
              @db.close                                                              
          end
      end
     @logger.info "done update account" 
     # return render :nothing => true
  end
  
  
  
  def dlaccfile
    @logger.info "sogou dlaccfile start"
    
    @all_network = @db[:network].find()
    @db.close
    
    @dl_limit = @all_network.count.to_i / 4  
    
    @all_work_network = @db[:network].find('worker' => @port.to_i)
    @db.close
    
    if @all_work_network.count.to_i >= @dl_limit.to_i
        @logger.info "360 dlaccfile limit"
        return render :nothing => true
    end
    
    
    @id = params[:id]
    if @id.nil?
        @current_network = @db[:network].find({ "$and" => [{:type => 'sogou'}, {:file_update_1 => 1}, {:file_update_2 => 1}, {:file_update_3 => 1}, {:file_update_4 => 1}, {:worker => @port.to_i}] }).sort({ last_update: -1 }).limit(1)
        @db.close
        
        if @current_network.count.to_i >= 1
            @logger.info "sogou dl working"
            return render :nothing => true
        end
          
        @network = @db[:network].find({ "$and" => [{:type => 'sogou'}, {:file_update_1 => 0}, {:file_update_2 => 0}, {:file_update_3 => 0}, {:file_update_4 => 0}, {:worker => @port.to_i}] }).sort({ last_update: -1 }).limit(1)
        @db.close
        
        if @network.count.to_i == 0
            
            @network = @db[:network].find({ "$and" => [{:type => 'sogou'}, {:file_update_1 => 0}, {:file_update_2 => 0}, {:file_update_3 => 0}, {:file_update_4 => 0}, {:worker => ""}] }).sort({ last_update: -1 }).limit(1)
            @db.close
          
            if @network.count.to_i == 0
                @logger.info "no need to dl sogou"
                return render :nothing => true
            end
        end
        
    else
        @network = @db[:network].find({ "$and" => [{:type => 'sogou'}, {:id => @id.to_i}] })
        @db.close
    end
    
    
    @network.no_cursor_timeout.each do |doc|
        
        @logger.info "sogou dlaccfile " + doc['id'].to_s + " running"
        @db[:network].find(id: doc["id"].to_i).update_one('$set'=> {'file_update_1' => 1, 'file_update_2' => 1, 'file_update_3' => 1, 'file_update_4' => 1, 'worker' => @port.to_i, 'last_update' => @now})
        @db.close
        
        if doc["run_time"].to_i >= 10
            @db[:network].find(id: doc["id"].to_i).update_one('$set'=> { 'fileid' => "", 'tmp_file' => "",'run_time' => 0,'file_update_1' => 0, 'file_update_2' => 0, 'file_update_3' => 0, 'file_update_4' => 0, 'worker' => "", 'last_update' => @now })
            @db.close
        else
            
            getsogoufile(doc["username"],doc["password"],doc["api_token"], doc["fileid"].to_s, doc["id"].to_s, doc["tmp_file"].to_s)
        
            if @run_csv == 1
                @account = nil
                csvdetail(@acc_file_id, @acc_file_path, "account")
                  
                if @account.nil?
                    if doc["tmp_file"] != ""
                      unzip_folder = @tmp+"/"+doc["tmp_file"]
                      FileUtils.remove_dir unzip_folder, true
                    end
                    
                    @db[:network].find(id: doc["id"].to_i).update_one('$set'=> { 'tmp_file' => "" , 'fileid' => "",'run_time' => 0, 'file_update_1' => 0, 'file_update_2' => 0, 'file_update_3' => 0, 'file_update_4' => 0, 'worker' => "", 'last_update' => @now})
                    @db.close
                    @logger.info "sogou apifile " + doc['id'].to_s + " csv not exist"
                else
                    update_account
                    @logger.info "sogou dlaccfile " + doc['id'].to_s + " done"
                    @db[:network].find(id: doc["id"].to_i).update_one('$set'=> {'file_update_1' => 2, 'file_update_2' => 2, 'file_update_3' => 2, 'file_update_4' => 2, 'last_update' => @now, 'worker' => @port.to_i})
                    @db.close
                end
                
            elsif @run_csv == 2
                # set its done in
                @db[:network].find(id: doc["id"].to_i).update_one('$set'=> { 'file_update_1' => 4,'file_update_2' => 4,'file_update_3' => 4,'file_update_4' => 4, 'last_update' => @now, 'worker' => "" })
                @db.close 
                
            else    
                run_time = doc["run_time"].to_i + 1
                @db[:network].find(id: doc["id"].to_i).update_one('$set'=> { 'run_time' => run_time.to_i, 'file_update_1' => 0, 'file_update_2' => 0, 'file_update_3' => 0, 'file_update_4' => 0, 'last_update' => @now})
                @db.close
                @logger.info "sogou apifile " + doc['id'].to_s + " still pending"
            end
          
        end
    end
    
    @logger.info "sogou dlaccfile done"
    return render :nothing => true 
  end
  
  
  def apiadgroup
    
    @logger.info "sogou api group start"
    @campaign_id = params[:id]
    
    if @campaign_id.nil?
        @current_campaign = @db[:all_campaign].find({ "$and" => [{:api_update => 4}, {:network_type => 'sogou'}, {:api_worker => @port.to_i}] })
        @db.close
        
        if @current_campaign.count.to_i >= 1
            @logger.info "working, no need update sogou api campaign"
            return render :nothing => true
        end
        
        @campaign = @db[:all_campaign].find({ "$and" => [{:api_update => 3}, {:network_type => 'sogou'}, {:api_worker => @port.to_i}] }).sort({ last_update: -1 }).limit(1)
        @db.close
        
        if @campaign.count.to_i == 0
            @logger.info "no need update sogou api campaign"
            return render :nothing => true
        end
        
    else
        @campaign = @db[:all_campaign].find({ "$and" => [{:cpc_plan_id => @campaign_id.to_i}, {:network_type => 'sogou'}] })
        @db.close
    end
    
    if @campaign.count.to_i
      
        @campaign.no_cursor_timeout.each do |campaign|
            @network_id = campaign["network_id"].to_i
            @campaign_id = campaign["cpc_plan_id"].to_i
        end
        
        @network = @db[:network].find({ "$and" => [{:id => @network_id.to_i}, {:type => 'sogou'}] })
        @db.close
        
        if @network.count.to_i > 0
          
            @network.no_cursor_timeout.each do |network_d|
              
                @tracking_type = network_d["tracking_type"].to_s
                @ad_redirect = network_d["ad_redirect"].to_s
                @keyword_redirect = network_d["keyword_redirect"].to_s
                @company_id = network_d["company_id"].to_s
                @cookie_length = network_d["cookie_length"].to_s
                
                sogou_api(network_d["username"],network_d["password"],network_d["api_token"],"AccountService")
                sogou_result = @sogou_api.call(:get_account_info)
                
                if sogou_result.header[:res_header][:desc].to_s == "success" && sogou_result.header[:res_header][:rquota].to_i >= 500
                    
                    @remain_quote = sogou_result.header[:res_header][:rquota].to_i
                
                    db_name = "adgroup_sogou_"+@network_id.to_s
             
                    @adgroup = @sogou_db[db_name].find({ "$and" => [{:cpc_plan_id => @campaign_id.to_i}, {:api_update_ad => 1}, {:api_update_keyword => 1}, {:api_worker => @port.to_i}] })
                    @sogou_db.close()
                    
                    @adgroup_id_array = []
                    
                    if @adgroup.count.to_i
                        
                        @adgroup.no_cursor_timeout.each do |adgroup_d|
                            @adgroup_id_array << adgroup_d["cpc_grp_id"].to_i
                        end
                                                
                        # start , status
                        
                        sogou_api(network_d["username"],network_d["password"],network_d["api_token"],"CpcGrpService")
                                        
                        requesttypearray = []
                        requesttype = {}
                             
                        @update_status = @sogou_api.call(:get_cpc_grp_by_cpc_grp_id, message: { cpcGrpIds: @adgroup_id_array })
                        @header = @update_status.header.to_hash
                        @msg = @header[:res_header][:desc]
                        @remain_quote = @header[:res_header][:rquota]
                        
                        if @msg.to_s.downcase == "success"
                            @update_status_body = @update_status.body.to_hash
                            @update_status_body = @update_status_body[:get_cpc_grp_by_cpc_grp_id_response][:cpc_grp_types]
                                    
                            @result_adgroup_array = @update_status_body
                                
                            if @result_adgroup_array.is_a?(Hash)
                                if !@result_adgroup_array.empty?
                                    @adgroup_id_array << @result_adgroup_array[:cpc_grp_id].to_i
                                    
                                    db_name = "adgroup_sogou_"+@network_id.to_s
                                    result = @sogou_db[db_name].find('cpc_grp_id' => @result_adgroup_array[:cpc_grp_id].to_i).update_one('$set'=> { 
                                                                                                                                    'name' => @result_adgroup_array[:cpc_grp_name].to_s,
                                                                                                                                    'max_price' => @result_adgroup_array[:max_price].to_f,
                                                                                                                                    'pause' => @result_adgroup_array[:pause].to_s,
                                                                                                                                    'status' => @result_adgroup_array[:status].to_i,
                                                                                                                                    'api_update_ad' => 2,
                                                                                                                                    'api_update_keyword' => 2,
                                                                                                                                    'update_date' => @now
                                                                                                                               })
                                    @sogou_db.close()
                                    
                                    if result.n.to_i == 0
                                        
                                        @sogou_db[db_name].insert_one({ 
                                                                        network_id: @network_id.to_i,
                                                                        cpc_plan_id: @campaign_id.to_i,
                                                                        cpc_grp_id: @result_adgroup_array[:cpc_grp_id].to_i,
                                                                        name: @result_adgroup_array[:cpc_grp_name].to_s,
                                                                        max_price: @result_adgroup_array[:max_price].to_f,
                                                                        negative_words: "",
                                                                        exact_negative_words: "",
                                                                        pause: @result_adgroup_array[:pause].to_s,
                                                                        status: @result_adgroup_array[:status].to_i,
                                                                        opt: "",
                                                                        update_date: @now,                                            
                                                                        create_date: @now })
                                        @sogou_db.close()
                                    end
                                end
                                
                            else
                                
                                if !@result_adgroup_array.nil? && @result_adgroup_array.count.to_i > 0
                                    @result_adgroup_array.each do |result_adgroup_array|
                                        
                                        @adgroup_id_array << result_adgroup_array[:cpc_grp_id].to_i
                                    
                                        db_name = "adgroup_sogou_"+@network_id.to_s
                                        result = @sogou_db[db_name].find('cpc_grp_id' => result_adgroup_array[:cpc_grp_id].to_i).update_one('$set'=> { 
                                                                                                                                            'name' => result_adgroup_array[:cpc_grp_name].to_s,
                                                                                                                                            'max_price' => result_adgroup_array[:max_price].to_f,
                                                                                                                                            'pause' => result_adgroup_array[:pause].to_s,
                                                                                                                                            'status' => result_adgroup_array[:status].to_i,
                                                                                                                                            'api_update_ad' => 2,
                                                                                                                                            'api_update_keyword' => 2,
                                                                                                                                            'update_date' => @now
                                                                                                                                       })
                                        @sogou_db.close()
                                        
                                        # @logger.info result.n  
                                        if result.n.to_i == 0
                                            
                                            @sogou_db[db_name].insert_one({ 
                                                                            network_id: @network_id.to_i,
                                                                            cpc_plan_id: @campaign_id.to_i,
                                                                            cpc_grp_id: result_adgroup_array[:cpc_grp_id].to_i,
                                                                            name: result_adgroup_array[:cpc_grp_name].to_s,
                                                                            max_price: result_adgroup_array[:max_price].to_f,
                                                                            negative_words: "",
                                                                            exact_negative_words: "",
                                                                            pause: result_adgroup_array[:pause].to_s,
                                                                            status: result_adgroup_array[:status].to_i,
                                                                            opt: "",
                                                                            update_date: @now,                                            
                                                                            create_date: @now })
                                            @sogou_db.close()     
                                            
                                            
                                        end
                                    end
                                end
                            end  
                            
                            # adgroup done
                            
                            
                            
                            if @adgroup_id_array.count.to_i > 0
                                      
                                sogou_api(network_d["username"],network_d["password"],network_d["api_token"],"CpcIdeaService")
                                
                                @update_status = @sogou_api.call(:get_cpc_idea_by_cpc_grp_id, message: { cpcGrpIds: @adgroup_id_array, getTemp: 0 })
                                @header = @update_status.header.to_hash
                                @msg = @header[:res_header][:desc]
                                @remain_quote = @header[:res_header][:rquota]
                                
                                if @msg.to_s.downcase == "success" && @remain_quote.to_i >= 500
                                  
                                    @update_status_body = @update_status.body.to_hash
                                    @result_active_grp = @update_status_body[:get_cpc_idea_by_cpc_grp_id_response][:cpc_grp_ideas]
                                    
                                    @all_ad = []
                                    
                                    if @result_active_grp.is_a?(Hash)
                                        if !@result_active_grp.empty?
                                          
                                            @result_ad = @result_active_grp[:cpc_idea_types]
                                          
                                            if @result_ad.is_a?(Hash)
                                                if !@result_ad.empty?
                                                    @all_ad << @result_ad
                                                end
                                            else 
                                                if !@result_ad.nil? && @result_ad.count.to_i > 0
                                                    @all_ad = @all_ad + @result_ad
                                                end
                                            end
                                        end
                                    else
                                        if !@result_active_grp.nil? && @result_active_grp.count.to_i > 0
                                          
                                            @result_active_grp.each do |result_adgroup_array|
                                              
                                                @result_ad = result_adgroup_array[:cpc_idea_types]  
                                                
                                                if @result_ad.is_a?(Hash)
                                                    if !@result_ad.empty?
                                                        @all_ad << @result_ad
                                                    end
                                                else 
                                                    if !@result_ad.nil? && @result_ad.count.to_i > 0
                                                        @all_ad = @all_ad + @result_ad
                                                    end
                                                end
                                            end
                                        end
                                    end
                                     
                                    # @logger.info @all_ad
                                    
                                    if @all_ad.count.to_i > 0
                                        @all_ad.each do |all_ad_d|
                                            
                                            # @logger.info all_ad_d
                                            
                                            url_tag = 0
                                            m_url_tag = 0
                                            
                                            if all_ad_d[:visit_url].nil?
                                                @final_url = ""
                                            else
                                                @final_url = all_ad_d[:visit_url]
                                            end
                                            
                                            if all_ad_d[:mobile_visit_url].nil?
                                                @m_final_url = ""
                                            else
                                                @m_final_url = all_ad_d[:mobile_visit_url]  
                                            end
                                            
                                            if all_ad_d[:show_url].nil?
                                                @show_url = ""
                                            else
                                                @show_url = all_ad_d[:show_url]
                                            end
                                            
                                            if all_ad_d[:visit_url].nil?
                                                @m_show_url = ""
                                            else  
                                                @m_show_url = all_ad_d[:mobile_show_url]
                                            end  
                                            
                                            
                                            if !@final_url.to_s.include?(".adeqo.") && @tracking_type.to_s.downcase == "adeqo" && @ad_redirect.to_s.downcase == "yes" && @final_url.to_s != ""
                                                @temp_final_url = @final_url
                                                @final_url = "http://t.adeqo.com/click?company_id="+@company_id.to_s+"&network_id="+@network_id.to_s
                                                @final_url = @final_url + "&campaign_id="+@campaign_id.to_s+"&adgroup_id="+all_ad_d[:cpc_grp_id].to_s+"&ad_id={creative}&keyword_id={keywordid}"
                                                @final_url = @final_url + "&cookie="+@cookie_length.to_s
                                                @final_url = @final_url + "&device=pc"
                                                @final_url = @final_url + "&tv=v1&durl="+CGI.escape(@temp_final_url.to_s)
                                                
                                                url_tag = 0
                                            end
                                            
                                            
                                            if !@m_final_url.to_s.include?(".adeqo.") && @tracking_type.to_s.downcase == "adeqo" && @ad_redirect.to_s.downcase == "yes" && @m_final_url.to_s != ""
                                                m_url_tag = 1  
                                                
                                                @temp_m_final_url = @m_final_url
                                                @m_final_url = "http://t.adeqo.com/click?company_id="+@company_id.to_s+"&network_id="+@network_id.to_s
                                                @final_url = @m_final_url + "&campaign_id="+@campaign_id.to_s+"&adgroup_id="+all_ad_d[:cpc_grp_id].to_s+"&ad_id={creative}&keyword_id={keywordid}"
                                                @m_final_url = @m_final_url + "&cookie="+@cookie_length.to_s
                                                @m_final_url = @m_final_url + "&device=mobile"
                                                @m_final_url = @m_final_url + "&tv=v1&durl="+CGI.escape(@temp_m_final_url.to_s)
                                            end
                                            
                                            if url_tag == 1 || m_url_tag == 1 && @remain_quote.to_i >= 500
                                                sogou_api(network_d["username"],network_d["password"],network_d["api_token"],"CpcIdeaService")
                                                requesttypearray = [] 
                                                requesttype = {}
                                                requesttype[:cpcIdeaId]    =     all_ad_d[:cpc_idea_id].to_i
                                                requesttype[:cpcGrpId]    =     0
                                                requesttype[:visitUrl]    =     @final_url
                                                requesttype[:mobileVisitUrl] =    @m_final_url
                                                
                                                requesttypearray << requesttype
                                                # @logger.info requesttypearray
                                                @update_status = @sogou_api.call(:update_cpc_idea, message: { cpcIdeaTypes: requesttypearray })
                                                
                                                # @logger.info @update_status
                                                                         
                                                @header = @update_status.header.to_hash
                                                @msg = @header[:res_header][:desc]
                                                @remain_quote = @header[:res_header][:rquota]
                                                
                                                if @msg.to_s.downcase != "success"
                                                    @final_url = all_ad_d[:visit_url].to_s
                                                    @m_final_url = all_ad_d[:mobile_visit_url].to_s
                                                end
                                            end
                                            
                                            
                                            db_name = "ad_sogou_"+@network_id.to_s
                                            result = @sogou_db[db_name].find('cpc_idea_id' => all_ad_d[:cpc_idea_id].to_i).update_one('$set'=> { 
                                                                                                                                      'title' => all_ad_d[:title].to_s,
                                                                                                                                      'description_1' => all_ad_d[:description1].to_s,
                                                                                                                                      'description_2' => all_ad_d[:description2].to_s,
                                                                                                                                      'visit_url' => @final_url.to_s,
                                                                                                                                      'mobile_visit_url' => @m_final_url.to_s,
                                                                                                                                      'show_url' => @show_url.to_s,
                                                                                                                                      'mobile_show_url' => @m_show_url.to_s,
                                                                                                                                      'pause' => all_ad_d[:pause].to_s,
                                                                                                                                      'status' => all_ad_d[:status].to_i,
                                                                                                                                      'update_date' => @now
                                                                                                                                 })
                                            @sogou_db.close()
                                            
                                            
                                            if result.n.to_i == 0
                                              
                                                @sogou_db[db_name].insert_one({ 
                                                                                network_id: @network_id.to_i,
                                                                                cpc_plan_id: @campaign_id.to_i, 
                                                                                cpc_grp_id: all_ad_d[:cpc_grp_id].to_i,
                                                                                cpc_idea_id: all_ad_d[:cpc_idea_id].to_i,
                                                                                cpc_idea_id_2: "",
                                                                                title: all_ad_d[:title].to_s, 
                                                                                description_1: all_ad_d[:description1].to_s, 
                                                                                description_2: all_ad_d[:description2].to_s, 
                                                                                visit_url: @final_url.to_s,
                                                                                show_url: @show_url.to_s,
                                                                                mobile_visit_url: @m_final_url.to_s,
                                                                                mobile_show_url: @m_show_url.to_s,
                                                                                pause: all_ad_d[:pause].to_s,
                                                                                status: all_ad_d[:status].to_i,
                                                                                active: "",
                                                                                idea_not_approve_reason: "",
                                                                                mobile_visit_not_approve_reason: "",
                                                                                update_date: @now,                                            
                                                                                create_date: @now })
                                                @sogou_db.close()
                                            end
                                        
                                        end
                                    end
                                    
                                    # data = { :tmasdp => @tmp, :status => "true"}
                                    # return render :json => data, :status => :ok

                                    # ad done
                                    # keyword start here
                                    
                                    
                                    sogou_api(network_d["username"],network_d["password"],network_d["api_token"],"CpcService")
                                
                                    @update_status = @sogou_api.call(:get_cpc_by_cpc_grp_id, message: { cpcGrpIds: @adgroup_id_array, getTemp: 0 })
                                    @header = @update_status.header.to_hash
                                    @msg = @header[:res_header][:desc]
                                    @remain_quote = @header[:res_header][:rquota]
                            
                                    if @msg.to_s.downcase == "success" && @remain_quote.to_i >= 500
                                        @update_status_body = @update_status.body.to_hash
                                        @result_active_grp = @update_status_body[:get_cpc_by_cpc_grp_id_response][:cpc_grp_cpcs]
                                        
                                        @all_keyword = []
                                        # @logger.info @result_active_grp
                                        
                                        if @result_active_grp.is_a?(Hash)
                                            if !@result_active_grp.empty?
#                                                       
                                                @result_keyword = @result_active_grp[:cpc_types]
#                                                       
                                                if @result_keyword.is_a?(Hash)
                                                    if !@result_keyword.empty?
                                                        @all_keyword << @result_keyword
                                                    end
                                                else 
                                                    if !@result_keyword.nil? && @result_keyword.count.to_i > 0
                                                        @all_keyword = @all_keyword + @result_keyword
                                                    end
                                                end
                                            end
                                        else
                                            if !@result_active_grp.nil? && @result_active_grp.count.to_i > 0
#                                                       
                                                @result_active_grp.each do |result_adgroup_array|
#                                                           
                                                    @result_keyword = result_adgroup_array[:cpc_types]  
#                                                             
                                                    if @result_keyword.is_a?(Hash)
                                                        if !@result_keyword.empty?
                                                            @all_keyword << @result_keyword
                                                        end
                                                    else 
                                                       
                                                        if !@result_keyword.nil? && @result_keyword.count.to_i > 0
                                                            @all_keyword = @all_keyword + @result_keyword
                                                        end
                                                        
                                                    end
                                                end
                                            end
                                        end
                                        
                                        
                                        if @all_keyword.count.to_i > 0
                                            @all_keyword.each do |all_keyword_d|
                                              
                                                url_tag = 0
                                                m_url_tag = 0
                                                
                                                if all_keyword_d[:visit_url].nil?
                                                    @final_url = ""
                                                else
                                                    @final_url = all_keyword_d[:visit_url]
                                                end
                                                
                                                if all_keyword_d[:mobile_visit_url].nil?
                                                    @m_final_url = ""
                                                else
                                                    @m_final_url = all_keyword_d[:mobile_visit_url]  
                                                end
                                              
                                              
                                                if !@final_url.to_s.include?(".adeqo.") && @tracking_type.to_s.downcase == "adeqo" && @keyword_redirect.to_s.downcase == "yes" && @final_url.to_s != ""
                                                    url_tag = 1
                                                    @temp_final_url = @final_url
                                                    @final_url = "http://t.adeqo.com/click?company_id="+@company_id.to_s+"&network_id="+@network_id.to_s
                                                    @final_url = @final_url + "&campaign_id="+@campaign_id.to_s+"&adgroup_id="+all_keyword_d[:cpc_grp_id].to_s+"&ad_id={creative}&keyword_id={keywordid}"
                                                    @final_url = @final_url + "&cookie="+@cookie_length.to_s
                                                    @final_url = @final_url + "&device=pc"
                                                    @final_url = @final_url + "&tv=v1&durl="+CGI.escape(@temp_final_url.to_s)
                                                end
                                                
                                                if !@m_final_url.to_s.include?(".adeqo.") && @tracking_type.to_s.downcase == "adeqo" && @keyword_redirect.to_s.downcase == "yes" && @m_final_url.to_s != ""
                                                    m_url_tag = 1
                                                    @temp_m_final_url = @m_final_url
                                                    @m_final_url = "http://t.adeqo.com/click?company_id="+@company_id.to_s+"&network_id="+@network_id.to_s
                                                    @m_final_url = @m_final_url + "&campaign_id="+@campaign_id.to_s+"&adgroup_id="+all_keyword_d[:cpc_grp_id].to_s+"&ad_id={creative}&keyword_id={keywordid}"
                                                    @m_final_url = @m_final_url + "&cookie="+@cookie_length.to_s
                                                    @m_final_url = @m_final_url + "&device=mobile"
                                                    @m_final_url = @m_final_url + "&tv=v1&durl="+CGI.escape(@temp_m_final_url.to_s)
                                                end
                                                
                                                if url_tag == 1 || m_url_tag == 1 && @remain_quote.to_i >= 500
                                    
                                                    sogou_api(network_d["username"],network_d["password"],network_d["api_token"],"CpcService")
                                                    requesttypearray = [] 
                                                    requesttype = {}
                                                    requesttype[:cpcId]    =     all_keyword_d[:cpc_id].to_i
                                                    requesttype[:cpc]    =     0
                                                    requesttype[:cpcGrpId]    =     0
                                                    requesttype[:visitUrl]    =     @final_url
                                                    requesttype[:mobileVisitUrl] =    @m_final_url
                                                    
                                                    requesttypearray << requesttype
                                                    @update_status = @sogou_api.call(:update_cpc, message: { cpcTypes: requesttypearray })
                                                                             
                                                    @header = @update_status.header.to_hash
                                                    @msg = @header[:res_header][:desc]
                                                    @remain_quote = @header[:res_header][:rquota]
                                                    
                                                    if @msg.to_s.downcase != "success"
                                                        @final_url = all_keyword_d[:visit_url].to_s
                                                        @m_final_url = all_keyword_d[:mobile_visit_url].to_s
                                                    end
                                                        
                                                end
                                                
                                                
                                                db_name = "keyword_sogou_"+@network_id.to_s
                                                result = @sogou_db[db_name].find('keyword_id' => all_keyword_d[:cpc_id].to_i).update_one('$set'=> { 
                                                                                                                                          'keyword' => all_keyword_d[:cpc].to_s,
                                                                                                                                          'price' => all_keyword_d[:price].to_f,
                                                                                                                                          'visit_url' => @final_url.to_s,
                                                                                                                                          'mobile_visit_url' => @m_final_url.to_s,
                                                                                                                                          'match_type' => all_keyword_d[:match_type].to_s,
                                                                                                                                          'cpc_quality' => all_keyword_d[:cpc_quality].to_f,
                                                                                                                                          'pause' => all_keyword_d[:pause].to_s,
                                                                                                                                          'status' => all_keyword_d[:status].to_i,
                                                                                                                                          'update_date' => @now
                                                                                                                                     })
                                                @sogou_db.close()
                                                
                                                
                                                
                                                if result.n.to_i == 0
                                                  
                                                    @sogou_db[db_name].insert_one({ 
                                                                                      network_id: @network_id.to_i,
                                                                                      cpc_plan_id: @campaign_id.to_i, 
                                                                                      cpc_grp_id: all_keyword_d[:cpc_grp_id].to_i,
                                                                                      keyword_id: all_keyword_d[:cpc_id].to_i,
                                                                                      keyword: all_keyword_d[:cpc].to_s,
                                                                                      price: all_keyword_d[:price].to_f, 
                                                                                      visit_url: @final_url.to_s,
                                                                                      mobile_visit_url: @m_final_url.to_s,
                                                                                      match_type: all_keyword_d[:match_type].to_i,
                                                                                      pause: all_keyword_d[:pause].to_s,
                                                                                      status: all_keyword_d[:status].to_i,
                                                                                      cpc_quality: all_keyword_d[:cpc_quality].to_f,
                                                                                      active: 0,
                                                                                      display: 0,
                                                                                      use_grp_price: 0,
                                                                                      mobile_match_type: 3,
                                                                                      keyword_not_show_reason: "",
                                                                                      keyword_not_approve_reason: "",
                                                                                      update_date: @now,                                            
                                                                                      create_date: @now })
                                                      @sogou_db.close()
                                                end
                                              
                                              
                                            end
                                        end
                                    end
                                end
                                
                                                                        
                                db_name = "adgroup_sogou_"+@network_id.to_s
                                @sogou_db[db_name].find('cpc_grp_id' => { "$in" => @adgroup_id_array}).update_many('$set'=> { 
                                                                                                                                'api_update_ad' => 0,
                                                                                                                                'api_update_keyword' => 0,
                                                                                                                                'api_worker' => "", 
                                                                                                                                'update_date' => @now
                                                                                                                           }) 
                                @sogou_db.close()
                                
                            end
                            
                             
                        end
                        
                        
                    end
                end
                
            end
        end
        
        
         
        # the end update status for the group
        db_name = "adgroup_sogou_"+@network_id.to_s
        @list_adgroup = @sogou_db[db_name].find('$and' => [{'cpc_plan_id' => @campaign_id.to_i},{'api_update_ad' => { "$ne" => 0}},{'api_update_keyword' => { "$ne" => 0}},{'api_update_ad' => { '$exists' => true }},'api_update_keyword' => { '$exists' => true }])
        @sogou_db.close() 
        
        if @list_adgroup.count.to_i == 0
          
            
          
            @db["all_campaign"].find({ "$and" => [{:cpc_plan_id => @campaign_id.to_i}, {:network_type => "sogou"}, {:api_update=> 3}] }).update_one('$set'=> {'api_update' => 0, 'api_worker' => "", 'update_date' => @now})
            @db.close 
            
            @db[:network].find({ "$and" => [{:id => @network_id.to_i}, {:type => "sogou"}] }).update_one('$set'=> {'file_update_1' => 4,'file_update_2' => 4,'file_update_3' => 4,'file_update_4' => 4, 'last_update' => @now})
            @db.close
        end
        
        
    end
    
    
    @logger.info "sogou api group done"
    return render :nothing => true
  end
  
  
  def apicampaign
    @logger.info "sogou api campaign start"
    
    
    @campaign_id = params[:id]
    
    if @campaign_id.nil?
        @current_campaign = @db[:all_campaign].find({ "$and" => [{:api_update => 2}, {:network_type => "sogou"}, {:api_worker => @port.to_i}] })
        @db.close
        
        if @current_campaign.count.to_i >= 1
            @logger.info "working, no need update sogou api campaign"
            return render :nothing => true
        end
        
        @campaign = @db[:all_campaign].find({ "$and" => [{:api_update => 1}, {:network_type => "sogou"}, {:api_worker => @port.to_i}] }).sort({ last_update: -1 }).limit(1)
        @db.close
        
        if @campaign.count.to_i == 0
            @logger.info "no need update sogou api campaign"
            return render :nothing => true
        end
        
    else
      
        @campaign = @db[:all_campaign].find({ "$and" => [{:cpc_plan_id => @campaign_id.to_i}, {:network_type => "sogou"}] })
        @db.close
    end
    
    @network_id = 0
    
    if @campaign.count.to_i > 0
        @campaign.no_cursor_timeout.each do |campaign|
          
            # begin
                @network_id = campaign["network_id"].to_i
                @campaign_id = campaign["cpc_plan_id"].to_i
                @campaign_status_body = ""
                
                @db["all_campaign"].find({ "$and" => [{:cpc_plan_id => @campaign_id.to_i}, {:network_type => "sogou"}] }).update_one('$set'=> { 'api_update' => 2 })
                @db.close
                
                @network = @db[:network].find({ "$and" => [{:id => @network_id.to_i}, {:type => "sogou"}] })
                @db.close
                
                if @network.count.to_i > 0
                  
                    @network.no_cursor_timeout.each do |network_d|
                      
                        @tracking_type = network_d["tracking_type"].to_s
                        @ad_redirect = network_d["ad_redirect"].to_s
                        @keyword_redirect = network_d["keyword_redirect"].to_s
                        @company_id = network_d["company_id"].to_s
                        @cookie_length = network_d["cookie_length"].to_s
                      
                        sogou_api(network_d["username"],network_d["password"],network_d["api_token"],"AccountService")
                        sogou_result = @sogou_api.call(:get_account_info)
                        
                        if sogou_result.header[:res_header][:desc].to_s == "success" && sogou_result.header[:res_header][:rquota].to_i >= 500
                            
                                @remain_quote = sogou_result.header[:res_header][:rquota].to_i
                            
                                sogou_api(network_d["username"],network_d["password"],network_d["api_token"],"CpcPlanService")
                                
                                requesttypearray = []
                                requesttype = {}
                                                 
                                requesttypearray << @campaign_id.to_i
                                     
                                @update_status = @sogou_api.call(:get_cpc_plan_by_cpc_plan_id, message: { cpcPlanIds: requesttypearray })
                                @header = @update_status.header.to_hash
                                @msg = @header[:res_header][:desc]
                                @remain_quote = @header[:res_header][:rquota]
                                
                                # @logger.info @remain_quote
                                @logger.info @update_status
                                
                                # @return_num =  @header[:res_header][:oprs]
        
                                if @msg.to_s.downcase == "success"
                                  
                                    @update_status_body = @update_status.body.to_hash
                                    @update_status_body = @update_status_body[:get_cpc_plan_by_cpc_plan_id_response][:cpc_plan_types]
                                        
                                    @campaign_status_body = @update_status_body     
                                        
                                    sogou_api(network_d["username"],network_d["password"],network_d["api_token"],"CpcGrpService")
                                
                                    @update_status = @sogou_api.call(:get_cpc_grp_by_cpc_plan_id, message: { cpcPlanIds: requesttypearray })
                                    @header = @update_status.header.to_hash
                                    @msg = @header[:res_header][:desc]
                                    @remain_quote = @header[:res_header][:rquota]
                                    
                                    # @logger.info @update_status
                                    
                                    if @msg.to_s.downcase == "success" && @remain_quote.to_i >= 500
                                      
                                        @update_status_body = @update_status.body.to_hash
                                        @update_status_body = @update_status_body[:get_cpc_grp_by_cpc_plan_id_response][:cpc_plan_grps]
                                        
                                        @result_adgroup_array = @update_status_body[:cpc_grp_types]
                                        @adgroup_id_array = []
                                        
                                        # @logger.info @update_status
                                    
                                        if @result_adgroup_array.is_a?(Hash)
                                            if !@result_adgroup_array.empty?
                                                @adgroup_id_array << @result_adgroup_array[:cpc_grp_id].to_i
                                                
                                                db_name = "adgroup_sogou_"+@network_id.to_s
                                                result = @sogou_db[db_name].find('cpc_grp_id' => @result_adgroup_array[:cpc_grp_id].to_i).update_one('$set'=> { 
                                                                                                                                                'name' => @result_adgroup_array[:cpc_grp_name].to_s,
                                                                                                                                                'max_price' => @result_adgroup_array[:max_price].to_f,
                                                                                                                                                'pause' => @result_adgroup_array[:pause].to_s,
                                                                                                                                                'status' => @result_adgroup_array[:status].to_i,
                                                                                                                                                'api_update_ad' => 2,
                                                                                                                                                'api_update_keyword' => 2,
                                                                                                                                                'update_date' => @now
                                                                                                                                           })
                                                @sogou_db.close()
                                                
                                                if result.n.to_i == 0
                                                    
                                                    @sogou_db[db_name].insert_one({ 
                                                                                    network_id: @network_id.to_i,
                                                                                    cpc_plan_id: @campaign_id.to_i,
                                                                                    cpc_grp_id: @result_adgroup_array[:cpc_grp_id].to_i,
                                                                                    name: @result_adgroup_array[:cpc_grp_name].to_s,
                                                                                    max_price: @result_adgroup_array[:max_price].to_f,
                                                                                    negative_words: "",
                                                                                    exact_negative_words: "",
                                                                                    pause: @result_adgroup_array[:pause].to_s,
                                                                                    status: @result_adgroup_array[:status].to_i,
                                                                                    opt: "",
                                                                                    update_date: @now,                                            
                                                                                    create_date: @now })
                                                    @sogou_db.close()
                                                end
                                            end
                                        else
                                            
                                            if !@result_adgroup_array.nil? && @result_adgroup_array.count.to_i > 0
                                                @result_adgroup_array.each do |result_adgroup_array|
                                                    
                                                    @adgroup_id_array << result_adgroup_array[:cpc_grp_id].to_i
                                                
                                                    db_name = "adgroup_sogou_"+@network_id.to_s
                                                    result = @sogou_db[db_name].find('cpc_grp_id' => result_adgroup_array[:cpc_grp_id].to_i).update_one('$set'=> { 
                                                                                                                                                        'name' => result_adgroup_array[:cpc_grp_name].to_s,
                                                                                                                                                        'max_price' => result_adgroup_array[:max_price].to_f,
                                                                                                                                                        'pause' => result_adgroup_array[:pause].to_s,
                                                                                                                                                        'status' => result_adgroup_array[:status].to_i,
                                                                                                                                                        'api_update_ad' => 2,
                                                                                                                                                        'api_update_keyword' => 2,
                                                                                                                                                        'update_date' => @now
                                                                                                                                                   })
                                                    @sogou_db.close()
                                                    
                                                    # @logger.info result.n  
                                                    if result.n.to_i == 0
                                                        
                                                        @sogou_db[db_name].insert_one({ 
                                                                                        network_id: @network_id.to_i,
                                                                                        cpc_plan_id: @campaign_id.to_i,
                                                                                        cpc_grp_id: result_adgroup_array[:cpc_grp_id].to_i,
                                                                                        name: result_adgroup_array[:cpc_grp_name].to_s,
                                                                                        max_price: result_adgroup_array[:max_price].to_f,
                                                                                        negative_words: "",
                                                                                        exact_negative_words: "",
                                                                                        pause: result_adgroup_array[:pause].to_s,
                                                                                        status: result_adgroup_array[:status].to_i,
                                                                                        opt: "",
                                                                                        update_date: @now,                                            
                                                                                        create_date: @now })
                                                        @sogou_db.close()     
                                                        
                                                        
                                                    end
                                                end
                                            end
                                        end
                                        
                                        # data = { :tmasdp => @tmp, :status => "true"}
                                        # return render :json => data, :status => :ok
                                        
                                        
                                        if @adgroup_id_array.count.to_i > 0
                                          
                                            sogou_api(network_d["username"],network_d["password"],network_d["api_token"],"CpcIdeaService")
                                            
                                            @update_status = @sogou_api.call(:get_cpc_idea_by_cpc_grp_id, message: { cpcGrpIds: @adgroup_id_array, getTemp: 0 })
                                            @header = @update_status.header.to_hash
                                            @msg = @header[:res_header][:desc]
                                            @remain_quote = @header[:res_header][:rquota]
                                            
                                            if @msg.to_s.downcase == "success" && @remain_quote.to_i >= 500
                                              
                                                @update_status_body = @update_status.body.to_hash
                                                @result_active_grp = @update_status_body[:get_cpc_idea_by_cpc_grp_id_response][:cpc_grp_ideas]
                                                
                                                # @logger.info @update_status_body
                                                
                                                @all_ad = []
                                                
                                                if @result_active_grp.is_a?(Hash)
                                                    if !@result_active_grp.empty?
                                                      
                                                        @result_ad = @result_active_grp[:cpc_idea_types]
                                                      
                                                        if @result_ad.is_a?(Hash)
                                                            if !@result_ad.empty?
                                                                @all_ad << @result_ad
                                                            end
                                                        else 
                                                            if !@result_ad.nil? && @result_ad.count.to_i > 0
                                                                @all_ad = @all_ad + @result_ad
                                                            end
                                                        end
                                                    end
                                                else
                                                    if !@result_active_grp.nil? && @result_active_grp.count.to_i > 0
                                                      
                                                        @result_active_grp.each do |result_adgroup_array|
                                                          
                                                            @result_ad = result_adgroup_array[:cpc_idea_types]  
                                                            
                                                            if @result_ad.is_a?(Hash)
                                                                if !@result_ad.empty?
                                                                    @all_ad << @result_ad
                                                                end
                                                            else 
                                                                if !@result_ad.nil? && @result_ad.count.to_i > 0
                                                                    @all_ad = @all_ad + @result_ad
                                                                end
                                                            end
                                                        end
                                                    end
                                                end
                                                 
                                                # @logger.info @all_ad
                                                
                                                if @all_ad.count.to_i > 0
                                                    @all_ad.each do |all_ad_d|
                                                        
                                                        @logger.info all_ad_d
                                                        
                                                        url_tag = 0
                                                        m_url_tag = 0
                                                        
                                                        if all_ad_d[:visit_url].nil?
                                                            @final_url = ""
                                                        else
                                                            @final_url = all_ad_d[:visit_url]
                                                        end
                                                        
                                                        if all_ad_d[:mobile_visit_url].nil?
                                                            @m_final_url = ""
                                                        else
                                                            @m_final_url = all_ad_d[:mobile_visit_url]  
                                                        end
                                                        
                                                        if all_ad_d[:show_url].nil?
                                                            @show_url = ""
                                                        else
                                                            @show_url = all_ad_d[:show_url]
                                                        end
                                                        
                                                        if all_ad_d[:visit_url].nil?
                                                            @m_show_url = ""
                                                        else  
                                                            @m_show_url = all_ad_d[:mobile_show_url]
                                                        end  
                                                        
                                                        
                                                        if !@final_url.to_s.include?(".adeqo.") && @tracking_type.to_s.downcase == "adeqo" && @ad_redirect.to_s.downcase == "yes" && @final_url.to_s != ""
                                                            @temp_final_url = @final_url
                                                            @final_url = "http://t.adeqo.com/click?company_id="+@company_id.to_s+"&network_id="+@network_id.to_s
                                                            @final_url = @final_url + "&campaign_id="+@campaign_id.to_s+"&adgroup_id="+all_ad_d[:cpc_grp_id].to_s+"&ad_id={creative}&keyword_id={keywordid}"
                                                            @final_url = @final_url + "&cookie="+@cookie_length.to_s
                                                            @final_url = @final_url + "&device=pc"
                                                            @final_url = @final_url + "&tv=v1&durl="+CGI.escape(@temp_final_url.to_s)
                                                            
                                                            url_tag = 0
                                                        end
                                                        
                                                        
                                                        if !@m_final_url.to_s.include?(".adeqo.") && @tracking_type.to_s.downcase == "adeqo" && @ad_redirect.to_s.downcase == "yes" && @m_final_url.to_s != ""
                                                            m_url_tag = 1  
                                                            
                                                            @temp_m_final_url = @m_final_url
                                                            @m_final_url = "http://t.adeqo.com/click?company_id="+@company_id.to_s+"&network_id="+@network_id.to_s
                                                            @final_url = @m_final_url + "&campaign_id="+@campaign_id.to_s+"&adgroup_id="+all_ad_d[:cpc_grp_id].to_s+"&ad_id={creative}&keyword_id={keywordid}"
                                                            @m_final_url = @m_final_url + "&cookie="+@cookie_length.to_s
                                                            @m_final_url = @m_final_url + "&device=mobile"
                                                            @m_final_url = @m_final_url + "&tv=v1&durl="+CGI.escape(@temp_m_final_url.to_s)
                                                        end
                                                        
                                                        if url_tag == 1 || m_url_tag == 1 && @remain_quote.to_i >= 500
                                                            sogou_api(network_d["username"],network_d["password"],network_d["api_token"],"CpcIdeaService")
                                                            requesttypearray = [] 
                                                            requesttype = {}
                                                            requesttype[:cpcIdeaId]    =     all_ad_d[:cpc_idea_id].to_i
                                                            requesttype[:cpcGrpId]    =     0
                                                            requesttype[:visitUrl]    =     @final_url
                                                            requesttype[:mobileVisitUrl] =    @m_final_url
                                                            
                                                            requesttypearray << requesttype
                                                            # @logger.info requesttypearray
                                                            @update_status = @sogou_api.call(:update_cpc_idea, message: { cpcIdeaTypes: requesttypearray })
                                                            
                                                            # @logger.info @update_status
                                                                                     
                                                            @header = @update_status.header.to_hash
                                                            @msg = @header[:res_header][:desc]
                                                            @remain_quote = @header[:res_header][:rquota]
                                                            
                                                            if @msg.to_s.downcase != "success"
                                                                @final_url = all_ad_d[:visit_url].to_s
                                                                @m_final_url = all_ad_d[:mobile_visit_url].to_s
                                                            end
                                                        end
                                                        
                                                        
                                                        db_name = "ad_sogou_"+@network_id.to_s
                                                        result = @sogou_db[db_name].find('cpc_idea_id' => all_ad_d[:cpc_idea_id].to_i).update_one('$set'=> { 
                                                                                                                                                  'title' => all_ad_d[:title].to_s,
                                                                                                                                                  'description_1' => all_ad_d[:description1].to_s,
                                                                                                                                                  'description_2' => all_ad_d[:description2].to_s,
                                                                                                                                                  'visit_url' => @final_url.to_s,
                                                                                                                                                  'mobile_visit_url' => @m_final_url.to_s,
                                                                                                                                                  'show_url' => @show_url.to_s,
                                                                                                                                                  'mobile_show_url' => @m_show_url.to_s,
                                                                                                                                                  'pause' => all_ad_d[:pause].to_s,
                                                                                                                                                  'status' => all_ad_d[:status].to_i,
                                                                                                                                                  'update_date' => @now
                                                                                                                                             })
                                                        @sogou_db.close()
                                                        
                                                        
                                                        if result.n.to_i == 0
                                                          
                                                            @sogou_db[db_name].insert_one({ 
                                                                                            network_id: @network_id.to_i,
                                                                                            cpc_plan_id: @campaign_id.to_i, 
                                                                                            cpc_grp_id: all_ad_d[:cpc_grp_id].to_i,
                                                                                            cpc_idea_id: all_ad_d[:cpc_idea_id].to_i,
                                                                                            cpc_idea_id_2: "",
                                                                                            title: all_ad_d[:title].to_s, 
                                                                                            description_1: all_ad_d[:description1].to_s, 
                                                                                            description_2: all_ad_d[:description2].to_s, 
                                                                                            visit_url: @final_url.to_s,
                                                                                            show_url: @show_url.to_s,
                                                                                            mobile_visit_url: @m_final_url.to_s,
                                                                                            mobile_show_url: @m_show_url.to_s,
                                                                                            pause: all_ad_d[:pause].to_s,
                                                                                            status: all_ad_d[:status].to_i,
                                                                                            active: "",
                                                                                            idea_not_approve_reason: "",
                                                                                            mobile_visit_not_approve_reason: "",
                                                                                            update_date: @now,                                            
                                                                                            create_date: @now })
                                                            @sogou_db.close()
                                                        end
                                                    
                                                    end
                                                end
                                                
                                                # data = { :tmasdp => @tmp, :status => "true"}
                                                # return render :json => data, :status => :ok
    
                                                # ad done
                                                # keyword start here
                                                
                                                
                                                sogou_api(network_d["username"],network_d["password"],network_d["api_token"],"CpcService")
                                            
                                                @update_status = @sogou_api.call(:get_cpc_by_cpc_grp_id, message: { cpcGrpIds: @adgroup_id_array, getTemp: 0 })
                                                @header = @update_status.header.to_hash
                                                @msg = @header[:res_header][:desc]
                                                @remain_quote = @header[:res_header][:rquota]
                                        
                                                if @msg.to_s.downcase == "success" && @remain_quote.to_i >= 500
                                                    @update_status_body = @update_status.body.to_hash
                                                    @result_active_grp = @update_status_body[:get_cpc_by_cpc_grp_id_response][:cpc_grp_cpcs]
                                                    
                                                    # @logger.info @update_status_body
                                                    
                                                    @all_keyword = []
                                                    # @logger.info @result_active_grp
                                                    
                                                    if @result_active_grp.is_a?(Hash)
                                                        if !@result_active_grp.empty?
    #                                                       
                                                            @result_keyword = @result_active_grp[:cpc_types]
    #                                                       
                                                            if @result_keyword.is_a?(Hash)
                                                                if !@result_keyword.empty?
                                                                    @all_keyword << @result_keyword
                                                                end
                                                            else 
                                                                if !@result_keyword.nil? && @result_keyword.count.to_i > 0
                                                                    @all_keyword = @all_keyword + @result_keyword
                                                                end
                                                            end
                                                        end
                                                    else
                                                        if !@result_active_grp.nil? && @result_active_grp.count.to_i > 0
    #                                                       
                                                            @result_active_grp.each do |result_adgroup_array|
    #                                                           
                                                                @result_keyword = result_adgroup_array[:cpc_types]  
    #                                                             
                                                                if @result_keyword.is_a?(Hash)
                                                                    if !@result_keyword.empty?
                                                                        @all_keyword << @result_keyword
                                                                    end
                                                                else 
                                                                   
                                                                    if !@result_keyword.nil? && @result_keyword.count.to_i > 0
                                                                        @all_keyword = @all_keyword + @result_keyword
                                                                    end
                                                                    
                                                                end
                                                            end
                                                        end
                                                    end
                                                    
                                                    
                                                    if @all_keyword.count.to_i > 0
                                                        @all_keyword.each do |all_keyword_d|
                                                          
                                                            # @logger.info all_keyword_d
                                                            
                                                            url_tag = 0
                                                            m_url_tag = 0
                                                            
                                                            if all_keyword_d[:visit_url].nil?
                                                                @final_url = ""
                                                            else
                                                                @final_url = all_keyword_d[:visit_url]
                                                            end
                                                            
                                                            if all_keyword_d[:mobile_visit_url].nil?
                                                                @m_final_url = ""
                                                            else
                                                                @m_final_url = all_keyword_d[:mobile_visit_url]  
                                                            end
                                                          
                                                          
                                                            if !@final_url.to_s.include?(".adeqo.") && @tracking_type.to_s.downcase == "adeqo" && @keyword_redirect.to_s.downcase == "yes" && @final_url.to_s != ""
                                                                url_tag = 1
                                                                @temp_final_url = @final_url
                                                                @final_url = "http://t.adeqo.com/click?company_id="+@company_id.to_s+"&network_id="+@network_id.to_s
                                                                @final_url = @final_url + "&campaign_id="+@campaign_id.to_s+"&adgroup_id="+all_keyword_d[:cpc_grp_id].to_s+"&ad_id={creative}&keyword_id={keywordid}"
                                                                @final_url = @final_url + "&cookie="+@cookie_length.to_s
                                                                @final_url = @final_url + "&device=pc"
                                                                @final_url = @final_url + "&tv=v1&durl="+CGI.escape(@temp_final_url.to_s)
                                                            end
                                                            
                                                            if !@m_final_url.to_s.include?(".adeqo.") && @tracking_type.to_s.downcase == "adeqo" && @keyword_redirect.to_s.downcase == "yes" && @m_final_url.to_s != ""
                                                                m_url_tag = 1
                                                                @temp_m_final_url = @m_final_url
                                                                @m_final_url = "http://t.adeqo.com/click?company_id="+@company_id.to_s+"&network_id="+@network_id.to_s
                                                                @m_final_url = @m_final_url + "&campaign_id="+@campaign_id.to_s+"&adgroup_id="+all_keyword_d[:cpc_grp_id].to_s+"&ad_id={creative}&keyword_id={keywordid}"
                                                                @m_final_url = @m_final_url + "&cookie="+@cookie_length.to_s
                                                                @m_final_url = @m_final_url + "&device=mobile"
                                                                @m_final_url = @m_final_url + "&tv=v1&durl="+CGI.escape(@temp_m_final_url.to_s)
                                                            end
                                                            
                                                            if url_tag == 1 || m_url_tag == 1 && @remain_quote.to_i >= 500
                                                
                                                                sogou_api(network_d["username"],network_d["password"],network_d["api_token"],"CpcService")
                                                                requesttypearray = [] 
                                                                requesttype = {}
                                                                requesttype[:cpcId]    =     all_keyword_d[:cpc_id].to_i
                                                                requesttype[:cpc]    =     0
                                                                requesttype[:cpcGrpId]    =     0
                                                                requesttype[:visitUrl]    =     @final_url
                                                                requesttype[:mobileVisitUrl] =    @m_final_url
                                                                
                                                                requesttypearray << requesttype
                                                                @update_status = @sogou_api.call(:update_cpc, message: { cpcTypes: requesttypearray })
                                                                                         
                                                                @header = @update_status.header.to_hash
                                                                @msg = @header[:res_header][:desc]
                                                                @remain_quote = @header[:res_header][:rquota]
                                                                
                                                                if @msg.to_s.downcase != "success"
                                                                    @final_url = all_keyword_d[:visit_url].to_s
                                                                    @m_final_url = all_keyword_d[:mobile_visit_url].to_s
                                                                end
                                                                    
                                                            end
                                                            
                                                            
                                                            db_name = "keyword_sogou_"+@network_id.to_s
                                                            result = @sogou_db[db_name].find('keyword_id' => all_keyword_d[:cpc_id].to_i).update_one('$set'=> { 
                                                                                                                                                      'keyword' => all_keyword_d[:cpc].to_s,
                                                                                                                                                      'price' => all_keyword_d[:price].to_f,
                                                                                                                                                      'visit_url' => @final_url.to_s,
                                                                                                                                                      'mobile_visit_url' => @m_final_url.to_s,
                                                                                                                                                      'match_type' => all_keyword_d[:match_type].to_s,
                                                                                                                                                      'cpc_quality' => all_keyword_d[:cpc_quality].to_f,
                                                                                                                                                      'pause' => all_keyword_d[:pause].to_s,
                                                                                                                                                      'status' => all_keyword_d[:status].to_i,
                                                                                                                                                      'update_date' => @now
                                                                                                                                                 })
                                                            @sogou_db.close()
                                                            
                                                            
                                                            
                                                            if result.n.to_i == 0
                                                              
                                                                @sogou_db[db_name].insert_one({ 
                                                                                                  network_id: @network_id.to_i,
                                                                                                  cpc_plan_id: @campaign_id.to_i, 
                                                                                                  cpc_grp_id: all_keyword_d[:cpc_grp_id].to_i,
                                                                                                  keyword_id: all_keyword_d[:cpc_id].to_i,
                                                                                                  keyword: all_keyword_d[:cpc].to_s,
                                                                                                  price: all_keyword_d[:price].to_f, 
                                                                                                  visit_url: @final_url.to_s,
                                                                                                  mobile_visit_url: @m_final_url.to_s,
                                                                                                  match_type: all_keyword_d[:match_type].to_i,
                                                                                                  pause: all_keyword_d[:pause].to_s,
                                                                                                  status: all_keyword_d[:status].to_i,
                                                                                                  cpc_quality: all_keyword_d[:cpc_quality].to_f,
                                                                                                  active: 0,
                                                                                                  display: 0,
                                                                                                  use_grp_price: 0,
                                                                                                  mobile_match_type: 3,
                                                                                                  keyword_not_show_reason: "",
                                                                                                  keyword_not_approve_reason: "",
                                                                                                  update_date: @now,                                            
                                                                                                  create_date: @now })
                                                                  @sogou_db.close()
                                                            end
                                                          
                                                          
                                                        end
                                                    end
                                                end
                                            end
                                            
                                                                                    
                                            db_name = "adgroup_sogou_"+@network_id.to_s
                                            @sogou_db[db_name].find('cpc_grp_id' => { "$in" => @adgroup_id_array}).update_many('$set'=> { 
                                                                                                                                            'api_update_ad' => 0,
                                                                                                                                            'api_update_keyword' => 0
                                                                                                                                       }) 
                                            @sogou_db.close()
                                            
                                        end
                                        
                                         
                                    end
                                     
                                    # data = {:tmp => @all_keyword, :tmasdp => @tmp, :status => "true"}
                                    # return render :json => data, :status => :ok
                                    
                                    
                                    # data = {:tmp => @update_status_body, :adgroup_id => @adgroup_id_array, :status => "true"}
                                    # return render :json => data, :status => :ok
                                    
                                    if @campaign_status_body != ""
                                        @db["all_campaign"].find({ "$and" => [{:cpc_plan_id => @campaign_id.to_i}, {:network_type => "sogou"}] }).update_one('$set'=> { 
                                                                                                                                                      'campaign_name' => @campaign_status_body[:cpc_plan_name].to_s,
                                                                                                                                                      'budget' => @campaign_status_body[:budget].to_f,
                                                                                                                                                      'regions' => @campaign_status_body[:regions],
                                                                                                                                                      'negative_words' => @campaign_status_body[:negative_words].to_s,
                                                                                                                                                      'schedule' => @campaign_status_body[:schedule].to_s,
                                                                                                                                                      'pause' => @campaign_status_body[:pause].to_s,
                                                                                                                                                      'join_union' => @campaign_status_body[:join_union].to_s,
                                                                                                                                                      'mobile_price_rate' => @campaign_status_body[:mobile_price_rate].to_f,
                                                                                                                                                      'status' => @campaign_status_body[:status].to_i
                                                                                                                                                   })
                                        @db.close
                                    end
                                end
                        end
                    end
                    
                    
                    
                end
                
                @db["all_campaign"].find({ "$and" => [{:cpc_plan_id => @campaign_id.to_i}, {:network_type => "sogou"}] }).update_one('$set'=> {'api_update' => 0, 'api_worker' => "", 'update_date' => @now})
                @db.close
              
            # rescue Exception
    #           
                # @db["all_campaign"].find('cpc_plan_id' => @campaign_id.to_i,'network_type' => "sogou").update_one('$set'=> { 'api_update' => 1 })
                # @db.close
    #             
                # @db[:network].find('type' => 'sogou', 'id' => campaign["network_id"].to_i).update_one('$set'=> {'file_update_1' => 3,'file_update_2' => 3,'file_update_3' => 3,'file_update_4' => 3, 'last_update' => @now})
                # @db.close
            # end
            
            
            
            @list_campaign = @db["all_campaign"].find( '$and' => [ { 'api_update' => { '$exists' => true } }, {'network_id' => @network_id.to_i}, {'network_type' => "sogou"},{'api_update' => { "$ne" => 0}},{'api_update' => { "$ne" => 0}} ])
            @db.close
            
            
            if @list_campaign.count.to_i == 0
                @db[:network].find({ "$and" => [{:id => @network_id.to_i}, {:type => "sogou"}] }).update_one('$set'=> {'file_update_1' => 4,'file_update_2' => 4,'file_update_3' => 4,'file_update_4' => 4, 'last_update' => @now})
                @db.close
            end
            
        end
    end
    
    
    
    @logger.info "sogou api done start"
    return render :nothing => true
  end
  
  
  
  
  
  
  
  def campaign 
    @logger.info "sogou campaign start"
    
    @id = params[:id]
    if @id.nil?
      
        # @current_network = @db[:network].find('type' => 'sogou', 'file_update_1' => 3)
        @current_network = @db[:network].find({ '$or' => [ {'file_update_1' => 3},{'file_update_2' => 3},{'file_update_3' => 3},{'file_update_4' => 3} ],'type' => 'sogou', 'worker' => @port.to_i})
        @db.close
        
        if @current_network.count.to_i >= 3
            @logger.info "working, no need update sogou campaign"
            return render :nothing => true
        end
        @network = @db[:network].find({ "$and" => [{:type => 'sogou'}, {:file_update_1 => 2}, {:worker => @port.to_i}] }).sort({ last_update: -1 }).limit(1)
        @db.close

        if @network.count.to_i == 0
            @logger.info "no need update sogou campaign"
            return render :nothing => true
        end
    else
        @network = @db[:network].find({ "$and" => [{:type => 'sogou'}, {:id => @id.to_i}] })
        @db.close
    end
    
    @network.no_cursor_timeout.each do |doc|
        begin
            @do = 1
            
            #check if file exist
            if doc['tmp_file'].to_s != ""
                @tmp_file = "/datadrive/"+doc['tmp_file'].to_s
                if !File.directory?(@tmp_file)
                    redownload(doc["id"])
                    @do = 0
                    @logger.info "sogou " + doc['id'].to_s + " need to re download structure"
                    return render :nothing => true
                end
            end
            
            if @do == 1
            
                @logger.info "sogou campaign " + doc['id'].to_s + " running"
                getsogoufile(doc["username"],doc["password"],doc["api_token"], doc["fileid"].to_s, doc["id"].to_s, doc["tmp_file"].to_s)
                
                if @run_csv == 1
                    @cpcplan = nil
                    csvdetail(@acc_file_id, @acc_file_path, "campaign")
                      
                    if @cpcplan.nil?
                        redownload(doc["id"])
                        @logger.info "sogou campaign " + doc['id'].to_s + " need to re download structure"
                        return render :nothing => true
                    else
                        @logger.info "sogou campaign " + doc["id"].to_s + " updating "
                        
                        @db[:network].find(id: doc["id"].to_i).update_one('$set'=> {'file_update_1' => 3})
                        @db.close
                        
                        @db["all_campaign"].find({ "$and" => [{:network_id => doc["id"].to_i}] }).delete_many
                        @db.close
                        
                        data_arr = []
                        
                        CSV.foreach(@cpcplan, :encoding => 'GB18030').each_with_index do |campaign, index|
                            if index != 0
                              
                                data_hash = {}
                                insert_hash = {}
                              
                                insert_hash[:network_id] = doc["id"].to_i
                                insert_hash[:network_type] = "sogou"
                                insert_hash[:account_name] = doc["name"].to_s
                                insert_hash[:cpc_plan_id] = campaign[0].to_i
                                insert_hash[:campaign_name] = campaign[1].to_s
                                insert_hash[:budget] = campaign[2].to_f
                                insert_hash[:regions] = campaign[3]
                                insert_hash[:exclude_ips] = campaign[5].to_s
                                insert_hash[:negative_words] = campaign[6]
                                insert_hash[:exact_negative_words] = campaign[7]
                                insert_hash[:schedule] = campaign[8]
                                insert_hash[:budget_offline_time] = campaign[9]
                                insert_hash[:show_prob] = campaign[10].to_i
                                insert_hash[:pause] = campaign[11].to_s
                                insert_hash[:join_union] = campaign[12].to_s
                                insert_hash[:union_price] = campaign[13].to_f
                                insert_hash[:status] = campaign[14].to_i
                                insert_hash[:mobile_price_rate] = campaign[15].to_f
                                insert_hash[:opt] = campaign[16].to_s
                                
                                insert_hash[:api_update] = 0
                                insert_hash[:api_worker] = ""
                                
                                insert_hash[:update_date] = @now
                                insert_hash[:create_date] = @now
                                
                                    
                                data_hash[:insert_one] = insert_hash
                                data_arr << data_hash
                              
                                if data_arr.count.to_i > 1000
                                    @db[:all_campaign].bulk_write(data_arr)
                                    @db.close
                                    
                                    data_arr = []
                                end
                                # begin
                                    # insert_campaign(doc["id"],doc["name"],campaign)
                                # rescue Exception
                                    # redownload(doc["id"])
                                    # return render :nothing => true
                                # end
                            end
                        end
                        
                        
                        begin
                          
                            if data_arr.count.to_i > 0
                                @db[:all_campaign].bulk_write(data_arr)
                                @db.close
                            end
                            
                        rescue Exception
                            redownload(doc["id"])
                            return render :nothing => true
                        end
                        
                        update_account
                        @logger.info "sogou campaign " + doc['id'].to_s + " update done"
                        @db[:network].find(id: doc["id"].to_i).update_one('$set'=> {'file_update_1' => 4, 'last_update' => @now})
                        @db.close
                        
                    end
                end
            end
        rescue Exception
            redownload(doc["id"])
            return render :nothing => true
        end
    end
    @logger.info "sogou campaign done"
    return render :nothing => true 
  end 
    
    
  
  
  def adgroup
    @logger.info "called sogou adgroup"
    
    @id = params[:id]
    if @id.nil?
      
        @current_network = @db[:network].find({ '$or' => [ {'file_update_1' => 3},{'file_update_2' => 3},{'file_update_3' => 3},{'file_update_4' => 3} ],'type' => 'sogou', 'worker' => @port.to_i})
        @db.close
        
        if @current_network.count.to_i >= 2
            @logger.info "working, no need update sogou adgroup"
            return render :nothing => true
        end
        
        @network = @db[:network].find({ "$and" => [{:type => 'sogou'}, {:file_update_1 => 4}, {:file_update_2 => 2}, {:worker => @port.to_i}] }).sort({ last_update: -1 }).limit(1)
        @db.close
        
        if @network.count.to_i == 0
            @logger.info "no need to update sogou adgroup"
            return render :nothing => true
        end
    else
        
        
      
        @network = @db[:network].find({ "$and" => [{:type => 'sogou'}, {:id => @id.to_i}] })
        @db.close
    end
    
    
    
    @network.no_cursor_timeout.each do |doc|
        
        begin
            @do = 1
            
            # check if file exist
            if doc['tmp_file'].to_s != ""
                @tmp_file = "/datadrive/"+doc['tmp_file'].to_s
                if !File.directory?(@tmp_file)
                    redownload(doc["id"])
                    @do = 0
                    @logger.info "sogou " + doc['id'].to_s + " need to re download structure"
                    return render :nothing => true
                end
            end
                
                
            if @do == 1
                @logger.info "sogou adgroup " + doc['id'].to_s + " running"
                getsogoufile(doc["username"],doc["password"],doc["api_token"], doc["fileid"].to_s, doc["id"].to_s, doc["tmp_file"].to_s)
                
                if @run_csv == 1
                    @adgroup = nil
                    csvdetail(@acc_file_id, @acc_file_path, "adgroup")
                    
                    if @adgroup.nil?
                        redownload(doc["id"])
                        @logger.info "sogou adroup " + doc['id'].to_s + " need to re download structure"
                        return render :nothing => true
                    else
                      
                        @logger.info "sogou adgroup " + doc["id"].to_s + " updating "
                        @db[:network].find(id: doc["id"].to_i).update_one('$set'=> {'file_update_2' => 3})
                        @db.close
                        
                        db_name = "adgroup_sogou_"+doc['id'].to_s
                        @sogou_db[db_name].drop
                        @sogou_db.close()
                        
                        begin
                        @sogou_db[db_name].indexes.create_one(network_id: Mongo::Index::ASCENDING)
                        @sogou_db[db_name].indexes.create_one(cpc_plan_id: Mongo::Index::ASCENDING)
                        @sogou_db[db_name].indexes.create_one(cpc_grp_id: Mongo::Index::ASCENDING)
                        @sogou_db[db_name].indexes.create_one(name: Mongo::Index::ASCENDING)
                        @sogou_db[db_name].indexes.create_one(max_price: Mongo::Index::ASCENDING)
                        @sogou_db[db_name].indexes.create_one(negative_words: Mongo::Index::ASCENDING)
                        @sogou_db[db_name].indexes.create_one(exact_negative_words: Mongo::Index::ASCENDING)
                        @sogou_db[db_name].indexes.create_one(pause: Mongo::Index::ASCENDING)
                        @sogou_db[db_name].indexes.create_one(status: Mongo::Index::ASCENDING)
                        @sogou_db[db_name].indexes.create_one(opt: Mongo::Index::ASCENDING)
                        @sogou_db[db_name].indexes.create_one(api_update_ad: Mongo::Index::ASCENDING)
                        @sogou_db[db_name].indexes.create_one(api_update_keyword: Mongo::Index::ASCENDING)
                        @sogou_db[db_name].indexes.create_one(api_worker: Mongo::Index::ASCENDING)
                        @sogou_db[db_name].indexes.create_one(update_date: Mongo::Index::ASCENDING)
                        @sogou_db[db_name].indexes.create_one(create_date: Mongo::Index::ASCENDING)
                        rescue Exception
                        end
                        
                        data_arr = []
                        
                        CSV.foreach(@adgroup, :encoding => 'GB18030').each_with_index do |adgroup, index|  
                            if index != 0
                            
                                data_hash = {}
                                insert_hash = {}
                              
                                insert_hash[:network_id] = doc["id"].to_i
                                insert_hash[:cpc_plan_id] = adgroup[0].to_i
                                insert_hash[:cpc_grp_id] = adgroup[1].to_i
                                insert_hash[:name] = adgroup[2].to_s
                                insert_hash[:max_price] = adgroup[3].to_f
                                insert_hash[:negative_words] = adgroup[4]
                                insert_hash[:exact_negative_words] = adgroup[5]
                                insert_hash[:pause] = adgroup[6].to_s
                                insert_hash[:status] = adgroup[7].to_i
                                insert_hash[:opt] = adgroup[8].to_s
                                
                                insert_hash[:api_update_ad] = 0
                                insert_hash[:api_update_keyword] = 0
                                insert_hash[:api_worker] = ""
                                
                                insert_hash[:update_date] = @now
                                insert_hash[:create_date] = @now
                                
                                data_hash[:insert_one] = insert_hash
                                data_arr << data_hash
                              
                                if data_arr.count.to_i > 10000
                          
                                    db_name = "adgroup_sogou_"+doc["id"].to_s
                                  
                                    @sogou_db[db_name].bulk_write(data_arr)
                                    @sogou_db.close()  
                                    
                                    data_arr = []
                                end
                               
                              
                                # begin
                                    # insert_adgroup(doc["id"],adgroup)
                                # rescue Exception
                                    # redownload(doc["id"])
                                    # return render :nothing => true
                                # end
                            end
                        end
                        
                        if data_arr.count.to_i > 0
                          
                            db_name = "adgroup_sogou_"+doc["id"].to_s
                          
                            @sogou_db[db_name].bulk_write(data_arr)
                            @sogou_db.close()  
                        end
                         
                        @logger.info "sogou adgroup " + doc['id'].to_s + " update done"
                        update_account
                        @db[:network].find(id: doc["id"].to_i).update_one('$set'=> {'file_update_2' => 4, 'last_update' => @now})
                        @db.close
                    end  
                end
            end
        rescue Exception
            redownload(doc["id"])
            return render :nothing => true
        end
    end
    @logger.info "adgroup done"
    return render :nothing => true 
  end  
  
  
  
  def ad
    
      @logger.info "called sogou ad"
            
      @id = params[:id]
      if @id.nil?
        
          @current_network = @db[:network].find({ '$or' => [ {'file_update_1' => 3},{'file_update_2' => 3},{'file_update_3' => 3},{'file_update_4' => 3} ],'type' => 'sogou', 'worker' => @port.to_i})
          @db.close
          
          if @current_network.count.to_i >= 2
              @logger.info "working, no need update sogou ad"
              return render :nothing => true
          end
          
          @network = @db[:network].find({ "$and" => [{:type => 'sogou'}, {:file_update_1 => 4}, {:file_update_2 => 4}, {:file_update_3 => 2}, {:worker => @port.to_i}] }).sort({ last_update: -1 }).limit(1)
          @db.close
          
          if @network.count.to_i == 0
            @logger.info "no need to update sogou ad"
            return render :nothing => true
          end
      else
        
          @network = @db[:network].find({ "$and" => [{:type => 'sogou'}, {:id => @id.to_i}] })
          @db.close
          
      end
      
      @network.no_cursor_timeout.each do |doc|
          begin
            
              @tracking_type = doc["tracking_type"].to_s
              @ad_redirect = doc["ad_redirect"].to_s
              @keyword_redirect = doc["keyword_redirect"].to_s
              @company_id = doc["company_id"].to_s
              @cookie_length = doc["cookie_length"].to_s
              
              @do = 1
              @remain_quote = 0
            
              if doc['tmp_file'].to_s != ""
                  @tmp_file = "/datadrive/"+doc['tmp_file'].to_s
                  if !File.directory?(@tmp_file)
                      redownload(doc["id"])                      
                      @do = 0
                      @logger.info "sogou " + doc['id'].to_s + " need to re download structure"
                      return render :nothing => true
                  end
              end
              
              
              if @do == 1
                
                  sogou_api(doc["username"],doc["password"],doc["api_token"],"AccountService")
                  sogou_result = @sogou_api.call(:get_account_info)
                  
                  if sogou_result.header[:res_header][:desc].to_s == "success"
                      @remain_quote = sogou_result.header[:res_header][:rquota].to_i
                  end
                
                  @logger.info "sogou ad " + doc['id'].to_s + " running"
                  getsogoufile(doc["username"],doc["password"],doc["api_token"], doc["fileid"].to_s, doc["id"].to_s, doc["tmp_file"].to_s)
                
                  if @run_csv == 1
                      
                      @ad = nil
                      csvdetail(@acc_file_id, @acc_file_path, "ad")
                        
                      if @ad.nil?
                        redownload(doc["id"])
                        @logger.info "sogou ad " + doc['id'].to_s + " need to re download structure"
                        return render :nothing => true
                      else
                        
                        @logger.info "sogou ad " + doc["id"].to_s + " updating "
                        @db[:network].find(id: doc["id"].to_i).update_one('$set'=> {'file_update_3' => 3})
                        @db.close
                        
                        db_name = "ad_sogou_"+doc['id'].to_s
                        @sogou_db[db_name].drop
                        @sogou_db.close()
                        
                        begin
                        @sogou_db[db_name].indexes.create_one(network_id: Mongo::Index::ASCENDING)
                        @sogou_db[db_name].indexes.create_one(cpc_plan_id: Mongo::Index::ASCENDING)
                        @sogou_db[db_name].indexes.create_one(cpc_grp_id: Mongo::Index::ASCENDING)
                        @sogou_db[db_name].indexes.create_one(cpc_idea_id: Mongo::Index::ASCENDING)
                        @sogou_db[db_name].indexes.create_one(cpc_idea_id_2: Mongo::Index::ASCENDING)
                        @sogou_db[db_name].indexes.create_one(title: Mongo::Index::ASCENDING)
                        @sogou_db[db_name].indexes.create_one(description_1: Mongo::Index::ASCENDING)
                        @sogou_db[db_name].indexes.create_one(description_2: Mongo::Index::ASCENDING)
                        # @sogou_db[db_name].indexes.create_one(visit_url: Mongo::Index::ASCENDING)
                        # @sogou_db[db_name].indexes.create_one(show_url: Mongo::Index::ASCENDING)
                        # @sogou_db[db_name].indexes.create_one(mobile_visit_url: Mongo::Index::ASCENDING)
                        # @sogou_db[db_name].indexes.create_one(mobile_show_url: Mongo::Index::ASCENDING)
                        @sogou_db[db_name].indexes.create_one(pause: Mongo::Index::ASCENDING)
                        @sogou_db[db_name].indexes.create_one(status: Mongo::Index::ASCENDING)
                        @sogou_db[db_name].indexes.create_one(active: Mongo::Index::ASCENDING)
                        @sogou_db[db_name].indexes.create_one(idea_not_approve_reason: Mongo::Index::ASCENDING)
                        @sogou_db[db_name].indexes.create_one(mobile_visit_not_approve_reason: Mongo::Index::ASCENDING)
                        # @sogou_db[db_name].indexes.create_one(watchdog: Mongo::Index::ASCENDING)
                        @sogou_db[db_name].indexes.create_one(response_code: Mongo::Index::ASCENDING)
                        @sogou_db[db_name].indexes.create_one(m_response_code: Mongo::Index::ASCENDING)
                        @sogou_db[db_name].indexes.create_one(update_date: Mongo::Index::ASCENDING)
                        @sogou_db[db_name].indexes.create_one(create_date: Mongo::Index::ASCENDING)
                        rescue Exception
                        end
                                  
                        data_arr = []
                                  
                        CSV.foreach(@ad, :encoding => 'GB18030').each_with_index do |ad, index|
                          if index != 0
                            
                                data_hash = {}
                                insert_hash = {}
                              
                                begin
                                    
                                    url_tag = 0
                                    m_url_tag = 0
                                    
                                    @final_url = ad[7].to_s
                                    @m_final_url = ad[9].to_s
                                            
                                    if !@final_url.to_s.include?(".adeqo.") && @tracking_type.to_s.downcase == "adeqo" && @ad_redirect.to_s.downcase == "yes" && @final_url.to_s != ""
                                        url_tag = 1
                                        
                                        @temp_final_url = @final_url
                                        @final_url = "http://t.adeqo.com/click?company_id="+@company_id.to_s+"&network_id="+doc['id'].to_s
                                        @final_url = @final_url + "&campaign_id="+ad[0].to_s+"&adgroup_id="+ad[1].to_s+"&ad_id={creative}&keyword_id={keywordid}"
                                        @final_url = @final_url + "&cookie="+@cookie_length.to_s
                                        @final_url = @final_url + "&device=pc"
                                        @final_url = @final_url + "&tv=v1&durl="+CGI.escape(@temp_final_url.to_s)
                                    end
                                    
                                    if !@m_final_url.to_s.include?(".adeqo.") && @tracking_type.to_s.downcase == "adeqo" && @ad_redirect.to_s.downcase == "yes" && @m_final_url.to_s != ""
                                        m_url_tag = 1  
                                        
                                        @temp_m_final_url = @m_final_url
                                        @m_final_url = "http://t.adeqo.com/click?company_id="+@company_id.to_s+"&network_id="+doc['id'].to_s
                                        @m_final_url = @m_final_url + "&campaign_id="+ad[0].to_s+"&adgroup_id="+ad[1].to_s+"&ad_id={creative}&keyword_id={keywordid}"
                                        @m_final_url = @m_final_url + "&cookie="+@cookie_length.to_s
                                        @m_final_url = @m_final_url + "&device=mobile"
                                        @m_final_url = @m_final_url + "&tv=v1&durl="+CGI.escape(@temp_m_final_url.to_s)
                                    end
                                    
                                                                        
                                    begin
                                        if url_tag == 1 || m_url_tag == 1
                                            
                                            if @remain_quote.to_i >= 500
                                                sogou_api(doc["username"],doc["password"],doc["api_token"],"CpcIdeaService")
                                                requesttypearray = [] 
                                                requesttype = {}
                                                requesttype[:cpcIdeaId]    =     ad[2].to_i
                                                requesttype[:cpcGrpId]    =     0
                                                requesttype[:visitUrl]    =     @final_url
                                                requesttype[:mobileVisitUrl] =    @m_final_url
                                                
                                                requesttypearray << requesttype
                                                @logger.info requesttypearray
                                                @update_status = @sogou_api.call(:update_cpc_idea, message: { cpcIdeaTypes: requesttypearray })
                                                
                                                @logger.info @update_status
                                                                         
                                                @header = @update_status.header.to_hash
                                                @msg = @header[:res_header][:desc]
                                                @remain_quote = @header[:res_header][:rquota]
                                                
                                                if @msg.to_s.downcase != "success"
                                                    @final_url = ad[7].to_s
                                                    @m_final_url = ad[9].to_s
                                                end
                                            end    
                                        end
                                    rescue Exception
                                        @final_url = ad[7].to_s
                                        @m_final_url = ad[9].to_s
                                    end
                                    
                                    
                                    
                                    
                                    insert_hash[:network_id] = doc["id"].to_i
                                    insert_hash[:cpc_plan_id] = ad[0].to_i
                                    insert_hash[:cpc_grp_id] = ad[1].to_i
                                    insert_hash[:cpc_idea_id] = ad[2].to_i
                                    insert_hash[:cpc_idea_id_2] = ad[3].to_s
                                    insert_hash[:title] = ad[4].to_s
                                    insert_hash[:description_1] = ad[5].to_s
                                    insert_hash[:description_2] = ad[6].to_s
                                    insert_hash[:visit_url] = @final_url.to_s
                                    insert_hash[:show_url] = ad[8].to_s
                                    insert_hash[:mobile_visit_url] = @m_final_url.to_s
                                    insert_hash[:mobile_show_url] = ad[10].to_s
                                    insert_hash[:pause] = ad[11].to_s
                                    insert_hash[:status] = ad[12].to_i
                                    insert_hash[:active] = ad[13].to_s
                                    insert_hash[:idea_not_approve_reason] = ad[14]
                                    insert_hash[:mobile_visit_not_approve_reason] = ad[15]
                                    insert_hash[:response_code] = ""
                                    insert_hash[:m_response_code] = ""
                                    insert_hash[:update_date] = @now
                                    insert_hash[:create_date] = @now
                                    
                                    
                                    data_hash[:insert_one] = insert_hash
                                    data_arr << data_hash
                                  
                                    if data_arr.count.to_i > 20000
                              
                                        db_name = "ad_sogou_"+doc["id"].to_s
                                      
                                        @sogou_db[db_name].bulk_write(data_arr)
                                        @sogou_db.close()  
                                        
                                        data_arr = []
                                    end
                                    
                                    
                                    # insert_ad(doc["id"],ad,@final_url,@m_final_url)
                                    
                                rescue Exception
                                    redownload(doc["id"])
                                    return render :nothing => true
                                end
                          end
                        end
                        
                        
                        if data_arr.count.to_i > 0
                            db_name = "ad_sogou_"+doc["id"].to_s
                          
                            @sogou_db[db_name].bulk_write(data_arr)
                            @sogou_db.close()  
                            
                        end
                        
                        
                        @logger.info "sogou ad " + doc['id'].to_s + " update done"
                        update_account
                        @db[:network].find(id: doc["id"].to_i).update_one('$set'=> {'file_update_3' => 4, 'last_update' => @now})
                        @db.close          
                      end
                  end
              end
          rescue Exception
              redownload(doc["id"])
              return render :nothing => true
          end                        
      end
      @logger.info "ad done"
      return render :nothing => true 
  end
    
    
    
    
    
    
  def keyword
    @logger.info "called sogou keyword"    
    
    @id = params[:id]
    if @id.nil?
      
        @current_network = @db[:network].find({ '$or' => [ {'file_update_1' => 3},{'file_update_2' => 3},{'file_update_3' => 3},{'file_update_4' => 3} ],'type' => 'sogou', 'worker' => @port.to_i})
        @db.close
        
        if @current_network.count.to_i >= 2
            @logger.info "working, no need update sogou keyword"
            return render :nothing => true
        end
        
        @network = @db[:network].find({ "$and" => [{:type => 'sogou'}, {:file_update_1 => 4}, {:file_update_2 => 4}, {:file_update_3 => 4}, {:file_update_4 => 2}, {:worker => @port.to_i}] }).sort({ last_update: -1 }).limit(1)
        @db.close
        
        if @network.count.to_i == 0
            @logger.info "no need to update sogou keyword"
            return render :nothing => true
        end
    else
      
        @network = @db[:network].find({ "$and" => [{:type => 'sogou'}, {:id => @id.to_i}] })
        @db.close
    end
    
    
    
    @network.no_cursor_timeout.each do |doc|
          begin
            
              @tracking_type = doc["tracking_type"].to_s
              @ad_redirect = doc["ad_redirect"].to_s
              @keyword_redirect = doc["keyword_redirect"].to_s
              @company_id = doc["company_id"].to_s
              @cookie_length = doc["cookie_length"].to_s
            
              @remain_quote = 0
              @do = 1
            
              if doc['tmp_file'].to_s != ""
                  @tmp_file = "/datadrive/"+doc['tmp_file'].to_s
                  if !File.directory?(@tmp_file)
                      redownload(doc["id"])
                      @do = 0
                      @logger.info "sogou " + doc['id'].to_s + " need to re download structure"
                      return render :nothing => true
                  end
              end
              
              sogou_api(doc["username"],doc["password"],doc["api_token"],"AccountService")
              sogou_result = @sogou_api.call(:get_account_info)
              
              if sogou_result.header[:res_header][:desc].to_s == "success"
                  @remain_quote = sogou_result.header[:res_header][:rquota].to_i    
              end
              
              
              getsogoufile(doc["username"],doc["password"],doc["api_token"], doc["fileid"].to_s, doc["id"].to_s, doc["tmp_file"].to_s)
              @logger.info "sogou keyword " + doc['id'].to_s + " running"
              
              if @run_csv == 1
                  
                  @keyword = nil
                  csvdetail(@acc_file_id, @acc_file_path, "keyword")
                  
                  if @keyword.nil? 
                    redownload(doc["id"])
                    @logger.info "sogou keyword " + doc['id'].to_s + " need to re download structure"
                    return render :nothing => true
                  else
                    @logger.info "sogou keyword " + doc["id"].to_s + " updating "
                    @db[:network].find(id: doc["id"].to_i).update_one('$set'=> {'file_update_4' => 3})
                    @db.close
                    
                    db_name = "keyword_sogou_"+doc['id'].to_s
                    @sogou_db[db_name].drop
                    @sogou_db.close()
                              
                    begin
                    @sogou_db[db_name].indexes.create_one(network_id: Mongo::Index::ASCENDING)
                    @sogou_db[db_name].indexes.create_one(cpc_plan_id: Mongo::Index::ASCENDING)
                    @sogou_db[db_name].indexes.create_one(cpc_grp_id: Mongo::Index::ASCENDING)
                    @sogou_db[db_name].indexes.create_one(keyword_id: Mongo::Index::ASCENDING)
                    @sogou_db[db_name].indexes.create_one(keyword: Mongo::Index::ASCENDING)
                    @sogou_db[db_name].indexes.create_one(price: Mongo::Index::ASCENDING)
                    # @sogou_db[db_name].indexes.create_one(visit_url: Mongo::Index::ASCENDING)
                    # @sogou_db[db_name].indexes.create_one(mobile_visit_url: Mongo::Index::ASCENDING)
                    @sogou_db[db_name].indexes.create_one(match_type: Mongo::Index::ASCENDING)
                    @sogou_db[db_name].indexes.create_one(pause: Mongo::Index::ASCENDING)
                    @sogou_db[db_name].indexes.create_one(status: Mongo::Index::ASCENDING)
                    @sogou_db[db_name].indexes.create_one(cpc_quality: Mongo::Index::ASCENDING)
                    @sogou_db[db_name].indexes.create_one(active: Mongo::Index::ASCENDING)
                    @sogou_db[db_name].indexes.create_one(display: Mongo::Index::ASCENDING)
                    @sogou_db[db_name].indexes.create_one(use_grp_price: Mongo::Index::ASCENDING)
                    @sogou_db[db_name].indexes.create_one(mobile_match_type: Mongo::Index::ASCENDING)
                    @sogou_db[db_name].indexes.create_one(keyword_not_show_reason: Mongo::Index::ASCENDING)
                    @sogou_db[db_name].indexes.create_one(keyword_not_approve_reason: Mongo::Index::ASCENDING)
                    # @sogou_db[db_name].indexes.create_one(watchdog: Mongo::Index::ASCENDING)
                    @sogou_db[db_name].indexes.create_one(response_code: Mongo::Index::ASCENDING)
                    @sogou_db[db_name].indexes.create_one(m_response_code: Mongo::Index::ASCENDING)
                    @sogou_db[db_name].indexes.create_one(update_date: Mongo::Index::ASCENDING)
                    @sogou_db[db_name].indexes.create_one(create_date: Mongo::Index::ASCENDING)
                    rescue Exception
                    end
                    
                    
                    data_arr = []
                           
                    
                    CSV.foreach(@keyword, :encoding => 'GB18030').each_with_index do |keyword, index|
                      if index != 0
                          begin
                              
                              @logger.info index.to_s
                            
                              url_tag = 0
                              m_url_tag = 0
                              
                              @final_url = keyword[5].to_s
                              @m_final_url = keyword[6].to_s
                              
                              if !@final_url.to_s.include?(".adeqo.") && @tracking_type.to_s.downcase == "adeqo" && @keyword_redirect.to_s.downcase == "yes" && @final_url.to_s != ""
                                  url_tag = 1
                                  
                                  @temp_final_url = @final_url
                                  @final_url = "http://t.adeqo.com/click?company_id="+@company_id.to_s+"&network_id="+doc["id"].to_i.to_s
                                  @final_url = @final_url + "&campaign_id="+keyword[0].to_s+"&adgroup_id="+keyword[1].to_s+"&ad_id={creative}&keyword_id={keywordid}"
                                  @final_url = @final_url + "&cookie="+@cookie_length.to_s
                                  @final_url = @final_url + "&device=pc"
                                  @final_url = @final_url + "&tv=v1&durl="+CGI.escape(@temp_final_url.to_s)
                              end
                              
                              if !@m_final_url.to_s.include?(".adeqo.") && @tracking_type.to_s.downcase == "adeqo" && @keyword_redirect.to_s.downcase == "yes" && @m_final_url.to_s != ""
                                  m_url_tag = 1  
                                  
                                  @temp_m_final_url = @m_final_url
                                  @m_final_url = "http://t.adeqo.com/click?company_id="+@company_id.to_s+"&network_id="+doc["id"].to_i.to_s
                                  @m_final_url = @m_final_url + "&campaign_id="+keyword[0].to_s+"&adgroup_id="+keyword[1].to_s+"&ad_id={creative}&keyword_id={keywordid}"
                                  @m_final_url = @m_final_url + "&cookie="+@cookie_length.to_s
                                  @m_final_url = @m_final_url + "&device=mobile"
                                  @m_final_url = @m_final_url + "&tv=v1&durl="+CGI.escape(@temp_m_final_url.to_s)
                              end
                              
                              @add_url = @final_url.to_s
                              @add_m_url = @m_final_url.to_s
                              
                              begin
                                
                                  if url_tag == 1 || m_url_tag == 1
                                            
                                      if @remain_quote.to_i >= 500
                                          sogou_api(doc["username"],doc["password"],doc["api_token"],"CpcService")
                                          requesttypearray = [] 
                                          requesttype = {}
                                          requesttype[:cpcId]    =     keyword[2].to_i
                                          requesttype[:cpc]    =     0
                                          requesttype[:cpcGrpId]    =     0
                                          requesttype[:visitUrl]    =     @final_url
                                          requesttype[:mobileVisitUrl] =    @m_final_url
                                          
                                          requesttypearray << requesttype
                                          # @logger.info requesttypearray
                                          @update_status = @sogou_api.call(:update_cpc, message: { cpcTypes: requesttypearray })
                                                                   
                                          @header = @update_status.header.to_hash
                                          @msg = @header[:res_header][:desc]
                                          @remain_quote = @header[:res_header][:rquota]
                                          
                                          # @logger.info @header
                                          
                                          if @msg.to_s.downcase != "success"
                                              @add_url = keyword[5].to_s
                                              @add_m_url = keyword[6].to_s
                                          end
                                          
                                      end    
                                  end
                              
                              rescue Exception
                                  @add_url = keyword[5].to_s
                                  @add_m_url = keyword[6].to_s
                              end
                              
                              data_hash = {}
                              insert_hash = {}
                            
                              insert_hash[:network_id] = doc["id"].to_i
                              insert_hash[:cpc_plan_id] = keyword[0].to_i
                              insert_hash[:cpc_grp_id] = keyword[1].to_i
                              insert_hash[:keyword_id] = keyword[2].to_i
                              insert_hash[:keyword] = keyword[3].to_s
                              insert_hash[:price] = keyword[4]
                              insert_hash[:visit_url] = @add_url.to_s
                              insert_hash[:mobile_visit_url] = @add_m_url.to_s
                              insert_hash[:match_type] = keyword[7].to_i
                              insert_hash[:pause] = keyword[8].to_s
                              insert_hash[:status] = keyword[9].to_i
                              insert_hash[:cpc_quality] = keyword[10].to_f
                              insert_hash[:active] = keyword[11].to_i
                              insert_hash[:display] = keyword[12].to_i
                              insert_hash[:use_grp_price] = keyword[13].to_i
                              insert_hash[:mobile_match_type] = keyword[14].to_f
                              insert_hash[:keyword_not_show_reason] = keyword[15]
                              insert_hash[:keyword_not_approve_reason] = keyword[16]
                              insert_hash[:response_code] = ""
                              insert_hash[:m_response_code] = ""
                              insert_hash[:update_date] = @now
                              insert_hash[:create_date] = @now
                              
                              data_hash[:insert_one] = insert_hash
                              data_arr << data_hash
                            
                              
                              if data_arr.count.to_i > 10000
                                  
                                  db_name = "keyword_sogou_"+doc["id"].to_s
                                
                                  @sogou_db[db_name].bulk_write(data_arr)
                                  @sogou_db.close()  
                                  
                                  data_arr = []
                                  
                                  @logger.info db_name.to_s
                              end
                              
                              
                              # insert_keyword(doc["id"],keyword,@add_url,@add_m_url)
                          
                          rescue Exception
                              redownload(doc["id"])
                              return render :nothing => true
                          end
                      end
                    end  
                      
                    
                    if data_arr.count.to_i > 0
                        
                        db_name = "keyword_sogou_"+doc["id"].to_s
                      
                        @sogou_db[db_name].bulk_write(data_arr)
                        @sogou_db.close()  
                        
                        @logger.info db_name.to_s
                        
                    end
                    
                      
                    @logger.info "sogou keyword " + doc['id'].to_s + " update done"
                    update_account
                    @db[:network].find(id: doc["id"].to_i).update_one('$set'=> {'file_update_4' => 4, 'last_update' => @now, 'worker' => ""})
                    @db.close     
                    
                    if doc["tmp_file"] != ""
                        unzip_folder = @tmp+"/"+doc["tmp_file"]
                        if File.directory?(unzip_folder)
                            FileUtils.remove_dir unzip_folder, true
                        end
                    end
                      
                  end
              end                         
          rescue Exception
              redownload(doc["id"])
              return render :nothing => true
          end
    end  
    @logger.info "keyword done"
    return render :nothing => true 
  end
  
  
  def avgpositionupper
      @logger.info "called sogou avg position upper"
        
      @days = params[:day]
      @default_day = 1
      
      if !@days.nil?
        @default_day = @days  
      end
      
      @id = params[:id]
      
      if @id.nil?
        
          if @days.nil?
              @current_network = @db[:network].find({ "$and" => [{:type => 'sogou'}, {:avg_pos_upper => 1}, {:avgupper_worker => @port.to_i}] })
              @db.close
              
              if @current_network.count.to_i >= 1
                  @logger.info "one sogou avg pos upper working"
                  return render :nothing => true
              end
              
              
              @network = @db[:network].find({ "$and" => [{:type => 'sogou'}, {:report => 2}, {:avg_pos => 2}, {:avg_pos_upper => 0}, {:avgupper_worker => @port.to_i}] }).sort({ last_update: -1 }).limit(1)
              @db.close
              
              if @network.count.to_i == 0
                  @network = @db[:network].find({ "$and" => [{:type => 'sogou'}, {:report => 2}, {:avg_pos => 2}, {:avg_pos_upper => 0}, {:avgupper_worker => ""}] }).sort({ last_update: -1 }).limit(1)
                  @db.close
              end
              
              
          else
              @network = @db[:network].find('type' => 'sogou')
              @db.close  
          end
          
      else
          @network = @db[:network].find({ "$and" => [{:type => 'sogou'}, {:id => @id.to_i}] })
          @db.close
      end
      
    
      @today = Date.today.in_time_zone('Beijing') 
      edit_day = @today - @default_day.to_i.days
      @today = edit_day.strftime("%Y-%m-%d")
      
      
      @network.no_cursor_timeout.each do |doc|
        
             begin
             @logger.info "sogou avg position upper network " + doc['id'].to_s + " running"
             
             if @id.nil? && @days.nil?
                 @db[:network].find('id' => doc["id"].to_i).update_one('$set'=> { 'avg_pos_upper' => 1,'last_update' => @now, 'avgupper_worker' => @port.to_i })
                 @db.close
             end
            
            
             @logger.info "sogou avg position network " + doc['id'].to_s + " update adgroup"
             
             db_name = "adgroup_sogou_"+doc['id'].to_s
             @adgroup = @sogou_db[db_name].find()
             
             temp_adgroup_id_arr = []
             temp_adgroup_id_hash = {}
             
             
             
             if @adgroup.count.to_i > 0
                @adgroup.no_cursor_timeout.each do |adgroup|
                    temp_adgroup_id_arr << adgroup["cpc_grp_id"].to_i
    
                    temp_adgroup_id_hash["display"+adgroup["cpc_grp_id"].to_s] = 0
                    temp_adgroup_id_hash["avg_pos"+adgroup["cpc_grp_id"].to_s] = 0
                end
             end
             
             
             if temp_adgroup_id_arr.count.to_i
                @keyword_report = @db3[:sogou_report_keyword].find('cpc_grp_id' => { "$in" => temp_adgroup_id_arr}, "report_date" => @today.to_s)
                @db3.close()
                 
                if @keyword_report.count.to_i > 0
      
                    @keyword_report.no_cursor_timeout.each do |keyword_report|
                        temp_adgroup_id_hash["display"+keyword_report["cpc_grp_id"].to_s] = temp_adgroup_id_hash["display"+keyword_report["cpc_grp_id"].to_s] + keyword_report["display"].to_i
                        if keyword_report['display'].to_i > 0 && keyword_report['avg_position'].to_f > 0
                          temp_adgroup_id_hash["avg_pos"+keyword_report["cpc_grp_id"].to_s] = temp_adgroup_id_hash["avg_pos"+keyword_report["cpc_grp_id"].to_s].to_f.round(2) + (keyword_report['avg_position'].to_f.round(2) * keyword_report['display'].to_f)
                        end
                    end
                  
                end
             end
             
             
             if @adgroup.count.to_i > 0
                @adgroup.no_cursor_timeout.each do |adgroup|
                    if temp_adgroup_id_hash["display"+adgroup["cpc_grp_id"].to_s].to_i > 0 && temp_adgroup_id_hash["avg_pos"+adgroup["cpc_grp_id"].to_s].to_f > 0
                    
                        insert_avg_value = temp_adgroup_id_hash["avg_pos"+adgroup["cpc_grp_id"].to_s].to_f / temp_adgroup_id_hash["display"+adgroup["cpc_grp_id"].to_s].to_f
                        
                        @db3[:sogou_report_adgroup].find({ "$and" => [{:cpc_grp_id => adgroup["cpc_grp_id"].to_i}, {:report_date => @today.to_s}] }).update_one('$set'=>{     
                                                                                                                                                            avg_position: insert_avg_value.to_f
                                                                                                                                                  })
                        @db3.close()
                    
                    end
                end
             end
             
             @logger.info "shenma avg position network " + doc['id'].to_s + " update adgroup done"
             @logger.info "shenma avg position network " + doc['id'].to_s + " update campaign"
             
             
             
             @campaign = @db["all_campaign"].find('network_id' => doc['id'].to_i, 'network_type' => "sogou")
             @db.close
             
             temp_campaign_id_arr = []
             temp_campaign_id_hash = {}
             
             if @campaign.count.to_i > 0
                 @campaign.no_cursor_timeout.each do |campaign|
                    temp_campaign_id_arr << campaign["cpc_plan_id"]
                    
                    temp_campaign_id_hash["display"+campaign["cpc_plan_id"].to_s] = 0
                    temp_campaign_id_hash["avg_pos"+campaign["cpc_plan_id"].to_s] = 0
                 end
             end
             
             
             @keyword_report = @db3[:sogou_report_keyword].find('cpc_plan_id' => { "$in" => temp_campaign_id_arr}, "report_date" => @today.to_s)
             @db3.close()
             
             if @keyword_report.count.to_i > 0
#       
                @keyword_report.no_cursor_timeout.each do |keyword_report|
                    temp_campaign_id_hash["display"+keyword_report["cpc_plan_id"].to_s] = temp_campaign_id_hash["display"+keyword_report["cpc_plan_id"].to_s] + keyword_report["display"].to_i
                    
                    if keyword_report['display'].to_i > 0 && keyword_report['avg_position'].to_f > 0
                        temp_campaign_id_hash["avg_pos"+keyword_report["cpc_plan_id"].to_s] = temp_campaign_id_hash["avg_pos"+keyword_report["cpc_plan_id"].to_s].to_f.round(2) + (keyword_report['avg_position'].to_f.round(2) * keyword_report['display'].to_f)
                    end
                end
             end
             
             
             
             if @campaign.count.to_i > 0
                @campaign.no_cursor_timeout.each do |campaign|
                  
                  
                    if temp_campaign_id_hash["display"+campaign["cpc_plan_id"].to_s].to_i > 0 && temp_campaign_id_hash["avg_pos"+campaign["cpc_plan_id"].to_s].to_f > 0
                    
                        insert_avg_value = temp_campaign_id_hash["avg_pos"+campaign["cpc_plan_id"].to_s].to_f / temp_campaign_id_hash["display"+campaign["cpc_plan_id"].to_s].to_f
                        
                        @db3[:sogou_report_campaign].find({ "$and" => [{:cpc_plan_id => campaign["cpc_plan_id"].to_i}, {:report_date => @today.to_s}] }).update_one('$set'=>{     
                                                                                                                                                            avg_position: insert_avg_value.to_f
                                                                                                                                                  })
                        
                        @db3.close()
                    
                    end
                end
             end
             
             @logger.info "sogou avg position network " + doc['id'].to_s + " update campaign done"
             @logger.info "sogou avg position network " + doc['id'].to_s + " update account"
             
             
             @keyword_report = @db3[:sogou_report_keyword].find('network_id' => doc['id'].to_i, "report_date" => @today.to_s)
             @db3.close()      
             
             temp_account_display = 0
             temp_account_avg_pos = 0
             
             if @keyword_report.count.to_i > 0
                
                @keyword_report.no_cursor_timeout.each do |keyword_report|
                    temp_account_display = temp_account_display + keyword_report["display"].to_i
                    
                    if keyword_report['display'].to_i > 0 && keyword_report['avg_position'].to_f > 0
                        temp_account_avg_pos = temp_account_avg_pos.to_f.round(2) + (keyword_report['display'].to_f * keyword_report['avg_position'].to_f)
                    end
                end
             end
             
             if temp_account_display.to_i > 0 && temp_account_avg_pos.to_f > 0
                insert_avg_value = temp_account_avg_pos.to_f / temp_account_display.to_f
                
                @db3[:sogou_report_account].find({ "$and" => [{:network_id => doc['id'].to_i}, {:report_date => @today.to_s}] }).update_one('$set'=>{     
                                                                                                                                          avg_position: insert_avg_value.to_f
                                                                                                                                })                                                                                                                   
                @db3.close()
             end
             
             
                
                                                                                                                                
             @logger.info "sogou avg position network " + doc['id'].to_s + " update account done"
             
             if @id.nil? && @days.nil?
                 @db[:network].find('id' => doc["id"].to_i).update_one('$set'=> { 'avg_pos_upper' => 2,'last_update' => @now, 'avgupper_worker' => "" })
                 @db.close
             end
             
             rescue Exception
                @logger.info "sogou avg position network " + doc['id'].to_s + " fail"
                
                @db[:network].find('id' => doc["id"].to_i).update_one('$set'=> { 'avg_pos_upper' => 0,'last_update' => @now})
                @db.close
             end
      end   
      
      @logger.info "called sogou avg position upper done"
      return render :nothing => true
  end
  
  
   
  
  def avgposition
    
      @logger.info "called sogou avg position"
        
      @days = params[:day]
      @default_day = 1
      
      if !@days.nil?
        @default_day = @days  
      end
      
      @id = params[:id]
      
      if @id.nil?
        
          if @days.nil?
              @current_network = @db[:network].find({ "$and" => [{:type => 'sogou'}, {:avg_pos => 1}, {:avg_worker => @port.to_i}] })
              @db.close
              
              if @current_network.count.to_i >= 2
                  @logger.info "one sogou avg pos working"
                  return render :nothing => true
              end
              
              @network = @db[:network].find({ "$and" => [{:type => 'sogou'}, {:report => 2}, {:avg_pos => 0}, {:avg_worker => @port.to_i}] }).sort({ last_update: -1 }).limit(1)
              @db.close
              
              if @network.count.to_i == 0
                  @network = @db[:network].find({ "$and" => [{:type => 'sogou'}, {:report => 2}, {:avg_pos => 0}, {:avg_worker => ""}] }).sort({ last_update: -1 }).limit(1)
                  @db.close  
              end
               
              
          else
              @network = @db[:network].find('type' => 'sogou')
              @db.close  
          end
          
      else
        
          @network = @db[:network].find({ "$and" => [{:type => 'sogou'}, {:id => @id.to_i}] })
          @db.close
      end
     
    
      @today = Date.today.in_time_zone('Beijing') 
      edit_day = @today - @default_day.to_i.days
      @today = edit_day.strftime("%Y-%m-%d")
    
      
      @network.no_cursor_timeout.each do |doc|
          
          begin
            
              if @id.nil? && @days.nil?
                  @db[:network].find('id' => doc["id"].to_i).update_one('$set'=> { 'avg_pos' => 1,'last_update' => @now, "avg_worker" => @port.to_i })
                  @db.close
              end
              
              @logger.info "sogou avg position network " + doc['id'].to_s + " running" 
              
              sogou_api(doc["username"],doc["password"],doc["api_token"],"CpcRankService")
              
              @today = Date.today.in_time_zone('Beijing') 
              edit_day = @today - 1.days
              @today = edit_day.strftime("%Y-%m-%d")
              
              for i in 0..1
                  @rank_id = @sogou_api.call(:get_cpc_rank_id, message: { deviceType: i })
                  
                  @header = @rank_id.header.to_hash
                  @return_num =  @header[:res_header][:oprs]
                  
                  @logger.info @header 
                  
                  if @header[:res_header][:desc].to_s != 'failure'
                      if @return_num.to_i == 1
                          @rank_id = @rank_id.body.to_hash
                          @rank_id = @rank_id[:get_cpc_rank_id_response][:rank_id]
                          
                          @report_status = @sogou_api.call(:get_cpc_rank_status, message: { rankId: @rank_id })
                      end
                      
                      @header = @report_status.header.to_hash
                      @return_num =  @header[:res_header][:oprs]
                      
                      @logger.info @header
                      
                      if @header[:res_header][:desc].to_s != 'failure'
                          if @return_num.to_i == 1
                              @report_status = @report_status.body.to_hash
                              @report_status = @report_status[:get_cpc_rank_status_response][:is_generated]
                              
                              if @report_status.to_i == 1
                                  @rank_path = @sogou_api.call(:get_cpc_rank_path, message: { rankId: @rank_id })
                                
                                  @header = @rank_path.header.to_hash
                                  @return_num =  @header[:res_header][:oprs]
                                  
                                  @logger.info @header
                                  
                                  if @return_num.to_i == 1
                                        @rank_path = @rank_path.body.to_hash
                                        @rank_path = @rank_path[:get_cpc_rank_path_response][:rank_path]
                                         
                                        @unzip_name = @tmp+"/"+@rank_id+"_avg"
                                        @zip_file = @tmp+"/"+@rank_id+"_avg" + ".zip"
                                        
                                        
                                        open(@zip_file.to_s, 'wb') do |file|
                                          file << open(@rank_path.to_s).read
                                        end
                                        
                                        unzip_file(@zip_file.to_s, @unzip_name.to_s)
                                        File.delete(@zip_file)
                                        
                                        @unzip_folder = @unzip_name + "/*"
                                        @files = Dir.glob(@unzip_folder)
                                        
                                        if i == 0
                                              
                                              @logger.info "sogou avg position network " + doc['id'].to_s + " update keyword"
                                              
                                              @files.each_with_index do |file, index|
                                                  @rank = CSV.read(file, :encoding => 'GB18030')
                                                  
                                                  data_arr = []
                                                  @rank.each_with_index do |rank, index|
                                                        if index != 0
                                                            
                                                            # set_detail_hash = {}
                                                            # set_detail_hash[:avg_position] = rank[3].to_f
#                                                             
                                                            # set_hash = {}
                                                            # set_hash['$set'] = set_detail_hash
#                                                             
                                                            # filter_hash = {}
                                                            # filter_hash[:keyword_id] = rank[1].to_i
                                                            # filter_hash[:report_date] = @today.to_s
#                                                             
                                                            # update_hash = {}
                                                            # update_hash[:filter] = filter_hash
                                                            # update_hash[:update] = set_hash
#                                                             
                                                            # data_hash = {}
                                                            # data_hash[:update_one] = update_hash
#                                                             
                                                            # data_arr << data_hash
#                                                             
                                                            # if data_arr.count.to_i > 100
#                                                               
                                                                # @db3[:sogou_report_keyword].bulk_write(data_arr)
                                                                # @db3.close()  
#                                                                 
                                                                # data_arr = []
                                                            # end
                                                                                                                                                                   
                                                            @db3[:sogou_report_keyword].find({ "$and" => [{:keyword_id => rank[1].to_i}, {:report_date => @today.to_s}] }).update_one('$set'=>{     
                                                                                                                                                                    avg_position: rank[3].to_f
                                                                                                                                                                   })                                                                                                       
                                                            @db3.close()
                                                        end
                                                  end 
                                                  
                                                  # if data_arr.count.to_i > 0
                                                      # @db3[:sogou_report_keyword].bulk_write(data_arr)
                                                      # @db3.close()  
                                                  # end                                     
                                              end
                                              
                                              @logger.info "sogou avg position network " + doc['id'].to_s + " update keyword done"
                                        end
                                        
                                        # if doc["id"].to_i != 71
                                        FileUtils.remove_dir @unzip_name, true
                                        # end
                                      
                                   end
                                   
                              else 
                                  @logger.info "sogou avg position network " + doc['id'].to_s + " fail"
                                  
                      
                                  @db[:network].find('id' => doc["id"].to_i).update_one('$set'=> { 'avg_pos' => 0,'last_update' => @now})
                                  @db.close    
                              end
                          end
                      
                      else     
                          @logger.info "sogou avg position network " + doc['id'].to_s + " pending"
                      
                          @db[:network].find('id' => doc["id"].to_i).update_one('$set'=> { 'avg_pos' => 0,'last_update' => @now})
                          @db.close
                      end
                      
                  else
                      @logger.info "sogou avg position network " + doc['id'].to_s + " pending"
                      
                      @db[:network].find('id' => doc["id"].to_i).update_one('$set'=> { 'avg_pos' => 0,'last_update' => @now })
                      @db.close
                  end
              end
              
              @logger.info "sogou avg position network " + doc['id'].to_s + " done"
              
              if @id.nil? && @days.nil?
                  @db[:network].find('id' => doc["id"].to_i).update_one('$set'=> { 'avg_pos' => 2,'last_update' => @now, "avg_worker" => "" })
                  @db.close
              end
          
          rescue Exception
              @logger.info "sogou avg position network " + doc['id'].to_s + " fail"
              
              @db[:network].find('id' => doc["id"].to_i).update_one('$set'=> { 'avg_pos' => 0,'last_update' => @now })
              @db.close
          end
      end 
      
      
      
      @logger.info "sogou avg position done"
      return render :nothing => true
         
  end
  
  def resetreport
    
      @logger.info "sogou reset report"
    
      # @db[:network].find('type' => 'sogou').update_many('$set'=> { 'report' => 0,'avg_pos' => 0,'avg_pos_upper' => 0,'last_update' => @now,'report_worker' => "",'avg_worker' => "",'avgupper_worker' => "" })
      # @db.close
      
      
      @network = @db["network"].find('type' => "sogou")
      @db.close
      
      @network_id_arr = []
      
      if @network.count.to_i >= 0
          @network.no_cursor_timeout.each do |network_d|
              @network_id_arr << network_d["id"]
          end
      end
      
      
      port_array = [81,83,85,89]
      
      @network_id_arr.shuffle
      array_limit = port_array.count.to_i
      arr = @network_id_arr.in_groups(array_limit)
      
      if arr.count.to_i >= 0
          arr.each_with_index do |arr_d, index|
            
            @network = @db["network"].find('id' => { "$in" => arr_d}).update_many('$set'=> { 'report' => 0,'avg_pos' => 0,'avg_pos_upper' => 0,'last_update' => @now,'report_worker' => port_array[index].to_i,'avg_worker' => port_array[index].to_i,'avgupper_worker' =>port_array[index].to_i })
            @db.close
            
          end
      end
      
      
      
      @logger.info "sogou reset report done"
      return render :nothing => true 
  end
    
  def report
      #this one is get data from file
      @days = params[:day]
      @default_day = 1
      
      if !@days.nil?
        @default_day = @days  
      end
      
      @id = params[:id]
      
      
      if @id.nil?
        
          if @days.nil?
            
              @current_network = @db[:network].find({ "$and" => [{:type => 'sogou'}, {:report => 1}, {:report_worker => @port.to_i}] })
              @db.close
              
              if @current_network.count.to_i >= 2
                  @logger.info "one sogou report working"
                  return render :nothing => true
              end
              
              @network = @db[:network].find({ "$and" => [{:type => 'sogou'}, {:report => 0}, {:report_worker => @port.to_i}] }).sort({ last_update: -1 }).limit(1)
              @db.close
              
              if @network.count.to_i == 0
              
                  @network = @db[:network].find({ "$and" => [{:type => 'sogou'}, {:report => 0}, {:report_worker => ""}] }).sort({ last_update: -1 }).limit(1)
                  @db.close
                    
              end
              
          else
              @network = @db[:network].find('type' => 'sogou')
              @db.close  
          end
          
      else
          @network = @db[:network].find({ "$and" => [{:type => 'sogou'}, {:id => @id.to_i}] })
          @db.close
      end
      
      
      
      @today = Date.today.in_time_zone('Beijing') 
      edit_day = @today - @default_day.to_i.days
      @today = edit_day.strftime("%Y-%m-%d")
      
      
      @startdate = @today+"T00:00:00"
      @enddate = @today+"T23:59:59"
      
      @network.no_cursor_timeout.each do |doc|
          
          begin
              
              if @id.nil? && @days.nil?
                  @db[:network].find('id' => doc["id"].to_i).update_one('$set'=> { 'report' => 1,'report_worker' => @port.to_i,'last_update' => @now })
                  @db.close  
              end
                
              sogou_api(doc["username"],doc["password"],doc["api_token"],"ReportService")
              
              performanceData = ['cost','cpc','impression','ctr']
              
              
              
              for i in 1..5
                    requesttype = {}
                    requesttype[:performanceData]    =     performanceData
                    requesttype[:reportType]         =     i
                    requesttype[:startDate] =  @startdate.to_s 
                    requesttype[:endDate]   =  @enddate.to_s
                    
                          
                    @report_id = @sogou_api.call(:get_report_id, message: { reportRequestType: requesttype })
                    
                    @header = @report_id.header.to_hash
                    # @return_num =  @header[:res_header][:oprs]
                    
                    if @header[:res_header][:desc].to_s != 'failure'
                          @report_id = @report_id.body.to_hash
                          @report_id = @report_id[:get_report_id_response][:report_id]
                          
                          @report_status = @sogou_api.call(:get_report_state, message: { reportId: @report_id })
                           
                          @header = @report_status.header.to_hash
                          # @return_num =  @header[:res_header][:oprs]
            
                           
                          if @header[:res_header][:desc].to_s != 'failure'
                                @report_status = @report_status.body.to_hash
                                @report_status = @report_status[:get_report_state_response][:is_generated]
                                
                                if @report_status.to_i == 1
                                    @report_path = @sogou_api.call(:get_report_path, message: { reportId: @report_id })
                                  
                                    @header = @report_path.header.to_hash
                                    # @return_num =  @header[:res_header][:oprs]
                                    
                                    if @header[:res_header][:desc].to_s != 'failure'
                                        @report_path = @report_path.body.to_hash
                                        @report_path = @report_path[:get_report_path_response][:report_file_path]
                                        
                                        @logger.info "report sogou dl, network: " +doc["id"].to_s
                                        
                                        @unzip_name = @tmp+"/"+@report_id+"_report"
                                        @zip_file = @tmp+"/"+@report_id+"_report" + ".zip"
                                        open(@zip_file.to_s, 'wb') do |file|
                                          file << open(@report_path.to_s).read
                                        end
                                        
                                        infile = open(@zip_file.to_s)
                                        gz = Zlib::GzipReader.new(infile)
                                        
                                        # @today = Date.today.in_time_zone('Beijing').strftime("%Y-%m-%d")
                                        
    
                                        @logger.info "report sogou loop start, network: " +doc["id"].to_s
                                        if i == 1
                                          
                                              @db3[:sogou_report_account].find({ "$and" => [{:network_id => doc["id"].to_i}, {:report_date => @today.to_s}] }).delete_many
                                              @db3.close()
                                              
                                              @index = 0
                                              @logger.info "report sogou first loop, network " +doc["id"].to_s
                                              @logger.info @today.to_s
                                              
                                              data_arr = []
                                              gz.each_line do |line|
                                                
                                                    # @logger.info line
                                                    
                                                    if @index.to_i > 1
                                                          line_array = line.force_encoding("GB18030").encode("UTF-8").split(",")
                                                          
                                                          clicks_d = (line_array[5].to_f * line_array[6].strip.to_s.gsub('%', '').to_f ).to_f/100
                                                           
                                                          data_hash = {}
                                                          insert_hash = {}
                                                           
                                                          insert_hash[:network_id] = doc["id"].to_i
                                                          insert_hash[:report_date] = @today.to_s
                                                          insert_hash[:name] = doc["name"].to_s
                                                          insert_hash[:total_cost] = line_array[3].to_f
                                                          insert_hash[:clicks_avg_price] = line_array[4].to_f
                                                          insert_hash[:display] = line_array[5].to_i
                                                          insert_hash[:click_rate] = line_array[6].strip.to_s
                                                          insert_hash[:clicks] = clicks_d.to_i
                                                          insert_hash[:avg_position] = 0 
                                                          
                                                          data_hash[:insert_one] = insert_hash
                                                          data_arr << data_hash
                                                          
                                                          # @db3[:sogou_report_account].insert_one({
                                                                                        # network_id: doc["id"].to_i,
                                                                                        # report_date: @today.to_s,
                                                                                        # name: doc["name"].to_s,
                                                                                        # total_cost: line_array[3].to_f,
                                                                                        # clicks_avg_price: line_array[4].to_f,
                                                                                        # display:  line_array[5].to_i,
                                                                                        # click_rate: line_array[6].strip.to_s,
                                                                                        # clicks: clicks_d.to_i,
                                                                                        # avg_position: 0
                                                                                      # })
#                                                                                       
                                                          # @db3.close()                           
                                                    end
                                                    @index = @index + 1
                                              end
                                              
                                              
                                              if data_arr.count.to_i > 0
                                                  @db3[:sogou_report_account].bulk_write(data_arr)
                                                  @db3.close()  
                                              end
                                              
                                              @logger.info "report sogou first loop ok " +doc["id"].to_s
                                              # if doc["id"].to_i != 71
                                              File.delete(@zip_file)
                                              # end
                                        end
                                        
                                        if i == 2
                                              @db3[:sogou_report_campaign].find({ "$and" => [{:network_id => doc["id"].to_i}, {:report_date => @today.to_s}] }).delete_many
                                              @db3.close()
                                              
                                              @index = 0
                                              @logger.info "report sogou second loop , network " +doc["id"].to_s
                                              @logger.info @today.to_s
                                              
                                              data_arr = []
                                              gz.each_line do |line|
                                                
                                                    # @logger.info line
                                                
                                                    if @index.to_i > 1
                                                          
                                                          line_array = line.force_encoding("GB18030").encode("UTF-8").split(",")
                                                          
                                                          clicks_d = (line_array[7].to_f * line_array[8].strip.to_s.gsub('%', '').to_f ).to_f/100
                                                          
                                                          data_hash = {}
                                                          insert_hash = {}
                                                           
                                                          insert_hash[:network_id] = doc["id"].to_i
                                                          insert_hash[:report_date] = @today.to_s
                                                          insert_hash[:name] = doc["name"].to_s
                                                          insert_hash[:cpc_plan_id] = line_array[3].to_i
                                                          insert_hash[:cpc_plan_name] = line_array[4].to_s
                                                          insert_hash[:total_cost] = line_array[5].to_f
                                                          insert_hash[:clicks_avg_price] = line_array[6].to_f
                                                          insert_hash[:display] = line_array[7].to_i
                                                          insert_hash[:click_rate] = line_array[8].strip.to_s 
                                                          insert_hash[:clicks] = clicks_d.to_i
                                                          insert_hash[:avg_position] = 0
                                                          
                                                          data_hash[:insert_one] = insert_hash
                                                          data_arr << data_hash
                                                          
                                                          if data_arr.count.to_i > 1000
                                                            
                                                              @db3[:sogou_report_campaign].bulk_write(data_arr)
                                                              @db3.close()  
                                                              
                                                              data_arr = []
                                                          end                            
                                                          # @db3[:sogou_report_campaign].insert_one({
                                                                                        # network_id: doc["id"].to_i,
                                                                                        # report_date: @today.to_s,
                                                                                        # name: doc["name"].to_s,
                                                                                        # cpc_plan_id: line_array[3].to_i,
                                                                                        # cpc_plan_name: line_array[4].to_s,
                                                                                        # total_cost: line_array[5].to_f,
                                                                                        # clicks_avg_price: line_array[6].to_f,
                                                                                        # display:  line_array[7].to_i,
                                                                                        # click_rate:  line_array[8].strip.to_s,
                                                                                        # clicks: clicks_d.to_i,
                                                                                        # avg_position: 0
                                                                                      # })
                                                          # @db3.close()                            
                                                          
                                                    end
                                                    @index = @index + 1
                                              end
                                              
                                              if data_arr.count.to_i > 0
                                                  @db3[:sogou_report_campaign].bulk_write(data_arr)
                                                  @db3.close()  
                                              end
                                              
                                              @logger.info "report sogou loop second loop ok" +doc["id"].to_s
                                              # if doc["id"].to_i != 71
                                              File.delete(@zip_file)
                                              # end
                                        end
                                        
                                        if i == 3
                                              
                                              @db3[:sogou_report_adgroup].find({ "$and" => [{:network_id => doc["id"].to_i}, {:report_date => @today.to_s}] }).delete_many
                                              @db3.close()
                                              
                                              @index = 0
                                              @logger.info "report sogou third loop , network " +doc["id"].to_s
                                              @logger.info @today.to_s
                                              
                                              data_arr = []
                                              gz.each_line do |line|
                                                
                                                    # @logger.info line
                                                    
                                                    if @index.to_i > 1
                                                          
                                                          line_array = line.force_encoding("GB18030").encode("UTF-8").split(",")
                                                          
                                                          if line_array[9].to_i != 0
                                                              clicks_d = (line_array[9].to_f * line_array[10].strip.to_s.gsub('%', '').to_f).to_f/100
                                                                     
                                                                     
                                                              data_hash = {}
                                                              insert_hash = {}
                                                               
                                                              insert_hash[:network_id] = doc["id"].to_i
                                                              insert_hash[:report_date] = @today.to_s
                                                              insert_hash[:name] = doc["name"].to_s
                                                              insert_hash[:cpc_plan_id] = line_array[3].to_i
                                                              insert_hash[:cpc_plan_name] = line_array[4].to_s
                                                              insert_hash[:cpc_grp_id] = line_array[5].to_i
                                                              insert_hash[:cpc_grp_name] = line_array[6].to_s
                                                              insert_hash[:total_cost] = line_array[7].to_f
                                                              insert_hash[:clicks_avg_price] = line_array[8].to_f 
                                                              insert_hash[:display] = line_array[9].to_i
                                                              insert_hash[:click_rate] = line_array[10].strip.to_s
                                                              insert_hash[:clicks] = clicks_d.to_i
                                                              insert_hash[:avg_position] = 0
                                                              
                                                              data_hash[:insert_one] = insert_hash
                                                              data_arr << data_hash
                                                              
                                                              if data_arr.count.to_i > 1000
                                                                
                                                                  @db3[:sogou_report_adgroup].bulk_write(data_arr)
                                                                  @db3.close()  
                                                                  
                                                                  data_arr = []
                                                              end      
                                                                                          
                                                               # @db3[:sogou_report_adgroup].insert_one({
                                                                                            # network_id: doc["id"].to_i,
                                                                                            # report_date: @today.to_s,
                                                                                            # name: doc["name"].to_s,
                                                                                            # cpc_plan_id: line_array[3].to_i,
                                                                                            # cpc_plan_name: line_array[4].to_s,
                                                                                            # cpc_grp_id: line_array[5].to_i,
                                                                                            # cpc_grp_name: line_array[6].to_s,
                                                                                            # total_cost: line_array[7].to_f,
                                                                                            # clicks_avg_price: line_array[8].to_f,
                                                                                            # display:  line_array[9].to_i,
                                                                                            # click_rate:  line_array[10].strip.to_s,
                                                                                            # clicks: clicks_d.to_i,
                                                                                            # avg_position: 0
                                                                                          # })       
                                                               # @db3.close()
                                                           end
                                                      
                                                    end
                                                    @index = @index + 1
                                              end
                                              
                                              if data_arr.count.to_i > 0
                                                  @db3[:sogou_report_adgroup].bulk_write(data_arr)
                                                  @db3.close()  
                                              end
                                              @logger.info "report sogou loop third loop ok " +doc["id"].to_s
                                              # if doc["id"].to_i != 71
                                              File.delete(@zip_file)
                                              # end
    
                                        end
                                        
                                        if i == 4
                                              
                                              @db3[:sogou_report_ad].find({ "$and" => [{:network_id => doc["id"].to_i}, {:report_date => @today.to_s}] }).delete_many
                                              @db3.close()
                                              
                                              @index = 0
                                              @logger.info "report sogou fourth loop , network " +doc["id"].to_s
                                              @logger.info @today.to_s
                                              
                                              data_arr = []
                                              gz.each_line do |line|
                                                
                                                    # @logger.info line
                                                    
                                                    if @index.to_i > 1
                                                          
                                                          line_array = line.force_encoding("GB18030").encode("UTF-8").split(",")
                                                          
                                                          if line_array[17].to_i != 0
                                                              clicks_d = (line_array[17].to_f * line_array[18].strip.to_s.gsub('%', '').to_f).to_f/100
                                                                                    
                                                                                    
                                                              data_hash = {}
                                                              insert_hash = {}
                                                               
                                                              insert_hash[:network_id] = doc["id"].to_i
                                                              insert_hash[:report_date] = @today.to_s
                                                              insert_hash[:name] = doc["name"].to_s
                                                              insert_hash[:cpc_plan_id] = line_array[3].to_i
                                                              insert_hash[:cpc_plan_name] = line_array[4].to_s
                                                              insert_hash[:cpc_grp_id] = line_array[5].to_i
                                                              insert_hash[:cpc_grp_name] = line_array[6].to_s
                                                              insert_hash[:ad_id] = line_array[7].to_i
                                                              insert_hash[:title] = line_array[8].to_s 
                                                              insert_hash[:description_1] = line_array[9].to_s
                                                              insert_hash[:description_2] = line_array[10].to_s
                                                              insert_hash[:visit_url] = line_array[11].to_s
                                                              insert_hash[:show_url] = line_array[12].to_s
                                                              insert_hash[:mobile_visit_url] = line_array[13].to_s
                                                              insert_hash[:mobile_show_url] = line_array[14].to_s
                                                              insert_hash[:total_cost] = line_array[15].to_f
                                                              insert_hash[:clicks_avg_price] = line_array[16].to_f
                                                              insert_hash[:display] = line_array[17].to_i
                                                              insert_hash[:click_rate] = line_array[18].strip.to_s
                                                              insert_hash[:clicks] = clicks_d.to_i
                                                              insert_hash[:avg_position] = 0
                                                              
                                                              data_hash[:insert_one] = insert_hash
                                                              data_arr << data_hash
                                                              
                                                              if data_arr.count.to_i > 1000
                                                                
                                                                  @db3[:sogou_report_ad].bulk_write(data_arr)
                                                                  @db3.close()  
                                                                  
                                                                  data_arr = []
                                                              end                      
                                                                                    
                                                              # @db3[:sogou_report_ad].insert_one({
                                                                                    # network_id: doc["id"].to_i,
                                                                                    # report_date: @today.to_s,
                                                                                    # name: doc["name"].to_s,
                                                                                    # cpc_plan_id: line_array[3].to_i,
                                                                                    # cpc_plan_name: line_array[4].to_s,
                                                                                    # cpc_grp_id: line_array[5].to_i,
                                                                                    # cpc_grp_name: line_array[6].to_s,
                                                                                    # ad_id: line_array[7].to_i,
                                                                                    # title: line_array[8].to_s,
                                                                                    # description_1: line_array[9].to_s,
                                                                                    # description_2: line_array[10].to_s,
                                                                                    # visit_url: line_array[11].to_s,
                                                                                    # show_url: line_array[12].to_s,
                                                                                    # mobile_visit_url: line_array[13].to_s,
                                                                                    # mobile_show_url: line_array[14].to_s,
                                                                                    # total_cost: line_array[15].to_f,
                                                                                    # clicks_avg_price: line_array[16].to_f,
                                                                                    # display:  line_array[17].to_i,
                                                                                    # click_rate:  line_array[18].strip.to_s,
                                                                                    # clicks: clicks_d.to_i,
                                                                                    # avg_position: 0
                                                                                  # })            
                                                               # @db3.close()      
                                                           end
                                                      
                                                    end
                                                    @index = @index + 1
                                              end
                                              
                                              
                                              if data_arr.count.to_i > 0
                                                                
                                                  @db3[:sogou_report_ad].bulk_write(data_arr)
                                                  @db3.close()  
                                                  
                                                  data_arr = []
                                              end
                                              
                                              @logger.info "report sogou fourth loop ok " +doc["id"].to_s
                                              # if doc["id"].to_i != 71
                                              File.delete(@zip_file)
                                              # end
                                        end
                                        
                                        if i == 5
                                              
                                              @db3[:sogou_report_keyword].find({ "$and" => [{:network_id => doc["id"].to_i}, {:report_date => @today.to_s}] }).delete_many
                                              @db3.close()
                                              
                                              @index = 0
                                              @logger.info "report sogou fifth loop , network " +doc["id"].to_s
                                              @logger.info @today.to_s
                                              
                                              data_arr = []
                                              gz.each_line do |line|
                                                  
                                                    # @logger.info line
                                                
                                                    if @index.to_i > 1
                                                          
                                                          line_array = line.force_encoding("GB18030").encode("UTF-8").split(",")
                                                          
                                                          if line_array[11].to_i != 0
                                                              clicks_d = (line_array[11].to_f * line_array[12].strip.to_s.gsub('%', '').to_f).to_f/100
                                                              
                                                              
                                                              data_hash = {}
                                                              insert_hash = {}
                                                               
                                                              insert_hash[:network_id] = doc["id"].to_i
                                                              insert_hash[:report_date] = @today.to_s
                                                              insert_hash[:name] = doc["name"].to_s
                                                              insert_hash[:cpc_plan_id] = line_array[3].to_i
                                                              insert_hash[:cpc_plan_name] = line_array[4].to_s
                                                              insert_hash[:cpc_grp_id] = line_array[5].to_i
                                                              insert_hash[:cpc_grp_name] = line_array[6].to_s
                                                              insert_hash[:keyword_id] = line_array[7].to_i
                                                              insert_hash[:keyword] = line_array[8].to_s 
                                                              insert_hash[:total_cost] = line_array[9].to_f
                                                              insert_hash[:clicks_avg_price] = line_array[10].to_f
                                                              insert_hash[:display] = line_array[11].to_i
                                                              insert_hash[:click_rate] = line_array[12].strip.to_s
                                                              insert_hash[:clicks] = clicks_d.to_i
                                                              insert_hash[:avg_position] = 0
                                                              
                                                              data_hash[:insert_one] = insert_hash
                                                              data_arr << data_hash
                                                              
                                                              if data_arr.count.to_i > 1000
                                                                
                                                                  @db3[:sogou_report_keyword].bulk_write(data_arr)
                                                                  @db3.close()  
                                                                  
                                                                  data_arr = []
                                                              end       
                                                              
                                                              
                                                              # @db3[:sogou_report_keyword].insert_one({
                                                                                    # network_id: doc["id"].to_i,
                                                                                    # report_date: @today.to_s,
                                                                                    # name: doc["name"].to_s,
                                                                                    # cpc_plan_id: line_array[3].to_i,
                                                                                    # cpc_plan_name: line_array[4].to_s,
                                                                                    # cpc_grp_id: line_array[5].to_i,
                                                                                    # cpc_grp_name: line_array[6].to_s,
                                                                                    # keyword_id: line_array[7].to_i,
                                                                                    # keyword: line_array[8].to_s,
                                                                                    # total_cost: line_array[9].to_f,
                                                                                    # clicks_avg_price: line_array[10].to_f,
                                                                                    # display:  line_array[11].to_i,
                                                                                    # click_rate:  line_array[12].strip.to_s,
                                                                                    # clicks: clicks_d.to_i,
                                                                                    # avg_position: 0
                                                                                  # })
                                                               # @db3.close()                   
                                                                                                        
                                                          end
                                                      
                                                    end
                                                    @index = @index + 1
                                              end
                                              
                                              
                                              if data_arr.count.to_i > 0
                                                  @db3[:sogou_report_keyword].bulk_write(data_arr)
                                                  @db3.close()  
                                              end
                                              @logger.info "report sogou loop fifth loop ok " +doc["id"].to_s
                                              # if doc["id"].to_i != 71
                                              File.delete(@zip_file)
                                              # end
                                        end
                                        
                                        
                                    end
                      
                                else
                                    # @db[:network].find(id: doc["id"].to_i).update_one('$set'=>{     
                                                                                       # reportid: @report_id.to_s
                                                                                    # })
                                end
                          end
                    end   
              end
              
              if @id.nil? && @days.nil?
                  @db[:network].find('id' => doc["id"].to_i).update_one('$set'=> { 'report' => 2,'last_update' => @now, "report_worker" => "" })
                  @db.close
              end
          
          rescue Exception
              @logger.info "report sogou fail" +doc["id"].to_s
              
              @db[:network].find('id' => doc["id"].to_i).update_one('$set'=> { 'report' => 0,'last_update' => @now })
              @db.close
          end
          
      end
      
      @logger.info "report sogou done"
      return render :nothing => true
  end
    
  def threemonthsreport
    
      @logger.info "sogou report keep 3 months run"
      @three_months_ago = @now.to_date - 3.months
      @three_months_ago = @three_months_ago.strftime("%Y-%m") + "-01"
      
      @db3["sogou_report_account"].find('report_date' => { '$lt' => @three_months_ago.to_s }).delete_many
      @db3.close()
      @db3["sogou_report_campaign"].find('report_date' => { '$lt' => @three_months_ago.to_s }).delete_many
      @db3.close()
      @db3["sogou_report_adgroup"].find('report_date' => { '$lt' => @three_months_ago.to_s }).delete_many
      @db3.close()
      @db3["sogou_report_ad"].find('report_date' => { '$lt' => @three_months_ago.to_s }).delete_many
      @db3.close()
      @db3["sogou_report_keyword"].find('report_date' => { '$lt' => @three_months_ago.to_s }).delete_many
      @db3.close()
      
      @logger.info "sogou report keep 3 months done"
      return render :nothing => true 
  end
  
end
