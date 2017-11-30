class ShenmasController < ApplicationController
  before_action :set_shenma, only: [:show, :edit, :update, :destroy]
  before_action :tmp

  
  require 'rubygems'
  require 'mongo'
  require 'zlib'
  
  require 'fileutils'




  def checkreport
  
      @logger.info "checkreport shenmas start"
      
      @current_not_done_report = @db[:miss_report].find({ "$and" => [{:status => 1},{:worker => @port.to_i},{:network_type => "shenma"}] })
      @db.close
      
      if @current_not_done_report.count.to_i > 0
          data = {:message => "check report shenma running", :status => "true"}
          return render :json => data, :status => :ok  
      end
    
      @not_done_report = @db[:miss_report].find({ "$and" => [{:status => 0},{:worker => @port.to_i},{:network_type => "shenma"}] }).limit(1)
      @db.close
      
      
      
      if @not_done_report.count.to_i > 0
          @not_done_report.no_cursor_timeout.each do |not_done_report_d|
            
              begin
                  @db[:miss_report].find('_id' => not_done_report_d["_id"], 'status' => 0 ).update_one('$set'=> { 'status' => 1, 'update_date' => @now })
                  @db.close
                  
                  id = not_done_report_d["network_id"]
                  report_day = not_done_report_d["report_date"]
                  
                  @logger.info "checkreport shenma running "+id.to_s+" - "+report_day.to_s
                  
                  days = @today.to_date - report_day.to_date
                  
                  url = "http://china.adeqo.com:"+@port.to_s+"/shenmas/report?day="+days.to_i.to_s+"&id="+id.to_s
                  # res = Net::HTTP.get_response(URI(url))
                  
                  link = URI.parse(url)
                  http = Net::HTTP.new(link.host, link.port)
                  
                  http.read_timeout = 800
                  http.open_timeout = 800
                  res = http.start() {|http|
                    http.get(URI(url))
                  }
    
                  
                  @logger.info "checkreport running shenma report "+id.to_s+" - "+report_day.to_s
                  
                  if res.code.to_i == 200 
                          
                      @db[:miss_report].find('_id' => not_done_report_d["_id"], 'status' => 1 ).delete_one
                      @db.close
                      
                      @logger.info "checkreport done shenma report "+id.to_s+" - "+report_day.to_s
                      
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

  def shenma_api(service,method,json)
      # json = {'header' => { 
                              # 'token' => '954b904a7a3e4721954d1fed3183f098',
                              # 'username' => 'baidu-Guam2161498',
                              # 'password' => 'Dfshk2016' 
                          # },
               # 'body'  => {
                              # 'accountFields' => ["userId","balance","cost","payment","budgetType","budget","regionTarget","excludeIp","openDomains","regDomain","budgetOfflineTime","weeklyBudget","userStat","isDynamicCreative","dynamicCreativeParam","pcBalance","mobileBalance"]
                          # }
              # }                                  
              
      url = "https://e.sm.cn/api/#{service}/#{method}"
      
      response = HTTParty.post(url, 
                            :body => json.to_json,
                            :headers => { 'Content-Type' => 'application/json', 'Accept' => 'application/json'} 
                            )
      
      
        
      @response = response                
      return response.parsed_response
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



  def redownload(networkid)
    
      
    
      @redownload_network = @db[:network].find({ "$and" => [{:type => 'shenma'},{:id => networkid.to_i}] })
      @db.close
    
      if @redownload_network.count.to_i == 1
          
          @redownload_network.no_cursor_timeout.each do |doc|
              if doc["tmp_file"] != ""
                  unzip_folder = "/datadrive/shenma_"+doc['id'].to_s+"_"+doc['tmp_file'].to_s
                  
                  if File.directory?(unzip_folder)
                      FileUtils.remove_dir unzip_folder, true
                  end
                  
              end
          end
          
          @db[:network].find(id: networkid.to_i).update_one('$set'=> { 'tmp_file' => "" , 'fileid' => "",'run_time' => 0, 'file_update_1' => 0, 'file_update_2' => 0, 'file_update_3' => 0, 'file_update_4' => 0, 'worker' => "", 'last_update' => @now})  
          @db.close
      end
  end
  
  
  
  
  
  
  
  
  
  
  
  
  def apiadgroup
      @logger.info "shenma api adgroup start"
      
      @campaign_id = params[:id]
    
      if @campaign_id.nil?
        
          # @current_campaign = @db[:all_campaign].find({ 'api_update' => 4 ,'network_type' => 'baidu', 'api_worker' => @port.to_i})
          # @db.close
          
          
        
          @current_campaign = @db[:all_campaign].find({ "$and" => [{:api_update => 4}, {:network_type => 'shenma'}, {:api_worker => @port.to_i}] })
          @db.close
          
          if @current_campaign.count.to_i >= 1
              @logger.info "working, no need update shenma api adgroup"
              return render :nothing => true
          end
          
          
          # @campaign = @db[:all_campaign].find('network_type' => 'baidu', 'api_update' => 3, 'api_worker' => @port.to_i).sort({ last_update: -1 }).limit(1)
          # @db.close
          
          
          @campaign = @db[:all_campaign].find({ "$and" => [{:api_update => 3}, {:network_type => 'shenma'}, {:api_worker => @port.to_i}] }).sort({ last_update: -1 }).limit(1)
          @db.close
          
          if @campaign.count.to_i == 0
              @logger.info "no need update shenma api adgroup"
              return render :nothing => true
          end
          
      else
        
          # @campaign = @db[:all_campaign].find({ 'campaign_id' => @campaign_id.to_i ,'network_type' => 'baidu'})
          # @db.close
          
          @campaign = @db[:all_campaign].find({ "$and" => [{:campaign_id => @campaign_id.to_i}, {:network_type => 'shenma'}] })
          @db.close
      end
      
      
      
                                      
      if @campaign.count.to_i
          @campaign.no_cursor_timeout.each do |campaign|
              @network_id = campaign["network_id"].to_i
              @campaign_id = campaign["campaign_id"].to_i
          end
          
          # @network = @db[:network].find('type' => 'baidu', 'id' => @network_id.to_i)
          # @db.close
          
          
          @network = @db[:network].find({ "$and" => [{:id => @network_id.to_i}, {:type => 'shenma'}] })
          @db.close
        
          if @network.count.to_i > 0
              @network.no_cursor_timeout.each do |network_d|
                  @tracking_type = network_d["tracking_type"].to_s
                  @ad_redirect = network_d["ad_redirect"].to_s
                  @keyword_redirect = network_d["keyword_redirect"].to_s
                  @company_id = network_d["company_id"].to_s
                  @cookie_length = network_d["cookie_length"].to_s
                  
                  @username = network_d["username"]
                  @password = network_d["password"]
                  @apitoken = network_d["api_token"]
                  
                  service = "account"
                  method = "getAccount"
                  
                  json = {'header' => { 
                                          'token' => @apitoken.to_s,
                                          'username' => @username.to_s,
                                          'password' => @password.to_s
                                      },
                           'body'  => {
                                          'requestData' => ["account_all"]
                                      }
                          }
                          
                          
                  @account_info = shenma_api(service,method,json)
                  
                  
      
                  if !@account_info["header"]["desc"].nil? && @account_info["header"]["desc"].to_s == "执行成功"
                      @header = @account_info["header"]
                      @remain_quote = @header["leftQuota"]
                      
                      if @remain_quote.to_i >= 500
                        
                          db_name = "adgroup_shenma_"+@network_id.to_s
                          
                          # @adgroup = @baidu_db[db_name].find('campaign_id' => @campaign_id.to_i, 'api_update_ad' => 1,'api_update_keyword' => 1, 'api_worker' => @port.to_i)
                          # @baidu_db.close()
                          
                          
                          @adgroup = @baidu_db[db_name].find({ "$and" => [{:campaign_id => @campaign_id.to_i}, {:api_update_ad => 1}, {:api_update_keyword => 1}, {:api_worker => @port.to_i}] })
                          @baidu_db.close()
                          
                                      
                          @adgroup_id_array = []  
                          
                          if @adgroup.count.to_i
                              
                              # start , status
                              
                              @adgroup.no_cursor_timeout.each do |adgroup_d|
                                  @adgroup_id_array << adgroup_d["adgroup_id"].to_i
                              end
                              
                              @group_adgroup_id_arr = @adgroup_id_array.each_slice(1000).to_a
                              
                              @group_adgroup_id_arr.each do |group_adgroup_id_arr_d|
                                
                                  if @remain_quote >= 500
                                  
                                      service = "adgroup"
                                      method = "getAdgroupByAdgroupId"
                                      
                                      json = {'header' => { 
                                                              'token' => @apitoken.to_s,
                                                              'username' => @username.to_s,
                                                              'password' => @password.to_s 
                                                          },
                                               'body'  => {
                                                              'adgroupIds'=> group_adgroup_id_arr_d
                                                          }
                                              }
                                              
                                      @adgroup_info = shenma_api(service,method,json)
                                      
          
                                      if !@adgroup_info["header"]["desc"].nil? && @adgroup_info["header"]["desc"].to_s == "执行成功"
                                          @header = @adgroup_info["header"]
                                          @remain_quote = @header["leftQuota"]
                                          
                                          @adgroup = @adgroup_info["body"]["adgroupTypes"]
                                          
                                          
                                          if @adgroup.count.to_i > 0
                                                    
                                              db_name = "adgroup_shenma_"+@network_id.to_s
                                                  
                                              @adgroup.each do |adgroup_d|
                                                  
                                                  
                                                  result = @baidu_db[db_name].find({ "$and" => [{:adgroup_id => adgroup_d["adgroupId"].to_i}, {:campaign_id => adgroup_d["campaignId"].to_i}] } ).update_one('$set'=> { 
                                                                                                                                                    'name' => adgroup_d["adgroupName"].to_s,
                                                                                                                                                    'max_price' => adgroup_d["maxPrice"].to_f,
                                                                                                                                                    'status' => adgroup_d["status"].to_i,
                                                                                                                                                    'pause' => adgroup_d["pause"].to_s,
                                                                                                                                                    'negative_words' => adgroup_d["negativeWords"],
                                                                                                                                                    'exact_negative_words' => adgroup_d["exactNegativeWords"],
                                                                                                                                                    'api_update_ad' => 2,
                                                                                                                                                    'api_update_keyword' => 2,
                                                                                                                                                    'adPlatformOS' => adgroup_d["adPlatformOS"].to_i,
                                                                                                                                                    'update_date' => @now
                                                                                                                                               })
                                                  @baidu_db.close()
                                                  
                                                  if result.n.to_i == 0
                                                      
                                                      @baidu_db[db_name].insert_one({ 
                                                                                  network_id: @network_id.to_i,
                                                                                  campaign_id: adgroup_d["campaignId"].to_i,
                                                                                  adgroup_id: adgroup_d["adgroupId"].to_i,
                                                                                  name: adgroup_d["adgroupName"].to_s,
                                                                                  max_price: adgroup_d["maxPrice"].to_f,
                                                                                  negative_words: adgroup_d["negativeWords"],
                                                                                  exact_negative_words: adgroup_d["exactNegativeWords"],
                                                                                  pause: adgroup_d["pause"].to_s,
                                                                                  status: adgroup_d["status"].to_i,
                                                                                  adPlatformOS: adgroup_d["adPlatformOS"].to_i,
                                                                                  api_update_ad: 2,
                                                                                  api_update_keyword: 2,
                                                                                  update_date: @now,                                            
                                                                                  create_date: @now })
                                                      @baidu_db.close() 
                                                      
                                                  end
                                              end
                                          end
                                      end
                                      
                                      # adgroup done
                                      # ad start
                                      service = "creative"
                                      method = "getCreativeByAdgroupId"
                                    
                                    
                                      json = {'header' => { 
                                                              'token' => @apitoken.to_s,
                                                              'username' => @username.to_s,
                                                              'password' => @password.to_s 
                                                          },
                                               'body'  => {
                                                              'adgroupIds'=> group_adgroup_id_arr_d
                                                          }
                                              }
                                      @ad_info = shenma_api(service,method,json)
                                      
                                      # data = {:message => @ad_info, :status => "false"}
                                      # return render :json => data, :status => :ok
                                      
                                      
                                      if !@ad_info["header"]["desc"].nil? && @ad_info["header"]["desc"].to_s == "执行成功"
                                          @header = @ad_info["header"]
                                          @remain_quote = @header["leftQuota"]
                                          
                                          @ad_result = @ad_info["body"]["groupCreatives"]
                                          
                                          
                                          if @ad_result.count.to_i > 0
                                              @ad_result.each do |ad_result_d|
                                          
                                                  @ad = ad_result_d["creativeTypes"]
                                          
                                                  if @ad.count.to_i > 0
                                                          
                                                      @ad.each do |ad_d|
                                                        
                                                          url_tag = 0
                                                          
                                                          @final_url = ad_d["destinationUrl"].to_s
                                                        
                                                          if !@final_url.to_s.include?(".adeqo.") && @tracking_type.to_s.downcase == "adeqo" && @ad_redirect.to_s.downcase == "yes" && @final_url.to_s != ""
                                        
                                                              @temp_final_url = @final_url
                                                              @final_url = "http://t.adeqo.com/click?company_id="+@company_id.to_s+"&network_id="+@network_id.to_s
                                                              @final_url = @final_url + "&campaign_id="+@campaign_id.to_s+"&adgroup_id="+ad_d["adgroupId"].to_s+"&ad_id="+ad_d["creativeId"].to_s+"&keyword_id=0"
                                                              @final_url = @final_url + "&cookie="+@cookie_length.to_s
                                                              @final_url = @final_url + "&device=pc"
                                                              @final_url = @final_url + "&tv=v1&durl="+CGI.escape(@temp_final_url.to_s)
                                                              
                                                              url_tag = 1
                                                          end
                                                        
                                                        
                                                          if url_tag == 1 
                                                              if @remain_quote.to_i >= 500
                                                                  requesttypearray = [] 
                                                                  requesttype = {}
                                                                  requesttype[:creativeId]    =     ad_d["creativeId"].to_i
                                                                  requesttype[:destinationUrl]    =     @final_url
                                                                  requesttype[:title] = ad_d["title"].to_s
                                                                  requesttype[:description1] = ad_d["description1"].to_s
                                                                  
                                                                  
                                                                  requesttypearray << requesttype
                                                              
                                                                  service = "creative"
                                                                  method = "updateCreative"
                                                                  
                                                                  json = {'header' => { 
                                                                                          'token' => @apitoken.to_s,
                                                                                          'username' => @username.to_s,
                                                                                          'password' => @password.to_s 
                                                                                      },
                                                                          'body'  => {
                                                                                          'creativeTypes' => requesttypearray
                                                                                     }
                                                                          }       
                                                                      
                                                                  @urt_tag_update_info = shenma_api(service,method,json)
                                                                  
                                                                  if !@urt_tag_update_info["header"]["desc"].nil? && @urt_tag_update_info["header"]["desc"].to_s == "执行成功"
                                                
                                                                  else
                                                                      @final_url = ad_d["destinationUrl"].to_s
                                                                  end
                                                              end
                                                          end
                                                        
                                                          # @logger.info ad_d["creativeId"].to_s
                                                          
                                                          db_name = "ad_shenma_"+@network_id.to_s
                                                          
                                                          
                                                          result = @baidu_db[db_name].find({ "$and" => [{:adgroup_id => adgroup_d["adgroupId"].to_i}, {:ad_id => ad_d["creativeId"].to_i}] } ).update_one('$set'=> { 
                                                                                                                                                                  'title' => ad_d["title"].to_s,
                                                                                                                                                                  'status' => ad_d["status"].to_i,
                                                                                                                                                                  'pause' => ad_d["pause"].to_s,
                                                                                                                                                                  'description' => ad_d["description1"].to_s,
                                                                                                                                                                  'show_url' => ad_d["displayUrl"].to_s,
                                                                                                                                                                  'visit_url' => @final_url.to_s,
                                                                                                                                                                  'update_date' => @now
                                                                                                                                                             })
                                                          @baidu_db.close()
                                                          
                                                          if result.n.to_i == 0
                                                            
                                                              @baidu_db[db_name].insert_one({ 
                                                                                              network_id: @network_id.to_i,
                                                                                              campaign_id: @campaign_id.to_i, 
                                                                                              adgroup_id: ad_d["adgroupId"].to_i,
                                                                                              ad_id: ad_d["creativeId"].to_i,
                                                                                              title: ad_d["title"].to_s, 
                                                                                              description: ad_d["description1"].to_s, 
                                                                                              visit_url: @final_url.to_s,
                                                                                              show_url: ad_d["displayUrl"].to_s,
                                                                                              pause: ad_d["pause"].to_s,
                                                                                              status: ad_d["status"].to_i,
                                                                                              update_date: @now,                                            
                                                                                              create_date: @now })
                                                              @baidu_db.close()
                                                             
                                                          end
                                                      end
                                                  end
                                              end
                                          end
                                      end
                                      
                                      # ad done
                                      
                                  end
                              end
                              
                              
                              # keyword start, keyword has to be another loop cause the limit is small
                              @group_adgroup_id_arr = @adgroup_id_array.each_slice(50).to_a
                              
                              @group_adgroup_id_arr.each do |group_adgroup_id_arr_d|
                                      
                                  if @remain_quote >= 500
                                      service = "keyword"
                                      method = "getKeywordByAdgroupId"
                                    
                                    
                                      json = {'header' => { 
                                                              'token' => @apitoken.to_s,
                                                              'username' => @username.to_s,
                                                              'password' => @password.to_s 
                                                          },
                                               'body'  => {
                                                              'adgroupIds'=> group_adgroup_id_arr_d
                                                          }
                                              }
                                              
                                      @keyword_info = baidu_api(service,method,json)
                                      
                                      if !@keyword_info["header"]["desc"].nil? && @keyword_info["header"]["desc"].to_s == "执行成功"
                                          @header = @keyword_info["header"]
                                          @remain_quote = @header["leftQuota"]
                                          
                                          @keyword = @keyword_info["body"]["data"]
                                          
                                          @keyword_result = @keyword_info["body"]["groupKeywords"]
                                          
                                          if @keyword_result.count.to_i > 0
                                              @keyword_result.each do |keyword_result_d|
                                              
                                                  @keyword = keyword_result_d["keywordTypes"]
                                                  
                                                  if @keyword.count.to_i > 0
                                                      @keyword.each do |keyword_d|
                                                            
                                                          url_tag = 0
                                                          
                                                          @final_url = keyword_d["destinationUrl"].to_s
                                                        
                                                          if !@final_url.to_s.include?(".adeqo.") && @tracking_type.to_s.downcase == "adeqo" && @keyword_redirect.to_s.downcase == "yes" && @final_url.to_s != ""
                                          
                                                              url_tag = 1
                                                              @temp_final_url = @final_url
                                                              @final_url = "http://t.adeqo.com/click?company_id="+@company_id.to_s+"&network_id="+@network_id.to_s
                                                              @final_url = @final_url + "&campaign_id="+@campaign_id.to_s+"&adgroup_id="+keyword_d["adgroupId"].to_s+"&ad_id=0&keyword_id="+keyword_d["keywordId"].to_s
                                                              @final_url = @final_url + "&cookie="+@cookie_length.to_s
                                                              @final_url = @final_url + "&device=pc"
                                                              @final_url = @final_url + "&tv=v1&durl="+CGI.escape(@temp_final_url.to_s)
                                                          end
                                                          
                                                        
                                                          if url_tag == 1 
                                                              if @remain_quote.to_i >= 500
                                                                  requesttypearray = [] 
                                                                  requesttype = {}
                                                                  
                                                                  requesttype[:keywordId]    =     keyword_d["keywordId"].to_i
                                                                  requesttype[:destinationUrl]    =     @final_url
                                                                  
                                                                  
                                                                  requesttypearray << requesttype
                                                              
                                                                  service = "keyword"
                                                                  method = "updateKeyword"
                                                                  
                                                                  json = {'header' => { 
                                                                                        'token' => @apitoken.to_s,
                                                                                        'username' => @username.to_s,
                                                                                        'password' => @password.to_s 
                                                                                      },
                                                                          'body'  => {
                                                                                        'keywordTypes' => requesttypearray
                                                                                     }
                                                                          }       
                                                                      
                                                                  @keyword_tag_update_info = baidu_api(service,method,json)
                                                                  
                                                                  @logger.info requesttypearray
                                                                  @logger.info @keyword_tag_update_info["header"]
                                                                  
                                                                  @header = @keyword_tag_update_info["header"]
                                                                  @remain_quote = @header["leftQuota"]
                                                                      
                                                                  if !@keyword_tag_update_info["header"]["desc"].nil? && @keyword_tag_update_info["header"]["desc"].to_s == "执行成功"
                                                                      
                                                                  else
                                                                      @final_url = keyword_d["destinationUrl"].to_s
                                                                  end
                                                              end
                                                          end
                                                        
                                                          # @logger.info keyword_d["keywordId"].to_s
                                                          # @logger.info keyword_d["adgroupId"].to_s
                                                          
                                                          db_name = "keyword_shenma_"+@network_id.to_s
                                                          
                                                          # @logger.info db_name.to_s
                                                          
                                                          
                                                          result = @baidu_db[db_name].find({ "$and" => [{:adgroup_id => keyword_d["adgroupId"].to_i}, {:keyword_id => keyword_d["keywordId"].to_i}] } ).update_one('$set'=> { 
                                                                                                                                                                  'keyword' => keyword_d["keyword"].to_s,
                                                                                                                                                                  'pause' => keyword_d["pause"].to_s,
                                                                                                                                                                  'status' => keyword_d["status"].to_i,
                                                                                                                                                                  'match_type' => keyword_d["matchType"].to_i,
                                                                                                                                                                  'visit_url' => @final_url.to_s,
                                                                                                                                                                  'price' => keyword_d["price"].to_f,
                                                                                                                                                                  'update_date' => @now
                                                                                                                                                             })
                                                          @baidu_db.close()
                                                          
                                                          if result.n.to_i == 0
                                                              @baidu_db[db_name].insert_one({ 
                                                                                                network_id: @network_id.to_i,
                                                                                                campaign_id: @campaign_id.to_i,
                                                                                                adgroup_id: keyword_d["adgroupId"].to_i,
                                                                                                keyword_id: keyword_d["keywordId"].to_i,
                                                                                                keyword: keyword_d["keyword"].to_s,
                                                                                                price: keyword_d["price"].to_f, 
                                                                                                visit_url: @final_url.to_s,
                                                                                                match_type: keyword_d["matchType"].to_i,
                                                                                                pause: keyword_d["pause"].to_s,
                                                                                                status: keyword_d["status"].to_i,
                                                                                                update_date: @now,                                            
                                                                                                create_date: @now })
                                                              @baidu_db.close()
                                                          end
                                                          
                                                      end
                                                  end
                                              end
                                          end
                                      end
                                  end
                              end
                              
                              # keyword done
                             
                              db_name = "adgroup_shenma_"+@network_id.to_s
                              @baidu_db[db_name].find('adgroup_id' => { "$in" => @adgroup_id_array}).update_many('$set'=> { 
                                                                                                                              'api_update_ad' => 0,
                                                                                                                              'api_update_keyword' => 0,
                                                                                                                              'api_worker' => ""
                                                                                                                         })
                              @baidu_db.close()
                              
                              
                          end
                      end
                  end
              end
          end
          
          db_name = "adgroup_shenma_"+@network_id.to_s
          @list_adgroup = @baidu_db[db_name].find('$and' => [{'campaign_id' => @campaign_id.to_i},{'api_update_ad' => { "$ne" => 0}},{'api_update_keyword' => { "$ne" => 0}},{'api_update_ad' => { '$exists' => true }},'api_update_keyword' => { '$exists' => true }])
          @baidu_db.close() 
          
          if @list_adgroup.count.to_i == 0
            
              # @db["all_campaign"].find('campaign_id' => @campaign_id.to_i,'network_type' => "baidu", 'api_update' => 3).update_one('$set'=> {'api_update' => 0, 'api_worker' => "", 'update_date' => @now})
              # @sogou_db.close() 
#               
              # @db[:network].find('type' => 'baidu', 'id' => @network_id.to_i).update_one('$set'=> {'file_update_1' => 4,'file_update_2' => 4,'file_update_3' => 4,'file_update_4' => 4, 'last_update' => @now})
              # @db.close
            
            
              
              
              @db["all_campaign"].find({ "$and" => [{:campaign_id => @campaign_id.to_i}, {:network_type => "shenma"}, {:api_update => 3}] }).update_one('$set'=> {'api_update' => 0, 'api_worker' => "", 'update_date' => @now})
              @db.close 
              
              @db[:network].find({ "$and" => [{:id => @network_id.to_i}, {:type => "shenma"}] }).update_one('$set'=> {'file_update_1' => 4,'file_update_2' => 4,'file_update_3' => 4,'file_update_4' => 4, 'last_update' => @now})
              @db.close
          end
      end
      
      
      
      @logger.info "baidu api adgroup done"
      return render :nothing => true
  end

  def apicampaign
    
      @logger.info "shenma api campaign start"
    
      
      
      @campaign_id = params[:id]
      
      
    
      if @campaign_id.nil?
        
          @current_campaign = @db[:all_campaign].find({ "$and" => [{:api_update => 2}, {:network_type => "shenma"}, {:api_worker => @port.to_i}] })
          @db.close
          
          if @current_campaign.count.to_i >= 1
              @logger.info "working, no need update shenma api campaign"
              return render :nothing => true
          end
          
          
          
          @campaign = @db[:all_campaign].find({ "$and" => [{:api_update => 1}, {:network_type => "shenma"}, {:api_worker => @port.to_i}] }).sort({ last_update: -1 }).limit(1)
          @db.close
          
          if @campaign.count.to_i == 0
              @logger.info "no need update shenma api campaign"
              return render :nothing => true
          end
          
      else
        
          @campaign = @db[:all_campaign].find({ "$and" => [{:campaign_id => @campaign_id.to_i}, {:network_type => "shenma"}] })
          @db.close
      end
      
      
      @network_id = 0
      
      
      if @campaign.count.to_i > 0
          @campaign.no_cursor_timeout.each do |campaign|
            
              @logger.info "campaign"
              
              @network_id = campaign["network_id"].to_i
              @campaign_id = campaign["campaign_id"].to_i
              @campaign_status_body = ""
              
              
              
              @db["all_campaign"].find({ "$and" => [{:campaign_id => @campaign_id.to_i}, {:network_type => "shenma"}] }).update_one('$set'=> { 'api_update' => 2 })
              @db.close
              
              @network = @db[:network].find({ "$and" => [{:id => @network_id.to_i}, {:type => "shenma"}] })
              @db.close
              
              if @network.count.to_i > 0
                  @network.no_cursor_timeout.each do |network_d|
                      
                      @tracking_type = network_d["tracking_type"].to_s
                      @ad_redirect = network_d["ad_redirect"].to_s
                      @keyword_redirect = network_d["keyword_redirect"].to_s
                      @company_id = network_d["company_id"].to_s
                      @cookie_length = network_d["cookie_length"].to_s
                    
                      @username = network_d["username"]
                      @password = network_d["password"]
                      @apitoken = network_d["api_token"]
                      
                      
                      service = "account"
                      method = "getAccount"
                      
                      json = {'header' => { 
                                              'token' => @apitoken.to_s,
                                              'username' => @username.to_s,
                                              'password' => @password.to_s
                                          },
                               'body'  => {
                                              'requestData' => ["account_all"]
                                          }
                              }
                              
                              
                      @account_info = shenma_api(service,method,json)
                      
                      
                      
                      if !@account_info["header"]["desc"].nil? && @account_info["header"]["desc"] == "执行成功"
                          @header = @account_info["header"]
                          @remain_quote = @header["leftQuota"]
                          
                          if @remain_quote.to_i >= 500
                            
                            
                              service = "campaign"
                              method = "getCampaignByCampaignId"
                            
                            
                              json = {'header' => { 
                                                      'token' => @apitoken.to_s,
                                                      'username' => @username.to_s,
                                                      'password' => @password.to_s 
                                                  },
                                       'body'  => {
                                                      'campaignIds'=> [@campaign_id]
                                                  }
                                      }
                            
                              @campaign_info = shenma_api(service,method,json)
                              
                              
                              
                              # data = {:message => @campaign_info, :status => "false"}
                              # return render :json => data, :status => :ok
                              
                              if !@campaign_info["header"]["desc"].nil? && @campaign_info["header"]["desc"].to_s == "执行成功"
                              
                                  @header = @campaign_info["header"]
                                  @remain_quote = @header["leftQuota"]
                                  
                                  @adgroup_id_arr = []
                                  
                                  if @remain_quote.to_i > 500
                                      service = "adgroup"
                                      method = "getAdgroupByCampaignId"
                                    
                                    
                                      json = {'header' => { 
                                                              'token' => @apitoken.to_s,
                                                              'username' => @username.to_s,
                                                              'password' => @password.to_s 
                                                          },
                                               'body'  => {
                                                              'campaignIds'=> [@campaign_id],
                                                          }
                                              }
                                      @adgroup_info = shenma_api(service,method,json)
                                      
                                      
                                      if !@adgroup_info["header"]["desc"].nil? && @adgroup_info["header"]["desc"].to_s == "执行成功"
                                        
                                          @header = @adgroup_info["header"]
                                          @remain_quote = @header["leftQuota"]
                                          
                                          @adgroup = @adgroup_info["body"]["campaignAdgroups"][0]["adgroupTypes"]
                                          
                                          
                                          if @adgroup.count.to_i > 0
                                            
                                              db_name = "adgroup_shenma_"+@network_id.to_s
                                                  
                                              @adgroup.each do |adgroup_d|
                                                  
                                                  @adgroup_id_arr << adgroup_d["adgroupId"].to_i
                                                  
                                                  # result = @baidu_db[db_name].find('adgroup_id' => adgroup_d["adgroupId"].to_i, "campaign_id" => adgroup_d["campaignId"].to_i ).update_one('$set'=> { 
                                                                                                                                                    # 'name' => adgroup_d["adgroupName"].to_s,
                                                                                                                                                    # 'max_price' => adgroup_d["maxPrice"].to_f,
                                                                                                                                                    # 'status' => adgroup_d["status"].to_i,
                                                                                                                                                    # 'pause' => adgroup_d["pause"].to_s,
                                                                                                                                                    # 'api_update_ad' => 2,
                                                                                                                                                    # 'api_update_keyword' => 2,
                                                                                                                                                    # 'update_date' => @now
                                                                                                                                               # })
                                                                                                                                               
                                                  
                                                  
                                                  result = @baidu_db[db_name].find({ "$and" => [{:adgroup_id => adgroup_d["adgroupId"].to_i}, {:campaign_id => adgroup_d["campaignId"].to_i}] } ).update_one('$set'=> { 
                                                                                                                                                    'name' => adgroup_d["adgroupName"].to_s,
                                                                                                                                                    'max_price' => adgroup_d["maxPrice"].to_f,
                                                                                                                                                    'status' => adgroup_d["status"].to_i,
                                                                                                                                                    'pause' => adgroup_d["pause"].to_s,
                                                                                                                                                    'negative_words' => adgroup_d["negativeWords"],
                                                                                                                                                    'exact_negative_words' => adgroup_d["exactNegativeWords"],
                                                                                                                                                    'api_update_ad' => 2,
                                                                                                                                                    'api_update_keyword' => 2,
                                                                                                                                                    'update_date' => @now
                                                                                                                                               })
                                                  @baidu_db.close()
                                                  
                                                  if result.n.to_i == 0
                                                      
                                                      @baidu_db[db_name].insert_one({ 
                                                                                  network_id: @network_id.to_i,
                                                                                  campaign_id: adgroup_d["campaignId"].to_i,
                                                                                  adgroup_id: adgroup_d["adgroupTypes"]["adgroupId"].to_i,
                                                                                  name: adgroup_d["adgroupTypes"]["adgroupName"].to_s,
                                                                                  max_price: adgroup_d["adgroupTypes"]["maxPrice"].to_f,
                                                                                  negative_words: adgroup_d["adgroupTypes"]["negativeWords"],
                                                                                  exact_negative_words: adgroup_d["adgroupTypes"]["exactNegativeWords"],
                                                                                  pause: adgroup_d["adgroupTypes"]["pause"].to_s,
                                                                                  status: adgroup_d["adgroupTypes"]["status"].to_i,
                                                                                  update_date: @now,                                            
                                                                                  create_date: @now })
                                                      @baidu_db.close() 
                                                      
                                                  end
                                              end
                                          end
                                      end
                                      
                                  end
                                  
                                  if @adgroup_id_arr.count.to_i > 0 && @remain_quote >= 500
                                      
                                      @group_adgroup_id_arr = @adgroup_id_arr.each_slice(1000).to_a
                                      
                                      @group_adgroup_id_arr.each do |group_adgroup_id_arr_d|
                                          if @remain_quote >= 500    
                                              
                                              service = "creative"
                                              method = "getCreativeByAdgroupId"
                                            
                                            
                                              json = {'header' => { 
                                                                      'token' => @apitoken.to_s,
                                                                      'username' => @username.to_s,
                                                                      'password' => @password.to_s 
                                                                  },
                                                       'body'  => {
                                                                      'adgroupIds'=> group_adgroup_id_arr_d
                                                                  }
                                                      }
                                              @ad_info = shenma_api(service,method,json)
                                              
                                              
                                              if !@ad_info["header"]["desc"].nil? && @ad_info["header"]["desc"].to_s == "执行成功"
                                        
                                                  @header = @ad_info["header"]
                                                  @remain_quote = @header["leftQuota"]
                                                  
                                                  
                                                  @ad_result = @ad_info["body"]["groupCreatives"]
                                                  
                                                  
                                                  if @ad_result.count.to_i > 0
                                                      @ad_result.each do |ad_result_d|
                                                        
                                                          @ad = ad_result_d["creativeTypes"]
                                                          
                                                          if @ad.count.to_i > 0
                                                              @ad.each do |ad_d|
                                                                  
                                                                  url_tag = 0
                                                          
                                                                  @final_url = ad_d["destinationUrl"].to_s
                                                                
                                                                  if !@final_url.to_s.include?(".adeqo.") && @tracking_type.to_s.downcase == "adeqo" && @ad_redirect.to_s.downcase == "yes" && @final_url.to_s != ""
                                                
                                                                      @temp_final_url = @final_url
                                                                      @final_url = "http://t.adeqo.com/click?company_id="+@company_id.to_s+"&network_id="+@network_id.to_s
                                                                      @final_url = @final_url + "&campaign_id="+@campaign_id.to_s+"&adgroup_id="+ad_d["adgroupId"].to_s+"&ad_id="+ad_d["creativeId"].to_s+"&keyword_id=0"
                                                                      @final_url = @final_url + "&cookie="+@cookie_length.to_s
                                                                      @final_url = @final_url + "&device=pc"
                                                                      @final_url = @final_url + "&tv=v1&durl="+CGI.escape(@temp_final_url.to_s)
                                                                      
                                                                      url_tag = 1
                                                                  end
                                                                
                                                                  if url_tag == 1 || m_url_tag == 1
                                                                      if @remain_quote.to_i >= 500
                                                                          requesttypearray = [] 
                                                                          requesttype = {}
                                                                          requesttype[:creativeId]    =     ad_d["creativeId"].to_i
                                                                          requesttype[:destinationUrl]    =     @final_url
                                                                          requesttype[:title] = ad_d["title"].to_s
                                                                          requesttype[:description1] = ad_d["description1"].to_s
                                                                          
                                                                          
                                                                          requesttypearray << requesttype
                                                                      
                                                                          service = "creative"
                                                                          method = "updateCreative"
                                                                          
                                                                          json = {'header' => { 
                                                                                                  'token' => @apitoken.to_s,
                                                                                                  'username' => @username.to_s,
                                                                                                  'password' => @password.to_s 
                                                                                              },
                                                                                  'body'  => {
                                                                                                  'creativeTypes' => requesttypearray
                                                                                             }
                                                                                  }       
                                                                              
                                                                          @urt_tag_update_info = shenma_api(service,method,json)
                                                                          
                                                                          if !@urt_tag_update_info["header"]["desc"].nil? && @urt_tag_update_info["header"]["desc"].to_s == "执行成功"
                                                        
                                                                          else
                                                                              @final_url = ad_d["destinationUrl"].to_s
                                                                          end
                                                                      end
                                                                  end
                                                                
                                                                  # @logger.info ad_d["creativeId"].to_s
                                                                  
                                                                  db_name = "ad_shenma_"+@network_id.to_s
                                                                  
                                                                  # result = @baidu_db[db_name].find('adgroup_id' => ad_d["adgroupId"].to_i, "ad_id" => ad_d["creativeId"].to_i ).update_one('$set'=> { 
                                                                                                                                                                          # 'title' => ad_d["title"].to_s,
                                                                                                                                                                          # 'status' => ad_d["status"].to_i,
                                                                                                                                                                          # 'pause' => ad_d["pause"].to_s,
                                                                                                                                                                          # 'description_1' => ad_d["description1"].to_s,
                                                                                                                                                                          # 'description_2' => ad_d["description2"].to_s,
                                                                                                                                                                          # 'show_url' => ad_d["pcDisplayUrl"].to_s,
                                                                                                                                                                          # 'visit_url' => @final_url.to_s,
                                                                                                                                                                          # 'mobile_show_url' => ad_d["mobileDisplayUrl"].to_s,
                                                                                                                                                                          # 'mobile_visit_url' => @m_final_url.to_s,
                                                                                                                                                                          # 'devicePreference' => ad_d["devicePreference"].to_i,
                                                                                                                                                                          # 'tabs' => ad_d["tabs"],
                                                                                                                                                                          # 'update_date' => @now
                                                                                                                                                                     # })
                                                                  
                                                                  
                                                                  
                                                                  
                                                                  result = @baidu_db[db_name].find({ "$and" => [{:adgroup_id => ad_d["adgroupId"].to_i}, {:ad_id => ad_d["creativeId"].to_i}] } ).update_one('$set'=> { 
                                                                                                                                                                          'title' => ad_d["title"].to_s,
                                                                                                                                                                          'status' => ad_d["status"].to_i,
                                                                                                                                                                          'pause' => ad_d["pause"].to_s,
                                                                                                                                                                          'description' => ad_d["description1"].to_s,
                                                                                                                                                                          'show_url' => ad_d["displayUrl"].to_s,
                                                                                                                                                                          'visit_url' => @final_url.to_s,
                                                                                                                                                                          'update_date' => @now
                                                                                                                                                                     })
                                                                  @baidu_db.close()
                                                                  
                                                                  if result.n.to_i == 0
                                                                    
                                                                      @baidu_db[db_name].insert_one({ 
                                                                                                      network_id: @network_id.to_i,
                                                                                                      campaign_id: @campaign_id.to_i, 
                                                                                                      adgroup_id: ad_d["adgroupId"].to_i,
                                                                                                      ad_id: ad_d["creativeId"].to_i,
                                                                                                      title: ad_d["title"].to_s, 
                                                                                                      description: ad_d["description1"].to_s, 
                                                                                                      visit_url: @final_url.to_s,
                                                                                                      show_url: ad_d["displayUrl"].to_s,
                                                                                                      pause: ad_d["pause"].to_s,
                                                                                                      status: ad_d["status"].to_i,
                                                                                                      update_date: @now,                                            
                                                                                                      create_date: @now })
                                                                      @baidu_db.close()
                                                                     
                                                                  end
                                                                  
                                                              end
                                                          end
                                                          
                                                          
                                                        
                                                          
                                                      end
                                                  end
                                              end
                                              
                                              # ad done
                                              # inside groupid group array    
                                          end
                                      end
                                      
                                      
                                      
                                      # keyword, 2 loop cause their limit are different on baidu
                                      @group_adgroup_id_arr = @adgroup_id_arr.each_slice(50).to_a
                                      @group_adgroup_id_arr.each do |group_adgroup_id_arr_d|
                                      
                                          if @remain_quote >= 500
                                              service = "keyword"
                                              method = "getKeywordByAdgroupId"
                                            
                                            
                                              json = {'header' => { 
                                                                      'token' => @apitoken.to_s,
                                                                      'username' => @username.to_s,
                                                                      'password' => @password.to_s 
                                                                  },
                                                       'body'  => {
                                                                      'adgroupIds'=> group_adgroup_id_arr_d
                                                                  }
                                                      }
                                                      
                                              @keyword_info = shenma_api(service,method,json)
                                              
                                                          
                                              if !@keyword_info["header"]["desc"].nil? && @keyword_info["header"]["desc"].to_s == "执行成功"
                                                  @header = @keyword_info["header"]
                                                  @remain_quote = @header["leftQuota"]
                                                  
                                                  @keyword_result = @keyword_info["body"]["groupKeywords"]
                                                  
                                                  if @keyword_result.count.to_i > 0
                                                      @keyword_result.each do |keyword_result_d|
                                                            
                                                            
                                                          @keyword = keyword_result_d["keywordTypes"]
                                                          
                                                          if @keyword.count.to_i > 0
                                                              @keyword.each do |keyword_d|
                                                                  url_tag = 0
                                                                  
                                                                  @final_url = keyword_d["destinationUrl"].to_s
                                                                
                                                                  if !@final_url.to_s.include?(".adeqo.") && @tracking_type.to_s.downcase == "adeqo" && @keyword_redirect.to_s.downcase == "yes" && @final_url.to_s != ""
                                                  
                                                                      url_tag = 1
                                                                      @temp_final_url = @final_url
                                                                      @final_url = "http://t.adeqo.com/click?company_id="+@company_id.to_s+"&network_id="+@network_id.to_s
                                                                      @final_url = @final_url + "&campaign_id="+@campaign_id.to_s+"&adgroup_id="+keyword_d["adgroupId"].to_s+"&ad_id=0&keyword_id="+keyword_d["keywordId"].to_s
                                                                      @final_url = @final_url + "&cookie="+@cookie_length.to_s
                                                                      @final_url = @final_url + "&device=pc"
                                                                      @final_url = @final_url + "&tv=v1&durl="+CGI.escape(@temp_final_url.to_s)
                                                                  end
                                                                  
                                                                
                                                                  if url_tag == 1
                                                                      if @remain_quote.to_i >= 500
                                                                          requesttypearray = [] 
                                                                          requesttype = {}
                                                                          
                                                                          requesttype[:keywordId]    =     keyword_d["keywordId"].to_i
                                                                          requesttype[:destinationUrl]    =     @final_url
                                                                          
                                                                          requesttypearray << requesttype
                                                                      
                                                                          service = "keyword"
                                                                          method = "updateKeyword"
                                                                          
                                                                          json = {'header' => { 
                                                                                                'token' => @apitoken.to_s,
                                                                                                'username' => @username.to_s,
                                                                                                'password' => @password.to_s 
                                                                                              },
                                                                                  'body'  => {
                                                                                                'keywordTypes' => requesttypearray
                                                                                             }
                                                                                  }       
                                                                              
                                                                          @keyword_tag_update_info = shenma_api(service,method,json)
                                                                          
                                                                          @logger.info requesttypearray
                                                                          @logger.info @keyword_tag_update_info["header"]
                                                                          
                                                                          @header = @keyword_tag_update_info["header"]
                                                                          @remain_quote = @header["leftQuota"]
                                                                              
                                                                          if !@keyword_tag_update_info["header"]["desc"].nil? && @keyword_tag_update_info["header"]["desc"].to_s == "执行成功"
                                                                              
                                                                          else
                                                                              @final_url = keyword_d["destinationUrl"].to_s
                                                                          end
                                                                      end
                                                                  end
                                                                
                                                                  # @logger.info keyword_d["keywordId"].to_s
                                                                  # @logger.info keyword_d["adgroupId"].to_s
                                                                  
                                                                  db_name = "keyword_shenma_"+@network_id.to_s
                                                                  
                                                                  # @logger.info db_name.to_s
                                                                  
                                                                  
                                                                  result = @baidu_db[db_name].find({ "$and" => [{:adgroup_id => keyword_d["adgroupId"].to_i}, {:keyword_id => keyword_d["keywordId"].to_i}] } ).update_one('$set'=> { 
                                                                                                                                                                          'keyword' => keyword_d["keyword"].to_s,
                                                                                                                                                                          'pause' => keyword_d["pause"].to_s,
                                                                                                                                                                          'status' => keyword_d["status"].to_i,
                                                                                                                                                                          'match_type' => keyword_d["matchType"].to_i,
                                                                                                                                                                          'visit_url' => @final_url.to_s,
                                                                                                                                                                          'price' => keyword_d["price"].to_f,
                                                                                                                                                                          'update_date' => @now
                                                                                                                                                                     })
                                                                  @baidu_db.close()
                                                                  
                                                                  if result.n.to_i == 0
                                                                      @baidu_db[db_name].insert_one({ 
                                                                                                        network_id: @network_id.to_i,
                                                                                                        campaign_id: @campaign_id.to_i,
                                                                                                        adgroup_id: keyword_d["adgroupId"].to_i,
                                                                                                        keyword_id: keyword_d["keywordId"].to_i,
                                                                                                        keyword: keyword_d["keyword"].to_s,
                                                                                                        price: keyword_d["price"].to_f, 
                                                                                                        visit_url: @final_url.to_s,
                                                                                                        match_type: keyword_d["matchType"].to_i,
                                                                                                        pause: keyword_d["pause"].to_s,
                                                                                                        status: keyword_d["status"].to_i,
                                                                                                        update_date: @now,                                            
                                                                                                        create_date: @now })
                                                                      @baidu_db.close()
                                                                  end
                                                              end
                                                          end
                                                      end
                                                  end
                                              end
                                          end
                                      end
                                      
                                      db_name = "adgroup_shenma_"+@network_id.to_s
                                      @baidu_db[db_name].find('adgroup_id' => { "$in" => @adgroup_id_arr}).update_many('$set'=> { 
                                                                                                                                      'api_update_ad' => 0,
                                                                                                                                      'api_update_keyword' => 0,
                                                                                                                                 })
                                      @baidu_db.close()
                                  end
                                  
                                  
                                  
                                  
                                  @db["all_campaign"].find({ "$and" => [{:campaign_id => @campaign_id.to_i}, {:network_type => "shenma"}] }).update_one('$set'=> { 
                                                                                                                                             'campaign_name' => @campaign_info["body"]["campaignTypes"][0]["campaignName"].to_s,
                                                                                                                                             'budget' => @campaign_info["body"]["campaignTypes"][0]["budget"].to_f,
                                                                                                                                             'regions' => @campaign_info["body"]["campaignTypes"][0]["regionTarget"],
                                                                                                                                             'negative_words' => @campaign_info["body"]["campaignTypes"][0]["negativeWords"],
                                                                                                                                             'exact_negative_words' => @campaign_info["body"]["campaignTypes"][0]["exactNegativeWords"],
                                                                                                                                             'exclude_ips' => @campaign_info["body"]["campaignTypes"][0]["excludeIp"],
                                                                                                                                             'schedule' => @campaign_info["body"]["campaignTypes"][0]["schedule"],
                                                                                                                                             'show_prob' => @campaign_info["body"]["campaignTypes"][0]["showProb"].to_i,
                                                                                                                                             'pause' => @campaign_info["body"]["campaignTypes"][0]["pause"].to_s,
                                                                                                                                             'status' => @campaign_info["body"]["campaignTypes"][0]["status"].to_i
                                                                                                                                           })
                                  @db.close
                              
                              
                              
                              end
                          end
                      end
                      
                      # data = {:tmp => @keyword_info, :status => "true"}
                      # return render :json => data, :status => :ok
                      
                      
                  end
                  
                  
                  
                  
              end
              
              
              
              @db["all_campaign"].find({ "$and" => [{:campaign_id => @campaign_id.to_i}, {:network_type => "baidu"}] }).update_one('$set'=> { 
                                                                                                                         'api_worker' => "",
                                                                                                                         'update_date' => @now,
                                                                                                                         'api_update' => 0
                                                                                                                       })
              @db.close
              
              
              @list_campaign = @db["all_campaign"].find( '$and' => [ { 'api_update' => { '$exists' => true } }, {'network_id' => @network_id.to_i}, {'network_type' => "baidu"},{'api_update' => { "$ne" => 0}},{'api_update' => { "$ne" => 0}} ])
              @db.close
              
              if @list_campaign.count.to_i == 0
                  @db[:network].find({ "$and" => [{:id => @network_id.to_i}, {:type => "baidu"}] }).update_one('$set'=> {'file_update_1' => 4,'file_update_2' => 4,'file_update_3' => 4,'file_update_4' => 4, 'last_update' => @now})
                  @db.close
              end
          end
      end
      
      @logger.info "shenma api campaign done"
      return render :nothing => true
    
  end
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  def keyword 
    @logger.info "shenma keyword start"
    
    @id = params[:id]
    if @id.nil?
      
        # @current_network = @db[:network].find('type' => 'sogou', 'file_update_1' => 3)
        @current_network = @db[:network].find({ '$or' => [ {'file_update_1' => 3},{'file_update_2' => 3},{'file_update_3' => 3},{'file_update_4' => 3} ],'type' => 'shenma', 'worker' => @port.to_i})
        @db.close
        
        if @current_network.count.to_i >= 2
            @logger.info "working, no need update shenma"
            return render :nothing => true
        end
      
        
        @network = @db[:network].find({ "$and" => [{:type => 'shenma'},{:file_update_1 => 4},{:file_update_2 => 4},{:file_update_3 => 4},{:file_update_4 => 2}, {:worker => @port.to_i}] }).sort({ last_update: -1 }).limit(1)
        @db.close

        if @network.count.to_i == 0
            @logger.info "no need update shenma keyword"
            return render :nothing => true
        end
    else
        @network = @db[:network].find({ "$and" => [{:type => 'shenma'},{:id => @id.to_i}] })
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
            
            #check if file exist
            if doc['tmp_file'].to_s != ""
                
                @tmp_file = "/datadrive/shenma_"+doc['id'].to_s+"_"+doc['tmp_file'].to_s
                if !File.directory?(@tmp_file)
                    redownload(doc["id"])
                    @do = 0
                    @logger.info "shenma " + doc['id'].to_s + " need to re download structure"
                    return render :nothing => true
                end
            end
            
            if @do == 1
              
              
                service = "account"
                method = "getAccount"
                
                json = {'header' => { 
                                        'token' => doc["api_token"].to_s,
                                        'username' => doc["username"].to_s,
                                        'password' => doc["password"].to_s
                                    },
                         'body'  => {
                                        'requestData' => ["account_all"]
                                    }
                        }
                        
                        
                @account_info = shenma_api(service,method,json)
                if !@account_info["header"]["desc"].nil? && @account_info["header"]["desc"] == "执行成功"
                    @header = @account_info["header"]
                    @remain_quote = @header["leftQuota"]
                end
            
                @logger.info "shenma " + doc['id'].to_s + " running"
                
                @unzip_folder = @tmp_file + "/*"
                @files = Dir.glob(@unzip_folder)
                
                @logger.info "shenma " + doc["id"].to_s + " updating "
                        
                @db[:network].find(id: doc["id"].to_i).update_one('$set'=> {'file_update_4' => 3})
                @db.close
                
                db_name = "keyword_shenma_"+doc['id'].to_s
                @baidu_db[db_name].drop
                @baidu_db.close()
                    
                    
                begin
                    @baidu_db[db_name].indexes.create_one(network_id: Mongo::Index::ASCENDING)
                    @baidu_db[db_name].indexes.create_one(campaign_id: Mongo::Index::ASCENDING)
                    @baidu_db[db_name].indexes.create_one(campaign_name: Mongo::Index::ASCENDING)
                    @baidu_db[db_name].indexes.create_one(adgroup_id: Mongo::Index::ASCENDING)
                    @baidu_db[db_name].indexes.create_one(adgroup_name: Mongo::Index::ASCENDING)
                    @baidu_db[db_name].indexes.create_one(keyword_id: Mongo::Index::ASCENDING)
                    @baidu_db[db_name].indexes.create_one(keyword: Mongo::Index::ASCENDING)
                    @baidu_db[db_name].indexes.create_one(price: Mongo::Index::ASCENDING)
                    @baidu_db[db_name].indexes.create_one(match_type: Mongo::Index::ASCENDING)
                    @baidu_db[db_name].indexes.create_one(pause: Mongo::Index::ASCENDING)
                    @baidu_db[db_name].indexes.create_one(status: Mongo::Index::ASCENDING)
                    @baidu_db[db_name].indexes.create_one(cpc_quality: Mongo::Index::ASCENDING)
                    # @baidu_db[db_name].indexes.create_one(watchdog: Mongo::Index::ASCENDING)
                    @baidu_db[db_name].indexes.create_one(response_code: Mongo::Index::ASCENDING)
                    @baidu_db[db_name].indexes.create_one(m_response_code: Mongo::Index::ASCENDING)
                    @baidu_db[db_name].indexes.create_one(update_date: Mongo::Index::ASCENDING)
                    @baidu_db[db_name].indexes.create_one(create_date: Mongo::Index::ASCENDING)
                rescue Exception
                end
                
      
                @files.each_with_index do |file, index|
                    # weird quote in some field
                    
                    if file.downcase.include?("keyword") && file.downcase.include?("csv")
                        data_arr = []
                        
                        # CSV.foreach(file, :encoding => 'GB18030', :quote_char => "\x00").each_with_index do |csv, index|\
                        CSV.foreach(file).each_with_index do |csv, index|
                            begin
                                if index.to_i != 0
                                    
                                    
                                    url_tag = 0
                                    m_url_tag = 0
                                        
                                    @final_url = csv[7]
                                    
                                    # @logger.info @final_url
                                    # @logger.info "--"
                                    # @logger.info @m_final_url
                                    # @logger.info "||||||||||||||||||||||||"
                                    
                                    if !@final_url.to_s.include?(".adeqo.") && @tracking_type.to_s.downcase == "adeqo" && @keyword_redirect.to_s.downcase == "yes" && @final_url.to_s != ""
                                      
                                        url_tag = 1
                                        @temp_final_url = @final_url
                                        @final_url = "http://t.adeqo.com/click?company_id="+@company_id.to_s+"&network_id="+doc["id"].to_s
                                        @final_url = @final_url + "&campaign_id="+csv[0].to_s+"&adgroup_id="+csv[2].to_s+"&ad_id=0&keyword_id="+csv[4].to_s
                                        @final_url = @final_url + "&cookie="+@cookie_length.to_s
                                        @final_url = @final_url + "&device=pc"
                                        @final_url = @final_url + "&tv=v1&durl="+CGI.escape(@temp_final_url.to_s)
                                    end
#                                     
                                    # if !@m_final_url.to_s.include?(".adeqo.") && @tracking_type.to_s.downcase == "adeqo" && @keyword_redirect.to_s.downcase == "yes" && @final_url.to_s != ""
#                                       
                                        # m_url_tag = 1 
                                        # @temp_m_final_url = @m_final_url
                                        # @m_final_url = "http://t.adeqo.com/click?company_id="+@company_id.to_s+"&network_id="+doc["id"].to_s
                                        # @m_final_url = @m_final_url + "&campaign_id="+csv_array[@campaignId_index].gsub('"', '').to_s+"&adgroup_id="+csv_array[@adgroupId_index].to_s+"&ad_id=0&keyword_id="+csv_array[@keywordId_index].to_s
                                        # @m_final_url = @m_final_url + "&cookie="+@cookie_length.to_s
                                        # @m_final_url = @m_final_url + "&device=mobile"
                                        # @m_final_url = @m_final_url + "&tv=v1&durl="+CGI.escape(@temp_m_final_url.to_s)
                                    # end
#                                      
#                                       
                                    if url_tag == 1 
                                      
                                        @logger.info "add keyword with tag"
                                        # @logger.info @remain_quote
                                      
                                        if @remain_quote.to_i >= 500
                                            requesttypearray = [] 
                                            requesttype = {}
                                            
                                            requesttype[:keywordId]    =     csv[4].to_i
                                            requesttype[:destinationUrl]    =     @final_url
                                            
                                            
                                            requesttypearray << requesttype
                                        
                                            service = "keyword"
                                            method = "updateKeyword"
                                            
                                            json = {'header' => { 
                                                        'token' => doc["api_token"].to_s,
                                                        'username' => doc["username"].to_s,
                                                        'password' => doc["password"].to_s 
                                                          },
                                                     'body'  => {
                                                            'keywordTypes' => requesttypearray
                                                          }
                                                    }       
                                                
                                            @update_info = shenma_api(service,method,json)
                                                                    
                                            @logger.info @update_info
                                            
                                            @header = @update_info["header"]
                                            @remain_quote = @header["leftQuota"]
                                                
                                            if !@update_info["header"]["desc"].nil? && @update_info["header"]["desc"] == "执行成功"
                                                
                                            else
                                                @final_url = csv[7]
                                                # @m_final_url = csv_array[@mobileDestinationUrl_index].to_s.gsub('""', '').gsub('-', '')
                                            end
                                             
                                        end
                                    end
                                    
                                    # insert_keyword(doc["id"],csv_array,@final_url,@m_final_url)
                                    
                                    
                                    data_hash = {}
                                    insert_hash = {}
                                  
                                    insert_hash[:network_id] = doc["id"].to_i
                                    insert_hash[:campaign_id] = csv[0].to_i
                                    insert_hash[:campaign_name] = csv[1].to_s
                                    insert_hash[:adgroup_id] = csv[2].to_i
                                    insert_hash[:adgroup_name] = csv[3].to_s
                                    insert_hash[:keyword_id] = csv[4].to_i
                                    insert_hash[:keyword] = csv[5].to_s
                                    insert_hash[:price] = csv[6].to_f
                                    insert_hash[:visit_url] = @final_url.to_s
                                    insert_hash[:match_type] = csv[8].to_i
                                    insert_hash[:pause] = csv[9].to_s
                                    insert_hash[:status] = csv[10].to_i
                                    insert_hash[:cpc_quality] = csv[11].to_i
                                    insert_hash[:response_code] = ""
                                    insert_hash[:m_response_code] = ""
                                    insert_hash[:create_date] = @now
                                    insert_hash[:update_date] = @now
                                    
                                    
                                    data_hash[:insert_one] = insert_hash
                                    data_arr << data_hash
                                  
                                    if data_arr.count.to_i > 20000
                                        db_name = "keyword_shenma_"+doc["id"].to_s
                                        @baidu_db[db_name].bulk_write(data_arr)
                                        @baidu_db.close()
                                        
                                        data_arr = []
                                    end
                                    
                                end
                            rescue Exception
                                redownload(doc["id"])
                                return render :nothing => true
                            end
                        end
                        
                        if data_arr.count.to_i > 0
                            db_name = "keyword_shenma_"+doc["id"].to_s
                            @baidu_db[db_name].bulk_write(data_arr)
                            @baidu_db.close()
                            
                            data_arr = []
                        end
                    
                    end
                end      
                              
                @db[:network].find(id: doc["id"].to_i).update_one('$set'=> {'file_update_4' => 4, 'last_update' => @now, 'worker' => ""})
                @db.close              
                
                unzip_folder = "/datadrive/shenma_"+doc['id'].to_s+"_"+doc['tmp_file'].to_s
                if File.directory?(unzip_folder)
                    FileUtils.remove_dir unzip_folder, true
                end
                
            end
        rescue Exception
            redownload(doc["id"])
            return render :nothing => true
        end
    end
    
    @logger.info "shenma keyword done"
    return render :nothing => true 
  end
  
  
  
  
  
  
  
  
  
  
  
  def ad 
    @logger.info "shenma ad start"
    
    @id = params[:id]
    if @id.nil?
      
        # @current_network = @db[:network].find('type' => 'sogou', 'file_update_1' => 3)
        @current_network = @db[:network].find({ '$or' => [ {'file_update_1' => 3},{'file_update_2' => 3},{'file_update_3' => 3},{'file_update_4' => 3} ],'type' => 'shenma', 'worker' => @port.to_i})
        @db.close
        
        if @current_network.count.to_i >= 2
            @logger.info "working, no need update shenma ad"
            return render :nothing => true
        end
      
        
        
        @network = @db[:network].find({ "$and" => [{:type => 'shenma'},{:file_update_1 => 4},{:file_update_2 => 4},{:file_update_3 => 2},{:worker => @port.to_i}] }).sort({ last_update: -1 }).limit(1)
        @db.close

        if @network.count.to_i == 0
            @logger.info "no need update shenma ad"
            return render :nothing => true
        end
    else
      
        @network = @db[:network].find({ "$and" => [{:type => 'shenma'},{:id => @id.to_i}] })
        @db.close
    end
    
    @network.no_cursor_timeout.each do |doc|
        begin
        
            @tracking_type = doc["tracking_type"].to_s
            @ad_redirect = doc["ad_redirect"].to_s
            @keyword_redirect = doc["keyword_redirect"].to_s
            @company_id = doc["company_id"].to_s
            @cookie_length = doc["cookie_length"].to_s
#             
#             
            
            @do = 1
            
            #check if file exist
            if doc['tmp_file'].to_s != ""
                @tmp_file = "/datadrive/shenma_"+doc['id'].to_s+"_"+doc['tmp_file'].to_s
                if !File.directory?(@tmp_file)
                    redownload(doc["id"])
                    @do = 0
                    @logger.info "shenma " + doc['id'].to_s + " need to re download structure"
                    return render :nothing => true
                end
            end
            
            if @do == 1
            
            
                service = "account"
                method = "getAccount"
                
                json = {'header' => { 
                                        'token' => doc["api_token"].to_s,
                                        'username' => doc["username"].to_s,
                                        'password' => doc["password"].to_s
                                    },
                         'body'  => {
                                        'requestData' => ["account_all"]
                                    }
                        }
                        
                        
                @account_info = shenma_api(service,method,json)
                # @logger.info @account_info
                
                if !@account_info["header"]["desc"].nil? && @account_info["header"]["desc"] == "执行成功"
                    @header = @account_info["header"]
                    @remain_quote = @header["leftQuota"]
                end
            
            
                @logger.info "shenma ad " + doc['id'].to_s + " running"
                
                @unzip_folder = @tmp_file + "/*"
                @files = Dir.glob(@unzip_folder)
                
                @logger.info "shenma ad " + doc["id"].to_s + " updating "
                        
                @db[:network].find(id: doc["id"].to_i).update_one('$set'=> {'file_update_3' => 3})
                @db.close
                
                db_name = "ad_shenma_"+doc['id'].to_s
                @baidu_db[db_name].drop
                @baidu_db.close()
                
                begin
                    @baidu_db[db_name].indexes.create_one(network_id: Mongo::Index::ASCENDING)
                    @baidu_db[db_name].indexes.create_one(campaign_id: Mongo::Index::ASCENDING)
                    @baidu_db[db_name].indexes.create_one(campaign_name: Mongo::Index::ASCENDING)
                    @baidu_db[db_name].indexes.create_one(adgroup_id: Mongo::Index::ASCENDING)
                    @baidu_db[db_name].indexes.create_one(adgroup_name: Mongo::Index::ASCENDING)
                    @baidu_db[db_name].indexes.create_one(ad_id: Mongo::Index::ASCENDING)
                    @baidu_db[db_name].indexes.create_one(title: Mongo::Index::ASCENDING)
                    @baidu_db[db_name].indexes.create_one(description: Mongo::Index::ASCENDING)
                    @baidu_db[db_name].indexes.create_one(pause: Mongo::Index::ASCENDING)
                    @baidu_db[db_name].indexes.create_one(status: Mongo::Index::ASCENDING)
                    @baidu_db[db_name].indexes.create_one(response_code: Mongo::Index::ASCENDING)
                    @baidu_db[db_name].indexes.create_one(m_response_code: Mongo::Index::ASCENDING)
                    @baidu_db[db_name].indexes.create_one(update_date: Mongo::Index::ASCENDING)
                    @baidu_db[db_name].indexes.create_one(create_date: Mongo::Index::ASCENDING)
                rescue Exception
                end
                
                @files.each_with_index do |file, index|
                    # only this level need the quote, cause some string has weird quote
                    if file.downcase.include?("creatives") && file.downcase.include?("csv")
                        data_arr = []
                        
                        # CSV.foreach(file, :encoding => 'GB18030', :quote_char => "\x00").each_with_index do |csv, index|
                        CSV.foreach(file).each_with_index do |csv, index|
                            begin
                                if index.to_i != 0
                                    
                                    url_tag = 0
                                    # m_url_tag = 0
                                    
                                    @final_url = csv[7]
                                    # @m_final_url = csv_array[@mobileDestinationUrl_index].to_s.gsub('""', '').gsub('-', '')
                                    
                                    
                                    if !@final_url.to_s.include?(".adeqo.") && @tracking_type.to_s.downcase == "adeqo" && @ad_redirect.to_s.downcase == "yes" && @final_url.to_s != ""
                                        
                                        @temp_final_url = @final_url
                                        @final_url = "http://t.adeqo.com/click?company_id="+@company_id.to_s+"&network_id="+doc["id"].to_s
                                        @final_url = @final_url + "&campaign_id="+csv[0].to_s+"&adgroup_id="+csv[2].to_s+"&ad_id="+csv[4].to_s+"&keyword_id=0"
                                        @final_url = @final_url + "&cookie="+@cookie_length.to_s
                                        @final_url = @final_url + "&device=pc"
                                        @final_url = @final_url + "&tv=v1&durl="+CGI.escape(@temp_final_url.to_s)
                                        
                                        url_tag = 1
                                    end
#                                     
                                    # if !@m_final_url.to_s.include?(".adeqo.") && @tracking_type.to_s.downcase == "adeqo" && @ad_redirect.to_s.downcase == "yes" && @m_final_url.to_s != ""
#                                          
                                        # @temp_m_final_url = @m_final_url
                                        # @m_final_url = "http://t.adeqo.com/click?company_id="+@company_id.to_s+"&network_id="+doc["id"].to_s
                                        # @m_final_url = @m_final_url + "&campaign_id="+csv_array[@campaignId_index].gsub('"', '').to_s+"&adgroup_id="+csv_array[@adgroupId_index].to_s+"&ad_id="+csv_array[@creativeId_index].to_s+"&keyword_id=0"
                                        # @m_final_url = @m_final_url + "&cookie="+@cookie_length.to_s
                                        # @m_final_url = @m_final_url + "&device=mobile"
                                        # @m_final_url = @m_final_url + "&tv=v1&durl="+CGI.escape(@temp_m_final_url.to_s)
#                                         
                                        # m_url_tag = 1
                                    # end
#                                     
                                    # # @logger.info csv_array
                                    # # @logger.info @final_url
                                    # # @logger.info "-"
                                    # # @logger.info @m_final_url
#                                       
                                    if url_tag == 1 
                                      
                                        @logger.info "add ad with tag"
                                        # @logger.info @remain_quote
                                      
                                        if @remain_quote.to_i >= 500
                                            requesttypearray = [] 
                                            requesttype = {}
                                            requesttype[:creativeId]    =     csv[4].to_i
                                            requesttype[:destinationUrl]    =     @final_url
                                            
                                            requesttypearray << requesttype
                                        
                                            service = "creative"
                                            method = "updateCreative"
                                            
                                            json = {'header' => { 
                                                        'token' => doc["api_token"].to_s,
                                                        'username' => doc["username"].to_s,
                                                        'password' => doc["password"].to_s 
                                                          },
                                                     'body'  => {
                                                            'creativeTypes' => requesttypearray
                                                          }
                                                    }       
                                                
                                            @update_info = shenma_api(service,method,json)
                                                                                                               
                                            @logger.info @update_info 
                                            
                                            @header = @update_info["header"]
                                            @remain_quote = @header["leftQuota"]
                                                
                                            if !@update_info["header"]["desc"].nil? && @update_info["header"]["desc"] == "执行成功"
                                                
                                            else
                                                @final_url = @final_url = csv[7]
                                            end
                                             
                                        end
                                    end
                                    
                                    # insert_ad(doc["id"],csv_array,@final_url,@m_final_url)
                                    
                                    
                                    data_hash = {}
                                    insert_hash = {}
                                  
                                    insert_hash[:network_id] = doc["id"].to_i
                                    insert_hash[:campaign_id] = csv[0].to_i
                                    insert_hash[:campaign_name] = csv[1].to_s
                                    insert_hash[:adgroup_id] = csv[2].to_i
                                    insert_hash[:adgroup_name] = csv[3].to_s
                                    insert_hash[:ad_id] = csv[4].to_i
                                    insert_hash[:title] = csv[5].to_s
                                    insert_hash[:description] = csv[6].to_s
                                    insert_hash[:visit_url] = @final_url.to_s
                                    insert_hash[:show_url] = csv[8].to_s
                                    insert_hash[:pause] = csv[9].to_s
                                    insert_hash[:status] = csv[10].to_i
                                    insert_hash[:response_code] = ""
                                    insert_hash[:m_response_code] = ""
                                    insert_hash[:create_date] = @now
                                    insert_hash[:update_date] = @now
                                    
                                    
                                        
                                    data_hash[:insert_one] = insert_hash
                                    data_arr << data_hash
                                  
                                    if data_arr.count.to_i > 5000
                                        db_name = "ad_shenma_"+doc["id"].to_s
                                        @baidu_db[db_name].bulk_write(data_arr)
                                        @baidu_db.close()
                                        
                                        data_arr = []
                                    end
                                    
                                    
                                    
                                    
                                end
                            rescue Exception
                                redownload(doc["id"])
                                return render :nothing => true
                            end
                        end
                        
                        if data_arr.count.to_i > 0
                            db_name = "ad_shenma_"+doc["id"].to_s
                            @baidu_db[db_name].bulk_write(data_arr)
                            @baidu_db.close()
                            
                            data_arr = []
                        end
                    end
                end      
                              
                @db[:network].find(id: doc["id"].to_i).update_one('$set'=> {'file_update_3' => 4, 'last_update' => @now})
                @db.close              
                
                unzip_folder = "/datadrive/shenma_"+doc['id'].to_s+"_"+doc['tmp_file'].to_s
                if File.directory?(unzip_folder)
                    # FileUtils.remove_dir unzip_folder, true
                end
                
            end
        rescue Exception
            redownload(doc["id"])
            return render :nothing => true
        end
    end
    
    @logger.info "shenma ad done"
    return render :nothing => true 
  end
  
  
  
  
  
  
  
  
  
  def adgroup 
    @logger.info "shenma adgroup start"
    
    @id = params[:id]
    if @id.nil?
      
        
        @current_network = @db[:network].find({ '$or' => [ {'file_update_1' => 3},{'file_update_2' => 3},{'file_update_3' => 3},{'file_update_4' => 3} ],'type' => 'shenma', 'worker' => @port.to_i})
        @db.close
        
        if @current_network.count.to_i >= 3
            @logger.info "working, no need update shenma adgroup"
            return render :nothing => true
        end
      
        
        @network = @db[:network].find({ "$and" => [{:type => 'shenma'},{:file_update_1 => 4},{:file_update_2 => 2},{:worker => @port.to_i}] }).sort({ last_update: -1 }).limit(1)
        @db.close

        if @network.count.to_i == 0
            @logger.info "no need update shenma adgroup"
            return render :nothing => true
        end
    else
      
        @network = @db[:network].find({ "$and" => [{:type => 'shenma'},{:id => @id.to_i}] })
        @db.close
    end
    
    @network.no_cursor_timeout.each do |doc|
        begin
            @do = 1
            
            #check if file exist
            if doc['tmp_file'].to_s != ""
                @tmp_file = "/datadrive/shenma_"+doc['id'].to_s+"_"+doc['tmp_file'].to_s
                if !File.directory?(@tmp_file)
                    redownload(doc["id"])
                    @do = 0
                    @logger.info "shenma " + doc['id'].to_s + " need to re download structure"
                    return render :nothing => true
                end
            end
            
            if @do == 1
            
                @logger.info "shenma adgroup " + doc['id'].to_s + " running"
                
                @unzip_folder = @tmp_file + "/*"
                @files = Dir.glob(@unzip_folder)
                
                @logger.info "shenma adgroup " + doc["id"].to_s + " updating "
                        
                @db[:network].find(id: doc["id"].to_i).update_one('$set'=> {'file_update_2' => 3})
                @db.close
                
                db_name = "adgroup_shenma_"+doc['id'].to_s
                @baidu_db[db_name].drop
                @baidu_db.close()
                                
                begin
                    @baidu_db[db_name].indexes.create_one(network_id: Mongo::Index::ASCENDING)
                    @baidu_db[db_name].indexes.create_one(campaign_id: Mongo::Index::ASCENDING)
                    @baidu_db[db_name].indexes.create_one(campaign_name: Mongo::Index::ASCENDING)
                    @baidu_db[db_name].indexes.create_one(adgroup_id: Mongo::Index::ASCENDING)
                    @baidu_db[db_name].indexes.create_one(adgroup_name: Mongo::Index::ASCENDING)
                    @baidu_db[db_name].indexes.create_one(max_price: Mongo::Index::ASCENDING)
                    @baidu_db[db_name].indexes.create_one(adPlatformOS: Mongo::Index::ASCENDING)
                    @baidu_db[db_name].indexes.create_one(negative_words: Mongo::Index::ASCENDING)
                    @baidu_db[db_name].indexes.create_one(exact_negative_words: Mongo::Index::ASCENDING)
                    @baidu_db[db_name].indexes.create_one(pause: Mongo::Index::ASCENDING)
                    @baidu_db[db_name].indexes.create_one(status: Mongo::Index::ASCENDING)
                    
                    @baidu_db[db_name].indexes.create_one(api_update_ad: Mongo::Index::ASCENDING)
                    @baidu_db[db_name].indexes.create_one(api_update_keyword: Mongo::Index::ASCENDING)
                    @baidu_db[db_name].indexes.create_one(api_worker: Mongo::Index::ASCENDING)
                    
                    @baidu_db[db_name].indexes.create_one(update_date: Mongo::Index::ASCENDING)
                    @baidu_db[db_name].indexes.create_one(create_date: Mongo::Index::ASCENDING)
                rescue Exception
                end
                
                @files.each_with_index do |file, index|
                    if file.downcase.include?("adgroup") && file.downcase.include?("csv")
                    data_arr = []
                  
                    # CSV.foreach(file, :encoding => 'GB18030', :quote_char => "\x00").each_with_index do |csv, index|
                    CSV.foreach(file).each_with_index do |csv, index|
                        begin
                            if index.to_i != 0
                                
                                data_hash = {}
                                insert_hash = {}
                              
                                insert_hash[:network_id] = doc["id"].to_i
                                insert_hash[:campaign_id] = csv[0].to_i
                                insert_hash[:campaign_name] = csv[1].to_s
                                insert_hash[:adgroup_id] = csv[2].to_i
                                insert_hash[:adgroup_name] = csv[3].to_s
                                insert_hash[:max_price] = csv[4].to_f
                                insert_hash[:adPlatformOS] = csv[5].to_i
                                insert_hash[:negative_words] = csv[6].to_s
                                insert_hash[:exact_negative_words] = csv[7].to_s
                                insert_hash[:pause] = csv[8].to_s
                                insert_hash[:status] = csv[9].to_i
                                
                                insert_hash[:api_update_ad] = 0
                                insert_hash[:api_update_keyword] = 0
                                insert_hash[:api_worker] = ""
                                
                                insert_hash[:create_date] = @now
                                insert_hash[:update_date] = @now
                                
                                
                                    
                                data_hash[:insert_one] = insert_hash
                                data_arr << data_hash
                              
                                if data_arr.count.to_i > 5000
                                    db_name = "adgroup_shenma_"+doc["id"].to_s
                                    @baidu_db[db_name].bulk_write(data_arr)
                                    @baidu_db.close()
                                    
                                    data_arr = []
                                end
                                
                                
                            end
                        rescue Exception
                            redownload(doc["id"])
                            return render :nothing => true
                        end
                    end
                    
                    if data_arr.count.to_i > 0
                        db_name = "adgroup_shenma_"+doc["id"].to_s
                        @baidu_db[db_name].bulk_write(data_arr)
                        @baidu_db.close()
                        
                        data_arr = []
                    end
                    
                    end
                end      
                              
                @db[:network].find(id: doc["id"].to_i).update_one('$set'=> {'file_update_2' => 4, 'last_update' => @now})
                @db.close              
                
                unzip_folder = "/datadrive/shenma_"+doc['id'].to_s+"_"+doc['tmp_file'].to_s
                if File.directory?(unzip_folder)
                    # FileUtils.remove_dir unzip_folder, true
                end
                
            end
        rescue Exception
            redownload(doc["id"])
            return render :nothing => true
        end
    end
    
    @logger.info "baidu adgroup done"
    return render :nothing => true 
  end
  
  
  



  def campaign 
    @logger.info "shenma campaign start"
    
    @id = params[:id]
    if @id.nil?
        
        @current_network = @db[:network].find({ '$or' => [ {'file_update_1' => 3},{'file_update_2' => 3},{'file_update_3' => 3},{'file_update_4' => 3} ],'type' => 'shenma', 'worker' => @port.to_i})
        @db.close
        
        if @current_network.count.to_i >= 3
            @logger.info "working, no need update shenma campaign"
            return render :nothing => true
        end
        
        
      
        @network = @db[:network].find({ "$and" => [{:type => 'shenma'},{:file_update_1 => 2},{:worker => @port.to_i}] }).sort({ last_update: -1 }).limit(1)
        @db.close

        if @network.count.to_i == 0
            @logger.info "no need update shenma campaign"
            return render :nothing => true
        end
    else
        @network = @db[:network].find({ "$and" => [{:type => 'shenma'},{:id => @id.to_i}] })
        @db.close
    end
    
    @network.no_cursor_timeout.each do |doc|
        begin
            @do = 1
            
            #check if file exist
            if doc['tmp_file'].to_s != ""
                @tmp_file = "/datadrive/shenma_"+doc['id'].to_s+"_"+doc['tmp_file'].to_s
                if !File.directory?(@tmp_file)
                    redownload(doc["id"])
                    @do = 0
                    @logger.info "shenma " + doc['id'].to_s + " need to re download structure"
                    return render :nothing => true
                end
            end
            
            if @do == 1
            
                @logger.info "shenma campaign " + doc['id'].to_s + " running"
                
                @unzip_folder = @tmp_file + "/*"
                @files = Dir.glob(@unzip_folder)
                
                @logger.info "shenma campaign " + doc["id"].to_s + " updating "
                        
                @db[:network].find(id: doc["id"].to_i).update_one('$set'=> {'file_update_1' => 3})
                @db.close
                @db["all_campaign"].find(network_id: doc["id"].to_i).delete_many
                @db.close
                
                @files.each_with_index do |file, file_index|
                    
                    
                    if file.downcase.include?("campaign") && file.downcase.include?("csv")
                        # @logger.info file
                        data_arr = []
                        # CSV.foreach(file, :encoding => 'GB18030', :quote_char => "\x00").each_with_index do |csv, index|
                        CSV.foreach(file).each_with_index do |csv, index|
                          
                            @logger.info csv
                            
                            # data = {
                                  # :result => csv,
                                  # :status => "true"
                                  # }
                            # return render :json => data, :status => :ok
                          
                          
                          
                            begin
                                if index.to_i != 0
                                  
                                    data_hash = {}
                                    insert_hash = {}
                                  
                                    insert_hash[:network_id] = doc["id"].to_i
                                    insert_hash[:network_type] = "shenma"
                                    insert_hash[:account_name] = doc["name"].to_s
                                    insert_hash[:campaign_id] = csv[0].to_i
                                    insert_hash[:campaign_name] = csv[1].to_s
                                    insert_hash[:budget] = csv[2].to_f
                                    insert_hash[:regions] = csv[3].to_s
                                    insert_hash[:exclude_ips] = csv[4].to_s
                                    insert_hash[:negative_words] = csv[5]
                                    insert_hash[:exact_negative_words] = csv[6]
                                    insert_hash[:schedule] = csv[7]
                                    insert_hash[:show_prob] = csv[8].to_s
                                    insert_hash[:pause] = csv[9].to_s
                                    insert_hash[:status] = csv[10].to_i
                                    
                                    insert_hash[:api_update] = 0
                                    insert_hash[:api_worker] = ""
                                
                                    insert_hash[:create_date] = @now
                                    insert_hash[:update_date] = @now
                                    
                                    
                                        
                                    data_hash[:insert_one] = insert_hash
                                    data_arr << data_hash
                                  
                                    if data_arr.count.to_i > 1000
                                        @db["all_campaign"].bulk_write(data_arr)
                                        @db.close
                                        
                                        data_arr = []
                                    end
                                  
                                    
                                    
                                end
                            rescue Exception
                                redownload(doc["id"])
                                return render :nothing => true
                            end
                        end
                        
                        if data_arr.count.to_i > 0
                            @db["all_campaign"].bulk_write(data_arr)
                            @db.close
                            
                            data_arr = []
                        end
                    
                    end
                end   
                              
                @db[:network].find(id: doc["id"].to_i).update_one('$set'=> {'file_update_1' => 4, 'last_update' => @now})
                @db.close              
                
                unzip_folder = "/datadrive/shenma_"+doc['id'].to_s+"_"+doc['tmp_file'].to_s
                if File.directory?(unzip_folder)
                    # FileUtils.remove_dir unzip_folder, true
                end
                
            end
        rescue Exception
            redownload(doc["id"])
            return render :nothing => true
        end
    end
    
    
    @logger.info "shenma campaign done"
    return render :nothing => true 
  end



  def dlaccfile
    
      @logger.info "shenma dlaccfile start"
      
      
      @all_network = @db[:network].find()
      @db.close
      
      @dl_limit = @all_network.count.to_i / 4  
      
      @all_work_network = @db[:network].find('worker' => @port.to_i)
      @db.close
      
      if @all_work_network.count.to_i >= @dl_limit.to_i
          @logger.info "shenma dlaccfile limit"
          return render :nothing => true
      end
    
    
      @id = params[:id]
      if @id.nil?
          # @network = @db[:network].find('type' => 'sogou', 'file_update_1' => {'$lt' => 2}, 'file_update_2' => {'$lt' => 2}, 'file_update_3' => {'$lt' => 2}, 'file_update_4' => {'$lt' => 2})
          
          
          
          @current_network = @db[:network].find({ "$and" => [{:type => 'shenma'},{:file_update_1 => 1},{:file_update_2 => 1},{:file_update_3 => 1},{:file_update_4 => 1},{:worker => @port.to_i}] }).sort({ last_update: -1 }).limit(1)
          @db.close
          
          if @current_network.count.to_i >= 1
              @logger.info "shenma dl working"
              return render :nothing => true
          end
              
          @network = @db[:network].find({ "$and" => [{:type => 'shenma'},{:file_update_1 => 0},{:file_update_2 => 0},{:file_update_3 => 0},{:file_update_4 => 0},{:worker => @port.to_i}] }).sort({ last_update: -1 }).limit(1)
          @db.close
          
          if @network.count.to_i == 0
            
              @network = @db[:network].find({ "$and" => [{:type => 'shenma'},{:file_update_1 => 0},{:file_update_2 => 0},{:file_update_3 => 0},{:file_update_4 => 0},{:worker => ""}] }).sort({ last_update: -1 }).limit(1)
              @db.close
            
              if @network.count.to_i == 0
                  @logger.info "no need to dl shenma"
                  return render :nothing => true
              end
          end
          
      else
          
          @network = @db[:network].find({ "$and" => [{:type => 'shenma'},{:id => @id.to_i}] })
          @db.close
      end
      
      @network.no_cursor_timeout.each do |doc|
          
              @logger.info "shenma dlaccfile " + doc['id'].to_s + " running"
              @db[:network].find(id: doc["id"].to_i).update_one('$set'=> {'file_update_1' => 1, 'file_update_2' => 1, 'file_update_3' => 1, 'file_update_4' => 1, 'worker' => @port.to_i, 'last_update' => @now})
              @db.close
              
              if doc["run_time"].to_i >= 10
                  @db[:network].find(id: doc["id"].to_i).update_one('$set'=> { 'fileid' => "", 'tmp_file' => "",'run_time' => 0,'file_update_1' => 0, 'file_update_2' => 0, 'file_update_3' => 0, 'file_update_4' => 0, 'worker' => "", 'last_update' => @now })
                  @db.close
              else
                  
                  if doc['tmp_file'].to_s == "" && doc['fileid'].to_s == ""
                    
                      service = "bulkJob"
                      method = "getAllObjects"
                      
                      json = {'header' => { 
                                              'token' => doc['api_token'].to_s,
                                              'username' => doc['username'].to_s,
                                              'password' => doc['password'].to_s
                                          },
                               'body'  => {
                                          }
                              }
                               
                      @result = shenma_api(service,method,json)
                    
                    
                      # data = {
                              # :result => @result,
                              # :status => "true"
                              # }
                      # return render :json => data, :status => :ok
                      
                      
                      @header = @result["header"]
                      @quota = @header["leftQuota"]
                      
                      # @logger.info @result
                      # @logger.info @header
                      
                      if @header["desc"].downcase == "success"
                          @db[:network].find(id: doc["id"].to_i).update_one('$set'=> { 'file_update_1' => 0, 'file_update_2' => 0, 'file_update_3' => 0, 'file_update_4' => 0, 'fileid' => @result["body"]["taskId"].to_i, 'last_update' => @now })
                          @db.close
                      else
                          @db[:network].find(id: doc["id"].to_i).update_one('$set'=> { 'file_update_1' => 0, 'file_update_2' => 0, 'file_update_3' => 0, 'file_update_4' => 0, 'run_time' => 0, 'fileid' => "", 'tmp_file' => "", 'worker' => "", 'last_update' => @now })
                          @db.close
                      end
                      
                  
                  elsif doc['tmp_file'].to_s != ""
                    
                      @db[:network].find(id: doc["id"].to_i).update_one('$set'=> { 'file_update_1' => 2, 'file_update_2' => 2, 'file_update_3' => 2, 'file_update_4' => 2, 'worker' => @port.to_i })
                      @db.close
                      
                  elsif doc['tmp_file'].to_s == "" && doc['fileid'].to_s != ""
                      
                      
                      service = "task"
                      method = "getTaskState"
                      
                      json = {'header' => { 
                                              'token' => doc['api_token'].to_s,
                                              'username' => doc['username'].to_s,
                                              'password' => doc['password'].to_s
                                          },
                               'body'  => {
                                              "taskId" => doc['fileid'].to_i
                                          }
                              }
                              
                      @result = shenma_api(service,method,json)
                      
                      
                      
                      
                      @header = @result["header"]
                      @quota = @header["leftQuota"]
                      
                      if @header["desc"].downcase == "success"
                          if @result["body"]["status"].to_s == "FINISHED" && @result["body"]["progress"].to_i == 1 && @result["body"]["success"].to_s == "true"
                            
                              service = "file"
                              method = "download"
                              
                              json = {'header' => { 
                                                      'token' => doc['api_token'].to_s,
                                                      'username' => doc['username'].to_s,
                                                      'password' => doc['password'].to_s
                                                  },
                                       'body'  => {
                                                      "fileId" => @result["body"]["fileId"].to_i
                                                  }
                                      }
                                      
                              @result = shenma_api(service,method,json)
                              
                              
                              
                              zip_file_name = "/datadrive/"+doc['type'].to_s + "_" +doc['id'].to_s + "_" + doc['fileid'].to_s+".zip"
                              unzip_file_name = "/datadrive/"+doc['type'].to_s + "_" +doc['id'].to_s + "_" + doc['fileid'].to_s
                              
                              
                              File.binwrite zip_file_name, @result
                              
                              unzip_file(zip_file_name.to_s, unzip_file_name.to_s)
                              File.delete(zip_file_name)
                              
                              @db[:network].find(id: doc["id"].to_i).update_one('$set'=> {  
                                                                                            'tmp_file' => doc['fileid'].to_s,
                                                                                            'fileid' => "",
                                                                                            'run_time' => 0,
                                                                                            'file_update_1' => 2,
                                                                                            'file_update_2' => 2,
                                                                                            'file_update_3' => 2,
                                                                                            'file_update_4' => 2,
                                                                                            'last_update' => @now
                                                                                          })
                            
                              @db.close
                              
                          else
                              @logger.info "shenma dlacc not ready, retry later"
                            
                              @db[:network].find(id: doc["id"].to_i).update_one('$set'=> { 'file_update_1' => 0, 'file_update_2' => 0, 'file_update_3' => 0, 'file_update_4' => 0, 'worker' => '', 'last_update' => @now })
                              @db.close
                          end
                          
                      else
                          @logger.info "baidu dlacc fail, reset"
                          run_time = doc["run_time"].to_i + 1
                          @db[:network].find(id: doc["id"].to_i).update_one('$set'=> { 'tmp_file' => "" , 'fileid' => doc['fileid'].to_s, 'run_time' => run_time.to_i, 'file_update_1' => 0, 'file_update_2' => 0, 'file_update_3' => 0, 'file_update_4' => 0, 'worker' => "", 'last_update' => @now})
                          @db.close
                      end
                  end
                  
              end
              
         
          
      end
    
      @logger.info "baidu dlaccfile done"
      return render :nothing => true 
    
                          
  end











  def test
    
    
      # service = "bulkJob"
      # method = "getAllObjects"
#       
      # json = {'header' => { 
                              # 'token' => "fea038ce-b8a5-4ec2-9512-a27fb594c4b2",
                              # 'username' => "携程周末游",
                              # 'password' => "Ctrip2017+"
                          # },
               # 'body'  => {
                          # }
              # }
#                
      # @bulk = shenma_api(service,method,json)
    
    
      # service = "task"
      # method = "getTaskState"
#       
      # json = {'header' => { 
                              # 'token' => "fea038ce-b8a5-4ec2-9512-a27fb594c4b2",
                              # 'username' => "携程周末游",
                              # 'password' => "Ctrip2017+"
                          # },
               # 'body'  => {
                              # 'taskId' => 1079623753
                          # }
              # }
#                
      # @tmp2 = shenma_api(service,method,json)
#       
      # service = "file"
      # method = "download"
#       
      # json = {'header' => { 
                              # 'token' => "fea038ce-b8a5-4ec2-9512-a27fb594c4b2",
                              # 'username' => "携程周末游",
                              # 'password' => "Ctrip2017+"
                          # },
               # 'body'  => {
                              # 'fileId' => 1079623753
                          # }
              # }
#               
      # @tmp_file = shenma_api(service,method,json)
#     
#       
      # # contents = open(@tmp_file, "rb") {|io| io.read }  
#       
#       
      # # FileUtils.mv(@tmp_file, "/datadrive")
#       
      # # IO.copy_stream @tmp_file, "/datadrive"
#       
      # File.binwrite "/datadrive/test.zip", @tmp_file
#       
      # # send_data @tmp_file, filename: "sad", type: 'zip', disposition: 'attachment'
# 
#     
      # data = {
              # # :bulk => contents,
              # :status => "true"
              # }
      # return render :json => data, :status => :ok
    
  end



  def avgposition
      @logger.info "called shenma avg position upper"
        
      @days = params[:day]
      @default_day = 1
      
      if !@days.nil?
        @default_day = @days  
      end
      
      @id = params[:id]
      
      if @id.nil?
        
          if @days.nil?
              @current_network = @db[:network].find({ "$and" => [{:type => 'shenma'}, {:avg_pos => 1}, {:avg_worker => @port.to_i}] })
              @db.close
              
              if @current_network.count.to_i >= 1
                  @logger.info "one shenma avg pos upper working"
                  return render :nothing => true
              end
              
              
              
              
              @network = @db[:network].find({ "$and" => [{:type => 'shenma'}, {:report => 2}, {:avg_pos => 0}, {:avg_worker => @port.to_i}] }).sort({ last_update: -1 }).limit(1)
              @db.close
              
              if @network.count.to_i == 0
                  @network = @db[:network].find({ "$and" => [{:type => 'shenma'}, {:report => 2}, {:avg_pos => 0}, {:avg_worker => ""}] }).sort({ last_update: -1 }).limit(1)
                  @db.close
              end
              
              
          else
              @network = @db[:network].find('type' => 'shenma')
              @db.close  
          end
          
      else
          @network = @db[:network].find({ "$and" => [{:type => 'shenma'}, {:id => @id.to_i}] })
          @db.close
      end
      
    
      @today = Date.today.in_time_zone('Beijing') 
      edit_day = @today - @default_day.to_i.days
      @today = edit_day.strftime("%Y-%m-%d")
      
      
      @network.no_cursor_timeout.each do |doc|
        
             begin
             @logger.info "shenma avg position network " + doc['id'].to_s + " running"
             
             
             if @id.nil? && @days.nil?
                 @db[:network].find('id' => doc["id"].to_i).update_one('$set'=> { 'avg_pos' => 1,'last_update' => @now, 'avg_worker' => @port.to_i })
                 @db.close
             end
            
            
             @logger.info "shenma avg position network " + doc['id'].to_s + " update adgroup"
             
             db_name = "adgroup_shenma_"+doc['id'].to_s
             @adgroup = @baidu_db[db_name].find()
             # @adgroup = @baidu_db[db_name].find("adgroup_id" => 2191333616)
             @baidu_db.close()
             
             temp_adgroup_id_arr = []
             temp_adgroup_id_hash = {}
             
             if @adgroup.count.to_i > 0
                @adgroup.no_cursor_timeout.each do |adgroup|
                    temp_adgroup_id_arr << adgroup["adgroup_id"].to_i
    
                    temp_adgroup_id_hash["display"+adgroup["adgroup_id"].to_s] = 0
                    temp_adgroup_id_hash["avg_pos"+adgroup["adgroup_id"].to_s] = 0
                end
             end
             
             
              
             if temp_adgroup_id_arr.count.to_i
                @keyword_report = @db3[:shenma_report_keyword].find('adgroup_id' => { "$in" => temp_adgroup_id_arr}, "report_date" => @today.to_s)
                @db3.close()
                 
                if @keyword_report.count.to_i > 0
      
                    @keyword_report.no_cursor_timeout.each do |keyword_report|
                        temp_adgroup_id_hash["display"+keyword_report["adgroup_id"].to_s] = temp_adgroup_id_hash["display"+keyword_report["adgroup_id"].to_s] + keyword_report["display"].to_i
                        if keyword_report['display'].to_i > 0 && keyword_report['avg_position'].to_f > 0
                          temp_adgroup_id_hash["avg_pos"+keyword_report["adgroup_id"].to_s] = temp_adgroup_id_hash["avg_pos"+keyword_report["adgroup_id"].to_s].to_f.round(2) + (keyword_report['avg_position'].to_f.round(2) * keyword_report['display'].to_f)
                        end
                    end
                  
                end
             end
             
             
             
             if @adgroup.count.to_i > 0
                @adgroup.no_cursor_timeout.each do |adgroup|
                    if temp_adgroup_id_hash["display"+adgroup["adgroup_id"].to_s].to_i > 0 && temp_adgroup_id_hash["avg_pos"+adgroup["adgroup_id"].to_s].to_f > 0
                    
                        insert_avg_value = temp_adgroup_id_hash["avg_pos"+adgroup["adgroup_id"].to_s].to_f / temp_adgroup_id_hash["display"+adgroup["adgroup_id"].to_s].to_f
                        
                        @db3[:shenma_report_adgroup].find({ "$and" => [{:cpc_grp_id => adgroup["adgroup_id"].to_i}, {:report_date => @today.to_s}] }).update_one('$set'=>{     
                                                                                                                                                            avg_position: insert_avg_value.to_f
                                                                                                                                                  })
                        @db3.close()
                    
                    end
                end
             end
           
             
             @logger.info "shenma avg position network " + doc['id'].to_s + " update adgroup done"
             @logger.info "shenma avg position network " + doc['id'].to_s + " update campaign"
             
             @campaign = @db["all_campaign"].find('network_id' => doc['id'].to_i, 'network_type' => "shenma")
             @db.close
             
             temp_campaign_id_arr = []
             temp_campaign_id_hash = {}
             
             if @campaign.count.to_i > 0
                 @campaign.no_cursor_timeout.each do |campaign|
                    temp_campaign_id_arr << campaign["campaign_id"]
                    
                    temp_campaign_id_hash["display"+campaign["campaign_id"].to_s] = 0
                    temp_campaign_id_hash["avg_pos"+campaign["campaign_id"].to_s] = 0
                 end
             end
             
             @keyword_report = @db3[:shenma_report_keyword].find('campaign_id' => { "$in" => temp_campaign_id_arr}, "report_date" => @today.to_s)
             @db3.close()      
             
             
             if @keyword_report.count.to_i > 0
#       
                @keyword_report.no_cursor_timeout.each do |keyword_report|
                    temp_campaign_id_hash["display"+keyword_report["campaign_id"].to_s] = temp_campaign_id_hash["display"+keyword_report["campaign_id"].to_s] + keyword_report["display"].to_i
                    
                    if keyword_report['display'].to_i > 0 && keyword_report['avg_position'].to_f > 0
                        temp_campaign_id_hash["avg_pos"+keyword_report["campaign_id"].to_s] = temp_campaign_id_hash["avg_pos"+keyword_report["campaign_id"].to_s].to_f.round(2) + (keyword_report['avg_position'].to_f.round(2) * keyword_report['display'].to_f)
                    end
                end
             end
             
             
             if @campaign.count.to_i > 0
                @campaign.no_cursor_timeout.each do |campaign|
                  
                  
                    if temp_campaign_id_hash["display"+campaign["campaign_id"].to_s].to_i > 0 && temp_campaign_id_hash["avg_pos"+campaign["campaign_id"].to_s].to_f > 0
                    
                        insert_avg_value = temp_campaign_id_hash["avg_pos"+campaign["campaign_id"].to_s].to_f / temp_campaign_id_hash["display"+campaign["campaign_id"].to_s].to_f
                        
                        @db3[:shenma_report_campaign].find({ "$and" => [{:campaign_id => campaign["campaign_id"].to_i}, {:report_date => @today.to_s}] }).update_one('$set'=>{     
                                                                                                                                                            avg_position: insert_avg_value.to_f
                                                                                                                                                  })
                        
                        @db3.close()
                    
                    end
                end
             end       
             
             
                   
             @logger.info "campaign avg position network " + doc['id'].to_s + " update campaign done"
             @logger.info "campaign avg position network " + doc['id'].to_s + " update account"
             
             
             @keyword_report = @db3[:shenma_report_keyword].find('network_id' => doc['id'].to_i, "report_date" => @today.to_s)
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
                
                @db3[:shenma_report_account].find({ "$and" => [{:network_id => doc['id'].to_i}, {:report_date => @today.to_s}] }).update_one('$set'=>{     
                                                                                                                                          avg_position: insert_avg_value.to_f
                                                                                                                                })                                                                                                                   
                @db3.close()
             end
             
             @logger.info "shenma avg position network " + doc['id'].to_s + " update account done"
             
             
             if @id.nil? && @days.nil?
                 @db[:network].find('id' => doc["id"].to_i).update_one('$set'=> { 'avg_pos' => 2,'last_update' => @now, 'avg_worker' => "" })
                 @db.close
             end
             
             rescue Exception
                @logger.info "shenma avg position network " + doc['id'].to_s + " fail"
                
                @db[:network].find('id' => doc["id"].to_i).update_one('$set'=> { 'avg_upper' => 0,'last_update' => @now, 'avg_worker' => "" })
                @db.close
             end
      end   
      
      @logger.info "called shenma avg position upper done"
      return render :nothing => true
  end



  
  def report
    
      @logger.info "called report shenma"
      
      @days = params[:day]
      @default_day = 1
      
      if !@days.nil?
        @default_day = @days  
      end
      
      @id = params[:id]
      
      @today = Date.today.in_time_zone('Beijing') 
      edit_day = @today - @default_day.to_i.days
      
      request_end_date = edit_day
      request_start_date = request_end_date
      
      @end_date = request_end_date.strftime("%Y-%m-%d")
      @start_date = request_start_date.strftime("%Y-%m-%d")
      
      
      if @id.nil?
        
          if @days.nil?
              @current_network = @db[:network].find({ "$and" => [{:type => 'shenma'},{:report => 1},{:report_worker => @port.to_i}] })
              @db.close
              
              if @current_network.count.to_i >= 1
                  @logger.info "one shenma report working"
                  return render :nothing => true
              end
              
              @network = @db[:network].find({ "$and" => [{:type => 'shenma'},{:report => 0},{:report_worker => @port.to_i}] }).sort({ last_update: -1 }).limit(1)
              @db.close
              
              if @network.count.to_i == 0
                  @network = @db[:network].find({ "$and" => [{:type => 'shenma'},{:report => 0},{:report_worker => ""}] }).sort({ last_update: -1 }).limit(1)
                  @db.close  
              end
          else
              @network = @db[:network].find('type' => 'shenma')
              @db.close  
          end
          
      else
          @network = @db[:network].find({ "$and" => [{:type => 'shenma'},{:id => @id.to_i}] })
          @db.close
      end
  
  
      # @network = @db[:network].find('type' => 'baidu', 'id' => 113)
      # @db.close
      
      if @network.count.to_i > 0 
      
          @network.no_cursor_timeout.each do |network_d|
              # @apitoken = network_d["api_token"]
              # @username = network_d["username"]
              # @password = network_d["password"]
              
              # begin
                  @logger.info "get report shenma network "+network_d["id"].to_s
                  
                  if @id.nil? && @days.nil?
                      @db[:network].find(id: network_d["id"].to_i).update_one('$set'=> {'report' => 1, "report_worker" => @port.to_i,'last_update' => @now})
                      @db.close
                  end
                  getreport(network_d["id"],network_d["username"],network_d["password"],network_d["api_token"],network_d["report_account"],network_d["report_campaign"],network_d["report_adgroup"],network_d["report_ad"],network_d["report_keyword"],@start_date,@end_date)
              
              # rescue Exception
#                   
                  # resetnetworkreport(network_d["id"])
#                 
              # end

          end
      end
                        
      data = {:message => "shenma report done", :status => "true"}
      return render :json => data, :status => :ok
    
  end



  def getreport(networkid,username,password,apitoken,account,campaign,adgroup,ad,keyword,start_date,end_date)
    
      @logger.info "called getreport shenma" + networkid.to_s
      @logger.info account.to_s
      @logger.info campaign.to_s
      @logger.info adgroup.to_s
      @logger.info ad.to_s
      
      if account.to_s == "" || campaign.to_s == "" || adgroup.to_s == "" || ad.to_s == "" || keyword.to_s == ""
        
        # if one of them doenst has report id, then get the id first, must download all report together
        @logger.info "called getreport id shenma"+networkid.to_s
        getfileid(networkid,username,password,apitoken,account,campaign,adgroup,ad,keyword,start_date,end_date)
      else
        # if all of them has id, dl report and insert
        @logger.info "called download report file shenma"+networkid.to_s
        getreportfile(networkid,username,password,apitoken,account,campaign,adgroup,ad,keyword,start_date,end_date)
      end
    
  end
  
  
  
  def getreportfile(networkid,username,password,apitoken,account,campaign,adgroup,ad,keyword,start_date,end_date)
      
      #before dl, check all file status
      @logger.info "shenma getreportfile, check file status" + networkid.to_s
      account_report_status = reportfilestatus(networkid,username,password,apitoken,account)
      campaign_report_status = reportfilestatus(networkid,username,password,apitoken,campaign)
      adgroup_report_status = reportfilestatus(networkid,username,password,apitoken,adgroup)
      ad_report_status = reportfilestatus(networkid,username,password,apitoken,ad)
      keyword_report_status = reportfilestatus(networkid,username,password,apitoken,keyword)
    
      @logger.info "shenma getreportfile, check file status done"
      
      if account_report_status.to_i == 1 && campaign_report_status.to_i == 1 && adgroup_report_status.to_i == 1 && ad_report_status.to_i == 1 && keyword_report_status.to_i == 1
        
          @logger.info "shenma getreportfile, dl file start"
          dlreportfile(networkid,username,password,apitoken,account,"account",start_date,end_date)
          dlreportfile(networkid,username,password,apitoken,campaign,"campaign",start_date,end_date)
          dlreportfile(networkid,username,password,apitoken,adgroup,"adgroup",start_date,end_date)
          dlreportfile(networkid,username,password,apitoken,ad,"ad",start_date,end_date)
          dlreportfile(networkid,username,password,apitoken,keyword,"keyword",start_date,end_date)
          
          
          @logger.info "shenma getreportfile, report all done. set to 2"
          
          if @id.nil? && @days.nil?
              @db[:network].find(id: networkid.to_i).update_one('$set'=> {'report' => 2,'report_account' => "",'report_campaign' => "",'report_adgroup' => "",'report_ad' => "",'report_keyword' => "",'report_worker' => ""})
              @db.close
          end
          
      elsif account_report_status.to_i == 0 && campaign_report_status.to_i == 0 && adgroup_report_status.to_i == 0 && ad_report_status.to_i == 0 && keyword_report_status.to_i == 0
        
          @db[:network].find(id: networkid.to_i).update_one('$set'=> {'report' => 0, 'report_account' => "",'report_campaign' => "",'report_adgroup' => "",'report_ad' => "",'report_keyword' => ""})
          @db.close
        
      else
        
          # @logger.info "baidu account_report_status, "+account_report_status.to_s
          # @logger.info "baidu campaign_report_status, "+campaign_report_status.to_s
          # @logger.info "baidu adgroup_report_status, "+adgroup_report_status.to_s
          # @logger.info "baidu ad_report_status, "+ad_report_status.to_s
          # @logger.info "baidu keyword_report_status, "+keyword_report_status.to_s
          
          @db[:network].find(id: networkid.to_i).update_one('$set'=> {'report' => 0})
          @db.close      
      end
    
  end
  
  
  
  def getfileid(networkid,username,password,apitoken,account,campaign,adgroup,ad,keyword,start_date,end_date)
      @logger.info "called shenma getfileid network "+networkid.to_s
      
      account_report_id = account
      campaign_report_id = campaign
      adgroup_report_id = adgroup
      ad_report_id = ad
      keyword_report_id = keyword
      
      service = "report"
      method = "getReport"
      
      
      # @logger.info account.to_s
      # @logger.info campaign.to_s
      # @logger.info adgroup.to_s
      # @logger.info ad.to_s
      # @logger.info keyword.to_s
      
      
      
      if account.to_s == ""
          
          json = {'header' => { 
                                  'token' => apitoken.to_s,
                                  'username' => username.to_s,
                                  'password' => password.to_s
                              },
                   'body'  => {
                                  'startDate' => start_date,
                                  'endDate' => end_date,
                                  'reportType' => 2,
                                  "levelOfDetails" => 2,
                                  "statRange"=> 2,
                                  'idOnly' => false,
                                  'unitOfTime' => 5,
                                  'performanceData' => ["cost","cpc","click","impression","ctr"],
                                  "format" => 2
                              }
                  }
                  
           @result = shenma_api(service,method,json)
           # @logger.info "______________________________________________________________"
           @logger.info @result.to_s+" "+networkid.to_s
           # @logger.info "______________________________________________________________"
           
           if @result["header"].nil?
               data = {:message => "shenma api error", :result => @result, :json => json, :status => "false"}
               return render :json => data, :status => :ok
           else
               @header = @result["header"]
               
               if @header["desc"].downcase == "success"
                  @quota = @header["leftQuota"]
                  
                  if !@result["body"]["taskId"].nil?
                      account_report_id = @result["body"]["taskId"].to_s
                  end
               end
           end
      end
      
      if campaign.to_s == ""
          
          json = {'header' => { 
                                  'token' => apitoken.to_s,
                                  'username' => username.to_s,
                                  'password' => password.to_s
                              },
                   'body'  => {
                                  'startDate' => start_date,
                                  'endDate' => end_date,
                                  'reportType' => 10,
                                  "levelOfDetails" => 3,
                                  # "statRange"=> 3,
                                  'idOnly' => false,
                                  'unitOfTime' => 5,
                                  'performanceData' => ["cost","cpc","click","impression","ctr"],
                                  "format" => 2
                              }
                  }
                  
           @result = shenma_api(service,method,json)
           # @logger.info "______________________________________________________________"
           @logger.info @result.to_s+" "+networkid.to_s
           # @logger.info "______________________________________________________________"
           
           if @result["header"].nil?
               data = {:message => "shenma api error", :result => @result, :json => json, :status => "false"}
               return render :json => data, :status => :ok
           else
               @header = @result["header"]
               if @header["desc"].downcase == "success"
                 
                  @quota = @header["leftQuota"]
                  
                  if !@result["body"]["taskId"].nil?
                      campaign_report_id = @result["body"]["taskId"].to_s
                  end
               end
           end
           
      end
      
      if adgroup.to_s == ""
          
          json = {'header' => { 
                                  'token' => apitoken.to_s,
                                  'username' => username.to_s,
                                  'password' => password.to_s
                              },
                   'body'  => {
                                  'startDate' => start_date,
                                  'endDate' => end_date,
                                  'reportType' => 11,
                                  "levelOfDetails" => 5,
                                  # "statRange"=> 5,
                                  'idOnly' => false,
                                  'unitOfTime' => 5,
                                  'performanceData' => ["cost","cpc","click","impression","ctr"],
                                  "format" => 2
                              }
                  }
                  
           @result = shenma_api(service,method,json)
           # @logger.info "______________________________________________________________"
           @logger.info @result.to_s+" "+networkid.to_s
           # @logger.info "______________________________________________________________"
           
           if @result["header"].nil?
               data = {:message => "shenma api error", :result => @result, :json => json, :status => "false"}
               return render :json => data, :status => :ok
           else
               @header = @result["header"]
               if @header["desc"].downcase == "success"
                  @quota = @header["leftQuota"]
                  
                  if !@result["body"]["taskId"].nil?
                      adgroup_report_id = @result["body"]["taskId"].to_s
                  end
               end
           end
      end
      
      if ad.to_s == ""
          
          json = {'header' => { 
                                  'token' => apitoken.to_s,
                                  'username' => username.to_s,
                                  'password' => password.to_s
                              },
                   'body'  => {
                                  'startDate' => start_date,
                                  'endDate' => end_date,
                                  'reportType' => 12,
                                  "levelOfDetails" => 7,
                                  # "statRange"=> 7,
                                  'idOnly' => false,
                                  'unitOfTime' => 5,
                                  'performanceData' => ["cost","cpc","click","impression","ctr"],
                                  "format" => 2
                              }
                  }
                  
           @result = shenma_api(service,method,json)
           # @logger.info "______________________________________________________________"
           @logger.info @result.to_s+" "+networkid.to_s
           # @logger.info "______________________________________________________________"
           
           if @result["header"].nil?
               data = {:message => "shenma api error", :result => @result, :json => json, :status => "false"}
               return render :json => data, :status => :ok
           else
               @header = @result["header"]
               if @header["desc"].downcase == "success"
                  @quota = @header["rquota"]
                  
                  if !@result["body"]["taskId"].nil?
                      ad_report_id = @result["body"]["taskId"].to_s
                  end
               end
           end
          
      end
      
      if keyword.to_s == ""
      
      
          json = {'header' => { 
                                  'token' => apitoken.to_s,
                                  'username' => username.to_s,
                                  'password' => password.to_s
                              },
                   'body'  => {
                                  'startDate' => start_date,
                                  'endDate' => end_date,
                                  'reportType' => 14,
                                  "levelOfDetails" => 11,
                                  # "statRange"=> 11,
                                  'idOnly' => false,
                                  'unitOfTime' => 5,
                                  'performanceData' => ["cost","cpc","click","impression","ctr","rank"],
                                  "format" => 2
                              }
                  }
                  
           @result = shenma_api(service,method,json)
           # @logger.info "______________________________________________________________"
           @logger.info @result.to_s+" "+networkid.to_s
           # @logger.info "______________________________________________________________"
           
           if @result["header"].nil?
               data = {:message => "shenma api error", :result => @result, :json => json, :status => "false"}
               return render :json => data, :status => :ok
           else
               @header = @result["header"]
               if @header["desc"].downcase == "success"
                  @quota = @header["rquota"]
                  
                  if !@result["body"]["taskId"].nil?
                      keyword_report_id = @result["body"]["taskId"].to_s
                  end
               end
           end
      
      end
       
      @db[:network].find('id' => networkid.to_i).update_one('$set'=> { 'report_account' => account_report_id.to_s,
                                                                       'report_campaign' => campaign_report_id.to_s,
                                                                       'report_adgroup' => adgroup_report_id.to_s,
                                                                       'report_ad' => ad_report_id.to_s,
                                                                       'report_keyword' => keyword_report_id.to_s,
                                                                       'report' => 0,
                                                                       'last_update' => @now })
      @db.close
      
  end
  
  
  


  def report_tmp
    
      @logger.info "called report shenma"
    
      @yesterday = @today.to_date - 1.days
      
      @account_report_status = ""
      @account_report_dl = ""
      
      
      
      
      service = "file"
      method = "download"
       
      
      json = {'header' => { 
                              'token' => "fea038ce-b8a5-4ec2-9512-a27fb594c4b2",
                              'username' => "携程周末游",
                              'password' => "Ctrip2017+"
                          },
               'body'  => {
                              "fileId" => 1152921504612881616
                          }
              }
              
              
              
      @result = shenma_api(service,method,json)
              
      
      
      data = {
              :report => @result,
              :today => @today, 
              :yesterday => @yesterday, 
              :status => "true"
              }
      return render :json => data, :status => :ok
      
      
      # json = {'header' => { 
                              # 'token' => "fea038ce-b8a5-4ec2-9512-a27fb594c4b2",
                              # 'username' => "携程周末游",
                              # 'password' => "Ctrip2017+"
                          # },
               # 'body'  => {
                              # 'startDate' => "2016-01-01",
                              # 'endDate' => "2016-01-01",
#                               
                              # # 'startDate' => @yesterday.to_s,
                              # # 'endDate' => @today.to_s,
                              # 'reportType' => 10,
                              # 'idOnly' => false,
                              # 'unitOfTime' => 5,
                              # 'performanceData' => ["cost","cpc","click","impression","ctr"],
                              # "format" => 2
                          # }
              # }
#               
      # @campaign_report = shenma_api(service,method,json)
#       
#       
#       
#       
#       
      # json = {'header' => { 
                              # 'token' => "fea038ce-b8a5-4ec2-9512-a27fb594c4b2",
                              # 'username' => "携程周末游",
                              # 'password' => "Ctrip2017+"
                          # },
               # 'body'  => {
                              # 'startDate' => "2016-01-01",
                              # 'endDate' => "2016-01-01",
#                               
                              # # 'startDate' => @yesterday.to_s,
                              # # 'endDate' => @today.to_s,
                              # 'reportType' => 11,
                              # 'idOnly' => false,
                              # 'unitOfTime' => 5,
                              # 'performanceData' => ["cost","cpc","click","impression","ctr"],
                              # "format" => 2
                          # }
              # }
#               
      # @adgroup_report = shenma_api(service,method,json)
#       
#       
      # json = {'header' => { 
                              # 'token' => "fea038ce-b8a5-4ec2-9512-a27fb594c4b2",
                              # 'username' => "携程周末游",
                              # 'password' => "Ctrip2017+"
                          # },
               # 'body'  => {
                              # 'startDate' => "2016-01-01",
                              # 'endDate' => "2016-01-01",
#                               
                              # # 'startDate' => @yesterday.to_s,
                              # # 'endDate' => @today.to_s,
                              # 'reportType' => 14,
                              # 'idOnly' => false,
                              # 'unitOfTime' => 5,
                              # 'performanceData' => ["cost","cpc","click","impression","ctr","rank"],
                              # "format" => 2
                          # }
              # }
#               
      # @keyword_report = shenma_api(service,method,json)
#       
#       
#       
#       
      # json = {'header' => { 
                              # 'token' => "fea038ce-b8a5-4ec2-9512-a27fb594c4b2",
                              # 'username' => "携程周末游",
                              # 'password' => "Ctrip2017+"
                          # },
               # 'body'  => {
                              # 'startDate' => "2016-01-01",
                              # 'endDate' => "2016-01-01",
#                               
                              # # 'startDate' => @yesterday.to_s,
                              # # 'endDate' => @today.to_s,
                              # 'reportType' => 12,
                              # 'idOnly' => false,
                              # 'unitOfTime' => 5,
                              # 'performanceData' => ["cost","cpc","click","impression","ctr"],
                              # "format" => 2
                          # }
              # }
#               
      # @ad_report = shenma_api(service,method,json)
      
#       
      # service = "task"
      # method = "getTaskState"
#       
      # json = {'header' => { 
                              # 'token' => "fea038ce-b8a5-4ec2-9512-a27fb594c4b2",
                              # 'username' => "携程周末游",
                              # 'password' => "Ctrip2017+"
                          # },
               # 'body'  => {
                              # 'taskId' => @account_report["body"]["taskId"]
                          # }
              # }
#                
      # @report_status = shenma_api(service,method,json)
#       
#       
#       
      # service = "file"
      # method = "download"
#                       
      # json = {'header' => { 
                              # 'token' => "fea038ce-b8a5-4ec2-9512-a27fb594c4b2",
                              # 'username' => "携程周末游",
                              # 'password' => "Ctrip2017+"
                          # },
               # 'body'  => {
                              # 'fileId' => @report_status["body"]["fileId"]
                          # }
              # }
#                               
      # @report_dl = shenma_api(service,method,json) 
      
      
      # if @account_report["header"]["desc"] == "success"
          # if !@account_report["body"]["taskId"].nil? && @account_report["body"]["taskId"] != ""
#             
              # service = "task"
              # method = "getTaskState"
#               
              # json = {'header' => { 
                                      # 'token' => "fea038ce-b8a5-4ec2-9512-a27fb594c4b2",
                                      # 'username' => "携程周末游",
                                      # 'password' => "Ctrip2017+"
                                  # },
                       # 'body'  => {
                                      # 'taskId' => @account_report["body"]["taskId"]
                                  # }
                      # }
#                        
              # @account_report_status = shenma_api(service,method,json)
#             
#             
#             
              # if @account_report_status["header"]["desc"] == "success"
                  # if !@account_report_status["body"]["fileId"].nil? && @account_report_status["body"]["fileId"] != "" && @account_report_status["body"]["status"] == "FINISHED" && @account_report_status["body"]["success"] == true
#                       
                      # service = "file"
                      # method = "download"
#                       
                      # json = {'header' => { 
                                              # 'token' => "fea038ce-b8a5-4ec2-9512-a27fb594c4b2",
                                              # 'username' => "携程周末游",
                                              # 'password' => "Ctrip2017+"
                                          # },
                               # 'body'  => {
                                              # 'fileId' => @account_report_status["body"]["fileId"]
                                          # }
                              # }
#                               
                      # @account_report_dl = shenma_api(service,method,json) 
                  # end
              # end
#             
          # end
      # end
#       
      # service = "task"
      # method = "getTaskState"
#       
      # json = {'header' => { 
                              # 'token' => "fea038ce-b8a5-4ec2-9512-a27fb594c4b2",
                              # 'username' => "携程周末游",
                              # 'password' => "Ctrip2017+"
                          # },
               # 'body'  => {
                              # 'taskId' => 1152921504612714500
                          # }
              # }
#                
      # @account_report_status = shenma_api(service,method,json)
      
      
      # service = "bulkJob"
      # method = "getAllObjects"
#       
      # json = {'header' => { 
                              # 'token' => "fea038ce-b8a5-4ec2-9512-a27fb594c4b2",
                              # 'username' => "携程周末游",
                              # 'password' => "Ctrip2017+"
                          # },
               # 'body'  => {
                          # }
              # }
#                
      # @tmp1 = shenma_api(service,method,json)
#       
#       
      # service = "task"
      # method = "getTaskState"
#       
      # json = {'header' => { 
                              # 'token' => "fea038ce-b8a5-4ec2-9512-a27fb594c4b2",
                              # 'username' => "携程周末游",
                              # 'password' => "Ctrip2017+"
                          # },
               # 'body'  => {
                              # 'taskId' => 1079608311
                          # }
              # }
#                
      # @tmp2 = shenma_api(service,method,json)
#       
      # service = "file"
      # method = "download"
#       
      # json = {'header' => { 
                              # 'token' => "fea038ce-b8a5-4ec2-9512-a27fb594c4b2",
                              # 'username' => "携程周末游",
                              # 'password' => "Ctrip2017+"
                          # },
               # 'body'  => {
                              # 'fileId' => 1079608311
                          # }
              # }
#               
      # @tmp_file = shenma_api(service,method,json)
      
      
      
      
      
      
      
      
      
      
      
      
      # @days = params[:day]
      # @default_day = 1
#       
      # if !@days.nil?
        # @default_day = @days  
      # end
#       
      # @id = params[:id]
#       
      # @today = Date.today.in_time_zone('Beijing') 
      # edit_day = @today - @default_day.to_i.days
#       
      # request_end_date = edit_day
      # request_start_date = request_end_date
#       
      # @end_date = request_end_date.strftime("%Y-%m-%d")
      # @start_date = request_start_date.strftime("%Y-%m-%d")
#       
#       
      # if @id.nil?
#         
          # if @days.nil?
              # @current_network = @db[:network].find({ "$and" => [{:type => 'shenma'},{:report => 1},{:report_worker => @port.to_i}] })
              # @db.close
#               
              # if @current_network.count.to_i >= 1
                  # @logger.info "one baidu report working"
                  # return render :nothing => true
              # end
#               
              # @network = @db[:network].find({ "$and" => [{:type => 'shenma'},{:report => 0},{:report_worker => @port.to_i}] }).sort({ last_update: -1 }).limit(1)
              # @db.close
#               
              # if @network.count.to_i == 0
                  # @network = @db[:network].find({ "$and" => [{:type => 'shenma'},{:report => 0},{:report_worker => ""}] }).sort({ last_update: -1 }).limit(1)
                  # @db.close  
              # end
          # else
              # @network = @db[:network].find('type' => 'shenma')
              # @db.close  
          # end
#           
      # else
          # @network = @db[:network].find({ "$and" => [{:type => 'shenma'},{:id => @id.to_i}] })
          # @db.close
      # end
#       
#       
#       
      # if @network.count.to_i > 0 
#       
          # @network.each do |network_d|
#               
              # # begin
                  # @logger.info "get report baidu network "+network_d["id"].to_s
                  # @db[:network].find(id: network_d["id"].to_i).update_one('$set'=> {'report' => 1, "report_worker" => @port.to_i,'last_update' => @now})
                  # @db.close
                  # getreport(network_d["id"],network_d["username"],network_d["password"],network_d["api_token"],network_d["report_account"],network_d["report_campaign"],network_d["report_adgroup"],network_d["report_ad"],network_d["report_keyword"],@start_date,@end_date)
#               
              # # rescue Exception
# #                   
                  # # resetnetworkreport(network_d["id"])
# #                 
              # # end
# 
          # end
      # end
#                         
      # data = {:message => "shenma report done", :status => "true"}
      # return render :json => data, :status => :ok
      
      # return render :nothing => true
  end


  def getreport(networkid,username,password,apitoken,account,campaign,adgroup,ad,keyword,start_date,end_date)
    
      @logger.info "called getreport shenma" + networkid.to_s
      @logger.info account.to_s
      @logger.info campaign.to_s
      @logger.info adgroup.to_s
      @logger.info ad.to_s
      
      if account.to_s == "" || campaign.to_s == "" || adgroup.to_s == "" || ad.to_s == "" || keyword.to_s == ""
        
        # if one of them doenst has report id, then get the id first, must download all report together
        @logger.info "called getreport id shenma"+networkid.to_s
        getfileid(networkid,username,password,apitoken,account,campaign,adgroup,ad,keyword,start_date,end_date)
        @logger.info "done getfileid shenma "+networkid.to_s
      else
        # if all of them has id, dl report and insert
        @logger.info "called download report file shenma"+networkid.to_s
        getreportfile(networkid,username,password,apitoken,account,campaign,adgroup,ad,keyword,start_date,end_date)
      end
    
  end 
  
  
  


  
  
  
  def reportfilestatus(networkid,username,password,apitoken,reportid)
      
      # @logger.info "reportfilestatus"
      
      
      
      service = "task"
      method = "getTaskState"
      
      json = {'header' => { 
                          'token' => apitoken.to_s,
                              'username' => username.to_s,
                              'password' => password.to_s 
                          },
               'body'  => {
                              "taskId" => reportid.to_i
                          }
              }       
      
      @result = shenma_api(service,method,json)
      
      @header = @result["header"]
      @quota = @header["leftQuota"]
       
      @logger.info @result.to_s
      
      if @result["header"]["desc"] == "success"
          if !@result["body"]["fileId"].nil? && @result["body"]["fileId"] != "" && @result["body"]["status"] == "FINISHED" && @result["body"]["success"] == true
              # return @result["body"]["data"][0]["isGenerated"]
              return 1
          end
      else
          return 0    
      end
    
  end
  

  def dlreportfile(networkid,username,password,apitoken,reportid,level,start_date,end_date)
    
      @logger.info "called dlreportfile shenma"+networkid.to_s
      
      
      
      
      service = "file"
      method = "download"
       
      json = {'header' => { 
                              'token' => apitoken.to_s,
                              'username' => username.to_s,
                              'password' => password.to_s 
                          },
               'body'  => {
                              "fileId" => reportid.to_i
                          }
              }
              
      @result = shenma_api(service,method,json)
      
      # @header = @result["header"]
      # @quota = @header["leftQuota"]
      
      # @logger.info @result
      # @logger.info "account"
      
      if !@result.nil?                              
            
            zip_file_name = "/datadrive/shenma_"+networkid.to_s + "_" + reportid.to_s+"_report_"+level.to_s+".csv"
            
            
            File.binwrite zip_file_name, @result
            
            # unzip_file(zip_file_name.to_s, unzip_file_name.to_s)
            # File.delete(zip_file_name)
            
            @file = zip_file_name
            
            # quote_chars = %w(" | ~ ^ & * \\)
            
            
            @logger.info zip_file_name
            
            
            if level == "account"
              
              
                # @logger.info "work in account"
              
                @db3[:shenma_report_account].find({ "$and" => [{:network_id => networkid.to_i}, {:report_date => end_date.to_s}] }).delete_many
                @db3.close()
                
                data_arr = []
              
                # CSV.foreach(@file, :encoding => 'GB18030', :quote_char => quote_chars.shift).each_with_index do |csv, index|
                CSV.foreach(@file, :quote_char => "|").each_with_index do |csv, index|
                  
                    # @logger.info csv
                    
                    if index.to_i == 0
                        set_csv_header(csv)
                    else
                      
                        data_hash = {}
                        insert_hash = {}
                      
                        insert_hash[:network_id] = networkid.to_i
                        insert_hash[:report_date] = csv[@time_index].to_s
                        insert_hash[:name] = csv[@account_index].to_s
                        insert_hash[:total_cost] = csv[@cost_index].gsub('"', '').to_f
                        insert_hash[:clicks_avg_price] = csv[@avg_cpc_index].gsub('"', '').to_f
                        insert_hash[:display] = csv[@impression_index].gsub('"', '').to_i
                        insert_hash[:click_rate] = csv[@click_rate_index].gsub('"', '').gsub('%', '').strip.to_f
                        insert_hash[:clicks] = csv[@click_index].gsub('"', '').to_i
                        insert_hash[:avg_position] = 0
                        
                        
                        data_hash[:insert_one] = insert_hash
                        data_arr << data_hash
                      
                        if data_arr.count.to_i > 200
                            @db3[:shenma_report_account].bulk_write(data_arr)
                            @db3.close()
                            
                            data_arr = []
                        end
                      
                      
                    end
                end
                
                if data_arr.count.to_i > 0
                    @db3[:shenma_report_account].bulk_write(data_arr)
                    @db3.close()
                end
                
                
            elsif level == "campaign"
              
                @db3[:shenma_report_campaign].find({ "$and" => [{:network_id => networkid.to_i}, {:report_date => end_date.to_s}] }).delete_many
                @db3.close()
                
                data_arr = []
              
              
                CSV.foreach(@file, :quote_char => "|").each_with_index do |csv, index|
                    
                    
                    if index.to_i == 0
                        set_csv_header(csv)
                    else
                      
                        data_hash = {}
                        insert_hash = {}
                      
                        insert_hash[:network_id] = networkid.to_i
                        insert_hash[:report_date] = csv[@time_index].to_s
                        insert_hash[:name] = csv[@account_index].to_s
                        insert_hash[:campaign_id] = csv[@campaign_id_index].to_i
                        insert_hash[:campaign_name] = csv[@campaign_index].to_s
                        insert_hash[:total_cost] = csv[@cost_index].gsub('"', '').to_f
                        insert_hash[:clicks_avg_price] = csv[@avg_cpc_index].gsub('"', '').to_f
                        insert_hash[:display] = csv[@impression_index].gsub('"', '').to_i
                        insert_hash[:click_rate] = csv[@click_rate_index].gsub('"', '').gsub('%', '').strip.to_f
                        insert_hash[:clicks] = csv[@click_index].gsub('"', '').to_i
                        insert_hash[:avg_position] = 0
                        
                        
                        data_hash[:insert_one] = insert_hash
                        data_arr << data_hash
                      
                        if data_arr.count.to_i > 200
                            @db3[:shenma_report_campaign].bulk_write(data_arr)
                            @db3.close()
                            
                            data_arr = []
                        end
                      
                    end
                    
                end
                
                if data_arr.count.to_i > 0
                    @db3[:shenma_report_campaign].bulk_write(data_arr)
                    @db3.close()
                end
              
            elsif level == "adgroup"
              
              
                @db3[:shenma_report_adgroup].find({ "$and" => [{:network_id => networkid.to_i}, {:report_date => end_date.to_s}] }).delete_many
                @db3.close()
              
              
                data_arr = []
                
                CSV.foreach(@file, :quote_char => "|").each_with_index do |csv, index|
                    # @logger.info csv
                    
                    if index.to_i == 0
                        set_csv_header(csv)
                    else
                        data_hash = {}
                        insert_hash = {}
                      
                        insert_hash[:network_id] = networkid.to_i
                        insert_hash[:report_date] = csv[@time_index].to_s
                        insert_hash[:name] = csv[@account_index].to_s
                        insert_hash[:campaign_id] = csv[@campaign_id_index].to_i
                        insert_hash[:campaign_name] = csv[@campaign_index].to_s
                        insert_hash[:adgroup_id] = csv[@adgroup_id_index].to_i
                        insert_hash[:adgroup_name] = csv[@adgroup_index].to_s
                        insert_hash[:total_cost] = csv[@cost_index].gsub('"', '').to_f
                        insert_hash[:clicks_avg_price] = csv[@avg_cpc_index].gsub('"', '').to_f
                        insert_hash[:display] = csv[@impression_index].gsub('"', '').to_i
                        insert_hash[:click_rate] = csv[@click_rate_index].gsub('"', '').gsub('%', '').strip.to_f
                        insert_hash[:clicks] = csv[@click_index].gsub('"', '').to_i
                        insert_hash[:avg_position] = 0
                        
                        
                        data_hash[:insert_one] = insert_hash
                        data_arr << data_hash
                      
                        if data_arr.count.to_i > 500
                            @db3[:shenma_report_adgroup].bulk_write(data_arr)
                            @db3.close()
                            
                            data_arr = []
                        end
                    end
                end
                
                if data_arr.count.to_i > 0
                    @db3[:shenma_report_adgroup].bulk_write(data_arr)
                    @db3.close()
                end
                
            elsif level == "ad"
              
                @db3[:shenma_report_ad].find({ "$and" => [{:network_id => networkid.to_i}, {:report_date => end_date.to_s}] }).delete_many
                @db3.close()
                
                data_arr = []
                
                CSV.foreach(@file, :col_sep => "\t", :force_quotes => false, :quote_char => "|").each_with_index do |csv, index|
                # CSV.foreach(@file, :quote_char => "|").each_with_index do |csv, index|
                    
                    csv_str = csv[0]
                    
                    
                    if index.to_i == 0
                        
                        csv_str = csv_str.gsub('"', '').to_s
                        csv_array = csv_str.split(",")
                        
                        @logger.info csv_str
                        @logger.info "||||||||||||||||||||||||||||||||||||||||||"
                        @logger.info csv_array
                        
                        set_csv_header(csv_array)
                        
                        # @logger.info @ad_id_index
                        # @logger.info @impression_index
                        # @logger.info @cost_index
                        # @logger.info @avg_cpc_index
                        # @logger.info @click_index
                    else
                      
                        # @logger.info csv[0]
                        # @logger.info csv
                        
                        # @logger.info csv_str
                        
                        scan_ad_str = csv_str.scan(/"([^"]*)"/)
                        scan_ad_str = '"'+scan_ad_str[0][0]+'"'
                        
                        csv_str = csv_str.gsub(scan_ad_str, '""').to_s
                        csv_array = csv_str.split(",")
                        
                        scan_ad_str = scan_ad_str.gsub('"', '').to_s
                        
                        # if csv_array[@ad_id_index].to_i == 1172595130
                            # @logger.info csv_array[@time_index]
                            # @logger.info csv_array[@account_index]
                            # @logger.info csv_array[@campaign_id_index]
                            # @logger.info csv_array[@adgroup_id_index]
                            # @logger.info csv_array[@adgroup_index]
                            # @logger.info csv_array[@ad_id_index]
                            # @logger.info scan_ad_str
                            # @logger.info csv_array[@cost_index]
                            # @logger.info csv_array[@avg_cpc_index]
                            # @logger.info csv_array[@impression_index]
                            # @logger.info csv_array[@click_rate_index]
                            # @logger.info csv_array[@click_index]
                        # end
                        
                        data_hash = {}
                        insert_hash = {}
                        
                        insert_hash[:network_id] = networkid.to_i
                        insert_hash[:report_date] = csv_array[@time_index].to_s
                        insert_hash[:name] = csv_array[@account_index].to_s
                        insert_hash[:campaign_id] = csv_array[@campaign_id_index].to_i
                        insert_hash[:campaign_name] = csv_array[@campaign_index].to_s
                        insert_hash[:adgroup_id] = csv_array[@adgroup_id_index].to_i
                        insert_hash[:adgroup_name] = csv_array[@adgroup_index].to_s
                        insert_hash[:ad_id] = csv_array[@ad_id_index].to_i
                        insert_hash[:title] = scan_ad_str.to_s
                        insert_hash[:total_cost] = csv_array[@cost_index].gsub('"', '').to_f
                        insert_hash[:clicks_avg_price] = csv_array[@avg_cpc_index].gsub('"', '').to_f
                        insert_hash[:display] = csv_array[@impression_index].gsub('"', '').to_i
                        insert_hash[:click_rate] = csv_array[@click_rate_index].gsub('"', '').gsub('%', '').strip.to_f
                        insert_hash[:clicks] = csv_array[@click_index].gsub('"', '').to_i
                        insert_hash[:avg_position] = 0
#                         
                        data_hash[:insert_one] = insert_hash
                        data_arr << data_hash
                      
                        if data_arr.count.to_i > 500
                            @db3[:shenma_report_ad].bulk_write(data_arr)
                            @db3.close()
                            
                            data_arr = []
                        end
                        
                    end
                    
                end
              
                if data_arr.count.to_i > 0
                    @db3[:shenma_report_ad].bulk_write(data_arr)
                    @db3.close()
                end
              
            elsif level == "keyword"
              
                @db3[:shenma_report_keyword].find({ "$and" => [{:network_id => networkid.to_i}, {:report_date => end_date.to_s}] }).delete_many
                @db3.close()
              
                data_arr = []
              
                CSV.foreach(@file, :quote_char => "|").each_with_index do |csv, index|
                # CSV.foreach(@file, :col_sep => "\t", :force_quotes => false, :quote_char => "|").each_with_index do |csv, index|
                    @logger.info csv
                    
                    if index.to_i == 0
                        set_csv_header(csv)
                    else
                        
                        data_hash = {}
                        insert_hash = {}
                        
                        
                        insert_hash[:network_id] = networkid.to_i
                        insert_hash[:report_date] = csv[@time_index].to_s
                        insert_hash[:name] = csv[@account_index].to_s
                        insert_hash[:campaign_id] = csv[@campaign_id_index].to_i
                        insert_hash[:campaign_name] = csv[@campaign_index].to_s
                        insert_hash[:adgroup_id] = csv[@adgroup_id_index].to_i
                        insert_hash[:adgroup_name] = csv[@adgroup_index].to_s
                        insert_hash[:keyword_id] = csv[@keyword_id_index].to_i
                        insert_hash[:keyword] = csv[@keyword_index].to_s
                        insert_hash[:total_cost] = csv[@cost_index].gsub('"', '').to_f
                        insert_hash[:clicks_avg_price] = csv[@avg_cpc_index].gsub('"', '').to_f
                        insert_hash[:display] = csv[@impression_index].gsub('"', '').to_i
                        insert_hash[:click_rate] = csv[@click_rate_index].gsub('"', '').gsub('%', '').strip.to_f
                        insert_hash[:clicks] = csv[@click_index].gsub('"', '').to_i
                        insert_hash[:avg_position] = csv[@avg_position_index].gsub('"', '').to_f
                        
                        
                        data_hash[:insert_one] = insert_hash
                        data_arr << data_hash
                      
                        if data_arr.count.to_i > 1000
                            @db3[:shenma_report_keyword].bulk_write(data_arr)
                            @db3.close()
                            
                            data_arr = []
                        end
                    end
                end
              
                if data_arr.count.to_i > 0
                    @db3[:shenma_report_keyword].bulk_write(data_arr)
                    @db3.close()
                end
              
              
            end
            
            
                
            @logger.info "shenma called dl reportfile done, remove file"
            
            if File.exists?(zip_file_name)
                File.delete(zip_file_name)
            end
      end
      
  end


  def updateaccount
      @network = @db[:network].find('type' => 'shenma')
      @db.close
  
      if @network.count.to_i > 0 
      
          @network.no_cursor_timeout.each do |network_d|
              @apitoken = network_d["api_token"]
              @username = network_d["username"]
              @password = network_d["password"]
              
              
              service = "account"
              method = "getAccount"
              
              json = {'header' => { 
                                      'token' => @apitoken.to_s,
                                      'username' => @username.to_s,
                                      'password' => @password.to_s 
                                  },
                       'body'  => {
                                      'requestData' => ["account_all"]
                                  }
                      }
                      
                      
              @account_info = shenma_api(service,method,json)
      
              if !@account_info["header"]["desc"].nil? && @account_info["header"]["desc"] == "执行成功"
            
                 @header = @account_info["header"]
                 @quota = @header["leftQuota"]
                 
                 @data = @account_info["body"]["accountInfoType"]
                 
                 # data = {:message => "baidu index", :datas => @data, :id => network_d['id'], :status => "true"}
                 # return render :json => data, :status => :ok 
                 
                 @db[:network].find(id: network_d["id"].to_i).update_one('$set'=> {  
                                        'balance' => @data["balance"].to_f,
                                        'cost' => @data["cost"].to_f,
                                        'payment' => @data["payment"].to_f,
                                        'budgettype' => @data["budgetType"].to_i,
                                        'budget' => @data["budget"].to_f,
                                        'regiontarget' => @data["regionTarget"],
                                        'excludeip' => @data["excludeIp"],
                                        'opendomains' => @data["openDomains"],
                                        'regdomain' => @data["regDomain"].to_s,
                                        'weeklyBudget' => @data["weeklyBudget"],
                                        'userstat' => @data["userStat"].to_i,
                                        'last_update' => @now
                                  })
    
                 @db.close
              end
              
          end
      end
      
      
      
      
      
      data = {:message => "shenma update account", :status => "true"}
      return render :json => data, :status => :ok
      
  end


  def resetreport
    
      @logger.info "shenma reset report"
    
      @db[:network].find('type' => 'shenma').update_many('$set'=> { 'report' => 0,'avg_pos' => 0,'avg_pos_upper' => 0,'last_update' => @now,'report_worker' => "",'avg_worker' => "",'avgupper_worker' => "" })
      @db.close
      
      
      @network = @db["network"].find('type' => "shenma")
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
            
            @network = @db["network"].find('id' => { "$in" => arr_d}).update_many('$set'=> { 'report' => 0,'avg_pos' => 0,'avg_pos_upper' => 0,'last_update' => @now,'report_worker' => port_array[index].to_i,'avg_worker' => port_array[index].to_i,'avgupper_worker' => port_array[index].to_i })
            @db.close
            
          end
      end
      
      @logger.info "shenma reset report done"
      return render :nothing => true 
  end
  
  
  def resetnetwork
    @logger.info "start reset shenma network api status"
    
    @id = params[:id]
    if @id.nil?
        @network = @db[:network].find({ "$and" => [{:type => 'shenma'}, {:file_update_1 => 4}, {:file_update_2 => 4}, {:file_update_3 => 4}, {:file_update_4 => 4}] })
        @db.close
    else
        @network = @db[:network].find({ "$and" => [{:type => 'shenma'}, {:id => @id.to_i}] })
        @db.close
    end
    
    @network.no_cursor_timeout.each do |doc|
          @db[:network].find(id: doc["id"].to_i).update_one('$set'=> { 'file_update_1' => 2, 'file_update_2' => 2, 'file_update_3' => 2, 'file_update_4' => 2 })
          @db.close
          @logger.info "done reset shenma network api status " +doc['id'].to_s
    end
    
    @logger.info "done reset shenma api network api status"
    return render :nothing => true
  end
  
  
  
  def resetdlfile
    @logger.info "start reset shenma api download file"
    
    @id = params[:id]
    if @id.nil?
        @network = @db[:network].find({ "$and" => [{:type => 'shenma'}, {:file_update_1 => { '$gte' => 4 }}, {:file_update_2 => { '$gte' => 4 }}, {:file_update_3 => { '$gte' => 4 }}, {:file_update_4 => { '$gte' => 4 }}] })
        # @network = @db[:network].find('type' => 'sogou')
        @db.close
    else
        @network = @db[:network].find({ "$and" => [{:type => 'shenma'}, {:id => @id.to_i}] })
        @db.close
    end
    
    @network.no_cursor_timeout.each do |doc|
          if doc["tmp_file"] != ""
              unzip_folder = "/datadrive/shenma_"+doc['id'].to_s+"_"+doc['tmp_file'].to_s
              if File.directory?(unzip_folder)
                  FileUtils.remove_dir unzip_folder, true
              end
          end
          @logger.info "done reset shenma api download file " +doc['id'].to_s
          
          @db[:network].find(id: doc["id"].to_i).update_one('$set'=> { 'fileid' => "", 'tmp_file' => "", 'run_time' => 0, 'file_update_1' => 0, 'file_update_2' => 0, 'file_update_3' => 0, 'file_update_4' => 0, 'worker' => "", 'last_update' => @now })
          @db.close
    end
    
    @logger.info "done reset sogou api download file"
    return render :nothing => true
  end

  def set_csv_header(array)
    
      # @logger.info "set_csv_header run"
      # @logger.info array
    
      array.each_with_index do |csv_header, header_index|
          
          # @logger.info csv_header
          
          if csv_header.to_s.strip.include?("时间")
            @time_index = header_index
          end
          
          if csv_header.to_s.strip == "账户ID"
            @account_id_index = header_index
          end
          
          if csv_header.to_s.strip == "账户"
            @account_index = header_index
          end
          
          if csv_header.to_s.strip == "展现量"
            @impression_index = header_index
          end
          
          if csv_header.to_s.strip == "点击量"
            @click_index = header_index
          end
          
          if csv_header.to_s.strip == "消费"
            @cost_index = header_index
          end
          
          if csv_header.to_s.strip == "点击率"
            @click_rate_index = header_index
          end
          
          if csv_header.to_s.strip == "平均点击价格"
            @avg_cpc_index = header_index
          end
          
          if csv_header.to_s.strip == "推广计划ID"
            @campaign_id_index = header_index
          end
          
          if csv_header.to_s.strip == "推广计划"
            @campaign_index = header_index
          end
          
          if csv_header.to_s.strip == "推广单元ID"
            @adgroup_id_index = header_index
          end
          
          if csv_header.to_s.strip == "推广单元"
            @adgroup_index = header_index
          end
          
          if csv_header.to_s.strip == "创意ID"
            @ad_id_index = header_index
          end
          
          if csv_header.to_s.strip == "创意"
            @ad_index = header_index
          end
          
          
          if csv_header.to_s.strip == "关键词ID"
            @keyword_id_index = header_index
          end
          
          if csv_header.to_s.strip == "关键词"
            @keyword_index = header_index
          end
          
          if csv_header.to_s.strip == "平均排名"
            @avg_position_index = header_index
          end
          
          
      end
  end


  # GET /shenmas
  # GET /shenmas.json
  def index
    @shenmas = Shenma.all
  end

  # GET /shenmas/1
  # GET /shenmas/1.json
  def show
  end

  # GET /shenmas/new
  def new
    @shenma = Shenma.new
  end

  # GET /shenmas/1/edit
  def edit
  end

  # POST /shenmas
  # POST /shenmas.json
  def create
    @shenma = Shenma.new(shenma_params)

    respond_to do |format|
      if @shenma.save
        format.html { redirect_to @shenma, notice: 'Shenma was successfully created.' }
        format.json { render :show, status: :created, location: @shenma }
      else
        format.html { render :new }
        format.json { render json: @shenma.errors, status: :unprocessable_entity }
      end
    end
  end

  # PATCH/PUT /shenmas/1
  # PATCH/PUT /shenmas/1.json
  def update
    respond_to do |format|
      if @shenma.update(shenma_params)
        format.html { redirect_to @shenma, notice: 'Shenma was successfully updated.' }
        format.json { render :show, status: :ok, location: @shenma }
      else
        format.html { render :edit }
        format.json { render json: @shenma.errors, status: :unprocessable_entity }
      end
    end
  end

  # DELETE /shenmas/1
  # DELETE /shenmas/1.json
  def destroy
    @shenma.destroy
    respond_to do |format|
      format.html { redirect_to shenmas_url, notice: 'Shenma was successfully destroyed.' }
      format.json { head :no_content }
    end
  end

  private
    # Use callbacks to share common setup or constraints between actions.
    def set_shenma
      @shenma = Shenma.find(params[:id])
    end

    # Never trust parameters from the scary internet, only allow the white list through.
    def shenma_params
      params[:shenma]
    end
end
