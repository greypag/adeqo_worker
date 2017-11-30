class BaidusController < ApplicationController
  # before_action :set_baidu, only: [:show, :edit, :update, :destroy]

  before_action :tmp
  
  require 'savon'
  require 'rubygems'
  require 'httparty'
  
  require 'mongo'
  require 'zlib'

  def test
    
  end



  def apiadgroup
      @logger.info "baidu api adgroup start"
      
      @campaign_id = params[:id]
    
      if @campaign_id.nil?
        
          # @current_campaign = @db[:all_campaign].find({ 'api_update' => 4 ,'network_type' => 'baidu', 'api_worker' => @port.to_i})
          # @db.close
          
          
        
          @current_campaign = @db[:all_campaign].find({ "$and" => [{:api_update => 4}, {:network_type => 'baidu'}, {:api_worker => @port.to_i}] })
          @db.close
          
          if @current_campaign.count.to_i >= 1
              @logger.info "working, no need update baidu api adgroup"
              return render :nothing => true
          end
          
          
          # @campaign = @db[:all_campaign].find('network_type' => 'baidu', 'api_update' => 3, 'api_worker' => @port.to_i).sort({ last_update: -1 }).limit(1)
          # @db.close
          
          
          @campaign = @db[:all_campaign].find({ "$and" => [{:api_update => 3}, {:network_type => 'baidu'}, {:api_worker => @port.to_i}] }).sort({ last_update: -1 }).limit(1)
          @db.close
          
          if @campaign.count.to_i == 0
              @logger.info "no need update baidu api adgroup"
              return render :nothing => true
          end
          
      else
        
          # @campaign = @db[:all_campaign].find({ 'campaign_id' => @campaign_id.to_i ,'network_type' => 'baidu'})
          # @db.close
          
          @campaign = @db[:all_campaign].find({ "$and" => [{:campaign_id => @campaign_id.to_i}, {:network_type => 'baidu'}] })
          @db.close
      end
      
      if @campaign.count.to_i
          @campaign.no_cursor_timeout.each do |campaign|
              @network_id = campaign["network_id"].to_i
              @campaign_id = campaign["campaign_id"].to_i
          end
          
          # @network = @db[:network].find('type' => 'baidu', 'id' => @network_id.to_i)
          # @db.close
          
          
          @network = @db[:network].find({ "$and" => [{:id => @network_id.to_i}, {:type => 'baidu'}] })
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
                  
                  service = "AccountService"
                  method = "getAccountInfo"
                  
                  json = {'header' => { 
                                          'token' => @apitoken.to_s,
                                          'username' => @username.to_s,
                                          'password' => @password.to_s 
                                      },
                           'body'  => {
                                          'accountFields' => ["userId","balance","cost","payment","budgetType","budget","regionTarget","excludeIp","openDomains","regDomain","budgetOfflineTime","weeklyBudget","userStat","isDynamicCreative","dynamicCreativeParam","pcBalance","mobileBalance"]
                                      }
                          }
                          
                          
                  @account_info = baidu_api(service,method,json)
                  
                  if !@account_info["header"]["desc"].nil? && @account_info["header"]["desc"].to_s == "success"
                      @header = @account_info["header"]
                      @remain_quote = @header["rquota"]
                      
                      if @remain_quote.to_i >= 500
                        
                          db_name = "adgroup_baidu_"+@network_id.to_s
                          
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
                                  
                                      service = "AdgroupService"
                                      method = "getAdgroup"
                                      
                                      json = {'header' => { 
                                                              'token' => @apitoken.to_s,
                                                              'username' => @username.to_s,
                                                              'password' => @password.to_s 
                                                          },
                                               'body'  => {
                                                              'ids'=> group_adgroup_id_arr_d,
                                                              'idType'=> 5,
                                                              'adgroupFields' => ["adgroupId","campaignId","adgroupName","pause","maxPrice",
                                                                                   "negativeWords","exactNegativeWords","status","accuPriceFactor","wordPriceFactor","widePriceFactor",
                                                                                   "matchPriceStatus","priceRatio"]
                                                          }
                                              }
                                              
                                      @adgroup_info = baidu_api(service,method,json)
                                      
                                      if !@adgroup_info["header"]["desc"].nil? && @adgroup_info["header"]["desc"].to_s == "success"
                                          @header = @adgroup_info["header"]
                                          @remain_quote = @header["rquota"]
                                          
                                          @adgroup = @adgroup_info["body"]["data"]
                                          
                                          
                                          if @adgroup.count.to_i > 0
                                                    
                                              db_name = "adgroup_baidu_"+@network_id.to_s
                                                  
                                              @adgroup.each do |adgroup_d|
                                                  
                                                  
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
                                                                                                                                                    'api_update_ad' => 2,
                                                                                                                                                    'api_update_keyword' => 2,
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
                                                                                  negative_words: "",
                                                                                  exact_negative_words: "",
                                                                                  pause: adgroup_d["pause"].to_s,
                                                                                  status: adgroup_d["status"].to_i,
                                                                                  accuPriceFactor: 0,
                                                                                  wordPriceFactor: 0,
                                                                                  widePriceFactor: 0,
                                                                                  matchPriceFactorStatus: 0,
                                                                                  priceRatio: "",
                                                                                  update_date: @now,                                            
                                                                                  create_date: @now })
                                                      @baidu_db.close() 
                                                      
                                                  end
                                              end
                                          end
                                      end
                                      
                                      
                                      
                                      # adgroup done
                                      # ad start
                                      service = "CreativeService"
                                      method = "getCreative"
                                    
                                    
                                      json = {'header' => { 
                                                              'token' => @apitoken.to_s,
                                                              'username' => @username.to_s,
                                                              'password' => @password.to_s 
                                                          },
                                               'body'  => {
                                                              'ids'=> group_adgroup_id_arr_d,
                                                              'idType'=> 5,
                                                              'creativeFields' => ["creativeId","adgroupId","title","pause","status",
                                                                                   "description1","description2","pcDestinationUrl","pcDisplayUrl","mobileDestinationUrl","mobileDisplayUrl",
                                                                                   "devicePreference","tabs"]
                                                          }
                                              }
                                      @ad_info = baidu_api(service,method,json)
                                      
                                      
                                      if !@ad_info["header"]["desc"].nil? && @ad_info["header"]["desc"].to_s == "success"
                                          @header = @ad_info["header"]
                                          @remain_quote = @header["rquota"]
                                          
                                          @ad = @ad_info["body"]["data"]
                                          
                                          
                                          if @ad.count.to_i > 0
                                                  
                                              @ad.each do |ad_d|
                                                
                                                  url_tag = 0
                                                  m_url_tag = 0
                                                  
                                                  @final_url = ad_d["pcDestinationUrl"].to_s
                                                  @m_final_url = ad_d["mobileDestinationUrl"].to_s
                                                
                                                  if !@final_url.to_s.include?(".adeqo.") && @tracking_type.to_s.downcase == "adeqo" && @ad_redirect.to_s.downcase == "yes" && @final_url.to_s != ""
                                
                                                      @temp_final_url = @final_url
                                                      @final_url = "http://t.adeqo.com/click?company_id="+@company_id.to_s+"&network_id="+@network_id.to_s
                                                      @final_url = @final_url + "&campaign_id="+@campaign_id.to_s+"&adgroup_id="+ad_d["adgroupId"].to_s+"&ad_id="+ad_d["creativeId"].to_s+"&keyword_id=0"
                                                      @final_url = @final_url + "&cookie="+@cookie_length.to_s
                                                      @final_url = @final_url + "&device=pc"
                                                      @final_url = @final_url + "&tv=v1&durl="+CGI.escape(@temp_final_url.to_s)
                                                      
                                                      url_tag = 1
                                                  end
                                                  
                                                  if !@m_final_url.to_s.include?(".adeqo.") && @tracking_type.to_s.downcase == "adeqo" && @ad_redirect.to_s.downcase == "yes" && @m_final_url.to_s != ""
                                                       
                                                      @temp_m_final_url = @m_final_url
                                                      @m_final_url = "http://t.adeqo.com/click?company_id="+@company_id.to_s+"&network_id="+@network_id.to_s
                                                      @m_final_url = @m_final_url + "&campaign_id="+@campaign_id.to_s+"&adgroup_id="+ad_d["adgroupId"].to_s+"&ad_id="+ad_d["creativeId"].to_s+"&keyword_id=0"
                                                      @m_final_url = @m_final_url + "&cookie="+@cookie_length.to_s
                                                      @m_final_url = @m_final_url + "&device=mobile"
                                                      @m_final_url = @m_final_url + "&tv=v1&durl="+CGI.escape(@temp_m_final_url.to_s)
                                                      
                                                      m_url_tag = 1
                                                  end
                                                
                                                  if url_tag == 1 || m_url_tag == 1
                                                      if @remain_quote.to_i >= 500
                                                          requesttypearray = [] 
                                                          requesttype = {}
                                                          requesttype[:creativeId]    =     ad_d["creativeId"].to_i
                                                          requesttype[:adgroupId]    =     0
                                                          requesttype[:status]    =     0
                                                          requesttype[:mobileDestinationUrl] =    @m_final_url
                                                          requesttype[:pcDestinationUrl]    =     @final_url
                                                          requesttype[:title] = ad_d["title"].to_s
                                                          requesttype[:description1] = ad_d["description1"].to_s
                                                          
                                                          
                                                          requesttypearray << requesttype
                                                      
                                                          service = "CreativeService"
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
                                                              
                                                          @urt_tag_update_info = baidu_api(service,method,json)
                                                          
                                                          if !@urt_tag_update_info["header"]["desc"].nil? && @urt_tag_update_info["header"]["desc"].to_s == "success"
                                        
                                                          else
                                                              @final_url = ad_d["pcDestinationUrl"].to_s
                                                              @m_final_url = ad_d["mobileDestinationUrl"].to_s
                                                          end
                                                      end
                                                  end
                                                
                                                  # @logger.info ad_d["creativeId"].to_s
                                                  
                                                  db_name = "ad_baidu_"+@network_id.to_s
                                                  
                                                  
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
                                                                                                                                                     
                                                  
                                                  
                                                  
                                                  
                                                  result = @baidu_db[db_name].find({ "$and" => [{:adgroup_id => adgroup_d["adgroupId"].to_i}, {:ad_id => ad_d["creativeId"].to_i}] } ).update_one('$set'=> { 
                                                                                                                                                          'title' => ad_d["title"].to_s,
                                                                                                                                                          'status' => ad_d["status"].to_i,
                                                                                                                                                          'pause' => ad_d["pause"].to_s,
                                                                                                                                                          'description_1' => ad_d["description1"].to_s,
                                                                                                                                                          'description_2' => ad_d["description2"].to_s,
                                                                                                                                                          'show_url' => ad_d["pcDisplayUrl"].to_s,
                                                                                                                                                          'visit_url' => @final_url.to_s,
                                                                                                                                                          'mobile_show_url' => ad_d["mobileDisplayUrl"].to_s,
                                                                                                                                                          'mobile_visit_url' => @m_final_url.to_s,
                                                                                                                                                          'devicePreference' => ad_d["devicePreference"].to_i,
                                                                                                                                                          'tabs' => ad_d["tabs"],
                                                                                                                                                          'update_date' => @now
                                                                                                                                                     })
                                                  @baidu_db.close()
                                                  
                                                  if result.n.to_i == 0
                                                    
                                                      @tmp << ad_d
                                                      @baidu_db[db_name].insert_one({ 
                                                                                      network_id: @network_id.to_i,
                                                                                      campaign_id: @campaign_id.to_i, 
                                                                                      adgroup_id: ad_d["adgroupId"].to_i,
                                                                                      ad_id: ad_d["creativeId"].to_i,
                                                                                      title: ad_d["title"].to_s, 
                                                                                      description_1: ad_d["description1"].to_s, 
                                                                                      description_2: ad_d["description2"].to_s, 
                                                                                      visit_url: @final_url.to_s,
                                                                                      show_url: ad_d["pcDisplayUrl"].to_s,
                                                                                      mobile_visit_url: @m_final_url.to_s,
                                                                                      mobile_show_url: ad_d["mobileDisplayUrl"].to_s,
                                                                                      pause: ad_d["pause"].to_s,
                                                                                      status: ad_d["status"].to_i,
                                                                                      temp: 0,
                                                                                      devicePreference: ad_d["devicePreference"].to_i,
                                                                                      tabs: ad_d["tabs"],
                                                                                      update_date: @now,                                            
                                                                                      create_date: @now })
                                                      @baidu_db.close()
                                                     
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
                                      service = "KeywordService"
                                      method = "getWord"
                                    
                                    
                                      json = {'header' => { 
                                                              'token' => @apitoken.to_s,
                                                              'username' => @username.to_s,
                                                              'password' => @password.to_s 
                                                          },
                                               'body'  => {
                                                              'ids'=> group_adgroup_id_arr_d,
                                                              'idType'=> 5,
                                                              'wordFields' => ["keywordId","campaignId","adgroupId","keyword","price",
                                                                               "pause","matchType","phraseType","status","wmatchprefer","pcDestinationUrl",
                                                                               "pcQuality","pcScale","mobileDestinationUrl","mobileQuality","mobileScale","tabs"]
                                                          }
                                              }
                                              
                                      @keyword_info = baidu_api(service,method,json)
                                      
                                      if !@keyword_info["header"]["desc"].nil? && @keyword_info["header"]["desc"].to_s == "success"
                                          @header = @keyword_info["header"]
                                          @remain_quote = @header["rquota"]
                                          
                                          @keyword = @keyword_info["body"]["data"]
                                          
                                          if @keyword.count.to_i > 0
                                              @keyword.each do |keyword_d|
                                                    
                                                  url_tag = 0
                                                  m_url_tag = 0
                                                  
                                                  @final_url = keyword_d["pcDestinationUrl"].to_s
                                                  @m_final_url = keyword_d["mobileDestinationUrl"].to_s
                                                
                                                  if !@final_url.to_s.include?(".adeqo.") && @tracking_type.to_s.downcase == "adeqo" && @keyword_redirect.to_s.downcase == "yes" && @final_url.to_s != ""
                                  
                                                      url_tag = 1
                                                      @temp_final_url = @final_url
                                                      @final_url = "http://t.adeqo.com/click?company_id="+@company_id.to_s+"&network_id="+@network_id.to_s
                                                      @final_url = @final_url + "&campaign_id="+@campaign_id.to_s+"&adgroup_id="+keyword_d["adgroupId"].to_s+"&ad_id=0&keyword_id="+keyword_d["keywordId"].to_s
                                                      @final_url = @final_url + "&cookie="+@cookie_length.to_s
                                                      @final_url = @final_url + "&device=pc"
                                                      @final_url = @final_url + "&tv=v1&durl="+CGI.escape(@temp_final_url.to_s)
                                                  end
                                                  
                                                  if !@m_final_url.to_s.include?(".adeqo.") && @tracking_type.to_s.downcase == "adeqo" && @keyword_redirect.to_s.downcase == "yes" && @final_url.to_s != ""
                                                    
                                                      m_url_tag = 1 
                                                      @temp_m_final_url = @m_final_url
                                                      @m_final_url = "http://t.adeqo.com/click?company_id="+@company_id.to_s+"&network_id="+@network_id.to_s
                                                      @m_final_url = @m_final_url + "&campaign_id="+@campaign_id.to_s+"&adgroup_id="+keyword_d["adgroupId"].to_s+"&ad_id=0&keyword_id="+keyword_d["keywordId"].to_s
                                                      @m_final_url = @m_final_url + "&cookie="+@cookie_length.to_s
                                                      @m_final_url = @m_final_url + "&device=mobile"
                                                      @m_final_url = @m_final_url + "&tv=v1&durl="+CGI.escape(@temp_m_final_url.to_s)
                                                  end
                                                
                                                  if url_tag == 1 || m_url_tag == 1
                                                      if @remain_quote.to_i >= 500
                                                          requesttypearray = [] 
                                                          requesttype = {}
                                                          
                                                          requesttype[:keywordId]    =     keyword_d["keywordId"].to_i
                                                          requesttype[:adgroupId]    =     0
                                                          requesttype[:status]    =     0
                                                          requesttype[:mobileDestinationUrl] =    @m_final_url
                                                          requesttype[:pcDestinationUrl]    =     @final_url
                                                          
                                                          
                                                          requesttypearray << requesttype
                                                      
                                                          service = "KeywordService"
                                                          method = "updateWord"
                                                          
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
                                                          @remain_quote = @header["rquota"]
                                                              
                                                          if !@keyword_tag_update_info["header"]["desc"].nil? && @keyword_tag_update_info["header"]["desc"].to_s == "success"
                                                              
                                                          else
                                                              @final_url = keyword_d["pcDestinationUrl"].to_s
                                                              @m_final_url = keyword_d["mobileDestinationUrl"].to_s
                                                          end
                                                      end
                                                  end
                                                
                                                  # @logger.info keyword_d["keywordId"].to_s
                                                  # @logger.info keyword_d["adgroupId"].to_s
                                                  
                                                  db_name = "keyword_baidu_"+@network_id.to_s
                                                  
                                                  # @logger.info db_name.to_s
                                                  
                                                  
                                                  # result = @baidu_db[db_name].find('adgroup_id' => keyword_d["adgroupId"].to_i, "keyword_id" => keyword_d["keywordId"].to_i ).update_one('$set'=> { 
                                                                                                                                                          # 'keyword' => keyword_d["keyword"].to_s,
                                                                                                                                                          # 'pause' => keyword_d["pause"].to_s,
                                                                                                                                                          # 'status' => keyword_d["status"].to_i,
                                                                                                                                                          # 'match_type' => keyword_d["matchType"].to_i,
                                                                                                                                                          # 'visit_url' => @final_url.to_s,
                                                                                                                                                          # 'mobile_visit_url' => @m_final_url.to_s,
                                                                                                                                                          # 'price' => keyword_d["price"].to_f,
                                                                                                                                                          # 'reason' => keyword_d["pcReason"].to_i,
                                                                                                                                                          # 'mobilereason' => keyword_d["mobileReason"].to_i,
                                                                                                                                                          # 'wmatchprefer' => keyword_d["wmatchprefer"].to_i,
                                                                                                                                                          # 'pc_quality' => keyword_d["pcQuality"].to_i,
                                                                                                                                                          # 'mobilequality' => keyword_d["mobileQuality"].to_i,
                                                                                                                                                          # 'phrase_type' => keyword_d["phraseType"].to_i,
                                                                                                                                                          # 'owmatch' => keyword_d["owmatch"].to_i,
                                                                                                                                                          # 'reliable' => keyword_d["pcReliable"].to_i,
                                                                                                                                                          # 'mobilereliable' => keyword_d["mobileReliable"].to_i,
                                                                                                                                                          # 'tabs' => keyword_d["tabs"],
                                                                                                                                                          # 'update_date' => @now
                                                                                                                                                     # })
                                                                                                                                                     
                                                  
                                                  result = @baidu_db[db_name].find({ "$and" => [{:adgroup_id => keyword_d["adgroupId"].to_i}, {:keyword_id => keyword_d["keywordId"].to_i}] } ).update_one('$set'=> { 
                                                                                                                                                          'keyword' => keyword_d["keyword"].to_s,
                                                                                                                                                          'pause' => keyword_d["pause"].to_s,
                                                                                                                                                          'status' => keyword_d["status"].to_i,
                                                                                                                                                          'match_type' => keyword_d["matchType"].to_i,
                                                                                                                                                          'visit_url' => @final_url.to_s,
                                                                                                                                                          'mobile_visit_url' => @m_final_url.to_s,
                                                                                                                                                          'price' => keyword_d["price"].to_f,
                                                                                                                                                          'reason' => keyword_d["pcReason"].to_i,
                                                                                                                                                          'mobilereason' => keyword_d["mobileReason"].to_i,
                                                                                                                                                          'wmatchprefer' => keyword_d["wmatchprefer"].to_i,
                                                                                                                                                          'pc_quality' => keyword_d["pcQuality"].to_i,
                                                                                                                                                          'mobilequality' => keyword_d["mobileQuality"].to_i,
                                                                                                                                                          'phrase_type' => keyword_d["phraseType"].to_i,
                                                                                                                                                          'owmatch' => keyword_d["owmatch"].to_i,
                                                                                                                                                          'reliable' => keyword_d["pcReliable"].to_i,
                                                                                                                                                          'mobilereliable' => keyword_d["mobileReliable"].to_i,
                                                                                                                                                          'tabs' => keyword_d["tabs"],
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
                                                                                        mobile_visit_url: @m_final_url.to_s,
                                                                                        match_type: keyword_d["matchType"].to_i,
                                                                                        pause: keyword_d["pause"].to_s,
                                                                                        status: keyword_d["status"].to_i,
                                                                                        pc_quality: keyword_d["pcQuality"].to_f,
                                                                                        temp: 0,
                                                                                        phrase_type: keyword_d["phraseType"].to_i,
                                                                                        reliable: keyword_d["pcReliable"].to_i,
                                                                                        reason: keyword_d["pcReason"].to_i,
                                                                                        mobilequality: keyword_d["mobileQuality"].to_f,
                                                                                        mobilereliable: keyword_d["mobileReliable"].to_i,
                                                                                        mobilereason: keyword_d["mobileReason"].to_i,
                                                                                        wmatchprefer: keyword_d["wmatchprefer"].to_i,
                                                                                        tabs: keyword_d["tabs"],
                                                                                        update_date: @now,                                            
                                                                                        create_date: @now })
                                                        @baidu_db.close()
                                                  end
                                                  
                                              end
                                          end
                                      end
                                  end
                              end
                              
                              # keyword done
                             
                              db_name = "adgroup_baidu_"+@network_id.to_s
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
          
          db_name = "adgroup_baidu_"+@network_id.to_s
          @list_adgroup = @baidu_db[db_name].find('$and' => [{'campaign_id' => @campaign_id.to_i},{'api_update_ad' => { "$ne" => 0}},{'api_update_keyword' => { "$ne" => 0}},{'api_update_ad' => { '$exists' => true }},'api_update_keyword' => { '$exists' => true }])
          @baidu_db.close() 
          
          if @list_adgroup.count.to_i == 0
            
              # @db["all_campaign"].find('campaign_id' => @campaign_id.to_i,'network_type' => "baidu", 'api_update' => 3).update_one('$set'=> {'api_update' => 0, 'api_worker' => "", 'update_date' => @now})
              # @sogou_db.close() 
#               
              # @db[:network].find('type' => 'baidu', 'id' => @network_id.to_i).update_one('$set'=> {'file_update_1' => 4,'file_update_2' => 4,'file_update_3' => 4,'file_update_4' => 4, 'last_update' => @now})
              # @db.close
            
            
              
              
              @db["all_campaign"].find({ "$and" => [{:campaign_id => @campaign_id.to_i}, {:network_type => "baidu"}, {:api_update => 3}] }).update_one('$set'=> {'api_update' => 0, 'api_worker' => "", 'update_date' => @now})
              @db.close 
              
              @db[:network].find({ "$and" => [{:id => @network_id.to_i}, {:type => "baidu"}] }).update_one('$set'=> {'file_update_1' => 4,'file_update_2' => 4,'file_update_3' => 4,'file_update_4' => 4, 'last_update' => @now})
              @db.close
          end
      end
      
      
      
      @logger.info "baidu api adgroup done"
      return render :nothing => true
  end

  def apicampaign
    
      @logger.info "baidu api campaign start"
    
      
      @campaign_id = params[:id]
    
      if @campaign_id.nil?
        
          @current_campaign = @db[:all_campaign].find({ "$and" => [{:api_update => 2}, {:network_type => "baidu"}, {:api_worker => @port.to_i}] })
          @db.close
          
          if @current_campaign.count.to_i >= 1
              @logger.info "working, no need update baidu api campaign"
              return render :nothing => true
          end
          
          
          
          @campaign = @db[:all_campaign].find({ "$and" => [{:api_update => 1}, {:network_type => "baidu"}, {:api_worker => @port.to_i}] }).sort({ last_update: -1 }).limit(1)
          @db.close
          
          if @campaign.count.to_i == 0
              @logger.info "no need update baidu api campaign"
              return render :nothing => true
          end
          
      else
        
          @campaign = @db[:all_campaign].find({ "$and" => [{:campaign_id => @campaign_id.to_i}, {:network_type => "baidu"}] })
          @db.close
      end
      
      @network_id = 0
      
      
      if @campaign.count.to_i > 0
          @campaign.no_cursor_timeout.each do |campaign|
            
              @logger.info "campaign"
              
              @network_id = campaign["network_id"].to_i
              @campaign_id = campaign["campaign_id"].to_i
              @campaign_status_body = ""
              
              
              
              @db["all_campaign"].find({ "$and" => [{:campaign_id => @campaign_id.to_i}, {:network_type => "baidu"}] }).update_one('$set'=> { 'api_update' => 2 })
              @db.close
              
              @network = @db[:network].find({ "$and" => [{:id => @network_id.to_i}, {:type => "baidu"}] })
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
                      
                      
                      service = "AccountService"
                      method = "getAccountInfo"
                      
                      json = {'header' => { 
                                              'token' => @apitoken.to_s,
                                              'username' => @username.to_s,
                                              'password' => @password.to_s 
                                          },
                               'body'  => {
                                              'accountFields' => ["userId","balance","cost","payment","budgetType","budget","regionTarget","excludeIp","openDomains","regDomain","budgetOfflineTime","weeklyBudget","userStat","isDynamicCreative","dynamicCreativeParam","pcBalance","mobileBalance"]
                                          }
                              }
                              
                              
                      @account_info = baidu_api(service,method,json)
                      
                      
                      
                      if !@account_info["header"]["desc"].nil? && @account_info["header"]["desc"].to_s == "success"
                          @header = @account_info["header"]
                          @remain_quote = @header["rquota"]
                          
                          if @remain_quote.to_i >= 500
                            
                            
                              service = "CampaignService"
                              method = "getCampaign"
                            
                            
                              json = {'header' => { 
                                                                    'token' => @apitoken.to_s,
                                                                    'username' => @username.to_s,
                                                                    'password' => @password.to_s 
                                                                },
                                                     'body'  => {
                                                                    'campaignIds'=> [@campaign_id],
                                                                    'campaignFields' => ["campaignId","campaignName","budget","campaignType","budgetOfflineTime",
                                                                                         "device","exactNegativeWords","isDynamicCreative","isDynamicTagSublink","isDynamicTitle","isDynamicHotRedirect",
                                                                                         "regionTarget","negativeWords","pause","rmktStatus","priceRatio","schedule",
                                                                                         "showProb","status","rmktPriceRatio"]
                                                                }
                                                    }
                            
                              @campaign_info = baidu_api(service,method,json)
                              
                              if !@campaign_info["header"]["desc"].nil? && @campaign_info["header"]["desc"].to_s == "success"
                              
                                  @header = @campaign_info["header"]
                                  @remain_quote = @header["rquota"]
                                  
                                  @adgroup_id_arr = []
                                  
                                  if @remain_quote.to_i > 500
                                      service = "AdgroupService"
                                      method = "getAdgroup"
                                    
                                    
                                      json = {'header' => { 
                                                              'token' => @apitoken.to_s,
                                                              'username' => @username.to_s,
                                                              'password' => @password.to_s 
                                                          },
                                               'body'  => {
                                                              'ids'=> [@campaign_id],
                                                              'idType'=> 3,
                                                              'adgroupFields' => ["adgroupId","campaignId","adgroupName","pause","maxPrice",
                                                                                   "negativeWords","exactNegativeWords","status","accuPriceFactor","wordPriceFactor","widePriceFactor",
                                                                                   "matchPriceStatus","priceRatio"]
                                                          }
                                              }
                                      @adgroup_info = baidu_api(service,method,json)
                                      
                                      
                                      
                                      if !@adgroup_info["header"]["desc"].nil? && @adgroup_info["header"]["desc"].to_s == "success"
                                        
                                          @header = @adgroup_info["header"]
                                          @remain_quote = @header["rquota"]
                                          
                                          @adgroup = @adgroup_info["body"]["data"]
                                          
                                          if @adgroup.count.to_i > 0
                                            
                                              db_name = "adgroup_baidu_"+@network_id.to_s
                                                  
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
                                                                                                                                                    'api_update_ad' => 2,
                                                                                                                                                    'api_update_keyword' => 2,
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
                                                                                  negative_words: "",
                                                                                  exact_negative_words: "",
                                                                                  pause: adgroup_d["pause"].to_s,
                                                                                  status: adgroup_d["status"].to_i,
                                                                                  accuPriceFactor: 0,
                                                                                  wordPriceFactor: 0,
                                                                                  widePriceFactor: 0,
                                                                                  matchPriceFactorStatus: 0,
                                                                                  priceRatio: "",
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
                                              
                                              service = "CreativeService"
                                              method = "getCreative"
                                            
                                            
                                              json = {'header' => { 
                                                                      'token' => @apitoken.to_s,
                                                                      'username' => @username.to_s,
                                                                      'password' => @password.to_s 
                                                                  },
                                                       'body'  => {
                                                                      'ids'=> group_adgroup_id_arr_d,
                                                                      'idType'=> 5,
                                                                      'creativeFields' => ["creativeId","adgroupId","title","pause","status",
                                                                                           "description1","description2","pcDestinationUrl","pcDisplayUrl","mobileDestinationUrl","mobileDisplayUrl",
                                                                                           "devicePreference","tabs"]
                                                                  }
                                                      }
                                              @ad_info = baidu_api(service,method,json)
                                              
                                              
                                              if !@ad_info["header"]["desc"].nil? && @ad_info["header"]["desc"].to_s == "success"
                                        
                                                  @header = @ad_info["header"]
                                                  @remain_quote = @header["rquota"]
                                                  
                                                  @ad = @ad_info["body"]["data"]
                                                  
                                                  if @ad.count.to_i > 0
                                                  
                                                      @ad.each do |ad_d|
                                                        
                                                          url_tag = 0
                                                          m_url_tag = 0
                                                          
                                                          @final_url = ad_d["pcDestinationUrl"].to_s
                                                          @m_final_url = ad_d["mobileDestinationUrl"].to_s
                                                        
                                                          if !@final_url.to_s.include?(".adeqo.") && @tracking_type.to_s.downcase == "adeqo" && @ad_redirect.to_s.downcase == "yes" && @final_url.to_s != ""
                                        
                                                              @temp_final_url = @final_url
                                                              @final_url = "http://t.adeqo.com/click?company_id="+@company_id.to_s+"&network_id="+@network_id.to_s
                                                              @final_url = @final_url + "&campaign_id="+@campaign_id.to_s+"&adgroup_id="+ad_d["adgroupId"].to_s+"&ad_id="+ad_d["creativeId"].to_s+"&keyword_id=0"
                                                              @final_url = @final_url + "&cookie="+@cookie_length.to_s
                                                              @final_url = @final_url + "&device=pc"
                                                              @final_url = @final_url + "&tv=v1&durl="+CGI.escape(@temp_final_url.to_s)
                                                              
                                                              url_tag = 1
                                                          end
                                                          
                                                          if !@m_final_url.to_s.include?(".adeqo.") && @tracking_type.to_s.downcase == "adeqo" && @ad_redirect.to_s.downcase == "yes" && @m_final_url.to_s != ""
                                                               
                                                              @temp_m_final_url = @m_final_url
                                                              @m_final_url = "http://t.adeqo.com/click?company_id="+@company_id.to_s+"&network_id="+@network_id.to_s
                                                              @m_final_url = @m_final_url + "&campaign_id="+@campaign_id.to_s+"&adgroup_id="+ad_d["adgroupId"].to_s+"&ad_id="+ad_d["creativeId"].to_s+"&keyword_id=0"
                                                              @m_final_url = @m_final_url + "&cookie="+@cookie_length.to_s
                                                              @m_final_url = @m_final_url + "&device=mobile"
                                                              @m_final_url = @m_final_url + "&tv=v1&durl="+CGI.escape(@temp_m_final_url.to_s)
                                                              
                                                              m_url_tag = 1
                                                          end
                                                        
                                                          if url_tag == 1 || m_url_tag == 1
                                                              if @remain_quote.to_i >= 500
                                                                  requesttypearray = [] 
                                                                  requesttype = {}
                                                                  requesttype[:creativeId]    =     ad_d["creativeId"].to_i
                                                                  requesttype[:adgroupId]    =     0
                                                                  requesttype[:status]    =     0
                                                                  requesttype[:mobileDestinationUrl] =    @m_final_url
                                                                  requesttype[:pcDestinationUrl]    =     @final_url
                                                                  requesttype[:title] = ad_d["title"].to_s
                                                                  requesttype[:description1] = ad_d["description1"].to_s
                                                                  
                                                                  
                                                                  requesttypearray << requesttype
                                                              
                                                                  service = "CreativeService"
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
                                                                      
                                                                  @urt_tag_update_info = baidu_api(service,method,json)
                                                                  
                                                                  if !@urt_tag_update_info["header"]["desc"].nil? && @urt_tag_update_info["header"]["desc"].to_s == "success"
                                                
                                                                  else
                                                                      @final_url = ad_d["pcDestinationUrl"].to_s
                                                                      @m_final_url = ad_d["mobileDestinationUrl"].to_s
                                                                  end
                                                              end
                                                          end
                                                        
                                                          # @logger.info ad_d["creativeId"].to_s
                                                          
                                                          db_name = "ad_baidu_"+@network_id.to_s
                                                          
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
                                                                                                                                                                  'description_1' => ad_d["description1"].to_s,
                                                                                                                                                                  'description_2' => ad_d["description2"].to_s,
                                                                                                                                                                  'show_url' => ad_d["pcDisplayUrl"].to_s,
                                                                                                                                                                  'visit_url' => @final_url.to_s,
                                                                                                                                                                  'mobile_show_url' => ad_d["mobileDisplayUrl"].to_s,
                                                                                                                                                                  'mobile_visit_url' => @m_final_url.to_s,
                                                                                                                                                                  'devicePreference' => ad_d["devicePreference"].to_i,
                                                                                                                                                                  'tabs' => ad_d["tabs"],
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
                                                                                              description_1: ad_d["description1"].to_s, 
                                                                                              description_2: ad_d["description2"].to_s, 
                                                                                              visit_url: @final_url.to_s,
                                                                                              show_url: ad_d["pcDisplayUrl"].to_s,
                                                                                              mobile_visit_url: @m_final_url.to_s,
                                                                                              mobile_show_url: ad_d["mobileDisplayUrl"].to_s,
                                                                                              pause: ad_d["pause"].to_s,
                                                                                              status: ad_d["status"].to_i,
                                                                                              temp: 0,
                                                                                              devicePreference: ad_d["devicePreference"].to_i,
                                                                                              tabs: ad_d["tabs"],
                                                                                              update_date: @now,                                            
                                                                                              create_date: @now })
                                                              @baidu_db.close()
                                                             
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
                                              service = "KeywordService"
                                              method = "getWord"
                                            
                                            
                                              json = {'header' => { 
                                                                      'token' => @apitoken.to_s,
                                                                      'username' => @username.to_s,
                                                                      'password' => @password.to_s 
                                                                  },
                                                       'body'  => {
                                                                      'ids'=> group_adgroup_id_arr_d,
                                                                      'idType'=> 5,
                                                                      'wordFields' => ["keywordId","campaignId","adgroupId","keyword","price",
                                                                                       "pause","matchType","phraseType","status","wmatchprefer","pcDestinationUrl",
                                                                                       "pcQuality","pcScale","mobileDestinationUrl","mobileQuality","mobileScale","tabs"]
                                                                  }
                                                      }
                                                      
                                              @keyword_info = baidu_api(service,method,json)
                                              
                                              if !@keyword_info["header"]["desc"].nil? && @keyword_info["header"]["desc"].to_s == "success"
                                                  @header = @keyword_info["header"]
                                                  @remain_quote = @header["rquota"]
                                                  
                                                  @keyword = @keyword_info["body"]["data"]
                                                  
                                                  if @keyword.count.to_i > 0
                                                      @keyword.each do |keyword_d|
                                                            
                                                          url_tag = 0
                                                          m_url_tag = 0
                                                          
                                                          @final_url = keyword_d["pcDestinationUrl"].to_s
                                                          @m_final_url = keyword_d["mobileDestinationUrl"].to_s
                                                        
                                                          if !@final_url.to_s.include?(".adeqo.") && @tracking_type.to_s.downcase == "adeqo" && @keyword_redirect.to_s.downcase == "yes" && @final_url.to_s != ""
                                          
                                                              url_tag = 1
                                                              @temp_final_url = @final_url
                                                              @final_url = "http://t.adeqo.com/click?company_id="+@company_id.to_s+"&network_id="+@network_id.to_s
                                                              @final_url = @final_url + "&campaign_id="+@campaign_id.to_s+"&adgroup_id="+keyword_d["adgroupId"].to_s+"&ad_id=0&keyword_id="+keyword_d["keywordId"].to_s
                                                              @final_url = @final_url + "&cookie="+@cookie_length.to_s
                                                              @final_url = @final_url + "&device=pc"
                                                              @final_url = @final_url + "&tv=v1&durl="+CGI.escape(@temp_final_url.to_s)
                                                          end
                                                          
                                                          if !@m_final_url.to_s.include?(".adeqo.") && @tracking_type.to_s.downcase == "adeqo" && @keyword_redirect.to_s.downcase == "yes" && @final_url.to_s != ""
                                                            
                                                              m_url_tag = 1 
                                                              @temp_m_final_url = @m_final_url
                                                              @m_final_url = "http://t.adeqo.com/click?company_id="+@company_id.to_s+"&network_id="+@network_id.to_s
                                                              @m_final_url = @m_final_url + "&campaign_id="+@campaign_id.to_s+"&adgroup_id="+keyword_d["adgroupId"].to_s+"&ad_id=0&keyword_id="+keyword_d["keywordId"].to_s
                                                              @m_final_url = @m_final_url + "&cookie="+@cookie_length.to_s
                                                              @m_final_url = @m_final_url + "&device=mobile"
                                                              @m_final_url = @m_final_url + "&tv=v1&durl="+CGI.escape(@temp_m_final_url.to_s)
                                                          end
                                                        
                                                          if url_tag == 1 || m_url_tag == 1
                                                              if @remain_quote.to_i >= 500
                                                                  requesttypearray = [] 
                                                                  requesttype = {}
                                                                  
                                                                  requesttype[:keywordId]    =     keyword_d["keywordId"].to_i
                                                                  requesttype[:adgroupId]    =     0
                                                                  requesttype[:status]    =     0
                                                                  requesttype[:mobileDestinationUrl] =    @m_final_url
                                                                  requesttype[:pcDestinationUrl]    =     @final_url
                                                                  
                                                                  
                                                                  requesttypearray << requesttype
                                                              
                                                                  service = "KeywordService"
                                                                  method = "updateWord"
                                                                  
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
                                                                  @remain_quote = @header["rquota"]
                                                                      
                                                                  if !@keyword_tag_update_info["header"]["desc"].nil? && @keyword_tag_update_info["header"]["desc"].to_s == "success"
                                                                      
                                                                  else
                                                                      @final_url = keyword_d["pcDestinationUrl"].to_s
                                                                      @m_final_url = keyword_d["mobileDestinationUrl"].to_s
                                                                  end
                                                              end
                                                          end
                                                        
                                                          # @logger.info keyword_d["keywordId"].to_s
                                                          # @logger.info keyword_d["adgroupId"].to_s
                                                          
                                                          db_name = "keyword_baidu_"+@network_id.to_s
                                                          
                                                          # @logger.info db_name.to_s
                                                          
                                                          
                                                          # result = @baidu_db[db_name].find('adgroup_id' => keyword_d["adgroupId"].to_i, "keyword_id" => keyword_d["keywordId"].to_i ).update_one('$set'=> { 
                                                                                                                                                                  # 'keyword' => keyword_d["keyword"].to_s,
                                                                                                                                                                  # 'pause' => keyword_d["pause"].to_s,
                                                                                                                                                                  # 'status' => keyword_d["status"].to_i,
                                                                                                                                                                  # 'match_type' => keyword_d["matchType"].to_i,
                                                                                                                                                                  # 'visit_url' => @final_url.to_s,
                                                                                                                                                                  # 'mobile_visit_url' => @m_final_url.to_s,
                                                                                                                                                                  # 'price' => keyword_d["price"].to_f,
                                                                                                                                                                  # 'reason' => keyword_d["pcReason"].to_i,
                                                                                                                                                                  # 'mobilereason' => keyword_d["mobileReason"].to_i,
                                                                                                                                                                  # 'wmatchprefer' => keyword_d["wmatchprefer"].to_i,
                                                                                                                                                                  # 'pc_quality' => keyword_d["pcQuality"].to_i,
                                                                                                                                                                  # 'mobilequality' => keyword_d["mobileQuality"].to_i,
                                                                                                                                                                  # 'phrase_type' => keyword_d["phraseType"].to_i,
                                                                                                                                                                  # 'owmatch' => keyword_d["owmatch"].to_i,
                                                                                                                                                                  # 'reliable' => keyword_d["pcReliable"].to_i,
                                                                                                                                                                  # 'mobilereliable' => keyword_d["mobileReliable"].to_i,
                                                                                                                                                                  # 'tabs' => keyword_d["tabs"],
                                                                                                                                                                  # 'update_date' => @now
                                                                                                                                                             # })
                                                          
                                                          
                                                          
                                                          result = @baidu_db[db_name].find({ "$and" => [{:adgroup_id => keyword_d["adgroupId"].to_i}, {:keyword_id => keyword_d["keywordId"].to_i}] } ).update_one('$set'=> { 
                                                                                                                                                                  'keyword' => keyword_d["keyword"].to_s,
                                                                                                                                                                  'pause' => keyword_d["pause"].to_s,
                                                                                                                                                                  'status' => keyword_d["status"].to_i,
                                                                                                                                                                  'match_type' => keyword_d["matchType"].to_i,
                                                                                                                                                                  'visit_url' => @final_url.to_s,
                                                                                                                                                                  'mobile_visit_url' => @m_final_url.to_s,
                                                                                                                                                                  'price' => keyword_d["price"].to_f,
                                                                                                                                                                  'reason' => keyword_d["pcReason"].to_i,
                                                                                                                                                                  'mobilereason' => keyword_d["mobileReason"].to_i,
                                                                                                                                                                  'wmatchprefer' => keyword_d["wmatchprefer"].to_i,
                                                                                                                                                                  'pc_quality' => keyword_d["pcQuality"].to_i,
                                                                                                                                                                  'mobilequality' => keyword_d["mobileQuality"].to_i,
                                                                                                                                                                  'phrase_type' => keyword_d["phraseType"].to_i,
                                                                                                                                                                  'owmatch' => keyword_d["owmatch"].to_i,
                                                                                                                                                                  'reliable' => keyword_d["pcReliable"].to_i,
                                                                                                                                                                  'mobilereliable' => keyword_d["mobileReliable"].to_i,
                                                                                                                                                                  'tabs' => keyword_d["tabs"],
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
                                                                                                mobile_visit_url: @m_final_url.to_s,
                                                                                                match_type: keyword_d["matchType"].to_i,
                                                                                                pause: keyword_d["pause"].to_s,
                                                                                                status: keyword_d["status"].to_i,
                                                                                                pc_quality: keyword_d["pcQuality"].to_f,
                                                                                                temp: 0,
                                                                                                phrase_type: keyword_d["phraseType"].to_i,
                                                                                                reliable: keyword_d["pcReliable"].to_i,
                                                                                                reason: keyword_d["pcReason"].to_i,
                                                                                                mobilequality: keyword_d["mobileQuality"].to_f,
                                                                                                mobilereliable: keyword_d["mobileReliable"].to_i,
                                                                                                mobilereason: keyword_d["mobileReason"].to_i,
                                                                                                wmatchprefer: keyword_d["wmatchprefer"].to_i,
                                                                                                tabs: keyword_d["tabs"],
                                                                                                update_date: @now,                                            
                                                                                                create_date: @now })
                                                                @baidu_db.close()
                                                          end
                                                          
                                                      end
                                                  end
                                              end
                                          end
                                      end
                                      
                                      db_name = "adgroup_baidu_"+@network_id.to_s
                                      @baidu_db[db_name].find('adgroup_id' => { "$in" => @adgroup_id_arr}).update_many('$set'=> { 
                                                                                                                                      'api_update_ad' => 0,
                                                                                                                                      'api_update_keyword' => 0,
                                                                                                                                 })
                                      @baidu_db.close()
                                  end
                                  
                                  
                                  
                                  
                                  @db["all_campaign"].find({ "$and" => [{:campaign_id => @campaign_id.to_i}, {:network_type => "baidu"}] }).update_one('$set'=> { 
                                                                                                                                             'campaign_name' => @campaign_info["body"]["data"][0]["campaignName"].to_s,
                                                                                                                                             'budget' => @campaign_info["body"]["data"][0]["budget"].to_f,
                                                                                                                                             'regions' => @campaign_info["body"]["data"][0]["regionTarget"],
                                                                                                                                             'negative_words' => @campaign_info["body"]["data"][0]["negativeWords"],
                                                                                                                                             'isDynamicTagSublink' => @campaign_info["body"]["data"][0]["isDynamicTagSublink"].to_s,
                                                                                                                                             'pause' => @campaign_info["body"]["data"][0]["pause"].to_s,
                                                                                                                                             'exact_negative_words' => @campaign_info["body"]["data"][0]["exactNegativeWords"],
                                                                                                                                             'isDynamicCreative' => @campaign_info["body"]["data"][0]["isDynamicCreative"].to_s,
                                                                                                                                             'isDynamicTitle' => @campaign_info["body"]["data"][0]["isDynamicTitle"].to_s,
                                                                                                                                             'budget_offline_time' => @campaign_info["body"]["data"][0]["budgetOfflineTime"],
                                                                                                                                             'campaignType' => @campaign_info["body"]["data"][0]["campaignType"].to_i,
                                                                                                                                             'status' => @campaign_info["body"]["data"][0]["status"].to_i,
                                                                                                                                             'show_prob' => @campaign_info["body"]["data"][0]["showProb"].to_i,
                                                                                                                                             'isDynamicHotRedirect' => @campaign_info["body"]["data"][0]["isDynamicHotRedirect"].to_s,
                                                                                                                                             'device' => @campaign_info["body"]["data"][0]["device"].to_i,
                                                                                                                                             'priceRatio' => @campaign_info["body"]["data"][0]["priceRatio"].to_i
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
      
      @logger.info "baidu api done start"
      return render :nothing => true
    
  end

  def getreport(networkid,username,password,apitoken,account,campaign,adgroup,ad,keyword,start_date,end_date)
    
      @logger.info "called getreport baidu" + networkid.to_s
      @logger.info account.to_s
      @logger.info campaign.to_s
      @logger.info adgroup.to_s
      @logger.info ad.to_s
      
      if account.to_s == "" || campaign.to_s == "" || adgroup.to_s == "" || ad.to_s == "" || keyword.to_s == ""
        
        # if one of them doenst has report id, then get the id first, must download all report together
        @logger.info "called getreport id baidu"+networkid.to_s
        getfileid(networkid,username,password,apitoken,account,campaign,adgroup,ad,keyword,start_date,end_date)
      else
        # if all of them has id, dl report and insert
        @logger.info "called download report file baidu"+networkid.to_s
        getreportfile(networkid,username,password,apitoken,account,campaign,adgroup,ad,keyword,start_date,end_date)
      end
    
  end
  


  def getfileid(networkid,username,password,apitoken,account,campaign,adgroup,ad,keyword,start_date,end_date)
      @logger.info "called baidu getfileid network "+networkid.to_s
      
      account_report_id = account
      campaign_report_id = campaign
      adgroup_report_id = adgroup
      ad_report_id = ad
      keyword_report_id = keyword
      
      
      service = "ReportService"
      method = "getProfessionalReportId"
      
      if account.to_s == ""
          json = {'header' => { 
                                  'token' => apitoken.to_s,
                                  'username' => username.to_s,
                                  'password' => password.to_s
                              },
                   'body'  => {
                                  "reportRequestType" => {
                                      # "performanceData" => ["cost","cpc","click","impression","ctr","cpm","position","conversion","phoneConversion","bridgeConversion"],
                                      "performanceData" => ["cost","cpc","click","impression","ctr","cpm","conversion","phoneConversion","bridgeConversion"],
                                      "startDate" => start_date,
                                      "endDate" => end_date,
                                      "levelOfDetails" => 2,
                                      "reportType" => 2,
                                      "statRange"=> 2
                                      # "unitOfTime"=>5,
                                  }
                              }
                  }
                  
           @result = baidu_api(service,method,json)
           
           if @result["header"].nil?
               data = {:message => "baidu api error", :result => @result, :json => json, :status => "false"}
               return render :json => data, :status => :ok
           else
               @header = @result["header"]
               if @header["desc"].downcase == "success"
                  @quota = @header["rquota"]
                  
                  
                  if !@result["body"]["data"][0]["reportId"].nil?
                      account_report_id = @result["body"]["data"][0]["reportId"].to_s
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
                                  "reportRequestType" => {
                                      # "performanceData" => ["cost","cpc","click","impression","ctr","cpm","position","conversion","phoneConversion","bridgeConversion"],
                                      "performanceData" => ["cost","cpc","click","impression","ctr","cpm","conversion","phoneConversion","bridgeConversion"],
                                      "startDate" => start_date,
                                      "endDate" => end_date,
                                      "levelOfDetails" => 3,
                                      "reportType" => 10,
                                      "statRange"=> 3
                                      # "unitOfTime"=>5,
                                  }
                              }
                  }
                  
           @result = baidu_api(service,method,json)
           
           if @result["header"].nil?
               data = {:message => "baidu api error", :result => @result, :json => json, :status => "false"}
               return render :json => data, :status => :ok
           else
               @header = @result["header"]
               if @header["desc"].downcase == "success"
                  @quota = @header["rquota"]
                  
                  
                  if !@result["body"]["data"][0]["reportId"].nil?
                      campaign_report_id = @result["body"]["data"][0]["reportId"].to_s
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
                                  "reportRequestType" => {
                                      # "performanceData" => ["cost","cpc","click","impression","ctr","cpm","position","conversion","phoneConversion","bridgeConversion"],
                                      "performanceData" => ["cost","cpc","click","impression","ctr","cpm","conversion","phoneConversion","bridgeConversion"],
                                      "startDate" => start_date,
                                      "endDate" => end_date,
                                      "levelOfDetails" => 5,
                                      "reportType" => 11,
                                      "statRange"=> 5
                                      # "unitOfTime"=>5,
                                  }
                              }
                  }
                  
           @result = baidu_api(service,method,json)
           
           if @result["header"].nil?
               data = {:message => "baidu api error", :result => @result, :json => json, :status => "false"}
               return render :json => data, :status => :ok
           else
               @header = @result["header"]
               if @header["desc"].downcase == "success"
                  @quota = @header["rquota"]
                  
                  
                  if !@result["body"]["data"][0]["reportId"].nil?
                      adgroup_report_id = @result["body"]["data"][0]["reportId"].to_s
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
                                  "reportRequestType" => {
                                      # "performanceData" => ["cost","cpc","click","impression","ctr","cpm","position","conversion","phoneConversion","bridgeConversion"],
                                      "performanceData" => ["cost","cpc","click","impression","ctr","cpm","position","conversion","bridgeConversion"],
                                      "startDate" => start_date,
                                      "endDate" => end_date,
                                      "levelOfDetails" => 7,
                                      "reportType" => 12,
                                      "statRange"=> 7
                                      # "unitOfTime"=>5,
                                  }
                              }
                  }
                  
           @result = baidu_api(service,method,json)
           
           if @result["header"].nil?
               data = {:message => "baidu api error", :result => @result, :json => json, :status => "false"}
               return render :json => data, :status => :ok
           else
               @header = @result["header"]
               if @header["desc"].downcase == "success"
                  @quota = @header["rquota"]
                  
                  if !@result["body"]["data"][0]["reportId"].nil?
                      ad_report_id = @result["body"]["data"][0]["reportId"].to_s
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
                                  "reportRequestType" => {
                                      # "performanceData" => ["cost","cpc","click","impression","ctr","cpm","position","conversion","phoneConversion","bridgeConversion"],
                                      "performanceData" => ["cost","cpc","click","impression","ctr","cpm","position","conversion","bridgeConversion"],
                                      "startDate" => start_date,
                                      "endDate" => end_date,
                                      "levelOfDetails" => 11,
                                      "reportType" => 14,
                                      "statRange"=> 11
                                      # "unitOfTime"=>5,
                                  }
                              }
                  }
                  
           @result = baidu_api(service,method,json)
           
           if @result["header"].nil?
               data = {:message => "baidu api error", :result => @result, :json => json, :status => "false"}
               return render :json => data, :status => :ok
           else
               @header = @result["header"]
               if @header["desc"].downcase == "success"
                  @quota = @header["rquota"]
                  
                  if !@result["body"]["data"][0]["reportId"].nil?
                      keyword_report_id = @result["body"]["data"][0]["reportId"].to_s
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


  def getreportfile(networkid,username,password,apitoken,account,campaign,adgroup,ad,keyword,start_date,end_date)
      
      #before dl, check all file status
      @logger.info "baidu getreportfile, check file status" + networkid.to_s
      account_report_status = reportfilestatus(networkid,username,password,apitoken,account)
      campaign_report_status = reportfilestatus(networkid,username,password,apitoken,campaign)
      adgroup_report_status = reportfilestatus(networkid,username,password,apitoken,adgroup)
      ad_report_status = reportfilestatus(networkid,username,password,apitoken,ad)
      keyword_report_status = reportfilestatus(networkid,username,password,apitoken,keyword)
    
      @logger.info "baidu getreportfile, check file status done"
      
      if account_report_status.to_i == 3 && campaign_report_status.to_i == 3 && adgroup_report_status.to_i == 3 && ad_report_status.to_i == 3 && keyword_report_status.to_i == 3
        
          @logger.info "baidu getreportfile, dl file start"
          dlreportfile(networkid,username,password,apitoken,account,"account",start_date,end_date)
          dlreportfile(networkid,username,password,apitoken,campaign,"campaign",start_date,end_date)
          dlreportfile(networkid,username,password,apitoken,adgroup,"adgroup",start_date,end_date)
          dlreportfile(networkid,username,password,apitoken,ad,"ad",start_date,end_date)
          dlreportfile(networkid,username,password,apitoken,keyword,"keyword",start_date,end_date)
          
          @logger.info "baidu getreportfile, report all done. set to 2"
          
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
  
  
  def reportfilestatus(networkid,username,password,apitoken,reportid)
      
      # @logger.info "reportfilestatus"
      
      service = "ReportService"
      method = "getReportState"
      
      json = {'header' => { 
                          'token' => apitoken.to_s,
                              'username' => username.to_s,
                              'password' => password.to_s 
                          },
               'body'  => {
                              "reportId" => reportid
                          }
              }       
      
      @result = baidu_api(service,method,json)
      
      @header = @result["header"]
      @quota = @header["rquota"]
       
      @logger.info @result.to_s
      
      if @header["desc"].downcase == "success"
          return @result["body"]["data"][0]["isGenerated"]
      else
          return 0    
      end
    
  end
  

  def dlreportfile(networkid,username,password,apitoken,reportid,level,start_date,end_date)
    
      @logger.info "called dlreportfile baidu"+networkid.to_s
      
      service = "ReportService"
      method = "getReportFileUrl"
       
      json = {'header' => { 
                              'token' => apitoken.to_s,
                              'username' => username.to_s,
                              'password' => password.to_s 
                          },
               'body'  => {
                              "reportId" => reportid
                          }
              }
              
      @result = baidu_api(service,method,json)
      
      @header = @result["header"]
      @quota = @header["rquota"]
      
      if @header["desc"].downcase == "success"                              
            @reporturl = @result["body"]["data"][0]["reportFilePath"]
            
            if @reporturl.to_s != ""
              
                @logger.info "baidu called dl reportfile account"
                
                @zip_file = @tmp+"/baidu_" + level.to_s + "_report_" + networkid.to_s + ".csv"
                open(@zip_file.to_s, 'wb') do |file|
                    file << open(@reporturl.to_s).read
                end
                
                @file = @zip_file
                
                if level == "account"
                    
                    @logger.info "baidu called dl reportfile account insert"
                    
                    
                    
                    
                    @db3[:baidu_report_account].find({ "$and" => [{:network_id => networkid.to_i}, {:report_date => end_date.to_s}] }).delete_many
                    @db3.close()
                    
                    data_arr = []
                    
                    CSV.foreach(@file, :encoding => 'GB18030').each_with_index do |csv, index|
                        
                        
                        if index.to_i == 0
                            csv_array = csv[0].split("\t")
                            set_csv_header(csv_array)
                        end 
                        
                        if index.to_i > 0    
                            csv = csv.to_csv
                            csv_array = csv.split("\t")
                            # csv_array = csv[0].split("\t")
                            
                            
                            data_hash = {}
                            insert_hash = {}
                          
                            insert_hash[:network_id] = networkid.to_i
                            insert_hash[:report_date] = csv_array[@date_index].to_s
                            insert_hash[:name] = csv_array[@account_name_index].to_s
                            insert_hash[:total_cost] = csv_array[@cost_index].to_f
                            insert_hash[:clicks_avg_price] = csv_array[@clicks_avg_price_index].to_f
                            insert_hash[:display] = csv_array[@display_index].to_i
                            insert_hash[:click_rate] = csv_array[@click_rate_index].gsub('%', '').strip.to_f
                            insert_hash[:clicks] = csv_array[@clicks_index].to_i
                            insert_hash[:thousand_display_cost] = csv_array[@thousand_display_cost_index].to_f
                            insert_hash[:avg_position] = 0
                            
                            
                            data_hash[:insert_one] = insert_hash
                            data_arr << data_hash
                          
                            if data_arr.count.to_i > 200
                                @db3[:baidu_report_account].bulk_write(data_arr)
                                @db3.close()
                                
                                data_arr = []
                            end
                            
                            # @db3[:baidu_report_account].insert_one({
                                                                    # network_id: networkid.to_i,
                                                                    # report_date: csv_array[@date_index].to_s,
                                                                    # name: csv_array[@account_name_index].to_s,
                                                                    # total_cost: csv_array[@cost_index].to_f,
                                                                    # clicks_avg_price: csv_array[@clicks_avg_price_index].to_f,
                                                                    # display:  csv_array[@display_index].to_i,
                                                                    # click_rate: csv_array[@click_rate_index].gsub('%', '').strip.to_f,
                                                                    # clicks: csv_array[@clicks_index].to_i,
                                                                    # thousand_display_cost: csv_array[@thousand_display_cost_index].to_f,
                                                                    # avg_position: 0
                                                                  # })
#                                                                                       
                            # @db3.close()
                          
                        end
                    end
                    
                    
                    if data_arr.count.to_i > 0
                        @db3[:baidu_report_account].bulk_write(data_arr)
                        @db3.close()
                    end
                    
                    
                elsif level == "campaign"
                    
                    
                    @db3[:baidu_report_campaign].find({ "$and" => [{:network_id => networkid.to_i}, {:report_date => end_date.to_s}] }).delete_many
                    @db3.close()
                    
                    
                    data_arr = []
                    
                    CSV.foreach(@file, :encoding => 'GB18030').each_with_index do |csv, index|
                        if index.to_i == 0
                            csv_array = csv[0].split("\t")
                            set_csv_header(csv_array)
                        else  
                            csv = csv.to_csv
                            csv_array = csv.split("\t")
                            # csv_array = csv[0].split("\t")
                            
                            
                            data_hash = {}
                            insert_hash = {}
                          
                            insert_hash[:network_id] = networkid.to_i
                            insert_hash[:report_date] = csv_array[@date_index].to_s
                            insert_hash[:name] = csv_array[@account_name_index].to_s
                            insert_hash[:cpc_plan_id] = csv_array[@campaign_id_index].to_i
                            insert_hash[:cpc_plan_name] = csv_array[@campaign_name_index].to_s
                            insert_hash[:total_cost] = csv_array[@cost_index].to_f
                            insert_hash[:clicks_avg_price] = csv_array[@clicks_avg_price_index].to_f
                            insert_hash[:display] = csv_array[@display_index].to_i
                            insert_hash[:click_rate] = csv_array[@click_rate_index].gsub('%', '').strip.to_f
                            insert_hash[:clicks] = csv_array[@clicks_index].to_i
                            insert_hash[:thousand_display_cost] = csv_array[@thousand_display_cost_index].to_f
                            insert_hash[:avg_position] = 0
                            
                            
                            data_hash[:insert_one] = insert_hash
                            data_arr << data_hash
                          
                            if data_arr.count.to_i > 200
                                @db3[:baidu_report_campaign].bulk_write(data_arr)
                                @db3.close()
                                
                                data_arr = []
                            end
                            
                            
                            # @db3[:baidu_report_campaign].insert_one({
                                                                      # network_id: networkid.to_i,
                                                                      # report_date: csv_array[@date_index].to_s,
                                                                      # name: csv_array[@account_name_index].to_s,
                                                                      # cpc_plan_id: csv_array[@campaign_id_index].to_i,
                                                                      # cpc_plan_name: csv_array[@campaign_name_index].to_s,
                                                                      # total_cost: csv_array[@cost_index].to_f,
                                                                      # clicks_avg_price: csv_array[@clicks_avg_price_index].to_f,
                                                                      # display:  csv_array[@display_index].to_i,
                                                                      # click_rate:  csv_array[@click_rate_index].gsub('%', '').strip.to_f,
                                                                      # clicks: csv_array[@clicks_index].to_i,
                                                                      # thousand_display_cost: csv_array[@thousand_display_cost_index].to_f,
                                                                      # avg_position: 0
                                                                    # })
                            # @db3.close()
                        end
                    end
                    
                    if data_arr.count.to_i > 0
                        @db3[:baidu_report_campaign].bulk_write(data_arr)
                        @db3.close()
                    end
                    
                elsif level == "adgroup"
                    
                    
                    @db3[:baidu_report_adgroup].find({ "$and" => [{:network_id => networkid.to_i}, {:report_date => end_date.to_s}] }).delete_many
                    @db3.close()
                  
                  
                    data_arr = []
                  
                    CSV.foreach(@file, :encoding => 'GB18030').each_with_index do |csv, index|
                        if index.to_i == 0
                            csv_array = csv[0].split("\t")
                            set_csv_header(csv_array)
                        else
                          
                            csv = csv.to_csv
                            csv_array = csv.split("\t")
                            # csv_array = csv[0].split("\t")
                            
                            
                            
                            
                            data_hash = {}
                            insert_hash = {}
                          
                            insert_hash[:network_id] = networkid.to_i
                            insert_hash[:report_date] = csv_array[@date_index].to_s
                            insert_hash[:name] = csv_array[@account_name_index].to_s
                            insert_hash[:cpc_plan_id] = csv_array[@campaign_id_index].to_i
                            insert_hash[:cpc_plan_name] = csv_array[@campaign_name_index].to_s
                            insert_hash[:cpc_grp_id] = csv_array[@adgroup_id_index].to_i
                            insert_hash[:cpc_grp_name] = csv_array[@adgroup_name_index].to_s
                            insert_hash[:total_cost] = csv_array[@cost_index].to_f
                            insert_hash[:clicks_avg_price] = csv_array[@clicks_avg_price_index].to_f
                            insert_hash[:display] = csv_array[@display_index].to_i
                            insert_hash[:click_rate] = csv_array[@click_rate_index].gsub('%', '').strip.to_f
                            insert_hash[:clicks] = csv_array[@clicks_index].to_i
                            insert_hash[:thousand_display_cost] = csv_array[@thousand_display_cost_index].to_f
                            insert_hash[:avg_position] = 0
                            
                            
                            data_hash[:insert_one] = insert_hash
                            data_arr << data_hash
                          
                            if data_arr.count.to_i > 500
                                @db3[:baidu_report_adgroup].bulk_write(data_arr)
                                @db3.close()
                                
                                data_arr = []
                            end
                            
                            
                            
                            # @db3[:baidu_report_adgroup].insert_one({
                                                                    # network_id: networkid.to_i,
                                                                    # report_date: csv_array[@date_index].to_s,
                                                                    # name: csv_array[@account_name_index].to_s,
                                                                    # cpc_plan_id: csv_array[@campaign_id_index].to_i,
                                                                    # cpc_plan_name: csv_array[@campaign_name_index].to_s,
                                                                    # cpc_grp_id: csv_array[@adgroup_id_index].to_i,
                                                                    # cpc_grp_name: csv_array[@adgroup_name_index].to_s,
                                                                    # total_cost: csv_array[@cost_index].to_f,
                                                                    # clicks_avg_price: csv_array[@clicks_avg_price_index].to_f,
                                                                    # display:  csv_array[@display_index].to_i,
                                                                    # click_rate:  csv_array[@click_rate_index].gsub('%', '').strip.to_f,
                                                                    # clicks: csv_array[@clicks_index].to_i,
                                                                    # thousand_display_cost: csv_array[@thousand_display_cost_index].to_f,
                                                                    # avg_position: 0
                                                                  # })       
                            # @db3.close()
                              
                        end
                    end
                    
                    if data_arr.count.to_i > 0
                        @db3[:baidu_report_adgroup].bulk_write(data_arr)
                        @db3.close()
                    end
                    
                elsif level == "ad"
                    
                    
                    @db3[:baidu_report_ad].find({ "$and" => [{:network_id => networkid.to_i}, {:report_date => end_date.to_s}] }).delete_many
                    @db3.close()
                    
                    data_arr = []
                  
                    CSV.foreach(@file, :encoding => 'GB18030').each_with_index do |csv, index|
                        if index.to_i == 0
                            csv_array = csv[0].split("\t")
                            set_csv_header(csv_array)
                        else
                            csv = csv.to_csv
                            csv_array = csv.split("\t")
                            # csv_array = csv[0].split("\t")
                            
                            
                            
                            data_hash = {}
                            insert_hash = {}
                          
                            insert_hash[:network_id] = networkid.to_i
                            insert_hash[:report_date] = csv_array[@date_index].to_s
                            insert_hash[:name] = csv_array[@account_name_index].to_s
                            insert_hash[:cpc_plan_id] = csv_array[@campaign_id_index].to_i
                            insert_hash[:cpc_plan_name] = csv_array[@campaign_name_index].to_s
                            insert_hash[:cpc_grp_id] = csv_array[@adgroup_id_index].to_i
                            insert_hash[:cpc_grp_name] = csv_array[@adgroup_name_index].to_s
                            insert_hash[:ad_id] = csv_array[@ad_id_index].to_i
                            insert_hash[:title] = csv_array[@ad_title_index].to_s
                            insert_hash[:description_1] = csv_array[@ad_desc1_index].to_s
                            insert_hash[:description_2] = csv_array[@ad_desc2_index].to_s
                            insert_hash[:visit_url] = ""
                            insert_hash[:show_url] = csv_array[@display_url_index].to_s
                            insert_hash[:mobile_visit_url] = ""
                            insert_hash[:mobile_show_url] = ""
                            insert_hash[:total_cost] = csv_array[@cost_index].to_f
                            insert_hash[:clicks_avg_price] = csv_array[@clicks_avg_price_index].to_f
                            insert_hash[:display] = csv_array[@display_index].to_i
                            insert_hash[:click_rate] = csv_array[@click_rate_index].gsub('%', '').strip.to_f
                            insert_hash[:clicks] = csv_array[@clicks_index].to_i
                            insert_hash[:thousand_display_cost] = csv_array[@thousand_display_cost_index].to_f
                            insert_hash[:avg_position] = csv_array[@avg_pos_index].to_f
                            
                            
                            data_hash[:insert_one] = insert_hash
                            data_arr << data_hash
                          
                            if data_arr.count.to_i > 500
                                @db3[:baidu_report_ad].bulk_write(data_arr)
                                @db3.close()
                                
                                data_arr = []
                            end
                            
                            
                            
                            # @db3[:baidu_report_ad].insert_one({
                                                                # network_id: networkid.to_i,
                                                                # report_date: csv_array[@date_index].to_s,
                                                                # name: csv_array[@account_name_index].to_s,
                                                                # cpc_plan_id: csv_array[@campaign_id_index].to_i,
                                                                # cpc_plan_name: csv_array[@campaign_name_index].to_s,
                                                                # cpc_grp_id: csv_array[@adgroup_id_index].to_i,
                                                                # cpc_grp_name: csv_array[@adgroup_name_index].to_s,
                                                                # ad_id: csv_array[@ad_id_index].to_i,
                                                                # title: csv_array[@ad_title_index].to_s,
                                                                # description_1: csv_array[@ad_desc1_index].to_s,
                                                                # description_2: csv_array[@ad_desc2_index].to_s,
                                                                # visit_url: "",
                                                                # show_url: csv_array[@display_url_index].to_s,
                                                                # mobile_visit_url: "",
                                                                # mobile_show_url: "",
                                                                # total_cost: csv_array[@cost_index].to_f,
                                                                # clicks_avg_price: csv_array[@clicks_avg_price_index].to_f,
                                                                # display:  csv_array[@display_index].to_i,
                                                                # click_rate:  csv_array[@click_rate_index].gsub('%', '').strip.to_f,
                                                                # clicks: csv_array[@clicks_index].to_i,
                                                                # thousand_display_cost: csv_array[@thousand_display_cost_index].to_f,
                                                                # avg_position: csv_array[@avg_pos_index].to_f
                                                              # })            
                            # @db3.close()      
                        end
                    end
                    
                    if data_arr.count.to_i > 0
                        @db3[:baidu_report_ad].bulk_write(data_arr)
                        @db3.close()
                    end
                    
                elsif level == "keyword"
                  
                    
                    
                    @db3[:baidu_report_keyword].find({ "$and" => [{:network_id => networkid.to_i}, {:report_date => end_date.to_s}] }).delete_many
                    @db3.close()
                  
                    data_arr = []
                  
                    CSV.foreach(@file, :encoding => 'GB18030').each_with_index do |csv, index|
                        if index.to_i == 0
                            csv_array = csv[0].split("\t")
                            set_csv_header(csv_array)
                        else   
                            csv = csv.to_csv
                            csv_array = csv.split("\t")
                            # csv_array = csv[0].split("\t")
                            
                            
                            
                            
                            data_hash = {}
                            insert_hash = {}
                          
                            insert_hash[:network_id] = networkid.to_i
                            insert_hash[:report_date] = csv_array[@date_index].to_s
                            insert_hash[:name] = csv_array[@account_name_index].to_s
                            insert_hash[:cpc_plan_id] = csv_array[@campaign_id_index].to_i
                            insert_hash[:cpc_plan_name] = csv_array[@campaign_name_index].to_s
                            insert_hash[:cpc_grp_id] = csv_array[@adgroup_id_index].to_i
                            insert_hash[:cpc_grp_name] = csv_array[@adgroup_name_index].to_s
                            insert_hash[:keyword_id] = csv_array[@keyword_keywordid_index].to_i
                            insert_hash[:word_id] = csv_array[@keyword_wordid_index].to_i
                            insert_hash[:keyword] = csv_array[@keyword_keyword_index].to_s
                            insert_hash[:total_cost] = csv_array[@cost_index].to_f
                            insert_hash[:clicks_avg_price] = csv_array[@clicks_avg_price_index].to_f
                            insert_hash[:display] = csv_array[@display_index].to_i
                            insert_hash[:click_rate] = csv_array[@click_rate_index].gsub('%', '').strip.to_f
                            insert_hash[:clicks] = csv_array[@clicks_index].to_i
                            insert_hash[:thousand_display_cost] = csv_array[@thousand_display_cost_index].to_f
                            insert_hash[:avg_position] = csv_array[@avg_pos_index].to_f
                            
                            
                            data_hash[:insert_one] = insert_hash
                            data_arr << data_hash
                          
                            if data_arr.count.to_i > 1000
                                @db3[:baidu_report_keyword].bulk_write(data_arr)
                                @db3.close()
                                
                                data_arr = []
                            end
                            
                            
                            
                            # @db3[:baidu_report_keyword].insert_one({
                                                                    # network_id: networkid.to_i,
                                                                    # report_date: csv_array[@date_index].to_s,
                                                                    # name: csv_array[@account_name_index].to_s,
                                                                    # cpc_plan_id: csv_array[@campaign_id_index].to_i,
                                                                    # cpc_plan_name: csv_array[@campaign_name_index].to_s,
                                                                    # cpc_grp_id: csv_array[@adgroup_id_index].to_i,
                                                                    # cpc_grp_name: csv_array[@adgroup_name_index].to_s,
                                                                    # keyword_id: csv_array[@keyword_keywordid_index].to_i,
                                                                    # word_id: csv_array[@keyword_wordid_index].to_i,
                                                                    # keyword: csv_array[@keyword_keyword_index].to_s,
                                                                    # total_cost: csv_array[@cost_index].to_f,
                                                                    # clicks_avg_price: csv_array[@clicks_avg_price_index].to_f,
                                                                    # display:  csv_array[@display_index].to_i,
                                                                    # click_rate:  csv_array[@click_rate_index].gsub('%', '').strip.to_f,
                                                                    # clicks: csv_array[@clicks_index].to_i,
                                                                    # thousand_display_cost: csv_array[@thousand_display_cost_index].to_f,
                                                                    # avg_position: csv_array[@avg_pos_index].to_f
                                                                  # })
                            # @db3.close()
                          
                        end
                    end
                    
                    if data_arr.count.to_i > 0
                        @db3[:baidu_report_keyword].bulk_write(data_arr)
                        @db3.close()
                    end
                end
                
                @logger.info "baidu called dl reportfile done, remove file"
                unzip_folder = @zip_file
                if File.exists?(unzip_folder)
                    File.delete(unzip_folder)
                end
            end
      end
  end




  def set_csv_header(array)
    
      # @logger.info "set_csv_header run"
      # @logger.info array
    
      array.each_with_index do |csv_header, header_index|
          
          @logger.info csv_header
          
          if csv_header.to_s.strip == "keywordId"
            @keywordId_index = header_index
          end
          
          if csv_header.to_s.strip == "keyword"
            @keyword_index = header_index
          end
          
          if csv_header.to_s.strip.include?("price")
            @price_index = header_index
          end
          
          if csv_header.to_s.strip == "matchType"
            @matchType_index = header_index
          end
          
          if csv_header.to_s.strip == "phraseType"
            @phraseType_index = header_index
          end
          
          if csv_header.to_s.strip == "quality"
            @quality_index = header_index
          end
          
          if csv_header.to_s.strip == "reliable"
            @reliable_index = header_index
          end
          
          if csv_header.to_s.strip == "reason"
            @reason_index = header_index
          end
          
          if csv_header.to_s.strip == "mobilequality"
            @mobilequality_index = header_index
          end
          
          if csv_header.to_s.strip == "mobilereliable"
            @mobilereliable_index = header_index
          end
          
          if csv_header.to_s.strip == "mobilereason"
            @mobilereason_index = header_index
          end
          
          if csv_header.to_s.strip == "wmatchprefer"
            @wmatchprefer_index = header_index
          end
          
          
          
          
          if csv_header.to_s.strip == "creativeId"
            @creativeId_index = header_index
          end
          
          if csv_header.to_s.strip == "title"
            @title_index = header_index
          end
          
          if csv_header.to_s.strip == "description1"
            @description1_index = header_index
          end
          
          if csv_header.to_s.strip == "description2"
            @description2_index = header_index
          end
          
          if csv_header.to_s.strip == "pcDestinationUrl"
            @pcDestinationUrl_index = header_index
          end
          
          if csv_header.to_s.strip == "pcDisplayUrl"
            @pcDisplayUrl_index = header_index
          end
          
          if csv_header.to_s.strip == "mobileDestinationUrl"
            @mobileDestinationUrl_index = header_index
          end
          
          if csv_header.to_s.strip == "mobileDisplayUrl"
            @mobileDisplayUrl_index = header_index
          end
          
          if csv_header.to_s.strip == "temp"
            @temp_index = header_index
          end
          
          if csv_header.to_s.strip == "devicePreference"
            @devicePreference_index = header_index
          end
          
          if csv_header.to_s.strip == "tabs"
            @tabs_index = header_index
          end
          
          if csv_header.to_s.strip == "adgroupId"
            @adgroupId_index = header_index
          end
          
          if csv_header.to_s.strip == "adgroupName"
            @adgroupName_index = header_index
          end
          
          if csv_header.to_s.strip == "accuPriceFactor"
            @accuPriceFactor_index = header_index
          end
          
          if csv_header.to_s.strip == "wordPriceFactor"
            @wordPriceFactor_index = header_index
          end
          
          if csv_header.to_s.strip == "widePriceFactor"
            @widePriceFactor_index = header_index
          end
          
          if csv_header.to_s.strip == "matchPriceFactorStatus"
            @matchPriceFactorStatus_index = header_index
          end
          
          if csv_header.to_s.strip == "maxPrice"
            @maxPrice_index = header_index
          end
          
          if csv_header.to_s.strip == "campaignId"
            @campaignId_index = header_index
          end
          
          if csv_header.to_s.strip == "campaignName"
            @campaignName_index = header_index
          end
          
          if csv_header.to_s.strip == "negativeWords"
            @negativeWords_index = header_index
          end
          
          if csv_header.to_s.strip == "exactNegativeWords"
            @exactNegativeWords_index = header_index
          end
          
          if csv_header.to_s.strip.include?("schedule")
            @schedule_index = header_index
          end
          
          if csv_header.to_s.strip.include?("budgetOfflineTime")
            @budgetOfflineTime_index = header_index
          end
          
          if csv_header.to_s.strip == "showProb"
            @showProb_index = header_index
          end
          
          if csv_header.to_s.strip == "device"
            @device_index = header_index
          end
          
          if csv_header.to_s.strip == "priceRatio"
            @priceRatio_index = header_index
          end
          
          if csv_header.to_s.strip == "pause"
            @pause_index = header_index
          end
          
          if csv_header.to_s.strip == "status"
            @status_index = header_index
          end
          
          if csv_header.to_s.strip == "dynCreativeExclusion"
            @dynCreativeExclusion_index = header_index
          end
          
          if csv_header.to_s.strip == "campaignType"
            @campaignType_index = header_index
          end
          
          if csv_header.to_s.strip == "rmktStatus"
            @rmktStatus_index = header_index
          end
          
          if csv_header.to_s.strip == "rmktPriceRatio"
            @rmktPriceRatio_index = header_index
          end
          
          if csv_header.to_s.strip == "userId"
            @userId_index = header_index
          end
          
          if csv_header.to_s.strip == "balance"
            @balance_index = header_index
          end
          
          if csv_header.to_s.strip == "cost"
            @cost_index = header_index
          end
          
          if csv_header.to_s.strip == "payment"
            @payment_index = header_index
          end
          
          if csv_header.to_s.strip == "budgetType"
            @budgetType_index = header_index
          end
          
          if csv_header.to_s.strip == "budget"
            @budget_index = header_index
          end
          
          if csv_header.to_s.strip == "regionTarget"
            @regionTarget_index = header_index
          end
          
          if csv_header.to_s.strip == "excludeIp"
            @excludeIp_index = header_index
          end
          
          if csv_header.to_s.strip == "openDomains"
            @openDomains_index = header_index
          end
          
          if csv_header.to_s.strip == "regDomain"
            @regDomain_index = header_index
          end
          
          if csv_header.to_s.strip == "budgetOfflineTime"
            @budgetOfflineTime_index = header_index
          end
          
          if csv_header.to_s.strip == "weeklyBudget"
            @weeklyBudget_index = header_index
          end
          
          if csv_header.to_s.strip == "isDynamicCreative"
            @isDynamicCreative_index = header_index
          end
          
          if csv_header.to_s.strip == "dynamicCreativeParam"
            @dynamicCreativeParam_index = header_index
          end
          
          if csv_header.to_s.strip == "isDynamicTagSublink"
            @isDynamicTagSublink_index = header_index
          end
          
          if csv_header.to_s.strip == "isDynamicTitle"
            @isDynamicTitle_index = header_index
          end
          
          if csv_header.to_s.strip == "isDynamicHotRedirect"
            @isDynamicHotRedirect_index = header_index
          end
          
          if csv_header.to_s.strip == "pcBalance"
            @pcBalance_index = header_index
          end
          
          if csv_header.to_s.strip == "mobileBalance"
            @mobileBalance_index = header_index
          end
          
          if csv_header.to_s.strip == "userLevel"
            @userLevel_index = header_index
          end
          
          if csv_header.to_s.strip == "日期"
            @date_index = header_index
          end
          
          if csv_header.to_s.strip == "账户ID"
            @account_id_index = header_index
          end
          
          if csv_header.to_s.strip == "账户"
            @account_name_index = header_index
          end
          
          if csv_header.to_s.strip == "展现量"
            @display_index = header_index
          end
          
          if csv_header.to_s.strip == "点击量"
            @clicks_index = header_index
          end
          
          if csv_header.to_s.strip == "消费"
            @cost_index = header_index
          end
          
          if csv_header.to_s.strip == "点击率"
            @click_rate_index = header_index
            @logger.info @click_rate_index.to_s
          end
          
          
          if csv_header.to_s.strip == "平均点击价格"
            @clicks_avg_price_index = header_index
          end
          
          if csv_header.to_s.strip == "千次展现消费"
            @thousand_display_cost_index = header_index
          end
          
          if csv_header.to_s.strip == "推广计划ID"
            @campaign_id_index = header_index
          end
          
          if csv_header.to_s.strip == "推广计划"
            @campaign_name_index = header_index
          end
          
          if csv_header.to_s.strip == "推广单元ID"
            @adgroup_id_index = header_index
          end
          
          if csv_header.to_s.strip == "推广单元"
            @adgroup_name_index = header_index
          end
          
          
          if csv_header.to_s.strip == "创意ID"
            @ad_id_index = header_index
          end
          
          if csv_header.to_s.strip == "创意标题"
            @ad_title_index = header_index
          end
          
          if csv_header.to_s.strip == "创意描述1"
            @ad_desc1_index = header_index
          end
          
          if csv_header.to_s.strip == "创意描述2"
            @ad_desc2_index = header_index
          end
          
          if csv_header.to_s.strip == "显示URL"
            @display_url_index = header_index
          end
          
          if csv_header.to_s.strip == "平均排名"
            @avg_pos_index = header_index
          end
          
          if csv_header.to_s.strip == "关键词keywordID"
            @keyword_keywordid_index = header_index
          end
          
          if csv_header.to_s.strip == "关键词ID"
            @keyword_wordid_index = header_index
          end
          
          if csv_header.to_s.strip == "关键词"
            @keyword_keyword_index = header_index
          end
          
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
    
  end
  
  def insert_campaign(network_id,network_name,campaign)
    @logger.info "insert : network "+network_id.to_s+ " campaign: "+campaign.to_s
    
    @db["all_campaign"].insert_one({ 
                                    network_id: network_id.to_i,
                                    network_type: "baidu",
                                    account_name: network_name.to_s, 
                                    campaign_id: campaign[@campaignId_index].gsub('"', '').to_i,
                                    campaign_name: campaign[@campaignName_index].gsub('""', '"').to_s, 
                                    budget: campaign[@budget_index].to_f, 
                                    regions: campaign[@regionTarget_index].to_s, 
                                    exclude_ips: campaign[@excludeIp_index].to_s,
                                    negative_words: campaign[@negativeWords_index],
                                    exact_negative_words: campaign[@exactNegativeWords_index],
                                    schedule: campaign[@schedule_index],
                                    budget_offline_time: campaign[@budgetOfflineTime_index],
                                    show_prob: campaign[@showProb_index].to_i,
                                    pause: campaign[@pause_index].to_s,
                                    device: campaign[@device_index].to_i,
                                    priceRatio: campaign[@priceRatio_index].to_f,
                                    status: campaign[@status_index].to_i,
                                    isDynamicCreative: campaign[@isDynamicCreative_index].to_s,
                                    dynCreativeExclusion: campaign[@dynCreativeExclusion_index].to_s,
                                    campaignType: campaign[@campaignType_index].to_i,
                                    isDynamicTagSublink: campaign[@isDynamicTagSublink_index].to_s,
                                    isDynamicTitle: campaign[@isDynamicTitle_index].to_s,
                                    isDynamicHotRedirect: campaign[@isDynamicHotRedirect_index].to_s,
                                    rmktStatus: campaign[@rmktStatus_index].to_s,
                                    rmktPriceRatio: campaign[@rmktPriceRatio_index].gsub('"', '').to_f,
                                    update_date: @now,                                            
                                    create_date: @now })
                                    
    @db.close
  end
  
  
  def insert_adgroup(network_id,adgroup)
    # @logger.info "insert : network "+network_id.to_s+ " adgroup: "+adgroup.to_s
    
    db_name = "adgroup_baidu_"+network_id.to_s
    @baidu_db[db_name].insert_one({ 
                                network_id: network_id.to_i,
                                campaign_id: adgroup[@campaignId_index].gsub('"', '').to_i,
                                adgroup_id: adgroup[@adgroupId_index].to_i,
                                name: adgroup[@adgroupName_index].gsub('""', '"').to_s,
                                max_price: adgroup[@maxPrice_index].to_f,
                                negative_words: adgroup[@negativeWords_index].to_s,
                                exact_negative_words: adgroup[@exactNegativeWords_index].to_s,
                                pause: adgroup[@pause_index].to_s,
                                status: adgroup[@status_index].to_i,
                                accuPriceFactor: adgroup[@accuPriceFactor_index].to_f,
                                wordPriceFactor: adgroup[@wordPriceFactor_index].to_f,
                                widePriceFactor: adgroup[@widePriceFactor_index].to_f,
                                matchPriceFactorStatus: adgroup[@matchPriceFactorStatus_index].to_i,
                                priceRatio: adgroup[@priceRatio_index].gsub('"', '').to_s,
                                update_date: @now,                                            
                                create_date: @now })
    @baidu_db.close()                                    
  end
  
  
  def insert_ad(network_id,ad,url,m_url)
    # @logger.info "insert : network "+network_id.to_s+ " ad: "+ad.to_s
    db_name = "ad_baidu_"+network_id.to_s
    
    
    @baidu_db[db_name].insert_one({ 
                                    network_id: network_id.to_i,
                                    campaign_id: ad[@campaignId_index].gsub('"', '').to_i, 
                                    adgroup_id: ad[@adgroupId_index].to_i,
                                    ad_id: ad[@creativeId_index].to_i,
                                    title: ad[@title_index].gsub('""', '"').to_s, 
                                    description_1: ad[@description1_index].to_s, 
                                    description_2: ad[@description2_index].to_s, 
                                    visit_url: url.to_s,
                                    show_url: ad[@pcDisplayUrl_index].to_s,
                                    mobile_visit_url: m_url.to_s,
                                    mobile_show_url: ad[@mobileDisplayUrl_index].to_s,
                                    pause: ad[@pause_index].to_s,
                                    status: ad[@status_index].to_i,
                                    temp: ad[@temp_index].to_i,
                                    devicePreference: ad[@devicePreference_index].to_i,
                                    tabs: ad[@tabs_index].gsub('"', ''),
                                    response_code: "",
                                    m_response_code: "",
                                    update_date: @now,                                            
                                    create_date: @now })
    @baidu_db.close()                                    
  end
  
  
  
  
  
  def insert_keyword(network_id,keyword,url,m_url)
    # @logger.info "insert : network "+network_id.to_s+ " keyword: "+keyword.to_s
    
    db_name = "keyword_baidu_"+network_id.to_s
    
    @baidu_db[db_name].insert_one({ 
                                    network_id: network_id.to_i,
                                    campaign_id: keyword[@campaignId_index].gsub('"', '').to_i,
                                    adgroup_id: keyword[@adgroupId_index].to_i,
                                    keyword_id: keyword[@keywordId_index].to_i,
                                    keyword: keyword[@keyword_index].to_s,
                                    price: keyword[@price_index].to_f, 
                                    visit_url: url.to_s,
                                    mobile_visit_url: m_url.to_s,
                                    match_type: keyword[@matchType_index].to_i,
                                    pause: keyword[@pause_index].to_s,
                                    status: keyword[@status_index].to_i,
                                    pc_quality: keyword[@quality_index].to_f,
                                    temp: keyword[@temp_index].to_i,
                                    phrase_type: keyword[@phraseType_index].to_i,
                                    reliable: keyword[@reliable_index].to_i,
                                    reason: keyword[@reason_index].to_i,
                                    mobilequality: keyword[@mobilequality_index].to_f,
                                    mobilereliable: keyword[@mobilereliable_index].to_i,
                                    mobilereason: keyword[@mobilereason_index].to_i,
                                    wmatchprefer: keyword[@wmatchprefer_index].to_i,
                                    tabs: keyword[@tabs_index].gsub('"', ''),
                                    response_code: "",
                                    m_response_code: "",
                                    update_date: @now,                                            
                                    create_date: @now })
    @baidu_db.close()                                    
  end
  
  
  
  
  
  
  
  def keyword 
    @logger.info "baidu keyword start"
    
    @id = params[:id]
    if @id.nil?
      
        # @current_network = @db[:network].find('type' => 'sogou', 'file_update_1' => 3)
        @current_network = @db[:network].find({ '$or' => [ {'file_update_1' => 3},{'file_update_2' => 3},{'file_update_3' => 3},{'file_update_4' => 3} ],'type' => 'baidu', 'worker' => @port.to_i})
        @db.close
        
        if @current_network.count.to_i >= 2
            @logger.info "working, no need update baidu keyword"
            return render :nothing => true
        end
      
        
        @network = @db[:network].find({ "$and" => [{:type => 'baidu'},{:file_update_1 => 4},{:file_update_2 => 4},{:file_update_3 => 4},{:file_update_4 => 2}, {:worker => @port.to_i}] }).sort({ last_update: -1 }).limit(1)
        @db.close

        if @network.count.to_i == 0
            @logger.info "no need update baidu keyword"
            return render :nothing => true
        end
    else
        @network = @db[:network].find({ "$and" => [{:type => 'baidu'},{:id => @id.to_i}] })
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
                @tmp_file = "/datadrive/"+doc['tmp_file'].to_s+"_keyword"
                if !File.directory?(@tmp_file)
                    redownload(doc["id"])
                    @do = 0
                    @logger.info "baidu " + doc['id'].to_s + " need to re download structure"
                    return render :nothing => true
                end
            end
            
            if @do == 1
            
                @logger.info "baidu keyword " + doc['id'].to_s + " running"
                
                @unzip_folder = @tmp_file + "/*"
                @files = Dir.glob(@unzip_folder)
                
                @logger.info "baidu keyword " + doc["id"].to_s + " updating "
                        
                @db[:network].find(id: doc["id"].to_i).update_one('$set'=> {'file_update_4' => 3})
                @db.close
                
                db_name = "keyword_baidu_"+doc['id'].to_s
                @baidu_db[db_name].drop
                @baidu_db.close()
                    
                    
                begin
                    @baidu_db[db_name].indexes.create_one(network_id: Mongo::Index::ASCENDING)
                    @baidu_db[db_name].indexes.create_one(campaign_id: Mongo::Index::ASCENDING)
                    @baidu_db[db_name].indexes.create_one(adgroup_id: Mongo::Index::ASCENDING)
                    @baidu_db[db_name].indexes.create_one(keyword_id: Mongo::Index::ASCENDING)
                    @baidu_db[db_name].indexes.create_one(keyword: Mongo::Index::ASCENDING)
                    @baidu_db[db_name].indexes.create_one(price: Mongo::Index::ASCENDING)
                    # @db[db_name].indexes.create_one(visit_url: Mongo::Index::ASCENDING)
                    # @db[db_name].indexes.create_one(mobile_visit_url: Mongo::Index::ASCENDING)
                    @baidu_db[db_name].indexes.create_one(match_type: Mongo::Index::ASCENDING)
                    @baidu_db[db_name].indexes.create_one(pause: Mongo::Index::ASCENDING)
                    @baidu_db[db_name].indexes.create_one(status: Mongo::Index::ASCENDING)
                    @baidu_db[db_name].indexes.create_one(cpc_quality: Mongo::Index::ASCENDING)
                    @baidu_db[db_name].indexes.create_one(reliable: Mongo::Index::ASCENDING)
                    @baidu_db[db_name].indexes.create_one(reason: Mongo::Index::ASCENDING)
                    @baidu_db[db_name].indexes.create_one(mobilequality: Mongo::Index::ASCENDING)
                    @baidu_db[db_name].indexes.create_one(mobilereliable: Mongo::Index::ASCENDING)
                    @baidu_db[db_name].indexes.create_one(mobilereason: Mongo::Index::ASCENDING)
                    @baidu_db[db_name].indexes.create_one(wmatchprefer: Mongo::Index::ASCENDING)
                    @baidu_db[db_name].indexes.create_one(tabs: Mongo::Index::ASCENDING)
                    # @baidu_db[db_name].indexes.create_one(watchdog: Mongo::Index::ASCENDING)
                    @baidu_db[db_name].indexes.create_one(response_code: Mongo::Index::ASCENDING)
                    @baidu_db[db_name].indexes.create_one(m_response_code: Mongo::Index::ASCENDING)
                    @baidu_db[db_name].indexes.create_one(update_date: Mongo::Index::ASCENDING)
                    @baidu_db[db_name].indexes.create_one(create_date: Mongo::Index::ASCENDING)
                rescue Exception
                end
                
      
                @files.each_with_index do |file, index|
                    # weird quote in some field
                    
                    
                    data_arr = []
                    
                    CSV.foreach(file, :encoding => 'GB18030', :quote_char => "\x00").each_with_index do |csv, index|
                        begin
                            if index.to_i == 0
                                csv_array = csv[0].split("\t")
                                set_csv_header(csv_array)
                            else
                                csv = csv.to_csv
                                csv_array = csv.split("\t")
                                
                                
                                
                                url_tag = 0
                                m_url_tag = 0
                                    
                                @final_url = csv_array[@pcDestinationUrl_index].to_s.gsub('""', '').gsub('-', '')
                                @m_final_url = csv_array[@mobileDestinationUrl_index].to_s.gsub('""', '').gsub('-', '')
                                
                                # @logger.info @final_url
                                # @logger.info "--"
                                # @logger.info @m_final_url
                                # @logger.info "||||||||||||||||||||||||"
                                
                                if !@final_url.to_s.include?(".adeqo.") && @tracking_type.to_s.downcase == "adeqo" && @keyword_redirect.to_s.downcase == "yes" && @final_url.to_s != ""
                                  
                                    url_tag = 1
                                    @temp_final_url = @final_url
                                    @final_url = "http://t.adeqo.com/click?company_id="+@company_id.to_s+"&network_id="+doc["id"].to_s
                                    @final_url = @final_url + "&campaign_id="+csv_array[@campaignId_index].gsub('"', '').to_s+"&adgroup_id="+csv_array[@adgroupId_index].to_s+"&ad_id=0&keyword_id="+csv_array[@keywordId_index].to_s
                                    @final_url = @final_url + "&cookie="+@cookie_length.to_s
                                    @final_url = @final_url + "&device=pc"
                                    @final_url = @final_url + "&tv=v1&durl="+CGI.escape(@temp_final_url.to_s)
                                end
                                
                                if !@m_final_url.to_s.include?(".adeqo.") && @tracking_type.to_s.downcase == "adeqo" && @keyword_redirect.to_s.downcase == "yes" && @final_url.to_s != ""
                                  
                                    m_url_tag = 1 
                                    @temp_m_final_url = @m_final_url
                                    @m_final_url = "http://t.adeqo.com/click?company_id="+@company_id.to_s+"&network_id="+doc["id"].to_s
                                    @m_final_url = @m_final_url + "&campaign_id="+csv_array[@campaignId_index].gsub('"', '').to_s+"&adgroup_id="+csv_array[@adgroupId_index].to_s+"&ad_id=0&keyword_id="+csv_array[@keywordId_index].to_s
                                    @m_final_url = @m_final_url + "&cookie="+@cookie_length.to_s
                                    @m_final_url = @m_final_url + "&device=mobile"
                                    @m_final_url = @m_final_url + "&tv=v1&durl="+CGI.escape(@temp_m_final_url.to_s)
                                end
                                 
                                # @logger.info @final_url
                                # @logger.info "--"
                                # @logger.info @m_final_url
                                # @logger.info "||||||||||||||||||||||||"
#                                 
                                # @logger.info url_tag
                                # @logger.info "--"
                                # @logger.info m_url_tag
                                # @logger.info "||||||||||||||||||||||||" 
                                 
                                # @logger.info csv_array
                                
                                  
                                  
                                if url_tag == 1 || m_url_tag == 1
                                  
                                    @logger.info "add keyword with tag"
                                    # @logger.info @remain_quote
                                  
                                    if @remain_quote.to_i >= 500
                                        requesttypearray = [] 
                                        requesttype = {}
                                        
                                        requesttype[:keywordId]    =     csv_array[@keywordId_index].to_i
                                        requesttype[:adgroupId]    =     0
                                        requesttype[:status]    =     0
                                        requesttype[:mobileDestinationUrl] =    @m_final_url
                                        requesttype[:pcDestinationUrl]    =     @final_url
                                        
                                        
                                        requesttypearray << requesttype
                                    
                                        service = "KeywordService"
                                        method = "updateWord"
                                        
                                        json = {'header' => { 
                                                    'token' => doc["api_token"].to_s,
                                                    'username' => doc["username"].to_s,
                                                    'password' => doc["password"].to_s 
                                                      },
                                                 'body'  => {
                                                        'keywordTypes' => requesttypearray
                                                      }
                                                }       
                                            
                                        @update_info = baidu_api(service,method,json)
                                        
                                        @logger.info @update_info
                                        @logger.info requesttypearray
                                        
                                        @header = @update_info["header"]
                                        @remain_quote = @header["rquota"]
                                            
                                        if !@update_info["header"]["desc"].nil? && @update_info["header"]["desc"].to_s == "success"
                                            
                                        else
                                            @final_url = csv_array[@pcDestinationUrl_index].to_s.gsub('""', '').gsub('-', '')
                                            @m_final_url = csv_array[@mobileDestinationUrl_index].to_s.gsub('""', '').gsub('-', '')
                                        end
                                         
                                    end
                                end
                                
                                # insert_keyword(doc["id"],csv_array,@final_url,@m_final_url)
                                
                                
                                data_hash = {}
                                insert_hash = {}
                              
                                insert_hash[:network_id] = doc["id"].to_i
                                insert_hash[:campaign_id] = csv_array[@campaignId_index].gsub('"', '').to_i
                                insert_hash[:adgroup_id] = csv_array[@adgroupId_index].to_i
                                insert_hash[:keyword_id] = csv_array[@keywordId_index].to_i
                                insert_hash[:keyword] = csv_array[@keyword_index].to_s
                                insert_hash[:price] = csv_array[@price_index].to_f
                                insert_hash[:visit_url] = @final_url.to_s
                                insert_hash[:mobile_visit_url] = @m_final_url.to_s
                                insert_hash[:match_type] = csv_array[@matchType_index].to_i
                                insert_hash[:pause] = csv_array[@pause_index].to_s
                                insert_hash[:status] = csv_array[@status_index].to_i
                                insert_hash[:cpc_quality] = csv_array[@quality_index].to_f
                                insert_hash[:temp] = csv_array[@temp_index].to_i
                                insert_hash[:phrase_type] = csv_array[@phraseType_index].to_i
                                insert_hash[:reliable] = csv_array[@reliable_index].to_i
                                insert_hash[:reason] = csv_array[@reason_index].to_i
                                insert_hash[:mobilequality] = csv_array[@mobilequality_index].to_f
                                insert_hash[:mobilereliable] = csv_array[@mobilereliable_index].to_i
                                insert_hash[:mobilereason] = csv_array[@mobilereason_index].to_i
                                insert_hash[:wmatchprefer] = csv_array[@wmatchprefer_index].to_i
                                insert_hash[:tabs] = csv_array[@tabs_index].gsub('"', '')
                                insert_hash[:response_code] = ""
                                insert_hash[:m_response_code] = ""
                                insert_hash[:create_date] = @now
                                insert_hash[:update_date] = @now
                                
                                
                                data_hash[:insert_one] = insert_hash
                                data_arr << data_hash
                              
                                if data_arr.count.to_i > 20000
                                    db_name = "keyword_baidu_"+doc["id"].to_s
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
                        db_name = "keyword_baidu_"+doc["id"].to_s
                        @baidu_db[db_name].bulk_write(data_arr)
                        @baidu_db.close()
                        
                        data_arr = []
                    end
                end      
                              
                @db[:network].find(id: doc["id"].to_i).update_one('$set'=> {'file_update_4' => 4, 'last_update' => @now, 'worker' => ""})
                @db.close              
                
                unzip_folder = @tmp+"/"+doc["tmp_file"]+"_keyword"
                if File.directory?(unzip_folder)
                    FileUtils.remove_dir unzip_folder, true
                end
                
            end
        rescue Exception
            redownload(doc["id"])
            return render :nothing => true
        end
    end
    
    @logger.info "baidu keyword done"
    return render :nothing => true 
  end
  
  
   
  def ad 
    @logger.info "baidu ad start"
    
    @id = params[:id]
    if @id.nil?
      
        # @current_network = @db[:network].find('type' => 'sogou', 'file_update_1' => 3)
        @current_network = @db[:network].find({ '$or' => [ {'file_update_1' => 3},{'file_update_2' => 3},{'file_update_3' => 3},{'file_update_4' => 3} ],'type' => 'baidu', 'worker' => @port.to_i})
        @db.close
        
        if @current_network.count.to_i >= 2
            @logger.info "working, no need update baidu ad"
            return render :nothing => true
        end
      
        
        
        @network = @db[:network].find({ "$and" => [{:type => 'baidu'},{:file_update_1 => 4},{:file_update_2 => 4},{:file_update_3 => 2},{:worker => @port.to_i}] }).sort({ last_update: -1 }).limit(1)
        @db.close

        if @network.count.to_i == 0
            @logger.info "no need update baidu ad"
            return render :nothing => true
        end
    else
      
        @network = @db[:network].find({ "$and" => [{:type => 'baidu'},{:id => @id.to_i}] })
        @db.close
    end
    
    @network.no_cursor_timeout.each do |doc|
        # begin
        
            @tracking_type = doc["tracking_type"].to_s
            @ad_redirect = doc["ad_redirect"].to_s
            @keyword_redirect = doc["keyword_redirect"].to_s
            @company_id = doc["company_id"].to_s
            @cookie_length = doc["cookie_length"].to_s
            
            
            service = "AccountService"
            method = "getAccountInfo"
            
            json = {'header' => { 
                            'token' => doc["api_token"].to_s,
                            'username' => doc["username"].to_s,
                            'password' => doc["password"].to_s 
                          },
                     'body'  => {
                            'accountFields' => ["userId","balance","cost","payment","budgetType","budget","regionTarget","excludeIp","openDomains","regDomain","budgetOfflineTime","weeklyBudget","userStat","isDynamicCreative","dynamicCreativeParam","pcBalance","mobileBalance"]
                          }
                    }       
                
            @account_info = baidu_api(service,method,json)
            # @logger.info @account_info
            
            if !@account_info["header"]["desc"].nil? && @account_info["header"]["desc"].to_s == "success"
                @header = @account_info["header"]
                @remain_quote = @header["rquota"]
            end
            
            
            @do = 1
            
            #check if file exist
            if doc['tmp_file'].to_s != ""
                @tmp_file = "/datadrive/"+doc['tmp_file'].to_s+"_ad"
                if !File.directory?(@tmp_file)
                    redownload(doc["id"])
                    @do = 0
                    @logger.info "baidu " + doc['id'].to_s + " need to re download structure"
                    return render :nothing => true
                end
            end
            
            if @do == 1
            
                @logger.info "baidu ad " + doc['id'].to_s + " running"
                
                @unzip_folder = @tmp_file + "/*"
                @files = Dir.glob(@unzip_folder)
                
                @logger.info "baidu ad " + doc["id"].to_s + " updating "
                        
                @db[:network].find(id: doc["id"].to_i).update_one('$set'=> {'file_update_3' => 3})
                @db.close
                
                db_name = "ad_baidu_"+doc['id'].to_s
                @baidu_db[db_name].drop
                @baidu_db.close()
                
                begin
                    @baidu_db[db_name].indexes.create_one(network_id: Mongo::Index::ASCENDING)
                    @baidu_db[db_name].indexes.create_one(campaign_id: Mongo::Index::ASCENDING)
                    @baidu_db[db_name].indexes.create_one(adgroup_id: Mongo::Index::ASCENDING)
                    @baidu_db[db_name].indexes.create_one(ad_id: Mongo::Index::ASCENDING)
                    @baidu_db[db_name].indexes.create_one(title: Mongo::Index::ASCENDING)
                    @baidu_db[db_name].indexes.create_one(description_1: Mongo::Index::ASCENDING)
                    @baidu_db[db_name].indexes.create_one(description_2: Mongo::Index::ASCENDING)
                    # @baidu_db[db_name].indexes.create_one(visit_url: Mongo::Index::ASCENDING)
                    # @baidu_db[db_name].indexes.create_one(show_url: Mongo::Index::ASCENDING)
                    # @baidu_db[db_name].indexes.create_one(mobile_visit_url: Mongo::Index::ASCENDING)
                    # @baidu_db[db_name].indexes.create_one(mobile_show_url: Mongo::Index::ASCENDING)
                    @baidu_db[db_name].indexes.create_one(pause: Mongo::Index::ASCENDING)
                    @baidu_db[db_name].indexes.create_one(status: Mongo::Index::ASCENDING)
                    @baidu_db[db_name].indexes.create_one(temp: Mongo::Index::ASCENDING)
                    @baidu_db[db_name].indexes.create_one(devicePreference: Mongo::Index::ASCENDING)
                    @baidu_db[db_name].indexes.create_one(tabs: Mongo::Index::ASCENDING)
                    # @baidu_db[db_name].indexes.create_one(watchdog: Mongo::Index::ASCENDING)
                    @baidu_db[db_name].indexes.create_one(response_code: Mongo::Index::ASCENDING)
                    @baidu_db[db_name].indexes.create_one(m_response_code: Mongo::Index::ASCENDING)
                    @baidu_db[db_name].indexes.create_one(update_date: Mongo::Index::ASCENDING)
                    @baidu_db[db_name].indexes.create_one(create_date: Mongo::Index::ASCENDING)
                rescue Exception
                end
                
                @files.each_with_index do |file, index|
                    # only this level need the quote, cause some string has weird quote
                    
                    data_arr = []
                    
                    CSV.foreach(file, :encoding => 'GB18030', :quote_char => "\x00").each_with_index do |csv, index|
                        # begin
                            if index.to_i == 0
                                csv_array = csv[0].split("\t")
                                set_csv_header(csv_array)
                            else
                                csv = csv.to_csv
                                csv_array = csv.split("\t")
                                
                                url_tag = 0
                                m_url_tag = 0
                                
                                @final_url = csv_array[@pcDestinationUrl_index].to_s.gsub('""', '').gsub('-', '')
                                @m_final_url = csv_array[@mobileDestinationUrl_index].to_s.gsub('""', '').gsub('-', '')
                                
                                
                                if !@final_url.to_s.include?(".adeqo.") && @tracking_type.to_s.downcase == "adeqo" && @ad_redirect.to_s.downcase == "yes" && @final_url.to_s != ""
                                    
                                    @temp_final_url = @final_url
                                    @final_url = "http://t.adeqo.com/click?company_id="+@company_id.to_s+"&network_id="+doc["id"].to_s
                                    @final_url = @final_url + "&campaign_id="+csv_array[@campaignId_index].gsub('"', '').to_s+"&adgroup_id="+csv_array[@adgroupId_index].to_s+"&ad_id="+csv_array[@creativeId_index].to_s+"&keyword_id=0"
                                    @final_url = @final_url + "&cookie="+@cookie_length.to_s
                                    @final_url = @final_url + "&device=pc"
                                    @final_url = @final_url + "&tv=v1&durl="+CGI.escape(@temp_final_url.to_s)
                                    
                                    url_tag = 1
                                end
                                
                                if !@m_final_url.to_s.include?(".adeqo.") && @tracking_type.to_s.downcase == "adeqo" && @ad_redirect.to_s.downcase == "yes" && @m_final_url.to_s != ""
                                     
                                    @temp_m_final_url = @m_final_url
                                    @m_final_url = "http://t.adeqo.com/click?company_id="+@company_id.to_s+"&network_id="+doc["id"].to_s
                                    @m_final_url = @m_final_url + "&campaign_id="+csv_array[@campaignId_index].gsub('"', '').to_s+"&adgroup_id="+csv_array[@adgroupId_index].to_s+"&ad_id="+csv_array[@creativeId_index].to_s+"&keyword_id=0"
                                    @m_final_url = @m_final_url + "&cookie="+@cookie_length.to_s
                                    @m_final_url = @m_final_url + "&device=mobile"
                                    @m_final_url = @m_final_url + "&tv=v1&durl="+CGI.escape(@temp_m_final_url.to_s)
                                    
                                    m_url_tag = 1
                                end
                                
                                # @logger.info csv_array
                                # @logger.info @final_url
                                # @logger.info "-"
                                # @logger.info @m_final_url
                                  
                                if url_tag == 1 || m_url_tag == 1
                                  
                                    @logger.info "add ad with tag"
                                    # @logger.info @remain_quote
                                  
                                    if @remain_quote.to_i >= 500
                                        requesttypearray = [] 
                                        requesttype = {}
                                        requesttype[:creativeId]    =     csv_array[@creativeId_index].to_i
                                        requesttype[:adgroupId]    =     0
                                        requesttype[:status]    =     0
                                        requesttype[:mobileDestinationUrl] =    @m_final_url
                                        requesttype[:pcDestinationUrl]    =     @final_url
                                        requesttype[:title] = csv_array[@title_index].gsub('""', '"').to_s
                                        requesttype[:description1] = csv_array[@description1_index].to_s
                                        
                                        
                                        requesttypearray << requesttype
                                    
                                        service = "CreativeService"
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
                                            
                                        @update_info = baidu_api(service,method,json)
                                        
                                        @logger.info requesttype
                                        @logger.info @update_info
                                        
                                        @header = @update_info["header"]
                                        @remain_quote = @header["rquota"]
                                            
                                        if !@update_info["header"]["desc"].nil? && @update_info["header"]["desc"].to_s == "success"
                                            
                                        else
                                            @final_url = csv_array[@pcDestinationUrl_index].to_s.gsub('""', '').gsub('-', '')
                                            @m_final_url = csv_array[@mobileDestinationUrl_index].to_s.gsub('""', '').gsub('-', '')
                                        end
                                         
                                    end
                                end
                                
                                # insert_ad(doc["id"],csv_array,@final_url,@m_final_url)
                                
                                
                                data_hash = {}
                                insert_hash = {}
                              
                                insert_hash[:network_id] = doc["id"].to_i
                                insert_hash[:campaign_id] = csv_array[@campaignId_index].gsub('"', '').to_i
                                insert_hash[:adgroup_id] = csv_array[@adgroupId_index].to_i
                                insert_hash[:ad_id] = csv_array[@creativeId_index].to_i
                                insert_hash[:title] = csv_array[@title_index].gsub('""', '"').to_s
                                insert_hash[:description_1] = csv_array[@description1_index].to_s
                                insert_hash[:description_2] = csv_array[@description2_index].to_s
                                insert_hash[:visit_url] = @final_url.to_s
                                insert_hash[:show_url] = csv_array[@pcDisplayUrl_index].to_s
                                insert_hash[:mobile_visit_url] = @m_final_url.to_s
                                insert_hash[:mobile_show_url] = csv_array[@mobileDisplayUrl_index].to_s
                                insert_hash[:pause] = csv_array[@pause_index].to_s
                                insert_hash[:status] = csv_array[@status_index].to_i
                                insert_hash[:temp] = csv_array[@temp_index].to_i
                                insert_hash[:devicePreference] = csv_array[@devicePreference_index].to_i
                                insert_hash[:tabs] = csv_array[@tabs_index].gsub('"', '')
                                insert_hash[:response_code] = ""
                                insert_hash[:m_response_code] = ""
                                insert_hash[:create_date] = @now
                                insert_hash[:update_date] = @now
                                
                                
                                    
                                data_hash[:insert_one] = insert_hash
                                data_arr << data_hash
                              
                                if data_arr.count.to_i > 5000
                                    db_name = "ad_baidu_"+doc["id"].to_s
                                    @baidu_db[db_name].bulk_write(data_arr)
                                    @baidu_db.close()
                                    
                                    data_arr = []
                                end
                                
                                
                                
                                
                            end
                        # rescue Exception
                            # redownload(doc["id"])
                            # return render :nothing => true
                        # end
                    end
                    
                    if data_arr.count.to_i > 0
                        db_name = "ad_baidu_"+doc["id"].to_s
                        @baidu_db[db_name].bulk_write(data_arr)
                        @baidu_db.close()
                        
                        data_arr = []
                    end
                end      
                              
                @db[:network].find(id: doc["id"].to_i).update_one('$set'=> {'file_update_3' => 4, 'last_update' => @now})
                @db.close              
                
                unzip_folder = @tmp+"/"+doc["tmp_file"]+"_ad"
                if File.directory?(unzip_folder)
                    FileUtils.remove_dir unzip_folder, true
                end
                
            end
        # rescue Exception
            # redownload(doc["id"])
            # return render :nothing => true
        # end
    end
    
    @logger.info "baidu ad done"
    return render :nothing => true 
  end
  
  
  
  
  
  def adgroup 
    @logger.info "baidu adgroup start"
    
    @id = params[:id]
    if @id.nil?
      
        
        @current_network = @db[:network].find({ '$or' => [ {'file_update_1' => 3},{'file_update_2' => 3},{'file_update_3' => 3},{'file_update_4' => 3} ],'type' => 'baidu', 'worker' => @port.to_i})
        @db.close
        
        if @current_network.count.to_i >= 3
            @logger.info "working, no need update baidu adgroup"
            return render :nothing => true
        end
      
        
        @network = @db[:network].find({ "$and" => [{:type => 'baidu'},{:file_update_1 => 4},{:file_update_2 => 2},{:worker => @port.to_i}] }).sort({ last_update: -1 }).limit(1)
        @db.close

        if @network.count.to_i == 0
            @logger.info "no need update baidu adgroup"
            return render :nothing => true
        end
    else
      
        @network = @db[:network].find({ "$and" => [{:type => 'baidu'},{:id => @id.to_i}] })
        @db.close
    end
    
    @network.no_cursor_timeout.each do |doc|
        begin
            @do = 1
            
            #check if file exist
            if doc['tmp_file'].to_s != ""
                @tmp_file = "/datadrive/"+doc['tmp_file'].to_s+"_adgroup"
                if !File.directory?(@tmp_file)
                    redownload(doc["id"])
                    @do = 0
                    @logger.info "baidu " + doc['id'].to_s + " need to re download structure"
                    return render :nothing => true
                end
            end
            
            if @do == 1
            
                @logger.info "baidu adgroup " + doc['id'].to_s + " running"
                
                @unzip_folder = @tmp_file + "/*"
                @files = Dir.glob(@unzip_folder)
                
                @logger.info "baidu adgroup " + doc["id"].to_s + " updating "
                        
                @db[:network].find(id: doc["id"].to_i).update_one('$set'=> {'file_update_2' => 3})
                @db.close
                
                db_name = "adgroup_baidu_"+doc['id'].to_s
                @baidu_db[db_name].drop
                @baidu_db.close()
                                
                begin
                    @baidu_db[db_name].indexes.create_one(network_id: Mongo::Index::ASCENDING)
                    @baidu_db[db_name].indexes.create_one(campaign_id: Mongo::Index::ASCENDING)
                    @baidu_db[db_name].indexes.create_one(adgroup_id: Mongo::Index::ASCENDING)
                    @baidu_db[db_name].indexes.create_one(name: Mongo::Index::ASCENDING)
                    @baidu_db[db_name].indexes.create_one(max_price: Mongo::Index::ASCENDING)
                    @baidu_db[db_name].indexes.create_one(negative_words: Mongo::Index::ASCENDING)
                    @baidu_db[db_name].indexes.create_one(exact_negative_words: Mongo::Index::ASCENDING)
                    @baidu_db[db_name].indexes.create_one(pause: Mongo::Index::ASCENDING)
                    @baidu_db[db_name].indexes.create_one(status: Mongo::Index::ASCENDING)
                    @baidu_db[db_name].indexes.create_one(accuPriceFactor: Mongo::Index::ASCENDING)
                    @baidu_db[db_name].indexes.create_one(wordPriceFactor: Mongo::Index::ASCENDING)
                    @baidu_db[db_name].indexes.create_one(widePriceFactor: Mongo::Index::ASCENDING)
                    @baidu_db[db_name].indexes.create_one(matchPriceFactorStatus: Mongo::Index::ASCENDING)
                    @baidu_db[db_name].indexes.create_one(priceRatio: Mongo::Index::ASCENDING)
                    @baidu_db[db_name].indexes.create_one(api_update_ad: Mongo::Index::ASCENDING)
                    @baidu_db[db_name].indexes.create_one(api_update_keyword: Mongo::Index::ASCENDING)
                    @baidu_db[db_name].indexes.create_one(api_worker: Mongo::Index::ASCENDING)
                    @baidu_db[db_name].indexes.create_one(update_date: Mongo::Index::ASCENDING)
                    @baidu_db[db_name].indexes.create_one(create_date: Mongo::Index::ASCENDING)
                rescue Exception
                end
                
                @files.each_with_index do |file, index|
                  
                    data_arr = []
                  
                    CSV.foreach(file, :encoding => 'GB18030', :quote_char => "\x00").each_with_index do |csv, index|
                        begin
                            if index.to_i == 0
                                csv_array = csv[0].split("\t")
                                set_csv_header(csv_array)
                            else
                                csv = csv.to_csv
                                csv_array = csv.split("\t")
                                # insert_adgroup(doc["id"],csv_array)
                                
                                
                                data_hash = {}
                                insert_hash = {}
                              
                                insert_hash[:network_id] = doc["id"].to_i
                                insert_hash[:campaign_id] = csv_array[@campaignId_index].gsub('"', '').to_i
                                insert_hash[:adgroup_id] = csv_array[@adgroupId_index].to_i
                                insert_hash[:name] = csv_array[@adgroupName_index].gsub('""', '"').to_s
                                insert_hash[:max_price] = csv_array[@maxPrice_index].to_f
                                insert_hash[:negative_words] = csv_array[@negativeWords_index].to_s
                                insert_hash[:exact_negative_words] = csv_array[@exactNegativeWords_index].to_s
                                insert_hash[:pause] = csv_array[@pause_index].to_s
                                insert_hash[:status] = csv_array[@status_index].to_i
                                insert_hash[:accuPriceFactor] = csv_array[@accuPriceFactor_index].to_f
                                insert_hash[:wordPriceFactor] = csv_array[@wordPriceFactor_index].to_f
                                insert_hash[:widePriceFactor] = csv_array[@widePriceFactor_index].to_f
                                insert_hash[:matchPriceFactorStatus] = csv_array[@matchPriceFactorStatus_index].to_i
                                insert_hash[:priceRatio] = csv_array[@priceRatio_index].gsub('"', '').to_s
                                
                                insert_hash[:api_update_ad] = 0
                                insert_hash[:api_update_keyword] = 0
                                insert_hash[:api_worker] = ""
                                
                                insert_hash[:create_date] = @now
                                insert_hash[:update_date] = @now
                                
                                
                                    
                                data_hash[:insert_one] = insert_hash
                                data_arr << data_hash
                              
                                if data_arr.count.to_i > 5000
                                    db_name = "adgroup_baidu_"+doc["id"].to_s
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
                        db_name = "adgroup_baidu_"+doc["id"].to_s
                        @baidu_db[db_name].bulk_write(data_arr)
                        @baidu_db.close()
                        
                        data_arr = []
                    end
                end      
                              
                @db[:network].find(id: doc["id"].to_i).update_one('$set'=> {'file_update_2' => 4, 'last_update' => @now})
                @db.close              
                
                unzip_folder = @tmp+"/"+doc["tmp_file"]+"_adgroup"
                if File.directory?(unzip_folder)
                    FileUtils.remove_dir unzip_folder, true
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
    @logger.info "baidu campaign start"
    
    @id = params[:id]
    if @id.nil?
        
        @current_network = @db[:network].find({ '$or' => [ {'file_update_1' => 3},{'file_update_2' => 3},{'file_update_3' => 3},{'file_update_4' => 3} ],'type' => 'baidu', 'worker' => @port.to_i})
        @db.close
        
        if @current_network.count.to_i >= 3
            @logger.info "working, no need update baidu campaign"
            return render :nothing => true
        end
        
        
      
        @network = @db[:network].find({ "$and" => [{:type => 'baidu'},{:file_update_1 => 2},{:worker => @port.to_i}] }).sort({ last_update: -1 }).limit(1)
        @db.close

        if @network.count.to_i == 0
            @logger.info "no need update baidu campaign"
            return render :nothing => true
        end
    else
        @network = @db[:network].find({ "$and" => [{:type => 'baidu'},{:id => @id.to_i}] })
        @db.close
    end
    
    @network.no_cursor_timeout.each do |doc|
        begin
            @do = 1
            
            #check if file exist
            if doc['tmp_file'].to_s != ""
                @tmp_file = "/datadrive/"+doc['tmp_file'].to_s+"_campaign"
                if !File.directory?(@tmp_file)
                    redownload(doc["id"])
                    @do = 0
                    @logger.info "baidu " + doc['id'].to_s + " need to re download structure"
                    return render :nothing => true
                end
            end
            
            if @do == 1
            
                @logger.info "baidu campaign " + doc['id'].to_s + " running"
                
                @unzip_folder = @tmp_file + "/*"
                @files = Dir.glob(@unzip_folder)
                
                @logger.info "baidu campaign " + doc["id"].to_s + " updating "
                        
                @db[:network].find(id: doc["id"].to_i).update_one('$set'=> {'file_update_1' => 3})
                @db.close
                @db["all_campaign"].find(network_id: doc["id"].to_i).delete_many
                @db.close
                
                @files.each_with_index do |file, file_index|
                    
                    # @logger.info file
                    data_arr = []
                    CSV.foreach(file, :encoding => 'GB18030', :quote_char => "\x00").each_with_index do |csv, index|
                      
                        # @logger.info csv
                      
                        begin
                            if index.to_i == 0
                                csv_array = csv[0].split("\t")
                                set_csv_header(csv_array)
                            else
                                csv = csv.to_csv
                                csv_array = csv.split("\t")
                                # insert_campaign(doc["id"],doc["name"],csv_array)
                              
                              
                                data_hash = {}
                                insert_hash = {}
                              
                                insert_hash[:network_id] = doc["id"].to_i
                                insert_hash[:network_type] = "baidu"
                                insert_hash[:account_name] = doc["name"].to_s
                                insert_hash[:campaign_id] = csv_array[@campaignId_index].gsub('"', '').to_i
                                insert_hash[:campaign_name] = csv_array[@campaignName_index].gsub('""', '"').to_s
                                insert_hash[:budget] = csv_array[@budget_index].to_f
                                insert_hash[:regions] = csv_array[@regionTarget_index].to_s
                                insert_hash[:exclude_ips] = csv_array[@excludeIp_index].to_s
                                insert_hash[:negative_words] = csv_array[@negativeWords_index]
                                insert_hash[:exact_negative_words] = csv_array[@exactNegativeWords_index]
                                insert_hash[:schedule] = csv_array[@schedule_index]
                                insert_hash[:budget_offline_time] = csv_array[@budgetOfflineTime_index]
                                insert_hash[:show_prob] = csv_array[@showProb_index].to_i
                                insert_hash[:pause] = csv_array[@pause_index].to_s
                                insert_hash[:device] = csv_array[@device_index].to_i
                                insert_hash[:priceRatio] = csv_array[@priceRatio_index].to_f
                                insert_hash[:status] = csv_array[@status_index].to_i
                                insert_hash[:isDynamicCreative] = csv_array[@isDynamicCreative_index].to_s
                                insert_hash[:dynCreativeExclusion] = csv_array[@dynCreativeExclusion_index].to_s
                                insert_hash[:campaignType] = csv_array[@campaignType_index].to_i
                                insert_hash[:isDynamicTagSublink] = csv_array[@isDynamicTagSublink_index].to_s
                                insert_hash[:isDynamicTitle] = csv_array[@isDynamicTitle_index].to_s
                                insert_hash[:isDynamicHotRedirect] = csv_array[@isDynamicHotRedirect_index].to_s
                                insert_hash[:rmktStatus] = csv_array[@rmktStatus_index].to_s
                                insert_hash[:rmktPriceRatio] = csv_array[@rmktPriceRatio_index].gsub('"', '').to_f
                                
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
                              
                @db[:network].find(id: doc["id"].to_i).update_one('$set'=> {'file_update_1' => 4, 'last_update' => @now})
                @db.close              
                
                unzip_folder = @tmp+"/"+doc["tmp_file"]+"_campaign"
                if File.directory?(unzip_folder)
                    FileUtils.remove_dir unzip_folder, true
                end
                
            end
        rescue Exception
            redownload(doc["id"])
            return render :nothing => true
        end
    end
    
    @logger.info "baidu campaign done"
    return render :nothing => true 
  end
  
  
  def redownload(networkid)
    
      
    
      @redownload_network = @db[:network].find({ "$and" => [{:type => 'baidu'},{:id => networkid.to_i}] })
      @db.close
    
      if @redownload_network.count.to_i == 1
          
          @redownload_network.no_cursor_timeout.each do |doc|
              if doc["tmp_file"] != ""
                  unzip_folder = @tmp+"/"+doc["tmp_file"]+"_account"
                  if File.directory?(unzip_folder)
                      FileUtils.remove_dir unzip_folder, true
                  end
                  
                  unzip_folder = @tmp+"/"+doc["tmp_file"]+"_campaign"
                  if File.directory?(unzip_folder)
                      FileUtils.remove_dir unzip_folder, true
                  end
                  
                  unzip_folder = @tmp+"/"+doc["tmp_file"]+"_adgroup"
                  if File.directory?(unzip_folder)
                      FileUtils.remove_dir unzip_folder, true
                  end
                  
                  unzip_folder = @tmp+"/"+doc["tmp_file"]+"_ad"
                  if File.directory?(unzip_folder)
                      FileUtils.remove_dir unzip_folder, true
                  end
                  
                  unzip_folder = @tmp+"/"+doc["tmp_file"]+"_keyword"
                  if File.directory?(unzip_folder)
                      FileUtils.remove_dir unzip_folder, true
                  end
              end
          end
          
          @db[:network].find(id: networkid.to_i).update_one('$set'=> { 'tmp_file' => "" , 'fileid' => "",'run_time' => 0, 'file_update_1' => 0, 'file_update_2' => 0, 'file_update_3' => 0, 'file_update_4' => 0, 'worker' => "", 'last_update' => @now})  
          @db.close
      end
  end
  
  
  def dlaccfile
    
      @logger.info "baidu dlaccfile start"
      
      
      @all_network = @db[:network].find()
      @db.close
      
      @dl_limit = @all_network.count.to_i / 4  
      
      @all_work_network = @db[:network].find('worker' => @port.to_i)
      @db.close
      
      if @all_work_network.count.to_i >= @dl_limit.to_i
          @logger.info "baidu dlaccfile limit"
          return render :nothing => true
      end
    
    
      @id = params[:id]
      if @id.nil?
          # @network = @db[:network].find('type' => 'sogou', 'file_update_1' => {'$lt' => 2}, 'file_update_2' => {'$lt' => 2}, 'file_update_3' => {'$lt' => 2}, 'file_update_4' => {'$lt' => 2})
          
          
          
          @current_network = @db[:network].find({ "$and" => [{:type => 'baidu'},{:file_update_1 => 1},{:file_update_2 => 1},{:file_update_3 => 1},{:file_update_4 => 1},{:worker => @port.to_i}] }).sort({ last_update: -1 }).limit(1)
          @db.close
          
          if @current_network.count.to_i >= 1
              @logger.info "baidu dl working"
              return render :nothing => true
          end
              
          @network = @db[:network].find({ "$and" => [{:type => 'baidu'},{:file_update_1 => 0},{:file_update_2 => 0},{:file_update_3 => 0},{:file_update_4 => 0},{:worker => @port.to_i}] }).sort({ last_update: -1 }).limit(1)
          @db.close
          
          if @network.count.to_i == 0
            
              @network = @db[:network].find({ "$and" => [{:type => 'baidu'},{:file_update_1 => 0},{:file_update_2 => 0},{:file_update_3 => 0},{:file_update_4 => 0},{:worker => ""}] }).sort({ last_update: -1 }).limit(1)
              @db.close
            
              if @network.count.to_i == 0
                  @logger.info "no need to dl baidu"
                  return render :nothing => true
              end
          end
          
      else
          
          @network = @db[:network].find({ "$and" => [{:type => 'baidu'},{:id => @id.to_i}] })
          @db.close
      end
      
      @network.no_cursor_timeout.each do |doc|
              
              begin
                  @logger.info "baidu dlaccfile " + doc['id'].to_s + " running"
                  @db[:network].find(id: doc["id"].to_i).update_one('$set'=> {'file_update_1' => 1, 'file_update_2' => 1, 'file_update_3' => 1, 'file_update_4' => 1, 'worker' => @port.to_i, 'last_update' => @now})
                  @db.close
                  
                  if doc["run_time"].to_i >= 10
                      @db[:network].find(id: doc["id"].to_i).update_one('$set'=> { 'fileid' => "", 'tmp_file' => "",'run_time' => 0,'file_update_1' => 0, 'file_update_2' => 0, 'file_update_3' => 0, 'file_update_4' => 0, 'worker' => "", 'last_update' => @now })
                      @db.close
                  else
                      
                      if doc['tmp_file'].to_s == "" && doc['fileid'].to_s == ""
                          service = "BulkJobService"
                          method = "getAllObjects"
                           
                          json = {'header' => { 
                                                  'token' => doc['api_token'].to_s,
                                                  'username' => doc['username'].to_s,
                                                  'password' => doc['password'].to_s 
                                              },
                                   'body'  => {
                                                  "campaignIds" => [],
                                                  "accountFields" => ["all"],
                                                  "campaignFields" => ["all"],
                                                  "adgroupFields" => ["all"],
                                                  "keywordFields" => ["all"],
                                                  "creativeFields" => ["all"],
                                                  "format" => 0
                                              }
                                  }
                                  
                          @result = baidu_api(service,method,json)
                          @logger.info @result
                          
                          @header = @result["header"]
                          @quota = @header["rquota"]
                          
                          # @logger.info @result
                          # @logger.info @header
                          
                          if @header["desc"].downcase == "success"
                              @db[:network].find(id: doc["id"].to_i).update_one('$set'=> { 'file_update_1' => 0, 'file_update_2' => 0, 'file_update_3' => 0, 'file_update_4' => 0, 'fileid' => @result["body"]["data"][0]["fileId"].to_s, 'last_update' => @now })
                              @db.close
                          else
                              @db[:network].find(id: doc["id"].to_i).update_one('$set'=> { 'file_update_1' => 0, 'file_update_2' => 0, 'file_update_3' => 0, 'file_update_4' => 0, 'run_time' => 0, 'fileid' => "", 'tmp_file' => "", 'worker' => "", 'last_update' => @now })
                              @db.close
                          end
                          
                      
                      elsif doc['tmp_file'].to_s != ""
                        
                          @db[:network].find(id: doc["id"].to_i).update_one('$set'=> { 'file_update_1' => 2, 'file_update_2' => 2, 'file_update_3' => 2, 'file_update_4' => 2, 'worker' => @port.to_i })
                          @db.close
                          
                      elsif doc['tmp_file'].to_s == "" && doc['fileid'].to_s != ""
                          
                          service = "BulkJobService"
                          method = "getFileStatus"
                          
                          json = {'header' => { 
                                                  'token' => doc['api_token'].to_s,
                                                  'username' => doc['username'].to_s,
                                                  'password' => doc['password'].to_s
                                              },
                                   'body'  => {
                                                  "fileId" => doc['fileid'].to_s
                                              }
                                  }
                                  
                          @result = baidu_api(service,method,json)
                          @logger.info @result
                          
                          @header = @result["header"]
                          @quota = @header["rquota"]
                          
                          if @header["desc"].downcase == "success"
                              if @result["body"]["data"][0]["isGenerated"].to_i == 3
                                  service = "BulkJobService"
                                  method = "getFilePath"
                                  
                                  json = {'header' => { 
                                                          'token' => doc['api_token'].to_s,
                                                          'username' => doc['username'].to_s,
                                                          'password' => doc['password'].to_s
                                                      },
                                           'body'  => {
                                                          "fileId" => doc['fileid'].to_s
                                                      }
                                          }
                                          
                                  @result = baidu_api(service,method,json)
                                  @logger.info @result
                                  
                                      
                                  if !@result["body"]["data"][0]["campaignFilePath"].nil? && !@result["body"]["data"][0]["keywordFilePath"].nil? && !@result["body"]["data"][0]["adgroupFilePath"].nil? && !@result["body"]["data"][0]["accountFilePath"].nil? && !@result["body"]["data"][0]["creativeFilePath"].nil?
                                  
                                      @unzip_account_name = @tmp+"/"+doc['fileid'].to_s+"_account"
                                      @unzip_campaign_name = @tmp+"/"+doc['fileid'].to_s+"_campaign"
                                      @unzip_adgroup_name = @tmp+"/"+doc['fileid'].to_s+"_adgroup"
                                      @unzip_ad_name = @tmp+"/"+doc['fileid'].to_s+"_ad"
                                      @unzip_keyword_name = @tmp+"/"+doc['fileid'].to_s+"_keyword"
                                  
                                      @logger.info "read acccount file"
                                      @account_zip_file = @tmp+"/"+doc['fileid'].to_s + "_account.zip"
                                      open(@account_zip_file.to_s, 'wb') do |file|
                                        file << open(@result["body"]["data"][0]["accountFilePath"].to_s).read
                                      end
#                                       
                                      @logger.info "read campaign file"
                                      @campaign_zip_file = @tmp+"/"+doc['fileid'].to_s + "_campaign.zip"
                                      open(@campaign_zip_file.to_s, 'wb') do |file|
                                        file << open(@result["body"]["data"][0]["campaignFilePath"].to_s).read
                                      end
#                                       
                                      @logger.info "read adgroup file"
                                      @adgroup_zip_file = @tmp+"/"+doc['fileid'].to_s + "_adgroup.zip"
                                      open(@adgroup_zip_file.to_s, 'wb') do |file|
                                        file << open(@result["body"]["data"][0]["adgroupFilePath"].to_s).read
                                      end
#                                       
                                      @logger.info "read ad file"
                                      @ad_zip_file = @tmp+"/"+doc['fileid'].to_s + "_ad.zip"
                                      open(@ad_zip_file.to_s, 'wb') do |file|
                                        file << open(@result["body"]["data"][0]["creativeFilePath"].to_s).read
                                      end
                                      
                                      @logger.info "read keyword file"
                                      @keyword_zip_file = @tmp+"/"+doc['fileid'].to_s + "_keyword.zip"
                                      open(@keyword_zip_file.to_s, 'wb') do |file|
                                        file << open(@result["body"]["data"][0]["keywordFilePath"].to_s, :read_timeout => 1200).read
                                      end
                                      
                                      @logger.info "download file done"
                                      
#                                       
                                      
                                      unzip_file(@account_zip_file.to_s, @unzip_account_name.to_s)
                                      unzip_file(@campaign_zip_file.to_s, @unzip_campaign_name.to_s)
                                      unzip_file(@adgroup_zip_file.to_s, @unzip_adgroup_name.to_s)
                                      unzip_file(@ad_zip_file.to_s, @unzip_ad_name.to_s)
                                      unzip_file(@keyword_zip_file.to_s, @unzip_keyword_name.to_s)
                                      
                                      @logger.info "delete zip file start"
                                      File.delete(@account_zip_file)
                                      File.delete(@campaign_zip_file) 
                                      File.delete(@adgroup_zip_file) 
                                      File.delete(@ad_zip_file) 
                                      File.delete(@keyword_zip_file)  
                                      
                                      @unzip_folder = @unzip_account_name + "/*"
                                      @files = Dir.glob(@unzip_folder)
                                      
                                      @logger.info "baidu dlacc almost done, update account"
                                      @files.each_with_index do |file, index|
                                          CSV.foreach(file, :encoding => 'GB18030').each_with_index do |csv, index|
                                            
                                              if index.to_i == 0
                                                  csv_array = csv[0].split("\t")
                                                  set_csv_header(csv_array)
                                              
                                              else
                                                    csv = csv.to_csv
                                                    csv_array = csv.split("\t") 
                                                    
                                                    opendomains_arr = csv_array[@openDomains_index].split("||");
                                                    regionTarget_arr = csv_array[@regionTarget_index].split("||");
                                                    budgetofflineTime_arr = csv_array[@budgetOfflineTime_index].split("||");
                                                    budgetofflineTime_hash_arr = []
                                                    
                                                    budgetofflineTime_arr.each_with_index do |budgetofflineTime_arr_d, index|
                                                      
                                                        budgetofflineTime_arr_d_arr = budgetofflineTime_arr_d.split("**");
                                                        budgetofflineTime_hash = {}
                                                        budgetofflineTime_hash["time"] = budgetofflineTime_arr_d_arr[0]
                                                        budgetofflineTime_hash["flag"] = budgetofflineTime_arr_d_arr[1]
                                                        
                                                        budgetofflineTime_hash_arr << budgetofflineTime_hash
                                                    end
                                                      
                                                    
                                                    
                                                    @db[:network].find(id: doc["id"].to_i).update_one('$set'=> {  
                                                                        'balance' => csv_array[@balance_index].to_f,
                                                                        'pcbalance' => csv_array[@pcBalance_index].to_f,
                                                                        'mobilebalance' => csv_array[@mobileBalance_index].to_f,
                                                                        # 'budget' => csv_array[@budget_index].to_f,
                                                                        'cost' => csv_array[@cost_index].to_f,
                                                                        'payment' => csv_array[@payment_index].to_f,
                                                                        'opendomains' => opendomains_arr,
                                                                        'regdomain' => csv_array[@regDomain_index].to_s,
                                                                        'budgettype' => csv_array[@budgetType_index].to_i,
                                                                        'regiontarget' => regionTarget_arr,
                                                                        'budgetofflineTime' => budgetofflineTime_hash_arr,
                                                                        'isDynamiccreative' => csv_array[@isDynamicCreative_index].to_s,
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
                                                    
                                                    unzip_folder = @unzip_account_name
                                                    if File.directory?(unzip_folder)
                                                        FileUtils.remove_dir unzip_folder, true
                                                    end
                                              end
                                          end
                                      end
                                      
                                  else
                                      @logger.info "baidu dlacc fail, reset"
                                      @logger.info @result
                                      
                                      run_time = doc["run_time"].to_i + 1
                                      @db[:network].find(id: doc["id"].to_i).update_one('$set'=> { 'tmp_file' => "" , 'fileid' => doc['fileid'].to_s, 'run_time' => run_time.to_i, 'file_update_1' => 0, 'file_update_2' => 0, 'file_update_3' => 0, 'file_update_4' => 0, 'worker' => "", 'last_update' => @now})
                                      @db.close
                                  end
                                  
                              else
                                  @logger.info "baidu dlacc not ready, retry later"
                                
                                  @db[:network].find(id: doc["id"].to_i).update_one('$set'=> { 'file_update_1' => 0, 'file_update_2' => 0, 'file_update_3' => 0, 'file_update_4' => 0, 'worker' => '', 'last_update' => @now })
                                  @db.close
                              end
                              
                          else
                              @logger.info "baidu dlacc fail, reset"
                              @logger.info @result
                              
                              run_time = doc["run_time"].to_i + 1
                              @db[:network].find(id: doc["id"].to_i).update_one('$set'=> { 'tmp_file' => "" , 'fileid' => "", 'run_time' => run_time.to_i, 'file_update_1' => 0, 'file_update_2' => 0, 'file_update_3' => 0, 'file_update_4' => 0, 'worker' => "", 'last_update' => @now})
                              @db.close
                          end
                      end
                      
                  end
              
              rescue Exception
                
                  @logger.info "baidu dlacc fail, reset"
                  
                  run_time = doc["run_time"].to_i + 1
                  @db[:network].find(id: doc["id"].to_i).update_one('$set'=> { 'tmp_file' => "" , 'fileid' => doc['fileid'].to_s, 'run_time' => run_time.to_i, 'file_update_1' => 0, 'file_update_2' => 0, 'file_update_3' => 0, 'file_update_4' => 0, 'worker' => "", 'last_update' => @now})
                  @db.close
              end
          
      end
    
      @logger.info "baidu dlaccfile done"
      return render :nothing => true 
    
                          
  end


  def resetnetwork
    @logger.info "start reset baidu network api status"
    
    @id = params[:id]
    if @id.nil?
        @network = @db[:network].find({ "$and" => [{:type => 'baidu'},{:file_update_1 => 4},{:file_update_2 => 4},{:file_update_3 => 4},{:file_update_4 => 4}] })
        # @network = @db[:network].find('type' => 'sogou', 'file_update_1' => {'$gte' => 2}, 'file_update_2' => {'$gte' => 2}, 'file_update_3' => {'$gte' => 2}, 'file_update_4' => {'$gte' => 2})
        @db.close
    else
        @network = @db[:network].find('type' => 'baidu')
        @db.close
    end
    
    @network.no_cursor_timeout.each do |doc|
          @db[:network].find(id: doc["id"].to_i).update_one('$set'=> { 'file_update_1' => 2, 'file_update_2' => 2, 'file_update_3' => 2, 'file_update_4' => 2, 'worker' => "" })
          @db.close
          @logger.info "done reset baidu network api status " +doc['id'].to_s
    end
    
    @logger.info "done reset baidu api network api status"
    return render :nothing => true
  end  
    
    
    
  def resetdlfile
    @logger.info "start reset baidu api download file"
    
    @id = params[:id]
    if @id.nil?
        @network = @db[:network].find({ "$and" => [{:type => 'baidu'},{:file_update_1 => { '$gte' => 4 }},{:file_update_2 => { '$gte' => 4 }},{:file_update_3 => { '$gte' => 4 }},{:file_update_4 => { '$gte' => 4 }}] })
        # @network = @db[:network].find('type' => 'sogou')
        @db.close
    else
        @network = @db[:network].find('type' => 'baidu', 'id' => @id.to_i)
        @db.close
    end
    
    @network.no_cursor_timeout.each do |doc|
          if doc["tmp_file"] != ""
              
              unzip_folder = @tmp+"/"+doc["tmp_file"]+"_account"
              if File.directory?(unzip_folder)
                  FileUtils.remove_dir unzip_folder, true
              end
              
              unzip_folder = @tmp+"/"+doc["tmp_file"]+"_campaign"
              if File.directory?(unzip_folder)
                  FileUtils.remove_dir unzip_folder, true
              end
              
              unzip_folder = @tmp+"/"+doc["tmp_file"]+"_adgroup"
              if File.directory?(unzip_folder)
                  FileUtils.remove_dir unzip_folder, true
              end
              
              unzip_folder = @tmp+"/"+doc["tmp_file"]+"_ad"
              if File.directory?(unzip_folder)
                  FileUtils.remove_dir unzip_folder, true
              end
              
              unzip_folder = @tmp+"/"+doc["tmp_file"]+"_keyword"
              if File.directory?(unzip_folder)
                  FileUtils.remove_dir unzip_folder, true
              end
              
          end
          @logger.info "done reset baidu api download file " +doc['id'].to_s
          
          @db[:network].find(id: doc["id"].to_i).update_one('$set'=> { 'fileid' => "", 'tmp_file' => "", 'run_time' => 0, 'file_update_1' => 0, 'file_update_2' => 0, 'file_update_3' => 0, 'file_update_4' => 0, 'worker' => "" })
          @db.close
    end
    
    @logger.info "done reset baidu api download file"
    return render :nothing => true
  end
  


  def avgposition
    
      @logger.info "called baidu avg position upper"
        
      @days = params[:day]
      @default_day = 1
      
      if !@days.nil?
        @default_day = @days  
      end
      
      @id = params[:id]
      
      if @id.nil?
        
          if @days.nil?
              @current_network = @db[:network].find({ "$and" => [{:type => 'baidu'}, {:avg_pos => 1}, {:avg_worker => @port.to_i}] })
              @db.close
              
              if @current_network.count.to_i >= 1
                  @logger.info "one baidu avg pos working"
                  return render :nothing => true
              end
              
              
              @network = @db[:network].find({ "$and" => [{:type => 'baidu'}, {:report => 2}, {:avg_pos => 0}, {:avg_worker => @port.to_i}] }).sort({ last_update: -1 }).limit(1)
              @db.close
              
              if @network.count.to_i == 0
                  @network = @db[:network].find({ "$and" => [{:type => 'baidu'}, {:report => 2}, {:avg_pos => 0}, {:avg_worker => ""}] }).sort({ last_update: -1 }).limit(1)
                  @db.close
              end
              
              
          else
              @network = @db[:network].find('type' => 'baidu')
              @db.close  
          end
          
      else
          @network = @db[:network].find({ "$and" => [{:type => 'baidu'}, {:id => @id.to_i}] })
          @db.close
      end
      
    
      @today = Date.today.in_time_zone('Beijing') 
      edit_day = @today - @default_day.to_i.days
      @today = edit_day.strftime("%Y-%m-%d")
      
      
      @network.no_cursor_timeout.each do |doc|
        
             begin
             @logger.info "baidu avg position network " + doc['id'].to_s + " running"
             
             
             if @id.nil? && @days.nil?
                 @db[:network].find('id' => doc["id"].to_i).update_one('$set'=> { 'avg_pos' => 1,'last_update' => @now, 'avg_worker' => @port.to_i })
                 @db.close
             end
            
            
             @logger.info "baidu avg position network " + doc['id'].to_s + " update adgroup"
             
             db_name = "adgroup_baidu_"+doc['id'].to_s
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
                @keyword_report = @db3[:baidu_report_keyword].find('cpc_grp_id' => { "$in" => temp_adgroup_id_arr}, "report_date" => @today.to_s)
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
                    if temp_adgroup_id_hash["display"+adgroup["adgroup_id"].to_s].to_i > 0 && temp_adgroup_id_hash["avg_pos"+adgroup["adgroup_id"].to_s].to_f > 0
                    
                        insert_avg_value = temp_adgroup_id_hash["avg_pos"+adgroup["adgroup_id"].to_s].to_f / temp_adgroup_id_hash["display"+adgroup["adgroup_id"].to_s].to_f
                        
                        @db3[:baidu_report_adgroup].find({ "$and" => [{:cpc_grp_id => adgroup["adgroup_id"].to_i}, {:report_date => @today.to_s}] }).update_one('$set'=>{     
                                                                                                                                                            avg_position: insert_avg_value.to_f
                                                                                                                                                  })
                        @db3.close()
                    
                    end
                end
             end
#              
             @logger.info "baidu avg position network " + doc['id'].to_s + " update adgroup done"
             @logger.info "baidu avg position network " + doc['id'].to_s + " update campaign"
             
             @campaign = @db["all_campaign"].find('network_id' => doc['id'].to_i, 'network_type' => "baidu")
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
             
             @keyword_report = @db3[:baidu_report_keyword].find('cpc_plan_id' => { "$in" => temp_campaign_id_arr}, "report_date" => @today.to_s)
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
                  
                  
                    if temp_campaign_id_hash["display"+campaign["campaign_id"].to_s].to_i > 0 && temp_campaign_id_hash["avg_pos"+campaign["campaign_id"].to_s].to_f > 0
                    
                        insert_avg_value = temp_campaign_id_hash["avg_pos"+campaign["campaign_id"].to_s].to_f / temp_campaign_id_hash["display"+campaign["campaign_id"].to_s].to_f
                        
                        @db3[:baidu_report_campaign].find({ "$and" => [{:cpc_plan_id => campaign["campaign_id"].to_i}, {:report_date => @today.to_s}] }).update_one('$set'=>{     
                                                                                                                                                            avg_position: insert_avg_value.to_f
                                                                                                                                                  })
                        
                        @db3.close()
                    
                    end
                end
             end             
             @logger.info "campaign avg position network " + doc['id'].to_s + " update campaign done"
             @logger.info "campaign avg position network " + doc['id'].to_s + " update account"
             
             
             @keyword_report = @db3[:baidu_report_keyword].find('network_id' => doc['id'].to_i, "report_date" => @today.to_s)
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
                
                @db3[:baidu_report_account].find({ "$and" => [{:network_id => doc['id'].to_i}, {:report_date => @today.to_s}] }).update_one('$set'=>{     
                                                                                                                                          avg_position: insert_avg_value.to_f
                                                                                                                                })                                                                                                                   
                @db3.close()
             end
             
             @logger.info "baidu avg position network " + doc['id'].to_s + " update account done"
             
             
             if @id.nil? && @days.nil?
                 @db[:network].find('id' => doc["id"].to_i).update_one('$set'=> { 'avg_pos' => 2,'last_update' => @now, 'avg_worker' => "" })
                 @db.close
             end
             
             rescue Exception
                @logger.info "baidu avg position network " + doc['id'].to_s + " fail"
                
                @db[:network].find('id' => doc["id"].to_i).update_one('$set'=> { 'avg_upper' => 0,'last_update' => @now, 'avg_worker' => "" })
                @db.close
             end
      end   
      
      @logger.info "called baidu avg position done"
      return render :nothing => true
    
  end


  def report
    
      @logger.info "called report baidu"
      
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
              @current_network = @db[:network].find({ "$and" => [{:type => 'baidu'},{:report => 1},{:report_worker => @port.to_i}] })
              @db.close
              
              if @current_network.count.to_i >= 1
                  @logger.info "one baidu report working"
                  return render :nothing => true
              end
              
              @network = @db[:network].find({ "$and" => [{:type => 'baidu'},{:report => 0},{:report_worker => @port.to_i}] }).sort({ last_update: -1 }).limit(1)
              @db.close
              
              if @network.count.to_i == 0
                  @network = @db[:network].find({ "$and" => [{:type => 'baidu'},{:report => 0},{:report_worker => ""}] }).sort({ last_update: -1 }).limit(1)
                  @db.close  
              end
          else
              @network = @db[:network].find('type' => 'baidu')
              @db.close  
          end
          
      else
          @network = @db[:network].find({ "$and" => [{:type => 'baidu'},{:id => @id.to_i}] })
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
                  @logger.info "get report baidu network "+network_d["id"].to_s
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
                        
      data = {:message => "baidu report done", :status => "true"}
      return render :json => data, :status => :ok
    
  end
  
  def resetreport
    
      @logger.info "baidu reset report"
    
      @db[:network].find('type' => 'baidu').update_many('$set'=> { 'report' => 0,'last_update' => @now, 'report_worker' => "" })
      @db.close
      
      
      @network = @db["network"].find('type' => "baidu")
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
            
            @network = @db["network"].find('id' => { "$in" => arr_d}).update_many('$set'=> { 'report' => 0, 'avg_pos' => 0,'last_update' => @now, 'report_worker' => port_array[index].to_i, 'avg_worker' => port_array[index].to_i })
            @db.close
            
          end
      end
      
      
      @logger.info "baidu reset report done"
      return render :nothing => true 
  end
  
  def resetnetworkreport(networkid)
    
      @logger.info "reset report baidu network "+networkid.to_s+" start"
    
      @db[:network].find(id: networkid.to_i).update_one('$set'=> {'report' => 0,'report_account' => "",'report_campaign' => "",'report_adgroup' => "",'report_ad' => "",'report_keyword' => "",'last_update' => @now, 'report_worker' => ""})
      @db.close
                  
      @file = @tmp+"/baidu_account_report_"+networkid.to_s+".csv"
      unzip_folder = @file
      if File.exists?(unzip_folder)
          File.delete(unzip_folder)
      end
      
      @file = @tmp+"/baidu_campaign_report_"+networkid.to_s+".csv"
      unzip_folder = @file
      if File.exists?(unzip_folder)
          File.delete(unzip_folder)
      end
      
      @file = @tmp+"/baidu_adgroup_report_"+networkid.to_s+".csv"
      unzip_folder = @file
      if File.exists?(unzip_folder)
          File.delete(unzip_folder)
      end
      
      @file = @tmp+"/baidu_ad_report_"+networkid.to_s+".csv"
      unzip_folder = @file
      if File.exists?(unzip_folder)
          File.delete(unzip_folder)
      end
      
      @file = @tmp+"/baidu_keyword_report_"+networkid.to_s+".csv"
      unzip_folder = @file
      if File.exists?(unzip_folder)
          File.delete(unzip_folder)
      end
      
      @logger.info "reset report baidu network "+networkid.to_s+" done"
  end

  def baidu_api(service,method,json)
      # json = {'header' => { 
                              # 'token' => '954b904a7a3e4721954d1fed3183f098',
                              # 'username' => 'baidu-Guam2161498',
                              # 'password' => 'Dfshk2016' 
                          # },
               # 'body'  => {
                              # 'accountFields' => ["userId","balance","cost","payment","budgetType","budget","regionTarget","excludeIp","openDomains","regDomain","budgetOfflineTime","weeklyBudget","userStat","isDynamicCreative","dynamicCreativeParam","pcBalance","mobileBalance"]
                          # }
              # }                                  
              
      url = "https://api.baidu.com/json/sms/service/#{service}/#{method}"
      
      response = HTTParty.post(url, 
                            :body => json.to_json,
                            :headers => { 'Content-Type' => 'application/json', 'Accept' => 'application/json'} 
                            )
      
      
        
      @response = response                
      return response.parsed_response
  end


  def updateaccount
      @network = @db[:network].find('type' => 'baidu')
      @db.close
  
      if @network.count.to_i > 0 
      
          @network.no_cursor_timeout.each do |network_d|
              @apitoken = network_d["api_token"]
              @username = network_d["username"]
              @password = network_d["password"]
              
              
              service = "AccountService"
              method = "getAccountInfo"
              
              json = {'header' => { 
                                      'token' => @apitoken.to_s,
                                      'username' => @username.to_s,
                                      'password' => @password.to_s 
                                  },
                       'body'  => {
                                      'accountFields' => ["userId","balance","cost","payment","budgetType","budget","regionTarget","excludeIp","openDomains","regDomain","budgetOfflineTime","weeklyBudget","userStat","isDynamicCreative","dynamicCreativeParam","pcBalance","mobileBalance"]
                                  }
                      }
                      
                      
              @account_info = baidu_api(service,method,json)
              
              
              if !@account_info["header"]["desc"].nil?
            
                 @header = @account_info["header"]
                 @quota = @header["rquota"]
                 
                 @data = @account_info["body"]["data"][0]  
                 
                 # data = {:message => "baidu index", :datas => @data, :id => network_d['id'], :status => "true"}
                 # return render :json => data, :status => :ok 
                 
                 @db[:network].find(id: network_d["id"].to_i).update_one('$set'=> {  
                                        'balance' => @data["balance"].to_f,
                                        'pcbalance' => @data["pcBalance"].to_f,
                                        'mobilebalance' => @data["mobileBalance"].to_f,
                                        # 'budget' => csv_array[@budget_index].to_f,
                                        'cost' => @data["cost"].to_f,
                                        'payment' => @data["payment"].to_f,
                                        'opendomains' => @data["openDomains"],
                                        'regdomain' => @data["regDomain"].to_s,
                                        'budgettype' => @data["budgetType"].to_i,
                                        'regiontarget' => @data["regionTarget"],
                                        'budgetofflineTime' => @data["budgetOfflineTime"],
                                        'isDynamiccreative' => @data["isDynamicCreative"].to_s,
                                        'last_update' => @now
                                  })
    
                 @db.close
              end
              
          end
      end
      
      
      
      
      
      data = {:message => "baidu update account", :status => "true"}
      return render :json => data, :status => :ok
      
  end




  def checkreport
  
      @logger.info "checkreport baidu start"
      
      @current_not_done_report = @db[:miss_report].find({ "$and" => [{:status => 1},{:worker => @port.to_i},{:network_type => "baidu"}] })
      @db.close
      
      if @current_not_done_report.count.to_i > 0
          data = {:message => "check report baidu running", :status => "true"}
          return render :json => data, :status => :ok  
      end
    
      @not_done_report = @db[:miss_report].find({ "$and" => [{:status => 0},{:worker => @port.to_i},{:network_type => "baidu"}] }).limit(1)
      @db.close
      
      
      
      if @not_done_report.count.to_i > 0
          @not_done_report.no_cursor_timeout.each do |not_done_report_d|
            
              begin
                
                  @db[:miss_report].find('_id' => not_done_report_d["_id"], 'status' => 0 ).update_one('$set'=> { 'status' => 1, 'update_date' => @now })
                  @db.close
                  
                  id = not_done_report_d["network_id"]
                  report_day = not_done_report_d["report_date"]
                  
                  @logger.info "checkreport baidu running "+id.to_s+" - "+report_day.to_s
                  
                  days = @today.to_date - report_day.to_date
                  
                  url = "http://china.adeqo.com:"+@port.to_s+"/baidu/report?day="+days.to_i.to_s+"&id="+id.to_s
                  # res = Net::HTTP.get_response(URI(url))
                  
                  link = URI.parse(url)
                  http = Net::HTTP.new(link.host, link.port)
                  
                  http.read_timeout = 800
                  http.open_timeout = 800
                  res = http.start() {|http|
                    http.get(URI(url))
                  }
    
                  
                  @logger.info "checkreport running baidu report "+id.to_s+" - "+report_day.to_s
                  
                  if res.code.to_i == 200 
                          
                      @db[:miss_report].find('_id' => not_done_report_d["_id"], 'status' => 1 ).delete_one
                      @db.close
                      
                      @logger.info "checkreport done baidu report "+id.to_s+" - "+report_day.to_s
                      
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

  # GET /baidus
  # GET /baidus.json
  def index 
    
      # server time for token encrypt
      # https://api.baidu.com/sem/sms/ServerTime
        
      # @data = "start"
#       
      @network = @db[:network].find('type' => 'baidu')
      @db.close
  
      if @network.count.to_i > 0 
      
          @network.no_cursor_timeout.each do |network_d|
              @apitoken = network_d["api_token"]
              @username = network_d["username"]
              @password = network_d["password"]
          end
          
          service = "AccountService"
          method = "getAccountInfo"
          
          json = {'header' => { 
                                  'token' => @apitoken.to_s,
                                  'username' => @username.to_s,
                                  'password' => @password.to_s 
                              },
                   'body'  => {
                                  'accountFields' => ["userId","balance","cost","payment","budgetType","budget","regionTarget","excludeIp","openDomains","regDomain","budgetOfflineTime","weeklyBudget","userStat","isDynamicCreative","dynamicCreativeParam","pcBalance","mobileBalance"]
                              }
                  }       
                  
          @account_info = baidu_api(service,method,json)
          
          if @account_info["header"]["desc"].nil?
             
             @data = "error"
             
          else
            
             @header = @account_info["header"]
             @quota = @header["rquota"]
             
             @data = @account_info["body"]["data"][0]  
          end                              
      
      end
             
      # @file = @tmp+"/baidu_ad_report_114.csv"
#       
      # all_arr = []
#       
      # CSV.foreach(@file, :encoding => 'GB18030').each_with_index do |csv, index|
#         
          # if index > 0
#             
              # csv = csv.to_csv
              # csv_array = csv.split("\t")
#             
              # @logger.info csv_array
              # all_arr << csv_array
              # # all_arr << csv_array
#               
#               
#               
              # # csv_array = csv[0].split("\t")
#             
              # # data = {:message => "baidu login", :csv_count => csv.count, :csv => csv, :csv0 => csv.first, :csv1 => csv[1], :csv_array => csv_array, :csv_array0 => csv_array[0], :status => "true"}
          # end
#           
      # end
      
      # resetnetworkreport(112)
      # resetnetworkreport(113)
      # resetnetworkreport(114)
      # resetnetworkreport(115)
      # resetnetworkreport(116)
      
      data = {:message => "baidu index", :datas => @header, :status => "true"}
      return render :json => data, :status => :ok
                        
      # data = {:message => "baidu login", :tmp => @account_info, :quota => @quota, :status => "true"}
      # return render :json => data, :status => :ok   
      # return response.parsed_response
    
    # @baidus = Baidu.all
  end

  # GET /baidus/1
  # GET /baidus/1.json
  def show
  end

  # GET /baidus/new
  def new
    @baidu = Baidu.new
  end

  # GET /baidus/1/edit
  def edit
  end

  # POST /baidus
  # POST /baidus.json
  def create
    @baidu = Baidu.new(baidu_params)

    respond_to do |format|
      if @baidu.save
        format.html { redirect_to @baidu, notice: 'Baidu was successfully created.' }
        format.json { render :show, status: :created, location: @baidu }
      else
        format.html { render :new }
        format.json { render json: @baidu.errors, status: :unprocessable_entity }
      end
    end
  end

  # PATCH/PUT /baidus/1
  # PATCH/PUT /baidus/1.json
  def update
    respond_to do |format|
      if @baidu.update(baidu_params)
        format.html { redirect_to @baidu, notice: 'Baidu was successfully updated.' }
        format.json { render :show, status: :ok, location: @baidu }
      else
        format.html { render :edit }
        format.json { render json: @baidu.errors, status: :unprocessable_entity }
      end
    end
  end 
    
  # DELETE /baidus/1
  # DELETE /baidus/1.json
  def destroy
    @baidu.destroy
    respond_to do |format|
      format.html { redirect_to baidus_url, notice: 'Baidu was successfully destroyed.' }
      format.json { head :no_content }
    end
  end

  private
    # Use callbacks to share common setup or constraints between actions.
    def set_baidu
      @baidu = Baidu.find(params[:id])
    end

    # Never trust parameters from the scary internet, only allow the white list through.
    def baidu_params
      params[:baidu]
    end
end
