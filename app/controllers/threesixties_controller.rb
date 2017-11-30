class ThreesixtiesController < ApplicationController
  before_action :set_threesixty, only: [:show, :edit, :update, :destroy]
  before_action :tmp
  
  require 'rubygems'
  require 'httparty'
  
  require 'mongo'
  require 'zlib'
  
  def test
        # mg_client = Mailgun::Client.new("key-c72a65e7d3d818852d40757182bd82c9")  
        # mb_obj = Mailgun::MessageBuilder.new()
#      
        # mb_obj.set_from_address("do-not-reply@adeqo.com", {"first"=>"Adeqo", "last" => ""});
        # mb_obj.add_recipient(:to, "jkwan@bmgww.com", {"first" => "", "last" => ""});
#         
        # mb_obj.set_subject("Adeqo | test");  
        # # mb_obj.set_text_body(result_msg);
#         
        # result_msg = "<p>test</p>"
        # result_msg = result_msg + "<p>Please login and visit the account page:<br /><a href='http://china.adeqo.com/account'>http://china.adeqo.com/account</a></p>"
        # result_msg = result_msg + "<p>to update Password and API Token Information</p>"
        # result_msg = result_msg + "<p>Incorrect API credentials will stop us from keeping the information up-to-date. The users will not be able to use any editing functions.</p>"
        # result_msg = result_msg + "<br /><p>Team BMG</p>" 
        # mb_obj.set_html_body(result_msg);
        # mail = mg_client.send_message("china.adeqo.com", mb_obj)
  end
  
  def tmp
    @tmp = "/datadrive"    
  end
  
  def checkreport
    
      @logger.info "checkreport 360 start"
      
      @current_not_done_report = @db[:miss_report].find({ "$and" => [{:status => 1},{:worker => @port.to_i},{:network_type => "360"}] })
      @db.close
      
      if @current_not_done_report.count.to_i > 0
          data = {:message => "check report 360 running", :status => "true"}
          return render :json => data, :status => :ok  
      end
    
      @not_done_report = @db[:miss_report].find({ "$and" => [{:status => 0},{:worker => @port.to_i},{:network_type => "360"}] }).limit(1)
      @db.close
      
      # ?day=3
      
      
      if @not_done_report.count.to_i > 0
          @not_done_report.no_cursor_timeout.each do |not_done_report_d|
            
              begin
                  @db[:miss_report].find('_id' => not_done_report_d["_id"], 'status' => 0 ).update_one('$set'=> { 'status' => 1, 'update_date' => @now})
                  @db.close
                  
                  id = not_done_report_d["network_id"]
                  report_day = not_done_report_d["report_date"]
                  
                  @logger.info "checkreport 360 running "+id.to_s+" - "+report_day.to_s
                  
                  days = @today.to_date - report_day.to_date
                  
                  url = "http://china.adeqo.com:"+@port.to_s+"/threesixties/report?day="+days.to_i.to_s+"&id="+id.to_s
                  # res = Net::HTTP.get_response(URI(url))
                  
                  link = URI.parse(url)
                  http = Net::HTTP.new(link.host, link.port)
                  
                  http.read_timeout = 900
                  http.open_timeout = 900
                  res = http.start() {|http|
                    http.get(URI(url))
                  }
    
                  
                  @logger.info "checkreport running 360 report "+id.to_s+" - "+report_day.to_s
                  
                  if res.code.to_i == 200 
                      url = "http://china.adeqo.com:"+@port.to_s+"/threesixties/report_upper?day="+days.to_i.to_s+"&id="+id.to_s
                      # res = Net::HTTP.get_response(URI(upper_url))
                      
                      link = URI.parse(url)
                      http = Net::HTTP.new(link.host, link.port)
                      
                      http.read_timeout = 900
                      http.open_timeout = 900
                      res = http.start() {|http|
                        http.get(URI(url))
                      }
                      
                      @logger.info "checkreport running 360 report upper"+id.to_s+" - "+report_day.to_s
                      
                      if res.code.to_i == 200
                          
                          @db[:miss_report].find('_id' => not_done_report_d["_id"], 'status' => 1 ).delete_one
                          @db.close
                          
                          @logger.info "checkreport done 360 report "+id.to_s+" - "+report_day.to_s
                      else
                          @db[:miss_report].find('_id' => not_done_report_d["_id"], 'status' => 1 ).update_one('$set'=> { 'status' => 0, 'update_date' => @now })
                          @db.close
                          
                          data = {:message => @not_done_report, :status => "false"}
                          return render :json => data, :status => :ok
                      end
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
  
   
  def apiadgroup
      @logger.info "360 api adgroup start"
      
      @campaign_id = params[:id]
    
      if @campaign_id.nil?
          @current_campaign = @db[:all_campaign].find({ "$and" => [{:network_type => '360'}, {:api_worker => @port.to_i}, {:api_update => 4}] })
          @db.close
          
          if @current_campaign.count.to_i >= 1
              @logger.info "working, no need update campaign api adgroup"
              return render :nothing => true
          end
          
          @campaign = @db[:all_campaign].find({ "$and" => [{:network_type => '360'}, {:api_worker => @port.to_i}, {:api_update => 3}] }).sort({ last_update: -1 }).limit(1)
          @db.close
          
          if @campaign.count.to_i == 0
              @logger.info "no need update campaign api adgroup"
              return render :nothing => true
          end
          
      else
          @campaign = @db[:all_campaign].find({ "$and" => [{:campaign_id => @campaign_id.to_i}, {:network_type => '360'}] })
          @db.close
      end
      
      
      if @campaign.count.to_i
          
          @campaign.no_cursor_timeout.each do |campaign|
              @network_id = campaign["network_id"].to_i
              @campaign_id = campaign["campaign_id"].to_i
              @campaign_name = campaign["campaign_name"].to_s
          end
          
          @network = @db[:network].find({ "$and" => [{:id => @network_id.to_i}, {:type => '360'}] })
          @db.close
          
          if @network.count.to_i > 0
              @network.no_cursor_timeout.each do |network_d|
                  @tracking_type = network_d["tracking_type"].to_s
                  @ad_redirect = network_d["ad_redirect"].to_s
                  @keyword_redirect = network_d["keyword_redirect"].to_s
                  @company_id = network_d["company_id"].to_s
                  @cookie_length = network_d["cookie_length"].to_s
                  
                  @account_id = network_d["accountid"].to_i
                  @account_name = network_d["name"].to_s
                
                  @username = network_d["username"]
                  @password = network_d["password"]
                  @apitoken = network_d["api_token"]
                  @apisecret = network_d["api_secret"]
                  
                  login_info = threesixty_api_login(@username,@password,@apitoken,@apisecret)
                  @refresh_token = login_info["account_clientLogin_response"]["accessToken"]
                  
                  if !@refresh_token.nil?
                      @update_res = threesixty_api( @apitoken.to_s, @refresh_token, "account", "getInfo")
                      @remain_quote = @response.headers["quotaremain"].to_i
                      
                      if @remain_quote.to_i >= 500
                          db_name = "adgroup_360_"+@network_id.to_s
                          
                          @adgroup = @threesixty_db[db_name].find({ "$and" => [{:campaign_id => @campaign_id.to_i}, {:api_update_ad => 1}, {:api_update_keyword => 1}, {:api_worker => @port.to_i}] })
                          @threesixty_db.close()
                          
                          @adgroup_id_array = []
                          @adgroup_arr = []
                          
                          if @adgroup.count.to_i
                        
                              @adgroup.no_cursor_timeout.each do |adgroup_d|
                                  @adgroup_id_array << adgroup_d["adgroup_id"].to_i
                              end
                              
                              @adgroup_id_array_str = @adgroup_id_array.join(",")
                              body = {}
                              body[:idList] = "["+@adgroup_id_array_str.to_s+"]"
                              
                              @update_res = threesixty_api( @apitoken.to_s, @refresh_token, "group", "getInfoByIdList", body)
                              
                              if @update_res["group_getInfoByIdList_response"]["failures"].nil?
                                  if !@update_res["group_getInfoByIdList_response"]["groupList"].nil?
                                      @adgroup_status_body = @update_res["group_getInfoByIdList_response"]["groupList"]["item"]
                                      @remain_quote = @response.headers["quotaremain"].to_i
                                      
                                      if @adgroup_status_body.is_a?(Array)
                                          if @adgroup_status_body.count.to_i > 0
                                              @adgroup_status_body.each do |adgroup_status_body_d|
                                                  @adgroup_arr << adgroup_status_body_d
                                              end
                                          end
                                      else
                                          @adgroup_arr << @adgroup_status_body
                                      end
                                      
                                      if @adgroup_arr.count.to_i > 0
                                          @adgroup_arr.each do |adgroup_arr_d|
                                              db_name = "adgroup_360_"+@network_id.to_s
                                              
                                              if adgroup_arr_d["status"].to_s == "enable"
                                                  @status = "启用"
                                              else
                                                  @status = "暂停"
                                              end
                                                      
                                              result = @threesixty_db[db_name].find({ "$and" => [{:adgroup_id => adgroup_arr_d["id"].to_i}, {:campaign_id => adgroup_arr_d["campaignId"].to_i}] }).update_one('$set'=> { 
                                                                                                                                                'adgroup_name' => adgroup_arr_d["name"].to_s,
                                                                                                                                                'price' => adgroup_arr_d["price"].to_f,
                                                                                                                                                'status' => @status,
                                                                                                                                                'negative_words' => adgroup_arr_d["negativeWords"],
                                                                                                                                                'exact_negative_words' => adgroup_arr_d["exactNegativeWords"],
                                                                                                                                                'api_update_ad' => 2,
                                                                                                                                                'api_update_keyword' => 2,
                                                                                                                                                'update_date' => @now
                                                                                                                                           })
                                              @threesixty_db.close()
                                              
                                              if result.n.to_i == 0
                                                          
                                                  @threesixty_db[db_name].insert_one({ 
                                                                                      network_id: @network_id.to_i,
                                                                                      account_id: @account_id.to_i,
                                                                                      account_name: @account_name.to_s,
                                                                                      campaign_id: @campaign_id.to_i,
                                                                                      campaign_name: @campaign_name.to_s,
                                                                                      adgroup_id: adgroup_arr_d["id"].to_i,
                                                                                      adgroup_name: adgroup_arr_d["name"].to_s,
                                                                                      price: adgroup_arr_d["price"].to_f,
                                                                                      negative_words: adgroup_arr_d["negativeWords"],
                                                                                      exact_negative_words: adgroup_arr_d["exactNegativeWords"],
                                                                                      status: @status,
                                                                                      sys_status: "",
                                                                                      update_date: @now,                                            
                                                                                      create_date: @now 
                                                                                      })
                                                  @threesixty_db.close()
                                              end
                                          end 
                                          
                                          
                                          
                                          
                                          
                                      end
                                  end
                              end
                              
                              # adgroup done
                              # ad start/keyword
                              
                              if @adgroup_id_array.count.to_i > 0 && @remain_quote >= 500
                                  # ad
                              
                                  @ad_id_arr = []
                                  @ad_id_arr_str = ""
                                  
                                  @keyword_id_arr = []
                                  @keyword_id_arr_str = ""
                                  
                                  @all_ad = []
                                  @all_keyword = []
                                
                                  db_name = "ad_360_"+@network_id.to_s
                              
                                  @adgroup_id_array.each do |adgroup_id_arr_d|
                                      body = {}
                                      body[:groupId] = adgroup_id_arr_d.to_i
                                      @update_res = threesixty_api( @apitoken.to_s, @refresh_token, "creative", "getIdListByGroupId", body)
                                      
                                      if @update_res["creative_getIdListByGroupId_response"]["failures"].nil?
                                          if !@update_res["creative_getIdListByGroupId_response"]["creativeIdList"].nil?
                                              @ad_id_status_body = @update_res["creative_getIdListByGroupId_response"]["creativeIdList"]["item"]
                                              @remain_quote = @response.headers["quotaremain"].to_i
                                              
                                              if @ad_id_status_body.is_a?(Array)
                                                  if @ad_id_status_body.count.to_i > 0
                                                      @ad_id_status_body.each do |ad_id_status_body_d|
                                                          @ad_id_arr << ad_id_status_body_d.to_i
                                                      end
                                                  end
                                              else
                                                  @ad_id_arr << @ad_id_status_body.to_i
                                              end
                                          end
                                      end
                                  end 
                                  
                                  
                                  if @ad_id_arr.count.to_i > 0
                                    
                                      @ad_id_arr_str = @ad_id_arr.join(",")
                                              
                                      body = {}
                                      body[:idList] = "["+@ad_id_arr_str.to_s+"]"
                                      
                                      
                                      @update_res = threesixty_api( @apitoken.to_s, @refresh_token, "creative", "getInfoByIdList", body)
                                      
                                      if @update_res["creative_getInfoByIdList_response"]["failures"].nil?
                                          if !@update_res["creative_getInfoByIdList_response"]["creativeList"].nil?
                                              @ad_status_body = @update_res["creative_getInfoByIdList_response"]["creativeList"]["item"]
                                              @remain_quote = @response.headers["quotaremain"].to_i
                                              
                                              
                                              if @ad_status_body.is_a?(Hash)
                                                  if !@ad_status_body.empty?
                                                      @all_ad << @ad_status_body
                                                  end
                                              else
                                                  @all_ad = @all_ad + @ad_status_body
                                              end
                                              
                                              if @all_ad.count.to_i > 0
                                                  @all_ad.each do |all_ad_d|
                                                    
                                                      if all_ad_d["status"].to_s == "enable"
                                                          @status = "启用"
                                                      else
                                                          @status = "暂停"
                                                      end
                                                      
                                                      url_tag = 0
                                                      m_url_tag = 0
                                                      
                                                      @final_url = all_ad_d["destinationUrl"].to_s
                                                      @m_final_url = all_ad_d["mobileDestinationUrl"].to_s
                                                      
                                                      if !@final_url.to_s.include?(".adeqo.") && @tracking_type.to_s.downcase == "adeqo" && @ad_redirect.to_s.downcase == "yes" && @final_url.to_s != ""
                                                          @temp_final_url = @final_url
                                                          
                                                          @final_url = "http://t.adeqo.com/click?company_id="+@company_id.to_s+"&network_id="+@network_id.to_s
                                                          @final_url = @final_url + "&campaign_id={planid}&adgroup_id={groupid}&ad_id={creativeid}&keyword_id={wordid}"
                                                          @final_url = @final_url + "&cookie="+@cookie_length.to_s
                                                          @final_url = @final_url + "&device=pc"
                                                          @final_url = @final_url + "&tv=v1&durl="+CGI.escape(@temp_final_url.to_s)
                                                          
                                                          url_tag = 1
                                                      end
                                                      
                                                      if !@m_final_url.to_s.include?(".adeqo.") && @tracking_type.to_s.downcase == "adeqo" && @ad_redirect.to_s.downcase == "yes" && @m_final_url.to_s != ""
                                                          @temp_m_final_url = @m_final_url
                                                          
                                                          @m_final_url = "http://t.adeqo.com/click?company_id="+@company_id.to_s+"&network_id="+@network_id.to_s
                                                          @m_final_url = @m_final_url + "&campaign_id={planid}&adgroup_id={groupid}&ad_id={creativeid}&keyword_id={wordid}"
                                                          @m_final_url = @m_final_url + "&cookie="+@cookie_length.to_s
                                                          @m_final_url = @m_final_url + "&device=mobile"
                                                          @m_final_url = @m_final_url + "&tv=v1&durl="+CGI.escape(@temp_m_final_url.to_s)
                                                          
                                                          m_url_tag = 1
                                                      end
                                                      
                                                      begin
                                                          if url_tag == 1 || m_url_tag == 1
                                                              
                                                              if @remain_quote.to_i >= 500
                                                                
                                                                  requesttypearray = []
                                                                  request_str = '{"id":'+all_ad_d["id"].to_s+',"destinationUrl":"'+@final_url+'","mobileDestinationUrl":"'+@m_final_url+'"}'
                                                                  
                                                                  requesttypearray << request_str
                                                                  request = '['+requesttypearray.join(",")+']'
                                                                  
                                                                  # @logger.info request
                                                                  
                                                                  body = { 
                                                                      'creatives' => request
                                                                  }
                                                                  
                                                                  @update_res = threesixty_api( @apitoken.to_s, @refresh_token, "creative", "update", body)
                                                                  @affectedRecords = @update_res["creative_update_response"]["affectedRecords"]
                                                                  @remain_quote = @response.headers["quotaremain"].to_i
                                                                  
                                                                  @logger.info @update_res["creative_update_response"]
                                                                  
                                                                  if !@update_res["creative_update_response"]["failures"].nil?
                                                                      @final_url = all_ad_d["destinationUrl"].to_s
                                                                      @m_final_url = all_ad_d["mobileDestinationUrl"].to_s
                                                                  end
                                                              end
                                                          end
                                                      rescue Exception
                                                          @final_url = all_ad_d["destinationUrl"].to_s
                                                          @m_final_url = all_ad_d["mobileDestinationUrl"].to_s
                                                      end
                                                      
                                                      
                                                      db_name = "ad_360_"+@network_id.to_s
                                                      
                                                      result = @threesixty_db[db_name].find({ "$and" => [{:ad_id => all_ad_d["id"].to_i}, {:adgroup_id => all_ad_d["groupId"].to_i}] }).update_one('$set'=> { 
                                                                                                                                                                                    'title' => all_ad_d["title"].to_s,
                                                                                                                                                                                    'description_1' => all_ad_d["description1"].to_s,
                                                                                                                                                                                    'description_2' => all_ad_d["description2"].to_s,
                                                                                                                                                                                    'visit_url' => @final_url.to_s,
                                                                                                                                                                                    'show_url' => all_ad_d["displayUrl"].to_s,
                                                                                                                                                                                    'mobile_visit_url' => @m_final_url.to_s,
                                                                                                                                                                                    'mobile_show_url' => all_ad_d["mobileDisplayUrl"].to_s,
                                                                                                                                                                                    'status' => @status,
                                                                                                                                                                                    'update_date' => @now
                                                                                                                                                                                    
                                                                                                                                                                               })
                                                      @threesixty_db.close()
                                                      
                                                      if result.n.to_i == 0
                                                                                                        
                                                          @threesixty_db[db_name].insert_one({ 
                                                                      network_id: @network_id.to_i,
                                                                      account_id: @account_id.to_i,
                                                                      account_name: @account_name.to_s,
                                                                      campaign_id: @campaign_id.to_i,
                                                                      campaign_name: @campaign_name.to_s,
                                                                      adgroup_id: all_ad_d["groupId"].to_i,
                                                                      ad_id: all_ad_d["id"].to_i,
                                                                      title: all_ad_d["title"].to_s, 
                                                                      description_1: all_ad_d["description1"].to_s,
                                                                      description_2: all_ad_d["description2"].to_s, 
                                                                      status: @status.to_s,
                                                                      sys_status: "",
                                                                      show_url: all_ad_d["displayUrl"].to_s,
                                                                      visit_url: @final_url.to_s,
                                                                      mobile_show_url: all_ad_d["mobileDisplayUrl"].to_s,
                                                                      mobile_visit_url: @m_final_url.to_s,
                                                                      extend_ad_type: 0,
                                                                      update_date: @now,                                            
                                                                      create_date: @now 
                                                                      })
                                                          @threesixty_db.close()
                                                      end
                                                              
                                                  end
                                              end
                                              
                                              
                                          end
                                      end
                                  end
                                  # ad
                                  
                                  # keyword
                                  @adgroup_id_array.each do |adgroup_id_arr_d|
                                      body = {}
                                      body[:groupId] = adgroup_id_arr_d.to_i
                                      @update_res = threesixty_api( @apitoken.to_s, @refresh_token, "keyword", "getIdListByGroupId", body)
                                      
                                      if @update_res["keyword_getIdListByGroupId_response"]["failures"].nil?
                                          if !@update_res["keyword_getIdListByGroupId_response"]["keywordIdList"].nil?
                                              @keyword_id_status_body = @update_res["keyword_getIdListByGroupId_response"]["keywordIdList"]["item"]
                                              @remain_quote = @response.headers["quotaremain"].to_i
                                              
                                              if @keyword_id_status_body.is_a?(Array)
                                                  if @keyword_id_status_body.count.to_i > 0
                                                      @keyword_id_status_body.each do |keyword_id_status_body_d|
                                                          @keyword_id_arr << keyword_id_status_body_d.to_i
                                                      end
                                                  end
                                              else
                                                  @keyword_id_arr << @keyword_id_status_body.to_i
                                              end
                                          end
                                      end
                                  end
                                  
                                  if @keyword_id_arr.count.to_i > 0
                                              
                                      @keyword_id_arr_str = @keyword_id_arr.join(",")
                                      
                                      body = {}
                                      body[:idList] = "["+@keyword_id_arr_str.to_s+"]"
                                      
                                      @update_res = threesixty_api( @apitoken.to_s, @refresh_token, "keyword", "getInfoByIdList", body)
                                      
                                      if @update_res["keyword_getInfoByIdList_response"]["failures"].nil?
                                          if !@update_res["keyword_getInfoByIdList_response"]["keywordList"].nil?
                                              @keyword_status_body = @update_res["keyword_getInfoByIdList_response"]["keywordList"]["item"]
                                              @remain_quote = @response.headers["quotaremain"].to_i
                                              
                                              
                                              if @keyword_status_body.is_a?(Hash)
                                                  if !@keyword_status_body.empty?
                                                      @all_keyword << @keyword_status_body
                                                  end
                                              else
                                                  @all_keyword = @all_keyword + @keyword_status_body
                                              end
                                          end
                                      end
                                  end
                                  
                                  
                                  if @all_keyword.count.to_i > 0
                                      @all_keyword.each do |all_keyword_d|
                                        
                                          # @logger.info all_keyword_d
                                          
                                          if all_keyword_d["status"].to_s == "enable"
                                              @status = "启用"
                                          else
                                              @status = "暂停"
                                          end
                                          
                                          if all_keyword_d["match_type"].to_s.downcase.include?("intelligence")
                                              @match = "智能短语"
                                          elsif all_keyword_d["match_type"].to_s.downcase == "phrase"
                                              @match = "短语"
                                          elsif all_keyword_d["match_type"].to_s.downcase == "exact"
                                              @match = "精确"
                                          else
                                              @match = "广泛"
                                          end
                                          
                                          
                                          url_tag = 0
                                          m_url_tag = 0
                                          
                                          @final_url = all_keyword_d["destinationUrl"].to_s
                                          @m_final_url = all_keyword_d["mobileDestinationUrl"].to_s
                                          
                                          if !@final_url.to_s.include?(".adeqo.") && @tracking_type.to_s.downcase == "adeqo" && @ad_redirect.to_s.downcase == "yes" && @final_url.to_s != ""
                                              @temp_final_url = @final_url
                                              
                                              @final_url = "http://t.adeqo.com/click?company_id="+@company_id.to_s+"&network_id="+@network_id.to_s
                                              @final_url = @final_url + "&campaign_id={planid}&adgroup_id={groupid}&ad_id={creativeid}&keyword_id="+all_keyword_d["id"].to_s
                                              @final_url = @final_url + "&cookie="+@cookie_length.to_s
                                              @final_url = @final_url + "&device=pc"
                                              @final_url = @final_url + "&tv=v1&durl="+CGI.escape(@temp_final_url.to_s)
                                              
                                              url_tag = 1
                                          end
                                          
                                          if !@m_final_url.to_s.include?(".adeqo.") && @tracking_type.to_s.downcase == "adeqo" && @ad_redirect.to_s.downcase == "yes" && @m_final_url.to_s != ""
                                              @temp_m_final_url = @m_final_url
                                              
                                              @m_final_url = "http://t.adeqo.com/click?company_id="+@company_id.to_s+"&network_id="+@network_id.to_s
                                              @m_final_url = @m_final_url + "&campaign_id={planid}&adgroup_id={groupid}&ad_id={creativeid}&keyword_id="+all_keyword_d["id"].to_s
                                              @m_final_url = @m_final_url + "&cookie="+@cookie_length.to_s
                                              @m_final_url = @m_final_url + "&device=mobile"
                                              @m_final_url = @m_final_url + "&tv=v1&durl="+CGI.escape(@temp_m_final_url.to_s)
                                              
                                              m_url_tag = 1
                                          end
                                          
                                          begin
                                              if url_tag == 1 || m_url_tag == 1
                                                  
                                                  if @remain_quote.to_i >= 500
                                                    
                                                      requesttypearray = []
                                                      request_str = '{"id":'+all_keyword_d["id"].to_s+',"url":"'+@final_url+'","mobileUrl":"'+@m_final_url+'"}'
                                                      
                                                      requesttypearray << request_str
                                                      request = '['+requesttypearray.join(",")+']'
                                                      
                                                      # @logger.info request
                                                      
                                                      body = { 
                                                          'keywords' => request
                                                      }
                                                      
                                                      @update_res = threesixty_api( @apitoken.to_s, @refresh_token, "keyword", "update", body)
                                                      @affectedRecords = @update_res["keyword_update_response"]["affectedRecords"]
                                                      @remain_quote = @response.headers["quotaremain"].to_i
                                                      
                                                      
                                                      if !@update_res["creative_update_response"]["failures"].nil?
                                                          @final_url = all_keyword_d["destinationUrl"].to_s
                                                          @m_final_url = all_keyword_d["mobileDestinationUrl"].to_s
                                                      end
                                                  end
                                              end
                                          rescue Exception
                                              @final_url = all_keyword_d["destinationUrl"].to_s
                                              @m_final_url = all_keyword_d["mobileDestinationUrl"].to_s
                                          end
                                          
                                          
                                          # @logger.info all_keyword_d["id"].to_i
                                          db_name = "keyword_360_"+@network_id.to_s
                                          
                                          
                                          
                                          result = @threesixty_db[db_name].find({ "$and" => [{:keyword_id => all_keyword_d["id"].to_i}, {:adgroup_id => all_keyword_d["groupId"].to_i}] }).update_one('$set'=> { 
                                                                                                                                                                          'keyword' => all_keyword_d["word"].to_s,
                                                                                                                                                                          'price' => all_keyword_d["price"].to_f,
                                                                                                                                                                          'visit_url' => @final_url.to_s,
                                                                                                                                                                          'mobile_visit_url' => @m_final_url.to_s,
                                                                                                                                                                          'match_type' => @match.to_s,
                                                                                                                                                                          'status' => @status,
                                                                                                                                                                          'update_date' => @now
                                                                                                                                                                          
                                                                                                                                                                     })
                                          @threesixty_db.close()
                                          
                                          if result.n.to_i == 0
                                              
                                                                 
                                              @threesixty_db[db_name].insert_one({ 
                                                      network_id: @network_id.to_i,
                                                      account_id: @account_id.to_i,
                                                      account_name: @account_name.to_s,
                                                      campaign_id: @campaign_id.to_i,
                                                      campaign_name: @campaign_name.to_s,
                                                      adgroup_id: all_keyword_d["groupId"].to_i,
                                                      keyword_id: all_keyword_d["id"].to_i,
                                                      keyword: all_keyword_d["word"].to_s,
                                                      price: all_keyword_d["price"].to_f, 
                                                      status: @status,
                                                      sys_status: "",
                                                      match_type: @match.to_s,
                                                      visit_url: @final_url.to_s,
                                                      mobile_visit_url: @m_final_url.to_s,
                                                      cpc_quality: 0,
                                                      # negative_words: csv[@keyword_negative_index].to_s,
                                                      extend_ad_type: 0,
                                                      update_date: @now,                                            
                                                      create_date: @now 
                                                      })
                                                      
                                              @threesixty_db.close()  
                                          end
                                      end
                                  end
                                    
                                  db_name = "adgroup_360_"+@network_id.to_s
                                  @threesixty_db[db_name].find('adgroup_id' => { "$in" => @adgroup_id_array}).update_many('$set'=> { 
                                                                                                                                  'api_update_ad' => 0,
                                                                                                                                  'api_update_keyword' => 0,
                                                                                                                                  'api_worker' => ""
                                                                                                                                  })
                                  @threesixty_db.close()
                                  
                                   
                                  
                                  #keyword
                              end
                              
                              
                              # data = {:tmp => @adgroup_arr, :status => "true"}
                              # return render :json => data, :status => :ok
                      
                          end
                      end
                  end
              end
          end
          
          
          
          # the end update status for the group
          db_name = "adgroup_360_"+@network_id.to_s
          @list_adgroup = @threesixty_db[db_name].find('$and' => [{'campaign_id' => @campaign_id.to_i},{'api_update_ad' => { "$ne" => 0}},{'api_update_keyword' => { "$ne" => 0}},{'api_update_ad' => { '$exists' => true }},'api_update_keyword' => { '$exists' => true }])
          @threesixty_db.close() 
          
          if @list_adgroup.count.to_i == 0
            
              @db["all_campaign"].find({ "$and" => [{:campaign_id => @campaign_id.to_i}, {:network_type => "360"}, {:api_update => 3}] }).update_one('$set'=> {'api_update' => 0, 'api_worker' => "", 'update_date' => @now})
              @db.close 
              
              @db[:network].find({ "$and" => [{:id => @network_id.to_i}, {:type => "360"}] }).update_one('$set'=> {'file_update_1' => 4,'file_update_2' => 4,'file_update_3' => 4,'file_update_4' => 4, 'last_update' => @now})
              @db.close
          end   
          
          
      end
      
      
      
      @logger.info "360 api adgroup end"
      return render :nothing => true
  end
  
  def apicampaign
    
      @logger.info "360 api campaign start"
    
      
      @campaign_id = params[:id]
    
      if @campaign_id.nil?
        
          @current_campaign = @db[:all_campaign].find({ "$and" => [{:network_type => '360'}, {:api_worker => @port.to_i}, {:api_update => 2}] })
          @db.close
          
          if @current_campaign.count.to_i >= 1
              @logger.info "working, no need update 360 api campaign"
              return render :nothing => true
          end
          
          @campaign = @db[:all_campaign].find({ "$and" => [{:network_type => '360'}, {:api_worker => @port.to_i}, {:api_update => 1}] }).sort({ last_update: -1 }).limit(1)
          @db.close
          
          if @campaign.count.to_i == 0
              @logger.info "no need update 360 api campaign"
              return render :nothing => true
          end
          
      else
          @campaign = @db[:all_campaign].find({ "$and" => [{:campaign_id => @campaign_id.to_i}, {:network_type => '360'}] })
          @db.close
      end
      
      @network_id = 0
    
      if @campaign.count.to_i > 0
          @campaign.no_cursor_timeout.each do |campaign|
              @network_id = campaign["network_id"].to_i
              @campaign_id = campaign["campaign_id"].to_i
              @campaign_name = campaign["campaign_name"].to_s
              
              
              @campaign_status_body = ""
              
              @db["all_campaign"].find({ "$and" => [{:campaign_id => @campaign_id.to_i}, {:network_type => '360'}] }).update_one('$set'=> { 'api_update' => 2 })
              @db.close
              
              @network = @db[:network].find({ "$and" => [{:id => @network_id.to_i}, {:type => '360'}] })
              @db.close
              
              if @network.count.to_i > 0
                  @network.no_cursor_timeout.each do |network_d|
                    
                      @tracking_type = network_d["tracking_type"].to_s
                      @ad_redirect = network_d["ad_redirect"].to_s
                      @keyword_redirect = network_d["keyword_redirect"].to_s
                      @company_id = network_d["company_id"].to_s
                      @cookie_length = network_d["cookie_length"].to_s
                      
                      @account_id = network_d["accountid"].to_i
                      @account_name = network_d["name"].to_s
                    
                      @username = network_d["username"]
                      @password = network_d["password"]
                      @apitoken = network_d["api_token"]
                      @apisecret = network_d["api_secret"]
                                           
                      login_info = threesixty_api_login(@username,@password,@apitoken,@apisecret)
                      @refresh_token = login_info["account_clientLogin_response"]["accessToken"]
                      
                      if !@refresh_token.nil?
                          @update_res = threesixty_api( @apitoken.to_s, @refresh_token, "account", "getInfo")
                          @remain_quote = @response.headers["quotaremain"].to_i
                          
                          
                          if @remain_quote.to_i >= 500
                            
                              body = {}
                              body[:idList] = "["+@campaign_id.to_s+"]"
                              
                              @update_res = threesixty_api( @apitoken.to_s, @refresh_token, "campaign", "getInfoByIdList", body)
                              
                              if @update_res["campaign_getInfoByIdList_response"]["failures"].nil?
                                      
                                  @campaign_status_body = @update_res["campaign_getInfoByIdList_response"]["campaignList"]["item"]
                                  
                                  # # @logger.info @update_res
                                  @remain_quote = @response.headers["quotaremain"].to_i
                                  # # @logger.info @remain_quote
                          
                                  if @campaign_status_body["status"].to_s == "enable"
                                      @campaign_status = "启用"
                                  else
                                      @campaign_status = "暂停"
                                  end
                                  
                                  @adgroup_id_arr = []
                                  @adgroup_id_arr_str = ""
                                  
                                  db_name = "adgroup_360_"+@network_id.to_s
                                  
                                  if @remain_quote >= 500
                                      body = {}
                                      body[:campaignId] = @campaign_id.to_i
                                      @update_res = threesixty_api( @apitoken.to_s, @refresh_token, "group", "getIdListByCampaignId", body)
                                      
                                      if @update_res["group_getIdListByCampaignId_response"]["failures"].nil?
                                          @adgroup_id_status_body = @update_res["group_getIdListByCampaignId_response"]["groupIdList"]["item"]
                                          @remain_quote = @response.headers["quotaremain"].to_i
                                          
                                          if @adgroup_id_status_body.is_a?(Array)
                                              if @adgroup_id_status_body.count.to_i > 0
                                                  @adgroup_id_status_body.each do |adgroup_id_status_body_d|
                                                      @adgroup_id_arr << adgroup_id_status_body_d.to_i
                                                  end
                                              end
                                          else
                                              @adgroup_id_arr << @adgroup_id_status_body.to_i
                                          end
                                      end
                                  end
                                  
                                  
                                  if @adgroup_id_arr.count.to_i > 0 && @remain_quote >= 500
                                      
                                      @adgroup_id_arr_str = @adgroup_id_arr.join(",")
                                      
                                      body = {}
                                      body[:idList] = "["+@adgroup_id_arr_str.to_s+"]"
                                      
                                      
                                      @update_res = threesixty_api( @apitoken.to_s, @refresh_token, "group", "getInfoByIdList", body)
                                      
                                      if @update_res["group_getInfoByIdList_response"]["failures"].nil?
                                          
                                        
                                          @adgroup_status_body = @update_res["group_getInfoByIdList_response"]["groupList"]["item"]
                                          @remain_quote = @response.headers["quotaremain"].to_i
                                          
                                          if @adgroup_status_body.is_a?(Hash)
                                              
                                              if !@adgroup_status_body.empty?
                                                  
                                                  # @logger.info "hash"
                                                  
                                                  if @adgroup_status_body["status"].to_s == "enable"
                                                      @status = "启用"
                                                  else
                                                      @status = "暂停"
                                                  end
                                                  
                                                  db_name = "adgroup_360_"+@network_id.to_s
                                                  
                                                  result = @threesixty_db[db_name].find({ "$and" => [{:adgroup_id => @adgroup_status_body["id"].to_i}, {:campaign_id => @adgroup_status_body["campaignId"].to_i}] }).update_one('$set'=> { 
                                                                                                                                                    'adgroup_name' => @adgroup_status_body["name"].to_s,
                                                                                                                                                    'price' => @adgroup_status_body["price"].to_f,
                                                                                                                                                    'status' => @status,
                                                                                                                                                    'negative_words' => @adgroup_status_body["negativeWords"],
                                                                                                                                                    'exact_negative_words' => @adgroup_status_body["exactNegativeWords"],
                                                                                                                                                    'api_update_ad' => 2,
                                                                                                                                                    'api_update_keyword' => 2,
                                                                                                                                                    'update_date' => @now
                                                                                                                                               })
                                                  @threesixty_db.close()
                                                  
                                                  if result.n.to_i == 0
                                                      @threesixty_db[db_name].insert_one({ 
                                                                                                network_id: @network_id.to_i,
                                                                                                account_id: @account_id.to_i,
                                                                                                account_name: @account_name.to_s,
                                                                                                campaign_id: @campaign_id.to_i,
                                                                                                campaign_name: @campaign_name.to_s,
                                                                                                adgroup_id: @adgroup_status_body["id"].to_i,
                                                                                                adgroup_name: @adgroup_status_body["name"].to_s,
                                                                                                price: @adgroup_status_body["price"].to_f,
                                                                                                negative_words: @adgroup_status_body["negativeWords"],
                                                                                                exact_negative_words: @adgroup_status_body["exactNegativeWords"],
                                                                                                status: @status,
                                                                                                sys_status: "",
                                                                                                update_date: @now,                                            
                                                                                                create_date: @now 
                                                                                                })
                                                      @threesixty_db.close()
                                                  end
                                              end
                                          else
                                            
                                              if !@adgroup_status_body.nil? && @adgroup_status_body.count.to_i > 0
                                                  
                                                  @adgroup_status_body.each do |adgroup_status_body_d|
                                                      @logger.info adgroup_status_body_d
                                                         
                                                      if adgroup_status_body_d["status"].to_s == "enable"
                                                          @status = "启用"
                                                      else
                                                          @status = "暂停"
                                                      end
                                                    
                                                      db_name = "adgroup_360_"+@network_id.to_s
                                                      
                                                      result = @threesixty_db[db_name].find({ "$and" => [{:adgroup_id => adgroup_status_body_d["id"].to_i}, {:campaign_id => adgroup_status_body_d["campaignId"].to_i}] }).update_one('$set'=> { 
                                                                                                                                                        'adgroup_name' => adgroup_status_body_d["name"].to_s,
                                                                                                                                                        'price' => adgroup_status_body_d["price"].to_f,
                                                                                                                                                        'status' => @status,
                                                                                                                                                        'negative_words' => adgroup_status_body_d["negativeWords"],
                                                                                                                                                        'exact_negative_words' => adgroup_status_body_d["exactNegativeWords"],
                                                                                                                                                        'api_update_ad' => 2,
                                                                                                                                                        'api_update_keyword' => 2,
                                                                                                                                                        'update_date' => @now
                                                                                                                                                   })
                                                      @threesixty_db.close()
                                                      
                                                      if result.n.to_i == 0
                                                          
                                                          @threesixty_db[db_name].insert_one({ 
                                                                                                    network_id: @network_id.to_i,
                                                                                                    account_id: @account_id.to_i,
                                                                                                    account_name: @account_name.to_s,
                                                                                                    campaign_id: @campaign_id.to_i,
                                                                                                    campaign_name: @campaign_name.to_s,
                                                                                                    adgroup_id: adgroup_status_body_d["id"].to_i,
                                                                                                    adgroup_name: adgroup_status_body_d["name"].to_s,
                                                                                                    price: adgroup_status_body_d["price"].to_f,
                                                                                                    negative_words: adgroup_status_body_d["negativeWords"],
                                                                                                    exact_negative_words: adgroup_status_body_d["exactNegativeWords"],
                                                                                                    status: @status,
                                                                                                    sys_status: "",
                                                                                                    update_date: @now,                                            
                                                                                                    create_date: @now 
                                                                                                    })
                                                          @threesixty_db.close()
                                                      end
                                                      
                                                  end
                                              end
                                            
                                          end
                                      end
                                      
                                      
                                      
                                      # ad start
                                      if @adgroup_id_arr.count.to_i > 0 && @remain_quote >= 500
                                        
                                          @ad_id_arr = []
                                          @ad_id_arr_str = ""
                                          
                                          @keyword_id_arr = []
                                          @keyword_id_arr_str = ""
                                          
                                          @all_ad = []
                                          @all_keyword = []
                                        
                                          db_name = "ad_360_"+@network_id.to_s
                                          
                                          @adgroup_id_arr.each do |adgroup_id_arr_d|
                                              body = {}
                                              body[:groupId] = adgroup_id_arr_d.to_i
                                              @update_res = threesixty_api( @apitoken.to_s, @refresh_token, "creative", "getIdListByGroupId", body)
                                              
                                              @logger.info "||||||||||||||||||||||||||||||||||||||||||||||||"
                                              @logger.info @update_res
                                              
                                              if @update_res["creative_getIdListByGroupId_response"]["failures"].nil?
                                                  if !@update_res["creative_getIdListByGroupId_response"]["creativeIdList"].nil?
                                                      @ad_id_status_body = @update_res["creative_getIdListByGroupId_response"]["creativeIdList"]["item"]
                                                      @remain_quote = @response.headers["quotaremain"].to_i
                                                      
                                                      if @ad_id_status_body.is_a?(Array)
                                                          if @ad_id_status_body.count.to_i > 0
                                                              @ad_id_status_body.each do |ad_id_status_body_d|
                                                                  @ad_id_arr << ad_id_status_body_d.to_i
                                                              end
                                                          end
                                                      else
                                                          @ad_id_arr << @ad_id_status_body.to_i
                                                      end
                                                  end
                                              end
                                          end
                                          
                                              
                                          # @ad = @threesixty_db[db_name].find('adgroup_id' => { "$in" => @adgroup_id_arr})
                                          # @threesixty_db.close()
                                          
                                          if @ad_id_arr.count.to_i > 0
                                              # @ad.each do |ad_d|
                                                  # @ad_id_arr << ad_d["ad_id"]
                                              # end
                                              
                                              @ad_id_arr_str = @ad_id_arr.join(",")
                                              
                                              
                                              body = {}
                                              body[:idList] = "["+@ad_id_arr_str.to_s+"]"
                                              
                                              
                                              @update_res = threesixty_api( @apitoken.to_s, @refresh_token, "creative", "getInfoByIdList", body)
                                              
                                              if @update_res["creative_getInfoByIdList_response"]["failures"].nil?
                                                  if !@update_res["creative_getInfoByIdList_response"]["creativeList"].nil?
                                                      @ad_status_body = @update_res["creative_getInfoByIdList_response"]["creativeList"]["item"]
                                                      @remain_quote = @response.headers["quotaremain"].to_i
                                                      
                                                      
                                                      if @ad_status_body.is_a?(Hash)
                                                          if !@ad_status_body.empty?
                                                              @all_ad << @ad_status_body
                                                          end
                                                      else
                                                          @all_ad = @all_ad + @ad_status_body
                                                      end
                                                      
                                                      
                                                      if @all_ad.count.to_i > 0
                                                          @all_ad.each do |all_ad_d|
                                                            
                                                              if all_ad_d["status"].to_s == "enable"
                                                                  @status = "启用"
                                                              else
                                                                  @status = "暂停"
                                                              end
                                                              
                                                              url_tag = 0
                                                              m_url_tag = 0
                                                              
                                                              @final_url = all_ad_d["destinationUrl"].to_s
                                                              @m_final_url = all_ad_d["mobileDestinationUrl"].to_s
                                                              
                                                              if !@final_url.to_s.include?(".adeqo.") && @tracking_type.to_s.downcase == "adeqo" && @ad_redirect.to_s.downcase == "yes" && @final_url.to_s != ""
                                                                  @temp_final_url = @final_url
                                                                  
                                                                  @final_url = "http://t.adeqo.com/click?company_id="+@company_id.to_s+"&network_id="+@network_id.to_s
                                                                  @final_url = @final_url + "&campaign_id={planid}&adgroup_id={groupid}&ad_id={creativeid}&keyword_id={wordid}"
                                                                  @final_url = @final_url + "&cookie="+@cookie_length.to_s
                                                                  @final_url = @final_url + "&device=pc"
                                                                  @final_url = @final_url + "&tv=v1&durl="+CGI.escape(@temp_final_url.to_s)
                                                                  
                                                                  url_tag = 1
                                                              end
                                                              
                                                              if !@m_final_url.to_s.include?(".adeqo.") && @tracking_type.to_s.downcase == "adeqo" && @ad_redirect.to_s.downcase == "yes" && @m_final_url.to_s != ""
                                                                  @temp_m_final_url = @m_final_url
                                                                  
                                                                  @m_final_url = "http://t.adeqo.com/click?company_id="+@company_id.to_s+"&network_id="+@network_id.to_s
                                                                  @m_final_url = @m_final_url + "&campaign_id={planid}&adgroup_id={groupid}&ad_id={creativeid}&keyword_id={wordid}"
                                                                  @m_final_url = @m_final_url + "&cookie="+@cookie_length.to_s
                                                                  @m_final_url = @m_final_url + "&device=mobile"
                                                                  @m_final_url = @m_final_url + "&tv=v1&durl="+CGI.escape(@temp_m_final_url.to_s)
                                                                  
                                                                  m_url_tag = 1
                                                              end
                                                              
                                                              begin
                                                                  if url_tag == 1 || m_url_tag == 1
                                                                      
                                                                      if @remain_quote.to_i >= 500
                                                                        
                                                                          requesttypearray = []
                                                                          request_str = '{"id":'+all_ad_d["id"].to_s+',"destinationUrl":"'+@final_url+'","mobileDestinationUrl":"'+@m_final_url+'"}'
                                                                          
                                                                          requesttypearray << request_str
                                                                          request = '['+requesttypearray.join(",")+']'
                                                                          
                                                                          # @logger.info request
                                                                          
                                                                          body = { 
                                                                              'creatives' => request
                                                                          }
                                                                          
                                                                          @update_res = threesixty_api( @apitoken.to_s, @refresh_token, "creative", "update", body)
                                                                          @affectedRecords = @update_res["creative_update_response"]["affectedRecords"]
                                                                          @remain_quote = @response.headers["quotaremain"].to_i
                                                                          
                                                                          @logger.info @update_res["creative_update_response"]
                                                                          
                                                                          if !@update_res["creative_update_response"]["failures"].nil?
                                                                              @final_url = all_ad_d["destinationUrl"].to_s
                                                                              @m_final_url = all_ad_d["mobileDestinationUrl"].to_s
                                                                          end
                                                                      end
                                                                  end
                                                              rescue Exception
                                                                  @final_url = all_ad_d["destinationUrl"].to_s
                                                                  @m_final_url = all_ad_d["mobileDestinationUrl"].to_s
                                                              end
                                                              
                                                              
                                                              db_name = "ad_360_"+@network_id.to_s
                                                              
                                                              result = @threesixty_db[db_name].find({ "$and" => [{:ad_id => all_ad_d["id"].to_i}, {:adgroup_id => all_ad_d["groupId"].to_i}] }).update_one('$set'=> { 
                                                                                                                                                                                            'title' => all_ad_d["title"].to_s,
                                                                                                                                                                                            'description_1' => all_ad_d["description1"].to_s,
                                                                                                                                                                                            'description_2' => all_ad_d["description2"].to_s,
                                                                                                                                                                                            'visit_url' => @final_url.to_s,
                                                                                                                                                                                            'show_url' => all_ad_d["displayUrl"].to_s,
                                                                                                                                                                                            'mobile_visit_url' => @m_final_url.to_s,
                                                                                                                                                                                            'mobile_show_url' => all_ad_d["mobileDisplayUrl"].to_s,
                                                                                                                                                                                            'status' => @status,
                                                                                                                                                                                            'update_date' => @now
                                                                                                                                                                                            
                                                                                                                                                                                       })
                                                              @threesixty_db.close()
                                                               
                                                              if result.n.to_i == 0
                                                                                                        
                                                                  @threesixty_db[db_name].insert_one({ 
                                                                              network_id: @network_id.to_i,
                                                                              account_id: @account_id.to_i,
                                                                              account_name: @account_name.to_s,
                                                                              campaign_id: @campaign_id.to_i,
                                                                              campaign_name: @campaign_name.to_s,
                                                                              adgroup_id: all_ad_d["groupId"].to_i,
                                                                              ad_id: all_ad_d["id"].to_i,
                                                                              title: all_ad_d["title"].to_s, 
                                                                              description_1: all_ad_d["description1"].to_s,
                                                                              description_2: all_ad_d["description2"].to_s, 
                                                                              status: @status.to_s,
                                                                              sys_status: "",
                                                                              show_url: all_ad_d["displayUrl"].to_s,
                                                                              visit_url: @final_url.to_s,
                                                                              mobile_show_url: all_ad_d["mobileDisplayUrl"].to_s,
                                                                              mobile_visit_url: @m_final_url.to_s,
                                                                              extend_ad_type: 0,
                                                                              update_date: @now,                                            
                                                                              create_date: @now 
                                                                              })
                                                                  @threesixty_db.close()
                                                              end
                                                          end
                                                      end
                                                  end
                                              end 
                                          end
                                          
                                          
                                          
                                          # ad done
                                          
                                          
                                          @adgroup_id_arr.each do |adgroup_id_arr_d|
                                              body = {}
                                              body[:groupId] = adgroup_id_arr_d.to_i
                                              @update_res = threesixty_api( @apitoken.to_s, @refresh_token, "keyword", "getIdListByGroupId", body)
                                              
                                              if @update_res["keyword_getIdListByGroupId_response"]["failures"].nil?
                                                  if !@update_res["keyword_getIdListByGroupId_response"]["keywordIdList"].nil?
                                                      @keyword_id_status_body = @update_res["keyword_getIdListByGroupId_response"]["keywordIdList"]["item"]
                                                      @remain_quote = @response.headers["quotaremain"].to_i
                                                      
                                                      if @keyword_id_status_body.is_a?(Array)
                                                          if @keyword_id_status_body.count.to_i > 0
                                                              @keyword_id_status_body.each do |keyword_id_status_body_d|
                                                                  @keyword_id_arr << keyword_id_status_body_d.to_i
                                                              end
                                                          end
                                                      else
                                                          @keyword_id_arr << @keyword_id_status_body.to_i
                                                      end
                                                  end
                                              end
                                          end
                                          
                                          
                                          if @keyword_id_arr.count.to_i > 0
                                              
                                              @keyword_id_arr_str = @keyword_id_arr.join(",")
                                              
                                              body = {}
                                              body[:idList] = "["+@keyword_id_arr_str.to_s+"]"
                                              
                                              @update_res = threesixty_api( @apitoken.to_s, @refresh_token, "keyword", "getInfoByIdList", body)
                                              
                                              if @update_res["keyword_getInfoByIdList_response"]["failures"].nil?
                                                  if !@update_res["keyword_getInfoByIdList_response"]["keywordList"].nil?
                                                      @keyword_status_body = @update_res["keyword_getInfoByIdList_response"]["keywordList"]["item"]
                                                      @remain_quote = @response.headers["quotaremain"].to_i
                                                      
                                                      
                                                      if @keyword_status_body.is_a?(Hash)
                                                          if !@keyword_status_body.empty?
                                                              @all_keyword << @keyword_status_body
                                                          end
                                                      else
                                                          @all_keyword = @all_keyword + @keyword_status_body
                                                      end
                                                  end
                                              end
                                          end
                                          
                                          
                                          if @all_keyword.count.to_i > 0
                                              @all_keyword.each do |all_keyword_d|
                                                
                                                  # @logger.info all_keyword_d
                                                  
                                                  if all_keyword_d["status"].to_s == "enable"
                                                      @status = "启用"
                                                  else
                                                      @status = "暂停"
                                                  end
                                                  
                                                  if all_keyword_d["match_type"].to_s.downcase.include?("intelligence")
                                                      @match = "智能短语"
                                                  elsif all_keyword_d["match_type"].to_s.downcase == "phrase"
                                                      @match = "短语"
                                                  elsif all_keyword_d["match_type"].to_s.downcase == "exact"
                                                      @match = "精确"
                                                  else
                                                      @match = "广泛"
                                                  end
                                                  
                                                  
                                                  url_tag = 0
                                                  m_url_tag = 0
                                                  
                                                  @final_url = all_keyword_d["destinationUrl"].to_s
                                                  @m_final_url = all_keyword_d["mobileDestinationUrl"].to_s
                                                  
                                                  if !@final_url.to_s.include?(".adeqo.") && @tracking_type.to_s.downcase == "adeqo" && @ad_redirect.to_s.downcase == "yes" && @final_url.to_s != ""
                                                      @temp_final_url = @final_url
                                                      
                                                      @final_url = "http://t.adeqo.com/click?company_id="+@company_id.to_s+"&network_id="+@network_id.to_s
                                                      @final_url = @final_url + "&campaign_id={planid}&adgroup_id={groupid}&ad_id={creativeid}&keyword_id="+all_keyword_d["id"].to_s
                                                      @final_url = @final_url + "&cookie="+@cookie_length.to_s
                                                      @final_url = @final_url + "&device=pc"
                                                      @final_url = @final_url + "&tv=v1&durl="+CGI.escape(@temp_final_url.to_s)
                                                      
                                                      url_tag = 1
                                                  end
                                                  
                                                  if !@m_final_url.to_s.include?(".adeqo.") && @tracking_type.to_s.downcase == "adeqo" && @ad_redirect.to_s.downcase == "yes" && @m_final_url.to_s != ""
                                                      @temp_m_final_url = @m_final_url
                                                      
                                                      @m_final_url = "http://t.adeqo.com/click?company_id="+@company_id.to_s+"&network_id="+@network_id.to_s
                                                      @m_final_url = @m_final_url + "&campaign_id={planid}&adgroup_id={groupid}&ad_id={creativeid}&keyword_id="+all_keyword_d["id"].to_s
                                                      @m_final_url = @m_final_url + "&cookie="+@cookie_length.to_s
                                                      @m_final_url = @m_final_url + "&device=mobile"
                                                      @m_final_url = @m_final_url + "&tv=v1&durl="+CGI.escape(@temp_m_final_url.to_s)
                                                      
                                                      m_url_tag = 1
                                                  end
                                                  
                                                  begin
                                                      if url_tag == 1 || m_url_tag == 1
                                                          
                                                          if @remain_quote.to_i >= 500
                                                            
                                                              requesttypearray = []
                                                              request_str = '{"id":'+all_keyword_d["id"].to_s+',"url":"'+@final_url+'","mobileUrl":"'+@m_final_url+'"}'
                                                              
                                                              requesttypearray << request_str
                                                              request = '['+requesttypearray.join(",")+']'
                                                              
                                                              # @logger.info request
                                                              
                                                              body = { 
                                                                  'keywords' => request
                                                              }
                                                              
                                                              @update_res = threesixty_api( @apitoken.to_s, @refresh_token, "keyword", "update", body)
                                                              @affectedRecords = @update_res["keyword_update_response"]["affectedRecords"]
                                                              @remain_quote = @response.headers["quotaremain"].to_i
                                                              
                                                              
                                                              if !@update_res["creative_update_response"]["failures"].nil?
                                                                  @final_url = all_keyword_d["destinationUrl"].to_s
                                                                  @m_final_url = all_keyword_d["mobileDestinationUrl"].to_s
                                                              end
                                                          end
                                                      end
                                                  rescue Exception
                                                      @final_url = all_keyword_d["destinationUrl"].to_s
                                                      @m_final_url = all_keyword_d["mobileDestinationUrl"].to_s
                                                  end
                                                  
                                                  
                                                  # @logger.info all_keyword_d["id"].to_i
                                                  db_name = "keyword_360_"+@network_id.to_s
                                                  
                                                  result = @threesixty_db[db_name].find({ "$and" => [{:keyword_id => all_keyword_d["id"].to_i}, {:adgroup_id => all_keyword_d["groupId"].to_i}] }).update_one('$set'=> { 
                                                                                                                                                                                  'keyword' => all_keyword_d["word"].to_s,
                                                                                                                                                                                  'price' => all_keyword_d["price"].to_f,
                                                                                                                                                                                  'visit_url' => @final_url.to_s,
                                                                                                                                                                                  'mobile_visit_url' => @m_final_url.to_s,
                                                                                                                                                                                  'match_type' => @match.to_s,
                                                                                                                                                                                  'status' => @status,
                                                                                                                                                                                  'update_date' => @now
                                                                                                                                                                                  
                                                                                                                                                                             })
                                                  @threesixty_db.close()
                                                  
                                                  if result.n.to_i == 0
                                                                         
                                                      @threesixty_db[db_name].insert_one({ 
                                                              network_id: @network_id.to_i,
                                                              account_id: @account_id.to_i,
                                                              account_name: @account_name.to_s,
                                                              campaign_id: @campaign_id.to_i,
                                                              campaign_name: @campaign_name.to_s,
                                                              adgroup_id: all_keyword_d["groupId"].to_i,
                                                              keyword_id: all_keyword_d["id"].to_i,
                                                              keyword: all_keyword_d["word"].to_s,
                                                              price: all_keyword_d["price"].to_f, 
                                                              status: @status,
                                                              sys_status: "",
                                                              match_type: @match.to_s,
                                                              visit_url: @final_url.to_s,
                                                              mobile_visit_url: @m_final_url.to_s,
                                                              cpc_quality: 0,
                                                              # negative_words: csv[@keyword_negative_index].to_s,
                                                              extend_ad_type: 0,
                                                              update_date: @now,                                            
                                                              create_date: @now 
                                                              })
                                                              
                                                      @threesixty_db.close()  
                                                  end
                                              end
                                          end
                                            
                                          db_name = "adgroup_360_"+@network_id.to_s
                                          @threesixty_db[db_name].find('adgroup_id' => { "$in" => @adgroup_id_arr}).update_many('$set'=> { 
                                                                                                                                          'api_update_ad' => 0,
                                                                                                                                          'api_update_keyword' => 0
                                                                                                                                          })
                                          @threesixty_db.close()    
                                          
                                      end
                                      
                                      
                                  end
                                  
                                  
                                  
                                  # data = {:tmp => @all_keyword, :tmsp => "sasd", :status => "true"}
                                  # return render :json => data, :status => :ok
                                  
                                  
                                  if !@campaign_status_body.nil?
                                      
                                      @db["all_campaign"].find({ "$and" => [{:campaign_id => @campaign_id.to_i}, {:network_type => "360"}] }).update_one('$set'=> { 
                                                                                                                                                 'campaign_name' => @campaign_status_body["name"],
                                                                                                                                                 'budget' => @campaign_status_body["budget"].to_f,
                                                                                                                                                 'regions' => @campaign_status_body["region"],
                                                                                                                                                 'schedule' => @campaign_status_body["schedule"],
                                                                                                                                                 'start_date' => @campaign_status_body["startDate"],
                                                                                                                                                 'end_date' => @campaign_status_body["endDate"],
                                                                                                                                                 'status' => @campaign_status,
                                                                                                                                                 'extend_ad_type' => @campaign_status_body["extendAdType"],
                                                                                                                                                 'mobile_price_rate' => @campaign_status_body["mobilePriceRate"].to_i
                                                                                                                                               })
                                      @db.close
                                  end
                              end
                              
                          end
                          
                      end
                     
                      # data = {:tmp => @update_res, :status => "true"}
                      # return render :json => data, :status => :ok
                  end
                  
                  
                  
              end
              
              
              @db["all_campaign"].find({ "$and" => [{:campaign_id => @campaign_id.to_i}, {:network_type => "360"}] }).update_one('$set'=> { 
                                                                                                                         'api_worker' => "",
                                                                                                                         'update_date' => @now,
                                                                                                                         'api_update' => 0
                                                                                                                       })
              @db.close
              
              
              @list_campaign = @db["all_campaign"].find( '$and' => [ { 'api_update' => { '$exists' => true } }, {'network_id' => @network_id.to_i}, {'network_type' => "360"},{'api_update' => { "$ne" => 0}},{'api_update' => { "$ne" => 0}} ])
              @db.close
              
              if @list_campaign.count.to_i == 0
                  @db[:network].find({ "$and" => [{:id => @network_id.to_i}, {:type => "360"}] }).update_one('$set'=> {'file_update_1' => 4,'file_update_2' => 4,'file_update_3' => 4,'file_update_4' => 4, 'last_update' => @now})
                  @db.close
              end
                  
          end    
      end
    
      @logger.info "360 api done start"
      return render :nothing => true
  end
  
  
  
  
  
  
  
  def redownload(networkid)
      
      @redownload_network = @db[:network].find({ "$and" => [{:id => networkid.to_i}, {:type => "360"}] })
      @db.close
    
      if @redownload_network.count.to_i == 1
          
          @redownload_network.no_cursor_timeout.each do |doc|
              if doc["tmp_file"] != ""
                  unzip_folder = @tmp+"/"+doc["tmp_file"]+".csv"
                  if File.exists?(unzip_folder)
                    # FileUtils.remove_dir unzip_folder, true
                    File.delete(unzip_folder)
                  end
              end
          end
          
          @db[:network].find(id: networkid.to_i).update_one('$set'=> { 'tmp_file' => "" , 'fileid' => "",'run_time' => 0, 'file_update_1' => 0, 'file_update_2' => 0, 'file_update_3' => 0, 'file_update_4' => 0, 'last_update' => @now, 'worker' => ""})  
          @db.close
      end
  end


  def resetnetwork
    
    @logger.info "start reset 360 network api status"
    
    @id = params[:id]
    if @id.nil?
      
        @network = @db[:network].find({ "$and" => [{:type => '360'}, {:file_update_1 => 4}, {:file_update_2 => 4}, {:file_update_3 => 4}, {:file_update_4 => 4}] })
        @db.close
        
    else
        @network = @db[:network].find({ "$and" => [{:id => @id.to_i}, {:type => '360'}] })
        @db.close
    end
    
    
    @network.no_cursor_timeout.each do |doc|
      
      @logger.info "start reset 360 network api status "+doc['id'].to_s
            
      @db[:network].find(id: doc["id"].to_i).update_one('$set'=> { 'file_update_1' => 2, 'file_update_2' => 2, 'file_update_3' => 2, 'file_update_4' => 2, 'last_update' => @now}) 
      @db.close
    end
    
    @logger.info "done reset 360  network api status"
    return render :nothing => true
  end



  def resetdlfile
    
    @logger.info "start reset 360 api download file"
    
    @id = params[:id]
    if @id.nil?
        @network = @db[:network].find({ "$and" => [{:type => '360'}, {:file_update_1 => { '$gte' => 4 }}, {:file_update_2 => { '$gte' => 4 }}, {:file_update_3 => { '$gte' => 4 }}, {:file_update_4 => { '$gte' => 4 }}] })
        @db.close
    else
        @network = @db[:network].find({ "$and" => [{:id => @id.to_i}, {:type => '360'}] })
        @db.close
    end
    
    
    @network.no_cursor_timeout.each do |doc|
      
      @logger.info "start reset 360 api download file "+doc['id'].to_s
      
      if doc['tmp_file'] != ""
        unzip_folder = @tmp+"/"+doc["tmp_file"]+".csv"
        if File.exists?(unzip_folder)
          # FileUtils.remove_dir unzip_folder, true
          File.delete(unzip_folder)
        end
      end
      
      
      @db[:network].find(id: doc["id"].to_i).update_one('$set'=> { 'run_time' => 0, 'tmp_file' => "", 'fileid' => "", 'file_update_1' => 0, 'file_update_2' => 0, 'file_update_3' => 0, 'file_update_4' => 0, 'worker' => "", 'last_update' => @now})
      @db.close
    end
    
    @logger.info "done reset 360 api download file"
    return render :nothing => true
  end   
  
  
  
  

  def threesixty_api( api_key, access_token, service, method, params = {})
    url = "https://api.e.360.cn/2.0/#{service}/#{method}"
      response = HTTParty.post(url,
            timeout: 300, 
            body: params,
            headers: {
                        'apiKey' => api_key, 
                        'accessToken' => access_token, 
                        'serveToken' => Time.now.to_i.to_s  
                      })

      @response = response                      
      return response.parsed_response
  end
  
  
  
  def login(username,password,api_key,api_secret)
    cipher_aes = OpenSSL::Cipher::AES.new(128, :CBC)
    cipher_aes.encrypt
    cipher_aes.key = api_secret[0,16]
    cipher_aes.iv = api_secret[16,16]
    encrypted = (cipher_aes.update(Digest::MD5.hexdigest(password)) + cipher_aes.final).unpack('H*').join
    url = "https://api.e.360.cn/account/clientLogin"
    response = HTTParty.post(url,
        :timeout => 300,
        :body => {
        :username => username,
        :passwd => encrypted[0,64]
        },
        :headers => {'apiKey' => api_key }
    )
    
    return response.parsed_response
  end
  
  
  def getthreesixtyfile(networkid,username,password,apitoken,apisecret,fileid,tmpfile)
      
      @logger.info "getthreesixtyfile start "+networkid.to_s
      @run_csv = 0
      @tmp_file_path = ""
      @fileid = fileid.to_s
      
      begin
          login_info = login(username.to_s,password.to_s,apitoken.to_s,apisecret.to_s)            
          @refresh_token = login_info["account_clientLogin_response"]["accessToken"]
          
          if @refresh_token.nil?
              # data = {:message => "API Info not correct", :status => "false"}
              # return render :json => data, :status => :ok
              
              @logger.info "getthreesixtyfile login fail" + login_info.to_s
          else
            
              if tmpfile.to_s == "" && fileid.to_s == ""
                  @logger.info "get file id for network: "+networkid.to_s
                  getfileid = threesixty_api( apitoken.to_s, @refresh_token, "account", "getAllObjects", nil)
                  getfileid = getfileid["account_getAllObjects_response"]["fileId"]
                  
                  @logger.info "insert file id "+networkid.to_s
                  if getfileid.to_s != ""
                      @db[:network].find(id: networkid.to_i).update_one('$set'=> {
                                                                            'fileid' => getfileid.to_s                                                                       
                                                                          })
                      @db.close
                      fileid = getfileid                                                                   
                  end
              end
              
              # trace only
              @fileid = fileid.to_s
              # trace only
              
              if fileid != ""
                  @logger.info "dl file start"
                  request = { 'fileId' => fileid }
                  file_status = threesixty_api( apitoken.to_s, @refresh_token, "account", "getFileState", request)
                  
                  # trace only
                  @filestatus = file_status["account_getFileState_response"]["isGenerated"].to_s
                  # trace only
                  @logger.info "file status is "+@filestatus.to_s+" for network id: "+networkid.to_s
                  
                  if file_status["account_getFileState_response"]["isGenerated"].to_s == "success"
                      @logger.info "file path done  for network id: "+networkid.to_s
                      @tmp_file_path = file_status["account_getFileState_response"]["filePath"].to_s    
                      @run_csv = 1                                                
                  end
                  
                  if file_status["account_getFileState_response"]["isGenerated"].to_s == "fail"
                      @logger.info "get file fail, reset "+networkid.to_s
                      @db[:network].find(id: networkid.to_i).update_one('$set'=> {
                                                                            'tmp_file' => "",
                                                                            'fileid' => "",
                                                                            "file_update_1"=> 0, 
                                                                            "file_update_2"=> 0, 
                                                                            "file_update_3"=> 0, 
                                                                            "file_update_4"=> 0                                                                       
                                                                          })
                      @db.close                                                    
                      tmpfile = ""
                  end
              end
              
              if tmpfile.to_s != ""
                  @logger.info "already have file id, run "+networkid.to_s
                  @run_csv = 1
                  @fileid = tmpfile.to_s
              end
          
          end
          @logger.info "getthreesixtyfile done "
      rescue Exception
          @run_csv = 0
          @tmp_file_path = ""
          @logger.info "getthreesixtyfile fail "
      end
      
  end
  
  
  
  def csvdetail(acc_file_id,acc_file_path, table, networkid)
            
      @logger.info "csvdetail start "+networkid.to_s
      @zip_file = @tmp+"/"+acc_file_id+".zip"
      
      @logger.info acc_file_path.to_s
      
      # begin
          if acc_file_path.to_s != ""
              @logger.info "download file start "+networkid.to_s 
              @zip_file = @tmp+"/"+acc_file_id + ".zip"
              open(@zip_file.to_s, 'wb') do |file|
                file << open(acc_file_path.to_s).read
              end
              @logger.info "download file done"
              
              @db[:network].find(id: networkid.to_i).update_one('$set'=> {
                                                                    'tmp_file' => acc_file_id.to_s,
                                                                    'fileid' => "",                                                                       
                                                                    'file_path' => acc_file_path.to_s
                                                                  })
              @db.close                                                        
          end
          
          
          @logger.info "read csv data start "+networkid.to_s
          @file = @zip_file
          # @three_sixty_csv = CSV.read(@file, :encoding => 'GB18030')
          
          @logger.info "read csv data done "+networkid.to_s
          @logger.info "csvdetail done "+networkid.to_s
      # rescue Exception
          # if File.exists?(@zip_file)
            # File.delete(@zip_file)
          # end
          # @file = ""
          # @logger.info "csvdetail fail "+networkid.to_s
      # end
  end
  
  
  
  def updateaccount
    
      @id = params[:id]
      
      if @id.nil?
          @network = @db[:network].find('type' => '360')
          @db.close
      else
          @network = @db[:network].find({ "$and" => [{:id => @id.to_i}, {:type => '360'}] })
          @db.close
      end
      
      @network.no_cursor_timeout.each do |doc|
          
          login_info = login(doc[:username].to_s,doc[:password].to_s,doc[:api_token].to_s,doc[:api_secret].to_s)
          @refresh_token = login_info["account_clientLogin_response"]["accessToken"]
          
          @logger.info @refresh_token
          
          if !@refresh_token.nil?
          
              @account_info = threesixty_api( doc[:api_token].to_s, @refresh_token, "account", "getInfo", nil)
              @account_info = @account_info["account_getInfo_response"] 
              
              # data = {:message => "360 index", :datas => @account_info, :id => doc['id'], :status => "true"}
              # return render :json => data, :status => :ok 
              
              @accountid = @account_info["uid"]
              @email = @account_info["email"]
              @category = @account_info["category"]
              @industry1 = @account_info["industry1"]
              @industry2 = @account_info["industry2"]
              @balance = @account_info["balance"]
              # @budget = @account_info["budget"]
              @mvBudget = @account_info["mvBudget"]
              @resources = @account_info["resources"]
              @domains = @account_info["allowDomain"]
              @mobile_domains = @account_info["allowMobileDomain"]
              @status = @account_info["status"]
              
              @db[:network].find(id: doc["id"].to_i).update_one('$set'=> { 
                                                                            'balance' => @balance.to_f,
                                                                            'mvbudget' => @mvBudget.to_f,
                                                                            'domains' => @domains.to_s,
                                                                            'category' => @category.to_s,
                                                                            'industry1' => @industry1.to_s,
                                                                            'industry2' => @industry2.to_s,
                                                                            'mobile_domains' => @mobile_domains.to_s,
                                                                            'status' => @status                                                 
                                                                          })
              @db.close    
          end                                                        
      end
      
      return render :nothing => true
  end
  
  
  def dlaccfile
    
    @logger.info "called 360 structure download file only"
    
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
        @current_network = @db[:network].find({ "$and" => [{:type => '360'}, {:file_update_1 => 1}, {:file_update_2 => 1}, {:file_update_3 => 1}, {:file_update_4 => 1}, {:worker => @port.to_i}] }).sort({ last_update: -1 }).limit(1)
        @db.close
        
        if @current_network.count.to_i >= 1
            @logger.info "360 dl working"
            return render :nothing => true
        end
        @network = @db[:network].find({ "$and" => [{:type => '360'}, {:file_update_1 => 0}, {:file_update_2 => 0}, {:file_update_3 => 0}, {:file_update_4 => 0}, {:worker => @port.to_i}] }).sort({ last_update: -1 }).limit(1)
        @db.close
        
        if @network.count.to_i == 0
            @network = @db[:network].find({ "$and" => [{:type => '360'}, {:file_update_1 => 0}, {:file_update_2 => 0}, {:file_update_3 => 0}, {:file_update_4 => 0}, {:worker => ""}] }).sort({ last_update: -1 }).limit(1)
            @db.close
          
            if @network.count.to_i == 0
                @logger.info "no need to dl 360"
                return render :nothing => true
            end
        end
        
    else
        @network = @db[:network].find({ "$and" => [{:id => @id.to_i}, {:type => '360'}] })
        @db.close
    end
    
    
    @network.no_cursor_timeout.each do |doc|
              
        login_info = login(doc["username"].to_s,doc["password"].to_s,doc["api_token"].to_s,doc["api_secret"].to_s)            
        @refresh_token = login_info["account_clientLogin_response"]["accessToken"]
        
        @logger.info "360 dlaccfile " + doc['id'].to_s + " running"
        
        if @refresh_token.nil?
            @logger.info "360 dl acc file " + doc['id'].to_s + " " +login_info.to_s
            
            if !login_info["account_clientLogin_response"]["failures"].nil? && login_info["account_clientLogin_response"]["failures"]["item"]["code"].to_s == "70001"
                @db[:network].find(id: doc["id"].to_i).update_one('$set'=> { 'fileid' => "", 'tmp_file' => "",'run_time' => 0,'file_update_1' => 4, 'file_update_2' => 4, 'file_update_3' => 4, 'file_update_4' => 4, 'worker' => "", 'last_update' => @now })
                @db.close                            
            end
        else
            if doc["run_time"].to_i >= 10
                @db[:network].find(id: doc["id"].to_i).update_one('$set'=> { 'fileid' => "", 'tmp_file' => "",'run_time' => 0,'file_update_1' => 0, 'file_update_2' => 0, 'file_update_3' => 0, 'file_update_4' => 0, 'worker' => "", 'last_update' => @now })
                @db.close
            else
                @db[:network].find(id: doc["id"].to_i).update_one('$set'=> {'file_update_1' => 1, 'file_update_2' => 1, 'file_update_3' => 1, 'file_update_4' => 1, 'worker' => @port.to_i, 'last_update' => @now})
                @db.close    
                getthreesixtyfile(doc["id"].to_s, doc["username"],doc["password"],doc["api_token"],doc["api_secret"].to_s, doc["fileid"].to_s, doc["tmp_file"].to_s)
                
                if @run_csv.to_i == 1
                      
                    @logger.info "360 network " + doc['id'].to_s + " done download csv"
                    csvdetail(@fileid.to_s, @tmp_file_path, "campaign", doc["id"].to_s)
                    
                    if File.exists?(@file)
                        @db[:network].find(id: doc["id"].to_i).update_one('$set'=> {'file_update_1' => 2, 'file_update_2' => 2, 'file_update_3' => 2, 'file_update_4' => 2, 'worker' => @port.to_i, 'last_update' => @now})
                        @db.close
                    else
                        @db[:network].find(id: doc["id"].to_i).update_one('$set'=> { 'tmp_file' => "" , 'fileid' => "",'run_time' => 0, 'file_update_1' => 0, 'file_update_2' => 0, 'file_update_3' => 0, 'file_update_4' => 0, 'worker' => "", 'last_update' => @now})
                        @db.close
                    end
                else
                    run_time = doc["run_time"].to_i + 1
                    @db[:network].find(id: doc["id"].to_i).update_one('$set'=> { 'run_time' => run_time.to_i, 'file_update_1' => 0, 'file_update_2' => 0, 'file_update_3' => 0, 'file_update_4' => 0, 'worker' => @port.to_i, 'last_update' => @now})
                    @db.close
                end
            end
        end
        @logger.info "360 structure, network "+doc["id"].to_s+ " done"

    end
    
    @logger.info "called 360 structure download file only done"     
    return render :nothing => true   
  end
  
  
  
  
  
  
  def campaign
    @logger.info "called 360 structure campaign"
    
    @id = params[:id]
    if @id.nil?
        @current_network = @db[:network].find({ '$or' => [ {'file_update_1' => 3},{'file_update_2' => 3},{'file_update_3' => 3},{'file_update_4' => 3} ],'type' => '360', 'worker' => @port.to_i})
        @db.close
        
        if @current_network.count.to_i >= 3
            @logger.info "working, no need to update 360 campaign"
            return render :nothing => true
        end
      
        # @network = @db[:network].find('type' => '360', 'file_update_1' => {'$gte' => 2}, 'file_update_1' => {'$lt' => 4}).limit(1)
        @network = @db[:network].find('type' => '360', 'file_update_1' => 2, 'worker' => @port.to_i).sort({ last_update: -1 }).limit(1)
        @db.close
        
        if @network.count.to_i == 0
            @logger.info "no need to update 360 campaign"
            return render :nothing => true
        end
    else
        @network = @db[:network].find('type' => '360', 'id' => @id.to_i)
        @db.close
    end
    
    
    @network.no_cursor_timeout.each do |doc|
          # begin
              @do = 1
              
              if doc['tmp_file'].to_s != ""
                  @tmp_file = "/datadrive/"+ doc['tmp_file'].to_s + ".csv"
                  if !File.exists?(@tmp_file)
                      @db[:network].find(id: doc["id"].to_i).update_one('$set'=> { 'fileid' => "", 'tmp_file' => "",'run_time' => 0,'file_update_1' => 0, 'file_update_2' => 0, 'file_update_3' => 0, 'file_update_4' => 0 })
                      @db.close
                      
                      @do = 0
                      @logger.info "need to re download structure" + doc['id'].to_s
                  end
              end
              
              
              if @do == 1
                  login_info = login(doc["username"].to_s,doc["password"].to_s,doc["api_token"].to_s,doc["api_secret"].to_s)            
                  @refresh_token = login_info["account_clientLogin_response"]["accessToken"]
                  
                  if !@refresh_token.nil?
                      
                      getthreesixtyfile(doc["id"].to_s, doc["username"],doc["password"],doc["api_token"],doc["api_secret"].to_s, doc["fileid"].to_s, doc["tmp_file"].to_s)
                      
                      if @run_csv.to_i == 1
                            
                          @logger.info "360 network " + doc['id'].to_s + " done download csv/have csv"
                          csvdetail(@fileid.to_s, @tmp_file_path, "not_using", doc["id"].to_s)
                          
                          if File.exists?(@file)
                              #remove first
                              @db[:network].find(id: doc["id"].to_i).update_one('$set'=> {'file_update_1' => 3})
                              @db.close
                              
                              @logger.info "360 network " + doc['id'].to_s + " campaign clean up first"
                              
                              @db["all_campaign"].find(network_id: doc["id"].to_i, 'network_type' => "360").delete_many
                              @db.close

                              @logger.info "360 network " + doc['id'].to_s + " update campaign"
                              CSV.foreach(@file, :encoding => 'GB18030').each_with_index do |csv, index|
                              
                                  if index.to_i == 0
                                      set_csv_header(csv)  
                                  end
                                  
                                  begin
                                      if index.to_i != 0
                                                                            
                                          if csv[@adgroup_name_index].nil? && csv[@keyword_index].nil? && csv[@ad_title_index].nil? && !csv[@update_time_index].nil? && !csv[@create_time_index].nil? && !csv[@campaign_status_index].nil? && !csv[@campaign_sys_status_index].nil?
                                              
                                              @db["all_campaign"].insert_one({ 
                                                          network_id: doc["id"].to_i,
                                                          network_type: "360", 
                                                          account_id: csv[@account_id_index].to_i,
                                                          account_name: csv[@account_name_index].to_s,
                                                          campaign_id: csv[@id_index].to_i,
                                                          campaign_name: csv[@campaign_name_index].to_s, 
                                                          budget: csv[@budget_index].to_f, 
                                                          regions: csv[@campaign_region_index].to_s, 
                                                          schedule: csv[@campaign_schedule_index].to_s,
                                                          start_date: csv[@campaign_start_time_index].to_s,
                                                          end_date: csv[@campaign_end_time_index].to_s,
                                                          status: csv[@campaign_status_index].to_s,
                                                          sys_status: csv[@campaign_sys_status_index].to_s,
                                                          extend_ad_type: csv[@extend_ad_type_index].to_i,
                                                          negative_words: csv[@campaign_negative_index].to_s,
                                                          exact_negative_words: csv[@campaign_exact_negative_mode_index].to_s,
                                                          mobile_price_rate: csv[@mobile_search_price_index].to_f,
                                                          update_date: csv[@update_time_index].to_s,                                            
                                                          create_date: csv[@create_time_index].to_s  
                                                        })
                                              @db.close          
                                          end
                                      end
                                  rescue Exception
                                      @db[:network].find(id: doc["id"].to_i).update_one('$set'=> {'file_update_1' => 2})
                                      @db.close
                                  end     
                              end
                              
                              # updateaccount
                              @db[:network].find(id: doc["id"].to_i).update_one('$set'=> {'file_update_1' => 4, 'last_update' => @now})
                              @db.close
                          else
                              @db[:network].find(id: doc["id"].to_i).update_one('$set'=> { 'tmp_file' => "" , 'fileid' => "", 'file_update_1' => 0, 'file_update_2' => 0, 'file_update_3' => 0, 'file_update_4' => 0})
                              @db.close
                          end
                      end
                  end
                  @logger.info "360 structure campaign, network "+doc["id"].to_s+ " done"
              end
          # rescue Exception
              # @db[:network].find(id: doc["id"].to_i).update_one('$set'=> {'file_update_1' => 2})
              # @db.close
          # end
    end 
    
    @logger.info "360 structure done campaign"     
    return render :nothing => true 
  end
  
  
  
  
  
  
  
  def adgroup
    @logger.info "called 360 structure adgroup"
    
    @id = params[:id]
    if @id.nil?
        
        # @current_network = @db[:network].find('type' => '360', 'file_update_1' => 4, 'file_update_2' => 3)
        @current_network = @db[:network].find({ '$or' => [ {'file_update_1' => 3},{'file_update_2' => 3},{'file_update_3' => 3},{'file_update_4' => 3} ]})
        @db.close
        
        if @current_network.count.to_i >= 6
            @logger.info "working, no need to update 360 adgroup"
            return render :nothing => true
        end
      
        # @network = @db[:network].find('type' => '360', 'file_update_1' => 4, 'file_update_2' => {'$gte' => 2}, 'file_update_2' => {'$lt' => 4}).limit(1)
        @network = @db[:network].find('type' => '360', 'file_update_1' => 4, 'file_update_2' => 2).sort({ last_update: -1 }).limit(1)
        @db.close
        
        if @network.count.to_i == 0
            @logger.info "no need to update 360 adgroup"
            return render :nothing => true
        end
    else
        @network = @db[:network].find('type' => '360', 'id' => @id.to_i)
        @db.close
    end
    
    campaign_hash = {}
    
    @network.no_cursor_timeout.each do |doc|
        
          begin
              @do = 1
              
              if doc['tmp_file'].to_s != ""
                  @tmp_file = "/datadrive/"+ doc['tmp_file'].to_s + ".csv"
                  if !File.exists?(@tmp_file)
                      @db[:network].find(id: doc["id"].to_i).update_one('$set'=> { 'fileid' => "", 'tmp_file' => "",'run_time' => 0,'file_update_1' => 0, 'file_update_2' => 0, 'file_update_3' => 0, 'file_update_4' => 0 })
                      @db.close
                      
                      @do = 0
                      @logger.info "need to re download structure" + doc['id'].to_s
                  end
              end
        
              
              if @do == 1
                  login_info = login(doc["username"].to_s,doc["password"].to_s,doc["api_token"].to_s,doc["api_secret"].to_s)            
                  @refresh_token = login_info["account_clientLogin_response"]["accessToken"]
                  
                  if !@refresh_token.nil?
                      
                      getthreesixtyfile(doc["id"].to_s, doc["username"],doc["password"],doc["api_token"],doc["api_secret"].to_s, doc["fileid"].to_s, doc["tmp_file"].to_s)
                      
                      if @run_csv.to_i == 1
                            
                          @logger.info "360 network " + doc['id'].to_s + " done download csv/have csv"
                          csvdetail(@fileid.to_s, @tmp_file_path, "not_using", doc["id"].to_s)
                          
                          if File.exists?(@file)
                              
                              # campaign_db_name = "campaign_360_"+doc['id'].to_s
                              adgroup_db_name = "adgroup_360_"+doc['id'].to_s
                              # ad_db_name = "ad_360_"+doc['id'].to_s
                              # keyword_db_name = "keyword_360_"+doc['id'].to_s
                              
                              
                              #remove first
                              @db[:network].find(id: doc["id"].to_i).update_one('$set'=> {'file_update_2' => 3})
                              @db.close
                              
                              @logger.info "360 network adgroup " + doc['id'].to_s + " clean up first"
                              @threesixty_db[adgroup_db_name].drop
                              @threesixty_db.close()
                              
                              @logger.info "360 network " + doc['id'].to_s + " update adgroup"
                              @threesixty_db[adgroup_db_name].indexes.create_one(network_id: Mongo::Index::ASCENDING)
                              @threesixty_db[adgroup_db_name].indexes.create_one(account_id: Mongo::Index::ASCENDING)
                              @threesixty_db[adgroup_db_name].indexes.create_one(account_name: Mongo::Index::ASCENDING)
                              @threesixty_db[adgroup_db_name].indexes.create_one(campaign_id: Mongo::Index::ASCENDING)
                              @threesixty_db[adgroup_db_name].indexes.create_one(campaign_name: Mongo::Index::ASCENDING)
                              @threesixty_db[adgroup_db_name].indexes.create_one(adgroup_id: Mongo::Index::ASCENDING)
                              @threesixty_db[adgroup_db_name].indexes.create_one(adgroup_name: Mongo::Index::ASCENDING)
                              @threesixty_db[adgroup_db_name].indexes.create_one(price: Mongo::Index::ASCENDING)
                              @threesixty_db[adgroup_db_name].indexes.create_one(status: Mongo::Index::ASCENDING)
                              @threesixty_db[adgroup_db_name].indexes.create_one(sys_status: Mongo::Index::ASCENDING)
                              @threesixty_db[adgroup_db_name].indexes.create_one(update_date: Mongo::Index::ASCENDING)
                              @threesixty_db[adgroup_db_name].indexes.create_one(create_date: Mongo::Index::ASCENDING)
                              
                              CSV.foreach(@file, :encoding => 'GB18030').each_with_index do |csv, index|
                              
                                  if index.to_i == 0
                                      set_csv_header(csv)  
                                  end
                              
                                  begin
                                      if index.to_i != 0
                                          if !csv[@adgroup_name_index].nil? && csv[@keyword_index].nil? && csv[@ad_title_index].nil? && !csv[@update_time_index].nil? && !csv[@create_time_index].nil? && !csv[@adgroup_status_index].nil? && !csv[@adgroup_sys_status_index].nil? 
                                              
                                              @campaign_id = ""
                                              
                                              if campaign_hash["name"+csv[@campaign_name_index].to_s]
                                                  @campaign_id = campaign_hash["name"+csv[@campaign_name_index].to_s].to_i
                                              else
                                                
                                                  @campaign = @db["all_campaign"].find('campaign_name' => csv[@campaign_name_index].to_s, 'network_type' => "360", 'network_id' => doc['id'].to_i).limit(1)
                                                  @db.close
                                                  
                                                  if @campaign.count.to_i > 0
                                                      @campaign.no_cursor_timeout.each do |doc|
                                                          @campaign_id = doc["campaign_id"]
                                                          
                                                          campaign_hash["name"+csv[@campaign_name_index].to_s] = doc["campaign_id"].to_i 
                                                      end
                                                  end
                                                  
                                              end
                                              
                                              
                                              @threesixty_db[adgroup_db_name].insert_one({ 
                                                          network_id: doc["id"].to_i,
                                                          account_id: csv[@account_id_index].to_i,
                                                          account_name: csv[@account_name_index].to_s,
                                                          campaign_id: @campaign_id.to_i,
                                                          campaign_name: csv[@campaign_name_index].to_s,
                                                          adgroup_id: csv[@id_index].to_i,
                                                          adgroup_name: csv[@adgroup_name_index].to_s,
                                                          price: csv[@adgroup_price_index].to_f,
                                                          negative_words: csv[@adgroup_negative_index].to_s,
                                                          exact_negative_words: csv[@adgroup_exact_negative_mode_index].to_s,
                                                          status: csv[@adgroup_status_index].to_s,
                                                          sys_status: csv[@adgroup_sys_status_index].to_s,
                                                          update_date: csv[@update_time_index],                                            
                                                          create_date: csv[@create_time_index] 
                                                          })
                                               @threesixty_db.close()           
                                                          
                                          end
                                      end
                                  rescue Exception
                                      @db[:network].find(id: doc["id"].to_i).update_one('$set'=> {'file_update_2' => 2})
                                      @db.close
                                  end     
                              end
                              
                              # updateaccount
                              @db[:network].find(id: doc["id"].to_i).update_one('$set'=> {'file_update_2' => 4, 'last_update' => @now})
                              @db.close
                          else
                              @db[:network].find(id: doc["id"].to_i).update_one('$set'=> { 'tmp_file' => "" , 'fileid' => "", 'file_update_1' => 0, 'file_update_2' => 0, 'file_update_3' => 0, 'file_update_4' => 0})
                              @db.close
                          end
                      end
                  end
                  @logger.info "360 structure, network "+doc["id"].to_s+ " done adgroup"
              end
          rescue Exception
              @db[:network].find(id: doc["id"].to_i).update_one('$set'=> {'file_update_2' => 2})
              @db.close
          end     
    end
    
    @logger.info "360 structure done adgroup"     
    return render :nothing => true 
  end
  
  
  
  
  
  
  
  
  def campaignandadgroup
    @logger.info "called 360 structure campaign"
    
    @id = params[:id]
    if @id.nil?
      
        @current_network = @db[:network].find({ '$or' => [ {'file_update_1' => 3},{'file_update_2' => 3},{'file_update_3' => 3},{'file_update_4' => 3} ],'type' => '360', 'worker' => @port.to_i})
        @db.close
        
        if @current_network.count.to_i >= 2
            @logger.info "working, no need to update 360 campaign and adgroup"
            return render :nothing => true
        end
        
        @network = @db[:network].find({ "$and" => [{:type => '360'}, {:file_update_1 => 2}, {:file_update_2 => 2}, {:worker => @port.to_i}] }).sort({ last_update: -1 }).limit(1)
        @db.close
        
        if @network.count.to_i == 0
            @logger.info "no need to update 360 campaign and adgroup"
            return render :nothing => true
        end
    else
        @network = @db[:network].find({ "$and" => [{:id => @id.to_i}, {:type => '360'}] })
        @db.close
    end
    
    campaign_hash = {}
    
    @network.no_cursor_timeout.each do |doc|
        
          begin
              @do = 1
              
              if doc['tmp_file'].to_s != ""
                  @tmp_file = "/datadrive/"+ doc['tmp_file'].to_s + ".csv"
                  if !File.exists?(@tmp_file)
                      
                      redownload(doc["id"])
                      @do = 0
                      @logger.info "need to re download structure" + doc['id'].to_s
                      return render :nothing => true
                  end
              end
              
              
              if @do == 1
                  login_info = login(doc["username"].to_s,doc["password"].to_s,doc["api_token"].to_s,doc["api_secret"].to_s)            
                  @refresh_token = login_info["account_clientLogin_response"]["accessToken"]
                  
                  if !@refresh_token.nil?
                      
                      getthreesixtyfile(doc["id"].to_s, doc["username"],doc["password"],doc["api_token"],doc["api_secret"].to_s, doc["fileid"].to_s, doc["tmp_file"].to_s)
                      
                      if @run_csv.to_i == 1
                            
                          @logger.info "360 network " + doc['id'].to_s + " done download csv/have csv"
                          csvdetail(@fileid.to_s, @tmp_file_path, "not_using", doc["id"].to_s)
                          
                          if File.exists?(@file)
                              #remove first
                              @db[:network].find(id: doc["id"].to_i).update_one('$set'=> {'file_update_1' => 3,'file_update_2' => 3, 'last_update' => @now})
                              @db.close
                              
                              @logger.info "360 network " + doc['id'].to_s + " campaign and adgroup clean up first"
                              
                              @db["all_campaign"].find({ "$and" => [{:network_id => doc["id"].to_i}, {:network_type => '360'}] }).delete_many
                              @db.close
                              
                              adgroup_db_name = "adgroup_360_"+doc['id'].to_s
                              @threesixty_db[adgroup_db_name].drop
                              @threesixty_db.close()
                              
                              @logger.info "360 network " + doc['id'].to_s + " update adgroup"
                              @threesixty_db[adgroup_db_name].indexes.create_one(network_id: Mongo::Index::ASCENDING)
                              @threesixty_db[adgroup_db_name].indexes.create_one(account_id: Mongo::Index::ASCENDING)
                              @threesixty_db[adgroup_db_name].indexes.create_one(account_name: Mongo::Index::ASCENDING)
                              @threesixty_db[adgroup_db_name].indexes.create_one(campaign_id: Mongo::Index::ASCENDING)
                              @threesixty_db[adgroup_db_name].indexes.create_one(campaign_name: Mongo::Index::ASCENDING)
                              @threesixty_db[adgroup_db_name].indexes.create_one(adgroup_id: Mongo::Index::ASCENDING)
                              @threesixty_db[adgroup_db_name].indexes.create_one(adgroup_name: Mongo::Index::ASCENDING)
                              @threesixty_db[adgroup_db_name].indexes.create_one(price: Mongo::Index::ASCENDING)
                              @threesixty_db[adgroup_db_name].indexes.create_one(status: Mongo::Index::ASCENDING)
                              @threesixty_db[adgroup_db_name].indexes.create_one(sys_status: Mongo::Index::ASCENDING)
                              @threesixty_db[adgroup_db_name].indexes.create_one(update_date: Mongo::Index::ASCENDING)
                              @threesixty_db[adgroup_db_name].indexes.create_one(create_date: Mongo::Index::ASCENDING)
                              
                              

                              @logger.info "360 network " + doc['id'].to_s + " update campaign"
                              CSV.foreach(@file, :encoding => 'GB18030').each_with_index do |csv, index|
                              
                                  if index.to_i == 0
                                      set_csv_header(csv)  
                                  end
                              
                                  begin
                                      if index.to_i != 0
                                                                            
                                          if csv[@adgroup_name_index].nil? && csv[@keyword_index].nil? && csv[@ad_title_index].nil? && !csv[@update_time_index].nil? && !csv[@create_time_index].nil? && !csv[@campaign_status_index].nil? && !csv[@campaign_sys_status_index].nil?
                                              
                                              @db["all_campaign"].insert_one({ 
                                                          network_id: doc["id"].to_i,
                                                          network_type: "360", 
                                                          account_id: csv[@account_id_index].to_i,
                                                          account_name: csv[@account_name_index].to_s,
                                                          campaign_id: csv[@id_index].to_i,
                                                          campaign_name: csv[@campaign_name_index].to_s, 
                                                          budget: csv[@budget_index].to_f, 
                                                          regions: csv[@campaign_region_index], 
                                                          schedule: csv[@campaign_schedule_index],
                                                          start_date: csv[@campaign_start_time_index].to_s,
                                                          end_date: csv[@campaign_end_time_index].to_s,
                                                          status: csv[@campaign_status_index].to_s,
                                                          sys_status: csv[@campaign_sys_status_index].to_s,
                                                          extend_ad_type: csv[@extend_ad_type_index],
                                                          negative_words: csv[@campaign_negative_index].to_s,
                                                          exact_negative_words: csv[@campaign_exact_negative_mode_index].to_s,
                                                          mobile_price_rate: csv[@mobile_search_price_index].to_f,
                                                          update_date: @now,                                            
                                                          create_date: @now
                                                        })
                                              @db.close
                                          
                                          
                                          
                                          elsif !csv[@adgroup_name_index].nil? && csv[@keyword_index].nil? && csv[@ad_title_index].nil? && !csv[@update_time_index].nil? && !csv[@create_time_index].nil? && !csv[@adgroup_status_index].nil? && !csv[@adgroup_sys_status_index].nil? 
                                              
                                              @campaign_id = ""
                                              
                                              if campaign_hash["name"+csv[@campaign_name_index].to_s]
                                                  @campaign_id = campaign_hash["name"+csv[@campaign_name_index].to_s].to_i
                                              else
                                                  @campaign = @db["all_campaign"].find({ "$and" => [{:campaign_name => csv[@campaign_name_index].to_s}, {:network_type => '360'}, {:network_id => doc['id'].to_i}] }).limit(1)
                                                  @db.close
                                                  
                                                  if @campaign.count.to_i > 0
                                                      @campaign.no_cursor_timeout.each do |doc|
                                                          @campaign_id = doc["campaign_id"]
                                                          
                                                          campaign_hash["name"+csv[@campaign_name_index].to_s] = doc["campaign_id"].to_i 
                                                      end
                                                  end
                                              
                                              end
                                              
                                              @threesixty_db[adgroup_db_name].insert_one({ 
                                                          network_id: doc["id"].to_i,
                                                          account_id: csv[@account_id_index].to_i,
                                                          account_name: csv[@account_name_index].to_s,
                                                          campaign_id: @campaign_id.to_i,
                                                          campaign_name: csv[@campaign_name_index].to_s,
                                                          adgroup_id: csv[@id_index].to_i,
                                                          adgroup_name: csv[@adgroup_name_index].to_s,
                                                          price: csv[@adgroup_price_index].to_f,
                                                          negative_words: csv[@adgroup_negative_index].to_s,
                                                          exact_negative_words: csv[@adgroup_exact_negative_mode_index].to_s,
                                                          status: csv[@adgroup_status_index].to_s,
                                                          sys_status: csv[@adgroup_sys_status_index].to_s,
                                                          update_date: @now,                                            
                                                          create_date: @now 
                                                          })
                                                          
                                               @threesixty_db.close()           
                                          else
                                          end
                                          
                                          
                                      end
                                  rescue Exception
                                      
                                      redownload(doc["id"])
                                      return render :nothing => true
                                  end     
                              end
                              
                              # updateaccount
                              @db[:network].find(id: doc["id"].to_i).update_one('$set'=> {'file_update_1' => 4, 'file_update_2' => 4, 'last_update' => @now})
                              @db.close
                          else
                              redownload(doc["id"])
                              return render :nothing => true
                          end
                      end
                  end
                  @logger.info "360 structure campaign and adgroup, network "+doc["id"].to_s+ " done"
              end
              
          rescue Exception
            
              redownload(doc["id"])
              return render :nothing => true
          end 
          
    end
    
    @logger.info "360 structure done campaign and adgroup"     
    return render :nothing => true 
  end
  
  
  
  
  
  
  def ad
    @logger.info "called 360 structure ad"
    
    @id = params[:id]
    if @id.nil?
      
        @current_network = @db[:network].find({ '$or' => [ {'file_update_1' => 3},{'file_update_2' => 3},{'file_update_3' => 3},{'file_update_4' => 3} ]})
        @db.close
      
        if @current_network.count.to_i >= 5
            @logger.info "working, no need to update 360 ad"
            return render :nothing => true
        end
      
        # @network = @db[:network].find('type' => '360', 'file_update_1' => 4, 'file_update_2' => 4, 'file_update_3' => {'$gte' => 2}, 'file_update_3' => {'$lt' => 4}).limit(1)
        @network = @db[:network].find('type' => '360', 'file_update_1' => 4, 'file_update_2' => 4, 'file_update_3' => 2).sort({ last_update: -1 }).limit(1)
        @db.close
        
        if @network.count.to_i == 0
            @logger.info "no need to update 360 ad"
            return render :nothing => true
        end
    else
        @network = @db[:network].find('type' => '360', 'id' => @id.to_i)
        @db.close
    end
    
    campaign_adgroup_hash = {}
    
    @network.no_cursor_timeout.each do |doc|
        
          begin
              @do = 1
              
              if doc['tmp_file'].to_s != ""
                  @tmp_file = "/datadrive/"+ doc['tmp_file'].to_s + ".csv"
                  if !File.exists?(@tmp_file)
                      @db[:network].find(id: doc["id"].to_i).update_one('$set'=> { 'fileid' => "", 'tmp_file' => "",'run_time' => 0,'file_update_1' => 0, 'file_update_2' => 0, 'file_update_3' => 0, 'file_update_4' => 0 })
                      @db.close
                      @do = 0
                      @logger.info "need to re download structure" + doc['id'].to_s
                  end
              end
              
              if @do == 1
                  login_info = login(doc["username"].to_s,doc["password"].to_s,doc["api_token"].to_s,doc["api_secret"].to_s)            
                  @refresh_token = login_info["account_clientLogin_response"]["accessToken"]
                  
                  if !@refresh_token.nil?
                      
                      getthreesixtyfile(doc["id"].to_s, doc["username"],doc["password"],doc["api_token"],doc["api_secret"].to_s, doc["fileid"].to_s, doc["tmp_file"].to_s)
                      
                      if @run_csv.to_i == 1
                            
                          @logger.info "360 network " + doc['id'].to_s + " done download csv/have csv"
                          csvdetail(@fileid.to_s, @tmp_file_path, "not_using", doc["id"].to_s)
                          
                          
                          if File.exists?(@file)
                              
                              # campaign_db_name = "campaign_360_"+doc['id'].to_s
                              adgroup_db_name = "adgroup_360_"+doc['id'].to_s
                              ad_db_name = "ad_360_"+doc['id'].to_s
                              # keyword_db_name = "keyword_360_"+doc['id'].to_s
                              
                              #remove first
                              @db[:network].find(id: doc["id"].to_i).update_one('$set'=> {'file_update_3' => 3})
                              @db.close
                              
                              @logger.info "360 network ad " + doc['id'].to_s + " clean up first"
                              @threesixty_db[ad_db_name].drop
                              @threesixty_db.close()
                              
                              @logger.info "360 network " + doc['id'].to_s + " update ad"
                              @threesixty_db[ad_db_name].indexes.create_one(network_id: Mongo::Index::ASCENDING)
                              @threesixty_db[ad_db_name].indexes.create_one(account_id: Mongo::Index::ASCENDING)
                              @threesixty_db[ad_db_name].indexes.create_one(account_name: Mongo::Index::ASCENDING)
                              @threesixty_db[ad_db_name].indexes.create_one(campaign_id: Mongo::Index::ASCENDING)
                              @threesixty_db[ad_db_name].indexes.create_one(campaign_name: Mongo::Index::ASCENDING)
                              @threesixty_db[ad_db_name].indexes.create_one(adgroup_id: Mongo::Index::ASCENDING)
                              @threesixty_db[ad_db_name].indexes.create_one(ad_id: Mongo::Index::ASCENDING)
                              @threesixty_db[ad_db_name].indexes.create_one(title: Mongo::Index::ASCENDING)
                              @threesixty_db[ad_db_name].indexes.create_one(status: Mongo::Index::ASCENDING)
                              @threesixty_db[ad_db_name].indexes.create_one(sys_status: Mongo::Index::ASCENDING)
                              @threesixty_db[ad_db_name].indexes.create_one(show_url: Mongo::Index::ASCENDING)
                              @threesixty_db[ad_db_name].indexes.create_one(visit_url: Mongo::Index::ASCENDING)
                              @threesixty_db[ad_db_name].indexes.create_one(mobile_show_url: Mongo::Index::ASCENDING)
                              @threesixty_db[ad_db_name].indexes.create_one(mobile_visit_url: Mongo::Index::ASCENDING)
                              @threesixty_db[ad_db_name].indexes.create_one(extend_ad_type: Mongo::Index::ASCENDING)
                              @threesixty_db[ad_db_name].indexes.create_one(update_date: Mongo::Index::ASCENDING)
                              @threesixty_db[ad_db_name].indexes.create_one(create_date: Mongo::Index::ASCENDING)
                              
                              CSV.foreach(@file, :encoding => 'GB18030').each_with_index do |csv, index|
                              
                                  if index.to_i == 0
                                      set_csv_header(csv)  
                                  end
                                  
                                  begin
                                      if index.to_i != 0
                                          if !csv[@ad_title_index].nil? && csv[@keyword_index].nil? && !csv[@update_time_index].nil? && !csv[@create_time_index].nil? && !csv[@ad_status_index].nil? && !csv[@ad_sys_status_index].nil?
                                              
                                              # @campaign_id = ""
                                              # @adgroup_id = ""
                                              
                                              if campaign_adgroup_hash["name"+csv[@adgroup_name_index].to_s+"camp"+csv[@campaign_name_index].to_s]
                                                  @campaign_id = campaign_adgroup_hash["name"+csv[@adgroup_name_index].to_s+"camp"+csv[@campaign_name_index].to_s][0].to_i
                                                  @adgroup_id = campaign_adgroup_hash["name"+csv[@adgroup_name_index].to_s+"camp"+csv[@campaign_name_index].to_s][1].to_i
                                              else
                                                  @adgroup = @threesixty_db[adgroup_db_name].find('campaign_name' => csv[@campaign_name_index].to_s,'adgroup_name' => csv[@adgroup_name_index].to_s)
                                                  @threesixty_db.close()
                                                  
                                                  if @adgroup.count.to_i > 0
                                                      temp_arr = []
                                                    
                                                      @adgroup.no_cursor_timeout.each do |doc|
                                                          
                                                          @adgroup_id = doc["adgroup_id"]
                                                          @campaign_id = doc["campaign_id"]
                                                          
                                                          temp_arr << doc["campaign_id"]
                                                          temp_arr << doc["adgroup_id"]
                                                          
                                                          campaign_adgroup_hash["name"+csv[@adgroup_name_index].to_s+"camp"+csv[@campaign_name_index].to_s] = temp_arr
                                                      end
                                                  end
                                              end
                                              
                                              @threesixty_db[ad_db_name].insert_one({ 
                                                          network_id: doc["id"].to_i,
                                                          account_id: csv[@account_id_index].to_i,
                                                          account_name: csv[@account_name_index].to_s,
                                                          campaign_id: @campaign_id.to_i,
                                                          campaign_name: csv[@campaign_name_index].to_s,
                                                          adgroup_id: @adgroup_id.to_i,
                                                          ad_id: csv[@id_index].to_i,
                                                          title: csv[@ad_title_index].to_s, 
                                                          description: csv[@ad_desc_index].to_s, 
                                                          status: csv[@ad_status_index].to_s,
                                                          sys_status: csv[@ad_sys_status_index].to_s,
                                                          show_url: csv[@display_url_index].to_s,
                                                          visit_url: csv[@final_url_index].to_s,
                                                          mobile_show_url: csv[@ad_mobile_display_index].to_s,
                                                          mobile_visit_url: csv[@ad_mobile_final_index].to_s,
                                                          extend_ad_type: csv[@extend_ad_type_index].to_i,
                                                          update_date: csv[@update_time_index],                                            
                                                          create_date: csv[@create_time_index] 
                                                          })
                                              @threesixty_db.close()            
                                          end
                                      end
                                  rescue Exception
                                      @db[:network].find(id: doc["id"].to_i).update_one('$set'=> {'file_update_3' => 2})
                                      @db.close
                                  end     
                              end
                              
                              # updateaccount
                              @db[:network].find(id: doc["id"].to_i).update_one('$set'=> {'file_update_3' => 4, 'last_update' => @now})
                              @db.close
                          else
                              @db[:network].find(id: doc["id"].to_i).update_one('$set'=> { 'tmp_file' => "" , 'fileid' => "", 'file_update_1' => 0, 'file_update_2' => 0, 'file_update_3' => 0, 'file_update_4' => 0})
                              @db.close
                          end
                      end
                  end
                  @logger.info "360 structure, network "+doc["id"].to_s+ " done ad"
              end
          rescue Exception
              @db[:network].find(id: doc["id"].to_i).update_one('$set'=> {'file_update_3' => 2})
              @db.close
          end
    end
    
    @logger.info "360 structure done ad"     
    return render :nothing => true 
    
    # @data = {
            # :campaign_adgroup_hash => campaign_adgroup_hash,
            # :status => "true"
    # }
    # return render :json => @data, :status => :ok
  end
  
  
  
  
  
  
  
  def keyword
    @logger.info "called 360 structure keyword"
    
    @id = params[:id]
    if @id.nil?
        
        @current_network = @db[:network].find({ '$or' => [ {'file_update_1' => 3},{'file_update_2' => 3},{'file_update_3' => 3},{'file_update_4' => 3} ],'type' => '360'})
        # @current_network = @db[:network].find('type' => '360', 'file_update_1' => 4, 'file_update_2' => 4, 'file_update_3' => 4, 'file_update_4' => 3)
        @db.close
        
        if @current_network.count.to_i >= 3
            @logger.info "working, no need to update 360 keyword"
            return render :nothing => true
        end
      
        # @network = @db[:network].find('type' => '360', 'file_update_1' => 4, 'file_update_2' => 4, 'file_update_3' => 4, 'file_update_4' => {'$gte' => 2}, 'file_update_4' => {'$lt' => 4}).limit(1)
        @network = @db[:network].find('type' => '360', 'file_update_1' => 4, 'file_update_2' => 4, 'file_update_3' => 4, 'file_update_4' => 2).sort({ last_update: -1 }).limit(1)
        @db.close
        
        if @network.count.to_i == 0
            @logger.info "no need to update 360 keyword"
            return render :nothing => true
        end
    else
        @network = @db[:network].find('type' => '360', 'id' => @id.to_i)
        @db.close
    end
    
    campaign_adgroup_hash = {}
    
    @network.no_cursor_timeout.each do |doc|
          begin
              @do = 1
              
              if doc['tmp_file'].to_s != ""
                  @tmp_file = "/datadrive/"+ doc['tmp_file'].to_s + ".csv"
                  if !File.exists?(@tmp_file)
                      @db[:network].find(id: doc["id"].to_i).update_one('$set'=> { 'fileid' => "", 'tmp_file' => "",'run_time' => 0,'file_update_1' => 0, 'file_update_2' => 0, 'file_update_3' => 0, 'file_update_4' => 0 })
                      @db.close
                      
                      @do = 0
                      @logger.info "need to re download structure" + doc['id'].to_s
                  end
              end
              
              
              if @do == 1
                  login_info = login(doc["username"].to_s,doc["password"].to_s,doc["api_token"].to_s,doc["api_secret"].to_s)            
                  @refresh_token = login_info["account_clientLogin_response"]["accessToken"]
                  
                  if !@refresh_token.nil?
                      
                      getthreesixtyfile(doc["id"].to_s, doc["username"],doc["password"],doc["api_token"],doc["api_secret"].to_s, doc["fileid"].to_s, doc["tmp_file"].to_s)
                      
                      if @run_csv.to_i == 1
                            
                          @logger.info "360 network " + doc['id'].to_s + " done download csv/have csv"
                          csvdetail(@fileid.to_s, @tmp_file_path, "not_using", doc["id"].to_s)
                          
                          if File.exists?(@file)
                              
                              # campaign_db_name = "campaign_360_"+doc['id'].to_s
                              adgroup_db_name = "adgroup_360_"+doc['id'].to_s
                              # ad_db_name = "ad_360_"+doc['id'].to_s
                              keyword_db_name = "keyword_360_"+doc['id'].to_s
                              
                              #remove first
                              @db[:network].find(id: doc["id"].to_i).update_one('$set'=> {'file_update_4' => 3})
                              @db.close
                              
                              @logger.info "360 network keyword " + doc['id'].to_s + " clean up first"
                              @threesixty_db[keyword_db_name].drop
                              @threesixty_db.close()
                              
                              @logger.info "360 network " + doc['id'].to_s + " update keyword"
                              @threesixty_db[keyword_db_name].indexes.create_one(network_id: Mongo::Index::ASCENDING)
                              @threesixty_db[keyword_db_name].indexes.create_one(account_id: Mongo::Index::ASCENDING)
                              @threesixty_db[keyword_db_name].indexes.create_one(account_name: Mongo::Index::ASCENDING)
                              @threesixty_db[keyword_db_name].indexes.create_one(campaign_id: Mongo::Index::ASCENDING)
                              @threesixty_db[keyword_db_name].indexes.create_one(campaign_name: Mongo::Index::ASCENDING)
                              @threesixty_db[keyword_db_name].indexes.create_one(adgroup_id: Mongo::Index::ASCENDING)
                              @threesixty_db[keyword_db_name].indexes.create_one(keyword_id: Mongo::Index::ASCENDING)
                              @threesixty_db[keyword_db_name].indexes.create_one(keyword: Mongo::Index::ASCENDING)
                              @threesixty_db[keyword_db_name].indexes.create_one(price: Mongo::Index::ASCENDING)
                              @threesixty_db[keyword_db_name].indexes.create_one(status: Mongo::Index::ASCENDING)
                              @threesixty_db[keyword_db_name].indexes.create_one(sys_status: Mongo::Index::ASCENDING)
                              @threesixty_db[keyword_db_name].indexes.create_one(match_type: Mongo::Index::ASCENDING)
                              @threesixty_db[keyword_db_name].indexes.create_one(visit_url: Mongo::Index::ASCENDING)
                              @threesixty_db[keyword_db_name].indexes.create_one(mobile_visit_url: Mongo::Index::ASCENDING)
                              @threesixty_db[keyword_db_name].indexes.create_one(cpc_quality: Mongo::Index::ASCENDING)
                              @threesixty_db[keyword_db_name].indexes.create_one(extend_ad_type: Mongo::Index::ASCENDING)
                              @threesixty_db[keyword_db_name].indexes.create_one(update_date: Mongo::Index::ASCENDING)
                              @threesixty_db[keyword_db_name].indexes.create_one(create_date: Mongo::Index::ASCENDING)
                              
                              CSV.foreach(@file, :encoding => 'GB18030').each_with_index do |csv, index|
                              
                                  if index.to_i == 0
                                      set_csv_header(csv)  
                                  end
                              
                                  begin
                                      if index.to_i != 0
                                          if !csv[@keyword_index].nil? && csv[@ad_title_index].nil? && !csv[@update_time_index].nil? && !csv[@create_time_index].nil? && !csv[@keyword_status_index].nil? && !csv[@keyword_sys_status_index].nil? 
                                                
                                              @campaign_id = ""
                                              @adgroup_id = ""
                                              
                                              if campaign_adgroup_hash["name"+csv[@adgroup_name_index].to_s+"camp"+csv[@campaign_name_index].to_s]
                                                  @campaign_id = campaign_adgroup_hash["name"+csv[@adgroup_name_index].to_s+"camp"+csv[@campaign_name_index].to_s][0].to_i
                                                  @adgroup_id = campaign_adgroup_hash["name"+csv[@adgroup_name_index].to_s+"camp"+csv[@campaign_name_index].to_s][1].to_i
                                              else
                                              
                                                  @adgroup = @threesixty_db[adgroup_db_name].find('campaign_name' => csv[@campaign_name_index].to_s,'adgroup_name' => csv[@adgroup_name_index].to_s)
                                                  @threesixty_db.close()
                                                  
                                                  if @adgroup.count.to_i > 0
                                                    
                                                      temp_arr = []
                                                    
                                                      @adgroup.no_cursor_timeout.each do |doc|
                                                          @adgroup_id = doc["adgroup_id"]
                                                          @campaign_id = doc["campaign_id"]
                                                          
                                                          temp_arr << doc["campaign_id"]
                                                          temp_arr << doc["adgroup_id"]
                                                          
                                                          campaign_adgroup_hash["name"+csv[@adgroup_name_index].to_s+"camp"+csv[@campaign_name_index].to_s] = temp_arr
                                                      end
                                                  end
                                              
                                              end
                                            
                                            
                                             @threesixty_db[keyword_db_name].insert_one({ 
                                                      network_id: doc["id"].to_i,
                                                      account_id: csv[@account_id_index].to_i,
                                                      account_name: csv[@account_name_index].to_s,
                                                      campaign_id: @campaign_id.to_i,
                                                      campaign_name: csv[@campaign_name_index].to_s,
                                                      adgroup_id: @adgroup_id.to_i,
                                                      keyword_id: csv[@id_index].to_i,
                                                      keyword: csv[@keyword_index].to_s,
                                                      price: csv[@keyword_price_index].to_f, 
                                                      status: csv[@keyword_status_index].to_s,
                                                      sys_status: csv[@keyword_sys_status_index].to_s,
                                                      match_type: csv[@keyword_match_type_index].to_s,
                                                      visit_url: csv[@final_url_index].to_s,
                                                      mobile_visit_url: csv[@keyword_mobile_final_url_index].to_s,
                                                      cpc_quality: csv[@keyword_cpc_quality_index].to_f,
                                                      extend_ad_type: csv[@extend_ad_type_index].to_i,
                                                      # negative_words: csv[@keyword_negative_index].to_s,
                                                      update_date: csv[@update_time_index],                                            
                                                      create_date: csv[@create_time_index] 
                                                      })
                                                      
                                              @threesixty_db.close()        
                                          end
                                      end
                                  rescue Exception
                                      @db[:network].find(id: doc["id"].to_i).update_one('$set'=> {'file_update_4' => 2})
                                      @db.close
                                  end     
                              end
                              
                              # updateaccount
                              @db[:network].find(id: doc["id"].to_i).update_one('$set'=> {'file_update_4' => 4, 'last_update' => @now})
                              @db.close
                          else
                              @db[:network].find(id: doc["id"].to_i).update_one('$set'=> { 'tmp_file' => "" , 'fileid' => "", 'file_update_1' => 0, 'file_update_2' => 0, 'file_update_3' => 0, 'file_update_4' => 0})
                              @db.close
                          end
                      end
                  end
                  @logger.info "360 structure, network "+doc["id"].to_s+ " done keyword"
              end
          rescue Exception
              @db[:network].find(id: doc["id"].to_i).update_one('$set'=> {'file_update_4' => 2})
              @db.close
          end
    end
    
    @logger.info "360 structure done keyword"     
    return render :nothing => true 
  end
  
   
  
  def adandkeyword
    @logger.info "called 360 structure ad and keyword" 
    
    @id = params[:id]
    if @id.nil?
          
        @current_network = @db[:network].find({ '$or' => [ {'file_update_1' => 3},{'file_update_2' => 3},{'file_update_3' => 3},{'file_update_4' => 3} ],'type' => '360', 'worker' => @port.to_i})
        @db.close
        
        if @current_network.count.to_i >= 2
            @logger.info "working,no need to update 360 ad and keyword"
            return render :nothing => true
        end
        
        @network = @db[:network].find({ "$and" => [{:type => '360'}, {:file_update_1 => 4}, {:file_update_2 => 4}, {:file_update_3 => 2}, {:file_update_4 => 2}, {:worker => @port.to_i}] }).sort({ last_update: -1 }).limit(1)
        @db.close
        
        if @network.count.to_i == 0
            @logger.info "no need to update 360 ad and keyword"
            return render :nothing => true
        end
    else
        @network = @db[:network].find({ "$and" => [{:id => @id.to_i}, {:type => 360}] })
        @db.close
    end
    
    campaign_adgroup_hash = {}
    
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
                  @tmp_file = "/datadrive/"+ doc['tmp_file'].to_s + ".csv"
                  if !File.exists?(@tmp_file)
                      redownload(doc["id"])
                      @do = 0
                      @logger.info "need to re download structure" + doc['id'].to_s
                      return render :nothing => true
                  end
              end
              
              if @do == 1
                  login_info = login(doc["username"].to_s,doc["password"].to_s,doc["api_token"].to_s,doc["api_secret"].to_s)            
                  @refresh_token = login_info["account_clientLogin_response"]["accessToken"]
                  
                  if !@refresh_token.nil?
                      
                      @update_res = threesixty_api( doc["api_token"].to_s, @refresh_token, "account", "getInfo")
                      @remain_quote = @response.headers["quotaremain"].to_i
                      
                      getthreesixtyfile(doc["id"].to_s, doc["username"],doc["password"],doc["api_token"],doc["api_secret"].to_s, doc["fileid"].to_s, doc["tmp_file"].to_s)
                      
                      if @run_csv.to_i == 1
                            
                          @logger.info "360 network " + doc['id'].to_s + " done download csv/have csv"
                          csvdetail(@fileid.to_s, @tmp_file_path, "campaign", doc["id"].to_s)
                          
                          if File.exists?(@file)
                              
                              # campaign_db_name = "campaign_360_"+doc['id'].to_s
                              adgroup_db_name = "adgroup_360_"+doc['id'].to_s
                              ad_db_name = "ad_360_"+doc['id'].to_s
                              keyword_db_name = "keyword_360_"+doc['id'].to_s
                              
                              #remove first
                              @db[:network].find(id: doc["id"].to_i).update_one('$set'=> {'file_update_3' => 3,'file_update_4' => 3})
                              @db.close
                              
                              @logger.info "360 network ad " + doc['id'].to_s + " clean up first"
                              @threesixty_db[ad_db_name].drop
                              @threesixty_db.close()
                              @threesixty_db[keyword_db_name].drop
                              @threesixty_db.close()
                              
                              @logger.info "360 network " + doc['id'].to_s + " update ad and keyword"
                              @threesixty_db[ad_db_name].indexes.create_one(network_id: Mongo::Index::ASCENDING)
                              @threesixty_db[ad_db_name].indexes.create_one(account_id: Mongo::Index::ASCENDING)
                              @threesixty_db[ad_db_name].indexes.create_one(account_name: Mongo::Index::ASCENDING)
                              @threesixty_db[ad_db_name].indexes.create_one(campaign_id: Mongo::Index::ASCENDING)
                              @threesixty_db[ad_db_name].indexes.create_one(campaign_name: Mongo::Index::ASCENDING)
                              @threesixty_db[ad_db_name].indexes.create_one(adgroup_id: Mongo::Index::ASCENDING)
                              @threesixty_db[ad_db_name].indexes.create_one(ad_id: Mongo::Index::ASCENDING)
                              @threesixty_db[ad_db_name].indexes.create_one(title: Mongo::Index::ASCENDING)
                              @threesixty_db[ad_db_name].indexes.create_one(status: Mongo::Index::ASCENDING)
                              @threesixty_db[ad_db_name].indexes.create_one(sys_status: Mongo::Index::ASCENDING)
                              # @threesixty_db[ad_db_name].indexes.create_one(show_url: Mongo::Index::ASCENDING)
                              # @threesixty_db[ad_db_name].indexes.create_one(visit_url: Mongo::Index::ASCENDING)
                              # @threesixty_db[ad_db_name].indexes.create_one(mobile_show_url: Mongo::Index::ASCENDING)
                              # @threesixty_db[ad_db_name].indexes.create_one(mobile_visit_url: Mongo::Index::ASCENDING)
                              @threesixty_db[ad_db_name].indexes.create_one(extend_ad_type: Mongo::Index::ASCENDING)
                              # @threesixty_db[ad_db_name].indexes.create_one(watchdog: Mongo::Index::ASCENDING)
                              @threesixty_db[ad_db_name].indexes.create_one(response_code: Mongo::Index::ASCENDING)
                              @threesixty_db[ad_db_name].indexes.create_one(m_response_code: Mongo::Index::ASCENDING)
                              @threesixty_db[ad_db_name].indexes.create_one(update_date: Mongo::Index::ASCENDING)
                              @threesixty_db[ad_db_name].indexes.create_one(create_date: Mongo::Index::ASCENDING)
                              
                              
                              
                              @threesixty_db[keyword_db_name].indexes.create_one(network_id: Mongo::Index::ASCENDING)
                              @threesixty_db[keyword_db_name].indexes.create_one(account_id: Mongo::Index::ASCENDING)
                              @threesixty_db[keyword_db_name].indexes.create_one(account_name: Mongo::Index::ASCENDING)
                              @threesixty_db[keyword_db_name].indexes.create_one(campaign_id: Mongo::Index::ASCENDING)
                              @threesixty_db[keyword_db_name].indexes.create_one(campaign_name: Mongo::Index::ASCENDING)
                              @threesixty_db[keyword_db_name].indexes.create_one(adgroup_id: Mongo::Index::ASCENDING)
                              @threesixty_db[keyword_db_name].indexes.create_one(keyword_id: Mongo::Index::ASCENDING)
                              @threesixty_db[keyword_db_name].indexes.create_one(keyword: Mongo::Index::ASCENDING)
                              @threesixty_db[keyword_db_name].indexes.create_one(price: Mongo::Index::ASCENDING)
                              @threesixty_db[keyword_db_name].indexes.create_one(status: Mongo::Index::ASCENDING)
                              @threesixty_db[keyword_db_name].indexes.create_one(sys_status: Mongo::Index::ASCENDING)
                              @threesixty_db[keyword_db_name].indexes.create_one(match_type: Mongo::Index::ASCENDING)
                              # @threesixty_db[keyword_db_name].indexes.create_one(visit_url: Mongo::Index::ASCENDING)
                              # @threesixty_db[keyword_db_name].indexes.create_one(mobile_visit_url: Mongo::Index::ASCENDING)
                              @threesixty_db[keyword_db_name].indexes.create_one(cpc_quality: Mongo::Index::ASCENDING)
                              @threesixty_db[keyword_db_name].indexes.create_one(extend_ad_type: Mongo::Index::ASCENDING)
                              # @threesixty_db[keyword_db_name].indexes.create_one(watchdog: Mongo::Index::ASCENDING)
                              @threesixty_db[keyword_db_name].indexes.create_one(response_code: Mongo::Index::ASCENDING)
                              @threesixty_db[keyword_db_name].indexes.create_one(m_response_code: Mongo::Index::ASCENDING)
                              @threesixty_db[keyword_db_name].indexes.create_one(update_date: Mongo::Index::ASCENDING)
                              @threesixty_db[keyword_db_name].indexes.create_one(create_date: Mongo::Index::ASCENDING)
                              
                              CSV.foreach(@file, :encoding => 'GB18030').each_with_index do |csv, index|
                              
                                  if index.to_i == 0
                                      set_csv_header(csv)  
                                  end
                                  
                                  begin
                                      if index.to_i != 0
                                          if !csv[@ad_title_index].nil? && csv[@keyword_index].nil? && !csv[@update_time_index].nil? && !csv[@create_time_index].nil? && !csv[@ad_status_index].nil? && !csv[@ad_sys_status_index].nil?
                                              
                                              @ad_campaign_id = ""
                                              @ad_adgroup_id = ""
                                              
                                              if !campaign_adgroup_hash["name"+csv[@adgroup_name_index].to_s+"camp"+csv[@campaign_name_index].to_s].nil?
                                                  @ad_campaign_id = campaign_adgroup_hash["name"+csv[@adgroup_name_index].to_s+"camp"+csv[@campaign_name_index].to_s][0].to_i
                                                  @ad_adgroup_id = campaign_adgroup_hash["name"+csv[@adgroup_name_index].to_s+"camp"+csv[@campaign_name_index].to_s][1].to_i
                                              else
                                                  @adgroup = @threesixty_db[adgroup_db_name].find('campaign_name' => csv[@campaign_name_index].to_s,'adgroup_name' => csv[@adgroup_name_index].to_s)
                                                  @threesixty_db.close()
                                                  
                                                  if @adgroup.count.to_i > 0
                                                      temp_arr = []
                                                    
                                                      @adgroup.no_cursor_timeout.each do |doc|
                                                          
                                                          @ad_adgroup_id = doc["adgroup_id"]
                                                          @ad_campaign_id = doc["campaign_id"]
                                                          
                                                          temp_arr << doc["campaign_id"]
                                                          temp_arr << doc["adgroup_id"]
                                                          
                                                          campaign_adgroup_hash["name"+csv[@adgroup_name_index].to_s+"camp"+csv[@campaign_name_index].to_s] = temp_arr
                                                      end
                                                  end
                                              end
                                              
                                              url_tag = 0
                                              m_url_tag = 0
                                              
                                              @final_url = csv[@final_url_index].to_s
                                              @m_final_url = csv[@ad_mobile_final_index].to_s
                                              
                                              if !@final_url.to_s.include?(".adeqo.") && @tracking_type.to_s.downcase == "adeqo" && @ad_redirect.to_s.downcase == "yes" && @final_url.to_s != ""
                                                  @temp_final_url = @final_url
                                                  
                                                  @final_url = "http://t.adeqo.com/click?company_id="+@company_id.to_s+"&network_id="+doc['id'].to_s
                                                  @final_url = @final_url + "&campaign_id={planid}&adgroup_id={groupid}&ad_id={creativeid}&keyword_id={wordid}"
                                                  @final_url = @final_url + "&cookie="+@cookie_length.to_s
                                                  @final_url = @final_url + "&device=pc"
                                                  @final_url = @final_url + "&tv=v1&durl="+CGI.escape(@temp_final_url.to_s)
                                                  
                                                  url_tag = 1
                                              end
                                              
                                              if !@m_final_url.to_s.include?(".adeqo.") && @tracking_type.to_s.downcase == "adeqo" && @ad_redirect.to_s.downcase == "yes" && @m_final_url.to_s != ""
                                                  @temp_m_final_url = @m_final_url
                                                  
                                                  @m_final_url = "http://t.adeqo.com/click?company_id="+@company_id.to_s+"&network_id="+doc["id"].to_s
                                                  @m_final_url = @m_final_url + "&campaign_id={planid}&adgroup_id={groupid}&ad_id={creativeid}&keyword_id={wordid}"
                                                  @m_final_url = @m_final_url + "&cookie="+@cookie_length.to_s
                                                  @m_final_url = @m_final_url + "&device=mobile"
                                                  @m_final_url = @m_final_url + "&tv=v1&durl="+CGI.escape(@temp_m_final_url.to_s)
                                                  
                                                  m_url_tag = 1
                                              end
                                              
                                              
                                              begin
                                                  if url_tag == 1 || m_url_tag == 1
                                                      
                                                      if @remain_quote.to_i >= 500
                                                        
                                                          requesttypearray = []
                                                          request_str = '{"id":'+csv[@id_index].to_s+',"destinationUrl":"'+@final_url+'","mobileDestinationUrl":"'+@m_final_url+'"}'
                                                          
                                                          requesttypearray << request_str
                                                          request = '['+requesttypearray.join(",")+']'
                                                          
                                                          # @logger.info request
                                                          
                                                          body = { 
                                                              'creatives' => request
                                                          }
                                                          
                                                          @update_res = threesixty_api( doc["api_token"].to_s, @refresh_token, "creative", "update", body)
                                                          @affectedRecords = @update_res["creative_update_response"]["affectedRecords"]
                                                          @remain_quote = @response.headers["quotaremain"].to_i
                                                          
                                                          if !@update_res["creative_update_response"]["failures"].nil?
                                                              @final_url = csv[@final_url_index].to_s
                                                              @m_final_url = csv[@ad_mobile_final_index].to_s
                                                          end
                                                      end
                                                  end
                                              rescue Exception
                                                  @final_url = csv[@final_url_index].to_s
                                                  @m_final_url = csv[@ad_mobile_final_index].to_s
                                              end
                                              
                                              
                                    
                                              @threesixty_db[ad_db_name].insert_one({ 
                                                          network_id: doc["id"].to_i,
                                                          account_id: csv[@account_id_index].to_i,
                                                          account_name: csv[@account_name_index].to_s,
                                                          campaign_id: @ad_campaign_id.to_i,
                                                          campaign_name: csv[@campaign_name_index].to_s,
                                                          adgroup_id: @ad_adgroup_id.to_i,
                                                          ad_id: csv[@id_index].to_i,
                                                          title: csv[@ad_title_index].to_s, 
                                                          description_1: csv[@ad_desc1_index].to_s,
                                                          description_2: csv[@ad_desc2_index].to_s, 
                                                          status: csv[@ad_status_index].to_s,
                                                          sys_status: csv[@ad_sys_status_index].to_s,
                                                          show_url: csv[@display_url_index].to_s,
                                                          visit_url: @final_url.to_s,
                                                          mobile_show_url: csv[@ad_mobile_display_index].to_s,
                                                          mobile_visit_url: @m_final_url.to_s,
                                                          extend_ad_type: csv[@extend_ad_type_index].to_i,
                                                          response_code: "",
                                                          m_response_code: "",
                                                          update_date: @now,                                            
                                                          create_date: @now 
                                                          })
                                              @threesixty_db.close()            
                                              
                                              
                                                
                                                
                                               
                                               
                                              
                                              # @logger.info "ad ." + doc["id"].to_s + ad_db_name.to_s
                                          
                                          elsif !csv[@keyword_index].nil? && csv[@ad_title_index].nil? && !csv[@update_time_index].nil? && !csv[@create_time_index].nil? && !csv[@keyword_status_index].nil? && !csv[@keyword_sys_status_index].nil? 
                                                
                                                @keyword_campaign_id = ""
                                                @keyword_adgroup_id = ""
                                                
                                                
                                                if campaign_adgroup_hash["name"+csv[@adgroup_name_index].to_s+"camp"+csv[@campaign_name_index].to_s]
                                                    @keyword_campaign_id = campaign_adgroup_hash["name"+csv[@adgroup_name_index].to_s+"camp"+csv[@campaign_name_index].to_s][0].to_i
                                                    @keyword_adgroup_id = campaign_adgroup_hash["name"+csv[@adgroup_name_index].to_s+"camp"+csv[@campaign_name_index].to_s][1].to_i
                                                else
                                                
                                                    @adgroup = @threesixty_db[adgroup_db_name].find('campaign_name' => csv[@campaign_name_index].to_s,'adgroup_name' => csv[@adgroup_name_index].to_s)
                                                    @threesixty_db.close()
                                                    
                                                    if @adgroup.count.to_i > 0
                                                        
                                                        temp_arr = []
                                                        @adgroup.no_cursor_timeout.each do |doc|
                                                            @keyword_adgroup_id = doc["adgroup_id"]
                                                            @keyword_campaign_id = doc["campaign_id"]
                                                            
                                                            temp_arr << doc["campaign_id"]
                                                            temp_arr << doc["adgroup_id"]
                                                            
                                                            campaign_adgroup_hash["name"+csv[@adgroup_name_index].to_s+"camp"+csv[@campaign_name_index].to_s] = temp_arr
                                                        end
                                                    end
                                                    
                                                end
                                                
                                                
                                                
                                                url_tag = 0
                                                m_url_tag = 0
                                                
                                                @final_url = csv[@final_url_index].to_s
                                                @m_final_url = csv[@keyword_mobile_final_url_index].to_s
                                                
                                                if !@final_url.to_s.include?(".adeqo.") && @tracking_type.to_s.downcase == "adeqo" && @keyword_redirect.to_s.downcase == "yes" && @final_url.to_s != ""
                                                    @temp_final_url = @final_url
                                                    
                                                    @final_url = "http://t.adeqo.com/click?company_id="+@company_id.to_s+"&network_id="+doc["id"].to_s
                                                    @final_url = @final_url + "&campaign_id={planid}&adgroup_id={groupid}&ad_id={creativeid}&keyword_id="+csv[@id_index].to_s
                                                    @final_url = @final_url + "&cookie="+@cookie_length.to_s
                                                    @final_url = @final_url + "&device=pc"
                                                    @final_url = @final_url + "&tv=v1&durl="+CGI.escape(@temp_final_url.to_s)
                                                    
                                                    url_tag = 1
                                                end
                                                
                                                if !@m_final_url.to_s.include?(".adeqo.") && @tracking_type.to_s.downcase == "adeqo" && @keyword_redirect.to_s.downcase == "yes" && @m_final_url.to_s != ""
                                                    @temp_m_final_url = @m_final_url
                                                    
                                                    @m_final_url = "http://t.adeqo.com/click?company_id="+@company_id.to_s+"&network_id="+doc["id"].to_s
                                                    @m_final_url = @m_final_url + "&campaign_id={planid}&adgroup_id={groupid}&ad_id={creativeid}&keyword_id="+csv[@id_index].to_s
                                                    @m_final_url = @m_final_url + "&cookie="+@cookie_length.to_s
                                                    @m_final_url = @m_final_url + "&device=mobile"
                                                    @m_final_url = @m_final_url + "&tv=v1&durl="+CGI.escape(@temp_m_final_url.to_s)
                                                    
                                                    m_url_tag = 1
                                                end
                                                
                                                begin
                                                    if url_tag == 1 || m_url_tag == 1
                                                      
                                                        if @remain_quote.to_i >= 500
                                                          
                                                            requesttypearray = []
                                                            request_str = '{"id":'+csv[@id_index].to_s+',"url":"'+@final_url+'","mobileUrl":"'+@m_final_url+'"}'
                                                            
                                                            requesttypearray << request_str
                                                            request = '['+requesttypearray.join(",")+']'
                                                            
                                                            @logger.info request
                                                            
                                                            body = { 
                                                                'keywords' => request
                                                            }
                                                            
                                                            @update_res = threesixty_api( doc["api_token"].to_s, @refresh_token, "keyword", "update", body)
                                                            @affectedRecords = @update_res["keyword_update_response"]["affectedRecords"]
                                                            @remain_quote = @response.headers["quotaremain"].to_i
                                                            
                                                            # @logger.info @update_res
                                                            if !@update_res["keyword_update_response"]["failures"].nil?
                                                                @final_url = csv[@final_url_index].to_s
                                                                @m_final_url = csv[@keyword_mobile_final_url_index].to_s
                                                            end
                                                        end
                                                    end
                                                rescue Exception
                                                    @final_url = csv[@final_url_index].to_s
                                                    @m_final_url = csv[@keyword_mobile_final_url_index].to_s
                                                end
                                            
                                                @threesixty_db[keyword_db_name].insert_one({ 
                                                        network_id: doc["id"].to_i,
                                                        account_id: csv[@account_id_index].to_i,
                                                        account_name: csv[@account_name_index].to_s,
                                                        campaign_id: @keyword_campaign_id.to_i,
                                                        campaign_name: csv[@campaign_name_index].to_s,
                                                        adgroup_id: @keyword_adgroup_id.to_i,
                                                        keyword_id: csv[@id_index].to_i,
                                                        keyword: csv[@keyword_index].to_s,
                                                        price: csv[@keyword_price_index].to_f, 
                                                        status: csv[@keyword_status_index].to_s,
                                                        sys_status: csv[@keyword_sys_status_index].to_s,
                                                        match_type: csv[@keyword_match_type_index].to_s,
                                                        visit_url: @final_url.to_s,
                                                        mobile_visit_url: @m_final_url.to_s,
                                                        cpc_quality: csv[@keyword_cpc_quality_index].to_f,
                                                        # negative_words: csv[@keyword_negative_index].to_s,
                                                        extend_ad_type: csv[@extend_ad_type_index].to_i,
                                                        response_code: "",
                                                        m_response_code: "",
                                                        update_date: @now,                                            
                                                        create_date: @now 
                                                        })
                                                        
                                                @threesixty_db.close() 
                                               
                                               
                                               
                                               
                                          else
                                              # @logger.info "ad keyword insert error."
                                          end
                                          
                                      end
                                  rescue Exception
                                      redownload(doc["id"])
                                      return render :nothing => true
                                  end     
                              end
                              
                              # updateaccount
                              @db[:network].find(id: doc["id"].to_i).update_one('$set'=> {'file_update_3' => 4,'file_update_4' => 4, 'last_update' => @now, 'worker' => ""})
                              @db.close
                              
                              
                              if doc['tmp_file'] != ""
                                unzip_folder = @tmp+"/"+doc["tmp_file"]+".csv"
                                if File.exists?(unzip_folder)
                                  File.delete(unzip_folder)
                                end
                              end
                              
                          else
                              redownload(doc["id"])
                              return render :nothing => true
                          end
                      end
                  end
                  @logger.info "360 structure, network "+doc["id"].to_s+ " done ad and keyword"
              end
          rescue Exception
              redownload(doc["id"])
              return render :nothing => true
          end
    end
    
    @logger.info "360 structure done ad and keyword"     
    return render :nothing => true 
  end
  
  
  
  def index
    
    @logger.info "called 360 structure"
    
    @id = params[:id]
    if @id.nil?
      
        @current_network = @db[:network].find({ '$or' => [ {'file_update_1' => 3},{'file_update_2' => 3},{'file_update_3' => 3},{'file_update_4' => 3} ],'type' => '360', 'worker' => @port.to_i})
        @db.close
        
        if @current_network.count.to_i >= 1
            @logger.info "working, no need update 360 index"
            return render :nothing => true
        end
      
        @network = @db[:network].find({ "$and" => [{:type => '360'}, {:file_update_1 => 2}, {:file_update_2 => 2}, {:file_update_3 => 2}, {:file_update_4 => 2}, {:worker => @port.to_i}] }).sort({ last_update: -1 }).limit(1)
        @db.close
        
        if @network.count.to_i == 0
            @logger.info "no need update 360 index"
            return render :nothing => true
        end
    else
        @network = @db[:network].find({ "$and" => [{:id => @id.to_i}, {:type => '360'}] })
        @db.close
    end
    
    campaign_hash = {}
    campaign_adgroup_hash = {}
    
    @network.no_cursor_timeout.each do |doc|
          # begin
              @do = 1
              
              if doc['tmp_file'].to_s != ""
                  @tmp_file = "/datadrive/"+ doc['tmp_file'].to_s + ".zip"
                  
                  @unzip_name = @tmp_file.gsub('.zip', '')
                  @unzip_folder = @unzip_name + "/*"
                  
                  if !File.exists?(@tmp_file) && !File.directory?(@unzip_name)
                      
                      redownload(doc["id"])
                      @do = 0
                      @logger.info "need to re download structure" + doc['id'].to_s
                      return render :nothing => true
                  end
              
              else
                
                  redownload(doc["id"])
                  @do = 0
                  @logger.info "need to re download structure" + doc['id'].to_s
                  return render :nothing => true
                
              end
              
              
              
              if !File.directory?(@unzip_name)
                  begin
                      unzip_file(@tmp_file.to_s, @unzip_name.to_s)
                      File.delete(@tmp_file)
                  
                  rescue Exception
                      redownload(doc["id"])
                      return render :nothing => true
                  end 
              end
              
              # data = {:message => "sad", :status => "false"}
              # return render :json => data, :status => :ok
      
              if @do == 1
                  login_info = login(doc["username"].to_s,doc["password"].to_s,doc["api_token"].to_s,doc["api_secret"].to_s)            
                  @refresh_token = login_info["account_clientLogin_response"]["accessToken"]
                  
                  if !@refresh_token.nil?
                      
                      # getthreesixtyfile(doc["id"].to_s, doc["username"],doc["password"],doc["api_token"],doc["api_secret"].to_s, doc["fileid"].to_s, doc["tmp_file"].to_s)
                            
                          @logger.info "360 network " + doc['id'].to_s + " done download csv/have csv"
                          # csvdetail(@fileid.to_s, @tmp_file_path, "account", doc["id"].to_s)
                          
                          @files = Dir.glob(@unzip_folder)
                              
                          @files.each_with_index do |file, index|
                              @file = file
                          end
                          
                          if File.exists?(@file)
                              
                              @db[:network].find(id: doc["id"].to_i).update_one('$set'=> {'file_update_1' => 3, 'file_update_2' => 3 ,'file_update_3' => 3 , 'file_update_4' => 3})
                              @db.close
                              
                              # campaign_db_name = "campaign_360_"+doc['id'].to_s
                              adgroup_db_name = "adgroup_360_"+doc['id'].to_s
                              ad_db_name = "ad_360_"+doc['id'].to_s
                              keyword_db_name = "keyword_360_"+doc['id'].to_s
                              
                              
                              #remove first
                              @logger.info "360 network " + doc['id'].to_s + " clean up first"
                              
                              @db["all_campaign"].find({ "$and" => [{:network_id => doc["id"].to_i}] }).delete_many
                              @db.close
                              
                              @threesixty_db[adgroup_db_name].drop
                              @threesixty_db[ad_db_name].drop
                              @threesixty_db[keyword_db_name].drop
                              
                              
                              @logger.info "360 network " + doc['id'].to_s + " update"
                              
                              @threesixty_db[adgroup_db_name].indexes.create_one(network_id: Mongo::Index::ASCENDING)
                              @threesixty_db[adgroup_db_name].indexes.create_one(account_id: Mongo::Index::ASCENDING)
                              @threesixty_db[adgroup_db_name].indexes.create_one(account_name: Mongo::Index::ASCENDING)
                              @threesixty_db[adgroup_db_name].indexes.create_one(campaign_id: Mongo::Index::ASCENDING)
                              @threesixty_db[adgroup_db_name].indexes.create_one(campaign_name: Mongo::Index::ASCENDING)
                              @threesixty_db[adgroup_db_name].indexes.create_one(adgroup_id: Mongo::Index::ASCENDING)
                              @threesixty_db[adgroup_db_name].indexes.create_one(adgroup_name: Mongo::Index::ASCENDING)
                              @threesixty_db[adgroup_db_name].indexes.create_one(price: Mongo::Index::ASCENDING)
                              @threesixty_db[adgroup_db_name].indexes.create_one(status: Mongo::Index::ASCENDING)
                              @threesixty_db[adgroup_db_name].indexes.create_one(sys_status: Mongo::Index::ASCENDING)
                              @threesixty_db[adgroup_db_name].indexes.create_one(api_update_ad: Mongo::Index::ASCENDING)
                              @threesixty_db[adgroup_db_name].indexes.create_one(api_update_keyword: Mongo::Index::ASCENDING)
                              @threesixty_db[adgroup_db_name].indexes.create_one(api_worker: Mongo::Index::ASCENDING)
                              @threesixty_db[adgroup_db_name].indexes.create_one(update_date: Mongo::Index::ASCENDING)
                              @threesixty_db[adgroup_db_name].indexes.create_one(create_date: Mongo::Index::ASCENDING)
                              
                              
                              @threesixty_db[ad_db_name].indexes.create_one(network_id: Mongo::Index::ASCENDING)
                              @threesixty_db[ad_db_name].indexes.create_one(account_id: Mongo::Index::ASCENDING)
                              @threesixty_db[ad_db_name].indexes.create_one(account_name: Mongo::Index::ASCENDING)
                              @threesixty_db[ad_db_name].indexes.create_one(campaign_id: Mongo::Index::ASCENDING)
                              @threesixty_db[ad_db_name].indexes.create_one(campaign_name: Mongo::Index::ASCENDING)
                              @threesixty_db[ad_db_name].indexes.create_one(adgroup_id: Mongo::Index::ASCENDING)
                              @threesixty_db[ad_db_name].indexes.create_one(ad_id: Mongo::Index::ASCENDING)
                              @threesixty_db[ad_db_name].indexes.create_one(title: Mongo::Index::ASCENDING)
                              @threesixty_db[ad_db_name].indexes.create_one(status: Mongo::Index::ASCENDING)
                              @threesixty_db[ad_db_name].indexes.create_one(sys_status: Mongo::Index::ASCENDING)
                              # @threesixty_db[ad_db_name].indexes.create_one(show_url: Mongo::Index::ASCENDING)
                              # @threesixty_db[ad_db_name].indexes.create_one(visit_url: Mongo::Index::ASCENDING)
                              # @threesixty_db[ad_db_name].indexes.create_one(mobile_show_url: Mongo::Index::ASCENDING)
                              # @threesixty_db[ad_db_name].indexes.create_one(mobile_visit_url: Mongo::Index::ASCENDING)
                              @threesixty_db[ad_db_name].indexes.create_one(extend_ad_type: Mongo::Index::ASCENDING)
                              # @threesixty_db[ad_db_name].indexes.create_one(watchdog: Mongo::Index::ASCENDING)
                              @threesixty_db[ad_db_name].indexes.create_one(response_code: Mongo::Index::ASCENDING)
                              @threesixty_db[ad_db_name].indexes.create_one(m_response_code: Mongo::Index::ASCENDING)
                              @threesixty_db[ad_db_name].indexes.create_one(update_date: Mongo::Index::ASCENDING)
                              @threesixty_db[ad_db_name].indexes.create_one(create_date: Mongo::Index::ASCENDING)
                              
                              
                              @threesixty_db[keyword_db_name].indexes.create_one(network_id: Mongo::Index::ASCENDING)
                              @threesixty_db[keyword_db_name].indexes.create_one(account_id: Mongo::Index::ASCENDING)
                              @threesixty_db[keyword_db_name].indexes.create_one(account_name: Mongo::Index::ASCENDING)
                              @threesixty_db[keyword_db_name].indexes.create_one(campaign_id: Mongo::Index::ASCENDING)
                              @threesixty_db[keyword_db_name].indexes.create_one(campaign_name: Mongo::Index::ASCENDING)
                              @threesixty_db[keyword_db_name].indexes.create_one(adgroup_id: Mongo::Index::ASCENDING)
                              @threesixty_db[keyword_db_name].indexes.create_one(keyword_id: Mongo::Index::ASCENDING)
                              @threesixty_db[keyword_db_name].indexes.create_one(keyword: Mongo::Index::ASCENDING)
                              @threesixty_db[keyword_db_name].indexes.create_one(price: Mongo::Index::ASCENDING)
                              @threesixty_db[keyword_db_name].indexes.create_one(status: Mongo::Index::ASCENDING)
                              @threesixty_db[keyword_db_name].indexes.create_one(sys_status: Mongo::Index::ASCENDING)
                              @threesixty_db[keyword_db_name].indexes.create_one(match_type: Mongo::Index::ASCENDING)
                              # @threesixty_db[keyword_db_name].indexes.create_one(visit_url: Mongo::Index::ASCENDING)
                              # @threesixty_db[keyword_db_name].indexes.create_one(mobile_visit_url: Mongo::Index::ASCENDING)
                              @threesixty_db[keyword_db_name].indexes.create_one(cpc_quality: Mongo::Index::ASCENDING)
                              @threesixty_db[keyword_db_name].indexes.create_one(extend_ad_type: Mongo::Index::ASCENDING)
                              # @threesixty_db[keyword_db_name].indexes.create_one(watchdog: Mongo::Index::ASCENDING)
                              @threesixty_db[keyword_db_name].indexes.create_one(response_code: Mongo::Index::ASCENDING)
                              @threesixty_db[keyword_db_name].indexes.create_one(m_response_code: Mongo::Index::ASCENDING)
                              @threesixty_db[keyword_db_name].indexes.create_one(update_date: Mongo::Index::ASCENDING)
                              @threesixty_db[keyword_db_name].indexes.create_one(create_date: Mongo::Index::ASCENDING)
                              
                              
                              campaign_data_arr = []
                              adgroup_data_arr = []
                              ad_data_arr = []
                              keyword_data_arr = []
                              
                              # GB18030
                              
                              CSV.foreach(@file, :encoding => 'GB18030').each_with_index do |csv, index|
                                  # @logger.info csv
                                  # if index.to_i > 10
                                      # data = {:message => csv, :status => "false"}
                                      # return render :json => data, :status => :ok
                                  # end
                                  
                                  if index.to_i == 0
                                      set_csv_header(csv)  
                                  end
                                  
                                  begin
                                      if index.to_i != 0
                                           
                                          if csv[@adgroup_name_index].nil? && csv[@keyword_index].nil? && csv[@ad_title_index].nil? && !csv[@update_time_index].nil? && !csv[@create_time_index].nil? && !csv[@campaign_status_index].nil? && !csv[@campaign_sys_status_index].nil?
                                              
                                              data_hash = {}
                                              insert_hash = {}
                                              
                                              regions_arr = []
                                              regions = csv[@campaign_region_index].to_s
                                              if regions.include?(";")
                                                  regions_arr = regions.split(";")
                                              else
                                                  regions_arr << regions
                                              end
                                              
                                              
                                              insert_hash[:network_id] = doc["id"].to_i
                                              insert_hash[:network_type] = "360"
                                              insert_hash[:account_id] = csv[@account_id_index].to_i
                                              insert_hash[:account_name] = csv[@account_name_index].to_s
                                              insert_hash[:campaign_id] = csv[@id_index].to_i
                                              insert_hash[:campaign_name] = csv[@campaign_name_index].to_s
                                              insert_hash[:budget] = csv[@budget_index].to_f
                                              insert_hash[:regions] = regions_arr
                                              insert_hash[:schedule] = csv[@campaign_schedule_index]
                                              insert_hash[:start_date] = csv[@campaign_start_time_index].to_s
                                              insert_hash[:end_date] = csv[@campaign_end_time_index].to_s
                                              insert_hash[:status] = csv[@campaign_status_index].to_s
                                              insert_hash[:sys_status] = csv[@campaign_sys_status_index].to_s
                                              insert_hash[:extend_ad_type] = csv[@extend_ad_type_index]
                                              insert_hash[:negative_words] = csv[@campaign_negative_index].to_s
                                              insert_hash[:exact_negative_words] = csv[@campaign_exact_negative_mode_index].to_s
                                              insert_hash[:mobile_price_rate] = csv[@mobile_search_price_index].to_f
                                              
                                              insert_hash[:api_update] = 0
                                              insert_hash[:api_worker] = ""
                                              
                                              insert_hash[:update_date] = @now
                                              insert_hash[:create_date] = @now
                                              
                                                  
                                              data_hash[:insert_one] = insert_hash
                                              campaign_data_arr << data_hash
                                            
                                              begin
                                              if campaign_data_arr.count.to_i > 0
                                                  @db[:all_campaign].bulk_write(campaign_data_arr)
                                                  @db.close
                                                  
                                                  campaign_data_arr = []
                                              end
                                              rescue Exception
                                                  data = {:campaign_data_arr => campaign_data_arr, :status => "true"}
                                                  return render :json => data, :status => :ok
                                              end
                                              # @db["all_campaign"].insert_one({ 
                                                          # network_id: doc["id"].to_i,
                                                          # network_type: "360", 
                                                          # account_id: csv[@account_id_index].to_i,
                                                          # account_name: csv[@account_name_index].to_s,
                                                          # campaign_id: csv[@id_index].to_i,
                                                          # campaign_name: csv[@campaign_name_index].to_s, 
                                                          # budget: csv[@budget_index].to_f, 
                                                          # regions: csv[@campaign_region_index], 
                                                          # schedule: csv[@campaign_schedule_index],
                                                          # start_date: csv[@campaign_start_time_index].to_s,
                                                          # end_date: csv[@campaign_end_time_index].to_s,
                                                          # status: csv[@campaign_status_index].to_s,
                                                          # sys_status: csv[@campaign_sys_status_index].to_s,
                                                          # extend_ad_type: csv[@extend_ad_type_index],
                                                          # negative_words: csv[@campaign_negative_index].to_s,
                                                          # exact_negative_words: csv[@campaign_exact_negative_mode_index].to_s,
                                                          # mobile_price_rate: csv[@mobile_search_price_index].to_f,
                                                          # update_date: @now,                                            
                                                          # create_date: @now
                                                        # })
                                              # @db.close
                                           
                                          elsif !csv[@adgroup_name_index].nil? && csv[@keyword_index].nil? && csv[@ad_title_index].nil? && !csv[@update_time_index].nil? && !csv[@create_time_index].nil? && !csv[@adgroup_status_index].nil? && !csv[@adgroup_sys_status_index].nil? 
                                              begin
                                              if campaign_data_arr.count.to_i > 0
                                                  @db[:all_campaign].bulk_write(campaign_data_arr)
                                                  @db.close
                                                  
                                                  campaign_data_arr = []
                                              end
                                              rescue Exception
                                                  data = {:campaign_data_arr => campaign_data_arr, :status => "true"}
                                                  return render :json => data, :status => :ok
                                              end
                                              
                                              @campaign_id = ""
                                              
                                              if campaign_hash["name"+csv[@campaign_name_index].to_s]
                                                  @campaign_id = campaign_hash["name"+csv[@campaign_name_index].to_s].to_i
                                              else
                                                  @campaign = @db["all_campaign"].find({ "$and" => [{:campaign_name => csv[@campaign_name_index].to_s}, {:network_type => '360'}, {:network_id => doc['id'].to_i}] }).limit(1)
                                                  @db.close
                                                  
                                                  if @campaign.count.to_i > 0
                                                      @campaign.no_cursor_timeout.each do |doc|
                                                          @campaign_id = doc["campaign_id"]
                                                          
                                                          campaign_hash["name"+csv[@campaign_name_index].to_s] = doc["campaign_id"].to_i 
                                                      end
                                                  end
                                              
                                              end
                                              
                                              data_hash = {}
                                              insert_hash = {}
                                            
                                              insert_hash[:network_id] = doc["id"].to_i
                                              insert_hash[:account_id] = csv[@account_id_index].to_i
                                              insert_hash[:account_name] = csv[@account_name_index].to_s
                                              insert_hash[:campaign_id] = @campaign_id.to_i
                                              insert_hash[:campaign_name] = csv[@campaign_name_index].to_s
                                              insert_hash[:adgroup_id] = csv[@id_index].to_i
                                              insert_hash[:adgroup_name] = csv[@adgroup_name_index].to_s
                                              insert_hash[:price] = csv[@adgroup_price_index].to_f
                                              insert_hash[:negative_words] = csv[@adgroup_negative_index].to_s
                                              insert_hash[:exact_negative_words] = csv[@adgroup_exact_negative_mode_index].to_s
                                              insert_hash[:status] = csv[@adgroup_status_index].to_s
                                              insert_hash[:sys_status] = csv[@adgroup_sys_status_index].to_s
                                              
                                              insert_hash[:api_update_ad] = 0
                                              insert_hash[:api_update_keyword] = 0
                                              insert_hash[:api_worker] = ""
                                              
                                              insert_hash[:update_date] = @now
                                              insert_hash[:create_date] = @now
                                                  
                                              data_hash[:insert_one] = insert_hash
                                              adgroup_data_arr << data_hash
                                            
                                              if adgroup_data_arr.count.to_i > 1000
                                                  @threesixty_db[adgroup_db_name].bulk_write(adgroup_data_arr)
                                                  @threesixty_db.close()
                                                  
                                                  adgroup_data_arr = []
                                              end
                                              
                                              
                                              # @threesixty_db[adgroup_db_name].insert_one({ 
                                                          # network_id: doc["id"].to_i,
                                                          # account_id: csv[@account_id_index].to_i,
                                                          # account_name: csv[@account_name_index].to_s,
                                                          # campaign_id: @campaign_id.to_i,
                                                          # campaign_name: csv[@campaign_name_index].to_s,
                                                          # adgroup_id: csv[@id_index].to_i,
                                                          # adgroup_name: csv[@adgroup_name_index].to_s,
                                                          # price: csv[@adgroup_price_index].to_f,
                                                          # negative_words: csv[@adgroup_negative_index].to_s,
                                                          # exact_negative_words: csv[@adgroup_exact_negative_mode_index].to_s,
                                                          # status: csv[@adgroup_status_index].to_s,
                                                          # sys_status: csv[@adgroup_sys_status_index].to_s,
                                                          # update_date: @now,                                            
                                                          # create_date: @now 
                                                          # })
#                                                           
                                              # @threesixty_db.close()
                                           
                                          elsif !csv[@ad_title_index].nil? && csv[@keyword_index].nil? && !csv[@update_time_index].nil? && !csv[@create_time_index].nil? && !csv[@ad_status_index].nil? && !csv[@ad_sys_status_index].nil?
                                              
                                              if adgroup_data_arr.count.to_i > 0
                                                  @threesixty_db[adgroup_db_name].bulk_write(adgroup_data_arr)
                                                  @threesixty_db.close()
                                                  
                                                  adgroup_data_arr = []
                                              end
                                              
                                              @ad_campaign_id = ""
                                              @ad_adgroup_id = ""
                                              
                                              if !campaign_adgroup_hash["name"+csv[@adgroup_name_index].to_s+"camp"+csv[@campaign_name_index].to_s].nil?
                                                
                                                  @test = 0
                                                  @ad_campaign_id = campaign_adgroup_hash["name"+csv[@adgroup_name_index].to_s+"camp"+csv[@campaign_name_index].to_s][0].to_i
                                                  @ad_adgroup_id = campaign_adgroup_hash["name"+csv[@adgroup_name_index].to_s+"camp"+csv[@campaign_name_index].to_s][1].to_i
                                              else
                                                
                                                  @test = 1
                                                  @adgroup = @threesixty_db[adgroup_db_name].find('campaign_name' => csv[@campaign_name_index].to_s, 'adgroup_name' => csv[@adgroup_name_index].to_s)
                                                  @threesixty_db.close()
                                                  
                                                  if @adgroup.count.to_i > 0
                                                      temp_arr = []
                                                    
                                                      @adgroup.no_cursor_timeout.each do |doc|
                                                          
                                                          @ad_adgroup_id = doc["adgroup_id"]
                                                          @ad_campaign_id = doc["campaign_id"]
                                                          
                                                          temp_arr << doc["campaign_id"]
                                                          temp_arr << doc["adgroup_id"]
                                                          
                                                          campaign_adgroup_hash["name"+csv[@adgroup_name_index].to_s+"camp"+csv[@campaign_name_index].to_s] = temp_arr
                                                      end
                                                  end
                                              end
                                              
                                              # data = {
                                                        # :adgroup_db_name => adgroup_db_name,
                                                        # :test => @test,
                                                        # :adgroup => @adgroup, 
                                                        # :campaign_name_index => csv[@campaign_name_index], 
                                                        # :adgroup_name_index => csv[@adgroup_name_index],
                                                        # :ad_campaign_id => @ad_campaign_id,
                                                        # :ad_adgroup_id => @ad_adgroup_id,
                                                        # :status => "true"}
                                              # return render :json => data, :status => :ok
                                              
                                              url_tag = 0
                                              m_url_tag = 0
                                              
                                              @final_url = csv[@final_url_index].to_s
                                              @m_final_url = csv[@ad_mobile_final_index].to_s
                                              
                                              if !@final_url.to_s.include?(".adeqo.") && @tracking_type.to_s.downcase == "adeqo" && @ad_redirect.to_s.downcase == "yes" && @final_url.to_s != ""
                                                  @temp_final_url = @final_url
                                                  
                                                  @final_url = "http://t.adeqo.com/click?company_id="+@company_id.to_s+"&network_id="+doc['id'].to_s
                                                  @final_url = @final_url + "&campaign_id={planid}&adgroup_id={groupid}&ad_id={creativeid}&keyword_id={wordid}"
                                                  @final_url = @final_url + "&cookie="+@cookie_length.to_s
                                                  @final_url = @final_url + "&device=pc"
                                                  @final_url = @final_url + "&tv=v1&durl="+CGI.escape(@temp_final_url.to_s)
                                                  
                                                  url_tag = 1
                                              end
                                              
                                              if !@m_final_url.to_s.include?(".adeqo.") && @tracking_type.to_s.downcase == "adeqo" && @ad_redirect.to_s.downcase == "yes" && @m_final_url.to_s != ""
                                                  @temp_m_final_url = @m_final_url
                                                  
                                                  @m_final_url = "http://t.adeqo.com/click?company_id="+@company_id.to_s+"&network_id="+doc["id"].to_s
                                                  @m_final_url = @m_final_url + "&campaign_id={planid}&adgroup_id={groupid}&ad_id={creativeid}&keyword_id={wordid}"
                                                  @m_final_url = @m_final_url + "&cookie="+@cookie_length.to_s
                                                  @m_final_url = @m_final_url + "&device=mobile"
                                                  @m_final_url = @m_final_url + "&tv=v1&durl="+CGI.escape(@temp_m_final_url.to_s)
                                                  
                                                  m_url_tag = 1
                                              end
                                              
                                              
                                              begin
                                                  if url_tag == 1 || m_url_tag == 1
                                                      
                                                      if @remain_quote.to_i >= 500
                                                        
                                                          requesttypearray = []
                                                          request_str = '{"id":'+csv[@id_index].to_s+',"destinationUrl":"'+@final_url+'","mobileDestinationUrl":"'+@m_final_url+'"}'
                                                          
                                                          requesttypearray << request_str
                                                          request = '['+requesttypearray.join(",")+']'
                                                          
                                                          # @logger.info request
                                                          
                                                          body = { 
                                                              'creatives' => request
                                                          }
                                                          
                                                          @update_res = threesixty_api( doc["api_token"].to_s, @refresh_token, "creative", "update", body)
                                                          @affectedRecords = @update_res["creative_update_response"]["affectedRecords"]
                                                          @remain_quote = @response.headers["quotaremain"].to_i
                                                          
                                                          if !@update_res["creative_update_response"]["failures"].nil?
                                                              @final_url = csv[@final_url_index].to_s
                                                              @m_final_url = csv[@ad_mobile_final_index].to_s
                                                          end
                                                      end
                                                  end
                                              rescue Exception
                                                  @final_url = csv[@final_url_index].to_s
                                                  @m_final_url = csv[@ad_mobile_final_index].to_s
                                              end
                                              

                                              
                                              data_hash = {}
                                              insert_hash = {}
                                            
                                              insert_hash[:network_id] = doc["id"].to_i
                                              insert_hash[:account_id] = csv[@account_id_index].to_i
                                              insert_hash[:account_name] = csv[@account_name_index].to_s
                                              insert_hash[:campaign_id] = @ad_campaign_id.to_i
                                              insert_hash[:campaign_name] = csv[@campaign_name_index].to_s
                                              insert_hash[:adgroup_id] = @ad_adgroup_id.to_i
                                              insert_hash[:ad_id] = csv[@id_index].to_i
                                              insert_hash[:title] = csv[@ad_title_index].to_s
                                              insert_hash[:description_1] = csv[@ad_desc1_index].to_s
                                              insert_hash[:description_2] = csv[@ad_desc2_index].to_s
                                              insert_hash[:status] = csv[@ad_status_index].to_s
                                              insert_hash[:sys_status] = csv[@ad_sys_status_index].to_s
                                              insert_hash[:show_url] = csv[@display_url_index].to_s
                                              insert_hash[:visit_url] = @final_url.to_s
                                              insert_hash[:mobile_show_url] = csv[@ad_mobile_display_index].to_s
                                              insert_hash[:mobile_visit_url] = @m_final_url.to_s
                                              insert_hash[:extend_ad_type] = csv[@extend_ad_type_index].to_i
                                              insert_hash[:response_code] = ""
                                              insert_hash[:m_response_code] = ""
                                              insert_hash[:create_date] = @now
                                              insert_hash[:update_date] = @now
                                              
                                              
                                                  
                                              data_hash[:insert_one] = insert_hash
                                              ad_data_arr << data_hash
                                            
                                              if ad_data_arr.count.to_i > 1000
                                                  @threesixty_db[ad_db_name].bulk_write(ad_data_arr)
                                                  @threesixty_db.close()
                                                  
                                                  ad_data_arr = []
                                              end
                                              
                                    
                                              # @threesixty_db[ad_db_name].insert_one({ 
                                                          # network_id: doc["id"].to_i,
                                                          # account_id: csv[@account_id_index].to_i,
                                                          # account_name: csv[@account_name_index].to_s,
                                                          # campaign_id: @ad_campaign_id.to_i,
                                                          # campaign_name: csv[@campaign_name_index].to_s,
                                                          # adgroup_id: @ad_adgroup_id.to_i,
                                                          # ad_id: csv[@id_index].to_i,
                                                          # title: csv[@ad_title_index].to_s, 
                                                          # description_1: csv[@ad_desc1_index].to_s,
                                                          # description_2: csv[@ad_desc2_index].to_s, 
                                                          # status: csv[@ad_status_index].to_s,
                                                          # sys_status: csv[@ad_sys_status_index].to_s,
                                                          # show_url: csv[@display_url_index].to_s,
                                                          # visit_url: @final_url.to_s,
                                                          # mobile_show_url: csv[@ad_mobile_display_index].to_s,
                                                          # mobile_visit_url: @m_final_url.to_s,
                                                          # extend_ad_type: csv[@extend_ad_type_index].to_i,
                                                          # response_code: "",
                                                          # m_response_code: "",
                                                          # update_date: @now,                                            
                                                          # create_date: @now 
                                                          # })
                                              # @threesixty_db.close()
                                         
                                          elsif !csv[@keyword_index].nil? && csv[@ad_title_index].nil? && !csv[@update_time_index].nil? && !csv[@create_time_index].nil? && !csv[@keyword_status_index].nil? && !csv[@keyword_sys_status_index].nil? 
                                                
                                                if adgroup_data_arr.count.to_i > 0
                                                    @threesixty_db[adgroup_db_name].bulk_write(adgroup_data_arr)
                                                    @threesixty_db.close()
                                                    
                                                    adgroup_data_arr = []
                                                end
                                                
                                                @keyword_campaign_id = ""
                                                @keyword_adgroup_id = ""
                                                
                                                
                                                if campaign_adgroup_hash["name"+csv[@adgroup_name_index].to_s+"camp"+csv[@campaign_name_index].to_s]
                                                    @keyword_campaign_id = campaign_adgroup_hash["name"+csv[@adgroup_name_index].to_s+"camp"+csv[@campaign_name_index].to_s][0].to_i
                                                    @keyword_adgroup_id = campaign_adgroup_hash["name"+csv[@adgroup_name_index].to_s+"camp"+csv[@campaign_name_index].to_s][1].to_i
                                                else
                                                
                                                    @adgroup = @threesixty_db[adgroup_db_name].find('campaign_name' => csv[@campaign_name_index].to_s,'adgroup_name' => csv[@adgroup_name_index].to_s)
                                                    @threesixty_db.close()
                                                    
                                                    if @adgroup.count.to_i > 0
                                                        
                                                        temp_arr = []
                                                        @adgroup.no_cursor_timeout.each do |doc|
                                                            @keyword_adgroup_id = doc["adgroup_id"]
                                                            @keyword_campaign_id = doc["campaign_id"]
                                                            
                                                            temp_arr << doc["campaign_id"]
                                                            temp_arr << doc["adgroup_id"]
                                                            
                                                            campaign_adgroup_hash["name"+csv[@adgroup_name_index].to_s+"camp"+csv[@campaign_name_index].to_s] = temp_arr
                                                        end
                                                    end
                                                    
                                                end
                                                
                                                
                                                
                                                url_tag = 0
                                                m_url_tag = 0
                                                
                                                @final_url = csv[@final_url_index].to_s
                                                @m_final_url = csv[@keyword_mobile_final_url_index].to_s
                                                
                                                if !@final_url.to_s.include?(".adeqo.") && @tracking_type.to_s.downcase == "adeqo" && @keyword_redirect.to_s.downcase == "yes" && @final_url.to_s != ""
                                                    @temp_final_url = @final_url
                                                    
                                                    @final_url = "http://t.adeqo.com/click?company_id="+@company_id.to_s+"&network_id="+doc["id"].to_s
                                                    @final_url = @final_url + "&campaign_id={planid}&adgroup_id={groupid}&ad_id={creativeid}&keyword_id="+csv[@id_index].to_s
                                                    @final_url = @final_url + "&cookie="+@cookie_length.to_s
                                                    @final_url = @final_url + "&device=pc"
                                                    @final_url = @final_url + "&tv=v1&durl="+CGI.escape(@temp_final_url.to_s)
                                                    
                                                    url_tag = 1
                                                end
                                                
                                                if !@m_final_url.to_s.include?(".adeqo.") && @tracking_type.to_s.downcase == "adeqo" && @keyword_redirect.to_s.downcase == "yes" && @m_final_url.to_s != ""
                                                    @temp_m_final_url = @m_final_url
                                                    
                                                    @m_final_url = "http://t.adeqo.com/click?company_id="+@company_id.to_s+"&network_id="+doc["id"].to_s
                                                    @m_final_url = @m_final_url + "&campaign_id={planid}&adgroup_id={groupid}&ad_id={creativeid}&keyword_id="+csv[@id_index].to_s
                                                    @m_final_url = @m_final_url + "&cookie="+@cookie_length.to_s
                                                    @m_final_url = @m_final_url + "&device=mobile"
                                                    @m_final_url = @m_final_url + "&tv=v1&durl="+CGI.escape(@temp_m_final_url.to_s)
                                                    
                                                    m_url_tag = 1
                                                end
                                                
                                                begin
                                                    if url_tag == 1 || m_url_tag == 1
                                                      
                                                        if @remain_quote.to_i >= 500
                                                          
                                                            requesttypearray = []
                                                            request_str = '{"id":'+csv[@id_index].to_s+',"url":"'+@final_url+'","mobileUrl":"'+@m_final_url+'"}'
                                                            
                                                            requesttypearray << request_str
                                                            request = '['+requesttypearray.join(",")+']'
                                                            
                                                            @logger.info request
                                                            
                                                            body = { 
                                                                'keywords' => request
                                                            }
                                                            
                                                            @update_res = threesixty_api( doc["api_token"].to_s, @refresh_token, "keyword", "update", body)
                                                            @affectedRecords = @update_res["keyword_update_response"]["affectedRecords"]
                                                            @remain_quote = @response.headers["quotaremain"].to_i
                                                            
                                                            # @logger.info @update_res
                                                            if !@update_res["keyword_update_response"]["failures"].nil?
                                                                @final_url = csv[@final_url_index].to_s
                                                                @m_final_url = csv[@keyword_mobile_final_url_index].to_s
                                                            end
                                                        end
                                                    end
                                                rescue Exception
                                                    @final_url = csv[@final_url_index].to_s
                                                    @m_final_url = csv[@keyword_mobile_final_url_index].to_s
                                                end
                                                
                                                
                                                
                                                
                                                data_hash = {}
                                                insert_hash = {}
                                              
                                                insert_hash[:network_id] = doc["id"].to_i
                                                insert_hash[:account_id] = csv[@account_id_index].to_i
                                                insert_hash[:account_name] = csv[@account_name_index].to_s
                                                insert_hash[:campaign_id] = @keyword_campaign_id.to_i
                                                insert_hash[:campaign_name] = csv[@campaign_name_index].to_s
                                                insert_hash[:adgroup_id] = @keyword_adgroup_id.to_i
                                                insert_hash[:keyword_id] = csv[@id_index].to_i
                                                insert_hash[:keyword] = csv[@keyword_index].to_s
                                                insert_hash[:price] = csv[@keyword_price_index].to_f
                                                insert_hash[:status] = csv[@keyword_status_index].to_s
                                                insert_hash[:sys_status] = csv[@keyword_sys_status_index].to_s
                                                insert_hash[:match_type] = csv[@keyword_match_type_index].to_s
                                                insert_hash[:visit_url] = @final_url.to_s
                                                insert_hash[:mobile_visit_url] = @m_final_url.to_s
                                                insert_hash[:cpc_quality] = csv[@keyword_cpc_quality_index].to_f
                                                insert_hash[:extend_ad_type] = csv[@extend_ad_type_index].to_i
                                                insert_hash[:response_code] = ""
                                                insert_hash[:m_response_code] = ""
                                                insert_hash[:create_date] = @now
                                                insert_hash[:update_date] = @now
                                                
                                                
                                                    
                                                data_hash[:insert_one] = insert_hash
                                                keyword_data_arr << data_hash
                                              
                                                if keyword_data_arr.count.to_i > 1000
                                                    @threesixty_db[keyword_db_name].bulk_write(keyword_data_arr)
                                                    @threesixty_db.close()
                                                    
                                                    keyword_data_arr = []
                                                end
                                                
                                                
                                                
                                            
                                                # @threesixty_db[keyword_db_name].insert_one({ 
                                                        # network_id: doc["id"].to_i,
                                                        # account_id: csv[@account_id_index].to_i,
                                                        # account_name: csv[@account_name_index].to_s,
                                                        # campaign_id: @keyword_campaign_id.to_i,
                                                        # campaign_name: csv[@campaign_name_index].to_s,
                                                        # adgroup_id: @keyword_adgroup_id.to_i,
                                                        # keyword_id: csv[@id_index].to_i,
                                                        # keyword: csv[@keyword_index].to_s,
                                                        # price: csv[@keyword_price_index].to_f, 
                                                        # status: csv[@keyword_status_index].to_s,
                                                        # sys_status: csv[@keyword_sys_status_index].to_s,
                                                        # match_type: csv[@keyword_match_type_index].to_s,
                                                        # visit_url: @final_url.to_s,
                                                        # mobile_visit_url: @m_final_url.to_s,
                                                        # cpc_quality: csv[@keyword_cpc_quality_index].to_f,
                                                        # # negative_words: csv[@keyword_negative_index].to_s,
                                                        # extend_ad_type: csv[@extend_ad_type_index].to_i,
                                                        # response_code: "",
                                                        # m_response_code: "",
                                                        # update_date: @now,                                            
                                                        # create_date: @now 
                                                        # })
#                                                         
                                                # @threesixty_db.close()     
                                          else
                                          end
                                         
                                         
                                      end
                                  rescue Exception
                                  end     
                              end
                              
                              
                              if campaign_data_arr.count.to_i > 0
                                  @db[:all_campaign].bulk_write(campaign_data_arr)
                                  @db.close
                              end
                              
                              if adgroup_data_arr.count.to_i > 0
                                  @threesixty_db[adgroup_db_name].bulk_write(adgroup_data_arr)
                                  @threesixty_db.close()
                              end
                              
                              if ad_data_arr.count.to_i > 0
                                  @threesixty_db[ad_db_name].bulk_write(ad_data_arr)
                                  @threesixty_db.close()
                              end
                              
                              if keyword_data_arr.count.to_i > 0
                                  @threesixty_db[keyword_db_name].bulk_write(keyword_data_arr)
                                  @threesixty_db.close()
                              end
                              
                              @logger.info "360 network " + doc['id'].to_s + " update success"
                              @db[:network].find(id: doc["id"].to_i).update_one('$set'=> {'file_update_1' => 4, 'file_update_2' => 4 ,'file_update_3' => 4 , 'file_update_4' => 4, 'last_update' => @now, 'worker' => ""})
                              @db.close
                              
                              # if doc['tmp_file'] != ""
                                  # unzip_folder = @tmp+"/"+doc["tmp_file"]+".csv"
                                  # if File.exists?(unzip_folder)
                                    # File.delete(unzip_folder)
                                  # end
                              # end
                              
                              if File.directory?(@unzip_name)
                                  FileUtils.remove_dir @unzip_name, true
                              end
                              
                          else
                            
                              @logger.info "360 network " + doc['id'].to_s + " csv not exist"
                              redownload(doc["id"])
                              return render :nothing => true
                          end
                  end
                  @logger.info "360 structure, network "+doc["id"].to_s+ " done"
              end
          # rescue Exception
              # redownload(doc["id"])
              # return render :nothing => true
          # end
    end
    
    @logger.info "360 structure done"     
    return render :nothing => true        
  end
  
  
  def set_csv_header(array)
      array.each_with_index do |csv_header, header_index|
        
          if csv_header.to_s.strip == "ID"
            @id_index = header_index
          end
          
          if csv_header.to_s.strip == "推广账户ID"
            @account_id_index = header_index
          end
          
          if csv_header.to_s.strip == "推广账户"
            @account_name_index = header_index
          end
          
          if csv_header.to_s.strip == "推广计划名称"
            @campaign_name_index = header_index
          end
          
          if csv_header.to_s.strip == "推广计划每日预算"
            @budget_index = header_index
          end
          
          if csv_header.to_s.strip == "推广计划设备类型"
            @campaign_device_index = header_index
          end
          
          if csv_header.to_s.strip == "推广计划状态"
            @campaign_status_index = header_index
          end
          
          if csv_header.to_s.strip == "推广计划系统状态"
            @campaign_sys_status_index = header_index
          end
          
          if csv_header.to_s.strip == "推广组名称"
            @adgroup_name_index = header_index
          end
          
          if csv_header.to_s.strip == "推广组出价"
            @adgroup_price_index = header_index
          end
          
          if csv_header.to_s.strip == "推广组状态"
            @adgroup_status_index = header_index
          end
          
          if csv_header.to_s.strip == "推广组系统状态"
            @adgroup_sys_status_index = header_index
          end
          
          if csv_header.to_s.strip == "关键词"
            @keyword_index = header_index
          end
          
          if csv_header.to_s.strip == "关键词匹配模式"
            @keyword_match_type_index = header_index
          end
          
          if csv_header.to_s.strip == "关键词出价"
            @keyword_price_index = header_index
          end
          
          if csv_header.to_s.strip == "关键词状态"
            @keyword_status_index = header_index
          end
          
          if csv_header.to_s.strip == "关键词系统状态"
            @keyword_sys_status_index = header_index
          end
          
          if csv_header.to_s.strip == "关键词建议最低起价"
            @keyword_min_price_index = header_index
          end
          
          
          if csv_header.to_s.strip == "计划否定关键词"
            @campaign_negative_index = header_index
          end
          
          if csv_header.to_s.strip == "计划精确否定关键词"
            @campaign_exact_negative_mode_index = header_index
          end
          
          if csv_header.to_s.strip == "计划否定关键词匹配模式"
            @campaign_negative_mode_index = header_index
          end
          
          if csv_header.to_s.strip == "组精确否定关键词"
            @adgroup_exact_negative_mode_index = header_index
          end
          
          if csv_header.to_s.strip == "组否定关键词匹配模式"
            @adgroup_negative_mode_index = header_index
          end
          
          if csv_header.to_s.strip == "组否定关键词"
            @adgroup_negative_index = header_index
          end
          
          if csv_header.to_s.strip == "否定关键词"
            @keyword_negative_index = header_index
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
          
          if csv_header.to_s.strip == "创意状态"
            @ad_status_index = header_index
          end
          
          if csv_header.to_s.strip == "创意系统状态"
            @ad_sys_status_index = header_index
          end
          
          if csv_header.to_s.strip == "显示网址"
            @display_url_index = header_index
          end
          
          if csv_header.to_s.strip == "链接网址"
            @final_url_index = header_index
          end
          
          if csv_header.to_s.strip == "推广计划推广地域"
            @campaign_region_index = header_index
          end
          
          if csv_header.to_s.strip == "推广计划推广时段"
            @campaign_schedule_index = header_index
          end
          
          if csv_header.to_s.strip == "推广计划开始时间"
            @campaign_start_time_index = header_index
          end
          
          if csv_header.to_s.strip == "推广计划结束时间"
            @campaign_end_time_index = header_index
          end
          
          if csv_header.to_s.strip == "创建时间"
            @create_time_index = header_index
          end
          
          if csv_header.to_s.strip == "修改时间"
            @update_time_index = header_index
          end
          
          if csv_header.to_s.strip == "关键词质量度"
            @keyword_cpc_quality_index = header_index
          end
          
          if csv_header.to_s.strip == "物料类型"
            @extend_ad_type_index = header_index
          end
          
          if csv_header.to_s.strip == "关键词移动链接网址"
            @keyword_mobile_final_url_index = header_index
          end
          
          if csv_header.to_s.strip == "创意移动显示网址"
            @ad_mobile_display_index = header_index
          end
          
          if csv_header.to_s.strip == "创意移动链接网址"
            @ad_mobile_final_index = header_index
          end
          
          if csv_header.to_s.strip == "移动搜索出价比例"
            @mobile_search_price_index = header_index
          end
          
          if csv_header.to_s.strip == "推广电话"
            @campaign_mobile_number_index = header_index
          end
          
      end
  end
  
   
  def report_upper
      
      @days = params[:day]
      @default_day = 1
      
      if !@days.nil?
        @default_day = @days  
      end
      
      @id = params[:id]
      
      
      
      if @id.nil?
        
          if @days.nil?
            
              @current_network = @db[:network].find({ "$and" => [{:type => '360'}, {:report_upper => 1}, {:reportupper_worker => @port.to_i}] })
              @db.close
              
              if @current_network.count.to_i >= 1
                  @logger.info "one 360 report upper working"
                  return render :nothing => true
              end
              
              @network = @db[:network].find({ "$and" => [{:type => '360'}, {:report => 2}, {:report_upper => 0}, {:reportupper_worker => @port.to_i}] }).sort({ last_update: -1 }).limit(1)
              @db.close
              
              if @network.count.to_i == 0
                  @network = @db[:network].find({ "$and" => [{:type => '360'}, {:report => 2}, {:report_upper => 0}, {:reportupper_worker => ""}] }).sort({ last_update: -1 }).limit(1)
                  @db.close
              end
          else
              @network = @db[:network].find('type' => '360')
              @db.close  
          end
          
      else
          
          @network = @db[:network].find({ "$and" => [{:type => '360'}, {:id => @id.to_i}] })
          @db.close
      end 
      
      
      @today = Date.today.in_time_zone('Beijing') 
      edit_day = @today - @default_day.to_i.days
      
      request_end_date = edit_day
      request_start_date = request_end_date
      
      @end_date = request_end_date.strftime("%Y-%m-%d")
      @start_date = request_start_date.strftime("%Y-%m-%d")
      
      @logger.info "called 360 report upper"
      
      
      all_campaign_hash = {}
      all_adgroup_hash = {}
      
      all_account_display_hash = {}
      all_account_click_hash = {}
      all_account_total_cost_hash = {}
      all_account_avg_position_hash = {}
      all_account_click_avg_price_hash = {}
      all_account_click_rate_hash = {}

      
      all_network_id_array = []
      
      
      if @network.count.to_i > 0
          @network.no_cursor_timeout.each do |doc|
            
              begin
                  
                  if @id.nil? && @days.nil?
                      @db[:network].find('id' => doc["id"].to_i).update_one('$set'=> { 'report_upper' => 1,'last_update' => @now, 'reportupper_worker' => @port.to_i })
                      @db.close
                  end
                  
                  @logger.info "360 report upper network " + doc["id"].to_s + " " + @end_date.to_s + " campaign start"
                  
                  all_campaign_id_array = []
                  
                  all_campaign_display_hash = {}
                  all_campaign_click_hash = {}
                  all_campaign_total_cost_hash = {}
                  all_campaign_avg_position_hash = {}
                  all_campaign_click_avg_price_hash = {}
                  all_campaign_click_rate_hash = {}
                  
                  all_network_id_array << doc["id"]
                  
                  all_account_display_hash["id"+doc["id"].to_s] = 0
                  all_account_click_hash["id"+doc["id"].to_s] = 0
                  all_account_total_cost_hash["id"+doc["id"].to_s] = 0
                  all_account_avg_position_hash["id"+doc["id"].to_s] = 0
                  
                  all_account_click_avg_price_hash["id"+doc["id"].to_s] = 0
                  all_account_click_rate_hash["id"+doc["id"].to_s] = 0
                  
                  
                  @campaign = @db[:all_campaign].find(:network_id => doc["id"])
                  
                  if @campaign.count.to_i > 0
              
                      @campaign.no_cursor_timeout.each do |campaign|
                          all_campaign_display_hash["id"+campaign["campaign_id"].to_s] = 0
                          all_campaign_click_hash["id"+campaign["campaign_id"].to_s] = 0
                          all_campaign_total_cost_hash["id"+campaign["campaign_id"].to_s] = 0
                          all_campaign_avg_position_hash["id"+campaign["campaign_id"].to_s] = 0
                          all_campaign_click_avg_price_hash["id"+campaign["campaign_id"].to_s] = 0
                          all_campaign_click_rate_hash["id"+campaign["campaign_id"].to_s] = 0
                          
                          all_campaign_id_array << campaign["campaign_id"]
                      end
                  end
                  
                  all_campaign_id_array.uniq
                
                  @report_keyword = @db3[:report_keyword_360].find({ "$and" => [{:campaign_id => { "$in" => all_campaign_id_array}}, {:report_date => @end_date.to_s}] })
                  @db3.close()
                  
                  
                  @report_keyword.no_cursor_timeout.each do |report|
                        
                        @logger.info report
                        
                        all_campaign_display_hash["id"+report["campaign_id"].to_s] = all_campaign_display_hash["id"+report["campaign_id"].to_s] + report["views"].to_i
                        all_campaign_click_hash["id"+report["campaign_id"].to_s] = all_campaign_click_hash["id"+report["campaign_id"].to_s] + report["clicks"].to_i
                        all_campaign_total_cost_hash["id"+report["campaign_id"].to_s] = all_campaign_total_cost_hash["id"+report["campaign_id"].to_s].to_f.round(2) + report["total_cost"].to_f.round(2) 
                        
                        if report["avg_position"].to_f > 0 && report["views"].to_f > 0
                            all_campaign_avg_position_hash["id"+report["campaign_id"].to_s] = all_campaign_avg_position_hash["id"+report["campaign_id"].to_s] + (report["views"].to_f * report["avg_position"].to_f)
                        end
                        
                        all_account_display_hash["id"+report["network_id"].to_s] = all_account_display_hash["id"+report["network_id"].to_s] + report["views"].to_i
                        all_account_click_hash["id"+report["network_id"].to_s] = all_account_click_hash["id"+report["network_id"].to_s] + report["clicks"].to_i
                        all_account_total_cost_hash["id"+report["network_id"].to_s] = all_account_total_cost_hash["id"+report["network_id"].to_s].to_f.round(2) + report["total_cost"].to_f.round(2)
                        
                        @logger.info "."
                  end
                   
                  # data = {:all_campaign_total_cost_hash => all_campaign_total_cost_hash, :all_account_total_cost_hash => all_account_total_cost_hash, :status => "true"}
                  # return render :json => data, :status => :ok
                  
                  # @logger.info "360 report upper,campaign all start"
                  data_arr = []
                  @campaign.no_cursor_timeout.each do |campaign|
                    
                      # begin
                          @logger.info "360 report upper,campaign "+campaign["campaign_id"].to_s+ " date "+ @end_date.to_s + " updating"
                          campaign_id = campaign["campaign_id"]
                          # @logger.info campaign_id
                          
                          if all_campaign_total_cost_hash["id"+campaign_id.to_s].to_i > 0
                              all_campaign_click_avg_price_hash["id"+campaign_id.to_s] = all_campaign_total_cost_hash["id"+campaign_id.to_s].to_f / all_campaign_click_hash["id"+campaign_id.to_s].to_f
                          end
                          
                          if all_campaign_click_hash["id"+campaign_id.to_s].to_i > 0
                              all_campaign_click_rate_hash["id"+campaign_id.to_s] = (all_campaign_click_hash["id"+campaign_id.to_s].to_f/all_campaign_display_hash["id"+campaign_id.to_s].to_f)*100
                          end
                          
                          if all_campaign_avg_position_hash["id"+campaign_id.to_s].to_f > 0
                              all_campaign_avg_position_hash["id"+campaign_id.to_s] = all_campaign_avg_position_hash["id"+campaign_id.to_s].to_f / all_campaign_display_hash["id"+campaign_id.to_s].to_f
                          end
                          
                          
                          @db3[:report_campaign_360].find({ "$and" => [{:cpc_plan_id => campaign_id.to_i}, {:report_date => @end_date.to_s}] }).delete_many
                          @db3.close()
                          
                          data_hash = {}
                          insert_hash = {}
                        
                          insert_hash[:network_id] = campaign["network_id"].to_i
                          insert_hash[:report_date] = @end_date.to_s
                          insert_hash[:name] = campaign["account_name"].to_s
                          insert_hash[:cpc_plan_id] = campaign["campaign_id"].to_i
                          insert_hash[:cpc_plan_name] = campaign["campaign_name"].to_s
                          insert_hash[:total_cost] = all_campaign_total_cost_hash["id"+campaign_id.to_s].to_f
                          insert_hash[:clicks_avg_price] = all_campaign_click_avg_price_hash["id"+campaign_id.to_s].to_f
                          insert_hash[:display] = all_campaign_display_hash["id"+campaign_id.to_s].to_i
                          insert_hash[:avg_position] = all_campaign_avg_position_hash["id"+campaign_id.to_s].to_f
                          insert_hash[:click_rate] = all_campaign_click_rate_hash["id"+campaign_id.to_s].to_f
                          insert_hash[:clicks] = all_campaign_click_hash["id"+campaign_id.to_s].to_i
                          
                              
                          data_hash[:insert_one] = insert_hash
                          data_arr << data_hash
                          
                          all_campaign_display_hash["id"+campaign_id.to_s] = nil
                          all_campaign_click_hash["id"+campaign_id.to_s] = nil
                          all_campaign_total_cost_hash["id"+campaign_id.to_s] = nil
                          all_campaign_avg_position_hash["id"+campaign_id.to_s] = nil
                          all_campaign_click_avg_price_hash["id"+campaign_id.to_s] = nil
                          all_campaign_click_rate_hash["id"+campaign_id.to_s] = nil
                          
                        
                          if data_arr.count.to_i > 1000
                              @db3[:report_campaign_360].bulk_write(data_arr)
                              @db3.close()
                              
                              data_arr = []
                          end
                              
                      # rescue Exception
                          # @logger.info "360 report upper network " + campaign["campaign_id"].to_s + " " + @end_date.to_s + " campaign fail"
                      # end
                  end
                  
                  if data_arr.count.to_i > 0
                      @db3[:report_campaign_360].bulk_write(data_arr)
                      @db3.close()
                  end    
                  
                  @logger.info "360 report upper network " + doc["id"].to_s + " " + @end_date.to_s + " campaign done"
                  
                  @logger.info "360 report upper network " + doc["id"].to_s + " " + @end_date.to_s + " account start"
                  
                  
                  
                  @keyword_report = @db3[:report_keyword_360].find('network_id' => doc['id'].to_i, "report_date" => @end_date.to_s)
                  @db3.close()      
                 
                  
                  temp_account_display = 0
                  temp_account_avg_pos = 0
                 
                  if @keyword_report.count.to_i > 0
                    
                     @keyword_report.no_cursor_timeout.each do |keyword_report|
                         temp_account_display = temp_account_display + keyword_report["views"].to_i
                        
                         if keyword_report['views'].to_i > 0 && keyword_report['avg_position'].to_f > 0
                             # temp_account_avg_pos = temp_account_avg_pos.to_f.round(2) + (keyword_report['display'].to_f * keyword_report['avg_position'].to_f)
                             all_account_avg_position_hash["id"+doc["id"].to_s] = all_account_avg_position_hash["id"+doc["id"].to_s].round(2) + (keyword_report['views'].to_f * keyword_report['avg_position'].to_f)
                         end
                     end
                  end
                  
                  
                  
                  
                  
                  begin
                      if all_account_avg_position_hash["id"+doc["id"].to_s].to_f > 0
                          all_account_avg_position_hash["id"+doc["id"].to_s] = all_account_avg_position_hash["id"+doc["id"].to_s].to_f / all_account_display_hash["id"+doc["id"].to_s].to_f
                      end 
                      
                      if all_account_total_cost_hash["id"+doc["id"].to_s].to_f > 0
                          all_account_click_avg_price_hash["id"+doc["id"].to_s] = all_account_total_cost_hash["id"+doc["id"].to_s].to_f / all_account_click_hash["id"+doc["id"].to_s].to_f
                      end
                      
                      if all_account_click_hash["id"+doc["id"].to_s].to_i > 0
                          all_account_click_rate_hash["id"+doc["id"].to_s] = (all_account_click_hash["id"+doc["id"].to_s].to_f/all_account_display_hash["id"+doc["id"].to_s].to_f)*100
                      end
                      
                      
                      
                      @db3[:report_account_360].find({ "$and" => [{:network_id => doc["id"].to_i}, {:report_date => @end_date.to_s}] }).delete_many
                      @db3.close()
                                
                                                  
                      @db3[:report_account_360].insert_one({
                                                              network_id: doc["id"].to_i,
                                                              report_date: @end_date.to_s,
                                                              name: doc["name"].to_s,
                                                              total_cost: all_account_total_cost_hash["id"+doc["id"].to_s].to_f,
                                                              clicks_avg_price: all_account_click_avg_price_hash["id"+doc["id"].to_s].to_f,
                                                              display:  all_account_display_hash["id"+doc["id"].to_s].to_i,
                                                              avg_position:  all_account_avg_position_hash["id"+doc["id"].to_s].to_f,
                                                              click_rate: all_account_click_rate_hash["id"+doc["id"].to_s].to_f,
                                                              clicks: all_account_click_hash["id"+doc["id"].to_s].to_i
                                                            })                      
                      @db3.close()
                  rescue Exception
                      @logger.info "360 report upper network " + doc["id"].to_s + " " + @end_date.to_s + " fail"
                  end
                  
                  @logger.info "360 report upper network " + doc["id"].to_s + " " + @end_date.to_s + " account done"
                
                  @logger.info "360 report upper network " + doc["id"].to_s + " " + @end_date.to_s + " adgroup start"
                  
                  
                  
                  
                  all_adgroup_id_array = []
                  
                  all_adgroup_display_hash = {}
                  all_adgroup_click_hash = {}
                  all_adgroup_total_cost_hash = {}
                  all_adgroup_avg_position_hash = {}
                  all_adgroup_click_avg_price_hash = {}
                  all_adgroup_click_rate_hash = {}
                  all_adgroup_keyword_views_hash = {}
                  
                  
                  
                  
                  db_name = "adgroup_360_"+doc['id'].to_s
                  @adgroup = @threesixty_db[db_name].find("network_id" => doc["id"].to_i)
                  @threesixty_db.close()
                  
                  if @adgroup.count.to_i > 0
                      @adgroup.no_cursor_timeout.each do |adgroup|
                          all_adgroup_id_array << adgroup["adgroup_id"]  
                          
                          all_adgroup_display_hash["id"+adgroup["adgroup_id"].to_s] = 0
                          all_adgroup_click_hash["id"+adgroup["adgroup_id"].to_s] = 0
                          all_adgroup_total_cost_hash["id"+adgroup["adgroup_id"].to_s] = 0
                          all_adgroup_avg_position_hash["id"+adgroup["adgroup_id"].to_s] = 0
                          all_adgroup_click_avg_price_hash["id"+adgroup["adgroup_id"].to_s] = 0
                          all_adgroup_click_rate_hash["id"+adgroup["adgroup_id"].to_s] = 0
                      end
                      
                      @report_keyword = @db3[:report_keyword_360].find({ "$and" => [{:group_id => { "$in" => all_adgroup_id_array}}, {:report_date => @end_date.to_s}] })
                      @db3.close()
                      
                      # @logger.info "------------------------------first-----------------------------------------------"
                      # @logger.info all_adgroup_display_hash["id1086888070"]
                      # @logger.info "------------------------------first-----------------------------------------------"
                      
                      if @report_keyword.count.to_i > 0  
                          @report_keyword.no_cursor_timeout.each do |report|
                              
                              # if report["group_id"].to_i == 1086888070
                                  # @logger.info "-----------------------------------------------------------------------------"
                                  # @logger.info report
#                                   
#                                   
                                  # @logger.info all_adgroup_display_hash["id1086888070"]
                                  # @logger.info "-----------------------------------------------------------------------------"
                              # end
                              
                              all_adgroup_click_hash["id"+report["group_id"].to_s] = all_adgroup_click_hash["id"+report["group_id"].to_s] + report['clicks'].to_i
                              all_adgroup_display_hash["id"+report["group_id"].to_s] = all_adgroup_display_hash["id"+report["group_id"].to_s] + report['views'].to_i
                              all_adgroup_total_cost_hash["id"+report["group_id"].to_s] = all_adgroup_total_cost_hash["id"+report["group_id"].to_s] + report['total_cost'].to_f
                              
                              
                              if report["avg_position"].to_f > 0 && report["views"].to_f > 0
                                  all_adgroup_avg_position_hash["id"+report["group_id"].to_s] = all_adgroup_avg_position_hash["id"+report["group_id"].to_s] + (report["views"].to_f * report["avg_position"].to_f) 
                              end
                          end
                      end
                      
                      # @logger.info all_adgroup_id_array.count.to_s
                      # @logger.info all_adgroup_display_hash
                       
                      # data = {:message => "85 report upper", :all_adgroup_id_array => all_adgroup_id_array.count.to_i, :all_adgroup_id_display => all_adgroup_display_hash, :status => "true"}
                      # return render :json => data, :status => :ok
                     
                     
                      data_arr = []
                     
                      @adgroup.no_cursor_timeout.each do |adgroup|
                          # begin
                              if all_adgroup_avg_position_hash["id"+adgroup["adgroup_id"].to_s].to_i > 0
                                  all_adgroup_avg_position_hash["id"+adgroup["adgroup_id"].to_s] = all_adgroup_avg_position_hash["id"+adgroup["adgroup_id"].to_s].to_f/all_adgroup_display_hash["id"+adgroup["adgroup_id"].to_s].to_f 
                              end
                              
                              if all_adgroup_total_cost_hash["id"+adgroup["adgroup_id"].to_s].to_i > 0
                                  all_adgroup_click_avg_price_hash["id"+adgroup["adgroup_id"].to_s] = all_adgroup_total_cost_hash["id"+adgroup["adgroup_id"].to_s].to_f/all_adgroup_click_hash["id"+adgroup["adgroup_id"].to_s].to_f
                              end
                              
                              if all_adgroup_click_hash["id"+adgroup["adgroup_id"].to_s].to_i > 0
                                  all_adgroup_click_rate_hash["id"+adgroup["adgroup_id"].to_s] = all_adgroup_display_hash["id"+adgroup["adgroup_id"].to_s].to_f/all_adgroup_click_hash["id"+adgroup["adgroup_id"].to_s].to_f 
                              end
                              
                              
                              @db3[:report_adgroup_360].find({ "$and" => [{:cpc_grp_id => adgroup["adgroup_id"].to_i}, {:report_date => @end_date.to_s}] }).delete_many
                              @db3.close()
                              
                              if all_adgroup_display_hash["id"+adgroup["adgroup_id"].to_s].to_i != 0
                                                            
                                  data_hash = {}
                                  insert_hash = {}
                                
                                  insert_hash[:network_id] = adgroup["network_id"].to_i
                                  insert_hash[:report_date] = @end_date.to_s
                                  insert_hash[:name] = adgroup['account_name'].to_s
                                  insert_hash[:cpc_plan_id] = adgroup['campaign_id'].to_i
                                  insert_hash[:cpc_plan_name] = adgroup['campaign_name'].to_s
                                  insert_hash[:cpc_grp_id] = adgroup['adgroup_id'].to_i
                                  insert_hash[:cpc_grp_name] = adgroup['adgroup_name'].to_s
                                  insert_hash[:total_cost] = all_adgroup_total_cost_hash["id"+adgroup["adgroup_id"].to_s].to_f
                                  insert_hash[:clicks_avg_price] = all_adgroup_click_avg_price_hash["id"+adgroup["adgroup_id"].to_s].to_f
                                  insert_hash[:display] = all_adgroup_display_hash["id"+adgroup["adgroup_id"].to_s].to_i
                                  insert_hash[:avg_position] = all_adgroup_avg_position_hash["id"+adgroup["adgroup_id"].to_s].to_f
                                  insert_hash[:click_rate] = all_adgroup_click_rate_hash["id"+adgroup["adgroup_id"].to_s].to_f
                                  insert_hash[:clicks] = all_adgroup_click_hash["id"+adgroup["adgroup_id"].to_s].to_i
                                  
                                      
                                  data_hash[:insert_one] = insert_hash
                                  data_arr << data_hash
                                
                                  if data_arr.count.to_i > 1000
                                      @db3[:report_adgroup_360].bulk_write(data_arr)
                                      @db3.close()
                                      
                                      data_arr = []
                                  end                          
                                                           
                                                            
                                  # @db3[:report_adgroup_360].insert_one({
                                                                        # network_id: adgroup["network_id"].to_i,
                                                                        # report_date: @end_date.to_s,
                                                                        # name: adgroup['account_name'].to_s,
                                                                        # cpc_plan_id: adgroup['campaign_id'].to_i,
                                                                        # cpc_plan_name: adgroup['campaign_name'].to_s,
                                                                        # cpc_grp_id: adgroup['adgroup_id'].to_i,
                                                                        # cpc_grp_name: adgroup['adgroup_name'].to_s,
                                                                        # total_cost: all_adgroup_total_cost_hash["id"+adgroup["adgroup_id"].to_s].to_f,
                                                                        # clicks_avg_price: all_adgroup_click_avg_price_hash["id"+adgroup["adgroup_id"].to_s].to_f,
                                                                        # display:  all_adgroup_display_hash["id"+adgroup["adgroup_id"].to_s].to_i,
                                                                        # avg_position:  all_adgroup_avg_position_hash["id"+adgroup["adgroup_id"].to_s].to_f,
                                                                        # click_rate:  all_adgroup_click_rate_hash["id"+adgroup["adgroup_id"].to_s].to_f,
                                                                        # clicks: all_adgroup_click_hash["id"+adgroup["adgroup_id"].to_s].to_i
                                                                      # })                                    
                                   # @db3.close()                               
                               end
                           # rescue Exception
                                # @logger.info "360 report upper network " + adgroup["adgroup_id"].to_s + " " + @end_date.to_s + " adgroup fail"
#                                 
                                # @db[:network].find('id' => doc["id"].to_i).update_one('$set'=> { 'report_upper' => 0,'last_update' => @now })
                                # @db.close
                           # end
                      end
                      
                      
                      if data_arr.count.to_i > 0
                          @db3[:report_adgroup_360].bulk_write(data_arr)
                          @db3.close()
                      end
                  end
                  
                   
                  # data = {:data_arr => data_arr.count.to_i,  :status => "true" }
                  # return render :json => data, :status => :ok
      
                  
                  
                  @logger.info "360 report upper network " + doc["id"].to_s + " " + @end_date.to_s + " adgroup end"
                  
                  if @id.nil? && @days.nil?
                      @db[:network].find('id' => doc["id"].to_i).update_one('$set'=> { 'report_upper' => 2,'last_update' => @now, 'reportupper_worker' => "" })
                      @db.close
                  end
                  
              rescue Exception
                  @logger.info "360 report upper network " + doc["id"].to_s + " " + @end_date.to_s + " fail"
                  
                  @db[:network].find('id' => doc["id"].to_i).update_one('$set'=> { 'report_upper' => 0,'last_update' => @now })
                  @db.close
              end
          end 
      end
      
      @logger.info "360 report upper done"
      return render :nothing => true 
  end
  
  def threemonthsreport
      
      @logger.info "360 report keep 3 months run"
      @three_months_ago = @now.to_date - 3.months
      @three_months_ago = @three_months_ago.strftime("%Y-%m") + "-01"
      
      @db3["report_account_360"].find('report_date' => { '$lt' => @three_months_ago.to_s }).delete_many
      @db3.close()
      @db3["report_campaign_360"].find('report_date' => { '$lt' => @three_months_ago.to_s }).delete_many
      @db3.close()
      @db3["report_adgroup_360"].find('report_date' => { '$lt' => @three_months_ago.to_s }).delete_many
      @db3.close()
      @db3["report_ad_360"].find('report_date' => { '$lt' => @three_months_ago.to_s }).delete_many
      @db3.close()
      @db3["report_keyword_360"].find('report_date' => { '$lt' => @three_months_ago.to_s }).delete_many
      @db3.close()
      
      @logger.info "360 report keep 3 months complete" 
      return render :nothing => true 
  end
  
  
  def resetreport
    
      @logger.info "360 reset report"
      
      # @db[:network].find('type' => '360').update_many('$set'=> { 'report' => 0,'report_upper' => 0,'last_update' => @now, 'report_worker' => "", 'reportupper_worker' => "" })
      # @db.close
      
      
      @network = @db["network"].find('type' => "360")
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
            
            @network = @db["network"].find('id' => { "$in" => arr_d}).update_many('$set'=> { 'report' => 0,'report_upper' => 0,'last_update' => @now, 'report_worker' => port_array[index].to_i, 'reportupper_worker' => port_array[index].to_i })
            @db.close
            
          end
      end
      
      
      @logger.info "360 reset report done"
      return render :nothing => true 
  end
  
  
  def report
      
      @days = params[:day]
      @default_day = 1
      
      if !@days.nil?
        @default_day = @days  
      end
      
      @id = params[:id]
      
      
      if @id.nil?
        
          if @days.nil?
           
              @current_network = @db[:network].find({ "$and" => [{:type => '360'}, {:report => 1}, {:report_worker => @port.to_i}] })
              @db.close
              
              if @current_network.count.to_i >= 2
                  @logger.info "one 360 report working"
                  return render :nothing => true
              end
              
              @network = @db[:network].find({ "$and" => [{:type => '360'}, {:report => 0}, {:report_worker => @port.to_i}] }).sort({ last_update: -1 }).limit(1)
              @db.close
              
              if @network.count.to_i == 0
                  @network = @db[:network].find({ "$and" => [{:type => '360'}, {:report => 0}, {:report_worker => ""}] }).sort({ last_update: -1 }).limit(1)
                  @db.close
              end
          else
              @network = @db[:network].find('type' => '360')
              @db.close  
          end
          
      else
        
          @network = @db[:network].find({ "$and" => [{:id => @id.to_i}, {:type => '360'}] })
          @db.close
      end 
      
      
      @today = Date.today.in_time_zone('Beijing') 
      edit_day = @today - @default_day.to_i.days
      
      request_end_date = edit_day
      request_start_date = request_end_date
      
      @end_date = request_end_date.strftime("%Y-%m-%d")
      @start_date = request_start_date.strftime("%Y-%m-%d")
      
      @logger.info "called report 360"
      @network.no_cursor_timeout.each do |doc|
                begin
                  
                    if @id.nil? && @days.nil?
                        @db[:network].find('id' => doc["id"].to_i).update_one('$set'=> { 'report' => 1,'last_update' => @now, 'report_worker' => @port.to_i })
                        @db.close
                    end
                    
                    @logger.info "360 report network " + doc["id"].to_s + " " + @end_date.to_s + " start"
              
                    login_info = login(doc["username"].to_s,doc["password"].to_s,doc["api_token"].to_s,doc["api_secret"].to_s)
                    @refresh_token = login_info["account_clientLogin_response"]["accessToken"]
                
                    if !@refresh_token.nil?
                        # @logger.info "360 report network " + doc["id"].to_s + " " + @end_date.to_s + " Api not correct"
                    
                    
                        @logger.info "360 report network " + doc["id"].to_s + " " + @end_date.to_s + " clear first"
                        # @db[:report_ad_360].find('network_id' => doc["id"].to_i, 'report_date' => @end_date.to_s).delete_many
                        # @db[:report_keyword_360].find('network_id' => doc["id"].to_i, 'report_date' => @end_date.to_s).delete_many
                        
                        
                        @db3[:report_ad_360].find({ "$and" => [{:network_id => doc["id"].to_i}, {:report_date => @end_date.to_s}] }).delete_many
                        @db3[:report_keyword_360].find({ "$and" => [{:network_id => doc["id"].to_i}, {:report_date => @end_date.to_s}] }).delete_many
                        @db3.close()
                        @db3.close()
                        
                        request = { 
                            'startDate' => @start_date,
                            'endDate' => @end_date,
                            'level' => "account"
                        }
                        
                        @ad_account_report_count = threesixty_api( doc["api_token"].to_s, @refresh_token, "report", "creativeCount", request)
                        @total_page = @ad_account_report_count["report_creativeCount_response"]["totalPage"]
                        @total_number = @ad_account_report_count["report_creativeCount_response"]["totalNumber"]
                        
                        @logger.info @total_number.to_s
                        @logger.info @total_page.to_s
                        
                        if @total_page.to_i > 0
                          
                            (1..@total_page.to_i).each do |i|
                                  
                                  @logger.info "360 report network " + doc["id"].to_s + " " + @end_date.to_s + " download ad report, page " + i.to_s
                                  
                                  request = { 
                                      'startDate' => @start_date,
                                      'endDate' => @end_date,
                                      'level' => "account",
                                      'page' => i
                                  }
                                  
                                  @ad_account_report = threesixty_api( doc["api_token"].to_s, @refresh_token, "report", "creative", request)
                                  @ad_account_report = @ad_account_report["report_creative_response"]["creativeList"]["item"]
                                  
                                  @logger.info "360 report network " + doc["id"].to_s + " " + @end_date.to_s + " ad report update, page " + i.to_s
                                  # @logger.info @ad_account_report.count.to_s
                                  
                                  
                                  if @total_number.to_i == 1
                                      @logger.info "the fucking last one"
                                      @logger.info @ad_account_report
                                      @logger.info @ad_account_report["views"]
                                      
                                      # if @ad_account_report["views"].to_i != 0
                                          @db3[:report_ad_360].insert_one({ 
                                                                            network_id: doc["id"].to_i,
                                                                            ad_id: @ad_account_report["creativeId"].to_i,
                                                                            group_id: @ad_account_report["groupId"].to_i,
                                                                            campaign_id: @ad_account_report["campaignId"].to_i,
                                                                            clicks: @ad_account_report["clicks"].to_i,
                                                                            views: @ad_account_report["views"].to_i,
                                                                            total_cost: @ad_account_report["totalCost"].to_f,
                                                                            report_date: @ad_account_report["date"].to_s, 
                                                                         })
                                           @db3.close()
                                      # end
                                  else
                                    
                                      ad_data_arr = []
                                      
                                      @ad_account_report.each do |report|
                                        
                                          # @logger.info report
                                          # if report["views"].to_i != 0
                                            
                                              data_hash = {}
                                              insert_hash = {}
                                            
                                              insert_hash[:network_id] = doc["id"].to_i
                                              insert_hash[:ad_id] = report["creativeId"].to_i
                                              insert_hash[:group_id] = report["groupId"].to_i
                                              insert_hash[:campaign_id] = report["campaignId"].to_i
                                              insert_hash[:clicks] = report["clicks"].to_i
                                              insert_hash[:views] = report["views"].to_i
                                              insert_hash[:total_cost] = report["totalCost"].to_f
                                              insert_hash[:report_date] = report["date"].to_s
                                              
                                                  
                                              data_hash[:insert_one] = insert_hash
                                              ad_data_arr << data_hash
                                            
                                              if ad_data_arr.count.to_i > 1000
                                                  @db3[:report_ad_360].bulk_write(ad_data_arr)
                                                  @db3.close()
                                                  
                                                  ad_data_arr = []
                                              end
                                            
                                              # @db3[:report_ad_360].insert_one({ 
                                                                                # network_id: doc["id"].to_i,
                                                                                # ad_id: report["creativeId"].to_i,
                                                                                # group_id: report["groupId"].to_i,
                                                                                # campaign_id: report["campaignId"].to_i,
                                                                                # clicks: report["clicks"].to_i,
                                                                                # views: report["views"].to_i,
                                                                                # total_cost: report["totalCost"].to_f,
                                                                                # report_date: report["date"].to_s, 
                                                                             # })
                                              # @db3.close()                              
                                          # end
                                      end
                                      
                                      if ad_data_arr.count.to_i > 0
                                          @db3[:report_ad_360].bulk_write(ad_data_arr)
                                          @db3.close()
                                      end
                                  end
                                  
                                  @logger.info "360 report network " + doc["id"].to_s + " " + @end_date.to_s + " ad report update done, page " + i.to_s
                                  @total_number = @total_number.to_i - @ad_account_report.count.to_i
                            end
                            
                            
                            
                            # data = {:ad_data_arr => ad_data_arr.count.to_i, :status => "true" }
                            # return render :json => data, :status => :ok
                        else
                            # @db[:network].find('id' => doc["id"].to_i).update_one('$set'=> { 'report' => 0,'last_update' => @now })
                            # @db.close 
                            
                            # @logger.info "360 report done with no pages"
                            # return render :nothing => true
                        end
                        
                        # __________________________________________________________________________________________________________________________
                        
                        
                        request = { 
                            'startDate' => @start_date,
                            'endDate' => @end_date,
                            'level' => "account"
                        }
                        
                        @keyword_account_report_count = threesixty_api( doc["api_token"].to_s, @refresh_token, "report", "keywordCount", request)
                        @total_page = @keyword_account_report_count["report_keywordCount_response"]["totalPage"]
                        @total_number = @keyword_account_report_count["report_keywordCount_response"]["totalNumber"]
                        
                        @logger.info "360 report network " + doc["id"].to_s + " " + @end_date.to_s + " download keyword report"
                        @logger.info @total_number.to_s
                        
                        if @total_page.to_i > 0
                            
                            
                            
                            (1..@total_page.to_i).each do |i|
                                  
                                  request = {
                                      'startDate' => @start_date,
                                      'endDate' => @end_date,
                                      'level' => "account",
                                      'page' => i
                                  }
                                  
                                  @keyword_account_report = threesixty_api( doc["api_token"].to_s, @refresh_token, "report", "keyword", request)
                                  @keyword_account_report = @keyword_account_report["report_keyword_response"]["keywordList"]["item"]
                                  
                                  @logger.info "360 report network " + doc["id"].to_s + " " + @end_date.to_s + " keyword report update, page " + i.to_s
                                  
                                  
                                  if @total_number.to_i == 1
                                      @logger.info "the fucking last one"
                                      # if @keyword_account_report["views"].to_i != 0
                                          @db3[:report_keyword_360].insert_one({ 
                                                                          network_id: doc["id"].to_i,
                                                                          keyword_id: @keyword_account_report["keywordId"].to_i,
                                                                          keyword: @keyword_account_report["keyword"].to_s,
                                                                          group_id: @keyword_account_report["groupId"].to_i,
                                                                          campaign_id: @keyword_account_report["campaignId"].to_i,
                                                                          clicks: @keyword_account_report["clicks"].to_i,
                                                                          views: @keyword_account_report["views"].to_i,
                                                                          total_cost: @keyword_account_report["totalCost"].to_f,                                                      
                                                                          avg_position: @keyword_account_report["avgPosition"].to_f,
                                                                          report_date: @keyword_account_report["date"].to_s, 
                                                                       })                              
                                                                       
                                          @db3.close()
                                      # end
                                  else
                                      keyword_data_arr = []
                                      
                                      @keyword_account_report.each do |report|
                                            # if report["views"].to_i != 0
                                              
                                                # @logger.info report
                                                
                                                
                                                data_hash = {}
                                                insert_hash = {}
                                              
                                                insert_hash[:network_id] = doc["id"].to_i
                                                insert_hash[:keyword_id] = report["keywordId"].to_i
                                                insert_hash[:keyword] = report["keyword"].to_s
                                                insert_hash[:group_id] = report["groupId"].to_i
                                                insert_hash[:campaign_id] = report["campaignId"].to_i
                                                insert_hash[:clicks] = report["clicks"].to_i
                                                insert_hash[:views] = report["views"].to_i
                                                insert_hash[:total_cost] = report["totalCost"].to_f
                                                insert_hash[:avg_position] = report["avgPosition"].to_f
                                                insert_hash[:report_date] = report["date"].to_s
                                                
                                                    
                                                data_hash[:insert_one] = insert_hash
                                                keyword_data_arr << data_hash
                                              
                                                if keyword_data_arr.count.to_i > 1000
                                                    @db3[:report_keyword_360].bulk_write(keyword_data_arr)
                                                    @db3.close()
                                                    
                                                    keyword_data_arr = []
                                                end
                                                
                                                
                                                
                                                 # @db3[:report_keyword_360].insert_one({ 
                                                                                  # network_id: doc["id"].to_i,
                                                                                  # keyword_id: report["keywordId"].to_i,
                                                                                  # keyword: report["keyword"].to_s,
                                                                                  # group_id: report["groupId"].to_i,
                                                                                  # campaign_id: report["campaignId"].to_i,
                                                                                  # clicks: report["clicks"].to_i,
                                                                                  # views: report["views"].to_i,
                                                                                  # total_cost: report["totalCost"].to_f,                                                      
                                                                                  # avg_position: report["avgPosition"].to_i,
                                                                                  # report_date: report["date"].to_s, 
                                                                               # })                              
#                                                                                
                                                 # @db3.close()                              
                                            # end
                                      end
                                      
                                      if keyword_data_arr.count.to_i > 0
                                          @db3[:report_keyword_360].bulk_write(keyword_data_arr)
                                          @db3.close()
                                      end
                                  end
                                  
                                  @logger.info "360 report network " + doc["id"].to_s + " " + @end_date.to_s + " keyword report update done, page " + i.to_s
                                  @total_number = @total_number.to_i - @keyword_account_report.count.to_i
                            end
                            
                            
                            
                            
                        else    
                          
                            # @logger.info "360 report network " + doc["id"].to_s + " " + @end_date.to_s + " none, reset "
                            # @db[:network].find('id' => doc["id"].to_i).update_one('$set'=> { 'report' => 0,'last_update' => @now})
                            # @db.close
                        end
                        
                    end
                    
                    
                    # @logger.info "360 report upper network " + doc["id"].to_s + " " + @end_date.to_s + " end"
                    
                    if @id.nil? && @days.nil?
                        @db[:network].find('id' => doc["id"].to_i).update_one('$set'=> { 'report' => 2,'last_update' => @now, 'report_worker' => "" })
                        @db.close
                    end
                    
                rescue Exception
                    @db[:network].find('id' => doc["id"].to_i).update_one('$set'=> { 'report' => 0,'last_update' => @now })
                    @db.close
                    # @logger.info "360 report upper network " + doc["id"].to_s + " " + @end_date.to_s + " fail"
                end
      end
      
      @logger.info "360 report done"
      return render :nothing => true 
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
  

end
