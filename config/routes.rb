Rails.application.routes.draw do
 


  get '/checktodayreport' => 'application#checktodayreport'
  get '/checklastdayreport' => 'application#checklastdayreport'
  
  
  get '/threesixties/checkreport' => 'threesixties#checkreport'
  get '/sogous/checkreport' => 'sogous#checkreport'
  get '/shenmas/checkreport' => 'shenmas#checkreport'
  get '/baidu/checkreport' => 'baidus#checkreport'


  get '/click' => 'application#click'
  get '/event' => 'application#event'

  get '/g_normal_conversion' => 'application#g_normal_conversion'

  get '/ctriprevenuereport' => 'application#ctriprevenuereport'

  get '/watchdog' => 'application#watchdog'
  get '/watchdograndom' => 'application#watchdograndom'
  get '/watchdogreset' => 'application#watchdogreset'
  get '/watchdogreport' => 'application#watchdogreport'
  
  get '/cleanwatchdogfile' => 'application#cleanwatchdogfile'
  
  

  get '/getmissingeventfile' => 'application#getmissingeventfile'
  get '/cleaneventfile' => 'application#cleaneventfile'
  get '/exporteventfile' => 'application#exporteventfile'
  get '/geteventfile' => 'application#geteventfile'
  
  get '/getmissingclickfile' => 'application#getmissingclickfile'
  get '/cleanclickfile' => 'application#cleanclickfile'
  get '/exportclickfile' => 'application#exportclickfile'
  get '/getclickfile' => 'application#getclickfile'




  get '/checktag' => 'application#checktag'

  get '/checkkeywordtag' => 'application#checkkeywordtag'
  get '/checkadtag' => 'application#checkadtag'
  
  get '/cleanlogfile' => 'application#cleanlogfile'
  get '/cleanbulkfile' => 'application#cleanbulkfile'
  get '/cleanadvancedsearchfile' => 'application#cleanadvancedsearchfile'

  post '/bulkjob' => 'application#bulkjob'
  get '/runbulkjob' => 'application#runbulkjob'
  
  
  
  get '/shenmas/test' => 'shenmas#test'
  get '/shenmas/report' => 'shenmas#report'
  
  get '/shenmas/resetreport' => 'shenmas#resetreport'
  get '/shenmas/resetnetwork' => 'shenmas#resetnetwork'
  get '/shenmas/resetdlfile' => 'shenmas#resetdlfile'
  
  get '/shenmas/avgposition' => 'shenmas#avgposition'
  
  get '/shenmas/report_tmp' => 'shenmas#report_tmp'
  
  get '/shenmas/dlaccfile' => 'shenmas#dlaccfile'
  get '/shenmas/campaign' => 'shenmas#campaign'
  get '/shenmas/adgroup' => 'shenmas#adgroup'
  get '/shenmas/ad' => 'shenmas#ad'
  get '/shenmas/keyword' => 'shenmas#keyword'
  
  get '/shenmas/apicampaign' => 'shenmas#apicampaign'
  get '/shenmas/apiadgroup' => 'shenmas#apiadgroup'
  
  get '/shenmas/updateaccount' => 'shenmas#updateaccount'
  

  # this 4 route are only for run it manually
  get '/sogous/campaign' => 'sogous#campaign'
  get '/sogous/adgroup' => 'sogous#adgroup'
  get '/sogous/ad' => 'sogous#ad'
  get '/sogous/keyword' => 'sogous#keyword'
  
  get '/sogous/apiadgroup' => 'sogous#apiadgroup'
  get '/sogous/apicampaign' => 'sogous#apicampaign'
  # this 4 route are only for run it manually
  
  get '/sogous/resetreport' => 'sogous#resetreport'
  get '/sogous/threemonthsreport' => 'sogous#threemonthsreport'
  get '/sogous/report' => 'sogous#report'
  get '/sogous/avgposition' => 'sogous#avgposition'
  get '/sogous/avgposition_upper' => 'sogous#avgpositionupper'
  get '/sogous/resetdlfile' => 'sogous#resetdlfile'
  get '/sogous/resetnetwork' => 'sogous#resetnetwork'
  
  get '/sogous/updateaccount' => 'sogous#updateaccount'
  get '/sogous/dlaccfile' => 'sogous#dlaccfile'
  
  
  # this only for testing
  # get '/sogous/drop' => 'sogous#drop'
  get '/test' => 'sogous#test'
  get '/test2' => 'threesixties#test'
  get '/test3' => 'baidus#test'
  get '/test4' => 'shenmas#test'

  
  get '/threesixties/campaign' => 'threesixties#campaign'
  get '/threesixties/adgroup' => 'threesixties#adgroup'
  get '/threesixties/ad' => 'threesixties#ad'
  get '/threesixties/keyword' => 'threesixties#keyword'
  
  get '/threesixties/campaignandadgroup' => 'threesixties#campaignandadgroup'
  get '/threesixties/adandkeyword' => 'threesixties#adandkeyword'
  
  get '/threesixties/apicampaign' => 'threesixties#apicampaign'
  get '/threesixties/apiadgroup' => 'threesixties#apiadgroup'
  
  get '/threesixties/resetreport' => 'threesixties#resetreport'
  get '/threesixties/threemonthsreport' => 'threesixties#threemonthsreport'
  get '/threesixties/report' => 'threesixties#report'
  get '/threesixties/report_upper' => 'threesixties#report_upper'
  get '/threesixties/resetdlfile' => 'threesixties#resetdlfile'
  get '/threesixties/resetnetwork' => 'threesixties#resetnetwork'
  
  get '/threesixties/updateaccount' => 'threesixties#updateaccount'
  get '/threesixties/dlaccfile' => 'threesixties#dlaccfile'
  
  get '/threemonthsevent' => 'application#threemonthsevent'
  get '/threemonthsclicks' => 'application#threemonthsclicks'
  get '/g_conversion' => 'application#g_conversion'
  get '/g_revenue' => 'application#g_revenue'
  
  get '/advancedsearchjob' => 'application#advancedsearchjob'
  get '/checkquote' => 'application#checkquote'
  
  
  get '/baidu/apicampaign' => 'baidus#apicampaign'
  get '/baidu/apiadgroup' => 'baidus#apiadgroup'
  
  get '/baidu/resetreport' => 'baidus#resetreport'
  get '/baidu/report' => 'baidus#report'
  get '/baidu/avgposition' => 'baidus#avgposition'
  
  
  
  get '/baidu/dlaccfile' => 'baidus#dlaccfile'
  
  get '/baidu/updateaccount' => 'baidus#updateaccount'
  
  get '/baidu/campaign' => 'baidus#campaign'
  get '/baidu/adgroup' => 'baidus#adgroup'
  get '/baidu/ad' => 'baidus#ad'
  get '/baidu/keyword' => 'baidus#keyword'
  
  get '/baidu/resetdlfile' => 'baidus#resetdlfile'
  get '/baidu/resetnetwork' => 'baidus#resetnetwork'
  
  
  # get '/get_event' => 'application#get_event'
  resources :sogous
  resources :threesixties
  resources :baidus
  resources :shenmas

  get '*unmatched_route' => 'application#not_found'
  
  # The priority is based upon order of creation: first created -> highest priority.
  # See how all your routes lay out with "rake routes".

  # You can have the root of your site routed with "root"
  # root 'welcome#index'

  # Example of regular route:
  #   get 'products/:id' => 'catalog#view'

  # Example of named route that can be invoked with purchase_url(id: product.id)
  #   get 'products/:id/purchase' => 'catalog#purchase', as: :purchase

  # Example resource route (maps HTTP verbs to controller actions automatically):

  
  # Example resource route with options:
  #   resources :products do
  #     member do
  #       get 'short'
  #       post 'toggle'
  #     end
  #
  #     collection do
  #       get 'sold'
  #     end
  #   end

  # Example resource route with sub-resources:
  #   resources :products do
  #     resources :comments, :sales
  #     resource :seller
  #   end

  # Example resource route with more complex sub-resources:
  #   resources :products do
  #     resources :comments
  #     resources :sales do
  #       get 'recent', on: :collection
  #     end
  #   end

  # Example resource route with concerns:
  #   concern :toggleable do
  #     post 'toggle'
  #   end
  #   resources :posts, concerns: :toggleable
  #   resources :photos, concerns: :toggleable

  # Example resource route within a namespace:
  #   namespace :admin do
  #     # Directs /admin/products/* to Admin::ProductsController
  #     # (app/controllers/admin/products_controller.rb)
  #     resources :products
  #   end
end
