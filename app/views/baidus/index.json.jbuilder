json.array!(@baidus) do |baidu|
  json.extract! baidu, :id
  json.url baidu_url(baidu, format: :json)
end
