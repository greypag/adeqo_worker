json.array!(@shenmas) do |shenma|
  json.extract! shenma, :id
  json.url shenma_url(shenma, format: :json)
end
