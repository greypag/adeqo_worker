json.array!(@sogous) do |sogou|
  json.extract! sogou, :id
  json.url sogou_url(sogou, format: :json)
end
