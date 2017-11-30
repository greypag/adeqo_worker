json.array!(@threesixties) do |threesixty|
  json.extract! threesixty, :id
  json.url threesixty_url(threesixty, format: :json)
end
