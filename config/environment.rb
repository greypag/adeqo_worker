# Load the Rails application.
require File.expand_path('../application', __FILE__)

# Initialize the Rails application.
Rails.application.initialize!

# ActionMailer::Base.smtp_settings = {
  # # :user_name => 'adeqo',
  # # :password => 'QQwe@123',
  # # :domain => 'china.adeqo.com',
  # # :address => 'smtp.sendgrid.net',
  # # :port => 587,
  # # :authentication => :plain,
  # # :enable_starttls_auto => true
#   
  # :user_name => 'jkwan@bmgww.com',
  # :password => 'DiuLaSing1999',
  # :expires => 60,
  # :domain => 'china.adeqo.com',
  # :address => 'smtp.gmail.com',
  # :port => 1111,
  # :authentication => :plain,
  # :ssl => true,
  # :enable_starttls_auto => true
# }

ActionMailer::Base.smtp_settings = {
  :port           => 587,
  :address        => 'smtp.mailgun.org',
  :user_name      => 'postmaster@china.adeqo.com',
  :password       => '637aec439e2750064b66250e547ae595',
  :domain         => 'china.adeqo.com',
  :authentication => :plain,
}
ActionMailer::Base.delivery_method = :smtp