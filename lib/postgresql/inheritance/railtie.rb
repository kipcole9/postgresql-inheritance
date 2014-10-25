module Postgresql
  module Inheritance
    class Railtie < Rails::Railtie
      initializer "postgres-inheritance.configure_rails_initialization" do
        dir = File.dirname(__FILE__)
        load "#{dir}/multi_table_inheritance.rb"
      end
    end
  end
end

