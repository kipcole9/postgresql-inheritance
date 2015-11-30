module Postgresql
  module Inheritance
    class Railtie < Rails::Railtie
      initializer "postgres-inheritance.configure_rails_initialization" do
        ActiveSupport.on_load(:active_record) do
          dir = File.dirname(__FILE__)
          require "#{dir}/multi_table_inheritance.rb"
        end
      end
      
      initializer "postgres-inheritance.configure_sti" do
        config.after_initialize do |app|
          dir = File.dirname(__FILE__)
          require "#{dir}/query_methods.rb"
        end
      end
    end
  end
end

