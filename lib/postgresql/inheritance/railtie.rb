module Postgresql
  module Inheritance
    class Railtie < Rails::Railtie
      initializer "postgres_inheritance.configure_rails_initialization" do
        dir = File.dirname(__FILE__)
        load "#{dir}/multi_table_inheritance.rb"
        load "#{dir}/schema_statements.rb"
        load "#{dir}/postgres_oid.rb"
        load "#{dir}/schema_dumper.rb"
      end
    end
  end
end

