ActiveSupport.on_load(:active_record) do
  module ActiveRecord
    module ConnectionAdapters
      class PostgreSQLAdapter < AbstractAdapter
        module OID
          # TODO:  Rather than use a serializer, here may be a better place to
          # do casting of geo data to RGeo types
          class Geometry < Type
            def type_cast(value)
              return if value.nil?
              value
              # ConnectionAdapters::PostgreSQLColumn.string_to_rgeo value
            end
          end
        
          class Geography < Type
            def type_cast(value)
              return if value.nil?
              value
              # ConnectionAdapters::PostgreSQLColumn.string_to_rgeo value
            end
          end
          
          class Point < Type
            def type_cast(value)
              return if value.nil?
              value
              # ConnectionAdapters::PostgreSQLColumn.string_to_rgeo value
            end
          end
          
          class Varbit < Type
            def type; :varbit; end
            
            def type_cast(value)
              return if value.nil?
              value
            end
          end
        end
      end
    end
  end

  module ActiveRecord
    module ConnectionAdapters
      # PostgreSQL-specific extensions to column definitions in a table.
      class PostgreSQLColumn < Column #:nodoc:

        private

        # Maps PostgreSQL-specific data types to logical Rails types.
        # Used by schema_dumper to decide what column type to generate
        alias :simplified_type_without_geo :simplified_type
        def simplified_type(field_type)
          case field_type
          when 'geometry'
            :geometry
          when 'geography'
            :geography
          when 'point'
            :point
          when /bit/i
            :varbit
          else
            if ActiveRecord::Base.connection.enum_types.include?(field_type)
              :string
            else
              simplified_type_without_geo(field_type)
            end
          end
        end
      end
    end
  end

  # Used in migrations to create columns.  Because we can't assume postgis is always
  # available (ie Heroku) we'll translate Geo types to Point if no postgis, and from Point
  # to Geometry if there is.
  module ActiveRecord
    module ConnectionAdapters
      class PostgreSQLAdapter
        class TableDefinition
          def geometry(name, options = {})
            if ActiveRecord::Base.connection.extensions.include? 'postgis'
              column(name, 'geometry', options)
            else
              point(name, options)
            end
          end
        
          def geography(name, options = {})
            if ActiveRecord::Base.connection.extensions.include? 'postgis'
              column(name, 'geography', options)
            else
              point(name, options)
            end             
          end
          
          def point(name, options = {})
            if ActiveRecord::Base.connection.extensions.include? 'postgis'
              geography(name, options)
            else
              column(name, 'point', options)
            end
          end
          
          def varbit(name, options = {})
            column(name, 'varbit', options)
          end
        end
      
        base_types = NATIVE_DATABASE_TYPES.dup
        geo_types = {geography: {name: 'geography'}, geometry: {name: 'geometry'}, point: {name: 'point'}}
        other_types = {varbit: {name: 'varbit'}}
        self.send(:remove_const, :NATIVE_DATABASE_TYPES)
        self.const_set(:NATIVE_DATABASE_TYPES, base_types.merge(geo_types).merge(other_types))
      end
    end
  end

  # Register additional data types for Postgres so we don't get a console warning. 
  ActiveRecord::ConnectionAdapters::PostgreSQLAdapter::OID.instance_eval do
    register_type 'geography',  ::ActiveRecord::ConnectionAdapters::PostgreSQLAdapter::OID::Geography.new
    register_type 'geometry',   ::ActiveRecord::ConnectionAdapters::PostgreSQLAdapter::OID::Geometry.new
    register_type 'point',      ::ActiveRecord::ConnectionAdapters::PostgreSQLAdapter::OID::Point.new
    register_type 'varbit',     ::ActiveRecord::ConnectionAdapters::PostgreSQLAdapter::OID::Varbit.new
    alias_type 'regclass',      'text'

    # Register enum types as well
    ActiveRecord::Base.connection.enum_types.each do |enum|
      register_type enum, ::ActiveRecord::ConnectionAdapters::PostgreSQLAdapter::OID::Identity.new
    end
  end
  ActiveRecord::Base.connection.send(:reload_type_map) if ActiveRecord::Base.connected?
end
