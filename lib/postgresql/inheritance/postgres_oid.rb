ActiveSupport.on_load(:active_record) do
  module ActiveRecord
    module Type
      class Geometry < Value
        include Mutable
        
        FACTORY = RGeo::Geographic.simple_mercator_factory(
          :wkb_parser => {:support_ewkb => true}, :wkb_generator => {:type_format => :ewkb, :hex_format => true, :emit_ewkb_srid => true})
        PROJECTION_FACTORY = FACTORY.projection_factory
        
        def type
          :geometry
        end

        def type_cast_from_database(value)
          value ? FACTORY.unproject(PROJECTION_FACTORY.parse_wkb(geo)) : nil
        end

        def type_cast_for_database(value)
          value ? FACTORY.project(value).as_binary.unpack('H*').first : nil
        end
      end
    
      class Geography < Value
        include Mutable
        
        FACTORY = RGeo::Geographic.spherical_factory(
          :wkb_parser => {:support_ewkb => true}, :wkb_generator => {:hex_format => true, :type_format => :ewkb, :emit_ewkb_srid => true})
        
        def type
          :geography
        end

        def type_cast_from_database(value)
          value ? FACTORY.parse_wkb(value) : nil
        end

        def type_cast_for_database(value)
          value ? value.as_binary : nil
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
        base_types = NATIVE_DATABASE_TYPES.dup
        geo_types = {geography: {name: 'geography'}, geometry: {name: 'geometry'}}
        enum_type = {enum: {name: 'enum'}}
        self.send(:remove_const, :NATIVE_DATABASE_TYPES)
        self.const_set(:NATIVE_DATABASE_TYPES, base_types.merge(geo_types).merge(enum_type))
      end
    end
  end

  # Register additional data types for Postgres so we don't get a console warning. 
  ActiveRecord::Base.connection.tap do |conn|
    conn.type_map.register_type 'geography',  ::ActiveRecord::Type::Geography.new
    conn.type_map.register_type 'geometry',   ::ActiveRecord::Type::Geometry.new
    conn.type_map.alias_type 'regclass',      'text'
    
    #ActiveRecord::Base.connection.enum_types.each do |enum|
    #  conn.type_map.register_type enum, ::ActiveRecord::ConnectionAdapters::PostgreSQL::OID::Enum.new
    #end
  end
end
