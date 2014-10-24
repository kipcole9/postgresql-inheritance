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

module ActiveRecord
  module ConnectionAdapters
    module PostgreSQL
      module OID # :nodoc:
        class TypeMapInitializer
          private
          
          # If we are planning to manage the type on our own (as evidenced by 
          # the existence of an ActiveRecord::Type manager) then don't override
          def register_domain_type(row)
            return if defined?("ActiveRecord::Type::#{row['typname'].capitalize}".constantize)
            if base_type = @store.lookup(row["typbasetype"].to_i)
              register row['oid'], base_type
            else
              warn "unknown base type (OID: #{row["typbasetype"]}) for domain #{row["typname"]}."
            end
          end
        end
      end
    end
  end
end

ActiveSupport.on_load(:active_record) do
  # Used in migrations to create columns.  Because we can't assume postgis is always
  # available (ie Heroku) we'll translate Geo types to Point if no postgis, and from Point
  # to Geometry if there is.
  module ActiveRecord
    module ConnectionAdapters
      class PostgreSQLAdapter
        def reload_type_map
          super
          load_custom_database_types
        end
        
        def native_database_types
          @additional_db_types ||= {
            geography:  {name: 'geography'}, 
            geometry:   {name: 'geometry'},
          }.merge(NATIVE_DATABASE_TYPES)
        end

        def load_custom_database_types
          # FIXME Find a way to restore custom configuration of types after reload
          ActiveRecord::Base.connection.tap do |conn|
            conn.type_map.register_type 'geography',  ::ActiveRecord::Type::Geography.new
            conn.type_map.register_type 'geometry',   ::ActiveRecord::Type::Geometry.new
            conn.type_map.alias_type    'regclass',   'varchar'
          end
        end
      end
    end
  end

  # Register additional data types for Postgresql. 
  ActiveRecord::Base.connection.load_custom_database_types
end
