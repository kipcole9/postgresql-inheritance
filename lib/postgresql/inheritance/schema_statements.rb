module ActiveRecord
  module ConnectionAdapters # :nodoc:
    module SchemaStatements

      # Creates a new table with the name +table_name+. +table_name+ may either
      # be a String or a Symbol.
      #
      # Add :inherits options for Postgres table inheritance.  If a table is inherited then
      # the primary key column is also inherited.  Therefore the :primary_key options is set to false
      # so we don't duplicate that colume.
      #
      # However the primary key column from the parent is not inherited as primary key so
      # we manually add it.  Lastly we also create indexes on the child table to match those
      # on the parent table since indexes are also not inherited.
      def create_table(table_name, options = {})
        if options[:inherits]
          options[:id] = false
          options.delete(:primary_key)
        end

        if schema = options.delete(:schema)
          # If we specify a schema then we only create it if it doesn't exist
          # and we only force create it if only the specific schema is in the search path
          table_name = "#{schema}.#{table_name}"
          return if table_exists?(table_name) && !just_this_schema_in_search_path?(schema)
        else
          # We only create (or force recreate) tables with no specified schema
          # if we have schemas that are not in the 'shared_schemas' or there are
          # no shared schemas in the search path.  That is we assume its a tenanted model
          # and tables need to go in a tenant schema.
          return if table_exists?(table_name) && !tenant_schema_in_search_path?(schema)
        end
        
        if parent_table = options.delete(:inherits)
          options[:options] = ["INHERITS (#{parent_table})", options[:options]].compact.join
        end
          
        td = create_table_definition table_name, options[:temporary], options[:options]

        unless options[:id] == false
          pk = options.fetch(:primary_key) {
            Base.get_primary_key table_name.to_s.singularize
          }

          td.primary_key pk, options.fetch(:id, :primary_key), options
        end

        yield td if block_given?

        if options[:force] && table_exists?(table_name)
          drop_table(table_name, options)
        end
        
        execute schema_creation.accept td
        
        if parent_table
          parent_table_primary_key = primary_key(parent_table)
          execute "ALTER TABLE #{table_name} ADD PRIMARY KEY (#{parent_table_primary_key})"
          indexes(parent_table).each do |index|
            add_index table_name, index.columns, :unique => index.unique
          end
        end

        td.indexes.each_pair { |c,o| add_index table_name, c, o }
      end
      
      # Should not be called normally, but this operation is non-destructive.
      # The migrations module handles this automatically.
      def initialize_schema_migrations_table
        # puts "Initializing schema migrations with schema search path as #{ActiveRecord::Base.connection.schema_search_path}"
        ActiveRecord::SchemaMigration.create_table
      end    
      
    private 
      def in_schema_search_path?(schema)
        schema_search_paths.include? schema.to_s
      end
      
      def just_this_schema_in_search_path?(schema)
        schema_search_paths.length == 1 && schema_search_paths.first == schema
      end
      
      def tenant_schema_in_search_path?(schema)
        (schema_search_paths - shared_schemas - Array(extensions_schema)).present?
      end
      
      def index_already_exists?(table_name, column_name, options)
        index_name = options[:name].to_s || index_name(table_name, column: column_name)
        index_name_exists?(table_name, index_name, false)
      end
    end
  end
end

module ActiveRecord
  module ConnectionAdapters
    module PostgreSQL
      module SchemaStatements
        # Parent of inherited tabled
        def parent_tables(table_name)
          sql = <<-SQL
            SELECT pg_namespace.nspname, pg_class.relname 
            FROM pg_catalog.pg_inherits 
              INNER JOIN pg_catalog.pg_class ON (pg_inherits.inhparent = pg_class.oid) 
              INNER JOIN pg_catalog.pg_namespace ON (pg_class.relnamespace = pg_namespace.oid) 
            WHERE inhrelid = '#{table_name}'::regclass
          SQL
          result = exec_query(sql, "SCHEMA")
          result.map{|a| a['relname']}
        end
      
        def parent_table(table_name)
          parents = parent_tables(table_name)
          parents.first
        end
      
        def add_index(table_name, column_name, options = {}) #:nodoc:
          return if index_already_exists?(table_name, column_name, options) && !just_this_schema_in_search_path?(schema_for_table(table_name))
          index_name, index_type, index_columns, index_options, index_algorithm, index_using = add_index_options(table_name, column_name, options)
          execute "CREATE #{index_type} INDEX #{index_algorithm} #{quote_column_name(index_name)} ON #{quote_table_name(table_name)} #{index_using} (#{index_columns})#{index_options}"
        end
      
        # Creates a schema for the given schema name.
        def create_schema schema_name
          exec_query("CREATE SCHEMA #{schema_name}") unless schema_exists?(schema_name)
        end

        # Drops the schema for the given schema name.
        def drop_schema schema_name
          exec_query("DROP SCHEMA #{schema_name} CASCADE") if schema_exists?(schema_name)
        end
      
        def create_enum(name, values, options = {})
          full_name = options[:schema] ? "#{options[:schema]}.#{name}" : name.to_s
          unless enum_type_exists?(full_name)
            execute "CREATE TYPE #{full_name} AS ENUM (#{values.map{|v| quote(v.to_s)}.join(', ')})"
          end
        end
      
        def create_domain(name, options = {})
          full_name = options[:schema] ? "#{options[:schema].to_s}.#{name.to_s}" : name.to_s
          unless domain_type_exists?(full_name)
            execute "CREATE DOMAIN #{full_name} AS #{options[:as]}"
          end
        end
      
        def create_composite_type(name, options = {})
          return if composite_type_exists?(name)
          schema_creation = ActiveRecord::ConnectionAdapters::AbstractAdapter::SchemaCreation.new(ActiveRecord::Base.connection)
          composite_definition = ActiveRecord::Base.connection.send :create_table_definition, :t, nil, {}
          yield composite_definition
          column_creation = composite_definition.columns.map{|col| schema_creation.send :visit_ColumnDefinition, col }.join(', ')
          qualified_name = [options[:schema], name].join('.')
          composite_sql = "CREATE TYPE #{qualified_name} AS (#{column_creation})"
          execute composite_sql 
        end
      
        def drop_composite_type(name)
          drop_type(name)
        end
      
        def drop_enum(name)
          drop_type(name)
        end
      
        def drop_type(name)
          execute "DROP TYPE IF EXISTS #{name}"
        end
        
        def drop_domain(name)
          execute "DROP DOMAIN IF EXISTS #{name} CASCADE"
        end
      
        def enum_types
          enum_query = <<-SQL
            SELECT pg_type.typname AS enumtype
            FROM pg_type 
            JOIN pg_enum 
              ON pg_enum.enumtypid = pg_type.oid;
          SQL
          exec_query(enum_query, "SCHEMA").rows.flatten.uniq
        end
      
        def composite_types
          composite_query = <<-SQL
            SELECT t.typname AS name
                FROM pg_catalog.pg_type t
                LEFT JOIN pg_catalog.pg_namespace n
                    ON n.oid = t.typnamespace
                WHERE ( t.typrelid = 0
                        OR ( SELECT c.relkind = 'c'
                                FROM pg_catalog.pg_class c
                                WHERE c.oid = t.typrelid
                            )
                    )
                    AND NOT EXISTS
                        ( SELECT 1
                            FROM pg_catalog.pg_type el
                            WHERE el.oid = t.typelem
                                AND el.typarray = t.oid
                        )
                    AND n.nspname <> 'pg_catalog'
                    AND n.nspname <> '#{extensions_schema}'
                    AND pg_catalog.pg_type_is_visible ( t.oid )
          SQL
          @composite_types ||= exec_query(composite_query, "SCHEMA").rows.flatten - enum_types - domain_types
        end
        
        def domain_types
          sql = "SELECT domain_name FROM information_schema.domains WHERE domain_schema <> 'information_schema'"
          exec_query(sql, "SCHEMA").rows.flatten
        end
        
        def domains_with_type_and_namespace
          sql = "SELECT domain_schema, domain_name, data_type FROM information_schema.domains WHERE domain_schema <> 'information_schema'"
          exec_query(sql, "SCHEMA").rows
        end
        
        def domain_type_exists?(domain)
          domain_types.include? domain
        end
        
        def enum_type_exists?(enum)
          enum_types.include? enum
        end

        def composite_type_exists?(composite)
          composite_types.include? composite
        end
             
        def extensions_with_namespace
          sql = <<-SQL
            SELECT pg_extension.extname, pg_namespace.nspname
            FROM pg_catalog.pg_extension
            INNER JOIN pg_catalog.pg_namespace ON (pg_extension.extnamespace = pg_namespace.oid) 
          SQL
          res = exec_query(sql, "SCHEMA")
          res.rows
        end
      
        def sequences_with_namespace
          sql = <<-SQL
            SELECT relname, nspname 
            FROM pg_class JOIN pg_namespace ON pg_namespace.oid = pg_class.relnamespace 
            WHERE relkind = 'S';
          SQL
          res = exec_query(sql, "SCHEMA")
          res.rows
        end
      
        # CASCADE added since we may have inherited tables and when :force => true
        # then they have to go as well.
        def drop_table(table_name, options = {})
          execute "DROP TABLE #{quote_table_name(table_name)} CASCADE"
        end
      
        # Sets the schema search path to a string of comma-separated schema names.
        # Names beginning with $ have to be quoted (e.g. $user => '$user').
        # See: http://www.postgresql.org/docs/current/static/ddl-schemas.html
        #
        # This should be not be called manually but set in database.yml.
        def schema_search_path=(schema_csv)
          if schema_csv
            execute("SET search_path TO #{schema_csv}")
            @schema_search_path = schema_csv
          end
        end

        # Returns the active schema search path.
        def schema_search_path
          #@schema_search_path ||= query('SHOW search_path', 'SCHEMA')[0][0]
          @schema_search_path = query('SHOW search_path', 'SCHEMA')[0][0]
        end
      
        def schema_for_table(table_name)
          schema_list = schema_search_paths.map{|s| '\'' + s.strip + '\''}.join(',')
          sql = "SELECT schemaname FROM pg_tables where tablename = '#{table_name}' and schemaname in (#{schema_list})"
          res = exec_query sql, "SCHEMA"
          res.rows.try(:first).try(:first)
        end
      
        def schema_for_enum(enum_name)
          sql = <<-SQL
            select n.nspname as enum_schema,  
                   t.typname as enum_name
            from pg_type t 
               join pg_enum e on t.oid = e.enumtypid  
               join pg_catalog.pg_namespace n ON n.oid = t.typnamespace
            where t.typname = '#{enum_name}'
            group by n.nspname, t.typname;
          SQL
          exec_query(sql, "SCHEMA").rows.flatten.try(:first)
        end
      
        def schema_for_composite_type(composite_type)
          composite_query = <<-SQL
            SELECT n.nspname AS name
                FROM pg_catalog.pg_type t
                LEFT JOIN pg_catalog.pg_namespace n
                    ON n.oid = t.typnamespace
                WHERE ( t.typrelid = 0
                        OR ( SELECT c.relkind = 'c'
                                FROM pg_catalog.pg_class c
                                WHERE c.oid = t.typrelid
                            )
                    )
                    AND NOT EXISTS
                        ( SELECT 1
                            FROM pg_catalog.pg_type el
                            WHERE el.oid = t.typelem
                                AND el.typarray = t.oid
                        )
                    AND n.nspname <> 'pg_catalog'
                    AND n.nspname <> '#{extensions_schema}'
                    AND pg_catalog.pg_type_is_visible ( t.oid )
                    AND typname = '#{composite_type}'
          SQL
          exec_query(composite_query, "SCHEMA").rows.flatten.try(:first)
        end
      
        def schema_search_paths
          schema_search_path.split(',').map(&:strip)
        end
      
        def default_search_path
          @default_search_path ||= Rails.configuration.database_configuration[Rails.env]['schema_search_path'] 
        end
      
        def shared_search_path
          @shared_search_path ||= (Rails.configuration.database_configuration[Rails.env]['shared_schemas'] || 'public')
        end
      
        def shared_schemas
          @shared_schemas ||= shared_search_path.split(',')
        end
      
        def extensions_schema
          @extensions_schema ||= Rails.configuration.database_configuration[Rails.env]['extensions_schema'] || 'public'
        end
      end
    end
  end
end
