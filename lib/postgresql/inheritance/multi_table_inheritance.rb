# Similar to STI we want to instantiate the right class type.  However unlike STI we don't have 
# a database column, we use the tableoid::regclass which is basically the source table name.
# so we convert it to a class name and use that.
module ActiveRecord
  module QueryMethods
  
  private
    # Retrieve the OID as well on a default select
    def build_select(arel, selects)
      unless selects.empty?
        @implicit_readonly = false
        arel.project(*selects)
      else
        arel.project("\"#{klass.table_name}\".\"tableoid\"::regclass as \"type\"") if @klass.using_multi_table_inheritance?
        arel.project(@klass.arel_table[Arel.star])
      end
    end
  end
end

module ActiveRecord
  module Inheritance
    module ClassMethods
      # base_class gets called early in the framework load so columns[] and columns_hash[] aren't set
      # up yet.  Hence the workaround.
      # TODO Revisit access
      #
      # Its a multi-table inheritance (Postgres inheritance) if this class has descendants, or its superclass is an active_record class
      # and the inheritance_column isn't in the table.  Which means its being derived from Postgres.
      # Also note that @columns_hash isn't set up until needed - on first database access.  Which means this will fail
      # if there hasn't been a database access before asking this question.  In practise this seems ok
      def using_multi_table_inheritance?
        respond_to?(:inheritable) && inheritable
      end
      
      def base_class
        unless self < Base
          raise ActiveRecordError, "#{name} doesn't belong in a hierarchy descending from ActiveRecord"
        end

        if superclass == Base || superclass.abstract_class? || using_multi_table_inheritance?
          self
        else
          superclass.base_class
        end
      end
      
      private

      # Called by +instantiate+ to decide which class to use for a new
      # record instance. For single-table inheritance, we check the record
      # for a +type+ column and return the corresponding class.
      def discriminate_class_for_record(record)
        if using_single_table_inheritance?(record) || using_multi_table_inheritance?
          find_sti_class(record[inheritance_column])
        else
          super
        end
      end
      
      # For inherited classes use the type name supplied which, for postgres, will
      # have been conformed from the tableoid
      def find_sti_class(type_name)
        if type_name.present? && !columns_hash.include?(inheritance_column)
          type_name.singularize.classify.constantize
        elsif type_name.blank? || !columns_hash.include?(inheritance_column)
          self
        else
          begin
            if store_full_sti_class
              ActiveSupport::Dependencies.constantize(type_name)
            else
              compute_type(type_name)
            end
          rescue NameError
            raise SubclassNotFound,
              "The single-table inheritance mechanism failed to locate the subclass: '#{type_name}'. " +
              "This error is raised because the column '#{inheritance_column}' is reserved for storing the class in case of inheritance. " +
              "Please rename this column if you didn't intend it to be used for storing the inheritance class " +
              "or overwrite #{name}.inheritance_column to use another column for that information."
          end
        end
      end
    end
  end
  
  # We'll allow descendants of the reflection class since they also
  # quack like a duck
  module Associations
    # Raises ActiveRecord::AssociationTypeMismatch unless +record+ is of
    # the kind of the class of the associated objects. Meant to be used as
    # a sanity check when you are about to assign an associated record.
    def raise_on_type_mismatch!(record)
      unless record.is_a?(reflection.klass) || record.is_a?(reflection.class_name.constantize) || reflection.class.descendants.include?(record.class)
        message = "#{reflection.class_name}(##{reflection.klass.object_id}) expected, got #{record.class}(##{record.class.object_id})"
        raise ActiveRecord::AssociationTypeMismatch, message
      end
    end
  end
end

module Postgresql
  module MultiTableInheritable
    def self.included(base)
      base.instance_eval do
        # Sets multi-table-inheritance behaviour to this and all descendants
        cattr_accessor :inheritable
        self.inheritable = true

        def self.oid
          select("#{self.table_name}.tableoid::regclass as #{inheritance_column}")
        end
      
        def self.ar_ancestors
          ancestors.delete_if {|a| !a.respond_to? :descends_from_active_record?}[1..-2]
        end
      
        def ancestor
          ar_ancestors.try(:first)
        end
      
        def self.generate_attribute_methods
          return unless connected? && table_exists?
          ar_ancestors.each do |ancestor|
            next unless ancestor.table_exists?
            ancestor.define_attribute_methods unless ancestor.attribute_methods_generated? 
          end
        end
      end
    end
  end
end

module ActiveRecord
  class Relation
    # If we getting rows from a table with inherited tables then we only get back the columns
    # that are inherited.  Hence we may want to reload to get the full rows of each inherited
    # table.  We group by table so that there is only one database query per subclass table.
    # We don't reload records from the base class itself.
    #
    # Note that row order is preserved. Also note that we assume model is derived from table name
    def reload_subclasses
      load
      if @records.any?
        grouped_results = @records.group_by{|row| row[self.inheritance_column]}.each do |table, rows|
          rows.collect!{ |row| row[self.primary_key] }
        end

        grouped_results.each do |table, ids|
          table_class = table.singularize.classify.constantize
          primary_key = table_class.primary_key
          table_class.find(ids).each do |row|
            key = row[primary_key]
            index = @records.index{|i| i[primary_key] == key}
            @records[index] = row
          end unless table_class == klass
        end
      end
      self
    end
  end
end

# Handle methods which represent user created enum types and geo types
module ActiveRecord
  module ConnectionAdapters
    module PostgreSQL
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
        
        def method_missing(method, *args, &block)
          if ActiveRecord::Base.connection.enum_type_exists? method.to_s
            options = args.extract_options!
            column(args[0], method.to_s, options)
          elsif ActiveRecord::Base.connection.composite_type_exists? method.to_s
            options = args.extract_options!
            column(args[0], method.to_s, options)
          elsif ActiveRecord::Base.connection.domain_type_exists? method.to_s
            options = args.extract_options!
            column(args[0], method.to_s, options)
          else
            super
          end
        end
      end
    end
  end
end

# Enable extensions in a given SCHEMA either supplied or
# defined in database configuration
module ActiveRecord
  module ConnectionAdapters
    class PostgreSQLAdapter 
      def enable_extension(name, options = {})
        schema = options[:schema] || Rails.configuration.database_configuration[Rails.env]['extensions_schema']
        exec_query("CREATE EXTENSION IF NOT EXISTS \"#{name}\" #{' SCHEMA ' + schema if schema}").tap {
          reload_type_map
        }
      end
      
      def create_sequence(name, options = {})
        full_name = options[:schema] ? "#{options[:schema]}.#{name}" : name
        unless ActiveRecord::Base.connection.sequences_with_namespace.map{|s| "#{s.second}.#{s.first}"}.include?(full_name)
          exec_query("CREATE SEQUENCE #{quote_table_name(full_name)}")
        end
      end
      
      # def disable_extension(name)
      #   exec_query("DROP EXTENSION IF EXISTS \"#{name}\" CASCADE").tap {
      #     reload_type_map
      #   }
      # end
      
      def clear_cache_for_search_path!(search_path)
        @statements.clear_for_search_path(search_path)
      end
      
      def prepare_column_options(column, types)
        spec = {}
        spec[:name]      = column.name.inspect
        spec[:type]      = column.type.to_s
        spec[:limit]     = column.limit.inspect if types[column.type] && column.limit != types[column.type][:limit]
        spec[:precision] = column.precision.inspect if column.precision
        spec[:scale]     = column.scale.inspect if column.scale
        spec[:null]      = 'false' unless column.null
        spec[:default]   = schema_default(column) if column.has_default?
        spec.delete(:default) if spec[:default].nil?
        spec[:array] = 'true' if column.respond_to?(:array) && column.array
        spec[:default] = "\"#{column.default_function}\"" if column.default_function
        if enum_types.include?(column.sql_type) || composite_types.include?(column.sql_type) || domain_types.include?(column.sql_type)
          spec[:type] = column.sql_type 
          spec.delete(:limit)
        end
        spec
      end
      
      class StatementPool < ConnectionAdapters::StatementPool
        def clear_for_search_path(search_path)
          cache.each_key do |key|
            path_key = key.split('-').first
            if path_key.gsub(' ','') == search_path.gsub(' ','')
              # puts "Deleting statement cache entry #{key}"
              delete(key)
            else
              puts "SHOULD HAVE DELETED #{path_key} given #{search_path}" if path_key =~ /test/
            end 
          end
        end
      end
    end
  end
end

module ActiveRecord
  module ConnectionAdapters
    class PostgreSQLAdapter < AbstractAdapter
      class ColumnDefinition < ActiveRecord::ConnectionAdapters::ColumnDefinition
        attr_accessor :array
      end
      
      class TableDefinition
        def column(name, type = nil, options = {})
          super
          column = self[name]
          column.array = options[:array]
          self
        end
      end
      
      class SchemaCreation < AbstractAdapter::SchemaCreation
        private
        
        # Modified because the table name may be schema.table_name
        def visit_TableDefinition(o)
          create_sql = "CREATE#{' TEMPORARY' if o.temporary} TABLE "
          create_sql << "#{quote_table_name(o.name)} ("
          create_sql << o.columns.map { |c| accept c }.join(', ')
          create_sql << ") #{o.options}"
          create_sql
        end
        
        # Modified so allow specification of pk sequence        
        def visit_ColumnDefinition(o)
          sql = super
          if o.primary_key? && o.type == :uuid
            sql << " PRIMARY KEY "
            add_column_options!(sql, column_options(o))
          end
          sql
        end
      end
    end
  end
end


module ActiveRecord
  module ConnectionAdapters
    # PostgreSQL-specific extensions to column definitions in a table.
    class PostgreSQLColumn < Column #:nodoc:

      # Extracts the value from a PostgreSQL column default definition.
      def self.extract_value_from_default(default)
        # Also extract default from user_defined enums
        return default unless default

        case default
          when /\A'(.*)'::(num|date|tstz|ts|int4|int8)range\z/m
            $1
          # Numeric types
          when /\A\(?(-?\d+(\.\d*)?\)?(::bigint)?)\z/
            $1
          # Character types
          when /\A\(?'(.*)'::.*\b(?:character varying|bpchar|text)\z/m
            $1
          # Binary data types
          when /\A'(.*)'::bytea\z/m
            $1
          # Date/time types
          when /\A'(.+)'::(?:time(?:stamp)? with(?:out)? time zone|date)\z/
            $1
          when /\A'(.*)'::interval\z/
            $1
          # Boolean type
          when 'true'
            true
          when 'false'
            false
          # Geometric types
          when /\A'(.*)'::(?:point|line|lseg|box|"?path"?|polygon|circle)\z/
            $1
          # Network address types
          when /\A'(.*)'::(?:cidr|inet|macaddr)\z/
            $1
          # Bit string types
          when /\AB'(.*)'::"?bit(?: varying)?"?\z/
            $1
          # XML type
          when /\A'(.*)'::xml\z/m
            $1
          # Arrays
          when /\A'(.*)'::"?\D+"?\[\]\z/
            $1
          # Hstore
          when /\A'(.*)'::hstore\z/
            $1
          # JSON
          when /\A'(.*)'::json\z/
            $1
          # Object identifier types
          when /\A-?\d+\z/
            $1
          else
            if ActiveRecord::Base.connection.enum_types.include? default.match(/\'(.*)\'::(.*)/).try(:[],2)
              $1
            else
              # Anything else is blank, some user type, or some function
              # and we can't know the value of that, so return nil.
              nil
            end
        end
      end
    end
  end
end
