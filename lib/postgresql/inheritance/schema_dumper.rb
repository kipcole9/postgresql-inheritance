# Modified SchemaDumper that knows how to dump
# inherited tables. Key is that we have to dump parent
# tables before we dump child tables (of course).
# In addition we have to make sure we don't dump columns
# that are inherited.
module ActiveRecord
  # = Active Record Schema Dumper
  #
  # This class is used to dump the database schema for some connection to some
  # output format (i.e., ActiveRecord::Schema).
  class SchemaDumper #:nodoc:
    private_class_method :new

    ##
    # :singleton-method:
    # A list of tables which should not be dumped to the schema.
    # Acceptable values are strings as well as regexp.
    # This setting is only used if ActiveRecord::Base.schema_format == :ruby
    cattr_accessor :ignore_tables
    @@ignore_tables = []

    def self.dump(connection=ActiveRecord::Base.connection, stream=STDOUT)
      new(connection).dump(stream)
      stream
    end

    def dump(stream)
      header(stream)
      schemas(stream)
      extensions(stream)
      enums(stream)
      domains(stream)
      composite_types(stream)
      tables(stream)
      trailer(stream)
      stream
    end

    private

    def initialize(connection)
      @connection = connection
      @types = @connection.native_database_types
      @version = Migrator::current_version rescue nil
      @dumped_tables = []
    end

    def header(stream)
      define_params = @version ? "version: #{@version}" : ""

      if stream.respond_to?(:external_encoding) && stream.external_encoding
        stream.puts "# encoding: #{stream.external_encoding.name}"
      end

      stream.puts <<-HEADER
# This file is auto-generated from the current state of the database. Instead
# of editing this file, please use the migrations feature of Active Record to
# incrementally modify your database, and then regenerate this schema definition.
#
# Note that this schema.rb definition is the authoritative source for your
# database schema. If you need to create the application database on another
# system, you should be using db:schema:load, not running all the migrations
# from scratch. The latter is a flawed and unsustainable approach (the more migrations
# you'll amass, the slower it'll run and the greater likelihood for issues).
#
# It's strongly recommended that you check this file into your version control system.

ActiveRecord::Schema.define(#{define_params}) do

HEADER
    end

    def trailer(stream)
      stream.puts "end"
    end
    
    def schemas(stream)
      if (search_paths = @connection.schema_search_paths).any?
        search_paths = search_paths - ['public']
        stream.puts "  # Create schemas (except public schema) configured in search_path option in database.yml" 
        search_paths.each do |schema|
          stream.puts "  create_schema #{schema.inspect}"
        end
        stream.puts
      end
    end

    def extensions(stream)
      return unless @connection.supports_extensions?
      extensions = @connection.extensions_with_namespace
      if extensions.any?
        stream.puts "  # These are extensions that must be enabled in order to support this database"
        extensions.each do |extension|
          stream.puts "  enable_extension #{extension.first.inspect}, schema: #{extension.second.inspect}"
        end
        stream.puts
      end
    end
    
    # Export enums types
    def enums(stream)
      enum_query = <<-SQL
        SELECT pg_type.typname AS enumtype, 
            pg_enum.enumlabel AS enumlabel
        FROM pg_type 
        JOIN pg_enum 
            ON pg_enum.enumtypid = pg_type.oid;
      SQL
      enums = @connection.execute(enum_query, "SCHEMA").group_by{|e| e['enumtype']}.each_with_object({}) do |(k, v), h|
         h[k] = v.map{|g| g['enumlabel']}
      end
      if enums.any?
        stream.puts "  # Create user defined Enum types"        
        enums.each do |enum, values|
          schema = @connection.schema_for_enum(enum)
          if @connection.shared_schemas.include?(schema)
            stream.puts "  create_enum :#{enum}, #{values.map(&:to_sym).inspect}, schema: :#{schema}"
          else
            stream.puts "  create_enum :#{enum}, #{values.map(&:to_sym).inspect}"
          end        
        end
        stream.puts
      end
    end
    
    # Domains
    def domains(stream)
      domains = @connection.domains_with_type_and_namespace
      if domains.any?
        stream.puts "  # These are user defined domains for this application"
        domains.each do |domain|
          stream.puts "  create_domain #{domain.second.inspect}, as: #{domain.third.inspect}, schema: #{domain.first.inspect}"
        end
        stream.puts
      end
    end
    
    # User defined composite types
    def composite_types(stream)
      composite_types = @connection.composite_types
      if composite_types.any?
        stream.puts "  # These are user defined composite data types for this application"
        composite_types.each do |composite_type|
          schema = @connection.schema_for_composite_type(composite_type)
          if @connection.shared_schemas.include?(schema)
            stream.puts "  create_composite_type :#{composite_type}, schema: :#{schema} do |t|"
          else
            stream.puts "  create_composite_type :#{composite_type} do |t|"
          end
          column_specs = @connection.columns(composite_type).map do |column|
            @connection.column_spec(column, @types)
          end.compact
        
          column_specs.each do |s|
            options = s.reject{|k, v| [:name,:type].include? k}.values
            stream.puts "    t.#{s[:type]}\t#{s[:name]}, #{options.join(', ')}"
          end
          stream.puts "  end"
          stream.puts
        end
      end
    end

    def tables(stream)
      @connection.tables.sort.each do |tbl|
        next if ['schema_migrations', ignore_tables].flatten.any? do |ignored|
          case ignored
          when String; remove_prefix_and_suffix(tbl) == ignored
          when Regexp; remove_prefix_and_suffix(tbl) =~ ignored
          else
            raise StandardError, 'ActiveRecord::SchemaDumper.ignore_tables accepts an array of String and / or Regexp values.'
          end
        end
        table(tbl, stream)
      end
    end

    # Output table and columns - but don't output columns that are inherited from
    # a parent table.
    #
    # TODO: Qualify with the schema name IF the table is in a schema other than the first
    # schema in the search path (not including the $user schema)
    def table(table, stream)
      return if already_dumped?(table)
      if parent_table = @connection.parent_table(table)
        table(parent_table, stream) 
        parent_column_names = @connection.columns(parent_table).map(&:name)
      end
      columns = @connection.columns(table)
      schema = @connection.schema_for_table(table)
      begin
        tbl = StringIO.new

        # first dump primary key column
        if @connection.respond_to?(:pk_and_sequence_for)
          pk, seq = @connection.pk_and_sequence_for(table)
        elsif @connection.respond_to?(:primary_key)
          pk = @connection.primary_key(table)
        end

        tbl.print "  create_table #{remove_prefix_and_suffix(table).inspect}"
        if parent_table
          tbl.print %Q(, inherits: "#{parent_table}")
        else
          if column = columns.detect { |c| c.name == pk }
            if pk != 'id'
              tbl.print %Q(, primary_key: "#{pk}")
            elsif column.sql_type == 'uuid'
              tbl.print ", id: :uuid"
              tbl.print %Q(, default: "#{column.default_function}") if column.default_function
            end
          else
            tbl.print ", id: false"
          end
        end
        if @connection.shared_schemas.include?(schema)
          tbl.print ", schema: #{schema.inspect}"
        else
          tbl.print ", force: true"
        end
        tbl.puts " do |t|"

        # then dump all non-primary key columns that are not inherited from a parent table
        column_specs = columns.map do |column|
          # raise StandardError, "Unknown type '#{column.sql_type}' for column '#{column.name}' with type #{column.type}" if @types[column.type].nil?
          next if column.name == pk
          next if parent_column_names && parent_column_names.include?(column.name)
          @connection.column_spec(column, @types)
        end.compact

        # find all migration keys used in this table
        keys = @connection.migration_keys

        # figure out the lengths for each column based on above keys
        lengths = keys.map { |key|
          column_specs.map { |spec|
            spec[key] ? spec[key].length + 2 : 0
          }.max
        }

        # the string we're going to sprintf our values against, with standardized column widths
        format_string = lengths.map{ |len| "%-#{len}s" }

        # find the max length for the 'type' column, which is special
        type_length = column_specs.map{ |column| column[:type].length }.max

        # add column type definition to our format string
        format_string.unshift "    t.%-#{type_length}s "

        format_string *= ''

        column_specs.each do |colspec|
          values = keys.zip(lengths).map{ |key, len| colspec.key?(key) ? colspec[key] + ", " : " " * len }
          values.unshift colspec[:type]
          tbl.print((format_string % values).gsub(/,\s*$/, ''))
          tbl.puts
        end

        tbl.puts "  end"
        tbl.puts

        indexes(table, tbl)

        tbl.rewind
        stream.print tbl.read
      # rescue => e
      #   stream.puts "# Could not dump table #{table.inspect} because of following #{e.class}"
      #   stream.puts "#   #{e.message}"
      #   stream.puts
      end

      @dumped_tables << table
      stream
    end

    # Output indexes but don't output indexes that are inherited from parent tables
    # since those will be created by create_table.        
    def indexes(table, stream)
      if (indexes = @connection.indexes(table)).any?
        if parent_table = @connection.parent_table(table)
          parent_indexes = @connection.indexes(parent_table)
        end
        
        indexes.delete_if {|i| is_parent_index?(i, parent_indexes) } if parent_indexes
        return if indexes.empty?
        
        add_index_statements = indexes.map do |index|
          statement_parts = [
            ('add_index ' + remove_prefix_and_suffix(index.table).inspect),
            index.columns.inspect,
            ('name: ' + index.name.inspect),
          ]
          statement_parts << 'unique: true' if index.unique
          
          index_lengths = (index.lengths || []).compact
          statement_parts << ('length: ' + Hash[index.columns.zip(index.lengths)].inspect) unless index_lengths.empty?
          
          index_orders = (index.orders || {})
          statement_parts << ('order: ' + index.orders.inspect) unless index_orders.empty?
          
          statement_parts << ('where: ' + index.where.inspect) if index.where
          
          statement_parts << ('using: ' + index.using.inspect) if index.using
          
          statement_parts << ('type: ' + index.type.inspect) if index.type
          
          '  ' + statement_parts.join(', ')
        end
          
        stream.puts add_index_statements.sort.join("\n")
        stream.puts
      end
    end

    def remove_prefix_and_suffix(table)
      table.gsub(/^(#{ActiveRecord::Base.table_name_prefix})(.+)(#{ActiveRecord::Base.table_name_suffix})$/,  "\\2")
    end
    
    def already_dumped?(table)
      @dumped_tables.include? table
    end
    
    def is_parent_index?(index, parent_indexes)
      parent_indexes.each do |pindex|
        return true if pindex.columns == index.columns
      end
      return false
    end
    
    def custom_primary_key_sequence(table, pk)
      # Get the default value of the pk, save it if its a nextval()
      # Get the sequence, return false if the sequence is owned by the table
      # else return the sequence name in schema.sequence format
    end
  end
end
