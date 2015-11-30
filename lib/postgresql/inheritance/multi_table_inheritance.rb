# Similar to STI we want to instantiate the right class type.  However unlike STI we don't have 
# a database column, we use the tableoid::regclass which is basically the source table name.
# so we convert it to a class name and use that.

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
